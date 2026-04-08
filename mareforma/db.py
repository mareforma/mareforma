"""
db.py — SQLite-backed provenance and epistemic graph for mareforma.

Tables
------
  transform_runs  : one row per @transform execution
  transform_deps  : DAG edges (transform_name → depends_on_name)
  artifacts       : artifacts saved via ctx.save() per run
  claims          : explicit scientific assertions
  evidence        : links from claims to transform runs or artifacts
  build_meta      : key-value store for build-level metadata

Schema version
--------------
  Stored in PRAGMA user_version. Current: 1.
  Version 0 → fresh db, full schema applied, user_version=1.
  Version >1 → DatabaseError with upgrade guidance.

Connection lifecycle
--------------------
  Use open_db(root) to get a connection. Close when done.
  For a build: one connection for the whole build, closed in a finally block.

  ┌─ runner.run() ─────────────────────────────────────────────────────────┐
  │  conn = open_db(root)                                                   │
  │  try:                                                                   │
  │    for record:                                                          │
  │      begin_run(conn, ...)      → transform_runs row (status=running)    │
  │      ctx = BuildContext(..., run_id=run_id, db=conn)                    │
  │      record.fn(ctx)            → ctx.claim() writes claims + evidence   │
  │      record_artifacts(conn, ..) → artifacts rows                        │
  │      end_run(conn, ...)        → update transform_runs row              │
  │  finally:                                                               │
  │    conn.close()                → SIGINT / success / exception all close │
  └─────────────────────────────────────────────────────────────────────────┘

Confidence scale (categorical → internal float)
------------------------------------------------
  anecdotal   → 0.20  : single observation, no systematic analysis
  exploratory → 0.40  : systematic, single dataset, not replicated
  preliminary → 0.60  : internally replicated or consistent across subsets
  supported   → 0.80  : externally replicated or large N
  established → 0.95  : multiple independent replications

ERD
---
  transform_runs ──< artifacts      (run_id FK)
  transform_runs ──< evidence       (run_id FK, nullable)
  claims         ──< evidence       (claim_id FK)
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mareforma.registry import MareformaError


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_FILENAME = "graph.db"
_SCHEMA_VERSION = 1

CONFIDENCE_SCALE: dict[str, float] = {
    "anecdotal": 0.20,
    "exploratory": 0.40,
    "preliminary": 0.60,
    "supported": 0.80,
    "established": 0.95,
}

VALID_STATUSES = ("open", "supported", "contested", "retracted")

VALID_REPLICATION_STATUSES = (
    "unknown",
    "single_study",
    "independently_replicated",
    "failed_replication",
    "meta_analyzed",
)

_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS transform_runs (
    run_id           TEXT PRIMARY KEY,
    transform_name   TEXT NOT NULL,
    input_hash       TEXT NOT NULL,
    source_hash      TEXT NOT NULL,
    output_hash      TEXT,
    status           TEXT NOT NULL DEFAULT 'running',
    error_message    TEXT,
    duration_ms      INTEGER,
    timestamp        TEXT NOT NULL,
    transform_class  TEXT,
    class_confidence REAL,
    class_method     TEXT,
    class_reason     TEXT
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        TEXT NOT NULL REFERENCES transform_runs(run_id),
    artifact_name TEXT NOT NULL,
    path          TEXT NOT NULL,
    format        TEXT,
    sha256        TEXT,
    size_bytes    INTEGER,
    schema_json   TEXT,
    timestamp     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS claims (
    claim_id              TEXT PRIMARY KEY,
    text                  TEXT NOT NULL,
    confidence            TEXT NOT NULL DEFAULT 'exploratory',
    confidence_float      REAL NOT NULL DEFAULT 0.4,
    generation_method     TEXT NOT NULL DEFAULT 'explicit',
    status                TEXT NOT NULL DEFAULT 'open',
    replication_status    TEXT NOT NULL DEFAULT 'unknown',
    source_name           TEXT,
    generated_by          TEXT NOT NULL DEFAULT 'human',
    supports_json         TEXT NOT NULL DEFAULT '[]',
    contradicts_json      TEXT NOT NULL DEFAULT '[]',
    comparison_summary    TEXT,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS evidence (
    evidence_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_id      TEXT NOT NULL REFERENCES claims(claim_id),
    run_id        TEXT REFERENCES transform_runs(run_id),
    artifact_name TEXT,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS build_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE INDEX IF NOT EXISTS idx_claims_status
    ON claims(status);
CREATE INDEX IF NOT EXISTS idx_claims_source
    ON claims(source_name);
CREATE INDEX IF NOT EXISTS idx_claims_generated_by
    ON claims(generated_by);
CREATE INDEX IF NOT EXISTS idx_transform_runs_name
    ON transform_runs(transform_name);
CREATE INDEX IF NOT EXISTS idx_transform_runs_status
    ON transform_runs(status);
CREATE INDEX IF NOT EXISTS idx_transform_runs_output_hash
    ON transform_runs(output_hash);

CREATE TABLE IF NOT EXISTS transform_deps (
    transform_name  TEXT NOT NULL,
    depends_on_name TEXT NOT NULL,
    PRIMARY KEY (transform_name, depends_on_name)
);
"""

# Explicit column list for list_claims() — avoids SELECT * coupling to schema.
_CLAIM_COLUMNS = (
    "claim_id", "text", "confidence", "confidence_float",
    "generation_method", "status", "replication_status", "source_name",
    "generated_by", "supports_json", "contradicts_json",
    "comparison_summary", "created_at", "updated_at",
)
_CLAIM_SELECT = ", ".join(_CLAIM_COLUMNS)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class DatabaseError(MareformaError):
    """Raised when a graph.db operation fails."""


class ClaimNotFoundError(MareformaError):
    """Raised when a claim lookup finds no matching record."""


class ContextError(MareformaError):
    """Raised when ctx.claim() is called outside a @transform context."""


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def _db_path(root: Path) -> Path:
    return root / ".mareforma" / DB_FILENAME


def open_db(root: Path) -> sqlite3.Connection:
    """Open (and initialise if needed) the graph database.

    Returns an open sqlite3.Connection with row_factory set to
    sqlite3.Row for dict-like access.

    Schema migration
    ----------------
    - version 0 : fresh db — apply full schema, set user_version=1
    - version 1 : ready to use
    - version >1: DatabaseError — upgrade mareforma

    Raises
    ------
    DatabaseError
        On SQLite errors or unsupported schema version.
    """
    path = _db_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        conn = sqlite3.connect(str(path), check_same_thread=False)
        conn.row_factory = sqlite3.Row

        version = conn.execute("PRAGMA user_version").fetchone()[0]

        if version == 0:
            # Fresh database — apply full schema and set version.
            conn.executescript(_SCHEMA_SQL)
            conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            conn.commit()
        elif version == _SCHEMA_VERSION:
            pass  # Current version — ready to use.
        else:
            conn.close()
            raise DatabaseError(
                f"graph.db schema v{version} is newer than this mareforma supports. "
                "Upgrade with: pip install --upgrade mareforma"
            )
        return conn

    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Could not open database at {path}: {exc}") from exc


# ---------------------------------------------------------------------------
# DAG dependency recording
# ---------------------------------------------------------------------------

def record_deps(
    conn: sqlite3.Connection,
    transform_name: str,
    depends_on: list[str],
) -> None:
    """Persist the DAG edges for *transform_name* into transform_deps.

    Uses INSERT OR IGNORE — idempotent across re-runs of the same build.
    Called from runner.py immediately after begin_run().

    Parameters
    ----------
    transform_name:
        The name of the transform whose dependencies are being recorded.
    depends_on:
        List of transform names this transform directly depends on.
    """
    try:
        conn.executemany(
            "INSERT OR IGNORE INTO transform_deps (transform_name, depends_on_name) "
            "VALUES (?, ?)",
            [(transform_name, dep) for dep in depends_on],
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(
            f"Failed to record deps for '{transform_name}': {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_confidence(confidence: str) -> float:
    """Return the internal float for *confidence* category.

    Raises
    ------
    ValueError
        If *confidence* is not a recognised category name.
    """
    if confidence not in CONFIDENCE_SCALE:
        allowed = ", ".join(CONFIDENCE_SCALE)
        raise ValueError(
            f"Unknown confidence level '{confidence}'. "
            f"Use one of: {allowed}"
        )
    return CONFIDENCE_SCALE[confidence]


def validate_status(status: str) -> None:
    """Raise ValueError if *status* is not a recognised claim status."""
    if status not in VALID_STATUSES:
        allowed = ", ".join(VALID_STATUSES)
        raise ValueError(
            f"Unknown claim status '{status}'. Use one of: {allowed}"
        )


def validate_replication_status(replication_status: str) -> None:
    """Raise ValueError if *replication_status* is not recognised."""
    if replication_status not in VALID_REPLICATION_STATUSES:
        allowed = ", ".join(VALID_REPLICATION_STATUSES)
        raise ValueError(
            f"Unknown replication status '{replication_status}'. "
            f"Use one of: {allowed}"
        )


# ---------------------------------------------------------------------------
# Transform run lifecycle
# ---------------------------------------------------------------------------

def is_stale(
    conn: sqlite3.Connection,
    transform_name: str,
    input_hash: str,
    source_hash: str,
    *,
    force: bool = False,
) -> bool:
    """Return True if the transform needs to run.

    A transform is stale if:
      1. No previous successful run exists in the database
      2. The input_hash has changed (raw data changed)
      3. The source_hash has changed (transform code changed)
      4. ``force`` is True
    """
    if force:
        return True
    try:
        row = conn.execute(
            """
            SELECT input_hash, source_hash FROM transform_runs
            WHERE transform_name = ? AND status = 'success'
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (transform_name,),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Staleness check failed for '{transform_name}': {exc}") from exc

    if row is None:
        return True  # never run successfully
    if row["input_hash"] != input_hash:
        return True  # raw data changed
    if row["source_hash"] != source_hash:
        return True  # transform code changed
    return False


def begin_run(
    conn: sqlite3.Connection,
    run_id: str,
    transform_name: str,
    input_hash: str,
    source_hash: str,
) -> None:
    """Insert a transform_runs row with status='running'."""
    now = _now()
    try:
        conn.execute(
            """
            INSERT INTO transform_runs
                (run_id, transform_name, input_hash, source_hash, status, timestamp)
            VALUES (?, ?, ?, ?, 'running', ?)
            """,
            (run_id, transform_name, input_hash, source_hash, now),
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to begin run for '{transform_name}': {exc}") from exc


def end_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    status: str,
    output_hash: str = "",
    duration_ms: int = 0,
    error_message: str | None = None,
) -> None:
    """Update the transform_runs row when a transform finishes."""
    try:
        conn.execute(
            """
            UPDATE transform_runs
            SET status = ?, output_hash = ?, duration_ms = ?, error_message = ?
            WHERE run_id = ?
            """,
            (status, output_hash, duration_ms, error_message, run_id),
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to end run '{run_id}': {exc}") from exc


def record_artifact(
    conn: sqlite3.Connection,
    run_id: str,
    artifact_name: str,
    path: Path,
    fmt: str,
    *,
    sha256: str | None = None,
    size_bytes: int | None = None,
    schema: dict | None = None,
) -> None:
    """Record an artifact saved by ctx.save()."""
    now = _now()
    schema_json = json.dumps(schema) if schema else None
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO artifacts
                (run_id, artifact_name, path, format, sha256, size_bytes,
                 schema_json, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, artifact_name, str(path), fmt,
                sha256, size_bytes, schema_json, now,
            ),
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to record artifact '{artifact_name}': {exc}") from exc


# ---------------------------------------------------------------------------
# Transform classification
# ---------------------------------------------------------------------------

def write_transform_class(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    transform_class: str,
    class_confidence: float,
    class_method: str,
    class_reason: str,
) -> None:
    """Write classification result for a completed run.

    Called by inspector.classify_run() after content inspection.

    Parameters
    ----------
    transform_class:
        One of: 'raw', 'processed', 'analysed', 'inferred', 'unknown'.
    class_confidence:
        0.0–1.0 confidence in the classification.
    class_method:
        How classification was determined: 'content_inspection', 'heuristic', 'manual'.
    class_reason:
        Human-readable explanation (capped at 500 chars by caller).
    """
    try:
        conn.execute(
            """
            UPDATE transform_runs
            SET transform_class = ?, class_confidence = ?,
                class_method = ?, class_reason = ?
            WHERE run_id = ?
            """,
            (transform_class, class_confidence, class_method, class_reason, run_id),
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(
            f"Failed to write transform class for run '{run_id}': {exc}"
        ) from exc


def lookup_cached_class(
    conn: sqlite3.Connection,
    output_hash: str,
) -> tuple[str, float, str, str] | None:
    """Return cached classification for *output_hash* if available.

    Uses idx_transform_runs_output_hash for O(log N) lookup.

    Returns
    -------
    (transform_class, class_confidence, class_method, class_reason)
        or None if no cached non-unknown classification exists.
    """
    if not output_hash:
        return None
    try:
        row = conn.execute(
            """
            SELECT transform_class, class_confidence, class_method, class_reason
            FROM transform_runs
            WHERE output_hash = ?
              AND transform_class IS NOT NULL
              AND transform_class != 'unknown'
            LIMIT 1
            """,
            (output_hash,),
        ).fetchone()
        if row:
            return (
                row["transform_class"],
                row["class_confidence"] or 0.0,
                row["class_method"] or "content_inspection",
                row["class_reason"] or "",
            )
        return None
    except sqlite3.OperationalError:
        return None


def get_artifact_paths(
    conn: sqlite3.Connection,
    run_id: str,
) -> list[str]:
    """Return all artifact paths recorded for *run_id*."""
    try:
        rows = conn.execute(
            "SELECT path FROM artifacts WHERE run_id = ? ORDER BY artifact_id",
            (run_id,),
        ).fetchall()
        return [row["path"] for row in rows]
    except sqlite3.OperationalError:
        return []


def get_artifacts_for_run(
    conn: sqlite3.Connection,
    run_id: str,
) -> list[dict]:
    """Return all artifacts recorded for *run_id* with name, path, sha256, format, size.

    Used by ``mareforma cross-diff`` to compare artifacts across two transform runs.
    """
    try:
        rows = conn.execute(
            """
            SELECT artifact_name, path, format, sha256, size_bytes
            FROM artifacts
            WHERE run_id = ?
            ORDER BY artifact_name
            """,
            (run_id,),
        ).fetchall()
        return [dict(row) for row in rows]
    except sqlite3.OperationalError:
        return []


def get_parent_artifact_paths(
    conn: sqlite3.Connection,
    transform_name: str,
) -> list[str]:
    """Return artifact paths from the most recent successful run of each parent transform.

    Used by inspector to get input files for content comparison.
    """
    try:
        # Get direct parents from transform_deps
        parents = conn.execute(
            "SELECT depends_on_name FROM transform_deps WHERE transform_name = ?",
            (transform_name,),
        ).fetchall()

        paths: list[str] = []
        for (parent_name,) in parents:
            # Most recent successful run for this parent
            row = conn.execute(
                """
                SELECT run_id FROM transform_runs
                WHERE transform_name = ? AND status = 'success'
                ORDER BY timestamp DESC LIMIT 1
                """,
                (parent_name,),
            ).fetchone()
            if row:
                parent_paths = get_artifact_paths(conn, row["run_id"])
                paths.extend(parent_paths)
        return paths
    except sqlite3.OperationalError:
        return []


# ---------------------------------------------------------------------------
# Build metadata
# ---------------------------------------------------------------------------

def set_build_meta(
    conn: sqlite3.Connection,
    *,
    timestamp: str,
    git_sha: str | None,
) -> None:
    """Store build-level metadata (written by CLI after runner finishes)."""
    try:
        conn.execute(
            "INSERT OR REPLACE INTO build_meta (key, value) VALUES ('last_build_timestamp', ?)",
            (timestamp,),
        )
        conn.execute(
            "INSERT OR REPLACE INTO build_meta (key, value) VALUES ('last_git_sha', ?)",
            (git_sha,),
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to write build metadata: {exc}") from exc


def get_build_meta(conn: sqlite3.Connection) -> dict[str, str | None]:
    """Return last build timestamp and git_sha (or None if never built)."""
    try:
        rows = conn.execute(
            "SELECT key, value FROM build_meta WHERE key IN "
            "('last_build_timestamp', 'last_git_sha')"
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to read build metadata: {exc}") from exc
    meta: dict[str, str | None] = {"last_build_timestamp": None, "last_git_sha": None}
    for row in rows:
        meta[row["key"]] = row["value"]
    return meta


def all_transform_runs(conn: sqlite3.Connection) -> dict[str, dict]:
    """Return the latest run record for each transform, keyed by transform_name.

    Used by ``mareforma log``.
    """
    try:
        rows = conn.execute(
            """
            SELECT t1.transform_name, t1.status, t1.duration_ms,
                   t1.timestamp, t1.error_message
            FROM transform_runs t1
            WHERE t1.timestamp = (
                SELECT MAX(t2.timestamp)
                FROM transform_runs t2
                WHERE t2.transform_name = t1.transform_name
            )
            ORDER BY t1.transform_name
            """
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to read transform runs: {exc}") from exc
    return {
        row["transform_name"]: {
            "status": row["status"],
            "duration_ms": row["duration_ms"] or 0,
            "timestamp": row["timestamp"],
            "error_message": row["error_message"],
        }
        for row in rows
    }


def get_runs_for_transform(
    conn: sqlite3.Connection,
    transform_name: str,
    limit: int | None = None,
) -> list[dict]:
    """Return all runs for *transform_name* ordered by timestamp DESC.

    Used by ``mareforma diff``.

    Parameters
    ----------
    limit:
        If provided, return at most this many rows (e.g. 2 for diff).
    """
    query = """
        SELECT run_id, transform_name, status, input_hash, source_hash,
               output_hash, duration_ms, timestamp, error_message
        FROM transform_runs
        WHERE transform_name = ?
        ORDER BY timestamp DESC
    """
    params: list = [transform_name]
    if limit is not None:
        query += " LIMIT ?"
        params.append(int(limit))
    try:
        rows = conn.execute(query, params).fetchall()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(
            f"Failed to read runs for '{transform_name}': {exc}"
        ) from exc
    return [dict(row) for row in rows]


def get_unclaimed_transforms(conn: sqlite3.Connection) -> list[str]:
    """Return transform names with successful runs but no evidence rows.

    Used by health.py and ctx.claim() warnings.
    """
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT transform_name FROM transform_runs
            WHERE status = 'success'
            AND run_id NOT IN (
                SELECT DISTINCT run_id FROM evidence
                WHERE run_id IS NOT NULL
            )
            ORDER BY transform_name
            """
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to get unclaimed transforms: {exc}") from exc
    return [row["transform_name"] for row in rows]


# ---------------------------------------------------------------------------
# Claims
# ---------------------------------------------------------------------------

def add_claim(
    conn: sqlite3.Connection,
    root: Path,
    text: str,
    *,
    confidence: str = "exploratory",
    status: str = "open",
    replication_status: str = "unknown",
    source_name: str | None = None,
    run_id: str | None = None,
    artifact_name: str | None = None,
    generated_by: str = "human",
    generation_method: str = "explicit",
    supports: list[str] | None = None,
    contradicts: list[str] | None = None,
) -> str:
    """Insert a new claim and return its claim_id.

    Also writes a claims.toml backup and links evidence if run_id is provided.

    Parameters
    ----------
    supports:
        List of DOI strings or claim_ids this claim rests on.
    contradicts:
        List of DOI strings or claim_ids this claim contests.
    generated_by:
        'human' or a model identifier string.
    generation_method:
        'explicit' | 'agent-wrapped' | 'inferred'

    Raises
    ------
    ValueError
        If confidence, status, or replication_status values are invalid.
    """
    if not text or not text.strip():
        raise ValueError("Claim text cannot be empty.")
    confidence_float = validate_confidence(confidence)
    validate_status(status)
    validate_replication_status(replication_status)

    claim_id = str(uuid.uuid4())
    now = _now()
    supports_json = json.dumps(supports or [])
    contradicts_json = json.dumps(contradicts or [])

    try:
        conn.execute(
            """
            INSERT INTO claims
                (claim_id, text, confidence, confidence_float, generation_method,
                 status, replication_status, source_name, generated_by,
                 supports_json, contradicts_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim_id, text.strip(), confidence, confidence_float,
                generation_method, status, replication_status,
                source_name, generated_by,
                supports_json, contradicts_json, now, now,
            ),
        )
        if run_id is not None:
            conn.execute(
                """
                INSERT INTO evidence (claim_id, run_id, artifact_name, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (claim_id, run_id, artifact_name, now),
            )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to add claim: {exc}") from exc

    _backup_claims_toml(conn, root)
    return claim_id


def update_claim(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
    *,
    confidence: str | None = None,
    status: str | None = None,
    replication_status: str | None = None,
    text: str | None = None,
    supports: list[str] | None = None,
    contradicts: list[str] | None = None,
    comparison_summary: str | None = None,
) -> None:
    """Update fields on an existing claim.

    Raises
    ------
    ClaimNotFoundError
        If no claim with *claim_id* exists.
    ValueError
        If any updated field has an invalid value.
    """
    existing = get_claim(conn, claim_id)
    if existing is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")

    new_confidence = existing["confidence"]
    new_confidence_float = existing["confidence_float"]
    new_status = existing["status"]
    new_replication_status = existing["replication_status"]
    new_text = existing["text"]
    new_supports_json = existing.get("supports_json", "[]")
    new_contradicts_json = existing.get("contradicts_json", "[]")
    new_comparison_summary = existing.get("comparison_summary")

    if confidence is not None:
        new_confidence_float = validate_confidence(confidence)
        new_confidence = confidence
    if status is not None:
        validate_status(status)
        new_status = status
    if replication_status is not None:
        validate_replication_status(replication_status)
        new_replication_status = replication_status
    if text is not None:
        if not text.strip():
            raise ValueError("Claim text cannot be empty.")
        new_text = text.strip()
    if supports is not None:
        new_supports_json = json.dumps(supports)
    if contradicts is not None:
        new_contradicts_json = json.dumps(contradicts)
    if comparison_summary is not None:
        new_comparison_summary = comparison_summary

    try:
        conn.execute(
            """
            UPDATE claims
            SET text = ?, confidence = ?, confidence_float = ?, status = ?,
                replication_status = ?, supports_json = ?, contradicts_json = ?,
                comparison_summary = ?, updated_at = ?
            WHERE claim_id = ?
            """,
            (
                new_text, new_confidence, new_confidence_float,
                new_status, new_replication_status,
                new_supports_json, new_contradicts_json,
                new_comparison_summary, _now(), claim_id,
            ),
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to update claim '{claim_id}': {exc}") from exc

    _backup_claims_toml(conn, root)


def delete_claim(conn: sqlite3.Connection, root: Path, claim_id: str) -> None:
    """Delete a claim and its evidence links.

    Raises
    ------
    ClaimNotFoundError
        If no claim with *claim_id* exists.
    """
    if get_claim(conn, claim_id) is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")
    try:
        conn.execute("DELETE FROM evidence WHERE claim_id = ?", (claim_id,))
        conn.execute("DELETE FROM claims WHERE claim_id = ?", (claim_id,))
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to delete claim '{claim_id}': {exc}") from exc

    _backup_claims_toml(conn, root)


def get_claim(conn: sqlite3.Connection, claim_id: str) -> dict | None:
    """Return a claim dict or None if not found."""
    try:
        row = conn.execute(
            f"SELECT {_CLAIM_SELECT} FROM claims WHERE claim_id = ?",
            (claim_id,),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to fetch claim '{claim_id}': {exc}") from exc
    return dict(row) if row else None


def list_claims(
    conn: sqlite3.Connection,
    *,
    status: str | None = None,
    source_name: str | None = None,
    generated_by: str | None = None,
) -> list[dict]:
    """Return all claims, optionally filtered.

    Uses an explicit column list (not SELECT *) to avoid coupling to schema changes.
    """
    conditions: list[str] = []
    params: list[Any] = []
    if status is not None:
        conditions.append("status = ?")
        params.append(status)
    if source_name is not None:
        conditions.append("source_name = ?")
        params.append(source_name)
    if generated_by is not None:
        conditions.append("generated_by = ?")
        params.append(generated_by)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    try:
        rows = conn.execute(
            f"SELECT {_CLAIM_SELECT} FROM claims {where} ORDER BY created_at DESC",
            params,
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to list claims: {exc}") from exc
    return [dict(row) for row in rows]


def list_claims_with_evidence(conn: sqlite3.Connection, claim_id: str) -> list[dict]:
    """Return evidence rows for a specific claim."""
    try:
        rows = conn.execute(
            "SELECT * FROM evidence WHERE claim_id = ? ORDER BY created_at",
            (claim_id,),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to fetch evidence for '{claim_id}': {exc}") from exc
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Migration from pipeline.lock.json (legacy format)
# ---------------------------------------------------------------------------

def migrate_from_lock_json(conn: sqlite3.Connection, root: Path) -> bool:
    """Import pipeline.lock.json into graph.db if present.

    Returns True if migration ran, False if skipped (no lock file or already done).
    No-op for fresh installs that never had a lock file.
    """
    lock_path = root / ".mareforma" / "pipeline.lock.json"
    bak_path = root / ".mareforma" / "pipeline.lock.json.bak"

    if bak_path.exists() or not lock_path.exists():
        return False

    try:
        data = json.loads(lock_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return False  # corrupt or unreadable — skip, don't crash

    nodes = data.get("nodes", {})
    build_ts = data.get("build_timestamp")
    git_sha = data.get("git_sha")

    try:
        for name, node in nodes.items():
            run_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT OR IGNORE INTO transform_runs
                    (run_id, transform_name, input_hash, source_hash,
                     output_hash, status, duration_ms, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id, name,
                    node.get("input_hash", ""),
                    node.get("source_hash", ""),
                    node.get("output_hash", ""),
                    node.get("status", "unknown"),
                    node.get("duration_ms", 0),
                    node.get("timestamp", _now()),
                ),
            )
        if build_ts:
            conn.execute(
                "INSERT OR REPLACE INTO build_meta (key, value) VALUES "
                "('last_build_timestamp', ?)",
                (build_ts,),
            )
        if git_sha:
            conn.execute(
                "INSERT OR REPLACE INTO build_meta (key, value) VALUES "
                "('last_git_sha', ?)",
                (git_sha,),
            )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Migration from lock.json failed: {exc}") from exc

    # Rename only after successful SQLite writes (atomicity guarantee).
    try:
        lock_path.rename(bak_path)
    except OSError:
        pass  # Rename failed — next run will detect existing rows and skip.

    return True


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _backup_claims_toml(conn: sqlite3.Connection, root: Path) -> None:
    """Write all claims to claims.toml in the project root.

    Called after every claim mutation (add, update, delete).
    Uses an explicit column list to avoid coupling to schema changes.
    Failure is non-fatal: a warning is printed but the exception is not raised.
    """
    try:
        import tomli_w

        claims = list_claims(conn)
        data: dict[str, Any] = {"claims": {}}
        for c in claims:
            supports = json.loads(c.get("supports_json", "[]") or "[]")
            contradicts = json.loads(c.get("contradicts_json", "[]") or "[]")
            entry: dict[str, Any] = {
                "text": c["text"],
                "confidence": c["confidence"],
                "generation_method": c.get("generation_method", "explicit"),
                "generated_by": c.get("generated_by", "human"),
                "status": c["status"],
                "replication_status": c["replication_status"],
                "supports": supports,
                "contradicts": contradicts,
                "comparison_summary": c.get("comparison_summary") or "",
                "created_at": c["created_at"],
                "updated_at": c["updated_at"],
            }
            if c.get("source_name"):
                entry["source_name"] = c["source_name"]
            data["claims"][c["claim_id"]] = entry

        out = root / "claims.toml"
        out.write_bytes(tomli_w.dumps(data).encode("utf-8"))

    except Exception as exc:  # noqa: BLE001
        import warnings
        warnings.warn(f"claims.toml backup failed (claim is saved in graph.db): {exc}")


# ---------------------------------------------------------------------------
# Utility (moved from pipeline/lock.py)
# ---------------------------------------------------------------------------

def hash_string(s: str) -> str:
    """Return SHA-256 hex digest of a UTF-8 string."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()
