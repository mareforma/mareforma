"""
db.py — SQLite-backed epistemic graph for mareforma.

Tables
------
  claims : explicit scientific assertions with provenance

Schema version
--------------
  Stored in PRAGMA user_version. Current: 1.
  Version 0 → fresh db, full schema applied, user_version=1.
  Version 1 → ready to use.
  Any other version → DatabaseError — delete graph.db to start fresh.

Connection lifecycle
--------------------
  Use open_db(root) to get a connection. Close when done.

Support levels — graph-derived trust signal
-------------------------------------------
  PRELIMINARY  : one agent claimed it
  REPLICATED   : ≥2 agents with different generated_by share the same
                 upstream claim in supports[] (auto-detected at INSERT)
  ESTABLISHED  : explicit human validation via validate_claim() only
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_FILENAME = "graph.db"
_SCHEMA_VERSION = 1

VALID_STATUSES = ("open", "contested", "retracted")

VALID_CLASSIFICATIONS = ("INFERRED", "ANALYTICAL", "DERIVED")

VALID_SUPPORT_LEVELS = ("PRELIMINARY", "REPLICATED", "ESTABLISHED")

# Maps min_support value to the set of levels that satisfy it.
_SUPPORT_LEVEL_TIERS: dict[str, tuple[str, ...]] = {
    "PRELIMINARY": ("PRELIMINARY", "REPLICATED", "ESTABLISHED"),
    "REPLICATED":  ("REPLICATED", "ESTABLISHED"),
    "ESTABLISHED": ("ESTABLISHED",),
}

_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS claims (
    claim_id        TEXT PRIMARY KEY,
    text            TEXT NOT NULL,
    classification  TEXT NOT NULL DEFAULT 'INFERRED',
    support_level   TEXT NOT NULL DEFAULT 'PRELIMINARY',
    idempotency_key TEXT,
    validated_by    TEXT,
    validated_at    TEXT,
    status          TEXT NOT NULL DEFAULT 'open',
    source_name     TEXT,
    generated_by    TEXT NOT NULL DEFAULT 'human',
    supports_json   TEXT NOT NULL DEFAULT '[]',
    contradicts_json TEXT NOT NULL DEFAULT '[]',
    comparison_summary TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_claims_status
    ON claims(status);
CREATE INDEX IF NOT EXISTS idx_claims_source
    ON claims(source_name);
CREATE INDEX IF NOT EXISTS idx_claims_generated_by
    ON claims(generated_by);
CREATE INDEX IF NOT EXISTS idx_claims_support_level
    ON claims(support_level);
CREATE UNIQUE INDEX IF NOT EXISTS idx_claims_idempotency_key
    ON claims(idempotency_key) WHERE idempotency_key IS NOT NULL;
"""


# Explicit column list — avoids SELECT * coupling to schema changes.
_CLAIM_COLUMNS = (
    "claim_id", "text", "classification", "support_level",
    "idempotency_key", "validated_by", "validated_at",
    "status", "source_name", "generated_by",
    "supports_json", "contradicts_json",
    "comparison_summary", "created_at", "updated_at",
)
_CLAIM_SELECT = ", ".join(_CLAIM_COLUMNS)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class MareformaError(Exception):
    """Base exception for all mareforma errors."""


class DatabaseError(MareformaError):
    """Raised when a graph.db operation fails."""


class ClaimNotFoundError(MareformaError):
    """Raised when a claim lookup finds no matching record."""


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def _db_path(root: Path) -> Path:
    return root / ".mareforma" / DB_FILENAME


def open_db(root: Path) -> sqlite3.Connection:
    """Open (and initialise if needed) the graph database.

    Returns an open sqlite3.Connection with row_factory set to
    sqlite3.Row for dict-like access.

    Schema version
    --------------
    - version 0 : fresh db — full schema applied, user_version set to 1
    - version 1 : ready to use
    - any other : DatabaseError — delete graph.db to start fresh

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
            conn.executescript(_SCHEMA_SQL)
            conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            conn.commit()
        elif version == _SCHEMA_VERSION:
            pass  # Current version — ready to use.
        else:
            conn.close()
            raise DatabaseError(
                f"graph.db schema v{version} is not compatible with mareforma v0.3.0. "
                "Delete .mareforma/graph.db to start fresh — "
                "your claims are backed up in claims.toml."
            )
        return conn

    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Could not open database at {path}: {exc}") from exc


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_status(status: str) -> None:
    """Raise ValueError if *status* is not a recognised claim status."""
    if status not in VALID_STATUSES:
        allowed = ", ".join(VALID_STATUSES)
        raise ValueError(
            f"Unknown claim status '{status}'. Use one of: {allowed}"
        )


# ---------------------------------------------------------------------------
# Claims
# ---------------------------------------------------------------------------

def add_claim(
    conn: sqlite3.Connection,
    root: Path,
    text: str,
    *,
    classification: str = "INFERRED",
    idempotency_key: str | None = None,
    supports: list[str] | None = None,
    contradicts: list[str] | None = None,
    generated_by: str = "human",
    source_name: str | None = None,
    status: str = "open",
) -> str:
    """Insert a new claim and return its claim_id.

    Returns the existing claim_id without inserting if idempotency_key
    already exists. After insert, checks for REPLICATED: if ≥2 claims share
    the same upstream claim_id in supports[] with different generated_by,
    all are promoted to support_level='REPLICATED'.

    Parameters
    ----------
    classification:
        'INFERRED' | 'ANALYTICAL' | 'DERIVED'
    idempotency_key:
        Retry-safe writes — same key returns the same claim_id.
    supports:
        Upstream claim_ids or DOIs this claim is grounded in.
    contradicts:
        Claim_ids or DOIs this claim contests.
    generated_by:
        Agent or human identifier.
    source_name:
        Data source this claim derives from.
    status:
        Editorial status: 'open' | 'contested' | 'retracted'

    Raises
    ------
    ValueError
        If classification or status are invalid.
    """
    if not text or not text.strip():
        raise ValueError("Claim text cannot be empty.")
    if classification not in VALID_CLASSIFICATIONS:
        raise ValueError(
            f"Unknown classification '{classification}'. "
            f"Use one of: {', '.join(VALID_CLASSIFICATIONS)}"
        )
    validate_status(status)

    # Idempotency check — return existing claim_id if key already present.
    if idempotency_key is not None:
        try:
            row = conn.execute(
                "SELECT claim_id FROM claims WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row:
                return row["claim_id"]
        except sqlite3.OperationalError as exc:
            raise DatabaseError(f"Idempotency check failed: {exc}") from exc

    claim_id = str(uuid.uuid4())
    now = _now()
    supports_json = json.dumps(supports or [])
    contradicts_json = json.dumps(contradicts or [])

    try:
        conn.execute(
            """
            INSERT INTO claims
                (claim_id, text, classification, support_level, idempotency_key,
                 status, source_name, generated_by,
                 supports_json, contradicts_json, created_at, updated_at)
            VALUES (?, ?, ?, 'PRELIMINARY', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim_id, text.strip(), classification, idempotency_key,
                status, source_name, generated_by,
                supports_json, contradicts_json, now, now,
            ),
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to add claim: {exc}") from exc

    # Check whether this claim triggers REPLICATED status on shared upstreams.
    _maybe_update_replicated(conn, claim_id, supports or [], generated_by)

    _backup_claims_toml(conn, root)
    return claim_id


def _maybe_update_replicated(
    conn: sqlite3.Connection,
    new_claim_id: str,
    supports: list[str],
    generated_by: str,
) -> None:
    """Promote claims to REPLICATED when convergence is detected.

    Convergence: ≥2 claims share the same upstream claim_id in their
    supports[] and have different generated_by values. Uses json_each()
    for correct JSON array element extraction (no fragile LIKE).

    Called immediately after a successful INSERT in add_claim().
    Failures are swallowed — convergence detection must not crash writes.
    """
    if not supports:
        return
    try:
        placeholders = ",".join("?" * len(supports))
        rows = conn.execute(
            f"""
            SELECT DISTINCT c.claim_id, c.generated_by
            FROM claims c, json_each(c.supports_json) j
            WHERE j.value IN ({placeholders})
              AND c.claim_id != ?
              AND c.generated_by != ?
              AND c.support_level != 'ESTABLISHED'
            """,
            (*supports, new_claim_id, generated_by),
        ).fetchall()

        if not rows:
            return

        peer_ids = [r["claim_id"] for r in rows] + [new_claim_id]
        peer_placeholders = ",".join("?" * len(peer_ids))
        conn.execute(
            f"UPDATE claims SET support_level = 'REPLICATED', updated_at = ? "
            f"WHERE claim_id IN ({peer_placeholders})",
            (_now(), *peer_ids),
        )
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Convergence detection is best-effort — never crash a write.


def validate_claim(
    conn: sqlite3.Connection,
    claim_id: str,
    *,
    validated_by: str | None = None,
) -> None:
    """Promote a REPLICATED claim to ESTABLISHED (human validation).

    Raises
    ------
    ClaimNotFoundError
        If no claim with claim_id exists.
    ValueError
        If the claim's support_level is not 'REPLICATED'.
    """
    row = conn.execute(
        "SELECT support_level FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")
    if row["support_level"] != "REPLICATED":
        raise ValueError(
            f"Claim '{claim_id}' has support_level='{row['support_level']}'. "
            "Only REPLICATED claims can be promoted to ESTABLISHED."
        )
    now = _now()
    try:
        conn.execute(
            """
            UPDATE claims
            SET support_level = 'ESTABLISHED',
                validated_by = ?,
                validated_at = ?,
                updated_at   = ?
            WHERE claim_id = ?
            """,
            (validated_by, now, now, claim_id),
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to validate claim '{claim_id}': {exc}") from exc


def update_claim(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
    *,
    status: str | None = None,
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
        If status is invalid.
    """
    existing = get_claim(conn, claim_id)
    if existing is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")

    new_status = existing["status"]
    new_text = existing["text"]
    new_supports_json = existing.get("supports_json", "[]")
    new_contradicts_json = existing.get("contradicts_json", "[]")
    new_comparison_summary = existing.get("comparison_summary")

    if status is not None:
        validate_status(status)
        new_status = status
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
            SET text = ?, status = ?, supports_json = ?, contradicts_json = ?,
                comparison_summary = ?, updated_at = ?
            WHERE claim_id = ?
            """,
            (
                new_text, new_status,
                new_supports_json, new_contradicts_json,
                new_comparison_summary, _now(), claim_id,
            ),
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to update claim '{claim_id}': {exc}") from exc

    _backup_claims_toml(conn, root)


def delete_claim(conn: sqlite3.Connection, root: Path, claim_id: str) -> None:
    """Delete a claim.

    Raises
    ------
    ClaimNotFoundError
        If no claim with *claim_id* exists.
    """
    if get_claim(conn, claim_id) is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")
    try:
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


def delete_claims_by_generated_by(
    conn: sqlite3.Connection,
    root: Path,
    generated_by: str,
) -> int:
    """Delete all claims with the given generated_by tag.

    Returns the number of claims deleted.
    """
    try:
        rows = conn.execute(
            "SELECT claim_id FROM claims WHERE generated_by = ?",
            (generated_by,),
        ).fetchall()
        claim_ids = [r[0] for r in rows]
        if not claim_ids:
            return 0
        placeholders = ",".join("?" * len(claim_ids))
        conn.execute(
            f"DELETE FROM claims WHERE claim_id IN ({placeholders})", claim_ids
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to delete claims: {exc}") from exc

    _backup_claims_toml(conn, root)
    return len(claim_ids)


def query_claims(
    conn: sqlite3.Connection,
    *,
    limit: int = 10,
    text: str | None = None,
    min_support: str | None = None,
    classification: str | None = None,
) -> list[dict]:
    """Return claims ordered by support_level (desc) then recency (desc).

    Parameters
    ----------
    limit:
        Maximum number of claims to return. Default 10.
    text:
        Optional substring filter — case-insensitive LIKE match on claim text.
    min_support:
        Minimum support level: 'PRELIMINARY' | 'REPLICATED' | 'ESTABLISHED'.
    classification:
        Filter by classification: 'INFERRED' | 'ANALYTICAL' | 'DERIVED'.
    """
    conditions: list[str] = []
    params: list = []

    if text is not None:
        conditions.append("text LIKE ?")
        params.append(f"%{text}%")

    if min_support is not None:
        if min_support not in VALID_SUPPORT_LEVELS:
            raise ValueError(
                f"Unknown min_support '{min_support}'. "
                f"Use one of: {', '.join(VALID_SUPPORT_LEVELS)}"
            )
        tiers = _SUPPORT_LEVEL_TIERS[min_support]
        tier_placeholders = ",".join("?" * len(tiers))
        conditions.append(f"support_level IN ({tier_placeholders})")
        params.extend(tiers)

    if classification is not None:
        if classification not in VALID_CLASSIFICATIONS:
            raise ValueError(
                f"Unknown classification '{classification}'. "
                f"Use one of: {', '.join(VALID_CLASSIFICATIONS)}"
            )
        conditions.append("classification = ?")
        params.append(classification)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    try:
        rows = conn.execute(
            f"SELECT {_CLAIM_SELECT} FROM claims {where} "
            f"ORDER BY CASE support_level "
            f"WHEN 'ESTABLISHED' THEN 3 WHEN 'REPLICATED' THEN 2 ELSE 1 END DESC, "
            f"created_at DESC LIMIT ?",
            params,
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to query claims: {exc}") from exc
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _backup_claims_toml(conn: sqlite3.Connection, root: Path) -> None:
    """Write all claims to claims.toml in the project root.

    Called after every claim mutation (add, update, delete).
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
                "classification": c.get("classification") or "INFERRED",
                "support_level": c.get("support_level") or "PRELIMINARY",
                "generated_by": c.get("generated_by", "human"),
                "status": c["status"],
                "supports": supports,
                "contradicts": contradicts,
                "comparison_summary": c.get("comparison_summary") or "",
                "created_at": c["created_at"],
                "updated_at": c["updated_at"],
            }
            if c.get("source_name"):
                entry["source_name"] = c["source_name"]
            if c.get("validated_by"):
                entry["validated_by"] = c["validated_by"]
            if c.get("validated_at"):
                entry["validated_at"] = c["validated_at"]
            data["claims"][c["claim_id"]] = entry

        out = root / "claims.toml"
        out.write_bytes(tomli_w.dumps(data).encode("utf-8"))

    except Exception as exc:  # noqa: BLE001
        import warnings
        warnings.warn(f"claims.toml backup failed (claim is saved in graph.db): {exc}")
