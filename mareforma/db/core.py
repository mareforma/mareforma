"""
core.py: live-write path, queries, verdicts, and TOML backup.

Schema DDL in ``_schema_sql.py``; exceptions in ``errors.py``;
``restore()`` in ``restore.py``. Everything else stays here because
the threat-model locality is load-bearing: every defensive measure
names the threat it blocks, and the callers that must hold
``BEGIN IMMEDIATE`` together live in one buffer.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import sqlite3
import tempfile
import uuid
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from ._schema_sql import (  # noqa: F401
    _ADDITIVE_TABLES_SQL,
    _CLAIM_COLUMNS,
    _CLAIM_SELECT,
    _MANAGED_TRIGGERS,
    _PROMOTION_MARKER_TABLE,
    _SCHEMA_SQL,
    _SIGNED_FIELDS_TRIGGER_NAME,
    _SIGNED_FIELDS_TRIGGER_SQL,
)
from .errors import (  # noqa: F401
    MareformaError,
    DatabaseError,
    ScanCeilingReached,
    ClaimNotFoundError,
    UnverifiedClaimError,
    SignedClaimImmutableError,
    IdempotencyConflictError,
    IllegalStateTransitionError,
    ChainIntegrityError,
    LLMValidatorPromotionError,
    SelfValidationError,
    EvidenceCitationError,
    InvalidValidationEnvelopeError,
    RestoreError,
    CycleDetectedError,
    GraphTooLargeError,
    VerdictIssuerError,
)

_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_FILENAME = "graph.db"
_SCHEMA_VERSION = 1

# Hard cap on a single claim's ``text`` field. 100k chars covers any
# realistic scientific finding (≈ a 15k-word paragraph) and matches the
# truncation point in ``prompt_safety._MAX_FIELD_LEN`` so claim text
# never silently degrades when consumed by an LLM. A multi-MB claim is
# either a bug or a write-side DoS attempt; rejecting is the simpler
# defence than silently truncating.
_MAX_CLAIM_TEXT_LEN = 100_000

VALID_STATUSES = ("open", "contested", "retracted")

VALID_CLASSIFICATIONS = ("INFERRED", "ANALYTICAL", "DERIVED")

VALID_SUPPORT_LEVELS = ("PRELIMINARY", "REPLICATED", "ESTABLISHED")

# Maps min_support value to the set of levels that satisfy it.
_SUPPORT_LEVEL_TIERS: dict[str, tuple[str, ...]] = {
    "PRELIMINARY": ("PRELIMINARY", "REPLICATED", "ESTABLISHED"),
    "REPLICATED":  ("REPLICATED", "ESTABLISHED"),
    "ESTABLISHED": ("ESTABLISHED",),
}



def _serialize_predicate_payload(payload: dict | None) -> str:
    """Serialize an adapter's structured predicate_payload for storage.

    Canonical JSON (sorted keys, NFC Unicode, no whitespace, ``allow_nan=False``)
    so the column round-trips byte-stably across writers. ``None`` becomes
    the empty string to match the column's ``DEFAULT ''`` so callers that
    never pass this kwarg write the same bytes they did before. The active
    signed envelope is the authoritative copy of the predicate body; this
    column is the queryable denormalisation.

    Raises :class:`TypeError` if payload is non-dict. Adapters MUST pass
    a JSON-object-shaped dict (the typed predicate body); passing a
    string, list, int, or other non-object JSON value would serialize
    successfully but break mareforma's "predicate body is a dict"
    contract that downstream consumers (eg. PROV-O exporter,
    role-attestation walker) assume.
    """
    if payload is None:
        return ""
    if not isinstance(payload, dict):
        raise TypeError(
            f"predicate_payload must be a dict (the typed predicate body), "
            f"got {type(payload).__name__}. Wrap non-dict values in a dict "
            "with a documented key, e.g. {'value': <your value>}."
        )
    from .._canonical import canonicalize
    return canonicalize(payload).decode("utf-8")


def _serialize_observed_grounding(record: dict | None) -> str | None:
    """Serialize the observed-grounding record for its queryable column.

    Canonical JSON so the column round-trips byte-stably and matches the same
    record bound into the signed predicate. ``None`` stays NULL, the column
    default, so a claim asserted without the observer writes exactly the bytes
    it did before this field existed. The signed envelope is authoritative; this
    column is the denormalisation the split measurement and the promotion gate
    read.
    """
    if record is None:
        return None
    if not isinstance(record, dict):
        raise TypeError(
            f"observed_grounding must be a dict (the computed verdict record), "
            f"got {type(record).__name__}."
        )
    from .._canonical import canonicalize
    return canonicalize(record).decode("utf-8")


def _observed_grounding_promotes(stored: str | None) -> bool:
    """Whether a stored observed-grounding column permits support-level promotion.

    A NULL column (every claim asserted without the observer) is unaffected and
    promotes as before. A recorded verdict promotes only when it is GROUNDED;
    UNGROUNDED and OPAQUE never count toward promotion. Any non-NULL value that
    is not GROUNDED JSON (including an empty string) is non-promoting
    (fail-closed): a verdict we cannot read is not a GROUNDED one. This matches
    the peer-promotion SQL guard, which excludes a non-``json_valid`` column.
    """
    if stored is None:
        return True
    try:
        record = json.loads(stored)
        return record.get("grounding") == "GROUNDED"
    except (ValueError, TypeError, AttributeError):
        return False


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

class _GraphConnection(sqlite3.Connection):
    """A ``sqlite3.Connection`` that permits attribute storage.

    Stdlib ``sqlite3.Connection`` rejects arbitrary attribute assignment,
    which silently disabled the per-connection chain-verification cache in
    :func:`mareforma.validators._conn_cache`: ``setattr`` raised, the cache
    fell through to a fresh empty set on every call, and every
    ``is_enrolled`` re-walked the validator chain. A trivial subclass gains
    ``__dict__`` so the cache actually persists for the life of the
    connection and dies with it, no module-level ``id()``-keyed dict (which
    aliases recycled object ids) and no weakref (sqlite3 connections are not
    weak-referenceable).
    """


def _db_path(root: Path) -> Path:
    return root / ".mareforma" / DB_FILENAME


def _open_failure(
    path: Path, exc: sqlite3.Error, corrupt_remedy: str,
) -> DatabaseError:
    """Wrap a failed open, choosing the remedy that fits the cause.

    A file the process may not write and a file whose bytes are damaged both
    arrive here as ``sqlite3.Error``. Answering both with the corruption
    remedy tells an operator whose graph is intact to delete their only copy,
    so the permission case gets its own sentence and names no deletion.
    """
    if "readonly database" in str(exc):
        remedy = (
            "That is a file permission problem, not damage to the graph. "
            "Make the file writable, or copy the project somewhere writable "
            "and open it there."
        )
    else:
        remedy = corrupt_remedy
    return DatabaseError(f"Could not open database at {path}: {exc}. {remedy}")


def _ensure_supports_revision_row(conn: sqlite3.Connection) -> None:
    """Seed the supports_revision singleton when it is missing.

    ``INSERT OR IGNORE`` takes a write lock even when the row is already
    there, so running it on every open makes a read-only graph.db
    unopenable. The row is written once, on the fresh db and on the first
    open of a graph that predates the table.
    """
    if conn.execute("SELECT 1 FROM supports_revision WHERE id = 1").fetchone():
        return
    conn.execute("INSERT INTO supports_revision (id, revision) VALUES (1, 0)")
    conn.commit()


def _ensure_managed_triggers(conn: sqlite3.Connection) -> None:
    """Reconcile the claims-table write guards with their wanted text.

    _SCHEMA_SQL never runs again on an initialised db, so a trigger whose
    definition changed shape reaches an existing graph only from here. Doing
    that as an unconditional drop-and-recreate would open a window on every
    single open() in which another connection sees the claims table with no
    laundering guard on it, which is exactly the substitution the triggers
    exist to refuse. Compare against sqlite_master instead: the steady-state
    open is a pure read, and a genuine rewrite runs inside one transaction so
    the absence is never observable.
    """
    for name, wanted in _MANAGED_TRIGGERS:
        stored = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
            (name,),
        ).fetchone()
        if stored is not None and stored[0] == wanted:
            continue
        own_transaction = not conn.in_transaction
        if own_transaction:
            conn.execute("BEGIN IMMEDIATE")
        conn.execute(f"DROP TRIGGER IF EXISTS {name}")
        conn.execute(wanted)
        if own_transaction:
            conn.commit()


def _open_existing_db(
    conn: sqlite3.Connection, root: Path, version: int,
) -> None:
    """Enforce and migrate the on-disk contract of an already-initialised db.

    Shared by :func:`open_db` and :func:`open_db_from_db_path` so both entry
    points refuse the same files and migrate the same ones: a db reached by a
    literal path must meet the contract a db reached by project root meets.
    Closes *conn* and raises :class:`DatabaseError` when the file cannot be
    served. *root* is the project root used for the grandfather event and the
    claims.toml remediation hint.
    """
    # No in-place migrations in this release. A db whose user_version
    # is neither 0 nor _SCHEMA_VERSION was written by a different
    # build of the dev branch and may carry a partial schema (e.g.
    # a v2-stranded db is missing the retracted-is-terminal trigger
    # that the fix relies on, even though its column set
    # happens to match). Refuse rather than open silently.
    if version != _SCHEMA_VERSION:
        conn.close()
        raise DatabaseError(
            f"graph.db has user_version={version} but this mareforma "
            f"expects user_version={_SCHEMA_VERSION}. The dev branch does "
            "not migrate schemas. Delete .mareforma/graph.db to start "
            "fresh; claims.toml is a human-readable record of the prior "
            "state (the chain and signatures cannot be reconstructed "
            "from it)."
        )

    # Initialised db, validate the schema by exact column-set match.
    # Catching extras as well as missing columns means a partially-migrated
    # or hand-edited claims table fails loudly instead of silently passing
    # through code that assumes _CLAIM_COLUMNS is exhaustive.
    existing_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(claims)").fetchall()
    }
    # Auto-migrate the two columns added between v0.3.0 and v0.3.1.
    # Both are non-signed, non-CHECK'd query-side denormalisations with
    # safe defaults, so ALTER ADD COLUMN is a non-disruptive in-place
    # additive migration that preserves every existing row's signed
    # bytes. Concurrent first-opens hit a "duplicate column name" race
    # we treat as benign.
    added_cols = _ensure_claims_columns_for_upgrade(conn, existing_cols)
    # The open that first adds asserter_keyid grandfathers every existing
    # REPLICATED row (all promoted under the retired generated_by rule)
    # with a one-time durable health event. Runs once: subsequent opens
    # already have the column and skip the ALTER.
    if "asserter_keyid" in added_cols:
        _grandfather_legacy_replicated(conn, root)
    existing_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(claims)").fetchall()
    }
    expected_cols = set(_CLAIM_COLUMNS)
    if existing_cols != expected_cols:
        missing = expected_cols - existing_cols
        extra = existing_cols - expected_cols
        conn.close()

        # Extras-only is the downgrade case: the db was written by a
        # newer mareforma. Direct the user to upgrade rather than to
        # delete, claims.toml may not be a faithful backup for columns
        # the older version does not understand.
        if extra and not missing:
            raise DatabaseError(
                f"graph.db was created by a newer mareforma version "
                f"(extra columns: {sorted(extra)}). Upgrade the mareforma "
                "package or back up claims.toml before downgrading."
            )

        parts: list[str] = []
        if missing:
            parts.append(f"missing: {sorted(missing)}")
        if extra:
            parts.append(f"unexpected: {sorted(extra)}")
        # Only claim a backup after looking for one. Recovery is two
        # steps, and restore refuses to run while graph.db still holds
        # claims, so naming the deletion alone leaves the operator with
        # a TOML file and no stated way to use it.
        if (root / "claims.toml").exists():
            remedy = (
                "Delete .mareforma/graph.db, then run `mareforma "
                "restore` to rebuild it from claims.toml with signature "
                "verification."
            )
        else:
            remedy = (
                "No claims.toml is present, so deleting "
                ".mareforma/graph.db discards the only copy of these "
                "claims. Copy graph.db elsewhere first."
            )
        raise DatabaseError(
            f"graph.db schema mismatch ({'; '.join(parts)}). {remedy}"
        )
    # Additive tables (project_policy, the trust layer) must be
    # present on every initialised db, not just fresh ones ,
    # otherwise an existing legacy graph.db lacks them and the first
    # trust-layer write raises 'no such table'.
    conn.executescript(_ADDITIVE_TABLES_SQL)
    _ensure_evidence_lines_columns(conn)


def open_db(root: Path) -> sqlite3.Connection:
    """Open (and initialise if needed) the graph database.

    Returns an open sqlite3.Connection with row_factory set to
    sqlite3.Row for dict-like access.

    Schema validation
    -----------------
    Fresh db (user_version=0): full schema applied, user_version set to
    ``_SCHEMA_VERSION``.

    Initialised db (user_version equals ``_SCHEMA_VERSION``): claims
    table must have every column in ``_CLAIM_COLUMNS``. Missing columns
    raise DatabaseError instructing the user to delete graph.db.
    ``_CLAIM_COLUMNS`` is the source of truth for what the schema must
    contain.

    Raises
    ------
    DatabaseError
        On SQLite errors or schema drift (missing columns).
    """
    path = _db_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Minimum SQLite version. FTS5 with `remove_diacritics 2` (used by
    # claims_fts) requires ≥ 3.27 (released 2019-02). We pick 3.30 as a
    # comfortable floor that gives us window functions + UPSERT + the
    # `||` operator parsing fixes that have shaken out over the years.
    # Common LTS distros that ship below this floor (Ubuntu 18.04 EOL,
    # CentOS 7 EOL) are well outside the support window. Fail loudly
    # with a concrete remediation rather than a cryptic SQL syntax
    # error deep in trigger creation.
    _MIN_SQLITE = (3, 30, 0)
    _have = tuple(int(p) for p in sqlite3.sqlite_version.split("."))
    if _have < _MIN_SQLITE:
        raise DatabaseError(
            f"mareforma requires SQLite >= "
            f"{'.'.join(str(p) for p in _MIN_SQLITE)}, "
            f"this Python build links {sqlite3.sqlite_version}. "
            "Upgrade your system SQLite (apt / brew / etc.) or install "
            "`pysqlite3-binary` and import it as the `sqlite3` module."
        )

    try:
        conn = sqlite3.connect(
            str(path), check_same_thread=False, factory=_GraphConnection
        )
        conn.row_factory = sqlite3.Row
        # SQLite default is foreign_keys = OFF. Every REFERENCES clause
        # in the schema is advisory without this PRAGMA. Verdict-issuer
        # tables FK to validators(keyid) and claims(claim_id); without
        # this set on every connection the FK is unenforced and direct-
        # SQL INSERTs with fabricated keyids would succeed.
        conn.execute("PRAGMA foreign_keys = ON")

        version = conn.execute("PRAGMA user_version").fetchone()[0]

        if version == 0:
            conn.executescript(_SCHEMA_SQL)
            conn.executescript(_ADDITIVE_TABLES_SQL)
            _ensure_supports_revision_row(conn)
            _ensure_managed_triggers(conn)
            conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            conn.commit()
            _attach_supports_cache(conn, root)
            return conn

        _open_existing_db(conn, root, version)
        _attach_supports_cache(conn, root)
        _ensure_managed_triggers(conn)
        conn.commit()
        return conn

    except sqlite3.Error as exc:
        # sqlite3.DatabaseError ('file is not a database') is the PARENT of
        # OperationalError, so a corrupt or truncated graph.db raised it at the
        # PRAGMA user_version read and sailed past a narrow OperationalError
        # catch as a bare traceback. Catch the whole sqlite3.Error family so the
        # documented "Raises DatabaseError on SQLite errors" contract holds and
        # the corruption case reaches the claims.toml remediation.
        raise DatabaseError(
            f"Could not open database at {path}: {exc}. If graph.db is "
            "corrupt or truncated, delete .mareforma/graph.db and start "
            "fresh; claims.toml is a human-readable record of the prior state."
        ) from exc


def open_db_from_db_path(db_path: "str | Path") -> sqlite3.Connection:
    """Open the graph DB from a direct path to ``graph.db`` (not a project root).

    ``open_db`` takes the project root and re-derives the file path. This
    helper reverses that for a caller who already holds the file path, so the
    path is honoured instead of silently rewritten. An existing db goes through
    the same version guard and column migration either entry point applies.

    Accepted shapes:
      - ``<root>/.mareforma/graph.db``: opens ``<root>`` as project root.
      - any other path: opens the DB file directly; the user supplied
        a non-conventional location and we honour it. The parent
        directory becomes the "project root" for cache lookups, and the
        DB lives at the supplied path (NOT at ``<parent>/.mareforma/``).
    """
    db_file = Path(db_path).resolve()
    if db_file.parent.name == ".mareforma":
        return open_db(db_file.parent.parent)

    # Non-conventional path: connect to db_file directly and apply the
    # schema script (idempotent). This preserves the user-supplied
    # filename instead of silently rewriting it under .mareforma/.
    db_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = sqlite3.connect(
            str(db_file), check_same_thread=False, factory=_GraphConnection
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version == 0:
            conn.executescript(_SCHEMA_SQL)
            conn.executescript(_ADDITIVE_TABLES_SQL)
            _ensure_supports_revision_row(conn)
            conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
        else:
            _open_existing_db(conn, db_file.parent, version)
        _ensure_managed_triggers(conn)
        conn.commit()
        # Attach the rebuildable supports cache just like open_db does. Without
        # it add_claim's unconditional supports-edge maintenance hits
        # 'no such table: supports_cache.cache_meta' and every write fails,
        # even a claim with no supports. The parent directory is the "project
        # root" for cache lookups per this function's own docstring, so the
        # sidecar lands at <parent>/.mareforma/claim_supports_cache.db.
        _attach_supports_cache(conn, db_file.parent)
        return conn
    except sqlite3.Error as exc:
        # Same contract as open_db: a corrupt or truncated file raises
        # sqlite3.DatabaseError at the PRAGMA read, which is NOT an
        # OperationalError. Wrap the whole sqlite3.Error family so a literal
        # path never leaks a raw sqlite3 exception.
        raise DatabaseError(
            f"Could not open database at {db_file}: {exc}. If the file is "
            "corrupt or truncated, delete it and restore from claims.toml."
        ) from exc


def _ensure_claims_columns_for_upgrade(
    conn: sqlite3.Connection, existing_cols: set[str],
) -> set[str]:
    """Auto-add the claims-table columns introduced in this release.

    Returns the set of columns this call actually added (empty when the db
    is already current, or when a concurrent opener won every ALTER race).
    The caller uses an ``asserter_keyid`` entry to fire the one-time
    legacy-promotion grandfather event exactly once.

    ``predicate_payload``, ``original_signature_bundle``, and
    ``asserter_keyid`` are query-side fields that are NOT part of the
    signed envelope or the chain hash. ALTER TABLE ADD COLUMN with the
    documented defaults leaves every existing row's signed bytes
    byte-identical, so the migration is safe to run on any legacy graph.db.

    Concurrent first-opens race the ALTER: SQLite serialises writes;
    the loser's ALTER fails with ``duplicate column name`` and we
    re-check + return. Same posture as ``_ensure_evidence_lines_columns``.
    """
    # If the claims table itself is missing, there's nothing to ALTER , 
    # let the column-set validation below surface the schema-mismatch
    # error with its actionable message.
    if not existing_cols:
        return set()
    added: set[str] = set()
    upgrades = [
        ("predicate_payload",
         "ALTER TABLE claims ADD COLUMN predicate_payload "
         "TEXT NOT NULL DEFAULT ''"),
        ("original_signature_bundle",
         "ALTER TABLE claims ADD COLUMN original_signature_bundle TEXT"),
        # Denormalized asserter keyid. New rows populate it at write from the
        # signature_bundle; legacy rows stay NULL (not backfilled) so the
        # promotion query's NULL guard treats them as "not a distinct signer."
        ("asserter_keyid",
         "ALTER TABLE claims ADD COLUMN asserter_keyid TEXT"),
        # Observed grounding verdict (computed axis). Added NULL on every
        # existing row; a NULL verdict is omitted from the signed predicate, so
        # this ALTER leaves every existing row's signed bytes byte-identical,
        # exactly like the query-side columns above.
        ("observed_grounding",
         "ALTER TABLE claims ADD COLUMN observed_grounding TEXT"),
    ]
    for col, alter_sql in upgrades:
        if col in existing_cols:
            continue
        try:
            conn.execute(alter_sql)
            conn.commit()
            added.add(col)
        except sqlite3.OperationalError as exc:
            # Re-check: a concurrent process may have won the ALTER
            # race. Duplicate-column-name is benign; any other failure
            # is real. The race-loser did NOT add the column, so it is
            # left out of ``added`` and never fires the grandfather event.
            cols_after = {
                row[1]
                for row in conn.execute("PRAGMA table_info(claims)").fetchall()
            }
            if col in cols_after:
                continue
            raise DatabaseError(
                f"Could not add claims.{col} column: {exc}"
            ) from exc

    # The partial index on asserter_keyid lives in _SCHEMA_SQL, which only runs
    # on a fresh db. An upgraded db gets the column via the ALTER above but
    # never re-runs _SCHEMA_SQL, so create the index here too. asserter_keyid is
    # always present by this point (in existing_cols, or added by the loop
    # above), so this runs unconditionally; IF NOT EXISTS keeps it a no-op once
    # present.
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_claims_asserter_keyid "
            "ON claims(asserter_keyid) WHERE asserter_keyid IS NOT NULL"
        )
        conn.commit()
    except sqlite3.OperationalError:
        # A concurrent opener may be mid-ALTER; the index is a perf
        # optimisation, not a correctness gate, so a transient failure
        # here must not block the open. The next open retries.
        pass

    return added


def _grandfather_legacy_replicated(
    conn: sqlite3.Connection, root: Path,
) -> int:
    """Grandfather REPLICATED rows that predate the asserter_keyid rule.

    Runs exactly once, in the same open that first adds ``asserter_keyid``
    (its column is brand-new, so every existing REPLICATED row was promoted
    under the old ``generated_by`` rule and now carries a NULL keyid). Those
    rows keep their level: the new rule never re-promotes a NULL-keyid row,
    so nothing downgrades them, and the read-path verify gate exempts them
    because they carry no participant signature to check. We record a durable
    ``legacy_promotion`` health event so the grandfathered set stays
    distinguishable from rows promoted under the new rule. Returns the count.

    A REPLICATED row with a NULL ``asserter_keyid`` is, by construction, a
    legacy promotion: the current promotion query refuses to promote a
    NULL-keyid row, so no post-build row can land in this state.
    """
    from mareforma.health import append_health_event
    n = conn.execute(
        "SELECT COUNT(*) AS n FROM claims "
        "WHERE support_level = 'REPLICATED' AND asserter_keyid IS NULL"
    ).fetchone()["n"]
    if n:
        append_health_event(
            root, "legacy_promotion", outcome="ok",
            replicated_grandfathered=int(n),
        )
    return int(n)


def _ensure_evidence_lines_columns(conn: sqlite3.Connection) -> None:
    """Add the ``model_lineage`` column to legacy evidence_lines tables.

    The trust-layer evidence tree is additive and not part of the signed
    claim-envelope integrity surface, so in-place ALTER is safe: a NULL
    ``model_lineage`` on an existing line is exactly a line authored without an
    observed model call. CREATE TABLE IF NOT EXISTS on a fresh DB already creates
    the column; this fills the gap on DBs created by older mareforma builds. Runs
    after the additive-tables script, so the table itself is guaranteed present.
    """
    cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(evidence_lines)").fetchall()
    }
    if "model_lineage" not in cols:
        try:
            conn.execute("ALTER TABLE evidence_lines ADD COLUMN model_lineage TEXT")
            conn.commit()
        except sqlite3.OperationalError as exc:
            # Concurrent open: another process won the ALTER race. Re-check
            # before raising, "duplicate column name" is benign.
            cols2 = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(evidence_lines)"
                ).fetchall()
            }
            if "model_lineage" in cols2:
                return
            raise DatabaseError(
                f"Could not add evidence_lines.model_lineage column: {exc}"
            ) from exc


def _attach_supports_cache(conn: sqlite3.Connection, root: Path) -> None:
    """Attach the rebuildable claim_supports cache.

    The cache lives outside the versioned schema (separate DB file) so
    the file can be deleted with no consequence beyond a one-time
    rebuild on next open. Errors during attach are surfaced as
    :class:`DatabaseError` so the operator sees a clear remediation
    message rather than a deferred sqlite3 error on the first
    provenance query.
    """
    from mareforma import _supports
    try:
        _supports.attach_cache(conn, root)
    except sqlite3.Error as exc:
        raise DatabaseError(
            f"Could not attach claim_supports cache: {exc}. "
            "Delete .mareforma/claim_supports_cache.db and re-open "
            "the project (the cache is rebuildable from claims.toml "
            "and graph.db; this file is not part of the signed graph)."
        ) from exc


# ---------------------------------------------------------------------------
# Append-only hash chain
# ---------------------------------------------------------------------------

def _chain_input_for_claim(
    claim_fields: dict, evidence: dict | None = None,
) -> bytes:
    """Canonical bytes for the chain hash on a single claim row.

    Uses the in-toto Statement v1 canonical bytes: the exact same
    bytes that get signed (after DSSE PAE wrap). Chain integrity and
    signature integrity bind to one authoritative byte sequence. The
    evidence vector is part of the Statement, so it is part of the
    chain input.
    """
    from mareforma import signing as _signing
    return _signing.canonical_statement(claim_fields, evidence or {})


# Evidence-vector field defaults. The signed predicate carries a plain
# dict; a claim asserted without an explicit vector binds this all-zeros
# shape so the canonical bytes stay stable across releases.
_EVIDENCE_DOWNGRADE_DOMAINS = (
    "risk_of_bias",
    "inconsistency",
    "indirectness",
    "imprecision",
    "publication_bias",
)
_EVIDENCE_UPGRADE_FLAGS = (
    "large_effect",
    "dose_response",
    "opposing_confounding",
)


def _normalize_evidence(evidence: dict | None) -> dict:
    """Project an evidence dict onto the canonical signed-predicate shape.

    Fills every downgrade domain (0), upgrade flag (False), the rationale
    dict, and the reporting_compliance list. ``study_design`` and the
    grounding snapshot are carried only when present, so a claim without
    them produces byte-identical canonical bytes to a legacy claim.

    Every field is type-checked here, because every field signs into the
    immutable predicate and nothing downstream re-reads it: a wrong type is
    refused with a ValueError rather than coerced into a permanent record.
    """
    src = evidence or {}
    out: dict = {}
    for domain in _EVIDENCE_DOWNGRADE_DOMAINS:
        val = src.get(domain, 0)
        if isinstance(val, bool) or not isinstance(val, int) or not -2 <= val <= 0:
            raise ValueError(
                f"evidence downgrade domain {domain!r} must be an integer in "
                f"[-2, 0], got {val!r}: an out-of-range value must not sign into "
                "the immutable predicate"
            )
        out[domain] = val
    for flag in _EVIDENCE_UPGRADE_FLAGS:
        val = src.get(flag, False)
        if not isinstance(val, bool):
            raise ValueError(
                f"evidence upgrade flag {flag!r} must be a bool, got {val!r}: "
                "a non-flag value must not sign into the immutable predicate"
            )
        out[flag] = val
    rationale = src.get("rationale") or {}
    if not isinstance(rationale, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in rationale.items()
    ):
        raise ValueError(
            f"evidence rationale must be a dict of str to str, got "
            f"{rationale!r}: an unreadable justification must not sign into "
            "the immutable predicate"
        )
    out["rationale"] = dict(rationale)
    compliance = src.get("reporting_compliance") or ()
    # A bare str is the silent case: list("CONSORT") splats into seven
    # single-letter guidelines and signs compliance with every one of them.
    if not isinstance(compliance, (list, tuple)) or not all(
        isinstance(item, str) for item in compliance
    ):
        raise ValueError(
            f"evidence reporting_compliance must be a list of guideline "
            f"names, got {compliance!r}: pass ['CONSORT'], not 'CONSORT', so "
            "a permanent record does not claim compliance the asserter never "
            "meant"
        )
    out["reporting_compliance"] = list(compliance)
    study_design = src.get("study_design")
    if study_design is not None:
        if not isinstance(study_design, str):
            raise ValueError(
                f"evidence study_design must be a str, got {study_design!r}: "
                "a non-string value must not sign into the immutable predicate"
            )
        out["study_design"] = study_design
    if src.get("grounding_score") is not None:
        raw = src["grounding_score"]
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise ValueError(
                f"grounding_score must be a number in [0, 1], got {raw!r}: a "
                "flag is not a score and must not sign into the predicate"
            )
        score = float(raw)
        if not 0.0 <= score <= 1.0:
            raise ValueError(
                f"grounding_score must be in [0, 1], got {score!r}: an evidence "
                "value out of range must not sign into the immutable predicate"
            )
        rationale = src.get("grounding_rationale")
        if not (isinstance(rationale, str) and rationale.strip()):
            raise ValueError(
                "grounding_score requires a non-empty grounding_rationale: a "
                "scored claim must say why, not carry a bare number in the record"
            )
        out["grounding_score"] = score
        out["grounding_rationale"] = rationale
    return out


def _compute_prev_hash(
    conn: sqlite3.Connection,
    claim_fields: dict,
    evidence: dict | None = None,
) -> str:
    """Compute the new ``prev_hash`` value for a claim about to be inserted.

    The new chain link is ``sha256(prev_chain_link || canonical_statement_bytes)``.
    For the genesis row (no prior rows), the prior link is empty bytes.

    MUST be called inside ``BEGIN IMMEDIATE``: the SELECT-then-INSERT
    pattern depends on the write lock to prevent two writers from
    branching the chain on the same predecessor.
    """
    row = conn.execute(
        "SELECT prev_hash FROM claims ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    prev = (row["prev_hash"] or "").encode("ascii") if row else b""
    chain_input = _chain_input_for_claim(claim_fields, evidence)
    return hashlib.sha256(prev + chain_input).hexdigest()


# ---------------------------------------------------------------------------
# Cycle / self-loop detection
# ---------------------------------------------------------------------------

# Pattern for the UUID format we generate via uuid.uuid4(). Strict
# UUIDv4, version nibble is exactly ``4`` and variant nibble is one
# of {8, 9, a, b} (RFC 4122 §4.1.1, "10xx" binary variant). Tightening
# from the looser "any hex-shape UUID" rejects v1/v3/v5/zero UUIDs in
# ``supports[]`` as non-graph-nodes, which makes the shape-vs-version
# check explicit instead of accidental. Strings in ``supports[]`` that
# DON'T match are external references (DOIs etc.) and do not
# participate in cycle checking, they are not graph nodes.
_CLAIM_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)

# Cap on the number of DISTINCT nodes the acyclicity walk may reach before it
# gives up. It is the LIMIT on the walk's recursive CTE, so the walk stops here
# rather than being measured after the fact. With node-dedup the walk always
# terminates, so this is not a correctness bound, it is a runaway guard against
# an absurdly large reachable set. A node is a claim_id or an external
# reference carried in a reached claim's supports_json. Generous: a legitimate
# fan-out in a mature graph can reach many thousands of claims at shallow
# depth, and exceeding the cap is reported as GraphTooLargeError, never as a
# cycle.
_REACHABLE_CLAIM_CAP = 100_000


def _is_claim_id(value: str) -> bool:
    return bool(_CLAIM_ID_RE.match(value))


def _validate_claim_text(text: str) -> str:
    """Enforce the write-side text invariants and return the clean text.

    Shared by ``add_claim`` and ``update_claim`` so the two write paths cannot
    drift: both reject empty text, cap the length at ``_MAX_CLAIM_TEXT_LEN``, and
    run sanitize-on-write (stripping zero-width / bidi / tag-plane codepoints)
    BEFORE the text is stored or signed. Any consumer reading ``text`` directly
    then sees a clean, bounded string.
    """
    if not text or not text.strip():
        raise ValueError("Claim text cannot be empty.")
    if len(text) > _MAX_CLAIM_TEXT_LEN:
        raise ValueError(
            f"Claim text exceeds {_MAX_CLAIM_TEXT_LEN}-char cap "
            f"(got {len(text)}). Split the finding into smaller claims "
            "and link them via supports=[]."
        )
    from mareforma import prompt_safety as _ps
    # Strip AFTER sanitizing too: none of the codepoints the sanitizer deletes
    # are Python whitespace, so the first strip cannot reach the whitespace they
    # hide. Re-stripping keeps one canonical string flowing to the column, the
    # signature and every comparison.
    cleaned = _ps.sanitize_for_llm(text.strip()).strip()
    if not cleaned:
        raise ValueError(
            "Claim text became empty after stripping zero-width / control "
            "characters. The input contained no visible content."
        )
    return cleaned


def _refuse_supports_contradicts_overlap(
    supports: "list | None", contradicts: "list | None",
) -> None:
    """Refuse a claim that supports AND contradicts the same upstream UUID.

    A row that simultaneously builds on and refutes the same upstream is
    logically incoherent (a reader cannot tell which relation is real). Only
    UUID-shaped refs are compared; DOI / external string refs are out of scope.
    Shared by ``add_claim`` and ``update_claim`` (an edit to either side can
    create the overlap) so the gate cannot drift between the two write paths.
    """
    if supports and contradicts:
        sup_ids = {s for s in supports if isinstance(s, str) and _is_claim_id(s)}
        con_ids = {c for c in contradicts if isinstance(c, str) and _is_claim_id(c)}
        overlap = sup_ids & con_ids
        if overlap:
            raise ValueError(
                f"supports[] and contradicts[] reference the same "
                f"upstream claim(s): {sorted(overlap)}. A claim that "
                "simultaneously builds on and refutes the same upstream "
                "is logically incoherent; pick one relation."
            )


# Three-way classification of ``supports[]`` and ``contradicts[]`` entries.
# The flat string API stays, mareforma auto-classifies each entry so
# JSON-LD export, audit helpers, and future query surfaces can distinguish
# the three semantic types without forcing callers to wrap strings.
SUPPORT_TYPE_CLAIM = "claim"
SUPPORT_TYPE_DOI = "doi"
SUPPORT_TYPE_EXTERNAL = "external"

_VALID_SUPPORT_TYPES = (
    SUPPORT_TYPE_CLAIM,
    SUPPORT_TYPE_DOI,
    SUPPORT_TYPE_EXTERNAL,
)


def classify_support(value: str) -> str:
    """Return the type tag for a single ``supports[]`` entry.

    Three buckets:

      * ``"claim"``: strict UUIDv4 shape, candidate graph-node edge.
        REPLICATED detection and cycle detection walk these.
      * ``"doi"``: DOI form (``10.<registrant>/<suffix>``) per Crossref +
        DataCite syntax; ineligible as a REPLICATED anchor (the upstream
        is not a local claim).
      * ``"external"``: anything else. Free-form strings (URLs, ORCID
        ids, lab-internal references). Stored verbatim, not walked, not
        resolved.

    Classification is deterministic and regex-only: no network, no
    database lookup. The same string always yields the same tag.
    """
    if not isinstance(value, str):
        return SUPPORT_TYPE_EXTERNAL
    # Late import, ``doi_resolver`` itself is import-light, but keep
    # this helper free of network-y modules at module-import time.
    from mareforma import doi_resolver as _doi
    if _is_claim_id(value):
        return SUPPORT_TYPE_CLAIM
    if _doi.is_doi(value):
        return SUPPORT_TYPE_DOI
    return SUPPORT_TYPE_EXTERNAL


def classify_supports(values: list[str]) -> list[dict[str, str]]:
    """Classify every entry in a ``supports[]`` / ``contradicts[]`` list.

    Returns ``[{"value": <original>, "type": <one of SUPPORT_TYPE_*>}, ...]``
    in input order. Empty list → empty list.

    Used by:

      * the JSON-LD exporter, which emits each entry under a typed
        predicate (``mare:supportsClaim``, ``mare:supportsDoi``,
        ``mare:supportsReference``) so consumers can distinguish a
        local graph edge from an external citation;
      * operator audits: pair with :func:`find_dangling_supports` for a
        complete view of which entries are graph nodes, which are
        external references, and which are dangling claim_ids that point
        nowhere.
    """
    return [{"value": v, "type": classify_support(v)} for v in values]


def _check_no_cycle(
    conn: sqlite3.Connection,
    new_claim_id: str,
    supports: list[str],
) -> None:
    """Raise :class:`CycleDetectedError` if extending the graph with
    ``new_claim_id → supports`` would create a cycle.

    Algorithm: one recursive-CTE reachability walk. Seed the walk with
    the new claim's ``supports[]`` and follow each reached claim's own
    ``supports[]`` forward; if the walk ever reaches ``new_claim_id`` the
    new edge closes a cycle. A single query replaces the former
    per-node DFS (one ``SELECT`` per visited claim), so the cost no longer
    scales with the depth of the ancestral chain.

    The walk reads ``claims.supports_json`` directly (the authoritative
    edge source), not the ``supports_cache`` sidecar: cycle detection must
    stay correct on connections where that cache is not attached
    (``open_db_from_db_path`` on a non-conventional path).

    Why reachability (not Tarjan's SCC): the existing graph is acyclic by
    induction (we reject cycles on every write). A new claim has no
    incoming edges at INSERT time, so the only cycle it can create is one
    that goes ``new → supports → ... → new``. A forward walk from each
    support entry is sufficient. For ``update_claim``, the new edge is the
    changed ``supports[]``; same algorithm applies.

    DOI strings in ``supports[]`` are external references, they are kept
    as seeds but never match a ``claim_id``, so they drop out of the walk.
    ``UNION`` dedupes the reachable set by ``node``, so each claim is
    visited once: the walk is O(reachable claims), it terminates even if a
    stored cycle exists among other claims (a DB-write adversary could
    plant one), and such a stored cycle never turns into a spurious verdict
    here, only ``new_claim_id`` being reachable from the seeds is a cycle.
    The walk itself stops at ``_REACHABLE_CLAIM_CAP`` nodes: the ``reach``
    body carries a ``LIMIT`` of one past the cap, which short-circuits row
    generation, so a runaway graph costs the cap rather than its full size.
    Coming back with that extra row means the walk was truncated and raises
    :class:`GraphTooLargeError` (a distinct condition, never a cycle), so a
    legitimate wide fan-out is not mislabeled. ``hit`` is checked first, so a
    cycle inside the truncated prefix is still reported as a cycle; one whose
    target sits past the truncation point reads as GraphTooLargeError. Either
    way the write is refused.
    """
    seeds = [s for s in supports if _is_claim_id(s)]
    if not seeds:
        return
    if new_claim_id in seeds:
        raise CycleDetectedError(
            f"Claim {new_claim_id!r} cannot support itself "
            f"(self-loop in supports[])."
        )

    row = conn.execute(
        """
        WITH RECURSIVE reach(node) AS (
            SELECT value FROM json_each(?)
            UNION
            SELECT je.value
              FROM reach r
              JOIN claims c ON c.claim_id = r.node
              JOIN json_each(c.supports_json) je
             WHERE c.supports_json IS NOT NULL
               AND json_valid(c.supports_json)
            LIMIT ?
        )
        SELECT
            MAX(CASE WHEN node = ? THEN 1 ELSE 0 END) AS hit,
            COUNT(*) AS visited
        FROM reach
        """,
        (json.dumps(seeds), _REACHABLE_CLAIM_CAP + 1, new_claim_id),
    ).fetchone()

    if row is not None and row["hit"]:
        raise CycleDetectedError(
            f"Inserting/updating {new_claim_id!r} with the given "
            "supports[] would create a cycle."
        )
    if row is not None and (row["visited"] or 0) > _REACHABLE_CLAIM_CAP:
        raise GraphTooLargeError(
            f"supports[] reaches more than {_REACHABLE_CLAIM_CAP} distinct "
            "upstream nodes; the acyclicity walk stopped at the cap. This is "
            "not a cycle the walk found, the reachable graph is "
            "extraordinarily large and nothing past the cap was walked. "
            "Investigate before relaxing the cap."
        )


def _signed_delete_error(
    exc: sqlite3.IntegrityError, claim_id: str | None = None,
) -> "MareformaError":
    """Translate a delete-blocked IntegrityError into a typed error.

    The ``claims_signed_no_delete`` trigger raises
    ``mareforma:append_only:signed_claim_no_delete`` when a caller tries to
    delete a signed claim (its signature + Rekor entry + chain hash attest
    the assertion; a delete would let a DB-write process forget it). Map
    that marker to the documented :class:`SignedClaimImmutableError` so the
    delete path surfaces the same typed failure as the update path instead
    of a raw sqlite3.IntegrityError. Any other IntegrityError is a genuine
    DB fault and becomes :class:`DatabaseError`.
    """
    msg = str(exc)
    if "mareforma:append_only:signed_claim_no_delete" in msg:
        target = f" '{claim_id}'" if claim_id else ""
        return SignedClaimImmutableError(
            f"Signed claim{target} cannot be deleted: its signature commits "
            "the assertion to the append-only chain. To withdraw it, assert "
            "a retraction (status='retracted') that cites the claim via "
            "contradicts=[...]."
        )
    detail = f" claim '{claim_id}'" if claim_id else "s"
    return DatabaseError(f"Failed to delete claim{detail}: {exc}")


def _state_error_from_integrity(
    exc: sqlite3.IntegrityError,
) -> "MareformaError | None":
    """Translate trigger / UNIQUE violations into mareforma exceptions.

    Returns ``None`` if the IntegrityError is not one of the patterns
    we own; callers should re-raise as ``DatabaseError`` then.
    """
    msg = str(exc)
    if "mareforma:state:" in msg:
        # Extract the suffix after the prefix for callers that want to
        # pattern-match. The full SQLite message looks like:
        #   IntegrityError: mareforma:state:illegal_transition:from_preliminary
        # (Static suffixes only, SQLite < 3.46 rejects `'prefix:' || NEW.x`
        # in RAISE() as a syntax error. See the schema preamble.)
        marker = "mareforma:state:"
        suffix = msg[msg.index(marker) + len(marker):]
        return IllegalStateTransitionError(f"State transition refused: {suffix}")
    if "idx_claims_prev_hash" in msg or (
        "UNIQUE constraint failed" in msg and "prev_hash" in msg
    ):
        return ChainIntegrityError(
            "prev_hash UNIQUE violation, two writers raced past BEGIN "
            "IMMEDIATE, or a manual SQL tamper re-used an existing chain "
            "link. Treat as corruption, not a retry."
        )
    return None


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


def normalize_artifact_hash(value: str | None) -> str | None:
    """Validate and lowercase a SHA256 hex digest. Returns None for None.

    A claim's ``artifact_hash`` is the SHA256 of the output bytes that
    backed the claim (a figure, a CSV, a pickled model). It is signed
    into the claim envelope and used as a parallel REPLICATED signal:
    when two peers cite the same upstream and both supply a hash, the
    hashes must match for REPLICATED to fire.

    Accepts canonical hex digests only: no ``sha256:`` prefix, no
    base64, no whitespace. Case is normalised to lowercase so two
    spellings of the same digest compare equal in the REPLICATED query.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(
            f"artifact_hash must be a string or None, got {type(value).__name__}."
        )
    candidate = value.strip().lower()
    if not _SHA256_HEX_RE.match(candidate):
        raise ValueError(
            f"artifact_hash {value!r} is not a 64-character lowercase SHA256 "
            "hex digest. Compute with hashlib.sha256(bytes).hexdigest()."
        )
    return candidate


# ---------------------------------------------------------------------------
# Claims
# ---------------------------------------------------------------------------

def _reconcile_idempotency_row(
    row: sqlite3.Row,
    idempotency_key: str,
    text: str,
    classification: str,
    generated_by: str | None,
    supports: list[str] | None,
    contradicts: list[str] | None,
    source_name: str | None,
    artifact_hash: str | None,
    predicate_payload: dict | None = None,
    original_signature_bundle: str | None = None,
) -> str:
    """Compare a found row against the current call's semantic fields.

    Same key + every semantic field matching → return the existing
    ``claim_id`` (true retry). Any divergence → raise
    :class:`IdempotencyConflictError` listing every mismatched field.

    Called from two places:

    1. The pre-INSERT idempotency SELECT, the happy path. Catches the
       common case where a deterministic agent retries an in-flight
       assertion after a crash.
    2. The post-INSERT race-recovery path. The pre-SELECT runs outside
       BEGIN IMMEDIATE, so two concurrent writers with the same key
       both see "no existing row" and both proceed to INSERT. SQLite's
       ``idx_claims_idempotency_key`` UNIQUE index makes the second
       INSERT fail; the loser re-SELECTs and routes through this
       helper to deliver the same epistemic error as the happy path,
       not a bare ``sqlite3.IntegrityError``.
    """
    expected_supports = json.dumps(supports or [])
    expected_contradicts = json.dumps(contradicts or [])
    mismatches: list[str] = []
    if row["text"] != text:
        mismatches.append("text")
    if row["classification"] != classification:
        mismatches.append("classification")
    if row["generated_by"] != generated_by:
        mismatches.append("generated_by")
    if row["supports_json"] != expected_supports:
        mismatches.append("supports")
    if row["contradicts_json"] != expected_contradicts:
        mismatches.append("contradicts")
    if row["source_name"] != source_name:
        mismatches.append("source_name")
    if row["artifact_hash"] != artifact_hash:
        mismatches.append("artifact_hash")
    # predicate_payload is intentionally NOT compared. It is a query-
    # side denormalisation that does not enter the signed envelope or
    # the chain hash; treating it as a semantic field for idempotency
    # would mean federation exports (which drop predicate_payload)
    # round-trip differently than direct asserts. The signed bytes
    # are the only authoritative semantic identity.
    expected_original = _canonical_envelope(original_signature_bundle)
    stored_original = _canonical_envelope(row["original_signature_bundle"])
    if stored_original != expected_original:
        mismatches.append("original_signature_bundle")
    if mismatches:
        raise IdempotencyConflictError(
            f"idempotency_key={idempotency_key!r} already exists "
            f"with different {', '.join(mismatches)}. Use a "
            "different idempotency_key: silently merging two "
            "different claims into one row would discard the "
            "second author's content and break REPLICATED "
            "detection. For cross-lab convergence, assert two "
            "separate claims signed by distinct keys that share "
            "an ESTABLISHED upstream claim in supports[]."
        )
    return row["claim_id"]


def add_claim(
    conn: sqlite3.Connection,
    root: Path,
    text: str,
    *,
    classification: str = "INFERRED",
    idempotency_key: str | None = None,
    supports: list[str] | None = None,
    contradicts: list[str] | None = None,
    generated_by: str = "agent",
    source_name: str | None = None,
    status: str = "open",
    unresolved: bool = False,
    artifact_hash: str | None = None,
    evidence: "object | None" = None,
    seed: bool = False,
    signer: "object | None" = None,
    rekor_url: str | None = None,
    require_rekor: bool = False,
    trust_insecure_rekor: bool = False,
    on_convergence_error: "Callable[[Exception], None] | None" = None,
    rekor_log_pubkey_pem: bytes | None = None,
    predicate_payload: dict | None = None,
    original_signature_bundle: str | None = None,
    observed_grounding: dict | None = None,
    strict_promotion: bool = False,
) -> str:
    """Insert a new claim and return its claim_id.

    Returns the existing claim_id without inserting if idempotency_key
    already exists. After insert, checks for REPLICATED: if ≥2 claims share
    the same ESTABLISHED upstream claim_id in supports[] and carry distinct,
    non-NULL asserter_keyid values, all are promoted to
    support_level='REPLICATED'.

    Parameters
    ----------
    classification:
        'INFERRED' | 'ANALYTICAL' | 'DERIVED'
    idempotency_key:
        Retry-safe writes: same key returns the same claim_id.
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
    unresolved:
        True if any DOI in supports[]/contradicts[] failed to resolve.
        Unresolved claims are ineligible for REPLICATED promotion.
    artifact_hash:
        Optional SHA256 hex digest of the artifact bytes (figure, CSV,
        model) backing this claim. When supplied it is included in the
        signed payload and used as a parallel REPLICATED signal: peers
        that share an upstream AND both supply a hash must agree on the
        hash to converge. When ``None`` on either peer, behaviour falls
        back to identity-only REPLICATED.
    signer:
        Optional Ed25519 private key. When provided, the claim is signed
        before INSERT and the signature envelope is persisted to the
        ``signature_bundle`` column. ``None`` skips signing.
    rekor_url:
        When set, every signed claim is submitted to the Rekor
        transparency log at this URL. Success augments the signature
        bundle with the log entry coordinates and sets
        ``transparency_logged=1``. Failure persists ``transparency_logged=0``,
        blocking REPLICATED promotion until
        :meth:`EpistemicGraph.refresh_unsigned` retries.
    require_rekor:
        When True, raise :class:`SigningError` if the initial Rekor
        submission fails. Use for production high-assurance flows.
    trust_insecure_rekor:
        The session opt-in that lets ``rekor_url`` point at a private
        Rekor on a non-public address. Forwarded to the SSRF / scheme
        re-validation the submit and fetch calls run.

    Raises
    ------
    ValueError
        If classification or status are invalid.
    SigningError
        If ``require_rekor=True`` and the Rekor submission fails.
    """
    # Enforce the empty / cap / sanitize-on-write invariants. Shared with
    # update_claim so the two write paths cannot drift.
    text = _validate_claim_text(text)
    if classification not in VALID_CLASSIFICATIONS:
        raise ValueError(
            f"Unknown classification '{classification}'. "
            f"Use one of: {', '.join(VALID_CLASSIFICATIONS)}"
        )
    validate_status(status)
    artifact_hash = normalize_artifact_hash(artifact_hash)

    # Idempotency check, return existing claim_id if key already present.
    # Strict contract: same key MUST match on every semantic field. True
    # retries pass silently; anything else raises IdempotencyConflictError.
    #
    # Prior behavior, match on artifact_hash only and silently return the
    # existing claim_id, was anti-epistemic: a second caller's text and
    # generated_by were discarded into the first caller's row, collapsing
    # what should have been two independent claims into one. The
    # "convergence convention" documented around this primitive actively
    # destroyed what REPLICATED is supposed to detect (distinct signer
    # identities converging on a shared upstream). The correct path
    # for cross-lab convergence is two separate claims that share an entry
    # in supports[] with distinct asserter_keyid values, which fires REPLICATED.
    # Idempotency_key is retry-safety only.
    if idempotency_key is not None:
        try:
            row = conn.execute(
                "SELECT claim_id, text, classification, generated_by, "
                "supports_json, contradicts_json, source_name, artifact_hash, "
                "predicate_payload, original_signature_bundle "
                "FROM claims WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row:
                existing_id = _reconcile_idempotency_row(
                    row, idempotency_key, text, classification, generated_by,
                    supports, contradicts, source_name, artifact_hash,
                    predicate_payload=predicate_payload,
                    original_signature_bundle=original_signature_bundle,
                )
                return existing_id
        except sqlite3.OperationalError as exc:
            raise DatabaseError(f"Idempotency check failed: {exc}") from exc

    claim_id = str(uuid.uuid4())
    now = _now()
    supports_json = json.dumps(supports or [])
    contradicts_json = json.dumps(contradicts or [])

    # Refuse a claim that simultaneously supports AND contradicts the same
    # upstream, the row would be logically incoherent (downstream readers
    # cannot tell which interpretation is "real"). Shared with update_claim.
    _refuse_supports_contradicts_overlap(supports, contradicts)

    # Cycle / self-loop check on supports[]. DOI entries are external
    # references and not graph nodes, _check_no_cycle filters them
    # out. The walk runs before signing and INSERT so we don't strand
    # half-built state on rejection.
    _check_no_cycle(conn, claim_id, supports or [])

    # Seed-claim bootstrap. A seed claim is asserted by an
    # enrolled validator and inserted directly with
    # support_level='ESTABLISHED' + a signed seed envelope. This is
    # the only path that can place a claim at ESTABLISHED without
    # going through REPLICATED + validate(); it exists to bootstrap
    # the chain of trust on a fresh graph (otherwise the
    # ESTABLISHED-upstream rule blocks the first REPLICATED forever).
    seed_envelope_json: str | None = None
    if seed:
        # Seed envelopes sign claim_id + validator_keyid + seeded_at , 
        # NOT status. A non-open seed could be flipped back to 'open'
        # via update_claim later (status is mutable on signed rows) and
        # the resurrection would carry no envelope evidence. Refuse the
        # mismatched-status seed up-front to preserve seed-as-anchor.
        if status != "open":
            raise ValueError(
                f"seed=True refused with status='{status}'. Seed claims "
                "bootstrap the trust chain and must be born open."
            )
        if signer is None:
            raise ValueError(
                "seed=True requires a signing key (open the graph with "
                "key_path=... or run `mareforma bootstrap` once)."
            )
        from mareforma import signing as _signing
        from mareforma import validators as _validators
        signer_keyid = _signing.public_key_id(signer.public_key())
        if not _validators.is_enrolled(conn, signer_keyid):
            raise ValueError(
                f"seed=True refused: key {signer_keyid[:12]}… is not an "
                "enrolled validator on this project. Only enrolled "
                "validators can bootstrap the trust chain."
            )
        # Seed produces a born-ESTABLISHED row. Without the same
        # validator_type gate validate_claim applies, an LLM-typed
        # validator could route around the ESTABLISHED ceiling via
        # the seed path. Apply the gate here so all paths to
        # ESTABLISHED enforce the same human-witnessed rule.
        signer_row = _validators.get_validator(conn, signer_keyid)
        if signer_row is not None and signer_row["validator_type"] == "llm":
            raise LLMValidatorPromotionError(
                f"seed=True refused: validator {signer_keyid[:12]}… is "
                "enrolled with validator_type='llm'. Seed claims bootstrap "
                "the ESTABLISHED tier; only human-typed validators can "
                "produce them."
            )
        seed_envelope = _signing.sign_seed_claim(
            {
                "claim_id": claim_id,
                "validator_keyid": signer_keyid,
                "seeded_at": now,
            },
            signer,
        )
        seed_envelope_json = json.dumps(
            seed_envelope, sort_keys=True, separators=(",", ":"),
        )

    # Sign the claim if a signer was supplied. The signature is bound to the
    # in-toto Statement v1 wrapping claim fields + the evidence vector, so
    # any later tamper (text edit, support reattribution, evidence override)
    # breaks verification.
    #
    # Callers can supply a populated evidence-vector dict via the
    # ``evidence`` parameter, the asserter's confidence in the evidence
    # backing this claim. Default all-zeros means the asserter flagged
    # no quality concerns; downstream readers should interpret a
    # default-zero vector as "asserter made no claim about quality,"
    # not as "evidence is high-quality."
    if evidence is not None and not isinstance(evidence, dict):
        raise TypeError(
            f"evidence must be a dict or None; got {type(evidence).__name__}"
        )
    evidence_dict = _normalize_evidence(evidence)
    evidence_json = json.dumps(
        evidence_dict, sort_keys=True, separators=(",", ":"),
    )
    signature_bundle: str | None = None
    envelope: dict | None = None
    statement_cid: str | None = None
    if signer is not None:
        from mareforma import signing as _signing
        from mareforma import _statement as _stmt
        claim_fields = {
            "claim_id": claim_id,
            "text": text,
            "classification": classification,
            "generated_by": generated_by,
            "supports": supports or [],
            "contradicts": contradicts or [],
            "source_name": source_name,
            "artifact_hash": artifact_hash,
            "created_at": now,
        }
        # Bind the observed verdict into the signed bytes only when one was
        # recorded. Absent → the key never enters claim_fields, so the signed
        # statement and its cid are byte-identical to a pre-observer claim.
        if observed_grounding is not None:
            claim_fields["observed_grounding"] = observed_grounding
        envelope = _signing.sign_claim(
            claim_fields, signer, evidence=evidence_dict,
        )
        signature_bundle = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
        statement_cid = _stmt.statement_cid(
            _stmt.build_statement(
                claim_id=claim_fields["claim_id"],
                text=claim_fields["text"],
                classification=claim_fields["classification"],
                generated_by=claim_fields["generated_by"],
                supports=claim_fields["supports"],
                contradicts=claim_fields["contradicts"],
                source_name=claim_fields["source_name"],
                artifact_hash=claim_fields["artifact_hash"],
                created_at=claim_fields["created_at"],
                evidence=evidence_dict,
                observed_grounding=observed_grounding,
            )
        )

    # ``transparency_logged`` defaults to 1 (ready). We flip it to 0 only when
    # Rekor is enabled AND we have something to submit, the row then waits
    # for either a successful submission below or a refresh_unsigned() retry.
    rekor_enabled = rekor_url is not None and signer is not None and envelope is not None
    transparency_logged = 0 if rekor_enabled else 1

    # BEGIN IMMEDIATE: serialize the read-latest-chain-link + INSERT so
    # two writers cannot branch the append-only hash chain. Defaults
    # would let them race past the SELECT and both insert with the same
    # prev_hash, splitting the chain silently, the UNIQUE index on
    # prev_hash catches that case as a backstop, but BEGIN IMMEDIATE is
    # the primary defense.
    chain_fields = {
        "claim_id": claim_id,
        "text": text,
        "classification": classification,
        "generated_by": generated_by,
        "supports": supports or [],
        "contradicts": contradicts or [],
        "source_name": source_name,
        "artifact_hash": artifact_hash,
        "created_at": now,
    }
    # The chain hash binds the same optional field the signature does, so the
    # verdict is tamper-evident on the append-only chain as well. Absent when no
    # verdict was recorded, keeping the chain link identical for pre-observer
    # claims (the chain input is the canonical statement, which omits the key).
    if observed_grounding is not None:
        chain_fields["observed_grounding"] = observed_grounding
    # BEGIN IMMEDIATE is only valid when no transaction is currently
    # open. Python's default sqlite3 isolation_level='' auto-starts a
    # transaction before DML, so callers that already wrote within the
    # same connection will be in-transaction when they reach us. In
    # that case the caller's transaction supplies the serialization;
    # our SELECT runs inside their snapshot and the chain stays linear.
    _own_transaction = not conn.in_transaction
    # Seed claims insert with support_level='ESTABLISHED' and carry
    # the seed envelope in validation_signature. The INSERT trigger
    # accepts ESTABLISHED rows when validation_signature is non-NULL.
    initial_level = "ESTABLISHED" if seed else "PRELIMINARY"
    initial_validation_signature = seed_envelope_json
    initial_validated_at = now if seed else None
    # Seed claims carry their signer's keyid in validator_keyid so the
    # reputation aggregation counts the bootstrap event. Non-seed rows
    # acquire validator_keyid later at validate_claim time.
    initial_validator_keyid = (
        signer_keyid if seed and signer is not None else None
    )
    # Denormalize the asserter keyid from the signed envelope so the
    # REPLICATED promotion query and the trust-layer independence count read
    # an indexed column rather than walking the bundle JSON. The
    # signature_bundle stays authoritative. NULL on unsigned claims.
    asserter_keyid = _extract_signature_bundle_keyid(signature_bundle)
    try:
        if _own_transaction:
            conn.execute("BEGIN IMMEDIATE")
        prev_hash = _compute_prev_hash(conn, chain_fields, evidence_dict)
        conn.execute(
            """
            INSERT INTO claims
                (claim_id, text, classification, support_level, idempotency_key,
                 status, source_name, generated_by,
                 supports_json, contradicts_json, unresolved,
                 signature_bundle, transparency_logged,
                 validation_signature, validator_keyid, asserter_keyid,
                 validated_at,
                 artifact_hash, prev_hash,
                 ev_risk_of_bias, ev_inconsistency, ev_indirectness,
                 ev_imprecision, ev_pub_bias,
                 evidence_json, statement_cid,
                 predicate_payload, original_signature_bundle,
                 observed_grounding,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim_id, text, classification, initial_level, idempotency_key,
                status, source_name, generated_by,
                supports_json, contradicts_json, 1 if unresolved else 0,
                signature_bundle, transparency_logged,
                initial_validation_signature, initial_validator_keyid,
                asserter_keyid,
                initial_validated_at,
                artifact_hash, prev_hash,
                evidence_dict["risk_of_bias"], evidence_dict["inconsistency"],
                evidence_dict["indirectness"], evidence_dict["imprecision"],
                evidence_dict["publication_bias"],
                evidence_json, statement_cid,
                _serialize_predicate_payload(predicate_payload),
                _canonical_envelope(original_signature_bundle),
                _serialize_observed_grounding(observed_grounding),
                now, now,
            ),
        )
        # Maintain claim_supports rebuildable cache inside the same
        # transaction so the edge rows and the main-claim INSERT commit
        # together on the normal path. The cache is an attached WAL
        # database, so SQLite commits it separately and a crash between
        # the two commits can write one without the other. That torn
        # write moves the claim count out of step with the stamped one,
        # and the next open rebuilds the cache.
        from mareforma import _supports
        _supports.record_supports_edges(conn, claim_id, supports)
        if _own_transaction:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        if _own_transaction:
            conn.rollback()
        # Race-loss recovery: two concurrent writers with the same
        # idempotency_key both passed the pre-INSERT SELECT (it runs
        # outside BEGIN IMMEDIATE), and the second INSERT tripped the
        # UNIQUE index on claims.idempotency_key. Re-SELECT and route
        # through the same comparison helper as the happy path so the
        # loser gets IdempotencyConflictError-with-field-list (true
        # retry) or a clean return (everything matched), not a bare
        # IntegrityError. SQLite reports the failure as
        # "UNIQUE constraint failed: claims.idempotency_key", match on
        # the qualified column name rather than the index name.
        exc_msg = str(exc)
        if (
            idempotency_key is not None
            and "UNIQUE constraint failed" in exc_msg
            and "claims.idempotency_key" in exc_msg
        ):
            try:
                row = conn.execute(
                    "SELECT claim_id, text, classification, generated_by, "
                    "supports_json, contradicts_json, source_name, "
                    "artifact_hash, predicate_payload, "
                    "original_signature_bundle "
                    "FROM claims WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
            except sqlite3.OperationalError as fetch_exc:
                raise DatabaseError(
                    f"Idempotency race recovery failed: {fetch_exc}",
                ) from fetch_exc
            if row is not None:
                return _reconcile_idempotency_row(
                    row, idempotency_key, text, classification, generated_by,
                    supports, contradicts, source_name, artifact_hash,
                    predicate_payload=predicate_payload,
                    original_signature_bundle=original_signature_bundle,
                )
        translated = _state_error_from_integrity(exc)
        if translated is not None:
            raise translated from exc
        raise DatabaseError(f"Failed to add claim: {exc}") from exc
    except sqlite3.OperationalError as exc:
        if _own_transaction:
            conn.rollback()
        raise DatabaseError(f"Failed to add claim: {exc}") from exc

    # Attempt Rekor submission. The saga (submit → sidecar → row UPDATE)
    # is its own concern; the helper returns the new transparency_logged
    # value so the REPLICATED check below can short-circuit when the
    # log entry failed to attach.
    if rekor_enabled:
        transparency_logged = _attempt_rekor_saga(
            conn,
            root,
            claim_id=claim_id,
            envelope=envelope,
            signer=signer,
            rekor_url=rekor_url,
            require_rekor=require_rekor,
            trust_insecure_rekor=trust_insecure_rekor,
            rekor_log_pubkey_pem=rekor_log_pubkey_pem,
            own_transaction=_own_transaction,
        )

    # Check whether this claim triggers REPLICATED status on shared upstreams.
    # Unresolved DOIs OR pending transparency-log inclusion block eligibility.
    if not unresolved and transparency_logged == 1:
        _maybe_update_replicated(
            conn, claim_id, supports or [], generated_by, artifact_hash,
            on_error=on_convergence_error,
            own_transaction=_own_transaction,
            strict_promotion=strict_promotion,
        )

    # Snapshot committed state only. When this call joined a caller's open
    # transaction the rows are not committed yet, so backing up here would put a
    # claim in the DR artifact that a caller rollback then erases from the DB.
    # The owning caller runs the backup after it commits (submit_finding does).
    if _own_transaction:
        _backup_claims_toml(conn, root)
    return claim_id


def _claim_model_lineage(
    conn: sqlite3.Connection, claim_id: str,
) -> "dict | None":
    """The model/method lineage recorded on a claim's finding, or None.

    A finding-derived claim carries its authoring scope's lineage on its evidence
    lines (written identically on every line), so the first non-NULL value
    represents it. A plain claim with no finding, every claims-graph REPLICATED
    peer, has none, which reads as absent (no model constraint). Any missing
    table (a schema without the evidence tree) or unparseable value also reads as
    absent, never as a fabricated distinct model.
    """
    try:
        row = conn.execute(
            "SELECT el.model_lineage FROM findings f "
            "JOIN evidence_lines el ON el.finding_id = f.finding_id "
            "WHERE f.claim_id = ? AND el.model_lineage IS NOT NULL LIMIT 1",
            (claim_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None or row["model_lineage"] is None:
        return None
    try:
        return json.loads(row["model_lineage"])
    except (ValueError, TypeError):
        return None


def _maybe_update_replicated_unlocked(
    conn: sqlite3.Connection,
    new_claim_id: str,
    supports: list[str],
    generated_by: str,
    artifact_hash: str | None = None,
    *,
    strict_promotion: bool = False,
) -> None:
    """REPLICATED-detection SQL without a commit: caller controls the txn.

    Used by ``mark_claim_resolved`` so the unresolved-flag clear and the
    REPLICATED promotion land in the same SQLite transaction.

    ``strict_promotion`` (opt-in, off by default) requires **non-NULL data on
    both sides** of the converging pair: the new claim and every candidate peer
    must carry an ``artifact_hash``. The default rule promotes on the
    distinct-signer axis alone (absent data never blocks); an operator who wants
    data-distinctness as a hard gate turns this on. It never loosens the default
   , it only adds the data-presence requirement.

    Independence axis: distinct asserter_keyid
    ------------------------------------------
    Two converging claims count as independent lines only when they carry
    **distinct, non-NULL** ``asserter_keyid`` values (the WHO of the claim,
    denormalized from the signature_bundle). A NULL asserter_keyid is "not a
    valid distinct signer": the new claim is not promoted at all, and two
    legacy NULL-keyid rows never read as two distinct signers. ``generated_by``
    is a display label and plays no part in the gate.

    Data is a secondary collapse, never a gate
    ------------------------------------------
    Distinct asserter_keyid alone promotes. Where output data exists on BOTH
    sides and is **equal**, the two lines collapse to one (a byte-identical
    rerun is the same output, not corroboration) and do not promote on their
    own. Absent data (NULL ``artifact_hash`` on either side) never blocks: the
    pair promotes on the keyid axis. So a double-NULL pair with distinct
    signers promotes on the signer axis, never "on hash alone."

    ESTABLISHED-upstream requirement
    --------------------------------
    The candidate peer's ``supports[]`` must include at least one
    claim with ``support_level = 'ESTABLISHED'``. Matches Cochrane /
    GRADE evidence-chain methodology: REPLICATED-of-noise is not
    replication. Bootstrap a fresh graph with the ``seed=True``
    parameter on :func:`add_claim` to create an ESTABLISHED root.
    """
    if not supports:
        return

    # A tainted new claim (status != 'open') must not enter the trust
    # ladder. The candidate-peer SQL filter below blocks an existing
    # tainted row from acting as a partner, but the new row itself
    # would otherwise still ride an honest peer's INSERT into REPLICATED
    # (peer_ids appends new_claim_id unconditionally at the UPDATE
    # below). Short-circuit before the SELECT so neither the new row
    # nor any open peer is promoted.
    new_status_row = conn.execute(
        "SELECT status, support_level, asserter_keyid, observed_grounding, "
        "t_invalid, transparency_logged FROM claims WHERE claim_id = ?",
        (new_claim_id,),
    ).fetchone()
    if new_status_row is None or new_status_row["status"] != "open":
        return
    # An already-ESTABLISHED new claim (a seed) is not a convergence candidate:
    # the candidate-peer SELECT below already excludes ESTABLISHED rows as peers
    # (support_level != 'ESTABLISHED'), so the new claim must be held to the same
    # bar. Without this a seed citing a shared anchor rode its own promotion into
    # peer_ids, and the promotion UPDATE then attempted an illegal
    # ESTABLISHED -> REPLICATED transition that aborted the whole statement,
    # stranding the honest peer and setting a retry flag no retry could clear.
    if new_status_row["support_level"] == "ESTABLISHED":
        return
    # A claim a signed contradiction verdict marked invalid (t_invalid set) must
    # not climb the trust ladder through convergence, nor ride an honest peer's
    # promotion. record_replication_verdict already refuses to promote such a
    # claim; the convergence path agrees. Gated here for the new claim, in the
    # candidate-peer SELECT below, and again in the promotion UPDATE (the UPDATE
    # guard closes the TOCTOU window if a peer is invalidated after the SELECT).
    if new_status_row["t_invalid"] is not None:
        return
    # The new claim must carry a non-NULL asserter_keyid to enter the new
    # promotion rule. An unsigned / legacy row is not a valid distinct signer,
    # so it cannot start a convergence and cannot ride a peer's promotion.
    # (Legacy REPLICATED rows keep their level via the one-time grandfather;
    # they are never re-promoted here.)
    new_asserter_keyid = new_status_row["asserter_keyid"]
    if new_asserter_keyid is None:
        return
    # Observed-grounding gate: a finding that execution shows is NOT grounded
    # (UNGROUNDED or OPAQUE) never counts toward support-level promotion. A
    # claim without a computed verdict (NULL, every pre-observer claim) is
    # unaffected, so this is purely additive. Grounding is necessary, not
    # sufficient: a GROUNDED verdict still has to clear the signer axis below.
    if not _observed_grounding_promotes(new_status_row["observed_grounding"]):
        return

    # Strict-promotion gate (opt-in): the new claim must carry data. Without an
    # artifact_hash there is no data to distinguish from a peer, so under strict
    # mode it cannot start or join a convergence. The default rule promotes on
    # the signer axis alone; this adds the data-presence requirement.
    if strict_promotion and artifact_hash is None:
        return

    # Shared-anchor rule: the converged-on-same-upstream contract requires
    # that there exists a SINGLE upstream X such that
    #   X ∈ new_claim.supports  ∧  X ∈ peer.supports  ∧  X is ESTABLISHED+open.
    # Pre-filter the new claim's supports[] to those that are ESTABLISHED
    # and open; then the shared-element match below (`j.value IN
    # ({placeholders})`) automatically guarantees the shared element is
    # itself the anchor. A prior implementation gated on three separate
    # conditions (peer-shares-something + new-has-some-established +
    # peer-has-some-established) which let two unrelated established
    # anchors plus a shared preliminary throwaway promote, strictly
    # weaker than the spec.
    #
    # The status='open' filter on the anchor closes a hand-edited
    # claims.toml planting a born-retracted ESTABLISHED seed (the seed
    # envelope binds claim_id + validator_keyid + seeded_at, NOT status)
    # then having downstream peers ride it into REPLICATED.
    sup_placeholders = ",".join("?" * len(supports))
    established_anchors = [
        r["claim_id"] for r in conn.execute(
            f"SELECT claim_id FROM claims "
            f"WHERE claim_id IN ({sup_placeholders}) "
            f"AND support_level = 'ESTABLISHED' "
            f"AND status = 'open'",
            supports,
        ).fetchall()
    ]
    if not established_anchors:
        return

    # Candidate peers: the claims that cite one of the established anchors.
    # Found through the indexed reverse-edge cache (idx_supports_reverse) rather
    # than json_each over every claim, so the per-insert cost is O(deg(anchor))
    # instead of O(N), the same reverse store walk_upstream / walk_downstream
    # already use. The cache is a rebuildable convenience, so it only narrows the
    # candidate set; each candidate's authoritative supports_json is re-checked
    # below before it can promote, so a stale or drifted edge cannot slip a claim
    # that does not actually cite the anchor into a promotion.
    anchor_placeholders = ",".join("?" * len(established_anchors))
    candidate_ids = [
        r["claim_id"] for r in conn.execute(
            f"SELECT DISTINCT claim_id FROM supports_cache.claim_supports "
            f"WHERE supports_claim_id IN ({anchor_placeholders}) "
            f"AND claim_id != ?",
            (*established_anchors, new_claim_id),
        ).fetchall()
    ]
    if not candidate_ids:
        return

    # status='open' filter on the peer: a contested or retracted peer
    # is editorially tainted and must not participate in REPLICATED
    # convergence. Without this, an adversary could plant a born-retracted
    # claim and ride an honest peer's INSERT into REPLICATED (and from
    # there, via validate(), into ESTABLISHED, usable as a fake upstream
    # for further chains).
    # Under strict promotion, a candidate peer must ALSO carry data, an
    # artifact_hash on both sides is the data-distinctness the operator opted
    # into. Off by default, this clause is empty and behaviour is unchanged.
    strict_peer_clause = (
        "\n          AND c.artifact_hash IS NOT NULL" if strict_promotion else ""
    )
    # The candidate list is as wide as the anchor's in-degree, so it is bound as
    # a single JSON array rather than one variable per id: a well-cited anchor
    # would otherwise cross SQLite's per-statement variable cap, and the failure
    # is swallowed into a retry flag whose retry rebuilds the same statement.
    rows = conn.execute(
        f"""
        SELECT c.claim_id, c.asserter_keyid, c.supports_json
        FROM claims c
        WHERE c.claim_id IN (SELECT value FROM json_each(?))
          AND c.asserter_keyid IS NOT NULL
          AND c.asserter_keyid != ?
          AND c.support_level != 'ESTABLISHED'
          AND c.status = 'open'
          AND c.t_invalid IS NULL
          AND c.unresolved = 0
          AND c.transparency_logged = 1
          AND (
              c.observed_grounding IS NULL
              OR (
                  CASE
                      WHEN json_valid(c.observed_grounding)
                      THEN json_extract(c.observed_grounding, '$.grounding')
                      ELSE NULL
                  END
              ) = 'GROUNDED'
          )
          AND NOT (
              c.artifact_hash IS NOT NULL
              AND ? IS NOT NULL
              AND c.artifact_hash = ?
          ){strict_peer_clause}
        """,
        (json.dumps(candidate_ids), new_asserter_keyid,
         artifact_hash, artifact_hash),
    ).fetchall()

    # Authoritative anchor re-check against claims.supports_json, the cache
    # narrows, the claims row decides. A candidate stays only when its own
    # supports_json genuinely cites one of the established anchors, so a stale or
    # drifted cache edge cannot carry a non-citing claim into the promotion.
    anchor_set = set(established_anchors)
    confirmed = []
    for r in rows:
        try:
            refs = json.loads(r["supports_json"] or "[]")
        except (ValueError, TypeError):
            continue
        if isinstance(refs, list) and any(
            isinstance(ref, str) and ref in anchor_set for ref in refs
        ):
            confirmed.append(r)
    rows = confirmed

    if not rows:
        return

    # Model/method independence gate. A converging peer counts only when its
    # model lineage is distinct from the new claim's: two same-model checks
    # (COMPUTED, same family root) are one line of evidence, not two, even under
    # distinct signers; a pair with soft (PROXY/UNVERIFIABLE) lineage on either
    # side is UNVERIFIABLE for independence, never a silent pass; absent lineage
    # (no observed model call) imposes no model constraint. It uses the same key
    # as the read-side count (``model_distinct_pair`` / ``independence_model_key``).
    # NOTE: the load-bearing model-independence signal is that read-side
    # effective-independence number (``trust_map`` / ``effective_independence``),
    # NOT this gate. REPLICATED is a deprecated public label, and on the primary
    # path a plain claims-graph claim carries no finding lineage (findings are
    # read after this runs), so both sides read absent and this filter is a
    # consistent no-op here rather than the enforcement point.
    from mareforma.observe._lineage import model_distinct_pair

    new_lineage = _claim_model_lineage(conn, new_claim_id)
    rows = [
        r for r in rows
        if model_distinct_pair(
            new_lineage, _claim_model_lineage(conn, r["claim_id"])
        )
    ]
    if not rows:
        return

    peer_ids = [r["claim_id"] for r in rows] + [new_claim_id]
    # status='open' folded into the UPDATE's WHERE closes the TOCTOU
    # window between the SELECT above and this UPDATE: another writer
    # could flip a peer (or the new row) to contested/retracted between
    # the two statements. The row-level lock SQLite acquires during
    # UPDATE is the actual gate; the pre-SELECT is a cheap fast-path.
    # One JSON-array variable for the peers, same reason as the SELECT above.
    # The support_level guard keeps the write to the rows that actually change
    # level, matching record_replication_verdict. Peers already at REPLICATED
    # stay in the candidate set (they still corroborate) but must not have their
    # updated_at re-dated to this insert: that field is a claim's end time on
    # every export surface.
    with _promotion_window(conn):
        conn.execute(
            "UPDATE claims SET support_level = 'REPLICATED', updated_at = ? "
            "WHERE claim_id IN (SELECT value FROM json_each(?)) "
            "AND support_level = 'PRELIMINARY' "
            "AND status = 'open' AND t_invalid IS NULL",
            (_now(), json.dumps(peer_ids)),
        )


def _maybe_update_replicated(
    conn: sqlite3.Connection,
    new_claim_id: str,
    supports: list[str],
    generated_by: str,
    artifact_hash: str | None = None,
    on_error: "Callable[[Exception], None] | None" = None,
    *,
    own_transaction: bool = True,
    strict_promotion: bool = False,
) -> bool:
    """Promote claims to REPLICATED when convergence is detected.

    Convergence: ≥2 claims share the same ESTABLISHED upstream claim_id in
    their supports[] and carry distinct, non-NULL ``asserter_keyid`` values
    (equal output artifacts collapse to one line). Uses json_each() for
    correct JSON array element extraction (no fragile LIKE).

    Called immediately after a successful INSERT in add_claim().
    Failures are swallowed: convergence detection must not crash writes.

    ``own_transaction`` mirrors ``add_claim``'s flag: when ``False`` the caller
    already holds an open transaction (e.g. ``submit_finding``'s BEGIN
    IMMEDIATE), so this helper makes its convergence + retry-flag writes but
    does NOT commit: the caller's outer commit flushes them, keeping the
    claim INSERT and the finding write atomic. Committing here would strand a
    signed claim if a later step in the caller's transaction rolls back.

    Returns ``True`` if detection ran cleanly, ``False`` if a SQLite
    error was swallowed. When ``on_error`` is supplied, the exception is
    handed to that callback before the WARNING is logged; caller can
    increment a counter or surface the failure however it sees fit.
    """
    try:
        _maybe_update_replicated_unlocked(
            conn, new_claim_id, supports, generated_by, artifact_hash,
            strict_promotion=strict_promotion,
        )
        if own_transaction:
            conn.commit()
        return True
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        # Convergence detection is best-effort, never crash a write.
        # A trigger-raised IntegrityError here would mean a state transition
        # we asked for is illegal (e.g. ESTABLISHED peer being downgraded);
        # the underlying invariant remains intact. Surface a WARNING so
        # silently-swallowed failures are debuggable, without it, a
        # mis-configured trigger or contention pattern would let claims sit
        # at PRELIMINARY with no record of why. EpistemicGraph wires
        # ``on_error`` to a counter so callers can detect drift without
        # parsing log records, and we flip the per-claim retry flag so
        # :meth:`EpistemicGraph.refresh_convergence` can re-run detection
        # on demand. The two surfaces are complementary: the counter
        # reports the live error rate, the flag preserves the work
        # remaining across restarts.
        if on_error is not None:
            try:
                on_error(exc)
            except Exception:  # pragma: no cover - defensive
                pass
        try:
            conn.execute(
                "UPDATE claims SET convergence_retry_needed = 1 "
                "WHERE claim_id = ?",
                (new_claim_id,),
            )
            if own_transaction:
                conn.commit()
        except (sqlite3.OperationalError, sqlite3.IntegrityError):
            # If even the retry-flag UPDATE fails mareforma is in a
            # worse state than this helper can paper over. Log it, but
            # do not propagate, when we own the transaction the originating
            # write already committed; when we don't, the caller's rollback
            # cleans up. The WARNING below makes the failure visible.
            pass
        import logging
        logging.getLogger("mareforma").warning(
            "Convergence detection swallowed %s for claim %s: %s "
            "(retry flag set; call graph.refresh_convergence() to retry)",
            type(exc).__name__, new_claim_id, exc,
        )
        return False


def _maybe_update_replicated_best_effort(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
    supports: list[str],
    generated_by: str,
    artifact_hash: str | None,
    *,
    strict_promotion: bool = False,
) -> None:
    """Re-check REPLICATED after a caller-owned flag flip without losing work.

    The flag-flip sites (mark_claim_logged, mark_claim_resolved, update_claim)
    re-run convergence inside their own transaction. A bare
    ``except OperationalError: pass`` here left the claim PRELIMINARY with no
    retry flag on a transient lock, invisible to refresh_convergence and health.
    Route them through the same wrapper add_claim uses (``own_transaction=False``,
    so it sets ``convergence_retry_needed`` on transient failure without
    committing the caller's transaction), and record a health event when the
    re-check is stranded so the strand is not silent.
    """
    ok = _maybe_update_replicated(
        conn, claim_id, supports, generated_by, artifact_hash,
        own_transaction=False, strict_promotion=strict_promotion,
    )
    if not ok:
        from mareforma.health import append_health_event
        append_health_event(
            root, "convergence_retry", outcome="degraded", claim_id=claim_id,
        )


def list_convergence_retry_claims(
    conn: sqlite3.Connection,
) -> list[dict]:
    """Return every claim with ``convergence_retry_needed = 1``.

    Caller-side iteration target for the retry path. Rows are returned
    in ``created_at`` order so a retry pass that promotes peer claims
    sees the earlier upstream first.
    """
    rows = conn.execute(
        f"SELECT {_CLAIM_SELECT} FROM claims "
        "WHERE convergence_retry_needed = 1 "
        "ORDER BY created_at, claim_id"
    ).fetchall()
    return [dict(r) for r in rows]


def clear_convergence_retry_flag(
    conn: sqlite3.Connection, root: Path, claim_id: str,
) -> None:
    """Clear ``convergence_retry_needed`` on a single claim after retry.

    Mirrors :func:`mark_claim_resolved`: flag-flip + TOML mirror update.
    """
    conn.execute(
        "UPDATE claims SET convergence_retry_needed = 0 "
        "WHERE claim_id = ?",
        (claim_id,),
    )
    conn.commit()
    _backup_claims_toml(conn, root)


def find_dangling_supports(conn: sqlite3.Connection) -> list[dict]:
    """Return UUID-shaped ``supports[]`` entries that point to no local claim.

    A ``supports`` entry can be:

      * a UUID-shaped string: interpreted by mareforma as a claim_id;
      * a DOI like ``10.1234/abc``: an external reference;
      * any other free-form string: also treated as external.

    Only UUID-shaped entries can plausibly point at a local claim and so
    only those are checked. A dangling reference is not necessarily a
    bug: it could legitimately reference a claim from another project,
    a not-yet-asserted upstream, or a DOI mistyped as a UUID. But
    operators auditing graph integrity want a single query that surfaces
    every such hanging arrow, so they can decide case by case.

    Returns a list of ``{"claim_id", "dangling_ref"}`` dicts, sorted by
    ``claim_id`` then ``dangling_ref`` for deterministic output. Returns
    an empty list when nothing is dangling.

    REPLICATED detection already refuses to promote on a dangling
    reference (it requires the referenced ESTABLISHED claim to actually
    exist and be open), so a dangling entry cannot trigger spurious
    promotion. This helper is for auditing, not enforcement.
    """
    rows = conn.execute(
        "SELECT c.claim_id, j.value AS ref "
        "FROM claims c, json_each(c.supports_json) j"
    ).fetchall()

    if not rows:
        return []

    candidates = [
        (row["claim_id"], row["ref"])
        for row in rows
        if isinstance(row["ref"], str) and _CLAIM_ID_RE.match(row["ref"])
    ]
    if not candidates:
        return []

    # One JSON-array variable, not one per ref: the distinct-citation count
    # scales with the graph and would cross SQLite's per-statement variable cap.
    refs = sorted({ref for (_cid, ref) in candidates})
    existing = {
        r["claim_id"]
        for r in conn.execute(
            "SELECT claim_id FROM claims "
            "WHERE claim_id IN (SELECT value FROM json_each(?))",
            (json.dumps(refs),),
        ).fetchall()
    }

    dangling = [
        {"claim_id": cid, "dangling_ref": ref}
        for (cid, ref) in candidates
        if ref not in existing
    ]
    dangling.sort(key=lambda r: (r["claim_id"], r["dangling_ref"]))
    return dangling


def _extract_validation_signer_keyid(validation_signature: str) -> str | None:
    """Return the signing keyid from a validation envelope, or None if the
    envelope is malformed.

    The envelope's ``signatures[0].keyid`` is the authoritative signer.
    Malformed envelopes return None: the gates short-circuit
    rather than failing closed on top of the (already-failing) signing
    layer; the underlying UPDATE will then proceed via the legacy path
    and the row's ``validation_signature`` column will carry the broken
    envelope for later forensic inspection.
    """
    try:
        envelope = json.loads(validation_signature)
        return envelope["signatures"][0]["keyid"]
    except (json.JSONDecodeError, KeyError, IndexError, TypeError):
        return None


def _refuse_llm_validator(conn: sqlite3.Connection, validator_keyid: str) -> None:
    """Raise :class:`LLMValidatorPromotionError` if *validator_keyid* is an
    enrolled validator whose ``validator_type`` is ``'llm'``.

    A keyid that is not enrolled (no row in validators) does not trip
    this gate; that case is the enrollment check in
    ``_graph.validate`` and need not be re-litigated here.
    """
    row = conn.execute(
        "SELECT validator_type FROM validators WHERE keyid = ?",
        (validator_keyid,),
    ).fetchone()
    if row is None:
        return
    if row["validator_type"] == "llm":
        raise LLMValidatorPromotionError(
            f"Validator {validator_keyid[:12]}… is enrolled with "
            "validator_type='llm'. LLM validators may sign validation "
            "envelopes but cannot promote a claim past REPLICATED. "
            "Have a human-typed validator co-sign or re-sign to promote."
        )


def _refuse_llm_contradiction_issuer(
    conn: sqlite3.Connection, validator_keyid: str,
) -> None:
    """Raise :class:`LLMValidatorPromotionError` if *validator_keyid* is an
    enrolled LLM-typed validator attempting to issue a contradiction.

    Symmetric to :func:`_refuse_llm_validator`. A signed contradiction
    sets ``t_invalid`` on the older of two claims via the
    ``contradiction_invalidates_older`` trigger; that is equivalent in
    blast radius to demoting a human-validated ESTABLISHED claim (it
    drops from default ``query()`` results). The human-only rule must
    apply to both directions of the trust ladder: humans-only-to-promote
    AND humans-only-to-demote. Without this gate an enrolled LLM key
    could mark down any ESTABLISHED claim by signing a contradiction,
    breaking the README's "promotion requires a human" framing in the
    opposite direction.

    A keyid that is not enrolled (no row in validators) does not trip
    this gate; the enrollment check in :func:`_require_enrolled_issuer`
    handles that case.
    """
    row = conn.execute(
        "SELECT validator_type FROM validators WHERE keyid = ?",
        (validator_keyid,),
    ).fetchone()
    if row is None:
        return
    if row["validator_type"] == "llm":
        raise LLMValidatorPromotionError(
            f"Validator {validator_keyid[:12]}… is enrolled with "
            "validator_type='llm'. LLM validators may sign validation "
            "envelopes but cannot issue contradictions that invalidate "
            "human-validated claims, the human-only rule applies to "
            "both promotion AND demotion. Have a human-typed validator "
            "sign the contradiction instead."
        )


def _canonical_envelope(envelope_str: str | None) -> str | None:
    """Canonicalise a JSON envelope so byte-level comparison is stable.

    Two semantically identical envelopes that differ only in JSON key
    order or whitespace should compare equal during idempotency
    reconciliation. Refuses non-JSON input and refuses JSON that
    isn't shaped like a DSSE envelope (top-level object with a
    ``signatures`` list of objects carrying ``keyid`` + ``sig``);
    mareforma stores this on the federation-import path and
    callers who passed garbage previously got a silent fallback.

    Mareforma does NOT cross-verify the envelope against any
    source-graph validator set; that is an adapter responsibility.
    Shape validation alone keeps tamperers from poisoning the
    column with non-DSSE content.
    """
    if envelope_str is None:
        return None
    if not isinstance(envelope_str, str):
        raise ValueError(
            f"original_signature_bundle must be a JSON string; got "
            f"{type(envelope_str).__name__}"
        )
    try:
        parsed = json.loads(envelope_str)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError(
            f"original_signature_bundle is not valid JSON: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError(
            "original_signature_bundle must decode to a JSON object"
        )
    sigs = parsed.get("signatures")
    if not isinstance(sigs, list) or not sigs:
        raise ValueError(
            "original_signature_bundle: signatures must be a non-empty list"
        )
    for s in sigs:
        if not isinstance(s, dict):
            raise ValueError(
                "original_signature_bundle: signature entries must be objects"
            )
        if not isinstance(s.get("keyid"), str) or not s.get("keyid"):
            raise ValueError(
                "original_signature_bundle: every signature entry needs a keyid"
            )
        if not isinstance(s.get("sig"), str) or not s.get("sig"):
            raise ValueError(
                "original_signature_bundle: every signature entry needs a sig"
            )
    return json.dumps(parsed, sort_keys=True, separators=(",", ":"))



def _refuse_self_verdict(
    conn: sqlite3.Connection,
    issuer_keyid: str,
    claim_id: str,
    *,
    relation: str,
    verdict_kind: str,
) -> None:
    """Raise :class:`VerdictIssuerError` if *issuer_keyid* signed ANY
    role on *claim_id*'s envelope.

    Walks every keyid in the claim's ``signature_bundle.signatures[*]``
    so a planner / executor / reviewer / validator on a
    ``claim-with-roles:v1`` envelope cannot also issue a replication
    or contradiction verdict on the same claim.

    Unsigned claims (``signature_bundle IS NULL``) pass the gate: same
    posture as :func:`_refuse_self_validation`. The gate is layered
    AFTER the enrollment check, so a non-enrolled key was already
    rejected; here the issuer is enrolled and we just check role
    overlap.
    """
    row = conn.execute(
        "SELECT signature_bundle FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None or row["signature_bundle"] is None:
        return
    # Reject malformed / empty-array envelopes outright, they would
    # slip through the keyid-match check and let the issuer bypass
    # the self-verdict gate either by corrupting the JSON or by
    # writing a structurally-empty signatures array. Fail closed.
    try:
        _bundle = json.loads(row["signature_bundle"])
    except (json.JSONDecodeError, TypeError):
        raise VerdictIssuerError(
            f"{verdict_kind} verdict refused: claim '{claim_id}' "
            "(relation=" + relation + ") has a signature_bundle that "
            "is not valid JSON. Refusing to gate against an unknowable "
            "identity set."
        )
    if not isinstance(_bundle, dict):
        raise VerdictIssuerError(
            f"{verdict_kind} verdict refused: claim '{claim_id}' "
            "(relation=" + relation + ") signature_bundle did not "
            "decode to a JSON object."
        )
    _sigs = _bundle.get("signatures")
    if _sigs is not None and (
        not isinstance(_sigs, list) or len(_sigs) == 0
    ):
        raise VerdictIssuerError(
            f"{verdict_kind} verdict refused: claim '{claim_id}' "
            "(relation=" + relation + ") has a signature_bundle "
            "whose signatures field is empty or non-list. Refusing "
            "to gate against an empty identity set."
        )
    keyids = _claim_signer_keyids(row["signature_bundle"])
    if issuer_keyid in keyids:
        try:
            bundle = json.loads(row["signature_bundle"])
            role = next(
                (s.get("role") for s in bundle.get("signatures") or []
                 if isinstance(s, dict) and s.get("keyid") == issuer_keyid),
                None,
            ) or "asserter"
        except (json.JSONDecodeError, TypeError, AttributeError):
            role = "asserter"
        raise VerdictIssuerError(
            f"{verdict_kind} verdict issuer {issuer_keyid[:12]}… signed "
            f"claim '{claim_id}' (relation={relation}) as {role!r}; "
            "self-verdicts are refused. The issuer must be an external "
            "witness whose keyid does not appear on the claim envelope."
        )


def _claim_signer_keyids(claim_signature_bundle: str | None) -> list[str]:
    """Return every keyid that signed the claim envelope.

    For ``claim:v1`` envelopes (single signature) the result has one
    entry, the asserter. For ``claim-with-roles:v1`` envelopes (multi-
    signature) the result has one entry per role-actor (planner /
    executor / reviewer / validator).

    Malformed bundles return an empty list: mareforma cannot
    decide identity against a corrupted envelope, so downstream gates
    short-circuit and the row falls through to whatever pre-existing
    layer handles unsigned data. Same conservative posture as the
    earlier single-sig code path.
    """
    if claim_signature_bundle is None:
        return []
    try:
        bundle = json.loads(claim_signature_bundle)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(bundle, dict):
        return []
    sigs = bundle.get("signatures")
    # Structurally invalid envelopes, empty signatures list or a non-
    # list signatures field, must NOT silently collapse to []. Callers
    # use that empty result to mean "unsigned claim, gates pass". An
    # empty-array envelope would slip through every keyid match.
    # _refuse_self_validation / _refuse_self_verdict layer the explicit
    # rejection on top; this helper preserves the "no envelope" return
    # only for the genuine no-envelope case (signatures key absent).
    if sigs is None:
        return []
    if not isinstance(sigs, list):
        return []
    return [
        s["keyid"] for s in sigs
        if isinstance(s, dict) and isinstance(s.get("keyid"), str)
    ]


def _refuse_self_validation(
    claim_id: str,
    claim_signature_bundle: str | None,
    validator_keyid: str,
) -> None:
    """Raise :class:`SelfValidationError` if the validator signed ANY
    role on the claim envelope.

    Walks every keyid in ``signature_bundle.signatures[*].keyid``:
    the primary asserter AND any role-attestation signer (planner /
    executor / reviewer / validator on a ``claim-with-roles:v1``
    envelope). Promotion to ESTABLISHED requires a witnessing
    validator whose keyid does not appear on the envelope at all.

    Unsigned claims (``signature_bundle IS NULL``) carry no signer
    identity to compare against and pass this gate. A malformed bundle
    is treated as absent (conservative posture).
    """
    matched_role: str | None = None
    if claim_signature_bundle is not None:
        try:
            bundle = json.loads(claim_signature_bundle)
        except (json.JSONDecodeError, TypeError):
            # Fail closed: a non-NULL bundle that doesn't parse means
            # the keyid set is unknowable, and the safe posture for a
            # self-loop refusal gate is to refuse the validation
            # rather than pass it.
            raise SelfValidationError(
                f"Claim '{claim_id}' has a signature_bundle that is "
                "not valid JSON. Refusing to gate self-validation "
                "against an unknowable identity set; investigate the "
                "row's signature_bundle column before retrying."
            )
        if not isinstance(bundle, dict):
            raise SelfValidationError(
                f"Claim '{claim_id}' signature_bundle did not decode "
                "to a JSON object; refusing to validate."
            )
        signatures = bundle.get("signatures")
        # A signed claim with an empty / non-list signatures field is a
        # structurally invalid envelope. Treating it as "absent" would
        # let a tamperer drop their keyid from the gate and self-promote
        #, refuse the operation rather than silently pass.
        if signatures is None:
            return  # no signature_bundle subfield at all → unsigned
        if not isinstance(signatures, list) or not signatures:
            raise SelfValidationError(
                f"Claim '{claim_id}' has a signature_bundle whose "
                "signatures field is empty or non-list. Refusing to "
                "gate self-validation against an empty identity set."
            )
        for sig in signatures:
            if not isinstance(sig, dict):
                continue
            if sig.get("keyid") == validator_keyid:
                matched_role = sig.get("role") or "asserter"
                break
    if matched_role is not None:
        raise SelfValidationError(
            f"Validator {validator_keyid[:12]}… signed claim "
            f"'{claim_id}' as {matched_role!r}; self-promotion is "
            "refused. Promotion requires a witnessing validator "
            "whose keyid does not appear on the claim envelope. "
            "Have a different enrolled key call graph.validate(...)."
        )


def _refuse_self_validation_across_set(
    conn: sqlite3.Connection, claim_id: str, validator_keyid: str,
) -> None:
    """Refuse a validator that asserted ANY claim in the converging set.

    The claim being promoted is REPLICATED: it converged with peer claims
    that share an ESTABLISHED+open anchor and carry distinct asserter keyids.
    A validator whose keyid equals the ``asserter_keyid`` of any claim in that
    set is a participant witnessing its own convergence into ESTABLISHED, so
    the promotion is refused. :func:`_refuse_self_validation` already covers
    the claim's own signers; this extends the refusal to the converging peers.
    """
    sup_row = conn.execute(
        "SELECT supports_json FROM claims WHERE claim_id = ?", (claim_id,),
    ).fetchone()
    if sup_row is None:
        return
    try:
        supports = json.loads(sup_row["supports_json"] or "[]")
    except (json.JSONDecodeError, TypeError) as exc:
        # Fail closed: a malformed supports_json on the row being promoted is a
        # tamper signal, not a normal state (the system always writes valid
        # JSON). We cannot enumerate the converging peers to clear the across-set
        # refusal, so refuse rather than fall through to validation. Mirrors
        # _refuse_self_validation failing closed on an unparseable bundle.
        raise SelfValidationError(
            f"Cannot verify the converging set behind '{claim_id}': its "
            f"supports_json does not parse ({exc}). Refusing validation."
        ) from exc
    if not supports:
        return
    sup_placeholders = ",".join("?" * len(supports))
    anchors = [
        r["claim_id"] for r in conn.execute(
            f"SELECT claim_id FROM claims "
            f"WHERE claim_id IN ({sup_placeholders}) "
            f"AND support_level = 'ESTABLISHED' AND status = 'open'",
            supports,
        ).fetchall()
    ]
    if not anchors:
        return
    placeholders = ",".join("?" * len(anchors))
    peer_keyids = {
        r["asserter_keyid"] for r in conn.execute(
            f"SELECT DISTINCT c.asserter_keyid "
            f"FROM claims c, json_each(c.supports_json) j "
            f"WHERE j.value IN ({placeholders}) "
            f"AND c.support_level = 'REPLICATED' "
            f"AND c.status = 'open' "
            f"AND c.asserter_keyid IS NOT NULL",
            anchors,
        ).fetchall()
    }
    if validator_keyid in peer_keyids:
        raise SelfValidationError(
            f"Validator {validator_keyid[:12]}… asserted a claim in the "
            f"converging set behind '{claim_id}'; a participant cannot "
            "witness its own convergence into ESTABLISHED. Have an "
            "independent enrolled key call graph.validate(...)."
        )


def _verify_evidence_seen(
    conn: sqlite3.Connection,
    promoted_claim_id: str,
    evidence_seen: list[str],
    validated_at: str,
) -> None:
    """Verify every entry in ``evidence_seen`` is a valid citation.

    Each entry must be:
      * a string,
      * a strict-v4 UUID (``_is_claim_id``),
      * the id of a claim that exists in this graph,
      * a claim whose ``created_at`` is no later than ``validated_at``.

    Raises :class:`EvidenceCitationError` naming the first failing entry.
    An empty list is the explicit "I reviewed nothing" admission and
    passes the gate without inspection.

    The validator's enumeration is self-declared: this gate cannot
    prove the validator actually opened those claims, only that the
    claims they cited exist and predate validation. That's the
    strongest property mareforma can enforce; everything else
    rests on the validator's honesty.
    """
    if not evidence_seen:
        return
    for entry in evidence_seen:
        if not isinstance(entry, str):
            raise EvidenceCitationError(
                f"evidence_seen entry {entry!r} is not a string."
            )
        if not _is_claim_id(entry):
            raise EvidenceCitationError(
                f"evidence_seen entry '{entry}' is not a strict-v4 UUID; "
                "only local claim_ids can be cited as reviewed evidence."
            )
        if entry == promoted_claim_id:
            raise EvidenceCitationError(
                f"evidence_seen cites the claim being promoted "
                f"('{promoted_claim_id}'); the validator cannot count "
                "the promotion target as evidence for itself."
            )
        row = conn.execute(
            "SELECT created_at FROM claims WHERE claim_id = ?",
            (entry,),
        ).fetchone()
        if row is None:
            raise EvidenceCitationError(
                f"evidence_seen entry '{entry}' does not exist in the "
                "graph; cite only claims the validator actually reviewed."
            )
        cited_created_at = row["created_at"]
        if cited_created_at > validated_at:
            raise EvidenceCitationError(
                f"evidence_seen entry '{entry}' was created at "
                f"{cited_created_at} which is after validated_at "
                f"{validated_at}; the validator could not have reviewed "
                "a claim that didn't exist yet."
            )


def validate_claim(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
    *,
    validated_by: str | None = None,
    validation_signature: str | None = None,
    validated_at: str | None = None,
    evidence_seen: list[str] | None = None,
) -> None:
    """Promote a REPLICATED claim to ESTABLISHED (human validation).

    Parameters
    ----------
    validation_signature:
        Required JSON-encoded DSSE-style envelope binding
        ``(claim_id, validator_keyid, validated_at, evidence_seen)``.
        Produced by :func:`mareforma.signing.sign_validation` and stored
        verbatim on the row so the validation event itself is
        independently verifiable (tampering with
        ``validated_by``/``validated_at``/``evidence_seen`` post-hoc is
        detectable). Promotion to ESTABLISHED is gated on this envelope:
        an unsigned call raises ``ValueError`` up front because the
        storage layer refuses an ESTABLISHED row with a NULL signature,
        so there is no unsigned promotion path.
    validated_at:
        Optional ISO 8601 UTC timestamp to write to the row. The caller
        signs a validation envelope binding a timestamp, so the SAME
        timestamp must be threaded through here for the envelope's
        ``validated_at`` to match the row's ``validated_at``
        byte-for-byte; a divergent value is rejected by the
        envelope-agreement gate. If ``None``, a fresh timestamp is
        generated (only self-consistent when the envelope was signed
        against that exact value).
    evidence_seen:
        Optional list of claim_ids the validator declares to have
        reviewed before signing the promotion. ``None`` is normalized
        to ``[]`` and bound into the signed envelope: a positive
        statement that the validator reviewed nothing, which is then
        visible in the audit trail rather than hidden by absence. Each
        cited entry must be a strict-v4 UUID matching an existing
        claim with ``created_at <= validated_at``. The validator's
        enumeration is self-declared; mareforma cannot prove they
        actually opened the cited claims, but it CAN verify the cited
        claims exist and predate validation.

    Verification gates
    ------------------
    When ``validation_signature`` is supplied, mareforma fires the
    following defense-in-depth gates before the row is updated. All
    consult mareforma directly, calling :func:`validate_claim`
    bypassing :meth:`EpistemicGraph.validate` does not relax any of
    them, so a hostile in-process caller cannot route around them:

    1. The envelope must parse as JSON and carry a ``payloadType`` in
       ``{PAYLOAD_TYPE_VALIDATION, PAYLOAD_TYPE_SEED}`` (raises
       :class:`InvalidValidationEnvelopeError` on either failure).
    2. The envelope's signing keyid must be an enrolled validator
       (raises :class:`InvalidValidationEnvelopeError`).
    3. The envelope must verify cryptographically against the claimed
       signer's public key via :func:`signing.verify_envelope` (raises
       :class:`InvalidValidationEnvelopeError`).
    4. The signing validator's ``validator_type`` must be ``'human'``.
       An ``'llm'``-typed validator can sign a validation envelope but
       cannot promote past REPLICATED (raises
       :class:`LLMValidatorPromotionError`).
    5. The validator's keyid must NOT match the claim's
       ``signature_bundle`` signing keyid. Self-validation is the
       trivial-loop attack (raises :class:`SelfValidationError`).
    6. The envelope's signed payload must agree on ``claim_id``,
       ``validator_keyid``, and the timestamp (``validated_at`` for
       validation envelopes, ``seeded_at`` for seed envelopes) with the
       row being promoted and the kwargs being written (raises
       :class:`InvalidValidationEnvelopeError`).
    7. The envelope's ``evidence_seen`` field must equal the
       ``evidence_seen`` kwarg, and every cited entry must be a
       strict-v4 UUID matching an existing claim with
       ``created_at <= validated_at`` (raises
       :class:`EvidenceCitationError`).

    Raises
    ------
    ClaimNotFoundError
        If no claim with claim_id exists.
    ValueError
        If ``validation_signature`` is ``None`` (promotion requires a
        signed envelope; there is no unsigned path), or if the claim's
        support_level is not 'REPLICATED', or its status is not 'open'
        (contested/retracted claims are editorially tainted and must not
        be promoted; revisit the editorial flag via update_claim before
        validating).
    InvalidValidationEnvelopeError
        If the validation envelope is malformed, wrong-typed, signed
        by a non-enrolled key, fails cryptographic verification, or
        its payload disagrees with the row or kwargs on ``claim_id``,
        ``validator_keyid``, or the timestamp.
    LLMValidatorPromotionError
        If the validation envelope is signed by an LLM-typed validator.
    SelfValidationError
        If the validation envelope's signing keyid equals the claim's
        ``signature_bundle`` signing keyid.
    EvidenceCitationError
        If any entry in ``evidence_seen`` is not a strict-v4 UUID, does
        not point to an existing claim, or points to a claim with
        ``created_at > validated_at``.
    """
    if validation_signature is None:
        # Promotion to ESTABLISHED writes validation_signature straight
        # from this kwarg. A NULL signature is refused by both the table
        # CHECK and the claims_update_state_check trigger, so an unsigned
        # call could only ever surface as an IllegalStateTransitionError
        # that reads like row corruption. Reject it here with a message
        # that names the real requirement.
        raise ValueError(
            f"validate_claim for claim '{claim_id}' requires a signed "
            "validation envelope; promotion to ESTABLISHED has no unsigned "
            "path. Build the envelope with mareforma.signing.sign_validation "
            "or call graph.validate() from an enrolled session."
        )
    row = conn.execute(
        "SELECT support_level, status, signature_bundle, t_invalid "
        "FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")
    if row["support_level"] != "REPLICATED":
        raise ValueError(
            f"Claim '{claim_id}' has support_level='{row['support_level']}'. "
            "Only REPLICATED claims can be promoted to ESTABLISHED."
        )
    if row["status"] != "open":
        raise ValueError(
            f"Claim '{claim_id}' has status='{row['status']}'. "
            "Only claims with status='open' can be promoted to ESTABLISHED. "
            "Reset the status via update_claim if the editorial flag no "
            "longer applies."
        )
    if row["t_invalid"] is not None:
        # A signed contradiction verdict from an enrolled validator has
        # marked this claim invalid. Promotion would ride past the
        # terminal evidence and let validate() lift an already-refuted
        # claim back into the trust ladder.
        raise ValueError(
            f"Claim '{claim_id}' was invalidated by a signed contradiction "
            f"verdict at t_invalid={row['t_invalid']!r}. Refuse to promote "
            "an invalidated claim to ESTABLISHED."
        )

    # Verification gates over the validation envelope.
    #
    # validate_claim is a public-by-convention function (no leading
    # underscore) and is callable directly by any in-process code path , 
    # not only :meth:`EpistemicGraph.validate`. The wrapper builds the
    # envelope with the graph's loaded signer, so the wrapper path is
    # safe by construction; this function is the defense-in-depth layer
    # that must also be safe when called with a caller-supplied envelope.
    #
    # Without cryptographic verification here, an enrolled LLM-typed
    # validator (or any in-process caller) could hand-craft an envelope
    # JSON claiming a human validator's keyid + a garbage signature,
    # then call ``db.validate_claim`` directly. Mareforma would
    # consult the CLAIMED keyid to enforce the trust-ladder gates
    # (LLM-type, self-validation), find them satisfied, and persist a
    # fraudulent ESTABLISHED row anchored by an envelope that does not
    # verify against the impersonated signer's public key. Restore would
    # eventually catch it, but the live DB would already have shipped
    # bad data to whoever queried in the meantime.
    #
    # Order of operations:
    #   1. Decode the envelope structure (refuse malformed JSON).
    #   2. Restrict ``payloadType`` to validation or seed, same set the
    #      restore path accepts on this column.
    #   3. Look up the claimed signer in the validators table.
    #   4. Cryptographically verify the envelope with the signer's
    #      pubkey via :func:`signing.verify_envelope`.
    #   5. Apply the trust-ladder gates (LLM-type ceiling, self-
    #      validation refusal). These can now safely consult the
    #      validator_keyid because step 4 proved the signer actually
    #      holds the private key.
    #   6. Compare the envelope's payload fields against the row + the
    #      kwargs mareforma is about to write, claim_id, the
    #      timestamp, validator_keyid, and evidence_seen all must
    #      agree byte-for-byte.
    #
    # An unsigned call was rejected up front, so validation_signature is
    # always present here; the ``is not None`` guard below is a defensive
    # restatement of that contract, not a live unsigned branch.
    validator_keyid: str | None = None
    env: dict | None = None
    declared_type: str | None = None
    if validation_signature is not None:
        from mareforma import signing as _signing
        from mareforma import validators as _validators

        try:
            env = json.loads(validation_signature)
            validator_keyid = env["signatures"][0]["keyid"]
            declared_type = env["payloadType"]
        except (json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
            raise InvalidValidationEnvelopeError(
                f"validation_signature for claim '{claim_id}' is malformed "
                f"({exc}); cannot extract signer or payloadType."
            ) from exc

        # The validation_signature column carries either a validation
        # envelope (REPLICATED→ESTABLISHED) or a seed envelope (born-
        # ESTABLISHED). Anything else is a type confusion attempt , 
        # cross-type acceptance lets an attacker pass an enrollment or
        # claim envelope through a verifier expecting a validation
        # event. verify_envelope's expected_payload_type is the formal
        # guard; the early-rejection here gives a clear error message.
        if declared_type not in (
            _signing.PAYLOAD_TYPE_VALIDATION,
            _signing.PAYLOAD_TYPE_SEED,
        ):
            raise InvalidValidationEnvelopeError(
                f"validation_signature payloadType {declared_type!r} for "
                f"claim '{claim_id}' is neither validation nor seed; "
                "refusing to persist a wrong-typed envelope as validation."
            )

        signer_row = _validators.get_validator(conn, validator_keyid)
        if signer_row is None:
            raise InvalidValidationEnvelopeError(
                f"validation_signature for claim '{claim_id}' is signed by "
                f"keyid {validator_keyid[:12]}… which is not an enrolled "
                "validator on this graph. Enroll the signer first via "
                "graph.enroll_validator() or call graph.validate() from a "
                "session whose loaded signer is already enrolled."
            )

        try:
            signer_pem = base64.standard_b64decode(signer_row["pubkey_pem"])
            signer_pub = _signing.public_key_from_pem(signer_pem)
            sig_ok = _signing.verify_envelope(
                env, signer_pub, expected_payload_type=declared_type,
            )
        except (ValueError, TypeError, _signing.SigningError) as exc:
            raise InvalidValidationEnvelopeError(
                f"validation_signature for claim '{claim_id}' did not verify "
                f"cryptographically against keyid {validator_keyid[:12]}…: "
                f"{exc}"
            ) from exc
        if not sig_ok:
            raise InvalidValidationEnvelopeError(
                f"validation_signature for claim '{claim_id}' failed Ed25519 "
                f"verification against keyid {validator_keyid[:12]}…. The "
                "envelope is not authorized by the claimed signer."
            )

        # Trust-ladder gates run AFTER signature verification, so the
        # validator_keyid is now known to be authentic, not just claimed.
        _refuse_llm_validator(conn, validator_keyid)
        _refuse_self_validation(
            claim_id, row["signature_bundle"], validator_keyid,
        )
        _refuse_self_validation_across_set(conn, claim_id, validator_keyid)

    now = validated_at if validated_at is not None else _now()

    # Envelope/kwarg/row payload-field agreement. verify_envelope above
    # proved the signer signed THESE BYTES, but it does NOT prove the
    # signed payload describes the row being updated. Without these
    # equality checks a caller could replay a legitimate validation
    # envelope from claim A onto row B (matching signer + matching
    # cryptography), promoting B to ESTABLISHED with an envelope that
    # binds a different claim_id and timestamp. Restore would catch the
    # divergence; this is the live-DB equivalent of the restore-path
    # checks at ``_verify_claim_signatures_on_restore``.
    if validation_signature is not None and env is not None:
        # envelope_payload raises InvalidEnvelopeError when the signed
        # payload bytes fail to base64-decode or do not parse as a JSON
        # object. verify_envelope only checks the DSSE PAE signature;
        # it does NOT enforce that the payload bytes are well-formed.
        # An enrolled validator with a real key could (intentionally or
        # by bug) sign non-JSON bytes; without this try/except the
        # InvalidEnvelopeError would propagate past mareforma's
        # documented contract.
        try:
            env_payload = _signing.envelope_payload(env)
        except _signing.InvalidEnvelopeError as exc:
            raise InvalidValidationEnvelopeError(
                f"validation envelope's signed payload is not a JSON "
                f"object ({exc}); refusing to persist an envelope whose "
                "payload contract is malformed."
            ) from exc
        if env_payload.get("claim_id") != claim_id:
            raise InvalidValidationEnvelopeError(
                f"validation envelope binds claim_id "
                f"{env_payload.get('claim_id')!r} but the row being promoted "
                f"is {claim_id!r}; envelope replay across claims refused."
            )
        if env_payload.get("validator_keyid") != validator_keyid:
            raise InvalidValidationEnvelopeError(
                "validation envelope's payload.validator_keyid does not "
                "match the signing keyid; envelope is internally "
                "inconsistent and refused."
            )
        # Seed envelopes bind ``seeded_at``; validation envelopes bind
        # ``validated_at``. The row's ``validated_at`` is being written
        # from ``now`` either way, so the comparison key is uniform on
        # the row side and varies only on the envelope side.
        timestamp_field = (
            "validated_at"
            if declared_type == _signing.PAYLOAD_TYPE_VALIDATION
            else "seeded_at"
        )
        if env_payload.get(timestamp_field) != now:
            raise InvalidValidationEnvelopeError(
                f"validation envelope's {timestamp_field} "
                f"({env_payload.get(timestamp_field)!r}) does not match the "
                f"validated_at value being written ({now!r}); envelope "
                "timestamp must agree with mareforma's write."
            )
        # evidence_seen is bound only on validation envelopes; seed
        # envelopes have no analog. Skip the comparison for seeds.
        if declared_type == _signing.PAYLOAD_TYPE_VALIDATION:
            env_evidence = env_payload.get("evidence_seen")
            kwarg_evidence = evidence_seen if evidence_seen is not None else []
            if env_evidence != kwarg_evidence:
                raise EvidenceCitationError(
                    "validation envelope's evidence_seen "
                    f"({env_evidence!r}) does not match the evidence_seen "
                    f"kwarg ({kwarg_evidence!r}); mareforma validates "
                    "what the caller passed, and the signed envelope must "
                    "bind the same list, refusing to persist a divergent "
                    "envelope."
                )

    # Evidence-citation gate. Every entry in evidence_seen must be a
    # strict-v4 UUID pointing at an existing claim that predates the
    # validation timestamp. An empty list is the "I reviewed nothing"
    # admission and passes the gate. None is normalized to [].
    _verify_evidence_seen(
        conn, claim_id, evidence_seen or [], now,
    )
    # The early gate above ran in an autocommit SELECT, then the crypto and
    # evidence-citation checks ran with no transaction open. Wrap the write in
    # BEGIN IMMEDIATE and re-assert the gate on the UPDATE itself so a signed
    # contradiction (t_invalid) or retraction (status) that lands in the
    # check-to-write window cannot ride into ESTABLISHED. Mirrors
    # record_replication_verdict's guarded promotion; when the caller already
    # holds a transaction its outer commit flushes this write.
    _own_txn = not conn.in_transaction
    try:
        if _own_txn:
            conn.execute("BEGIN IMMEDIATE")
        # COALESCE on validator_keyid guards a repeat signed re-validate.
        # The state-check trigger permits ESTABLISHED → ESTABLISHED, so a
        # second promotion can land on an already-validated row. Each
        # signed call carries its own authenticated validator_keyid
        # (unsigned calls are rejected up front), so the new signer's
        # keyid is written; COALESCE is the belt that keeps a stray NULL
        # from clearing the column and tanking a validator's reputation
        # count.
        cur = conn.execute(
            """
            UPDATE claims
            SET support_level = 'ESTABLISHED',
                validated_by = ?,
                validated_at = ?,
                validation_signature = ?,
                validator_keyid = COALESCE(?, validator_keyid),
                updated_at   = ?
            WHERE claim_id = ?
              AND support_level = 'REPLICATED'
              AND status = 'open'
              AND t_invalid IS NULL
            """,
            (validated_by, now, validation_signature, validator_keyid,
             now, claim_id),
        )
        if cur.rowcount == 0:
            # The guarded UPDATE matched nothing: a concurrent signed
            # contradiction set t_invalid (or a retraction flipped status)
            # after the early gate passed. Refuse rather than commit a silent
            # no-op, mirroring the early t_invalid refusal above.
            if _own_txn:
                conn.rollback()
            raise ValueError(
                f"Claim '{claim_id}' was invalidated by a signed contradiction "
                "verdict during validation (the check-to-write window closed). "
                "Refuse to promote an invalidated claim to ESTABLISHED."
            )
        if _own_txn:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        if _own_txn:
            conn.rollback()
        translated = _state_error_from_integrity(exc)
        if translated is not None:
            raise translated from exc
        raise DatabaseError(f"Failed to validate claim '{claim_id}': {exc}") from exc
    except sqlite3.OperationalError as exc:
        if _own_txn:
            conn.rollback()
        raise DatabaseError(f"Failed to validate claim '{claim_id}': {exc}") from exc
    _backup_claims_toml(conn, root)


def list_unresolved_claims(conn: sqlite3.Connection) -> list[dict]:
    """Return all claims currently marked unresolved=True."""
    rows = conn.execute(
        f"SELECT {_CLAIM_SELECT} FROM claims WHERE unresolved = 1 ORDER BY created_at"
    ).fetchall()
    return [dict(r) for r in rows]


def _attempt_rekor_saga(
    conn: sqlite3.Connection,
    root: Path,
    *,
    claim_id: str,
    envelope: dict,
    signer: "object",
    rekor_url: str,
    require_rekor: bool,
    trust_insecure_rekor: bool = False,
    rekor_log_pubkey_pem: bytes | None = None,
    own_transaction: bool = True,
) -> int:
    """Run the Rekor 4-step saga on a freshly-INSERTed signed claim.

    Returns the new ``transparency_logged`` value to write back to the
    caller's local variable (0 if the saga did not complete, 1 if the
    row UPDATE succeeded).

    Saga steps
    ----------
    1. The claim is already INSERTed with ``transparency_logged=0``
       (the caller's responsibility, before this helper runs).
    2. Submit the envelope to Rekor. On failure, warn and append a
       ``rekor_submit`` fail event to ``health.jsonl`` (so an outage is
       visible in ``mareforma activity``, not only in a per-claim trust
       map), then return 0.
    3. **(opt-in) Verify the inclusion proof.** If the caller supplied
       ``rekor_log_pubkey_pem``, re-fetch the entry via
       :func:`signing.fetch_inclusion_proof` and pass the full body to
       :func:`signing.verify_rekor_inclusion`. A verification failure
       refuses the saga (the row stays at ``transparency_logged=0``);
       a future ``refresh_unsigned`` will retry. When
       ``rekor_log_pubkey_pem`` is None, the residual gap is the trust
       posture documented in README "Limits of the Rekor integration".
    4. Persist the (uuid, logIndex, integratedTime) coords to the
       ``rekor_inclusions`` sidecar. The sidecar's append-only triggers
       guarantee no replay can rewrite this row.
    5. UPDATE the claim row's ``signature_bundle`` with the augmented
       envelope (Rekor block attached) and set ``transparency_logged=1``.

    If step 5 fails after step 4 succeeded, the sidecar holds the durable
    record. :meth:`EpistemicGraph.refresh_unsigned` reads the sidecar and
    replays step 5 instead of double-submitting to Rekor.

    Extracting this helper out of :func:`add_claim` keeps the
    happy-path read concise: ``add_claim`` is about claim insertion +
    chain integrity; the saga is a separate concern that lives next to
    its sidecar helper :func:`_record_rekor_inclusion`.

    Raises
    ------
    SigningError
        If the initial Rekor submission fails and ``require_rekor=True``,
        OR if Merkle inclusion-proof verification fails and
        ``require_rekor=True``.
    """
    from mareforma import signing as _signing

    logged, entry = _signing.submit_to_rekor(
        envelope, signer.public_key(), rekor_url=rekor_url,
        allow_insecure=trust_insecure_rekor,
    )
    if not logged or entry is None:
        if require_rekor:
            raise _signing.SigningError(
                f"Rekor submission to {rekor_url} failed and "
                "require_rekor=True. Claim was persisted with "
                "transparency_logged=0; call "
                "EpistemicGraph.refresh_unsigned() to retry."
            )
        warnings.warn(
            f"Rekor submission to {rekor_url} failed for claim {claim_id}. "
            "The claim is stored and signed, but transparency_logged stays 0, "
            "which blocks REPLICATED promotion until "
            "EpistemicGraph.refresh_unsigned() logs it.",
            stacklevel=2,
        )
        from mareforma.health import append_health_event
        append_health_event(
            root, "rekor_submit", outcome="fail", claim_id=claim_id,
        )
        return 0

    # Step 3 (opt-in): cryptographic inclusion-proof verification. The
    # submit-time response binding (OUR hash + OUR signature inside the
    # returned entry) is checked by submit_to_rekor; what's left to
    # close is "the log committed our entry and didn't tamper with it
    # afterward." That requires the signed checkpoint + Merkle audit
    # path, which submit_to_rekor's stripped response doesn't carry.
    # Re-fetch the entry by uuid (one extra GET) and run the full
    # verifier. Skipped when the caller hasn't supplied a log pubkey
    #, current trust posture is "trust submit-time response."
    proof_entry = None
    if rekor_log_pubkey_pem is not None:
        uuid = entry.get("uuid")
        if not isinstance(uuid, str) or not uuid:
            if require_rekor:
                raise _signing.SigningError(
                    "Rekor inclusion-proof verification requested but "
                    "the submit response had no uuid; cannot re-fetch "
                    "to obtain the inclusion proof."
                )
            return 0
        try:
            full_body = _signing.fetch_inclusion_proof(
                uuid, rekor_url, allow_insecure=trust_insecure_rekor,
            )
            _signing.verify_rekor_inclusion(
                full_body, rekor_log_pubkey_pem, envelope,
            )
            proof_entry = full_body
        except _signing.RekorInclusionError as exc:
            if require_rekor:
                raise _signing.SigningError(
                    f"Rekor inclusion-proof verification failed for "
                    f"claim {claim_id} (uuid {uuid}): {exc} "
                    f"[reason={exc.reason}]. require_rekor=True; refusing "
                    "to advance transparency_logged to 1."
                ) from exc
            warnings.warn(
                f"Rekor inclusion-proof verification failed for claim "
                f"{claim_id} (uuid {uuid}, reason={exc.reason}). The "
                "submit response itself was bound to OUR hash + sig, but "
                "the log's signed Merkle path did not verify. "
                "transparency_logged stays 0; refresh_unsigned() will "
                "retry.",
                stacklevel=2,
            )
            return 0

    # Step 4: durable sidecar write. Failure here means Rekor saw the
    # entry but we lost the record locally, the next refresh_unsigned
    # will re-submit and create a duplicate, which is the only recovery
    # path when no sidecar exists. _record_rekor_inclusion emits a
    # warning on that path; we honor its return value.
    if not _record_rekor_inclusion(
        conn, claim_id, entry,
        proof_entry=proof_entry, own_transaction=own_transaction,
    ):
        return 0

    # Step 4: augment the row's bundle with the Rekor coords and flip
    # the transparency flag. Failure here is benign: the sidecar holds
    # the truth, refresh_unsigned will replay this UPDATE from the
    # stored coords without re-submitting to Rekor.
    augmented = _signing.attach_rekor_entry(envelope, entry)
    new_bundle = json.dumps(
        augmented, sort_keys=True, separators=(",", ":"),
    )
    try:
        conn.execute(
            "UPDATE claims SET signature_bundle = ?, "
            "transparency_logged = 1, updated_at = ? "
            "WHERE claim_id = ?",
            (new_bundle, _now(), claim_id),
        )
        # Commit only when this call owns the transaction. When add_claim joined
        # a caller's open transaction (submit_finding's BEGIN IMMEDIATE),
        # committing here would flush the caller's in-flight writes early and
        # void its atomicity: a later fork or raise could no longer roll the
        # signed claim back, stranding an orphan. The caller's commit flushes
        # this UPDATE with the rest of its transaction.
        if own_transaction:
            conn.commit()
        return 1
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        warnings.warn(
            f"Claim {claim_id} accepted by Rekor (coords saved to "
            f"rekor_inclusions sidecar) but the local UPDATE failed "
            f"({exc}). transparency_logged remains 0; run "
            "EpistemicGraph.refresh_unsigned() to reconcile without "
            "re-submitting.",
            stacklevel=2,
        )
        return 0


def _record_rekor_inclusion(
    conn: sqlite3.Connection,
    claim_id: str,
    entry: dict,
    proof_entry: dict | None = None,
    own_transaction: bool = True,
) -> bool:
    """Step 3 of the Rekor saga: persist a successful inclusion.

    Called after Rekor returns a `(logged=True, entry)` response and
    before the claims-row UPDATE. The sidecar is the durable record of
    "Rekor witnessed this claim"; when the row UPDATE later fails,
    :meth:`refresh_unsigned` consults this table to replay the UPDATE
    from the coordinate columns instead of re-submitting.

    *entry* carries the submit-response coordinates (uuid, logIndex,
    integratedTime) that populate those columns. *proof_entry* is the
    re-fetched full entry, with ``body`` and ``verification``, verified
    against the pinned log key; when present it is what lands in
    ``raw_response_b64``, in the ``{uuid: entry}`` shape Rekor returns
    and :func:`mareforma.restore` re-verifies. Without a pinned log key
    there is no proof to store and the coordinates are stored instead,
    so such a row cannot be re-verified at restore time.

    Returns ``True`` on success. On failure, emits a WARNING and returns
    ``False``: the caller skips the subsequent UPDATE so we don't end
    up with `transparency_logged=1` but no sidecar record (the inverse
    of the gap this saga closes). The Rekor entry exists publicly; the
    operator must run :meth:`refresh_unsigned` which will detect the
    missing-sidecar-but-unflagged state and re-submit (creating a
    duplicate entry, the only recovery available when we have no
    record of the original inclusion).
    """
    try:
        uuid = entry.get("uuid")
        stored = (
            {uuid: proof_entry}
            if proof_entry is not None and isinstance(uuid, str)
            else entry
        )
        raw_json = json.dumps(stored, sort_keys=True, separators=(",", ":"))
        raw_b64 = base64.standard_b64encode(
            raw_json.encode("utf-8"),
        ).decode("ascii")
        # Defensive numeric parsing. Rekor returns ``logIndex`` and
        # ``integratedTime`` as JSON numbers, but a buggy or hostile
        # registry could return strings (``"42"``), floats, or non-
        # numeric tokens. Without this guard, an int() ValueError would
        # propagate out of add_claim AFTER the claim has been committed
        #, the user would see a stack trace instead of the documented
        # (False, None) sidecar-failure flow. Treat any parse failure
        # as a sidecar miss; the recovery path then re-submits.
        try:
            log_index_int = int(entry.get("logIndex") or 0)
        except (TypeError, ValueError):
            warnings.warn(
                f"Rekor returned a non-integer logIndex "
                f"({entry.get('logIndex')!r}) for claim {claim_id}. "
                "Treating as a sidecar miss; refresh_unsigned() will "
                "re-submit and create a duplicate Rekor entry, the "
                "only recovery available without a parseable record.",
                stacklevel=2,
            )
            return False
        try:
            integrated_time_int = (
                int(entry.get("integratedTime") or 0) or None
            )
        except (TypeError, ValueError):
            # integratedTime is informational. A malformed value gets
            # stored as NULL rather than failing the whole sidecar
            # write, the uuid and logIndex are sufficient to replay
            # the saga's step 4.
            integrated_time_int = None
        # ON CONFLICT DO NOTHING: a successful Rekor inclusion is
        # immutable. If a caller retries the saga and lands here twice
        # for the same claim_id, the original row stays, the
        # append-only trigger refuses overwrite anyway, but the explicit
        # conflict clause keeps the path crash-free. The PRIMARY KEY on
        # claim_id is the conflict target.
        conn.execute(
            "INSERT INTO rekor_inclusions "
            "(claim_id, uuid, log_index, integrated_time, "
            " raw_response_b64, recorded_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(claim_id) DO NOTHING",
            (
                claim_id,
                uuid,
                log_index_int,
                integrated_time_int,
                raw_b64,
                _now(),
            ),
        )
        # Same rule as the claims-row UPDATE one step later: commit only
        # when the saga owns the transaction. Committing here while joined
        # to a caller's open transaction would flush its in-flight writes,
        # so a later rollback could no longer discard the signed claim.
        if own_transaction:
            conn.commit()
        return True
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        warnings.warn(
            f"Claim {claim_id} accepted by Rekor but the sidecar INSERT "
            f"into rekor_inclusions failed ({exc}). The local row stays "
            "unflagged AND there is no recovery hint, refresh_unsigned() "
            "will RE-SUBMIT, creating a duplicate Rekor entry. This is "
            "the only recovery path when no record of the original "
            "submission exists.",
            stacklevel=2,
        )
        return False


def get_rekor_inclusion(
    conn: sqlite3.Connection,
    claim_id: str,
) -> dict | None:
    """Return the stored Rekor coordinates for a claim, if any.

    Used by the recovery path in :meth:`refresh_unsigned` to detect
    "Rekor ACK persisted, claims-row UPDATE pending" and replay the
    UPDATE from stored coords instead of re-submitting.

    Returns the (uuid, integratedTime, logIndex) dict in the shape
    :func:`signing.submit_to_rekor` returns, so the replayed bundle
    matches what the original UPDATE would have written. The coords
    come from the sidecar's own columns; ``raw_response_b64`` holds the
    inclusion proof for restore-time verification, not these values.
    Returns ``None`` when no sidecar row exists for this claim.
    """
    row = conn.execute(
        "SELECT uuid, log_index, integrated_time FROM rekor_inclusions "
        "WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "uuid": row["uuid"],
        "integratedTime": row["integrated_time"],
        "logIndex": row["log_index"],
    }


def list_unlogged_claims(conn: sqlite3.Connection) -> list[dict]:
    """Return signed claims still awaiting Rekor inclusion.

    A claim is "unlogged" when ``signature_bundle`` is non-NULL but
    ``transparency_logged`` is 0. Unsigned claims are excluded: they have
    no envelope to submit.
    """
    rows = conn.execute(
        f"SELECT {_CLAIM_SELECT} FROM claims "
        "WHERE signature_bundle IS NOT NULL AND transparency_logged = 0 "
        "ORDER BY created_at"
    ).fetchall()
    return [dict(r) for r in rows]


def mark_claim_logged(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
    new_signature_bundle: str,
    *,
    strict_promotion: bool = False,
) -> None:
    """Mark a claim as transparency-log included and update its bundle.

    The bundle is rewritten with the Rekor entry attached (uuid + logIndex +
    integratedTime). The flag-flip and REPLICATED re-evaluation happen in a
    single transaction so a crash between them cannot strand a claim at
    PRELIMINARY despite ``transparency_logged=1``.

    Verification
    ------------
    Before writing, FOUR gates apply:

    1. The row must already carry a non-NULL ``signature_bundle``.
       mark_claim_logged attaches a Rekor block to an existing
       envelope; it is not a path to sign an unsigned claim.
    2. The supplied bundle must be JSON.
    3. The bundle must be a structurally-valid claim envelope and its
       ``predicate.claim_id`` must equal the row's ``claim_id``. A buggy
       caller that mixes up claim ids cannot silently write Alice's
       bundle onto Bob's row.
    4. The supplied bundle's ``payload``, ``payloadType``, and
       ``signatures`` fields must be byte-identical to the row's
       existing ``signature_bundle``. The trigger
       ``claims_signed_fields_no_laundering`` refuses only a de-signing
       write to ``signature_bundle`` (non-NULL to NULL) and leaves the
       non-NULL rewrite legal, because the Rekor attachment needs it, so
       this function is the sole defense against a caller substituting a
       different envelope wholesale (different signer, different
       payload, different keyid). Only the optional
       top-level ``rekor`` block may differ between the existing and
       new bundles.

    Raises
    ------
    ClaimNotFoundError
        If no claim with claim_id exists.
    DatabaseError
        If the row has no existing signature_bundle, the supplied
        bundle is malformed, its payload's claim_id does not match,
        or it substantively differs from the existing bundle.
    """
    row = conn.execute(
        "SELECT supports_json, generated_by, unresolved, artifact_hash, "
        "signature_bundle "
        "FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")

    existing_bundle_raw = row["signature_bundle"]
    if existing_bundle_raw is None:
        raise DatabaseError(
            f"mark_claim_logged refused for claim '{claim_id}': the row "
            "carries no existing signature_bundle. Rekor inclusion attaches "
            "a transparency-log block to an already-signed envelope; an "
            "unsigned claim cannot be log-stamped retroactively. Sign the "
            "claim at assert time via mareforma.open(key_path=...)."
        )

    # Sanity-check that the supplied bundle actually belongs to this claim.
    # After Statement v1, claim_id lives inside the predicate.
    from mareforma import signing as _signing
    try:
        envelope = json.loads(new_signature_bundle)
        predicate = _signing.claim_predicate_from_envelope(envelope)
    except (json.JSONDecodeError, _signing.InvalidEnvelopeError) as exc:
        raise DatabaseError(
            f"mark_claim_logged given malformed bundle for {claim_id}: {exc}"
        ) from exc
    if predicate.get("claim_id") != claim_id:
        raise DatabaseError(
            f"mark_claim_logged bundle's predicate.claim_id "
            f"({predicate.get('claim_id')!r}) does not match row {claim_id!r}."
        )

    # Substitution gate. mark_claim_logged exists to attach a Rekor
    # inclusion block to the envelope that was already produced + signed
    # by add_claim. The new bundle must preserve the existing payload
    # bytes, signatures array, and payloadType, only the optional
    # top-level ``rekor`` block may differ. Without this check, a caller
    # could pass any DSSE-shaped envelope (different signer, freshly
    # forged signatures, same predicate.claim_id) and mareforma
    # would persist it: the claims_signed_fields_no_laundering trigger
    # refuses only a de-signing write, not a non-NULL substitution.
    try:
        existing_envelope = json.loads(existing_bundle_raw)
    except json.JSONDecodeError as exc:
        # Row's bundle column is corrupt, separate failure mode from
        # caller error. Surface so the operator can investigate.
        raise DatabaseError(
            f"mark_claim_logged refused for claim '{claim_id}': the "
            f"existing signature_bundle on the row is malformed ({exc}). "
            "Run graph.restore() to surface and recover from the "
            "corruption."
        ) from exc
    if (
        envelope.get("payload") != existing_envelope.get("payload")
        or envelope.get("payloadType") != existing_envelope.get("payloadType")
        or envelope.get("signatures") != existing_envelope.get("signatures")
    ):
        raise DatabaseError(
            f"mark_claim_logged refused for claim '{claim_id}': the new "
            "bundle's payload, payloadType, or signatures differ from the "
            "existing row's signature_bundle. This function attaches a "
            "Rekor inclusion block to an existing envelope; it does not "
            "substitute one envelope for another. To re-sign, retract the "
            "claim (status='retracted') and assert a new one citing the "
            "retracted via contradicts=[<old_claim_id>]."
        )

    # Whitelist of allowed top-level envelope keys. The field-equality
    # check above only compares the cryptographically meaningful trio
    # (payload, payloadType, signatures); extra keys would slip through
    # and get persisted to signature_bundle. The only legitimate addition
    # mark_claim_logged exists to enable is the ``rekor`` block. Anything
    # else is a smuggling vector for opaque metadata that downstream
    # consumers (jsonld exporter, restore) would have to defend against
    # individually.
    _ALLOWED_BUNDLE_KEYS = frozenset(
        {"payload", "payloadType", "signatures", "rekor"}
    )
    extra_keys = set(envelope.keys()) - _ALLOWED_BUNDLE_KEYS
    if extra_keys:
        raise DatabaseError(
            f"mark_claim_logged refused for claim '{claim_id}': the new "
            f"bundle carries unexpected top-level keys {sorted(extra_keys)!r}. "
            "Only payload, payloadType, signatures, and rekor are allowed; "
            "smuggling additional metadata into signature_bundle is refused."
        )

    supports = json.loads(row["supports_json"] or "[]")
    generated_by = row["generated_by"]
    unresolved = int(row["unresolved"] or 0)
    artifact_hash = row["artifact_hash"]
    now = _now()

    try:
        with conn:
            conn.execute(
                "UPDATE claims SET signature_bundle = ?, "
                "transparency_logged = 1, updated_at = ? "
                "WHERE claim_id = ?",
                (new_signature_bundle, now, claim_id),
            )
            # Convergence detection is best-effort by design: a transient
            # lock error during the REPLICATED check must not roll back the
            # flag flip the operator just committed, but it must not vanish
            # either (retry flag + health event).
            if not unresolved:
                _maybe_update_replicated_best_effort(
                    conn, root, claim_id, supports, generated_by, artifact_hash,
                    strict_promotion=strict_promotion,
                )
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        raise DatabaseError(f"Failed to mark claim logged: {exc}") from exc

    _backup_claims_toml(conn, root)


def mark_claim_resolved(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
    *,
    strict_promotion: bool = False,
) -> None:
    """Clear the unresolved flag on a claim and re-check REPLICATED eligibility.

    The flag-clear and the REPLICATED promotion happen in the same SQLite
    transaction. A crash between them would otherwise leave the claim with
    ``unresolved=0`` but stuck at PRELIMINARY, even though a sibling claim
    is waiting on it for convergence.

    Raises
    ------
    ClaimNotFoundError
        If no claim with claim_id exists.
    """
    row = conn.execute(
        "SELECT supports_json, generated_by, artifact_hash "
        "FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")

    supports = json.loads(row["supports_json"] or "[]")
    generated_by = row["generated_by"]
    artifact_hash = row["artifact_hash"]
    now = _now()

    try:
        # ``with conn`` opens a transaction and commits on exit; on exception
        # it rolls back, leaving the claim in its prior unresolved=1 state.
        with conn:
            conn.execute(
                "UPDATE claims SET unresolved = 0, updated_at = ? WHERE claim_id = ?",
                (now, claim_id),
            )
            # Convergence detection is best-effort by design: a transient
            # lock or convergence-query failure must not roll back the
            # flag-clear (the actual user intent), but it must stay retryable
            # (retry flag + health event) rather than strand the claim.
            _maybe_update_replicated_best_effort(
                conn, root, claim_id, supports, generated_by, artifact_hash,
                strict_promotion=strict_promotion,
            )
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        raise DatabaseError(f"Failed to mark claim resolved: {exc}") from exc

    _backup_claims_toml(conn, root)


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
    strict_promotion: bool = False,
) -> None:
    """Update fields on an existing claim.

    Signed claims are append-only across the signed surface. If the row
    carries a non-NULL ``signature_bundle``, this call refuses to mutate
    ``text`` / ``supports`` / ``contradicts``: those fields are part of
    the signed payload and editing them would silently invalidate the
    signature while leaving ``transparency_logged=1`` and the Rekor entry
    in place. ``status`` and ``comparison_summary`` remain editable since
    they are not part of the signed payload.

    To revise a signed claim, retract it (``status='retracted'``) and
    assert a new one with ``contradicts=[<old_claim_id>]``.

    Raises
    ------
    ClaimNotFoundError
        If no claim with *claim_id* exists.
    ValueError
        If status is invalid.
    SignedClaimImmutableError
        If the claim is signed and the caller tries to mutate a signed-
        surface field.
    """
    existing = get_claim(conn, claim_id)
    if existing is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")

    # Same empty / cap / sanitize-on-write gate add_claim applies, so an edit
    # cannot re-introduce an injection payload, blow past the cap, or leak an
    # unsanitized string into the FTS index via the update trigger. Runs before
    # the signed-surface diff so both compare the canonical stored string.
    clean_text = _validate_claim_text(text) if text is not None else None

    # Refuse signed-surface mutations on signed claims. text/supports/
    # contradicts are the only signed-surface fields currently exposed by
    # update_claim's parameter list.
    if existing.get("signature_bundle") is not None:
        signed_field_changes: list[str] = []
        if clean_text is not None and clean_text != existing.get("text"):
            signed_field_changes.append("text")
        if supports is not None:
            old_supports = json.loads(existing.get("supports_json") or "[]")
            if list(supports) != old_supports:
                signed_field_changes.append("supports")
        if contradicts is not None:
            old_contradicts = json.loads(existing.get("contradicts_json") or "[]")
            if list(contradicts) != old_contradicts:
                signed_field_changes.append("contradicts")
        if signed_field_changes:
            raise SignedClaimImmutableError(
                f"Claim '{claim_id}' is signed; refused to mutate "
                f"{signed_field_changes!r}. To revise, retract this claim "
                "(status='retracted') and assert a new one with "
                "contradicts=[<this_id>]."
            )

    new_status = existing["status"]
    new_text = existing["text"]
    new_supports_json = existing.get("supports_json", "[]")
    new_contradicts_json = existing.get("contradicts_json", "[]")
    new_comparison_summary = existing.get("comparison_summary")
    new_unresolved = int(existing.get("unresolved") or 0)

    if status is not None:
        validate_status(status)
        new_status = status
    if clean_text is not None:
        new_text = clean_text
    if supports is not None:
        new_supports_json = json.dumps(supports)
    if contradicts is not None:
        new_contradicts_json = json.dumps(contradicts)
    if comparison_summary is not None:
        new_comparison_summary = comparison_summary

    # Refuse a support+contradict on the same upstream, the same incoherent
    # state add_claim rejects. Check the EFFECTIVE post-update lists (the new
    # side where provided, else the existing one) since an edit to either side
    # can create the overlap.
    _refuse_supports_contradicts_overlap(
        json.loads(new_supports_json or "[]"),
        json.loads(new_contradicts_json or "[]"),
    )

    # DOIs are no longer network-resolved, so a supports/contradicts edit
    # clears any legacy `unresolved` quarantine rather than re-checking.
    # Diff-check against the prior JSON skips the hot path when callers pass
    # identical lists (e.g. when only `text` or `status` is being edited).
    old_supports_json = existing.get("supports_json") or "[]"
    old_contradicts_json = existing.get("contradicts_json") or "[]"
    old_unresolved = int(existing.get("unresolved") or 0)
    supports_changed = supports is not None and new_supports_json != old_supports_json
    contradicts_changed = (
        contradicts is not None and new_contradicts_json != old_contradicts_json
    )

    # Cycle / self-loop check on the NEW supports[] if it changed. Signed
    # claims refuse supports mutation upstream (SignedClaimImmutableError
    # raised earlier in this function), so reaching here implies an
    # unsigned claim, the cycle-introduction window the acyclicity check covers.
    if supports_changed:
        new_supports_list = json.loads(new_supports_json)
        _check_no_cycle(conn, claim_id, new_supports_list)

    if supports_changed or contradicts_changed:
        new_unresolved = 0

    # If the claim just became resolved (or supports changed while resolved),
    # we MUST re-evaluate REPLICATED. Otherwise a claim cured via update_claim
    # stays at PRELIMINARY even when a peer is already waiting for convergence.
    needs_replicated_check = (
        supports_changed
        and new_unresolved == 0
        and existing.get("support_level") != "ESTABLISHED"
    ) or (old_unresolved == 1 and new_unresolved == 0)

    try:
        # Wrap the UPDATE and (optional) convergence check in one txn so the
        # unresolved-flag transition and the REPLICATED promotion are atomic.
        with conn:
            conn.execute(
                """
                UPDATE claims
                SET text = ?, status = ?, supports_json = ?, contradicts_json = ?,
                    comparison_summary = ?, unresolved = ?, updated_at = ?
                WHERE claim_id = ?
                """,
                (
                    new_text, new_status,
                    new_supports_json, new_contradicts_json,
                    new_comparison_summary, new_unresolved, _now(), claim_id,
                ),
            )
            if supports_changed:
                # Keep the supports cache in step with the edited edges. The
                # count-only staleness heuristic never trips on an in-place
                # UPDATE, so the edges must be maintained here or
                # query_provenance serves the pre-edit lineage forever.
                from mareforma import _supports
                _supports.replace_supports_edges(
                    conn, claim_id, json.loads(new_supports_json))
            if needs_replicated_check:
                # Best-effort convergence: never crash an update, but keep a
                # stranded re-check retryable (retry flag + health event).
                new_supports = json.loads(new_supports_json)
                _maybe_update_replicated_best_effort(
                    conn, root, claim_id, new_supports,
                    existing["generated_by"], existing.get("artifact_hash"),
                    strict_promotion=strict_promotion,
                )
    except sqlite3.IntegrityError as exc:
        translated = _state_error_from_integrity(exc)
        if translated is not None:
            raise translated from exc
        raise DatabaseError(f"Failed to update claim '{claim_id}': {exc}") from exc
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
    # Own the transaction the same way add_claim does. The append-only trigger's
    # RAISE(ABORT) backs out the statement but leaves the transaction open, and
    # every write helper reads conn.in_transaction to decide who commits, so a
    # caller that catches the documented refusal and keeps writing would lose
    # every later claim without an error.
    _own_transaction = not conn.in_transaction
    try:
        if _own_transaction:
            conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM claims WHERE claim_id = ?", (claim_id,))
        # Drop the row's cache edges in the same transaction so downstream and
        # upstream walks stop surfacing a dangling claim before the next open.
        from mareforma import _supports
        _supports.remove_claim_edges(conn, claim_id)
        if _own_transaction:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        if _own_transaction:
            conn.rollback()
        raise _signed_delete_error(exc, claim_id) from exc
    except sqlite3.OperationalError as exc:
        if _own_transaction:
            conn.rollback()
        raise DatabaseError(f"Failed to delete claim '{claim_id}': {exc}") from exc

    _backup_claims_toml(conn, root)


# -- verify-on-read for high-trust rows --------------------------------------
#
# Persisted ``support_level`` is not signed: a process with DB write access can
# flip a row to REPLICATED/ESTABLISHED or tamper its envelope. The read path
# therefore re-verifies the row's signatures before serving a high-trust row.
# query_* EXCLUDES a row that fails; get_claim RETURNS it flagged
# verified=False. Neither ever raises, a verification miss must degrade the
# read, not crash it.
#
# Pubkey sourcing (the two tiers):
#   * ESTABLISHED (validator side): the validation envelope's keyid MUST be an
#     enrolled validator (revocation is out of scope, so a legitimately-signed
#     row's key persists through key rotation). A keyid absent from the
#     validators table, or a signature that does not verify, is a forgery and
#     the row is excluded. An ESTABLISHED row also carries the asserter bundle,
#     so the participant check below runs on it too.
#   * REPLICATED (participant side): the asserter need not be an enrolled
#     validator. When the asserter keyid IS enrolled, the bundle signature is
#     verified against that pubkey and a forged signature excludes the row.
#     When it is NOT enrolled there is no pubkey to check against (the lean
#     model carries no participant registry), so the row is verify-exempt:
#     detection where a key is available, never a false exclusion. Legacy
#     rows (NULL asserter_keyid, no bundle) are always exempt.
#
# Both tiers hold the bundle's signed predicate against the row's signed
# fields, not against its claim_id alone. A signature that verifies over a
# predicate nobody compares to the served content is decorative.
#
# A genuine signature over the right content still says nothing about the rung
# the row sits on, because the level is not part of what was signed. Both tiers
# therefore also hold the stored level against the signed evidence that earns
# it (:class:`_CorroborationIndex`); a row that cannot show it is not served as
# verified, exactly as a signature mismatch is not.
#
# The cache is a caller-owned dict keyed on (tier, keyid, digest): one
# verification per distinct signature within a bulk query, one validators
# lookup per distinct keyid, plus the one corroboration index the gated rows
# share. It is passed in and scoped to a single query on purpose, a bulk read
# must not persist verification results past the rows it was called for.

_CORROBORATION_CACHE_KEY = "corroboration_index"


def _cached_validator(conn: sqlite3.Connection, cache: dict, keyid: str):
    """The validators row for *keyid*, read once per caller's read.

    Every pubkey the read path needs comes through here. The lookup is keyed on
    the keyid alone, unlike the signature entries, because the answer does not
    depend on the row being checked; it is still scoped to the caller's cache,
    so an enrollment written after this read is picked up by the next one.
    Without it a bulk read costs one validators SELECT per row checked, and the
    corroboration index costs one per peer it verifies.
    """
    ck = ("K", keyid)
    if ck not in cache:
        from mareforma import validators as _validators
        cache[ck] = _validators.get_validator(conn, keyid)
    return cache[ck]

def _row_verified_on_read(
    conn: sqlite3.Connection, row: dict, cache: dict,
) -> bool:
    """True iff *row* may be served at its persisted support_level.

    PRELIMINARY and below are not gated here (they have their own
    enrolled-generator filter in query_claims) and pass through True.
    """
    level = row.get("support_level")
    if level not in ("REPLICATED", "ESTABLISHED"):
        return True
    if level == "ESTABLISHED" and not _verify_validation_on_read(conn, row, cache):
        # An ESTABLISHED row carries both envelopes. The validation signature
        # attests the promotion; the asserter bundle attests the content. Check
        # both, or a validated row's text could be rewritten under a validation
        # envelope that binds nothing but the claim id.
        return False
    return (
        _verify_participant_bundle_on_read(conn, row, cache)
        and _corroboration_backs_level(conn, row, cache)
    )


def _trust_domain_disclosure(conn: sqlite3.Connection) -> tuple[bool, str | None]:
    """(single_trust_domain, trust_domain_root) for this graph's validators.

    A graph-global property of the validator topology, attached per
    ESTABLISHED row so a consumer reading one promoted claim sees whether all
    validators trace to one root of trust. It discloses trust-domain
    concentration; it is NOT a Sybil guard over the participant topology.
    """
    from mareforma import validators as _validators
    return (
        _validators.single_trust_domain(conn),
        _validators.trust_domain_root(conn),
    )


def _verify_validation_on_read(
    conn: sqlite3.Connection, row: dict, cache: dict,
) -> bool:
    """Re-verify an ESTABLISHED row's validation envelope (validator side)."""
    vs = row.get("validation_signature")
    if not vs:
        # An ESTABLISHED row with no validation envelope violates the schema
        # CHECK; reaching here means a direct tamper. Refuse to serve it.
        return False
    try:
        env = json.loads(vs)
        keyid = env["signatures"][0]["keyid"]
        declared = env["payloadType"]
    except (json.JSONDecodeError, KeyError, IndexError, TypeError):
        return False
    # The cache value depends on the per-row claim_id binding check below, so
    # claim_id MUST be part of the key. Without it, two rows that carry the same
    # validation_signature bytes (an attacker copies a genuine envelope onto a
    # second row) would share a cache entry: the row evaluated first, sorted by
    # created_at, which the attacker controls, caches its result and poisons
    # the second, so a forged row could censor the legitimate ESTABLISHED claim.
    ck = (
        "V", keyid, row.get("claim_id"),
        hashlib.sha256(vs.encode("utf-8")).hexdigest(),
    )
    if ck in cache:
        return cache[ck]
    from mareforma import signing as _signing
    from mareforma import validators as _validators
    ok = False
    if declared in (_signing.PAYLOAD_TYPE_VALIDATION, _signing.PAYLOAD_TYPE_SEED):
        signer_row = _validators.get_validator(conn, keyid)
        if signer_row is not None:
            try:
                pem = base64.standard_b64decode(signer_row["pubkey_pem"])
                pub = _signing.public_key_from_pem(pem)
                if _signing.verify_envelope(
                    env, pub, expected_payload_type=declared,
                ):
                    # The signature is genuine; confirm the signed payload
                    # binds THIS claim so a valid envelope from another claim
                    # cannot be replayed onto this row.
                    payload = json.loads(
                        base64.standard_b64decode(env["payload"])
                    )
                    ok = payload.get("claim_id") == row.get("claim_id")
            except Exception:
                ok = False
        # signer_row is None -> the validator keyid is not enrolled. An
        # ESTABLISHED promotion can only come from an enrolled validator, so
        # this is a forged row: leave ok False (excluded).
    cache[ck] = ok
    return ok


def _signed_field_mismatch(pred: dict, row: dict) -> str | None:
    """Name the first SIGNED_FIELDS value the row and its predicate disagree on.

    Returns None when the signed predicate binds this row's content exactly.
    Binding the claim_id alone leaves the signature decorative: the row's text,
    classification, provenance and links can all be rewritten under an envelope
    that still verifies. Shared by the read-path gate and the audit path so the
    two cannot drift apart.
    """
    from mareforma import signing as _signing
    expected = {
        "claim_id": row.get("claim_id"),
        "text": row.get("text"),
        "classification": row.get("classification"),
        "generated_by": row.get("generated_by"),
        "supports": _json_list(row.get("supports_json")),
        "contradicts": _json_list(row.get("contradicts_json")),
        "source_name": row.get("source_name"),
        "artifact_hash": row.get("artifact_hash"),
        "created_at": row.get("created_at"),
    }
    for field in _signing.SIGNED_FIELDS:
        if pred.get(field) != expected[field]:
            return field
    # The evidence vector and the observed-grounding verdict are signed and
    # chained too, but they live outside SIGNED_FIELDS (one is a nested dict,
    # the other is optional), so restore checks them by hand. Do the same here
    # or a rewritten verdict reads clean and unlocks the promotion the real
    # verdict blocked. Both sides are parsed so key ordering cannot fake a
    # mismatch; absent on both sides is the pre-observer case and passes.
    if pred.get("evidence") != _json_object(row.get("evidence_json"), {}):
        return "evidence"
    if pred.get("observed_grounding") != _json_object(row.get("observed_grounding")):
        return "observed_grounding"
    return None


def _verify_participant_bundle_on_read(
    conn: sqlite3.Connection, row: dict, cache: dict,
) -> bool:
    """Re-verify a REPLICATED row's asserter bundle (participant side).

    Legacy (no bundle, no keyid) rows are verify-exempt, they carry no envelope
    to check. A row with a keyid but no bundle is not legacy: the keyid is
    derived from the bundle on the only honest write path, so that pair means
    the bundle was cleared by direct SQL, and the row is refused. Otherwise the
    bundle's signed predicate MUST match THIS row on every signed field and be
    subject/predicate-consistent. That binding holds even when the asserter is
    not an enrolled validator, so a genuine bundle copied off another claim
    cannot be stapled onto this row, a rewritten field cannot hide under a valid
    envelope, and a junk bundle is rejected, with no pubkey needed. The signer is
    read out of the bundle, never off the row: ``asserter_keyid`` is an unsigned
    denormalisation, so a row that disagrees with its own envelope is refused
    rather than trusted. When the signer IS enrolled the bundle signature is
    additionally verified against that pubkey; a forged or tampered signature
    excludes the row. When it is not enrolled there is no pubkey in the lean
    model, so a claim-bound bundle is served (exempt on authenticity, never on
    the claim binding).
    """
    ak = row.get("asserter_keyid")
    bundle_json = row.get("signature_bundle")
    if not bundle_json:
        return ak is None
    # claim_id is part of the key: the binding check below depends on the row,
    # so two rows sharing one bundle (a copy attack) must not share a cache
    # entry or the first-evaluated row poisons the second (same reasoning as
    # the validator path).
    ck = (
        "P", ak, row.get("claim_id"),
        hashlib.sha256(bundle_json.encode("utf-8")).hexdigest(),
    )
    if ck in cache:
        return cache[ck]
    from mareforma import signing as _signing
    from mareforma import validators as _validators
    ok = False
    try:
        env = json.loads(bundle_json)
        # The signer the bundle itself names. A missing or malformed signatures
        # array raises and is excluded.
        signer = env["signatures"][0]["keyid"]
        # asserter_keyid is an unsigned denormalisation of that signer. A row
        # that contradicts its own envelope was written outside the signing
        # path, so refuse it rather than let the column pick the pubkey.
        keyid_agrees = ak is None or ak == signer
        # Content binding (no pubkey needed): the signed predicate must match
        # THIS row on every signed field, not just its claim_id.
        # claim_predicate_from_envelope also enforces subject-vs-predicate
        # consistency, so a junk or internally-inconsistent bundle raises and is
        # excluded.
        pred = _signing.claim_predicate_from_envelope(env)
        if keyid_agrees and _signed_field_mismatch(pred, row) is None:
            signer_row = _cached_validator(conn, cache, signer)
            if signer_row is None:
                # Non-enrolled asserter: no pubkey in the lean model. The
                # binding above is the integrity we can offer; serve it.
                ok = True
            else:
                pem = base64.standard_b64decode(signer_row["pubkey_pem"])
                pub = _signing.public_key_from_pem(pem)
                ok = bool(_signing.verify_envelope(env, pub))
                # Multi-role parity: a claim-with-roles:v1 bundle carries role
                # attestations in signatures[1:]. verify_envelope only checked
                # signatures[0] (the asserter); walk the rest so a forged role
                # signature is caught on the LIVE read path, not only at restore.
                if ok:
                    ok = _verify_role_signatures(conn, env)
    except Exception:
        ok = False
    cache[ck] = ok
    return ok


def _verify_role_signatures(conn: sqlite3.Connection, env: dict) -> bool:
    """Verify the role attestations in signatures[1:] of a claim-with-roles bundle.

    The shared read-path routine for multi-signature envelopes. Applies the same
    contract restore enforces (:func:`db.restore._verify_claim_signatures_on_restore`):
    every signature beyond the asserter must carry a role in
    :data:`signing.VALID_CLAIM_ROLES`, roles are unique, each signer's keyid must
    be enrolled, and each signature must verify against that keyid's pubkey over
    the DSSE PAE. Any deviation → ``False``. A single-signature (asserter-only)
    bundle has nothing extra to check and passes.
    """
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    sigs = env.get("signatures") or []
    if len(sigs) <= 1:
        return True
    try:
        payload_bytes = base64.standard_b64decode(env["payload"])
    except (KeyError, TypeError, ValueError):
        return False
    pae = _signing.dsse_pae(_signing.PAYLOAD_TYPE_CLAIM, payload_bytes)
    seen_roles: set[str] = set()
    for entry in sigs[1:]:
        if not isinstance(entry, dict):
            return False
        role = entry.get("role")
        keyid = entry.get("keyid")
        if not isinstance(role, str) or role not in _signing.VALID_CLAIM_ROLES:
            return False
        if role in seen_roles:
            return False
        seen_roles.add(role)
        if not isinstance(keyid, str):
            return False
        signer_row = _validators.get_validator(conn, keyid)
        if signer_row is None:
            return False  # orphan signer, not enrolled
        try:
            pem = base64.standard_b64decode(signer_row["pubkey_pem"])
            pub = _signing.public_key_from_pem(pem)
            sig_bytes = base64.standard_b64decode(entry["sig"])
            pub.verify(sig_bytes, pae)
        except Exception:
            return False
    return True


def verify_claim_signatures(
    conn: sqlite3.Connection, row: dict,
) -> tuple[bool, str]:
    """Audit-grade, tier-independent re-verification of a claim's signatures.

    Unlike :func:`_row_verified_on_read`, which is gated by support level and
    passes PRELIMINARY rows through untouched, this re-checks a signed claim's
    bundle at ANY tier, for the explicit ``mareforma verify`` audit. It confirms
    the signed predicate binds THIS row (claim_id + every signed field matches,
    catching a hand-edited row), verifies the asserter signature when the
    asserter is enrolled, and verifies all role attestations. Returns
    ``(ok, reason)``; ``reason`` is empty on success.

    An unsigned claim returns ``(True, "")``, there is no signature to break;
    its lack of attribution is reported by the trust map, not failed here. A row
    that kept its ``asserter_keyid`` but lost its bundle is not unsigned, it was
    de-signed by direct SQL, and fails. A non-enrolled asserter cannot have its
    signature checked against a pubkey in the lean model, so the claim-binding
    (predicate names this row, signed fields match) is the integrity floor
    offered.
    """
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    bundle_json = row.get("signature_bundle")
    if not bundle_json:
        if row.get("asserter_keyid") is not None:
            return (False, "signature bundle was removed from a signed claim")
        return (True, "")
    try:
        env = json.loads(bundle_json)
    except (ValueError, TypeError):
        return (False, "signature bundle is not valid JSON")
    try:
        pred = _signing.claim_predicate_from_envelope(env)
    except Exception:
        return (False, "signature bundle envelope is structurally invalid")

    if pred.get("claim_id") != row.get("claim_id"):
        return (False, "signed predicate does not bind this claim id")

    mismatch = _signed_field_mismatch(pred, row)
    if mismatch is not None:
        return (False, f"signed field {mismatch!r} does not match the row (tampered)")

    keyid = _extract_signature_bundle_keyid(bundle_json) or row.get("asserter_keyid")
    if keyid is not None:
        signer_row = _validators.get_validator(conn, keyid)
        if signer_row is not None:
            try:
                pem = base64.standard_b64decode(signer_row["pubkey_pem"])
                pub = _signing.public_key_from_pem(pem)
                if not _signing.verify_envelope(env, pub):
                    return (False, "asserter signature failed verification")
            except Exception:
                return (False, "asserter signature could not be verified")

    if not _verify_role_signatures(conn, env):
        return (False, "a role signature failed verification")
    return (True, "")


def _json_object(value, empty=None):
    """Parse a JSON-object column into a dict; ``None``/empty → *empty*.

    A malformed value is returned as-is, so it can never compare equal to a
    signed predicate value and the row reads as tampered rather than clean.
    """
    if value is None or value == "":
        return empty
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except (ValueError, TypeError):
        return value
    return parsed if isinstance(parsed, dict) else value


def _json_list(value) -> list:
    """Parse a JSON-array column into a list; ``None``/malformed → ``[]``."""
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (ValueError, TypeError):
        return []
    return parsed if isinstance(parsed, list) else []


def get_claim(conn: sqlite3.Connection, claim_id: str) -> dict | None:
    """Return a claim dict or None if not found.

    High-trust rows (REPLICATED / ESTABLISHED) carry a ``verified`` boolean:
    the read path re-verifies the row's signatures and flags the result rather
    than excluding the row, so an auditor can still see a tampered row and know
    it failed. PRELIMINARY rows are always ``verified=True`` here.
    """
    try:
        row = conn.execute(
            f"SELECT {_CLAIM_SELECT} FROM claims WHERE claim_id = ?",
            (claim_id,),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to fetch claim '{claim_id}': {exc}") from exc
    if not row:
        return None
    d = dict(row)
    d["verified"] = _row_verified_on_read(conn, d, {})
    if d.get("support_level") == "ESTABLISHED":
        std, root_kid = _trust_domain_disclosure(conn)
        d["single_trust_domain"] = std
        d["trust_domain_root"] = root_kid
    return d


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
    # Same transaction ownership as delete_claim: a refusal must not leave the
    # connection in-transaction for the next writer to inherit.
    _own_transaction = not conn.in_transaction
    try:
        rows = conn.execute(
            "SELECT claim_id FROM claims WHERE generated_by = ?",
            (generated_by,),
        ).fetchall()
        claim_ids = [r[0] for r in rows]
        if not claim_ids:
            return 0
        if _own_transaction:
            conn.execute("BEGIN IMMEDIATE")
        placeholders = ",".join("?" * len(claim_ids))
        conn.execute(
            f"DELETE FROM claims WHERE claim_id IN ({placeholders})", claim_ids
        )
        # Drop every deleted claim's cache edges in the same transaction, and
        # decrement the staleness counter once per removed claim.
        from mareforma import _supports
        for deleted_id in claim_ids:
            _supports.remove_claim_edges(conn, deleted_id, count_delta=0)
        _supports._bump_source_count(conn, delta=-len(claim_ids))
        if _own_transaction:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        if _own_transaction:
            conn.rollback()
        raise _signed_delete_error(exc) from exc
    except sqlite3.OperationalError as exc:
        if _own_transaction:
            conn.rollback()
        raise DatabaseError(f"Failed to delete claims: {exc}") from exc

    _backup_claims_toml(conn, root)
    return len(claim_ids)


_VALID_REPLICATION_METHODS = (
    "hash-match",
    "semantic-cluster",
    "shared-resolved-upstream",
    "cross-method",
    # Signed-bracket tournament replay: an external verdict-issuer (e.g.
    # mareforma_elo) has independently replayed a signed Elo tournament
    # bracket and confirms two claims converge under that bracket's
    # outcome. The replay itself produces a signed
    # ``elo-bracket-snapshot/v1`` predicate; this verdict method records
    # the convergence it attests to.
    "signed-elo-bracket-replay",
)


_REPLICATION_VERDICT_FIELDS = (
    "verdict_id",
    "cluster_id",
    "member_claim_id",
    "other_claim_id",
    "method",
    "confidence",
)

_CONTRADICTION_VERDICT_FIELDS = (
    "verdict_id",
    "member_claim_id",
    "other_claim_id",
    "confidence",
)


def _verdict_canonical_payload(
    fields: tuple[str, ...], record: dict,
) -> bytes:
    """Canonical JSON of a verdict record under a fixed field set.

    Uses :func:`mareforma._canonical.canonicalize` so verdicts and
    claims share one canonicalization contract (sorted keys, NFC
    Unicode normalization, no whitespace, ``allow_nan=False``).
    A third-party verdict-issuer implementing against the same
    canonical-JSON contract produces signatures the OSS core
    verifies; a confidence dict containing NaN / Inf is rejected at
    sign time rather than producing a payload some verifiers refuse.
    """
    from .._canonical import canonicalize
    payload = {name: record.get(name) for name in fields}
    return canonicalize(payload)


def _require_enrolled_issuer(
    conn: sqlite3.Connection, issuer_keyid: str,
) -> None:
    """Refuse the verdict if issuer_keyid is not an enrolled validator.

    Walks the enrollment chain back to a self-signed root via
    ``validators.is_enrolled``: same gate the seed-claim path and
    ``graph.validate()`` use. A row that exists in the validators
    table but whose enrollment_envelope does not verify against its
    parent (e.g. a tampered DB or a partial restore) is rejected.
    Without the chain walk, the verdict path would be strictly more
    permissive than every other trust-bearing path.
    """
    from mareforma import validators as _validators
    if not _validators.is_enrolled(conn, issuer_keyid):
        raise VerdictIssuerError(
            f"Verdict-issuer keyid {issuer_keyid!r} is not enrolled "
            "(or its enrollment chain does not verify). Issuers must "
            "be in the validators table with a verifiable chain, "
            "call graph.enroll_validator() under a verified parent."
        )


def _require_claim_exists(
    conn: sqlite3.Connection, claim_id: str, role: str,
) -> None:
    row = conn.execute(
        "SELECT 1 FROM claims WHERE claim_id = ?", (claim_id,),
    ).fetchone()
    if row is None:
        raise VerdictIssuerError(
            f"Verdict references missing claim_id {claim_id!r} ({role})."
        )


def record_replication_verdict(
    conn: sqlite3.Connection,
    root: Path,
    *,
    verdict_id: str,
    cluster_id: str,
    member_claim_id: str,
    other_claim_id: str | None,
    method: str,
    confidence: dict[str, Any] | None,
    signer: "object",
) -> None:
    """Insert a signed replication verdict written by an enrolled validator.

    *signer* is an Ed25519 private key (the verdict-issuer's key).
    The issuer_keyid (sha256-hex of the signer's public key) must be
    present in the ``validators`` table; otherwise the call raises
    :class:`VerdictIssuerError`.

    The DSSE-PAE signature covers the canonical JSON of
    ``(verdict_id, cluster_id, member_claim_id, other_claim_id,
    method, confidence)``. Restore re-derives this binding to catch
    TOML tampering of verdict rows.

    The OSS core doesn't fire replication predicates itself:
    third-party verdict-issuers call this method after running their
    predicate logic. Mareforma just accepts the signed verdict and
    triggers the support_level promotion.
    """
    from mareforma import signing as _signing

    if method not in _VALID_REPLICATION_METHODS:
        raise VerdictIssuerError(
            f"Unknown verdict method {method!r}. "
            f"Use one of: {', '.join(_VALID_REPLICATION_METHODS)}"
        )
    issuer_keyid = _signing.public_key_id(signer.public_key())
    _require_enrolled_issuer(conn, issuer_keyid)
    _require_claim_exists(conn, member_claim_id, "member_claim_id")
    if other_claim_id is not None:
        _require_claim_exists(conn, other_claim_id, "other_claim_id")
    # Defense-in-depth: a verdict issuer cannot be a role-actor on
    # either claim under verdict. Walks ALL signatures on each
    # claim's signature_bundle so a planner / executor / reviewer /
    # validator on a claim-with-roles:v1 envelope cannot also issue
    # the verdict.
    _refuse_self_verdict(
        conn, issuer_keyid, member_claim_id,
        relation="member_claim_id", verdict_kind="replication",
    )
    if other_claim_id is not None:
        _refuse_self_verdict(
            conn, issuer_keyid, other_claim_id,
            relation="other_claim_id", verdict_kind="replication",
        )

    confidence_dict = confidence or {}
    # canonicalize() (NFC + sorted keys + no whitespace + allow_nan=False)
    # for stored confidence_json so restore round-trips byte-equally
    # AND callers can't sneak a NaN/Inf into a signed payload.
    from .._canonical import canonicalize as _canonicalize
    confidence_json = _canonicalize(confidence_dict).decode("utf-8")
    record = {
        "verdict_id": verdict_id,
        "cluster_id": cluster_id,
        "member_claim_id": member_claim_id,
        "other_claim_id": other_claim_id,
        "method": method,
        "confidence": confidence_dict,
    }
    payload = _verdict_canonical_payload(_REPLICATION_VERDICT_FIELDS, record)
    pae = _signing.dsse_pae(
        "application/vnd.mareforma.replication-verdict+json", payload,
    )
    signature = signer.sign(pae)
    created_at = _now()
    # Verdict INSERT + promotion UPDATE run in one BEGIN IMMEDIATE
    # transaction so a concurrent contradiction verdict cannot land
    # between the two commits and leave the claim in the contradictory
    # state (support_level=REPLICATED AND t_invalid IS NOT NULL).
    members = [member_claim_id]
    if other_claim_id is not None:
        members.append(other_claim_id)
    placeholders = ",".join("?" * len(members))
    _own_txn = not conn.in_transaction
    try:
        if _own_txn:
            conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO replication_verdicts(
                verdict_id, cluster_id, member_claim_id, other_claim_id,
                method, confidence_json, issuer_keyid, signature, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict_id, cluster_id, member_claim_id, other_claim_id,
                method, confidence_json, issuer_keyid, signature, created_at,
            ),
        )
        # Promote referenced claims to REPLICATED. The state-machine
        # trigger rejects PRELIMINARY → ESTABLISHED but accepts
        # PRELIMINARY → REPLICATED. Update only when the row is still
        # PRELIMINARY (do not downgrade an ESTABLISHED claim) AND not
        # invalidated (a signed contradiction verdict is terminal , 
        # a later replication verdict must not silently re-promote).
        #
        # The verdict path enforces the SAME computed gates the convergence
        # path applies to this identical PRELIMINARY → REPLICATED transition:
        # a claim execution observed as NOT grounded (UNGROUNDED / OPAQUE), an
        # unsigned / legacy row (NULL asserter_keyid, not a valid distinct
        # signer), or one whose transparency log is not settled must not ride a
        # verdict into the trust ladder. Without them an enrolled issuer could
        # launder an UNGROUNDED or unsigned claim to REPLICATED, and from there
        # validate() lifts it to ESTABLISHED. These gates are read inside the
        # same BEGIN IMMEDIATE transaction and applied through the shared
        # `_observed_grounding_promotes` helper (the convergence path gates its
        # new claim the same way), so a mixed batch still promotes the
        # qualifying members and the verdict is recorded either way. The
        # concurrency-sensitive gates (t_invalid, status) stay on the UPDATE's
        # WHERE to close the TOCTOU window if a member is invalidated after the
        # read.
        gate_rows = conn.execute(
            f"SELECT claim_id, observed_grounding, asserter_keyid, "
            f"transparency_logged FROM claims "
            f"WHERE claim_id IN ({placeholders})",
            members,
        ).fetchall()
        promotable = [
            r["claim_id"] for r in gate_rows
            if r["asserter_keyid"] is not None
            and r["transparency_logged"] == 1
            and _observed_grounding_promotes(r["observed_grounding"])
        ]
        if promotable:
            promote_placeholders = ",".join("?" * len(promotable))
            conn.execute(
                f"UPDATE claims SET support_level = 'REPLICATED', updated_at = ? "
                f"WHERE claim_id IN ({promote_placeholders}) "
                f"AND support_level = 'PRELIMINARY' "
                f"AND status = 'open' "
                f"AND t_invalid IS NULL",
                (created_at, *promotable),
            )
        if _own_txn:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        if _own_txn:
            conn.rollback()
        # The INSERT itself failing is a verdict-issuer error; a
        # promotion-trigger refusal would surface here too but at this
        # point everything either committed atomically or rolled back.
        raise VerdictIssuerError(
            f"Replication verdict {verdict_id!r} INSERT refused: {exc}"
        ) from exc

    _backup_claims_toml(conn, root)


def record_contradiction_verdict(
    conn: sqlite3.Connection,
    root: Path,
    *,
    verdict_id: str,
    member_claim_id: str,
    other_claim_id: str,
    confidence: dict[str, Any] | None,
    signer: "object",
) -> None:
    """Insert a signed contradiction verdict from an enrolled validator.

    Sets ``claims.t_invalid`` on the older of the two referenced
    claims via the ``contradiction_invalidates_older`` AFTER INSERT
    trigger. ``include_invalidated=False`` queries (the default) then
    exclude the invalidated claim from results.

    Same enrollment / claim-existence / signature-binding contract as
    :func:`record_replication_verdict`.
    """
    from mareforma import signing as _signing

    if member_claim_id == other_claim_id:
        # Self-contradiction is meaningless and would let a single
        # validator invalidate any claim unilaterally. The table CHECK
        # also blocks it, but raising here gives a clean Python error.
        raise VerdictIssuerError(
            f"Contradiction verdict {verdict_id!r} references the same "
            f"claim_id on both sides ({member_claim_id!r}), self-"
            "contradiction is not a valid verdict."
        )
    # Asymmetry with record_replication_verdict (which wraps INSERT +
    # promotion UPDATE in one BEGIN IMMEDIATE): contradiction is a
    # single INSERT + one AFTER-INSERT trigger that fires inside the
    # same auto-statement transaction. No second write follows, so no
    # race window opens between INSERT and the trigger's UPDATE.
    # Symmetric atomic-txn treatment would be a no-op.
    issuer_keyid = _signing.public_key_id(signer.public_key())
    _require_enrolled_issuer(conn, issuer_keyid)
    # Symmetric to validate_claim's LLM-validator gate: an LLM-typed
    # validator cannot issue a contradiction, because a contradiction
    # invalidates the older claim and effectively demotes it from default
    # query() results. Promotion-requires-human and demotion-requires-
    # human must move together; otherwise an enrolled LLM key can mark
    # down any ESTABLISHED claim with a signed contradiction.
    _refuse_llm_contradiction_issuer(conn, issuer_keyid)
    _require_claim_exists(conn, member_claim_id, "member_claim_id")
    _require_claim_exists(conn, other_claim_id, "other_claim_id")
    # Defense-in-depth: the contradiction issuer cannot be a role-
    # actor on either claim.
    _refuse_self_verdict(
        conn, issuer_keyid, member_claim_id,
        relation="member_claim_id", verdict_kind="contradiction",
    )
    _refuse_self_verdict(
        conn, issuer_keyid, other_claim_id,
        relation="other_claim_id", verdict_kind="contradiction",
    )

    confidence_dict = confidence or {}
    # canonicalize() (NFC + sorted keys + no whitespace + allow_nan=False)
    # for stored confidence_json so restore round-trips byte-equally
    # AND callers can't sneak a NaN/Inf into a signed payload.
    from .._canonical import canonicalize as _canonicalize
    confidence_json = _canonicalize(confidence_dict).decode("utf-8")
    record = {
        "verdict_id": verdict_id,
        "member_claim_id": member_claim_id,
        "other_claim_id": other_claim_id,
        "confidence": confidence_dict,
    }
    payload = _verdict_canonical_payload(_CONTRADICTION_VERDICT_FIELDS, record)
    pae = _signing.dsse_pae(
        "application/vnd.mareforma.contradiction-verdict+json", payload,
    )
    signature = signer.sign(pae)
    created_at = _now()
    try:
        conn.execute(
            """
            INSERT INTO contradiction_verdicts(
                verdict_id, member_claim_id, other_claim_id,
                confidence_json, issuer_keyid, signature, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict_id, member_claim_id, other_claim_id,
                confidence_json, issuer_keyid, signature, created_at,
            ),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        raise VerdictIssuerError(
            f"Contradiction verdict {verdict_id!r} INSERT refused: {exc}"
        ) from exc

    _backup_claims_toml(conn, root)


def list_replication_verdicts(
    conn: sqlite3.Connection,
    *,
    member_claim_id: str | None = None,
    cluster_id: str | None = None,
    include_invalidated: bool = False,
) -> list[dict]:
    """List signed replication verdicts, optionally filtered.

    By default, verdicts whose member or other claim has been
    invalidated (``claims.t_invalid IS NOT NULL``) are excluded: same
    surface as :func:`query_claims`. Pass ``include_invalidated=True``
    for audit-mode listings.
    """
    conditions: list[str] = []
    params: list[Any] = []
    if member_claim_id is not None:
        conditions.append("(v.member_claim_id = ? OR v.other_claim_id = ?)")
        params.extend([member_claim_id, member_claim_id])
    if cluster_id is not None:
        conditions.append("v.cluster_id = ?")
        params.append(cluster_id)
    if not include_invalidated:
        conditions.append(
            "NOT EXISTS ("
            "SELECT 1 FROM claims c "
            "WHERE (c.claim_id = v.member_claim_id OR c.claim_id = v.other_claim_id) "
            "AND c.t_invalid IS NOT NULL"
            ")"
        )
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    rows = conn.execute(
        f"SELECT v.verdict_id, v.cluster_id, v.member_claim_id, "
        f"v.other_claim_id, v.method, v.confidence_json, v.issuer_keyid, "
        f"v.signature, v.created_at "
        f"FROM replication_verdicts v {where} "
        f"ORDER BY v.created_at ASC, v.verdict_id ASC",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def list_contradiction_verdicts(
    conn: sqlite3.Connection,
    *,
    claim_id: str | None = None,
    include_invalidated: bool = False,
) -> list[dict]:
    """List signed contradiction verdicts, optionally filtered.

    By default, contradiction verdicts whose claims have been
    invalidated are excluded. Pass ``include_invalidated=True`` for
    audit-mode listings (the typical use, since a contradiction verdict
    is the EVIDENCE for invalidation, so callers inspecting "why was
    this invalidated" need audit mode).
    """
    conditions: list[str] = []
    params: list[Any] = []
    if claim_id is not None:
        conditions.append("(v.member_claim_id = ? OR v.other_claim_id = ?)")
        params.extend([claim_id, claim_id])
    if not include_invalidated:
        conditions.append(
            "NOT EXISTS ("
            "SELECT 1 FROM claims c "
            "WHERE (c.claim_id = v.member_claim_id OR c.claim_id = v.other_claim_id) "
            "AND c.t_invalid IS NOT NULL"
            ")"
        )
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    rows = conn.execute(
        f"SELECT v.verdict_id, v.member_claim_id, v.other_claim_id, "
        f"v.confidence_json, v.issuer_keyid, v.signature, v.created_at "
        f"FROM contradiction_verdicts v {where} "
        f"ORDER BY v.created_at ASC, v.verdict_id ASC",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


# Refutation taxonomy, the four states a claim can be in with respect
# to active refutations against it. Surfaced to callers via
# :func:`refutation_status` and as a filter argument to query_claims.
#   clean       , no signed contradiction, status='open', not retracted
#   contradicted, t_invalid IS NOT NULL (a signed contradiction
#                  verdict from an enrolled validator marked the older
#                  claim invalid)
#   contested   , status='contested' (an editorial-level flag set by
#                  update_claim; non-cryptographic, weaker than a
#                  contradiction verdict but visible to consumers)
#   retracted   , status='retracted' (the asserter withdrew the claim
#                  themselves; terminal state in the status state
#                  machine)
REFUTATION_STATES: tuple[str, ...] = (
    "clean", "contradicted", "contested", "retracted",
)
# Filter values that query_claims accepts on refutation_filter=. ``None``
# preserves the legacy behaviour gated by include_invalidated.
VALID_REFUTATION_FILTERS: tuple[str, ...] = (
    "clean", "contradicted", "contested", "retracted", "any",
)


def refutation_status(row: dict) -> dict:
    """Classify a claim row's refutation state.

    Returns a dict with three fields:

      * ``state``: one of :data:`REFUTATION_STATES`
      * ``reason``: short human-readable explanation
      * ``signal``: ``"signed-verdict"`` if backed by a cryptographic
                     verdict, ``"editorial"`` if backed only by a
                     status flag, or ``"none"`` for clean claims

    The presenter is a pure function over the row's queryable
    columns; it does NOT walk verdict tables (callers wanting the
    underlying verdicts use
    :meth:`EpistemicGraph.contradiction_verdicts`).

    Raises :class:`ValueError` when *row* lacks the required
    ``status`` field: a hand-crafted partial dict would otherwise
    fall through to a falsely-confident ``"clean"`` verdict.
    """
    if not isinstance(row, dict):
        raise ValueError(
            f"refutation_status: row must be a dict, got {type(row).__name__}"
        )
    if "status" not in row:
        raise ValueError(
            "refutation_status: row missing 'status' field; pass a row "
            "fetched via list_claims / get_claim, not a partial dict."
        )
    if row.get("t_invalid") is not None:
        return {
            "state": "contradicted",
            "reason": (
                "a signed contradiction verdict marked this claim "
                f"invalid at t_invalid={row['t_invalid']}"
            ),
            "signal": "signed-verdict",
        }
    status = row.get("status")
    if status == "retracted":
        return {
            "state": "retracted",
            "reason": "the asserter retracted this claim",
            "signal": "editorial",
        }
    if status == "contested":
        return {
            "state": "contested",
            "reason": "this claim was editorially flagged as contested",
            "signal": "editorial",
        }
    return {
        "state": "clean",
        "reason": "no refutation signal on this claim",
        "signal": "none",
    }


def _read_scan_ceiling(limit: int) -> int:
    """Max rows a read surface materialises before returning the survivors it
    has. Bounds the adversarial worst case: a flood of rows that fail
    verify-on-read (mass tamper or unenrolled-PRELIMINARY traffic) must not turn
    a cheap insert into a whole-table read amplifier. Generous enough that
    legitimate verified-heavy / PRELIMINARY-heavy projects are unaffected."""
    return max(limit * 50, 5000)


def _require_non_negative_limit(limit: int, surface: str) -> None:
    """Refuse a negative limit on a read surface.

    Zero is a legitimate boundary (a pager or budget loop that has drained) and
    returns no rows. A negative limit has no reading: it is a caller's
    arithmetic mistake, and quietly returning nothing would hide it."""
    if limit < 0:
        raise ValueError(
            f"{surface} limit must be zero or greater, got {limit}."
        )


def _enrolled_generator_condition(prefix: str = "") -> str:
    """SQL for the default read filter's enrolled-generator half.

    Mirrors the PRELIMINARY branch of :func:`_read_path_row` in SQL so LIMIT
    counts survivors rather than scanned rows: the dominant drain (unsigned and
    unenrolled-generator PRELIMINARY traffic) never enters the scan, and cannot
    push a real match past the scan ceiling. The Python filter stays as written,
    this only spares it the rows it would have dropped anyway. ``json_valid``
    guards the extract: a malformed bundle is not enrolled, and must not fail
    the whole statement. *prefix* qualifies the columns for joined statements.
    """
    bundle = f"{prefix}signature_bundle"
    return (
        f"({prefix}support_level != 'PRELIMINARY' OR (json_valid({bundle}) AND "
        f"json_extract({bundle}, '$.signatures[0].keyid') "
        f"IN (SELECT keyid FROM validators)))"
    )


def _scan_ceiling_error(surface: str, ceiling: int, found: int, limit: int):
    """The ScanCeilingReached a read surface raises when its scan ran out."""
    return ScanCeilingReached(
        f"{surface} stopped at the {ceiling}-row scan ceiling with {found} of "
        f"{limit} claims collected; rows past the ceiling were not read, so "
        f"this result would be short without saying so. Narrow the query or "
        f"lower limit."
    )


# Returned instead of None when a row is dropped because its signature did not
# re-verify. A failed re-verification is a tamper signal, not ordinary
# filtering, and the caller counts the two apart.
_VERIFY_EXCLUDED = object()


def _read_path_row(
    conn: sqlite3.Connection,
    row,
    *,
    reputation: dict,
    enrolled_keyids: set,
    include_unverified: bool,
    trust_domain: tuple,
    verify_cache: dict,
) -> dict | None | object:
    """Project one claims row for a read surface, or exclude it.

    Shared by :func:`query_claims` and :func:`search_claims` so the read-path
    verification cannot drift between the two surfaces. Attaches
    ``generator_enrolled`` and ``validator_reputation``; drops an
    unenrolled-generator PRELIMINARY row unless ``include_unverified`` (returns
    ``None``); drops a REPLICATED / ESTABLISHED row whose signature does not
    re-verify (returns :data:`_VERIFY_EXCLUDED`, independent of
    ``include_unverified``, which only relaxes the PRELIMINARY generator filter,
    never the high-trust signature check); and attaches the trust-domain
    disclosure to an ESTABLISHED row.
    """
    d = dict(row)
    gen_keyid = _extract_signature_bundle_keyid(d.get("signature_bundle"))
    d["generator_enrolled"] = (
        gen_keyid is not None and gen_keyid in enrolled_keyids
    )
    validator_kid = d.get("validator_keyid")
    d["validator_reputation"] = (
        reputation.get(validator_kid, 0) if validator_kid else 0
    )
    if not include_unverified and (
        d["support_level"] == "PRELIMINARY" and not d["generator_enrolled"]
    ):
        return None
    if not _row_verified_on_read(conn, d, verify_cache):
        return _VERIFY_EXCLUDED
    if d["support_level"] == "ESTABLISHED":
        d["single_trust_domain"], d["trust_domain_root"] = trust_domain
    return d


def _project_verified_rows(
    conn: sqlite3.Connection,
    rows: "Iterable",
    *,
    limit: int,
    include_unverified: bool,
    on_verify_excluded: Callable[[int], None] | None = None,
) -> tuple[list[dict], int]:
    """Filter and project rows for a read surface, stopping at ``limit`` survivors.

    Computes the per-call reputation, enrolled set, and trust-domain disclosure
    once, then applies :func:`_read_path_row` in sorted order until ``limit``
    survivors are collected. The single ordered fetch happens in the caller, so
    the table is sorted once, not re-sorted per batch. ``rows`` may be a live
    cursor: the early break then stops fetching, so the common path pulls a
    handful of rows rather than the whole scan ceiling.

    Rows dropped by verify-on-read are counted and reported: a WARNING names
    the count, and *on_verify_excluded* (when given) receives it, so a tampered
    row registers as a signal instead of as a shorter list. Without this the
    only trace of a tamper on an enumerating surface is a row that is not
    there, indistinguishable from a claim that never existed.

    Returns ``(survivors, scanned)``. ``scanned`` is how many rows were pulled,
    which the caller compares against the scan ceiling to tell "that is all
    there is" from "the scan ran out before the survivors did".
    """
    if limit <= 0:
        # The loop appends a survivor before testing the stop condition, so it
        # would hand back one row for a limit of zero. Nothing was asked for:
        # return nothing, and skip the per-call reputation and trust-domain work.
        return [], 0
    reputation = _compute_validator_reputation(conn)
    enrolled_keyids = _enrolled_validator_keyids(conn)
    trust_domain = _trust_domain_disclosure(conn)
    verify_cache: dict = {}
    results: list[dict] = []
    scanned = 0
    excluded = 0
    for row in rows:
        scanned += 1
        d = _read_path_row(
            conn, row,
            reputation=reputation, enrolled_keyids=enrolled_keyids,
            include_unverified=include_unverified, trust_domain=trust_domain,
            verify_cache=verify_cache,
        )
        if d is _VERIFY_EXCLUDED:
            excluded += 1
        elif d is not None:
            results.append(d)
            if len(results) >= limit:
                break
    if excluded:
        import logging
        logging.getLogger("mareforma").warning(
            "Read excluded %s claim(s) whose signature did not re-verify; "
            "call get_claim() or `mareforma verify` on the affected claim_id "
            "for the detail.",
            excluded,
        )
        if on_verify_excluded is not None:
            on_verify_excluded(excluded)
    return results, scanned


def query_claims(
    conn: sqlite3.Connection,
    *,
    limit: int = 10,
    text: str | None = None,
    min_support: str | None = None,
    classification: str | None = None,
    include_unverified: bool = False,
    include_invalidated: bool = False,
    refutation_filter: str | None = None,
    on_verify_excluded: Callable[[int], None] | None = None,
) -> list[dict]:
    """Return claims ordered by support_level (desc) then recency (desc).

    Parameters
    ----------
    limit:
        Maximum number of claims to return. Default 10. Zero returns no
        claims; a negative limit raises ``ValueError``.
    text:
        Optional substring filter: case-insensitive LIKE match on claim text.
    min_support:
        Minimum support level: 'PRELIMINARY' | 'REPLICATED' | 'ESTABLISHED'.
    classification:
        Filter by classification: 'INFERRED' | 'ANALYTICAL' | 'DERIVED'.
    include_unverified:
        When False (default), PRELIMINARY claims whose ``signature_bundle``
        is unsigned or signed by a keyid not present in the ``validators``
        table are excluded by default. REPLICATED and
        ESTABLISHED rows already require an enrolled validator chain and
        are never filtered by this flag. Pass ``True`` to surface
        unverified preliminary claims (e.g. inspection of pending work).
    include_invalidated:
        When False (default), claims with non-NULL ``t_invalid`` are
        excluded: a contradiction_verdicts row from an enrolled
        validator has marked them invalid. Pass ``True`` for audit /
        history queries where you want to see contradicted claims too.
    on_verify_excluded:
        Optional callback receiving the number of rows this call dropped
        because their signature did not re-verify. No flag surfaces those
        rows, so this is how a caller learns a result is short because the
        graph was tampered with rather than because it is empty.

    Each returned dict carries the standard claim columns plus two
    reputation projections computed at query time:

      - ``validator_reputation`` (int): for ESTABLISHED rows, the number
        of ESTABLISHED claims signed by the same validator (≥ 1). For
        other rows, ``0``.
      - ``generator_enrolled`` (bool): True iff the claim's
        ``signature_bundle`` is signed by an enrolled validator. False
        for unsigned claims and for signatures by unenrolled keys.
    """
    _require_non_negative_limit(limit, "query")

    conditions: list[str] = []
    params: list = []

    if text is not None:
        # SQLite treats % and _ as LIKE wildcards; a caller-supplied
        # text containing those metacharacters (or an empty string)
        # would otherwise behave as a wildcard match against every
        # row. Escape with \ + ESCAPE clause so the substring filter
        # is literal as documented.
        escaped = (
            text.replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
        )
        conditions.append("text LIKE ? ESCAPE '\\'")
        params.append(f"%{escaped}%")

    if not include_invalidated:
        conditions.append("t_invalid IS NULL")

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

    # Refutation filter is composable with include_invalidated:
    #   refutation_filter="clean"       , restrict to clean rows
    #   refutation_filter="contradicted", restrict to t_invalid IS NOT NULL
    #   refutation_filter="contested"   , restrict to status='contested'
    #   refutation_filter="retracted"   , restrict to status='retracted'
    #   refutation_filter="any"         , include every refutation state
    #                                      (implies include_invalidated=True)
    if refutation_filter is not None:
        if refutation_filter not in VALID_REFUTATION_FILTERS:
            raise ValueError(
                f"Unknown refutation_filter '{refutation_filter}'. "
                f"Use one of: {', '.join(VALID_REFUTATION_FILTERS)}"
            )
        if refutation_filter == "clean":
            # Guard against double-adding t_invalid IS NULL when
            # include_invalidated=False already pushed the same
            # predicate above. SQL is idempotent on AND-of-equals
            # today, but the conditions.remove() pattern below only
            # strips the first occurrence, if a future refactor
            # expects exactly-once semantics, the duplicate could
            # silently widen results.
            if "t_invalid IS NULL" not in conditions:
                conditions.append("t_invalid IS NULL")
            conditions.append("status = 'open'")
        elif refutation_filter == "contradicted":
            # Override include_invalidated=False so we can SELECT
            # contradicted rows even when the caller forgot to flip
            # the include flag.
            if "t_invalid IS NULL" in conditions:
                conditions.remove("t_invalid IS NULL")
            conditions.append("t_invalid IS NOT NULL")
        elif refutation_filter == "contested":
            # A row can be both contested AND contradicted; the
            # caller asking for "contested" wants every contested
            # row regardless of t_invalid, so override the default
            # invalidation gate.
            if "t_invalid IS NULL" in conditions:
                conditions.remove("t_invalid IS NULL")
            conditions.append("status = 'contested'")
        elif refutation_filter == "retracted":
            # Same posture: retracted-and-contradicted should still
            # surface under "retracted".
            if "t_invalid IS NULL" in conditions:
                conditions.remove("t_invalid IS NULL")
            conditions.append("status = 'retracted'")
        elif refutation_filter == "any":
            # Surface every refutation kind, implies include_invalidated.
            if "t_invalid IS NULL" in conditions:
                conditions.remove("t_invalid IS NULL")

    if not include_unverified:
        conditions.append(_enrolled_generator_condition())

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    # The signature re-verification runs in Python after the fetch, so a flat
    # `LIMIT limit` could under-return when the top rows are all drained. Order
    # the table once and materialise up to the scan ceiling of sorted rows in a
    # single statement (no growing OFFSET, so no per-batch re-scan and re-sort),
    # then filter for survivors.
    base_sql = (
        f"SELECT {_CLAIM_SELECT} FROM claims {where} "
        f"ORDER BY CASE support_level "
        f"WHEN 'ESTABLISHED' THEN 3 WHEN 'REPLICATED' THEN 2 ELSE 1 END DESC, "
        f"created_at DESC LIMIT ?"
    )
    ceiling = _read_scan_ceiling(limit)
    try:
        cursor = conn.execute(base_sql, params + [ceiling])
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to query claims: {exc}") from exc
    # Step the live cursor rather than .fetchall(): _project_verified_rows breaks
    # at `limit` survivors, and on the common path (the first `limit` rows all
    # survive) that break stops fetching too, so the ceiling stays the worst-case
    # bound for the adversarial drain path instead of the per-call materialisation.
    results, scanned = _project_verified_rows(
        conn, cursor, limit=limit, include_unverified=include_unverified,
        on_verify_excluded=on_verify_excluded,
    )
    if scanned >= ceiling and len(results) < limit:
        raise _scan_ceiling_error("query", ceiling, len(results), limit)
    return results


def _extract_signature_bundle_keyid(bundle_json: str | None) -> str | None:
    """Return the signing keyid embedded in a claim's signature_bundle,
    or None if the bundle is absent or malformed."""
    if bundle_json is None:
        return None
    try:
        bundle = json.loads(bundle_json)
        return bundle["signatures"][0]["keyid"]
    except (json.JSONDecodeError, KeyError, IndexError, TypeError):
        return None


def _enrolled_validator_keyids(conn: sqlite3.Connection) -> set[str]:
    """Return the set of keyids currently in the validators table.

    Membership only: does NOT walk the enrollment chain. The chain
    walk in :func:`mareforma.validators.is_enrolled` is the
    authoritative check for individual validations; this set is a
    cheap pre-filter used by :func:`query_claims` to decide whether a
    PRELIMINARY claim's generator is "enrolled enough" to surface
    without ``include_unverified=True``.
    """
    rows = conn.execute("SELECT keyid FROM validators").fetchall()
    return {r["keyid"] for r in rows}


def _compute_validator_reputation(
    conn: sqlite3.Connection,
) -> dict[str, int]:
    """Return ``{validator_keyid: count}`` for ESTABLISHED claims.

    Count is the number of ESTABLISHED rows whose ``validator_keyid``
    equals the key. Validators with zero ESTABLISHED rows are omitted
    from the dict (caller defaults to 0). Derived state, recomputed
    on every call, never cached.
    """
    rows = conn.execute(
        "SELECT validator_keyid, COUNT(*) AS n FROM claims "
        "WHERE support_level = 'ESTABLISHED' "
        "  AND validator_keyid IS NOT NULL "
        "GROUP BY validator_keyid"
    ).fetchall()
    return {r["validator_keyid"]: int(r["n"]) for r in rows}


def _validate_fts5_query(query: str) -> str:
    """Sanity-check an FTS5 MATCH expression.

    Refuses empty strings and queries consisting entirely of wildcards
    (e.g. ``"*"``, ``"* **"``). FTS5 prefix syntax is ``term*`` and the
    leading-``*`` form is not valid syntax anyway, but a user who
    expects shell-glob semantics deserves a clear error instead of
    SQLite's terse ``fts5: syntax error near "*"``.
    """
    stripped = query.strip()
    if not stripped:
        raise ValueError(
            "Empty search query. Pass at least one term, optionally "
            "with FTS5 prefix syntax: graph.search('gene*')."
        )
    tokens = stripped.split()
    if all(t.strip("*") == "" for t in tokens):
        raise ValueError(
            f"Search query {query!r} is just wildcards. FTS5 prefix "
            "search requires at least one term (e.g. 'gene*'). A pure "
            "wildcard would scan the whole table and is refused."
        )
    return stripped


def search_claims(
    conn: sqlite3.Connection,
    query: str,
    *,
    limit: int = 20,
    min_support: str | None = None,
    classification: str | None = None,
    include_unverified: bool = False,
    include_invalidated: bool = False,
    on_verify_excluded: Callable[[int], None] | None = None,
) -> list[dict]:
    """FTS5-ranked search over claim text.

    Returns claim dicts ordered by FTS5 rank (best match first). Each
    dict carries the same projection as :func:`query_claims`:
    ``validator_reputation`` and ``generator_enrolled`` are attached
    per row, and ``include_unverified`` / ``include_invalidated`` /
    ``on_verify_excluded`` behave identically.

    The ``query`` string is passed through to SQLite's FTS5 MATCH
    operator. FTS5 syntax (phrase matching with double quotes, prefix
    search with trailing ``*``, ``AND``/``OR``/``NOT`` operators, and
    parentheses) works as documented in SQLite. Pure-wildcard queries
    are refused (see :func:`_validate_fts5_query`). ``limit`` follows
    :func:`query_claims`: zero returns no claims, a negative limit raises
    ``ValueError``.
    """
    _require_non_negative_limit(limit, "search")
    fts_query = _validate_fts5_query(query)

    if min_support is not None and min_support not in VALID_SUPPORT_LEVELS:
        raise ValueError(
            f"Unknown min_support '{min_support}'. "
            f"Use one of: {', '.join(VALID_SUPPORT_LEVELS)}"
        )
    if classification is not None and classification not in VALID_CLASSIFICATIONS:
        raise ValueError(
            f"Unknown classification '{classification}'. "
            f"Use one of: {', '.join(VALID_CLASSIFICATIONS)}"
        )

    conditions: list[str] = ["claims_fts MATCH ?"]
    params: list = [fts_query]

    if not include_invalidated:
        conditions.append("c.t_invalid IS NULL")

    if min_support is not None:
        tiers = _SUPPORT_LEVEL_TIERS[min_support]
        placeholders = ",".join("?" * len(tiers))
        conditions.append(f"c.support_level IN ({placeholders})")
        params.extend(tiers)
    if classification is not None:
        conditions.append("c.classification = ?")
        params.append(classification)
    if not include_unverified:
        conditions.append(_enrolled_generator_condition("c."))

    where = " AND ".join(conditions)
    select_cols = ", ".join(f"c.{col}" for col in _CLAIM_COLUMNS)
    # Rank once and materialise up to the scan ceiling in a single statement,
    # then project through the SAME read-path filter as query_claims. Routing
    # both surfaces through _project_verified_rows re-verifies high-trust rows
    # here too, so search cannot serve a REPLICATED / ESTABLISHED row that query
    # correctly excludes, and the two projections cannot drift apart.
    base_sql = (
        f"SELECT {select_cols} FROM claims_fts f "
        f"JOIN claims c ON c.claim_id = f.claim_id "
        f"WHERE {where} "
        f"ORDER BY rank LIMIT ?"
    )
    ceiling = _read_scan_ceiling(limit)
    try:
        cursor = conn.execute(base_sql, params + [ceiling])
    except sqlite3.OperationalError as exc:
        # FTS5 raises OperationalError on malformed MATCH syntax.
        # Wrap so callers don't have to import sqlite3 to pattern-match.
        msg = str(exc)
        if "fts5" in msg or "syntax error" in msg:
            raise ValueError(
                f"Search query {query!r} is not valid FTS5 syntax: {msg}"
            ) from exc
        raise DatabaseError(f"Failed to search claims: {exc}") from exc
    # Step the ranked cursor lazily: _project_verified_rows stops at `limit`
    # survivors, so the common path fetches a handful, not the whole ceiling.
    results, scanned = _project_verified_rows(
        conn, cursor, limit=limit, include_unverified=include_unverified,
        on_verify_excluded=on_verify_excluded,
    )
    if scanned >= ceiling and len(results) < limit:
        raise _scan_ceiling_error("search", ceiling, len(results), limit)
    return results


def get_validator_reputation(conn: sqlite3.Connection) -> dict[str, int]:
    """Public wrapper around :func:`_compute_validator_reputation`.

    Returns a dict mapping every enrolled validator keyid to its
    ESTABLISHED-claim count. Validators with zero validations are
    included with ``count=0`` (the bulk map use case wants the full
    enrollment list, not just the active validators).
    """
    counts = _compute_validator_reputation(conn)
    enrolled = _enrolled_validator_keyids(conn)
    return {keyid: counts.get(keyid, 0) for keyid in enrolled}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# Backup deferral state, keyed by id(conn). A connection with an open deferral
# window records that a rewrite is due rather than writing claims.toml; the
# window writes once when the outermost window closes. The connection stays
# alive for the whole window, so its id is stable and unique for it. The value
# is ``[depth, dirty]``: nesting depth so an inner window does not end the outer
# one, and the pending-dirty flag a mutation sets.
_backup_suspended: dict[int, list] = {}


def suspend_backup(conn: sqlite3.Connection) -> None:
    """Open a backup-deferral window on *conn*. While open, _backup_claims_toml
    marks the backup dirty rather than writing claims.toml. Windows nest: a
    mutation writes once when the outermost window closes."""
    state = _backup_suspended.get(id(conn))
    if state is None:
        _backup_suspended[id(conn)] = [1, False]
    else:
        state[0] += 1


def resume_backup(conn: sqlite3.Connection, root: Path) -> None:
    """Close one deferral window on *conn*. The outermost close writes
    claims.toml once if a mutation marked it dirty; an inner close keeps the
    window open. A no-op when no window is open."""
    state = _backup_suspended.get(id(conn))
    if state is None:
        return
    state[0] -= 1
    if state[0] > 0:
        return
    dirty = state[1]
    del _backup_suspended[id(conn)]
    if dirty:
        _backup_claims_toml(conn, root)


def _drain_backup_window(conn: sqlite3.Connection, root: Path) -> None:
    """Close every open deferral level on *conn* at once and flush a pending
    write. For teardown: a graph closed mid-batch still leaves claims.toml
    current, and no window keyed on a soon-reused id() is left behind."""
    state = _backup_suspended.pop(id(conn), None)
    if state is not None and state[1]:
        _backup_claims_toml(conn, root)


def get_project_policy(conn: sqlite3.Connection) -> dict | None:
    """Return the singleton project-policy row as a dict, or None if unset.

    The signed ``envelope`` is the authority; the flat columns are a read
    cache. restore verifies the envelope against the enrolled root before
    trusting either.
    """
    row = conn.execute(
        "SELECT rekor_required, signer_keyid, envelope, created_at "
        "FROM project_policy WHERE id = 1"
    ).fetchone()
    return dict(row) if row is not None else None


def set_project_policy(
    conn: sqlite3.Connection,
    root: Path,
    *,
    envelope: str,
    signer_keyid: str,
    rekor_required: bool,
    created_at: str,
) -> dict:
    """Persist the singleton project policy and refresh the backup.

    One-way: a policy already present is returned unchanged (a project cannot
    revoke witnessing once required). Locks with BEGIN IMMEDIATE so a racing
    writer serializes on the singleton insert. Returns the effective policy.
    """
    existing = get_project_policy(conn)
    if existing is not None:
        return existing
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = get_project_policy(conn)
        if existing is not None:
            conn.execute("COMMIT")
            return existing
        conn.execute(
            "INSERT INTO project_policy "
            "(id, rekor_required, signer_keyid, envelope, created_at) "
            "VALUES (1, ?, ?, ?, ?)",
            (1 if rekor_required else 0, signer_keyid, envelope, created_at),
        )
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise
    _backup_claims_toml(conn, root)
    return get_project_policy(conn)


# Trust-layer tables round-tripped through claims.toml, in foreign-key order
# (parents before children) so restore can replay them without FK violations:
# predictions -> propositions, findings -> propositions/predictions/claims,
# evidence_lines -> findings, contrasts -> evidence_lines,
# effect_estimates -> contrasts. Each tuple is
# (section_name, table, primary_key, ordered non-PK columns). The explicit
# column list avoids SELECT * coupling and is the single source of truth shared
# by the backup reader and the restore writer.
_TRUST_TABLE_BACKUP: tuple = (
    ("propositions", "propositions", "content_id", (
        "frame_id", "subject", "relation", "object", "direction",
        "scope_json", "magnitude", "content_id_policy", "schema_version",
        "created_at",
    )),
    ("predictions", "predictions", "plan_id", (
        "content_id", "inference_regime", "test_type", "direction_of_interest",
        "equivalence_lower", "equivalence_upper", "alpha", "preregistered",
        "registered_at",
    )),
    ("findings", "findings", "finding_id", (
        "content_id", "plan_id", "claim_id", "bearing_direction", "created_at",
    )),
    ("evidence_lines", "evidence_lines", "line_id", (
        "finding_id", "modality", "provenance_id", "design_type", "data_id",
        "model_lineage", "created_at",
    )),
    ("contrasts", "contrasts", "contrast_id", (
        "line_id", "control_type",
    )),
    ("effect_estimates", "effect_estimates", "estimate_id", (
        "contrast_id", "estimate_value", "effect_type", "scale", "p_value",
        "ci_lower", "ci_upper", "ci_level", "n_total",
    )),
)


def _backup_trust_tables(conn: sqlite3.Connection, data: dict) -> None:
    """Add the populated trust-layer tables to the backup ``data`` dict.

    Each table becomes a TOML section keyed by its primary key, with NULL-valued
    columns omitted (TOML cannot serialize None, and restore reads each column
    with a NULL default). The finding tree is reconstructable on restore from
    these rows, which hang off the finding's own signed attestation claim.
    """
    for section, table, pk, cols in _TRUST_TABLE_BACKUP:
        rows = conn.execute(
            f"SELECT {pk}, {', '.join(cols)} FROM {table}"
        ).fetchall()
        if not rows:
            continue
        section_data: dict[str, Any] = {}
        for r in rows:
            entry: dict[str, Any] = {}
            for col in cols:
                value = r[col]
                if value is not None:
                    entry[col] = value
            section_data[r[pk]] = entry
        data[section] = section_data


def _backup_claims_toml(conn: sqlite3.Connection, root: Path) -> None:
    """Write all claims AND validators to claims.toml in the project root.

    Called after every claim or validator mutation. The TOML file is
    the source of truth for ``mareforma restore`` after catastrophic
    loss of ``graph.db``. Failure is non-fatal: an error line is
    printed to stderr but the exception is not raised: graph.db is
    still authoritative and the next successful mutation will rewrite
    the file. Stderr-ERROR (not ``warnings.warn``, which production
    callers often suppress) so divergence is visible by default.
    """
    state = _backup_suspended.get(id(conn))
    if state is not None:
        # A deferral window is open: mark a rewrite due; resume_backup writes it.
        state[1] = True
        return
    try:
        import tomli_w

        data: dict[str, Any] = {}

        # Validators first so a restore pass can verify enrollment
        # signatures before trying to verify the claims that reference
        # those keys.
        from mareforma import validators as _validators
        validator_rows = _validators.list_validators(conn)
        if validator_rows:
            data["validators"] = {}
            for v in validator_rows:
                data["validators"][v["keyid"]] = {
                    "pubkey_pem": v["pubkey_pem"],
                    "identity": v["identity"],
                    "validator_type": v["validator_type"],
                    "enrolled_at": v["enrolled_at"],
                    "enrolled_by_keyid": v["enrolled_by_keyid"],
                    "enrollment_envelope": v["enrollment_envelope"],
                }

        claims = list_claims(conn)
        data["claims"] = {}
        for c in claims:
            supports = json.loads(c.get("supports_json", "[]") or "[]")
            contradicts = json.loads(c.get("contradicts_json", "[]") or "[]")
            entry: dict[str, Any] = {
                "text": c["text"],
                "classification": c.get("classification") or "INFERRED",
                "support_level": c.get("support_level") or "PRELIMINARY",
                "generated_by": c.get("generated_by", "agent"),
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
            if c.get("unresolved"):
                entry["unresolved"] = True
            if c.get("idempotency_key"):
                # Not signed material, but dropping it breaks the retry-safe
                # write contract on a restored graph: the replay of a step
                # misses the key lookup and inserts a signed near-duplicate.
                entry["idempotency_key"] = c["idempotency_key"]
            if c.get("convergence_retry_needed"):
                # Audit flag: preserved across restore so the operator's
                # TODO list of "claims whose convergence detection still
                # needs a retry" doesn't reset to empty on a rebuild.
                entry["convergence_retry_needed"] = True
            if c.get("signature_bundle"):
                entry["signature_bundle"] = c["signature_bundle"]
            if c.get("validation_signature"):
                entry["validation_signature"] = c["validation_signature"]
            # transparency_logged: only record when it deviates from the
            # default (1). A 0 means "signed but awaiting Rekor inclusion".
            if c.get("transparency_logged") == 0:
                entry["transparency_logged"] = False
            if c.get("artifact_hash"):
                entry["artifact_hash"] = c["artifact_hash"]
            # Evidence vector: always present in the current schema.
            # Round-trip the full JSON so restore can rebuild the
            # canonical Statement v1 bytes, chain_hash + signature both
            # bind these values. statement_cid is the cross-check anchor
            # restore uses to detect envelope-vs-row drift.
            entry["evidence_json"] = c.get("evidence_json") or "{}"
            if c.get("statement_cid"):
                entry["statement_cid"] = c["statement_cid"]
            # t_invalid is derived (set by the contradiction trigger
            # on signed verdict INSERT). Restore replays the verdict
            # table; the trigger fires again and re-sets t_invalid.
            # We do NOT round-trip the column directly, that would
            # accept a TOML-tampered t_invalid value without verifying
            # it against a signed contradiction envelope.
            # Adapter-specific predicate_payload + federation-imported
            # original_signature_bundle: round-trip only when populated.
            # Empty/NULL defaults stay omitted from the TOML so backups
            # don't grow new fields uselessly.
            if c.get("predicate_payload"):
                entry["predicate_payload"] = c["predicate_payload"]
            if c.get("original_signature_bundle"):
                entry["original_signature_bundle"] = c["original_signature_bundle"]
            # Observed grounding verdict: round-trip only when populated, so a
            # backup of a graph that never used the observer grows no new field.
            # It is bound into the signed statement, so restore rebuilds the
            # canonical bytes from it and statement_cid catches any TOML tamper.
            if c.get("observed_grounding"):
                entry["observed_grounding"] = c["observed_grounding"]
            data["claims"][c["claim_id"]] = entry

        # Verdict tables. Each verdict carries its own signature
        # binding (issuer_keyid, payload bytes) so restore can
        # cryptographically verify before re-INSERT. The trigger that
        # sets t_invalid fires on the re-INSERT, restoring the
        # invalidation state without needing a separate t_invalid
        # round-trip.
        #
        # include_invalidated=True because backup MUST capture every
        # signed verdict regardless of whether its referenced claim
        # has been invalidated. The default-filter is for user-facing
        # query semantics; backup is audit-mode by definition.
        rep_rows = list_replication_verdicts(conn, include_invalidated=True)
        if rep_rows:
            data["replication_verdicts"] = {}
            for v in rep_rows:
                vid = v["verdict_id"]
                # other_claim_id is NULL for a single-row cross-method verdict.
                # TOML cannot serialize None, so emit the key only when present,
                # mirroring the conditional-key pattern in the claims section.
                # restore reads it with .get(), so an absent key yields None.
                verdict_entry: dict[str, Any] = {
                    "cluster_id": v["cluster_id"],
                    "member_claim_id": v["member_claim_id"],
                    "method": v["method"],
                    "confidence_json": v["confidence_json"],
                    "issuer_keyid": v["issuer_keyid"],
                    "signature": base64.b64encode(v["signature"]).decode("ascii"),
                    "created_at": v["created_at"],
                }
                if v["other_claim_id"] is not None:
                    verdict_entry["other_claim_id"] = v["other_claim_id"]
                data["replication_verdicts"][vid] = verdict_entry
        con_rows = list_contradiction_verdicts(conn, include_invalidated=True)
        if con_rows:
            data["contradiction_verdicts"] = {}
            for v in con_rows:
                vid = v["verdict_id"]
                data["contradiction_verdicts"][vid] = {
                    "member_claim_id": v["member_claim_id"],
                    "other_claim_id": v["other_claim_id"],
                    "confidence_json": v["confidence_json"],
                    "issuer_keyid": v["issuer_keyid"],
                    "signature": base64.b64encode(v["signature"]).decode("ascii"),
                    "created_at": v["created_at"],
                }

        # Rekor inclusion sidecar. Every successful submit is recorded
        # here independently of whether the claims-row UPDATE succeeded.
        # Round-tripping through TOML lets restore() re-verify inclusion
        # proofs against a pinned log pubkey.
        rekor_rows = conn.execute(
            "SELECT claim_id, uuid, log_index, integrated_time, "
            "raw_response_b64, recorded_at "
            "FROM rekor_inclusions ORDER BY recorded_at"
        ).fetchall()
        if rekor_rows:
            data["rekor_inclusions"] = {}
            for r in rekor_rows:
                # integrated_time is NULL when the log response carried a
                # malformed integratedTime. Emit the key only when present so
                # None is never handed to the TOML serializer; restore reads it
                # with .get() and tolerates its absence.
                rekor_entry: dict[str, Any] = {
                    "uuid": r["uuid"],
                    "log_index": r["log_index"],
                    "raw_response_b64": r["raw_response_b64"],
                    "recorded_at": r["recorded_at"],
                }
                if r["integrated_time"] is not None:
                    rekor_entry["integrated_time"] = r["integrated_time"]
                data["rekor_inclusions"][r["claim_id"]] = rekor_entry

        # Project policy: a root-signed, project-wide trust declaration. The
        # signed envelope is the authority restore verifies; the flat fields
        # are the read cache. Emitted only when set.
        policy_row = get_project_policy(conn)
        if policy_row is not None:
            data["project_policy"] = {
                "rekor_required": bool(policy_row["rekor_required"]),
                "signer_keyid": policy_row["signer_keyid"],
                "envelope": policy_row["envelope"],
                "created_at": policy_row["created_at"],
            }

        # Trust layer (propositions, predictions, findings, evidence_lines,
        # contrasts, effect_estimates). Round-trip these query-side tables so
        # the documented delete-and-restore recovery rebuilds the finding tree,
        # not just the surviving finding claims. Emitted only when populated.
        _backup_trust_tables(conn, data)

        out = root / "claims.toml"
        payload = tomli_w.dumps(data).encode("utf-8")
        # Atomic write: a crash during the rewrite must not destroy the sole DR
        # artifact on the exact crash class it exists for. Write a temp file in
        # the same directory, fsync it, then os.replace onto the target (an
        # atomic rename on POSIX). A failure anywhere before the replace leaves
        # the previous good claims.toml untouched, never truncated or empty.
        fd, tmp_name = tempfile.mkstemp(
            prefix=".claims.toml.", suffix=".tmp", dir=str(root),
        )
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, out)
            # fsync the directory so the rename itself survives a power loss,
            # not just the temp file's contents. Best-effort: os.replace has
            # already committed the write, so a directory-fsync failure must
            # not surface as a backup failure. Skip where the platform has no
            # directory fd (Windows).
            if hasattr(os, "O_DIRECTORY"):
                try:
                    dir_fd = os.open(str(root), os.O_DIRECTORY)
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
                except OSError:
                    pass
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    except Exception as exc:  # noqa: BLE001
        import sys
        # stderr at an ERROR-line prefix is harder for production to
        # silently swallow than warnings.warn (which downstream code
        # routinely filters out). graph.db remains authoritative;
        # this line surfaces the divergence so an operator notices.
        print(
            f"ERROR: claims.toml backup failed; graph.db is "
            f"authoritative, {exc}",
            file=sys.stderr,
        )
