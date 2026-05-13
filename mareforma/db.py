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

import base64
import hashlib
import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS claims (
    claim_id        TEXT PRIMARY KEY,
    text            TEXT NOT NULL,
    classification  TEXT NOT NULL DEFAULT 'INFERRED'
                        CHECK (classification IN ('INFERRED', 'ANALYTICAL', 'DERIVED')),
    support_level   TEXT NOT NULL DEFAULT 'PRELIMINARY'
                        CHECK (support_level IN ('PRELIMINARY', 'REPLICATED', 'ESTABLISHED')),
    idempotency_key TEXT,
    validated_by    TEXT,
    validated_at    TEXT,
    status          TEXT NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open', 'contested', 'retracted')),
    source_name     TEXT,
    generated_by    TEXT NOT NULL DEFAULT 'agent',
    supports_json   TEXT NOT NULL DEFAULT '[]',
    contradicts_json TEXT NOT NULL DEFAULT '[]',
    comparison_summary TEXT,
    branch_id       TEXT NOT NULL DEFAULT 'main',
    unresolved      INTEGER NOT NULL DEFAULT 0
                        CHECK (unresolved IN (0, 1)),
    signature_bundle TEXT,
    transparency_logged INTEGER NOT NULL DEFAULT 1
                        CHECK (transparency_logged IN (0, 1)),
    validation_signature TEXT,
    -- Denormalized from validation_signature's payload for indexable
    -- reputation queries. NULL for non-ESTABLISHED rows. The envelope
    -- remains authoritative; if this column ever drifts from the
    -- envelope it is the envelope that wins.
    validator_keyid TEXT,
    artifact_hash   TEXT,
    prev_hash       TEXT,
    -- GRADE 5-domain EvidenceVector. Stored inside the signed Statement
    -- v1 predicate; denormalised here for queryable filters
    -- ("WHERE ev_risk_of_bias <= -1"). Bounded [-2, 0] matches the GRADE
    -- downgrade scale. Default 0 = unflagged. CHECK rejects tamper
    -- attempts that set out-of-range values directly via SQL.
    ev_risk_of_bias     INTEGER NOT NULL DEFAULT 0
                            CHECK (ev_risk_of_bias    BETWEEN -2 AND 0),
    ev_inconsistency    INTEGER NOT NULL DEFAULT 0
                            CHECK (ev_inconsistency   BETWEEN -2 AND 0),
    ev_indirectness     INTEGER NOT NULL DEFAULT 0
                            CHECK (ev_indirectness    BETWEEN -2 AND 0),
    ev_imprecision      INTEGER NOT NULL DEFAULT 0
                            CHECK (ev_imprecision     BETWEEN -2 AND 0),
    ev_pub_bias         INTEGER NOT NULL DEFAULT 0
                            CHECK (ev_pub_bias        BETWEEN -2 AND 0),
    -- Full EvidenceVector serialised as JSON. The denormalised ev_*
    -- columns above carry the queryable subset; rationale, upgrade
    -- flags, and reporting_compliance live in this JSON blob. The
    -- envelope's signed predicate is the authoritative copy.
    evidence_json   TEXT NOT NULL DEFAULT '{}',
    -- statement_cid = sha256(canonicalize(statement)). The cross-check
    -- anchor restore uses to detect envelope-vs-row drift. Always
    -- recomputable from the row's fields + evidence_json + statement
    -- v1 builder. NULL is allowed for unsigned rows.
    statement_cid   TEXT,
    -- Verdict-derived invalidation timestamp. Set by the
    -- contradiction_invalidates_older trigger on contradiction_verdicts
    -- INSERT. NULL for non-invalidated claims. Outside the
    -- no-state-laundering trigger column list because invalidation is
    -- a derived state, not a row mutation — restore replays it from
    -- contradiction_verdicts rather than trusting the column value.
    t_invalid       INTEGER,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    -- ESTABLISHED rows must carry a signed validation envelope. The
    -- trigger below also enforces this on UPDATE; the CHECK is the
    -- row-level belt to the trigger's transition-level suspenders.
    -- ``validated_by`` is a display label (the cryptographic identity
    -- lives in ``validation_signature``) and may be NULL.
    CHECK (support_level != 'ESTABLISHED' OR validation_signature IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_claims_status
    ON claims(status);
CREATE INDEX IF NOT EXISTS idx_claims_source
    ON claims(source_name);
CREATE INDEX IF NOT EXISTS idx_claims_generated_by
    ON claims(generated_by);
CREATE INDEX IF NOT EXISTS idx_claims_support_level
    ON claims(support_level);
CREATE INDEX IF NOT EXISTS idx_claims_unresolved
    ON claims(unresolved);
CREATE INDEX IF NOT EXISTS idx_claims_transparency_logged
    ON claims(transparency_logged);
CREATE INDEX IF NOT EXISTS idx_claims_artifact_hash
    ON claims(artifact_hash) WHERE artifact_hash IS NOT NULL;
-- Reputation reads aggregate ESTABLISHED claims per validator. Partial
-- on NOT NULL keeps index storage proportional to ESTABLISHED-only rows.
CREATE INDEX IF NOT EXISTS idx_claims_validator_keyid
    ON claims(validator_keyid) WHERE validator_keyid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_claims_idempotency_key
    ON claims(idempotency_key) WHERE idempotency_key IS NOT NULL;
-- UNIQUE on prev_hash catches branched chains (two writers racing past
-- a missing BEGIN IMMEDIATE, or a manual SQL tamper that re-uses an
-- existing chain link). Partial index keeps the constraint scoped to
-- rows where the chain link is set.
CREATE UNIQUE INDEX IF NOT EXISTS idx_claims_prev_hash
    ON claims(prev_hash) WHERE prev_hash IS NOT NULL;

-- State-machine triggers. Reject illegal transitions with mareforma:
-- prefixed messages so Python can translate sqlite3.IntegrityError to
-- IllegalStateTransitionError without parsing English.

CREATE TRIGGER IF NOT EXISTS claims_insert_state_check
BEFORE INSERT ON claims
BEGIN
    SELECT CASE
        WHEN NEW.support_level NOT IN ('PRELIMINARY', 'ESTABLISHED') THEN
            RAISE(ABORT, 'mareforma:state:insert_invalid_level:' || NEW.support_level)
        WHEN NEW.support_level = 'ESTABLISHED' AND
             NEW.validation_signature IS NULL THEN
            RAISE(ABORT, 'mareforma:state:insert_established_without_validation')
        WHEN NEW.support_level = 'PRELIMINARY' AND
             (NEW.validated_by IS NOT NULL OR NEW.validated_at IS NOT NULL) THEN
            RAISE(ABORT, 'mareforma:state:insert_preliminary_with_validation')
    END;
END;

CREATE TRIGGER IF NOT EXISTS claims_update_state_check
BEFORE UPDATE OF support_level ON claims
BEGIN
    SELECT CASE
        WHEN OLD.support_level = 'PRELIMINARY' AND
             NEW.support_level NOT IN ('PRELIMINARY', 'REPLICATED') THEN
            RAISE(ABORT, 'mareforma:state:illegal_transition:PRELIMINARY->' || NEW.support_level)
        WHEN OLD.support_level = 'REPLICATED' AND
             NEW.support_level NOT IN ('REPLICATED', 'ESTABLISHED') THEN
            RAISE(ABORT, 'mareforma:state:illegal_transition:REPLICATED->' || NEW.support_level)
        WHEN OLD.support_level = 'ESTABLISHED' AND
             NEW.support_level != 'ESTABLISHED' THEN
            RAISE(ABORT, 'mareforma:state:illegal_transition:ESTABLISHED->' || NEW.support_level)
        WHEN NEW.support_level = 'ESTABLISHED' AND
             NEW.validation_signature IS NULL THEN
            RAISE(ABORT, 'mareforma:state:established_without_validation')
    END;
END;

-- Retracted is terminal. Without this, an adversary could assert a
-- born-retracted claim, flip it back to 'open' via update_claim (a pure
-- status mutation never triggers a REPLICATED re-check), and then ride
-- an honest peer's INSERT into REPLICATED. The signed envelope does not
-- bind status, so the resurrection carries no signature evidence. Make
-- retraction one-way at the storage layer: to resurrect a withdrawn
-- finding, assert a new claim citing the old via contradicts=[<old>].
CREATE TRIGGER IF NOT EXISTS claims_update_status_terminal
BEFORE UPDATE OF status ON claims
BEGIN
    SELECT CASE
        WHEN OLD.status = 'retracted' AND NEW.status != 'retracted' THEN
            RAISE(ABORT, 'mareforma:state:retracted_is_terminal:' || NEW.status)
    END;
END;

CREATE TABLE IF NOT EXISTS doi_cache (
    doi              TEXT PRIMARY KEY,
    resolved         INTEGER NOT NULL CHECK (resolved IN (0, 1)),
    registry         TEXT,
    last_checked_at  TEXT NOT NULL
);

-- Full-text search over claim text. Independent FTS5 virtual table
-- (not content=claims) so the storage cost is the only price of the
-- search feature and the sync triggers below stay readable.
-- ``claim_id`` is UNINDEXED — stored for join-back but not tokenized.
-- The unicode61 tokenizer is locale-agnostic; remove_diacritics=2 folds
-- accented characters so "gene" matches "géné".
CREATE VIRTUAL TABLE IF NOT EXISTS claims_fts USING fts5(
    claim_id UNINDEXED,
    text,
    tokenize='unicode61 remove_diacritics 2'
);

-- Keep claims_fts in lockstep with claims. The trigger fires AFTER the
-- INSERT/UPDATE/DELETE so any IntegrityError on the wrapping write
-- rolls back both the claims row and the FTS sync atomically.
CREATE TRIGGER IF NOT EXISTS claims_fts_ai AFTER INSERT ON claims BEGIN
    INSERT INTO claims_fts(claim_id, text) VALUES (NEW.claim_id, NEW.text);
END;

CREATE TRIGGER IF NOT EXISTS claims_fts_ad AFTER DELETE ON claims BEGIN
    DELETE FROM claims_fts WHERE claim_id = OLD.claim_id;
END;

-- text is in SIGNED_FIELDS, so update_claim refuses text mutation on a
-- signed claim. This trigger handles the unsigned-edit-text path AND
-- the legacy path before claim signing was the default.
CREATE TRIGGER IF NOT EXISTS claims_fts_au AFTER UPDATE OF text ON claims BEGIN
    UPDATE claims_fts SET text = NEW.text WHERE claim_id = OLD.claim_id;
END;

CREATE TABLE IF NOT EXISTS validators (
    keyid                TEXT PRIMARY KEY,
    pubkey_pem           TEXT NOT NULL,
    identity             TEXT NOT NULL,
    validator_type       TEXT NOT NULL DEFAULT 'human'
                             CHECK (validator_type IN ('human', 'llm')),
    enrolled_at          TEXT NOT NULL,
    enrolled_by_keyid    TEXT NOT NULL,
    enrollment_envelope  TEXT NOT NULL
);
"""


# Explicit column list — avoids SELECT * coupling to schema changes.
# Source of truth for the column-presence check in open_db().
_CLAIM_COLUMNS = (
    "claim_id", "text", "classification", "support_level",
    "idempotency_key", "validated_by", "validated_at",
    "status", "source_name", "generated_by",
    "supports_json", "contradicts_json",
    "comparison_summary", "branch_id", "unresolved",
    "signature_bundle", "transparency_logged",
    "validation_signature",
    "validator_keyid",
    "artifact_hash",
    "prev_hash",
    # GRADE EvidenceVector denormalised columns + full JSON.
    "ev_risk_of_bias", "ev_inconsistency", "ev_indirectness",
    "ev_imprecision", "ev_pub_bias",
    "evidence_json",
    # Statement v1 content identifier + verdict-derived invalidation.
    "statement_cid", "t_invalid",
    "created_at", "updated_at",
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


class SignedClaimImmutableError(MareformaError):
    """Raised when `update_claim` is asked to mutate a signed-surface field.

    Once a claim has a signature attached, mutating any field that was part
    of the signed payload (``text``, ``supports``, ``contradicts``,
    ``classification``, ``generated_by``, ``source_name``) would invalidate
    the signature without surfacing the change. To revise a signed claim,
    retract the old one (``status='retracted'``) and assert a new one that
    cites the old via ``contradicts=[<old_claim_id>]``.
    """


class IdempotencyConflictError(MareformaError):
    """Raised when an idempotency_key replay arrives with conflicting fields.

    Idempotency means "same logical operation." A retry that supplies a
    different ``artifact_hash`` is not a retry — it is a different claim
    that happens to share a key. Silently returning the first claim_id
    would let a caller believe their new hash was registered when it was
    not, losing tamper-evidence in the process. Surface the inconsistency.
    """


class IllegalStateTransitionError(MareformaError):
    """Raised when an SQLite state-machine trigger refuses a transition.

    The trigger raises ``mareforma:state:<from>-><to>`` strings via
    ``RAISE(ABORT, ...)``. Python catches the resulting
    ``sqlite3.IntegrityError`` and re-raises this exception with the
    parsed transition so callers can pattern-match on it instead of
    parsing opaque ``CHECK CONSTRAINT FAILED`` messages.
    """


class ChainIntegrityError(MareformaError):
    """Raised when the ``prev_hash`` append-only chain cannot extend.

    The chain hash is computed under ``BEGIN IMMEDIATE`` to serialize
    writers, and the ``prev_hash`` column carries a ``UNIQUE`` index.
    If a second writer races past the lock — or a raw-SQL tamper
    re-uses an existing chain link — the UNIQUE violation surfaces
    here. Treat it as a corruption signal, not a retry.
    """


class LLMValidatorPromotionError(MareformaError):
    """Raised when a validator with ``validator_type='llm'`` attempts
    a promotion past REPLICATED.

    The trust ladder treats human validators as the only path to
    ESTABLISHED. An LLM-typed validator may enroll and may sign
    validation envelopes, but those envelopes cannot promote a claim
    past REPLICATED. To promote, the claim must be co-signed (or
    re-signed) by an enrolled human validator.
    """


class SelfValidationError(MareformaError):
    """Raised when a validator attempts to promote a claim it signed itself.

    Self-validation is the trivial-loop attack: an agent asserts a claim
    under its own key, then promotes that same claim to ESTABLISHED under
    the same key. The trust ladder rests on the principle that promotion
    is an *external* witnessing event. ``validate_claim`` compares the
    signing keyid of the validation envelope with the keyid recorded in
    the claim's ``signature_bundle`` and refuses when they match.
    """


class RestoreError(MareformaError):
    """Raised by :func:`restore` when the rebuild refuses or fails.

    The ``kind`` attribute lets callers pattern-match on the failure
    mode without parsing the message string:

      - ``'graph_not_empty'``        — existing graph.db has claims
      - ``'toml_not_found'``         — claims.toml does not exist
      - ``'toml_malformed'``         — TOML parse error
      - ``'enrollment_unverified'``  — enrollment envelope fails verify
      - ``'claim_unverified'``       — claim signature fails verify
      - ``'mode_inconsistent'``      — signed-mode graph with unsigned claim
      - ``'orphan_signer'``          — claim signed by an unenrolled keyid
    """

    def __init__(self, message: str, *, kind: str) -> None:
        super().__init__(message)
        self.kind = kind


class CycleDetectedError(MareformaError):
    """Raised when an INSERT or UPDATE would create a cycle in ``supports[]``.

    The graph of claim → upstream supports is required to be acyclic.
    Self-loops (a claim that supports itself) and indirect cycles
    introduced by mutating ``supports`` on an unsigned claim are both
    rejected. Signed claims cannot mutate ``supports`` at all (see
    :class:`SignedClaimImmutableError`), so the cycle window is the
    unsigned-edit path.
    """


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def _db_path(root: Path) -> Path:
    return root / ".mareforma" / DB_FILENAME


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

    No in-place schema migrations during v0.3.0 development. Adding a
    column or trigger means updating ``_SCHEMA_SQL`` in place; existing
    dev-branch databases get the schema-validation error and the user
    deletes graph.db (``claims.toml`` is the human-readable backup).
    Versioned migrations become relevant only after a 1.0 release
    establishes a stable schema with real users on it.

    Raises
    ------
    DatabaseError
        On SQLite errors or schema drift (missing columns).
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
            return conn

        # No in-place migrations during v0.3.x. A db whose user_version
        # is neither 0 nor _SCHEMA_VERSION was written by a different
        # build of the dev branch and may carry a partial schema (e.g.
        # a v2-stranded db is missing the retracted-is-terminal trigger
        # that the substrate fix relies on, even though its column set
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

        # Initialised db — validate the schema by exact column-set match.
        # Catching extras as well as missing columns means a partially-migrated
        # or hand-edited claims table fails loudly instead of silently passing
        # through code that assumes _CLAIM_COLUMNS is exhaustive.
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
            # delete — claims.toml may not be a faithful backup for columns
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
            raise DatabaseError(
                f"graph.db schema mismatch ({'; '.join(parts)}). "
                "Delete .mareforma/graph.db to start fresh — "
                "your claims are backed up in claims.toml."
            )
        return conn

    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Could not open database at {path}: {exc}") from exc


# ---------------------------------------------------------------------------
# Append-only hash chain
# ---------------------------------------------------------------------------

def _chain_input_for_claim(
    claim_fields: dict, evidence: dict | None = None,
) -> bytes:
    """Canonical bytes for the chain hash on a single claim row.

    Uses the in-toto Statement v1 canonical bytes — the exact same
    bytes that get signed (after DSSE PAE wrap). Chain integrity and
    signature integrity bind to one authoritative byte sequence.
    EvidenceVector is part of the Statement, so it is part of the
    chain input.
    """
    from mareforma import signing as _signing
    return _signing.canonical_statement(claim_fields, evidence or {})


def _compute_prev_hash(
    conn: sqlite3.Connection,
    claim_fields: dict,
    evidence: dict | None = None,
) -> str:
    """Compute the new ``prev_hash`` value for a claim about to be inserted.

    The new chain link is ``sha256(prev_chain_link || canonical_statement_bytes)``.
    For the genesis row (no prior rows), the prior link is empty bytes.

    MUST be called inside ``BEGIN IMMEDIATE`` — the SELECT-then-INSERT
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

# Pattern for the UUID format we generate via uuid.uuid4(). Strings in
# ``supports[]`` that DON'T match are external references (DOIs etc.)
# and do not participate in cycle checking — they are not graph nodes.
_CLAIM_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

# Walk depth cap for cycle detection. Same value as the validator-chain
# cap; defends against pathologically long planted chains.
_CYCLE_MAX_DEPTH = 1024


def _is_claim_id(value: str) -> bool:
    return bool(_CLAIM_ID_RE.match(value))


def _check_no_cycle(
    conn: sqlite3.Connection,
    new_claim_id: str,
    supports: list[str],
) -> None:
    """Raise :class:`CycleDetectedError` if extending the graph with
    ``new_claim_id → supports`` would create a cycle.

    Algorithm: simple DFS reachability with a visited set. From each
    supports[] entry that looks like a claim_id, walk forward (i.e.
    follow that claim's own supports[]) and check whether we ever
    encounter ``new_claim_id``. If yes, the new edge closes a cycle.

    Why DFS (not Tarjan's SCC): the existing graph is acyclic by
    induction (we reject cycles on every write). A new claim has no
    incoming edges at INSERT time, so the only cycle it can create is
    one that goes ``new → supports → ... → new``. A forward walk from
    each support entry is sufficient. For ``update_claim``, the new
    edge is the changed ``supports[]``; same algorithm applies.

    DOI strings in ``supports[]`` are external references — skipped
    in the walk.
    """
    if not supports:
        return

    visited: set[str] = set()
    # Seed the DFS with the new-claim's supports themselves. If any
    # entry IS new_claim_id, that's a direct self-loop.
    stack: list[tuple[str, int]] = []
    for s in supports:
        if not _is_claim_id(s):
            continue
        if s == new_claim_id:
            raise CycleDetectedError(
                f"Claim {new_claim_id!r} cannot support itself "
                f"(self-loop in supports[])."
            )
        stack.append((s, 1))

    while stack:
        current, depth = stack.pop()
        if current in visited:
            continue
        visited.add(current)
        if depth > _CYCLE_MAX_DEPTH:
            raise CycleDetectedError(
                f"supports[] walk exceeded depth cap of {_CYCLE_MAX_DEPTH} "
                "hops. The graph contains a pathologically long chain — "
                "investigate before relaxing the cap."
            )
        row = conn.execute(
            "SELECT supports_json FROM claims WHERE claim_id = ?",
            (current,),
        ).fetchone()
        if row is None:
            # supports[] referenced a non-existent claim_id. Not a
            # cycle issue — typically a typo or out-of-order insert.
            # Leave the broader validation to the caller.
            continue
        try:
            child_supports = json.loads(row["supports_json"] or "[]")
        except json.JSONDecodeError:
            # Corrupt row — skip rather than crash. Quarantining is
            # the resolver path's responsibility, not the cycle check.
            continue
        for child in child_supports:
            if not _is_claim_id(child):
                continue
            if child == new_claim_id:
                raise CycleDetectedError(
                    f"Inserting/updating {new_claim_id!r} with the given "
                    f"supports[] would create a cycle through {current!r}."
                )
            if child not in visited:
                stack.append((child, depth + 1))


def _state_error_from_integrity(
    exc: sqlite3.IntegrityError,
) -> "MareformaError | None":
    """Translate trigger / UNIQUE violations into mareforma exceptions.

    Returns ``None`` if the IntegrityError is not one of the patterns
    we own — callers should re-raise as ``DatabaseError`` then.
    """
    msg = str(exc)
    if "mareforma:state:" in msg:
        # Extract the suffix after the prefix for callers that want to
        # pattern-match. The full SQLite message looks like:
        #   IntegrityError: mareforma:state:illegal_transition:PRELIMINARY->ESTABLISHED
        marker = "mareforma:state:"
        suffix = msg[msg.index(marker) + len(marker):]
        return IllegalStateTransitionError(f"State transition refused: {suffix}")
    if "idx_claims_prev_hash" in msg or (
        "UNIQUE constraint failed" in msg and "prev_hash" in msg
    ):
        return ChainIntegrityError(
            "prev_hash UNIQUE violation — two writers raced past BEGIN "
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

    Accepts canonical hex digests only — no ``sha256:`` prefix, no
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
    seed: bool = False,
    signer: "object | None" = None,
    rekor_url: str | None = None,
    require_rekor: bool = False,
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

    Raises
    ------
    ValueError
        If classification or status are invalid.
    SigningError
        If ``require_rekor=True`` and the Rekor submission fails.
    """
    if not text or not text.strip():
        raise ValueError("Claim text cannot be empty.")
    if len(text) > _MAX_CLAIM_TEXT_LEN:
        raise ValueError(
            f"Claim text exceeds {_MAX_CLAIM_TEXT_LEN}-char cap "
            f"(got {len(text)}). Split the finding into smaller claims "
            "and link them via supports=[]."
        )
    # Sanitize-on-write strips zero-width / bidi / Goodside-tag-plane
    # codepoints BEFORE the text is signed. Defense in depth: any
    # consumer that reads ``text`` directly (not just ``query_for_llm``)
    # gets a clean string, and the signed payload binds the cleaned
    # form so downstream verifiers see what the LLM will see.
    from mareforma import prompt_safety as _ps
    text = _ps.sanitize_for_llm(text.strip())
    if not text or not text.strip():
        raise ValueError(
            "Claim text became empty after stripping zero-width / control "
            "characters. The input contained no visible content."
        )
    if classification not in VALID_CLASSIFICATIONS:
        raise ValueError(
            f"Unknown classification '{classification}'. "
            f"Use one of: {', '.join(VALID_CLASSIFICATIONS)}"
        )
    validate_status(status)
    artifact_hash = normalize_artifact_hash(artifact_hash)

    # Idempotency check — return existing claim_id if key already present.
    # Replays must agree on artifact_hash: a retry that supplies a different
    # hash is a different claim, not the same op. Silently keeping the first
    # hash would let the caller think their new hash was registered.
    if idempotency_key is not None:
        try:
            row = conn.execute(
                "SELECT claim_id, artifact_hash FROM claims WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row:
                existing_hash = row["artifact_hash"]
                if existing_hash != artifact_hash:
                    raise IdempotencyConflictError(
                        f"idempotency_key={idempotency_key!r} already exists "
                        f"with artifact_hash={existing_hash!r}, but this call "
                        f"supplied {artifact_hash!r}. Use a different "
                        "idempotency_key or omit the conflicting field."
                    )
                return row["claim_id"]
        except sqlite3.OperationalError as exc:
            raise DatabaseError(f"Idempotency check failed: {exc}") from exc

    claim_id = str(uuid.uuid4())
    now = _now()
    supports_json = json.dumps(supports or [])
    contradicts_json = json.dumps(contradicts or [])

    # Cycle / self-loop check on supports[]. DOI entries are external
    # references and not graph nodes — _check_no_cycle filters them
    # out. The walk runs before signing and INSERT so we don't strand
    # half-built state on rejection.
    _check_no_cycle(conn, claim_id, supports or [])

    # Seed-claim bootstrap (P1.7). A seed claim is asserted by an
    # enrolled validator and inserted directly with
    # support_level='ESTABLISHED' + a signed seed envelope. This is
    # the only path that can place a claim at ESTABLISHED without
    # going through REPLICATED + validate(); it exists to bootstrap
    # the chain of trust on a fresh graph (otherwise the
    # ESTABLISHED-upstream rule blocks the first REPLICATED forever).
    seed_envelope_json: str | None = None
    if seed:
        # Seed envelopes sign claim_id + validator_keyid + seeded_at —
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
    # in-toto Statement v1 wrapping claim fields + GRADE EvidenceVector, so
    # any later tamper (text edit, support reattribution, evidence override)
    # breaks verification.
    #
    # In v0.3.0 Phase 1 every claim carries a default-zero EvidenceVector
    # (no quality concerns flagged by the asserter). Future API revisions
    # will let callers supply a populated vector; the envelope and chain
    # already bind whatever vector ends up in the row.
    from mareforma._evidence import EvidenceVector
    evidence_obj = EvidenceVector()
    evidence_dict = evidence_obj.to_dict()
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
            "text": text.strip(),
            "classification": classification,
            "generated_by": generated_by,
            "supports": supports or [],
            "contradicts": contradicts or [],
            "source_name": source_name,
            "artifact_hash": artifact_hash,
            "created_at": now,
        }
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
            )
        )

    # ``transparency_logged`` defaults to 1 (ready). We flip it to 0 only when
    # Rekor is enabled AND we have something to submit — the row then waits
    # for either a successful submission below or a refresh_unsigned() retry.
    rekor_enabled = rekor_url is not None and signer is not None and envelope is not None
    transparency_logged = 0 if rekor_enabled else 1

    # BEGIN IMMEDIATE: serialize the read-latest-chain-link + INSERT so
    # two writers cannot branch the append-only hash chain. Defaults
    # would let them race past the SELECT and both insert with the same
    # prev_hash, splitting the chain silently — the UNIQUE index on
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
                 validation_signature, validator_keyid, validated_at,
                 artifact_hash, prev_hash,
                 ev_risk_of_bias, ev_inconsistency, ev_indirectness,
                 ev_imprecision, ev_pub_bias,
                 evidence_json, statement_cid,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim_id, text, classification, initial_level, idempotency_key,
                status, source_name, generated_by,
                supports_json, contradicts_json, 1 if unresolved else 0,
                signature_bundle, transparency_logged,
                initial_validation_signature, initial_validator_keyid,
                initial_validated_at,
                artifact_hash, prev_hash,
                evidence_obj.risk_of_bias, evidence_obj.inconsistency,
                evidence_obj.indirectness, evidence_obj.imprecision,
                evidence_obj.publication_bias,
                evidence_json, statement_cid,
                now, now,
            ),
        )
        if _own_transaction:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        if _own_transaction:
            conn.rollback()
        translated = _state_error_from_integrity(exc)
        if translated is not None:
            raise translated from exc
        raise DatabaseError(f"Failed to add claim: {exc}") from exc
    except sqlite3.OperationalError as exc:
        if _own_transaction:
            conn.rollback()
        raise DatabaseError(f"Failed to add claim: {exc}") from exc

    # Attempt Rekor submission. On success, augment the envelope with the
    # log entry and flip transparency_logged → 1. On failure, leave the row
    # at transparency_logged=0 — REPLICATED is blocked until refresh_unsigned
    # succeeds.
    if rekor_enabled:
        from mareforma import signing as _signing
        logged, entry = _signing.submit_to_rekor(
            envelope, signer.public_key(), rekor_url=rekor_url,
        )
        if logged and entry is not None:
            augmented = _signing.attach_rekor_entry(envelope, entry)
            new_bundle = json.dumps(augmented, sort_keys=True, separators=(",", ":"))
            try:
                conn.execute(
                    "UPDATE claims SET signature_bundle = ?, "
                    "transparency_logged = 1, updated_at = ? "
                    "WHERE claim_id = ?",
                    (new_bundle, _now(), claim_id),
                )
                conn.commit()
                transparency_logged = 1
            except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
                # Rekor succeeded but the local UPDATE failed. The row stays
                # at transparency_logged=0; refresh_unsigned will re-submit
                # to Rekor and overwrite the bundle then. Warn loudly so
                # operators see the inconsistency.
                import warnings as _warnings
                _warnings.warn(
                    f"Claim {claim_id} was accepted by Rekor but the local "
                    f"UPDATE failed ({exc}). transparency_logged remains 0; "
                    "run EpistemicGraph.refresh_unsigned() to reconcile.",
                    stacklevel=2,
                )
        elif require_rekor:
            raise _signing.SigningError(
                f"Rekor submission to {rekor_url} failed and require_rekor=True. "
                "Claim was persisted with transparency_logged=0; call "
                "EpistemicGraph.refresh_unsigned() to retry."
            )

    # Check whether this claim triggers REPLICATED status on shared upstreams.
    # Unresolved DOIs OR pending transparency-log inclusion block eligibility.
    if not unresolved and transparency_logged == 1:
        _maybe_update_replicated(
            conn, claim_id, supports or [], generated_by, artifact_hash,
        )

    _backup_claims_toml(conn, root)
    return claim_id


def _maybe_update_replicated_unlocked(
    conn: sqlite3.Connection,
    new_claim_id: str,
    supports: list[str],
    generated_by: str,
    artifact_hash: str | None = None,
) -> None:
    """REPLICATED-detection SQL without a commit — caller controls the txn.

    Used by ``mark_claim_resolved`` so the unresolved-flag clear and the
    REPLICATED promotion land in the same SQLite transaction.

    Artifact-hash gating
    --------------------
    When BOTH the new claim and a candidate peer carry a non-NULL
    ``artifact_hash``, the hashes must match for the peer to count
    toward convergence. When either side is NULL (the back-compat
    case), behaviour falls back to identity-only REPLICATED: the
    hash signal is opt-in, not retroactive.

    ESTABLISHED-upstream requirement (P1.7)
    ---------------------------------------
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
        "SELECT status FROM claims WHERE claim_id = ?", (new_claim_id,),
    ).fetchone()
    if new_status_row is None or new_status_row["status"] != "open":
        return

    # P1.7: the NEW claim's supports[] must include at least one
    # ESTABLISHED claim. The SQL clause below applies the same rule
    # to candidate peers (their supports[] is checked there). If the
    # new claim doesn't satisfy the rule, no promotion fires — saves
    # an unnecessary SQL roundtrip and makes the semantics explicit.
    sup_placeholders = ",".join("?" * len(supports))
    # An ESTABLISHED upstream that is retracted or contested is
    # editorially tainted and must not anchor REPLICATED. Without the
    # status='open' filter, a hand-edited claims.toml could plant a
    # born-retracted ESTABLISHED seed (the seed envelope binds claim_id
    # + validator_keyid + seeded_at, NOT status), then have downstream
    # peers ride it into REPLICATED. The substrate gate closes that
    # injection vector at the canonical layer.
    has_established_upstream = conn.execute(
        f"SELECT 1 FROM claims WHERE claim_id IN ({sup_placeholders}) "
        f"AND support_level = 'ESTABLISHED' AND status = 'open' LIMIT 1",
        supports,
    ).fetchone()
    if has_established_upstream is None:
        return

    placeholders = ",".join("?" * len(supports))
    # status='open' filter: a contested or retracted peer is editorially
    # tainted and must not participate in REPLICATED convergence. Without
    # this, an adversary could plant a born-retracted claim and ride an
    # honest peer's INSERT into REPLICATED (and from there, via validate(),
    # into ESTABLISHED — usable as a fake upstream for further chains).
    rows = conn.execute(
        f"""
        SELECT DISTINCT c.claim_id, c.generated_by
        FROM claims c, json_each(c.supports_json) j
        WHERE j.value IN ({placeholders})
          AND c.claim_id != ?
          AND c.generated_by != ?
          AND c.support_level != 'ESTABLISHED'
          AND c.status = 'open'
          AND c.unresolved = 0
          AND c.transparency_logged = 1
          AND (
              c.artifact_hash IS NULL
              OR ? IS NULL
              OR c.artifact_hash = ?
          )
          AND EXISTS (
              SELECT 1
              FROM claims sup, json_each(c.supports_json) j2
              WHERE sup.claim_id = j2.value
                AND sup.support_level = 'ESTABLISHED'
                AND sup.status = 'open'
          )
        """,
        (*supports, new_claim_id, generated_by, artifact_hash, artifact_hash),
    ).fetchall()

    if not rows:
        return

    peer_ids = [r["claim_id"] for r in rows] + [new_claim_id]
    peer_placeholders = ",".join("?" * len(peer_ids))
    # status='open' folded into the UPDATE's WHERE closes the TOCTOU
    # window between the SELECT above and this UPDATE: another writer
    # could flip a peer (or the new row) to contested/retracted between
    # the two statements. The row-level lock SQLite acquires during
    # UPDATE is the actual gate; the pre-SELECT is a cheap fast-path.
    conn.execute(
        f"UPDATE claims SET support_level = 'REPLICATED', updated_at = ? "
        f"WHERE claim_id IN ({peer_placeholders}) AND status = 'open'",
        (_now(), *peer_ids),
    )


def _maybe_update_replicated(
    conn: sqlite3.Connection,
    new_claim_id: str,
    supports: list[str],
    generated_by: str,
    artifact_hash: str | None = None,
) -> None:
    """Promote claims to REPLICATED when convergence is detected.

    Convergence: ≥2 claims share the same upstream claim_id in their
    supports[] and have different generated_by values. Uses json_each()
    for correct JSON array element extraction (no fragile LIKE).

    Called immediately after a successful INSERT in add_claim().
    Failures are swallowed — convergence detection must not crash writes.
    """
    try:
        _maybe_update_replicated_unlocked(
            conn, new_claim_id, supports, generated_by, artifact_hash,
        )
        conn.commit()
    except (sqlite3.OperationalError, sqlite3.IntegrityError):
        # Convergence detection is best-effort — never crash a write.
        # A trigger-raised IntegrityError here would mean a state transition
        # we asked for is illegal (e.g. ESTABLISHED peer being downgraded);
        # the underlying invariant should remain — log the warning.
        pass


def _extract_validation_signer_keyid(validation_signature: str) -> str | None:
    """Return the signing keyid from a validation envelope, or None if the
    envelope is malformed.

    The envelope's ``signatures[0].keyid`` is the authoritative signer.
    Malformed envelopes return None — the substrate gates short-circuit
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
    this gate — that case is the enrollment check in
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


def _refuse_self_validation(
    claim_id: str,
    claim_signature_bundle: str | None,
    validator_keyid: str,
) -> None:
    """Raise :class:`SelfValidationError` if the claim's signing keyid
    equals the validator's keyid.

    Unsigned claims (``signature_bundle IS NULL``) carry no signer
    identity to compare against and pass this gate. A malformed bundle
    is treated as absent (the substrate cannot decide self-equality
    against a corrupted envelope).
    """
    if claim_signature_bundle is None:
        return
    try:
        bundle = json.loads(claim_signature_bundle)
        claim_signer_keyid = bundle["signatures"][0]["keyid"]
    except (json.JSONDecodeError, KeyError, IndexError, TypeError):
        return
    if claim_signer_keyid == validator_keyid:
        raise SelfValidationError(
            f"Validator {validator_keyid[:12]}… signed claim "
            f"'{claim_id}' itself; self-promotion is refused. "
            "Promotion requires an external witnessing validator. "
            "Have a different enrolled key call graph.validate(...)."
        )


def validate_claim(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
    *,
    validated_by: str | None = None,
    validation_signature: str | None = None,
    validated_at: str | None = None,
) -> None:
    """Promote a REPLICATED claim to ESTABLISHED (human validation).

    Parameters
    ----------
    validation_signature:
        Optional JSON-encoded DSSE-style envelope binding (claim_id,
        validator_keyid, validated_at). Produced by
        :func:`mareforma.signing.sign_validation` and stored verbatim
        on the row so the validation event itself is independently
        verifiable (tampering with ``validated_by``/``validated_at``
        post-hoc is detectable).
    validated_at:
        Optional ISO 8601 UTC timestamp to write to the row. When the
        caller has already signed a validation envelope binding a
        timestamp, the SAME timestamp must be threaded through here so
        the envelope's ``validated_at`` matches the row's
        ``validated_at`` byte-for-byte. If ``None``, a fresh timestamp
        is generated — appropriate only for the legacy unsigned path.

    Substrate gates
    ---------------
    When ``validation_signature`` is supplied, two substrate-level checks
    fire before the row is updated. Both decode the envelope's payload
    and consult the ``validators`` table directly — wrapping code in
    ``_graph.validate`` cannot bypass them:

    1. The signing validator's ``validator_type`` must be ``'human'``.
       An ``'llm'``-typed validator can sign a validation envelope but
       cannot promote past REPLICATED (raises :class:`LLMValidatorPromotionError`).
    2. The validator's keyid must NOT match the claim's
       ``signature_bundle`` signing keyid. Self-validation is the
       trivial-loop attack (raises :class:`SelfValidationError`).

    Raises
    ------
    ClaimNotFoundError
        If no claim with claim_id exists.
    ValueError
        If the claim's support_level is not 'REPLICATED', or its
        status is not 'open' (contested/retracted claims are editorially
        tainted and must not be promoted; revisit the editorial flag via
        update_claim before validating).
    LLMValidatorPromotionError
        If the validation envelope is signed by an LLM-typed validator.
    SelfValidationError
        If the validation envelope's signing keyid equals the claim's
        ``signature_bundle`` signing keyid.
    """
    row = conn.execute(
        "SELECT support_level, status, signature_bundle "
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

    # Substrate gates: parse the validation envelope, look up the
    # signer's validator_type, refuse LLM signers and self-validation.
    # Skipped on the legacy unsigned path (validation_signature=None) —
    # there is no envelope to inspect, and the legacy path is being
    # phased out by mareforma.open(require_signed=True) downstream.
    validator_keyid: str | None = None
    if validation_signature is not None:
        validator_keyid = _extract_validation_signer_keyid(validation_signature)
        if validator_keyid is not None:
            _refuse_llm_validator(conn, validator_keyid)
            _refuse_self_validation(
                claim_id, row["signature_bundle"], validator_keyid,
            )

    now = validated_at if validated_at is not None else _now()
    try:
        # COALESCE on validator_keyid: a legacy unsigned re-validate
        # (validation_signature=None) must NOT wipe a previously-set
        # validator_keyid. The state-check trigger permits
        # ESTABLISHED → ESTABLISHED, so a second call would otherwise
        # NULL the column and tank the validator's reputation count.
        conn.execute(
            """
            UPDATE claims
            SET support_level = 'ESTABLISHED',
                validated_by = ?,
                validated_at = ?,
                validation_signature = ?,
                validator_keyid = COALESCE(?, validator_keyid),
                updated_at   = ?
            WHERE claim_id = ?
            """,
            (validated_by, now, validation_signature, validator_keyid,
             now, claim_id),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        translated = _state_error_from_integrity(exc)
        if translated is not None:
            raise translated from exc
        raise DatabaseError(f"Failed to validate claim '{claim_id}': {exc}") from exc
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to validate claim '{claim_id}': {exc}") from exc
    _backup_claims_toml(conn, root)


def list_unresolved_claims(conn: sqlite3.Connection) -> list[dict]:
    """Return all claims currently marked unresolved=True."""
    rows = conn.execute(
        f"SELECT {_CLAIM_SELECT} FROM claims WHERE unresolved = 1 ORDER BY created_at"
    ).fetchall()
    return [dict(r) for r in rows]


def list_unlogged_claims(conn: sqlite3.Connection) -> list[dict]:
    """Return signed claims still awaiting Rekor inclusion.

    A claim is "unlogged" when ``signature_bundle`` is non-NULL but
    ``transparency_logged`` is 0. Unsigned claims are excluded — they have
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
) -> None:
    """Mark a claim as transparency-log included and update its bundle.

    The bundle is rewritten with the Rekor entry attached (uuid + logIndex +
    integratedTime). The flag-flip and REPLICATED re-evaluation happen in a
    single transaction so a crash between them cannot strand a claim at
    PRELIMINARY despite ``transparency_logged=1``.

    Verification
    ------------
    Before writing, the supplied bundle is decoded and its payload's
    ``claim_id`` is checked against the row's ``claim_id``. A buggy caller
    that mixes up claim ids cannot silently write Alice's bundle onto
    Bob's row.

    Raises
    ------
    ClaimNotFoundError
        If no claim with claim_id exists.
    DatabaseError
        If the supplied bundle is malformed or its payload's claim_id does
        not match.
    """
    row = conn.execute(
        "SELECT supports_json, generated_by, unresolved, artifact_hash "
        "FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")

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
            # lock error during the REPLICATED check must not roll back
            # the flag flip the operator just committed.
            if not unresolved:
                try:
                    _maybe_update_replicated_unlocked(
                        conn, claim_id, supports, generated_by, artifact_hash,
                    )
                except sqlite3.OperationalError:
                    pass
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        raise DatabaseError(f"Failed to mark claim logged: {exc}") from exc

    _backup_claims_toml(conn, root)


def mark_claim_resolved(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
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
            # flag-clear (which is the actual user intent).
            try:
                _maybe_update_replicated_unlocked(
                    conn, claim_id, supports, generated_by, artifact_hash,
                )
            except sqlite3.OperationalError:
                pass
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
) -> None:
    """Update fields on an existing claim.

    Signed claims are append-only across the signed surface. If the row
    carries a non-NULL ``signature_bundle``, this call refuses to mutate
    ``text`` / ``supports`` / ``contradicts`` — those fields are part of
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

    # Refuse signed-surface mutations on signed claims. text/supports/
    # contradicts are the only signed-surface fields currently exposed by
    # update_claim's parameter list.
    if existing.get("signature_bundle") is not None:
        signed_field_changes: list[str] = []
        if text is not None and text.strip() != existing.get("text"):
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

    # Re-resolve DOIs only when supports/contradicts actually change. Stale
    # `unresolved` flags would let a claim with a newly-added fake DOI reach
    # REPLICATED, or pin a claim as unresolved after its bad DOI is removed.
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
    # unsigned claim — the cycle-introduction window P1.6 closes.
    if supports_changed:
        new_supports_list = json.loads(new_supports_json)
        _check_no_cycle(conn, claim_id, new_supports_list)

    if supports_changed or contradicts_changed:
        from mareforma import doi_resolver as _doi
        all_refs = json.loads(new_supports_json) + json.loads(new_contradicts_json)
        dois = _doi.extract_dois(all_refs)
        if dois:
            results = _doi.resolve_dois_with_cache(conn, dois)
            new_unresolved = 0 if all(results.values()) else 1
        else:
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
            if needs_replicated_check:
                try:
                    new_supports = json.loads(new_supports_json)
                    _maybe_update_replicated_unlocked(
                        conn, claim_id, new_supports, existing["generated_by"],
                        existing.get("artifact_hash"),
                    )
                except sqlite3.OperationalError:
                    # Convergence detection is best-effort — never crash an update.
                    pass
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
    include_unverified: bool = False,
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
    include_unverified:
        When False (default), PRELIMINARY claims whose ``signature_bundle``
        is unsigned or signed by a keyid not present in the ``validators``
        table are excluded — the spec.md #96 default. REPLICATED and
        ESTABLISHED rows already require an enrolled validator chain and
        are never filtered by this flag. Pass ``True`` to surface
        unverified preliminary claims (e.g. inspection of pending work).

    Each returned dict carries the standard claim columns plus two
    reputation projections computed at query time:

      - ``validator_reputation`` (int): for ESTABLISHED rows, the number
        of ESTABLISHED claims signed by the same validator (≥ 1). For
        other rows, ``0``.
      - ``generator_enrolled`` (bool): True iff the claim's
        ``signature_bundle`` is signed by an enrolled validator. False
        for unsigned claims and for signatures by unenrolled keys.
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

    # When include_unverified is False, the substrate-filter runs in
    # Python after the SQL fetch. A flat `LIMIT N` could return zero
    # results when the top-N rows are all unverified PRELIMINARY,
    # silently under-returning. Pull rows in batches and keep going
    # until either we have `limit` survivors or the table is exhausted.
    # When include_unverified is True there is no filter, so one fetch
    # at the exact limit is enough.
    reputation = _compute_validator_reputation(conn)
    enrolled_keyids = _enrolled_validator_keyids(conn)

    base_sql = (
        f"SELECT {_CLAIM_SELECT} FROM claims {where} "
        f"ORDER BY CASE support_level "
        f"WHEN 'ESTABLISHED' THEN 3 WHEN 'REPLICATED' THEN 2 ELSE 1 END DESC, "
        f"created_at DESC LIMIT ? OFFSET ?"
    )

    results: list[dict] = []
    offset = 0
    # Fetch in batches sized to the caller's limit. For verified-heavy
    # projects the first batch is usually enough; for projects with
    # heavy unverified PRELIMINARY traffic the loop keeps pulling
    # until it has `limit` survivors or hits the end.
    batch_size = max(limit, 1)
    try:
        while len(results) < limit:
            rows = conn.execute(
                base_sql, params + [batch_size, offset],
            ).fetchall()
            if not rows:
                break
            offset += len(rows)
            for row in rows:
                d = dict(row)
                gen_keyid = _extract_signature_bundle_keyid(
                    d.get("signature_bundle"),
                )
                d["generator_enrolled"] = (
                    gen_keyid is not None and gen_keyid in enrolled_keyids
                )
                validator_kid = d.get("validator_keyid")
                d["validator_reputation"] = (
                    reputation.get(validator_kid, 0)
                    if validator_kid else 0
                )
                if not include_unverified:
                    if (d["support_level"] == "PRELIMINARY"
                            and not d["generator_enrolled"]):
                        continue
                results.append(d)
                if len(results) >= limit:
                    break
            if include_unverified:
                break  # one batch suffices; no filter dropping rows
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to query claims: {exc}") from exc
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

    Membership only — does NOT walk the enrollment chain. The chain
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
    from the dict (caller defaults to 0). Derived state — recomputed
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
    leading-``*`` form is not valid syntax anyway — but a user who
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
) -> list[dict]:
    """FTS5-ranked search over claim text.

    Returns claim dicts ordered by FTS5 rank (best match first). Each
    dict carries the same projection as :func:`query_claims`:
    ``validator_reputation`` and ``generator_enrolled`` are attached
    per row, and the ``include_unverified`` filter applies identically.

    The ``query`` string is passed through to SQLite's FTS5 MATCH
    operator. FTS5 syntax — phrase matching with double quotes, prefix
    search with trailing ``*``, ``AND``/``OR``/``NOT`` operators, and
    parentheses — works as documented in SQLite. Pure-wildcard queries
    are refused (see :func:`_validate_fts5_query`).
    """
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

    if min_support is not None:
        tiers = _SUPPORT_LEVEL_TIERS[min_support]
        placeholders = ",".join("?" * len(tiers))
        conditions.append(f"c.support_level IN ({placeholders})")
        params.extend(tiers)
    if classification is not None:
        conditions.append("c.classification = ?")
        params.append(classification)

    where = " AND ".join(conditions)
    select_cols = ", ".join(f"c.{col}" for col in _CLAIM_COLUMNS)
    base_sql = (
        f"SELECT {select_cols} FROM claims_fts f "
        f"JOIN claims c ON c.claim_id = f.claim_id "
        f"WHERE {where} "
        f"ORDER BY rank LIMIT ? OFFSET ?"
    )

    reputation = _compute_validator_reputation(conn)
    enrolled_keyids = _enrolled_validator_keyids(conn)

    # Same batched-pull pattern as query_claims — keep pulling until
    # we have `limit` survivors of the include_unverified filter, or
    # the FTS5 match-set is exhausted. include_unverified=True needs
    # no filter, so one batch covers it.
    results: list[dict] = []
    offset = 0
    batch_size = max(limit, 1)
    try:
        while len(results) < limit:
            rows = conn.execute(
                base_sql, params + [batch_size, offset],
            ).fetchall()
            if not rows:
                break
            offset += len(rows)
            for row in rows:
                d = dict(row)
                gen_keyid = _extract_signature_bundle_keyid(
                    d.get("signature_bundle"),
                )
                d["generator_enrolled"] = (
                    gen_keyid is not None and gen_keyid in enrolled_keyids
                )
                validator_kid = d.get("validator_keyid")
                d["validator_reputation"] = (
                    reputation.get(validator_kid, 0)
                    if validator_kid else 0
                )
                if not include_unverified:
                    if (d["support_level"] == "PRELIMINARY"
                            and not d["generator_enrolled"]):
                        continue
                results.append(d)
                if len(results) >= limit:
                    break
            if include_unverified:
                break
    except sqlite3.OperationalError as exc:
        # FTS5 raises OperationalError on malformed MATCH syntax.
        # Wrap so callers don't have to import sqlite3 to pattern-match.
        msg = str(exc)
        if "fts5" in msg or "syntax error" in msg:
            raise ValueError(
                f"Search query {query!r} is not valid FTS5 syntax: {msg}"
            ) from exc
        raise DatabaseError(f"Failed to search claims: {exc}") from exc
    return results


def restore(
    project_root: Path | str,
    *,
    claims_toml: Path | str | None = None,
) -> dict:
    """Rebuild a fresh graph.db from claims.toml.

    Reverse of :func:`_backup_claims_toml`. Intended for catastrophic-
    loss recovery: ``graph.db`` is missing or corrupt, the operator
    has a recent ``claims.toml``, the project must be reconstructable.

    The rebuild is **fresh-only**. ``restore`` refuses to run if
    ``.mareforma/graph.db`` already contains claims — merge semantics
    are out of scope for v0.3.0 (status drift, supports[] divergence,
    and validator chain conflicts have no clean answers). Wipe
    ``graph.db`` first if you really mean to overwrite.

    Signature verification is fail-all-or-nothing. Every enrollment
    envelope is verified against its parent key; every claim
    ``signature_bundle`` is verified against the enrolled signer key;
    every ``validation_signature`` is verified against its signer key.
    The first failure rolls back the entire transaction — the project
    stays in its pre-restore state.

    Parameters
    ----------
    project_root:
        Project directory. ``graph.db`` is reconstructed under
        ``<project_root>/.mareforma/``.
    claims_toml:
        Path to the source TOML. Defaults to
        ``<project_root>/claims.toml``.

    Returns
    -------
    dict
        ``{"validators_restored": N, "claims_restored": M}``.

    Raises
    ------
    RestoreError
        With a ``.kind`` field. See :class:`RestoreError`.
    """
    try:
        import tomli
    except ImportError as exc:  # pragma: no cover
        raise RestoreError(
            "tomli is required for restore() (it is a hard dependency "
            "of mareforma; re-install the package).",
            kind="toml_malformed",
        ) from exc
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    root = Path(project_root)
    toml_path = (
        Path(claims_toml) if claims_toml is not None else root / "claims.toml"
    )
    if not toml_path.exists():
        raise RestoreError(
            f"claims.toml not found at {toml_path}",
            kind="toml_not_found",
        )

    try:
        data = tomli.loads(toml_path.read_text(encoding="utf-8"))
    except tomli.TOMLDecodeError as exc:
        raise RestoreError(
            f"claims.toml at {toml_path} is malformed: {exc}",
            kind="toml_malformed",
        ) from exc

    validators_section: dict = data.get("validators", {}) or {}
    claims_section: dict = data.get("claims", {}) or {}

    conn = open_db(root)
    try:
        signed_mode = bool(validators_section)

        # Order validators by enrolled_at so the root (earliest) lands
        # first and chain-walk parent lookups always succeed in-table.
        ordered_validators = sorted(
            validators_section.items(),
            key=lambda kv: kv[1].get("enrolled_at", ""),
        )

        # BEGIN IMMEDIATE first, THEN re-check emptiness. The write lock
        # closes the window between "check" and "act" — a concurrent
        # writer cannot slip a row in between the SELECT and the
        # restore INSERTs.
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT COUNT(*) AS n FROM claims"
            ).fetchone()
            if existing["n"] > 0:
                raise RestoreError(
                    f"graph.db at {root}/.mareforma/graph.db already has "
                    f"{existing['n']} claim(s). restore() refuses to merge — "
                    "wipe graph.db first, or use a fresh project root.",
                    kind="graph_not_empty",
                )
            for keyid, v in ordered_validators:
                ctx_v = f"Validator {keyid[:12]}…"
                row = {
                    "keyid": keyid,
                    "pubkey_pem": _required_field(v, "pubkey_pem", ctx_v),
                    "identity": _required_field(v, "identity", ctx_v),
                    "validator_type": _required_field(
                        v, "validator_type", ctx_v,
                    ),
                    "enrolled_at": _required_field(v, "enrolled_at", ctx_v),
                    "enrolled_by_keyid": _required_field(
                        v, "enrolled_by_keyid", ctx_v,
                    ),
                    "enrollment_envelope": _required_field(
                        v, "enrollment_envelope", ctx_v,
                    ),
                }
                if row["enrolled_by_keyid"] == keyid:
                    parent_pem_b64 = row["pubkey_pem"]
                else:
                    parent_v = validators_section.get(row["enrolled_by_keyid"])
                    if parent_v is None:
                        raise RestoreError(
                            f"Validator {keyid[:12]}… claims to be enrolled "
                            f"by {row['enrolled_by_keyid'][:12]}… but that "
                            "parent is missing from claims.toml.",
                            kind="enrollment_unverified",
                        )
                    parent_pem_b64 = _required_field(
                        parent_v, "pubkey_pem",
                        f"Parent validator {row['enrolled_by_keyid'][:12]}…",
                    )
                try:
                    parent_pem = base64.standard_b64decode(parent_pem_b64)
                except (ValueError, TypeError) as exc:
                    raise RestoreError(
                        f"Parent pubkey_pem for validator "
                        f"{keyid[:12]}… is not valid base64.",
                        kind="enrollment_unverified",
                    ) from exc
                if not _validators.verify_enrollment(row, parent_pem):
                    raise RestoreError(
                        f"Enrollment envelope for validator "
                        f"{keyid[:12]}… failed verification.",
                        kind="enrollment_unverified",
                    )
                try:
                    conn.execute(
                        "INSERT INTO validators "
                        "(keyid, pubkey_pem, identity, validator_type, "
                        " enrolled_at, enrolled_by_keyid, "
                        " enrollment_envelope) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            keyid, row["pubkey_pem"], row["identity"],
                            row["validator_type"], row["enrolled_at"],
                            row["enrolled_by_keyid"],
                            row["enrollment_envelope"],
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    # Duplicate keyid PK, bad validator_type CHECK, or any
                    # other validator-table integrity violation. Translate
                    # to RestoreError so callers honour the documented
                    # contract.
                    raise RestoreError(
                        f"Validator {keyid[:12]}… could not be restored: "
                        f"{exc}",
                        kind="enrollment_unverified",
                    ) from exc

            # Order claims by created_at so prev_hash reconstruction
            # matches the original chain. SHA256 is deterministic — same
            # inputs in the same order produce the same chain.
            ordered_claims = sorted(
                claims_section.items(),
                key=lambda kv: kv[1].get("created_at", ""),
            )

            for claim_id, c in ordered_claims:
                ctx_c = f"Claim {claim_id}"
                # Pull required fields up-front via the helper so any
                # missing key surfaces as RestoreError(kind="toml_malformed")
                # instead of a bare KeyError past the contract.
                c_text = _required_field(c, "text", ctx_c)
                c_classification = _required_field(c, "classification", ctx_c)
                c_generated_by = _required_field(c, "generated_by", ctx_c)
                c_created_at = _required_field(c, "created_at", ctx_c)
                c_updated_at = _required_field(c, "updated_at", ctx_c)
                c_status = _required_field(c, "status", ctx_c)
                target_level = _required_field(c, "support_level", ctx_c)
                _verify_claim_signatures_on_restore(
                    claim_id, c, validators_section, signed_mode,
                    _signing,
                )
                # Reconstruct supports/contradicts JSON.
                supports_list = c.get("supports", []) or []
                contradicts_list = c.get("contradicts", []) or []
                # EvidenceVector round-trip. The TOML carries the
                # canonical JSON; we re-derive ev_* + chain_input from
                # it so the chain_hash matches the original.
                evidence_json_str = c.get("evidence_json") or "{}"
                try:
                    evidence_dict = json.loads(evidence_json_str)
                except (ValueError, TypeError):
                    evidence_dict = {}
                chain_fields = {
                    "claim_id": claim_id,
                    "text": c_text,
                    "classification": c_classification,
                    "generated_by": c_generated_by,
                    "supports": supports_list,
                    "contradicts": contradicts_list,
                    "source_name": c.get("source_name"),
                    "artifact_hash": c.get("artifact_hash"),
                    "created_at": c_created_at,
                }
                prev_hash = _compute_prev_hash(
                    conn, chain_fields, evidence_dict,
                )
                val_sig = c.get("validation_signature")
                validator_keyid = (
                    _extract_validation_signer_keyid(val_sig)
                    if val_sig else None
                )
                # The INSERT trigger only accepts PRELIMINARY or
                # ESTABLISHED as initial values — REPLICATED is reached
                # via the convergence detection path inside add_claim,
                # never as a born state. Restore inserts REPLICATED rows
                # as PRELIMINARY first, then UPDATEs into REPLICATED.
                # The UPDATE trigger accepts PRELIMINARY → REPLICATED.
                insert_level = (
                    "PRELIMINARY" if target_level == "REPLICATED"
                    else target_level
                )
                # ESTABLISHED rows born here carry validation_signature
                # (the CHECK constraint and the INSERT trigger both
                # require it). PRELIMINARY-during-promotion rows must
                # NOT carry validated_by / validated_at — the INSERT
                # trigger refuses that combination. We hold those
                # back to the UPDATE phase below for REPLICATED.
                insert_validated_by = (
                    c.get("validated_by") if insert_level == "ESTABLISHED"
                    else None
                )
                insert_validated_at = (
                    c.get("validated_at") if insert_level == "ESTABLISHED"
                    else None
                )
                insert_validation_signature = (
                    val_sig if insert_level == "ESTABLISHED" else None
                )
                insert_validator_keyid = (
                    validator_keyid if insert_level == "ESTABLISHED"
                    else None
                )
                # Denormalize ev_* from the canonical evidence_dict so
                # the row's CHECK constraints + the evidence_json blob
                # stay aligned. statement_cid is rebuilt from the same
                # chain_fields + evidence_dict and serves as restore's
                # adversarial anchor — any TOML tamper of an ev_* field
                # produces a different statement_cid here than the one
                # the original signing path computed.
                from mareforma import _statement as _stmt_mod
                statement_cid_str = _stmt_mod.statement_cid(
                    _stmt_mod.build_statement(
                        claim_id=claim_id,
                        text=c_text,
                        classification=c_classification,
                        generated_by=c_generated_by,
                        supports=supports_list,
                        contradicts=contradicts_list,
                        source_name=c.get("source_name"),
                        artifact_hash=c.get("artifact_hash"),
                        created_at=c_created_at,
                        evidence=evidence_dict,
                    )
                ) if c.get("signature_bundle") else None
                try:
                    conn.execute(
                        """
                        INSERT INTO claims
                            (claim_id, text, classification, support_level,
                             idempotency_key, validated_by, validated_at,
                             status, source_name, generated_by,
                             supports_json, contradicts_json,
                             comparison_summary, unresolved,
                             signature_bundle, transparency_logged,
                             validation_signature, validator_keyid,
                             artifact_hash, prev_hash,
                             ev_risk_of_bias, ev_inconsistency,
                             ev_indirectness, ev_imprecision, ev_pub_bias,
                             evidence_json, statement_cid, t_invalid,
                             created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                ?, ?, ?, ?, ?)
                        """,
                        (
                            claim_id, c_text, c_classification,
                            insert_level,
                            None,  # idempotency_key — TOML doesn't carry it
                            insert_validated_by, insert_validated_at,
                            c_status, c.get("source_name"),
                            c_generated_by,
                            json.dumps(supports_list, sort_keys=True,
                                       separators=(",", ":")),
                            json.dumps(contradicts_list, sort_keys=True,
                                       separators=(",", ":")),
                            c.get("comparison_summary") or "",
                            1 if c.get("unresolved") else 0,
                            c.get("signature_bundle"),
                            0 if c.get("transparency_logged") is False
                            else 1,
                            insert_validation_signature,
                            insert_validator_keyid,
                            c.get("artifact_hash"), prev_hash,
                            int(evidence_dict.get("risk_of_bias", 0) or 0),
                            int(evidence_dict.get("inconsistency", 0) or 0),
                            int(evidence_dict.get("indirectness", 0) or 0),
                            int(evidence_dict.get("imprecision", 0) or 0),
                            int(evidence_dict.get("publication_bias", 0) or 0),
                            evidence_json_str,
                            statement_cid_str,
                            c.get("t_invalid"),
                            c_created_at, c_updated_at,
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    # Trigger refusals (illegal initial support_level,
                    # ESTABLISHED without validation_signature) and CHECK
                    # violations (bad classification / support_level /
                    # status enum, duplicate prev_hash) all surface here.
                    # Translate to RestoreError so callers honour the
                    # documented contract.
                    raise RestoreError(
                        f"Claim {claim_id} could not be restored: {exc}",
                        kind="claim_unverified",
                    ) from exc
                if target_level == "REPLICATED":
                    # PRELIMINARY → REPLICATED — the UPDATE trigger
                    # accepts the transition. No validation_signature
                    # required on REPLICATED rows. Wrap the UPDATE so
                    # any trigger refusal surfaces as RestoreError.
                    try:
                        conn.execute(
                            "UPDATE claims SET support_level = 'REPLICATED' "
                            "WHERE claim_id = ?",
                            (claim_id,),
                        )
                    except sqlite3.IntegrityError as exc:
                        raise RestoreError(
                            f"Claim {claim_id} promote-to-REPLICATED "
                            f"refused: {exc}",
                            kind="claim_unverified",
                        ) from exc

            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            raise

        return {
            "validators_restored": len(ordered_validators),
            "claims_restored": len(ordered_claims),
        }
    finally:
        conn.close()


def _required_field(d: dict, key: str, context: str) -> Any:
    """Look up a required field on a TOML-deserialized row.

    Raises :class:`RestoreError` with ``kind='toml_malformed'`` when the
    field is missing. Direct ``d[key]`` would raise ``KeyError`` past
    the documented ``RestoreError`` contract.
    """
    if key not in d:
        raise RestoreError(
            f"{context}: required field {key!r} is missing from "
            "claims.toml.",
            kind="toml_malformed",
        )
    return d[key]


def _verify_claim_signatures_on_restore(
    claim_id: str,
    c: dict,
    validators_section: dict,
    signed_mode: bool,
    _signing,
) -> None:
    """Verify a single claim's signatures during restore.

    Raises :class:`RestoreError` with the appropriate ``kind`` on
    any of: orphan signer keyid, signature_bundle verification
    failure, validation_signature verification failure, or
    mixed-mode (signed-mode graph with an unsigned claim that
    isn't a benign PRELIMINARY-from-pre-signing-era row).
    """
    sig_bundle_json = c.get("signature_bundle")
    if sig_bundle_json:
        try:
            bundle = json.loads(sig_bundle_json)
            bundle_keyid = bundle["signatures"][0]["keyid"]
        except (json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
            raise RestoreError(
                f"Claim {claim_id} signature_bundle is malformed.",
                kind="claim_unverified",
            ) from exc
        if bundle_keyid not in validators_section:
            raise RestoreError(
                f"Claim {claim_id} is signed by keyid "
                f"{bundle_keyid[:12]}… which is not in the validators "
                "section. Restore refuses orphan signers.",
                kind="orphan_signer",
            )
        try:
            signer_pem = base64.standard_b64decode(
                validators_section[bundle_keyid]["pubkey_pem"],
            )
            signer_pub = _signing.public_key_from_pem(signer_pem)
        except (ValueError, TypeError, _signing.SigningError) as exc:
            raise RestoreError(
                f"Signer pubkey for keyid {bundle_keyid[:12]}… is not "
                "a valid PEM.",
                kind="claim_unverified",
            ) from exc
        # verify_envelope returns False on signature mismatch but raises
        # InvalidEnvelopeError on payloadType/structural mismatch (e.g.
        # a tampered TOML that swaps a validation envelope into the
        # claim-bundle slot). Wrap both into the documented RestoreError
        # contract so callers don't have to catch SigningError too.
        try:
            envelope_ok = _signing.verify_envelope(bundle, signer_pub)
        except _signing.InvalidEnvelopeError as exc:
            raise RestoreError(
                f"Claim {claim_id} signature_bundle is structurally "
                f"invalid: {exc}",
                kind="claim_unverified",
            ) from exc
        if not envelope_ok:
            raise RestoreError(
                f"Claim {claim_id} signature_bundle failed verification.",
                kind="claim_unverified",
            )
        # Defense in depth: every signed-predicate field must equal the
        # claim's restored field. Tampering with the row but reusing a
        # legitimate envelope is caught here. Statement v1 puts these
        # fields one level deeper under ``predicate``.
        try:
            predicate = _signing.claim_predicate_from_envelope(bundle)
        except _signing.InvalidEnvelopeError as exc:
            raise RestoreError(
                f"Claim {claim_id} envelope payload is unparseable.",
                kind="claim_unverified",
            ) from exc
        ctx_c = f"Claim {claim_id}"
        expected = {
            "claim_id": claim_id,
            "text": _required_field(c, "text", ctx_c),
            "classification": _required_field(c, "classification", ctx_c),
            "generated_by": _required_field(c, "generated_by", ctx_c),
            "supports": c.get("supports") or [],
            "contradicts": c.get("contradicts") or [],
            "source_name": c.get("source_name"),
            "artifact_hash": c.get("artifact_hash"),
            "created_at": _required_field(c, "created_at", ctx_c),
        }
        for field in _signing.SIGNED_FIELDS:
            if predicate.get(field) != expected[field]:
                raise RestoreError(
                    f"Claim {claim_id} signed-predicate field {field!r} "
                    "does not match the row — TOML tampered.",
                    kind="claim_unverified",
                )

        # EvidenceVector binding. The predicate carries the canonical
        # evidence dict that was signed; restore the row's TOML
        # evidence_json must round-trip to the same dict. Without this,
        # a TOML editor could flip ``risk_of_bias`` from -2 to 0 (a
        # quality upgrade by tamper) and the SIGNED_FIELDS loop above
        # would not catch it because evidence is not in SIGNED_FIELDS.
        try:
            row_evidence = json.loads(c.get("evidence_json") or "{}")
        except (ValueError, TypeError) as exc:
            raise RestoreError(
                f"Claim {claim_id} evidence_json is malformed.",
                kind="claim_unverified",
            ) from exc
        if predicate.get("evidence") != row_evidence:
            raise RestoreError(
                f"Claim {claim_id} signed evidence vector does not match "
                "evidence_json on the row — TOML tampered.",
                kind="claim_unverified",
            )

        # statement_cid cross-check. The row carries the cid the
        # original signing path computed. Restore re-derives the cid
        # from the row's fields + evidence and compares. A bare TOML
        # edit that leaves the bundle in place but flips any predicate
        # field is caught here as a second defense after SIGNED_FIELDS.
        if c.get("statement_cid"):
            from mareforma import _statement as _stmt_mod
            recomputed_cid = _stmt_mod.statement_cid(
                _stmt_mod.build_statement(
                    claim_id=claim_id,
                    text=expected["text"],
                    classification=expected["classification"],
                    generated_by=expected["generated_by"],
                    supports=expected["supports"],
                    contradicts=expected["contradicts"],
                    source_name=expected["source_name"],
                    artifact_hash=expected["artifact_hash"],
                    created_at=expected["created_at"],
                    evidence=row_evidence,
                )
            )
            if recomputed_cid != c["statement_cid"]:
                raise RestoreError(
                    f"Claim {claim_id} statement_cid mismatch: row stores "
                    f"{c['statement_cid']!r} but re-derived {recomputed_cid!r}. "
                    "TOML tampered.",
                    kind="claim_unverified",
                )
    elif signed_mode:
        raise RestoreError(
            f"Claim {claim_id} has no signature_bundle but the graph "
            "is in signed mode (validators are enrolled). Restore "
            "refuses mixed-mode reconstruction.",
            kind="mode_inconsistent",
        )

    val_sig = c.get("validation_signature")
    if val_sig:
        try:
            val_env = json.loads(val_sig)
            val_keyid = val_env["signatures"][0]["keyid"]
            declared_type = val_env["payloadType"]
        except (json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
            raise RestoreError(
                f"Claim {claim_id} validation_signature is malformed.",
                kind="claim_unverified",
            ) from exc
        # The validation_signature column carries either a validation
        # envelope (REPLICATED→ESTABLISHED promotion) or a seed envelope
        # (born-ESTABLISHED bootstrap). Both are legitimate; pass the
        # declared type back to verify_envelope so a mismatch surfaces
        # any tampering between row and column.
        if declared_type not in (
            _signing.PAYLOAD_TYPE_VALIDATION,
            _signing.PAYLOAD_TYPE_SEED,
        ):
            raise RestoreError(
                f"Claim {claim_id} validation_signature has unexpected "
                f"payloadType {declared_type!r}.",
                kind="claim_unverified",
            )
        if val_keyid not in validators_section:
            raise RestoreError(
                f"Claim {claim_id} validation envelope is signed by "
                f"keyid {val_keyid[:12]}… which is not enrolled.",
                kind="orphan_signer",
            )
        try:
            val_signer_pem = base64.standard_b64decode(
                validators_section[val_keyid]["pubkey_pem"],
            )
            val_signer_pub = _signing.public_key_from_pem(val_signer_pem)
        except (ValueError, TypeError, _signing.SigningError) as exc:
            raise RestoreError(
                f"Validation signer pubkey for keyid {val_keyid[:12]}… "
                "is not a valid PEM.",
                kind="claim_unverified",
            ) from exc
        try:
            val_ok = _signing.verify_envelope(
                val_env, val_signer_pub,
                expected_payload_type=declared_type,
            )
        except _signing.InvalidEnvelopeError as exc:
            raise RestoreError(
                f"Claim {claim_id} validation_signature is structurally "
                f"invalid: {exc}",
                kind="claim_unverified",
            ) from exc
        if not val_ok:
            raise RestoreError(
                f"Claim {claim_id} validation_signature failed "
                "verification.",
                kind="claim_unverified",
            )
        # Cryptographic verify_envelope only proves the validator signed
        # the embedded payload — it does NOT prove the embedded payload
        # is about THIS row. A hand-edited claims.toml could copy a
        # legitimate validation/seed envelope onto a different row;
        # without the field-equality check the row would inherit a
        # forged ESTABLISHED stamp anchored by a real validator
        # signature it never authorized for that claim. Mirror the
        # SIGNED_FIELDS cross-check the signature_bundle branch does.
        try:
            val_payload = _signing.envelope_payload(val_env)
        except _signing.InvalidEnvelopeError as exc:
            raise RestoreError(
                f"Claim {claim_id} validation envelope payload is "
                "unparseable.",
                kind="claim_unverified",
            ) from exc
        if val_payload.get("claim_id") != claim_id:
            raise RestoreError(
                f"Claim {claim_id} validation envelope binds a different "
                f"claim_id ({val_payload.get('claim_id')!r}); TOML "
                "tampered or envelope copy-pasted from another row.",
                kind="claim_unverified",
            )
        if val_payload.get("validator_keyid") != val_keyid:
            raise RestoreError(
                f"Claim {claim_id} validation envelope binds a different "
                "validator_keyid than the signing keyid; TOML tampered.",
                kind="claim_unverified",
            )
        # Validation envelopes bind validated_at; seed envelopes bind
        # seeded_at. Both must match the row's validated_at column —
        # the seed path writes seeded_at INTO validated_at at INSERT
        # time, so the comparison is uniform across envelope types.
        timestamp_field = (
            "validated_at"
            if declared_type == _signing.PAYLOAD_TYPE_VALIDATION
            else "seeded_at"
        )
        if val_payload.get(timestamp_field) != c.get("validated_at"):
            raise RestoreError(
                f"Claim {claim_id} validation envelope timestamp "
                f"({timestamp_field}={val_payload.get(timestamp_field)!r}) "
                f"does not match the row's validated_at "
                f"({c.get('validated_at')!r}); TOML tampered.",
                kind="claim_unverified",
            )


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


def _backup_claims_toml(conn: sqlite3.Connection, root: Path) -> None:
    """Write all claims AND validators to claims.toml in the project root.

    Called after every claim or validator mutation. The TOML file is
    the source of truth for ``mareforma restore`` after catastrophic
    loss of ``graph.db``. Failure is non-fatal: an error line is
    printed to stderr but the exception is not raised — graph.db is
    still authoritative and the next successful mutation will rewrite
    the file. Stderr-ERROR (not ``warnings.warn``, which production
    callers often suppress) so divergence is visible by default.
    """
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
            # GRADE EvidenceVector: always present in v0.3.0 schema.
            # Round-trip the full JSON so restore can rebuild the
            # canonical Statement v1 bytes — chain_hash + signature both
            # bind these values. statement_cid is the cross-check anchor
            # restore uses to detect envelope-vs-row drift.
            entry["evidence_json"] = c.get("evidence_json") or "{}"
            if c.get("statement_cid"):
                entry["statement_cid"] = c["statement_cid"]
            # t_invalid: derived from contradiction_verdicts; round-trip
            # the column so a restored graph re-acquires the invalidation
            # state without needing the contradiction_verdicts replay.
            if c.get("t_invalid") is not None:
                entry["t_invalid"] = c["t_invalid"]
            data["claims"][c["claim_id"]] = entry

        out = root / "claims.toml"
        out.write_bytes(tomli_w.dumps(data).encode("utf-8"))

    except Exception as exc:  # noqa: BLE001
        import sys
        # stderr at an ERROR-line prefix is harder for production to
        # silently swallow than warnings.warn (which downstream code
        # routinely filters out). graph.db remains authoritative;
        # this line surfaces the divergence so an operator notices.
        print(
            f"ERROR: claims.toml backup failed; graph.db is "
            f"authoritative — {exc}",
            file=sys.stderr,
        )
