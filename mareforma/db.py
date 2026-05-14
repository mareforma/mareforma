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
from typing import Any, Callable

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
    -- contradiction_invalidates_older trigger when a signed
    -- contradiction_verdicts row references this claim. NULL for
    -- non-invalidated claims. The column is intentionally OUTSIDE
    -- the claims_signed_fields_no_laundering watch list — invalidation
    -- IS a legitimate mutation, gated by the trigger that only fires
    -- on a signed verdict INSERT from an enrolled validator.
    t_invalid       INTEGER,
    -- Convergence-detection retry flag. Set to 1 by
    -- _maybe_update_replicated when a SQLite trigger or contention
    -- pattern causes the post-INSERT promotion check to fail. The
    -- substrate swallows the error so writes never crash, but a
    -- swallowed error leaves the claim stuck at PRELIMINARY forever
    -- unless someone retries. EpistemicGraph.refresh_convergence()
    -- walks every flagged row, re-runs detection, and clears the flag
    -- on success. Like ``unresolved``, this column is OUTSIDE the
    -- claims_signed_fields_no_laundering watch list — flipping it is
    -- a legitimate operational mutation, not predicate tampering.
    convergence_retry_needed INTEGER NOT NULL DEFAULT 0
                            CHECK (convergence_retry_needed IN (0, 1)),
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
-- Partial index on flagged retries only — refresh_convergence iterates
-- this set; the index keeps the walk O(retry-pending) rather than O(N).
CREATE INDEX IF NOT EXISTS idx_claims_convergence_retry
    ON claims(claim_id) WHERE convergence_retry_needed = 1;
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

-- RAISE() takes a string literal in SQLite < 3.46; the previous
-- `'prefix:' || NEW.x` form rejected as a syntax error on Ubuntu
-- 24.04 LTS (SQLite 3.45.1) and many current distros. Static prefixes
-- here keep the schema portable across SQLite ≥ 3.16 (our actual
-- FTS5-driven minimum). The Python translator at
-- `_state_error_from_integrity` keys off the suffix shape; downstream
-- callers that need to know "what NEW value was rejected" can inspect
-- the row's pre-image directly.
CREATE TRIGGER IF NOT EXISTS claims_insert_state_check
BEFORE INSERT ON claims
BEGIN
    SELECT CASE
        WHEN NEW.support_level NOT IN ('PRELIMINARY', 'ESTABLISHED') THEN
            RAISE(ABORT, 'mareforma:state:insert_invalid_level')
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
            RAISE(ABORT, 'mareforma:state:illegal_transition:from_preliminary')
        WHEN OLD.support_level = 'REPLICATED' AND
             NEW.support_level NOT IN ('REPLICATED', 'ESTABLISHED') THEN
            RAISE(ABORT, 'mareforma:state:illegal_transition:from_replicated')
        WHEN OLD.support_level = 'ESTABLISHED' AND
             NEW.support_level != 'ESTABLISHED' THEN
            RAISE(ABORT, 'mareforma:state:illegal_transition:from_established')
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
            RAISE(ABORT, 'mareforma:state:retracted_is_terminal')
    END;
END;

-- Append-only over the signed predicate. The Statement v1 envelope
-- + signature binds every SIGNED_FIELDS value plus the GRADE
-- EvidenceVector + the statement_cid anchor. Without this trigger,
-- a direct `UPDATE claims SET ev_risk_of_bias = 0 WHERE …` would
-- silently retroactively upgrade a claim's evidence quality —
-- signature verification on the unchanged envelope would still
-- pass, but the row no longer matches what was signed. Refuse the
-- mutation at the SQL layer; the envelope is the canonical source.
--
-- The trigger refuses only when (a) the row is signed
-- (signature_bundle IS NOT NULL) AND (b) at least one of the watched
-- columns actually changed (OLD ≠ NEW). A pure status-only update
-- that re-emits the same text + supports + evidence values via a
-- multi-column UPDATE passes through unblocked.
--
-- Note: signature_bundle itself is NOT watched. The system path
-- legitimately rewrites it on Rekor inclusion-proof attachment
-- (the rekor block is metadata, the payload + signatures stay
-- byte-equal). If an adversary edits signature_bundle directly,
-- restore's signature-vs-row binding catches the divergence.
CREATE TRIGGER IF NOT EXISTS claims_signed_fields_no_laundering
BEFORE UPDATE OF
    text, classification, generated_by,
    supports_json, contradicts_json,
    source_name, artifact_hash,
    ev_risk_of_bias, ev_inconsistency, ev_indirectness,
    ev_imprecision, ev_pub_bias,
    evidence_json, statement_cid,
    prev_hash, created_at
ON claims
WHEN OLD.signature_bundle IS NOT NULL
  AND (
        OLD.text IS NOT NEW.text
     OR OLD.classification IS NOT NEW.classification
     OR OLD.generated_by IS NOT NEW.generated_by
     OR OLD.supports_json IS NOT NEW.supports_json
     OR OLD.contradicts_json IS NOT NEW.contradicts_json
     OR OLD.source_name IS NOT NEW.source_name
     OR OLD.artifact_hash IS NOT NEW.artifact_hash
     OR OLD.ev_risk_of_bias IS NOT NEW.ev_risk_of_bias
     OR OLD.ev_inconsistency IS NOT NEW.ev_inconsistency
     OR OLD.ev_indirectness IS NOT NEW.ev_indirectness
     OR OLD.ev_imprecision IS NOT NEW.ev_imprecision
     OR OLD.ev_pub_bias IS NOT NEW.ev_pub_bias
     OR OLD.evidence_json IS NOT NEW.evidence_json
     OR OLD.statement_cid IS NOT NEW.statement_cid
     OR OLD.prev_hash IS NOT NEW.prev_hash
     OR OLD.created_at IS NOT NEW.created_at
  )
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:signed_field_locked');
END;

-- A signed claim cannot be deleted. The signature + Rekor entry + chain
-- hash collectively attest "this claim was asserted by this signer at
-- this time"; allowing a delete would let a process with DB access wipe
-- a Rekor-logged ESTABLISHED claim and rewrite claims.toml as if it never
-- existed (the Rekor entry persists, but the local graph forgets the
-- context that points to it). The whole "append-only over the signed
-- predicate" framing requires this trigger as the twin of
-- claims_signed_fields_no_laundering. Unsigned claims (legacy / no-key
-- mode) remain deletable — they carry no cryptographic commitment.
CREATE TRIGGER IF NOT EXISTS claims_signed_no_delete
BEFORE DELETE ON claims
WHEN OLD.signature_bundle IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:signed_claim_no_delete');
END;

-- Verdict-issuer protocol.
--
-- Every replication verdict and every contradiction verdict is a
-- signed row written by an enrolled validator. The OSS substrate
-- accepts verdicts from any party in the ``validators`` table; the
-- predicates that PRODUCE these verdicts (semantic-cluster,
-- cross-method, contradiction-detection) live outside the OSS
-- substrate. Any third-party verdict-issuer can write to these
-- tables via the Graph.record_*_verdict APIs.
--
-- The signed payload bound to ``signature`` is the canonical JSON
-- of the verdict record minus the signature itself; the
-- verdict-issuer's keyid is the FK reference to validators(keyid).
CREATE TABLE IF NOT EXISTS replication_verdicts (
    verdict_id      TEXT PRIMARY KEY,
    cluster_id      TEXT NOT NULL,
    member_claim_id TEXT NOT NULL REFERENCES claims(claim_id),
    other_claim_id  TEXT REFERENCES claims(claim_id),
    method          TEXT NOT NULL
                        CHECK (method IN (
                            'hash-match',
                            'semantic-cluster',
                            'shared-resolved-upstream',
                            'cross-method'
                        )),
    confidence_json TEXT NOT NULL DEFAULT '{}',
    issuer_keyid    TEXT NOT NULL REFERENCES validators(keyid),
    signature       BLOB NOT NULL,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_replication_cluster
    ON replication_verdicts(cluster_id);
CREATE INDEX IF NOT EXISTS idx_replication_member
    ON replication_verdicts(member_claim_id);

CREATE TABLE IF NOT EXISTS contradiction_verdicts (
    verdict_id      TEXT PRIMARY KEY,
    member_claim_id TEXT NOT NULL REFERENCES claims(claim_id),
    other_claim_id  TEXT NOT NULL REFERENCES claims(claim_id),
    confidence_json TEXT NOT NULL DEFAULT '{}',
    issuer_keyid    TEXT NOT NULL REFERENCES validators(keyid),
    signature       BLOB NOT NULL,
    created_at      TEXT NOT NULL,
    -- Self-contradiction is meaningless and would let a single
    -- validator unilaterally invalidate any claim. Reject at SQL.
    CHECK (member_claim_id != other_claim_id)
);
CREATE INDEX IF NOT EXISTS idx_contradiction_member
    ON contradiction_verdicts(member_claim_id);

-- Rekor inclusion sidecar. Records every successful Rekor submission
-- the substrate witnessed, independent of whether the corresponding
-- claims-row UPDATE that attaches the rekor coords to
-- ``signature_bundle`` succeeded. The two-write saga (sidecar INSERT
-- then claim UPDATE) closes the divergence window where Rekor would
-- have a permanent public record of a claim while the local row still
-- said transparency_logged=0:
--
--   step 1: claims INSERT with transparency_logged=0 (no rekor yet)
--   step 2: submit envelope to Rekor → receive (uuid, log_index, ts)
--   step 3: INSERT rekor_inclusions  ← durable record of Rekor's ACK
--   step 4: UPDATE claims SET transparency_logged=1, signature_bundle+=rekor
--
-- If step 4 fails, step 3 already persisted the inclusion. The
-- recovery path (refresh_unsigned) reads this table BEFORE deciding
-- to re-submit: a sidecar row means "Rekor already accepted this
-- claim; replay the local UPDATE instead of double-submitting." A
-- missing sidecar row means "we never got Rekor's ACK; re-submit is
-- safe." This eliminates duplicate Rekor entries on recovery.
--
-- The raw_response column carries the full Rekor response (base64-
-- encoded UTF-8 JSON), preserved so the recovery path can reconstruct
-- the augmented bundle byte-identically to what step 4 would have
-- written had it succeeded.
CREATE TABLE IF NOT EXISTS rekor_inclusions (
    claim_id        TEXT PRIMARY KEY REFERENCES claims(claim_id),
    uuid            TEXT NOT NULL,
    log_index       INTEGER NOT NULL,
    integrated_time INTEGER,
    raw_response_b64 TEXT NOT NULL,
    recorded_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rekor_inclusions_uuid
    ON rekor_inclusions(uuid);

-- Append-only sidecar. Once a Rekor entry is recorded for a claim it
-- must not change: the replay path in refresh_unsigned attaches
-- whatever is stored here, and a mutable sidecar would let a SQL-
-- writer launder forged Rekor coords through the recovery path. UPDATE
-- and DELETE are both refused; the saga's idempotency requirement is
-- handled by the caller (which uses INSERT ON CONFLICT DO NOTHING),
-- so legitimate replays of a successful add_claim never need to
-- overwrite a row. Mirrors the verdict-table protections.
CREATE TRIGGER IF NOT EXISTS rekor_inclusions_append_only
BEFORE UPDATE ON rekor_inclusions
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:rekor_inclusion_locked');
END;
CREATE TRIGGER IF NOT EXISTS rekor_inclusions_no_delete
BEFORE DELETE ON rekor_inclusions
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:rekor_inclusion_delete_blocked');
END;

-- Append-only verdicts. Any UPDATE on the immutable columns of an
-- existing row is refused — the envelope is the source of truth,
-- and a forged UPDATE would put the row out of sync with what was
-- signed. The only mutation on these tables is INSERT.
CREATE TRIGGER IF NOT EXISTS replication_verdicts_append_only
BEFORE UPDATE OF
    cluster_id, member_claim_id, other_claim_id, method,
    confidence_json, issuer_keyid, signature, created_at
ON replication_verdicts
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:verdict_locked');
END;
CREATE TRIGGER IF NOT EXISTS replication_verdicts_no_delete
BEFORE DELETE ON replication_verdicts
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:verdict_delete_blocked');
END;

CREATE TRIGGER IF NOT EXISTS contradiction_verdicts_append_only
BEFORE UPDATE OF
    member_claim_id, other_claim_id, confidence_json,
    issuer_keyid, signature, created_at
ON contradiction_verdicts
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:verdict_locked');
END;
CREATE TRIGGER IF NOT EXISTS contradiction_verdicts_no_delete
BEFORE DELETE ON contradiction_verdicts
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:verdict_delete_blocked');
END;

-- Contradiction verdicts invalidate the OLDER of the two referenced
-- claims by setting ``claims.t_invalid`` to the verdict's created_at.
-- This is the verdict-derived invalidation pattern: t_invalid is
-- never directly written by user code, only set by this trigger in
-- response to a signed contradiction_verdicts INSERT.
--
-- ``t_invalid IS NULL`` guard makes the trigger idempotent: a second
-- contradiction on an already-invalidated claim is a no-op rather
-- than overwriting the earlier invalidation timestamp.
--
-- DESIGN RULE — DO NOT PROPAGATE DOWNSTREAM. The trigger marks only the
-- directly-contradicted claim. Claims that cited the now-invalidated one
-- via ``supports[]`` are unaffected. This is a deliberate boundary, not
-- an oversight: transitive falsification is a different model with
-- different semantics from per-claim contradiction, and conflicts with
-- the ``per-claim contradiction`` model documented in AGENTS.md. Any
-- future attempt to add downstream propagation needs a separate design
-- review before the commit.
CREATE TRIGGER IF NOT EXISTS contradiction_invalidates_older
AFTER INSERT ON contradiction_verdicts
BEGIN
    UPDATE claims
    SET t_invalid = NEW.created_at
    WHERE claim_id = (
        -- Tie-break on identical created_at by lex-smaller claim_id
        -- so the verdict's argument order does NOT determine which
        -- claim gets invalidated when timestamps collide.
        SELECT CASE
            WHEN c1.created_at < c2.created_at THEN c1.claim_id
            WHEN c2.created_at < c1.created_at THEN c2.claim_id
            WHEN c1.claim_id < c2.claim_id THEN c1.claim_id
            ELSE c2.claim_id
        END
        FROM claims c1, claims c2
        WHERE c1.claim_id = NEW.member_claim_id
          AND c2.claim_id = NEW.other_claim_id
    )
      AND t_invalid IS NULL;
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
    # Convergence-detection retry queue.
    "convergence_retry_needed",
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


class EvidenceCitationError(MareformaError):
    """Raised when ``evidence_seen`` on a validation envelope is malformed.

    ``validate_claim`` accepts an ``evidence_seen`` list of claim_ids the
    validator declares to have reviewed before signing the promotion. The
    substrate cannot prove the validator actually opened those claims,
    but it CAN verify that every cited entry is a strict-v4 UUID pointing
    at an existing claim with ``created_at <= validated_at``. Any failure
    in that check raises this exception:

      * non-string entry,
      * UUID that does not match the strict-v4 pattern,
      * claim_id that does not exist in the graph,
      * claim_id whose ``created_at`` is later than the validation
        timestamp (the validator could not have reviewed a claim that
        didn't exist yet),
      * envelope's ``evidence_seen`` field does not equal the
        ``evidence_seen`` kwarg passed alongside (the envelope's signed
        citations must match the substrate-validated kwarg byte-for-byte).

    The error message names the first failing entry so the caller can
    fix it without trial-and-error.
    """


class InvalidValidationEnvelopeError(MareformaError):
    """Raised when a validation envelope is structurally or cryptographically
    invalid.

    Distinct from :class:`EvidenceCitationError` (which is specifically
    about evidence_seen citations failing the substrate's existence /
    timestamp check). This exception fires when the envelope itself
    fails any of the substrate's defense-in-depth gates inside
    :func:`validate_claim`:

      * envelope cannot be parsed as JSON or is missing required fields,
      * envelope's ``payloadType`` is neither validation nor seed,
      * envelope's signing keyid is not an enrolled validator,
      * envelope fails Ed25519 verification against the claimed signer's
        public key (cryptographic forgery or wrong signer),
      * envelope's payload binds a different ``claim_id`` than the row
        being promoted (replay across claims),
      * envelope's payload binds a ``validator_keyid`` that does not
        equal the signing keyid (internal inconsistency),
      * envelope's payload's timestamp (``validated_at`` for validation
        envelopes, ``seeded_at`` for seed envelopes) does not equal the
        ``validated_at`` value being written.

    These checks make :func:`validate_claim` safe to call directly:
    bypassing :meth:`EpistemicGraph.validate` does not relax any
    substrate-level invariant. A caller that hand-crafts an envelope to
    impersonate an enrolled human validator will fail one of these
    gates before any row is mutated.
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
        conn = sqlite3.connect(str(path), check_same_thread=False)
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

# Pattern for the UUID format we generate via uuid.uuid4(). Strict
# UUIDv4 — version nibble is exactly ``4`` and variant nibble is one
# of {8, 9, a, b} (RFC 4122 §4.1.1, "10xx" binary variant). Tightening
# from the looser "any hex-shape UUID" rejects v1/v3/v5/zero UUIDs in
# ``supports[]`` as non-graph-nodes, which makes the shape-vs-version
# check explicit instead of accidental. Strings in ``supports[]`` that
# DON'T match are external references (DOIs etc.) and do not
# participate in cycle checking — they are not graph nodes.
_CLAIM_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)

# Walk depth cap for cycle detection. Same value as the validator-chain
# cap; defends against pathologically long planted chains.
_CYCLE_MAX_DEPTH = 1024


def _is_claim_id(value: str) -> bool:
    return bool(_CLAIM_ID_RE.match(value))


# Three-way classification of ``supports[]`` and ``contradicts[]`` entries.
# The flat string API stays — the substrate auto-classifies each entry so
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

      * ``"claim"`` — strict UUIDv4 shape, candidate graph-node edge.
        REPLICATED detection and cycle detection walk these.
      * ``"doi"`` — DOI form (``10.<registrant>/<suffix>``) per Crossref +
        DataCite syntax. Resolved against the DOI registry at assert
        time; ineligible as a REPLICATED anchor (the upstream is not a
        local claim).
      * ``"external"`` — anything else. Free-form strings (URLs, ORCID
        ids, lab-internal references). Stored verbatim, not walked, not
        resolved.

    Classification is deterministic and regex-only — no network, no
    database lookup. The same string always yields the same tag.
    """
    if not isinstance(value, str):
        return SUPPORT_TYPE_EXTERNAL
    # Late import — ``doi_resolver`` itself is import-light, but keep
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
      * operator audits — pair with :func:`find_dangling_supports` for a
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
        #   IntegrityError: mareforma:state:illegal_transition:from_preliminary
        # (Static suffixes only — SQLite < 3.46 rejects `'prefix:' || NEW.x`
        # in RAISE() as a syntax error. See the schema preamble.)
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
) -> str:
    """Compare a found row against the current call's semantic fields.

    Same key + every semantic field matching → return the existing
    ``claim_id`` (true retry). Any divergence → raise
    :class:`IdempotencyConflictError` listing every mismatched field.

    Called from two places:

    1. The pre-INSERT idempotency SELECT — the happy path. Catches the
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
    if row["text"] != text.strip():
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
    if mismatches:
        raise IdempotencyConflictError(
            f"idempotency_key={idempotency_key!r} already exists "
            f"with different {', '.join(mismatches)}. Use a "
            "different idempotency_key — silently merging two "
            "different claims into one row would discard the "
            "second author's content and break REPLICATED "
            "detection. For cross-lab convergence assert two "
            "separate claims that share an entry in supports[] "
            "with different generated_by values."
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
    on_convergence_error: "Callable[[Exception], None] | None" = None,
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
    # Strict contract: same key MUST match on every semantic field. True
    # retries pass silently; anything else raises IdempotencyConflictError.
    #
    # Prior behavior — match on artifact_hash only and silently return the
    # existing claim_id — was anti-epistemic: a second caller's text and
    # generated_by were discarded into the first caller's row, collapsing
    # what should have been two independent claims into one. The
    # "convergence convention" documented around this primitive actively
    # destroyed what REPLICATED is supposed to detect (different
    # generated_by values converging on shared upstream). The correct path
    # for cross-lab convergence is two separate claims that share an entry
    # in supports[] with different generated_by — that fires REPLICATED.
    # Idempotency_key is retry-safety only.
    if idempotency_key is not None:
        try:
            row = conn.execute(
                "SELECT claim_id, text, classification, generated_by, "
                "supports_json, contradicts_json, source_name, artifact_hash "
                "FROM claims WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row:
                existing_id = _reconcile_idempotency_row(
                    row, idempotency_key, text, classification, generated_by,
                    supports, contradicts, source_name, artifact_hash,
                )
                return existing_id
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

    # Seed-claim bootstrap. A seed claim is asserted by an
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
    # Callers can supply a populated GRADE EvidenceVector via the
    # ``evidence`` parameter — the asserter's confidence in the evidence
    # backing this claim. Default all-zeros means the asserter flagged
    # no quality concerns; downstream readers should interpret a
    # default-zero vector as "asserter made no claim about quality,"
    # not as "evidence is high-quality."
    from mareforma._evidence import EvidenceVector
    if evidence is None:
        evidence_obj = EvidenceVector()
    elif isinstance(evidence, EvidenceVector):
        evidence_obj = evidence
    elif isinstance(evidence, dict):
        evidence_obj = EvidenceVector.from_dict(evidence)
    else:
        raise TypeError(
            f"evidence must be EvidenceVector | dict | None; "
            f"got {type(evidence).__name__}"
        )
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
        # Race-loss recovery: two concurrent writers with the same
        # idempotency_key both passed the pre-INSERT SELECT (it runs
        # outside BEGIN IMMEDIATE), and the second INSERT tripped the
        # UNIQUE index on claims.idempotency_key. Re-SELECT and route
        # through the same comparison helper as the happy path so the
        # loser gets IdempotencyConflictError-with-field-list (true
        # retry) or a clean return (everything matched), not a bare
        # IntegrityError. SQLite reports the failure as
        # "UNIQUE constraint failed: claims.idempotency_key" — match on
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
                    "artifact_hash FROM claims WHERE idempotency_key = ?",
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
            claim_id=claim_id,
            envelope=envelope,
            signer=signer,
            rekor_url=rekor_url,
            require_rekor=require_rekor,
        )

    # Check whether this claim triggers REPLICATED status on shared upstreams.
    # Unresolved DOIs OR pending transparency-log inclusion block eligibility.
    if not unresolved and transparency_logged == 1:
        _maybe_update_replicated(
            conn, claim_id, supports or [], generated_by, artifact_hash,
            on_error=on_convergence_error,
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
        "SELECT status FROM claims WHERE claim_id = ?", (new_claim_id,),
    ).fetchone()
    if new_status_row is None or new_status_row["status"] != "open":
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
    # anchors plus a shared preliminary throwaway promote — strictly
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

    placeholders = ",".join("?" * len(established_anchors))
    # status='open' filter on the peer: a contested or retracted peer
    # is editorially tainted and must not participate in REPLICATED
    # convergence. Without this, an adversary could plant a born-retracted
    # claim and ride an honest peer's INSERT into REPLICATED (and from
    # there, via validate(), into ESTABLISHED — usable as a fake upstream
    # for further chains).
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
        """,
        (*established_anchors, new_claim_id, generated_by,
         artifact_hash, artifact_hash),
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
    on_error: "Callable[[Exception], None] | None" = None,
) -> bool:
    """Promote claims to REPLICATED when convergence is detected.

    Convergence: ≥2 claims share the same upstream claim_id in their
    supports[] and have different generated_by values. Uses json_each()
    for correct JSON array element extraction (no fragile LIKE).

    Called immediately after a successful INSERT in add_claim().
    Failures are swallowed — convergence detection must not crash writes.

    Returns ``True`` if detection ran cleanly, ``False`` if a SQLite
    error was swallowed. When ``on_error`` is supplied, the exception is
    handed to that callback before the WARNING is logged — caller can
    increment a counter or surface the failure however it sees fit.
    """
    try:
        _maybe_update_replicated_unlocked(
            conn, new_claim_id, supports, generated_by, artifact_hash,
        )
        conn.commit()
        return True
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        # Convergence detection is best-effort — never crash a write.
        # A trigger-raised IntegrityError here would mean a state transition
        # we asked for is illegal (e.g. ESTABLISHED peer being downgraded);
        # the underlying invariant remains intact. Surface a WARNING so
        # silently-swallowed failures are debuggable — without it, a
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
            conn.commit()
        except (sqlite3.OperationalError, sqlite3.IntegrityError):
            # If even the retry-flag UPDATE fails the substrate is in a
            # worse state than this helper can paper over. Log it, but
            # do not propagate — the originating write already committed
            # and the WARNING below makes the failure visible.
            pass
        import logging
        logging.getLogger("mareforma").warning(
            "Convergence detection swallowed %s for claim %s: %s "
            "(retry flag set; call graph.refresh_convergence() to retry)",
            type(exc).__name__, new_claim_id, exc,
        )
        return False


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

    Mirrors :func:`mark_claim_resolved` — flag-flip + TOML mirror update.
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

      * a UUID-shaped string — interpreted by the substrate as a claim_id;
      * a DOI like ``10.1234/abc`` — an external reference;
      * any other free-form string — also treated as external.

    Only UUID-shaped entries can plausibly point at a local claim and so
    only those are checked. A dangling reference is not necessarily a
    bug — it could legitimately reference a claim from another project,
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

    refs = sorted({ref for (_cid, ref) in candidates})
    placeholders = ",".join("?" * len(refs))
    existing = {
        r["claim_id"]
        for r in conn.execute(
            f"SELECT claim_id FROM claims WHERE claim_id IN ({placeholders})",
            refs,
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


def _refuse_llm_contradiction_issuer(
    conn: sqlite3.Connection, validator_keyid: str,
) -> None:
    """Raise :class:`LLMValidatorPromotionError` if *validator_keyid* is an
    enrolled LLM-typed validator attempting to issue a contradiction.

    Symmetric to :func:`_refuse_llm_validator`. A signed contradiction
    sets ``t_invalid`` on the older of two claims via the
    ``contradiction_invalidates_older`` trigger — that is equivalent in
    blast radius to demoting a human-validated ESTABLISHED claim (it
    drops from default ``query()`` results). The human-only rule must
    apply to both directions of the trust ladder: humans-only-to-promote
    AND humans-only-to-demote. Without this gate an enrolled LLM key
    could mark down any ESTABLISHED claim by signing a contradiction —
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
            "human-validated claims — the human-only rule applies to "
            "both promotion AND demotion. Have a human-typed validator "
            "sign the contradiction instead."
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

    The validator's enumeration is self-declared — this gate cannot
    prove the validator actually opened those claims, only that the
    claims they cited exist and predate validation. That's the
    strongest property the substrate can enforce; everything else
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
        Optional JSON-encoded DSSE-style envelope binding
        ``(claim_id, validator_keyid, validated_at, evidence_seen)``.
        Produced by :func:`mareforma.signing.sign_validation` and stored
        verbatim on the row so the validation event itself is
        independently verifiable (tampering with
        ``validated_by``/``validated_at``/``evidence_seen`` post-hoc is
        detectable).
    validated_at:
        Optional ISO 8601 UTC timestamp to write to the row. When the
        caller has already signed a validation envelope binding a
        timestamp, the SAME timestamp must be threaded through here so
        the envelope's ``validated_at`` matches the row's
        ``validated_at`` byte-for-byte. If ``None``, a fresh timestamp
        is generated — appropriate only for the legacy unsigned path.
    evidence_seen:
        Optional list of claim_ids the validator declares to have
        reviewed before signing the promotion. ``None`` is normalized
        to ``[]`` and bound into the signed envelope — a positive
        statement that the validator reviewed nothing, which is then
        visible in the audit trail rather than hidden by absence. Each
        cited entry must be a strict-v4 UUID matching an existing
        claim with ``created_at <= validated_at``. The validator's
        enumeration is self-declared; the substrate cannot prove they
        actually opened the cited claims, but it CAN verify the cited
        claims exist and predate validation.

    Substrate gates
    ---------------
    When ``validation_signature`` is supplied, the substrate fires the
    following defense-in-depth gates before the row is updated. All
    consult the substrate directly — calling :func:`validate_claim`
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
        If the claim's support_level is not 'REPLICATED', or its
        status is not 'open' (contested/retracted claims are editorially
        tainted and must not be promoted; revisit the editorial flag via
        update_claim before validating).
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

    # Substrate gates over the validation envelope.
    #
    # validate_claim is a public-by-convention function (no leading
    # underscore) and is callable directly by any in-process code path —
    # not only :meth:`EpistemicGraph.validate`. The wrapper builds the
    # envelope with the graph's loaded signer, so the wrapper path is
    # safe by construction; this function is the defense-in-depth layer
    # that must also be safe when called with a caller-supplied envelope.
    #
    # Without cryptographic verification here, an enrolled LLM-typed
    # validator (or any in-process caller) could hand-craft an envelope
    # JSON claiming a human validator's keyid + a garbage signature,
    # then call ``db.validate_claim`` directly. The substrate would
    # consult the CLAIMED keyid to enforce the trust-ladder gates
    # (LLM-type, self-validation), find them satisfied, and persist a
    # fraudulent ESTABLISHED row anchored by an envelope that does not
    # verify against the impersonated signer's public key. Restore would
    # eventually catch it, but the live DB would already have shipped
    # bad data to whoever queried in the meantime.
    #
    # Order of operations:
    #   1. Decode the envelope structure (refuse malformed JSON).
    #   2. Restrict ``payloadType`` to validation or seed — same set the
    #      restore path accepts on this column.
    #   3. Look up the claimed signer in the validators table.
    #   4. Cryptographically verify the envelope with the signer's
    #      pubkey via :func:`signing.verify_envelope`.
    #   5. Apply the trust-ladder gates (LLM-type ceiling, self-
    #      validation refusal). These can now safely consult the
    #      validator_keyid because step 4 proved the signer actually
    #      holds the private key.
    #   6. Compare the envelope's payload fields against the row + the
    #      kwargs the substrate is about to write — claim_id, the
    #      timestamp, validator_keyid, and evidence_seen all must
    #      agree byte-for-byte.
    #
    # The legacy unsigned path (validation_signature=None) bypasses
    # this whole block; that path is being phased out by
    # ``mareforma.open(require_signed=True)`` downstream.
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
        # ESTABLISHED). Anything else is a type confusion attempt —
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
        # validator_keyid is now known to be authentic — not just claimed.
        _refuse_llm_validator(conn, validator_keyid)
        _refuse_self_validation(
            claim_id, row["signature_bundle"], validator_keyid,
        )

    now = validated_at if validated_at is not None else _now()

    # Envelope/kwarg/row payload-field agreement. verify_envelope above
    # proved the signer signed THESE BYTES — but it does NOT prove the
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
        # InvalidEnvelopeError would propagate past the substrate's
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
                "timestamp must agree with the substrate write."
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
                    f"kwarg ({kwarg_evidence!r}); the substrate validates "
                    "what the caller passed, and the signed envelope must "
                    "bind the same list — refusing to persist a divergent "
                    "envelope."
                )

    # Evidence-citation gate. Every entry in evidence_seen must be a
    # strict-v4 UUID pointing at an existing claim that predates the
    # validation timestamp. An empty list is the "I reviewed nothing"
    # admission and passes the gate. None is normalized to [].
    _verify_evidence_seen(
        conn, claim_id, evidence_seen or [], now,
    )
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


def _attempt_rekor_saga(
    conn: sqlite3.Connection,
    *,
    claim_id: str,
    envelope: dict,
    signer: "object",
    rekor_url: str,
    require_rekor: bool,
) -> int:
    """Run the Rekor 4-step saga on a freshly-INSERTed signed claim.

    Returns the new ``transparency_logged`` value to write back to the
    caller's local variable (0 if the saga did not complete, 1 if the
    row UPDATE succeeded).

    Saga steps
    ----------
    1. The claim is already INSERTed with ``transparency_logged=0``
       (the caller's responsibility, before this helper runs).
    2. Submit the envelope to Rekor; on failure, return 0.
    3. Persist the (uuid, logIndex, integratedTime) coords to the
       ``rekor_inclusions`` sidecar. The sidecar's append-only triggers
       guarantee no replay can rewrite this row.
    4. UPDATE the claim row's ``signature_bundle`` with the augmented
       envelope (Rekor block attached) and set ``transparency_logged=1``.

    If step 4 fails after step 3 succeeded, the sidecar holds the durable
    record. :meth:`EpistemicGraph.refresh_unsigned` reads the sidecar and
    replays step 4 instead of double-submitting to Rekor.

    Extracting this helper out of :func:`add_claim` keeps the
    happy-path read concise: ``add_claim`` is about claim insertion +
    chain integrity; the saga is a separate concern that lives next to
    its sidecar helper :func:`_record_rekor_inclusion`.

    Raises
    ------
    SigningError
        If the initial Rekor submission fails and ``require_rekor=True``.
    """
    from mareforma import signing as _signing

    logged, entry = _signing.submit_to_rekor(
        envelope, signer.public_key(), rekor_url=rekor_url,
    )
    if not logged or entry is None:
        if require_rekor:
            raise _signing.SigningError(
                f"Rekor submission to {rekor_url} failed and "
                "require_rekor=True. Claim was persisted with "
                "transparency_logged=0; call "
                "EpistemicGraph.refresh_unsigned() to retry."
            )
        return 0

    # Step 3: durable sidecar write. Failure here means Rekor saw the
    # entry but we lost the record locally — the next refresh_unsigned
    # will re-submit and create a duplicate, which is the only recovery
    # path when no sidecar exists. _record_rekor_inclusion emits a
    # warning on that path; we honor its return value.
    if not _record_rekor_inclusion(conn, claim_id, entry):
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
        conn.commit()
        return 1
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        import warnings as _warnings
        _warnings.warn(
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
) -> bool:
    """Step 3 of the Rekor saga: persist a successful inclusion.

    Called after Rekor returns a `(logged=True, entry)` response and
    before the claims-row UPDATE. The sidecar is the durable record of
    "Rekor witnessed this claim" — when the row UPDATE later fails,
    :meth:`refresh_unsigned` consults this table to replay the UPDATE
    instead of re-submitting.

    Stores the full Rekor response (base64-encoded UTF-8 JSON) so the
    recovery path can reconstruct the augmented signature bundle byte-
    identically to what the original UPDATE would have written.

    Returns ``True`` on success. On failure, emits a WARNING and returns
    ``False`` — the caller skips the subsequent UPDATE so we don't end
    up with `transparency_logged=1` but no sidecar record (the inverse
    of the gap this saga closes). The Rekor entry exists publicly; the
    operator must run :meth:`refresh_unsigned` which will detect the
    missing-sidecar-but-unflagged state and re-submit (creating a
    duplicate entry — the only recovery available when we have no
    record of the original inclusion).
    """
    try:
        raw_json = json.dumps(entry, sort_keys=True, separators=(",", ":"))
        raw_b64 = base64.standard_b64encode(
            raw_json.encode("utf-8"),
        ).decode("ascii")
        # Defensive numeric parsing. Rekor returns ``logIndex`` and
        # ``integratedTime`` as JSON numbers, but a buggy or hostile
        # registry could return strings (``"42"``), floats, or non-
        # numeric tokens. Without this guard, an int() ValueError would
        # propagate out of add_claim AFTER the claim has been committed
        # — the user would see a stack trace instead of the documented
        # (False, None) sidecar-failure flow. Treat any parse failure
        # as a sidecar miss; the recovery path then re-submits.
        try:
            log_index_int = int(entry.get("logIndex") or 0)
        except (TypeError, ValueError):
            import warnings as _warnings
            _warnings.warn(
                f"Rekor returned a non-integer logIndex "
                f"({entry.get('logIndex')!r}) for claim {claim_id}. "
                "Treating as a sidecar miss; refresh_unsigned() will "
                "re-submit and create a duplicate Rekor entry — the "
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
            # write — the uuid and logIndex are sufficient to replay
            # the saga's step 4.
            integrated_time_int = None
        # ON CONFLICT DO NOTHING: a successful Rekor inclusion is
        # immutable. If a caller retries the saga and lands here twice
        # for the same claim_id, the original row stays — the
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
                entry.get("uuid"),
                log_index_int,
                integrated_time_int,
                raw_b64,
                _now(),
            ),
        )
        conn.commit()
        return True
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        import warnings as _warnings
        _warnings.warn(
            f"Claim {claim_id} accepted by Rekor but the sidecar INSERT "
            f"into rekor_inclusions failed ({exc}). The local row stays "
            "unflagged AND there is no recovery hint — refresh_unsigned() "
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
    """Return the stored Rekor inclusion entry for a claim, if any.

    Used by the recovery path in :meth:`refresh_unsigned` to detect
    "Rekor ACK persisted, claims-row UPDATE pending" and replay the
    UPDATE from stored coords instead of re-submitting.

    Returns the original Rekor response dict (uuid, logIndex,
    integratedTime, etc.) parsed back from the base64 storage form, or
    ``None`` when no sidecar row exists for this claim.
    """
    row = conn.execute(
        "SELECT raw_response_b64 FROM rekor_inclusions WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        return None
    try:
        raw_json = base64.standard_b64decode(
            row["raw_response_b64"],
        ).decode("utf-8")
        return json.loads(raw_json)
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


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
    Before writing, FOUR gates apply:

    1. The row must already carry a non-NULL ``signature_bundle``.
       mark_claim_logged attaches a Rekor block to an existing
       envelope — it is not a path to sign an unsigned claim.
    2. The supplied bundle must be JSON.
    3. The bundle must be a structurally-valid claim envelope and its
       ``predicate.claim_id`` must equal the row's ``claim_id``. A buggy
       caller that mixes up claim ids cannot silently write Alice's
       bundle onto Bob's row.
    4. The supplied bundle's ``payload``, ``payloadType``, and
       ``signatures`` fields must be byte-identical to the row's
       existing ``signature_bundle``. The trigger
       ``claims_signed_fields_no_laundering`` intentionally does NOT
       watch ``signature_bundle`` (the Rekor attachment legitimately
       rewrites it), so this function is the sole defense against a
       caller substituting a different envelope wholesale (different
       signer, different payload, different keyid). Only the optional
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
    # bytes, signatures array, and payloadType — only the optional
    # top-level ``rekor`` block may differ. Without this check, a caller
    # could pass any DSSE-shaped envelope (different signer, freshly
    # forged signatures, same predicate.claim_id) and the substrate
    # would persist it, since the claims_signed_fields_no_laundering
    # trigger intentionally does not watch signature_bundle.
    try:
        existing_envelope = json.loads(existing_bundle_raw)
    except json.JSONDecodeError as exc:
        # Row's bundle column is corrupt — separate failure mode from
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
    # unsigned claim — the cycle-introduction window the DFS check covers.
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


_VALID_REPLICATION_METHODS = (
    "hash-match",
    "semantic-cluster",
    "shared-resolved-upstream",
    "cross-method",
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


class VerdictIssuerError(MareformaError):
    """Raised when a verdict-issuer write is refused.

    Reasons: issuer not enrolled, referenced claim_id missing, method
    not in the allowed enum, or the signature payload binding fails.
    """


def _verdict_canonical_payload(
    fields: tuple[str, ...], record: dict,
) -> bytes:
    """Canonical JSON of a verdict record under a fixed field set.

    Uses :func:`mareforma._canonical.canonicalize` so verdicts and
    claims share one canonicalization contract (sorted keys, NFC
    Unicode normalization, no whitespace, ``allow_nan=False``).
    A third-party verdict-issuer implementing against the same
    canonical-JSON contract produces signatures the OSS substrate
    verifies; a confidence dict containing NaN / Inf is rejected at
    sign time rather than producing a payload some verifiers refuse.
    """
    from ._canonical import canonicalize
    payload = {name: record.get(name) for name in fields}
    return canonicalize(payload)


def _require_enrolled_issuer(
    conn: sqlite3.Connection, issuer_keyid: str,
) -> None:
    """Refuse the verdict if issuer_keyid is not an enrolled validator.

    Walks the enrollment chain back to a self-signed root via
    ``validators.is_enrolled`` — same gate the seed-claim path and
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
            "be in the validators table with a verifiable chain — "
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

    The OSS substrate doesn't fire replication predicates itself —
    third-party verdict-issuers call this method after running their
    predicate logic. The substrate just accepts the signed verdict and
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

    confidence_dict = confidence or {}
    # canonicalize() (NFC + sorted keys + no whitespace + allow_nan=False)
    # for stored confidence_json so restore round-trips byte-equally
    # AND callers can't sneak a NaN/Inf into a signed payload.
    from ._canonical import canonicalize as _canonicalize
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
        # invalidated (a signed contradiction verdict is terminal —
        # a later replication verdict must not silently re-promote).
        conn.execute(
            f"UPDATE claims SET support_level = 'REPLICATED', updated_at = ? "
            f"WHERE claim_id IN ({placeholders}) "
            f"AND support_level = 'PRELIMINARY' "
            f"AND status = 'open' "
            f"AND t_invalid IS NULL",
            (created_at, *members),
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
            f"claim_id on both sides ({member_claim_id!r}) — self-"
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

    confidence_dict = confidence or {}
    # canonicalize() (NFC + sorted keys + no whitespace + allow_nan=False)
    # for stored confidence_json so restore round-trips byte-equally
    # AND callers can't sneak a NaN/Inf into a signed payload.
    from ._canonical import canonicalize as _canonicalize
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
    invalidated (``claims.t_invalid IS NOT NULL``) are excluded — same
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
    audit-mode listings (the typical use — a contradiction verdict
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


def query_claims(
    conn: sqlite3.Connection,
    *,
    limit: int = 10,
    text: str | None = None,
    min_support: str | None = None,
    classification: str | None = None,
    include_unverified: bool = False,
    include_invalidated: bool = False,
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
        table are excluded by default. REPLICATED and
        ESTABLISHED rows already require an enrolled validator chain and
        are never filtered by this flag. Pass ``True`` to surface
        unverified preliminary claims (e.g. inspection of pending work).
    include_invalidated:
        When False (default), claims with non-NULL ``t_invalid`` are
        excluded — a contradiction_verdicts row from an enrolled
        validator has marked them invalid. Pass ``True`` for audit /
        history queries where you want to see contradicted claims too.

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
    include_invalidated: bool = False,
) -> list[dict]:
    """FTS5-ranked search over claim text.

    Returns claim dicts ordered by FTS5 rank (best match first). Each
    dict carries the same projection as :func:`query_claims`:
    ``validator_reputation`` and ``generator_enrolled`` are attached
    per row, and ``include_unverified`` / ``include_invalidated``
    filters apply identically.

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
    # TOML parser: stdlib `tomllib` on Python 3.11+, PyPI `tomli` on 3.10.
    # Both share the same `loads` + `TOMLDecodeError` API. The previous
    # code imported `tomli` unconditionally; pyproject only declares it
    # for Python < 3.11, so a 3.11+ install hit ModuleNotFoundError the
    # moment restore() ran — silently breaking the catastrophic-loss
    # recovery path on the most common modern Python.
    try:
        import tomllib  # Python 3.11+ stdlib
    except ImportError:  # pragma: no cover  -- Python 3.10 path
        import tomli as tomllib  # type: ignore[no-redef]
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
        data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
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
                    conn, claim_id, c, validators_section, signed_mode,
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
                # transparency_logged: trust the TOML flag ONLY when the
                # bundle actually carries a rekor block with a uuid.
                # Otherwise a hand-edited claims.toml could flip the
                # flag to true and the row would then satisfy the
                # REPLICATED-detection gate (transparency_logged=1)
                # without ever having been witnessed by the log.
                toml_logged = c.get("transparency_logged")
                bundle_has_rekor = False
                if c.get("signature_bundle"):
                    try:
                        _env = json.loads(c["signature_bundle"])
                        _rekor = _env.get("rekor") or {}
                        bundle_has_rekor = bool(_rekor.get("uuid"))
                    except (json.JSONDecodeError, AttributeError, TypeError):
                        bundle_has_rekor = False
                resolved_transparency = (
                    1 if (toml_logged is not False and bundle_has_rekor)
                    else 0
                )
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
                             evidence_json, statement_cid,
                             convergence_retry_needed,
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
                            resolved_transparency,
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
                            1 if c.get("convergence_retry_needed") else 0,
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

            # Verdict-table replay. Each verdict envelope carries its
            # own signature binding; we verify before INSERT. The
            # contradiction trigger fires on the contradiction INSERT
            # and re-derives t_invalid — restore doesn't need to
            # round-trip t_invalid separately.
            #
            # Sort by created_at before replay so the contradiction
            # trigger (WHERE t_invalid IS NULL) sets t_invalid to the
            # earliest contradiction's timestamp, preserving the
            # truthful first-invalidation moment. Without sorting,
            # tomli's insertion-order iteration lets a hand-edited
            # TOML reorder contradictions to backdate or postdate the
            # invalidation timestamp.
            rep_section = data.get("replication_verdicts") or {}
            rep_ordered = sorted(
                rep_section.items(),
                key=lambda kv: kv[1].get("created_at") or "",
            )
            for verdict_id, v in rep_ordered:
                _verify_and_insert_replication_verdict(
                    conn, verdict_id, v, validators_section,
                )
            con_section = data.get("contradiction_verdicts") or {}
            con_ordered = sorted(
                con_section.items(),
                key=lambda kv: kv[1].get("created_at") or "",
            )
            for verdict_id, v in con_ordered:
                _verify_and_insert_contradiction_verdict(
                    conn, verdict_id, v, validators_section,
                )

            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            raise

        # Restore inserted many validator rows; drop any per-connection
        # chain-verification cache so the next is_enrolled walk operates
        # against the fresh state. (Restore opens its own connection and
        # closes it on the next line, so this is technically belt-and-
        # suspenders, but the symmetric treatment is the right invariant
        # for any future restore caller that reuses the connection.)
        from mareforma.validators import invalidate_conn_cache
        invalidate_conn_cache(conn)
        return {
            "validators_restored": len(ordered_validators),
            "claims_restored": len(ordered_claims),
        }
    finally:
        conn.close()


def _verify_and_insert_replication_verdict(
    conn: sqlite3.Connection,
    verdict_id: str,
    v: dict,
    validators_section: dict,
) -> None:
    """Cryptographically verify + INSERT a replication verdict from TOML.

    The signed payload binds (verdict_id, cluster_id, member_claim_id,
    other_claim_id, method, confidence) under DSSE PAE with
    payloadType ``application/vnd.mareforma.replication-verdict+json``.
    The issuer_keyid is looked up in the restored validators_section;
    forged keyids without a matching enrollment fail verification.
    """
    from mareforma import signing as _signing

    ctx = f"Replication verdict {verdict_id}"
    cluster_id = _required_field(v, "cluster_id", ctx)
    member_claim_id = _required_field(v, "member_claim_id", ctx)
    other_claim_id = v.get("other_claim_id")
    method = _required_field(v, "method", ctx)
    confidence_json = _required_field(v, "confidence_json", ctx)
    issuer_keyid = _required_field(v, "issuer_keyid", ctx)
    signature_b64 = _required_field(v, "signature", ctx)
    created_at = _required_field(v, "created_at", ctx)

    try:
        signature_bytes = base64.b64decode(signature_b64)
    except (ValueError, TypeError) as exc:
        raise RestoreError(
            f"{ctx} signature is not valid base64.",
            kind="claim_unverified",
        ) from exc

    enrollment = validators_section.get(issuer_keyid)
    if enrollment is None:
        raise RestoreError(
            f"{ctx} issuer_keyid {issuer_keyid!r} is not in the validators "
            "section — verdict signer is not enrolled.",
            kind="claim_unverified",
        )
    try:
        pem_bytes = base64.standard_b64decode(enrollment["pubkey_pem"])
        pubkey = _signing.public_key_from_pem(pem_bytes)
    except (KeyError, ValueError, TypeError, _signing.SigningError) as exc:
        raise RestoreError(
            f"{ctx} validator PEM unparseable: {exc}",
            kind="claim_unverified",
        ) from exc

    try:
        confidence_dict = json.loads(confidence_json or "{}")
    except (ValueError, TypeError) as exc:
        raise RestoreError(
            f"{ctx} confidence_json unparseable: {exc}",
            kind="claim_unverified",
        ) from exc

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
    from cryptography.exceptions import InvalidSignature
    try:
        pubkey.verify(signature_bytes, pae)
    except InvalidSignature as exc:
        raise RestoreError(
            f"{ctx} signature verification failed — TOML tampered or "
            "signature forged.",
            kind="claim_unverified",
        ) from exc

    try:
        conn.execute(
            """
            INSERT INTO replication_verdicts(
                verdict_id, cluster_id, member_claim_id, other_claim_id,
                method, confidence_json, issuer_keyid, signature, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict_id, cluster_id, member_claim_id, other_claim_id,
                method, confidence_json, issuer_keyid, signature_bytes,
                created_at,
            ),
        )
    except sqlite3.IntegrityError as exc:
        raise RestoreError(
            f"{ctx} INSERT refused: {exc}",
            kind="claim_unverified",
        ) from exc


def _verify_and_insert_contradiction_verdict(
    conn: sqlite3.Connection,
    verdict_id: str,
    v: dict,
    validators_section: dict,
) -> None:
    """Cryptographically verify + INSERT a contradiction verdict from TOML.

    Same shape as the replication verdict path. The
    ``contradiction_invalidates_older`` trigger fires on this INSERT
    and re-derives ``claims.t_invalid`` automatically.
    """
    from mareforma import signing as _signing

    ctx = f"Contradiction verdict {verdict_id}"
    member_claim_id = _required_field(v, "member_claim_id", ctx)
    other_claim_id = _required_field(v, "other_claim_id", ctx)
    confidence_json = _required_field(v, "confidence_json", ctx)
    issuer_keyid = _required_field(v, "issuer_keyid", ctx)
    signature_b64 = _required_field(v, "signature", ctx)
    created_at = _required_field(v, "created_at", ctx)

    try:
        signature_bytes = base64.b64decode(signature_b64)
    except (ValueError, TypeError) as exc:
        raise RestoreError(
            f"{ctx} signature is not valid base64.",
            kind="claim_unverified",
        ) from exc

    enrollment = validators_section.get(issuer_keyid)
    if enrollment is None:
        raise RestoreError(
            f"{ctx} issuer_keyid {issuer_keyid!r} is not in the validators "
            "section — verdict signer is not enrolled.",
            kind="claim_unverified",
        )
    try:
        pem_bytes = base64.standard_b64decode(enrollment["pubkey_pem"])
        pubkey = _signing.public_key_from_pem(pem_bytes)
    except (KeyError, ValueError, TypeError, _signing.SigningError) as exc:
        raise RestoreError(
            f"{ctx} validator PEM unparseable: {exc}",
            kind="claim_unverified",
        ) from exc

    try:
        confidence_dict = json.loads(confidence_json or "{}")
    except (ValueError, TypeError) as exc:
        raise RestoreError(
            f"{ctx} confidence_json unparseable: {exc}",
            kind="claim_unverified",
        ) from exc

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
    from cryptography.exceptions import InvalidSignature
    try:
        pubkey.verify(signature_bytes, pae)
    except InvalidSignature as exc:
        raise RestoreError(
            f"{ctx} signature verification failed — TOML tampered or "
            "signature forged.",
            kind="claim_unverified",
        ) from exc

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
                confidence_json, issuer_keyid, signature_bytes,
                created_at,
            ),
        )
    except sqlite3.IntegrityError as exc:
        raise RestoreError(
            f"{ctx} INSERT refused: {exc}",
            kind="claim_unverified",
        ) from exc


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
    conn: sqlite3.Connection,
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
        # evidence_seen verification — only relevant for the
        # PAYLOAD_TYPE_VALIDATION case (seed envelopes don't carry
        # evidence_seen). Every cited claim_id must already exist in
        # the restored graph and predate the validation timestamp.
        # Since claims are inserted in created_at order and validations
        # cite earlier claims, the cited entries should be present by
        # the time this row's validation is checked.
        if declared_type == _signing.PAYLOAD_TYPE_VALIDATION:
            cited = val_payload.get("evidence_seen")
            if cited is None:
                raise RestoreError(
                    f"Claim {claim_id} validation envelope is missing "
                    "the evidence_seen field; v0.3.0 envelopes always "
                    "bind this field (use [] for the no-review case).",
                    kind="claim_unverified",
                )
            if not isinstance(cited, list):
                raise RestoreError(
                    f"Claim {claim_id} validation envelope's "
                    f"evidence_seen is not a list: {cited!r}.",
                    kind="claim_unverified",
                )
            row_validated_at = c.get("validated_at")
            for entry in cited:
                if not isinstance(entry, str) or not _is_claim_id(entry):
                    raise RestoreError(
                        f"Claim {claim_id} evidence_seen entry "
                        f"{entry!r} is not a strict-v4 UUID.",
                        kind="claim_unverified",
                    )
                cited_row = conn.execute(
                    "SELECT created_at FROM claims WHERE claim_id = ?",
                    (entry,),
                ).fetchone()
                if cited_row is None:
                    raise RestoreError(
                        f"Claim {claim_id} evidence_seen cites "
                        f"'{entry}' which does not exist in the "
                        "restored graph.",
                        kind="claim_unverified",
                    )
                if cited_row["created_at"] > row_validated_at:
                    raise RestoreError(
                        f"Claim {claim_id} evidence_seen cites "
                        f"'{entry}' (created_at "
                        f"{cited_row['created_at']!r}) which post-dates "
                        f"the validation (validated_at "
                        f"{row_validated_at!r}).",
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
            # GRADE EvidenceVector: always present in v0.3.0 schema.
            # Round-trip the full JSON so restore can rebuild the
            # canonical Statement v1 bytes — chain_hash + signature both
            # bind these values. statement_cid is the cross-check anchor
            # restore uses to detect envelope-vs-row drift.
            entry["evidence_json"] = c.get("evidence_json") or "{}"
            if c.get("statement_cid"):
                entry["statement_cid"] = c["statement_cid"]
            # t_invalid is derived (set by the contradiction trigger
            # on signed verdict INSERT). Restore replays the verdict
            # table; the trigger fires again and re-sets t_invalid.
            # We do NOT round-trip the column directly — that would
            # accept a TOML-tampered t_invalid value without verifying
            # it against a signed contradiction envelope.
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
                data["replication_verdicts"][vid] = {
                    "cluster_id": v["cluster_id"],
                    "member_claim_id": v["member_claim_id"],
                    "other_claim_id": v["other_claim_id"],
                    "method": v["method"],
                    "confidence_json": v["confidence_json"],
                    "issuer_keyid": v["issuer_keyid"],
                    "signature": base64.b64encode(v["signature"]).decode("ascii"),
                    "created_at": v["created_at"],
                }
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
