"""Schema DDL, column contract, and related constants for mareforma."""


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
    -- Denormalized asserter keyid from the claim's signature_bundle (the
    -- primary/asserter-role signature). NULL on unsigned rows and on legacy
    -- rows written before this column existed. Mirrors validator_keyid: the
    -- signature_bundle stays authoritative, this is the indexable projection
    -- the REPLICATED promotion query and the trust-layer independence count
    -- both read, so neither walks the bundle JSON. A REPLICATED row with a
    -- NULL asserter_keyid is necessarily a legacy (pre-build) promotion: the
    -- current rule refuses to promote a NULL-asserter row.
    asserter_keyid  TEXT,
    artifact_hash   TEXT,
    prev_hash       TEXT,
    -- Evidence vector, 5 downgrade domains. Stored inside the signed
    -- Statement v1 predicate; denormalised here for queryable filters
    -- ("WHERE ev_risk_of_bias <= -1"). Bounded [-2, 0]. Default 0 =
    -- unflagged. CHECK rejects tamper attempts that set out-of-range
    -- values directly via SQL.
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
    -- Full evidence vector serialised as JSON. The denormalised ev_*
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
    -- the claims_signed_fields_no_laundering watch list, invalidation
    -- IS a legitimate mutation, gated by the trigger that only fires
    -- on a signed verdict INSERT from an enrolled validator.
    t_invalid       INTEGER,
    -- Convergence-detection retry flag. Set to 1 by
    -- _maybe_update_replicated when a SQLite trigger or contention
    -- pattern causes the post-INSERT promotion check to fail. The
    -- mareforma swallows the error so writes never crash, but a
    -- swallowed error leaves the claim stuck at PRELIMINARY forever
    -- unless someone retries. EpistemicGraph.refresh_convergence()
    -- walks every flagged row, re-runs detection, and clears the flag
    -- on success. Like ``unresolved``, this column is OUTSIDE the
    -- claims_signed_fields_no_laundering watch list, flipping it is
    -- a legitimate operational mutation, not predicate tampering.
    convergence_retry_needed INTEGER NOT NULL DEFAULT 0
                            CHECK (convergence_retry_needed IN (0, 1)),
    -- Predicate-type-specific structured payload. Adapters that ship
    -- a distinct predicateType (tool-call/v1, ingested-trace/v1,
    -- gemini/*/v1, wet-lab-assay/*, review/v1, elo-match/v1, ...)
    -- write their typed payload here so mareforma queries can filter
    -- by predicate_type without parsing the claim text JSON. Default
    -- empty string keeps existing graphs forward-compatible.
    --
    -- TRUST MODEL: this column is NOT bound into the signed envelope
    -- or chain hash. It is a QUERY-SIDE DENORMALISATION, not a source
    -- of truth. Adapters that need cryptographic integrity of the
    -- predicate body must encode it inside the claim text JSON.
    -- Idempotency reconciliation does NOT compare this field for the
    -- same reason, federation exports that drop the column would
    -- otherwise round-trip differently than direct asserts.
    predicate_payload TEXT NOT NULL DEFAULT '',
    -- Federation-import preservation. When a claim is re-asserted on
    -- a receiving graph after federation bundle import, the ORIGINAL
    -- signature envelope from the source graph is preserved here.
    -- The active ``signature_bundle`` column carries the receiver's
    -- re-signed envelope (different keyid, different claim_id under
    -- mareforma UUID re-mapping). Verifiers that want to reconstruct
    -- the source-side proof read this column; mareforma's own
    -- verification path uses ``signature_bundle``. NULL on claims
    -- that were not federation-imported.
    --
    -- NOTE: this column accepts arbitrary string content; structural
    -- validation (JSON parse, DSSE-envelope shape) is not enforced
    -- here. Callers writing this field directly are responsible for
    -- supplying a valid DSSE envelope JSON string.
    original_signature_bundle TEXT,
    -- Observed grounding verdict (the computed axis), as a JSON record:
    -- {version, grounding, reason, cited_sources, grounded_sources,
    -- receipt_digest}. Distinct from the declared
    -- ``classification`` column above and never overlapping its value space.
    -- Written from the same record bound into the signed predicate, so this is
    -- a queryable denormalisation the split measurement and the promotion gate
    -- read; the signed envelope stays authoritative. NULL on every claim
    -- asserted without the observer (including every row that predates this
    -- column), and a NULL here means the signed predicate omits the field too,
    -- so the signed bytes are byte-identical to a pre-observer claim.
    observed_grounding TEXT,
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
-- Partial index on flagged retries only, refresh_convergence iterates
-- this set; the index keeps the walk O(retry-pending) rather than O(N).
CREATE INDEX IF NOT EXISTS idx_claims_convergence_retry
    ON claims(claim_id) WHERE convergence_retry_needed = 1;
-- Reputation reads aggregate ESTABLISHED claims per validator. Partial
-- on NOT NULL keeps index storage proportional to ESTABLISHED-only rows.
CREATE INDEX IF NOT EXISTS idx_claims_validator_keyid
    ON claims(validator_keyid) WHERE validator_keyid IS NOT NULL;
-- Independence counting and REPLICATED distinctness filter on a non-NULL
-- asserter_keyid. Partial on NOT NULL keeps storage proportional to signed rows.
CREATE INDEX IF NOT EXISTS idx_claims_asserter_keyid
    ON claims(asserter_keyid) WHERE asserter_keyid IS NOT NULL;
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

-- A signed claim cannot be deleted. The signature + Rekor entry + chain
-- hash collectively attest "this claim was asserted by this signer at
-- this time"; allowing a delete would let a process with DB access wipe
-- a Rekor-logged ESTABLISHED claim and rewrite claims.toml as if it never
-- existed (the Rekor entry persists, but the local graph forgets the
-- context that points to it). The whole "append-only over the signed
-- predicate" framing requires this trigger as the twin of
-- claims_signed_fields_no_laundering. Unsigned claims (legacy / no-key
-- mode) remain deletable, they carry no cryptographic commitment.
CREATE TRIGGER IF NOT EXISTS claims_signed_no_delete
BEFORE DELETE ON claims
WHEN OLD.signature_bundle IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:signed_claim_no_delete');
END;

-- Verdict-issuer protocol.
--
-- Every replication verdict and every contradiction verdict is a
-- signed row written by an enrolled validator. The OSS core
-- accepts verdicts from any party in the ``validators`` table; the
-- predicates that PRODUCE these verdicts (semantic-cluster,
-- cross-method, contradiction-detection) live outside the OSS
-- core. Any third-party verdict-issuer can write to these
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
                            'cross-method',
                            'signed-elo-bracket-replay'
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
-- mareforma witnessed, independent of whether the corresponding
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
-- existing row is refused, the envelope is the source of truth,
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
-- DESIGN RULE, DO NOT PROPAGATE DOWNSTREAM. The trigger marks only the
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

-- Full-text search over claim text. Independent FTS5 virtual table
-- (not content=claims) so the storage cost is the only price of the
-- search feature and the sync triggers below stay readable.
-- ``claim_id`` is UNINDEXED, stored for join-back but not tokenized.
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


# Append-only over the signed predicate, reconciled on every open_db() call
# (both fresh and already-initialised dbs). Lives outside _SCHEMA_SQL, which
# runs only on a fresh db: a db written by an earlier build still carries the
# earlier trigger, and CREATE TRIGGER IF NOT EXISTS would leave it in place.
# open_db compares this text against sqlite_master and rewrites only on a
# mismatch, so an already-current graph is never left without the guard and
# never has to take the write lock to open. DROP plus CREATE is a durable
# write to sqlite_master, not a free operation.
# The text below is stored verbatim in sqlite_master.sql, which is what makes
# that comparison exact: keep it a single CREATE statement with no trailing
# semicolon and no leading blank line.
#
# The Statement v1 envelope + signature binds every SIGNED_FIELDS value plus
# the evidence vector, the observed-grounding verdict and the statement_cid
# anchor. observed_grounding is watched for the same reason as the evidence
# vector, one step sharper: it gates support-level promotion, so a single
# UPDATE flipping it to GROUNDED lifts exactly the claims the observer refused
# to promote. Without this trigger, a
# direct `UPDATE claims SET ev_risk_of_bias = 0 WHERE …` would silently
# retroactively upgrade a claim's evidence quality , signature verification on
# the unchanged envelope would still pass, but the row no longer matches what
# was signed. Refuse the mutation at the SQL layer; the envelope is the
# canonical source.
#
# The trigger refuses only when (a) the row is signed (signature_bundle IS NOT
# NULL) AND (b) at least one of the watched columns actually changed
# (OLD ≠ NEW), or the update de-signs the row. A pure status-only update that
# re-emits the same text + supports + evidence values via a multi-column
# UPDATE passes through unblocked.
#
# signature_bundle is watched for one transition only: non-NULL to NULL.
# Nulling it on a signed row clears this trigger's own guard and the guard of
# claims_signed_no_delete, so three statements (null, rewrite a signed field,
# put the original bundle back) would leave a valid envelope over substituted
# content, and two would delete a Rekor-logged claim outright. The system path
# rewrites the bundle non-NULL to non-NULL on Rekor inclusion-proof
# attachment, which stays legal; an adversarial non-NULL edit is caught by
# restore's signature-vs-row binding.
#
# asserter_keyid is watched for the same reason, one step removed. It is an
# unsigned denormalisation of the bundle's signer that the REPLICATED promotion
# query and the trust-layer independence count both read, so a row that
# contradicts its own envelope inflates the distinct-signer count.
_SIGNED_FIELDS_TRIGGER_SQL = """
DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering;

CREATE TRIGGER claims_signed_fields_no_laundering
BEFORE UPDATE OF
    text, classification, generated_by,
    supports_json, contradicts_json,
    source_name, artifact_hash,
    ev_risk_of_bias, ev_inconsistency, ev_indirectness,
    ev_imprecision, ev_pub_bias,
    evidence_json, observed_grounding, statement_cid,
    prev_hash, created_at, signature_bundle, asserter_keyid
ON claims
WHEN OLD.signature_bundle IS NOT NULL
  AND (
        NEW.signature_bundle IS NULL
     OR OLD.text IS NOT NEW.text
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
     OR OLD.observed_grounding IS NOT NEW.observed_grounding
     OR OLD.statement_cid IS NOT NEW.statement_cid
     OR OLD.prev_hash IS NOT NEW.prev_hash
     OR OLD.created_at IS NOT NEW.created_at
     OR OLD.asserter_keyid IS NOT NEW.asserter_keyid
  )
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:signed_field_locked');
END"""


# support_level is the trust ladder, and it is the one column the honest paths
# rewrite after signing, so it cannot join the list above: the level is derived
# state, promoted later than the signature that binds the claim's content. What
# it can be held to is the writer. The two transitions the state machine permits
# (PRELIMINARY -> REPLICATED, REPLICATED -> ESTABLISHED) are legal only inside a
# promotion window, and only ``core._promotion_window`` opens one. A statement
# from anywhere else is refused, on a signed row, for the same reason the
# laundering trigger refuses one: the row carries a commitment the writer did
# not make. The marker is a temp table, so it is per connection: a co-resident
# process opening graph.db with plain sqlite3 has no temp schema of its own to
# find it in and is refused.
#
# The marker has to be state, not a connection-scoped SQL function. Trigger text
# is durable schema and SQLite resolves the names in it when it compiles the
# UPDATE, so a function only this release registers makes support_level
# unwritable by every other connection that opens the file, an older mareforma
# included, instead of refusing the two guarded transitions. A temp table cannot
# be named directly from a trigger (cross-schema references are refused at CREATE
# time), so the WHEN clause probes for it through pragma_table_info, a
# table-valued pragma any connection can compile since SQLite 3.16, well under
# the 3.30 floor open_db enforces.
#
# The marker is a speed bump, not the guarantee: a writer with SQL access can
# create the same temp table, or drop this trigger outright. The guarantee is on
# the read path, where a level above PRELIMINARY has to be backed by the signed
# evidence that earns it (``core._CorroborationIndex``). This trigger keeps a
# stray write from reaching that check at all.
#
# Reconciled onto existing graphs by the same sqlite_master comparison as the
# laundering trigger, so keep the text a single CREATE statement.
_PROMOTION_MARKER_TABLE = "mareforma_promotion_open"

_PROMOTION_TRIGGER_NAME = "claims_signed_promotion_backed"

_PROMOTION_TRIGGER_SQL = f"""\
CREATE TRIGGER {_PROMOTION_TRIGGER_NAME}
BEFORE UPDATE OF support_level ON claims
WHEN OLD.signature_bundle IS NOT NULL
  AND (
        (OLD.support_level = 'PRELIMINARY' AND NEW.support_level = 'REPLICATED')
     OR (OLD.support_level = 'REPLICATED' AND NEW.support_level = 'ESTABLISHED')
  )
  AND NOT EXISTS (
        SELECT 1 FROM pragma_table_info('{_PROMOTION_MARKER_TABLE}', 'temp')
  )
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:promotion_unmarked');
END"""


# The triggers open_db reconciles against sqlite_master on every open, name
# first so a definition that changed shape reaches an existing graph.
_MANAGED_TRIGGERS = (
    (_SIGNED_FIELDS_TRIGGER_NAME, _SIGNED_FIELDS_TRIGGER_SQL),
    (_PROMOTION_TRIGGER_NAME, _PROMOTION_TRIGGER_SQL),
)


# Additive tables created on every open_db() call (both fresh and
# already-initialised dbs). All CREATE statements are IF NOT EXISTS so
# the script is idempotent. Lives outside _SCHEMA_SQL because it must
# also run on the v1 path: existing graph.db files have user_version=1
# and skip _SCHEMA_SQL entirely, so the trust-layer tables would
# otherwise be missing on every upgrade.
_ADDITIVE_TABLES_SQL = """
-- project_policy: a root-signed, single-row declaration of project-wide
-- trust policy. Today it carries rekor_required: once set, the project's
-- findings must be witnessed by the transparency log before they can
-- converge. The row is a singleton (id = 1). The signed envelope is the
-- authority; the flat columns are a denormalized read cache. restore
-- verifies the envelope against the enrolled root before enforcing.
CREATE TABLE IF NOT EXISTS project_policy (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    rekor_required INTEGER NOT NULL,
    signer_keyid   TEXT NOT NULL,
    envelope       TEXT NOT NULL,
    created_at     TEXT NOT NULL
);

-- Trust layer: the structured meaning above the signed claim graph. A finding
-- rides a signed claim (the attestation: who asserted it, when, chained), and
-- these tables carry what was asserted (a content-addressed proposition), the
-- pre-registered plan, the evidence tree, and the computed bearing. The
-- identity hash and the proposition field set are frozen; Status is a versioned
-- policy computed at read time from the independent lines, not stored. Every
-- CHECK mirrors a closed Python enum in
-- mareforma.trust so a direct-SQL write is rejected at the storage layer like
-- everything else. These tables are additive: existing graphs gain them on
-- open with no migration, and the legacy free-text surface is untouched.

-- The unit of sameness. content_id is the answer (subject, relation, object,
-- scope, direction, magnitude); frame_id is the question (drops direction and
-- magnitude). UNSPECIFIED is absent from the direction CHECK because a
-- non-falsifiable proposition is refused at registration and never stored.
CREATE TABLE IF NOT EXISTS propositions (
    content_id        TEXT PRIMARY KEY,
    frame_id          TEXT NOT NULL,
    subject           TEXT NOT NULL,
    relation          TEXT NOT NULL,
    object            TEXT NOT NULL,
    direction         TEXT NOT NULL CHECK (direction IN
                          ('INCREASES','DECREASES','NO_EFFECT','PRESENT','ABSENT')),
    scope_json        TEXT NOT NULL,
    magnitude         TEXT,
    content_id_policy  TEXT NOT NULL DEFAULT 'content_id@v1',
    schema_version    TEXT NOT NULL DEFAULT 'trust@v1',
    created_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prop_frame     ON propositions(frame_id);
CREATE INDEX IF NOT EXISTS idx_prop_frame_dir ON propositions(frame_id, direction);

-- The pre-registered plan, bound to one proposition and immutable once
-- registered (see the append-only trigger below). assert_finding registers it
-- inline today; a later release exposes a register-plan-then-submit split,
-- which becomes additive because the plan already stands alone here.
CREATE TABLE IF NOT EXISTS predictions (
    plan_id               TEXT PRIMARY KEY,
    content_id            TEXT NOT NULL REFERENCES propositions(content_id),
    inference_regime      TEXT NOT NULL DEFAULT 'frequentist'
                              CHECK (inference_regime IN ('frequentist')),
    test_type             TEXT NOT NULL
                              CHECK (test_type IN ('superiority','equivalence')),
    direction_of_interest TEXT CHECK (direction_of_interest IN ('increase','decrease')),
    equivalence_lower     REAL,
    equivalence_upper     REAL,
    alpha                 REAL NOT NULL CHECK (alpha > 0 AND alpha < 1),
    preregistered         INTEGER NOT NULL DEFAULT 0 CHECK (preregistered IN (0, 1)),
    registered_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pred_content ON predictions(content_id);

-- A registered plan is append-only: refuse any UPDATE of its immutable columns
-- and any DELETE, so the gap between registration and evidence is a real
-- pre-registration guarantee. Mirrors the verdict tables' append-only protection.
CREATE TRIGGER IF NOT EXISTS predictions_append_only
BEFORE UPDATE OF
    content_id, inference_regime, test_type, direction_of_interest,
    equivalence_lower, equivalence_upper, alpha, preregistered, registered_at
ON predictions
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:prediction_locked');
END;
CREATE TRIGGER IF NOT EXISTS predictions_no_delete
BEFORE DELETE ON predictions
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:prediction_delete_blocked');
END;

-- A finding: one attestation (claim_id) plus its computed bearing_direction on
-- a proposition under a plan. The direction is denormalised here for queryable
-- Status counting; the gate inputs are persisted on the estimate so any reader
-- can recompute it and catch drift.
CREATE TABLE IF NOT EXISTS findings (
    finding_id        TEXT PRIMARY KEY,
    content_id        TEXT NOT NULL REFERENCES propositions(content_id),
    plan_id           TEXT NOT NULL REFERENCES predictions(plan_id),
    claim_id          TEXT NOT NULL REFERENCES claims(claim_id),
    bearing_direction TEXT NOT NULL CHECK (bearing_direction IN ('supports','refutes','neutral')),
    created_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_find_content ON findings(content_id);
CREATE INDEX IF NOT EXISTS idx_find_claim   ON findings(claim_id);

-- One independent line of evidence. data_id is the distinct-artifact key the
-- independence heuristic counts over: two lines are independent iff their
-- data_id differs. The current cut fills one line per finding.
CREATE TABLE IF NOT EXISTS evidence_lines (
    line_id        TEXT PRIMARY KEY,
    finding_id     TEXT NOT NULL REFERENCES findings(finding_id),
    modality       TEXT,
    provenance_id  TEXT,
    design_type    TEXT,
    data_id        TEXT NOT NULL,
    -- Model/method lineage observed at the call boundary, as a JSON record
    -- {tier, model_id, family_root, provider, version, method, decoding,
    -- attestor, digest}. attestor names how the identity was established
    -- (provider-host, weights-digest for a local model, or declared); digest is
    -- the served weights' sha256 for a weights-digest lineage and its
    -- distinctness key. The tier mirrors the data_id axis: COMPUTED (body-parse
    -- at the socket seam, or a local weights digest),
    -- PROXY (producer-declared), UNVERIFIABLE (a fine-tune / alias / wrapper
    -- whose base is not declarable). Identity only, it records which model and
    -- method authored the line, never a claim about training-time contamination.
    -- NULL on every line authored without an observed model call (including
    -- every row that predates this column).
    model_lineage  TEXT,
    created_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_line_finding ON evidence_lines(finding_id);
CREATE INDEX IF NOT EXISTS idx_line_data    ON evidence_lines(data_id);

-- The comparison a line quantifies. It carries only the control type today.
CREATE TABLE IF NOT EXISTS contrasts (
    contrast_id    TEXT PRIMARY KEY,
    line_id        TEXT NOT NULL REFERENCES evidence_lines(line_id),
    control_type   TEXT NOT NULL DEFAULT 'negative' CHECK (control_type IN
                       ('positive','negative','vehicle','sham','comparative'))
);
-- independence_counts joins evidence_lines → contrasts on line_id, then
-- contrasts → effect_estimates on contrast_id. Without this index the
-- planner has no way into contrasts by line_id and falls back to scanning
-- effect_estimates for the whole join; index it so the walk stays keyed.
CREATE INDEX IF NOT EXISTS idx_contrast_line ON contrasts(line_id);

-- The effect estimate the gate reads. Minimal field set (metafor field
-- names); variance, IRIs, test statistics, per-group n, and 2x2 cells are
-- deferred. effect_type is the stable, identity-relevant enum.
CREATE TABLE IF NOT EXISTS effect_estimates (
    estimate_id    TEXT PRIMARY KEY,
    contrast_id    TEXT NOT NULL REFERENCES contrasts(contrast_id),
    estimate_value REAL NOT NULL,
    effect_type    TEXT NOT NULL CHECK (effect_type IN
                       ('SMD','Hedges_g','OR','logOR','RR','HR','COR','ZCOR',
                        'MD','ROM','beta','log2FC','GEN')),
    scale          TEXT NOT NULL CHECK (scale IN ('raw','log')),
    p_value        REAL CHECK (p_value IS NULL OR (p_value >= 0 AND p_value <= 1)),
    ci_lower       REAL,
    ci_upper       REAL,
    ci_level       REAL,
    n_total        INTEGER
);
CREATE INDEX IF NOT EXISTS idx_estimate_contrast ON effect_estimates(contrast_id);
"""


# Explicit column list, avoids SELECT * coupling to schema changes.
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
    # Denormalized asserter keyid from the signature_bundle (REPLICATED
    # distinctness axis + trust-layer independence count read this column).
    "asserter_keyid",
    "artifact_hash",
    "prev_hash",
    # Evidence-vector denormalised columns + full JSON.
    "ev_risk_of_bias", "ev_inconsistency", "ev_indirectness",
    "ev_imprecision", "ev_pub_bias",
    "evidence_json",
    # Statement v1 content identifier + verdict-derived invalidation.
    "statement_cid", "t_invalid",
    # Convergence-detection retry queue.
    "convergence_retry_needed",
    # Adapter-specific structured predicate payload (queryable
    # denormalisation of the signed envelope's predicate body).
    "predicate_payload",
    # Federation-import preservation of source-side signature.
    "original_signature_bundle",
    # Observed grounding verdict (computed axis), queryable denormalisation of
    # the signed predicate's observed_grounding record.
    "observed_grounding",
    "created_at", "updated_at",
)
_CLAIM_SELECT = ", ".join(_CLAIM_COLUMNS)
