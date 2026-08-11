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
#
# predicate_payload is watched on the same ground. It stays outside the signed
# envelope, but the audit path reads the finding's citation set out of it to
# re-check a GROUNDED verdict against the sources the finding names, so one
# UPDATE clearing it turns a binding violation into a clean verdict. The column
# is only ever written at INSERT; a change on a signed row is tampering.
_SIGNED_FIELDS_TRIGGER_NAME = "claims_signed_fields_no_laundering"

_SIGNED_FIELDS_TRIGGER_SQL = """\
CREATE TRIGGER claims_signed_fields_no_laundering
BEFORE UPDATE OF
    text, classification, generated_by,
    supports_json, contradicts_json,
    source_name, artifact_hash,
    ev_risk_of_bias, ev_inconsistency, ev_indirectness,
    ev_imprecision, ev_pub_bias,
    evidence_json, observed_grounding, statement_cid,
    prev_hash, created_at, signature_bundle, asserter_keyid,
    predicate_payload
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
     OR OLD.predicate_payload IS NOT NEW.predicate_payload
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


# findings and evidence_lines are the two gate-input tables the read path reads
# to derive a proposition's status, and neither carried a write guard: an UPDATE
# could re-point a finding's plan or rewrite a line's data_id, and a DELETE could
# drop a refuting line so the survivors read as consensus. No honest path updates
# or deletes either table (a finding is written once by submit_finding and read
# forever after), so both are append-only and no-delete, mirroring the guards
# predictions and plan_retirements already carry.
#
# These go through _MANAGED_TRIGGERS rather than a CREATE TRIGGER IF NOT EXISTS
# in _ADDITIVE_TABLES_SQL for the reason the learning
# schema-if-not-exists-hides-constraint-change names: the tables already exist in
# graphs written by an earlier release, and IF NOT EXISTS is a no-op the moment a
# trigger of the same name is present, so a later change to the body would never
# reach an existing graph. The managed path compares the stored text against the
# wanted text and drop-and-recreates on a mismatch, so a definition that changes
# shape reconciles onto every graph on open. Keep each SQL a single CREATE
# statement with no trailing semicolon and no leading blank line: the text is
# compared verbatim against sqlite_master.sql. The bodies name no per-connection
# SQL function so a connection that registered none can still compile an UPDATE
# against the guarded table (the trigger refuses the write, it does not make the
# table unwritable to an older release).
_FINDINGS_APPEND_ONLY_TRIGGER_NAME = "findings_append_only"

_FINDINGS_APPEND_ONLY_TRIGGER_SQL = """\
CREATE TRIGGER findings_append_only
BEFORE UPDATE ON findings
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:finding_locked');
END"""

_FINDINGS_NO_DELETE_TRIGGER_NAME = "findings_no_delete"

_FINDINGS_NO_DELETE_TRIGGER_SQL = """\
CREATE TRIGGER findings_no_delete
BEFORE DELETE ON findings
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:finding_delete_blocked');
END"""

_EVIDENCE_LINES_APPEND_ONLY_TRIGGER_NAME = "evidence_lines_append_only"

_EVIDENCE_LINES_APPEND_ONLY_TRIGGER_SQL = """\
CREATE TRIGGER evidence_lines_append_only
BEFORE UPDATE ON evidence_lines
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:evidence_line_locked');
END"""

_EVIDENCE_LINES_NO_DELETE_TRIGGER_NAME = "evidence_lines_no_delete"

_EVIDENCE_LINES_NO_DELETE_TRIGGER_SQL = """\
CREATE TRIGGER evidence_lines_no_delete
BEFORE DELETE ON evidence_lines
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:evidence_line_delete_blocked');
END"""


# propositions, contrasts and effect_estimates are the remaining gate-input
# tables the read path reads to derive a proposition's status, and none carried
# a write guard: a direct UPDATE could flip an estimate's value (the demonstrated
# convergent-to-refuted attack) or rewrite a proposition's text so the evidence
# now backs a different sentence, and a DELETE could drop a contrast or estimate
# so a refutation reads as consensus. validators is guarded too: swapping a row's
# pubkey_pem makes a forged signature verify, and dropping a row rewrites who the
# graph will trust. None of the four carries a legitimate post-insert mutation
# (a proposition is content-addressed and frozen, an estimate and contrast are
# written once by insert_finding, and enrollment is one-way), so each is
# append-only and no-delete. Idempotent re-registration uses ON CONFLICT DO
# NOTHING, which fires neither trigger.
#
# These join _MANAGED_TRIGGERS for the same reason findings and evidence_lines
# do (see the note above): the tables already exist in graphs written by an
# earlier release, so a CREATE TRIGGER IF NOT EXISTS in _ADDITIVE_TABLES_SQL
# would never reach an existing graph if a later change altered the body. The
# managed path drop-and-recreates on a text mismatch, so a definition change
# reconciles onto every graph on open. Keep each SQL a single CREATE statement
# with no trailing semicolon and no leading blank line: the text is compared
# verbatim against sqlite_master.sql. The bodies name no per-connection SQL
# function, so a connection that registered none (an older release, a co-resident
# reader) can still compile an UPDATE against the guarded table with foreign keys
# off; the trigger refuses the write rather than making the table unwritable.
_PROPOSITIONS_APPEND_ONLY_TRIGGER_NAME = "propositions_append_only"

_PROPOSITIONS_APPEND_ONLY_TRIGGER_SQL = """\
CREATE TRIGGER propositions_append_only
BEFORE UPDATE ON propositions
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:proposition_locked');
END"""

_PROPOSITIONS_NO_DELETE_TRIGGER_NAME = "propositions_no_delete"

_PROPOSITIONS_NO_DELETE_TRIGGER_SQL = """\
CREATE TRIGGER propositions_no_delete
BEFORE DELETE ON propositions
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:proposition_delete_blocked');
END"""

_CONTRASTS_APPEND_ONLY_TRIGGER_NAME = "contrasts_append_only"

_CONTRASTS_APPEND_ONLY_TRIGGER_SQL = """\
CREATE TRIGGER contrasts_append_only
BEFORE UPDATE ON contrasts
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:contrast_locked');
END"""

_CONTRASTS_NO_DELETE_TRIGGER_NAME = "contrasts_no_delete"

_CONTRASTS_NO_DELETE_TRIGGER_SQL = """\
CREATE TRIGGER contrasts_no_delete
BEFORE DELETE ON contrasts
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:contrast_delete_blocked');
END"""

_EFFECT_ESTIMATES_APPEND_ONLY_TRIGGER_NAME = "effect_estimates_append_only"

_EFFECT_ESTIMATES_APPEND_ONLY_TRIGGER_SQL = """\
CREATE TRIGGER effect_estimates_append_only
BEFORE UPDATE ON effect_estimates
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:effect_estimate_locked');
END"""

_EFFECT_ESTIMATES_NO_DELETE_TRIGGER_NAME = "effect_estimates_no_delete"

_EFFECT_ESTIMATES_NO_DELETE_TRIGGER_SQL = """\
CREATE TRIGGER effect_estimates_no_delete
BEFORE DELETE ON effect_estimates
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:effect_estimate_delete_blocked');
END"""

_VALIDATORS_APPEND_ONLY_TRIGGER_NAME = "validators_append_only"

_VALIDATORS_APPEND_ONLY_TRIGGER_SQL = """\
CREATE TRIGGER validators_append_only
BEFORE UPDATE ON validators
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:validator_locked');
END"""

_VALIDATORS_NO_DELETE_TRIGGER_NAME = "validators_no_delete"

_VALIDATORS_NO_DELETE_TRIGGER_SQL = """\
CREATE TRIGGER validators_no_delete
BEFORE DELETE ON validators
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:validator_delete_blocked');
END"""


# project_policy carries the project's root-signed, one-way trust rules, and it
# was the one trust table with no write guard at all. A rule is only one-way if
# the row that records it cannot be removed: DELETE FROM project_policy launders
# the whole declaration away, the backup written after it carries no
# [project_policy] section, and restore then accepts the policyless graph as
# authentic. So the row cannot be deleted, and it cannot be updated from outside
# the one writer that signs a replacement.
#
# The columns are watched rather than read. A trigger BODY that named them
# would pin them in place: ALTER TABLE ... DROP COLUMN refuses to drop a column
# a trigger references, and a legacy table that predates
# ``strict_promotion_required`` is migrated in place on open, so a WHEN clause
# comparing OLD/NEW flags would make the very upgrade path this guard protects
# unrunnable. A ``BEFORE UPDATE OF`` list is an event filter, not a reference,
# and the column stays droppable.
#
# So the WHEN clause keys on the same per-connection marker
# ``claims_signed_promotion_backed`` uses: ``set_project_policy`` opens the
# window around its upsert, and no other connection has that temp table to find.
# The marker is a speed bump, not the guarantee (a writer with SQL access can
# create the same temp table, drop the trigger, or reach the row through
# INSERT OR REPLACE, which SQLite runs without firing either guard while
# recursive_triggers is off). The guarantee is on the read path, where the flat
# columns are bound to the root-signed envelope before either is trusted
# (``core._verified_project_policy``). This keeps a stray write from reaching
# that check at all.
_POLICY_MARKER_TABLE = "mareforma_policy_open"

_PROJECT_POLICY_APPEND_ONLY_TRIGGER_NAME = "project_policy_append_only"

_PROJECT_POLICY_APPEND_ONLY_TRIGGER_SQL = f"""\
CREATE TRIGGER {_PROJECT_POLICY_APPEND_ONLY_TRIGGER_NAME}
BEFORE UPDATE OF
    rekor_required, strict_promotion_required, signer_keyid, envelope,
    created_at, rekor_declared_at, strict_promotion_declared_at
ON project_policy
WHEN NOT EXISTS (
        SELECT 1 FROM pragma_table_info('{_POLICY_MARKER_TABLE}', 'temp')
  )
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:project_policy_locked');
END"""

_PROJECT_POLICY_NO_DELETE_TRIGGER_NAME = "project_policy_no_delete"

_PROJECT_POLICY_NO_DELETE_TRIGGER_SQL = """\
CREATE TRIGGER project_policy_no_delete
BEFORE DELETE ON project_policy
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:project_policy_delete_blocked');
END"""


# predictions is append-only too, and its guard already exists, but it watched
# every immutable column EXCEPT plan_id, the primary key. Rewriting plan_id
# re-points a whole rule at a different identity and makes every line of
# evidence gated under it vanish from the count with nothing disclosed, so
# plan_id joins the watch list. The guard moves out of _ADDITIVE_TABLES_SQL and
# into the managed set for exactly the reason above: this body change reaches an
# existing graph only through the drop-and-recreate reconciliation. The message
# stays 'prediction_locked' so callers keying off it are unaffected.
_PREDICTIONS_APPEND_ONLY_TRIGGER_NAME = "predictions_append_only"

_PREDICTIONS_APPEND_ONLY_TRIGGER_SQL = """\
CREATE TRIGGER predictions_append_only
BEFORE UPDATE OF
    plan_id, content_id, inference_regime, test_type, direction_of_interest,
    equivalence_lower, equivalence_upper, alpha, preregistered, registered_at
ON predictions
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:prediction_locked');
END"""


# The triggers open_db reconciles against sqlite_master on every open, name
# first so a definition that changed shape reaches an existing graph.
_MANAGED_TRIGGERS = (
    (_SIGNED_FIELDS_TRIGGER_NAME, _SIGNED_FIELDS_TRIGGER_SQL),
    (_PROMOTION_TRIGGER_NAME, _PROMOTION_TRIGGER_SQL),
    (_FINDINGS_APPEND_ONLY_TRIGGER_NAME, _FINDINGS_APPEND_ONLY_TRIGGER_SQL),
    (_FINDINGS_NO_DELETE_TRIGGER_NAME, _FINDINGS_NO_DELETE_TRIGGER_SQL),
    (
        _EVIDENCE_LINES_APPEND_ONLY_TRIGGER_NAME,
        _EVIDENCE_LINES_APPEND_ONLY_TRIGGER_SQL,
    ),
    (
        _EVIDENCE_LINES_NO_DELETE_TRIGGER_NAME,
        _EVIDENCE_LINES_NO_DELETE_TRIGGER_SQL,
    ),
    (
        _PROPOSITIONS_APPEND_ONLY_TRIGGER_NAME,
        _PROPOSITIONS_APPEND_ONLY_TRIGGER_SQL,
    ),
    (_PROPOSITIONS_NO_DELETE_TRIGGER_NAME, _PROPOSITIONS_NO_DELETE_TRIGGER_SQL),
    (_CONTRASTS_APPEND_ONLY_TRIGGER_NAME, _CONTRASTS_APPEND_ONLY_TRIGGER_SQL),
    (_CONTRASTS_NO_DELETE_TRIGGER_NAME, _CONTRASTS_NO_DELETE_TRIGGER_SQL),
    (
        _EFFECT_ESTIMATES_APPEND_ONLY_TRIGGER_NAME,
        _EFFECT_ESTIMATES_APPEND_ONLY_TRIGGER_SQL,
    ),
    (
        _EFFECT_ESTIMATES_NO_DELETE_TRIGGER_NAME,
        _EFFECT_ESTIMATES_NO_DELETE_TRIGGER_SQL,
    ),
    (_VALIDATORS_APPEND_ONLY_TRIGGER_NAME, _VALIDATORS_APPEND_ONLY_TRIGGER_SQL),
    (_VALIDATORS_NO_DELETE_TRIGGER_NAME, _VALIDATORS_NO_DELETE_TRIGGER_SQL),
    (
        _PROJECT_POLICY_APPEND_ONLY_TRIGGER_NAME,
        _PROJECT_POLICY_APPEND_ONLY_TRIGGER_SQL,
    ),
    (
        _PROJECT_POLICY_NO_DELETE_TRIGGER_NAME,
        _PROJECT_POLICY_NO_DELETE_TRIGGER_SQL,
    ),
    (
        _PREDICTIONS_APPEND_ONLY_TRIGGER_NAME,
        _PREDICTIONS_APPEND_ONLY_TRIGGER_SQL,
    ),
)


# Additive tables created on every open_db() call (both fresh and
# already-initialised dbs). All CREATE statements are IF NOT EXISTS so
# the script is idempotent. Lives outside _SCHEMA_SQL because it must
# also run on the v1 path: existing graph.db files have user_version=1
# and skip _SCHEMA_SQL entirely, so the trust-layer tables would
# otherwise be missing on every upgrade.
_ADDITIVE_TABLES_SQL = """
-- The enumerating read's ordering, as an index. `query()` orders by the support
-- tier then created_at, which is a CASE expression no column index can serve, so
-- every call scanned the table and built a temp B-tree to sort it: the LIMIT
-- bounded what was returned, never what was read, and the whole cost was paid
-- under the process-wide graph lock every other caller waits on. An index on the
-- same expression turns the sort into an ordered scan the LIMIT can stop early,
-- which also makes the cost independent of table size rather than linear in it.
-- Measured over 2,000 claims at limit 20: 0.918 ms per call before, 0.009 ms
-- after, and the plan drops USE TEMP B-TREE FOR ORDER BY. It lives here rather than in the fresh-database
-- schema so an existing graph gets it on the next open (the
-- schema-if-not-exists-hides-constraint-change trap: statements that run only
-- for a fresh database never reach a graph written by an earlier release).
CREATE INDEX IF NOT EXISTS idx_claims_read_order ON claims(
    CASE support_level WHEN 'ESTABLISHED' THEN 3
         WHEN 'REPLICATED' THEN 2 ELSE 1 END DESC,
    created_at DESC
);

-- project_policy: a root-signed, single-row declaration of project-wide
-- trust policy. rekor_required: the project's findings must be witnessed by
-- the transparency log before they can converge. strict_promotion_required:
-- a converging pair must carry data (artifact_hash) on both sides. Both are
-- one-way once declared, and both bind every writer, not just the handle
-- that declared them. The row is a singleton (id = 1). created_at is when the
-- row was last signed, so extending the policy moves it; the *_declared_at
-- columns hold when each flag was first declared and are what a grandfathering
-- check reads. NULL until the flag is declared. The signed envelope is the
-- authority; the flat columns are a denormalized read cache. restore verifies
-- the envelope against the enrolled root before enforcing.
CREATE TABLE IF NOT EXISTS project_policy (
    id                           INTEGER PRIMARY KEY CHECK (id = 1),
    rekor_required               INTEGER NOT NULL,
    signer_keyid                 TEXT NOT NULL,
    envelope                     TEXT NOT NULL,
    created_at                   TEXT NOT NULL,
    strict_promotion_required    INTEGER NOT NULL DEFAULT 0,
    rekor_declared_at            TEXT,
    strict_promotion_declared_at TEXT
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
-- The alpha bound the gates need is (0, 0.5) and Prediction enforces it on
-- every write. The CHECK here stays at the wider (0, 1) on purpose: this file
-- is all CREATE TABLE IF NOT EXISTS, so an existing graph keeps the bound it
-- was created with, and restore replays that graph's backup into a fresh
-- schema. A tighter CHECK here would reject a plan a live graph holds and cost
-- the operator the whole recovery.
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

-- A registered plan is append-only: refuse any DELETE, so the gap between
-- registration and evidence is a real pre-registration guarantee. Mirrors the
-- verdict tables' append-only protection. The UPDATE half (predictions_append_only)
-- is a managed trigger in _MANAGED_TRIGGERS: it watches plan_id as well as the
-- other immutable columns, and a managed definition reaches an existing graph
-- whose trigger predates the plan_id addition, which an IF NOT EXISTS here would
-- not.
CREATE TRIGGER IF NOT EXISTS predictions_no_delete
BEFORE DELETE ON predictions
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:prediction_delete_blocked');
END;

-- A retired plan. A plan written by a release with a wider alpha bound can
-- carry a rule the gates cannot run, and the row above can be neither corrected
-- nor removed, so the evidence standing under it would count as nothing for
-- good. Retirement is the operator's recovery: it names the plan, the plan that
-- supersedes it (the same rule at an alpha the gates can run) and why, so the
-- read path can gate that evidence under the replacement. It is recorded state,
-- never a rewrite: the retired row stays exactly as it was registered. Both
-- ends reference predictions, so a retirement can only point at rules the graph
-- holds, and superseded_by is a different plan by CHECK. The claim is the
-- signed retirement attestation, whose text renders the same triple the row
-- carries, so restore re-derives the row from signed material.
CREATE TABLE IF NOT EXISTS plan_retirements (
    plan_id       TEXT PRIMARY KEY REFERENCES predictions(plan_id),
    superseded_by TEXT NOT NULL REFERENCES predictions(plan_id)
                      CHECK (superseded_by <> plan_id),
    reason        TEXT NOT NULL,
    claim_id      TEXT NOT NULL REFERENCES claims(claim_id),
    retired_at    TEXT NOT NULL
);

-- A retirement is append-only like the plan it retires: an operator who could
-- re-point or drop one could move a proposition's counts by rewriting which
-- rule its evidence stands under, with nothing on the read saying so.
CREATE TRIGGER IF NOT EXISTS plan_retirements_append_only
BEFORE UPDATE ON plan_retirements
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:plan_retirement_locked');
END;
CREATE TRIGGER IF NOT EXISTS plan_retirements_no_delete
BEFORE DELETE ON plan_retirements
BEGIN
    SELECT RAISE(ABORT, 'mareforma:append_only:plan_retirement_delete_blocked');
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

-- supports_revision: a monotonic counter over supports-edge mutations, bumped
-- by every write that changes the claim_supports cache. It lives in graph.db,
-- not in the cache file, so it commits in the same database as the claims row
-- it describes. The cache stamps the value it last saw; a mismatch means the
-- cache missed a mutation (a crash between the two WAL commits, or a writer
-- that did not maintain the cache) and the cache is rebuilt on next open. The
-- claim-count check alone cannot see an in-place supports edit, which moves no
-- count. Additive: existing graphs gain the table and the singleton row on the
-- next open, and the first open after the upgrade rebuilds the cache once.
-- The singleton row is seeded by _ensure_supports_revision_row, not here: an
-- INSERT in this script would need write access on every open, including the
-- opens that change nothing.
CREATE TABLE IF NOT EXISTS supports_revision (
    id       INTEGER PRIMARY KEY CHECK (id = 1),
    revision INTEGER NOT NULL DEFAULT 0
);
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
