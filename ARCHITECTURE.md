# Mareforma — Architecture

## In one sentence

Mareforma is an opinionated wrapper around in-toto Statement v1 +
DSSE PAE + local SQLite, with GRADE-shaped evidence vectors and signed
convergence verdicts, packaged as a Python library that an AI agent
writes to as it works.

## The lane

Every individual capability mareforma uses exists in mature form
elsewhere — Ed25519 signing (`cryptography`), DSSE envelopes (`in-toto`),
Sigstore transparency (`rekor`), JSON canonicalization (RFC 8785-style),
local-first SQLite (Datasette ecosystem), GRADE evidence grading
(Cochrane). What is missing in the OSS landscape is the **combination**:
a runtime, opt-in, local Python library that takes those primitives
and gives an agent a place to assert a claim, cite its upstream
evidence, sign the assertion under a key the agent controls, and find
out whether an independent agent already converged on the same
conclusion.

Mareforma is that combination. It is **not** trying to replace:

- W3C PROV-O (richer provenance vocabulary — mareforma is a runtime
  substrate, not an RDF graph)
- FAIRSCAPE's EVI (research-evidence ontology — an EVI export adapter
  is on the deferred-features backlog and would map mareforma claims onto EVI Claim
  / EvidenceGraph / supports / challenges classes; the schema stays
  mareforma-native, the export is the interop surface)
- IETF SCITT (federated supply-chain transparency — a SCITT submission
  path alongside Rekor is on the deferred-features backlog)
- Sigstore (transparency for software artifacts — mareforma uses Rekor
  for claim transparency; the protocols are the same shape)
- RO-Crate (FAIR research-object packaging — an RO-Crate 1.2 export
  from `export_bundle.py` is on the deferred-features backlog)
- MLflow / DVC / W&B (run + dataset versioning — orthogonal; those
  track artifacts, mareforma tracks claims)

## Rails, not trains

Mareforma ships **the rails**: the storage substrate, the signing
discipline, the trust-ladder state machine, the convergence-detection
SQL, the restore-from-TOML recovery path. What it deliberately does
**not** ship — the **trains** that produce verdicts — lives outside
the OSS:

- **Semantic-cluster verdicts** (which embedding model, which similarity
  threshold, which clustering algorithm — research-domain specific)
- **Cross-method verdicts** (when do two analytical pipelines count as
  "different methods" — domain-specific)
- **Contradiction-detection verdicts via NLI** (which NLI model, which
  contradiction threshold — research-domain specific)
- **Sakana / FutureHouse-style end-to-end AI scientist agents** (those
  consume mareforma; they don't live in it)

The verdict-issuer protocol in mareforma (`record_replication_verdict`
and `record_contradiction_verdict`) is the public API that any of those
trains can write to. The OSS substrate accepts any signed verdict from
an enrolled validator; the predicates that produce those verdicts are
out of scope by design. The OSS substrate stays narrow and verifiable;
the trains plug in through the public protocol.

## Data flow

```
agent
  │
  │ assert_claim(text, classification, supports=[...], generated_by=...)
  ▼
EpistemicGraph (mareforma/_graph.py)
  │
  │ ─ classifies (caller-supplied; substrate does not verify)
  │ ─ canonical_statement(claim_fields) → bytes (NFC + sorted keys + no whitespace)
  │ ─ in-toto Statement v1 wrapping (mareforma/_statement.py)
  │ ─ DSSE PAE encoding (mareforma/signing.py)
  │ ─ Ed25519 signature
  ▼
db.add_claim (mareforma/db.py)
  │
  │ ─ BEGIN IMMEDIATE
  │ ─ prev_hash chain extension under lock
  │ ─ INSERT INTO claims (signed envelope + ev_* columns + statement_cid)
  │ ─ COMMIT
  │ ─ optionally submit to Rekor (if rekor_url= was passed)
  │ ─ _maybe_update_replicated() — detect convergence
  │ ─ _backup_claims_toml() — write the TOML mirror (post-commit;
  │                            see "What survives restore" for the
  │                            crash-window gap)
  ▼
graph.db (SQLite, WAL, ACID)
claims.toml (TOML, deterministic, signed-fields-byte-identical;
             canonical for restore, derived for chain integrity)
```

The same path runs whether you call `g.assert_claim(...)` from Python
or `mareforma claim add ...` from the CLI. Both go through
`mareforma.open()` and pick up the XDG-default signing key.

## Trust ladder

```
PRELIMINARY ──(≥2 agents share ESTABLISHED upstream)──▶ REPLICATED ──(graph.validate())──▶ ESTABLISHED
```

Three rules:

1. **PRELIMINARY → REPLICATED is automatic, structural, and gated.**
   The new claim and a candidate peer must share at least one
   `ESTABLISHED` upstream in `supports[]`, must have different
   `generated_by`, and (if both supply `artifact_hash`) must agree on
   the hash. Status, transparency log, and DOI resolution gates apply
   too — see `_maybe_update_replicated_unlocked` in db.py.
2. **REPLICATED → ESTABLISHED is human-only.** `graph.validate()`
   requires an enrolled validator key whose `validator_type` is
   `'human'`. LLM-typed validators may sign validations but cannot
   promote past REPLICATED.
3. **No back-transitions.** The state-machine triggers refuse any
   ESTABLISHED → REPLICATED or REPLICATED → PRELIMINARY UPDATE. Status
   changes (open / contested / retracted) live on a separate axis
   that is mutable but `retracted` is terminal.

The `seed=True` bootstrap is the only way to insert at ESTABLISHED
directly. It exists to break the chicken-and-egg of "REPLICATED needs
an ESTABLISHED upstream that doesn't exist on a fresh graph yet" — and
it is gated to enrolled human-typed validators only.

## Contestation model

Contradiction in mareforma is a **per-claim demotion**, not a
transitive falsification. When an enrolled validator signs a
`record_contradiction_verdict(member, other)`, the substrate sets
`t_invalid` on the older of the two claims (lex-order tiebreak on
identical timestamps). Default `query()` excludes invalidated claims;
`include_invalidated=True` returns the full audit set.

What contradiction does **not** do:
- It does not propagate downstream. Claims that cited the
  now-invalidated one via `supports[]` are unaffected.
- It does not retract the upstream claim's signed envelope. The
  envelope and the Rekor entry remain valid attestations of the
  asserter's belief at the time of writing.

This per-claim boundary is a deliberate design rule, not an oversight.
Transitive falsification is a different model with different semantics
and a different freedom-to-operate posture; see the design comment on
the `contradiction_invalidates_older` trigger in `db.py` for context.

## Signing surface

Every signed payload in mareforma uses DSSE PAE (`dsse_pae(payload_type,
body)`) with these payload types:

| Payload type | What it signs |
|---|---|
| `application/vnd.in-toto+json` (Statement v1) | Per-claim assertion (text + classification + supports + contradicts + source + artifact_hash + evidence + created_at) |
| `application/vnd.mareforma.validator-enrollment+json` | Per-validator enrollment (keyid + pubkey + identity + validator_type + parent) |
| `application/vnd.mareforma.validation+json` | Per-validation event (claim_id + validator_keyid + validated_at + evidence_seen) |
| `application/vnd.mareforma.seed-claim+json` | Per-seed bootstrap (claim_id + validator_keyid + seeded_at) |
| `application/vnd.mareforma.replication-verdict+json` | Per-replication verdict from an issuer |
| `application/vnd.mareforma.contradiction-verdict+json` | Per-contradiction verdict from an issuer |

The bundle export (`export_bundle.py`) signs the entire JSON-LD graph
under a separate `application/vnd.mareforma.graph-bundle+json` payload
type. The bundle signature attests "this set of claims was bundled by
this key" — it does **not** re-attest the per-claim signatures. To
verify per-claim signatures end-to-end, use the `claims.toml` backup,
which preserves each row's `signature_bundle` field.

### Canonicalization — RFC 8785 strict

`canonicalize` (in [`mareforma/_canonical.py`](mareforma/_canonical.py))
normalizes every string in the payload to Unicode NFC, then serializes
via the `rfc8785` library — a strict implementation of RFC 8785 (JSON
Canonicalization Scheme, JCS). The dependency was added currently;
prior versions used `json.dumps(sort_keys=True, ...)` and were only
JCS-shaped, not JCS-strict.

What strict JCS gets us:

- Keys sorted lexicographically by UTF-16 code unit at every nesting
  level (JCS §3.2.3).
- No whitespace, minimal JSON string escape set, UTF-8 output
  (JCS §3.2.1–§3.2.2).
- **Numbers per the ECMAScript `Number.prototype.toString` algorithm**
  (JCS §3.2.2.3). `1.0` renders as `1`; `1e10` renders as
  `10000000000`; exponent boundaries follow ES rules. This is the
  load-bearing difference vs. Python's stdlib `json.dumps`: the day
  the substrate adds a float-valued field, a Go / Rust / JavaScript
  verifier re-canonicalizing per RFC 8785 will produce the same bytes
  and verify the same signature.
- `NaN` / `±Infinity` are rejected (JSON has no representation; RFC
  8785 explicitly forbids them).
- Integers outside the IEEE-754 double-precision safe-integer range
  are rejected (JCS would otherwise lose precision on round-trip).
- Dict keys that NFC-normalize to the same string raise `ValueError`
  rather than silently dropping a value — canonical JSON requires
  distinct keys, and dropping one would produce a non-deterministic
  envelope under adversarial input.

NFC normalization is layered above JCS as a mareforma-internal
discipline. RFC 8785 itself operates on whatever code points the input
contains; pre-normalizing to NFC means visually-identical text with
different decomposition (`é` U+00E9 vs `e` + U+0301) produces the same
canonical bytes. Decoupling NFC from JCS keeps the JCS layer
interoperable with any other RFC 8785 implementation.

For cross-tool verification: use any RFC 8785 implementation
(`rfc8785` in Python, `github.com/sigsum/sigsum-go/pkg/jcs` in Go,
`serde_jcs` in Rust, `canonicalize` in JS) to re-derive the bytes
mareforma signed, then verify the DSSE envelope's PAE signature with
the signer's Ed25519 public key. The in-toto Statement v1 subject
digest (`sha256` over `text`) is canonical without depending on number
serialization at all — it's the same bytes any in-toto verifier
(`in-toto-golang`, the Sigstore stack) will produce.

## Storage substrate

SQLite, WAL mode, `check_same_thread=False`, `PRAGMA foreign_keys = ON`,
minimum version 3.30.0 (enforced at `open_db()`).

Tables:

- `claims` — every assertion. Includes denormalized `ev_*` columns for
  query, the full `evidence_json` for round-trip, the
  `signature_bundle` DSSE envelope, a `prev_hash` chain link, and the
  `convergence_retry_needed` flag set by `_maybe_update_replicated`
  when a swallowed error needs operator follow-up.
- `validators` — per-project enrolled-validator chain, rooted at a
  self-signed row. Singleton-root invariant: more than one self-signed
  row → entire chain forfeit.
- `replication_verdicts` / `contradiction_verdicts` — signed verdicts
  from enrolled issuers. Append-only at the trigger level.
- `rekor_inclusions` — sidecar recording every successful Rekor
  submission, independent of whether the claims-row UPDATE that
  attaches the rekor coords to `signature_bundle` succeeded. Closes
  the divergence window where Rekor would have a permanent public
  record while the local row still said `transparency_logged=0`:
  `refresh_unsigned` consults this table to replay the UPDATE
  instead of re-submitting (no duplicate Rekor entry). Append-only
  at the trigger level (UPDATE and DELETE both refused), so a
  SQL-writer cannot launder forged Rekor coords through the replay
  path.
- `claims_fts` — FTS5 virtual table (independent of `claims`, not
  `content=` linked) for substring + tokenized search.
- `doi_cache` — 30-day positive / 24-hour negative cache for DOI HEAD
  checks against Crossref + DataCite.

SQL triggers enforce the state machine, the append-only invariants on
signed predicate fields, the no-delete rule on signed claims, the
verdict tables' append-only-and-no-delete invariants, the rekor-
inclusions sidecar's same invariants, the contradiction-invalidates-
older logic, and the FTS sync. A tampered Python interpreter cannot
relax these rules.

## What survives restore

`claims.toml` is the canonical source for `mareforma.restore(project_root)` —
canonical for rebuilding `graph.db` and re-verifying signatures,
**derived** for the `prev_hash` chain (regenerated, not preserved).

The restore path:

1. Re-verifies every validator's enrollment envelope against its
   parent's pubkey (chain walk back to a self-signed root).
2. Re-verifies every claim's `signature_bundle` against the signer's
   enrolled pubkey.
3. Re-derives `statement_cid` from the claim's canonical statement and
   cross-checks against the stored value.
4. Re-derives `prev_hash` chain in claim order. Note: this is regeneration,
   not preservation — see below.
5. Replays all verdicts in chronological order so the
   `contradiction_invalidates_older` trigger sets earliest-first.

Failure of ANY check rolls the entire restore back. Restore is
`fresh-only` and `fail-all-or-nothing` by design; partial-restore mode
is on the deferred-features backlog.

### Two known gaps in what TOML guarantees

**Chain order is not externally anchored.** A tampered TOML that
reorders claims (swap two `created_at` values) restores to a different
but internally-consistent chain. The signatures bind canonical statement
bytes, not chain position. For tamper-evidence across restore boundaries,
the per-claim Rekor entry is the external anchor — though Merkle
inclusion proof verification is itself on the deferred-features backlog.

**The TOML write lags the SQLite commit.** `_backup_claims_toml` runs
**after** the INSERT/UPDATE transaction commits. A process crash between
`COMMIT` and the TOML write leaves a row in `graph.db` that's missing
from `claims.toml`. The next mutation rewrites the TOML from current DB
state, so the crash window closes on the next successful write. For a
clean recovery snapshot, finish any in-flight writes before snapshotting
the TOML. The perf rewrite addresses both the foreground-commit-
path cost and the crash gap by moving to an append-only sidecar +
periodic compaction model.

## Substrate at a glance

A 30-minute audit map. Each row links a substrate property to the
exact mechanism that enforces it and the specific threat it
defends against. Designed for the reader who wants to verify
mareforma's invariants without scrolling through 4,600 lines of
`db.py`.

### State-machine transitions

```
                seed=True               graph.validate()
                   │                          │
                   ▼                          ▼
              ┌─────────────┐            ┌─────────────┐
              │ ESTABLISHED │ ◄───────── │ REPLICATED  │
              └─────────────┘            └─────────────┘
                                              ▲
                                              │ ≥2 claims, different
                                              │ generated_by, sharing
                                              │ ESTABLISHED upstream
                                              │
                                         ┌─────────────┐
                                         │ PRELIMINARY │
                                         └─────────────┘
                                              ▲
                                              │ assert_claim()
                                              │ (default)
```

Each arrow is enforced by a SQL trigger that refuses illegal
transitions at the storage layer. A tampered Python interpreter
cannot bypass them.

| Transition | Trigger | Refuses |
|---|---|---|
| INSERT at any level | `claims_insert_state_check` | ESTABLISHED without `validation_signature`; PRELIMINARY with `validated_by` set; non-PRELIMINARY non-ESTABLISHED birth states |
| PRELIMINARY → REPLICATED → ESTABLISHED (one-way) | `claims_update_state_check` | downgrades; bypass of REPLICATED via PRELIMINARY → ESTABLISHED |
| status = 'retracted' is terminal | `claims_update_status_terminal` | the resurrection attack where a born-retracted ESTABLISHED seed is later flipped to 'open' |
| signed claims are append-only over the predicate | `claims_signed_fields_no_laundering` | direct-SQL UPDATE of `text` / `classification` / `generated_by` / `supports_json` / `contradicts_json` / `source_name` / `artifact_hash` / `ev_*` / `evidence_json` / `statement_cid` / `prev_hash` / `created_at` on a row with `signature_bundle IS NOT NULL` |
| signed claims cannot be deleted | `claims_signed_no_delete` | the wipe-and-rewrite attack where a Rekor-logged ESTABLISHED claim is deleted from `graph.db` and `claims.toml` is regenerated as if it never existed |

### Append-only sidecars

| Table | Trigger | Refuses |
|---|---|---|
| `rekor_inclusions` | `rekor_inclusions_append_only` + `rekor_inclusions_no_delete` | any UPDATE or DELETE — once Rekor witnessed a claim, the saga's step-3 record is immutable; SQL writers cannot launder forged Rekor coords through the recovery path |
| `replication_verdicts` | `replication_verdicts_append_only` + `replication_verdicts_no_delete` | UPDATE of signed columns; DELETE of any row — verdicts are signed evidence, not editable records |
| `contradiction_verdicts` | `contradiction_verdicts_append_only` + `contradiction_verdicts_no_delete` | same; plus the `contradiction_invalidates_older` AFTER INSERT trigger that sets `t_invalid` on the older of two referenced claims (lex-tie-break, idempotent via `WHERE t_invalid IS NULL`) |

### Signed-fields vs mutable-fields

The DSSE envelope signs an in-toto Statement v1 whose predicate
binds the values in `mareforma.signing.SIGNED_FIELDS` plus the
GRADE `EvidenceVector`. Any post-INSERT mutation of those values
on a signed row is refused at the SQL layer.

| Field | Signed (predicate) | Mutable on a signed row |
|---|---|---|
| `claim_id` | ✓ | — |
| `text` | ✓ | — |
| `classification` | ✓ | — |
| `generated_by` | ✓ | — |
| `supports_json` | ✓ | — |
| `contradicts_json` | ✓ | — |
| `source_name` | ✓ | — |
| `artifact_hash` | ✓ | — |
| `created_at` | ✓ | — |
| `evidence_json` + `ev_*` | ✓ | — |
| `statement_cid` | derived from signed bytes | — |
| `prev_hash` | derived (chain link) | — |
| `status` | not signed | ✓ (one-way: open → contested → retracted) |
| `support_level` | not signed | ✓ (one-way ladder) |
| `validated_by` / `validated_at` / `validation_signature` / `validator_keyid` | not signed (validation is its own envelope) | written by `validate_claim` only |
| `signature_bundle` | self-referential | only rewritten by `mark_claim_logged` to attach a Rekor block; payload + signatures bytes must be byte-identical to the existing value, only the optional `rekor` top-level key may differ |
| `unresolved` / `transparency_logged` / `convergence_retry_needed` / `t_invalid` | not signed (operational flags) | ✓ (gated mutations — `t_invalid` by trigger only) |

### What `restore()` proves vs what the live DB proves

| Property | Proved by live DB | Proved by `restore()` |
|---|---|---|
| The claim was signed by an enrolled key at insert time | yes (`signature_bundle` set + validator chain walk) | yes — re-verifies every envelope against the validator's PEM, refuses orphan signers |
| The row's signed fields match the envelope | trigger blocks mutation; row never drifts unless a SQL-tamper bypasses Python | yes — re-derives canonical bytes and compares to `predicate.*`, refuses on mismatch |
| EvidenceVector hasn't been tampered after signing | trigger blocks `ev_*` and `evidence_json` mutation | yes — re-derives the canonical evidence dict and compares to `predicate.evidence` |
| `statement_cid` cross-check | column never directly written by user code | yes — re-derives from the row's fields + evidence and compares to the stored `statement_cid` |
| Validation envelope binds this claim | substrate gates: `_extract_validation_signer_keyid`, `_refuse_llm_validator`, `_refuse_self_validation`, `_verify_evidence_seen`, envelope/kwarg agreement; cryptographic verify on the envelope | yes — verifies the validation envelope's signature, then checks `claim_id` / `validator_keyid` / timestamp / `evidence_seen` fields against the row |
| Contradiction verdict is signed by an enrolled validator | enforced at `record_contradiction_verdict`; chain walk via `is_enrolled` | yes — replays each verdict envelope in `created_at` order, verifies before INSERT, the contradiction trigger re-sets `t_invalid` |
| Rekor inclusion proof is cryptographically valid | only when opt-in `rekor_log_pubkey_pem` was supplied at `mareforma.open()`; submit path + `refresh_unsigned()` verify the Merkle path against the signed checkpoint | deferred — the `rekor_inclusions` sidecar isn't yet round-tripped through `claims.toml`, so restore loses sidecar entries (next-release item) |

### One-page threat model

The substrate names what it does NOT prove right alongside what it
does. Every gate in the code carries a comment to that effect;
this is the consolidated view.

| Threat the substrate DOES catch | Mechanism |
|---|---|
| Direct-SQL `UPDATE` of a signed claim's text / supports / evidence | `claims_signed_fields_no_laundering` trigger |
| Direct-SQL `DELETE` of a signed claim | `claims_signed_no_delete` trigger |
| Resurrection of a retracted claim by flipping status | `claims_update_status_terminal` trigger |
| Born-retracted ESTABLISHED seed riding an honest peer into REPLICATED | `_maybe_update_replicated_unlocked` filters peers AND new claim on `status='open'`; ESTABLISHED-upstream + open required |
| Same-agent self-replication | `c.generated_by != ?` clause in REPLICATED detection |
| Self-validation (validator signs the claim they are validating) | `_refuse_self_validation` |
| LLM-typed validator promoting past REPLICATED | `_refuse_llm_validator` (also applies to contradictions: `_refuse_llm_contradiction_issuer`) |
| Validator who didn't review the cited evidence | `_verify_evidence_seen` — each cited claim_id must exist in the graph with `created_at <= validated_at` |
| Forged validation envelope (different signer, same claim_id) | `db.validate_claim` now `verify_envelope`s against the claimed signer's pubkey from the validators table before any gate fires |
| Replay of a validation envelope onto a different claim | envelope payload-field equality check refuses `claim_id` mismatch |
| Tampered TOML in restore (any signed field, any verdict field, any evidence value) | restore re-derives canonical bytes and refuses on mismatch |
| SQL-injected parallel root validator | singleton-root invariant: any second self-signed root breaks `is_enrolled` for every key |
| Rekor log operator mutates / removes / repositions an entry after submit | opt-in inclusion-proof verification re-derives the Merkle root and checks against the log's signed checkpoint |
| Hostile Rekor returns a `uuid` with path-traversal or query-string characters | `fetch_inclusion_proof` validates uuid against a hex regex before URL substitution |
| Hostile Rekor returns a `logIndex` / `treeSize` that's a float or bool | strict int parsing surfaces as `malformed_proof` |
| `rekor_url` pointing at loopback / private IP / non-HTTPS | `validate_rekor_url` SSRF defense; also called by `fetch_inclusion_proof` and `fetch_log_pubkey` |

| Threat the substrate does NOT catch (deliberate scope) | Why |
|---|---|
| Colluding agents producing fake `REPLICATED` via two `generated_by` strings | `generated_by` is self-declared; no cross-org PKI |
| Misclassified `INFERRED` / `ANALYTICAL` / `DERIVED` | declared by the agent, not verified |
| Fabricated DOI content (publisher silently replaces PDF) | DOIs are HEAD-checked, not content-verified |
| Colluding log operator publishing two checkpoints to different audiences | needs gossip / witness protocols, out of scope for the single-checkpoint trust model |
| Compromised log signing key | mareforma trusts whichever pubkey the caller pinned via TOFU; rotation requires deleting the pin |
| Compromised user signing key | mareforma trusts the local Ed25519 key; key-management is the user's concern |
| Wrong-but-internally-consistent claims | mareforma proves that the agent stood behind the claim cryptographically, not that the claim is true |

### Where each property lives in the code

For the reader who wants to read the actual enforcement:

- **State-machine triggers** — [`mareforma/db.py`](mareforma/db.py) `_SCHEMA_SQL`
  (search for `claims_insert_state_check`, `claims_update_state_check`,
  `claims_update_status_terminal`, `claims_signed_fields_no_laundering`,
  `claims_signed_no_delete`)
- **Convergence detection** — `_maybe_update_replicated_unlocked` in `db.py`
- **Validation gates** — `validate_claim` in `db.py` (substrate-bypass
  defense: cryptographic verify + LLM-type ceiling + self-validation
  refusal + payload field equality + evidence_seen citation gate)
- **Verdict-issuer protocol** — `record_replication_verdict` /
  `record_contradiction_verdict` in `db.py`; trigger
  `contradiction_invalidates_older`
- **Restore proofs** — `_verify_claim_signatures_on_restore`,
  `_verify_and_insert_replication_verdict`,
  `_verify_and_insert_contradiction_verdict` in `db.py`
- **Rekor inclusion verification** — `verify_rekor_inclusion`,
  `verify_merkle_inclusion_proof`, `verify_rekor_checkpoint`,
  `fetch_inclusion_proof`, `fetch_log_pubkey` in
  [`mareforma/signing.py`](mareforma/signing.py)
- **TOFU pubkey pinning** — `_pem_canonical_der` +
  `O_CREAT|O_EXCL` write in [`mareforma/__init__.py`](mareforma/__init__.py)
- **Validator chain walk** — `_verify_chain`, `is_enrolled` in
  [`mareforma/validators.py`](mareforma/validators.py)

## Honest scope

Read [`README.md`](README.md#what-mareforma-is-not) for the bulleted
"What mareforma is NOT" honesty section. The short version: trust is
local to a project's enrolled validators; `classification` and
`generated_by` are self-declared (the substrate is no stronger than
agent discipline); Rekor inclusion is logged-not-proof-verified; DOIs
are HEAD-checked-not-content-verified; contradiction is per-claim;
`EvidenceVector` is GRADE-shaped storage, not GRADE evaluation; no
automated fraud detection beyond the structural invariants the
substrate enforces.

## Engineering discipline — code as audit trail

The substrate carries its own design review forward in time. Three
conventions, applied consistently:

- **Every defensive measure names the threat it blocks.** Each SQL
  trigger comment names the attack chain its `RAISE(ABORT, ...)`
  refuses — e.g. `claims_signed_no_delete` documents that without
  the trigger "an adversary could wipe a Rekor-logged ESTABLISHED
  claim and rewrite claims.toml as if it never existed." The
  contradiction-invalidates trigger carries a `DESIGN RULE — DO NOT
  PROPAGATE DOWNSTREAM` comment with rationale, so a future
  contributor adding transitive falsification has to engage with the
  reasoning rather than discover it from a broken test.
- **Every invariant names what it does NOT prove.** The
  `evidence_seen` substrate check verifies that each cited claim
  exists and predates the validation timestamp; the docstring
  immediately follows with *"this gate cannot prove the validator
  actually opened those claims, only that the claims they cited
  exist and predate validation. That's the strongest property the
  substrate can enforce; everything else rests on the validator's
  honesty."* The same pattern recurs in `_refuse_self_validation`,
  in `_maybe_update_replicated_unlocked`, and in the
  `claims_signed_fields_no_laundering` trigger.
- **Substrate over surface.** When a defect is found, the fix lands
  at the root layer (DB trigger, signed payload field set, state
  machine) rather than in the wrapper. The public Python API
  inherits the property; an in-process caller bypassing
  `EpistemicGraph.validate` and calling `mareforma.db.validate_claim`
  directly meets the same gates. The trust ladder is not bypassable
  via a public path the wrapper happens not to expose. See
  [`CONTRIBUTING.md`](CONTRIBUTING.md#trust-substrate-changes) for
  the full rule.

The result is that any future contributor reading the code reads the
reasoning that produced it — including which properties are
load-bearing and which are intentionally out of scope. This is the
strongest single signal of how the substrate will age.

## See also

- [`README.md`](README.md) — user-facing pitch + honesty section
- [`AGENTS.md`](AGENTS.md) — agent integration guide (the contract
  agents follow when writing to the graph)
- [`SECURITY.md`](SECURITY.md) — threat model + responsible disclosure
- [`CHANGELOG.md`](CHANGELOG.md) — release notes
