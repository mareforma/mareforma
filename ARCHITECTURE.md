# Mareforma: Architecture

## In one sentence

Mareforma is an opinionated wrapper around in-toto Statement v1 +
DSSE PAE + local SQLite, with signed convergence verdicts,
execution-computed grounding (`observe()` records whether a finding's
cited data actually flowed into it), computed model/method lineage, and
a read-side trust map that leads with a finding's effective-independence
number, packaged as a Python library that an AI agent writes to as it
works.

## The lane

Every individual capability mareforma uses exists in mature form
elsewhere: Ed25519 signing (`cryptography`), DSSE envelopes (`in-toto`),
Sigstore transparency (`rekor`), JSON canonicalization (RFC 8785-style),
local-first SQLite (Datasette ecosystem). What is missing in OSS is
the **combination**:
a runtime, opt-in, local Python library that takes those primitives
and gives an agent a place to assert a claim, cite its upstream
evidence, sign the assertion under a key the agent controls, and find
out whether an independent agent already converged on the same
conclusion.

Mareforma is that combination. It is **not** trying to replace:

- W3C PROV-O (richer provenance vocabulary, mareforma is a runtime
  library, not an RDF graph; the graph exports as a PROV-O JSON-LD
  graph via `mareforma/exporters/prov_o.py`)
- FAIRSCAPE's EVI (research-evidence ontology; the schema stays
  mareforma-native and there is no EVI export)
- IETF SCITT (federated supply-chain transparency; mareforma uses Rekor
  as its claim-transparency path)
- Sigstore (transparency for software artifacts, mareforma uses Rekor
  for claim transparency; the protocols are the same shape)
- RO-Crate (FAIR research-object packaging; mareforma exports the graph
  as an RO-Crate 1.2 Process Run Crate via
  `mareforma/exporters/ro_crate.py`)
- MLflow / DVC / W&B (run + dataset versioning, orthogonal; those
  track artifacts, mareforma tracks claims)

## Rails, not trains

Mareforma ships **the rails**: the storage layer, the signing
discipline, the trust-ladder state machine, the convergence-detection
SQL, the restore-from-TOML recovery path. What it deliberately does
**not** ship, the **trains** that produce verdicts, lives outside
the OSS:

- **Semantic-cluster verdicts** (which embedding model, which similarity
  threshold, which clustering algorithm, research-domain specific)
- **Cross-method verdicts** (when do two analytical pipelines count as
  "different methods", domain-specific)
- **Contradiction-detection verdicts via NLI** (which NLI model, which
  contradiction threshold, research-domain specific)
- **Sakana / FutureHouse-style end-to-end AI scientist agents** (those
  consume mareforma; they don't live in it)

The verdict-issuer protocol in mareforma (`record_replication_verdict`
and `record_contradiction_verdict`) is the public API that any of those
trains can write to. The OSS core accepts any signed verdict from
an enrolled validator; the predicates that produce those verdicts are
out of scope by design. The OSS core stays narrow and verifiable;
the trains plug in through the public protocol.

## Data flow

```
agent
  │
  │ assert_claim(text, classification, supports=[...], generated_by=...)
  ▼
EpistemicGraph (mareforma/_graph.py)
  │
  │ ─ classifies (caller-supplied; mareforma does not verify)
  │ ─ canonical_statement(claim_fields) → bytes (NFC + sorted keys + no whitespace)
  │ ─ in-toto Statement v1 wrapping (mareforma/_statement.py)
  │ ─ DSSE PAE encoding (mareforma/signing/core.py)
  │ ─ Ed25519 signature
  ▼
db.add_claim (mareforma/db/core.py)
  │
  │ ─ BEGIN IMMEDIATE
  │ ─ prev_hash chain extension under lock
  │ ─ INSERT INTO claims (signed envelope + ev_* columns + statement_cid
  │                       + asserter_keyid denormalized from the envelope)
  │ ─ COMMIT
  │ ─ optionally submit to Rekor (if rekor_url= was passed)
  │ ─ _maybe_update_replicated() : detect convergence
  │ ─ _backup_claims_toml() : write the TOML mirror (post-commit;
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

## Trust: the derived axes and the stored ladder

Trust reads off two axes Mareforma **derives on every read**, not off a stored
label. `status` per `content_id` is the state of the answer (a `Status` enum
value: `UNTESTED` → `PRELIMINARY` → `CONVERGENT`, plus `REFUTED` and
`CONTESTED`); `question_status` per `frame_id` is the state of the question
(`consistent` / `divided`). Both come from `graph.proposition_status(prop)`, and
the read-side trust map leads with the effective-independence number rather than
a single word. These are the vocabulary a reader should reach for.

The **stored `support_level` ladder** below is the legacy promotion mechanism a
claim carries. It is still live and still gates as described, but its `REPLICATED`
and `ESTABLISHED` public labels are deprecated for v0.4.0 (see the note after the
rules). Read it as the deprecated stored axis, not as the trust vocabulary.

```
PRELIMINARY ──(≥2 distinct signers share ESTABLISHED upstream)──▶ REPLICATED ──(graph.validate())──▶ ESTABLISHED
```

Three rules govern the stored ladder:

1. **PRELIMINARY → REPLICATED is automatic, structural, and gated.**
   The new claim and a candidate peer must share at least one
   `ESTABLISHED` upstream in `supports[]` and carry **distinct, non-NULL
   `asserter_keyid`** values (the signer keyid denormalized from each
   claim's signature_bundle). Promotion keys on the signer axis. The
   load-bearing model-independence signal is the read-side
   effective-independence number the trust map reports, not this promotion.
   The promotion path does run a `model_distinct_pair` filter, but it stays
   inert on the primary path: a claim's finding model lineage is written
   after promotion runs, so both sides read absent and the filter passes
   everything through (see "Execution-observed grounding" and the trust
   layer below for where the model axis is enforced, on read). An unsigned
   (NULL keyid) claim is not a
   distinct signer and is never promoted, so two legacy NULL-keyid rows do
   not read as two signers. `generated_by` is a display label only and
   plays no part in the gate. `artifact_hash` is a secondary equal-data
   collapse: two converging claims that carry the same non-NULL hash are a
   byte-identical rerun, so they collapse to one line and do not promote on
   their own; a distinct hash, or an absent hash on either side, does not
   block the convergence. Status and transparency-log gates apply too; see
   `_maybe_update_replicated_unlocked` in db/core.py. Distinct keys are a
   cryptographic distinctness signal, NOT a proof of apparatus
   independence. REPLICATED is a convergence signal, not a truth claim.
   Opening with `strict_promotion=True` (opt-in, off by default) turns the
   equal-data collapse into a hard gate: a pair then promotes only when
   BOTH sides carry non-NULL data. The root validator signs that gate into
   the project policy, one-way, so it binds every later opener and not just
   the handle that asked for it. Independence itself is reported on its
   own axis by the read-side trust map (`graph.trust_map`), which surfaces
   the effective-independence number and marks it `UNVERIFIABLE` when the
   supporting lineage is too soft to certify a distinct model or when every
   validator traces to a single trust root.
2. **REPLICATED → ESTABLISHED is human-only.** `graph.validate()`
   requires an enrolled validator key whose `validator_type` is
   `'human'`. LLM-typed validators may sign validations but cannot
   promote past REPLICATED. A validator keyid that equals ANY
   `asserter_keyid` in the converging set is refused: a participant cannot
   witness its own convergence into ESTABLISHED.
3. **No back-transitions.** The state-machine triggers refuse any
   ESTABLISHED → REPLICATED or REPLICATED → PRELIMINARY UPDATE. Status
   changes (open / contested / retracted) live on a separate axis
   that is mutable but `retracted` is terminal.

The `seed=True` bootstrap is the only way to insert at ESTABLISHED
directly. It exists to break the chicken-and-egg of "REPLICATED needs
an ESTABLISHED upstream that doesn't exist on a fresh graph yet", and
it is gated to enrolled human-typed validators only.

**`REPLICATED` and `ESTABLISHED` are deprecated public labels, removed at
v0.4.0.** A single support word never carried the independence a reader needs,
so the public surface leads with the derived axes above and the
effective-independence number the trust map reports, not a rung name. The stored
`support_level` strings and the promotion machinery are unchanged this release;
only the two public labels `mareforma.REPLICATED` and `mareforma.ESTABLISHED` are
retired. They resolve for one release as string aliases and emit a
`DeprecationWarning`, and v0.4.0 removes them (the `seed=True` bootstrap is
deprecated on the same schedule and gains its replacement anchor there). Read
`status`, `question_status`, and the trust map's independence axis instead of a
rung name.

## Trust map

The read-side CLI commands expose a claim's trust state without
adding any signed field:

- `mareforma map <claim>` renders the per-claim trust map
  (`mareforma/trust_map.py`): each property (grounding, independence,
  standing, witnessing, and the rest) placed at its tier. `COMPUTED`
  means derived directly from stored evidence, `PROXIED` means computed
  through a proxy signal whose bound is named, `DEFERRED` means not
  evaluated this release with the residual named rather than guessed. The
  independence axis leads the map: it reports the effective-independence
  number (the count of pairwise-distinct model, data, and signer
  supporting checks) and reads `UNVERIFIABLE` where a supporting line's
  model lineage is too soft to certify a distinct model.
  `--json` and `--html` emit the same map for CI or review.
- `mareforma verify <claim>` re-checks the signatures, the
  grounding-to-citation binding, and the displayed support level, and
  exits on a stable four-code contract: `0` verified, `1` tampered, `2`
  unverifiable, `3` usage error. Example 06 wires it as a CI gate.
- `mareforma diagnose -- python run.py` runs a target in-process under
  the grounding observer and reports what data actually flowed and
  where a silent fallback hid; with `--cites` it also computes the
  grounding verdict for those sources.
- `mareforma audit --findings findings.json -- python run.py` extends
  diagnose to per-finding receipts for a pipeline that never imports
  mareforma: the mapping names what each finding claims to cite, one
  observed run yields one verdict per finding against that finding's
  cited set, and each verdict is emitted as a plain receipt (feeds
  `mareforma measure`) plus a DSSE-signed envelope `mareforma verify`
  checks from public material alone. Nothing the target prints or
  writes enters a verdict; the target does share the auditor's
  interpreter, though, so the receipts grade a pipeline that does not
  attack its auditor, a target written to defeat the audit could
  fabricate what the observer records, and the signature attests the
  auditor's observation, not the target's honesty. `--corpus` iterates
  run specs, one fresh interpreter per run, resumable (resume honors
  only a run record signed by the auditor's key), with a crashing
  target's partial receipts and its own exit code recorded.

The `map`, `verify`, `diagnose`, and `audit` commands live in
`mareforma/cli.py` (the audit runner in `mareforma/audit.py`); the tier
semantics and property placement live in `mareforma/trust_map.py`.

## Trust layer

The stored ladder above is the legacy per-claim axis. The trust layer
(`mareforma.trust`) is where the derived axes come from: a structured model for a
single content-addressed proposition, computed on every read. It is additive:
seven new tables, schema stays at v1, and every finding still rides a signed
claim. `graph.proposition_status(prop)` returns both derived axes under the keys
`status` (the answer, per `content_id`, a `Status` enum value) and
`question_status` (the question, per `frame_id`), alongside a `frame_status` key
deprecated for v0.4.0 in favour of `question_status`.

```
Proposition (content_id, frame_id)
  ├─ Prediction (the pre-registered rule, append-only)
  └─ Finding ──▶ signed claim
        └─ EvidenceLine[] (data_id + model lineage) ──▶ Contrast ──▶ EffectEstimate
```

A finding carries one evidence line or many. The single-line case is the common
one; a multi-line finding records several datasets or arms under one proposition
and prediction.

A prediction is append-only, so a plan written by a release with a wider alpha
bound can state a rule no gate can run and strand its evidence for good.
`retire_plan` is the way out: it records the retirement (`plan_retirements`)
and registers the same rule at an alpha the gate can run, both as signed
attestations, and leaves the retired row exactly as registered. Only a plan
whose rule cannot be run is retirable, and those lines count zero as they
stand, so retirement recovers a dropped line and never drops a counted one.

Three rules:

1. **The bearing is computed, not declared.** `compute_bearing(estimate, prediction)`
   in [`mareforma/trust/bearing.py`](mareforma/trust/bearing.py) returns
   supports / refutes / neutral from the pre-registered rule and the realised
   numbers. An agent cannot relabel a refutation as support. Each evidence line
   gets its own bearing, recomputed on read, so a multi-line finding whose lines
   disagree is counted line by line, not off a single per-finding label. The rule
   is also expressible as an ordered short-circuit `gates[]` chain (`gates_for`,
   `evaluate_gates`) over the existing prediction columns; the single binary gate
   is the one-element chain, bearing-identical to `compute_bearing`.
2. **Status counts independent lines by distinct model, data, and signer.**
   `compute_status` in [`mareforma/trust/status.py`](mareforma/trust/status.py)
   reads `independent_support` and `independent_refute` (UNTESTED, PRELIMINARY,
   CONVERGENT, REFUTED, CONTESTED). CONVERGENT is a convergence marker, not a
   corroboration or independence verdict: it says two or more lineage-distinct
   supporting lines converge, and names cross-model error correlation as the
   unmodeled residual. A supporting line counts as independent only when it
   stands on a **distinct model/method** as well as a distinct signer and
   dataset: two checks on the same model are one line of evidence, not two, so a
   same-model rerun no longer reaches CONVERGENT. The distinct-model
   test reads the computed model lineage on each evidence line; soft lineage
   (PROXY or UNVERIFIABLE) cannot certify a distinct model and never earns a
   second line, and where a supporting line's lineage is soft the effective count
   the trust map surfaces reads `UNVERIFIABLE` rather than a confident number. A
   **human check counts on the status ladder but is not certified by the map**:
   a supporting finding with no observed model call, signed by an enrolled
   `validator_type='human'` validator, needs no distinct model on the ladder (a
   human is not a model) and is never folded into a model root there. The
   per-finding map disclosure holds it soft instead: the type is self-declared,
   defaults to `'human'`, and no person attested to the finding, so the map
   reads `UNVERIFIABLE` rather than certifying a line nothing observed. One
   signer still
   contributes at most one support and one refute, and re-running the same
   dataset adds nothing. A line that names a signer counts on the
   distinct-signer axis only when that signer is an enrolled validator whose
   signature verifies: an unenrolled or unauthenticated signer counts on no
   axis and is disclosed, so an unregistered key cannot manufacture a second
   independent line. Only a legacy NULL-keyid line keeps the run-token
   fallback. Where dataset bytes are supplied the `data_id` is
   content-addressed (`sha256:`); a string `data_id` stays a flagged fallback.
   Legacy findings whose line carries no model lineage keep the distinct-signer
   axis so their counts are preserved. It is a versioned policy, recomputed on
   read, never baked into the schema. Distinct-model is binary this release; the
   graded cross-model residual (how far apart two distinct models are) is named
   but not computed.
3. **Identity is the frozen kernel.** `content_id` (the answer) and `frame_id`
   (the question) are sha256 over RFC 8785 canonical bytes of normalized tokens
   ([`mareforma/trust/proposition.py`](mareforma/trust/proposition.py)). Same
   truth conditions collapse to one node; contrary directions on a shared frame
   contradict.

The graph methods (`register_proposition`, `register_plan`, `submit_finding`,
`assert_finding`, `proposition_status`, `query_frame`) live in
[`mareforma/_graph.py`](mareforma/_graph.py); the SQL is in
[`mareforma/trust/_store.py`](mareforma/trust/_store.py) and the six tables in
`db/_schema_sql.py`. `register_plan` pre-registers the decision rule as its own
signed plan attestation before the numbers are seen, and `submit_finding` binds
an outcome to it, signing the plan → finding edge into the finding claim's
`supports[]`; `assert_finding` is the one-shot that composes both.
Pre-registration only means something when the rule is bound before the run
produces outcomes, so `submit_finding` refuses a plan whose `registered_at`
post-dates the run's first observed execution (its earliest prior finding under
the same `generated_by` run token) with `PostHocPlanError`, rather than
laundering a post-hoc plan as a pre-registration. A one-shot `assert_finding`
synthesises its plan with no pre-registration claim and never raises it. Both
write paths re-validate the plan's alpha against the same `(0, 0.5)` bound the
`Prediction` constructor holds, so a rule reaching persistence past the
constructor cannot mint an un-gateable plan; that stranded state stays the
property of a release that predates the bound.

**One boundary for every gate input.** A trust gate decides a proposition's
status from database rows, and the rows the count reads are denormalized
columns nothing signed. `mareforma/trust/_gate.py` owns their derivation: one
verifier (`_derive_units`) reached through two entry points, `verified_gate_inputs`
for the live read path and `verify_gate_inputs_or_refuse` for restore, so both
answer from the same checks rather than drifting. It hands back a frozen
`GateInputs` built behind a module-private token, so a caller cannot feed a raw
row to the counter by constructing one. Per gated line it recomputes the
bearing from the stored estimate, reads the signer and model axes from the
claim's signed envelope, and re-derives the line's gate plan two ways: the
`findings.plan_id` column must match the plan the finding's own claim recorded,
and the `predictions` rule columns must hash to the `plan_id` keying them
(`compute_plan_id`).

Three checks re-derive the rest of the verdict from signed material. A finding
binds a digest of its ordered `(data_id, control_type, estimate)` line set into
its claim's signed `finding_record`, so the verifier recomputes that digest from
the live rows and drops the whole finding when they no longer match: this is what
catches an altered estimate value that still recomputes a clean bearing, or a
deleted estimate, contrast, or line. The count enumerates from the signed
`findings` row and LEFT JOINs downward, so a deleted evidence row leaves the
finding visible with NULLs rather than vanishing from an inner join before its
digest can be checked. A finding's signed claim text is the rendering of the
proposition it attests, so the live proposition row must render back to it
(both sides normalised), which catches a rewritten `findings.content_id` or
proposition text that re-points a signed finding at a different sentence. And a
line whose claim names a signer (a non-NULL `asserter_keyid`) counts on the
distinct-signer axis only when that signer authenticates as an enrolled
validator whose signature verifies; a named signer that does not authenticate
counts on no axis and is disclosed, rather than falling back to the run token a
forged claim also controls. Only a NULL-keyid legacy line keeps that fallback.

A line that fails any check is dropped and disclosed on the
health channel on the read path, and refuses the whole recovery on restore. A
plan whose own rule cannot run is resolved through its retirement before the
line drops. `proposition_status` and `effective_independence` carry a
`lines_skipped` tally so a dropped line does not read as a silently lower count,
and `proposition_status` carries a `post_hoc` flag naming a count that
rests on a replacement or one-shot plan, so a reader can tell a pre-registered
gate from one chosen after the numbers were seen.

## Contestation model

Contradiction in mareforma is a **per-claim demotion**, not a
transitive falsification. When an enrolled validator signs a
`record_contradiction_verdict(member, other)`, mareforma sets
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
the `contradiction_invalidates_older` trigger in `db/_schema_sql.py` for context.

## Signing surface

Every signed payload in mareforma uses DSSE PAE (`dsse_pae(payload_type,
body)`) with these payload types:

| Payload type | What it signs |
|---|---|
| `application/vnd.in-toto+json` (Statement v1) | Per-claim assertion (text + classification + supports + contradicts + source + artifact_hash + evidence + created_at, plus an optional versioned `observed_grounding` verdict when the observer recorded one) |
| `application/vnd.mareforma.validator-enrollment+json` | Per-validator enrollment (keyid + pubkey + identity + validator_type + parent) |
| `application/vnd.mareforma.validation+json` | Per-validation event (claim_id + validator_keyid + validated_at + evidence_seen) |
| `application/vnd.mareforma.seed-claim+json` | Per-seed bootstrap (claim_id + validator_keyid + seeded_at) |
| `application/vnd.mareforma.replication-verdict+json` | Per-replication verdict from an issuer |
| `application/vnd.mareforma.contradiction-verdict+json` | Per-contradiction verdict from an issuer |

The bundle export (`export_bundle.py`) wraps the JSON-LD graph in an
in-toto Statement v1 and signs it over the DSSE PAE encoding
(`application/vnd.in-toto+json`), so it verifies with standard DSSE
tooling. `verify_bundle` checks the bundle signature AND, for each
claim, its own asserter signature bound to the presented content, the
enrolled validator chain to a single root (which must be the bundle
signer), and the displayed support level (ESTABLISHED against a
validator-signed validation envelope, REPLICATED against distinct-signer
corroboration). Editorial status (`retracted` / `contested`) carries no
signature and stays exporter-attested, and so does completeness: a
verified bundle attests the claims it carries, not that they are all the
claims in the graph. A claim removed together with its subject entry and
re-signed by the same key verifies clean.

### Canonicalization: RFC 8785 strict

`canonicalize` (in [`mareforma/_canonical.py`](mareforma/_canonical.py))
normalizes every string in the payload to Unicode NFC, then serializes
via the `rfc8785` library, a strict implementation of RFC 8785 (JSON
Canonicalization Scheme, JCS). The `rfc8785` dependency is what makes
the output JCS-strict; earlier the code used
`json.dumps(sort_keys=True, ...)`, which was only JCS-shaped, not
JCS-strict.

What strict JCS gets us:

- Keys sorted lexicographically by UTF-16 code unit at every nesting
  level (JCS §3.2.3).
- No whitespace, minimal JSON string escape set, UTF-8 output
  (JCS §3.2.1-§3.2.2).
- **Numbers per the ECMAScript `Number.prototype.toString` algorithm**
  (JCS §3.2.2.3). `1.0` renders as `1`; `1e10` renders as
  `10000000000`; exponent boundaries follow ES rules. This is the
  load-bearing difference vs. Python's stdlib `json.dumps`: the day
  mareforma adds a float-valued field, a Go / Rust / JavaScript
  verifier re-canonicalizing per RFC 8785 will produce the same bytes
  and verify the same signature.
- `NaN` / `±Infinity` are rejected (JSON has no representation; RFC
  8785 explicitly forbids them).
- Integers outside the IEEE-754 double-precision safe-integer range
  are rejected (JCS would otherwise lose precision on round-trip).
- Dict keys that NFC-normalize to the same string raise `ValueError`
  rather than silently dropping a value. Canonical JSON requires
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
serialization at all. It's the same bytes any in-toto verifier
(`in-toto-golang`, the Sigstore stack) will produce.

## Storage layer

SQLite, WAL mode, `check_same_thread=False`, `PRAGMA foreign_keys = ON`,
minimum version 3.30.0 (enforced at `open_db()`).

Tables:

- `claims`: every assertion. Includes denormalized `ev_*` columns for
  query, the full `evidence_json` for round-trip, the
  `signature_bundle` DSSE envelope, a `prev_hash` chain link, and the
  `convergence_retry_needed` flag set by `_maybe_update_replicated`
  when a swallowed error needs operator follow-up.
- `validators`: per-project enrolled-validator chain, rooted at a
  self-signed row. Singleton-root invariant: more than one self-signed
  row → entire chain forfeit. Append-only and no-delete at the trigger
  level: a swapped `pubkey_pem` would make a forged signature verify, and
  a dropped row would rewrite who the graph trusts.
- `replication_verdicts` / `contradiction_verdicts`: signed verdicts
  from enrolled issuers. Append-only at the trigger level.
- `rekor_inclusions`: sidecar recording every successful Rekor
  submission, independent of whether the claims-row UPDATE that
  attaches the rekor coords to `signature_bundle` succeeded. Closes
  the divergence window where Rekor would have a permanent public
  record while the local row still said `transparency_logged=0`:
  `refresh_unsigned` consults this table to replay the UPDATE
  instead of re-submitting (no duplicate Rekor entry). Append-only
  at the trigger level (UPDATE and DELETE both refused), so a
  SQL-writer cannot launder forged Rekor coords through the replay
  path.
- `claims_fts`: FTS5 virtual table (independent of `claims`, not
  `content=` linked) for substring + tokenized search.

The seven trust-layer tables (`propositions`, `predictions`,
`plan_retirements`, `findings`, `evidence_lines`, `contrasts`,
`effect_estimates`) are described in the Trust layer section above.

SQL triggers enforce the state machine, the append-only invariants on
signed predicate fields, the no-delete rule on signed claims, the
verdict tables' append-only-and-no-delete invariants, the rekor-
inclusions sidecar's same invariants, the contradiction-invalidates-
older logic, and the FTS sync. The gate-input tables the read path reads
to derive a proposition's status carry the same append-only-and-no-delete
guard: `propositions`, `contrasts`, and `effect_estimates`, alongside
`validators` and the `plan_id`-inclusive `predictions` UPDATE guard, so an
in-place edit or delete of any of them is refused at the storage layer, not
only caught on read. A tampered Python interpreter cannot relax these rules.

## What survives restore

`claims.toml` is the canonical source for `mareforma.restore(project_root)`:
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
   not preservation; see below.
5. Replays all verdicts in chronological order so the
   `contradiction_invalidates_older` trigger sets earliest-first.

Failure of ANY check rolls the entire restore back. Restore is
`fresh-only` and `fail-all-or-nothing` by design: it rebuilds a fresh
graph and does not merge into an existing one.

### Two known gaps in what TOML guarantees

**Chain order is not externally anchored.** A tampered TOML that
reorders claims (swap two `created_at` values) restores to a different
but internally-consistent chain. The signatures bind canonical statement
bytes, not chain position. For tamper-evidence across restore boundaries,
the per-claim Rekor entry is the external anchor, and Merkle inclusion
proof verification is available opt-in via the pinned log key
(`rekor_log_pubkey_pem`).

**The TOML write lags the SQLite commit.** `_backup_claims_toml` runs
**after** the INSERT/UPDATE transaction commits. A process crash between
`COMMIT` and the TOML write leaves a row in `graph.db` that's missing
from `claims.toml`. The next mutation rewrites the TOML from current DB
state, so the crash window closes on the next successful write. For a
clean recovery snapshot, finish any in-flight writes before snapshotting
the TOML.

## Mareforma at a glance

A 30-minute audit map. Each row links a mareforma property to the
exact mechanism that enforces it and the specific threat it
defends against. Designed for the reader who wants to verify
mareforma's invariants without scrolling through thousands of lines of
`db/core.py`.

### State-machine transitions

These are the transitions of the stored `support_level` ladder, the legacy
per-claim promotion axis (its `REPLICATED` / `ESTABLISHED` public labels are
deprecated for v0.4.0). The machine itself is unchanged and still enforced by the
triggers below.

```
                seed=True               graph.validate()
                   │                          │
                   ▼                          ▼
              ┌─────────────┐            ┌─────────────┐
              │ ESTABLISHED │ ◄───────── │ REPLICATED  │
              └─────────────┘            └─────────────┘
                                              ▲
                                              │ ≥2 claims, distinct
                                              │ asserter_keyid, sharing
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
| signed claims are append-only over the predicate | `claims_signed_fields_no_laundering` | direct-SQL UPDATE of `text` / `classification` / `generated_by` / `supports_json` / `contradicts_json` / `source_name` / `artifact_hash` / `ev_*` / `evidence_json` / `observed_grounding` / `statement_cid` / `prev_hash` / `created_at` / `asserter_keyid` / `predicate_payload` on a row with `signature_bundle IS NOT NULL`, and any UPDATE that sets `signature_bundle` back to NULL on such a row |
| a signed claim is promoted only by a promotion path | `claims_signed_promotion_backed` | direct-SQL UPDATE of `support_level` on a row with `signature_bundle IS NOT NULL` outside a promotion window; the level itself is re-derived from signed evidence on every read, so a row promoted after the trigger is dropped is served `verified=False` |
| signed claims cannot be deleted | `claims_signed_no_delete` | the wipe-and-rewrite attack where a Rekor-logged ESTABLISHED claim is deleted from `graph.db` and `claims.toml` is regenerated as if it never existed |

### Append-only sidecars

| Table | Trigger | Refuses |
|---|---|---|
| `rekor_inclusions` | `rekor_inclusions_append_only` + `rekor_inclusions_no_delete` | any UPDATE or DELETE; once Rekor witnessed a claim, the saga's step-3 record is immutable; SQL writers cannot launder forged Rekor coords through the recovery path |
| `replication_verdicts` | `replication_verdicts_append_only` + `replication_verdicts_no_delete` | UPDATE of signed columns; DELETE of any row; verdicts are signed evidence, not editable records |
| `contradiction_verdicts` | `contradiction_verdicts_append_only` + `contradiction_verdicts_no_delete` | same; plus the `contradiction_invalidates_older` AFTER INSERT trigger that sets `t_invalid` on the older of two referenced claims (lex-tie-break, idempotent via `WHERE t_invalid IS NULL`) |
| `findings` | `findings_append_only` + `findings_no_delete` | any UPDATE or DELETE; a finding's row (its `plan_id`, `content_id`, bearing) is written once and gates every evidence line under it, so an in-place edit is refused. These are reconciled onto an existing graph by the same `sqlite_master` text comparison the claims guards use, and their bodies name no per-connection function, so a connection that registered none still compiles an `UPDATE` against the table |
| `evidence_lines` | `evidence_lines_append_only` + `evidence_lines_no_delete` | any UPDATE or DELETE; a line's `data_id` and model lineage are the distinctness axes the count reads, written once at submit |
| `propositions` | `propositions_append_only` + `propositions_no_delete` | any UPDATE or DELETE; a proposition is content-addressed and frozen, so an in-place rewrite of its text would leave the evidence backing a different sentence, and a delete would drop a node findings still point at |
| `contrasts` | `contrasts_append_only` + `contrasts_no_delete` | any UPDATE or DELETE; a contrast is written once by `insert_finding` and a delete would drop it from the count so a refutation reads as consensus |
| `effect_estimates` | `effect_estimates_append_only` + `effect_estimates_no_delete` | any UPDATE or DELETE; the demonstrated convergent-to-refuted attack flips an estimate value in place, and a delete removes a line from the count without disclosure |
| `validators` | `validators_append_only` + `validators_no_delete` | any UPDATE or DELETE; swapping a row's `pubkey_pem` makes a forged signature verify, and dropping a row rewrites who the graph trusts. Idempotent re-registration uses `ON CONFLICT DO NOTHING`, which fires neither trigger |
| `predictions` | `predictions_append_only` + `predictions_no_delete` | UPDATE of any immutable rule column, `plan_id` included (rewriting the primary key re-points a whole rule at a different identity and vanishes its evidence from the count); any DELETE. The UPDATE guard is reconciled through the managed drop-and-recreate so its `plan_id` watch reaches a graph whose trigger predates it |

### Signed-fields vs mutable-fields

The DSSE envelope signs an in-toto Statement v1 whose predicate
binds the values in `mareforma.signing.SIGNED_FIELDS` plus the
opaque `evidence` dict. Any post-INSERT mutation of those values
on a signed row is refused at the SQL layer.

| Field | Signed (predicate) | Mutable on a signed row |
|---|---|---|
| `claim_id` | ✓ | no |
| `text` | ✓ | no |
| `classification` | ✓ | no |
| `generated_by` | ✓ | no |
| `supports_json` | ✓ | no |
| `contradicts_json` | ✓ | no |
| `source_name` | ✓ | no |
| `artifact_hash` | ✓ | no |
| `created_at` | ✓ | no |
| `evidence_json` + `ev_*` | ✓ | no |
| `statement_cid` | derived from signed bytes | no |
| `prev_hash` | derived (chain link) | no |
| `status` | not signed | ✓ (one-way: open → contested → retracted) |
| `support_level` | not signed | ✓ (one-way ladder) |
| `validated_by` / `validated_at` / `validation_signature` / `validator_keyid` | not signed (validation is its own envelope) | written by `validate_claim` only |
| `asserter_keyid` | not signed (denormalized from the signature_bundle's asserter signature) | written at insert only; the envelope stays authoritative |
| `signature_bundle` | self-referential | only rewritten by `mark_claim_logged` to attach a Rekor block; payload + signatures bytes must be byte-identical to the existing value, only the optional `rekor` top-level key may differ |
| `unresolved` / `transparency_logged` / `convergence_retry_needed` / `t_invalid` | not signed (operational flags) | ✓ (gated mutations, `t_invalid` by trigger only) |

### What `restore()` proves vs what the live DB proves

| Property | Proved by live DB | Proved by `restore()` |
|---|---|---|
| The claim was signed by an enrolled key at insert time | yes (`signature_bundle` set + validator chain walk) | yes, re-verifies every envelope against the validator's PEM, refuses orphan signers |
| The row's signed fields match the envelope | trigger blocks mutation; row never drifts unless a SQL-tamper bypasses Python | yes, re-derives canonical bytes and compares to `predicate.*`, refuses on mismatch |
| The evidence dict hasn't been tampered after signing | trigger blocks `ev_*` and `evidence_json` mutation | yes, re-derives the canonical evidence dict and compares to `predicate.evidence` |
| `statement_cid` cross-check | column never directly written by user code | yes, re-derives from the row's fields + evidence and compares to the stored `statement_cid` |
| Validation envelope binds this claim | the gates: `_extract_validation_signer_keyid`, `_refuse_llm_validator`, `_refuse_self_validation`, `_verify_evidence_seen`, envelope/kwarg agreement; cryptographic verify on the envelope | yes, verifies the validation envelope's signature, then checks `claim_id` / `validator_keyid` / timestamp / `evidence_seen` fields against the row |
| Contradiction verdict is signed by an enrolled validator | enforced at `record_contradiction_verdict`; chain walk via `is_enrolled` | yes, replays each verdict envelope in `created_at` order, verifies before INSERT, the contradiction trigger re-sets `t_invalid` |
| Rekor inclusion proof is cryptographically valid | only when opt-in `rekor_log_pubkey_pem` was supplied at `mareforma.open()`; submit path + `refresh_unsigned()` verify the Merkle path against the signed checkpoint | yes, `rekor_inclusions` sidecar round-tripped through `claims.toml`; `restore()` replays entries and (when `rekor_log_pubkey_pem` supplied) re-verifies each inclusion proof against the pinned key. A TOML file that predates the sidecar restores with `RekorSidecarSectionAbsentWarning` |

### One-page threat model

Mareforma names what it does NOT prove right alongside what it
does. Every gate in the code carries a comment to that effect;
this is the consolidated view.

| Threat mareforma DOES catch | Mechanism |
|---|---|
| Direct-SQL `UPDATE` of a signed claim's text / supports / evidence | `claims_signed_fields_no_laundering` trigger |
| Direct-SQL `DELETE` of a signed claim | `claims_signed_no_delete` trigger |
| Direct-SQL resurrection of a retracted claim by flipping status | `claims_update_status_terminal` trigger. It fires on UPDATE, so it covers the SQL edit and not the backup path; see the table below |
| Born-retracted ESTABLISHED seed riding an honest peer into REPLICATED | `_maybe_update_replicated_unlocked` filters peers AND new claim on `status='open'`; ESTABLISHED-upstream + open required |
| Same-signer self-replication | distinct, non-NULL `asserter_keyid` required in REPLICATED detection; a single signer's two claims share one keyid and do not converge |
| Self-validation (validator signs the claim they are validating) | `_refuse_self_validation` |
| Self-validation across the converging set (a participant validating its own convergence) | `_refuse_self_validation_across_set` refuses a validator keyid equal to any `asserter_keyid` in the converging set |
| LLM-typed validator promoting past REPLICATED | `_refuse_llm_validator` (also applies to contradictions: `_refuse_llm_contradiction_issuer`) |
| Validator who didn't review the cited evidence | `_verify_evidence_seen`, each cited claim_id must exist in the graph with `created_at <= validated_at` |
| Forged validation envelope (different signer, same claim_id) | `db.validate_claim` `verify_envelope`s against the claimed signer's pubkey from the validators table before any gate fires |
| Replay of a validation envelope onto a different claim | envelope payload-field equality check refuses `claim_id` mismatch |
| Direct-SQL forgery of a high-trust row served from the read path | verify-on-read: `get_claim` / `query` / `query_provenance` re-verify the asserter bundle (REPLICATED and ESTABLISHED) plus the validation envelope (ESTABLISHED), and hold the bundle's signed predicate against every signed field on the row, so a rewritten field cannot hide under an envelope that still verifies; a forged, tampered or removed signature is excluded from `query` and flagged `verified=false` from `get_claim`, never raising. Legacy unsigned rows (no bundle, no `asserter_keyid`) are verify-exempt |
| Direct-SQL promotion of a row whose signature still verifies | `support_level` is not signed, so the read path re-derives it: a stored REPLICATED (and the rung an ESTABLISHED climbed through) must show a signature-verified replication verdict naming the claim, or distinct-signer convergence on a shared ESTABLISHED anchor. The same rule refuses the row on restore. A row that cannot show it is excluded and flagged exactly like a signature mismatch |
| A promoted row read as healthy while its level is forged or its envelope tampered | `mareforma status` re-verifies the promoted rows (`count_unverified_promoted`, the same per-row gate `get_claim` applies) rather than counting stored levels, so the traffic light cannot read green while any REPLICATED / ESTABLISHED row fails re-verification. It also names `convergence_retry_pending`, promotions a swallowed convergence check left flagged, so that stuck state is visible instead of silent |
| Direct-SQL rewrite of a gate input the count reads (a finding's `plan_id`, a `predictions` rule column, a re-pointed `plan_retirements.superseded_by`, an in-place estimate value, a re-pointed `findings.content_id` or proposition text, a fabricated `asserter_keyid`) | the `_gate.py` boundary re-derives each on read: `findings.plan_id` against the plan the finding's own claim recorded, the rule columns against the `compute_plan_id` that keys them, the estimate line set against the signed `finding_record` digest, the proposition against the finding's signed claim text, and the signer against the claim's verifying envelope. A rewrite that would reflip a bearing or re-point a finding drops the line and discloses it rather than moving the count in silence; restore refuses the recovery. The append-only triggers on `findings`, `evidence_lines`, `propositions`, `contrasts`, and `effect_estimates` refuse the naive UPDATE or DELETE ahead of the read check |
| Tampered TOML in restore (any signed field, any verdict field, any evidence value) | restore re-derives canonical bytes and refuses on mismatch |
| SQL-injected parallel root validator | singleton-root invariant: any second self-signed root breaks `is_enrolled` for every key |
| Rekor log operator mutates / removes / repositions an entry after submit | opt-in inclusion-proof verification re-derives the Merkle root and checks against the log's signed checkpoint |
| Hostile Rekor answers the re-fetch with a different entry it really did log | `fetch_inclusion_proof` requires the returned uuid to be the one asked for, and `verify_rekor_inclusion` binds the proven body to the claim's payload hash + signature (`entry_claim_mismatch`) |
| Hostile Rekor returns a `uuid` with path-traversal or query-string characters | `fetch_inclusion_proof` validates uuid against a hex regex before URL substitution |
| Hostile Rekor returns a `logIndex` / `treeSize` that's a float or bool | strict int parsing surfaces as `malformed_proof` |
| `rekor_url` pointing at loopback / private IP / non-HTTPS | `validate_rekor_url` SSRF defense; also called by `fetch_inclusion_proof` and `fetch_log_pubkey` |

| Threat mareforma does NOT catch (deliberate scope) | Why |
|---|---|
| Colluding agents producing fake `REPLICATED` via two signing keys | distinct `asserter_keyid` is a cryptographic distinctness signal, not a proof of apparatus independence: one party can hold two keys. REPLICATED is a convergence signal, not a truth claim; the real trust anchor is human-validated ESTABLISHED. `single_trust_domain` discloses when all validators share one root, but does not prevent Sybils |
| A gate input flipped consistently across every table that carries it, with the guarding triggers dropped first | `plan_id` and `preregistered` are not in `SIGNED_FIELDS`, so the read path re-derives `plan_id` against the value the finding's claim recorded (itself guarded only by `claims_signed_fields_no_laundering`) and cannot re-derive `preregistered` at all. A writer with SQL access who drops the append-only triggers and rewrites both `findings.plan_id` and the claim's recorded copy, or flips `preregistered` on a plan, defeats the read check: this binding rests on the triggers plus the claim signature, not on the gate input being a signed field. A single-column edit is still caught |
| Erasure of a refutation by deleting the `findings` row it hangs from, with the append-only triggers dropped first | The count enumerates from `findings`, so removing that row removes the anchor there is anything to re-derive against. Every other tamper on this path drops the line and discloses it; this one leaves no line to drop. A contested proposition then reads CONVERGENT with `independent_refute` at 0, `lines_skipped` at 0, and nothing on the health channel, and the state survives a backup and restore. Deleting a supporting row is the same mechanism pointing the safe way, and is the case the suite pins. Closing this needs an anchor outside the database, which is why the design defers it rather than papering over it |
| Resurrection of a retracted claim through an edited backup replayed by restore | `status` is editorial and carries no signature, so nothing re-derives it, and `claims_update_status_terminal` fires before an UPDATE, which restore's INSERT never performs. A writer who can edit `claims.toml` flips `retracted` back to `open` with no key and no SQL access, and restore accepts it without a warning. What the claim says is unaffected: its text, provenance and support level still have to verify. Treat a retraction as an editorial signal, and the signed content as the record |
| A rung backed by a peer whose signer the project never registered | The read path re-derives a REPLICATED rung by looking for a distinct signed peer on the same ESTABLISHED anchor, and it accepts a peer whose signer has no validators row, because serving a stranger's claim is what the lean model is for and no signature is checked in that case. So a writer who signs with a key the project never enrolled can back a rung. Requiring enrolment here is a behaviour change rather than a repair: it also strips the rung from an honest claim whose only peer is an unregistered signer, and the write path promotes on convergence without asking, so the two would disagree. Held for a release that can move both together |
| Misclassified `INFERRED` / `ANALYTICAL` / `DERIVED` | declared by the agent, not verified |
| Colluding log operator publishing two checkpoints to different audiences | needs gossip / witness protocols, out of scope for the single-checkpoint trust model |
| Compromised log signing key | mareforma trusts whichever pubkey the caller pinned via TOFU; rotation requires deleting the pin |
| Compromised user signing key | mareforma trusts the local Ed25519 key; key-management is the user's concern |
| Wrong-but-internally-consistent claims | mareforma proves that the agent stood behind the claim cryptographically, not that the claim is true |

### Where each property lives in the code

For the reader who wants to read the actual enforcement:

- **State-machine triggers**: [`mareforma/db/_schema_sql.py`](mareforma/db/_schema_sql.py) `_SCHEMA_SQL`
  (search for `claims_insert_state_check`, `claims_update_state_check`,
  `claims_update_status_terminal`, `claims_signed_no_delete`).
  `claims_signed_fields_no_laundering` lives in `_SIGNED_FIELDS_TRIGGER_SQL`,
  re-created on every `open_db()` so an existing database gains the current
  watch list
- **Convergence detection**: `_maybe_update_replicated_unlocked` in [`mareforma/db/core.py`](mareforma/db/core.py) (distinct `asserter_keyid` + equal-data collapse)
- **Verify-on-read**: `_row_verified_on_read`, `_verify_validation_on_read`,
  `_verify_participant_bundle_on_read` in `db/core.py`, wired into `get_claim`,
  `query_claims`, and `query_provenance`
- **Validation gates**: `validate_claim` in `db/core.py` (core-bypass
  defense: cryptographic verify + LLM-type ceiling + self-validation
  refusal + converging-set self-validation refusal + payload field
  equality + evidence_seen citation gate)
- **Verdict-issuer protocol**: `record_replication_verdict` /
  `record_contradiction_verdict` in `db/core.py`; trigger
  `contradiction_invalidates_older`
- **Restore proofs**: `_verify_claim_signatures_on_restore`,
  `_verify_and_insert_replication_verdict`,
  `_verify_and_insert_contradiction_verdict` in [`mareforma/db/restore.py`](mareforma/db/restore.py)
- **Rekor inclusion verification**: `verify_rekor_inclusion`,
  `verify_merkle_inclusion_proof`, `verify_rekor_checkpoint`,
  `rekor_entry_binds_to_envelope`, `fetch_inclusion_proof`,
  `fetch_log_pubkey` in
  [`mareforma/signing/rekor.py`](mareforma/signing/rekor.py)
- **TOFU pubkey pinning**: `_pem_canonical_der` +
  `O_CREAT|O_EXCL` write in [`mareforma/__init__.py`](mareforma/__init__.py)
- **Validator chain walk**: `_verify_chain`, `is_enrolled` in
  [`mareforma/validators.py`](mareforma/validators.py)

## Adapter framework

The core is intentionally agnostic about which AI platforms
exist. `mareforma.adapters.*` is the opt-in extension point where
platform-specific translation lives. Three load-bearing properties:

- **Adapters live on top of the core, never inside it.** The
  core ships the storage + signing + state-machine + invariants;
  adapters ship platform plumbing (HTTP clients, payload shapes,
  event semantics). A new adapter never modifies `mareforma.db`,
  `_graph`, or `_canonical`; it imports them.
- **Opt-in by import.** Every adapter ships in the wheel and runs on
  core dependencies, so none carries an install extra. An adapter costs
  nothing until a caller imports it.
- **Convention surface, not framework.** Each adapter exposes the
  same minimum: a constructor taking `graph=`, `predicate_uris()`
  enumerating the URIs it may emit, `emit_sample()` for the
  cross-adapter coexistence test in
  `tests/adapters/test_coexistence.py`. The core does not
  prescribe HOW an adapter wraps its platform, only that any
  adapter writing into one graph composes with peers without
  predicate-URI collision.

Core primitives `mareforma.events` (EventSource Protocol +
typed payloads + source-name constants) and `mareforma.tools` (Tool
Protocol + ToolResult + ReplayResult) live alongside `_graph` /
`_canonical` / `signing` because the contracts ARE core. They
have no dependency on any adapter; an adapter that disappears does
not break the contracts. URI constants live in
`mareforma.predicate_types`: a single source of truth for the URIs
the core reserves, re-exported at the top level for
ergonomics. The core primitives (events, canonicalize, tools) each
follow the same core-first rule.

## Execution-observed grounding

Whether a finding is grounded in real data (`did the cited data flow into
it`) is computed from execution, not declared by the producer. The observer
lives in [`mareforma/observe/`](mareforma/observe/).

Wrap the span that authors a finding in `observe(cites=...)`. Inside it,
wrapped loaders (`builtins.open`, `io.open`, and `sqlite3.connect` always, so
`pathlib` reads are seen; `pandas`, `polars`, the keep-alive HTTP clients
`requests`/`httpx`/`aiohttp`, and the C-runtime scientific readers
`h5py`/`pyarrow`/`netCDF4` only if the host already imported them, so no new
core dep) record the reads that happen, and a PEP-578 audit hook plus direct
thread-start wrapping record the spawn seams the loaders cannot see across. A loader imported INSIDE an open scope is
wrapped too, by a late-import hook. On exit the scope classifies into one of
three states:

- `GROUNDED`: a read matching the finding's cited source returned non-empty
  data. An incidental read (config, tokenizer, cache) does not qualify;
  read-to-citation binding is what separates it from the cited source.
- `UNGROUNDED`: the scope was fully observed and the cited data never
  arrived. This is the silent-fallback tell, and it is the ONLY path to
  UNGROUNDED, which is what makes UNGROUNDED trustworthy.
- `OPAQUE`: a thread, subprocess, socket, or uninstrumented reader could
  have hidden a read, so absence cannot be trusted. A first-class state, not
  an error: a confident GROUNDED/UNGROUNDED across a boundary the observer
  cannot cross is worse than admitting the blind spot.

A seam forces OPAQUE only when it is **relevant** to the citation: a socket
seam cannot deliver a local-file read, so it does not hide a file-cited
finding's UNGROUNDED tell, but it does block a URL or content-address
citation whose bytes can arrive over the network. Thread, subprocess, and
coverage-gap seams hide anything; an unknown seam or citation kind blocks
(fail-closed). A cited C-runtime file with no observed read floors to OPAQUE
("bytes not observable via PEP-578"), never a false UNGROUNDED. A target that
exits non-zero records an `abort` seam: the run stopped part way, so what it
did not read says nothing about what the pipeline reads. An open of the cited
source the observer watched FAIL, accounting for every open of that path,
hides nothing: it is a failed-open seam, and the verdict stays UNGROUNDED with
the exception type named.

The observed axis is **separate and additive**. It never touches the
declared `classification` enum (`INFERRED` / `ANALYTICAL` / `DERIVED`) and
never shares its value space. A verdict rides into the signed in-toto
statement as an optional, versioned `observed_grounding` record (verdict,
reason, the cited set it was computed against, receipt digest, axis version):
present only when a verdict was recorded, so a claim asserted without the
observer produces byte-identical signed bytes, and an envelope that omits the
field reads as "no verdict," never as tampering. The chain hash and
`statement_cid` bind it too, and restore rejects a `claims.toml` whose stored
verdict no longer matches the signature.

**Verdict-to-citation binding** closes the gap between "a read happened" and
"the finding's own data was read." The verdict's cited set is cross-checked
against the finding's citation (its `data_id` set plus any `data_source=`) at
bind time; a GROUNDED whose cited set is disjoint downgrades to OPAQUE with a
signed reason and a `grounding_citation_mismatch` health event, or raises in
strict mode. The check re-runs on read as pure string comparison over stored
normalized identifiers, so a cross-host claim whose paths do not exist on the
verifier is never false-flagged. A verdict that is not `GROUNDED` never counts
toward support-level promotion; grounding is a necessary floor, never
sufficient.

`mareforma observe --doctor` reports which loaders are wrapped and which seams
force OPAQUE in the current environment; `mareforma measure` aggregates a run's
verdict receipts into the GROUNDED / UNGROUNDED / OPAQUE split, bucketed by
seam kind. When a receipt also carries a per-finding effective-independence
record (`effective_independence_receipt`), `measure` reports the independence
arm alongside the split: the distribution of the effective number (a single
supporting line versus corroboration at two or more), the fraction UNVERIFIABLE
where the lineage is soft, and the same-model-collapse rate (corroborations a
signer-axis counter would call independent that were one computed model counted
twice). `summarize_pilot` runs a slim natural-prevalence pilot over a receipts
file, reporting both arms with the honest OPAQUE-coverage bound: when OPAQUE
dominates, the grounded prevalence reads as a lower bound, not a trustworthy
number. `mareforma audit` produces such receipts for a pipeline that never
imports mareforma: one signed, verifiable receipt per finding from a single
observed run, computed only from what the observer recorded.

The verdict is computed from execution of a **cooperating producer**: the
binding is tamper-evidence over what a cooperating run did, not a proof
against an adversarial operator. The same limit holds with the roles
reversed under `mareforma audit`: the audited target runs in the auditor's
interpreter, so a target built to defeat the audit could fabricate what the
observer records, audit widens who can be graded, not the adversary the
grade withstands. A finding is authored inside the scope and
signed after it closes; asserting a claim while its grounding scope is still
open is refused.

An independent **causal oracle** ([`observe/oracle.py`](mareforma/observe/oracle.py))
validates the observer without reading its log: it perturbs the input,
re-runs the pipeline, and checks whether the finding moves. Flow (did the
bytes arrive) and influence (does the finding depend on them) are different
constructs, so the two can honestly disagree; `reconcile` labels
"read the data then ignored it" a construct difference, not a detector error.
A prose finding is reduced to a scalar by a declared reducer:
`numeric_extraction_reducer` pulls the reported number out of an answer with no
model, so the oracle stays model-independent, while a reducer that runs a model
(an embedding or LLM judge) sets `reinserts_model=True` and the result records
it. A `multiplicity` control widens the decision threshold when a finding is one
of many (so the noisiest of a family cannot cross the bar by chance), and a
thin-sigma guard widens it when the noise floor rests on too few repeats; both
default off, so the scalar path is unchanged.

## Honest scope

What mareforma is NOT: trust is
local to a project's enrolled validators; `classification` and
`generated_by` are self-declared (mareforma is no stronger than
agent discipline); Rekor inclusion is logged-not-proof-verified;
contradiction is per-claim; the signed `evidence` dict is opaque
storage, not an evaluated quality score; model lineage records model
and method identity only, never a claim about training-time
contamination; no automated fraud detection beyond the structural
invariants mareforma enforces. Observed grounding computes FLOW of a
cooperating
producer, not correctness or influence: it has documented bounds (a
load-once/reuse read looks UNGROUNDED, a stale-but-non-empty read looks
GROUNDED, and anything across a spawn seam or uninstrumented reader is
OPAQUE rather than a confident verdict), and it does not defend against an
adversarial operator.

## Engineering discipline: code as audit trail

Mareforma carries its own design review forward in time. Three
conventions, applied consistently:

- **Every defensive measure names the threat it blocks.** Each SQL
  trigger comment names the attack chain its `RAISE(ABORT, ...)`
  refuses, e.g. `claims_signed_no_delete` documents that without
  the trigger "an adversary could wipe a Rekor-logged ESTABLISHED
  claim and rewrite claims.toml as if it never existed." The
  contradiction-invalidates trigger carries a `DESIGN RULE: DO NOT
  PROPAGATE DOWNSTREAM` comment with rationale, so a future
  contributor adding transitive falsification has to engage with the
  reasoning rather than discover it from a broken test.
- **Every invariant names what it does NOT prove.** The
  `evidence_seen` check verifies that each cited claim
  exists and predates the validation timestamp; the docstring
  immediately follows with *"this gate cannot prove the validator
  actually opened those claims, only that the claims they cited
  exist and predate validation. That's the strongest property
  mareforma can enforce; everything else rests on the validator's
  honesty."* The same pattern recurs in `_refuse_self_validation`,
  in `_maybe_update_replicated_unlocked`, and in the
  `claims_signed_fields_no_laundering` trigger.
- **Core over surface.** When a defect is found, the fix lands
  at the root layer (DB trigger, signed payload field set, state
  machine) rather than in the wrapper. The public Python API
  inherits the property; an in-process caller bypassing
  `EpistemicGraph.validate` and calling `mareforma.db.validate_claim`
  directly meets the same gates. The trust ladder is not bypassable
  via a public path the wrapper happens not to expose. See
  [`CONTRIBUTING.md`](CONTRIBUTING.md#trust-layer-changes) for
  the full rule.

The result is that any future contributor reading the code reads the
reasoning that produced it, including which properties are
load-bearing and which are intentionally out of scope. This is the
strongest single signal of how mareforma will age.

## See also

- [`README.md`](README.md): user-facing pitch + honesty section
- [`AGENTS.md`](AGENTS.md): agent integration guide (the contract
  agents follow when writing to the graph)
- [`SECURITY.md`](SECURITY.md): threat model + responsible disclosure
- [`CHANGELOG.md`](CHANGELOG.md): release notes
