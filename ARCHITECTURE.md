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
  is on the v0.4 backlog and would map mareforma claims onto EVI Claim
  / EvidenceGraph / supports / challenges classes; the schema stays
  mareforma-native, the export is the interop surface)
- IETF SCITT (federated supply-chain transparency — a SCITT submission
  path alongside Rekor is on the v0.4 backlog)
- Sigstore (transparency for software artifacts — mareforma uses Rekor
  for claim transparency; the protocols are the same shape)
- RO-Crate (FAIR research-object packaging — an RO-Crate 1.2 export
  from `export_bundle.py` is on the v0.4 backlog)
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

### Canonicalization — RFC 8785-shaped, not RFC 8785-strict

`canonical_statement` (in [`mareforma/_canonical.py`](mareforma/_canonical.py))
normalizes the body to NFC, sorts keys, drops whitespace, and rejects
NaN/Infinity (`json.dumps(sort_keys=True, separators=(",", ":"),
ensure_ascii=False, allow_nan=False)`). That is deterministic and
sufficient for mareforma-internal signing — every signature is produced
and verified by the same canonicalizer.

It is **not** RFC 8785 (JCS) strict. RFC 8785 mandates ECMAScript-style
number serialization (e.g. `1.0` renders as `1`, specific exponential-
form rules); Python's `json.dumps` does not follow those rules. The
claim envelope today contains no floats — `text` is a string, the
EvidenceVector domains are signed ints in `{-2,-1,0,+1}`, the upgrade
flags are bools, timestamps are ISO-8601 strings — so the current
on-wire form is byte-identical to what an RFC 8785 verifier would
produce. The day mareforma adds a float-valued field (a confidence
score, a p-value, a duration), a cross-language verifier implementing
RFC 8785 strictly may produce different bytes for the same logical
payload and refuse to verify mareforma's signatures.

For cross-tool verification today, the in-toto Statement v1 subject
digest (`sha256` over `text`) is canonical without depending on number
serialization — recompute the digest, compare to the `subject[0].digest.sha256`
field in the envelope, and verify the DSSE signature over the envelope
bytes as stored. The subject digest is the same bytes any in-toto
verifier (`in-toto-golang`, the Sigstore stack) will produce. Future
schema work that introduces floats should ship the RFC 8785 tightening
in the same release.

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
  instead of re-submitting (no duplicate Rekor entry).
- `claims_fts` — FTS5 virtual table (independent of `claims`, not
  `content=` linked) for substring + tokenized search.
- `doi_cache` — 30-day positive / 24-hour negative cache for DOI HEAD
  checks against Crossref + DataCite.

SQL triggers enforce the state machine, the append-only invariants on
signed predicate fields, the no-delete rule on signed claims, the
verdict tables' append-only-and-no-delete invariants, the
contradiction-invalidates-older logic, and the FTS sync. A tampered
Python interpreter cannot relax these rules.

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
is on the v0.4 backlog.

### Two known gaps in what TOML guarantees

**Chain order is not externally anchored.** A tampered TOML that
reorders claims (swap two `created_at` values) restores to a different
but internally-consistent chain. The signatures bind canonical statement
bytes, not chain position. For tamper-evidence across restore boundaries,
the per-claim Rekor entry is the external anchor — though Merkle
inclusion proof verification is itself on the v0.4 backlog.

**The TOML write lags the SQLite commit.** `_backup_claims_toml` runs
**after** the INSERT/UPDATE transaction commits. A process crash between
`COMMIT` and the TOML write leaves a row in `graph.db` that's missing
from `claims.toml`. The next mutation rewrites the TOML from current DB
state, so the crash window closes on the next successful write. For a
clean recovery snapshot, finish any in-flight writes before snapshotting
the TOML. The v0.4 perf rewrite addresses both the foreground-commit-
path cost and the crash gap by moving to an append-only sidecar +
periodic compaction model.

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

## See also

- [`README.md`](README.md) — user-facing pitch + honesty section
- [`AGENTS.md`](AGENTS.md) — agent integration guide (the contract
  agents follow when writing to the graph)
- [`SECURITY.md`](SECURITY.md) — threat model + responsible disclosure
- [`CHANGELOG.md`](CHANGELOG.md) — release notes
