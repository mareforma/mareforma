# Mareforma

[![Python](https://img.shields.io/pypi/pyversions/mareforma)](https://pypi.org/project/mareforma/)
[![Tests](https://github.com/mareforma/mareforma/actions/workflows/tests.yml/badge.svg)](https://github.com/mareforma/mareforma/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/mareforma)](https://pypi.org/project/mareforma/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

AI scientists are being deployed on real research problems before any infrastructure exists to know which of their findings can be trusted. Observability tools record what an agent did — they do not record what it means, whether it converges with independent evidence, or how far a conclusion is from its raw data.

Mareforma is a local epistemic graph that accumulates findings across agent runs, detects convergence automatically when independent agents reach the same conclusion through different data paths, and exposes the full provenance chain so trust can be derived from structure rather than from self-reported confidence.

Every individual capability mareforma uses — Ed25519 signing, DSSE envelopes, Sigstore-Rekor transparency, GRADE-shaped evidence vectors, local SQLite — exists in mature form elsewhere. What's missing in the OSS landscape is the combination: a runtime, opt-in Python library that bundles them as the place an agent writes claims to. See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the lane.

**Mareforma records *what* the agent claimed and *how* — so silent pipeline failures, prior-knowledge fallbacks, and self-reported convergence become visible.** Trust signals are derived from graph structure (whose key signed what, which upstreams are cited, how many independent agents wrote the same conclusion), not from the agent's own confidence. The substrate is honest about what those signals do and do not prove — see "What mareforma is NOT" below.

```python
import mareforma

with mareforma.open() as graph:

    # Query what is already established before asserting
    prior = graph.query("topic X", min_support="REPLICATED")

    claim_id = graph.assert_claim(
        "Cell type A exhibits property X under condition Y (n=842, p<0.001)",
        classification="ANALYTICAL",
        generated_by="agent/model-a/lab_a",
        supports=[c["claim_id"] for c in prior],
    )
```

```mermaid
graph LR
    P(["ESTABLISHED upstream<br/>(prior literature)"]) --> A["ANALYTICAL · lab_a"]
    P --> B["ANALYTICAL · lab_b"]
    A --> R(["REPLICATED ✓"])
    B --> R
    R -->|"graph.validate()"| E(["ESTABLISHED ✓"])

    style P fill:#713f12,stroke:#f59e0b,color:#fde68a
    style A fill:#1e3a5f,stroke:#3b82f6,color:#93c5fd
    style B fill:#1e3a5f,stroke:#3b82f6,color:#93c5fd
    style R fill:#14532d,stroke:#22c55e,color:#86efac
    style E fill:#713f12,stroke:#f59e0b,color:#fde68a
```

`REPLICATED` requires that the converging claims share an `ESTABLISHED`
upstream in `supports[]` — matches Cochrane/GRADE evidence-chain methodology.
On a fresh graph, bootstrap an `ESTABLISHED` anchor with `seed=True`
(enrolled validator only); see [Example 03](examples/03_documented_contestation)
for the full seed-then-converge pattern.

## Findings contradict — both stay in the graph

```python
# ESTABLISHED consensus sits in the graph
prior = graph.query("Treatment X", min_support="ESTABLISHED")

# New larger study gets a different result — don't discard it, document it
graph.assert_claim(
    "Treatment X shows no effect (n=1240, p=0.21) — larger and more diverse cohort",
    classification="ANALYTICAL",
    contradicts=[c["claim_id"] for c in prior],
)
```

Science advances by documented contestation, not by one side disappearing.
Both claims coexist. A human reviewer sees the tension explicitly in the graph.

## The infrastructure gap

The current generation of agent systems provides no principled way to distinguish:

- a finding backed by data from one backed by LLM prior knowledge
- genuine independent replication from two agents repeating each other
- an established consensus from a single speculative assertion

Without that structure, every output looks like a result.

- **Tracing and observability tools** record what the agent did. They do not
record what it means, whether it can be trusted, or whether another agent
already found the same thing by a different path.

- **One-shot pipelines** evaporate findings between runs. There is no memory
of what was established, no way to detect convergence, no accumulated graph.
Each run starts from scratch.

- **Silent pipeline failures** look like results. If the data pipeline returns
null and the agent falls back to LLM prior knowledge, the finding looks
identical to a data-driven one — unless you record the classification at
assertion time.

## Architecture

```python
graph = mareforma.open()                                # zero setup, no init required
graph.assert_claim(text, classification="ANALYTICAL", supports=[...])
graph.query(text, min_support="REPLICATED")
graph.validate(claim_id, evidence_seen=[...])           # human promotes to ESTABLISHED, citing reviewed claims
graph.health()                                          # HealthReport: per-status + per-support-level counters, traffic light
graph.refresh_unresolved()                              # retry DOI verification for offline claims
graph.refresh_all_dois()                                # force-re-check every DOI (retraction drift)
graph.refresh_unsigned()                                # retry Rekor (replays from rekor_inclusions sidecar if present)
graph.refresh_convergence()                             # retry detection for claims whose post-INSERT promotion errored
graph.find_dangling_supports()                          # audit UUID refs that point nowhere
graph.classify_supports([...])                          # tag each entry as claim/doi/external
graph.get_tools(generated_by="agent/model-a/lab_a")     # framework-ready callables
```

**External verification, opt-in by component:**

- **DOIs in `supports[]`/`contradicts[]`** are HEAD-checked against Crossref
  and DataCite at assertion time. Failed verifications hold the claim out
  of `REPLICATED` until `refresh_unresolved()` succeeds.
- **Cryptographic signing** is opt-in. `mareforma bootstrap` once to
  generate an Ed25519 keypair; every claim is then signed and tamper-evident.
- **Sigstore-Rekor transparency log** is opt-in via
  `mareforma.open(rekor_url=mareforma.signing.PUBLIC_REKOR_URL)`. Signed
  claims are submitted to the log; the entry uuid + logIndex + raw
  response bytes are persisted to the local `rekor_inclusions` sidecar.
  Submission failures are retried by `refresh_unsigned()`. **Today the
  substrate records what Rekor returned; full Merkle inclusion-proof
  re-verification on load is a follow-up release** — see the Rekor
  limits below.

**Trust levels** — derived from graph topology, never self-reported:

| Level | Meaning |
|---|---|
| `PRELIMINARY` | One agent asserted it. The substrate has cryptographic provenance but no convergence signal yet. |
| `REPLICATED` | ≥2 enrolled keys signed claims sharing an `ESTABLISHED` upstream with different `generated_by` strings. As strong as the writers' honesty about independence — the substrate detects that two strings differ, not that two physical agents do. |
| `ESTABLISHED` | An enrolled human-typed key signed a validation envelope. Pass `evidence_seen=[...]` to bind the validator's review citations (every cited claim must exist and predate validation). Without `evidence_seen`, the envelope records that a human pressed a button; with it, the envelope records *what they reviewed*. |

**Classification** — declared by the agent, records epistemic origin:

| Value | When |
|---|---|
| `INFERRED` | LLM reasoning (default) |
| `ANALYTICAL` | Deterministic analysis against source data |
| `DERIVED` | Explicitly built on ESTABLISHED or REPLICATED claims |

> **Trust level and classification are independent axes.** An `INFERRED + ESTABLISHED`
> claim is possible (a human validated an LLM-prior-knowledge assertion); so is an
> `ANALYTICAL + PRELIMINARY` claim (a data-grounded finding with no convergence yet).
> Trust answers "how many parties witnessed this?"; classification answers "what did
> the agent compute it from?" Use both when querying: `graph.query(text,
> min_support="REPLICATED", classification="ANALYTICAL")`.

Storage: local SQLite, WAL mode, ACID guarantees. Network calls only for opt-in external verification: DOI lookups (Crossref + DataCite) and Sigstore-Rekor transparency log.

## Silent pipeline failures become visible

The reproduction-worthy use case mareforma was built for. A real AI scientist
agent runs a multi-step analysis: query a public dataset, regress a gene's
expression against a phenotype, return the top hit. The data lookup silently
returns null because of a stale EFO id. The agent's LLM reasoning fills the
gap with prior knowledge and returns a plausible-sounding answer. The output
looks identical to a data-driven result.

```python
finding_text = run_pipeline(target_gene, phenotype)

graph.assert_claim(
    finding_text,
    # The one line that breaks the symmetry: classification depends on
    # whether real data flowed through. The substrate doesn't compute
    # this — the agent's wrapper inspects the pipeline state and tells
    # the truth at assertion time.
    classification="ANALYTICAL" if generated_code_ran else "INFERRED",
    generated_by="agent/gpt-4o/lab_a",
    source_name="depmap_24q2" if data_actually_loaded else None,
)
```

Now a downstream consumer querying `min_support="REPLICATED",
classification="ANALYTICAL"` excludes the silent-fallback rows. The hallucinated
finding is in the graph (auditable, signed), but it's NOT in the trustworthy
result set. The wrapper that picks `"ANALYTICAL"` vs `"INFERRED"` is doing the
work — the substrate makes that work visible and tamper-evident.

[Example 05 — Drug Target Provenance](examples/05_drug_target_provenance/)
wraps MEDEA (a real AI scientist agent published on arXiv), reproduces a real
silent-failure mode in its EFO id lookup, and shows the classification gate
catching it.

## What mareforma is NOT

Honest scope, so the design choices land in the right frame:

- **Not a global trust system.** Trust is local to a project's enrolled
  validators. Two projects' `ESTABLISHED` claims are not comparable across
  installations — there is no federation layer, no cross-project key
  directory, no shared ground truth.
- **Classification is self-declared.** `ANALYTICAL`, `INFERRED`, `DERIVED`
  are the asserter's claims about epistemic origin. The substrate signs
  them tamper-evidently but does not verify them against the actual code
  path. A misclassified claim is a trust failure of the asserter, not the
  substrate.
- **`generated_by` is self-declared too.** It is the substrate's primary
  independence signal — `REPLICATED` fires only when two claims sharing
  an upstream have different `generated_by` values. But the substrate
  cannot verify that `agent/claude/lab_a` and `agent/claude/lab_b`
  correspond to different physical agents; both are free-form strings
  supplied by the caller. An adversary running both labs can produce
  `REPLICATED` at will. Honest agents produce a useful signal; the
  signal is no stronger than the discipline of the agents writing to
  the graph.
- **GRADE EvidenceVector is GRADE-shaped storage, not GRADE evaluation.**
  Each claim carries a 5-domain vector (`risk_of_bias`, `inconsistency`,
  `indirectness`, `imprecision`, `publication_bias`) plus three upgrade
  flags, bound into the signed predicate. The substrate stores and
  round-trips these values; it does not derive a single GRADE certainty
  rating (high/moderate/low/very-low), does not gate upgrade flags on
  study design (RCT vs. observational), and does not integrate the
  vector into the `PRELIMINARY → REPLICATED → ESTABLISHED` ladder.
  Full GRADE-style certainty derivation is out of scope for v0.3.0
  and on the v0.4 backlog.
- **Rekor inclusion is logged, not proof-verified (yet).** When
  `rekor_url=` is configured, signed claims are submitted to the log
  and the response — entry uuid, logIndex, integratedTime, and the
  raw body bytes — is persisted to the local `rekor_inclusions`
  sidecar. The submit-time response is cryptographically checked
  (the returned entry must record OUR hash + OUR signature; mismatch
  → submission treated as a failure), so a hostile log operator
  cannot get the substrate to accept an unrelated entry as ours.
  What the substrate does NOT yet do is re-fetch the Merkle inclusion
  proof on load and verify it against the log's current signed
  checkpoint — that step would close the residual "the log forked
  or rewrote history after we submitted" gap. Today the trust posture
  for inclusion proofs is: trust the log operator's submit-time
  response, persisted locally for future re-verification. Merkle
  inclusion-proof verification is the next-release item on this
  surface.
- **DOIs are HEAD-checked, not content-verified.** External references
  resolve to a 200 response; the substrate does not parse, sign, or
  archive the referenced content. A DOI that resolves today may resolve
  to different content tomorrow. The positive cache holds resolved DOIs
  for 30 days — operators worried about retraction drift can call
  `graph.refresh_all_dois()` to force-re-check every DOI against the
  registry and surface the diff.
- **No semantic deduplication.** Two claims with different `text` but
  identical meaning are distinct rows. Convergence detection runs on
  `supports[]` topology, not on text similarity.
- **Dangling references in `supports[]` are accepted.** The substrate
  does not refuse UUID-shaped strings that don't point to any existing
  claim — a `supports` entry could legitimately reference a claim from
  another project, a not-yet-asserted upstream, or a DOI. REPLICATED
  detection requires the referenced ESTABLISHED claim to actually
  exist + be open, so a dangling reference cannot trigger spurious
  promotion. Operators auditing a graph for integrity can call
  `graph.find_dangling_supports()` to surface every such hanging arrow.
- **Contradiction is per-claim, not propagated downstream.** A signed
  contradiction marks the older of two referenced claims; claims that
  cited the now-invalidated one via `supports[]` are unaffected.
  Deliberate boundary — see ARCHITECTURE.md.
- **Verdict-issuer method labels are self-declared.** When an enrolled
  issuer signs a `replication_verdict` with `method="semantic-cluster"`,
  the substrate stores the label and binds it into the signature. It
  does not verify that the labeled method was actually run, that two
  `semantic-cluster` verdicts from different issuers used the same
  algorithm or threshold, or that the issuer's confidence dict is
  calibrated. Like `generated_by`, method labels are as strong as the
  issuer's honesty. The verdict-issuer table is a typed-string registry
  with cryptographic stapling, not an epistemic primitive on its own.
- **No automated fraud detection.** The substrate refuses self-validation
  and gates LLM-typed validators on both promotion and contradiction
  paths, but it cannot detect colluding human validators, manufactured
  datasets, or fabricated DOIs.

Related work mareforma does not replace: W3C PROV-O / PROV-AGENT
(W3C-recommended provenance vocabulary), FAIRSCAPE's Evidence Graph
Ontology (EVI — research-evidence ontology at `w3id.org/EVI`, MIT-
licensed, not a W3C deliverable), IETF SCITT (signed supply-chain
transparency architecture, currently `draft-ietf-scitt-architecture-22`).
Mareforma is a runtime substrate for an agent's working graph, not a
publication-grade provenance record.

## Get started

```bash
uv add mareforma
mareforma bootstrap            # one-time: generate Ed25519 signing key
```

`mareforma bootstrap` is optional. Without it, claims are stored
unsigned. With it, every claim carries a tamper-evident signature and
can be published to a Sigstore-Rekor transparency log on demand.

See [AGENTS.md](AGENTS.md) — execution contract, forbidden patterns,
signing and transparency log, idempotency convention, `generated_by`
requirements.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the substrate design (rails
not trains), [CONTRIBUTING.md](CONTRIBUTING.md) for the dev workflow,
[SECURITY.md](SECURITY.md) for the threat model and disclosure
channel, [CHANGELOG.md](CHANGELOG.md) for release notes.

Full documentation: **https://docs.mareforma.com**

## Examples

| | Example | What it shows |
|---|---|---|
| 01 | [API Walkthrough](examples/01_api_walkthrough/) | Full API reference |
| 02 | [Compounding Agents](examples/02_compounding_agents/) | Findings accumulate across agent runs |
| 03 | [Documented Contestation](examples/03_documented_contestation/) | Agent challenges established consensus |
| 04 | [Private Data, Public Findings](examples/04_private_data_public_findings/) | Two labs share provenance without sharing data |
| 05 | [Drug Target Provenance](examples/05_drug_target_provenance/) | Real AI scientist with honest epistemic status |
