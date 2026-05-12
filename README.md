# Mareforma

[![Python](https://img.shields.io/pypi/pyversions/mareforma)](https://pypi.org/project/mareforma/)
[![Tests](https://github.com/mareforma/mareforma/actions/workflows/tests.yml/badge.svg)](https://github.com/mareforma/mareforma/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/mareforma)](https://pypi.org/project/mareforma/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

AI scientists are being deployed on real research problems before any infrastructure exists to know which of their findings can be trusted. Observability tools record what an agent did — they do not record what it means, whether it converges with independent evidence, or how far a conclusion is from its raw data.

Mareforma is a local epistemic graph that accumulates findings across agent runs, detects convergence automatically when independent agents reach the same conclusion through different data paths, and exposes the full provenance chain so trust can be derived from structure rather than from self-reported confidence.

**Trust in a finding should come from the graph, not from the agent that made it.**

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
    P([prior_ref]) --> A["ANALYTICAL · lab_a"]
    P --> B["ANALYTICAL · lab_b"]
    A --> R(["REPLICATED ✓"])
    B --> R
    R -->|"graph.validate()"| E(["ESTABLISHED ✓"])

    style P fill:#334155,stroke:#475569,color:#cbd5e1
    style A fill:#1e3a5f,stroke:#3b82f6,color:#93c5fd
    style B fill:#1e3a5f,stroke:#3b82f6,color:#93c5fd
    style R fill:#14532d,stroke:#22c55e,color:#86efac
    style E fill:#713f12,stroke:#f59e0b,color:#fde68a
```

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
graph = mareforma.open()          # zero setup, no init required
graph.assert_claim(text, classification="ANALYTICAL", supports=[...])
graph.query(text, min_support="REPLICATED")
graph.validate(claim_id)          # human promotes to ESTABLISHED
graph.refresh_unresolved()        # retry DOI verification for offline claims
graph.refresh_unsigned()          # retry Rekor transparency log submission
graph.get_tools(generated_by="agent/model-a/lab_a")  # framework-ready callables
```

**External verification, opt-in by component:**

- **DOIs in `supports[]`/`contradicts[]`** are HEAD-checked against Crossref
  and DataCite at assertion time. Failed verifications hold the claim out
  of `REPLICATED` until `refresh_unresolved()` succeeds.
- **Cryptographic signing** is opt-in. `mareforma bootstrap` once to
  generate an Ed25519 keypair; every claim is then signed and tamper-evident.
- **Sigstore-Rekor transparency log** is opt-in via
  `mareforma.open(rekor_url=mareforma.signing.PUBLIC_REKOR_URL)`. Signed
  claims become publicly verifiable; submission failures are retried by
  `refresh_unsigned()`.

**Trust levels** — derived from graph topology, never self-reported:

| Level | Meaning |
|---|---|
| `PRELIMINARY` | One agent claimed it |
| `REPLICATED` | ≥2 independent agents converged on the same upstream evidence |
| `ESTABLISHED` | Human-validated — only via `graph.validate()` |

**Classification** — declared by the agent, records epistemic origin:

| Value | When |
|---|---|
| `INFERRED` | LLM reasoning (default) |
| `ANALYTICAL` | Deterministic analysis against source data |
| `DERIVED` | Explicitly built on ESTABLISHED or REPLICATED claims |

Storage: local SQLite, WAL mode, ACID guarantees. Network calls only for opt-in external verification: DOI lookups (Crossref + DataCite) and Sigstore-Rekor transparency log.

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

Full documentation: **https://docs.mareforma.com**

## Examples

| | Example | What it shows |
|---|---|---|
| 01 | [API Walkthrough](examples/01_api_walkthrough/) | Full API reference |
| 02 | [Compounding Agents](examples/02_compounding_agents/) | Findings accumulate across agent runs |
| 03 | [Documented Contestation](examples/03_documented_contestation/) | Agent challenges established consensus |
| 04 | [Private Data, Public Findings](examples/04_private_data_public_findings/) | Two labs share provenance without sharing data |
| 05 | [Drug Target Provenance](examples/05_drug_target_provenance/) | Real AI scientist with honest epistemic status |
