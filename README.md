# Mareforma

[![Python](https://img.shields.io/pypi/pyversions/mareforma)](https://pypi.org/project/mareforma/)
[![Tests](https://github.com/mareforma/mareforma/actions/workflows/tests.yml/badge.svg)](https://github.com/mareforma/mareforma/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/mareforma)](https://pypi.org/project/mareforma/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

**Verify your AI agents' findings the way you'd verify a paper.**

Mareforma is the local store where research agents write their claims. Signed, cross-referenced, and promoted when independent agents converge. Trust comes from evidence, not the agent's confidence score.

## Why

AI agents now run real research problems with no infrastructure for knowing which findings to trust. Tracing tools record *what the agent did*. They do not record what it means, whether it converges with independent evidence, or how far a conclusion sits from its raw data. A silent pipeline failure, a prior-knowledge fallback, and a real result look identical.

## What it does

```python
import mareforma

with mareforma.open() as graph:

    # Query established prior claims. query_for_llm wraps text in
    # <untrusted_data>...</untrusted_data> tags so a downstream LLM
    # consumes it as data, not instructions.
    prior = graph.query_for_llm("topic X", min_support="REPLICATED")

    claim_id = graph.assert_claim(
        "Cell type A exhibits property X under condition Y (n=842, p<0.001)",
        classification="ANALYTICAL",
        generated_by="agent/model-a/lab_a",
        supports=[c["claim_id"] for c in prior],
    )

    # Walk the full lineage of any claim: upstream + downstream + signatures
    # + contradictions + verdicts in one deterministic dict.
    lineage = graph.query_provenance(claim_id, depth=4)
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

`REPLICATED` fires when two claims signed by distinct keys cite the same `ESTABLISHED` upstream in `supports[]`. The signing key is the independence unit, so one operator cannot manufacture it by relabelling a string. On a fresh graph, bootstrap an `ESTABLISHED` anchor with `seed=True` (enrolled validator only). See [Example 03](examples/03_documented_contestation) for the seed-then-converge pattern.

**Trust ladder.** Derived from graph topology, never self-reported. A `REPLICATED` or `ESTABLISHED` row is re-verified against its signatures on every read, so a tampered high-trust row in a shared graph is caught at query time, not served.

| Level | Meaning |
|---|---|
| `PRELIMINARY` | One agent asserted it. Cryptographic provenance, no convergence signal yet. |
| `REPLICATED` | Two claims signed by distinct keys, sharing an `ESTABLISHED` upstream. A convergence signal, not proof: distinct keys do not prove the data is independent. |
| `ESTABLISHED` | An enrolled human validator signed a validation envelope binding `evidence_seen=[...]` review citations. |

**Classification.** Declared by the agent, records what kind of work produced it: `INFERRED` (LLM reasoning), `ANALYTICAL` (deterministic analysis against source data), `DERIVED` (built on `ESTABLISHED` or `REPLICATED` claims). Trust level and classification are independent axes. Query both: `graph.query(text, min_support="REPLICATED", classification="ANALYTICAL")`.

**Findings: earned, not declared.** The trust ladder above is read from provenance. The trust layer (`mareforma.trust`) turns a claim into a content-addressed `Proposition` bound to a pre-registered `Prediction`, computes the direction of evidence with `compute_bearing` instead of letting the agent declare it, and derives a count-based `Status` (`PRELIMINARY` to `CORROBORATED`) from independent signers. A finding rides a signed claim, so it is additive. See [Findings](https://docs.mareforma.com/concepts/findings) and [Example 02](examples/02_compounding_agents).

### Core surface

```python
graph.assert_claim(text, classification, supports=[...])
graph.query(text, min_support="REPLICATED")
graph.query_provenance(claim_id, depth=4)
graph.validate(claim_id, evidence_seen=[...])

# Trust layer: a computed bearing and a derived status
graph.assert_finding(proposition, prediction, estimate, data_id="...")
graph.proposition_status(proposition)   # derived Status + independence counts
```

```bash
mareforma bootstrap            # generate Ed25519 signing key
mareforma --help               # all subcommands
mareforma ingest paper.md      # paper-abstract claim drafts
```

Full API and CLI reference: [docs.mareforma.com](https://docs.mareforma.com). Opt-in components (DSSE signing, Sigstore-Rekor transparency, RFC 6962 inclusion proofs, DOI HEAD-checks against Crossref/DataCite, grounding sensors): see [AGENTS.md](AGENTS.md). Storage is local SQLite, WAL mode, ACID. Network calls only for the opt-ins above.

## Silent pipeline failures become visible

An AI agent runs a multi-step analysis: query a public dataset, regress a gene's expression against a phenotype, return the top hit. The data lookup silently returns null because of a stale identifier. The agent's LLM reasoning fills the gap with prior knowledge and returns a plausible-sounding answer. The output looks identical to a data-driven result.

```python
finding_text = run_pipeline(target_gene, phenotype)

graph.assert_claim(
    finding_text,
    # The one line that breaks the symmetry: classification depends on
    # whether real data flowed through. Mareforma doesn't compute
    # this. The agent's wrapper inspects the pipeline state and tells
    # the truth at assertion time.
    classification="ANALYTICAL" if generated_code_ran else "INFERRED",
    generated_by="agent/gpt-4o/lab_a",
    source_name="depmap_24q2" if data_actually_loaded else None,
)
```

A downstream consumer querying `min_support="REPLICATED", classification="ANALYTICAL"` excludes the silent-fallback rows. The hallucinated finding stays in the graph (auditable, signed) but is NOT in the trustworthy result set. The wrapper that picks `ANALYTICAL` vs `INFERRED` is doing the work. Mareforma makes that work visible and tamper-evident.

[Example 05: Drug Target Provenance](examples/05_drug_target_provenance/) wraps the omics AI agent [MEDEA](https://medea.openscientist.ai/) and shows the classification gate catching a real silent failure in its identifier lookup.

## Findings contradict

```python
prior = graph.query("Treatment X", min_support="ESTABLISHED")

graph.assert_claim(
    "Treatment X shows no effect (n=1240, p=0.21): larger and more diverse cohort",
    classification="ANALYTICAL",
    contradicts=[c["claim_id"] for c in prior],
)
```

Both claims coexist. A human reviewer sees the tension in the graph. `graph.refutation_status(claim_id)` surfaces whether a claim is `clean`, `contested`, `contradicted`, or `retracted`.

## Get started

```bash
uv add mareforma
mareforma bootstrap            # optional: enable signing and transparency
```

Without `bootstrap`, claims store unsigned. With it, every claim carries a tamper-evident signature and can submit to a Sigstore-Rekor transparency log.

Optional: adapters for [ClawInstitute](https://clawinstitute.aiscientist.tools/), [ToolUniverse](https://aiscientist.tools/), [Gemini for Science](https://ai.google/gemini-for-science/), and a `mareforma.hooks` recorder for Claude Code tool calls. See [AGENTS.md → Adapter framework](AGENTS.md).

### Examples

| | Example | What it shows |
|---|---|---|
| 01 | [API Walkthrough](examples/01_api_walkthrough/) | Full API reference |
| 02 | [Compounding Agents](examples/02_compounding_agents/) | Findings accumulate across agent runs |
| 03 | [Documented Contestation](examples/03_documented_contestation/) | Agent challenges established consensus |
| 04 | [Private Data, Public Findings](examples/04_private_data_public_findings/) | Two labs share provenance without sharing data |
| 05 | [Drug Target Provenance](examples/05_drug_target_provenance/) | Real AI research agent with honest evidence labels |

[`AGENTS.md`](AGENTS.md): execution contract, signing setup, adapter framework.
[`ARCHITECTURE.md`](ARCHITECTURE.md): system design, trust ladder topology, honest scope.
[`SECURITY.md`](SECURITY.md): threat model and disclosure channel.
[`CONTRIBUTING.md`](CONTRIBUTING.md): dev workflow.
[`CHANGELOG.md`](CHANGELOG.md): release notes.

Full documentation: **https://docs.mareforma.com**
