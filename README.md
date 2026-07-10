<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/mareforma-wordmark-dark.svg">
    <img alt="Mareforma" src="assets/mareforma-wordmark.svg" width="420">
  </picture>
</p>

<p align="center">
  <strong>Verify your AI scientists' findings the way science verifies itself</strong>
  <br>
  <sub>signed provenance · computed grounding · effective independence · local-first</sub>
</p>

<p align="center">
  <a href="https://docs.mareforma.com/introduction/quickstart">Quickstart</a> &nbsp;·&nbsp;
  <a href="https://docs.mareforma.com">Docs</a> &nbsp;·&nbsp;
  <a href="examples/">Examples</a>
</p>

<p align="center">
  <a href="https://pypi.org/project/mareforma/"><img src="https://img.shields.io/pypi/v/mareforma" alt="PyPI"></a>
  <a href="https://pypi.org/project/mareforma/"><img src="https://img.shields.io/pypi/pyversions/mareforma" alt="Python"></a>
  <a href="https://github.com/mareforma/mareforma/actions/workflows/tests.yml"><img src="https://github.com/mareforma/mareforma/actions/workflows/tests.yml/badge.svg" alt="Tests"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT"></a>
</p>

<hr>

Mareforma is a local-first library where AI scientists record their findings as claims. Each claim cites the claims it builds on and can contradict others, forming a knowledge graph. Trust is read from that graph, not from the agents' self-reported confidence.

## The silent failure it catches

The most dangerous result an AI scientist returns is the one that looks right but never touched the data. A step fails quietly, the model fills the gap from memory, and the answer reads like a real one.

Mareforma catches this by observing the run, not by trusting a label. Wrap the work in `observe()` and the grounding verdict is computed from what the code actually did:

```python
from mareforma.observe import observe

with observe(cites="dataset_alpha") as obs:
    result = analyze("dataset_alpha")   # the step that reads the data

graph.assert_claim(
    finding_text,
    generated_by="agent/lab-a",
    source_name="dataset_alpha",
    observed_grounding=obs.verdict.to_signed_dict(),  # GROUNDED, UNGROUNDED, or OPAQUE: computed, not declared
)
```

A finding whose step never executed cannot earn `GROUNDED`. The observer watched the scope, saw no cited read, and returns `UNGROUNDED`, so the trust map's grounding edge names no source. The model filling the gap from memory is caught at the boundary, by observation, not by a label the agent chose. [Example 02](examples/02_compounding_agents/) runs the absence catch end to end.

## Install

```bash
uv add mareforma
mareforma bootstrap   # optional: sign your claims and enable the public log
```

## Capabilities

| What you get | What it does |
|---|---|
| **Signed claims** | Each claim shows who stands behind it and cannot be altered unnoticed. |
| **Grounding check** | Computes whether a finding actually rests on data it read, or on the model's memory, by observing the run. |
| **Trust map** | `mareforma map <claim>` places every trust property (grounding, independence, contestation, witnessing) at its tier, and states plainly what it does not evaluate. |
| **Audit-grade verify** | `mareforma verify <claim>` re-checks signatures, the grounding-to-citation binding, and support level, with stable exit codes for CI (0 verified, 1 tampered, 2 unverifiable). |
| **Diagnose a run** | `mareforma diagnose -- python run.py` runs a target under the observer and reports what data actually flowed, and where a silent fallback hid. |
| **Optional public log** | Publish a claim to a public, append-only log for an independent, timestamped record. |
| **Local-first** | Runs on local SQLite. Network only for the optional log. |

## Reading trust from the graph

Trust is a property of a claim's position in the graph, never a self-reported score. The lead signal is **effective independence**: the number of pairwise-distinct (model, data, signer) checks behind a finding. Two agents on the same model that converge are one line of evidence, not two, so the count holds until a genuinely different model, or a human check, raises it. `mareforma map <claim>` reports the number and marks it `UNVERIFIABLE` when the model lineage is too soft to tell distinct checks apart, or when every validator traces to a single trust root (the honest reading when one operator could mint every key).

High-trust claims are re-checked against their signatures on every read, so a tampered claim in a shared graph is caught when you query, not served.

The older support labels still resolve this release, as deprecated one-release aliases. Read the effective-independence number, not the label:

| Level (deprecated alias) | Meaning |
|---|---|
| `PRELIMINARY` | One agent asserted it. No independent agreement yet. |
| `REPLICATED` | Two signers converged on the same finding. A convergence marker, not proof of independence: signing keys are operator-mintable, so distinct signatures are a weak prior. |
| `ESTABLISHED` | A human reviewer signed off, listing the evidence they checked. |

Classification stays a secondary axis the agent declares: `INFERRED` (model reasoning), `ANALYTICAL` (analysis run against real data), `DERIVED` (built on higher-trust claims). It is a declaration, kept honest by the computed grounding verdict above, never the trust signal on its own.

## Examples

| | Example | What it shows |
|---|---|---|
| 01 | [API Walkthrough](examples/01_api_walkthrough/) | The full API in one runnable script |
| 02 | [Compounding Agents](examples/02_compounding_agents/) | The absence catch and computed independence, run end to end |
| 03 | [Documented Contestation](examples/03_documented_contestation/) | An agent challenges established consensus |
| 04 | [Private Data, Public Findings](examples/04_private_data_public_findings/) | Two labs share provenance without sharing data |
| 05 | [Drug Target Provenance](examples/05_drug_target_provenance/) | A real research agent with honest evidence labels |
| 06 | [Verify in CI](examples/06_ci_verify/) | `mareforma verify` as a GitHub Actions gate, keyed on exit codes |

<hr>

[`AGENTS.md`](AGENTS.md): execution contract and adapters &nbsp;·&nbsp;
[`ARCHITECTURE.md`](ARCHITECTURE.md): system design &nbsp;·&nbsp;
[`SECURITY.md`](SECURITY.md): threat model &nbsp;·&nbsp;
[`CONTRIBUTING.md`](CONTRIBUTING.md): dev workflow &nbsp;·&nbsp;
[`CHANGELOG.md`](CHANGELOG.md): releases
