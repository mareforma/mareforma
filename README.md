<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/mareforma-wordmark-dark.svg">
    <img alt="Mareforma" src="assets/mareforma-wordmark.svg" width="420">
  </picture>
</p>

<p align="center">
  <strong>Where AI discovery becomes science</strong>
  <br>
  <sub>trust earned, not declared · uses real data · effectively independent · publicly verifiable · local-first</sub>
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

A finding whose step never ran cannot earn `GROUNDED`. The observer saw no read of the cited data, so it returns `UNGROUNDED`. The model that filled the gap from memory is caught by what ran, not by a label it chose. [Example 02](examples/02_compounding_agents/) runs this catch end to end.

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
| **Trust map** | `mareforma map <claim>` shows every trust property (grounding, independence, contestation, witnessing) with how far it can be trusted, and says plainly what it cannot check. |
| **Verify** | `mareforma verify <claim>` re-checks the signatures, that the grounding verdict matches the data the finding cites, and the support level, with stable exit codes for CI (0 verified, 1 tampered, 2 unverifiable). |
| **Diagnose a run** | `mareforma diagnose -- python run.py` runs a target under the observer and reports what data actually flowed, and where a silent fallback hid. |
| **Audit a pipeline** | `mareforma audit --findings map.json -- python run.py` runs a pipeline that never imports mareforma and signs one grounding receipt per finding, from the observer alone. |
| **Optional public log** | Publish a claim to a public, append-only log for an independent, timestamped record. |
| **Local-first** | Runs on local SQLite. Network only for the optional log. |

## Reading trust from the graph

Trust comes from a claim's place in the graph, never from a self-reported score. The lead signal is **effective independence**: how many checks behind a finding differ in model, data, and signer. Two agents on the same model are one line of evidence, not two, so the count holds until a genuinely different model, or a human check, raises it. `mareforma map <claim>` reports the number and marks it `UNVERIFIABLE` when it cannot tell the models apart. When every signer traces back to one operator who could have made all the keys, it names that single trust root on the count: the number then rests on distinct model or human lines within one trust domain, not independence across operators.

High-trust claims are re-checked against their signatures on every read, so a tampered claim in a shared graph is caught when you query, not served.

Classification stays a secondary label the agent declares: `INFERRED` (model reasoning), `ANALYTICAL` (analysis run against real data), `DERIVED` (built on higher-trust claims). It is a declaration, kept honest by the computed grounding verdict above, never the trust signal on its own.

## Examples

| | Example | What it shows |
|---|---|---|
| 01 | [API Walkthrough](examples/01_api_walkthrough/) | The full API in one runnable script |
| 02 | [Compounding Agents](examples/02_compounding_agents/) | The absence catch and computed independence, run end to end |
| 03 | [Documented Contestation](examples/03_documented_contestation/) | An agent challenges established consensus |
| 04 | [Private Data, Public Findings](examples/04_private_data_public_findings/) | Two labs share how they reached a finding without sharing the data |
| 05 | [Drug Target Provenance](examples/05_drug_target_provenance/) | A real research agent that labels which findings come from real data and which from the model's guess |
| 06 | [Verify in CI](examples/06_ci_verify/) | `mareforma verify` as a GitHub Actions gate, keyed on exit codes |

<hr>

[`AGENTS.md`](AGENTS.md): execution contract and adapters &nbsp;·&nbsp;
[`ARCHITECTURE.md`](ARCHITECTURE.md): system design &nbsp;·&nbsp;
[`SECURITY.md`](SECURITY.md): threat model &nbsp;·&nbsp;
[`CONTRIBUTING.md`](CONTRIBUTING.md): dev workflow &nbsp;·&nbsp;
[`CHANGELOG.md`](CHANGELOG.md): releases
