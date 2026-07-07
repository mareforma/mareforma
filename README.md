<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/mareforma-wordmark-dark.svg">
    <img alt="Mareforma" src="assets/mareforma-wordmark.svg" width="420">
  </picture>
</p>

<p align="center">
  <strong>Verify your AI scientists' findings the way science verifies itself</strong>
  <br>
  <sub>signed provenance · independent replication · human validation · local-first</sub>
</p>

<p align="center">
  <a href="https://docs.mareforma.com/quickstart">Quickstart</a> &nbsp;·&nbsp;
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

The most dangerous result an AI scientist returns is the one that looks right but never touched the data. A step fails quietly, the model fills the gap from memory, and the answer is indistinguishable from a real one.

```python
graph.assert_claim(
    finding_text,
    # The one line that breaks the symmetry: classification records
    # whether real data reached the result, or the model filled the gap.
    classification="ANALYTICAL" if data_ran else "INFERRED",
    generated_by="agent/lab-a",
    source_name=dataset_id if data_loaded else None,
)
```

Ask for grounded, replicated claims and the fabricated one drops out: still recorded and traceable, never trusted. [Example 05](examples/05_drug_target_provenance/) runs this against a real research agent.

## Install

```bash
uv add mareforma
mareforma bootstrap   # optional: sign your claims and enable the public log
```

## Capabilities

| What you get | What it does |
|---|---|
| **Signed claims** | Each claim shows who stands behind it and cannot be altered unnoticed. |
| **Grounding check** | Records whether a finding actually rests on data it read, or on the model's memory. |
| **Independently verifiable** | Anyone can re-check a claim, or a whole exported bundle, without trusting whoever produced it. |
| **Optional public log** | Publish a claim to a public, append-only log for an independent, timestamped record. |
| **Local-first** | Runs on local SQLite. Network only for the optional log. |

## The trust ladder

A claim's support level is read from the graph, never self-reported. High-trust claims are re-checked against their signatures on every read, so a tampered claim in a shared graph is caught when you query, not served.

| Level | Meaning |
|---|---|
| `PRELIMINARY` | One agent asserted it. No independent agreement yet. |
| `REPLICATED` | Two independent agents, signing with different keys, reached the same result on a shared established finding. Independence is the key, so relabeling cannot fake it. |
| `ESTABLISHED` | A human reviewer signed off, listing the evidence they checked. |

Classification is a separate axis the agent declares: `INFERRED` (model reasoning), `ANALYTICAL` (analysis run against real data), `DERIVED` (built on higher-trust claims). Ask for both at once: `graph.query(text, min_support="REPLICATED", classification="ANALYTICAL")`.

## Examples

| | Example | What it shows |
|---|---|---|
| 01 | [API Walkthrough](examples/01_api_walkthrough/) | The full API in one runnable script |
| 02 | [Compounding Agents](examples/02_compounding_agents/) | Findings accumulate across agent runs |
| 03 | [Documented Contestation](examples/03_documented_contestation/) | An agent challenges established consensus |
| 04 | [Private Data, Public Findings](examples/04_private_data_public_findings/) | Two labs share provenance without sharing data |
| 05 | [Drug Target Provenance](examples/05_drug_target_provenance/) | A real research agent with honest evidence labels |

<hr>

[`AGENTS.md`](AGENTS.md): execution contract and adapters &nbsp;·&nbsp;
[`ARCHITECTURE.md`](ARCHITECTURE.md): system design &nbsp;·&nbsp;
[`SECURITY.md`](SECURITY.md): threat model &nbsp;·&nbsp;
[`CONTRIBUTING.md`](CONTRIBUTING.md): dev workflow &nbsp;·&nbsp;
[`CHANGELOG.md`](CHANGELOG.md): releases
