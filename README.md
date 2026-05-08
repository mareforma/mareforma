# mareforma

[![Python](https://img.shields.io/pypi/pyversions/mareforma)](https://pypi.org/project/mareforma/)
[![Tests](https://github.com/mareforma/mareforma/actions/workflows/tests.yml/badge.svg)](https://github.com/mareforma/mareforma/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/mareforma)](https://pypi.org/project/mareforma/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

The epistemic layer AI scientists run on.

---

## Install

```bash
uv pip install mareforma
```

Requires Python ≥ 3.10.

---

## Agent interface

```python
import mareforma

with mareforma.open() as graph:

    # Query before asserting — check what is already established
    prior = graph.query("inhibitory neurons", min_support="REPLICATED")
    prior_ids = [c["claim_id"] for c in prior]

    # Assert a finding
    claim_id = graph.assert_claim(
        "BC cells receive more inhibitory input than MC cells",
        classification="ANALYTICAL",
        supports=prior_ids,
        idempotency_key="run_abc_claim_1",
    )
```

No `mareforma init` required. `graph.db` is created on first `open()`.

Trust is derived from the graph, not from the agent.

**Support levels** — set automatically:

| Level | Meaning |
|---|---|
| `PRELIMINARY` | One agent claimed it |
| `REPLICATED` | ≥2 independent agents reached the same conclusion |
| `ESTABLISHED` | Human-validated via `graph.validate(claim_id)` only |

**Claim classification** — declared by the agent:

| Classification | Use when |
|---|---|
| `INFERRED` | LLM reasoning or extrapolation (default) |
| `ANALYTICAL` | Deterministic code ran against data |
| `DERIVED` | Explicitly built on ESTABLISHED or REPLICATED claims |

---

## Pipeline interface

For human-authored pipelines, `@transform` records provenance automatically:

```python
from mareforma import transform, BuildContext

@transform("morphology.load")
def load(ctx: BuildContext) -> None:
    files = list(ctx.source_path("morphology").glob("*.swc"))
    ctx.save("skeletons", files, fmt="pickle")

@transform("morphology.features", depends_on=["morphology.load"])
def compute_features(ctx: BuildContext) -> None:
    skeletons = ctx.load("morphology.load.skeletons")
    df = pd.DataFrame([_extract_features(s) for s in skeletons])
    ctx.save("features", df, fmt="csv")
    ctx.claim("Feature extraction complete", supports=["10.64898/2026.03.05.709819"])
```

```bash
mareforma build
mareforma trace morphology.features
mareforma status
```

---

## CLI reference

| Command | Description |
|---|---|
| `mareforma init` | Initialise a pipeline project |
| `mareforma add-source <name>` | Register a data source |
| `mareforma build [source]` | Run the pipeline DAG (`--dry-run`, `--force`) |
| `mareforma trace <transform>` | Ancestry tree with class and support level |
| `mareforma status` | Epistemic health dashboard (`--json`) |
| `mareforma diff <transform>` | Compare the two most recent runs |
| `mareforma log` | Last build status |
| `mareforma export` | Write `ontology.jsonld` (PROV-O, schema.org) |
| `mareforma claim add TEXT` | Add a claim with optional DOI support |
| `mareforma claim list` | List claims |

---

## Project layout

```
<project>/
  .mareforma/
    graph.db               ← epistemic graph (SQLite, WAL)
  claims.toml              ← human-readable claims backup
  mareforma.project.toml   ← project config (pipeline interface)
  ontology.jsonld          ← PROV-O export
```
