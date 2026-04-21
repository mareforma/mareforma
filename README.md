# mareforma

[![Python](https://img.shields.io/pypi/pyversions/mareforma)](https://pypi.org/project/mareforma/)
[![Tests](https://github.com/mareforma/mareforma/actions/workflows/tests.yml/badge.svg)](https://github.com/mareforma/mareforma/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/mareforma)](https://pypi.org/project/mareforma/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Automatic epistemic provenance for life sciences pipelines. Write transforms, run `build`, and mareforma figures out what kind of result you produced and how well-supported it is — no manual annotation required.

---

## Install

```bash
uv pip install mareforma
```

Requires Python ≥ 3.10.

---

## How it works

Write normal Python pipeline functions. mareforma auto-classifies each result.

```python
from mareforma import transform, BuildContext
import pandas as pd

@transform("morphology.load")
def load(ctx: BuildContext) -> None:
    files = list(ctx.source_path("morphology").glob("*.swc"))
    ctx.save("skeletons", files, fmt="pickle")

@transform("morphology.features", depends_on=["morphology.load"])
def compute_features(ctx: BuildContext) -> None:
    skeletons = ctx.load("morphology.load.skeletons")
    df = pd.DataFrame([_extract_features(s) for s in skeletons])
    ctx.save("features", df, fmt="csv")
```

```bash
mareforma build
# ✓ morphology.load      done  (1.2s)
# ✓ morphology.features  done  (3.8s)

mareforma trace morphology.features
# morphology
# └── morphology.load ──────── RAW        ── SINGLE
#     └── morphology.features  ANALYSED   ── REPLICATED ◇
```

That's it. No annotations. mareforma reads your artifacts, classifies each transform, and tracks support level automatically.

---

## What gets classified automatically

**Transform class** — inferred from artifact content:

| Class | Meaning |
|---|---|
| `RAW` | Root node — no upstream dependencies |
| `PROCESSED` | Output values ⊆ input values, row count ≤ input count |
| `ANALYSED` | New values computed, within input value range |
| `INFERRED` | Output values outside all input ranges |

**Support level** — inferred from run history:

| Level | Meaning |
|---|---|
| `SINGLE` | One run |
| `REPLICATED ◇` | Same output hash across ≥2 runs |
| `CONVERGED ●` | Same step name across ≥2 independent sources |
| `CONSISTENT ◆` | A run has a DOI-linked claim in `supports` |
| `ESTABLISHED ●●` | CONVERGED + CONSISTENT |

SINGLE through CONVERGED require no annotation. CONSISTENT and ESTABLISHED require one DOI string in a claim.

---

## Quickstart

```bash
# 1. Init
cd my_project/
mareforma init

# 2. Register a data source
mareforma add-source morphology --path data/morphology/raw/ \
    --description "Neuron skeleton reconstructions"

# 3. Build — classification is automatic
mareforma build

# 4. Inspect the epistemic graph
mareforma trace morphology.features

# 5. Check overall health
mareforma status

# 6. Optional: link a result to literature (unlocks CONSISTENT)
mareforma claim add "Neuron size increases with cortical depth" \
    --source morphology --supports 10.64898/2026.03.05.709819

# 7. Export provenance graph
mareforma export
```

---

## BuildContext API

| Method | Description |
|---|---|
| `ctx.source_path("name")` | Raw data path for a registered source |
| `ctx.save("name", data, fmt=...)` | Persist artifact (`pickle`, `parquet`, `csv`, `numpy`) |
| `ctx.load("transform.artifact")` | Load upstream artifact |
| `ctx.claim("text", supports=[DOI])` | Optional: link this run to literature |
| `ctx.log("message")` | Write to console |

---

## CLI reference

| Command | Description |
|---|---|
| `mareforma init` | Initialise project |
| `mareforma add-source <name>` | Register a data source |
| `mareforma check` | Validate paths and required fields |
| `mareforma build [source]` | Run the pipeline DAG (`--dry-run`, `--force`) |
| `mareforma trace <transform>` | Ancestry tree with class and support level (`--json`) |
| `mareforma status` | Epistemic health dashboard (`--json`) |
| `mareforma diff <transform>` | Compare the two most recent runs (`--json`) |
| `mareforma log` | Last build status (`--json`) |
| `mareforma explain [source]` | Dump project ontology (`--json`) |
| `mareforma export` | Write `ontology.jsonld` |
| `mareforma claim add TEXT` | Link a result to literature (`--supports DOI`) |
| `mareforma claim list` | List claims (`--status`, `--source`, `--json`) |
| `mareforma claim show ID` | Full claim detail |
| `mareforma claim update ID` | Update confidence, status, or supports |

---

## Project structure

```
my_project/
├── .mareforma/
│   └── graph.db               ← provenance graph (commit this)
├── mareforma.project.toml     ← project ontology (commit this)
├── claims.toml                ← claims backup, auto-generated (commit this)
├── ontology.jsonld            ← JSON-LD export (commit this)
└── data/
    └── source_name/
        ├── raw/               ← your data
        └── preprocessing/
            └── build_transform.py
```
