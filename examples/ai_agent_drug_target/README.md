# AI Agent Drug Target — Provenance for MEDEA Findings

MEDEA is an AI scientist that identifies drug targets from multi-omics data.

This example wraps MEDEA in `@transform` without changing a single line of
MEDEA's code. That one addition gives you three things for free:

1. **Silent failure detection** — know when a confident-looking answer has no data behind it
2. **Process comparison** — verify that two different findings were reached by two different data paths
3. **A trail you can rerun** — every artifact, every claim, committed and reproducible

## The two forks

Two MEDEA runs, one variable changed:

| Fork | Disease | Cell type |
|------|---------|-----------|
| `ra_cd4` | Rheumatoid Arthritis | CD4+ T cells |
| `sle_cd4` | Systemic Lupus Erythematosus | CD4+ T cells |

Everything else is identical: same LLM, same temperature, same panelists,
same debate rounds.

## Setup

```bash
# 1. Install dependencies
pip install git+https://github.com/mims-harvard/MEDEA
pip install mareforma python-dotenv

# 2. Download MedeaDB (~20 GB)
huggingface-cli download mims-harvard/MedeaDB --repo-type dataset --local-dir data/medeadb/raw/

# 3. Configure API access
cp .env.example .env
# Edit .env: set OPENAI_API_KEY
```

**.env.example**
```
OPENAI_API_KEY=your-openai-key
MEDEADB_PATH=data/medeadb/raw
MEDEA_LLM=gpt-4o   # optional, this is the default
```

## Run

```bash
# From this directory
mareforma build

# Compare the two findings
mareforma cross-diff ra_cd4.medea_run sle_cd4.medea_run
```

## What cross-diff shows

MEDEA captures several intermediate artifacts per run. The one that matters
most is `generated_code` — the Python code MEDEA generated to query MedeaDB.

**Case A — data pipeline ran and adapted to the disease:**
```
≠  generated_code   (2102B → 2806B)
≠  executed_output  (50B → 47B)
≠  final_hypothesis
```
MEDEA queried the database differently for each disease. The two targets
were reached by two different data paths. The divergence is data-driven.

**Case B — data pipeline failed silently:**
```
=  generated_code   (4 bytes = null)
=  executed_output  (4 bytes = null)
≠  final_hypothesis
```
Both runs returned null from the data layer. The final hypotheses still
look different — but the difference came entirely from LLM prior knowledge,
not from the data. Without capturing `generated_code` as an artifact, there
is no way to tell these two cases apart.

When we ran this example, we hit Case B first. `cross-diff` surfaced it
immediately. Investigating the null artifact led to a bug in MEDEA's EFO ID
lookup that caused the data pipeline to fail silently for common diseases
including Rheumatoid Arthritis and SLE — see
[mims-harvard/Medea#6](https://github.com/mims-harvard/Medea/pull/6).

## Interpreting the claims

After the run, `claims.toml` contains two entries — one per fork — each
tagged `generated_by = medea/<model>` and `generation_method = agent-wrapped`.
Both carry `confidence = exploratory`: a single AI-generated run with no
replication.

To upgrade confidence, run additional forks and check convergence:

| Evidence | Confidence |
|----------|------------|
| Single run | `exploratory` |
| Replicate run, `generated_code = SAME` | `preliminary` |
| Multiple forks (cell types, datasets) converging on same target | `reproducible` |

A replicate is just another transform with identical config:

```python
@transform("ra_cd4_rep2.medea_run")
def ra_cd4_rep2_run(ctx):
    # identical to ra_cd4_run
```

```bash
mareforma cross-diff ra_cd4.medea_run ra_cd4_rep2.medea_run
```

If `generated_code = SAME` — the data pipeline is reproducible and the
finding earns `preliminary`.

## Adding more forks

To stress-test further, add a third transform to `build_transform.py`:

```python
@transform("ra_cd8.medea_run")
def ra_cd8_run(ctx):
    """Fork: swap cell type (CD8+ instead of CD4+)."""
    result = medea(user_instruction=QUERY_RA_CD8, ...)
```

```bash
mareforma cross-diff ra_cd4.medea_run ra_cd8.medea_run
```

If `ra_cd4` and `ra_cd8` converge on the same target across cell types,
that target is robust — a stronger signal than either run alone.

## File layout

```
examples/ai_agent_drug_target/
├── mareforma.project.toml          project metadata and source registration
├── data/medeadb/
│   ├── raw/                        MedeaDB (gitignored — download separately)
│   └── preprocessing/
│       └── build_transform.py      the two MEDEA forks
├── claims.toml                     findings with provenance (committed)
├── ontology.jsonld                 JSON-LD export for publication (committed)
└── .mareforma/
    └── commits/transforms.jsonl   full run log (committed)
```
