# AI Agent Drug Target — MEDEA + Mareforma

[MEDEA](https://github.com/mims-harvard/Medea) is an AI scientist that identifies drug targets from multi-omics data.
This example wraps MEDEA in a `@transform` pipeline to give every finding
a verifiable epistemic status — not just a hypothesis text.

## The epistemic question

When an AI scientist returns a drug target candidate, the key question is not
"what did it find?" but "how did it find it?":

- Did MEDEA actually query MedeaDB and analyse omics data? → `ANALYTICAL`
- Or did the data pipeline fail silently and the answer came from LLM prior knowledge? → `INFERRED`

Mareforma records this distinction permanently in the claim. An `INFERRED`
finding and an `ANALYTICAL` finding can look identical as text — the graph
is what makes the difference visible.

## Two forks

| Fork | Disease | Cell type |
|------|---------|-----------|
| `ra_cd4` | Rheumatoid Arthritis | CD4+ T cells |
| `sle_cd4` | Systemic Lupus Erythematosus | CD4+ T cells |

Everything else is identical: same model, same panelists, same debate rounds.
One variable changed — the disease.

## What mareforma adds

**Classification at assertion time.** Each `ctx.claim()` call inspects
`generated_code` from MEDEA's output. If it is null — the data pipeline
failed — the claim is recorded as `INFERRED`. If real code ran and returned
output, it is recorded as `ANALYTICAL`.

```python
classification = "ANALYTICAL" if generated_code else "INFERRED"
ctx.claim(text=final, classification=classification, source_name="medeadb", ...)
```

**Query-before-assert.** Before running MEDEA, each transform checks the
graph for prior `REPLICATED` findings on the same disease. If they exist,
MEDEA can build on them rather than starting from scratch.

**Cross-diff for process comparison.** After both forks complete, `cross-diff`
shows whether MEDEA generated different database queries per disease, or ran
the same query regardless:

```bash
mareforma cross-diff ra_cd4.medea_run sle_cd4.medea_run
```

**Case A — data pipeline ran and adapted to the disease:**
```
≠  generated_code   (2102B → 2806B)
≠  executed_output
≠  final_hypothesis
```
The two targets were reached by two different data paths. The divergence
is data-driven.

**Case B — data pipeline failed silently:**
```
=  generated_code   (4 bytes = null)
=  executed_output  (4 bytes = null)
≠  final_hypothesis
```
Both runs returned null. The hypotheses still look different — but both
came from LLM prior knowledge, not from MedeaDB. Mareforma records both
as `INFERRED`. The graph makes this visible before anyone acts on the result.

When we ran this example, we hit Case B. `cross-diff` surfaced it
immediately and led to a bug report in MEDEA's EFO ID lookup:
[mims-harvard/Medea#6](https://github.com/mims-harvard/Medea/pull/6).

## Setup

```bash
# 1. Install and download data
python 05_drug_target_provenance.py --install
python 05_drug_target_provenance.py --data    # ~21 GB, takes a while

# 2. Configure API access
cp Medea/env_template.txt .env
# Edit .env: set OPENAI_API_KEY and MEDEADB_PATH=data/medeadb/raw
```

## Run

```bash
mareforma build

# Inspect the epistemic status of findings
mareforma claim list

# Compare the two forks artifact-by-artifact
mareforma cross-diff ra_cd4.medea_run sle_cd4.medea_run

# Full epistemic health
mareforma status
```

## Interpreting the claims

After the run, `claims.toml` contains one claim per fork.

If `classification = "INFERRED"` — the data pipeline did not run. The finding
is LLM prior knowledge. It is recorded, not discarded, but treated as
`PRELIMINARY` until independently replicated with real data.

If `classification = "ANALYTICAL"` — MEDEA queried MedeaDB and produced
output. The finding is grounded in omics data and the provenance chain
is complete.

To upgrade a finding to `REPLICATED`, run an additional fork (different
disease or cell type) that reaches the same conclusion through the same
upstream evidence. Mareforma detects this automatically.

