# AI Agent Drug Target

[MEDEA](https://github.com/mims-harvard/Medea) is an AI scientist that identifies drug targets from multi-omics data.
This example runs MEDEA as an agent and records every finding via the
mareforma EpistemicGraph — giving each hypothesis a verifiable epistemic status.

## The epistemic question

When an AI scientist returns a drug target candidate, the key question is not
"what did it find?" but "how did it find it?":

- Did MEDEA actually query MedeaDB and analyse omics data? → `ANALYTICAL`
- Or did the data pipeline fail silently and the answer came from LLM prior knowledge? → `INFERRED`

Mareforma records this distinction permanently at assertion time. An `INFERRED`
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

**Classification at assertion time.** After each MEDEA run, `run_experiment.py`
inspects `generated_code` from the output. If it is null — the data pipeline
failed — the claim is recorded as `INFERRED`. If real code ran and returned
output, it is recorded as `ANALYTICAL`.

```python
classification = "ANALYTICAL" if generated_code else "INFERRED"
graph.assert_claim(
    final_hypothesis,
    classification=classification,
    generated_by="medea/gpt-4o/ra_cd4",
    source_name="medeadb",
)
```

**Query-before-assert.** Before running MEDEA, `run_experiment.py` checks the
graph for prior `REPLICATED` findings. If they exist, MEDEA can build on them
rather than starting from scratch.

**Independent replication.** The two forks run with different `generated_by`
values (`ra_cd4` vs `sle_cd4`). If both reach the same conclusion through the
same upstream evidence, `REPLICATED` fires automatically — no extra step.

## What we found when we ran this

**Case B — data pipeline failed silently:**

Both forks returned `generated_code = null`. The final hypotheses still looked
different — but both came from LLM prior knowledge, not from MedeaDB. Mareforma
recorded both as `INFERRED`. The classification made the silent failure visible
immediately, before anyone acted on the results.

This led directly to a bug report in MEDEA's EFO ID lookup:
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
python 05_drug_target_provenance.py --run

# Inspect the epistemic status of findings
mareforma claim list
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
