# AI Agent Drug Target

[MEDEA](https://github.com/mims-harvard/Medea) is an AI scientist that identifies
drug targets from multi-omics data. This example runs MEDEA as an agent and
records every finding via the mareforma `EpistemicGraph`, giving each hypothesis
a verifiable epistemic status.

The driver is [`run_experiment.py`](run_experiment.py). Each step below is its
code followed by the console output it prints.

> **This example is not re-run on every change.** It needs the MEDEA package, a
> GPU-class machine, ~21 GB of MedeaDB, and an LLM API key. The output blocks
> below are from the recorded **Case B** run, where MEDEA's data pipeline
> returned `null`. The classification → support-level → warning are exactly what
> the code prints for that case; the hypothesis text and claim ids are run-specific
> and shown elided.

## The epistemic question

When an AI scientist returns a drug-target candidate, the question is not "what
did it find?" but "how did it find it?":

- Did MEDEA actually query MedeaDB and analyse omics data? → `ANALYTICAL`
- Or did the data pipeline fail silently and the answer come from LLM prior
  knowledge? → `INFERRED`

An `INFERRED` finding and an `ANALYTICAL` finding can read identically. The graph
records the distinction permanently, at assertion time.

Trust itself reads off the derived axes `graph.proposition_status(prop)` returns:
`status` per `content_id` (the answer) and `question_status` per `frame_id` (the
question). The `support_level` ladder this example prints is the legacy stored
per-claim axis, deprecated for v0.4.0.

## Two forks

| Fork | Disease | Cell type |
|------|---------|-----------|
| `ra_cd4` | Rheumatoid Arthritis | CD4+ T cells |
| `sle_cd4` | Systemic Lupus Erythematosus | CD4+ T cells |

Same model, same panelists, same debate rounds. One variable changed: the disease.

## Setup and run

```bash
# 1. Install and download data
python 05_drug_target_provenance.py --install
python 05_drug_target_provenance.py --data    # ~21 GB, takes a while

# 2. Configure API access
cp Medea/env_template.txt .env
# Edit .env: set OPENAI_API_KEY and MEDEADB_PATH=data/medeadb/raw

# 3. Run both forks
python 05_drug_target_provenance.py --run
```

`--run` writes this directory's `claims.toml`, the backup of the live graph.
The recorded Case B run is kept apart from it, under `recorded/`.

## Query-before-assert

```python
with mareforma.open(HERE) as graph:
    # Check for prior REPLICATED findings before running — MEDEA can build on
    # them rather than starting from scratch.
    prior = graph.query("drug target", min_support="REPLICATED")
```

```
  No prior REPLICATED findings — running both forks fresh.
```

## One fork: run, classify, record

```python
def _classify(result: dict) -> str:
    """ANALYTICAL if MEDEA's data pipeline ran, else INFERRED."""
    return "ANALYTICAL" if result.get("generated_code") else "INFERRED"

ra_result = _run_medea_fork(disease="rheumatoid arthritis", cell_type="CD4")
ra_classification = _classify(ra_result)   # null generated_code → INFERRED

# The classification is decided by what actually ran, then frozen into the claim.
ra_claim_id = graph.assert_claim(
    ra_result["final_hypothesis"],
    classification=ra_classification,
    generated_by="medea/gpt-4o/ra_cd4",
    source_name="medeadb",
)
```

```
  [1/2] Running MEDEA — Rheumatoid Arthritis / CD4+ T cells ...
    Classification: INFERRED
    Finding: <MEDEA's final hypothesis>
    Recorded claim: <claim-id>
```

The SLE fork is identical with `generated_by="medea/gpt-4o/sle_cd4"`.

## Epistemic status

```python
ra_claim  = graph.get_claim(ra_claim_id)
sle_claim = graph.get_claim(sle_claim_id)
print(f"  RA fork:   {ra_classification:10}  →  {ra_claim['support_level']}")
print(f"  SLE fork:  {sle_classification:10}  →  {sle_claim['support_level']}")

if ra_result["generated_code"] is None or sle_result["generated_code"] is None:
    # The data pipeline did not run — both findings are LLM prior knowledge.
    ...
```

```
  ============================================================
  EPISTEMIC STATUS
  ============================================================
    RA fork:   INFERRED    →  PRELIMINARY
    SLE fork:  INFERRED    →  PRELIMINARY

    ⚠  One or both forks returned null generated_code.
       Both findings are INFERRED — the data pipeline did not run.
       This was Case B in the original run. See the README for context.
```

## What this caught

Both forks returned `generated_code = null`. The final hypotheses still read
differently, but both came from LLM prior knowledge, not MedeaDB. mareforma
recorded both as `INFERRED`, making the silent failure visible immediately,
before anyone acted on the results. That led directly to a bug report in MEDEA's
EFO ID lookup: [mims-harvard/Medea#6](https://github.com/mims-harvard/Medea/pull/6).

The four claims of that run are kept at
[`recorded/case_b.claims.toml`](recorded/case_b.claims.toml). It is a 0.3.0
capture, taken before claims were signed, so it carries no signature bundles and
its `generated_by` reads `medea/gpt-4o` rather than the per-fork label the script
writes today. `mareforma.restore()` reads it and rebuilds the four claims, but
they come back unsigned, which the default `query()` drops. Pass
`include_unverified=True` to read them.

## Promoting a finding

An `INFERRED` finding is recorded, not discarded, and held at `PRELIMINARY`
until an independent line backs it. To reach `REPLICATED`, two conditions must
both hold: the converging claims carry **distinct, non-NULL** `asserter_keyid`
values, **and** both cite the same `ESTABLISHED` upstream claim in `supports[]`.
`generated_by` is a display label and plays no part in the gate. Without an
`ESTABLISHED` anchor, mareforma keeps both at `PRELIMINARY` rather than
promoting noise.

Both forks here go through one `mareforma.open(HERE)` handle, so one signing
key, and neither cites an upstream, so both stay `PRELIMINARY`. To promote,
sign the second fork under a distinct key (`mareforma.open(key_path=...)`, or
the per-call `signer=` override) and have both cite a shared `ESTABLISHED`
anchor. See [Example 03](../03_documented_contestation/) for the
seed-then-converge pattern.
