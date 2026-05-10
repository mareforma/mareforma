# Mareforma — agent integration guide

Mareforma is the epistemic layer AI scientists run on. It gives agents a
local graph for asserting claims with provenance and querying what has
already been established before making new assertions.

Trust in a claim is derived from the graph, not from the agent that made it.
No confidence score. No self-reporting. The structure of the provenance graph
is the only trust signal.

## Install

```bash
pip install mareforma
```

## Core pattern

```python
import mareforma

with mareforma.open() as graph:

    # 1. Query before asserting — check what is already established
    prior = graph.query("finding about topic X", min_support="REPLICATED")
    prior_ids = [c["claim_id"] for c in prior]

    # 2. Assert a claim, grounded in what the graph already supports
    claim_id = graph.assert_claim(
        "Cell type A exhibits property X under condition Y (n=842, p<0.001)",
        classification="ANALYTICAL",            # INFERRED (default) | ANALYTICAL | DERIVED
        generated_by="agent/model-a/lab_a",     # model + version + context
        supports=prior_ids,                     # upstream claim_ids this builds on
        source_name="dataset_alpha",            # data source this was derived from
        idempotency_key="run_abc_claim_1",      # retry-safe: same key → same id
    )

    # 3. Inspect the result
    claim = graph.get_claim(claim_id)
    print(claim["text"], claim["support_level"])
```

`graph.db` is created automatically on first `mareforma.open()`.
No `mareforma init` required.

---

## Classification

Classification encodes how knowledge was derived — the epistemic origin.
It is separate from trust level, which is graph-derived.

| Value | Use when |
|---|---|
| `INFERRED` | LLM reasoning, synthesis, extrapolation — default |
| `ANALYTICAL` | Deterministic analysis ran against source data and produced output |
| `DERIVED` | Explicitly built on ESTABLISHED or REPLICATED claims in the graph |

`DERIVED` incentivises agents to query the graph before asserting. A `DERIVED`
claim without `supports=` is unverifiable — the chain is broken.

---

## Support levels

| Level | Meaning | How reached |
|---|---|---|
| `PRELIMINARY` | One agent claimed it | Automatic on first assertion |
| `REPLICATED` | ≥2 independent agents converged on the same upstream | Automatic at INSERT |
| `ESTABLISHED` | Human-validated | `graph.validate()` only — requires REPLICATED first |

`REPLICATED` fires automatically when ≥2 claims share the same upstream
claim_id in `supports[]` and have different `generated_by` values.
No agent can self-promote to `ESTABLISHED`.

---

## Query filters

```python
# All claims
graph.query()

# Text search + minimum support
graph.query("topic X", min_support="REPLICATED", limit=10)

# By classification
graph.query(classification="ANALYTICAL")

# Only human-validated findings
graph.query(min_support="ESTABLISHED")

# Single record by id
graph.get_claim(claim_id)
```

---

## Idempotency

`idempotency_key` solves two distinct problems.

**Retry safety.** Same key → same `claim_id` returned, no duplicate inserted.
Use this whenever an agent run may be interrupted and retried:

```python
claim_id = graph.assert_claim("...", idempotency_key="run_abc_claim_1")
# Crash and retry — same claim_id returned, graph unchanged
claim_id = graph.assert_claim("...", idempotency_key="run_abc_claim_1")
```

**Convergence convention.** Agents running the same conceptual query should
use a structured key that encodes the semantic content of the claim — not a
random run ID. Two agents using the same key will converge on the same
`claim_id` even with different text phrasing, without needing explicit
`supports=` links between them:

```python
# Lab A
graph.assert_claim(
    "Target T is elevated in condition C (cohort_1, n=620)",
    idempotency_key="target_T_elevated_condition_C",
    generated_by="agent/model-a/lab_a",
)

# Lab B — same key, different text, different agent → same claim_id
graph.assert_claim(
    "Target T shows increased expression under condition C (cohort_2, n=580)",
    idempotency_key="target_T_elevated_condition_C",
    generated_by="agent/model-b/lab_b",
)
```

---

## generated_by convention

`generated_by` is the independence signal. `REPLICATED` fires only when two
claims have **different** `generated_by` values. If both claims share the same
identifier, convergence is not detected regardless of how different the text is.

Use a structured string encoding model + version + context:

```
"gpt-4o-2024-11/lab_a"          ✓ meaningful
"claude-sonnet-4-6/lab_b"        ✓ meaningful
"agent"                          ✗ meaningless — all claims look identical
"gpt-4o"                         ✗ no version, no context — indistinguishable across labs
```

This also makes provenance auditable over time: if a model version changes
behaviour, the `generated_by` field captures when the shift happened.

---

## Forbidden patterns

These patterns are accepted by the API but silently corrupt the epistemic graph.

**✗ Assert ANALYTICAL when the data pipeline returned null.**
If your analysis agent failed or returned no output, the finding came from
LLM prior knowledge. Record it as `INFERRED`.

```python
# Wrong
graph.assert_claim("Target T is relevant", classification="ANALYTICAL")  # no data ran

# Correct
result = run_analysis()
classification = "ANALYTICAL" if result else "INFERRED"
graph.assert_claim("Target T is relevant", classification=classification)
```

**✗ Assert DERIVED without `supports=`.**
A `DERIVED` claim with no upstream references is unverifiable. The provenance
chain is broken and a human reviewer cannot trace the reasoning.

```python
# Wrong
graph.assert_claim("...", classification="DERIVED")

# Correct
graph.assert_claim("...", classification="DERIVED", supports=[upstream_claim_id])
```

**✗ Use unstructured `generated_by`.**
`"agent"` or `"gpt-4o"` makes independence tracking meaningless. Two separate
labs become indistinguishable. `REPLICATED` will never fire between them.

**✗ Treat REPLICATED as proof of truth.**
Two agents repeating the same LLM prior — with no data pipeline behind either
finding — will both be `INFERRED` but can still trigger `REPLICATED` if they
share an upstream. Always check `classification` alongside `support_level`:

```python
results = graph.query("topic X", min_support="REPLICATED")
trustworthy = [
    r for r in results
    if r["classification"] == "ANALYTICAL" and r.get("source_name")
]
```

**✗ Call `graph.validate()` on a PRELIMINARY claim.**
`validate()` requires `support_level == "REPLICATED"`. Attempting to validate
a `PRELIMINARY` claim raises `ValueError`. ESTABLISHED is the gate for
consequential actions — it must not be reachable from a single-agent finding.

---

## Project layout

```
<project>/
  .mareforma/
    graph.db          ← epistemic graph (SQLite, WAL mode)
  claims.toml         ← human-readable backup, auto-generated after every write
```

---

## For more

- Examples: [github.com/mareforma/mareforma/examples](https://github.com/mareforma/mareforma/tree/main/examples)
- Full API reference: https://mareforma.com/docs
