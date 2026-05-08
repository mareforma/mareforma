# Mareforma — agent integration guide

Mareforma is the epistemic layer AI scientists run on. It gives agents a
local graph for asserting claims with provenance and querying what has
already been established before making new assertions.

## Install

```bash
pip install mareforma
```

## Core pattern

```python
import mareforma

with mareforma.open() as graph:

    # Query before asserting — check what is already established
    prior = graph.query("inhibitory input", min_support="REPLICATED")
    prior_ids = [c["claim_id"] for c in prior]

    # Assert a claim
    claim_id = graph.assert_claim(
        "BC cells receive more inhibitory input than MC cells",
        classification="ANALYTICAL",       # ANALYTICAL | DERIVED | INFERRED (default)
        supports=prior_ids,                # upstream claim_ids this builds on
        idempotency_key="run_abc_claim_1", # retry-safe: same key → same id
    )

    # Inspect the result
    claim = graph.get_claim(claim_id)
    print(claim["text"], claim["support_level"])
```

## Classification

| Value | Use when |
|---|---|
| `INFERRED` | LLM reasoning, synthesis, extrapolation (default) |
| `ANALYTICAL` | You ran deterministic code against data |
| `DERIVED` | You explicitly built on ESTABLISHED or REPLICATED claims |

## Support levels

Trust in a claim is derived from the graph, not from the agent that made it.

| Level | Meaning |
|---|---|
| `PRELIMINARY` | One agent claimed it |
| `REPLICATED` | ≥2 independent agents reached the same conclusion (auto-detected) |
| `ESTABLISHED` | Human-validated — only reachable via `graph.validate()` |

REPLICATED is set automatically when two claims share an upstream in `supports[]`
and have different `generated_by` values. No agent can self-promote to ESTABLISHED.

## Query filters

```python
# All claims
graph.query()

# Text search + minimum support
graph.query("synaptic", min_support="REPLICATED", limit=10)

# By classification
graph.query(classification="ANALYTICAL")

# Only human-validated findings
graph.query(min_support="ESTABLISHED")
```

## Idempotency

Pass `idempotency_key` for retry-safe writes. If the key already exists,
the existing `claim_id` is returned without inserting a duplicate:

```python
claim_id = graph.assert_claim("...", idempotency_key="my_run_claim_0")
# Retry after crash — same claim_id returned, no duplicate
claim_id = graph.assert_claim("...", idempotency_key="my_run_claim_0")
```

## Project layout

```
<project>/
  .mareforma/
    graph.db          ← epistemic graph (SQLite, WAL mode)
  claims.toml         ← human-readable backup, auto-generated
```

`graph.db` is created automatically on first `mareforma.open()`.
No `mareforma init` required.

## For more

Full API reference: https://mareforma.com/docs
