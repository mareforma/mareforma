# Mareforma — agent integration guide

Mareforma is the epistemic layer AI scientists run on. It gives agents a
local SQLite graph for asserting claims with provenance and querying what
has already been established before making new assertions.

## Install

```bash
pip install mareforma
```

## Core pattern

```python
import mareforma

# Open the graph (creates .mareforma/graph.db in cwd if missing)
graph = mareforma.open()

# Assert a claim — returns a stable claim_id
claim_id = graph.assert_claim(
    "BC cells receive more inhibitory input than MC cells",
    classification="ANALYTICAL",       # ANALYTICAL | DERIVED | INFERRED (default)
    stated_confidence=0.85,            # float 0.0–1.0, default 0.4
    supports=["prior_claim_uuid"],     # upstream claim_ids this builds on
    idempotency_key="run_abc_claim_1", # retry-safe: same key → same id
)

# Query before asserting
established = graph.query("inhibitory input", min_support="REPLICATED")
for claim in established:
    print(claim["text"], claim["support_level"], claim["stated_confidence"])

# Inspect a specific claim
claim = graph.get_claim(claim_id)

graph.close()
```

Context manager (recommended for agents):

```python
with mareforma.open() as graph:
    claim_id = graph.assert_claim("...", stated_confidence=0.9)
```

## Classification

| Value | Use when |
|---|---|
| `INFERRED` | LLM reasoning, synthesis, extrapolation (default) |
| `ANALYTICAL` | You ran deterministic code against data |
| `DERIVED` | You explicitly built on ESTABLISHED or REPLICATED claims |

## Support levels

| Level | Meaning |
|---|---|
| `PRELIMINARY` | One agent claimed it |
| `REPLICATED` | ≥2 independent agents reached the same conclusion (auto-detected) |
| `ESTABLISHED` | Human-validated — only reachable via `graph.validate()` |

REPLICATED is set automatically when two claims share an upstream in `supports[]`
and have different `generated_by` values.

## Query filters

```python
# All established claims
graph.query(min_support="ESTABLISHED")

# Text search + support filter
graph.query("synaptic", min_support="REPLICATED", limit=10)

# By classification
graph.query(classification="ANALYTICAL")
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
