# Quickstart

Five minutes to your first claim.

## Install

```bash
uv add mareforma
```

## Open a graph

```python
import mareforma

graph = mareforma.open()
```

No project setup. No init command. `graph.db` is created in `.mareforma/`
under the current directory on first call.

## Assert a claim

```python
claim_id = graph.assert_claim(
    "Cell type A receives more inhibitory input than cell type B",
    classification="ANALYTICAL",
    generated_by="agent/model-a/lab_a",
    source_name="dataset_alpha",
)
```

`claim_id` is a UUID. The claim is immediately persisted and backed up to
`claims.toml`.

## Query what is already known

```python
results = graph.query("cell type A", min_support="PRELIMINARY")
for r in results:
    print(r["text"], r["support_level"])
```

Always query before asserting. If the graph already has a REPLICATED or
ESTABLISHED finding on your topic, build on it — don't duplicate it.

## Let two agents converge

```python
# Agent A
id_a = graph.assert_claim(
    "Target T is elevated in condition C (n=620)",
    classification="ANALYTICAL",
    generated_by="agent/model-a/lab_a",
    supports=["upstream_ref_A"],
    source_name="dataset_alpha",
)

# Agent B — independent data, same upstream reference
id_b = graph.assert_claim(
    "Target T is elevated in condition C (n=580)",
    classification="ANALYTICAL",
    generated_by="agent/model-b/lab_b",
    supports=["upstream_ref_A"],
    source_name="dataset_beta",
)

# REPLICATED fires automatically
graph.get_claim(id_a)["support_level"]  # → "REPLICATED"
graph.get_claim(id_b)["support_level"]  # → "REPLICATED"
```

Two independent agents, shared upstream, different `generated_by` →
`REPLICATED` is set automatically at INSERT time. No extra step required.

## Promote to ESTABLISHED

```python
graph.validate(id_a, validated_by="reviewer@example.org")
graph.get_claim(id_a)["support_level"]  # → "ESTABLISHED"
```

Only `REPLICATED` claims can be promoted. No automated path to `ESTABLISHED`.

## Close the graph

```python
graph.close()
```

Or use a context manager — it closes automatically on exit:

```python
with mareforma.open() as graph:
    graph.assert_claim("...")
```

## Next

- [Mental model](02_mental_model.md) — what is a claim, what is the graph, what is trust
- [Why mareforma](03_why_mareforma.md) — the problem it solves
- [AGENTS.md](../AGENTS.md) — full execution contract for agents
- [Examples](../examples/) — five runnable examples
