# Migration guide — v0.2.x → v0.3.0

v0.3.0 is an **intentional breaking change**. The database schema, API, and claim model
have been redesigned for AI-native use. There is no automatic migration.

---

## Upgrade steps

1. Back up your project (especially `claims.toml` — it is always a human-readable copy
   of every claim).
2. Delete `.mareforma/graph.db`.
3. `pip install --upgrade mareforma`
4. Run `mareforma build` or `python -c "import mareforma; mareforma.open()"` to create
   a fresh v0.3.0 database.

> If you try to open a v0.2.x `graph.db` with v0.3.0, a `DatabaseError` is raised
> with instructions to delete the file.

---

## What changed and why

v0.2.x assumed a human designed the pipeline. An AI scientist could not interact
with the epistemic graph without a human first writing `@transform` decorators and
running `mareforma init`. v0.3.0 fixes this with a new primary interface:

```python
with mareforma.open() as graph:
    claim_id = graph.assert_claim(
        "BC cells receive more inhibitory input than MC cells",
        classification="ANALYTICAL",
        supports=["10.1038/s41586-023-06814-7"],
    )
    results = graph.query("inhibitory input", min_support="REPLICATED")
```

`@transform` and `BuildContext` are preserved and unchanged.

---

## Breaking changes

### 1. Database schema — no migration, delete graph.db

The `claims` table has been redesigned. Columns removed: `confidence_float`,
`replication_status`, `generation_method`. Columns added: `classification`,
`support_level`, `idempotency_key`, `validated_by`, `validated_at`.

### 2. `confidence_float` removed

Stated confidence is gone from the API entirely. Trust in a claim is derived from the
graph (how many independent agents reached the same conclusion), not from
agent self-reporting.

### 3. Support levels — 3 levels replace 5

The old `replication_status` values are replaced by three `support_level` values:

| Old | New |
|---|---|
| `single_study` / `unknown` | `PRELIMINARY` |
| `independently_replicated` | `REPLICATED` |
| `meta_analyzed` | `ESTABLISHED` |

`REPLICATED` is set automatically when ≥2 claims with different `generated_by`
share the same upstream in `supports[]`. `ESTABLISHED` can only be set by
`graph.validate(claim_id)` — there is no automated path.

### 4. Claim classification — 3 labels replace 4

| Label | Meaning |
|---|---|
| `INFERRED` | Default — LLM reasoning without explicit grounding |
| `ANALYTICAL` | Deterministic analysis against source data |
| `DERIVED` | Built on ESTABLISHED or REPLICATED claims |

### 5. `query_claims()` signature change

`min_confidence` is removed. New parameters: `text` (substring filter),
`min_support` (support level filter), `classification` (classification filter).

---

## What is unchanged

- `@transform` decorator — works exactly as before
- `BuildContext` — `ctx.claim()`, `ctx.save()`, `ctx.params`, etc.
- `MareformaObserver` and `LangChainAdapter`
- All CLI commands: `build`, `status`, `trace`, `diff`, `cross-diff`, `log`,
  `claim`, `export`
- `claims.toml` backup — auto-generated on every claim mutation
- `ontology.jsonld` export — extended with new fields
- Local-first, no network calls, SQLite
