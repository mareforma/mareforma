# Migration guide — v0.2.x → v0.3.0

v0.3.0 is an **intentional breaking change**. The API has been redesigned from the
ground up for AI-native use. This document explains what changed and how to update.

---

## What changed and why

v0.2.x assumed a human designed the pipeline. An AI scientist could not interact
with the epistemic graph without a human first writing `@transform` decorators and
running `mareforma init`. v0.3.0 fixes this with a new primary interface:

```python
graph = mareforma.open()
claim_id = graph.assert_claim("BC cells receive more inhibitory input than MC cells",
                              classification="ANALYTICAL",
                              stated_confidence=0.85)
results = graph.query("inhibitory input", min_support="REPLICATED")
```

`@transform` and `BuildContext` are preserved and unchanged. They remain the right
tool for human-authored pipeline steps. The new interface is the right tool for
agents that need to read from and write to the graph directly.

---

## Breaking changes

### 1. Database schema — `graph.db` migrates automatically

On first `mareforma.open()` or `mareforma build` with v0.3.0, `graph.db` migrates
from `user_version=1` to `user_version=2`. The migration:

- Renames `confidence_float` → `stated_confidence` in the `claims` table
- Adds columns: `classification`, `support_level`, `idempotency_key`,
  `validated_by`, `validated_at`
- Formally incorporates the `agent_events` table into the versioned schema

**Existing claims are preserved.** Defaults applied to existing rows:
- `classification = 'INFERRED'`
- `support_level = 'PRELIMINARY'`
- `stated_confidence` = former `confidence_float` value

**Back up `graph.db` before upgrading** if you need to retain the ability to
downgrade. `claims.toml` is always a human-readable backup.

### 2. `confidence_float` column renamed to `stated_confidence`

Any code that reads `row["confidence_float"]` directly from SQLite must be updated
to `row["stated_confidence"]`.

### 3. Support levels — 3 levels replace 5

The old `replication_status` values (`single_study`, `independently_replicated`,
`failed_replication`, `meta_analyzed`, `unknown`) are replaced by three
`support_level` values:

| Old | New |
|---|---|
| `single_study` / `unknown` | `PRELIMINARY` |
| `independently_replicated` | `REPLICATED` |
| `meta_analyzed` | `ESTABLISHED` (requires human validation) |

`ESTABLISHED` can **only** be set by `graph.validate(claim_id)` — there is no
automated path.

### 4. Claim classification — 3 labels replace 4

The old pipeline-level `transform_class` values (RAW/PROCESSED/ANALYSED/INFERRED)
are not claim-level classifications. Claims now have:

| Label | Meaning |
|---|---|
| `INFERRED` | Default — LLM reasoning without explicit grounding |
| `ANALYTICAL` | Deterministic analysis against source data (agent-declared) |
| `DERIVED` | Built on ESTABLISHED or REPLICATED claims (agent-declared) |

### 5. `query_claims()` signature change

The `min_confidence` parameter now accepts `stated_confidence` float threshold.
New parameters `text` (substring filter) and `min_support` (support level filter)
have been added. See `db.py` docstring.

---

## What is unchanged

- `@transform` decorator — works exactly as before
- `BuildContext` — all methods unchanged (`ctx.claim()`, `ctx.save()`, `ctx.params`, etc.)
- `MareformaObserver` and `LangChainAdapter` — unchanged
- `mareforma build`, `mareforma status`, `mareforma trace`, `mareforma diff`,
  `mareforma cross-diff`, `mareforma log`, `mareforma claim`, `mareforma export` — all CLI commands unchanged
- `claims.toml` backup — still auto-generated on every claim mutation
- `ontology.jsonld` export — extended with new fields, backward compatible
- Local-first, no network calls, SQLite, `open_db()` as the single entry point

---

## Upgrade steps

```bash
pip install --upgrade mareforma

# graph.db migrates automatically on next use — no manual step needed
mareforma build   # or: python -c "import mareforma; mareforma.open()"
```

If you read `confidence_float` directly anywhere in your code, rename to
`stated_confidence`.
