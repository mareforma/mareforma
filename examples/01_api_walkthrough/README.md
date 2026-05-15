# API Walkthrough

Complete EpistemicGraph API reference in a single runnable script.

## What it covers

1. **Open** — zero setup, no init required
2. **Assert** — INFERRED, ANALYTICAL, DERIVED classifications
3. **Query** — text filter, min_support, classification, limit
4. **Idempotency** — retry-safe writes and convergence convention
5. **REPLICATED** — automatic when two independent agents share upstream evidence
6. **ESTABLISHED** — human validation only, requires REPLICATED first
7. **Anti-patterns** — what breaks the epistemic model silently

## Run

```bash
python 01_api_walkthrough.py
```

No external dependencies. Uses a temporary directory — safe to run anywhere.
