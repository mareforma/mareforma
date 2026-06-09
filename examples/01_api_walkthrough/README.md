# API Walkthrough

Complete EpistemicGraph API reference in a single runnable script.

## What it covers

1. **Open**: zero setup, no init required
2. **Assert**: INFERRED, ANALYTICAL, DERIVED classifications
3. **Query**: text filter, min_support, classification, limit
4. **Idempotency**: retry-safe writes and convergence convention
5. **REPLICATED**: automatic when two independent agents share upstream evidence
6. **ESTABLISHED**: human validation only, requires REPLICATED first
7. **Anti-patterns**: what breaks the epistemic model silently

## Run

```bash
python 01_api_walkthrough.py
```

No external dependencies. Uses a temporary directory, safe to run anywhere.

## What you'll see

The script walks each surface and prints the result. The parts that matter:

```
2. Assert claims
  INFERRED id            7b4679e2…
  ANALYTICAL id          e53a6263…
  DERIVED id             4988b142…

3. Query
  text='cell type A'     3 claims
  min_support=REPLICATED 0 claims  ← expected 0
  get_claim support_level PRELIMINARY

4. Idempotency
  first call             800daeda…
  second call            800daeda…
  same id?               True              ← retry-safe, same id

5. REPLICATED (automatic)
  lab_a support_level    REPLICATED
  lab_b support_level    REPLICATED        ← two agents, shared upstream

6. ESTABLISHED (human only)
  validate(PRELIMINARY)  ValueError: Only REPLICATED claims can be promoted
  support_level          ESTABLISHED       ← after a valid validate()
  validated_by           jane@lab.org
```

The two guardrails to notice: idempotent writes converge on the same id
(step 4), and `validate()` refuses to promote anything below `REPLICATED`
(step 6). Step 8 then shows the inverse: a null-data claim is forced to
`INFERRED`, never `ANALYTICAL`.
