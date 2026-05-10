# Mental model

## What is a claim?

A claim is a falsifiable assertion recorded with its provenance. Not a log
entry, not a note, not a trace event. A claim has:

- **Text** — what was asserted
- **Classification** — how the knowledge was derived (INFERRED, ANALYTICAL, DERIVED)
- **Support level** — how much independent evidence backs it (PRELIMINARY, REPLICATED, ESTABLISHED)
- **Provenance** — who asserted it, from which data source, building on which upstream claims

Claims are append-oriented. Once recorded, a claim is not overwritten —
it is either built upon, contradicted, or retracted. The history is preserved.

## What is the graph?

The graph is the set of all claims and the relationships between them:
`supports[]` links (what this claim rests on) and `contradicts[]` links
(what this claim is in tension with).

```
upstream_ref ──► ANALYTICAL claim (lab_a)  ──┐
                                              ├──► REPLICATED
upstream_ref ──► ANALYTICAL claim (lab_b)  ──┘
                    │
                    └──► DERIVED synthesis ──► (human) ──► ESTABLISHED
```

Trust is derived from this structure. The more independent paths converge
on the same upstream evidence, the higher the support level. A single agent
asserting the same claim a thousand times is still PRELIMINARY — independence
of provenance paths is what matters.

## What is trust?

Trust in a claim is a property of its position in the graph, not a property
of the agent that made it.

**PRELIMINARY** — one agent claimed it. Could be correct. No independent
confirmation yet.

**REPLICATED** — ≥2 agents with different `generated_by` values share the
same upstream in `supports[]`. Set automatically at INSERT. This is the
minimum bar for treating a finding as non-anecdotal.

**ESTABLISHED** — a human reviewed the provenance chain and validated it.
Only reachable via `graph.validate()`. No agent can self-promote. This is
the gate for consequential actions.

There is no confidence score. An agent that is wrong is still confident.
The graph does not ask the agent how sure it is — it tracks what the agent
can demonstrate.

## What is classification?

Classification is orthogonal to trust. It records how knowledge was derived —
the epistemic origin — not how much it should be trusted.

**INFERRED** — LLM reasoning, synthesis, extrapolation. The default. Correct
to use even for sophisticated reasoning, as long as it is not grounded in
data that actually ran.

**ANALYTICAL** — deterministic analysis ran against source data and returned
output. Only use this when a real data pipeline ran and produced real output.
If the pipeline failed silently and the agent fell back to LLM knowledge,
the classification is still INFERRED — asserting ANALYTICAL on null data is
an epistemic lie that the graph will permanently record.

**DERIVED** — explicitly built on ESTABLISHED or REPLICATED claims already
in the graph. The `supports[]` field must point to those claims. A DERIVED
claim with empty `supports[]` is unverifiable.

## What is epistemic distance?

Epistemic distance measures how far a conclusion is from its raw data. A
short chain of ANALYTICAL steps close to raw data is more trustworthy than
a long chain of INFERRED steps, even if each step looks locally valid.

The `@transform` pipeline layer tracks this:

```
raw data → ANALYTICAL → ANALYTICAL → INFERRED → conclusion
   0.0        0.1          0.5          1.0         1.0  (max, pessimistic)
```

Distance accumulates pessimistically: one INFERRED step in a chain means
the full chain carries INFERRED-level epistemic fragility.

## What is idempotency?

Two distinct problems:

**Retry safety** — `idempotency_key` ensures that if an agent run is
interrupted and retried, the same claim is not duplicated. Same key → same
`claim_id` returned, no INSERT.

**Convergence convention** — agents running the same conceptual query should
use a structured key encoding the semantic content (`"target_T_condition_C"`)
rather than a random run ID. This allows independent agents to converge on
the same claim without needing identical text.

## What claims.toml is

`claims.toml` is a human-readable backup of the full graph state, written
after every mutation. It is not the source of truth — `graph.db` is. But
it is the safety net: if `graph.db` is deleted or corrupted, `claims.toml`
preserves the record in a format a human can read and reason about.
