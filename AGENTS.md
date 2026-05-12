# Mareforma — agent integration guide

Mareforma is the epistemic layer AI scientists run on. It gives agents a
local graph for asserting claims with provenance and querying what has
already been established before making new assertions.

Trust in a claim is derived from the graph, not from the agent that made it.
No confidence score. No self-reporting. The structure of the provenance graph
is the only trust signal.

## Install

```bash
uv add mareforma
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

## API reference

### `mareforma.open(path=None) → EpistemicGraph`

Open the epistemic graph and return an `EpistemicGraph`. Use as a context
manager to ensure the connection is closed.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str \| Path \| None` | `None` | Project root. Defaults to `cwd()`. Graph stored at `<path>/.mareforma/graph.db`. |

```python
graph = mareforma.open()                  # cwd
graph = mareforma.open("/path/to/project")
with mareforma.open() as graph: ...       # auto-closes
```

---

### `graph.assert_claim(text, *, ...) → str`

Assert a claim into the graph. Returns `claim_id` (UUID string).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `text` | `str` | required | Falsifiable assertion. Cannot be empty. |
| `classification` | `str` | `"INFERRED"` | Epistemic origin: `INFERRED` \| `ANALYTICAL` \| `DERIVED` |
| `generated_by` | `str \| None` | `"agent"` | Agent identifier. Use `model/version/context` format. |
| `supports` | `list[str] \| None` | `None` | Upstream claim_ids or reference strings. |
| `contradicts` | `list[str] \| None` | `None` | Claim_ids this finding is in explicit tension with. |
| `source_name` | `str \| None` | `None` | Data source name. Required for ANALYTICAL to be meaningful. |
| `idempotency_key` | `str \| None` | `None` | Retry-safe key. Same key → same claim_id, no INSERT. |

**Raises:** `ValueError` if `classification` is invalid or `text` is empty.

**Side effect:** if ≥2 claims now share the same upstream in `supports[]`
with different `generated_by`, both are promoted to `REPLICATED` automatically.

---

### `graph.query(text=None, *, ...) → list[dict]`

Query claims from the graph. Returns a list of claim dicts ordered by
support level (descending) then recency (descending).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `text` | `str \| None` | `None` | Substring filter on claim text (case-insensitive). |
| `min_support` | `str \| None` | `None` | Minimum support level: `PRELIMINARY` \| `REPLICATED` \| `ESTABLISHED` |
| `classification` | `str \| None` | `None` | Filter by classification. |
| `limit` | `int` | `20` | Maximum results. |

Each dict contains: `claim_id`, `text`, `classification`, `support_level`,
`idempotency_key`, `validated_by`, `validated_at`, `status`, `source_name`,
`generated_by`, `supports_json`, `contradicts_json`, `comparison_summary`,
`created_at`, `updated_at`.

**Raises:** `ValueError` if `min_support` or `classification` is invalid.

---

### `graph.get_claim(claim_id) → dict | None`

Return a single claim dict by ID, or `None` if not found.

---

### `graph.validate(claim_id, *, validated_by=None) → None`

Promote a `REPLICATED` claim to `ESTABLISHED`. Human-only gate.

**Raises:** `ClaimNotFoundError` if the claim does not exist.
**Raises:** `ValueError` if `support_level` is not `REPLICATED`.

---

### `mareforma.schema() → dict`

Return the full epistemic schema — valid values, defaults, and state
transitions. Call this before making any assertions to inspect the system.

```python
s = mareforma.schema()
s["classifications"]   # ['INFERRED', 'ANALYTICAL', 'DERIVED']
s["support_levels"]    # ['PRELIMINARY', 'REPLICATED', 'ESTABLISHED']
s["statuses"]          # ['open', 'contested', 'retracted']
s["transitions"]       # [{from: PRELIMINARY, to: REPLICATED, trigger: automatic}, ...]
s["schema_version"]    # 1
```

---

## Origin (`classification`)

The `classification` field encodes a claim's origin — how knowledge was derived.
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

## Claim status

Status is an editorial signal, separate from support level.

| Value | Meaning |
|---|---|
| `open` | Active claim — default |
| `contested` | Under active dispute |
| `retracted` | Withdrawn by the asserting agent or a reviewer |

```python
graph.assert_claim("...", status="open")      # default
graph.assert_claim("...", status="contested") # flagging dispute at assertion time
```

Status is mutable via `mareforma claim update` (CLI) or directly via the
database. It does not affect `support_level`.

---

## Contradiction pattern

When a new finding is in tension with an existing claim, assert with
`contradicts=` pointing to the existing claim. Both coexist in the graph
with an explicit link — neither is overwritten.

```python
# Find what is established on this topic
prior = graph.query("Treatment X", min_support="ESTABLISHED")

# New analysis gets a different result — document the tension
graph.assert_claim(
    "Treatment X shows no effect (n=1240, p=0.21)",
    classification="ANALYTICAL",
    contradicts=[c["claim_id"] for c in prior],
    supports=["upstream_ref_B"],
)
```

Science advances by documented contestation, not by one side disappearing.

---

## Query patterns

```python
# All claims about a topic
graph.query("topic X")

# Only independently replicated findings
graph.query("topic X", min_support="REPLICATED")

# Only human-validated findings
graph.query(min_support="ESTABLISHED")

# Filter genuine replication from spurious (both ANALYTICAL + source present)
results = graph.query("topic X", min_support="REPLICATED")
trustworthy = [
    r for r in results
    if r["classification"] == "ANALYTICAL" and r.get("source_name")
]

# Claims this finding contradicts
import json
claim = graph.get_claim(claim_id)
contradicts = json.loads(claim["contradicts_json"])

# Claims this finding rests on
supports = json.loads(claim["supports_json"])
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
random run ID. Two agents using the same key converge on the same `claim_id`
even with different text phrasing, without needing explicit `supports=` links:

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
share an upstream. Always check `classification` alongside `support_level`.

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

## Framework integrations

`graph.get_tools(generated_by="...")` returns `[query_graph, assert_finding]` as
plain Python callables. Wrap them in one line for any agent framework.
`generated_by` is baked into the closure — set it to the agent's identity so
REPLICATED detection works correctly across independent runs.

```python
tools = graph.get_tools(generated_by="agent/model-a/lab_a")
# tools[0] = query_graph(topic, min_support) -> str (JSON)
# tools[1] = assert_finding(text, classification, supports, contradicts, source) -> str
```

### Layer 1 — LLM providers

| Framework | Wrapping |
|---|---|
| **Anthropic SDK** | See full example below |
| **OpenAI SDK** | `tools = [openai_tool(fn) for fn in graph.get_tools(generated_by="...")]` |

**Anthropic SDK (full example):**

```python
import anthropic, json
import mareforma

client = anthropic.Anthropic()

with mareforma.open() as graph:
    query_graph, assert_finding = graph.get_tools(generated_by="agent/claude/lab_a")

    # Build Anthropic tool schemas from function signatures
    tools = [
        {
            "name": "query_graph",
            "description": query_graph.__doc__,
            "input_schema": {
                "type": "object",
                "properties": {
                    "topic": {"type": "string"},
                    "min_support": {"type": "string", "enum": ["PRELIMINARY", "REPLICATED", "ESTABLISHED"]},
                },
                "required": ["topic"],
            },
        },
        {
            "name": "assert_finding",
            "description": assert_finding.__doc__,
            "input_schema": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "classification": {"type": "string", "enum": ["INFERRED", "ANALYTICAL", "DERIVED"]},
                    "supports": {"type": "array", "items": {"type": "string"}},
                    "contradicts": {"type": "array", "items": {"type": "string"}},
                    "source": {"type": "string"},
                },
                "required": ["text"],
            },
        },
    ]

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        tools=tools,
        messages=[{"role": "user", "content": "Query for existing findings on target T and assert a new finding."}],
    )

    # Dispatch tool calls
    for block in response.content:
        if block.type == "tool_use":
            fn = query_graph if block.name == "query_graph" else assert_finding
            result = fn(**block.input)
```

### Layer 2 — Orchestration frameworks

| Framework | Wrapping |
|---|---|
| **LangChain** | `from langchain_core.tools import tool`<br>`lc_tools = [tool(fn) for fn in graph.get_tools(generated_by="...")]` |
| **LangGraph** | `from langchain_core.tools import tool`<br>`tools = [tool(fn) for fn in graph.get_tools(generated_by="...")]`<br>`agent = create_react_agent(llm, tools)` |
| **CrewAI** | `from crewai.tools import StructuredTool`<br>`tools = [StructuredTool.from_function(fn) for fn in graph.get_tools(generated_by="...")]` |
| **AutoGen** | `tools = graph.get_tools(generated_by="...")`<br>`agent = ConversableAgent(...)`<br>`for fn in tools: register_function(fn, caller=agent, executor=agent, ...)` |
| **LlamaIndex** | `from llama_index.core.tools import FunctionTool`<br>`tools = [FunctionTool.from_defaults(fn) for fn in graph.get_tools(generated_by="...")]` |
| **PydanticAI** | `tools = graph.get_tools(generated_by="...")`<br>`for fn in tools: agent.tool(fn)` |
| **Smol Agents** | `from smolagents import Tool`<br>`tools = [Tool.from_function(fn) for fn in graph.get_tools(generated_by="...")]` |

### Layer 3 — Observability (no integration needed)

Tracing tools (LangSmith, Langfuse, W&B) record execution traces — what the agent
did, which tools were called, how long it took. Mareforma records epistemic state —
what was found, how it was derived, how much independent evidence backs it.
Use both. They are parallel, not overlapping. No integration code needed.

### Layer 4 — Data pipelines (convention)

For DVC, MLflow, Prefect, and similar pipeline tools, link claims to pipeline
stages via `source_name`:

```python
# After a DVC stage runs:
graph.assert_claim(
    "Target T elevated in condition C (n=620)",
    classification="ANALYTICAL",
    source_name="dvc:stages/analyse_targets",  # DVC stage name
)

# After an MLflow run:
graph.assert_claim(
    "Model M achieves AUC 0.87 on held-out set",
    classification="ANALYTICAL",
    source_name=f"mlflow:run/{mlflow.active_run().info.run_id}",
)
```

The `source_name` field is a string — any convention that links the claim to
its data provenance works. The graph does not validate it.

---

## For more

- [Quickstart](docs/introduction/quickstart.mdx)
- [Why Mareforma](docs/introduction/why-mareforma.mdx)
- [Examples](examples/)
- Full API reference: https://docs.mareforma.com
