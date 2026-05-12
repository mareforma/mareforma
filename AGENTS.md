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

### `mareforma.open(path=None, *, ...) → EpistemicGraph`

Open the epistemic graph and return an `EpistemicGraph`. Use as a context
manager to ensure the connection is closed.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str \| Path \| None` | `None` | Project root. Defaults to `cwd()`. Graph stored at `<path>/.mareforma/graph.db`. |
| `key_path` | `str \| Path \| None` | `None` | Ed25519 private key (PEM). `None` → use the XDG default `~/.config/mareforma/key`. If the path does not exist, the graph operates unsigned. |
| `require_signed` | `bool` | `False` | Raise `KeyNotFoundError` if no key is found at `key_path`. |
| `rekor_url` | `str \| None` | `None` | Sigstore-Rekor transparency log endpoint. When set, every signed claim is submitted at INSERT time. `None` disables Rekor entirely. Use `mareforma.signing.PUBLIC_REKOR_URL` for the public instance. |
| `require_rekor` | `bool` | `False` | Raise `SigningError` if `rekor_url` is unset or initial submission fails. |
| `trust_insecure_rekor` | `bool` | `False` | Skip SSRF validation on `rekor_url` (only for private Rekor instances on internal networks). |

```python
graph = mareforma.open()                                # cwd, unsigned if no key
graph = mareforma.open(require_signed=True)             # fail-fast if no key
graph = mareforma.open(rekor_url=mareforma.signing.PUBLIC_REKOR_URL)  # public transparency log
with mareforma.open() as graph: ...                     # auto-closes
```

First-time setup: run `mareforma bootstrap` once to generate an Ed25519
keypair at `~/.config/mareforma/key`. After that, every `assert_claim`
auto-signs.

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
`branch_id`, `unresolved`, `signature_bundle`, `transparency_logged`,
`created_at`, `updated_at`.

**Raises:** `ValueError` if `min_support` or `classification` is invalid.

---

### `graph.get_claim(claim_id) → dict | None`

Return a single claim dict by ID, or `None` if not found.

---

### `graph.validate(claim_id, *, validated_by=None) → None`

Promote a `REPLICATED` claim to `ESTABLISHED`. Identity-gated.

The graph must have a loaded signer (from `mareforma bootstrap` or
`mareforma.open(key_path=...)`) AND that key must be enrolled in the
project's `validators` table. The first key opened against a fresh
graph auto-enrolls as the root validator. The validation event itself
is signed: a DSSE-style envelope binding `(claim_id, validator_keyid,
validated_at)` is persisted to the row's `validation_signature`
column, so the promotion is independently verifiable.

`validated_by` is a cosmetic display label. The authenticated identity
is the keyid embedded in the signed envelope; consumers that care about
who validated must check `validation_signature` against the validators
table, not the `validated_by` string.

**Raises:** `ClaimNotFoundError` if the claim does not exist.
**Raises:** `ValueError` if `support_level` is not `REPLICATED`, no
signer is loaded, or the loaded signer is not an enrolled validator.

---

### `graph.refresh_unresolved() → dict`

Retry external DOI verification for every claim currently flagged
`unresolved=1`. Returns `{"checked": N, "resolved": M, "still_unresolved": K}`.

DOIs in `supports[]`/`contradicts[]` are HEAD-checked against Crossref and
DataCite at `assert_claim` time. If the registries are unreachable, the
claim is persisted with `unresolved=True` and is ineligible for
`REPLICATED` promotion until the next `refresh_unresolved()` confirms the
DOIs.

---

### `graph.refresh_unsigned() → dict`

Retry transparency-log submission for every signed-but-unlogged claim
when the graph was opened with `rekor_url=...`. Returns
`{"checked": N, "logged": M, "still_unlogged": K}`. No-op when `rekor_url`
is unset.

Each retry compares the envelope's signed payload against the live row
before re-submitting — a tampered row is quarantined rather than
cementing a stale signature in the public log. An envelope whose keyid
no longer matches the current signer (key was rotated since
`assert_claim`) is skipped with a warning.

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

**Artifact-hash gate.** When two converging peers BOTH supply
`artifact_hash` (a SHA256 hex digest of the output bytes — figure, CSV,
model), the hashes must match for `REPLICATED` to fire. Identity
convergence alone is no longer enough in that case. When either peer
omits the hash, the gate is bypassed and identity-only `REPLICATED`
applies as before; the signal is opt-in, not retroactive. The hash is
part of the signed payload, so an attacker who edits the column without
the private key breaks verification.

```python
import hashlib
result_bytes = open("figure_3.png", "rb").read()
digest = hashlib.sha256(result_bytes).hexdigest()
graph.assert_claim(
    "Treatment X reduces response by 18% (95% CI 12-24)",
    classification="ANALYTICAL",
    supports=[upstream_id],
    artifact_hash=digest,
)
```

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

## Signing and transparency log

Mareforma can attach a verifiable cryptographic signature to every claim
and (optionally) log it to a public transparency log. Both are opt-in
features — agents that don't need them keep the default behavior.

**Local signing.** Run `mareforma bootstrap` once to generate an Ed25519
keypair at `~/.config/mareforma/key` (mode 0600). After that, every
`assert_claim` auto-signs and persists the signature envelope to the
`signature_bundle` field on the claim. The signed payload binds
`claim_id`, `text`, `classification`, `generated_by`, `supports`,
`contradicts`, `source_name`, `artifact_hash`, and `created_at` — any
tamper on the row breaks verification.

**Append-only invariant.** Signed claims refuse mutation of any
signed-surface field. `update_claim(text=...)` /
`update_claim(supports=...)` / `update_claim(contradicts=...)` on a
signed row raise `SignedClaimImmutableError`. `status` and
`comparison_summary` remain editable since neither is part of the signed
payload. To revise a signed claim, retract it (`status='retracted'`) and
assert a new one citing the old via `contradicts=[<old_claim_id>]`.

**Transparency log (Rekor).** Pass `rekor_url=mareforma.signing.PUBLIC_REKOR_URL`
to `mareforma.open()` and every signed claim is submitted to the public
Sigstore Rekor instance at INSERT time. The entry uuid + logIndex are
attached to the bundle and `transparency_logged` flips to 1. If Rekor is
unreachable, the claim persists with `transparency_logged=0` and is held
out of `REPLICATED` promotion until `graph.refresh_unsigned()` completes
the submission.

```python
# Prerequisite: run `mareforma bootstrap` once to create ~/.config/mareforma/key.
# Without a key, mareforma.open() falls through to unsigned mode and no Rekor
# submission is attempted, regardless of rekor_url. require_signed=True fails
# fast with KeyNotFoundError if the bootstrap was missed.

import mareforma
from mareforma.signing import PUBLIC_REKOR_URL

with mareforma.open(rekor_url=PUBLIC_REKOR_URL, require_signed=True) as graph:
    claim_id = graph.assert_claim("...", classification="ANALYTICAL")
    # claim is signed + logged to Rekor before this line returns

# Later, after a network outage:
with mareforma.open(rekor_url=PUBLIC_REKOR_URL, require_signed=True) as graph:
    result = graph.refresh_unsigned()
    # {"checked": N, "logged": M, "still_unlogged": K}
```

**Key rotation is destructive.** `mareforma bootstrap --overwrite`
strands every claim signed by the prior key — verification breaks AND
any claim not yet submitted to Rekor becomes permanently un-loggable.
Safe rotation: back up the old key, run `refresh_unsigned()` to drain
the pending queue, then rotate.

---

## Validators (who can promote ESTABLISHED)

`graph.validate()` is the only path to `ESTABLISHED` and is identity-
gated. Only keys enrolled in the project's per-graph `validators` table
can validate. Mareforma is local-trust: the table is just the set of
public keys the project's operator has chosen to trust, not a cross-org
PKI.

**Root of trust.** The first key opened against a fresh `graph.db`
auto-enrolls as the root with a self-signed enrollment envelope. This
is silent and zero-ceremony: run `mareforma bootstrap` once, open the
project, and you are the root.

**Adding more validators.** From the project root, with an already-
enrolled key loaded:

```bash
mareforma validator add --pubkey ./alice.pub.pem --identity alice@lab.example
mareforma validator list
```

Or programmatically:

```python
with mareforma.open() as graph:
    alice_pem = open("./alice.pub.pem", "rb").read()
    graph.enroll_validator(alice_pem, identity="alice@lab.example")
    for row in graph.list_validators():
        print(row["identity"], row["keyid"])
```

Each enrollment is signed by the parent validator (root for the first
additions, then any already-enrolled key thereafter). On read,
`graph.validate()` walks the chain back to a self-signed root and
verifies every link's enrollment envelope against the parent's pubkey
before accepting the validator — a row planted via direct sqlite
INSERT with a fabricated parent does not pass.

**Local-trust scope.** The chain anchors at a self-signed row inside
the project's own `graph.db`. A verifier who trusts that file's
integrity can verify; a verifier who suspects the file is tampered
has no external anchor in v0.3.0 (no cross-org PKI, no notary
endorsement). Mareforma is a local epistemic graph; this section
gates *who can validate within the project*, not who can vouch for
the project to the outside world.

**Removal is not supported in v0.3.0.** Validators are append-only,
mirroring claim history. If a key is compromised, rotate the bootstrap
key and re-bless validators under a fresh root.

**Auto-enrollment is irrevocable.** The first key opened against a
fresh graph silently becomes the immutable root. A `UserWarning`
fires on that first enrollment so an operator who opened the project
with the wrong key has a chance to notice — verify the warning's
keyid prefix against the one you intended before any further
`validate()` calls.

---

## DOI verification

DOIs anywhere in `supports[]` or `contradicts[]` are HEAD-checked against
Crossref then DataCite at `assert_claim` time. Failure persists the claim
with `unresolved=True` and blocks `REPLICATED` promotion until
`graph.refresh_unresolved()` confirms the DOIs. Strings in `supports[]`
that don't match the DOI format (`10.<registrant>/<suffix>`) are treated
as claim_id references and pass through without a network call.

Results are cached in the `doi_cache` table (30-day TTL for resolved
entries, 24-hour TTL for unresolved) so repeated assertions of the same
DOI don't hit the registries.

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

**Hash conflicts raise.** A replay that supplies a different `artifact_hash`
than the original is not a retry — it is a different claim that happens to
share a key. `assert_claim` raises `IdempotencyConflictError` rather than
silently dropping the new hash, so a caller cannot believe their new hash
was registered when it was not. Use a different `idempotency_key` or
re-assert without the conflicting field.

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
