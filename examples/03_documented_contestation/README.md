# Documented Contestation

An agent finds an `ESTABLISHED` consensus in tension with its own results. It
does not discard its finding — it names the tension with `contradicts=` and
publishes a stronger, better-powered claim alongside the existing one. Both
coexist in the graph. `ESTABLISHED` means human-validated evidence, not settled
truth.

Each step below is the code from
[`03_documented_contestation.py`](03_documented_contestation.py) followed by the
console output it prints.

```bash
pip install langchain-core
python 03_documented_contestation.py
```

No API key required.

## Setup — establish the prior consensus

```python
# Two enrolled keys: one signs the claims, one validates them (mareforma
# refuses self-validation). get_tools() gives each agent its own tool set.
query_graph, assert_finding_a = [tool(fn) for fn in graph.get_tools(
    generated_by="agent_lab_a/model-a")]
_, assert_finding_b = [tool(fn) for fn in graph.get_tools(
    generated_by="agent_lab_b/model-b")]
_, assert_finding_c = [tool(fn) for fn in graph.get_tools(
    generated_by="agent_lab_c/model-c")]

# An ESTABLISHED upstream both lab agents converge on (seed=True).
upstream_ref = graph.assert_claim(
    "Prior literature: Treatment X is studied in population P",
    classification="DERIVED", generated_by="agent_seed/literature", seed=True,
)
consensus_a = assert_finding_a.invoke({
    "text": "Treatment X reduces outcome Y in population P (cohort_1, n=500, p=0.003)",
    "classification": "ANALYTICAL", "supports": [upstream_ref], "source": "dataset_alpha"})
consensus_b = assert_finding_b.invoke({
    "text": "Treatment X reduces outcome Y in population P (cohort_2, n=480, p=0.011)",
    "classification": "ANALYTICAL", "supports": [upstream_ref], "source": "dataset_beta"})

# Re-open under the reviewer key and validate → ESTABLISHED. evidence_seen names
# the upstream the reviewer consulted; mareforma binds it into the signed envelope.
graph.close()
with mareforma.open(tmp, key_path=reviewer_key_path) as reviewer_graph:
    reviewer_graph.validate(consensus_a, validated_by="reviewer@lab.org",
                            evidence_seen=[upstream_ref])
graph = mareforma.open(tmp, key_path=agent_key_path)
```

```
  consensus_a support_level    REPLICATED
  after validate()             ESTABLISHED
```

## New agent — larger analysis, different result

```python
# Step 1: query what is already established on this topic.
prior = json.loads(query_graph.invoke(
    {"topic": "Treatment X", "min_support": "ESTABLISHED"}))
established_ids = [c["claim_id"] for c in prior]

# Step 2: the new analysis returns no significant effect. The agent does not
# discard it — it asserts it with contradicts= pointing at the consensus, and
# documents the methodological difference (a larger, more diverse cohort).
challenge = assert_finding_c.invoke({
    "text": "Treatment X shows no significant effect on outcome Y in population P"
            " (cohort_3, n=1240, p=0.21) — larger and more diverse cohort than prior studies",
    "classification": "ANALYTICAL", "supports": ["upstream_ref_B"],
    "contradicts": established_ids, "source": "dataset_gamma",
})
```

```
  query_graph('Treatment X', min_support='ESTABLISHED') → 2 claims
    [ESTABLISHED ] <untrusted_data>
Treatment X reduces outcome Y in population P (c…
    [ESTABLISHED ] <untrusted_data>
Prior literature: Treatment X is studied in popu…

  Prior consensus found. Running analysis on new cohort (n=1,240)…

  challenge claim_id           9bfd667a…
  challenge support_level      PRELIMINARY
  challenge classification     ANALYTICAL
  contradicts                  2 established claim(s)
```

## Graph state — consensus and challenge coexist

```python
all_claims = graph.query()
level_order = {"ESTABLISHED": 0, "REPLICATED": 1, "PRELIMINARY": 2}
for c in sorted(all_claims, key=lambda x: level_order.get(x["support_level"], 3)):
    flag = " ← contradicts ESTABLISHED" if json.loads(c.get("contradicts_json") or "[]") else ""
    print(f"[{c['support_level']:12}] [{c['classification']:10}]  {c['text'][:50]}…{flag}")
```

```
  Total claims in graph: 4

  [ESTABLISHED ] [ANALYTICAL]  Treatment X reduces outcome Y in population P (coh…
  [ESTABLISHED ] [DERIVED   ]  Prior literature: Treatment X is studied in popula…
  [REPLICATED  ] [ANALYTICAL]  Treatment X reduces outcome Y in population P (coh…
  [PRELIMINARY ] [ANALYTICAL]  Treatment X shows no significant effect on outcome… ← contradicts ESTABLISHED
```

The ESTABLISHED finding is not overwritten; the challenge is not discarded. Both
sit in the graph with full provenance. A reviewer can then
`query_graph('Treatment X')` to see both sides, filter to
`min_support='ESTABLISHED'` for validated consensus only, or read
`get_claim(challenge)['contradicts_json']` to trace the stated tension.

## What NOT to do

The script closes with the anti-patterns spelled out:

- **Asserting the challenge without `contradicts=`.** The tension is invisible;
  the graph looks like two unrelated claims and a future agent gets
  contradictory signals with no structure to reason about them.
- **Discarding the finding because the consensus is `ESTABLISHED`.**
  `ESTABLISHED` is human-validated evidence, not settled truth. A larger,
  better-powered study is legitimate progress. Silence is not.
- **The correct pattern** names the tension (`contradicts=[established_id]`) and
  grounds the provenance (`supports=[new_upstream_ref]`, `source=...`).

## Using a real LLM

```python
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools=[query_graph, assert_finding])
```
