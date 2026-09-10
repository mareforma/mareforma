# Documented Contestation

An agent finds a reviewed consensus in tension with its own results. It does
not discard its finding. It names the tension with `contradicts=` and publishes
a stronger, better-powered claim alongside the existing one. Both coexist in the
graph. A signed validation means a human read the evidence, not that the
question is settled.

Nothing on a claim ranks it. Trust reads off the derived axes
`graph.proposition_status(prop)` returns; the contestation this example
documents is what turns the derived `question_status` for the frame to
`divided`.

Each step below is the code from
[`03_documented_contestation.py`](03_documented_contestation.py) followed by the
console output it prints.

```bash
pip install langchain-core
python 03_documented_contestation.py
```

No API key required.

## Setup: establish the prior consensus

Independence is read off the signing key (`asserter_keyid`), so the two
converging claims must sign with distinct keys. `generated_by` is a display
label and carries no weight. The langchain tools sign with the single loaded key, so the
two consensus claims go through `graph.assert_claim(signer=...)` under two
distinct lab keys instead. The reviewer/validator key is distinct from both
(mareforma refuses self-validation).

```python
# query_graph and the challenge agent's tool come from get_tools(). The two
# converging consensus claims do not: they sign with distinct keys directly.
lab_a_priv = _signing.load_private_key(lab_a_key_path)
lab_b_priv = _signing.load_private_key(lab_b_key_path)
query_graph, _ = [tool(fn) for fn in graph.get_tools(
    generated_by="agent_lab_a/model-a")]
_, assert_finding_c = [tool(fn) for fn in graph.get_tools(
    generated_by="agent_lab_c/model-c")]

# The shared upstream both lab agents converge on.
upstream_ref = graph.assert_claim(
    "Prior literature: Treatment X is studied in population P",
    classification="DERIVED", generated_by="agent_seed/literature",
)
# Same upstream, distinct signing keys: that is what convergence looks like.
consensus_a = graph.assert_claim(
    "Treatment X reduces outcome Y in population P (cohort_1, n=500, p=0.003)",
    classification="ANALYTICAL", generated_by="agent_lab_a/model-a",
    supports=[upstream_ref], source_name="dataset_alpha", signer=lab_a_priv)
consensus_b = graph.assert_claim(
    "Treatment X reduces outcome Y in population P (cohort_2, n=480, p=0.011)",
    classification="ANALYTICAL", generated_by="agent_lab_b/model-b",
    supports=[upstream_ref], source_name="dataset_beta", signer=lab_b_priv)

# Re-open under the reviewer key and validate. evidence_seen names the upstream
# the reviewer consulted; mareforma binds it into the signed envelope.
graph.close()
with mareforma.open(tmp, key_path=reviewer_key_path) as reviewer_graph:
    reviewer_graph.validate(consensus_a, validated_by="reviewer@lab.org",
                            evidence_seen=[upstream_ref])
graph = mareforma.open(tmp, key_path=agent_key_path)
```

```
  consensus_a validated        False
  after validate()             reviewer@lab.org
```

## New agent: larger analysis, different result

```python
# Step 1: query what is already on record for this topic. The LLM-facing view
# carries no signature material, so the validation state is read from the graph
# by id rather than from a field that arrived inside a prompt.
prior = json.loads(query_graph.invoke(
    {"topic": "Treatment X"}))
signed_off = {
    c["claim_id"] for c in prior
    if (row := graph.get_claim(c["claim_id"])) and row.get("validation_signature")
}
prior_ids = [c["claim_id"] for c in prior]

# Step 2: the new analysis returns no significant effect. The agent does not
# discard it. It asserts it with contradicts= pointing at the consensus, and
# documents the methodological difference (a larger, more diverse cohort).
challenge = assert_finding_c.invoke({
    "text": "Treatment X shows no significant effect on outcome Y in population P"
            " (cohort_3, n=1240, p=0.21): larger and more diverse cohort than prior studies",
    "classification": "ANALYTICAL", "supports": ["upstream_ref_B"],
    "contradicts": prior_ids, "source": "dataset_gamma",
})
```

```
  query_graph('Treatment X') → 3 claims, 1 carrying a signed validation
    [unvalidated ] Treatment X reduces outcome Y in population P (cohort_2, n=480, p…
    [validated   ] Treatment X reduces outcome Y in population P (cohort_1, n=500, p…
    [unvalidated ] Prior literature: Treatment X is studied in population P…

  Prior consensus found. Running analysis on new cohort (n=1,240)…

  challenge claim_id           9bfd667a…
  challenge validated          False
  challenge classification     ANALYTICAL
  contradicts                  3 prior claim(s)
```

## Graph state: consensus and challenge coexist

```python
all_claims = graph.query()
for c in sorted(all_claims, key=lambda x: x["created_at"]):
    flag = " ← contradicts a prior claim" if json.loads(c.get("contradicts_json") or "[]") else ""
    mark = "validated" if c.get("validation_signature") else "unvalidated"
    print(f"[{mark:12}] [{c['classification']:10}]  {c['text'][:50]}…{flag}")
```

```
  Total claims in graph: 4

  [unvalidated ] [DERIVED   ]  Prior literature: Treatment X is studied in popula…
  [validated   ] [ANALYTICAL]  Treatment X reduces outcome Y in population P (coh…
  [unvalidated ] [ANALYTICAL]  Treatment X reduces outcome Y in population P (coh…
  [unvalidated ] [ANALYTICAL]  Treatment X shows no significant effect on outcome… ← contradicts a prior claim
```

The validated finding is not overwritten; the challenge is not discarded. Both
sit in the graph with full provenance. A reviewer can then
`query_graph('Treatment X')` to see both sides, keep the rows carrying a signed
validation for reviewed consensus only, or read
`get_claim(challenge)['contradicts_json']` to trace the stated tension.

## What NOT to do

The script closes with the anti-patterns spelled out:

- **Asserting the challenge without `contradicts=`.** The tension is invisible;
  the graph looks like two unrelated claims and a future agent gets
  contradictory signals with no structure to reason about them.
- **Discarding the finding because a human already signed off on the consensus.**
  A signed validation means a reviewer read the evidence, not that the question
  is settled. A larger, better-powered study is legitimate progress. Silence is
  not.
- **The correct pattern** names the tension (`contradicts=[prior_claim_id]`) and
  grounds the provenance (`supports=[new_upstream_ref]`, `source=...`).

## Using a real LLM

```python
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools=[query_graph, record_claim])
```
