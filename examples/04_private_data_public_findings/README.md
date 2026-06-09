# Private Data, Public Findings

Two autonomous labs share a Mareforma epistemic graph but never share raw data.
Provenance travels. Proprietary data does not.

## Story

Lab A discovers a candidate finding and publishes its provenance trace to the
shared graph: which sources were queried, which steps were executed, which
upstream evidence was cited. The raw data stays at Lab A.

Lab B reads the trace, not the data, reconstructs the experimental logic,
and runs an independent replication on its own private dataset. Both traces
are published back to the shared graph.

The graph then answers three questions automatically:

1. **Independent paths?** Did both labs use different data sources, or did
   they both rely on the same LLM prior with no real data behind either finding?

2. **Genuinely reproducible?** Is the finding stable across independent
   datasets, or an artifact of a specific data partition?

3. **Provenance distance?** How far is each conclusion from its raw data?
   How much reasoning is shared vs. independent?

The contrast section shows spurious REPLICATED from INFERRED claims, two
agents repeating the same LLM prior, and how to detect it from the graph.

## What you'll see

Lab B reads the trace but not the data, replicates, and the graph answers all
three questions:

```
Lab B — reads trace, runs independent replication
  query_graph('Target T') → 3 claims from Lab A
  Source:     private_dataset_A  ← Lab B cannot access this
  Class:      ANALYTICAL

Q1 — Independent data paths?
  distinct source_names   ['private_dataset_A', 'private_dataset_B']
  distinct generated_by   ['agent_seed/literature', 'lab_a/model-a', 'lab_b/model-b']
  ✓ Two independent data sources, two independent agents.

Q2 — Genuinely reproducible?
  Candidate target T shows elevated activity in… REPLICATED
  ✓ REPLICATED — independent agents, shared upstream, independent data paths.

Q3 — Provenance distance?
  Lab A's chain:  upstream_ref_A → ANALYTICAL (step_1) → ANALYTICAL (step_2)
  Lab B's chain:  upstream_ref_A → ANALYTICAL (rep_1)  → ANALYTICAL (rep_2)
```

Then the contrast that makes the example worth reading, spurious replication:

```
Contrast — spurious replication (what to watch for)
  spurious_a support_level    REPLICATED
  spurious_a classification   INFERRED
  REPLICATED fired — but classification=INFERRED and source_name=''.
  Two agents repeated the same LLM prior. No data behind either finding.
```

`REPLICATED` alone is not trust. The graph lets you filter it out:
`query('Target T', min_support='REPLICATED')` then keep only
`classification='ANALYTICAL'` with a non-empty `source_name`.

## Run

```bash
pip install langchain-core
python 04_private_data_public_findings.py
```

No API key required. To use a real LLM:

```python
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools=[
    query_graph, get_provenance_trace, assert_finding
])
```
