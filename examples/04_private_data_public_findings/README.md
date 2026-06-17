# Private Data, Public Findings

Two autonomous labs share a Mareforma epistemic graph but never share raw data.
Provenance travels; proprietary data does not. Lab A publishes a provenance
trace, Lab B reads the trace (not the data), replicates on its own private
dataset, and the graph answers three replication questions automatically.

Each step below is the code from
[`04_private_data_public_findings.py`](04_private_data_public_findings.py)
followed by the console output it prints.

```bash
pip install langchain-core
python 04_private_data_public_findings.py
```

No API key required.

## Setup: a shared graph, plus a provenance-trace tool

```python
query_graph, assert_finding_a = [tool(fn) for fn in graph.get_tools(
    generated_by="lab_a/model-a")]
_, assert_finding_b = [tool(fn) for fn in graph.get_tools(
    generated_by="lab_b/model-b")]

@tool
def get_provenance_trace(claim_id: str) -> dict:
    """Return a claim's full lineage — sources, upstream, classification,
    support level. This is what Lab B reads: the trace, not the raw data."""
    claim = graph.get_claim(claim_id)
    return {} if claim is None else {
        "claim_id": claim["claim_id"], "text": claim["text"],
        "classification": claim["classification"], "support_level": claim["support_level"],
        "source_name": claim.get("source_name"), "generated_by": claim.get("generated_by"),
        "supports": json.loads(claim.get("supports_json", "[]") or "[]"),
        "contradicts": json.loads(claim.get("contradicts_json", "[]") or "[]"),
    }
```

## Lab A: discovery and trace publication

```python
# An ESTABLISHED upstream both labs cite (seed=True). Lab A then runs a
# multi-step analysis on its private dataset; each step is a claim with
# provenance. The raw data never leaves Lab A — only source NAMES travel.
upstream_ref = graph.assert_claim(
    "Prior literature on Target T in condition C",
    classification="DERIVED", generated_by="agent_seed/literature", seed=True)

step_1 = assert_finding_a.invoke({
    "text": "Candidate target T shows elevated activity in condition C"
            " (partition_1, n=620, fold-change=2.3)",
    "classification": "ANALYTICAL", "supports": [upstream_ref],
    "source": "private_dataset_A"})           # name only — data stays at Lab A
step_2 = assert_finding_a.invoke({
    "text": "Target T activity in condition C is specific to cell subtype S"
            " (partition_1, pathway analysis, p=0.004)",
    "classification": "ANALYTICAL", "supports": [step_1],  # builds on the previous step
    "source": "private_dataset_A"})
```

```
  Lab A published 2 claims to the shared graph.
  step_1 id: 52a4d814…
  step_2 id: 8ad8ff3a…

  Raw data stays at Lab A.
  The trace — sources, steps, upstream evidence — is in the shared graph.
```

## Lab B: reads the trace, replicates independently

```python
# Lab B reads Lab A's trace from the shared graph — the experimental logic,
# not the data — then runs the same hypothesis on its own private dataset.
lab_a_findings = json.loads(query_graph.invoke(
    {"topic": "Target T", "min_support": "PRELIMINARY"}))
for f in lab_a_findings:
    trace = get_provenance_trace.invoke({"claim_id": f["claim_id"]})
    # trace['source_name'] names Lab A's data — which Lab B cannot access.

rep_1 = assert_finding_b.invoke({
    "text": "Candidate target T shows elevated activity in condition C"
            " (partition_2, n=580, fold-change=2.1)",
    "classification": "ANALYTICAL", "supports": [upstream_ref],  # same anchor, independent data
    "source": "private_dataset_B"})
rep_2 = assert_finding_b.invoke({
    "text": "Target T activity in condition C is specific to cell subtype S"
            " (partition_2, pathway analysis, p=0.009)",
    "classification": "ANALYTICAL", "supports": [step_2],  # cites Lab A's published claim
    "source": "private_dataset_B"})
```

```
  query_graph('Target T') → 3 claims from Lab A

  Claim:      Target T activity in condition C is specific to cell subtype…
  Source:     private_dataset_A  ← Lab B cannot access this
  Supports:   ['52a4d814-78fc-4940-ab30-eb2e421cae50']
  Class:      ANALYTICAL
  …

  Lab B published 2 claims.
  rep_1 id: 1693f5fd…
  rep_2 id: d61246e7…
```

## Q1: Independent data paths?

```python
all_claims = graph.query("Target T")
sources = {c.get("source_name") for c in all_claims if c.get("source_name")}
agents  = {c.get("generated_by") for c in all_claims if c.get("generated_by")}
# Independent iff >1 distinct source AND >1 distinct agent.
```

```
  distinct source_names          ['private_dataset_A', 'private_dataset_B']
  distinct generated_by          ['agent_seed/literature', 'lab_a/model-a', 'lab_b/model-b']

  ✓ Two independent data sources, two independent agents.
    If they converged, the finding is not a dataset artifact.
```

## Q2: Genuinely reproducible?

```python
for c in graph.query("Target T"):
    print(c["text"][:45], c["support_level"])
# Independent agents + shared ESTABLISHED upstream + independent data → REPLICATED.
```

```
  Candidate target T shows elevated activity in… REPLICATED
  Target T activity in condition C is specific … PRELIMINARY
  …
  ✓ REPLICATED — independent agents, shared upstream, independent data paths.
    The finding holds across datasets. Genuine replication.
```

## Q3: Provenance distance, and the spurious-replication trap

Provenance distance measures how far a conclusion is from raw data: short chains
of ANALYTICAL steps are strong; long chains of INFERRED steps are fragile. Both
labs' chains are anchored in ANALYTICAL findings from independent sources.

The contrast that makes the example worth reading: two agents repeating the
same LLM prior with **no data behind either**:

```python
spurious_a = assert_finding_a.invoke({
    "text": "Target T is likely relevant in condition C based on literature",
    "classification": "INFERRED", "supports": [upstream_ref]})   # no data pipeline ran
spurious_b = assert_finding_b.invoke({
    "text": "Target T is likely relevant in condition C based on literature",
    "classification": "INFERRED", "supports": [upstream_ref]})   # same upstream → REPLICATED
```

```
  spurious_a support_level       REPLICATED
  spurious_b support_level       REPLICATED
  spurious_a classification      INFERRED

  REPLICATED fired — but classification=INFERRED and source_name=''.
  Two agents repeated the same LLM prior. No data behind either finding.
```

`REPLICATED` alone is not trust. The graph lets you filter it out:
`query('Target T', min_support='REPLICATED')`, then keep only
`classification='ANALYTICAL'` with a non-empty `source_name`.

## Using a real LLM

```python
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools=[
    query_graph, get_provenance_trace, assert_finding])
```
