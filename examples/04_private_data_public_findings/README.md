# Private Data, Public Findings

Two autonomous labs share a Mareforma epistemic graph but never share raw data.
Provenance travels — proprietary data does not.

## Story

Lab A discovers a candidate finding and publishes its provenance trace to the
shared graph: which sources were queried, which steps were executed, which
upstream evidence was cited. The raw data stays at Lab A.

Lab B reads the trace — not the data — reconstructs the experimental logic,
and runs an independent replication on its own private dataset. Both traces
are published back to the shared graph.

The graph then answers three questions automatically:

1. **Independent paths?** Did both labs use different data sources, or did
   they both rely on the same LLM prior with no real data behind either finding?

2. **Genuinely reproducible?** Is the finding stable across independent
   datasets, or an artifact of a specific data partition?

3. **Provenance distance?** How far is each conclusion from its raw data?
   How much reasoning is shared vs. independent?

The contrast section shows spurious REPLICATED from INFERRED claims — two
agents repeating the same LLM prior — and how to detect it from the graph.

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
