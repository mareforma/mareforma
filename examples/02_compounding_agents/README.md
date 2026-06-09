# Compounding Agents

Two agents work sequentially on the same research question.
Knowledge accumulates in the graph instead of evaporating between runs.

## Story

Agent A (Analyst) runs analysis on two independent datasets, citing the same
`ESTABLISHED` upstream claim (seeded once with `seed=True` at the top of the
script). Two independent agents, shared `ESTABLISHED` upstream → `REPLICATED`
fires automatically.

Agent B (Synthesizer) queries the graph before asserting anything. It finds
the REPLICATED findings and builds a DERIVED synthesis on top, traceable
all the way back to the original upstream reference.

Without the graph, Agent B would have started from scratch. With it, findings
compound.

## What you'll see

Agent A's two runs converge; Agent B queries, then builds on top:

```
Agent A — Analyst (two independent runs)
  lab_a support_level        PRELIMINARY
  lab_b support_level        REPLICATED      ← shared upstream → REPLICATED

Agent B — Synthesizer
  query_graph('cell type A', min_support='REPLICATED') → 3 claims
  synthesis classification   DERIVED
  synthesis support_level    REPLICATED
  synthesis supports         3

Graph state — knowledge chain
  [ESTABLISHED ] [DERIVED   ]  Prior literature: cell type A → cell type B …
  [REPLICATED  ] [DERIVED   ]  Inhibitory dominance of cell type A over B …
  [REPLICATED  ] [ANALYTICAL]  Cell type A dominates inhibitory input …
  [REPLICATED  ] [ANALYTICAL]  Cell type A forms the majority of …
```

The chain is the point: `prior reference → ANALYTICAL (×2, independent) →
REPLICATED → DERIVED`. Agent B's synthesis traces all the way back to the
seed. Without querying first, it would have asserted from scratch.

## Run

```bash
pip install langchain-core
python 02_compounding_agents.py
```

No API key required. Uses LangChain `@tool` definitions with explicit control
flow simulating what an LLM would decide. To use a real LLM:

```python
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools=[query_graph, assert_finding])
```
