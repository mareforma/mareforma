# Compounding Agents

Two agents work sequentially on the same research question.
Knowledge accumulates in the graph instead of evaporating between runs.

## Story

Agent A (Analyst) runs analysis on two independent datasets, citing the same
`ESTABLISHED` upstream claim (seeded once with `seed=True` at the top of the
script). Two independent agents, shared `ESTABLISHED` upstream → `REPLICATED`
fires automatically.

Agent B (Synthesizer) queries the graph before asserting anything. It finds
the REPLICATED findings and builds a DERIVED synthesis on top — traceable
all the way back to the original upstream reference.

Without the graph, Agent B would have started from scratch. With it, findings
compound.

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
