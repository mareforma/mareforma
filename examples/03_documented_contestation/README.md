# Documented Contestation

An agent finds an ESTABLISHED consensus in tension with its own results.
It does not discard its finding — it documents the tension explicitly and
asserts a stronger, better-powered claim alongside the existing one.

## Story

An ESTABLISHED finding sits in the graph — two independent agents converged
and a human validated it.

A new agent runs a larger analysis on a different cohort and gets a different
result. Using `contradicts=`, it names the tension explicitly and publishes
its own claim with its own provenance. Both coexist in the graph.

Science advances by documented contestation, not by one side disappearing.
ESTABLISHED means human-validated evidence — not settled truth.

## Run

```bash
pip install langchain-core
python 03_documented_contestation.py
```

No API key required. To use a real LLM:

```python
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools=[query_graph, assert_finding])
```
