# Documented Contestation

An agent finds an ESTABLISHED consensus in tension with its own results.
It does not discard its finding. It documents the tension explicitly and
asserts a stronger, better-powered claim alongside the existing one.

## Story

An ESTABLISHED finding sits in the graph: two independent agents converged
and a human validated it.

A new agent runs a larger analysis on a different cohort and gets a different
result. Using `contradicts=`, it names the tension explicitly and publishes
its own claim with its own provenance. Both coexist in the graph.

Science advances by documented contestation, not by one side disappearing.
ESTABLISHED means human-validated evidence, not settled truth.

## What you'll see

The challenge lands as `PRELIMINARY` next to the `ESTABLISHED` consensus.
Neither overwrites the other:

```
New agent — larger analysis, different result
  query_graph('Treatment X', min_support='ESTABLISHED') → 2 claims
  challenge support_level      PRELIMINARY
  challenge classification     ANALYTICAL
  contradicts                  2 established claim(s)

Graph state — consensus and challenge coexist
  [ESTABLISHED ] [ANALYTICAL]  Treatment X reduces outcome Y in population P …
  [ESTABLISHED ] [DERIVED   ]  Prior literature: Treatment X is studied …
  [REPLICATED  ] [ANALYTICAL]  Treatment X reduces outcome Y in population P …
  [PRELIMINARY ] [ANALYTICAL]  Treatment X shows no significant effect … ← contradicts ESTABLISHED
```

A reviewer can then `query_graph('Treatment X')` to see both sides, or filter
to `min_support='ESTABLISHED'` to see only validated consensus. The script
closes with a "What NOT to do" section: asserting the challenge *without*
`contradicts=` makes the tension invisible, and discarding it because the
consensus is ESTABLISHED is silence, not science.

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
