# Compounding Agents

Two agents work sequentially on the same research question. Knowledge
accumulates in the graph instead of evaporating between runs. The closing act
tells the same story with the trust layer, where the support is **earned, not
declared**.

Each step below is the code from
[`02_compounding_agents.py`](02_compounding_agents.py) followed by the console
output it prints.

```bash
pip install langchain-core
python 02_compounding_agents.py
```

No API key required.

## Setup: a shared graph and per-agent tools

```python
import json, tempfile
from pathlib import Path

import mareforma
from mareforma import signing as _signing
from mareforma.trust import (
    Direction, DirectionOfInterest, EffectEstimate, EffectType,
    Prediction, Proposition, TestType,
)
from langchain_core.tools import tool

tmp = Path(tempfile.mkdtemp())
key_path = tmp / "_example_key"
_signing.bootstrap_key(key_path)
graph = mareforma.open(tmp, key_path=key_path)

# graph.get_tools(generated_by=...) returns [query_graph, assert_finding] as
# plain callables, with generated_by baked into the closure. One set per agent.
query_graph, assert_finding_a = [tool(fn) for fn in graph.get_tools(
    generated_by="analyst/model-a/lab_a")]
_, assert_finding_b = [tool(fn) for fn in graph.get_tools(
    generated_by="analyst/model-b/lab_b")]
_, assert_finding_synth = [tool(fn) for fn in graph.get_tools(
    generated_by="synthesizer/model-c/lab_b")]
```

## Agent A: two independent runs

```python
# Shared upstream anchor. seed=True asserts directly at ESTABLISHED with a
# signed seed envelope (enrolled validators only); REPLICATED needs it.
prior_ref = graph.assert_claim(
    "Prior literature: cell type A → cell type B inhibitory connectivity",
    classification="DERIVED", generated_by="agent_seed/literature", seed=True,
)

finding_a = assert_finding_a.invoke({
    "text": "Cell type A forms the majority of inhibitory connections onto cell type B"
            " (dataset_alpha, n=842, p<0.001)",
    "classification": "ANALYTICAL", "supports": [prior_ref], "source": "dataset_alpha",
})
finding_b = assert_finding_b.invoke({
    "text": "Cell type A dominates inhibitory input onto cell type B"
            " (dataset_beta, n=1104, p<0.001)",
    "classification": "ANALYTICAL", "supports": [prior_ref], "source": "dataset_beta",
})  # same upstream, different agent, different source → REPLICATED fires
```

```
  lab_a claim_id             36bda4ab…
  lab_a support_level        PRELIMINARY
  lab_b claim_id             2d170aaa…
  lab_b support_level        REPLICATED

  Two independent agents, shared upstream → REPLICATED fires automatically.
```

## Agent B: Synthesizer

```python
# Query before asserting — the standard agent pattern. query_graph returns
# LLM-safe text (sanitized, wrapped in <untrusted_data>) ready for a prompt.
existing = json.loads(query_graph.invoke(
    {"topic": "cell type A", "min_support": "REPLICATED"}))

replicated_ids = [c["claim_id"] for c in existing]
synthesis = assert_finding_synth.invoke({
    "text": "Inhibitory dominance of cell type A over cell type B is a replicated finding"
            " across independent datasets and consistent with prior literature",
    "classification": "DERIVED", "supports": replicated_ids,
})
```

```
  query_graph('cell type A', min_support='REPLICATED') → 3 claims
    [ESTABLISHED ] <untrusted_data>
Prior literature: cell type A → cell type B inhi…
    [REPLICATED  ] <untrusted_data>
Cell type A dominates inhibitory input onto cell…
    [REPLICATED  ] <untrusted_data>
Cell type A forms the majority of inhibitory con…
  synthesis claim_id         2a70d07f…
  synthesis classification   DERIVED
  synthesis support_level    REPLICATED
  synthesis supports         3
```

## Graph state: the knowledge chain

```python
all_claims = graph.query()
level_order = {"ESTABLISHED": 0, "REPLICATED": 1, "PRELIMINARY": 2}
for c in sorted(all_claims, key=lambda x: level_order.get(x["support_level"], 3)):
    print(f"[{c['support_level']:12}] [{c['classification']:10}]  {c['text'][:55]}…")
```

```
  Total claims in graph: 4

  [ESTABLISHED ] [DERIVED   ]  Prior literature: cell type A → cell type B inhibitory …
  [REPLICATED  ] [DERIVED   ]  Inhibitory dominance of cell type A over cell type B is…
  [REPLICATED  ] [ANALYTICAL]  Cell type A dominates inhibitory input onto cell type B…
  [REPLICATED  ] [ANALYTICAL]  Cell type A forms the majority of inhibitory connection…
```

The chain is the point: `prior reference → ANALYTICAL (×2, independent) →
REPLICATED → DERIVED`. Without querying first, Agent B would have asserted from
scratch.

## The trust layer: earned support

The claim graph above tracks *who asserted what*: each agent picks its own
classification and `supports[]` list. The trust layer makes the support
*earned*. The question becomes a falsifiable `Proposition`; each analyst
registers a `Prediction` **before** seeing the numbers; mareforma **computes**
the bearing from the result and **derives** the status from independent data.

```python
# The same research question, now as a truth-apt, falsifiable claim.
prop = Proposition(
    subject="cell type A", relation="inhibitory connectivity onto", object="cell type B",
    direction=Direction.INCREASES, scope={"region": "cortex", "species": "mouse"},
)
# A pre-registered rule bound to the proposition before any data is seen.
plan = Prediction(
    test_type=TestType.SUPERIORITY,
    direction_of_interest=DirectionOfInterest.INCREASE, alpha=0.05, preregistered=True,
)

# Analyst A on dataset_alpha: a positive SMD with a 90% CI excluding zero.
result_a = graph.assert_finding(
    prop, plan,
    EffectEstimate(estimate_value=0.42, effect_type=EffectType.SMD,
                   ci_lower=0.18, ci_upper=0.66, ci_level=0.90, n_total=842),
    data_id="dataset_alpha", generated_by="analyst/model-a/lab_a",
)
# Analyst B on dataset_beta: a distinct data_id is a second independent line.
result_b = graph.assert_finding(
    prop, plan,
    EffectEstimate(estimate_value=0.51, effect_type=EffectType.SMD,
                   ci_lower=0.20, ci_upper=0.82, ci_level=0.90, n_total=1104),
    data_id="dataset_beta", generated_by="analyst/model-b/lab_b",
)
```

```
  frame_id (the question)    e238fa89…
  content_id (the answer)    bd5b2d04…
  alpha bearing (computed)   supports
  alpha status (derived)     PRELIMINARY
  beta bearing (computed)    supports
  beta status (derived)      CORROBORATED
```

Neither agent declared `supports`. mareforma computed each bearing from the
pre-registered rule and derived `CORROBORATED` from two independent datasets.

## Synthesizer: query the frame

```python
# The frame is the question, not the wording. min_status filters on the
# UNTESTED < PRELIMINARY < CORROBORATED support ladder.
views = graph.query_frame(prop, min_status="PRELIMINARY")
```

```
  query_frame(prop, min_status='PRELIMINARY') → 1 proposition(s)
  status                     CORROBORATED
  independent support lines  2
  frame contest              consistent
```

The bearing is a function of the registered rule and the realised numbers, so a
refutation cannot be relabelled as support. Status is a count over independent
data, not a self-declared label.

## Using a real LLM

The script uses explicit control flow to simulate what an LLM would decide. The
same tools drive a real agent unchanged:

```python
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools=[query_graph, assert_finding])
```
