# What the instrument refuses to count

Two failure modes an honest trust layer has to catch, shown end to end:

1. **Absence.** A finding whose analysis step never ran cannot earn `GROUNDED`.
   The observer watched the scope, saw no cited read, and returns `UNGROUNDED` —
   so the trust map's grounding edge names no source. A number with no observed
   execution has an empty provenance edge, not a filled-in one.
2. **Overcounted convergence.** Two agents on the same model that reach the same
   answer are one line of evidence, not two. Distinct signing keys and distinct
   datasets do not change that: effective independence stays at 1 until a
   genuinely different model checks the result, and only then rises.

Each step below is the code from
[`02_compounding_agents.py`](02_compounding_agents.py) followed by the console
output it prints.

```bash
python 02_compounding_agents.py
```

No API key and no network required. The model calls run through an in-memory
httpx transport, so the model capture at the socket boundary is real while the
example runs anywhere.

## Setup: a shared graph, distinct agent keys, and datasets on disk

```python
import tempfile
from pathlib import Path

import httpx

import mareforma
from mareforma import signing as _signing
from mareforma.observe import observe
from mareforma.trust import (
    Direction, DirectionOfInterest, EffectEstimate, EffectType,
    Prediction, Proposition, TestType,
)

tmp = Path(tempfile.mkdtemp())
root_key = tmp / "_root_key"
_signing.bootstrap_key(root_key)
graph = mareforma.open(tmp, key_path=root_key)

# Three agents, three distinct signing keys — the strongest legacy signal of
# independent work. Each is enrolled as its own validator.
agent_graphs = {}
for name in ("agent-a", "agent-b", "agent-c"):
    kp = tmp / f"_{name}_key"
    _signing.bootstrap_key(kp)
    priv = _signing.load_private_key(kp)
    graph.enroll_validator(
        _signing.public_key_to_pem(priv.public_key()),
        identity=name, validator_type="llm")
    agent_graphs[name] = mareforma.open(tmp, key_path=kp)

# An offline httpx client: the transport answers locally, and the observer still
# reads the model off the request body at the socket boundary.
client = httpx.Client(
    transport=httpx.MockTransport(lambda req: httpx.Response(200, json={})))

# A pre-registered decision rule, and two falsifiable propositions: the absence
# demo and the independence demo use separate questions so neither leaks
# evidence into the other's count.
plan = Prediction(
    test_type=TestType.SUPERIORITY,
    direction_of_interest=DirectionOfInterest.INCREASE, alpha=0.05, preregistered=True)
prop_absence = Proposition(
    subject="cell type A", relation="gap-junction coupling onto", object="cell type C",
    direction=Direction.INCREASES, scope={"region": "cortex", "species": "mouse"})
prop = Proposition(
    subject="cell type A", relation="inhibitory connectivity onto", object="cell type B",
    direction=Direction.INCREASES, scope={"region": "cortex", "species": "mouse"})
graph.register_plan(prop_absence, plan, generated_by="protocol")
graph.register_plan(prop, plan, generated_by="protocol")
```

## 1. Absence: a number with no observed execution

An agent reports a result, but the step that reads the data never ran. The
observer saw the whole scope and no cited read, so the finding cannot earn
`GROUNDED`.

```python
# The analysis step never executed: nothing reads the cited dataset.
with observe(cites=str(delta_csv)) as handle:
    pass
absent_verdict = handle.verdict   # UNGROUNDED, grounded_sources empty

absent = agent_graphs["agent-a"].assert_finding(
    prop_absence, plan, estimate, data_id="dataset_delta",
    data_source=str(delta_csv), generated_by="agent-a/absent-run",
    grounding=absent_verdict)
absent_map = graph.trust_map(absent["claim_id"])
```

```
  observed grounding           UNGROUNDED
  grounded sources             (none)
  trust map grounding          UNGROUNDED
    scope fully observed; no read matching the cited source returned data; grounded on: (no cited read observed); ...
```

The bearing still computes from the pre-registered rule, but the grounding axis
reports `UNGROUNDED`: no observed read stands behind the number. The provenance
edge is empty because nothing grounded it.

The contrast is the same analysis, actually executed — the model is called and
the cited dataset is read:

```python
with observe(cites=str(epsilon_csv)) as handle:
    client.post(ANTHROPIC_URL, json={"model": "claude-3-5-sonnet-20241022", ...})
    with open(epsilon_csv) as fh:
        fh.read()
grounded_verdict = handle.verdict   # GROUNDED, grounded on epsilon_csv
```

```
  observed grounding           GROUNDED
  grounded sources             ['dataset_epsilon.csv']
  trust map grounding          GROUNDED
    the cited source was opened for reading and is non-empty (file; the open path proxies data flow by file size, it does not observe the bytes read); grounded on: .../dataset_epsilon.csv
```

## 2. Same-model convergence does not count twice

`agent-a` runs the first grounded check on the shared question. A second agent,
distinct key and distinct dataset, reaches the same answer — but on the same
model. The instrument does not read that as a second independent line.

```python
verdict_a = model_check(client, ANTHROPIC_URL, "claude-3-5-sonnet-20241022", alpha_csv)
agent_graphs["agent-a"].assert_finding(
    prop, plan, estimate, data_id="dataset_alpha",
    data_source=str(alpha_csv), generated_by="agent-a", grounding=verdict_a)

verdict_b = model_check(client, ANTHROPIC_URL, "claude-3-5-sonnet-20241022", beta_csv)
line_b = agent_graphs["agent-b"].assert_finding(
    prop, plan, estimate, data_id="dataset_beta",
    data_source=str(beta_csv), generated_by="agent-b", grounding=verdict_b)
graph.trust_map(line_b["claim_id"]).get("independence").value   # "1"
```

```
  agent-a model                claude-3-5-sonnet
  agent-b model                claude-3-5-sonnet
  distinct keys                yes
  distinct datasets            yes
  effective independence       1
    1 pairwise-distinct (model, data, signer) supporting check(s); coarse by design ...
```

Distinct in every legacy axis — signer and dataset — but one model. Two
same-model checks are one line of evidence, so the count holds at 1.

## 3. A different model raises the count

```python
verdict_c = model_check(client, OPENAI_URL, "gpt-4o-2024-08-06", gamma_csv)
line_c = agent_graphs["agent-c"].assert_finding(
    prop, plan, estimate, data_id="dataset_gamma",
    data_source=str(gamma_csv), generated_by="agent-c", grounding=verdict_c)
graph.trust_map(line_c["claim_id"]).get("independence").value   # "2"
```

```
  agent-c model                gpt-4o
  effective independence       2
    2 pairwise-distinct (model, data, signer) supporting check(s); coarse by design ...
```

A genuinely different model is a second line. The count moves from 1 to 2 only
when the evidence is independent in the axis that matters — the model — not
merely in the key that signed it.

## Where the numbers come from

The model is read off the request body at the socket boundary, which the
producer does not control, so the lineage is `COMPUTED`. The grounding verdict
comes from watching the scope for a read of the cited source. Both ride into the
signed finding through `grounding=`, and the trust map places each on its own
axis: grounding, and effective independence. Neither is a label the agent chose.
