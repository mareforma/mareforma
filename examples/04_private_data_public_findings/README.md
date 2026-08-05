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

## Setup: distinct keys per lab, plus a provenance-trace tool

Distinct signing keys are the legacy independence signal, used when no model
lineage is observed; effective independence counts distinct model and method. Two
claims that share an ESTABLISHED upstream, signed by **distinct keys**, reach the
REPLICATED support level. `generated_by` is a display label, not what drives it.
Each lab signs with its own key, passed per-call via `signer=`.

```python
# get_tools() gives the read tool; the write tool binds the graph's default key,
# so per-lab claims call graph.assert_claim(..., signer=...) directly.
query_graph, _ = [tool(fn) for fn in graph.get_tools(generated_by="lab_a/model-a")]

lab_a_priv = _signing.load_private_key(lab_a_key_path)  # Lab A's key
lab_b_priv = _signing.load_private_key(lab_b_key_path)  # Lab B's key

# Enroll Lab A as a validator so its PRELIMINARY step claims are visible on read;
# query() hides PRELIMINARY claims signed by a non-enrolled key by default.
graph.enroll_validator(
    _signing.public_key_to_pem(lab_a_priv.public_key()), identity="lab_a")

@tool
def get_provenance_trace(claim_id: str) -> dict:
    """Return a claim's full lineage, sources, upstream, classification,
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
# provenance, signed by Lab A's key. The raw data never leaves Lab A, only
# source NAMES travel.
upstream_ref = graph.assert_claim(
    "Prior literature on Target T in condition C",
    classification="DERIVED", generated_by="agent_seed/literature", seed=True)

step_1 = graph.assert_claim(
    "Candidate target T shows elevated activity in condition C"
    " (partition_1, n=620, fold-change=2.3)",
    classification="ANALYTICAL", supports=[upstream_ref],
    generated_by="lab_a/model-a", source_name="private_dataset_A",  # name only
    signer=lab_a_priv)                                              # Lab A's key
step_2 = graph.assert_claim(
    "Target T activity in condition C is specific to cell subtype S"
    " (partition_1, pathway analysis, p=0.004)",
    classification="ANALYTICAL", supports=[step_1],  # builds on the previous step
    generated_by="lab_a/model-a", source_name="private_dataset_A", signer=lab_a_priv)
```

```
  Lab A published 2 claims to the shared graph.
  step_1 id: f752301b…
  step_2 id: 99c0ccd6…

  Raw data stays at Lab A.
  The trace, sources, steps, upstream evidence, is in the shared graph.
```

## Lab B: reads the trace, replicates independently

```python
# Lab B reads Lab A's trace from the shared graph, the experimental logic,
# not the data, then runs the same hypothesis on its own private dataset.
lab_a_findings = json.loads(query_graph.invoke(
    {"topic": "Target T", "min_support": "PRELIMINARY"}))
for f in lab_a_findings:
    trace = get_provenance_trace.invoke({"claim_id": f["claim_id"]})
    # trace['source_name'] names Lab A's data, which Lab B cannot access.

rep_1 = graph.assert_claim(
    "Candidate target T shows elevated activity in condition C"
    " (partition_2, n=580, fold-change=2.1)",
    classification="ANALYTICAL", supports=[upstream_ref],  # same anchor, independent data
    generated_by="lab_b/model-b", source_name="private_dataset_B",
    signer=lab_b_priv)                                     # Lab B's key, distinct from Lab A
rep_2 = graph.assert_claim(
    "Target T activity in condition C is specific to cell subtype S"
    " (partition_2, pathway analysis, p=0.009)",
    classification="ANALYTICAL", supports=[step_2],  # cites Lab A's published claim
    generated_by="lab_b/model-b", source_name="private_dataset_B", signer=lab_b_priv)
```

```
  query_graph('Target T') → 3 claims from Lab A

  Claim:      Prior literature on Target T in condition C…
  Source:     None  ← Lab B cannot access this
  Supports:   []
  Class:      DERIVED

  Claim:      Target T activity in condition C is specific to cell subtype…
  Source:     private_dataset_A  ← Lab B cannot access this
  Supports:   ['f752301b-5f30-48a4-9d8a-b382bbd3f6ff']
  Class:      ANALYTICAL

  Claim:      Candidate target T shows elevated activity in condition C (p…
  Source:     private_dataset_A  ← Lab B cannot access this
  Supports:   ['e7b323e1-3cf8-4f49-a664-8efaad569557']
  Class:      ANALYTICAL

  Lab B published 2 claims.
  rep_1 id: 088283cf…
  rep_2 id: 6ce98680…
```

## Q1: Independent data paths?

```python
# The claims with a dataset behind them; the ESTABLISHED seed is the shared
# upstream, not one of the paths being compared.
paths = [c for c in graph.query("Target T") if c.get("source_name")]
sources = {c["source_name"] for c in paths}
keyids  = {c["asserter_keyid"] for c in paths if c.get("asserter_keyid")}
# Independent iff >1 distinct source AND >1 distinct signing key. generated_by
# is a display label the producer picks, so it is printed, never gated on.
```

```
  distinct source_names          ['private_dataset_A', 'private_dataset_B']
  distinct signing keys          ['51584981…', '9f7cb1a0…']
  generated_by (display)         ['lab_a/model-a', 'lab_b/model-b']

  ✓ Two independent data sources, two distinct signing keys.
    If they converged, the finding is not a dataset artifact.
```

Two labs under one key writing two labels would fail this check, which is the
point: the label is free text, the signature is not.

## Q2: Genuinely reproducible?

```python
for c in graph.query("Target T"):
    print(c["text"][:45], c["support_level"])
# Distinct signing keys + shared ESTABLISHED upstream + independent data → REPLICATED.
```

```
  Prior literature on Target T in condition C… ESTABLISHED
  Candidate target T shows elevated activity in… REPLICATED
  Candidate target T shows elevated activity in… REPLICATED
  Target T activity in condition C is specific … PRELIMINARY

  ✓ REPLICATED: distinct signing keys, shared upstream, independent data paths.
    The labs replicated across datasets. REPLICATED is a support label,
    though: read effective independence to know a distinct model checked
    the finding, not two runs of one. The spurious contrast below shows
    REPLICATED firing on nothing.
```

## Q3: Provenance distance, and the spurious-replication trap

Provenance distance measures how far a conclusion is from raw data: short chains
of ANALYTICAL steps are strong; long chains of INFERRED steps are fragile. The
example walks each chain from `supports_json` instead of drawing it by hand:

```python
def chain(claim_id):          # first cited support per hop, oldest first
    hops = []
    while claim_id:
        claim = graph.get_claim(claim_id)
        hops.append(f"{claim['classification']} ({claim_id[:8]}…)")
        supports = json.loads(claim.get("supports_json") or "[]")
        claim_id = supports[0] if supports else None
    return " → ".join(reversed(hops))
```

```
  Lab A, step_2:  DERIVED (e7fa3392…) → ANALYTICAL (5d642ff6…) → ANALYTICAL (5f18031c…)
  Lab B, rep_1:   DERIVED (e7fa3392…) → ANALYTICAL (fa868fbb…)
  Lab B, rep_2:   DERIVED (e7fa3392…) → ANALYTICAL (5d642ff6…) → ANALYTICAL (5f18031c…) → ANALYTICAL (407307ad…)
```

`rep_1` hangs off the prior literature alone, so it is an independent path: the
only node it shares with Lab A is the seed both labs cite. `rep_2` cites Lab A's
`step_2`, so it descends through Lab A's private-dataset chain. That branch is
corroboration built on Lab A, not a second path.

The contrast that makes the example worth reading: two distinct keys repeating
the same LLM prior with **no data behind either**:

```python
spurious_a = graph.assert_claim(
    "Target T is likely relevant in condition C based on literature",
    classification="INFERRED", supports=[upstream_ref],
    generated_by="lab_a/model-a", signer=lab_a_priv)   # no data pipeline ran
spurious_b = graph.assert_claim(
    "Target T is likely relevant in condition C based on literature",
    classification="INFERRED", supports=[upstream_ref],
    generated_by="lab_b/model-b", signer=lab_b_priv)   # distinct key + same upstream → REPLICATED
```

```
  spurious_a support_level       REPLICATED
  spurious_b support_level       REPLICATED
  spurious_a classification      INFERRED

  REPLICATED fired, but classification=INFERRED and source_name=''.
  Two distinct keys repeated the same LLM prior. No data behind either finding.
```

`REPLICATED` alone is not trust. The graph lets you filter it out:
`query('Target T', min_support='REPLICATED')`, then keep only
`classification='ANALYTICAL'` with a non-empty `source_name`.

## Using a real LLM

```python
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools=[
    query_graph, get_provenance_trace, record_claim])
```
