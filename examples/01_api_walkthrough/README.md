# API Walkthrough

The complete `EpistemicGraph` API in a single runnable script. Each step below
is the code from [`01_api_walkthrough.py`](01_api_walkthrough.py) followed by the
console output it prints.

```bash
python 01_api_walkthrough.py
```

No external dependencies. Uses a temporary directory, safe to run anywhere.
(`sep` and `show` in the listings are tiny print helpers; see the script.)

Trust reads off the derived axes `graph.proposition_status(prop)` returns:
`status` per `content_id` (the answer) and `question_status` per `frame_id` (the
question). Sections 5 and 6 cover what two independent agents converging is
worth and what a human signing off records. Neither writes anything to the
claim that a later reader has to take on the graph's word.

## 1. Open

```python
import json
import tempfile
from pathlib import Path

import mareforma
from mareforma import signing as _signing

tmp = Path(tempfile.mkdtemp())

# Generate signing keys in the temp dir so this example is self-contained. In
# real use you would run `mareforma bootstrap` once and mareforma.open() picks
# up ~/.config/mareforma/key automatically. Passing key_path= here also
# auto-enrolls the key as root validator on this fresh graph.
agent_key_path = tmp / "_agent_key"
reviewer_key_path = tmp / "_reviewer_key"
# Two distinct lab keys for the converging claims in section 5: independence is
# read off the signing key, so the two peers must sign with different keys.
lab_a_key_path = tmp / "_lab_a_key"
lab_b_key_path = tmp / "_lab_b_key"
_signing.bootstrap_key(agent_key_path)
_signing.bootstrap_key(reviewer_key_path)
_signing.bootstrap_key(lab_a_key_path)
_signing.bootstrap_key(lab_b_key_path)

graph = mareforma.open(tmp, key_path=agent_key_path)

# Enroll the reviewer as a second validator. Section 6 needs a validator whose
# key differs from the claim's signer, mareforma refuses self-validation.
reviewer_priv = _signing.load_private_key(reviewer_key_path)
reviewer_pem = _signing.public_key_to_pem(reviewer_priv.public_key())
graph.enroll_validator(reviewer_pem, identity="jane@lab.org")
```

```
  graph                  EpistemicGraph(root=/tmp/tmp…)
  db                     /tmp/tmp…/.mareforma/graph.db
```

## 2. Assert claims

```python
# INFERRED, default. LLM reasoning without explicit grounding.
c_inferred = graph.assert_claim(
    "Cell type A receives more inhibitory input than cell type B",
)

# ANALYTICAL, deterministic analysis against source data. Agent-declared.
# Only use this when the data pipeline actually ran and produced output.
c_analytical = graph.assert_claim(
    "Cell type A receives more inhibitory input than cell type B (n=1,204, p<0.001)",
    classification="ANALYTICAL",
    source_name="dataset_alpha",
    generated_by="agent_alpha/model-a",
    supports=["upstream_ref_A"],
)

# DERIVED, explicitly built on claims already in the graph.
c_derived = graph.assert_claim(
    "Inhibitory specialisation of cell type A is a conserved motif",
    classification="DERIVED",
    generated_by="agent_alpha/model-a",
    supports=[c_analytical],
)
```

```
  INFERRED id            b28499fe…
  ANALYTICAL id          a99a8b1e…
  DERIVED id             8c86faca…
```

## 3. Query

```python
graph.query("cell type A")                # text substring (case-insensitive)
graph.query(classification="ANALYTICAL")  # classification filter
graph.query(refutation_filter="clean")    # nothing contradicts, contests or retracts it
graph.query(limit=2)                      # limit
graph.get_claim(c_analytical)             # single record by id
```

```
  text='cell type A'     3 claims
  classification=ANALYTICAL 1 claim
  refutation=clean       3 claims
  limit=2                2 claims
  get_claim validated_by not validated
  get_claim classification ANALYTICAL
```

## 4. Idempotency

```python
# Same idempotency_key and same fields → same claim_id returned, no duplicate
# inserted. Useful for retry-safe agent loops. A replay with a different text or
# generated_by raises IdempotencyConflictError instead of merging the two.
KEY = "cell_A_inhibitory_dominance"

id_a = graph.assert_claim(
    "Cell type A receives more inhibitory input than cell type B",
    generated_by="agent_beta", idempotency_key=KEY,
)
id_b = graph.assert_claim(
    "Cell type A receives more inhibitory input than cell type B",
    generated_by="agent_beta", idempotency_key=KEY,
)
```

```
  first call             4758dca7…
  second call            4758dca7…
  same id?               True
```

## 5. Convergence

```python
# Two claims citing the same upstream in supports[] and signed by DISTINCT keys
# are what convergence looks like on the record. Nothing is written to either
# claim to say so: the graph stores who signed what and what each cites, and a
# reader derives independence from that. Distinct signing keys are the weakest
# form of it, used when no model lineage is observed; effective independence
# counts distinct model and method. generated_by is a display label and carries
# no weight at all.
lab_a_priv = _signing.load_private_key(lab_a_key_path)
lab_b_priv = _signing.load_private_key(lab_b_key_path)

upstream = graph.assert_claim(
    "Property X is elevated in compartment Y",
    classification="DERIVED",
    generated_by="agent_seed/model-a",                    # anchors the chain
)

rep_a = graph.assert_claim(
    "Cell type A preferentially targets compartment Y (lab_a, n=800)",
    classification="ANALYTICAL", generated_by="agent_lab_a/model-a",
    supports=[upstream], source_name="dataset_alpha",
    signer=lab_a_priv,            # signed by lab A's key
)
rep_b = graph.assert_claim(
    "Cell type A preferentially targets compartment Y (lab_b, n=1100)",
    classification="ANALYTICAL", generated_by="agent_lab_b/model-b",
    supports=[upstream], source_name="dataset_beta",   # same upstream, distinct key
    signer=lab_b_priv,            # signed by lab B's key
)

# The independence signal itself, read off the rows rather than asserted by
# them: two different signers, one shared upstream.
c_rep_a, c_rep_b = graph.get_claim(rep_a), graph.get_claim(rep_b)
c_rep_a["asserter_keyid"] != c_rep_b["asserter_keyid"]
upstream in json.loads(c_rep_b["supports_json"])
```

```
  lab_a validated        False
  lab_b validated        False
  distinct signers       True
  shared upstream        True
```

## 6. Validation (human only)

```python
# validate() records a signed attestation and changes nothing you can filter
# on. No agent can sign off on its own work: a validator whose key appears on
# the claim envelope is refused.
graph.validate(c_inferred)            # signed by this key, so refused

# Re-open under the reviewer key so the validator differs from rep_a's signer
# (mareforma refuses self-validation). evidence_seen names the upstream claims
# the reviewer consulted; mareforma binds the list into the signed envelope.
graph.close()
with mareforma.open(tmp, key_path=reviewer_key_path) as reviewer_graph:
    reviewer_graph.validate(
        rep_a,
        validated_by="jane@lab.org",
        evidence_seen=[upstream],     # the upstream anchor the reviewer read
    )
graph = mareforma.open(tmp, key_path=agent_key_path)
```

```
  validate(own claim)    SelfValidationError: Validator b26948851107… signed claim '5ca33fc0-1b9d-4ff…
  validated_by           jane@lab.org
  validated_at           …
```

## 7. Operational surfaces

```python
# graph.health(), single-call audit summary. Non-zero values flag work to do;
# mareforma reports the counters, it does not decide if anything is wrong.
h = graph.health()

# graph.classify_supports(), see how each supports[]/contradicts[] entry routes:
# claim (v4 UUID, a graph node), doi (Crossref/DataCite syntax), or external.
mixed = [upstream, "10.1038/cure", "https://example.org/preprint"]
graph.classify_supports(mixed)
```

```
  claim_count            7
  validator_count        2
  unsigned_claims        0
  unresolved_claims      0
  dangling_supports      0
  convergence_errors     0
    f1af42da…                        claim
    10.1038/cure                     doi
    https://example.org/preprint     external
```

## 8. Anti-patterns

```python
# ✗ ANALYTICAL on a failed data pipeline. If the pipeline returned null, the
#   finding came from LLM prior knowledge, recording it as ANALYTICAL is a
#   permanent epistemic lie. Classify honestly from what actually ran.
data_result = None                      # simulate silent pipeline failure
honest_classification = "ANALYTICAL" if data_result is not None else "INFERRED"
graph.assert_claim(
    "Gene X is a therapeutic target for disease Y",
    classification=honest_classification,
    generated_by="agent_example/model-a",
)
```

```
  null data → classification INFERRED

  See AGENTS.md → 'Forbidden patterns' for the full reference.
```

The script names three more anti-patterns in comments: correlated agents (same
model + data) do not converge, they agree with themselves; `DERIVED` with no
`supports=` is unverifiable; and two agents citing the same *hallucinated*
upstream look exactly like two agents converging. The graph records what agents
assert, not what is true. `validate()` exists so a human reads the chain before
anyone leans on it.
