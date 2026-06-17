# API Walkthrough

The complete `EpistemicGraph` API in a single runnable script. Each step below
is the code from [`01_api_walkthrough.py`](01_api_walkthrough.py) followed by the
console output it prints.

```bash
python 01_api_walkthrough.py
```

No external dependencies. Uses a temporary directory, safe to run anywhere.
(`sep` and `show` in the listings are tiny print helpers; see the script.)

## 1. Open

```python
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
_signing.bootstrap_key(agent_key_path)
_signing.bootstrap_key(reviewer_key_path)

graph = mareforma.open(tmp, key_path=agent_key_path)

# Enroll the reviewer as a second validator. Section 6 needs a validator whose
# key differs from the claim's signer — mareforma refuses self-validation.
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
# INFERRED — default. LLM reasoning without explicit grounding.
c_inferred = graph.assert_claim(
    "Cell type A receives more inhibitory input than cell type B",
)

# ANALYTICAL — deterministic analysis against source data. Agent-declared.
# Only use this when the data pipeline actually ran and produced output.
c_analytical = graph.assert_claim(
    "Cell type A receives more inhibitory input than cell type B (n=1,204, p<0.001)",
    classification="ANALYTICAL",
    source_name="dataset_alpha",
    generated_by="agent_alpha/model-a",
    supports=["upstream_ref_A"],
)

# DERIVED — explicitly built on claims already in the graph.
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
graph.query(min_support="REPLICATED")     # nothing is REPLICATED yet
graph.query(limit=2)                      # limit
graph.get_claim(c_analytical)             # single record by id
```

```
  text='cell type A'     3 claims
  classification=ANALYTICAL 1 claim
  min_support=REPLICATED 0 claims  ← expected 0
  limit=2                2 claims
  get_claim support_level PRELIMINARY
  get_claim classification ANALYTICAL
```

## 4. Idempotency

```python
# Same idempotency_key → same claim_id returned, no duplicate inserted.
# Useful for retry-safe agent loops, and a convergence convention: two agents
# using the same key converge on one claim-id even with different phrasing.
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

## 5. REPLICATED: automatic convergence

```python
# REPLICATED fires when ≥2 claims share the same upstream in supports[] AND
# have different generated_by values AND that upstream is itself ESTABLISHED
# (Cochrane/GRADE methodology — replication-of-noise is not replication).
# seed=True inserts the upstream directly at ESTABLISHED via a signed envelope.
upstream = graph.assert_claim(
    "Property X is elevated in compartment Y",
    classification="DERIVED",
    generated_by="agent_seed/model-a",
    seed=True,                    # ← directly ESTABLISHED, anchors the chain
)

rep_a = graph.assert_claim(
    "Cell type A preferentially targets compartment Y (lab_a, n=800)",
    classification="ANALYTICAL", generated_by="agent_lab_a/model-a",
    supports=[upstream], source_name="dataset_alpha",
)
rep_b = graph.assert_claim(
    "Cell type A preferentially targets compartment Y (lab_b, n=1100)",
    classification="ANALYTICAL", generated_by="agent_lab_b/model-b",
    supports=[upstream], source_name="dataset_beta",   # same upstream, different agent
)
```

```
  lab_a support_level    REPLICATED
  lab_b support_level    REPLICATED
  REPLICATED count       3
```

## 6. ESTABLISHED: human validation only

```python
# validate() requires support_level == REPLICATED. No agent can self-promote.
graph.validate(c_inferred)            # PRELIMINARY — raises ValueError

# Re-open under the reviewer key so the validator differs from rep_a's signer
# (mareforma refuses self-validation). evidence_seen names the upstream claims
# the reviewer consulted; mareforma binds the list into the signed envelope.
graph.close()
with mareforma.open(tmp, key_path=reviewer_key_path) as reviewer_graph:
    reviewer_graph.validate(
        rep_a,
        validated_by="jane@lab.org",
        evidence_seen=[upstream],     # the ESTABLISHED anchor the reviewer read
    )
graph = mareforma.open(tmp, key_path=agent_key_path)
```

```
  validate(PRELIMINARY)  ValueError: Claim '…' has support_level='PRELIMINARY'. Only REPLICATED claims can be promoted to ESTABLISHED.
  support_level          ESTABLISHED
  validated_by           jane@lab.org
  validated_at           2026-06-11
```

## 7. Operational surfaces

```python
# graph.health() — single-call audit summary. Non-zero values flag work to do;
# mareforma reports the counters, it does not decide if anything is wrong.
h = graph.health()

# graph.classify_supports() — see how each supports[]/contradicts[] entry routes:
# claim (v4 UUID — a graph node), doi (Crossref/DataCite syntax), or external.
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
  convergence_retry_pending 0
    f1af42da-db19-4a33-838c-1a6f6ff4 claim
    10.1038/cure                     doi
    https://example.org/preprint     external
```

## 8. Anti-patterns

```python
# ✗ ANALYTICAL on a failed data pipeline. If the pipeline returned null, the
#   finding came from LLM prior knowledge — recording it as ANALYTICAL is a
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

The script names three more anti-patterns in comments: correlated agents
(same model + data) do not produce genuine REPLICATED, `DERIVED` with no
`supports=` is unverifiable, and a shared *hallucinated* upstream produces a
false REPLICATED signal. The graph records what agents assert, not what is true.
`validate()` exists precisely so a human reviews the chain before ESTABLISHED.
