"""
examples/01_api_walkthrough.py — Full EpistemicGraph API walkthrough.

Run:
    python examples/01_api_walkthrough.py

No external dependencies. Uses a temporary directory — safe to run anywhere.

Sections
--------
  1. Open           zero setup, context manager
  2. Assert         INFERRED, ANALYTICAL, DERIVED
  3. Query          text, min_support, classification, limit
  4. Idempotency    retry-safe writes and convergence convention
  5. REPLICATED     automatic when two independent agents converge
  6. ESTABLISHED    human validation — requires REPLICATED first
  7. Anti-patterns  what breaks the epistemic model silently
"""

import tempfile
from pathlib import Path

import mareforma


def sep(title: str) -> None:
    print(f"\n{'─' * 58}")
    print(f"  {title}")
    print(f"{'─' * 58}")


def show(label: str, value: object) -> None:
    print(f"  {label:<22} {value}")


tmp = Path(tempfile.mkdtemp())

# ---------------------------------------------------------------------------
# 1. Open
# ---------------------------------------------------------------------------
sep("1. Open")

graph = mareforma.open(tmp)
# graph.db is created automatically on first call.
# No init, no TOML, no project directory required.

show("graph", graph)
show("db", tmp / ".mareforma" / "graph.db")


# ---------------------------------------------------------------------------
# 2. Assert — three classification labels
# ---------------------------------------------------------------------------
sep("2. Assert claims")

# INFERRED — default. LLM reasoning without explicit grounding.
c_inferred = graph.assert_claim(
    "Cell type A receives more inhibitory input than cell type B",
)
show("INFERRED id", c_inferred[:8] + "…")

# ANALYTICAL — deterministic analysis against source data. Agent-declared.
# Only use this when the data pipeline actually ran and produced output.
c_analytical = graph.assert_claim(
    "Cell type A receives more inhibitory input than cell type B (n=1,204, p<0.001)",
    classification="ANALYTICAL",
    source_name="dataset_alpha",
    generated_by="agent_alpha/model-a",
    supports=["upstream_ref_A"],   # upstream anchor: claim_id or reference
)
show("ANALYTICAL id", c_analytical[:8] + "…")

# DERIVED — explicitly built on claims already in the graph.
# Incentivises agents to query before asserting.
c_derived = graph.assert_claim(
    "Inhibitory specialisation of cell type A is a conserved motif",
    classification="DERIVED",
    generated_by="agent_alpha/model-a",
    supports=[c_analytical],
)
show("DERIVED id", c_derived[:8] + "…")


# ---------------------------------------------------------------------------
# 3. Query
# ---------------------------------------------------------------------------
sep("3. Query")

# Text substring (case-insensitive)
r = graph.query("cell type A")
show("text='cell type A'", f"{len(r)} claims")

# Classification filter
r = graph.query(classification="ANALYTICAL")
show("classification=ANALYTICAL", f"{len(r)} claim")

# Minimum support — nothing is REPLICATED yet
r = graph.query(min_support="REPLICATED")
show("min_support=REPLICATED", f"{len(r)} claims  ← expected 0")

# Limit
r = graph.query(limit=2)
show("limit=2", f"{len(r)} claims")

# get_claim — single record by id
claim = graph.get_claim(c_analytical)
if claim:
    show("get_claim support_level", claim["support_level"])
    show("get_claim classification", claim["classification"])


# ---------------------------------------------------------------------------
# 4. Idempotency
# ---------------------------------------------------------------------------
sep("4. Idempotency")

# Same idempotency_key → same claim_id returned, no duplicate inserted.
# Useful for retry-safe agent loops.
KEY = "cell_A_inhibitory_dominance"

id_a = graph.assert_claim(
    "Cell type A receives more inhibitory input than cell type B",
    generated_by="agent_beta",
    idempotency_key=KEY,
)
id_b = graph.assert_claim(
    "Cell type A receives more inhibitory input than cell type B",
    generated_by="agent_beta",
    idempotency_key=KEY,
)
show("first call", id_a[:8] + "…")
show("second call", id_b[:8] + "…")
show("same id?", id_a == id_b)

# Structured keys are also a convergence convention.
# Two agents using "cell_A_inhibitory_dominance" as their key
# will converge on the same claim-id even with different text phrasing,
# without needing explicit supports= links between them.


# ---------------------------------------------------------------------------
# 5. REPLICATED — automatic convergence
# ---------------------------------------------------------------------------
sep("5. REPLICATED (automatic)")

# REPLICATED fires when ≥2 claims share the same upstream in supports[]
# AND have different generated_by values.
# This is the signal that two independent agents reached the same conclusion.

upstream = graph.assert_claim(
    "Property X is elevated in compartment Y",
    classification="ANALYTICAL",
    generated_by="agent_seed/model-a",
    source_name="dataset_alpha",
)

rep_a = graph.assert_claim(
    "Cell type A preferentially targets compartment Y (lab_a, n=800)",
    classification="ANALYTICAL",
    generated_by="agent_lab_a/model-a",
    supports=[upstream],
    source_name="dataset_alpha",
)

rep_b = graph.assert_claim(
    "Cell type A preferentially targets compartment Y (lab_b, n=1100)",
    classification="ANALYTICAL",
    generated_by="agent_lab_b/model-b",
    supports=[upstream],          # same upstream, different agent → REPLICATED fires
    source_name="dataset_beta",
)

c_rep_a = graph.get_claim(rep_a)
c_rep_b = graph.get_claim(rep_b)
show("lab_a support_level", c_rep_a["support_level"] if c_rep_a else "—")
show("lab_b support_level", c_rep_b["support_level"] if c_rep_b else "—")
show("REPLICATED count", len(graph.query(min_support="REPLICATED")))


# ---------------------------------------------------------------------------
# 6. ESTABLISHED — human validation only
# ---------------------------------------------------------------------------
sep("6. ESTABLISHED (human only)")

# validate() requires support_level == REPLICATED.
# No automated path. No agent can self-promote to ESTABLISHED.

try:
    graph.validate(c_inferred)          # PRELIMINARY — raises
except ValueError as exc:
    show("validate(PRELIMINARY)", f"ValueError: {exc}")

graph.validate(rep_a, validated_by="jane@lab.org")
established = graph.get_claim(rep_a)
if established:
    show("support_level", established["support_level"])
    show("validated_by", established["validated_by"])
    show("validated_at", established["validated_at"][:10])


# ---------------------------------------------------------------------------
# 7. Anti-patterns
# ---------------------------------------------------------------------------
sep("7. Anti-patterns")

# ✗  ANALYTICAL on a failed data pipeline
#    If the pipeline returned null, the finding came from LLM prior knowledge.
#    Recording it as ANALYTICAL is a permanent epistemic lie.
data_result = None                      # simulate silent pipeline failure
honest_classification = "ANALYTICAL" if data_result is not None else "INFERRED"
cid = graph.assert_claim(
    "Gene X is a therapeutic target for disease Y",
    classification=honest_classification,
    generated_by="agent_example/model-a",
)
c_cid = graph.get_claim(cid)
show("null data → classification", c_cid["classification"] if c_cid else "—")

print()

# ✗  Correlated agents do not produce genuine REPLICATED
#    Two runs of the same model on the same data are not independent.
#    REPLICATED requires different generated_by AND divergent provenance paths.
#    Encoding model + version + lab context in generated_by makes this auditable:
#      "gpt-4o-2024-11/lab_a"   ← meaningful
#      "agent"                  ← meaningless

# ✗  DERIVED with no supports= is unverifiable
#    The provenance chain is broken. Always pass supports= with DERIVED.

# ✗  Shared upstream from a hallucinated source
#    Two agents citing the same wrong paper will produce a false REPLICATED signal.
#    The graph records what agents assert, not what is true.
#    Validate() exists precisely so a human reviews the chain before ESTABLISHED.

print("  See AGENTS.md → 'Forbidden patterns' for the full reference.")


# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
graph.close()
print(f"\n{'─' * 58}")
print("  Done. Graph written to:", tmp / ".mareforma" / "graph.db")
