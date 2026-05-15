"""
examples/04_private_data_public_findings.py — Private data, public findings.

Run:
    pip install langchain-core
    python examples/04_private_data_public_findings.py

No API key required.

Story
-----
Two autonomous AI scientists — Lab A and Lab B — work on the same hypothesis
using different private datasets. Neither has access to the other's raw data.
Both connect to a shared Mareforma epistemic graph.

  Lab A discovers a candidate finding and publishes its provenance trace
  to the shared graph. The trace contains the complete epistemic lineage:
  which sources were queried, which steps were executed, which claims were
  made at each stage, and which upstream evidence was cited.
  The raw data never leaves Lab A.

  Lab B receives only the provenance trace — not the data, not the model,
  not the code. From the trace, Lab B reconstructs the experimental logic,
  runs an independent replication on its own private dataset, and publishes
  its own trace back to the shared graph.

The shared graph then answers three questions automatically:

  Q1. Independent paths?
      Did the two labs reach the same finding through independent data paths?
      Or did they both rely on the same prior knowledge with no real data
      behind either finding?

  Q2. Genuinely reproducible?
      Is the finding reproducible across independent datasets,
      or an artifact of a specific data partition?

  Q3. Provenance distance?
      How much of the reasoning is shared vs. independent?
      How far is each conclusion from its raw data?

LangChain integration
---------------------
graph.get_tools(generated_by="...") returns [query_graph, assert_finding].
get_provenance_trace is defined separately (wraps graph.get_claim).

    from langchain_openai import ChatOpenAI
    from langgraph.prebuilt import create_react_agent
    from langchain_core.tools import tool as lc_tool

    lab_a_tools = [lc_tool(fn) for fn in graph.get_tools(generated_by="lab_a/model-a")]
    lab_a_tools.append(lc_tool(get_provenance_trace))
    agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools=lab_a_tools)
"""

import json
import tempfile
from pathlib import Path

import mareforma
from mareforma import signing as _signing
from langchain_core.tools import tool


def sep(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def show(label: str, value: object) -> None:
    print(f"  {label:<30} {value}")


# ---------------------------------------------------------------------------
# Shared epistemic graph
# Both labs connect to the same graph. Raw data stays private.
# ---------------------------------------------------------------------------

tmp = Path(tempfile.mkdtemp())
# Self-contained signing key for this example. In real use, run
# `mareforma bootstrap` once and mareforma.open() picks the key up
# automatically. The first key opened against a fresh graph auto-enrolls
# as the root validator, which is required to bootstrap the ESTABLISHED
# upstream with seed=True below.
key_path = tmp / "_example_key"
_signing.bootstrap_key(key_path)
graph = mareforma.open(tmp, key_path=key_path)


# ---------------------------------------------------------------------------
# Mareforma tools — get_tools() per lab, get_provenance_trace defined separately
# ---------------------------------------------------------------------------

query_graph, assert_finding_a = [tool(fn) for fn in graph.get_tools(
    generated_by="lab_a/model-a"
)]
_, assert_finding_b = [tool(fn) for fn in graph.get_tools(
    generated_by="lab_b/model-b"
)]


@tool
def get_provenance_trace(claim_id: str) -> dict:
    """Retrieve the full provenance trace for a claim.

    Returns the claim with its complete epistemic lineage:
    sources queried, upstream evidence cited, classification, support level.
    This is what Lab B reads from Lab A's published finding —
    the trace, not the raw data.
    """
    claim = graph.get_claim(claim_id)
    if claim is None:
        return {}
    return {
        "claim_id": claim["claim_id"],
        "text": claim["text"],
        "classification": claim["classification"],
        "support_level": claim["support_level"],
        "source_name": claim.get("source_name"),
        "generated_by": claim.get("generated_by"),
        "supports": json.loads(claim.get("supports_json", "[]") or "[]"),
        "contradicts": json.loads(claim.get("contradicts_json", "[]") or "[]"),
    }


# ---------------------------------------------------------------------------
# Lab A — discovers the finding, publishes the trace
# ---------------------------------------------------------------------------

sep("Lab A — discovery and trace publication")

# Bootstrap an ESTABLISHED upstream both labs cite. Under the ESTABLISHED-upstream
# rule, REPLICATED requires at least one ESTABLISHED claim in supports[] — matches
# Cochrane/GRADE evidence-chain methodology. seed=True asserts directly at
# ESTABLISHED via a signed seed envelope (enrolled validators only).
upstream_ref = graph.assert_claim(
    "Prior literature on Target T in condition C",
    classification="DERIVED",
    generated_by="agent_seed/literature",
    seed=True,
)

# Lab A runs a multi-step analysis on its private dataset.
# Each intermediate step is published as a claim with provenance.
# The raw data never leaves Lab A.

step_1 = assert_finding_a.invoke({
    "text": "Candidate target T shows elevated activity in condition C"
            " (partition_1, n=620, fold-change=2.3)",
    "classification": "ANALYTICAL",
    "supports": [upstream_ref],
    "source": "private_dataset_A",          # name only — data stays at Lab A
})

step_2 = assert_finding_a.invoke({
    "text": "Target T activity in condition C is specific to cell subtype S"
            " (partition_1, pathway analysis, p=0.004)",
    "classification": "ANALYTICAL",
    "supports": [step_1],                   # builds on the previous step
    "source": "private_dataset_A",
})

print("  Lab A published 2 claims to the shared graph.")
print(f"  step_1 id: {step_1[:8]}…")
print(f"  step_2 id: {step_2[:8]}…")
print()
print("  Raw data stays at Lab A.")
print("  The trace — sources, steps, upstream evidence — is in the shared graph.")


# ---------------------------------------------------------------------------
# Lab B — reads the trace, replicates independently
# ---------------------------------------------------------------------------

sep("Lab B — reads trace, runs independent replication")

# Step 1: Lab B reads Lab A's provenance trace from the shared graph.
# It sees the experimental logic — not the data.
lab_a_findings = json.loads(query_graph.invoke({"topic": "Target T", "min_support": "PRELIMINARY"}))
print(f"  query_graph('Target T') → {len(lab_a_findings)} claims from Lab A\n")

for f in lab_a_findings:
    trace = get_provenance_trace.invoke({"claim_id": f["claim_id"]})
    print(f"  Claim:      {trace['text'][:60]}…")
    print(f"  Source:     {trace['source_name']}  ← Lab B cannot access this")
    print(f"  Supports:   {trace['supports']}")
    print(f"  Class:      {trace['classification']}")
    print()

# Step 2: Lab B reconstructs the experimental logic from the trace.
# It runs the same hypothesis on its own private dataset.
print("  Lab B reconstructs experimental logic and replicates on private_dataset_B…\n")

rep_1 = assert_finding_b.invoke({
    "text": "Candidate target T shows elevated activity in condition C"
            " (partition_2, n=580, fold-change=2.1)",
    "classification": "ANALYTICAL",
    "supports": [upstream_ref],             # same upstream anchor, independent data
    "source": "private_dataset_B",          # different private dataset
})

rep_2 = assert_finding_b.invoke({
    "text": "Target T activity in condition C is specific to cell subtype S"
            " (partition_2, pathway analysis, p=0.009)",
    "classification": "ANALYTICAL",
    "supports": [step_2],                   # cites Lab A's published claim as upstream
    "source": "private_dataset_B",
})

print(f"  Lab B published 2 claims.")
print(f"  rep_1 id: {rep_1[:8]}…")
print(f"  rep_2 id: {rep_2[:8]}…")


# ---------------------------------------------------------------------------
# Q1 — Independent data paths?
# ---------------------------------------------------------------------------

sep("Q1 — Independent data paths?")

all_claims = graph.query("Target T")
sources = {c.get("source_name") for c in all_claims if c.get("source_name")}
agents  = {c.get("generated_by") for c in all_claims if c.get("generated_by")}

show("distinct source_names", sorted(sources))
show("distinct generated_by", sorted(agents))
print()

if len(sources) > 1 and len(agents) > 1:
    print("  ✓ Two independent data sources, two independent agents.")
    print("    If they converged, the finding is not a dataset artifact.")
else:
    print("  ✗ Same source or same agent — not genuinely independent.")


# ---------------------------------------------------------------------------
# Q2 — Genuinely reproducible?
# ---------------------------------------------------------------------------

sep("Q2 — Genuinely reproducible?")

for c in graph.query("Target T"):
    show(c["text"][:45] + "…", c["support_level"])

print()
c_rep1 = graph.get_claim(rep_1)
c_rep2 = graph.get_claim(rep_2)
support_1 = c_rep1["support_level"] if c_rep1 else "—"
support_2 = c_rep2["support_level"] if c_rep2 else "—"

if support_1 == "REPLICATED" or support_2 == "REPLICATED":
    print("  ✓ REPLICATED — independent agents, shared upstream, independent data paths.")
    print("    The finding holds across datasets. Genuine replication.")
else:
    print("  Claims are PRELIMINARY — replication pending.")


# ---------------------------------------------------------------------------
# Q3 — Provenance distance?
# ---------------------------------------------------------------------------

sep("Q3 — Provenance distance?")

print("""
  Provenance distance measures how far a conclusion is from raw data.
  A short chain close to raw ANALYTICAL steps = high epistemic confidence.
  A long chain of INFERRED steps = epistemic fragility.

  Lab A's chain:  upstream_ref_A → ANALYTICAL (step_1) → ANALYTICAL (step_2)
  Lab B's chain:  upstream_ref_A → ANALYTICAL (rep_1)  → ANALYTICAL (rep_2)

  Both chains are anchored in ANALYTICAL findings from independent sources.
  The shared node (upstream_ref_A) is the prior literature — not a model prior.

  Compare with spurious replication (see below): both chains INFERRED,
  no data behind either. REPLICATED fires but the signal is worthless.
""")


# ---------------------------------------------------------------------------
# Contrast — spurious replication
# ---------------------------------------------------------------------------

sep("Contrast — spurious replication (what to watch for)")

# Both labs assert INFERRED with no source_name and no real data.
# REPLICATED fires because they share the same upstream,
# but the finding is backed entirely by LLM prior knowledge.

spurious_a = assert_finding_a.invoke({
    "text": "Target T is likely relevant in condition C based on literature",
    "classification": "INFERRED",           # no data pipeline ran
    "supports": [upstream_ref],
})

spurious_b = assert_finding_b.invoke({
    "text": "Target T is likely relevant in condition C based on literature",
    "classification": "INFERRED",
    "supports": [upstream_ref],             # same upstream → REPLICATED fires
})

c_sp_a = graph.get_claim(spurious_a)
c_sp_b = graph.get_claim(spurious_b)
show("spurious_a support_level", c_sp_a["support_level"] if c_sp_a else "—")
show("spurious_b support_level", c_sp_b["support_level"] if c_sp_b else "—")
show("spurious_a classification", c_sp_a["classification"] if c_sp_a else "—")

print()
print("  REPLICATED fired — but classification=INFERRED and source_name=''.")
print("  Two agents repeated the same LLM prior. No data behind either finding.")
print()
print("  The graph makes this detectable:")
print("    graph.query('Target T', min_support='REPLICATED')")
print("    → filter for classification='ANALYTICAL' and source_name != ''")
print("    → spurious claims are excluded from the trustworthy result set.")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

graph.close()
print(f"\n{'─' * 60}")
print("  Done. Graph written to:", tmp / ".mareforma" / "graph.db")
