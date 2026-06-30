"""
examples/02_compounding_agents.py — How agent findings compound.

Run:
    pip install langchain-core
    python examples/02_compounding_agents.py

No API key required.

Story
-----
Two agents work sequentially on the same research question.

  Agent A (Analyst) runs on two independent datasets.
  Both analyses cite the same upstream evidence.
  Distinct signing keys, shared upstream -> REPLICATED fires automatically.

  Agent B (Synthesizer) queries the graph before asserting anything.
  Finds the REPLICATED findings, builds a DERIVED synthesis on top.
  Asserts only what the graph already supports.

  The result: Agent B's conclusion is traceable to raw data.
  Knowledge accumulates instead of evaporating between agent runs.

The trust layer — earned support
--------------------------------
Above, support rises from self-declared edges: each agent picks its own
classification and supports[] list. The closing act tells the same compounding
story with the trust layer, where the support is earned. The question becomes a
falsifiable Proposition; each analyst registers a Prediction BEFORE seeing the
numbers; mareforma COMPUTES the bearing from the result (never declared) and
DERIVES status from independent datasets: one line is PRELIMINARY, two
independent lines are CORROBORATED.

LangChain integration
---------------------
graph.get_tools(generated_by="...") returns [query_graph, assert_finding] as plain
callables. Wrap with @tool for LangChain, or pass directly to the Anthropic SDK.
Each agent gets its own tool set — generated_by is baked into the closure.

    from langchain_openai import ChatOpenAI
    from langgraph.prebuilt import create_react_agent
    from langchain_core.tools import tool as lc_tool

    lc_tools = [lc_tool(fn) for fn in graph.get_tools(generated_by="agent/gpt-4o/lab_a")]
    agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools=lc_tools)
    agent.invoke({"messages": [HumanMessage("Synthesise findings about cell type A.")]})
"""

import json
import tempfile
from pathlib import Path

import mareforma
from mareforma import signing as _signing
from mareforma.trust import (
    Direction,
    DirectionOfInterest,
    EffectEstimate,
    EffectType,
    Prediction,
    Proposition,
    TestType,
)
from langchain_core.tools import tool


def sep(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def show(label: str, value: object) -> None:
    print(f"  {label:<26} {value}")


# ---------------------------------------------------------------------------
# Shared epistemic graph
# ---------------------------------------------------------------------------

tmp = Path(tempfile.mkdtemp())
# Self-contained signing key for this example. In real use, run
# `mareforma bootstrap` once and mareforma.open() picks the key up
# from ~/.config/mareforma/key automatically. The first key opened
# against a fresh graph auto-enrolls as the root validator, which is
# required to bootstrap the ESTABLISHED upstream with seed=True below.
key_path = tmp / "_example_key"
_signing.bootstrap_key(key_path)
graph = mareforma.open(tmp, key_path=key_path)

# Two distinct lab keys for the converging claims below. REPLICATED keys on
# the signing key: the signing key is the independence unit, so the two peers
# must sign with different keys to promote. generated_by stays a display label.
lab_a_key_path = tmp / "_lab_a_key"
lab_b_key_path = tmp / "_lab_b_key"
_signing.bootstrap_key(lab_a_key_path)
_signing.bootstrap_key(lab_b_key_path)
lab_a_priv = _signing.load_private_key(lab_a_key_path)
lab_b_priv = _signing.load_private_key(lab_b_key_path)


# ---------------------------------------------------------------------------
# Mareforma tools via get_tools() — one set per agent, generated_by baked in
# ---------------------------------------------------------------------------

# query_graph is the shared read tool. The two converging analyst claims below
# assert via graph.assert_claim directly so each can pass its own signer=: the
# get_tools() closures bake in generated_by, not a signing key, and the signing
# key is what drives REPLICATED.
query_graph, _ = [tool(fn) for fn in graph.get_tools(
    generated_by="analyst/model-a/lab_a"
)]

# Synthesizer
_, assert_finding_synth = [tool(fn) for fn in graph.get_tools(
    generated_by="synthesizer/model-c/lab_b"
)]


# ---------------------------------------------------------------------------
# Agent A — Analyst (two independent runs)
# Explicit control flow simulates what the LLM would decide to do.
# ---------------------------------------------------------------------------

sep("Agent A — Analyst (two independent runs)")

# Shared upstream anchor — a prior claim both analyses build on. Under the
# ESTABLISHED-upstream rule, REPLICATED requires an upstream claim with
# support_level='ESTABLISHED' in supports[]; seed=True asserts directly at
# ESTABLISHED with a signed seed envelope, only available to enrolled
# validators (the loaded key auto-enrolled as root on first open).
prior_ref = graph.assert_claim(
    "Prior literature: cell type A → cell type B inhibitory connectivity",
    classification="DERIVED",
    generated_by="agent_seed/literature",
    seed=True,
)

# Run 1: Lab A, dataset alpha. Signed by lab A's key.
finding_a = graph.assert_claim(
    "Cell type A forms the majority of inhibitory connections onto cell type B"
    " (dataset_alpha, n=842, p<0.001)",
    classification="ANALYTICAL",
    supports=[prior_ref],
    source_name="dataset_alpha",
    generated_by="analyst/model-a/lab_a",
    signer=lab_a_priv,
)
c_a = graph.get_claim(finding_a)
show("lab_a claim_id", finding_a[:8] + "…")
show("lab_a support_level", c_a["support_level"] if c_a else "—")

# Run 2: Lab B, independent dataset beta. Same upstream reference, distinct
# signing key, different source: REPLICATED fires.
finding_b = graph.assert_claim(
    "Cell type A dominates inhibitory input onto cell type B"
    " (dataset_beta, n=1104, p<0.001)",
    classification="ANALYTICAL",
    supports=[prior_ref],
    source_name="dataset_beta",
    generated_by="analyst/model-b/lab_b",
    signer=lab_b_priv,
)
c_b = graph.get_claim(finding_b)
show("lab_b claim_id", finding_b[:8] + "…")
show("lab_b support_level", c_b["support_level"] if c_b else "—")

print()
print("  Two distinct signing keys, shared upstream -> REPLICATED fires automatically.")


# ---------------------------------------------------------------------------
# Agent B — Synthesizer
# Queries the graph first. Builds on what is already established.
# ---------------------------------------------------------------------------

sep("Agent B — Synthesizer")

# Step 1: query before asserting — the standard agent pattern
existing = json.loads(query_graph.invoke({"topic": "cell type A", "min_support": "REPLICATED"}))
print(f"  query_graph('cell type A', min_support='REPLICATED') → {len(existing)} claims")
for c in existing:
    print(f"    [{c['support_level']:12}] {c['text'][:65]}…")

# Step 2: build on what the graph already supports
replicated_ids = [c["claim_id"] for c in existing]

synthesis = assert_finding_synth.invoke({
    "text": "Inhibitory dominance of cell type A over cell type B is a replicated finding"
            " across independent datasets and consistent with prior literature",
    "classification": "DERIVED",
    "supports": replicated_ids,
})

c_synthesis = graph.get_claim(synthesis)
show("synthesis claim_id", synthesis[:8] + "…")
show("synthesis classification", c_synthesis["classification"] if c_synthesis else "—")
show("synthesis support_level", c_synthesis["support_level"] if c_synthesis else "—")
show("synthesis supports", len(replicated_ids))


# ---------------------------------------------------------------------------
# Graph state — the full knowledge chain
# ---------------------------------------------------------------------------

sep("Graph state — knowledge chain")

all_claims = graph.query()
print(f"  Total claims in graph: {len(all_claims)}\n")

level_order = {"ESTABLISHED": 0, "REPLICATED": 1, "PRELIMINARY": 2}
for c in sorted(all_claims, key=lambda x: level_order.get(x["support_level"], 3)):
    label = f"[{c['support_level']:12}] [{c['classification']:10}]"
    print(f"  {label}  {c['text'][:55]}…")

print()
print("  Agent B's synthesis is traceable:")
print("  prior reference -> ANALYTICAL (x2, distinct keys) -> REPLICATED -> DERIVED")
print()
print("  Without querying the graph, Agent B would have asserted from scratch.")
print("  The graph is what makes findings compound instead of evaporate.")


# ---------------------------------------------------------------------------
# The trust layer — support that is earned, not declared
# ---------------------------------------------------------------------------
# The claim graph above tracks who asserted what. The trust layer makes the
# support earned: the same question becomes a falsifiable Proposition, each
# analyst registers a Prediction before seeing the numbers, mareforma computes
# the bearing from the result, and status derives from independent datasets.

sep("The trust layer — earned support")

# The same research question, now as a truth-apt, falsifiable claim.
prop = Proposition(
    subject="cell type A",
    relation="inhibitory connectivity onto",
    object="cell type B",
    direction=Direction.INCREASES,
    scope={"region": "cortex", "species": "mouse"},
)

# A pre-registered decision rule bound to the proposition before any data is
# seen: if it holds, the effect lands on the INCREASE side of the null.
plan = Prediction(
    test_type=TestType.SUPERIORITY,
    direction_of_interest=DirectionOfInterest.INCREASE,
    alpha=0.05,
    preregistered=True,
)

show("frame_id (the question)", prop.frame_id()[:8] + "…")
show("content_id (the answer)", prop.content_id()[:8] + "…")

# Analyst A on dataset_alpha: a standardised mean difference, positive, with a
# 90% CI excluding zero (the one-sided test at alpha=0.05).
result_a = graph.assert_finding(
    prop,
    plan,
    EffectEstimate(
        estimate_value=0.42,
        effect_type=EffectType.SMD,
        ci_lower=0.18,
        ci_upper=0.66,
        ci_level=0.90,
        n_total=842,
    ),
    data_id="dataset_alpha",
    generated_by="analyst/model-a/lab_a",
)
show("alpha bearing (computed)", result_a["bearing"]["direction"])
show("alpha status (derived)", result_a["status"])

# Analyst B's independent run on dataset_beta. A distinct data_id is a second
# independent line of support for the same proposition.
result_b = graph.assert_finding(
    prop,
    plan,
    EffectEstimate(
        estimate_value=0.51,
        effect_type=EffectType.SMD,
        ci_lower=0.20,
        ci_upper=0.82,
        ci_level=0.90,
        n_total=1104,
    ),
    data_id="dataset_beta",
    generated_by="analyst/model-b/lab_b",
)
show("beta bearing (computed)", result_b["bearing"]["direction"])
show("beta status (derived)", result_b["status"])

print()
print("  Neither agent declared 'supports'. mareforma computed each bearing")
print("  from the pre-registered rule and derived CORROBORATED from two")
print("  independent datasets.")


# ---------------------------------------------------------------------------
# Synthesizer — query the frame (the question, not the wording)
# ---------------------------------------------------------------------------

sep("Synthesizer — query the frame")

views = graph.query_frame(prop, min_status="PRELIMINARY")
print(f"  query_frame(prop, min_status='PRELIMINARY') → {len(views)} proposition(s)")
for v in views:
    show("status", v["status"])
    show("independent support lines", v["independent_support"])
    show("frame contest", v["frame_status"])

print()
print("  The bearing is a function of the registered rule and the realised")
print("  numbers, so a refutation cannot be relabelled as support. Status is")
print("  a count over independent data, not a self-declared label.")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

graph.close()
print(f"\n{'─' * 60}")
print("  Done. Graph written to:", tmp / ".mareforma" / "graph.db")
