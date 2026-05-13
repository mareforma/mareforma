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
  Different agents, shared upstream → REPLICATED fires automatically.

  Agent B (Synthesizer) queries the graph before asserting anything.
  Finds the REPLICATED findings, builds a DERIVED synthesis on top.
  Asserts only what the graph already supports.

  The result: Agent B's conclusion is traceable to raw data.
  Knowledge accumulates instead of evaporating between agent runs.

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


# ---------------------------------------------------------------------------
# Mareforma tools via get_tools() — one set per agent, generated_by baked in
# ---------------------------------------------------------------------------

# Agent A: Lab A analyst
query_graph, assert_finding_a = [tool(fn) for fn in graph.get_tools(
    generated_by="analyst/model-a/lab_a"
)]

# Agent B Lab B analyst uses the same query tool, different assert identity
_, assert_finding_b = [tool(fn) for fn in graph.get_tools(
    generated_by="analyst/model-b/lab_b"
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

# Run 1: Lab A, dataset alpha
finding_a = assert_finding_a.invoke({
    "text": "Cell type A forms the majority of inhibitory connections onto cell type B"
            " (dataset_alpha, n=842, p<0.001)",
    "classification": "ANALYTICAL",
    "supports": [prior_ref],
    "source": "dataset_alpha",
})
c_a = graph.get_claim(finding_a)
show("lab_a claim_id", finding_a[:8] + "…")
show("lab_a support_level", c_a["support_level"] if c_a else "—")

# Run 2: Lab B, independent dataset beta
# Same upstream reference, different agent, different source → REPLICATED fires
finding_b = assert_finding_b.invoke({
    "text": "Cell type A dominates inhibitory input onto cell type B"
            " (dataset_beta, n=1104, p<0.001)",
    "classification": "ANALYTICAL",
    "supports": [prior_ref],
    "source": "dataset_beta",
})
c_b = graph.get_claim(finding_b)
show("lab_b claim_id", finding_b[:8] + "…")
show("lab_b support_level", c_b["support_level"] if c_b else "—")

print()
print("  Two independent agents, shared upstream → REPLICATED fires automatically.")


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
print("  prior reference → ANALYTICAL (×2, independent) → REPLICATED → DERIVED")
print()
print("  Without querying the graph, Agent B would have asserted from scratch.")
print("  The graph is what makes findings compound instead of evaporate.")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

graph.close()
print(f"\n{'─' * 60}")
print("  Done. Graph written to:", tmp / ".mareforma" / "graph.db")
