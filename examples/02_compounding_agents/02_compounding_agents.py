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
The mareforma primitives are defined as real @tools below.
The agent control flow is explicit Python — simulating what an LLM would decide.
To drive these tools with a real LLM, replace the control flow with:

    from langchain_openai import ChatOpenAI
    from langgraph.prebuilt import create_react_agent
    agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools=[query_graph, assert_finding])
    agent.invoke({"messages": [HumanMessage("Synthesise findings about cell type A.")]})
"""

import tempfile
from pathlib import Path

import mareforma
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
graph = mareforma.open(tmp)


# ---------------------------------------------------------------------------
# Mareforma as LangChain tools
# These are real @tools — schema, name, and description are exposed to any LLM.
# ---------------------------------------------------------------------------

@tool
def query_graph(text: str, min_support: str = "PRELIMINARY") -> list[dict]:
    """Query the epistemic graph for existing findings.

    Returns claims matching the text filter, ordered by support level.
    min_support: PRELIMINARY | REPLICATED | ESTABLISHED
    """
    results = graph.query(text, min_support=min_support)
    return [
        {
            "claim_id": r["claim_id"],
            "text": r["text"],
            "support_level": r["support_level"],
            "classification": r["classification"],
        }
        for r in results
    ]


@tool
def assert_finding(
    text: str,
    classification: str,
    generated_by: str,
    supports: list[str],
    source_name: str | None = None,
) -> str:
    """Assert a scientific finding into the epistemic graph. Returns claim_id.

    classification: INFERRED | ANALYTICAL | DERIVED
    supports: list of upstream claim_ids or reference strings
    source_name: data source this finding was derived from, or None
    """
    return graph.assert_claim(
        text,
        classification=classification,
        generated_by=generated_by,
        supports=supports,
        source_name=source_name,
    )


# ---------------------------------------------------------------------------
# Agent A — Analyst (two independent runs)
# Explicit control flow simulates what the LLM would decide to do.
# ---------------------------------------------------------------------------

sep("Agent A — Analyst (two independent runs)")

# Shared upstream anchor — a prior claim both analyses build on.
# In a real project this would be a claim_id already in the graph,
# or a reference from the actual literature.
prior_ref = "upstream_finding_X"

# Run 1: Lab A, dataset alpha
finding_a = assert_finding.invoke({
    "text": "Cell type A forms the majority of inhibitory connections onto cell type B"
            " (dataset_alpha, n=842, p<0.001)",
    "classification": "ANALYTICAL",
    "generated_by": "analyst/model-a/lab_a",
    "supports": [prior_ref],
    "source_name": "dataset_alpha",
})
c_a = graph.get_claim(finding_a)
show("lab_a claim_id", finding_a[:8] + "…")
show("lab_a support_level", c_a["support_level"] if c_a else "—")

# Run 2: Lab B, independent dataset beta
# Same upstream reference, different agent, different source → REPLICATED fires
finding_b = assert_finding.invoke({
    "text": "Cell type A dominates inhibitory input onto cell type B"
            " (dataset_beta, n=1104, p<0.001)",
    "classification": "ANALYTICAL",
    "generated_by": "analyst/model-b/lab_b",
    "supports": [prior_ref],
    "source_name": "dataset_beta",
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
existing = query_graph.invoke({"text": "cell type A", "min_support": "REPLICATED"})
print(f"  query_graph('cell type A', min_support='REPLICATED') → {len(existing)} claims")
for c in existing:
    print(f"    [{c['support_level']:12}] {c['text'][:65]}…")

# Step 2: build on what the graph already supports
replicated_ids = [c["claim_id"] for c in existing]

synthesis = assert_finding.invoke({
    "text": "Inhibitory dominance of cell type A over cell type B is a replicated finding"
            " across independent datasets and consistent with prior literature",
    "classification": "DERIVED",
    "generated_by": "synthesizer/model-c/lab_b",
    "supports": replicated_ids,
    "source_name": None,
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
