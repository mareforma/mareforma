"""
examples/02_compounding_agents.py — How agent findings compound.

Run:
    python examples/02_compounding_agents.py

No API key required. No external dependencies beyond mareforma.

Story
-----
Two agents work sequentially on the same research question.

  Agent A (Analyst) runs on two independent connectomics datasets.
  Both analyses cite the same upstream evidence.
  Different agents, shared upstream → REPLICATED fires automatically.

  Agent B (Synthesizer) queries the graph before asserting anything.
  Finds the REPLICATED findings, builds a DERIVED synthesis on top.
  Asserts only what the graph already supports.

  The result: Agent B's conclusion is traceable to raw data.
  Knowledge accumulates instead of evaporating between agent runs.

LangChain integration
---------------------
The agents below are plain Python to keep this example self-contained.
In a real deployment, each EpistemicAgent method becomes a @tool and the
class is driven by a LangChain AgentExecutor. The graph interaction is identical.
"""

import tempfile
from pathlib import Path

import mareforma


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
# Agent interface
# The two methods an agent needs. In a LangChain agent these become @tools.
# ---------------------------------------------------------------------------

class EpistemicAgent:
    def __init__(self, name: str) -> None:
        self.name = name

    def query(self, text: str, min_support: str = "PRELIMINARY") -> list[dict]:
        """Query the graph before asserting — standard agent pattern."""
        return graph.query(text, min_support=min_support)

    def assert_finding(
        self,
        text: str,
        classification: str = "INFERRED",
        supports: list[str] | None = None,
        source_name: str | None = None,
    ) -> str:
        return graph.assert_claim(
            text,
            classification=classification,
            generated_by=self.name,
            supports=supports or [],
            source_name=source_name,
        )


# ---------------------------------------------------------------------------
# Agent A — Analyst
# Runs on two independent datasets. Both cite the same prior literature.
# ---------------------------------------------------------------------------

sep("Agent A — Analyst (two independent runs)")

agent_a1 = EpistemicAgent("analyst/model-a/lab_a")
agent_a2 = EpistemicAgent("analyst/model-b/lab_b")

# Shared upstream anchor — a prior claim both analyses build on.
# In a real project this would be a claim_id already in the graph, 
# or a DOI from the actual literature.
prior_ref = "upstream_finding_X"

# Run 1: Lab A, dataset alpha
finding_a = agent_a1.assert_finding(
    "Cell type A forms the majority of inhibitory connections onto cell type B"
    " (dataset_alpha, n=842, p<0.001)",
    classification="ANALYTICAL",
    supports=[prior_ref],
    source_name="dataset_alpha",
)
c_a = graph.get_claim(finding_a)
show("lab_a claim_id", finding_a[:8] + "…")
show("lab_a support_level", c_a["support_level"] if c_a else "—")

# Run 2: Lab B, independent dataset beta
# Same upstream reference, different agent, different source → REPLICATED fires
finding_b = agent_a2.assert_finding(
    "Cell type A dominates inhibitory input onto cell type B"
    " (dataset_beta, n=1104, p<0.001)",
    classification="ANALYTICAL",
    supports=[prior_ref],
    source_name="dataset_beta",
)
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

agent_b = EpistemicAgent("synthesizer/model-c/lab_b")

# Step 1: query before asserting — the standard agent pattern
existing = agent_b.query("cell type A", min_support="REPLICATED")
print(f"  graph.query('cell type A', min_support='REPLICATED') → {len(existing)} claims")
for c in existing:
    print(f"    [{c['support_level']:12}] {c['text'][:65]}…")

# Step 2: build on what the graph already supports
replicated_ids = [c["claim_id"] for c in existing]

synthesis = agent_b.assert_finding(
    "Inhibitory dominance of cell type A over cell type B is a replicated finding"
    " across independent datasets and consistent with prior literature",
    classification="DERIVED",          # explicitly built on REPLICATED graph claims
    supports=replicated_ids,
)

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
print("  prior literature → ANALYTICAL (×2, independent) → REPLICATED → DERIVED")
print()
print("  Without querying the graph, Agent B would have asserted from scratch.")
print("  The graph is what makes findings compound instead of evaporate.")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

graph.close()
print(f"{'─' * 60}")
print("  Done. Graph written to:", tmp / ".mareforma" / "graph.db")
