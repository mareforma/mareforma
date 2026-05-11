"""
examples/03_documented_contestation.py — Documented contestation.

Run:
    pip install langchain-core
    python examples/03_documented_contestation.py

No API key required.

Story
-----
An ESTABLISHED finding sits in the graph — two independent agents converged
on it and a human validated it.

A new agent runs a larger analysis on a different cohort and gets a result
in tension with the established consensus. The agent does not discard its
finding. It asserts it explicitly, naming the tension with contradicts=.

Both claims coexist in the graph:
  - the ESTABLISHED consensus
  - the new ANALYTICAL challenge, with its own provenance

This is how science actually works. The graph captures the debate,
not just the winning side. A documented contradiction is more valuable
than silence.

LangChain integration
---------------------
graph.get_tools(generated_by="...") returns [query_graph, assert_finding] as plain
callables. Wrap with @tool for LangChain. generated_by is baked into the closure.

    from langchain_openai import ChatOpenAI
    from langgraph.prebuilt import create_react_agent
    from langchain_core.tools import tool as lc_tool

    lc_tools = [lc_tool(fn) for fn in graph.get_tools(generated_by="agent/gpt-4o/lab_c")]
    agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools=lc_tools)
    agent.invoke({"messages": [HumanMessage("Analyse cohort_3 and record your finding.")]})
"""

import json
import tempfile
from pathlib import Path

import mareforma
from langchain_core.tools import tool


def sep(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def show(label: str, value: object) -> None:
    print(f"  {label:<28} {value}")


tmp = Path(tempfile.mkdtemp())
graph = mareforma.open(tmp)


# ---------------------------------------------------------------------------
# Mareforma tools via get_tools() — one set per agent, generated_by baked in
# ---------------------------------------------------------------------------

query_graph, assert_finding_a = [tool(fn) for fn in graph.get_tools(
    generated_by="agent_lab_a/model-a"
)]
_, assert_finding_b = [tool(fn) for fn in graph.get_tools(
    generated_by="agent_lab_b/model-b"
)]
_, assert_finding_c = [tool(fn) for fn in graph.get_tools(
    generated_by="agent_lab_c/model-c"
)]


# ---------------------------------------------------------------------------
# Setup — establish the prior consensus
# Two independent agents converge, human validates → ESTABLISHED
# ---------------------------------------------------------------------------

sep("Setup — prior consensus (ESTABLISHED)")

upstream_ref = "upstream_ref_A"

consensus_a = assert_finding_a.invoke({
    "text": "Treatment X reduces outcome Y in population P (cohort_1, n=500, p=0.003)",
    "classification": "ANALYTICAL",
    "supports": [upstream_ref],
    "source": "dataset_alpha",
})

consensus_b = assert_finding_b.invoke({
    "text": "Treatment X reduces outcome Y in population P (cohort_2, n=480, p=0.011)",
    "classification": "ANALYTICAL",
    "supports": [upstream_ref],
    "source": "dataset_beta",
})

c_a = graph.get_claim(consensus_a)
show("consensus_a support_level", c_a["support_level"] if c_a else "—")

graph.validate(consensus_a, validated_by="reviewer@lab.org")
established = graph.get_claim(consensus_a)
show("after validate()", established["support_level"] if established else "—")


# ---------------------------------------------------------------------------
# New agent — larger analysis, different result
# ---------------------------------------------------------------------------

sep("New agent — larger analysis, different result")

# Step 1: query the graph — what is already established on this topic?
prior = json.loads(query_graph.invoke({"topic": "Treatment X", "min_support": "ESTABLISHED"}))
print(f"  query_graph('Treatment X', min_support='ESTABLISHED') → {len(prior)} claims")
for c in prior:
    print(f"    [{c['support_level']:12}] {c['text'][:65]}…")

established_ids = [c["claim_id"] for c in prior]

print()
print("  Prior consensus found. Running analysis on new cohort (n=1,240)…")
print()

# Step 2: analysis returns a different result — no significant effect.
# The agent does not discard this. It asserts it with contradicts= pointing
# to the established consensus, and documents the methodological difference.
challenge = assert_finding_c.invoke({
    "text": "Treatment X shows no significant effect on outcome Y in population P"
            " (cohort_3, n=1240, p=0.21) — larger and more diverse cohort than prior studies",
    "classification": "ANALYTICAL",
    "supports": ["upstream_ref_B"],
    "contradicts": established_ids,
    "source": "dataset_gamma",
})

c_challenge = graph.get_claim(challenge)
show("challenge claim_id", challenge[:8] + "…")
show("challenge support_level", c_challenge["support_level"] if c_challenge else "—")
show("challenge classification", c_challenge["classification"] if c_challenge else "—")

contradicts_list = json.loads(c_challenge["contradicts_json"] if c_challenge else "[]")
show("contradicts", f"{len(contradicts_list)} established claim(s)")


# ---------------------------------------------------------------------------
# Graph state — both claims coexist
# ---------------------------------------------------------------------------

sep("Graph state — consensus and challenge coexist")

all_claims = graph.query()
print(f"  Total claims in graph: {len(all_claims)}\n")

level_order = {"ESTABLISHED": 0, "REPLICATED": 1, "PRELIMINARY": 2}
for c in sorted(all_claims, key=lambda x: level_order.get(x["support_level"], 3)):
    contradicts_flag = " ← contradicts ESTABLISHED" if json.loads(
        c.get("contradicts_json", "[]") or "[]"
    ) else ""
    label = f"[{c['support_level']:12}] [{c['classification']:10}]"
    print(f"  {label}  {c['text'][:50]}…{contradicts_flag}")

print()
print("  The ESTABLISHED finding is not overwritten.")
print("  The challenge is not discarded.")
print("  Both are in the graph with full provenance.")
print()
print("  A human reviewer can now:")
print("    query_graph('Treatment X')                            — see both sides")
print("    query_graph('Treatment X', min_support='ESTABLISHED') — see only validated consensus")
print("    graph.get_claim(challenge_id)['contradicts_json']     — trace the stated tension")


# ---------------------------------------------------------------------------
# What NOT to do
# ---------------------------------------------------------------------------

sep("What NOT to do")

print("""
  ✗  Asserting the challenge without contradicts=

       assert_finding_c.invoke({"text": "Treatment X has no effect ...",
                                "contradicts": None, ...})

     The tension is invisible. The graph looks like two unrelated claims.
     A future agent querying 'Treatment X' gets contradictory signals
     with no structure to reason about them.

  ✗  Discarding the finding because the consensus is ESTABLISHED

     ESTABLISHED means human-validated evidence — not settled truth.
     A larger, better-powered study is legitimate scientific progress.
     Silence is not.

  ✓  The correct pattern:

       assert_finding_c.invoke({
           "text": "...",
           "classification": "ANALYTICAL",
           "contradicts": [established_id],   # name the tension
           "supports": [new_upstream_ref],    # ground the provenance
           "source": "dataset_gamma",
       })
""")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

graph.close()
print(f"{'─' * 60}")
print("  Done. Graph written to:", tmp / ".mareforma" / "graph.db")
