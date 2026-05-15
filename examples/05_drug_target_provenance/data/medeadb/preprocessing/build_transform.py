"""
build_transform.py — mareforma pipeline for AI-driven drug target identification.

Two MEDEA runs, one variable changed between them:
  ra_cd4  : Rheumatoid Arthritis, CD4+ T cells  (the original finding)
  sle_cd4 : Systemic Lupus Erythematosus, CD4+  (fork: swap disease)

After both runs complete:

    mareforma cross-diff ra_cd4.medea_run sle_cd4.medea_run

The cross-diff shows whether MEDEA generated different database queries
for the two diseases (generated_code: CHANGED) or ran the same query
regardless of disease (generated_code: SAME — a red flag).

Epistemic status
----------------
Each claim is classified at assertion time based on whether MEDEA's data
pipeline actually ran:

  ANALYTICAL — generated_code is not null: MEDEA queried MedeaDB and the
               finding is grounded in omics data.

  INFERRED   — generated_code is null: the data pipeline failed silently.
               The hypothesis came from LLM prior knowledge only. The graph
               records this honestly — not as ANALYTICAL.

Requirements
------------
    python 05_drug_target_provenance.py --install
    python 05_drug_target_provenance.py --data

    # .env in this directory:
    OPENAI_API_KEY=your-key
    MEDEADB_PATH=data/medeadb/raw/MedeaDB
"""

from __future__ import annotations

import os
from dotenv import load_dotenv  # type: ignore

import mareforma
from mareforma import transform  # type: ignore

load_dotenv()

# ---------------------------------------------------------------------------
# Config — change model and panelists here, not inside transforms
# ---------------------------------------------------------------------------

LLM_NAME      = os.getenv("MEDEA_LLM", "gpt-4o")
PANELISTS     = ["gpt-4o", "gpt-4o-mini", "o3-mini"]
TEMPERATURE   = 0.0   # 0.0 for maximum reproducibility
DEBATE_ROUNDS = 2
TIMEOUT       = 800   # seconds per medea() call

QUERY_RA_CD4 = (
    "Which gene is the best therapeutic target for Rheumatoid Arthritis "
    "in CD4+ T cells (cd4_positive_helper_t_cell)? "
    "Use the available omics data to rank candidates."
)
QUERY_SLE_CD4 = (
    "Which gene is the best therapeutic target for Systemic Lupus Erythematosus "
    "in CD4+ T cells (cd4_positive_helper_t_cell)? "
    "Use the available omics data to rank candidates."
)


def _make_llm():
    from medea import AgentLLM, LLMConfig  # type: ignore
    return AgentLLM(LLMConfig({"temperature": TEMPERATURE}), llm_name=LLM_NAME)


def _run_medea_and_claim(ctx, query: str, disease: str) -> None:
    """Run MEDEA and assert a claim with honest epistemic classification."""
    from medea import medea, ResearchPlanning, Analysis, LiteratureReasoning  # type: ignore

    # Query-before-assert: check what is already established in the graph.
    # If a prior REPLICATED finding exists for this disease, MEDEA can build
    # on it rather than starting from scratch.
    with mareforma.open(ctx.root) as graph:
        prior = graph.query(disease, min_support="REPLICATED")
        if prior:
            ctx.log(f"  found {len(prior)} prior REPLICATED finding(s) for '{disease}'")
        prior_ids = [c["claim_id"] for c in prior]

    llm = _make_llm()
    result = medea(
        user_instruction=query,
        research_planning_module=ResearchPlanning(llm),
        analysis_module=Analysis(llm),
        literature_module=LiteratureReasoning(llm),
        panelist_llms=PANELISTS,
        vote_merge=True,
        debate_rounds=DEBATE_ROUNDS,
        timeout=TIMEOUT,
    )

    # Save all intermediate MEDEA outputs as artifacts.
    # generated_code is the key artifact: if it is null, the data pipeline
    # failed silently and the finding is INFERRED, not ANALYTICAL.
    ctx.save("proposal", result.get("P"), fmt="json")

    pa = result.get("PA") or {}
    generated_code  = pa.get("code_snippet")
    executed_output = pa.get("executed_output")

    ctx.save("generated_code",  generated_code,  fmt="json")
    ctx.save("executed_output", executed_output, fmt="json")
    ctx.save("panelist_votes",  result.get("llm"),   fmt="json")
    ctx.save("final_hypothesis", result.get("final"), fmt="json")

    # Epistemic classification: honest about whether data was actually used.
    # ANALYTICAL requires the data pipeline ran and returned output.
    # INFERRED is the correct label when generated_code is null.
    classification = "ANALYTICAL" if generated_code else "INFERRED"

    if not generated_code:
        ctx.log(
            "  [warning] generated_code is null — data pipeline did not run. "
            "Asserting as INFERRED (LLM prior knowledge only)."
        )

    final = result.get("final") or "No hypothesis produced"
    ctx.claim(
        text=final,
        classification=classification,
        source_name="medeadb",
        generated_by=f"medea/{LLM_NAME}",
        supports=prior_ids or None,
    )


# ---------------------------------------------------------------------------
# Fork A — Rheumatoid Arthritis / CD4+ T cells
# ---------------------------------------------------------------------------

@transform("ra_cd4.medea_run")
def ra_cd4_run(ctx):
    """Run the full MEDEA pipeline on RA/CD4+ T cells."""
    _run_medea_and_claim(ctx, QUERY_RA_CD4, "Rheumatoid Arthritis")


# ---------------------------------------------------------------------------
# Fork B — Systemic Lupus Erythematosus / CD4+ T cells
# ---------------------------------------------------------------------------

@transform("sle_cd4.medea_run")
def sle_cd4_run(ctx):
    """Run the full MEDEA pipeline on SLE/CD4+ T cells.

    Identical configuration to ra_cd4.medea_run — only the disease query
    changes. Use cross-diff to see where the two runs diverge.
    """
    _run_medea_and_claim(ctx, QUERY_SLE_CD4, "Systemic Lupus Erythematosus")
