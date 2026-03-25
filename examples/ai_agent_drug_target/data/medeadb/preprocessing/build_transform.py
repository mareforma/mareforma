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

Requirements
------------
    pip install git+https://github.com/mims-harvard/MEDEA
    pip install mareforma

    huggingface-cli download mims-harvard/MedeaDB --repo-type dataset --local-dir data/medeadb/raw/

    # .env in this directory:
    OPENAI_API_KEY=your-key
    MEDEADB_PATH=data/medeadb/raw/MedeaDB
"""

from __future__ import annotations

import os
from dotenv import load_dotenv # type: ignore

from mareforma import transform  # type: ignore — install: pip install mareforma

load_dotenv()

# ---------------------------------------------------------------------------
# Config — change model and panelists here, not inside transforms
# ---------------------------------------------------------------------------

LLM_NAME   = os.getenv("MEDEA_LLM", "gpt-4o")
PANELISTS  = [
    "gpt-4o",
    "gpt-4o-mini",
    "o3-mini",
]
TEMPERATURE    = 0.0   # 0.0 for maximum reproducibility
DEBATE_ROUNDS  = 2
TIMEOUT        = 800   # seconds per medea() call

QUERY_RA_CD4  = (
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


# ---------------------------------------------------------------------------
# Fork A — Rheumatoid Arthritis / CD4+ T cells
# ---------------------------------------------------------------------------

@transform("ra_cd4.medea_run")
def ra_cd4_run(ctx):
    """Run the full MEDEA pipeline on RA/CD4+ T cells.

    Saves all intermediate MEDEA outputs so cross-diff can compare them
    artifact-by-artifact against the SLE fork.
    """
    from medea import medea, ResearchPlanning, Analysis, LiteratureReasoning  # type: ignore

    llm = _make_llm()
    result = medea(
        user_instruction=QUERY_RA_CD4,
        research_planning_module=ResearchPlanning(llm),
        analysis_module=Analysis(llm),
        literature_module=LiteratureReasoning(llm),
        panelist_llms=PANELISTS,
        vote_merge=True,
        debate_rounds=DEBATE_ROUNDS,
        timeout=TIMEOUT,
    )

    # Research plan — what MEDEA decided to investigate
    ctx.save("proposal", result.get("P"), fmt="json")

    # The Python code MEDEA generated to query MedeaDB, and its output.
    # This is the key artifact: if this is IDENTICAL in the SLE fork,
    # MEDEA ran the same query for two different diseases — a red flag.
    pa = result.get("PA") or {}
    ctx.save("generated_code",  pa.get("code_snippet"),    fmt="json")
    ctx.save("executed_output", pa.get("executed_output"), fmt="json")

    # Individual panelist votes before vote_merge collapsed them.
    # Lets you see whether the panel agreed or was split.
    ctx.save("panelist_votes",   result.get("llm"),   fmt="json")
    ctx.save("final_hypothesis", result.get("final"), fmt="json")

    final = result.get("final") or "No hypothesis produced"
    ctx.claim(
        text=final,
        confidence="exploratory",
        source_name="medeadb",
        generated_by=f"medea/{LLM_NAME}",
        generation_method="agent-wrapped",
    )


# ---------------------------------------------------------------------------
# Fork B — Systemic Lupus Erythematosus / CD4+ T cells
#           One variable changed: disease (RA → SLE)
# ---------------------------------------------------------------------------

@transform("sle_cd4.medea_run")
def sle_cd4_run(ctx):
    """Run the full MEDEA pipeline on SLE/CD4+ T cells.

    Identical configuration to ra_cd4.medea_run — only the disease query
    changes. Use cross-diff to see where the two runs diverge.
    """
    from medea import medea, ResearchPlanning, Analysis, LiteratureReasoning  # type: ignore

    llm = _make_llm()
    result = medea(
        user_instruction=QUERY_SLE_CD4,
        research_planning_module=ResearchPlanning(llm),
        analysis_module=Analysis(llm),
        literature_module=LiteratureReasoning(llm),
        panelist_llms=PANELISTS,
        vote_merge=True,
        debate_rounds=DEBATE_ROUNDS,
        timeout=TIMEOUT,
    )

    ctx.save("proposal",         result.get("P"),                fmt="json")
    pa = result.get("PA") or {}
    ctx.save("generated_code",   pa.get("code_snippet"),        fmt="json")
    ctx.save("executed_output",  pa.get("executed_output"),     fmt="json")
    ctx.save("panelist_votes",   result.get("llm"),             fmt="json")
    ctx.save("final_hypothesis", result.get("final"),           fmt="json")

    final = result.get("final") or "No hypothesis produced"
    ctx.claim(
        text=final,
        confidence="exploratory",
        source_name="medeadb",
        generated_by=f"medea/{LLM_NAME}",
        generation_method="agent-wrapped",
    )
