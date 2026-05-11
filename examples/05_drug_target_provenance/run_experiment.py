"""
run_experiment.py — Run both MEDEA forks and record findings via mareforma.

Runs inside medea_env. Called by 05_drug_target_provenance.py --run.

Two forks:
  ra_cd4   Rheumatoid Arthritis  / CD4+ T cells
  sle_cd4  Systemic Lupus Erythematosus / CD4+ T cells

Same model, same panelists, same debate rounds — one variable changed (the disease).
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).parent.resolve()

try:
    import mareforma
except ImportError:
    print("mareforma not found in this environment. Run --install first.")
    sys.exit(1)

try:
    from medea.agent.medea import MEDEA
except ImportError:
    print("MEDEA not found. Run --install first.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_medea_fork(disease: str, cell_type: str) -> dict:
    """Run one MEDEA fork and return its output dict.

    Returns dict with keys:
      final_hypothesis : str  — the final drug target text
      generated_code   : str | None — the data query code MEDEA generated
      executed_output  : str | None — the actual output of running that code
    """
    agent = MEDEA(disease=disease, cell_type=cell_type)
    result = agent.run()
    return {
        "final_hypothesis": result.get("final_hypothesis", ""),
        "generated_code":   result.get("generated_code"),
        "executed_output":  result.get("executed_output"),
    }


def _classify(result: dict) -> str:
    """Return 'ANALYTICAL' if MEDEA's data pipeline ran, else 'INFERRED'."""
    return "ANALYTICAL" if result.get("generated_code") else "INFERRED"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    with mareforma.open(HERE) as graph:
        # -------------------------------------------------------------------
        # Query-before-assert: check for prior REPLICATED findings
        # -------------------------------------------------------------------

        prior = graph.query("drug target", min_support="REPLICATED")
        if prior:
            print(f"\nFound {len(prior)} prior REPLICATED finding(s) — MEDEA will build on them.")
        else:
            print("\nNo prior REPLICATED findings — running both forks fresh.")

        # -------------------------------------------------------------------
        # Fork 1: Rheumatoid Arthritis, CD4+ T cells
        # -------------------------------------------------------------------

        print("\n[1/2] Running MEDEA — Rheumatoid Arthritis / CD4+ T cells ...")
        ra_result = _run_medea_fork(disease="rheumatoid arthritis", cell_type="CD4")
        ra_classification = _classify(ra_result)
        print(f"  Classification: {ra_classification}")
        print(f"  Finding: {ra_result['final_hypothesis'][:120]}")

        ra_claim_id = graph.assert_claim(
            ra_result["final_hypothesis"],
            classification=ra_classification,
            generated_by="medea/gpt-4o/ra_cd4",
            source_name="medeadb",
        )
        print(f"  Recorded claim: {ra_claim_id}")

        # -------------------------------------------------------------------
        # Fork 2: Systemic Lupus Erythematosus, CD4+ T cells
        # -------------------------------------------------------------------

        print("\n[2/2] Running MEDEA — Systemic Lupus Erythematosus / CD4+ T cells ...")
        sle_result = _run_medea_fork(disease="systemic lupus erythematosus", cell_type="CD4")
        sle_classification = _classify(sle_result)
        print(f"  Classification: {sle_classification}")
        print(f"  Finding: {sle_result['final_hypothesis'][:120]}")

        sle_claim_id = graph.assert_claim(
            sle_result["final_hypothesis"],
            classification=sle_classification,
            generated_by="medea/gpt-4o/sle_cd4",
            source_name="medeadb",
        )
        print(f"  Recorded claim: {sle_claim_id}")

        # -------------------------------------------------------------------
        # Epistemic status report
        # -------------------------------------------------------------------

        ra_claim  = graph.get_claim(ra_claim_id)
        sle_claim = graph.get_claim(sle_claim_id)

        print("\n" + "=" * 60)
        print("EPISTEMIC STATUS")
        print("=" * 60)
        print(f"  RA fork:   {ra_classification:10}  →  {ra_claim['support_level']}")
        print(f"  SLE fork:  {sle_classification:10}  →  {sle_claim['support_level']}")

        if ra_result["generated_code"] is None or sle_result["generated_code"] is None:
            print("\n  ⚠  One or both forks returned null generated_code.")
            print("     Both findings are INFERRED — the data pipeline did not run.")
            print("     This was Case B in the original run. See the README for context.")
        else:
            print("\n  ✓  Both forks ran the data pipeline (ANALYTICAL).")
            print("     If the findings converge on the same upstream target,")
            print("     REPLICATED fires automatically.")

    print(f"\nClaims written to: {HERE / 'claims.toml'}")
    print("Run 'mareforma status' for the full epistemic dashboard.")


if __name__ == "__main__":
    main()
