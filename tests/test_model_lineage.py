"""Model/method lineage recorded on the evidence line, tiered like data_id.

The lineage captured in an ``observe()`` scope rides the grounding verdict into
``assert_finding`` / ``submit_finding``, is written to the additive
``evidence_lines.model_lineage`` column, and is COMPUTED / PROXY / UNVERIFIABLE
exactly as the observer tiered it. A finding authored without a model call leaves
the column NULL, byte-identical to a pre-observer finding.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import mareforma
import mareforma.observe as mobs
from mareforma.observe import declare_model
from tests._helpers import _bootstrap_key


def _prop():
    from mareforma.trust import Direction, Proposition

    return Proposition(
        subject="BRCA1", relation="affects", object="tumour growth",
        direction=Direction.DECREASES,
        scope={"population": "TNBC", "condition": "in vitro"},
    )


def _pred():
    from mareforma.trust import DirectionOfInterest, Prediction, TestType

    return Prediction(
        TestType.SUPERIORITY,
        direction_of_interest=DirectionOfInterest.DECREASE,
        alpha=0.05,
    )


def _est():
    from mareforma.trust import EffectEstimate, EffectType

    return EffectEstimate(-0.8, EffectType.SMD, p_value=0.001)


def _lineage_rows(tmp_path: Path) -> list[str | None]:
    conn = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
    try:
        return [
            r[0]
            for r in conn.execute(
                "SELECT model_lineage FROM evidence_lines"
            ).fetchall()
        ]
    finally:
        conn.close()


def test_model_lineage_persisted_on_evidence_line(tmp_path: Path) -> None:
    key = _bootstrap_key(tmp_path, "root.key")
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=key) as g:
        with mobs.observe() as h:
            declare_model("gpt-4o-2024-08-06", method="agent-sdk", temperature=0.2)
        result = g.assert_finding(
            prop, pred, _est(), data_id="ds1", generated_by="run1",
            grounding=h.verdict,
        )
    assert result["model_lineage"]["tier"] == "PROXY"
    assert result["model_lineage"]["family_root"] == "gpt-4o"

    (stored,) = _lineage_rows(tmp_path)
    assert stored is not None
    parsed = json.loads(stored)
    assert parsed["tier"] == "PROXY"
    assert parsed["model_id"] == "gpt-4o-2024-08-06"
    assert parsed["family_root"] == "gpt-4o"


def test_finding_without_model_leaves_lineage_null(tmp_path: Path) -> None:
    key = _bootstrap_key(tmp_path, "root.key")
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=key) as g:
        with mobs.observe() as h:
            _ = 2 + 2  # no model call authored the finding
        result = g.assert_finding(
            prop, pred, _est(), data_id="ds1", generated_by="run1",
            grounding=h.verdict,
        )
    assert result["model_lineage"] is None
    (stored,) = _lineage_rows(tmp_path)
    assert stored is None


def test_finetune_lineage_persisted_unverifiable(tmp_path: Path) -> None:
    key = _bootstrap_key(tmp_path, "root.key")
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=key) as g:
        with mobs.observe() as h:
            declare_model("ft:gpt-4o-2024-08-06:acme::rExAbC12")
        result = g.assert_finding(
            prop, pred, _est(), data_id="ds1", generated_by="run1",
            grounding=h.verdict,
        )
    assert result["model_lineage"]["tier"] == "UNVERIFIABLE"
    assert result["model_lineage"]["family_root"] is None
    (stored,) = _lineage_rows(tmp_path)
    assert json.loads(stored)["tier"] == "UNVERIFIABLE"
