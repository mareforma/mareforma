"""Independence counted by distinct model/method, not signer + data alone.

A corroboration counts only when the two lines differ in model/method as well as
signer and dataset. Two same-model checks (distinct signer, distinct data) are a
single line of evidence, not independent corroboration, so they stay below
CORROBORATED. Where the model lineage is soft (PROXY / UNVERIFIABLE) on either
side, the pair is UNVERIFIABLE for independence — never a silent pass. A finding
with no observed model call carries no model constraint, so the pre-observer /
legacy behaviour (distinct signer + data corroborate) is unchanged.

The trust map surfaces the same axis as an effective-independence number, with an
explicit UNVERIFIABLE state where the lineage is soft.
"""
from __future__ import annotations

from pathlib import Path

import mareforma
from mareforma.observe import GroundingVerdict, ObservedGrounding
from mareforma.observe._lineage import resolve_lineage
from mareforma.trust._store import effective_independence
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


def _verdict(model_id: str, *, source: str = "socket") -> GroundingVerdict:
    """A grounding verdict carrying a model lineage of the requested tier.

    ``source="socket"`` earns COMPUTED (a body-parse at the seam), ``"declared"``
    earns PROXY; a fine-tune / alias string is UNVERIFIABLE regardless. The
    verdict itself is OPAQUE (the finding path only reads ``model_lineage`` off
    it); grounding state is irrelevant to the independence count.
    """
    lineage = resolve_lineage(
        model_id, source=source, method="m",
        decoding={"temperature": None, "top_p": None, "seed": None},
    )
    return GroundingVerdict(
        grounding=ObservedGrounding.OPAQUE,
        reason="test lineage",
        model_lineage=lineage,
    )


_CLAUDE = "claude-3-5-sonnet-20241022"   # COMPUTED root: claude-3-5-sonnet
_GPT = "gpt-4o-2024-08-06"               # COMPUTED root: gpt-4o
_FINETUNE = "ft:gpt-4o-2024-08-06:acme::rExAbC12"  # UNVERIFIABLE (no base)


# ---------------------------------------------------------------------------
# Trust core (red-first)
# ---------------------------------------------------------------------------

class TestModelDistinctness:
    def test_same_model_does_not_promote(self, tmp_path: Path) -> None:
        """Two checks — distinct signer, distinct data, SAME COMPUTED model —
        are one line of evidence, not two: they stay below CORROBORATED."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_CLAUDE),
            )
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 1
        assert status["status"] == "PRELIMINARY"

    def test_distinct_model_promotes(self, tmp_path: Path) -> None:
        """Distinct signer + distinct data + distinct COMPUTED model still
        corroborate — the model axis does not block a genuinely different one."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_GPT),
            )
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 2
        assert status["status"] == "CORROBORATED"

    def test_soft_lineage_is_unverifiable(self, tmp_path: Path) -> None:
        """A PROXY (declared) lineage on one side makes the pair UNVERIFIABLE
        for independence: it does not silently corroborate."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_GPT, source="declared"),  # PROXY, soft
            )
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 1
        assert status["status"] == "PRELIMINARY"

    def test_finetune_string_is_not_a_distinct_model(self, tmp_path: Path) -> None:
        """A distinct model STRING whose base is not declarable is UNVERIFIABLE,
        not a counted distinct model — so it cannot manufacture corroboration."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_FINETUNE),  # UNVERIFIABLE
            )
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 1
        assert status["status"] == "PRELIMINARY"

    def test_absent_lineage_keeps_legacy_corroboration(self, tmp_path: Path) -> None:
        """A finding with no observed model call carries no model constraint:
        two distinct-signer, distinct-data findings corroborate as before."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1")
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds2", generated_by="run2")
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 2
        assert status["status"] == "CORROBORATED"


# ---------------------------------------------------------------------------
# Trust map: the effective-independence number
# ---------------------------------------------------------------------------

class TestEffectiveIndependenceNumber:
    def test_effective_independence_number(self, tmp_path: Path) -> None:
        """N pairwise-distinct (model, data, signer) checks → effective N;
        same-model duplicates do not inflate it; soft lineage surfaces as
        UNVERIFIABLE on the trust map's independence axis."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        kc = _bootstrap_key(tmp_path, "kc.key")
        prop, pred = _prop(), _pred()
        cid = prop.content_id()

        # Two distinct COMPUTED models, distinct signers + data → effective 2.
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            r = g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_GPT),
            )
            eff = effective_independence(g._conn, cid)
            assert eff["number"] == 2
            assert eff["soft"] is False
            # The trust map surfaces the number on the independence axis.
            tmap = g.trust_map(r["claim_id"])
            assert tmap.get("independence").value == "2"

        # A third check on the SAME model as an existing one does not inflate.
        with mareforma.open(tmp_path, key_path=kc) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds3", generated_by="run3",
                grounding=_verdict(_CLAUDE),  # same root as ds1
            )
            assert effective_independence(g._conn, cid)["number"] == 2

    def test_soft_lineage_surfaces_unverifiable_on_map(self, tmp_path: Path) -> None:
        """When a supporting line's lineage is soft, the map's independence axis
        reads UNVERIFIABLE rather than a confident number."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            r = g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_GPT, source="declared"),  # PROXY, soft
            )
            eff = effective_independence(g._conn, prop.content_id())
            assert eff["soft"] is True
            assert eff["number"] == 1
            tmap = g.trust_map(r["claim_id"])
            assert tmap.get("independence").value == "UNVERIFIABLE"
