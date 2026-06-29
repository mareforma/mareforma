"""
tests/epistemic/test_trust.py: trust-layer correctness tests.

The successor to test_trust_ladder.py: where that file documents the old
support-level ladder, this validates the count-based trust layer that replaces
it. The graph tests are epistemic-correctness tests in the same spirit as
test_support_levels.py (the derived signals must be honest under independent and
contradictory evidence); the rest are unit tests of the primitives that produce
those signals.

Scenarios covered
-----------------
  Proposition identity (the frozen kernel)
    - same truth conditions collapse to one content_id
    - cosmetic variation (case, whitespace, Unicode) collapses
    - direction and magnitude fork content_id but share frame_id
    - scope forks both ids
    - contradiction is decidable without a model
    - the falsifiability gate
  The gate (computed bearing)
    - superiority supports / refutes / neutral
    - CI significance requires a matching confidence level
    - equivalence (TOST) supports / refutes / neutral
    - null value derivation per effect type and scale
  Derived Status
    - the count-based state machine, every transition
    - the frame-level contest
  Input consistency
    - EffectEstimate and Prediction reject inconsistent input
  Success criteria (graph, end to end)
    - record a finding and build on it by frame, no human in the loop
    - two independent data lines reach CORROBORATED
    - the same dataset does not re-count
    - an opposite outcome refutes, then contests
    - opposite findings surface a frame contest
    - legacy free-text claims coexist on the old surface
"""
from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from mareforma.trust import (
    BearingDirection,
    Contrast,
    ControlType,
    Direction,
    DirectionOfInterest,
    EffectEstimate,
    EffectType,
    EvidenceLine,
    FrameStatus,
    InconsistentEstimateError,
    NonFalsifiablePropositionError,
    Prediction,
    Proposition,
    Scale,
    Status,
    TestType,
    compute_bearing,
    compute_frame_status,
    compute_status,
    null_value,
)


# ---------------------------------------------------------------------------

from tests.epistemic._builders import _prop, _smd, _superiority, open_graph


def _equivalence(lower: float = -0.1, upper: float = 0.1, alpha: float = 0.05) -> Prediction:
    return Prediction(
        TestType.EQUIVALENCE,
        equivalence_lower=lower,
        equivalence_upper=upper,
        alpha=alpha,
    )


# ---------------------------------------------------------------------------
# Proposition identity: the frozen kernel
# ---------------------------------------------------------------------------

class TestPropositionIdentity:
    def test_same_truth_conditions_collapse(self) -> None:
        assert _prop().content_id() == _prop().content_id()
        assert _prop().same_as(_prop())

    def test_cosmetic_variation_collapses(self) -> None:
        p1 = Proposition("BRCA1", "affects", "growth", Direction.DECREASES, {"pop": "TNBC"})
        p2 = Proposition("  brca1 ", "AFFECTS", " growth ", Direction.DECREASES, {"POP": "tnbc"})
        assert p1.content_id() == p2.content_id()

    def test_different_direction_shares_frame(self) -> None:
        up, down = _prop(Direction.INCREASES), _prop(Direction.DECREASES)
        assert up.content_id() != down.content_id()
        assert up.frame_id() == down.frame_id()

    def test_magnitude_participates_in_content_not_frame(self) -> None:
        a = Proposition("X", "affects", "Y", Direction.INCREASES, {"p": "P"}, magnitude="20%")
        b = Proposition("X", "affects", "Y", Direction.INCREASES, {"p": "P"}, magnitude="80%")
        none = Proposition("X", "affects", "Y", Direction.INCREASES, {"p": "P"})
        assert a.content_id() != b.content_id()
        assert b.content_id() != none.content_id()
        assert a.content_id() != none.content_id()
        assert a.frame_id() == b.frame_id() == none.frame_id()

    def test_different_scope_forks_both_ids(self) -> None:
        p1 = _prop(Direction.INCREASES, population="P1")
        p2 = _prop(Direction.INCREASES, population="P2")
        assert p1.content_id() != p2.content_id()
        assert p1.frame_id() != p2.frame_id()

    def test_content_id_is_deterministic_hex(self) -> None:
        cid = _prop().content_id()
        assert len(cid) == 64
        int(cid, 16)  # raises if not hex
        a = Proposition("X", "affects", "Y", Direction.INCREASES, {"a": "1", "b": "2"})
        b = Proposition("X", "affects", "Y", Direction.INCREASES, {"b": "2", "a": "1"})
        assert a.content_id() == b.content_id()  # dict order is irrelevant

    def test_contradiction_is_decidable(self) -> None:
        up, down = _prop(Direction.INCREASES), _prop(Direction.DECREASES)
        assert up.contradicts(down) and down.contradicts(up)
        assert not up.contradicts(_prop(Direction.INCREASES))  # self is not a contradiction
        assert not up.contradicts(_prop(Direction.DECREASES, population="OTHER"))  # diff frame
        assert not Direction.UNSPECIFIED.contradicts(Direction.INCREASES)

    def test_falsifiability_gate(self) -> None:
        assert _prop().is_falsifiable()
        assert not Proposition("X", "affects", "Y").is_falsifiable()
        assert not Proposition("X", "affects", "Y", Direction.INCREASES).is_falsifiable()
        assert not Proposition("X", "affects", "Y", scope={"p": "P"}).is_falsifiable()

    def test_scope_key_collision_rejected(self) -> None:
        bad = Proposition("X", "affects", "Y", Direction.INCREASES, {"Dose": "hi", "dose": "lo"})
        with pytest.raises(ValueError):
            bad.content_id()

    def test_empty_core_field_rejected(self) -> None:
        for bad in (("", "r", "o"), ("s", "  ", "o"), ("s", "r", "")):
            with pytest.raises(ValueError):
                Proposition(*bad, direction=Direction.INCREASES, scope={"p": "P"})


# ---------------------------------------------------------------------------
# null value derivation
# ---------------------------------------------------------------------------

class TestNullValue:
    def test_difference_and_log_are_zero(self) -> None:
        assert null_value(EffectType.SMD, Scale.RAW) == 0.0
        assert null_value(EffectType.MD, Scale.RAW) == 0.0
        assert null_value(EffectType.LOG2FC, Scale.RAW) == 0.0
        assert null_value(EffectType.OR, Scale.LOG) == 0.0  # a logged ratio
        assert null_value(EffectType.LOG_OR, Scale.RAW) == 0.0

    def test_raw_ratio_is_one(self) -> None:
        for et in (EffectType.OR, EffectType.RR, EffectType.HR, EffectType.ROM):
            assert null_value(et, Scale.RAW) == 1.0


# ---------------------------------------------------------------------------
# the gate: computed bearing
# ---------------------------------------------------------------------------

class TestGate:
    def test_superiority_supports_refutes_neutral(self) -> None:
        pred = _superiority(DirectionOfInterest.DECREASE)
        assert compute_bearing(_smd(-2.6, p=0.003), pred).direction is BearingDirection.SUPPORTS
        assert compute_bearing(_smd(+2.9, p=0.002), pred).direction is BearingDirection.REFUTES
        assert compute_bearing(_smd(-2.6, p=0.20), pred).direction is BearingDirection.NEUTRAL

    def test_superiority_pvalue_is_two_sided_at_one_sided_alpha(self) -> None:
        # The supplied p is two-sided; the gate is one-sided at alpha, so the
        # significance bar is 2*alpha. A two-sided p in (alpha, 2*alpha) clears
        # it, and the p path agrees with the (1 - 2*alpha) CI path.
        pred = _superiority(DirectionOfInterest.INCREASE, alpha=0.05)
        assert compute_bearing(_smd(0.6, p=0.08), pred).significant is True
        assert compute_bearing(_smd(0.6, p=0.08), pred).direction is BearingDirection.SUPPORTS
        assert compute_bearing(_smd(0.6, p=0.12), pred).significant is False
        ci_sig = _smd(0.6, ci=(0.01, 1.19), ci_level=0.90)  # excludes the null
        assert compute_bearing(ci_sig, pred).significant is True

    def test_superiority_ci_significance_requires_matching_level(self) -> None:
        pred = _superiority(DirectionOfInterest.INCREASE, alpha=0.05)
        ok = EffectEstimate(2.0, EffectType.MD, ci_lower=0.5, ci_upper=3.5, ci_level=0.90)
        assert compute_bearing(ok, pred).direction is BearingDirection.SUPPORTS
        wrong = EffectEstimate(2.0, EffectType.MD, ci_lower=0.5, ci_upper=3.5, ci_level=0.50)
        with pytest.raises(InconsistentEstimateError):
            compute_bearing(wrong, pred)

    def test_superiority_ratio_uses_correct_null(self) -> None:
        pred = _superiority(DirectionOfInterest.INCREASE, alpha=0.05)
        sig = EffectEstimate(1.5, EffectType.OR, ci_lower=1.2, ci_upper=2.0, ci_level=0.90)
        assert compute_bearing(sig, pred).direction is BearingDirection.SUPPORTS
        ns = EffectEstimate(1.1, EffectType.OR, ci_lower=0.8, ci_upper=1.5, ci_level=0.90)
        assert compute_bearing(ns, pred).direction is BearingDirection.NEUTRAL

    def test_equivalence_tost(self) -> None:
        pred = _equivalence(-0.1, 0.1, alpha=0.05)  # CI level must be 1 - 2*alpha = 0.90
        inside = EffectEstimate(0.0, EffectType.SMD, ci_lower=-0.05, ci_upper=0.05, ci_level=0.90)
        assert compute_bearing(inside, pred).direction is BearingDirection.SUPPORTS
        outside = EffectEstimate(0.5, EffectType.SMD, ci_lower=0.3, ci_upper=0.7, ci_level=0.90)
        assert compute_bearing(outside, pred).direction is BearingDirection.REFUTES
        straddle = EffectEstimate(0.08, EffectType.SMD, ci_lower=-0.05, ci_upper=0.21, ci_level=0.90)
        assert compute_bearing(straddle, pred).direction is BearingDirection.NEUTRAL

    def test_equivalence_requires_correct_ci_level(self) -> None:
        pred = _equivalence(-0.1, 0.1, alpha=0.05)  # expects ci_level 0.90
        bad = EffectEstimate(0.0, EffectType.SMD, ci_lower=-0.05, ci_upper=0.05, ci_level=0.95)
        with pytest.raises(InconsistentEstimateError):
            compute_bearing(bad, pred)

    def test_equivalence_region_must_bracket_null(self) -> None:
        pred = _equivalence(0.2, 0.5, alpha=0.05)  # region excludes the SMD null of 0
        est = EffectEstimate(0.35, EffectType.SMD, ci_lower=0.25, ci_upper=0.45, ci_level=0.90)
        with pytest.raises(InconsistentEstimateError):
            compute_bearing(est, pred)

    def test_equivalence_without_ci_raises(self) -> None:
        with pytest.raises(InconsistentEstimateError):
            compute_bearing(_smd(0.0, p=0.5), _equivalence())


# ---------------------------------------------------------------------------
# derived Status: the count-based state machine
# ---------------------------------------------------------------------------

class TestStatusMachine:
    @pytest.mark.parametrize(
        "support,refute,expected",
        [
            (0, 0, Status.UNTESTED),
            (1, 0, Status.PRELIMINARY),
            (2, 0, Status.CORROBORATED),
            (3, 0, Status.CORROBORATED),
            (0, 1, Status.REFUTED),
            (0, 2, Status.REFUTED),
            (1, 1, Status.CONTESTED),
            (2, 1, Status.CONTESTED),
            (2, 2, Status.CONTESTED),
        ],
    )
    def test_transitions(self, support: int, refute: int, expected: Status) -> None:
        assert compute_status(support, refute) is expected

    def test_rejects_negative_counts(self) -> None:
        with pytest.raises(ValueError):
            compute_status(-1, 0)
        with pytest.raises(ValueError):
            compute_status(0, -1)

    def test_frame_status(self) -> None:
        assert compute_frame_status(0) is FrameStatus.CONSISTENT
        assert compute_frame_status(1) is FrameStatus.CONTESTED
        assert compute_frame_status(3) is FrameStatus.CONTESTED


# ---------------------------------------------------------------------------
# input consistency
# ---------------------------------------------------------------------------

class TestInputConsistency:
    def test_estimate_rejections(self) -> None:
        with pytest.raises(InconsistentEstimateError):  # NaN
            EffectEstimate(float("nan"), EffectType.SMD, p_value=0.01)
        with pytest.raises(InconsistentEstimateError):  # neither p nor CI
            EffectEstimate(1.0, EffectType.SMD)
        with pytest.raises(InconsistentEstimateError):  # partial CI triple
            EffectEstimate(1.0, EffectType.SMD, ci_lower=0.5)
        with pytest.raises(InconsistentEstimateError):  # p out of range
            EffectEstimate(1.0, EffectType.SMD, p_value=1.5)
        with pytest.raises(InconsistentEstimateError):  # bad ci_level
            EffectEstimate(1.0, EffectType.MD, ci_lower=0.5, ci_upper=1.5, ci_level=1.5)
        with pytest.raises(InconsistentEstimateError):  # CI does not bracket estimate
            EffectEstimate(5.0, EffectType.MD, ci_lower=0.0, ci_upper=1.0, ci_level=0.95)
        with pytest.raises(InconsistentEstimateError):  # inverted CI bounds
            EffectEstimate(1.5, EffectType.MD, ci_lower=2.0, ci_upper=1.0, ci_level=0.95)
        with pytest.raises(InconsistentEstimateError):  # n_total <= 0
            EffectEstimate(1.0, EffectType.SMD, p_value=0.01, n_total=0)
        with pytest.raises(InconsistentEstimateError):  # non-finite p_value
            EffectEstimate(1.0, EffectType.SMD, p_value=float("nan"))
        with pytest.raises(InconsistentEstimateError):  # +inf CI bound
            EffectEstimate(1.0, EffectType.MD, ci_lower=0.5, ci_upper=float("inf"), ci_level=0.95)
        with pytest.raises(InconsistentEstimateError):  # -inf CI bound slips past the bracket test
            EffectEstimate(1.0, EffectType.MD, ci_lower=float("-inf"), ci_upper=2.0, ci_level=0.95)

    def test_estimate_valid_constructions(self) -> None:
        EffectEstimate(1.0, EffectType.SMD, p_value=0.01)  # p only
        EffectEstimate(1.0, EffectType.MD, ci_lower=0.5, ci_upper=1.5, ci_level=0.95)  # CI only
        EffectEstimate(1.0, EffectType.SMD, p_value=0.01, ci_lower=0.5, ci_upper=1.5, ci_level=0.95)
        EffectEstimate(0.5, EffectType.SMD, p_value=0.0)  # boundary p
        EffectEstimate(0.5, EffectType.SMD, p_value=1.0)

    def test_evidence_line_guards(self) -> None:
        est = _smd(1.0, p=0.01)
        with pytest.raises(ValueError):
            EvidenceLine(estimate=est, data_id="  ")
        with pytest.raises(TypeError):
            EvidenceLine(estimate="not an estimate", data_id="d")
        line = EvidenceLine(estimate=est, data_id="d", contrast=Contrast("vehicle"))
        assert line.contrast.control_type is ControlType.VEHICLE

    def test_prediction_validation(self) -> None:
        with pytest.raises(ValueError):  # alpha out of range
            Prediction(TestType.SUPERIORITY, direction_of_interest=DirectionOfInterest.INCREASE, alpha=0)
        with pytest.raises(ValueError):  # superiority without direction
            Prediction(TestType.SUPERIORITY)
        with pytest.raises(ValueError):  # superiority with equivalence margins
            Prediction(
                TestType.SUPERIORITY,
                direction_of_interest=DirectionOfInterest.INCREASE,
                equivalence_lower=-0.1,
                equivalence_upper=0.1,
            )
        with pytest.raises(ValueError):  # equivalence without margins
            Prediction(TestType.EQUIVALENCE)
        with pytest.raises(ValueError):  # equivalence with lower >= upper
            Prediction(TestType.EQUIVALENCE, equivalence_lower=0.1, equivalence_upper=-0.1)
        with pytest.raises(ValueError):  # equivalence with direction_of_interest
            Prediction(
                TestType.EQUIVALENCE,
                equivalence_lower=-0.1,
                equivalence_upper=0.1,
                direction_of_interest=DirectionOfInterest.INCREASE,
            )


# ---------------------------------------------------------------------------
# Success criteria: the graph, end to end
# ---------------------------------------------------------------------------

class TestSuccessCriteria:
    def test_record_and_build_on_a_finding(self, tmp_path: Path) -> None:
        """An agent records a finding; a second retrieves it by frame and sees a
        derived count-Status, with no human in the loop."""
        h = _prop(Direction.DECREASES)
        with open_graph(tmp_path) as graph:
            result = graph.assert_finding(
                h, _superiority(), _smd(-2.6, p=0.003, n=842),
                data_id="dataA", generated_by="lab_a",
            )
            in_frame = graph.query_frame(h.frame_id())
            status = graph.proposition_status(h)
        assert result["bearing"]["direction"] == "supports"
        assert result["idempotent"] is False
        assert any(v["content_id"] == h.content_id() for v in in_frame)
        assert status["status"] == Status.PRELIMINARY.value
        assert status["independent_support"] == 1

    def test_two_independent_lines_reach_corroborated(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        # v0.3.7 counts independent support by distinct asserter_keyid (the
        # finding-claim signer). assert_finding signs with the graph's loaded
        # key, so two findings through ONE signed handle share one keyid and
        # count once. Open UNSIGNED so asserter_keyid is NULL and the legacy
        # generated_by axis applies: two unsigned findings with distinct
        # generated_by + distinct data_id are two independent lines — the two
        # labs this test models.
        with mareforma.open(tmp_path) as graph:
            graph.assert_finding(h, _superiority(), _smd(-2.6, p=0.003), data_id="dataA", generated_by="lab_a")
            graph.assert_finding(h, _superiority(), _smd(-2.4, p=0.01), data_id="dataB", generated_by="lab_b")
            status = graph.proposition_status(h)
        assert status["independent_support"] == 2
        assert status["status"] == Status.CORROBORATED.value

    def test_same_dataset_does_not_recount(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        with open_graph(tmp_path) as graph:
            graph.assert_finding(h, _superiority(), _smd(-2.6, p=0.003), data_id="dataA", generated_by="lab_a")
            again = graph.assert_finding(
                h, _superiority(), _smd(-2.6, p=0.003), data_id="dataA", generated_by="lab_b"
            )
            status = graph.proposition_status(h)
        assert again["idempotent"] is True
        assert status["independent_support"] == 1

    def test_opposite_outcome_refutes_then_contests(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        with open_graph(tmp_path) as graph:
            graph.assert_finding(h, _superiority(), _smd(+2.9, p=0.002), data_id="dataA", generated_by="lab_a")
            after_refute = graph.proposition_status(h)
            graph.assert_finding(h, _superiority(), _smd(-2.5, p=0.004), data_id="dataB", generated_by="lab_b")
            after_support = graph.proposition_status(h)
        assert after_refute["independent_refute"] == 1
        assert after_refute["status"] == Status.REFUTED.value
        assert after_support["status"] == Status.CONTESTED.value

    def test_opposite_findings_surface_frame_contest(self, tmp_path: Path) -> None:
        down = Proposition("X", "affects", "Y", Direction.DECREASES, {"pop": "P"})
        up = Proposition("X", "affects", "Y", Direction.INCREASES, {"pop": "P"})
        with open_graph(tmp_path) as graph:
            graph.assert_finding(
                down, _superiority(DirectionOfInterest.DECREASE), _smd(-2.5, p=0.004),
                data_id="dA", generated_by="a",
            )
            graph.assert_finding(
                up, _superiority(DirectionOfInterest.INCREASE), _smd(2.5, p=0.004),
                data_id="dB", generated_by="b",
            )
            sd = graph.proposition_status(down)
            su = graph.proposition_status(up)
        assert sd["frame_status"] == FrameStatus.CONTESTED.value
        assert su["frame_status"] == FrameStatus.CONTESTED.value
        # neither is silently corroborated; each stands at PRELIMINARY on its own row
        assert sd["status"] == Status.PRELIMINARY.value
        assert su["status"] == Status.PRELIMINARY.value

    def test_legacy_claims_coexist(self, tmp_path: Path) -> None:
        with open_graph(tmp_path) as graph:
            cid = graph.assert_claim("BRCA1 knockdown slows tumour growth", generated_by="legacy")
            found = graph.get_claim(cid)
            hits = graph.query(text="tumour")
            structured = graph.get_proposition(cid)
        assert found is not None
        assert any(r["claim_id"] == cid for r in hits)
        assert structured is None  # the legacy claim_id is not a structured proposition


# ---------------------------------------------------------------------------
# Graph: definitive null, registration gate, retrieval filter
# ---------------------------------------------------------------------------

class TestGraphBehaviour:
    def test_definitive_null_supports_no_effect(self, tmp_path: Path) -> None:
        """A passed equivalence test supports a NO_EFFECT proposition (a recorded
        dead end), unlike an inconclusive superiority null."""
        h = Proposition("X", "affects", "Y", Direction.NO_EFFECT, {"pop": "P"})
        est = EffectEstimate(0.0, EffectType.SMD, ci_lower=-0.05, ci_upper=0.05, ci_level=0.90)
        with open_graph(tmp_path) as graph:
            result = graph.assert_finding(h, _equivalence(-0.1, 0.1), est, data_id="dA", generated_by="a")
            status = graph.proposition_status(h)
        assert result["bearing"]["direction"] == "supports"
        assert status["status"] == Status.PRELIMINARY.value

    def test_inconclusive_null_is_neutral(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        with open_graph(tmp_path) as graph:
            result = graph.assert_finding(h, _superiority(), _smd(-0.3, p=0.4), data_id="dA", generated_by="a")
            status = graph.proposition_status(h)
        assert result["bearing"]["direction"] == "neutral"
        assert status["independent_support"] == 0
        assert status["independent_refute"] == 0
        assert status["status"] == Status.UNTESTED.value

    def test_register_rejects_non_falsifiable(self, tmp_path: Path) -> None:
        bad = Proposition("X", "relates to", "Y")  # no direction, no scope
        with open_graph(tmp_path) as graph:
            with pytest.raises(NonFalsifiablePropositionError):
                graph.register_proposition(bad)
            with pytest.raises(NonFalsifiablePropositionError):
                graph.assert_finding(bad, _superiority(), _smd(1.0, p=0.01), data_id="d", generated_by="a")

    def test_register_proposition_is_idempotent(self, tmp_path: Path) -> None:
        h = _prop(Direction.INCREASES)
        with open_graph(tmp_path) as graph:
            cid1 = graph.register_proposition(h)
            cid2 = graph.register_proposition(h)
        assert cid1 == cid2 == h.content_id()

    def test_query_frame_min_status_filter(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        # Unsigned graph so the two findings count as two independent lines
        # (legacy generated_by axis) — see test_two_independent_lines_reach_
        # corroborated. CORROBORATED is what the min_status floor checks here.
        with mareforma.open(tmp_path) as graph:
            graph.assert_finding(h, _superiority(), _smd(-2.6, p=0.003), data_id="dataA", generated_by="lab_a")
            preliminary_floor = graph.query_frame(h.frame_id(), min_status="PRELIMINARY")
            corroborated_floor = graph.query_frame(h.frame_id(), min_status="CORROBORATED")
            graph.assert_finding(h, _superiority(), _smd(-2.4, p=0.01), data_id="dataB", generated_by="lab_b")
            corroborated_after = graph.query_frame(h.frame_id(), min_status="CORROBORATED")
        assert preliminary_floor  # one support meets a PRELIMINARY floor
        assert corroborated_floor == []  # but not a CORROBORATED floor
        assert corroborated_after  # two independent supports do
