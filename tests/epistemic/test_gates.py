"""tests/epistemic/test_gates.py: the gates[] decision-rule chain.

The DecisionRule is re-expressed as an ordered short-circuit gates[] chain over
the EXISTING prediction columns. The single binary gate shipped in v0.3.4 is the
one-element chain, and a one-element chain must produce a Bearing IDENTICAL to
the v0.3.4 compute_bearing path — for superiority and for equivalence/TOST.
"""
from __future__ import annotations

import pytest

from mareforma.trust import (
    BearingDirection,
    DirectionOfInterest,
    EffectEstimate,
    EffectType,
    Gate,
    Prediction,
    TestType,
    compute_bearing,
    evaluate_gates,
    gates_for,
)


from tests.epistemic._builders import _smd


class TestSingleElementChain:
    def test_gates_for_is_one_element(self) -> None:
        pred = Prediction(TestType.SUPERIORITY, direction_of_interest=DirectionOfInterest.DECREASE)
        gates = gates_for(pred)
        assert len(gates) == 1
        assert isinstance(gates[0], Gate)

    @pytest.mark.parametrize(
        "estimate",
        [
            _smd(-2.6, p=0.003),   # supports a DECREASE prediction
            _smd(+2.9, p=0.002),   # refutes
            _smd(-2.6, p=0.20),    # neutral (not significant)
            _smd(-2.0, ci=(-3.5, -0.5), ci_level=0.90),  # CI path, supports
        ],
    )
    def test_superiority_parity(self, estimate) -> None:
        pred = Prediction(
            TestType.SUPERIORITY,
            direction_of_interest=DirectionOfInterest.DECREASE,
            alpha=0.05,
        )
        direct = compute_bearing(estimate, pred)
        via_chain = evaluate_gates(estimate, gates_for(pred))
        assert via_chain == direct
        assert via_chain.direction == direct.direction
        assert via_chain.significant == direct.significant

    @pytest.mark.parametrize(
        "estimate,expected",
        [
            (EffectEstimate(0.0, EffectType.SMD, ci_lower=-0.05, ci_upper=0.05, ci_level=0.90),
             BearingDirection.SUPPORTS),   # inside equivalence region
            (EffectEstimate(0.5, EffectType.SMD, ci_lower=0.3, ci_upper=0.7, ci_level=0.90),
             BearingDirection.REFUTES),    # entirely outside
            (EffectEstimate(0.08, EffectType.SMD, ci_lower=-0.05, ci_upper=0.21, ci_level=0.90),
             BearingDirection.NEUTRAL),    # straddles a margin
        ],
    )
    def test_equivalence_tost_parity(self, estimate, expected) -> None:
        pred = Prediction(
            TestType.EQUIVALENCE,
            equivalence_lower=-0.1,
            equivalence_upper=0.1,
            alpha=0.05,
        )
        direct = compute_bearing(estimate, pred)
        via_chain = evaluate_gates(estimate, gates_for(pred))
        assert direct.direction is expected  # sanity: the v0.3.4 path agrees
        assert via_chain == direct

    def test_multi_gate_chain_is_rejected(self) -> None:
        """Multi-gate chains raise until their precedence semantics are designed,
        so an undecided rule can never silently apply."""
        gate = Gate(
            test_type=TestType.SUPERIORITY,
            alpha=0.05,
            direction_of_interest=DirectionOfInterest.DECREASE,
        )
        with pytest.raises(NotImplementedError):
            evaluate_gates(_smd(-2.6, p=0.003), [gate, gate])

    def test_empty_chain_raises(self) -> None:
        with pytest.raises(ValueError):
            evaluate_gates(_smd(1.0, p=0.01), [])
