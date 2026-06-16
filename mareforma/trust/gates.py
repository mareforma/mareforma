"""gates.py: the DecisionRule as an ordered gates[] short-circuit chain.

A :class:`mareforma.trust.Prediction` carries the gate-bearing columns the
substrate stores today (test_type, direction_of_interest, alpha, the
equivalence margins). This module re-expresses that same rule as an ordered
list of :class:`Gate` — a short-circuit chain — *as a pure Python structure
over the existing columns*. There is no new schema column and no migration: a
Gate is reconstructed from a stored Prediction, never persisted on its own.

The north-star DecisionRule is a chain of gates evaluated in order, the first
that *fires* (returns a non-NEUTRAL bearing) deciding the finding; if none fire,
the rule is NEUTRAL. The single binary gate shipped in v0.3.4 (a superiority
test, or an equivalence/TOST test) is exactly the **one-element** chain, so this
is a faithful generalisation, not a behaviour change: a one-element chain
produces a Bearing identical to :func:`mareforma.trust.compute_bearing` on the
same Prediction (parity-tested).

Only the regimes already expressible from the existing columns ship here.
Multiplicity, magnitude bands, non-inferiority, dose-response, and Bayesian
regimes are deferred — they need fields the schema does not carry, and adding
them would be a migration this release does not do.
"""
from __future__ import annotations

from dataclasses import dataclass

from .bearing import Bearing, BearingDirection, compute_bearing
from .estimate import EffectEstimate
from .prediction import DirectionOfInterest, Prediction, TestType


@dataclass(frozen=True)
class Gate:
    """One gate in a decision-rule chain, over the existing prediction columns.

    A Gate is the same shape as the per-test fields of a :class:`Prediction`
    (the inference regime is fixed frequentist for every regime this release
    expresses). It reconstructs the single-test Prediction it stands for via
    :meth:`as_prediction`, so its evaluation is by definition identical to the
    v0.3.4 gate.
    """

    test_type: TestType
    alpha: float
    direction_of_interest: DirectionOfInterest | None = None
    equivalence_lower: float | None = None
    equivalence_upper: float | None = None

    def as_prediction(self) -> Prediction:
        """The single-test Prediction this gate stands for.

        Reusing the Prediction constructor means the gate inherits the exact
        validation and the exact gate arithmetic of the shipped path — there is
        no second, drifting implementation of the rule.
        """
        return Prediction(
            test_type=self.test_type,
            alpha=self.alpha,
            direction_of_interest=self.direction_of_interest,
            equivalence_lower=self.equivalence_lower,
            equivalence_upper=self.equivalence_upper,
        )

    def evaluate(self, estimate: EffectEstimate) -> Bearing:
        """The bearing of *estimate* under this single gate."""
        return compute_bearing(estimate, self.as_prediction())


def gates_for(prediction: Prediction) -> list[Gate]:
    """The gates[] chain for a stored Prediction.

    Today every Prediction is a single binary gate, so the chain has exactly
    one element. This is the seam a later release grows a multi-gate rule
    through without a schema change — the chain representation already exists.
    """
    return [
        Gate(
            test_type=prediction.test_type,
            alpha=prediction.alpha,
            direction_of_interest=prediction.direction_of_interest,
            equivalence_lower=prediction.equivalence_lower,
            equivalence_upper=prediction.equivalence_upper,
        )
    ]


def evaluate_gates(estimate: EffectEstimate, gates: list[Gate]) -> Bearing:
    """Evaluate a gates[] chain as a short-circuit rule.

    Walk the gates in order; the first that *fires* — returns a non-NEUTRAL
    bearing (SUPPORTS or REFUTES) — decides the finding and short-circuits the
    rest. If no gate fires, the rule is NEUTRAL. For the one-element chain this
    returns that single gate's bearing, identical to
    :func:`mareforma.trust.compute_bearing` on the equivalent Prediction.
    """
    if not gates:
        raise ValueError("a decision rule needs at least one gate")
    last: Bearing | None = None
    for gate in gates:
        bearing = gate.evaluate(estimate)
        if bearing.direction is not BearingDirection.NEUTRAL:
            return bearing
        last = bearing
    # No gate fired: the rule is inconclusive. Return the final gate's NEUTRAL
    # bearing (a NEUTRAL bearing always carries significant=False).
    assert last is not None  # guaranteed: gates is non-empty
    return last
