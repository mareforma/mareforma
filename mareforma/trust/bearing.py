"""bearing.py: the gate: compute a Bearing from an estimate + a prediction.

The bearing is computed from the pre-registered rule and the realised outcome,
never declared, so an agent cannot relabel a refutation as support. Both gates
are closed-form CI / p-value arithmetic derived from first principles: no
GPL/Cochrane code is transcribed, and there is no iterative estimation (hence no
cross-host float-determinism problem).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .errors import InconsistentEstimateError
from .estimate import EffectEstimate
from .prediction import DirectionOfInterest, Prediction, TestType


class BearingDirection(str, Enum):
    SUPPORTS = "supports"
    REFUTES = "refutes"
    NEUTRAL = "neutral"


@dataclass(frozen=True)
class Bearing:
    """The computed relation between one outcome and one proposition."""

    direction: BearingDirection
    significant: bool

    def to_dict(self) -> dict:
        return {
            "direction": self.direction.value,
            "significant": self.significant,
        }


def compute_bearing(estimate: EffectEstimate, prediction: Prediction) -> Bearing:
    """Derive the bearing of *estimate* on the proposition *prediction* tests.

    Superiority gate
    ----------------
    significance = ``p_value < alpha`` when a p-value is supplied, else the CI
    excludes the null. ``direction`` compares ``sign(estimate - null)`` against
    the pre-registered ``direction_of_interest`` -> SUPPORTS / REFUTES /
    NEUTRAL.

    Equivalence gate (TOST)
    -----------------------
    The estimate is equivalent to the null iff its ``(1 - 2*alpha)`` CI lies
    entirely within ``[equivalence_lower, equivalence_upper]`` -> SUPPORTS the
    no-effect proposition; a CI lying entirely outside the region -> REFUTES
    it; a CI straddling a margin -> NEUTRAL (inconclusive).
    """
    if prediction.test_type is TestType.SUPERIORITY:
        return _superiority(estimate, prediction)
    return _equivalence(estimate, prediction)


def _superiority(estimate: EffectEstimate, prediction: Prediction) -> Bearing:
    null = estimate.null_value

    # Significance: prefer the p-value when present; otherwise the CI must
    # exclude the null on one side.
    if estimate.p_value is not None:
        significant = estimate.p_value < prediction.alpha
    else:
        # No p-value: significance comes from the CI excluding the null. The CI
        # must be at the level the test's alpha implies, or "excludes the null"
        # silently tests at the wrong level (a 50% CI excluding the null is not
        # significance at alpha=0.05). The hypothesis is directional
        # (direction_of_interest), so this is a one-sided test at alpha: a
        # two-sided (1 - 2*alpha) CI whose bound excludes the null on one side
        # matches a one-sided p < alpha. __post_init__ guarantees a full CI
        # triple exists when p_value is None.
        expected_level = 1.0 - 2.0 * prediction.alpha
        if abs(estimate.ci_level - expected_level) > 1e-9:
            raise InconsistentEstimateError(
                "superiority significance from a confidence interval requires a "
                f"(1 - 2*alpha) CI; alpha={prediction.alpha} expects "
                f"ci_level={expected_level}, got ci_level={estimate.ci_level}"
            )
        significant = estimate.ci_lower > null or estimate.ci_upper < null

    delta = estimate.estimate_value - null
    observed_sign = 0 if delta == 0 else (1 if delta > 0 else -1)
    expected_sign = (
        1 if prediction.direction_of_interest is DirectionOfInterest.INCREASE else -1
    )

    if not significant:
        direction = BearingDirection.NEUTRAL
    elif observed_sign == expected_sign:
        direction = BearingDirection.SUPPORTS
    else:
        # Significant on the opposite side (or exactly at the null while
        # significant, a degenerate input) does not support the prediction.
        direction = BearingDirection.REFUTES

    return Bearing(direction=direction, significant=significant)


def _equivalence(estimate: EffectEstimate, prediction: Prediction) -> Bearing:
    if estimate.ci_lower is None:
        raise InconsistentEstimateError(
            "an equivalence (TOST) test requires a confidence interval; "
            "supply ci_lower, ci_upper, ci_level"
        )

    expected_level = 1.0 - 2.0 * prediction.alpha
    if abs(estimate.ci_level - expected_level) > 1e-9:
        raise InconsistentEstimateError(
            "equivalence (TOST) requires a (1 - 2*alpha) confidence interval; "
            f"alpha={prediction.alpha} expects ci_level={expected_level}, "
            f"got ci_level={estimate.ci_level}"
        )

    lo, hi = prediction.equivalence_lower, prediction.equivalence_upper
    null = estimate.null_value
    if not (lo <= null <= hi):
        raise InconsistentEstimateError(
            f"equivalence region [{lo}, {hi}] must bracket the null ({null}) "
            f"for effect_type={estimate.effect_type.value} on {estimate.scale.value}"
        )

    ci_lo, ci_hi = estimate.ci_lower, estimate.ci_upper

    if ci_lo >= lo and ci_hi <= hi:
        # CI entirely inside the equivalence region: equivalence established.
        direction = BearingDirection.SUPPORTS
        significant = True
    elif ci_lo > hi or ci_hi < lo:
        # CI entirely outside the region: a real effect refutes no-effect.
        direction = BearingDirection.REFUTES
        significant = True
    else:
        # CI straddles a margin: inconclusive.
        direction = BearingDirection.NEUTRAL
        significant = False

    return Bearing(direction=direction, significant=significant)
