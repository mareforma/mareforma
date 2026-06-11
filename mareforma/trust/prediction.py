"""prediction.py: the pre-registered decision rule (the Plan).

The question the legacy core could not answer: "how do we know the code
actually supports or contradicts the hypothesis?" In the legacy graph it
cannot, ``classification`` is a self-declared label and ``supports[]`` /
``contradicts[]`` are self-declared edges, so an agent can write a refutation
next to a confirming result and the core records it faithfully.

The fix is the pre-registered prediction, the load-bearing idea of the
hypothetico-deductive method. The chain is::

    Proposition H  --states-->   Prediction: "if H, the estimate lands on the
                                  expected side of the null"
    Procedure runs --------->    EffectEstimate: an actual value + uncertainty
    Bearing  =  gate(estimate, prediction)        <- COMPUTED, not declared

So the direction of evidence is a pure function of the registered rule and the
realised outcome. The agent does not get to choose the label, the label is
derived (see :mod:`mareforma.trust.bearing`). That is what makes the edge
earned.

This layer ships a concrete typed struct, NOT the general gate grammar: a
frequentist superiority test and a frequentist equivalence test (TOST). The
other regimes, test types, multiplicity, severity, and magnitude bands are the
north-star shape and are added when a second gate type exists (YAGNI).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class InferenceRegime(str, Enum):
    FREQUENTIST = "frequentist"


class TestType(str, Enum):
    SUPERIORITY = "superiority"
    EQUIVALENCE = "equivalence"


class DirectionOfInterest(str, Enum):
    """The side of the null the superiority hypothesis predicts.

    Lives on the Prediction, never on the EffectEstimate: the gate reads the
    predicted direction from the pre-registered rule, so an agent cannot
    retro-fit a direction to a result it has already seen.
    """

    INCREASE = "increase"
    DECREASE = "decrease"


@dataclass(frozen=True)
class Prediction:
    """A pre-registered decision rule bound (at registration) to one proposition.

    Superiority: declares the predicted side of the null
    (``direction_of_interest``). The gate computes SUPPORTS when the estimate
    is significant on that side, REFUTES when significant on the opposite side,
    NEUTRAL otherwise.

    Equivalence (TOST): declares an equivalence region
    (``equivalence_lower``/``equivalence_upper``) around the null. The gate
    computes SUPPORTS for the no-effect proposition when the estimate's
    ``(1 - 2*alpha)`` CI lies entirely inside the region, REFUTES when the CI
    lies entirely outside it, NEUTRAL when it straddles a margin.
    """

    test_type: TestType
    alpha: float = 0.05
    # superiority only
    direction_of_interest: DirectionOfInterest | None = None
    # equivalence only (the null is bracketed by [lower, upper])
    equivalence_lower: float | None = None
    equivalence_upper: float | None = None
    preregistered: bool = False
    inference_regime: InferenceRegime = InferenceRegime.FREQUENTIST

    def __post_init__(self) -> None:
        # Coerce string inputs to enums so callers may pass either form.
        if not isinstance(self.test_type, TestType):
            object.__setattr__(self, "test_type", TestType(self.test_type))
        if not isinstance(self.inference_regime, InferenceRegime):
            object.__setattr__(
                self, "inference_regime", InferenceRegime(self.inference_regime)
            )
        if self.direction_of_interest is not None and not isinstance(
            self.direction_of_interest, DirectionOfInterest
        ):
            object.__setattr__(
                self,
                "direction_of_interest",
                DirectionOfInterest(self.direction_of_interest),
            )

        if not (0.0 < self.alpha < 1.0):
            raise ValueError("alpha must be in (0, 1)")

        if self.test_type is TestType.SUPERIORITY:
            if self.direction_of_interest is None:
                raise ValueError(
                    "superiority prediction requires direction_of_interest "
                    "(increase | decrease)"
                )
            if self.equivalence_lower is not None or self.equivalence_upper is not None:
                raise ValueError(
                    "equivalence_margins are not valid on a superiority prediction"
                )
        else:  # EQUIVALENCE
            if self.equivalence_lower is None or self.equivalence_upper is None:
                raise ValueError(
                    "equivalence prediction requires equivalence_lower and "
                    "equivalence_upper"
                )
            if not (self.equivalence_lower < self.equivalence_upper):
                raise ValueError(
                    "equivalence_lower must be strictly less than equivalence_upper"
                )
            if self.direction_of_interest is not None:
                raise ValueError(
                    "direction_of_interest is not valid on an equivalence prediction"
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "test_type": self.test_type.value,
            "alpha": self.alpha,
            "direction_of_interest": (
                self.direction_of_interest.value
                if self.direction_of_interest
                else None
            ),
            "equivalence_lower": self.equivalence_lower,
            "equivalence_upper": self.equivalence_upper,
            "preregistered": self.preregistered,
            "inference_regime": self.inference_regime.value,
        }
