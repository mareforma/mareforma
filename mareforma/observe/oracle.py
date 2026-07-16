"""Input-perturbation causal oracle: an independent ground truth for grounding.

The observer computes FLOW: did the cited bytes arrive in the scope. The oracle
measures INFLUENCE: does the finding actually depend on the data. Perturb the
input, re-run the pipeline, and see if the finding moves. If it moves, the data
causally shaped the finding; if it does not, the finding would have come out the
same with different data, which is the signature of a silent fallback.

The oracle is the observer's validation because it is independent: it never
reads the observer's log, so a detector that agreed with itself cannot look
correct here. It also handles the honest hard case, a stochastic pipeline
(an LLM at nonzero temperature) moves run to run even with fixed input, so a
naive "did it change" test would call everything influenced. The oracle measures
that run-to-run noise first and only calls INFLUENCED when the perturbation moves
the finding by more than the noise floor. When the effect is comparable to the
noise, the answer is UNDECIDABLE, never a silent INFLUENCED.

Flow and influence are different constructs, so the observer and the oracle can
honestly disagree: a finding can read the cited data (flow) and then ignore it
(no influence). :func:`reconcile` labels that a construct difference, not a
detector error.
"""
from __future__ import annotations

import math
import re
import statistics
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Sequence

from ._verdict import ObservedGrounding

# Below this many repeats the base-run spread is a THIN estimate of the
# pipeline's noise: pstdev over 2-4 samples routinely understates the population
# sigma, so an INFLUENCED verdict resting on that floor is over-confident. The
# guard (opt-in) widens the noise margin by a small-sample factor; the informational
# ``noise_is_thin`` flag is always recorded so a reader sees the caveat.
_THIN_REPEATS = 5


class OracleInfluence(str, Enum):
    """The oracle's verdict on whether the data influenced the finding."""

    INFLUENCED = "INFLUENCED"
    NOT_INFLUENCED = "NOT_INFLUENCED"
    UNDECIDABLE = "UNDECIDABLE"


@dataclass(frozen=True)
class MetricReducer:
    """A DECLARED reduction of a finding to a scalar the oracle can compare.

    The oracle needs one number per finding. For a numeric finding that is
    ``float(finding)``. For a PROSE finding (the text output of a RAG or
    agent pipeline) the caller must supply a reduction, a named extraction to a
    scalar, and DECLARE what it is, because the choice of reducer is a
    measurement decision a reviewer must be able to audit. In particular an
    embedding-distance or LLM-judge reducer re-inserts a model into the ground
    truth the oracle is supposed to provide independently; that is sometimes the
    only option, but it must be stated, never hidden. ``reinserts_model=True``
    records it so the measurement artifact declares it.

    ``name`` identifies the reducer in the artifact; ``reduce`` is the callable;
    ``description`` is free-text for the run log.
    """

    name: str
    reduce: "Callable[[Any], float]"
    reinserts_model: bool = False
    description: str = ""

    def __call__(self, finding: Any) -> float:
        return self.reduce(finding)

    def declaration(self) -> dict:
        """The record a measurement artifact carries so the reducer is auditable."""
        return {
            "name": self.name,
            "reinserts_model": self.reinserts_model,
            "description": self.description,
        }


# The default reducer: a numeric finding reduced by float(). Named and declared
# like any other, so every OracleResult can say which reducer produced it.
scalar_reducer = MetricReducer(
    name="scalar",
    reduce=lambda finding: _coerce_scalar(finding),
    reinserts_model=False,
    description="float(finding); for a numeric finding or effect estimate",
)


def declared_reducer(
    name: str,
    reduce: "Callable[[Any], float]",
    *,
    reinserts_model: bool = False,
    description: str = "",
) -> MetricReducer:
    """Build a declared reducer for prose or structured findings.

    Use this to name the reduction a text-output pipeline needs (for example, an
    extraction of the reported effect from an answer string). Set
    ``reinserts_model=True`` when the reduction runs a model (an embedding
    distance, an LLM judge), so the measurement declares that it is no longer a
    model-independent ground truth.
    """
    return MetricReducer(
        name=name, reduce=reduce, reinserts_model=reinserts_model,
        description=description,
    )


# A number in a prose answer: an optional sign, digits with an optional decimal
# point, and an optional exponent. Finds "0.42", "-3", "1.2e-3".
_NUMBER_RE = re.compile(r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?")


def numeric_extraction_reducer(
    name: str = "numeric_extraction",
    *,
    index: int = 0,
    description: str = "",
) -> MetricReducer:
    """A model-FREE prose reducer: pull the reported number out of an answer string.

    The paper's target pipelines emit prose ("the effect was 0.42"), so the oracle
    needs a reduction to a scalar. This is the honest DEFAULT for prose that states
    a number: a regex extraction, no model, so ``reinserts_model=False`` and the
    oracle stays a model-independent ground truth. Prefer it to an embedding /
    LLM-judge reducer whenever the finding actually reports a number; reach for a
    ``declared_reducer(..., reinserts_model=True)`` only when the reduction genuinely
    needs a model, and then the measurement declares it.

    ``index`` selects which number when the answer carries several (default the
    first). A number-free answer raises, so a silent zero is never invented.
    """
    def reduce(finding: Any) -> float:
        if isinstance(finding, bool):  # bool is an int subclass; never a metric
            raise TypeError("a boolean is not a numeric finding")
        if isinstance(finding, (int, float)):
            return float(finding)
        text = finding if isinstance(finding, str) else str(finding)
        matches = _NUMBER_RE.findall(text)
        if not matches:
            raise ValueError(
                f"no number found in prose finding to reduce: {text[:60]!r}"
            )
        try:
            return float(matches[index])
        except IndexError as exc:
            raise ValueError(
                f"prose finding has {len(matches)} numbers, none at index {index}: "
                f"{text[:60]!r}"
            ) from exc

    return declared_reducer(
        name, reduce, reinserts_model=False,
        description=description
        or "extract the reported number from an answer string (model-free)",
    )


@dataclass
class OracleResult:
    """The oracle's measurement, with the numbers behind the verdict."""

    influence: OracleInfluence
    effect_size: float
    noise_floor: float
    decision_threshold: float
    reason: str
    base_values: tuple[float, ...] = ()
    perturbed_values: tuple[float, ...] = ()
    # The declared reducer used to reduce each finding to a scalar, so the
    # measurement artifact is auditable about how prose became a number.
    reducer: "MetricReducer | None" = None
    # The multiplicity the decision threshold was widened for (1 = a single
    # finding, no correction), and whether the noise floor rests on too few
    # repeats to be trusted. Both are recorded so the verdict is auditable.
    multiplicity: int = 1
    noise_is_thin: bool = False

    @property
    def reinserts_model(self) -> bool:
        """Whether the declared reducer ran a model to reduce the finding.

        True means the ground truth is no longer model-independent, the oracle's
        one guarantee, so the measurement must surface it. Read it off the result
        rather than re-inspecting the reducer.
        """
        return bool(self.reducer and self.reducer.reinserts_model)

    def declaration(self) -> "dict | None":
        """The reducer's audit record, or None when no reducer was declared."""
        return self.reducer.declaration() if self.reducer else None


def _coerce_scalar(finding: Any) -> float:
    try:
        return float(finding)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            "the causal oracle needs a scalar per finding: pass metric=... to "
            "reduce a structured finding to a float (e.g. the effect estimate). "
            "For a prose finding, declare a reducer with declared_reducer(...)"
        ) from exc


def _default_metric(finding: Any) -> float:
    return _coerce_scalar(finding)


def perturbation_oracle(
    run_fn: Callable[[Any], Any],
    base_input: Any,
    perturb: "Callable[[Any], Any] | Sequence[Any]",
    *,
    repeats: int = 1,
    metric: "Callable[[Any], float] | None" = None,
    effect_threshold: float = 0.0,
    noise_multiplier: float = 3.0,
    multiplicity: int = 1,
    thin_sigma_guard: bool = False,
) -> OracleResult:
    """Measure whether the cited data causally influences the finding.

    Parameters
    ----------
    run_fn:
        Runs the pipeline on an input and returns the finding. Called with the
        base input and each perturbed input.
    base_input:
        The unperturbed input.
    perturb:
        Either a callable that maps the base input to a perturbed input, or a
        sequence of already-perturbed inputs. Each perturbation is a different
        way of changing the cited data; the finding should move if it depends
        on that data.
    repeats:
        Runs per configuration. Above 1, the spread of the base runs measures
        the pipeline's run-to-run noise (LLM nondeterminism), which sets the
        floor a real effect must clear. Use temperature 0 / a pinned seed plus
        repeats to bound the noise honestly.
    metric:
        Reduces a finding to a scalar for comparison. Defaults to ``float()``.
    effect_threshold:
        A floor on the effect size below which a change is not meaningful,
        independent of noise (e.g. a domain-minimal effect).
    noise_multiplier:
        How many noise standard deviations the effect must exceed to count as
        influence. The decision threshold is ``max(effect_threshold,
        sigma_multiplier * noise_std)``, where ``sigma_multiplier`` starts at
        ``noise_multiplier`` and is widened by the multiplicity and thin-sigma
        controls below.
    multiplicity:
        The number of findings this call is one of. When influence is computed
        across many findings, a fixed ``noise_multiplier`` lets the noisiest
        finding cross the bar by chance, so the family produces false INFLUENCED
        calls at a rate that grows with the count. Passing the family size adds
        ``sqrt(2 * ln(multiplicity))`` sigmas to the multiplier, the scale of the
        largest spurious deviation expected across ``multiplicity`` standard
        draws, so the control is applied BEFORE any influence number is computed.
        ``1`` (the default) adds nothing: a single finding needs no correction.
    thin_sigma_guard:
        When True, widen the noise margin by a small-sample factor whenever the
        noise floor rests on fewer than ``_THIN_REPEATS`` repeats (a thin pstdev
        understates the real sigma). Off by default, so the scalar path is
        unchanged; ``noise_is_thin`` is recorded either way.

    Returns
    -------
    OracleResult
        INFLUENCED when the perturbation moves the finding past the threshold;
        NOT_INFLUENCED when the finding holds still; UNDECIDABLE when the effect
        is real-signed but within the noise band (never silently INFLUENCED).
    """
    if repeats < 1:
        raise ValueError("repeats must be >= 1")
    if multiplicity < 1:
        raise ValueError("multiplicity must be >= 1")
    # Resolve the metric to a declared reducer so the result records which one ran.
    # A bare callable is wrapped as an unnamed reducer; None uses scalar_reducer.
    if metric is None:
        reducer = scalar_reducer
    elif isinstance(metric, MetricReducer):
        reducer = metric
    else:
        reducer = MetricReducer(name="custom", reduce=metric)
    m = reducer

    base_values = tuple(m(run_fn(base_input)) for _ in range(repeats))
    perturbed_inputs = _resolve_perturbations(base_input, perturb)
    perturbed_values: list[float] = []
    for pin in perturbed_inputs:
        perturbed_values.extend(m(run_fn(pin)) for _ in range(repeats))
    perturbed_values = tuple(perturbed_values)

    base_mean = statistics.fmean(base_values)
    pert_mean = statistics.fmean(perturbed_values)
    effect_size = abs(pert_mean - base_mean)

    # Noise floor from run-to-run spread of the base configuration. With a
    # single deterministic run there is no measurable noise, so the floor is 0
    # and effect_threshold alone decides.
    noise_std = statistics.pstdev(base_values) if len(base_values) > 1 else 0.0

    # A noise floor estimated from too few repeats is thin (pstdev understates
    # the population sigma). Always record it; widen the margin only when the
    # guard is on, so the default scalar path is unchanged.
    noise_is_thin = 1 < repeats < _THIN_REPEATS
    sigma_multiplier = noise_multiplier
    if multiplicity > 1:
        # The extreme-value scale of the max of ``multiplicity`` standard draws:
        # the extra sigmas a family of that size needs to hold its false-influence
        # rate. Applied before the influence call, per finding.
        sigma_multiplier += math.sqrt(2.0 * math.log(multiplicity))
    if thin_sigma_guard and noise_is_thin:
        sigma_multiplier *= math.sqrt(_THIN_REPEATS / repeats)
    noise_margin = sigma_multiplier * noise_std
    decision_threshold = max(effect_threshold, noise_margin)

    # UNDECIDABLE is a NOISE verdict: it applies only when noise, not the domain
    # floor, sets the threshold. When effect_threshold is the binding constraint,
    # an effect below it is domain-insignificant (NOT_INFLUENCED), not ambiguous.
    noise_driven = noise_std > 0 and noise_margin >= effect_threshold

    if effect_size > decision_threshold:
        influence = OracleInfluence.INFLUENCED
        reason = (
            f"perturbing the input moved the finding by {effect_size:.4g}, past "
            f"the {decision_threshold:.4g} threshold: the data influences it"
        )
    elif noise_driven and effect_size > noise_std:
        # A signed move that clears one noise sd but not the full noise margin:
        # real enough not to call NOT_INFLUENCED, not clear enough for INFLUENCED.
        influence = OracleInfluence.UNDECIDABLE
        reason = (
            f"effect {effect_size:.4g} is within the noise band "
            f"(<= {decision_threshold:.4g}); undecidable, not called grounded"
        )
    else:
        influence = OracleInfluence.NOT_INFLUENCED
        reason = (
            f"perturbing the input barely moved the finding ({effect_size:.4g} "
            f"<= {decision_threshold:.4g}): the finding does not depend on the data"
        )

    return OracleResult(
        influence=influence,
        effect_size=effect_size,
        noise_floor=noise_std,
        decision_threshold=decision_threshold,
        reason=reason,
        base_values=base_values,
        perturbed_values=perturbed_values,
        reducer=reducer,
        multiplicity=multiplicity,
        noise_is_thin=noise_is_thin,
    )


def _resolve_perturbations(base_input, perturb) -> list:
    if callable(perturb):
        return [perturb(base_input)]
    perturbed = list(perturb)
    if not perturbed:
        raise ValueError("perturb must yield at least one perturbed input")
    return perturbed


class Reconciliation(str, Enum):
    """How the observer's flow verdict relates to the oracle's influence verdict."""

    AGREE = "AGREE"
    CONSTRUCT_DIFFERENCE = "CONSTRUCT_DIFFERENCE"
    OBSERVER_BLIND = "OBSERVER_BLIND"
    TENSION = "TENSION"
    INCONCLUSIVE = "INCONCLUSIVE"


@dataclass
class ReconcileResult:
    relation: Reconciliation
    reason: str


def reconcile(
    grounding: ObservedGrounding, oracle: OracleInfluence
) -> ReconcileResult:
    """Relate the observer's flow verdict to the oracle's influence verdict.

    Flow and influence are different constructs, so a mismatch is not
    automatically a detector error:

    - GROUNDED + INFLUENCED, UNGROUNDED + NOT_INFLUENCED → AGREE.
    - GROUNDED + NOT_INFLUENCED → CONSTRUCT_DIFFERENCE: the pipeline read the
      cited data and then did not use it. Flow without influence, not a bug.
    - OPAQUE + anything → OBSERVER_BLIND: the observer could not see, so there is
      nothing to reconcile against the oracle.
    - UNGROUNDED + INFLUENCED → TENSION: the data demonstrably influences the
      finding, yet the observer saw no cited read. This is the one combination
      worth investigating, the observer likely missed a read (a coverage gap).
    - anything + UNDECIDABLE → INCONCLUSIVE: the oracle could not decide.
    """
    if oracle is OracleInfluence.UNDECIDABLE:
        return ReconcileResult(
            Reconciliation.INCONCLUSIVE,
            "the oracle could not decide influence above the noise floor",
        )
    if grounding is ObservedGrounding.OPAQUE:
        return ReconcileResult(
            Reconciliation.OBSERVER_BLIND,
            "the observer could not see the scope; no flow verdict to reconcile",
        )
    if grounding is ObservedGrounding.GROUNDED:
        if oracle is OracleInfluence.INFLUENCED:
            return ReconcileResult(
                Reconciliation.AGREE, "cited data flowed in and influences the finding"
            )
        return ReconcileResult(
            Reconciliation.CONSTRUCT_DIFFERENCE,
            "cited data flowed in but does not influence the finding "
            "(flow without influence, not a detector error)",
        )
    # grounding is UNGROUNDED
    if oracle is OracleInfluence.NOT_INFLUENCED:
        return ReconcileResult(
            Reconciliation.AGREE,
            "no cited data flowed in and the finding does not depend on it",
        )
    return ReconcileResult(
        Reconciliation.TENSION,
        "the finding depends on the data, yet no cited read was observed: "
        "likely a coverage gap the observer missed",
    )
