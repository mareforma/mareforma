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
noise, the answer is UNDECIDABLE, never a silent INFLUENCED. Measuring the noise
takes repeats: at the default ``repeats=1`` the floor is 0 because nothing was
measured, and the result says so rather than passing 0 off as a trusted floor.

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
# pipeline's noise: pstdev over 1-4 samples routinely understates the population
# sigma, so an INFLUENCED verdict resting on that floor is over-confident. The
# guard (opt-in) widens the noise margin by a small-sample factor; the informational
# ``noise_is_thin`` flag is always recorded so a reader sees the caveat. A single
# repeat is the extreme of that: there is no estimate to widen, so ``noise_measured``
# records the floor as missing rather than small.
_THIN_REPEATS = 5


class OracleInfluence(str, Enum):
    """The oracle's verdict on whether the data influenced the finding."""

    INFLUENCED = "INFLUENCED"
    NOT_INFLUENCED = "NOT_INFLUENCED"
    UNDECIDABLE = "UNDECIDABLE"
    # The oracle did not run on this finding, so it has no influence verdict at
    # all. Distinct from UNDECIDABLE, where the oracle ran and the effect fell
    # inside the noise band: NOT_TESTED is the absence of a measurement, never a
    # measurement that came out ambiguous. A never-run row carries no effect
    # size, noise floor, or threshold, and no consumer may read a zero off it as
    # a measured value.
    NOT_TESTED = "NOT_TESTED"


class NotTestedReason(str, Enum):
    """Why the oracle did not produce an influence verdict for a finding.

    A typed field on the result, never string-matched from the English reason
    sentence, so a consumer branches on the reason without parsing prose. Each
    value names a distinct way the measurement could not be taken:

    - ``UNSUPPORTED_SHAPE``: no scramble family fits the finding's input shape,
      so there was no null to perturb with.
    - ``CRASHED_UNDER_NULL``: running the pipeline on a scrambled input raised,
      so the effect under that null is unknown; the traceback is recorded.
    - ``UNREDUCIBLE_VALUE``: a run produced a value the declared metric could
      not reduce to a comparable scalar.
    - ``NO_VALUE``: a run produced no value to compare at all.
    """

    UNSUPPORTED_SHAPE = "unsupported-shape"
    CRASHED_UNDER_NULL = "crashed-under-null"
    UNREDUCIBLE_VALUE = "unreducible-value"
    NO_VALUE = "no-value"


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
    # The three measurement numbers are None on a NOT_TESTED row and only there:
    # a never-run finding has no effect size, no noise floor, and no threshold,
    # and None is the ONLY legal value for those three when the oracle did not
    # run, so no consumer can read a zero off a never-run row as if it were a
    # measured value. On every other verdict they are floats.
    effect_size: "float | None"
    noise_floor: "float | None"
    decision_threshold: "float | None"
    reason: str
    base_values: tuple[float, ...] = ()
    perturbed_values: tuple[float, ...] = ()
    # One effect per perturbation, in the order they were tried: how far that
    # perturbation alone moved the finding from the base mean. effect_size is
    # the largest of these, so a reader can see which perturbation moved it.
    perturbation_effects: tuple[float, ...] = ()
    # The declared reducer used to reduce each finding to a scalar, so the
    # measurement artifact is auditable about how prose became a number.
    reducer: "MetricReducer | None" = None
    # The multiplicity the decision threshold was widened for (1 = a single
    # finding, no correction), and whether the noise floor rests on too few
    # repeats to be trusted. Both are recorded so the verdict is auditable.
    multiplicity: int = 1
    noise_is_thin: bool = False
    # Whether the noise floor was measured at all. False means a single base run,
    # so the floor is 0 by construction and run-to-run jitter is not ruled out:
    # a missing estimate, not a small one. Never claimed without the runs to back it.
    noise_measured: bool = False
    # Whether the pipeline was measured to be deterministic: the noise floor was
    # measured over more than one repeat AND came out exactly 0. Distinct from a
    # single unmeasured base run (``noise_measured`` False), where the floor is 0
    # by construction rather than by measurement. A deterministic pipeline has no
    # noise scale, so the threshold rests on the float-equality band, not sigmas.
    deterministic: bool = False
    # Set only on a NOT_TESTED row: the typed reason the oracle did not run, and
    # the traceback when a null crashed the pipeline. None on every measured row.
    not_tested_reason: "NotTestedReason | None" = None
    traceback: "str | None" = None

    @classmethod
    def not_tested(
        cls,
        reason: "NotTestedReason",
        *,
        detail: str = "",
        traceback: "str | None" = None,
        reducer: "MetricReducer | None" = None,
        perturbation_effects: tuple[float, ...] = (),
    ) -> "OracleResult":
        """Build a NOT_TESTED result: the oracle produced no influence verdict.

        The three measurement numbers are left None, the only legal value for a
        never-run row, so no consumer reads a zero as a measurement. ``detail``
        is folded into the English reason; the typed ``reason`` is what a
        consumer branches on.
        """
        sentence = f"the oracle did not run: {reason.value}"
        if detail:
            sentence += f" ({detail})"
        return cls(
            influence=OracleInfluence.NOT_TESTED,
            effect_size=None,
            noise_floor=None,
            decision_threshold=None,
            reason=sentence,
            perturbation_effects=perturbation_effects,
            reducer=reducer,
            not_tested_reason=reason,
            traceback=traceback,
        )

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
    determinism_rtol: float = 1e-6,
    determinism_atol: float = 0.0,
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
        on that data. Each is scored against the base on its own and the effect
        size is the largest move, so perturbations of opposite sign do not
        cancel; the per-perturbation effects are on the result.
    repeats:
        Runs per configuration. Above 1, the spread of the base runs measures
        the pipeline's run-to-run noise (LLM nondeterminism), which sets the
        floor a real effect must clear. Use temperature 0 / a pinned seed plus
        repeats to bound the noise honestly. At ``1`` (the default) no noise is
        measured at all: the floor is 0, so on a stochastic pipeline jitter alone
        can clear the threshold. The result records that as ``noise_measured``
        False and says so in its reason; raise repeats to measure the floor.
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
        ``1`` (the default) adds nothing: a single finding tried against a single
        perturbation needs no correction. The number of perturbations multiplies
        the family, since the effect is the max across them.
    thin_sigma_guard:
        When True, widen the noise margin by a small-sample factor whenever the
        noise floor rests on fewer than ``_THIN_REPEATS`` repeats (a thin pstdev
        understates the real sigma). Off by default, so the scalar path is
        unchanged; ``noise_is_thin`` is recorded either way. The guard cannot
        rescue ``repeats=1``: there is no sigma to widen, which is what
        ``noise_measured`` reports.
    determinism_rtol:
        The float-equality band for a DETERMINISTIC pipeline, one with no
        measurable run-to-run noise (``noise_floor`` 0). With no noise scale the
        threshold cannot come from sigmas, so a move is judged against a band
        relative to the finding's own magnitude: ``determinism_rtol * |base
        mean|``. A move inside that band is indistinguishable from summation-order
        artifacts (BLAS thread counts, reduction order) and reads UNDECIDABLE,
        never INFLUENCED; a move above it is a real dependency; a move of exactly
        0 is a provable invariant (NOT_INFLUENCED). This is what keeps the
        zero-config oracle from degenerating to exact float equality, where any
        nonzero jitter reads INFLUENCED. A domain ``effect_threshold`` still
        overrides it when larger.
    determinism_atol:
        An ABSOLUTE float-equality floor, added to the band above as its lower
        bound (``max(determinism_rtol * |base mean|, determinism_atol)``). The
        relative band vanishes when the finding's magnitude is ~0, so a finding
        that sits near zero has no float-equality band from ``determinism_rtol``
        alone; pass an absolute floor sized to the finding's own artifact scale
        to give a near-zero finding a band. Defaults to 0 so a finding with a
        real magnitude is unaffected.

    Returns
    -------
    OracleResult
        INFLUENCED when the perturbation moves the finding past the threshold;
        NOT_INFLUENCED when the finding holds still; UNDECIDABLE when the effect
        is real-signed but within the noise (or float-equality) band, never
        silently INFLUENCED.
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
    perturbed_runs = tuple(
        tuple(m(run_fn(pin)) for _ in range(repeats)) for pin in perturbed_inputs
    )
    perturbed_values = tuple(v for runs in perturbed_runs for v in runs)

    base_mean = statistics.fmean(base_values)
    # Each perturbation is a different way of changing the cited data, so each
    # is its own comparison against the base. Pooling them into one mean lets
    # opposing perturbations cancel, so take the largest move any one produced.
    perturbation_effects = tuple(
        abs(statistics.fmean(runs) - base_mean) for runs in perturbed_runs
    )
    effect_size = max(perturbation_effects)

    # Noise floor from run-to-run spread of the base configuration. With a
    # single run there is no measurable noise, so the floor is 0 and
    # effect_threshold alone decides.
    noise_measured = len(base_values) > 1
    noise_std = statistics.pstdev(base_values) if noise_measured else 0.0
    # No noise scale: the floor is exactly 0, either because it was measured over
    # more than one base run and came out 0 (a deterministic pipeline) OR because
    # a single base run never measured it. Both take the float-equality path
    # below, so the zero-config oracle does not degenerate to exact float
    # equality in either case (the spec requires the fix at repeats=1 and
    # repeats=5 alike). The two are told apart by ``noise_measured``: only the
    # measured case sets the ``deterministic`` field, and each carries its own
    # caveat in the reason.
    zero_noise = noise_std == 0.0

    # A noise floor estimated from too few repeats is thin (pstdev understates
    # the population sigma), and a single repeat estimates nothing at all. Always
    # record it; widen the margin only when the guard is on, so the default
    # scalar path is unchanged.
    noise_is_thin = repeats < _THIN_REPEATS
    sigma_multiplier = noise_multiplier
    # Taking the max over the perturbations is itself a multiple comparison, so
    # the family the threshold must hold for is the declared multiplicity times
    # the number of perturbations tried.
    family = multiplicity * len(perturbation_effects)
    if family > 1:
        # The extreme-value scale of the max of ``family`` standard draws:
        # the extra sigmas a family of that size needs to hold its false-influence
        # rate. Applied before the influence call, per finding.
        sigma_multiplier += math.sqrt(2.0 * math.log(family))
    if thin_sigma_guard and noise_is_thin:
        sigma_multiplier *= math.sqrt(_THIN_REPEATS / repeats)

    if not zero_noise:
        # Stochastic pipeline: run-to-run noise sets the scale. Unchanged path.
        noise_margin = sigma_multiplier * noise_std
        decision_threshold = max(effect_threshold, noise_margin)
        # UNDECIDABLE is a NOISE verdict: it applies only when noise, not the
        # domain floor, sets the threshold. When effect_threshold is the binding
        # constraint, an effect below it is domain-insignificant, not ambiguous.
        band_driven = noise_margin >= effect_threshold
        band_floor = noise_std
        undecidable_reason = (
            f"effect {effect_size:.4g} is within the noise band "
            f"(<= {decision_threshold:.4g}); undecidable, not called grounded"
        )
    else:
        # No measurable noise (floor 0): there is no noise scale, so the oracle
        # must not degenerate to exact float equality where any nonzero jitter
        # reads INFLUENCED. The threshold is the domain floor or the
        # float-equality band, whichever is larger; the band is relative to the
        # finding's magnitude plus an absolute floor for a finding whose
        # magnitude is ~0 (where a relative band alone would vanish). A move
        # inside that band is indistinguishable from summation-order artifacts
        # (BLAS thread counts, reduction order) and reads UNDECIDABLE; a move of
        # exactly 0 is a provable invariant (NOT_INFLUENCED). This is what makes
        # UNDECIDABLE reachable on the modal deterministic target.
        determinism_floor = max(
            determinism_rtol * abs(base_mean), determinism_atol
        )
        decision_threshold = max(effect_threshold, determinism_floor)
        band_driven = determinism_floor >= effect_threshold
        band_floor = 0.0
        undecidable_reason = (
            f"effect {effect_size:.4g} is within the float-equality band "
            f"(<= {decision_threshold:.4g}); undecidable, not distinguishable "
            f"from summation-order artifacts"
        )

    if effect_size > decision_threshold:
        influence = OracleInfluence.INFLUENCED
        reason = (
            f"perturbing the input moved the finding by {effect_size:.4g}, past "
            f"the {decision_threshold:.4g} threshold: the data influences it"
        )
    elif band_driven and effect_size > band_floor:
        # A move that clears the lower band edge but not the decision threshold:
        # for a stochastic pipeline that is one noise sd; for a deterministic one
        # it is any nonzero move inside the float-equality band. Real enough not
        # to call NOT_INFLUENCED, not clear enough for INFLUENCED.
        influence = OracleInfluence.UNDECIDABLE
        reason = undecidable_reason
    else:
        influence = OracleInfluence.NOT_INFLUENCED
        reason = (
            f"perturbing the input barely moved the finding ({effect_size:.4g} "
            f"<= {decision_threshold:.4g}): the finding does not depend on the data"
        )

    if zero_noise and noise_measured:
        # Measured-zero: the noise floor WAS measured, over more than one repeat,
        # and came out 0. A distinct state from an unmeasured floor, with its own
        # caveat, so a reader never confuses a pipeline shown to be deterministic
        # with one whose noise was simply never sampled.
        reason += (
            f" (deterministic pipeline: noise measured at 0 over {repeats} "
            f"repeats, so the threshold is the float-equality band, not a "
            f"noise margin)"
        )
    elif not noise_measured:
        # The threshold rested on a floor that was never measured. The caveat
        # belongs to the measurement, not to the verdict it produced, so every
        # reason carries it: a reader must be able to tell a missing floor from
        # one measured at 0, and the reason is the line a reader reads.
        reason += (
            " (no noise estimate: one base run, so the floor is unmeasured "
            "rather than measured small and run-to-run jitter is not ruled "
            "out; raise repeats to measure it)"
        )

    return OracleResult(
        influence=influence,
        effect_size=effect_size,
        noise_floor=noise_std,
        decision_threshold=decision_threshold,
        reason=reason,
        base_values=base_values,
        perturbed_values=perturbed_values,
        perturbation_effects=perturbation_effects,
        reducer=reducer,
        multiplicity=multiplicity,
        noise_is_thin=noise_is_thin,
        noise_measured=noise_measured,
        deterministic=zero_noise and noise_measured,
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
    # The oracle did not run, so there is no influence verdict to reconcile
    # against the flow verdict. Distinct from INCONCLUSIVE (the oracle ran and
    # could not decide): a row nothing measured must not borrow the meaning of a
    # row measured and found ambiguous, or a never-tested finding reads as an
    # accusation it never earned.
    NOT_TESTED = "NOT_TESTED"


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

    - anything + NOT_TESTED → NOT_TESTED: the oracle did not run, so there is no
      influence verdict to reconcile. Guarded FIRST, before every other branch,
      so a never-tested finding can never borrow another relation: a GROUNDED
      row left unguarded would fall through to CONSTRUCT_DIFFERENCE, issuing the
      cited-but-hollow accusation for a row nothing measured.
    - GROUNDED + INFLUENCED, UNGROUNDED + NOT_INFLUENCED → AGREE.
    - GROUNDED + NOT_INFLUENCED → CONSTRUCT_DIFFERENCE: the pipeline read the
      cited data and then did not use it. Flow without influence, not a bug.
    - OPAQUE + anything → OBSERVER_BLIND: the observer could not see, so there is
      nothing to reconcile against the oracle. Checked BEFORE the UNDECIDABLE
      branch so an OPAQUE row whose oracle could not decide keeps its opacity
      rather than collapsing to INCONCLUSIVE (two of the three coverage states
      folding into one).
    - UNGROUNDED + INFLUENCED → TENSION: the data demonstrably influences the
      finding, yet the observer saw no cited read. This is the one combination
      worth investigating, the observer likely missed a read (a coverage gap).
    - the remaining + UNDECIDABLE → INCONCLUSIVE: the oracle ran but could not
      decide above the noise floor.
    """
    if oracle is OracleInfluence.NOT_TESTED:
        return ReconcileResult(
            Reconciliation.NOT_TESTED,
            "the oracle did not run; no influence verdict to reconcile",
        )
    if grounding is ObservedGrounding.OPAQUE:
        return ReconcileResult(
            Reconciliation.OBSERVER_BLIND,
            "the observer could not see the scope; no flow verdict to reconcile",
        )
    if oracle is OracleInfluence.UNDECIDABLE:
        return ReconcileResult(
            Reconciliation.INCONCLUSIVE,
            "the oracle could not decide influence above the noise floor",
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
