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
import traceback as _traceback
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Sequence

from ._verdict import ObservedGrounding
from .scrambles import scramble_family

# Below this many repeats the base-run spread is a THIN estimate of the
# pipeline's noise: pstdev over 1-4 samples routinely understates the population
# sigma, so an INFLUENCED verdict resting on that floor is over-confident. The
# guard (opt-in) widens the noise margin by a small-sample factor; the informational
# ``noise_is_thin`` flag is always recorded so a reader sees the caveat. A single
# repeat is the extreme of that: there is no estimate to widen, so ``noise_measured``
# records the floor as missing rather than small.
_THIN_REPEATS = 5

# The label the unperturbed run is recorded under. Not a null name: it is the
# configuration every null is compared against.
_BASE_LABEL = "base"

# Every influence verdict carries this: the oracle grades a COOPERATING pipeline.
# A target that patches the observer, imports its internals, or fabricates reads
# is out of scope and no verdict here rules it out. The bound used to live in one
# module docstring almost nobody reads; it belongs on the verdict, where the claim
# is made.
THREAT_MODEL_STATEMENT = (
    "this verdict grades a pipeline that does not attack its auditor: a target "
    "that patches the observer or fabricates its reads is out of scope"
)


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
    value names a distinct way the measurement could not be taken, and each is
    produced by exactly one path in this module:

    - ``UNSUPPORTED_SHAPE``: no scramble family fits the finding's input shape,
      so there was no null to perturb with.
    - ``NULL_CONSTRUCTION_FAILED``: building the perturbed input raised, so the
      null itself never existed. A caller's ``perturb`` callable is the usual
      source. Distinct from a crash UNDER a null, where the null was built and
      the target then failed on it.
    - ``TARGET_FAILED``: the UNPERTURBED base run raised, so the target never ran
      successfully and no null is implicated. Kept distinct from
      ``CRASHED_UNDER_NULL`` because a broken target and a target broken BY a
      null route to different fixes, and bucketing them together would count a
      broken target as evidence about the null family's reach.
    - ``CRASHED_UNDER_NULL``: running the pipeline on a scrambled input raised,
      so the effect under that null is unknown; the traceback is recorded.
    - ``UNREDUCIBLE_VALUE``: a run produced a value the declared metric could
      not reduce to a comparable scalar.
    - ``NON_FINITE_VALUE``: a run reduced to NaN or an infinity, so there is no
      comparable number. A ratio whose denominator a null zeroes does this on the
      first null, and NaN compares False against every threshold, so an
      unguarded non-finite value would be silently counted as "the finding held
      still" and earn the finding the hollow verdict off a measurement that never
      happened.
    """

    UNSUPPORTED_SHAPE = "unsupported-shape"
    NULL_CONSTRUCTION_FAILED = "null-construction-failed"
    TARGET_FAILED = "target-failed"
    CRASHED_UNDER_NULL = "crashed-under-null"
    UNREDUCIBLE_VALUE = "unreducible-value"
    NON_FINITE_VALUE = "non-finite-value"


class NullOutcome(str, Enum):
    """How one null in the family was classified against the decision threshold.

    The router computes this once per null and the result carries it, so every
    reader (the reason sentence, :attr:`OracleResult.flat_nulls`, the blind-spot
    line) describes the same classification the verdict was routed on. Deriving
    it a second time from the effects is how a result comes to state one thing in
    its verdict and the opposite in its prose.
    """

    MOVED = "MOVED"
    # The effect landed inside the noise or float-equality band: it moved, but
    # not by enough to be told from jitter. Never "held invariant": the finding
    # did move, the measurement just cannot say whether the data caused it.
    AMBIGUOUS = "AMBIGUOUS"
    FLAT = "FLAT"


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
    # The name of each null in ``perturbation_effects`` order. A derived family
    # names them by what they do (``zeroed``, ``permuted``, ...) so a reader can
    # see which null the finding held invariant under; a caller-supplied family
    # gets generic names (``perturbation`` for a callable, ``perturbation-0`` ...
    # for a sequence). Empty only on a NOT_TESTED row that never ran a null.
    scramble_names: tuple[str, ...] = ()
    # The declared reducer used to reduce each finding to a scalar, so the
    # measurement artifact is auditable about how prose became a number.
    reducer: "MetricReducer | None" = None
    # The multiplicity the decision threshold was widened for (1 = a single
    # finding, no correction), and whether the noise floor rests on too few
    # repeats to be trusted. Both are recorded so the verdict is auditable.
    multiplicity: int = 1
    # Whether the multiplicity and family-size widening actually reached the
    # decision threshold. False on a pipeline with no measurable noise, where
    # there is no sigma to widen and the threshold is the float-equality band
    # instead: the correction was computed and did not apply. Recorded because
    # ``multiplicity`` alone reads as "this was corrected for", and on the modal
    # deterministic target it was not.
    multiplicity_applied: bool = False
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
    # How the router classified each null, in ``scramble_names`` order. Every
    # reader of "which nulls held still" reads THIS rather than re-deriving it
    # from the effects, so no line on the result can contradict the verdict.
    # Empty on a NOT_TESTED row, where no null was classified.
    null_outcomes: "tuple[NullOutcome, ...]" = ()
    # Nulls the shape would normally supply that this input ruled out, because
    # they would have been identical to the base (permuting a constant sequence
    # changes nothing). The verdict rests on a narrower family than the shape
    # normally gives, and saying which nulls are missing is the difference
    # between a narrow verdict and a verdict that reads as broad.
    dropped_nulls: "tuple[str, ...]" = ()
    # True when the caller supplied the nulls instead of letting the shape derive
    # them. A chosen null is a place to fish: pick one the finding is provably
    # invariant to and it reads NOT_INFLUENCED however honest the pipeline is. The
    # oracle still measures what it is asked to, but the result says who chose,
    # so a NOT_INFLUENCED off a caller's own null is never read as the derived
    # family's verdict.
    caller_chose_nulls: bool = False

    @classmethod
    def not_tested(
        cls,
        reason: "NotTestedReason",
        *,
        detail: str = "",
        traceback: "str | None" = None,
        reducer: "MetricReducer | None" = None,
        perturbation_effects: tuple[float, ...] = (),
        dropped_nulls: "tuple[str, ...]" = (),
        caller_chose_nulls: bool = False,
    ) -> "OracleResult":
        """Build a NOT_TESTED result: the oracle produced no influence verdict.

        The three measurement numbers are left None, the only legal value for a
        never-run row, so no consumer reads a zero as a measurement. ``detail``
        is folded into the English reason; the typed ``reason`` is what a
        consumer branches on.

        ``dropped_nulls`` and ``caller_chose_nulls`` describe the FAMILY, not the
        measurement, so they are carried here too: they are known as soon as the
        family resolves, and a row that says the caller did not choose the nulls
        when the caller did is a false statement on the field added to keep a
        caller's own null from reading as the derived family's.
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
            dropped_nulls=dropped_nulls,
            caller_chose_nulls=caller_chose_nulls,
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

    def _names(self) -> "tuple[str, ...]":
        """The null names, falling back to positional labels for an old record."""
        return self.scramble_names or tuple(
            f"perturbation-{i}" for i in range(len(self.perturbation_effects))
        )

    def _nulls_classified(self, outcome: "NullOutcome") -> "tuple[str, ...]":
        return tuple(
            name
            for name, got in zip(self._names(), self.null_outcomes)
            if got is outcome
        )

    @property
    def flat_nulls(self) -> "tuple[str, ...]":
        """The nulls the finding held invariant under: this verdict's blind spots.

        A null the finding held still under is one this verdict does not rule out
        a dependence hidden behind. For a hollow finding that is every null; for
        an influenced one, none; for the honest-invariant case, the
        marginal-preserving nulls it was invariant to. Empty on a NOT_TESTED row,
        where nothing was measured.

        Read off the router's own classification, never re-derived by comparing
        effects to the threshold: a null whose effect landed inside the band moved
        and is not flat, and re-deriving is what let a result call a null
        "invariant" that the verdict had counted as ambiguous.
        """
        return self._nulls_classified(NullOutcome.FLAT)

    @property
    def ambiguous_nulls(self) -> "tuple[str, ...]":
        """The nulls whose effect landed inside the noise or float-equality band."""
        return self._nulls_classified(NullOutcome.AMBIGUOUS)

    def blind_spot_line(self) -> str:
        """One line naming what this verdict does not see, with the threat bound.

        Built from the router's classification, so it can never disagree with the
        verdict beside it: the nulls the finding held still under, the nulls whose
        move was inside the band, and the nulls this input ruled out before
        anything ran. Every verdict carries the statement that the oracle grades a
        cooperating pipeline.
        """
        if self.influence is OracleInfluence.NOT_TESTED:
            return f"Influence not tested. {THREAT_MODEL_STATEMENT}."
        parts: list[str] = []
        flat = self.flat_nulls
        if flat:
            parts.append(
                f"Held invariant under {', '.join(flat)}, so the verdict does not "
                f"rule out a dependence those nulls cannot see."
            )
        ambiguous = self.ambiguous_nulls
        if ambiguous:
            parts.append(
                f"Moved under {', '.join(ambiguous)} by less than the decision "
                f"threshold, which is a move the measurement cannot tell from "
                f"jitter."
            )
        if not flat and not ambiguous:
            parts.append("Moved under every null that ran.")
        if self.dropped_nulls:
            parts.append(
                f"The input ruled out {', '.join(self.dropped_nulls)}: those "
                f"nulls would have been identical to it, so nothing here tested "
                f"what they test."
            )
        if self.caller_chose_nulls:
            parts.append(
                "The nulls were chosen by the caller, not derived from the "
                "finding's shape, so this verdict is only as strong as that "
                "choice."
            )
        return " ".join(parts) + " " + THREAT_MODEL_STATEMENT + "."


class NoPerturbationsError(ValueError):
    """A caller passed an empty ``perturb`` sequence: there is nothing to run.

    Raised, never turned into a NOT_TESTED result. NOT_TESTED records that the
    oracle could not measure something; an empty null list is a caller handing it
    nothing to measure, which is a bug in the call and must not be laundered into
    a measurement outcome a report would then count.
    """


class _NullFailure(Exception):
    """Internal: a run, or its reduction, failed under one specific null.

    Carries the typed NOT_TESTED reason, the null's name, and the traceback, so
    the oracle can turn a crash under a scramble into a NOT_TESTED result naming
    which null failed rather than aborting the whole measurement. A subclass of
    ``Exception`` (never ``BaseException``), so a ``KeyboardInterrupt`` still
    escapes the guard and ends the run.
    """

    def __init__(self, reason: "NotTestedReason", label: str, tb: str):
        super().__init__(reason.value)
        self.reason = reason
        self.label = label
        self.tb = tb


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
    perturb: "Callable[[Any], Any] | Sequence[Any] | None" = None,
    *,
    repeats: int = 1,
    metric: "Callable[[Any], float] | None" = None,
    effect_threshold: float = 0.0,
    noise_multiplier: float = 3.0,
    multiplicity: int = 1,
    thin_sigma_guard: bool = False,
    determinism_rtol: float = 1e-6,
    determinism_atol: float = 0.0,
    on_progress: "Callable[[int, int], None] | None" = None,
) -> OracleResult:
    """Measure whether the cited data causally influences the finding.

    The measurement is a five-step pipeline::

        base_input                                                  run_fn
            |                                                         |
            v                                                         v
        [1] derive the null family from the data shape        [2] run the base
            (scalar / mapping / sequence), or use the             and each null
            caller's perturb; no family -> NOT_TESTED             repeats times,
            (unsupported-shape); a null that would                guarded: a crash,
            equal the base is dropped and recorded                an unreducible or
            |                                                     non-finite value
            |                                                     -> NOT_TESTED
            +--------------------------> nulls ------------------------+
                                                                       v
        [5] route on the PROFILE            [4] set the threshold  [3] per-null
            flat everywhere -> NOT_INFLUENCED   from noise (sigma)     effect =
            moves everywhere -> INFLUENCED      or, at zero noise,     |mean(null)
            moves under some  -> UNDECIDABLE    the float-equality      - mean(base)|
            band lands a null -> UNDECIDABLE    band            <-------+
            |
            v
        OracleResult(influence, effect_size, perturbation_effects, ...)

    Parameters
    ----------
    run_fn:
        Runs the pipeline on an input and returns the finding. Called with the
        base input and each perturbed input.
    base_input:
        The unperturbed input.
    perturb:
        ``None`` (the default) derives the whole null family from the finding's
        data shape; a callable maps the base input to ONE perturbed input; a
        sequence supplies the nulls directly. A caller-supplied family is recorded
        on the result as :attr:`OracleResult.caller_chose_nulls`, because a chosen
        null is a place to fish and a NOT_INFLUENCED off the caller's own null is
        a weaker statement than one off the derived family. In particular a single
        caller-supplied null cannot reach the mixed profile at all, so the
        false-hollow protection below cannot fire for it: a genuine mean measured
        against a permutation the caller picked reads NOT_INFLUENCED, which is
        exactly what deriving the family prevents. See
        :func:`mareforma.observe.scrambles.scramble_family`). Deriving the family
        is the default because a chosen null is a place to fish: the finding's
        own shape picks the family, the caller does not. An input shape with no
        family yields a NOT_TESTED result (reason ``unsupported-shape``), never a
        verdict. Each null is scored against the base on its own and the effect
        size is the largest move, so nulls of opposite sign do not cancel; the
        per-null effects are on the result, and the verdict routes on their
        PROFILE, not on the single largest move. The profile rule governs a
        caller-supplied sequence exactly as it governs the derived family: a
        finding that moves under some nulls and holds invariant under others is
        UNDECIDABLE, whoever chose the nulls, since an invariant under a valid
        null is the honest-hard case rather than a clean pass.
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
        calls at a rate that grows with the count. The correction adds
        ``sqrt(2 * ln(multiplicity * n_nulls))`` sigmas to the multiplier: the
        number of nulls counts because the effect is the max across them, which
        is itself a multiple comparison. That is the scale of the largest
        spurious deviation expected across a family that size, not a quantile, so
        it controls no stated error rate; it is a widening sized to the family,
        and calling it a family-wise guarantee would overstate it. The control is
        applied BEFORE any influence number is computed. ``1`` (the default) is
        NOT a no-op on the zero-config path: the derived family has several nulls,
        so the widening fires from the null count alone.

        It has NO effect on a pipeline with no measurable noise, which is the
        modal deterministic target: with no sigma to widen, the threshold is the
        float-equality band and a multiplicity of 1 and of 10,000 give the same
        answer. :attr:`OracleResult.multiplicity_applied` records whether the
        widening actually reached the threshold, so a corpus report cannot claim a
        multiplicity-controlled rate it never got.
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
    on_progress:
        Optional callback invoked after every run of the pipeline, as
        ``on_progress(done, total)`` where ``total`` is
        ``repeats * (1 + number of nulls)``. One call can run the pipeline twenty
        or more times with no other output, so a caller measuring a slow target
        can surface progress. Defaults to None (no callback).

    Nothing about the target aborts the measurement into an exception: a crash, a
    value the reducer cannot reduce, a value that reduces to NaN or an infinity,
    and a null that could not be built all return NOT_TESTED naming where it
    happened and carrying the traceback, with reasons ``target-failed`` (the
    unperturbed base run), ``crashed-under-null``, ``unreducible-value``,
    ``non-finite-value`` and ``null-construction-failed``. A ``KeyboardInterrupt``
    still ends the run.

    Raises
    ------
    NoPerturbationsError
        When ``perturb`` is an empty sequence. A caller handing the oracle nothing
        to measure is a bug in the call, not a measurement outcome, so it raises
        rather than becoming a NOT_TESTED row a report would then count.

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

    # Resolve the null family before running anything: when the family is derived
    # from the shape and the shape has none, the finding is NOT_TESTED and no run
    # happens at all.
    try:
        resolved = _resolve_perturbations(base_input, perturb)
    except NoPerturbationsError:
        raise  # a caller bug, not a measurement outcome; see the class docstring
    except Exception:  # noqa: BLE001 — building a null is not the target's fault
        return OracleResult.not_tested(
            NotTestedReason.NULL_CONSTRUCTION_FAILED,
            detail="building the perturbed input raised",
            traceback=_traceback.format_exc(),
            reducer=reducer,
            caller_chose_nulls=perturb is not None,
        )
    if resolved is None:
        return OracleResult.not_tested(
            NotTestedReason.UNSUPPORTED_SHAPE,
            detail="no scramble family fits the finding's input shape",
            reducer=reducer,
        )
    perturbed_inputs, null_names, dropped_nulls, caller_chose = resolved

    # The pipeline runs ``repeats`` times on the base and on each null, so the
    # total is ``repeats * (1 + len(family))`` invocations of run_fn. A long
    # measurement (many repeats, many nulls) can report progress through
    # ``on_progress(done, total)``, called after every run.
    total_runs = repeats * (1 + len(perturbed_inputs))
    done = 0

    def _measure(label: str, pin: Any, *, is_base: bool = False) -> tuple:
        # Run and reduce one configuration, guarding each step. A crash in the
        # pipeline is expected input (a null can kill a fragile target), a value
        # the reducer cannot turn into a scalar is a different failure, and a
        # value that reduces to NaN or an infinity is a third: there is no
        # comparable number, and a non-finite value compares False against every
        # threshold, so leaving it in would have it counted as "the finding held
        # still". All three abort THIS measurement into a NOT_TESTED naming the
        # configuration, never a raised exception up the call stack.
        # KeyboardInterrupt / SystemExit are BaseException, so they are not caught
        # and an abort the operator asked for still ends the run.
        nonlocal done
        crashed = (
            NotTestedReason.TARGET_FAILED if is_base
            else NotTestedReason.CRASHED_UNDER_NULL
        )
        vals = []
        for _ in range(repeats):
            try:
                finding = run_fn(pin)
            except Exception:  # noqa: BLE001 — a target crash is expected input
                raise _NullFailure(crashed, label, _traceback.format_exc())
            try:
                value = m(finding)
                # Inside the same guard as the reduction. A reducer that hands
                # back None, a string, a complex, or an int too large for a
                # float is a value that could not be reduced to a comparable
                # scalar, which is what UNREDUCIBLE_VALUE means; checked outside,
                # math.isfinite raises those out of the call and breaks the
                # promise that nothing about the target aborts the measurement.
                finite = math.isfinite(value)
            except Exception:  # noqa: BLE001 — the reducer could not reduce it
                raise _NullFailure(
                    NotTestedReason.UNREDUCIBLE_VALUE, label, _traceback.format_exc()
                )
            if not finite:
                raise _NullFailure(
                    NotTestedReason.NON_FINITE_VALUE, label,
                    f"the run reduced to {value!r}, which has no comparable "
                    f"magnitude",
                )
            vals.append(value)
            done += 1
            if on_progress is not None:
                on_progress(done, total_runs)
        return tuple(vals)

    def _not_tested_from(failure: "_NullFailure") -> OracleResult:
        # The base is not a null, so it is never described as one, whatever went
        # wrong on it: a reader bucketing these must be able to tell a broken
        # target from a target a null broke.
        where = (
            "on the unperturbed base run"
            if failure.label == _BASE_LABEL
            else f"under the {failure.label!r} null"
        )
        return OracleResult.not_tested(
            failure.reason,
            detail=where,
            traceback=failure.tb,
            reducer=reducer,
            dropped_nulls=tuple(dropped_nulls),
            caller_chose_nulls=caller_chose,
        )

    try:
        base_values = _measure(_BASE_LABEL, base_input, is_base=True)
        perturbed_runs = tuple(
            _measure(name, pin)
            for name, pin in zip(null_names, perturbed_inputs)
        )
    except _NullFailure as failure:
        return _not_tested_from(failure)
    perturbed_values = tuple(v for runs in perturbed_runs for v in runs)

    base_mean = statistics.fmean(base_values)
    # Each perturbation is a different way of changing the cited data, so each
    # is its own comparison against the base. Pooling them into one mean lets
    # opposing perturbations cancel, so take the largest move any one produced.
    perturbation_effects = tuple(
        abs(statistics.fmean(runs) - base_mean) for runs in perturbed_runs
    )
    # Every value that fed these was finite, but the arithmetic between them can
    # still overflow (two magnitudes near the float ceiling). An infinite effect
    # is not a large effect, it is the absence of a comparable number, and it must
    # not reach the router, where it would be compared against a threshold and
    # counted as a move.
    for name, effect in zip(null_names, perturbation_effects):
        if not math.isfinite(effect):
            return OracleResult.not_tested(
                NotTestedReason.NON_FINITE_VALUE,
                detail=(
                    f"the effect under the {name!r} null came out {effect!r}, "
                    f"which has no comparable magnitude"
                ),
                reducer=reducer,
                dropped_nulls=tuple(dropped_nulls),
                caller_chose_nulls=caller_chose,
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

    # Route on the PROFILE of effects across the null family, not on the single
    # largest move. Classify each null against the shared threshold:
    #   MOVED     the effect cleared the decision threshold;
    #   AMBIGUOUS the band drives the threshold and the effect sits above the
    #             band's lower edge but below the threshold (a noise or
    #             float-equality move that cannot be called either way);
    #   FLAT      otherwise, the finding held still under that null.
    # The profile decides:
    #   any AMBIGUOUS      -> UNDECIDABLE, a null landed inside the band;
    #   every null MOVED   -> INFLUENCED, the finding depends on the data;
    #   no null MOVED      -> NOT_INFLUENCED, flat under the whole family (hollow);
    #   some MOVED, some FLAT -> UNDECIDABLE, the finding is a provable invariant
    #             of at least one valid null (a mean under a permutation), so it
    #             is not called hollow. This is the false-hollow discipline: an
    #             invariant reads UNDECIDABLE, never NOT_INFLUENCED.
    def _classify(effect: float) -> NullOutcome:
        if effect > decision_threshold:
            return NullOutcome.MOVED
        if band_driven and band_floor < effect:
            return NullOutcome.AMBIGUOUS
        return NullOutcome.FLAT

    null_outcomes = tuple(_classify(e) for e in perturbation_effects)
    moved = [o is NullOutcome.MOVED for o in null_outcomes]
    ambiguous = [o is NullOutcome.AMBIGUOUS for o in null_outcomes]
    if any(ambiguous):
        influence = OracleInfluence.UNDECIDABLE
        reason = undecidable_reason
    elif all(moved):
        influence = OracleInfluence.INFLUENCED
        reason = (
            f"the finding moved past the {decision_threshold:.4g} threshold under "
            f"every null (largest move {effect_size:.4g}): the data influences it"
        )
    elif not any(moved):
        influence = OracleInfluence.NOT_INFLUENCED
        reason = (
            f"the finding stayed within {decision_threshold:.4g} under every null "
            f"(largest move {effect_size:.4g}): it does not depend on the data"
        )
    else:
        influence = OracleInfluence.UNDECIDABLE
        held = [n for n, mv in zip(null_names, moved) if not mv]
        held_note = f" (held invariant under: {', '.join(held)})" if held else ""
        reason = (
            f"the finding moved under some nulls but held invariant under others"
            f"{held_note}: a provable invariant of at least one valid null, "
            f"undecidable, not called hollow"
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

    # What the family could not cover belongs on the same line as the verdict it
    # produced, not only in a blind-spot line a reader has to ask for. A verdict
    # that says "under every null" while the input silently dropped half the
    # family reads as broader than it is.
    if dropped_nulls:
        reason += (
            f" (the input ruled out the {', '.join(dropped_nulls)} "
            f"null{'s' if len(dropped_nulls) > 1 else ''}, which would have been "
            f"identical to it, so the family here is narrower than the shape "
            f"normally supplies)"
        )
    if caller_chose:
        reason += (
            " (nulls chosen by the caller, not derived from the finding's shape)"
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
        scramble_names=tuple(null_names),
        reducer=reducer,
        multiplicity=multiplicity,
        multiplicity_applied=band_driven and not zero_noise and family > 1,
        noise_is_thin=noise_is_thin,
        noise_measured=noise_measured,
        deterministic=zero_noise and noise_measured,
        null_outcomes=null_outcomes,
        dropped_nulls=tuple(dropped_nulls),
        caller_chose_nulls=caller_chose,
    )


def influence_sweep(
    findings: "Sequence[tuple]",
    **oracle_kwargs,
) -> "list[OracleResult]":
    """Run the oracle over a corpus, correcting multiplicity from the count.

    Each finding is one hypothesis in a family, so a fixed ``noise_multiplier``
    lets the noisiest of them clear the bar by chance and the corpus produces
    false INFLUENCED calls at a rate that grows with the count. The correction is
    ``multiplicity = number of findings``, and leaving it at the default 1 is easy
    to forget and silently overcounts influence across a corpus. This runs each
    finding with the multiplicity computed from the corpus size, so the caller
    passes the corpus and never has to remember to pass the count.

    ``findings`` is a sequence of ``(run_fn, base_input)`` or
    ``(run_fn, base_input, metric)`` tuples. ``oracle_kwargs`` are forwarded to
    every :func:`perturbation_oracle` call; passing ``multiplicity`` is an error,
    since the sweep owns it. A per-finding ``metric`` in the tuple overrides any
    ``metric`` in ``oracle_kwargs`` for that finding.
    """
    if "multiplicity" in oracle_kwargs:
        raise TypeError(
            "influence_sweep computes multiplicity from the corpus size; do not "
            "pass it"
        )
    specs = list(findings)
    multiplicity = max(1, len(specs))
    shared_metric = oracle_kwargs.pop("metric", None)
    results: list[OracleResult] = []
    for spec in specs:
        run_fn, base_input = spec[0], spec[1]
        metric = spec[2] if len(spec) > 2 else shared_metric
        results.append(
            perturbation_oracle(
                run_fn, base_input,
                multiplicity=multiplicity, metric=metric, **oracle_kwargs,
            )
        )
    return results


def _resolve_perturbations(base_input, perturb):
    """Return ``(inputs, names, dropped, caller_chose)`` or ``None`` for no family.

    ``None`` is returned only when ``perturb`` is derived from the shape and the
    shape has no scramble family, which the caller turns into a NOT_TESTED result.
    A caller-supplied ``perturb`` always resolves: names are generic, since the
    caller, not the shape library, chose the nulls, and ``caller_chose`` records
    that so the verdict can say whose nulls it rests on. ``dropped`` names the
    nulls the shape would have supplied that this input ruled out; a
    caller-supplied family drops nothing, because the caller's list is the family.

    Raises whatever building a perturbed input raises (a caller's ``perturb``
    callable, a container rebuild); the caller guards that into a NOT_TESTED with
    reason ``null-construction-failed``, so a null that cannot be built is never
    charged to the target as a crash.
    """
    if perturb is None:
        family = scramble_family(base_input)
        if family is None:
            return None
        return (
            [s.perturbed for s in family],
            [s.name for s in family],
            tuple(family.dropped),
            False,
        )
    if callable(perturb):
        return ([perturb(base_input)], ["perturbation"], (), True)
    perturbed = list(perturb)
    if not perturbed:
        raise NoPerturbationsError(
            "perturb must yield at least one perturbed input"
        )
    names = [f"perturbation-{i}" for i in range(len(perturbed))]
    return (perturbed, names, (), True)


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
