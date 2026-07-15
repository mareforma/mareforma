"""``mareforma reexec``: re-run a recorded pipeline and check the number reproduces.

A computed-PROXY faithfulness check. Given a *recorded run*, a declarative
record that names how to re-execute a pipeline and the number it reported , 
:func:`reexec` re-executes that pipeline in a clean scope and compares the
re-produced number to the recorded one within a declared tolerance.

What this attests, and what it does not:

- It attests REPRODUCIBILITY, not TRUTH. A ``REPRODUCED`` verdict says the
  recorded pipeline, re-run, yields the recorded number again. It says nothing
  about whether that number is correct.
- It is not INDEPENDENCE. Re-running the same code on the same inputs is not an
  independent line of evidence; a match is a same-arm replay, not convergence.

Both residuals are named on every verdict and carried onto the trust map at the
PROXY tier, so faithfulness never reads as truth or as independence.

The load-bearing honesty rule: **never return ``REPRODUCED`` where the
re-execution could not actually run.** A run the recorder marked non-reexecutable
(world contact, private data, expensive compute), a pipeline whose entry point
will not resolve, a re-execution that raises, or one that returns a non-number , 
each is ``COULD_NOT_REEXECUTE``. The verdict is three-valued precisely so an
inconclusive re-run is honest, never a false ``REPRODUCED`` and never silently a
``DIVERGED`` (a broken re-run is not evidence of divergence).

Trust scope, like :mod:`mareforma.adapters.tooluniverse.replay`: reexec proves
only that re-executing the recorded pipeline yields the recorded number. It does
not verify the integrity of the recorded run's provenance; a caller who needs to
trust the record itself must verify its signature separately. It also re-executes
the recorded pipeline's code, so a caller must only reexec records it trusts.

Isolation scope: the re-run gets a fresh working directory only, not a sandbox.
The network, environment, and absolute paths stay live, so the world-contact and
private-data exclusions rest on the recorder honestly declaring
``reexecutable: false``; an undeclared world-contact run will re-execute.
"""
from __future__ import annotations

import json
import math
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

# Version of the reexec record shape emitted onto the trust map, so a future
# revision of the faithfulness signal is distinguishable rather than silently
# reinterpreted.
REEXEC_RECORD_VERSION = "v0.3.10"

# Declared-tolerance defaults. Both zero means "exact match unless the record
# declares a tolerance": the record states how close counts as reproduced, and
# nothing is silently slackened on its behalf. A deterministic pipeline
# reproduces exactly; a numeric pipeline that needs slack declares it.
DEFAULT_ABS_TOLERANCE = 0.0
DEFAULT_REL_TOLERANCE = 0.0

# The reasons a recorder may mark a run non-reexecutable. Any of these forces
# COULD_NOT_REEXECUTE without running: the re-execution genuinely cannot be done
# faithfully in a clean scope, so faithfulness is unknown, never assumed.
NON_REEXECUTABLE_REASONS = frozenset(
    {"world_contact", "private_data", "expensive_compute"}
)


class FaithfulnessVerdict(str, Enum):
    """The three-valued outcome of a re-execution faithfulness check.

    - ``REPRODUCED``, the pipeline re-ran and matched the recorded number
      within the declared tolerance.
    - ``DIVERGED``, the pipeline re-ran and produced a different number.
    - ``COULD_NOT_REEXECUTE``, the re-execution could not run (declared
      non-reexecutable, unresolvable, raised, or returned no number); the
      honest verdict when there is no number to compare.
    """

    REPRODUCED = "REPRODUCED"
    DIVERGED = "DIVERGED"
    COULD_NOT_REEXECUTE = "COULD_NOT_REEXECUTE"


class MalformedRunError(ValueError):
    """Raised when the recorded run does not decode to a well-formed run record.

    Distinct from a ``COULD_NOT_REEXECUTE`` verdict: that is an honest answer
    about a well-formed run the checker could not re-execute. A malformed record
    is a usage error, there is nothing to check.
    """


def _tolerance_is_wide(
    recorded: float, reproduced: float, atol: float, rtol: float
) -> bool:
    """True when the declared tolerance is wide enough to make the match weak.

    A relative tolerance of 100%+, or an absolute slack as large as the recorded
    magnitude itself, means almost any number would "reproduce". The REPRODUCED
    verdict still stands (the recorder declared the tolerance), but it is flagged
    so a generous tolerance can never pass silently as a clean match.
    """
    slack = max(atol, rtol * max(abs(recorded), abs(reproduced)))
    return rtol >= 1.0 or (abs(recorded) > 0.0 and slack >= abs(recorded))


def _conclusive_residual(atol: float, rtol: float, *, wide: bool) -> str:
    """Residual for a conclusive (REPRODUCED / DIVERGED) verdict.

    Names the tolerance the comparison used and both bounds the proxy does NOT
    cross, so a reader never mistakes faithfulness for correctness or for
    independence, and a match obtained via a generous tolerance is never silent.
    """
    residual = (
        f"reproducibility proxy (compared within tolerance abs={atol}, rel={rtol}): "
        "a re-run of the same recorded pipeline on the same inputs; reproducible is "
        "not correct, and a same-arm re-run is not an independent line of evidence"
    )
    if wide:
        residual += (
            "; WARNING: the declared tolerance is wide relative to the recorded "
            "magnitude, so this match is weak evidence of reproduction"
        )
    return residual


@dataclass(frozen=True)
class ReexecResult:
    """The outcome of a :func:`reexec` check, with the residual always named.

    ``recorded_value`` is the number the run reported; ``reproduced_value`` is
    what the re-execution produced (``None`` when it could not run).
    ``tolerance`` / ``rel_tolerance`` are the declared bounds the comparison
    used. ``residual`` names what the verdict does NOT cover, the reproducible
    != correct / != independent bound on a conclusive verdict, or the reason the
    re-execution could not run.
    """

    verdict: FaithfulnessVerdict
    recorded_value: float | None
    reproduced_value: float | None
    tolerance: float
    rel_tolerance: float
    residual: str
    run_id: str | None = None

    @property
    def reproduced(self) -> bool:
        """True only for a ``REPRODUCED`` verdict (never for could-not-reexecute)."""
        return self.verdict is FaithfulnessVerdict.REPRODUCED

    def to_dict(self) -> dict:
        return {
            "verdict": self.verdict.value,
            "recorded_value": self.recorded_value,
            "reproduced_value": self.reproduced_value,
            "tolerance": self.tolerance,
            "rel_tolerance": self.rel_tolerance,
            "residual": self.residual,
            "run_id": self.run_id,
        }

    def to_map_record(self) -> dict:
        """The record the trust map consumes to place a PROXY faithfulness row."""
        return {
            "version": REEXEC_RECORD_VERSION,
            "verdict": self.verdict.value,
            "recorded_value": self.recorded_value,
            "reproduced_value": self.reproduced_value,
            "tolerance": self.tolerance,
            "residual": self.residual,
        }


def _as_number(value: Any) -> float | None:
    """Coerce a re-produced result to a float, or ``None`` if it is not a number.

    ``bool`` is rejected: a reported measurement is a magnitude, not a flag, and
    Python's ``bool``-is-``int`` would otherwise let ``True`` compare equal to a
    recorded ``1.0``.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        f = float(value)
        return f if math.isfinite(f) else None
    return None


def _normalize_run(run: Any) -> dict:
    """Coerce *run* (a record dict, or a path to a JSON record) into a run dict.

    Raises :class:`MalformedRunError` on anything that is not a well-formed
    record: a missing/bad ``reported_value``, a bad ``tolerance``, or a declared
    non-reexecutable reason that is not one of :data:`NON_REEXECUTABLE_REASONS`.
    """
    if isinstance(run, (str, os.PathLike)):
        path = Path(run)
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise MalformedRunError(f"could not read run record {path}: {exc}") from exc
        except ValueError as exc:
            raise MalformedRunError(f"run record {path} is not valid JSON: {exc}") from exc
    elif isinstance(run, Mapping):
        record = dict(run)
    else:
        raise MalformedRunError(
            f"run must be a record dict or a path to one, got {type(run).__name__}"
        )

    if not isinstance(record, Mapping):
        raise MalformedRunError(
            f"run record must be a JSON object, got {type(record).__name__}"
        )

    reported = _as_number(record.get("reported_value"))
    if reported is None:
        raise MalformedRunError(
            "run record is missing a numeric 'reported_value' (the recorded number)"
        )
    record["reported_value"] = reported

    atol = record.get("tolerance", DEFAULT_ABS_TOLERANCE)
    rtol = record.get("rel_tolerance", DEFAULT_REL_TOLERANCE)
    for name, val in (("tolerance", atol), ("rel_tolerance", rtol)):
        num = _as_number(val)
        if num is None or num < 0:
            raise MalformedRunError(
                f"run record '{name}' must be a non-negative number, got {val!r}"
            )
        record[name] = num

    if not record.get("reexecutable", True):
        reason = record.get("not_reexecutable_reason")
        if reason not in NON_REEXECUTABLE_REASONS:
            raise MalformedRunError(
                "a non-reexecutable run must declare 'not_reexecutable_reason' as "
                f"one of {sorted(NON_REEXECUTABLE_REASONS)}, got {reason!r}"
            )
    return record


@contextmanager
def _clean_scope() -> Iterator[Path]:
    """Run the re-execution in a fresh temporary working directory.

    A fresh cwd keeps a re-run from reading files the original run left behind in
    its own directory, so a match is not an artefact of leftover state. The
    previous cwd is always restored.

    This isolates the working directory ONLY; it is not a sandbox. The network,
    environment variables (including any API keys), and absolute paths stay live,
    so a pipeline that reaches the world or reads private data still does so. A
    run that cannot be re-executed faithfully must declare ``reexecutable: false``;
    the checker cannot detect undeclared world contact.
    """
    prev = os.getcwd()
    with tempfile.TemporaryDirectory(prefix="mareforma-reexec-") as tmp:
        os.chdir(tmp)
        try:
            yield Path(tmp)
        finally:
            os.chdir(prev)


def _resolve_target(
    pipeline: Mapping[str, Any], registry: "Mapping[str, Callable[..., Any]] | None"
) -> Callable[..., Any]:
    """Resolve the pipeline's ``target`` to a callable.

    A ``registry`` (name -> callable) is consulted first, so a caller can inject
    the pipeline without a dotted path. Otherwise ``target`` is a ``module:attr``
    dotted path resolved by import. Raises so the caller can turn an unresolvable
    target into COULD_NOT_REEXECUTE rather than a crash.
    """
    target = pipeline.get("target")
    if not isinstance(target, str) or not target:
        raise MalformedRunError("pipeline is missing a 'target' entry point")
    if registry is not None and target in registry:
        return registry[target]
    if ":" not in target:
        raise MalformedRunError(
            f"pipeline target {target!r} is not in the registry and is not a "
            "'module:attr' dotted path"
        )
    from importlib import import_module

    module_path, _, attr = target.partition(":")
    module = import_module(module_path)
    fn = getattr(module, attr)
    if not callable(fn):
        raise TypeError(f"pipeline target {target!r} resolved to a non-callable")
    return fn


def _could_not(
    record: dict, reason: str, *, reproduced: float | None = None
) -> ReexecResult:
    """Build a COULD_NOT_REEXECUTE result carrying *reason* as the residual."""
    return ReexecResult(
        verdict=FaithfulnessVerdict.COULD_NOT_REEXECUTE,
        recorded_value=record["reported_value"],
        reproduced_value=reproduced,
        tolerance=record["tolerance"],
        rel_tolerance=record["rel_tolerance"],
        residual=reason,
        run_id=record.get("run_id"),
    )


def reexec(
    run: Any, *, registry: "Mapping[str, Callable[..., Any]] | None" = None
) -> ReexecResult:
    """Re-execute a recorded run and return a three-valued faithfulness verdict.

    *run* is a run record (dict) or a path to a JSON record. It carries the
    recorded ``reported_value``, a ``pipeline`` naming how to re-execute, an
    optional declared ``tolerance`` / ``rel_tolerance``, and, when the run
    cannot be re-run faithfully, ``reexecutable: false`` with a
    ``not_reexecutable_reason`` from :data:`NON_REEXECUTABLE_REASONS`.

    ``registry`` optionally maps a pipeline ``target`` to a callable, so a caller
    can re-execute an in-memory pipeline without a dotted import path. The
    callable is invoked with the pipeline's ``args`` (a dict of kwargs, or a
    list of positional args) and must return a number.

    Raises :class:`MalformedRunError` for a record that is not well-formed;
    every well-formed run returns a :class:`ReexecResult`, never an exception , 
    a re-execution that fails is reported as ``COULD_NOT_REEXECUTE``, never a
    false ``REPRODUCED`` and never a spurious ``DIVERGED``.
    """
    record = _normalize_run(run)

    # 1. Declared non-reexecutable: never run, never assume reproduction.
    if not record.get("reexecutable", True):
        reason = record["not_reexecutable_reason"]
        return _could_not(
            record,
            f"recorded run declared non-reexecutable ({reason}); it was not re-run, "
            "so faithfulness is unknown, and REPRODUCED is never assumed here",
        )

    pipeline = record.get("pipeline")
    if not isinstance(pipeline, Mapping) or not pipeline:
        return _could_not(
            record,
            "no pipeline recorded to re-execute; faithfulness cannot be checked",
        )

    # 2. Resolve the entry point. An unresolvable target is could-not, not a crash.
    try:
        fn = _resolve_target(pipeline, registry)
    except Exception as exc:  # noqa: BLE001, import-time failure is could-not, honestly
        return _could_not(
            record,
            f"recorded pipeline entry point could not be resolved ({exc}); the "
            "re-execution did not run",
        )

    # 3. Re-execute in a clean scope. A raise is could-not, not divergence.
    args = pipeline.get("args") or {}
    try:
        with _clean_scope():
            raw = fn(**args) if isinstance(args, Mapping) else fn(*args)
    except Exception as exc:  # noqa: BLE001, any target failure is could-not, honestly
        return _could_not(
            record,
            f"re-execution raised ({type(exc).__name__}: {exc}); a failed re-run "
            "is not evidence of divergence",
        )

    reproduced = _as_number(raw)
    if reproduced is None:
        return _could_not(
            record,
            f"re-execution returned a non-numeric result ({raw!r}); there is no "
            "number to compare",
        )

    # 4. Compare within the declared tolerance.
    recorded = record["reported_value"]
    matched = math.isclose(
        reproduced, recorded, rel_tol=record["rel_tolerance"], abs_tol=record["tolerance"]
    )
    verdict = (
        FaithfulnessVerdict.REPRODUCED if matched else FaithfulnessVerdict.DIVERGED
    )
    wide = matched and _tolerance_is_wide(
        recorded, reproduced, record["tolerance"], record["rel_tolerance"]
    )
    return ReexecResult(
        verdict=verdict,
        recorded_value=recorded,
        reproduced_value=reproduced,
        tolerance=record["tolerance"],
        rel_tolerance=record["rel_tolerance"],
        residual=_conclusive_residual(
            record["tolerance"], record["rel_tolerance"], wide=wide
        ),
        run_id=record.get("run_id"),
    )
