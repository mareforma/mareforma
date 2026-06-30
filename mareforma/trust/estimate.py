"""estimate.py: the EffectEstimate and the single-line evidence tree.

The legacy ``(procedure_id, data_id, statistic, p_value)`` tuple is the
degenerate case of a real evidence tree::

    Finding --has_lines--> EvidenceLine[] --has_contrasts--> Contrast[]
                                                     --has_estimate--> EffectEstimate

The current cut fills exactly one line, one contrast, one estimate per finding;
later stages add rows to the same tables (multi-line, controls, replicate hierarchy)
with no migration. The field NAMES follow the metafor/escalc convention so
existing R tooling can ingest/emit with zero mapping (the names are
conventions, not copyrightable; no GPL code is vendored).

The current cut stores the minimal estimate the gate needs, not the full
~18-field set. The richer fields (``variance``/``sei``, ``effect_type_iri``,
``test_statistic_*``, per-group n, 2x2 cells, ``comparable_with``,
``conversion_provenance``) are deferred to later stages because the only
computation here is the gate.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .errors import InconsistentEstimateError


class EffectType(str, Enum):
    """metafor ``measure`` values. Identity-relevant and stable (the SQL CHECK
    and ``comparable_with`` family lookup ride this enum, never an unstable IRI).
    """

    SMD = "SMD"
    HEDGES_G = "Hedges_g"
    OR = "OR"
    LOG_OR = "logOR"
    RR = "RR"
    HR = "HR"
    COR = "COR"
    ZCOR = "ZCOR"
    MD = "MD"
    ROM = "ROM"
    BETA = "beta"
    LOG2FC = "log2FC"
    GEN = "GEN"


class Scale(str, Enum):
    """The current subset. ``logit`` / ``fisher_z`` are deferred."""

    RAW = "raw"
    LOG = "log"


class ControlType(str, Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    VEHICLE = "vehicle"
    SHAM = "sham"
    COMPARATIVE = "comparative"


# Ratio-type effects measured on the raw scale have a null of 1 (no effect = a
# ratio of one); everything else (difference-type effects, and anything on a log
# scale) has a null of 0. Core-derived from the stable enum, never agent-supplied.
_RATIO_TYPES: frozenset[EffectType] = frozenset(
    {EffectType.OR, EffectType.RR, EffectType.HR, EffectType.ROM}
)


def null_value(effect_type: EffectType, scale: Scale) -> float:
    """The value of ``estimate_value`` that means "no effect".

    Derived from ``(effect_type, scale)``: 0 for difference-type effects and
    for anything on a log scale; 1 for raw-scale ratio effects (OR/RR/HR/ROM).
    A logged ratio (e.g. ``logOR``, or an OR stored on the log scale) has a
    null of 0 because ``log(1) == 0``.
    """
    if scale is Scale.LOG:
        return 0.0
    if effect_type in _RATIO_TYPES:
        return 1.0
    return 0.0


@dataclass(frozen=True)
class EffectEstimate:
    """A point estimate plus the uncertainty the gate needs.

    Exactly one of (a) ``p_value`` or (b) the full CI triple
    (``ci_lower``, ``ci_upper``, ``ci_level``) is required, and both may be
    supplied. An equivalence test always needs the CI. The core runs basic
    input-consistency checks on construction and refuses inconsistent input
    rather than storing it.
    """

    estimate_value: float
    effect_type: EffectType
    scale: Scale = Scale.RAW
    p_value: float | None = None
    ci_lower: float | None = None
    ci_upper: float | None = None
    ci_level: float | None = None
    n_total: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.effect_type, EffectType):
            object.__setattr__(self, "effect_type", EffectType(self.effect_type))
        if not isinstance(self.scale, Scale):
            object.__setattr__(self, "scale", Scale(self.scale))

        ev = self.estimate_value
        if not math.isfinite(ev):
            raise InconsistentEstimateError(
                "estimate_value must be a finite number (not NaN or infinity)"
            )

        # Every supplied numeric field must be finite. The all-or-none and
        # bracket checks below do not catch a NaN or +/-inf bound on their own
        # (NaN comparisons are silently False, and an infinite bound passes the
        # bracket test), so an unchecked non-finite CI value would poison the
        # gate's comparisons downstream.
        for _name in ("ci_lower", "ci_upper", "ci_level", "p_value"):
            _val = getattr(self, _name)
            if _val is not None and not math.isfinite(_val):
                raise InconsistentEstimateError(
                    f"{_name}, when given, must be a finite number (not NaN or infinity)"
                )

        ci_parts = (self.ci_lower, self.ci_upper, self.ci_level)
        ci_given = [p is not None for p in ci_parts]
        if any(ci_given) and not all(ci_given):
            raise InconsistentEstimateError(
                "a confidence interval requires all of ci_lower, ci_upper, ci_level"
            )
        has_ci = all(ci_given)

        if self.p_value is None and not has_ci:
            raise InconsistentEstimateError(
                "supply a p_value, a (ci_lower, ci_upper, ci_level) triple, or both"
            )

        if self.p_value is not None and not (0.0 <= self.p_value <= 1.0):
            raise InconsistentEstimateError("p_value must be in [0, 1]")

        if has_ci:
            if not (0.0 < self.ci_level < 1.0):
                raise InconsistentEstimateError("ci_level must be in (0, 1)")
            if not (self.ci_lower <= self.ci_upper):
                raise InconsistentEstimateError(
                    "ci_lower must be <= ci_upper"
                )
            # The CI must bracket the point estimate, or the input is internally
            # inconsistent (a transcription error, or a mismatched estimate/CI).
            if not (self.ci_lower <= ev <= self.ci_upper):
                raise InconsistentEstimateError(
                    "confidence interval must bracket estimate_value "
                    f"({self.ci_lower} <= {ev} <= {self.ci_upper} is false)"
                )

        if self.n_total is not None:
            if not isinstance(self.n_total, int) or self.n_total <= 0:
                raise InconsistentEstimateError(
                    "n_total, when given, must be a positive integer"
                )

    @property
    def null_value(self) -> float:
        return null_value(self.effect_type, self.scale)

    def to_dict(self) -> dict[str, Any]:
        return {
            "estimate_value": self.estimate_value,
            "effect_type": self.effect_type.value,
            "scale": self.scale.value,
            "p_value": self.p_value,
            "ci_lower": self.ci_lower,
            "ci_upper": self.ci_upper,
            "ci_level": self.ci_level,
            "n_total": self.n_total,
        }


@dataclass(frozen=True)
class Contrast:
    """The comparison an estimate quantifies. It carries only the control type
    for now; treatment/control arm structure and 2x2 cells are deferred.
    """

    control_type: ControlType = ControlType.NEGATIVE

    def __post_init__(self) -> None:
        if not isinstance(self.control_type, ControlType):
            object.__setattr__(
                self, "control_type", ControlType(self.control_type)
            )


@dataclass(frozen=True)
class EvidenceLine:
    """One independent line of evidence for a finding.

    Independence is counted by distinct **signer** (the claim's
    ``asserter_keyid``) with ``data_id`` as a secondary guard: two lines count
    as independent only when both the signer AND the ``data_id`` differ, so the
    same dataset re-run under a second signer does not add a line. When the
    agent persists the dataset bytes, mareforma content-addresses them itself
    so ``data_id`` is core-computed and the guard is honest; when only a
    reference is supplied, ``data_id`` is agent-attested and the guard is soft.
    """

    estimate: EffectEstimate
    data_id: str
    contrast: Contrast = field(default_factory=Contrast)
    modality: str | None = None
    provenance_id: str | None = None
    design_type: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.estimate, EffectEstimate):
            raise TypeError("EvidenceLine.estimate must be an EffectEstimate")
        if not isinstance(self.data_id, str) or not self.data_id.strip():
            raise ValueError("EvidenceLine.data_id must be a non-empty string")
        if not isinstance(self.contrast, Contrast):
            raise TypeError("EvidenceLine.contrast must be a Contrast")
