"""
_evidence.py — GRADE 5-domain EvidenceVector.

The GRADE framework (Grading of Recommendations, Assessment, Development,
and Evaluations — the de-facto medical-evidence standard) scores evidence
along five orthogonal *downgrade* domains and three *upgrade* booleans.
Each downgrade domain is a non-positive integer in [-2, 0]; nonzero
values require a written rationale (anti-handwaving rule from the GRADE
handbook).

mareforma stores an EvidenceVector inside every signed Statement v1
predicate. The vector is *part of the signature* — values cannot be
retroactively changed without producing a different statement_cid and
a new signature.

In the OSS core the EvidenceVector is constructed by the asserter (the
agent or human producing the claim). Future platform-layer evidence
assistants may help authors fill it; the data model is the same.

Domains
-------
risk_of_bias      — methodological flaws (allocation, blinding, attrition)
inconsistency     — heterogeneity in effect across studies
indirectness      — population / intervention / outcome mismatch
imprecision       — wide confidence intervals / small N
publication_bias  — selective reporting / file-drawer effect

Each domain: 0 = no concern, -1 = serious concern, -2 = very serious.

Upgrade booleans
----------------
large_effect          — magnitude survives plausible confounding
dose_response         — clear dose-response gradient observed
opposing_confounding  — known confounders bias against the observed effect

(GRADE permits upgrades for observational evidence when these conditions
hold; they raise the certainty estimate above what RoB alone implies.)

References
----------
Guyatt et al. 2008, BMJ 336:924 — GRADE: an emerging consensus.
Schünemann et al. 2013, GRADE Handbook §5–9.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


_DOWNGRADE_DOMAINS = (
    "risk_of_bias",
    "inconsistency",
    "indirectness",
    "imprecision",
    "publication_bias",
)


# GRADE study-design starting points. The number is the certainty score
# the design starts with before any downgrades / upgrades apply.
# Mapping per the GRADE handbook §5.1:
#   randomised-trial → 4 (HIGH)
#   observational    → 2 (LOW)
#   case-series      → 1 (VERY LOW)
#   not-applicable   → 4 (treated as HIGH for asserter-level claims that
#                         are not empirical studies, e.g. an analytical
#                         derivation; downgrades may still apply if the
#                         asserter flags concerns)
_STUDY_DESIGN_BASELINE: dict[str, int] = {
    "randomised-trial": 4,
    "observational": 2,
    "case-series": 1,
    "not-applicable": 4,
}
VALID_STUDY_DESIGNS: tuple[str, ...] = tuple(_STUDY_DESIGN_BASELINE.keys())

# Final score → human-readable certainty band (GRADE four-tier).
_CERTAINTY_BANDS = (
    (4, "HIGH"),
    (3, "MODERATE"),
    (2, "LOW"),
    (1, "VERY_LOW"),
    (0, "VERY_LOW"),
)


class EvidenceVectorError(ValueError):
    """Raised when an EvidenceVector violates a GRADE invariant."""


@dataclass(frozen=True)
class EvidenceVector:
    """GRADE 5-domain evidence vector with upgrade flags + rationales.

    Defaults to all-zeros (every domain unflagged, no upgrades). A default
    EvidenceVector means "the asserter did not flag any quality concerns"
    — equivalent to GRADE "high certainty" before any context-specific
    downgrade. The graph still labels the claim PRELIMINARY until the
    trust-ladder gates fire; this vector is orthogonal to the ladder.

    Frozen + hashable so an EvidenceVector can be embedded inside a
    signed Statement without callers accidentally mutating it post-sign.

    study_design + certainty()
    --------------------------
    The optional ``study_design`` field sets the baseline that
    :meth:`certainty` starts from before applying downgrades and
    upgrades. Mareforma ships with four labels:

    * ``randomised-trial``  → baseline 4 (HIGH)
    * ``observational``     → baseline 2 (LOW)
    * ``case-series``       → baseline 1 (VERY LOW)
    * ``not-applicable``    → baseline 4 (HIGH); intended for claims
                              that are not empirical studies (analytical
                              derivations, tool outputs, asserter-level
                              assertions). Treating non-empirical claims
                              as HIGH-by-default is a mareforma
                              convention, NOT a GRADE recommendation —
                              callers MUST set downgrade domains
                              explicitly when an asserter-level claim is
                              not actually high-certainty.

    When ``study_design`` is ``None`` (the default), the baseline is
    also treated as HIGH (4). This preserves byte-equality for legacy
    EvidenceVectors that pre-date the field.
    """

    risk_of_bias: int = 0
    inconsistency: int = 0
    indirectness: int = 0
    imprecision: int = 0
    publication_bias: int = 0
    large_effect: bool = False
    dose_response: bool = False
    opposing_confounding: bool = False
    rationale: Mapping[str, str] = field(default_factory=dict)
    reporting_compliance: tuple[str, ...] = ()
    # GRADE study-design starting point. None = not declared (legacy
    # callers / asserter-level claims that aren't empirical studies);
    # one of :data:`VALID_STUDY_DESIGNS` otherwise. Drives the baseline
    # certainty score in :meth:`certainty`.
    study_design: str | None = None
    # Asserter-time grounding sensor verdict (in [0.0, 1.0]) + the
    # rationale string. Snapshotted at assertion time and signed
    # together with the rest of the vector. Future verifiers can re-
    # run independently and produce a different number; that
    # recomputed verdict is NOT stored on the claim. None = no
    # grounding sensor was wired into this assertion.
    grounding_score: float | None = None
    grounding_rationale: str | None = None

    def __post_init__(self) -> None:
        # Domains must be in [-2, 0]. The bound is checked here so a
        # caller passing e.g. risk_of_bias=-3 fails at construction
        # rather than at SQL CHECK time — better error message, same
        # invariant.
        for domain in _DOWNGRADE_DOMAINS:
            v = getattr(self, domain)
            if not isinstance(v, int) or isinstance(v, bool):
                raise EvidenceVectorError(
                    f"{domain}={v!r} must be an int in [-2, 0]"
                )
            if v < -2 or v > 0:
                raise EvidenceVectorError(
                    f"{domain}={v} out of range [-2, 0]"
                )

        # Booleans must be bool (not int, not None).
        for flag in ("large_effect", "dose_response", "opposing_confounding"):
            v = getattr(self, flag)
            if not isinstance(v, bool):
                raise EvidenceVectorError(
                    f"{flag}={v!r} must be a bool"
                )

        # Rationale: nonzero downgrade ⇒ rationale[domain] is required.
        # This is the GRADE anti-handwaving rule — any author who scores
        # a downgrade must justify it in writing.
        if not isinstance(self.rationale, Mapping):
            raise EvidenceVectorError(
                f"rationale must be a Mapping, got {type(self.rationale).__name__}"
            )
        for domain in _DOWNGRADE_DOMAINS:
            if getattr(self, domain) != 0:
                r = self.rationale.get(domain)
                if not isinstance(r, str) or not r.strip():
                    raise EvidenceVectorError(
                        f"rationale[{domain!r}] is required because "
                        f"{domain}={getattr(self, domain)} (nonzero)"
                    )

        # reporting_compliance: must be a tuple of strings. Empty default
        # for claims that don't reference a structured reporting checklist
        # (CONSORT / ARRIVE / PRISMA / etc.). Each entry is a free-form
        # short tag; the future platform layer can interpret them.
        if not isinstance(self.reporting_compliance, tuple):
            raise EvidenceVectorError(
                "reporting_compliance must be a tuple of strings; "
                f"got {type(self.reporting_compliance).__name__}"
            )
        for tag in self.reporting_compliance:
            if not isinstance(tag, str):
                raise EvidenceVectorError(
                    f"reporting_compliance entries must be strings; got {tag!r}"
                )

        # study_design: optional, but when present must be a known label.
        if self.study_design is not None:
            if not isinstance(self.study_design, str):
                raise EvidenceVectorError(
                    f"study_design must be a string or None; got "
                    f"{type(self.study_design).__name__}"
                )
            if self.study_design not in _STUDY_DESIGN_BASELINE:
                raise EvidenceVectorError(
                    f"study_design={self.study_design!r} is not one of "
                    f"{VALID_STUDY_DESIGNS}"
                )

        # grounding_score: optional float in [0.0, 1.0]; both score and
        # rationale must be present together or both absent.
        if self.grounding_score is not None:
            if isinstance(self.grounding_score, bool):
                raise EvidenceVectorError(
                    "grounding_score must be a float, not a bool"
                )
            if not isinstance(self.grounding_score, (int, float)):
                raise EvidenceVectorError(
                    f"grounding_score must be a float or None; got "
                    f"{type(self.grounding_score).__name__}"
                )
            gs = float(self.grounding_score)
            if gs != gs:  # NaN
                raise EvidenceVectorError("grounding_score must not be NaN")
            if gs < 0.0 or gs > 1.0:
                raise EvidenceVectorError(
                    f"grounding_score={gs} out of [0.0, 1.0]"
                )
            if not isinstance(self.grounding_rationale, str) or not (
                self.grounding_rationale.strip()
            ):
                raise EvidenceVectorError(
                    "grounding_rationale is required when grounding_score "
                    "is set; pass the verifier's explanation string"
                )
        elif self.grounding_rationale is not None:
            raise EvidenceVectorError(
                "grounding_rationale set without grounding_score; "
                "either pass both or neither"
            )

    def to_dict(self) -> dict:
        """Serialize to a JSON-compatible dict for inclusion in a Statement.

        ``study_design`` is omitted from the output when None so claims
        that pre-date the field round-trip byte-equal. The signed
        envelope canonical bytes for legacy EvidenceVectors are
        therefore unchanged.
        """
        out: dict = {
            "risk_of_bias": self.risk_of_bias,
            "inconsistency": self.inconsistency,
            "indirectness": self.indirectness,
            "imprecision": self.imprecision,
            "publication_bias": self.publication_bias,
            "large_effect": self.large_effect,
            "dose_response": self.dose_response,
            "opposing_confounding": self.opposing_confounding,
            "rationale": dict(self.rationale),
            "reporting_compliance": list(self.reporting_compliance),
        }
        if self.study_design is not None:
            out["study_design"] = self.study_design
        if self.grounding_score is not None:
            out["grounding_score"] = float(self.grounding_score)
            out["grounding_rationale"] = self.grounding_rationale
        return out

    def certainty(self) -> str:
        """Return the GRADE certainty band for this vector.

        Algorithm (GRADE handbook §5):

        1. Baseline = :data:`_STUDY_DESIGN_BASELINE[study_design]`.
           Falls back to ``4`` (HIGH) when ``study_design`` is None —
           legacy claims that did not declare a design are treated as
           asserter-level HIGH and only the downgrade signal applies.
        2. Sum the five downgrade domains (each in [-2, 0]).
        3. Add upgrade points (only when the design is observational
           AND the score has not been downgraded — GRADE forbids
           upgrading downgraded evidence):

           * large_effect          → +1 (or +2 when very large;
                                         mareforma exposes the single
                                         boolean only, so this is +1)
           * dose_response         → +1
           * opposing_confounding  → +1

        4. Clamp to [0, 4], map to band.

        Returns one of: ``"HIGH"``, ``"MODERATE"``, ``"LOW"``,
        ``"VERY_LOW"``.
        """
        baseline = (
            _STUDY_DESIGN_BASELINE.get(self.study_design, 4)
            if self.study_design is not None
            else 4
        )
        downgrade_sum = sum(
            getattr(self, domain) for domain in _DOWNGRADE_DOMAINS
        )
        score = baseline + downgrade_sum  # downgrade values are negative

        if self.study_design == "observational" and downgrade_sum == 0:
            if self.large_effect:
                score += 1
            if self.dose_response:
                score += 1
            if self.opposing_confounding:
                score += 1

        score = max(0, min(4, score))
        for threshold, band in _CERTAINTY_BANDS:
            if score >= threshold:
                return band
        return "VERY_LOW"

    @classmethod
    def from_dict(cls, data: dict) -> "EvidenceVector":
        """Reconstruct from the dict produced by ``to_dict``.

        Raises :class:`EvidenceVectorError` if invariants are violated.
        """
        return cls(
            risk_of_bias=data.get("risk_of_bias", 0),
            inconsistency=data.get("inconsistency", 0),
            indirectness=data.get("indirectness", 0),
            imprecision=data.get("imprecision", 0),
            publication_bias=data.get("publication_bias", 0),
            large_effect=data.get("large_effect", False),
            dose_response=data.get("dose_response", False),
            opposing_confounding=data.get("opposing_confounding", False),
            rationale=dict(data.get("rationale") or {}),
            reporting_compliance=tuple(data.get("reporting_compliance") or ()),
            study_design=data.get("study_design"),
            grounding_score=data.get("grounding_score"),
            grounding_rationale=data.get("grounding_rationale"),
        )
