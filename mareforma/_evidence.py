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

In v0.3.0 OSS the EvidenceVector is constructed by the asserter (the
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

    def to_dict(self) -> dict:
        """Serialize to a JSON-compatible dict for inclusion in a Statement."""
        return {
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
        )
