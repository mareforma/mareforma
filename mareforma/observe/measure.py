"""Aggregate grounding verdicts into the measurement a paper reports.

A single verdict answers one finding. The measurement answers a pipeline: over
many findings, what fraction is GROUNDED, UNGROUNDED, OPAQUE; how often did an
incidental read occur that citation binding correctly refused to count; and what
fraction of the cited reads the observer actually saw. These are the numbers
that turn "the detector works on a fixture" into "here is how the phenomenon
looks on a real pipeline."

The split is also a routing signal. If OPAQUE dominates on a target pipeline,
the observer cannot see enough of it to make the other numbers meaningful, and
the honest response is to attach deeper (child-process / thread instrumentation)
before publishing a measurement. :meth:`GroundingReport.opaque_dominates` is that
trigger.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from ._citation import read_matches_citation
from ._verdict import GroundingVerdict, ObservedGrounding


@dataclass(frozen=True)
class GroundingReport:
    """The split and the honest coverage bounds over a set of verdicts."""

    total: int
    grounded: int
    ungrounded: int
    opaque: int
    incidental_reads: int
    mean_read_coverage: float | None
    # OPAQUE verdicts bucketed by the seam kind(s) that hid them (a verdict with
    # more than one seam kind counts once per kind). This operationalizes the
    # "name what you cannot see" thesis in every measurement: it says WHY the
    # observer went blind, which routes the fix (child-process attach for
    # subprocess seams, deeper wrapping for coverage-gap seams).
    opaque_by_seam: dict[str, int] = field(default_factory=dict)

    def fractions(self) -> dict[str, float]:
        """GROUNDED / UNGROUNDED / OPAQUE as fractions of the total."""
        if self.total == 0:
            return {"GROUNDED": 0.0, "UNGROUNDED": 0.0, "OPAQUE": 0.0}
        return {
            "GROUNDED": self.grounded / self.total,
            "UNGROUNDED": self.ungrounded / self.total,
            "OPAQUE": self.opaque / self.total,
        }

    @property
    def opaque_fraction(self) -> float:
        return 0.0 if self.total == 0 else self.opaque / self.total

    @property
    def incidental_read_rate(self) -> float:
        """Fraction of findings that carried a non-cited (incidental) read.

        These are the findings where citation binding did the work: a read
        happened, but because it did not match the cited source it was refused
        as grounding. A high rate is why "some loader returned data" would have
        been a false-GROUNDED detector.
        """
        return 0.0 if self.total == 0 else self.incidental_reads / self.total

    def opaque_dominates(self, threshold: float = 0.5) -> bool:
        """Whether OPAQUE is frequent enough to trigger attaching deeper.

        When the observer cannot see at least ``threshold`` of the pipeline, the
        split is not yet a trustworthy measurement; pull forward the deeper
        attach (child-process / thread instrumentation) before reporting.
        """
        return self.opaque_fraction >= threshold

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "counts": {
                "GROUNDED": self.grounded,
                "UNGROUNDED": self.ungrounded,
                "OPAQUE": self.opaque,
            },
            "fractions": self.fractions(),
            "opaque_fraction": self.opaque_fraction,
            "incidental_read_rate": self.incidental_read_rate,
            "mean_read_coverage": self.mean_read_coverage,
            "opaque_by_seam": dict(self.opaque_by_seam),
        }

    def closing_sentence(self) -> str:
        """A plain-English one-line summary a reviewer can read without the JSON."""
        if self.total == 0:
            return "No verdicts to measure."
        f = self.fractions()
        lead = (
            f"Across {self.total} findings: "
            f"{f['GROUNDED']:.0%} GROUNDED, {f['UNGROUNDED']:.0%} UNGROUNDED, "
            f"{f['OPAQUE']:.0%} OPAQUE."
        )
        if self.opaque and self.opaque_by_seam:
            top = max(self.opaque_by_seam.items(), key=lambda kv: kv[1])
            lead += (
                f" The observer went blind mostly at {top[0]} seams "
                f"({top[1]} of {self.opaque} OPAQUE)."
            )
        if self.opaque_dominates():
            lead += (
                " OPAQUE dominates: attach deeper before trusting the split."
            )
        return lead


def _has_incidental_read(v: GroundingVerdict) -> bool:
    """Whether the verdict carried a non-empty read that matched no cited source."""
    for r in v.reads:
        if r.nonempty and not read_matches_citation(
            r.identifier, r.content_address, v.cited_sources
        ):
            return True
    return False


def summarize(verdicts: Iterable[GroundingVerdict]) -> GroundingReport:
    """Aggregate verdicts into the split, the incidental-read rate, and coverage.

    Read coverage is averaged only over verdicts where an open was detected (the
    fraction is undefined when nothing was opened), so a pipeline of pure-compute
    findings does not drag the mean to zero.
    """
    verdicts = list(verdicts)
    grounded = ungrounded = opaque = incidental = 0
    coverage_values: list[float] = []
    opaque_by_seam: dict[str, int] = {}
    for v in verdicts:
        if v.grounding is ObservedGrounding.GROUNDED:
            grounded += 1
        elif v.grounding is ObservedGrounding.UNGROUNDED:
            ungrounded += 1
        else:
            opaque += 1
            for kind in {s.kind for s in v.seams} or {"unattributed"}:
                opaque_by_seam[kind] = opaque_by_seam.get(kind, 0) + 1
        if _has_incidental_read(v):
            incidental += 1
        cov = v.read_coverage_fraction()
        if cov is not None:
            coverage_values.append(cov)
    mean_cov = (
        sum(coverage_values) / len(coverage_values) if coverage_values else None
    )
    return GroundingReport(
        total=len(verdicts),
        grounded=grounded,
        ungrounded=ungrounded,
        opaque=opaque,
        incidental_reads=incidental,
        mean_read_coverage=mean_cov,
        opaque_by_seam=dict(sorted(opaque_by_seam.items())),
    )


def summarize_receipts(receipts: Iterable[dict]) -> GroundingReport:
    """Aggregate persisted verdict RECEIPTS (dicts) into the same report.

    A measurement run persists each verdict's full receipt (which carries the
    reads and seams the signed envelope omits) so the report can bucket OPAQUE by
    seam kind. This reconstructs each verdict from its receipt and defers to
    :func:`summarize`, so a run that saved receipts to disk reports identically to
    one holding the live verdicts.
    """
    return summarize(GroundingVerdict.from_receipt(r) for r in receipts)


@dataclass(frozen=True)
class IndependenceReport:
    """The independence arm of the measurement, over per-finding receipts.

    Where :class:`GroundingReport` answers "did the cited data flow," this answers
    "how independent is the corroboration." Over a run's per-finding effective-
    independence records it reports three numbers the paper's independence arm
    needs:

    - the DISTRIBUTION of the effective-independence number: what fraction of
      findings sit at 1 (a single supporting line, no corroboration) versus >= 2
      (genuinely corroborated by a distinct model / human / data);
    - the UNVERIFIABLE fraction: findings whose supporting lineage is soft
      (PROXY / UNVERIFIABLE), so the count rests on lineage that cannot certify a
      distinct model;
    - the SAME-MODEL-COLLAPSE rate: of the corroborating lines a naive signer-axis
      counter would call independent, the fraction that were one COMPUTED model
      counted twice (``naive - number`` summed, over the naive total).

    A finding contributes a record ``{"number", "naive", "soft"}`` (see
    :func:`mareforma.trust._store.effective_independence_receipt`).
    """

    total: int
    at_zero: int
    at_one: int
    at_two_plus: int
    unverifiable: int
    naive_total: int
    collapsed_total: int

    def fraction_at_one(self) -> float:
        return 0.0 if self.total == 0 else self.at_one / self.total

    def fraction_two_plus(self) -> float:
        return 0.0 if self.total == 0 else self.at_two_plus / self.total

    @property
    def unverifiable_fraction(self) -> float:
        """Fraction of findings whose supporting lineage is soft (UNVERIFIABLE)."""
        return 0.0 if self.total == 0 else self.unverifiable / self.total

    @property
    def same_model_collapse_rate(self) -> float:
        """Fraction of naive-independent corroborations that were one model twice.

        The numerator is the collapse (``naive - number`` summed over findings);
        the denominator is the naive total (what a signer-axis counter would call
        independent). Zero when nothing corroborated, never a divide-by-zero.
        """
        if self.naive_total == 0:
            return 0.0
        return self.collapsed_total / self.naive_total

    def distribution(self) -> dict[str, float]:
        """The effective-independence distribution as fractions of the total."""
        if self.total == 0:
            return {"0": 0.0, "1": 0.0, ">=2": 0.0}
        return {
            "0": self.at_zero / self.total,
            "1": self.at_one / self.total,
            ">=2": self.at_two_plus / self.total,
        }

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "distribution_counts": {
                "0": self.at_zero,
                "1": self.at_one,
                ">=2": self.at_two_plus,
            },
            "distribution": self.distribution(),
            "unverifiable": self.unverifiable,
            "unverifiable_fraction": self.unverifiable_fraction,
            "naive_total": self.naive_total,
            "collapsed_total": self.collapsed_total,
            "same_model_collapse_rate": self.same_model_collapse_rate,
        }

    def closing_sentence(self) -> str:
        """A plain-English one-line summary a reviewer can read without the JSON."""
        if self.total == 0:
            return "No independence records to measure."
        d = self.distribution()
        lead = (
            f"Across {self.total} findings: {d['1']:.0%} rest on a single line "
            f"(effective independence 1), {d['>=2']:.0%} are corroborated (>= 2)."
        )
        if self.unverifiable:
            lead += (
                f" {self.unverifiable_fraction:.0%} are UNVERIFIABLE (soft lineage)."
            )
        if self.collapsed_total:
            lead += (
                f" {self.same_model_collapse_rate:.0%} of naive corroborations were "
                f"one model counted twice."
            )
        return lead


def _as_int(value: object) -> int:
    """Coerce a receipt field to a non-negative int, defaulting to 0.

    A hand-authored or older record may carry a missing, null, or non-numeric
    field; the independence arm degrades it to 0 rather than raising, so one bad
    record never denies the whole report.
    """
    try:
        return max(0, int(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def summarize_independence(records: Iterable[dict]) -> IndependenceReport:
    """Aggregate per-finding independence records into the independence report.

    Each record is ``{"number": <effective>, "naive": <signer-axis hard count>,
    "soft": <bool>}`` as written by
    :func:`mareforma.trust._store.effective_independence_receipt`. Missing fields
    degrade to their conservative default (``number``/``naive`` 0, ``soft`` False)
    so a hand-authored or older record still aggregates rather than raising.
    """
    total = at_zero = at_one = at_two_plus = 0
    unverifiable = naive_total = collapsed_total = 0
    for rec in records:
        total += 1
        number = _as_int(rec.get("number"))
        naive = _as_int(rec.get("naive"))
        if bool(rec.get("soft")):
            unverifiable += 1
        if number <= 0:
            at_zero += 1
        elif number == 1:
            at_one += 1
        else:
            at_two_plus += 1
        naive_total += naive
        # naive is the signer-axis count over HARD lineage and number folds the
        # model axis, so number <= naive on any hard body; clamp defensively so a
        # malformed record can never make the collapse negative.
        collapsed_total += max(0, naive - number)
    return IndependenceReport(
        total=total,
        at_zero=at_zero,
        at_one=at_one,
        at_two_plus=at_two_plus,
        unverifiable=unverifiable,
        naive_total=naive_total,
        collapsed_total=collapsed_total,
    )


def independence_records(receipts: Iterable[dict]) -> list[dict]:
    """The per-finding independence records carried by a run's receipts.

    A combined receipt carries the grounding fields plus an ``"independence"``
    sub-record. This pulls the sub-records present, so a run that wrote both arms
    to one receipts file reports the independence arm alongside the grounding
    split, and a receipts file with no independence records yields an empty list
    (the independence arm is simply not reported, never fabricated).
    """
    out: list[dict] = []
    for r in receipts:
        rec = r.get("independence")
        if isinstance(rec, dict):
            out.append(rec)
    return out


def summarize_independence_receipts(receipts: Iterable[dict]) -> IndependenceReport:
    """Aggregate the independence records carried by a run's receipts."""
    return summarize_independence(independence_records(receipts))


@dataclass(frozen=True)
class PilotReport:
    """A slim natural-prevalence pilot: both arms plus the honest coverage bound.

    The pilot is the cheap pre-check before the full natural-corpus run (see the
    kill-switch fixtures): a small receipts file of real findings yields the
    grounding split and the independence distribution TOGETHER, with the OPAQUE
    fraction reported as the honesty gate. When OPAQUE dominates, the observer
    could not see enough of the pipeline for the split to be a trustworthy
    prevalence number, so the report says so rather than over-claiming — the
    grounded prevalence reads as a lower bound until the observer attaches deeper.
    """

    grounding: GroundingReport
    independence: IndependenceReport

    def opaque_dominates(self, threshold: float = 0.5) -> bool:
        return self.grounding.opaque_dominates(threshold)

    def coverage_bound(self) -> str:
        """The honest one-line bound the OPAQUE fraction puts on the split."""
        frac = self.grounding.opaque_fraction
        if self.grounding.total == 0:
            return "No findings to bound."
        if self.opaque_dominates():
            return (
                f"OPAQUE covers {frac:.0%} of findings: the split is not yet a "
                f"trustworthy prevalence number. Report grounded prevalence as a "
                f"lower bound and attach deeper before publishing."
            )
        return (
            f"OPAQUE covers {frac:.0%} of findings: the split is a lower bound on "
            f"grounded prevalence to within that coverage gap."
        )

    def to_dict(self) -> dict:
        return {
            "n": self.grounding.total,
            "grounding": self.grounding.to_dict(),
            "independence": (
                self.independence.to_dict() if self.independence.total else None
            ),
            "opaque_fraction": self.grounding.opaque_fraction,
            "opaque_dominates": self.opaque_dominates(),
            "coverage_bound": self.coverage_bound(),
        }

    def closing_sentence(self) -> str:
        lead = self.grounding.closing_sentence()
        if self.independence.total:
            lead += " " + self.independence.closing_sentence()
        return lead + " " + self.coverage_bound()


def summarize_pilot(receipts: Iterable[dict]) -> PilotReport:
    """Run the slim natural-prevalence pilot over a receipts file.

    Reads the receipts once into a list (they are iterated twice: once for the
    grounding split, once for the independence arm) and returns both reports plus
    the OPAQUE-coverage bound. The independence arm is present only when a receipt
    carries an ``independence`` record, so a grounding-only pilot still reports.
    """
    receipts = list(receipts)
    return PilotReport(
        grounding=summarize_receipts(receipts),
        independence=summarize_independence(independence_records(receipts)),
    )
