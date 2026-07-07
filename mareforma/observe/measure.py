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
