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

from ._citation import read_norm_matches
from ._verdict import (
    GROUNDING_AXIS_VERSION,
    GroundingVerdict,
    ObservedGrounding,
    as_int,
)


class GroundingAxisMismatchError(ValueError):
    """A receipt was written under a different grounding-axis version."""


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
    """Whether the verdict carried a non-empty read that matched no cited source.

    Pure string comparison over identifiers both sides normalized at write time,
    the same rule the citation binding follows: a receipt is summarized from
    another directory, another run, or another host, and touching the filesystem
    here would make the number depend on where the report was produced.
    """
    for r in v.reads:
        if r.nonempty and not read_norm_matches(
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

    Raises :class:`GroundingAxisMismatchError` on a receipt stamped with a
    different axis version. Axis versions differ in what counts as a matching
    read, so folding one into the other's report would publish a number no
    definition produced. Summarize each axis separately.
    """
    return summarize(_verdict_on_this_axis(r) for r in receipts)


def _verdict_on_this_axis(receipt: dict) -> GroundingVerdict:
    """Reconstruct one receipt, refusing a version this axis did not define.

    An UNSTAMPED receipt is read on this axis, the same default
    :meth:`GroundingVerdict.from_receipt` applies: a hand-authored record with no
    version claims no other definition, and one such record must not deny the
    whole report.
    """
    version = receipt.get("version")
    if version is not None and version != GROUNDING_AXIS_VERSION:
        raise GroundingAxisMismatchError(
            f"receipt written under grounding axis {version}, this release "
            f"computes {GROUNDING_AXIS_VERSION}. The two disagree about which "
            "reads match the cited set, so one report cannot mix them. "
            "Summarize each axis separately."
        )
    return GroundingVerdict.from_receipt(receipt)


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

        The numerator is the collapse (``naive - number`` summed over
        corroborations); the denominator is the naive supporting lines from
        CORROBORATIONS only (findings with ``naive >= 2``), a single supporting
        line is not a corroboration and never dilutes it, so the rate cannot be
        understated by padding. Zero when nothing corroborated, never a
        divide-by-zero.
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
        number = as_int(rec.get("number"))
        naive = as_int(rec.get("naive"))
        if bool(rec.get("soft")):
            unverifiable += 1
        if number <= 0:
            at_zero += 1
        elif number == 1:
            at_one += 1
        else:
            at_two_plus += 1
        # The collapse rate is over CORROBORATIONS: a body a naive signer-axis
        # counter would call independent (naive >= 2). A single supporting line
        # (naive <= 1) is not a corroboration and must not dilute the denominator,
        # which would UNDERSTATE the same-model collapse, the audited-pipeline-
        # favorable direction the measurement exists to expose. naive folds the
        # model axis into number (number <= naive on any hard body); clamp so a
        # malformed record can never make the collapse negative.
        if naive >= 2:
            naive_total += naive
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


# The four influence verdicts a record can carry, matching
# :class:`mareforma.observe.oracle.OracleInfluence`. A record whose verdict is
# none of these is counted as NOT_TESTED with reason ``unknown``, the same
# degrade-don't-raise discipline the independence arm uses for a malformed field.
_INFLUENCE_VERDICTS = ("INFLUENCED", "NOT_INFLUENCED", "UNDECIDABLE", "NOT_TESTED")


@dataclass(frozen=True)
class InfluenceReport:
    """The influence arm of the measurement, over per-EDGE influence records.

    Where :class:`GroundingReport` answers "did the cited data flow" and
    :class:`IndependenceReport` answers "how independent is the corroboration,"
    this answers "does the finding depend on the data it cites." Its unit is the
    EDGE, a (finding, cited source) pair, because one finding can cite several
    sources and each is influenced or not on its own; a run's receipts flatten
    1:N into edges (see :func:`influence_records`). The reporting unit is the edge,
    but the inference unit is the run: a rate never prints without the number of
    distinct runs behind it, so a reader cannot mistake many edges from one run
    for many independent measurements.

    NOT_TESTED is first-class, not a gap. The causal oracle declines on many
    targets (it needs one that re-runs cheaply and near-deterministically), and
    the ``mareforma audit`` path observes flow with a single run and never
    perturbs, so on real audit output every edge is NOT_TESTED. That is the honest
    state the release records rather than letting a grounding verdict stand in for
    an influence claim nobody measured. NOT_TESTED is excluded from the rate
    denominator (a rate is over edges the oracle actually decided) but counted and
    bucketed by reason, and its dominance is a coverage signal like OPAQUE's.
    """

    total: int
    influenced: int
    not_influenced: int
    undecidable: int
    not_tested: int
    not_tested_by_reason: dict[str, int] = field(default_factory=dict)
    distinct_runs: int = 0

    @property
    def resolved(self) -> int:
        """Edges the oracle actually decided: the rate denominator (NOT_TESTED out)."""
        return self.total - self.not_tested

    def not_tested_dominates(self, threshold: float = 0.5) -> bool:
        """True when NOT_TESTED covers at least ``threshold`` of the edges.

        The influence analog of :meth:`GroundingReport.opaque_dominates`: when most
        edges were never tested, an influence rate over the few that were is not a
        trustworthy prevalence number and the report says so.
        """
        return self.total > 0 and self.not_tested / self.total >= threshold

    @property
    def influenced_fraction(self) -> float:
        """INFLUENCED over RESOLVED edges, never over the NOT_TESTED-inflated total."""
        return 0.0 if self.resolved == 0 else self.influenced / self.resolved

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "resolved": self.resolved,
            "counts": {
                "INFLUENCED": self.influenced,
                "NOT_INFLUENCED": self.not_influenced,
                "UNDECIDABLE": self.undecidable,
                "NOT_TESTED": self.not_tested,
            },
            "not_tested_by_reason": dict(self.not_tested_by_reason),
            "not_tested_dominates": self.not_tested_dominates(),
            "distinct_runs": self.distinct_runs,
            "influenced_fraction": self.influenced_fraction,
        }

    def closing_sentence(self) -> str:
        """A plain-English one-line summary a reviewer can read without the JSON."""
        if self.total == 0:
            return "No influence records to measure."
        if self.resolved == 0:
            # Every edge NOT_TESTED: say exactly that, never what a resolved
            # report says. A report with no scrambles declared reads differently.
            reasons = ", ".join(
                f"{r}={n}" for r, n in sorted(self.not_tested_by_reason.items())
            )
            tail = f" ({reasons})" if reasons else ""
            return (
                f"Influence not tested on any of {self.total} edges{tail}: the "
                f"oracle did not run, so no influence prevalence is claimed."
            )
        lead = (
            f"Across {self.resolved} of {self.total} edges the oracle decided "
            f"(over {self.distinct_runs} distinct runs): {self.influenced_fraction:.0%} "
            f"INFLUENCED."
        )
        if self.not_tested:
            lead += f" {self.not_tested} edges NOT_TESTED, excluded from the rate."
        return lead


def influence_records(receipts: Iterable[dict]) -> list[dict]:
    """The per-EDGE influence records carried by a run's receipts, flattened 1:N.

    A combined receipt carries a list under ``"influence"``, one record per cited
    source, since one finding can cite several. This flattens those lists into a
    flat list of edge records, so a finding that cites three sources contributes
    three edges to the arm. A receipts file with no influence records yields an
    empty list (the arm is simply not reported, never fabricated). This is the
    1:N difference from :func:`independence_records`, which is 1:1 per finding.
    """
    out: list[dict] = []
    for r in receipts:
        recs = r.get("influence")
        if isinstance(recs, list):
            for rec in recs:
                if isinstance(rec, dict):
                    out.append(rec)
    return out


def summarize_influence(records: Iterable[dict]) -> InfluenceReport:
    """Aggregate per-edge influence records into the influence report.

    Each record carries an ``"influence"`` verdict and, when NOT_TESTED, an
    optional typed ``"not_tested_reason"``. A record with an unrecognized verdict
    degrades to NOT_TESTED(unknown) rather than raising, so one malformed record
    never denies the whole arm. ``distinct_runs`` is summed from any record that
    carries a run count, so a rate always has the run number beside it.
    """
    total = influenced = not_influenced = undecidable = not_tested = 0
    by_reason: dict[str, int] = {}
    distinct_runs = 0
    for rec in records:
        total += 1
        verdict = rec.get("influence")
        if verdict == "INFLUENCED":
            influenced += 1
        elif verdict == "NOT_INFLUENCED":
            not_influenced += 1
        elif verdict == "UNDECIDABLE":
            undecidable += 1
        else:
            not_tested += 1
            reason = rec.get("not_tested_reason")
            if not reason:
                # NOT_TESTED with no typed reason is the audit path (the oracle
                # did not run at all); an unrecognized verdict is malformed.
                reason = "not-run" if verdict == "NOT_TESTED" else "unknown"
            by_reason[reason] = by_reason.get(reason, 0) + 1
        # A record's own run count, summed across edges. A writer that flattens
        # one finding's oracle result into N source-edges must stamp the run
        # count on ONE edge (or split it), never copy the finding's count onto
        # all N, or a single N-source finding measured over one run would report
        # N runs, the very "many edges from one run" confusion the edge/run split
        # exists to prevent.
        runs = rec.get("distinct_runs")
        if isinstance(runs, int) and runs > 0:
            distinct_runs += runs
    return InfluenceReport(
        total=total,
        influenced=influenced,
        not_influenced=not_influenced,
        undecidable=undecidable,
        not_tested=not_tested,
        not_tested_by_reason=dict(sorted(by_reason.items())),
        distinct_runs=distinct_runs,
    )


def summarize_influence_receipts(receipts: Iterable[dict]) -> InfluenceReport:
    """Aggregate the influence records carried by a run's receipts."""
    return summarize_influence(influence_records(receipts))


def _empty_influence() -> InfluenceReport:
    """The default influence arm: no edges, for a report built without one."""
    return InfluenceReport(0, 0, 0, 0, 0)


@dataclass(frozen=True)
class PilotReport:
    """A slim natural-prevalence pilot: both arms plus the honest coverage bound.

    The pilot is the cheap pre-check before the full natural-corpus run (see the
    kill-switch fixtures): a small receipts file of real findings yields the
    grounding split and the independence distribution TOGETHER, with the OPAQUE
    fraction reported as the honesty gate. When OPAQUE dominates, the observer
    could not see enough of the pipeline for the split to be a trustworthy
    prevalence number, so the report says so rather than over-claiming, the
    grounded prevalence reads as a lower bound until the observer attaches deeper.
    """

    grounding: GroundingReport
    independence: IndependenceReport
    # The influence arm is defaulted so the two existing construction sites, which
    # pass grounding and independence by keyword, keep working unchanged. A report
    # built without influence records carries the empty arm and prints nothing for
    # it, exactly as it did before the arm existed.
    influence: InfluenceReport = field(default_factory=_empty_influence)

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
            "influence": (
                self.influence.to_dict() if self.influence.total else None
            ),
            "opaque_fraction": self.grounding.opaque_fraction,
            "opaque_dominates": self.opaque_dominates(),
            "coverage_bound": self.coverage_bound(),
        }

    def closing_sentence(self) -> str:
        lead = self.grounding.closing_sentence()
        if self.independence.total:
            lead += " " + self.independence.closing_sentence()
        if self.influence.total:
            lead += " " + self.influence.closing_sentence()
        return lead + " " + self.coverage_bound()


def summarize_pilot(receipts: Iterable[dict]) -> PilotReport:
    """Run the slim natural-prevalence pilot over a receipts file.

    Reads the receipts once into a list (they are iterated three times: for the
    grounding split, the independence arm, and the influence arm) and returns the
    three reports plus the OPAQUE-coverage bound. The independence and influence
    arms are present only when a receipt carries the matching record, so a
    grounding-only pilot still reports.
    """
    receipts = list(receipts)
    return PilotReport(
        grounding=summarize_receipts(receipts),
        independence=summarize_independence(independence_records(receipts)),
        influence=summarize_influence(influence_records(receipts)),
    )
