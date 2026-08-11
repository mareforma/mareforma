"""Instrument self-check: seeded failures a correct instrument must catch.

Before spending on a natural-corpus run, the instrument has to prove it catches
the failures it claims to. Each fixture here builds ONE such failure with the real
observer and measurement machinery, no mocks of the thing under test, and reports
whether the instrument caught it. If any is missed, the measurement is not yet
trustworthy. These are seeded dissociation fixtures with known ground truth, not a
prevalence estimate.

This subpackage ships in the wheel so an installed user can run the self-check on
their own machine, not only from the source tree. It carries the four fixtures
that need nothing but the observer and the filesystem; the two model-axis fixtures
stay in the test suite because they need a live provider socket that ``pytest``'s
HTTP machinery supplies, and no substitution preserves their outcome.

**This is a self-check, not a sensitivity demonstration, and does not satisfy the
pillar that sensitivity be shown on the target's OWN path.** Every fixture runs its
own synthetic pipeline rather than a user's target, and all four are accidents, a
zero-row read, an unread partition, a number with no execution, a decoy read,
rather than the evasive case that pillar requires. No release copy may claim these
close that gap.

The four failures:

1. a silent zero-row fallback , a cited read that returned nothing is seen, never GROUNDED;
2. an excluded partition      , a cited source that was never read is named, not hidden;
3. a number with no execution , a finding with no observed cited read is UNGROUNDED (empty provenance);
4. a decoy incidental read    , a non-cited read is refused as grounding.

``run_selfcheck(tmp_path)`` runs the four and returns their outcomes.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from mareforma.observe import ObservedGrounding, observe
from mareforma.observe._citation import normalize_identifier
from mareforma.observe.measure import summarize


@dataclass(frozen=True)
class KillSwitchOutcome:
    """One self-check fixture's result: what a correct instrument must show."""

    name: str
    expectation: str
    observed: str
    caught: bool


def silent_zero_row_fallback(tmp_path: Path) -> KillSwitchOutcome:
    data = tmp_path / "ks1_empty.csv"
    data.write_text("")  # zero rows: the query "returned" nothing
    with observe(cites=str(data)) as h:
        open(str(data)).read()  # the read happens but carries nothing
    v = h.verdict
    # The honest shape, not merely "not GROUNDED": the observer saw the one
    # read and saw that it carried nothing. A blind seam records no read and
    # lands OPAQUE, which must not read as a catch.
    caught = (
        v.grounding is ObservedGrounding.UNGROUNDED
        and len(v.reads) == 1
        and not v.reads[0].nonempty
    )
    return KillSwitchOutcome(
        "silent_zero_row_fallback",
        "a zero-row cited read is observed and is never GROUNDED",
        f"{v.grounding.value}, nonempty={[r.nonempty for r in v.reads]}",
        caught,
    )


def excluded_partition(tmp_path: Path) -> KillSwitchOutcome:
    part_a = tmp_path / "ks2_part_a.csv"
    part_a.write_text("x\n1\n")
    part_b = tmp_path / "ks2_part_b.csv"
    part_b.write_text("x\n2\n")
    with observe(cites=[str(part_a), str(part_b)]) as h:
        open(str(part_a)).read()  # partition B is silently excluded
    v = h.verdict
    grounded = set(v.grounded_sources)
    excluded = set(v.cited_sources) - grounded
    # The excluded partition is named even though A grounded the finding, so
    # check both halves against the verdict's own normalized identifiers. A
    # binder that never binds reports BOTH partitions excluded, which is a
    # blind observer, not a catch.
    caught = (
        grounded == {normalize_identifier(str(part_a))}
        and excluded == {normalize_identifier(str(part_b))}
    )
    return KillSwitchOutcome(
        "excluded_partition",
        "a cited partition that was never read is named (cited minus grounded)",
        f"grounded={sorted(grounded)}, excluded={sorted(excluded)}", caught,
    )


def number_with_no_execution(tmp_path: Path) -> KillSwitchOutcome:
    data = tmp_path / "ks4.csv"
    data.write_text("x\n1\n")
    with observe(cites=str(data)) as h:
        _ = 2 + 2  # a number produced with no observed cited read: empty provenance
    v = h.verdict
    # Blind-equivalent by construction: the scope contains no read, so a blind
    # observer reports the same empty provenance. What makes this UNGROUNDED
    # worth reading is the read-axis control in the test suite, which proves the
    # seam was live for the run.
    caught = v.grounding is ObservedGrounding.UNGROUNDED and len(v.reads) == 0
    return KillSwitchOutcome(
        "number_with_no_execution",
        "a number with no observed cited read is UNGROUNDED (empty provenance)",
        f"{v.grounding.value}, reads={len(v.reads)}", caught,
    )


def decoy_incidental_read(tmp_path: Path) -> KillSwitchOutcome:
    data = tmp_path / "ks5_data.csv"
    data.write_text("x\n1\n")
    decoy = tmp_path / "ks5_config.yaml"
    decoy.write_text("k: v\n")
    with observe(cites=str(data)) as h:
        open(str(decoy)).read()  # a non-cited decoy read
    v = h.verdict
    report = summarize([v])
    caught = (
        v.grounding is not ObservedGrounding.GROUNDED
        and report.incidental_reads == 1
    )
    return KillSwitchOutcome(
        "decoy_incidental_read",
        "a non-cited read is refused as grounding and flagged incidental",
        f"{v.grounding.value}, incidental={report.incidental_reads}", caught,
    )


# The four self-checks that need only the observer and the filesystem.
SELF_CHECKS = (
    silent_zero_row_fallback,
    excluded_partition,
    number_with_no_execution,
    decoy_incidental_read,
)


def run_selfcheck(tmp_path: Path) -> "list[KillSwitchOutcome]":
    """Run the four shipped self-check fixtures and return their outcomes."""
    return [case(tmp_path) for case in SELF_CHECKS]
