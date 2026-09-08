"""Can a reported number be traced to a run that produced it.

The influence oracle asks whether a finding depends on the data it cites. This
asks a different and cheaper question: does the number in the write-up appear in
the outputs the work released. It runs nothing and needs no key. Where the
oracle needs a target that re-runs cheaply and near-deterministically, this needs
only two lists, so it reaches work the oracle cannot.

Three verdicts per reported number:

``TRACEABLE``
    a released output for that slot carries this value, within the precision the
    number was written to.
``UNTRACEABLE``
    nothing released covers that slot at all. Not an accusation on its own: the
    outputs handed in may simply not include it.
``CONTRADICTED``
    the released outputs DO cover that slot and none of them carry this value,
    or a claim about the direction of a change disagrees with the released
    numbers it is derived from.

Plus one check no per-number test can make. When the same quantity is stated
more than once with values that cannot all be right, each may trace to some run
on its own while the write-up contradicts itself. That is reported per slot,
separately from the per-number verdicts.

**Scope, and it is narrow.** A TRACEABLE number is one that appears in the
outputs supplied. It is not evidence the run was correct, that the output was
produced by the pipeline described, or that the right output was selected. The
caller supplies both sets, so what this compares is what it was handed.
"""
from __future__ import annotations

import decimal
import math
import re

from dataclasses import dataclass
from enum import Enum
from typing import Iterable


class NumericProvenance(str, Enum):
    """Where a reported number stands against the released outputs."""

    TRACEABLE = "TRACEABLE"
    UNTRACEABLE = "UNTRACEABLE"
    CONTRADICTED = "CONTRADICTED"


class Direction(str, Enum):
    """The direction a derived claim asserts a quantity moved."""

    INCREASE = "increase"
    DECREASE = "decrease"


@dataclass(frozen=True)
class ReleasedValue:
    """One number a run actually produced.

    ``slot`` names the quantity, and it is the caller's vocabulary: the two sets
    are joined on it and nothing here interprets it. ``source`` names the run,
    so a report can say which one a value came from when several disagree.
    """

    slot: str
    value: float
    source: str = ""


@dataclass(frozen=True)
class ReportedValue:
    """One number as the write-up states it.

    ``as_written`` is the number as it appears, and it sets the tolerance: a
    value written to three decimals is compared to three decimals, because that
    is the precision its author claimed. Passing the float alone would either
    reject honest rounding or accept anything.

    ``where`` names the place in the write-up. It has no part in the per-number
    verdict and is what makes the cross-section check legible when one quantity
    is stated in several places.

    ``compares`` and ``direction`` describe a derived claim: a statement that
    some quantity went up or down between two slots. Both are needed together,
    and with them the claim is checked against the released numbers rather than
    only looked up.
    """

    slot: str
    value: float
    as_written: str
    where: str = ""
    compares: "tuple[str, str] | None" = None
    direction: "Direction | None" = None


@dataclass(frozen=True)
class ProvenanceFinding:
    """One reported number, placed, with the reason and what it matched."""

    reported: ReportedValue
    verdict: NumericProvenance
    reason: str
    matched_sources: "tuple[str, ...]" = ()


class UnreadableNumber(ValueError):
    """A reported number's written form could not be read.

    Refused rather than guessed at. Every earlier attempt to be lenient here
    widened the tolerance instead of narrowing the input: a form this could not
    parse fell back to zero decimals, which is a band of half a unit, and a
    p-value written ``5e-8`` then matched a released ``0.49``.
    """


# The numeric token inside a written form, with whatever decoration the text
# carried around it. Significance stars, a trailing comma, a closing bracket, a
# percent sign or a plus-or-minus term all appear in extracted table cells, and
# counting them as digits inflates the precision and manufactures a
# disagreement out of a number that rounds correctly.
_WRITTEN_NUMBER = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")

# The forms a non-finite value is written in. They carry no precision, and the
# comparison handles them on their own, so they are recognised rather than read.
_NON_FINITE = re.compile(r"[-+]?(inf(inity)?|nan)", re.I)


def _exponent(as_written: str) -> int:
    """The power of ten of the last place *as_written* commits to.

    Zero for ``"3"``, minus three for ``"0.093"``, minus eight for ``"5e-8"``.
    Read through :class:`decimal.Decimal` rather than by counting characters
    after a dot, because a mantissa in scientific notation has no dot at all and
    character-counting scored it as an integer, turning the tightest numbers in
    a write-up into the loosest comparison it makes.

    **Scientific notation is read at the significant figures it states, and that
    cuts both ways.** ``"5e-8"`` commits to the eighth decimal, so it is held to
    a band of 5e-9. ``"1e5"`` commits to one figure, so it is held to plus or
    minus fifty thousand, and a released 149999 does carry it. The same quantity
    written ``"100000"`` commits to six figures and is held to half a unit, so
    the two forms of one number are compared very differently. That asymmetry is
    intended and it is what the notations mean: an author writing ``1e5`` has
    said one figure and nothing more, and holding them to six would manufacture
    a disagreement out of a number they never claimed. The cost is that a loosely
    written number is nearly uncontradictable, so the width is put in the
    finding's own reason rather than left for a reader to work out.

    Raises :class:`UnreadableNumber` when there is no number to read.
    """
    # A non-finite value has no last place to commit to, and the comparison
    # short-circuits on it anyway, so the exponent it reports is not used.
    if _NON_FINITE.fullmatch(as_written.strip()):
        return 0
    # Grouping separators are stripped first. Left in, the search took the
    # digits before the first comma, so "1,234.56" read as "1" and opened a band
    # of half a unit on a number written to two decimals. Cell counts and sample
    # sizes are normally written this way.
    cleaned = as_written.replace(",", "").replace("_", "").replace("\u2009", "")
    match = _WRITTEN_NUMBER.search(cleaned)
    if match is None:
        raise UnreadableNumber(
            f"no number to read in {as_written!r}, so the precision it claims "
            "cannot be known and nothing can be compared against it"
        )
    try:
        exponent = decimal.Decimal(match.group()).as_tuple().exponent
    except decimal.InvalidOperation as exc:      # pragma: no cover
        raise UnreadableNumber(f"cannot read {as_written!r}") from exc
    return int(exponent)


def _band(exponent: int) -> float:
    """Half a unit in the last place a written form committed to.

    Raises :class:`UnreadableNumber` on a form whose precision is out of float
    range, which is the same refusal an unreadable form gets: a band that cannot
    be computed is not a wide band, it is no comparison at all.
    """
    try:
        return 0.5 * (10.0 ** exponent)
    except OverflowError as exc:
        raise UnreadableNumber(
            f"a written precision of 1e{exponent} is out of range"
        ) from exc


def _matches(value: float, released: float, exponent: int) -> bool:
    """True when *released* rounds to *value* at the precision written.

    Half a unit in the last place the write-up committed to, which is exactly
    the set of values that round to what it says, and nothing else.

    There is no relative term. One rode alongside at five parts in ten thousand,
    said to cover "the float arithmetic that produced either number", and float
    arithmetic is a relative error of about 1e-16. Five parts in ten thousand is
    twelve orders of magnitude looser, and it won whenever the value was above
    ten at two decimals, which is most numbers in a results table. An accuracy
    written ``87.35`` matched a released ``87.39``, and a cell count written
    ``1235000`` matched ``1234567``, both reported as carrying the value at the
    precision it was written to.

    Non-finite values are the caller's to handle before here. Infinity minus
    infinity is not a number, and comparing that against anything is false, so
    an infinity would have reported that the outputs do not carry the infinity
    they carry.
    """
    if not (math.isfinite(value) and math.isfinite(released)):
        return value == released
    band = _band(exponent)
    # Relative, not additive. A flat 1e-12 rode here to cover float slop and
    # became the whole band below that magnitude: a p-value written 5e-300
    # matched a released 9e-13, 287 orders of magnitude away, reported as
    # carrying the value at the precision it was written to. Float slop is
    # relative to the numbers compared, so the term is too.
    slop = 1e-15 * max(abs(value), abs(released))
    return abs(value - released) <= band + slop


def _direction_of(earlier: float, later: float) -> "Direction | None":
    """Which way the quantity moved, or None when it did not move."""
    if later > earlier:
        return Direction.INCREASE
    if later < earlier:
        return Direction.DECREASE
    return None


@dataclass(frozen=True)
class ProvenanceReport:
    """The provenance arm of the measurement, over reported numbers.

    ``inconsistent_slots`` is the cross-section result and is deliberately not
    folded into the per-number verdicts: those numbers each trace, and the
    finding is about the write-up rather than about any one of them.
    """

    findings: "tuple[ProvenanceFinding, ...]" = ()
    inconsistent_slots: "tuple[str, ...]" = ()
    released_slots: int = 0

    @property
    def total(self) -> int:
        return len(self.findings)

    @property
    def traceable(self) -> int:
        return self._count(NumericProvenance.TRACEABLE)

    @property
    def untraceable(self) -> int:
        return self._count(NumericProvenance.UNTRACEABLE)

    @property
    def contradicted(self) -> int:
        return self._count(NumericProvenance.CONTRADICTED)

    def _count(self, verdict: NumericProvenance) -> int:
        return sum(1 for f in self.findings if f.verdict is verdict)

    @property
    def covered(self) -> int:
        """Numbers whose slot the released outputs cover at all.

        The denominator for any rate worth printing. UNTRACEABLE numbers are
        excluded because nothing was handed in that could have carried them, so
        counting them would report the gaps in the caller's output set as though
        they were gaps in the work.
        """
        return self.traceable + self.contradicted

    def closing_sentence(self) -> str:
        """One sentence naming the rate, its denominator and what it excludes.

        Refuses to print a bare rate, the same discipline the influence report
        holds: a number without its denominator and its uncovered count reads as
        a prevalence figure over the whole write-up, which it is not.
        """
        if not self.findings:
            return "No reported numbers were checked."
        parts = [
            f"{self.contradicted} of {self.covered} reported numbers whose slot "
            f"the released outputs cover disagree with them"
        ]
        if self.untraceable:
            parts.append(
                f"{self.untraceable} more name a slot nothing released covers, "
                "so they were not checkable either way"
            )
        if self.inconsistent_slots:
            parts.append(
                f"{len(self.inconsistent_slots)} quantit"
                f"{'y is' if len(self.inconsistent_slots) == 1 else 'ies are'} "
                "stated more than once with values that cannot all be right"
            )
        return "; ".join(parts) + "."


def provenance_diff(
    reported: "Iterable[ReportedValue]",
    released: "Iterable[ReleasedValue]",
) -> ProvenanceReport:
    """Place every reported number against the released outputs.

    Runs nothing and opens nothing. Both sets are the caller's: extracting
    numbers from a write-up and collecting the outputs of a run are jobs that
    differ per corpus, and building either into this would make the detector fit
    one corpus and no other.
    """
    by_slot: "dict[str, list[ReleasedValue]]" = {}
    for item in released:
        by_slot.setdefault(item.slot, []).append(item)

    findings: "list[ProvenanceFinding]" = []
    for claim in reported:
        findings.append(_place(claim, by_slot))

    return ProvenanceReport(
        findings=tuple(findings),
        inconsistent_slots=_inconsistent_slots(findings),
        released_slots=len(by_slot),
    )


def _place(
    claim: ReportedValue, by_slot: "dict[str, list[ReleasedValue]]",
) -> ProvenanceFinding:
    """The verdict for one reported number."""
    derived = _check_direction(claim, by_slot)
    if derived is not None:
        return derived

    candidates = by_slot.get(claim.slot, [])
    if not candidates:
        return ProvenanceFinding(
            claim, NumericProvenance.UNTRACEABLE,
            f"nothing released covers {claim.slot!r}, so this value could not "
            "be checked either way",
        )
    exponent = _exponent(claim.as_written)
    hits = tuple(
        c.source for c in candidates if _matches(claim.value, c.value, exponent)
    )
    # The width is named, not implied. A form stating one significant figure is
    # held to a band of tens of thousands and a form stating six is held to half
    # a unit, and both print the same word. A reader counting traceable numbers
    # cannot tell those apart unless the finding says how far it looked. Left off
    # a non-finite value, which has no last place and never reached the band.
    within = (
        f" (within {_band(exponent):g})" if math.isfinite(claim.value) else ""
    )
    if hits:
        return ProvenanceFinding(
            claim, NumericProvenance.TRACEABLE,
            f"a released output for {claim.slot!r} carries this value at the "
            f"precision it was written to ({claim.as_written}){within}",
            hits,
        )
    shown = ", ".join(f"{c.value:g}" for c in candidates[:4])
    return ProvenanceFinding(
        claim, NumericProvenance.CONTRADICTED,
        f"the released outputs for {claim.slot!r} carry {shown} and none of "
        f"them rounds to {claim.as_written}{within}",
    )


def _direction_pairs(
    earlier: "list[ReleasedValue]", later: "list[ReleasedValue]",
) -> "list[tuple[ReleasedValue, ReleasedValue]]":
    """The before-and-after pairs a direction can honestly be read from.

    Within a run first. A run that recorded both ends of the comparison is the
    only place the movement between them is a fact rather than an arithmetic
    accident. Crossing every earlier value with every later one asks what
    happened between one run's baseline and another run's result, which is a
    question neither run answers: two runs where the quantity rose in both came
    back as "the released runs disagree", because the higher run's baseline sits
    above the lower run's result.

    The cross product is the fallback and it is needed. The shape this detector
    was built on states its baseline and its own result from different runs, so
    no run carries both ends, and demanding one would refuse the case the module
    exists to catch.
    """
    shared = {v.source for v in later}
    within = [
        (before, after)
        for before in earlier if before.source in shared
        for after in later if after.source == before.source
    ]
    if within:
        return within
    return [(before, after) for before in earlier for after in later]


def _check_direction(
    claim: ReportedValue, by_slot: "dict[str, list[ReleasedValue]]",
) -> "ProvenanceFinding | None":
    """Place a derived claim about which way a quantity moved, or None.

    This is the shape that catches an improvement claimed over numbers that got
    worse. The stated value can be perfectly traceable while the sentence built
    around it points the wrong way, so the direction is checked before the
    membership test and stands in for it when it fails.
    """
    if claim.compares is None or claim.direction is None:
        return None
    first, second = claim.compares
    earlier = by_slot.get(first, [])
    later = by_slot.get(second, [])
    if not earlier or not later:
        return ProvenanceFinding(
            claim, NumericProvenance.UNTRACEABLE,
            f"the released outputs do not cover both {first!r} and {second!r}, "
            "so the direction of the change could not be checked",
        )
    # Every pair, not the first of each. Reading one released value per slot made
    # the verdict depend on the order of a list: the same claim against the same
    # released set came back untraceable or contradicted depending on which run
    # happened to be first. Everywhere else this module tests a reported value
    # against every candidate, and this is no different.
    pairs = _direction_pairs(earlier, later)
    seen = {_direction_of(before.value, after.value) for before, after in pairs}
    if seen == {claim.direction}:
        return None
    if claim.direction in seen:
        return ProvenanceFinding(
            claim, NumericProvenance.UNTRACEABLE,
            f"the released runs disagree about which way {first!r} moved to "
            f"{second!r}, so the direction this claims cannot be checked "
            "against them",
        )
    # The pairs the verdict was read from, and only those. The report quoted the
    # first value of each slot instead, which is not a pair the check looked at:
    # against runs that fell and held still, a claimed rise was refused with
    # "did not move: 10 then 7", a sentence contradicted by the two numbers in
    # it. A report a reader can check against its own quoted evidence is the
    # least this can offer, given it is accusing a write-up of the same fault.
    shown = ", ".join(
        f"{before.value:g} then {after.value:g}" for before, after in pairs[:4]
    )
    if len(seen) == 1:
        actual = next(iter(seen))
        moved = (
            "did not move" if actual is None
            else f"moved the other way ({actual.value})"
        )
        said = f"the released outputs say it {moved}: {shown}"
    else:
        # The released runs move it several ways and none of them is the way
        # this claims. Naming one of them as "the" direction would be picking a
        # disagreement out of a set and reporting it as the finding.
        said = f"no released run moves it that way: {shown}"
    article = "an" if claim.direction.value[0] in "aeiou" else "a"
    return ProvenanceFinding(
        claim, NumericProvenance.CONTRADICTED,
        f"this claims {article} {claim.direction.value} from {first!r} to "
        f"{second!r}, and {said}",
        tuple(dict.fromkeys(
            v.source for pair in pairs for v in pair
        )),
    )


def _readable(as_written: str) -> bool:
    """True when a written form carries a precision this can compare at."""
    try:
        _exponent(as_written)
    except UnreadableNumber:
        return False
    return True


def _inconsistent_slots(
    findings: "Iterable[ProvenanceFinding]",
) -> "tuple[str, ...]":
    """Slots stated more than once with values that cannot all be right.

    Each of those values may trace to some released run on its own, which is
    what makes this invisible to the per-number test: the write-up disagrees
    with itself rather than with the outputs. Compared at the coarser of the two
    precisions, so a number restated with fewer decimals is not called
    inconsistent for being rounded.

    Runs over every reported number, whatever its verdict. It was restricted to
    the traceable ones, and that made a check needing no released outputs depend
    on having them: a write-up stating one quantity three incompatible ways with
    nothing released said nothing at all. Coverage is the caller's to supply and
    varies with how well their two sets join, so tying this to it puts a property
    of the write-up at the mercy of that join. This is a property of the write-up
    alone.
    """
    seen: "dict[str, list[ReportedValue]]" = {}
    for finding in findings:
        seen.setdefault(finding.reported.slot, []).append(finding.reported)
    out = []
    for slot, claims in seen.items():
        if len(claims) < 2:
            continue
        # A form with no number to read has no precision to compare at, so it
        # drops out of this check rather than raising. It reaches here now that
        # every finding is considered, and a placeholder cell like "--" is
        # ordinary in an extracted table: one of them used to decide whether the
        # whole report existed, based on how many times it appeared.
        claims = [c for c in claims if _readable(c.as_written)]
        if len(claims) < 2:
            continue
        if any(
            not _matches(
                a.value, b.value,
                max(_exponent(a.as_written), _exponent(b.as_written)),
            )
            for i, a in enumerate(claims) for b in claims[i + 1:]
        ):
            out.append(slot)
    return tuple(sorted(out))


# --- the cross-tab -----------------------------------------------------------

class CrossTab(str, Enum):
    """How the two detectors line up on one finding.

    Neither number means much alone. A hollow rate with no provenance arm cannot
    tell a finding that ignores its data from one whose data was never in the
    outputs, and a provenance rate with no influence arm cannot tell a number
    that is merely absent from one that nothing computed. The two middle
    categories exist only because both ran on the same finding.
    """

    #: Both arms decided and both are clean: the finding depends on its data and
    #: the number is in the outputs.
    AGREE = "AGREE"
    #: Both arms decided and neither found anything: the finding does not depend
    #: on its data and nothing released covers the number either. No information
    #: from either side, which is not the same as a clean result and used to
    #: share a label with one.
    AGREE_ABSENT = "AGREE_ABSENT"
    #: The data demonstrably matters and the number is not in the outputs.
    TENSION = "TENSION"
    #: The number is in the outputs and the finding does not depend on the data.
    #: The cited-but-hollow case, and the reason both arms ship together.
    CONSTRUCT_DIFFERENCE = "CONSTRUCT_DIFFERENCE"
    #: The outputs contradict the number AND the finding does not depend on the
    #: data. Both arms found something, pointing the same way, which is the worst
    #: cell on the grid. It read as agreement, in the same bucket as the two
    #: clean cases, so a reader counting agreement could not tell them apart.
    CONVERGENT_FAILURE = "CONVERGENT_FAILURE"
    OBSERVER_BLIND = "OBSERVER_BLIND"
    INCONCLUSIVE = "INCONCLUSIVE"


def cross_tabulate(
    influence: "str | None", provenance: "NumericProvenance | None",
) -> CrossTab:
    """Place one finding on both axes at once.

    *influence* is an :class:`~mareforma.observe.oracle.Influence` value or its
    string, or None when the oracle did not run.

    Both arms are coerced from their string forms and an unrecognised token
    raises. The provenance arm used identity while the influence arm accepted a
    string, and both enums serialise to bare strings, so a report read back from
    JSON placed every finding in the wrong cell: the cited-but-hollow case, the
    one the cross-tab exists to name, came back as agreement.

    The grid, six outcomes rather than five:

    - both clean, the finding depends on its data and the number is there:
      ``AGREE``.
    - neither arm found anything, no dependence and no coverage: ``AGREE_ABSENT``.
      Not the same as clean, and it used to share a label with it.
    - the data matters and the number is not in the outputs: ``TENSION``.
    - the number is there and the finding does not depend on the data:
      ``CONSTRUCT_DIFFERENCE``, the cited-but-hollow case.
    - the outputs contradict the number and the finding does not depend on the
      data: ``CONVERGENT_FAILURE``, the worst cell, which also used to read as
      agreement.
    - the oracle could not see the target: ``OBSERVER_BLIND``.
    - anything either arm declined to call: ``INCONCLUSIVE``, which on real
      statistics is the modal answer rather than a rare edge.
    """
    # Both arms coerced before any routing. Validating the provenance arm after
    # the early returns meant garbage passed whenever the oracle did not run,
    # which is the modal case on real corpora.
    influence = _influence_token(influence)
    provenance = None if provenance is None else NumericProvenance(provenance)
    if influence in (None, "NOT_TESTED"):
        return CrossTab.OBSERVER_BLIND
    if provenance is None:
        return CrossTab.INCONCLUSIVE
    if influence == "UNDECIDABLE":
        return CrossTab.INCONCLUSIVE
    if influence == "INFLUENCED":
        return (
            CrossTab.AGREE
            if provenance is NumericProvenance.TRACEABLE
            else CrossTab.TENSION
        )
    if provenance is NumericProvenance.TRACEABLE:
        return CrossTab.CONSTRUCT_DIFFERENCE
    if provenance is NumericProvenance.CONTRADICTED:
        return CrossTab.CONVERGENT_FAILURE
    return CrossTab.AGREE_ABSENT


# The influence arm's four verdicts. Named here so an unrecognised token raises
# instead of falling through: every wrong spelling used to land on INCONCLUSIVE,
# which is a real verdict, so a typo or a swapped argument read as a measurement
# the oracle declined rather than as the mistake it was.
_INFLUENCE_TOKENS = frozenset(
    {"INFLUENCED", "NOT_INFLUENCED", "UNDECIDABLE", "NOT_TESTED"}
)


def _influence_token(influence) -> "str | None":
    """*influence* as one of the four verdicts, or None when the oracle
    did not run."""
    if influence is None:
        return None
    token = getattr(influence, "value", influence)
    if token not in _INFLUENCE_TOKENS:
        raise ValueError(
            f"{influence!r} is not an influence verdict. Expected one of "
            f"{sorted(_INFLUENCE_TOKENS)}, or None when the oracle did not run."
        )
    return token
