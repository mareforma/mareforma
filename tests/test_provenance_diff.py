"""Can a reported number be traced to a run that produced it.

The two shapes here are the two that were found on released work by running this
method by hand: a headline improvement claimed over numbers that moved the other
way, and one quantity stated in three places with three different values, each
of which traced to a different run on its own.

Neither is a missing number. Both write-ups reported values their own outputs
carried. What they got wrong was the sentence around them, which is the class
this catches and the influence oracle cannot: the oracle asks whether the
finding depends on the data, and both of these do.
"""

from __future__ import annotations

import pytest

from mareforma.observe import (
    CrossTab,
    Direction,
    NumericProvenance,
    ReleasedValue,
    ReportedValue,
    cross_tabulate,
    provenance_diff,
)


def _verdicts(report):
    return [f.verdict for f in report.findings]


class TestTheThreeVerdicts:
    def test_a_value_a_run_produced_is_traceable(self) -> None:
        report = provenance_diff(
            [ReportedValue("moons/kl", 0.093, "0.093")],
            [ReleasedValue("moons/kl", 0.0931, "run_1")],
        )
        assert _verdicts(report) == [NumericProvenance.TRACEABLE]
        assert report.findings[0].matched_sources == ("run_1",)

    def test_a_slot_nothing_covers_is_untraceable_not_an_accusation(
        self,
    ) -> None:
        """Absence of an output is a gap in what was handed in, not a finding."""
        report = provenance_diff(
            [ReportedValue("dino/kl", 0.5, "0.5")],
            [ReleasedValue("moons/kl", 0.093, "run_1")],
        )
        assert _verdicts(report) == [NumericProvenance.UNTRACEABLE]
        assert report.covered == 0
        assert "not checkable either way" in report.closing_sentence()

    def test_a_covered_slot_with_no_matching_value_is_contradicted(
        self,
    ) -> None:
        report = provenance_diff(
            [ReportedValue("moons/kl", 0.093, "0.093")],
            [ReleasedValue("moons/kl", 0.140, "run_1"),
             ReleasedValue("moons/kl", 0.150, "run_2")],
        )
        assert _verdicts(report) == [NumericProvenance.CONTRADICTED]
        assert "none of them rounds to 0.093" in report.findings[0].reason


class TestThePrecisionItWasWrittenTo:
    @pytest.mark.parametrize("as_written, released, expected", [
        ("0.093", 0.09327, NumericProvenance.TRACEABLE),
        ("0.093", 0.09351, NumericProvenance.CONTRADICTED),
        ("0.09", 0.0934, NumericProvenance.TRACEABLE),
        ("3", 3.4, NumericProvenance.TRACEABLE),
        ("3", 3.6, NumericProvenance.CONTRADICTED),
    ])
    def test_the_written_decimals_set_the_tolerance(
        self, as_written: str, released: float, expected,
    ) -> None:
        """Compare at the precision its author claimed, not at float equality.

        A fixed tolerance either rejects honest rounding or accepts anything.
        Half a unit in the last written place is exactly the set of values that
        round to what the write-up says.
        """
        report = provenance_diff(
            [ReportedValue("m", float(as_written), as_written)],
            [ReleasedValue("m", released, "run_1")],
        )
        assert _verdicts(report) == [expected]


class TestTheBandIsTheWrittenPrecisionAndNothingElse:
    """The defect that made the detector agree with numbers that disagree.

    A relative term of five parts in ten thousand rode alongside the half-unit
    band, justified as covering float arithmetic, which is a relative error of
    about 1e-16. Twelve orders of magnitude looser, and it won for any value
    above ten at two decimals, which is most of a results table.
    """

    @pytest.mark.parametrize("as_written, value, released", [
        ("87.35", 87.35, 87.39),        # accuracy, off by 0.04
        ("10000", 10000.0, 10005.0),    # a count, off by five
        ("1235000", 1235000.0, 1234567.0),   # off by 433 cells
        ("2.500", 2.5, 2.5012),
    ])
    def test_a_large_number_is_still_held_to_its_last_written_place(
        self, as_written: str, value: float, released: float,
    ) -> None:
        report = provenance_diff(
            [ReportedValue("m", value, as_written)],
            [ReleasedValue("m", released, "run_1")],
        )
        assert _verdicts(report) == [NumericProvenance.CONTRADICTED]

    @pytest.mark.parametrize("as_written, value, released, expected", [
        # Scientific notation has no decimal point, so counting characters after
        # a dot scored it as an integer and opened a band of half a unit. A
        # genome-wide p-value then matched a released 0.49.
        ("5e-8", 5e-8, 0.49, NumericProvenance.CONTRADICTED),
        ("5e-8", 5e-8, 5.02e-8, NumericProvenance.TRACEABLE),
        ("1e-300", 1e-300, 0.4, NumericProvenance.CONTRADICTED),
        ("1.2e-16", 1.2e-16, 3e-6, NumericProvenance.CONTRADICTED),
    ])
    def test_scientific_notation_keeps_its_precision(
        self, as_written: str, value: float, released: float, expected,
    ) -> None:
        report = provenance_diff(
            [ReportedValue("m", value, as_written)],
            [ReleasedValue("m", released, "run_1")],
        )
        assert _verdicts(report) == [expected]

    @pytest.mark.parametrize("as_written", ["0.42**", "0.42,", "0.05)", "0.42 %"])
    def test_decoration_around_a_number_does_not_inflate_its_precision(
        self, as_written: str,
    ) -> None:
        """Significance stars and punctuation are ordinary in extracted cells.

        Counting them as digits tightened the band and manufactured a
        disagreement out of a number that rounds correctly.
        """
        report = provenance_diff(
            [ReportedValue("m", 0.42, as_written)],
            [ReleasedValue("m", 0.4243, "run_1")],
        )
        assert _verdicts(report) == [NumericProvenance.TRACEABLE]

    @pytest.mark.parametrize("as_written, value, released", [
        ("5e-300", 5e-300, 9e-13),
        ("1.2e-16", 1.2e-16, 5e-13),
        ("3e-20", 3e-20, 1e-13),
    ])
    def test_a_tiny_number_is_not_swallowed_by_a_flat_epsilon(
        self, as_written: str, value: float, released: float,
    ) -> None:
        """A flat 1e-12 slop term became the whole band below that magnitude.

        Numbers hundreds of orders of magnitude apart read as carrying the value
        at the precision it was written to. Float slop is relative to what is
        compared, so the term is too.
        """
        report = provenance_diff(
            [ReportedValue("p", value, as_written)],
            [ReleasedValue("p", released, "run_1")],
        )
        assert _verdicts(report) == [NumericProvenance.CONTRADICTED]

    def test_a_grouped_number_keeps_its_precision(self) -> None:
        """A thousands separator made the reader take the leading digit group.

        ``1,234.56`` read as ``1``, which opens a band of half a unit on a
        number written to two decimals. Cell counts and sample sizes are
        normally written this way.
        """
        report = provenance_diff(
            [ReportedValue("n", 1234.56, "1,234.56")],
            [ReleasedValue("n", 1234.9, "run_1")],
        )
        assert _verdicts(report) == [NumericProvenance.CONTRADICTED]

    @pytest.mark.parametrize("released", [
        1.0,
        # Nothing finite to compare against, so the band was never reached and
        # the comparison could return without computing it. Refused all the
        # same: a verdict reached without a comparison is an accusation with
        # nothing behind it, and CONTRADICTED is the accusing one.
        float("inf"),
    ])
    def test_a_written_precision_out_of_range_is_refused(
        self, released: float,
    ) -> None:
        """Not an OverflowError out of a comparison, and not a verdict either."""
        from mareforma.observe.provenance import UnreadableNumber

        with pytest.raises(UnreadableNumber):
            provenance_diff(
                [ReportedValue("m", 1.0, "1e309")],
                [ReleasedValue("m", released, "run_1")],
            )

    def test_a_written_form_with_no_number_is_refused(self) -> None:
        """Refused rather than guessed at.

        A form this cannot read used to fall back to zero decimals, which is a
        band of half a unit, so being lenient here widened the tolerance instead
        of narrowing the input.
        """
        from mareforma.observe.provenance import UnreadableNumber

        with pytest.raises(UnreadableNumber):
            provenance_diff(
                [ReportedValue("m", 1.0, "n/a")],
                [ReleasedValue("m", 1.0, "run_1")],
            )

    @pytest.mark.parametrize("as_written, value, released, expected", [
        # One significant figure, so the band is fifty thousand and a released
        # 149999 does round to it. This is what the notation means and it is the
        # loosest comparison this makes.
        ("1e5", 1e5, 149999.0, NumericProvenance.TRACEABLE),
        ("1e5", 1e5, 151000.0, NumericProvenance.CONTRADICTED),
        # Two figures, so the band is five thousand.
        ("1.0e5", 1.0e5, 104000.0, NumericProvenance.TRACEABLE),
        ("1.0e5", 1.0e5, 106000.0, NumericProvenance.CONTRADICTED),
        # The same quantity written out commits to six figures and is held to
        # half a unit. The two forms are compared very differently, and that
        # asymmetry is the notation's, not this module's.
        ("100000", 100000.0, 149999.0, NumericProvenance.CONTRADICTED),
        ("100000", 100000.0, 100000.4, NumericProvenance.TRACEABLE),
    ])
    def test_a_written_form_is_held_to_the_figures_it_states(
        self, as_written: str, value: float, released: float, expected,
    ) -> None:
        """Significant-figure semantics, decided and not merely inherited.

        Holding ``1e5`` to six figures would manufacture a disagreement out of a
        precision its author never claimed, which is the error this release
        exists to avoid. The cost is that a loosely written number is nearly
        uncontradictable, so the width goes in the finding's own reason.
        """
        report = provenance_diff(
            [ReportedValue("m", value, as_written)],
            [ReleasedValue("m", released, "run_1")],
        )
        assert _verdicts(report) == [expected]

    def test_the_finding_says_how_wide_it_looked(self) -> None:
        """A match at fifty thousand and a match at half a unit print the same
        word, so the word is not enough."""
        loose = provenance_diff(
            [ReportedValue("m", 1e5, "1e5")],
            [ReleasedValue("m", 149999.0, "run_1")],
        )
        tight = provenance_diff(
            [ReportedValue("m", 0.093, "0.093")],
            [ReleasedValue("m", 0.0931, "run_1")],
        )
        assert "within 50000" in loose.findings[0].reason
        assert "within 0.0005" in tight.findings[0].reason

    def test_an_infinity_carries_no_band(self) -> None:
        """It has no last place, and it never reached the comparison band."""
        report = provenance_diff(
            [ReportedValue("m", float("inf"), "inf")],
            [ReleasedValue("m", 1.0, "run_1")],
        )
        assert _verdicts(report) == [NumericProvenance.CONTRADICTED]
        assert "within" not in report.findings[0].reason

    def test_an_infinity_matches_itself(self) -> None:
        """Infinity minus infinity is not a number, and that comparison is false.

        So the report said the outputs do not carry the infinity they carry.
        """
        report = provenance_diff(
            [ReportedValue("m", float("inf"), "inf")],
            [ReleasedValue("m", float("inf"), "run_1")],
        )
        assert _verdicts(report) == [NumericProvenance.TRACEABLE]


class TestTheFirstRealShape:
    """An improvement claimed over numbers that got worse."""

    def test_a_direction_that_disagrees_with_the_outputs_is_contradicted(
        self,
    ) -> None:
        report = provenance_diff(
            [ReportedValue(
                "moons/improvement", 3.3, "3.3",
                compares=("moons/kl_base", "moons/kl_ours"),
                direction=Direction.DECREASE,
            )],
            [ReleasedValue("moons/kl_base", 0.090, "run_0"),
             ReleasedValue("moons/kl_ours", 0.093, "run_1")],
        )
        assert _verdicts(report) == [NumericProvenance.CONTRADICTED]
        reason = report.findings[0].reason
        assert "moved the other way (increase)" in reason
        assert "0.09 then 0.093" in reason

    def test_a_direction_that_agrees_falls_through_to_the_lookup(self) -> None:
        """An honest derived claim is still checked for membership."""
        report = provenance_diff(
            [ReportedValue(
                "moons/improvement", 3.3, "3.3",
                compares=("moons/kl_base", "moons/kl_ours"),
                direction=Direction.DECREASE,
            )],
            [ReleasedValue("moons/kl_base", 0.093, "run_0"),
             ReleasedValue("moons/kl_ours", 0.090, "run_1"),
             ReleasedValue("moons/improvement", 3.3, "run_1")],
        )
        assert _verdicts(report) == [NumericProvenance.TRACEABLE]

    def test_the_verdict_does_not_depend_on_the_order_of_the_runs(self) -> None:
        """Reading one released value per slot made an accusation order-dependent.

        The same claim against the same released set came back untraceable or
        contradicted depending on which run happened to be first.
        """
        claim = ReportedValue(
            "imp", 1.0, "1.0", compares=("pre", "post"),
            direction=Direction.DECREASE,
        )
        released = [
            ReleasedValue("pre", 0.20, "run_a"),
            ReleasedValue("pre", 0.05, "run_b"),
            ReleasedValue("post", 0.10, "run_c"),
        ]
        forward = provenance_diff([claim], released)
        backward = provenance_diff([claim], list(reversed(released)))
        assert _verdicts(forward) == _verdicts(backward)

    def test_a_direction_it_cannot_check_says_so(self) -> None:
        report = provenance_diff(
            [ReportedValue(
                "x", 1.0, "1.0", compares=("a", "b"),
                direction=Direction.INCREASE,
            )],
            [ReleasedValue("a", 1.0, "run_0")],
        )
        assert _verdicts(report) == [NumericProvenance.UNTRACEABLE]
        assert "could not be checked" in report.findings[0].reason

    def test_runs_that_agree_are_not_read_as_disagreeing(self) -> None:
        """Two runs where the quantity rose in both is not a disagreement.

        Crossing every earlier value with every later one compared one run's
        baseline against another run's result, which is a movement neither run
        recorded. A high-baseline run beside a low-result run then produced a
        direction nothing in the outputs shows, and the honest claim built on
        both of them came back as "the released runs disagree".
        """
        claim = ReportedValue(
            "after", 20.0, "20", compares=("before", "after"),
            direction=Direction.INCREASE,
        )
        released = [
            ReleasedValue("before", 10.0, "run_a"),
            ReleasedValue("after", 20.0, "run_a"),
            ReleasedValue("before", 100.0, "run_b"),
            ReleasedValue("after", 200.0, "run_b"),
        ]
        report = provenance_diff([claim], released)
        assert _verdicts(report) == [NumericProvenance.TRACEABLE]

    def test_runs_that_really_disagree_still_say_so(self) -> None:
        """Within-run pairing must not make every disagreement disappear."""
        claim = ReportedValue(
            "after", 20.0, "20", compares=("before", "after"),
            direction=Direction.INCREASE,
        )
        released = [
            ReleasedValue("before", 10.0, "run_a"),
            ReleasedValue("after", 20.0, "run_a"),
            ReleasedValue("before", 10.0, "run_b"),
            ReleasedValue("after", 5.0, "run_b"),
        ]
        report = provenance_diff([claim], released)
        assert _verdicts(report) == [NumericProvenance.UNTRACEABLE]
        assert "disagree" in report.findings[0].reason

    def test_the_cross_product_still_runs_when_no_run_carries_both(self) -> None:
        """The shape this detector was built on needs it.

        A paper's baseline and its own result come from different runs, so no
        run carries both ends. Demanding one would refuse the case the module
        exists to catch.
        """
        report = provenance_diff(
            [ReportedValue(
                "moons/improvement", 3.3, "3.3",
                compares=("moons/kl_base", "moons/kl_ours"),
                direction=Direction.DECREASE,
            )],
            [ReleasedValue("moons/kl_base", 0.090, "run_0"),
             ReleasedValue("moons/kl_ours", 0.093, "run_1")],
        )
        assert _verdicts(report) == [NumericProvenance.CONTRADICTED]

    def test_a_contradicted_report_quotes_the_pair_it_read(self) -> None:
        """The numbers in the sentence have to be the ones it judged.

        The report quoted the first value of each slot, which is not a pair the
        check looked at. Against runs that fell and held still, a claimed rise
        was refused with "did not move: 10 then 7", a sentence its own two
        numbers contradict.
        """
        claim = ReportedValue(
            "after", 1.0, "1.0", compares=("before", "after"),
            direction=Direction.INCREASE,
        )
        released = [
            ReleasedValue("before", 10.0, "run_a"),
            ReleasedValue("after", 7.0, "run_a"),
            ReleasedValue("before", 10.0, "run_b"),
            ReleasedValue("after", 10.0, "run_b"),
        ]
        report = provenance_diff([claim], released)
        reason = report.findings[0].reason
        assert _verdicts(report) == [NumericProvenance.CONTRADICTED]
        # Several runs, moving several ways, none of them the way this claims.
        # Naming one of them as "the" direction picks a disagreement out of a set
        # and reports it as the finding.
        assert "no released run moves it that way" in reason
        assert "10 then 7" in reason and "10 then 10" in reason
        assert "did not move" not in reason

    def test_the_report_reads_as_english(self) -> None:
        report = provenance_diff(
            [ReportedValue(
                "x", 1.0, "1.0", compares=("a", "b"),
                direction=Direction.INCREASE,
            )],
            [ReleasedValue("a", 2.0, "run_0"), ReleasedValue("b", 1.0, "run_0")],
        )
        assert "claims an increase" in report.findings[0].reason


class TestTheSecondRealShape:
    """One quantity, three sections, three values that cannot all be right."""

    def test_mutually_inconsistent_values_are_reported_per_slot(self) -> None:
        report = provenance_diff(
            [ReportedValue("moons/kl", 0.093, "0.093", where="abstract"),
             ReportedValue("moons/kl", 0.089, "0.089", where="results"),
             ReportedValue("moons/kl", 0.101, "0.101", where="appendix")],
            [ReleasedValue("moons/kl", 0.093, "run_1"),
             ReleasedValue("moons/kl", 0.089, "run_2"),
             ReleasedValue("moons/kl", 0.101, "run_3")],
        )
        # Each traces on its own, which is what makes this invisible per number.
        assert _verdicts(report) == [NumericProvenance.TRACEABLE] * 3
        assert report.inconsistent_slots == ("moons/kl",)
        assert "cannot all be right" in report.closing_sentence()

    def test_it_runs_without_any_released_coverage(self) -> None:
        """The check needs no released outputs, so it must not require them.

        Restricting it to traceable numbers made a property of the write-up
        alone depend on having outputs to compare against, and on real input
        most numbers have none.
        """
        report = provenance_diff(
            [ReportedValue("moons/kl", 0.093, "0.093", where="abstract"),
             ReportedValue("moons/kl", 0.089, "0.089", where="results")],
            [],
        )
        assert _verdicts(report) == [NumericProvenance.UNTRACEABLE] * 2
        assert report.inconsistent_slots == ("moons/kl",)

    def test_a_placeholder_cell_does_not_take_the_report_down(self) -> None:
        """A form with no number has no precision to compare at.

        It reaches this check now that every finding is considered, and a
        placeholder is ordinary in an extracted table. One of them used to
        decide whether the whole report existed, based on how many times it
        appeared in the same slot.
        """
        report = provenance_diff(
            [ReportedValue("f1", 0.0, "--", where="t1"),
             ReportedValue("f1", 0.0, "--", where="t2")],
            [],
        )
        assert _verdicts(report) == [NumericProvenance.UNTRACEABLE] * 2
        assert report.inconsistent_slots == ()

    def test_one_value_restated_consistently_is_not_flagged(self) -> None:
        report = provenance_diff(
            [ReportedValue("moons/kl", 0.093, "0.093", where="abstract"),
             ReportedValue("moons/kl", 0.093, "0.093", where="results")],
            [ReleasedValue("moons/kl", 0.093, "run_1")],
        )
        assert report.inconsistent_slots == ()

    def test_a_rounded_restatement_is_not_an_inconsistency(self) -> None:
        """0.09 in the abstract and 0.093 in the table is rounding, not conflict.

        Compared at the coarser of the two precisions, or every write-up that
        rounds its headline would read as contradicting itself.
        """
        report = provenance_diff(
            [ReportedValue("moons/kl", 0.09, "0.09", where="abstract"),
             ReportedValue("moons/kl", 0.093, "0.093", where="table")],
            [ReleasedValue("moons/kl", 0.093, "run_1")],
        )
        assert _verdicts(report) == [NumericProvenance.TRACEABLE] * 2
        assert report.inconsistent_slots == ()


class TestTheReportRefusesABareRate:
    def test_the_closing_sentence_carries_its_denominator(self) -> None:
        """A rate without its denominator reads as prevalence over the whole.

        The uncovered count is part of the sentence because it is the part a
        reader would otherwise assume was zero.
        """
        report = provenance_diff(
            [ReportedValue("a", 1.0, "1.0"), ReportedValue("b", 2.0, "2.0")],
            [ReleasedValue("a", 9.0, "run_1")],
        )
        sentence = report.closing_sentence()
        assert "1 of 1" in sentence
        assert "1 more name a slot nothing released covers" in sentence

    def test_an_empty_check_says_nothing_was_checked(self) -> None:
        report = provenance_diff([], [])
        assert report.total == 0
        assert report.closing_sentence() == "No reported numbers were checked."


class TestTheCrossTab:
    @pytest.mark.parametrize("influence, prov, expected", [
        # Every cell of the grid, so a mapping cannot change unnoticed.
        ("INFLUENCED", NumericProvenance.TRACEABLE, CrossTab.AGREE),
        ("INFLUENCED", NumericProvenance.UNTRACEABLE, CrossTab.TENSION),
        ("INFLUENCED", NumericProvenance.CONTRADICTED, CrossTab.TENSION),
        ("NOT_INFLUENCED", NumericProvenance.TRACEABLE,
         CrossTab.CONSTRUCT_DIFFERENCE),
        ("NOT_INFLUENCED", NumericProvenance.UNTRACEABLE,
         CrossTab.AGREE_ABSENT),
        ("NOT_INFLUENCED", NumericProvenance.CONTRADICTED,
         CrossTab.CONVERGENT_FAILURE),
        ("UNDECIDABLE", NumericProvenance.TRACEABLE, CrossTab.INCONCLUSIVE),
        ("UNDECIDABLE", NumericProvenance.UNTRACEABLE, CrossTab.INCONCLUSIVE),
        ("UNDECIDABLE", NumericProvenance.CONTRADICTED, CrossTab.INCONCLUSIVE),
        ("NOT_TESTED", NumericProvenance.TRACEABLE, CrossTab.OBSERVER_BLIND),
        ("NOT_TESTED", NumericProvenance.CONTRADICTED,
         CrossTab.OBSERVER_BLIND),
        (None, NumericProvenance.TRACEABLE, CrossTab.OBSERVER_BLIND),
        ("INFLUENCED", None, CrossTab.INCONCLUSIVE),
    ])
    def test_the_two_axes_place_a_finding(
        self, influence, prov, expected,
    ) -> None:
        assert cross_tabulate(influence, prov) is expected

    def test_the_worst_cell_does_not_read_as_agreement(self) -> None:
        """Both arms found something, pointing the same way.

        The outputs contradict the number AND the finding does not depend on the
        data. It shared a label with two clean cases, so a reader counting
        agreement could not tell the best cell from the worst.
        """
        assert cross_tabulate(
            "NOT_INFLUENCED", NumericProvenance.CONTRADICTED,
        ) is CrossTab.CONVERGENT_FAILURE
        assert cross_tabulate(
            "NOT_INFLUENCED", NumericProvenance.UNTRACEABLE,
        ) is not CrossTab.AGREE

    def test_a_verdict_read_back_from_json_lands_in_the_right_cell(
        self,
    ) -> None:
        """Both enums serialise to bare strings, so this is the obvious caller.

        The influence arm accepted a string and the provenance arm compared by
        identity, so a report round-tripped through JSON placed every finding in
        the wrong cell, and the cited-but-hollow case came back as agreement.
        """
        import json

        payload = json.dumps({
            "influence": "NOT_INFLUENCED",
            "provenance": NumericProvenance.TRACEABLE,
        })
        back = json.loads(payload)
        assert cross_tabulate(
            back["influence"], back["provenance"],
        ) is CrossTab.CONSTRUCT_DIFFERENCE

    @pytest.mark.parametrize("bad", [
        "influenced", "NOT INFLUENCED", 0, object(), NumericProvenance.TRACEABLE,
    ])
    def test_an_unrecognised_influence_token_is_refused(self, bad) -> None:
        """Every wrong spelling used to land on INCONCLUSIVE.

        That is a real verdict, so a typo or a swapped argument read as a
        measurement the oracle declined rather than as the mistake it was.
        """
        with pytest.raises(ValueError, match="not an influence verdict"):
            cross_tabulate(bad, NumericProvenance.TRACEABLE)

    def test_the_middle_categories_need_both_arms(self) -> None:
        """Neither number means much alone, which is why both ship together.

        A number in the outputs that the finding does not depend on is the
        cited-but-hollow case, and it is only nameable because both ran on the
        same finding.
        """
        assert cross_tabulate(
            "NOT_INFLUENCED", NumericProvenance.TRACEABLE,
        ) is CrossTab.CONSTRUCT_DIFFERENCE
        assert cross_tabulate(
            "INFLUENCED", NumericProvenance.UNTRACEABLE,
        ) is CrossTab.TENSION

    def test_it_takes_the_oracle_enum_as_well_as_its_string(self) -> None:
        from mareforma.observe.oracle import OracleInfluence

        assert cross_tabulate(
            OracleInfluence.NOT_INFLUENCED, NumericProvenance.TRACEABLE,
        ) is CrossTab.CONSTRUCT_DIFFERENCE


class TestWhatItDoesNotClaim:
    def test_traceable_is_membership_and_nothing_more(self) -> None:
        """A value that appears in a released output is not a correct value.

        Nothing here re-runs anything, so a wrong number faithfully recorded by
        a wrong run traces cleanly. The detector compares the two sets it was
        handed.
        """
        report = provenance_diff(
            [ReportedValue("m", 999.0, "999.0")],
            [ReleasedValue("m", 999.0, "a_broken_run")],
        )
        assert _verdicts(report) == [NumericProvenance.TRACEABLE]
        assert report.findings[0].matched_sources == ("a_broken_run",)
