"""Causal oracle, aggregate measurement, and the documented known-bound guards.

The oracle tests validate the independent influence ground truth on the
deterministic A/B/C fixtures (no LLM, so they run in CI) plus its noise
handling. The known-bound guards pin the observer's honest limits AS intended
behavior: each carries a comment naming the bound, so a future change that flips
one fails here loudly instead of silently.
"""
from __future__ import annotations

import pytest

import mareforma.observe as obs
from mareforma.observe import ObservedGrounding as OG
from mareforma.observe.measure import summarize
from mareforma.observe.oracle import (
    NotTestedReason,
    OracleInfluence,
    OracleResult,
    Reconciliation,
    influence_sweep,
    perturbation_oracle,
    reconcile,
)


# -- causal oracle: deterministic A/B/C -------------------------------------

def test_oracle_A_grounded_finding_is_influenced():
    # A: the finding is a function of the data; perturbing the data moves it.
    run = lambda x: x * 2.0
    res = perturbation_oracle(run, 10.0, lambda x: x + 5.0)
    assert res.influence is OracleInfluence.INFLUENCED


def test_oracle_B_silent_fallback_is_not_influenced():
    # B: the finding ignores the input (a silent fallback returns a constant);
    # perturbing the data does not move it.
    run = lambda x: 42.0
    res = perturbation_oracle(run, 10.0, lambda x: x + 5.0)
    assert res.influence is OracleInfluence.NOT_INFLUENCED


def test_oracle_C_incidental_dependence_is_not_influenced():
    # C: the finding depends only on an incidental input, not the cited data, so
    # perturbing the cited data leaves it unmoved.
    run = lambda cited_value: 7.0  # depends on something else entirely
    res = perturbation_oracle(run, 3.0, [1.0, 2.0, 99.0])
    assert res.influence is OracleInfluence.NOT_INFLUENCED


def test_opposing_perturbations_do_not_cancel():
    # Each perturbation is its own comparison against the base. Pooling them all
    # into one mean let a +1 and a -1 perturbation of an identity pipeline
    # cancel to zero effect and report the data as ignored.
    res = perturbation_oracle(lambda x: x, 0.0, [1.0, -1.0], repeats=3)
    assert res.influence is OracleInfluence.INFLUENCED
    assert res.effect_size == 1.0
    assert res.perturbation_effects == (1.0, 1.0)


def test_effect_is_the_largest_perturbation_not_the_average():
    # Perturbations of differing size must not dilute each other: two +1 and one
    # -1 around base 10 on a doubling pipeline each move the finding by 2, so
    # the effect is 2, not the 0.667 a pooled mean reported.
    res = perturbation_oracle(lambda x: x * 2, 10.0, [11.0, 11.0, 9.0])
    assert res.effect_size == pytest.approx(2.0)
    assert res.perturbation_effects == pytest.approx((2.0, 2.0, 2.0))


def test_perturbation_count_widens_the_threshold():
    # Taking the max over k perturbations is itself a multiple comparison, so
    # the threshold gains the extreme-value sigmas a family of k would. Base
    # [0,1,2] (std ~0.816) with one perturbation moving the finding by 3 clears
    # the 2.449 margin; add a second perturbation that does not move it and the
    # margin widens to ~3.41, so the same move is UNDECIDABLE, not INFLUENCED.
    one = perturbation_oracle(_replays([0.0, 1.0, 2.0, 3.0, 4.0, 5.0]), 0.0,
                              [1.0], repeats=3)
    two = perturbation_oracle(_replays([0.0, 1.0, 2.0, 3.0, 4.0, 5.0,
                                        0.0, 1.0, 2.0]), 0.0, [1.0, 2.0],
                              repeats=3)
    assert one.influence is OracleInfluence.INFLUENCED
    assert two.effect_size == one.effect_size
    assert two.decision_threshold > one.decision_threshold
    assert two.influence is OracleInfluence.UNDECIDABLE


def _replays(values):
    """A run_fn that returns the given values in order, one per call."""
    seq = iter(values)
    return lambda x: next(seq)


def test_oracle_noise_below_threshold_is_undecidable():
    # A stochastic pipeline whose perturbation effect clears one noise sd but not
    # the full noise margin must be UNDECIDABLE, never silently INFLUENCED nor
    # NOT_INFLUENCED. Base runs [0,1,2] give mean 1.0, noise_std ~0.816, margin
    # ~2.449; perturbed runs [1.5,2.5,3.5] give mean 2.5, effect 1.5 — inside
    # (noise_std, margin], the exact UNDECIDABLE band. Asserted strictly so a
    # regression that collapses the band is caught.
    seq = iter([0.0, 1.0, 2.0, 1.5, 2.5, 3.5])
    run = lambda x: next(seq)
    res = perturbation_oracle(run, 0.0, lambda x: x + 1.0, repeats=3,
                              noise_multiplier=3.0)
    assert res.influence is OracleInfluence.UNDECIDABLE
    assert res.noise_floor > 0
    assert res.effect_size > res.noise_floor


def test_oracle_rejects_zero_repeats():
    with pytest.raises(ValueError):
        perturbation_oracle(lambda x: x, 1.0, lambda x: x + 1, repeats=0)


def test_oracle_below_domain_floor_is_not_influenced():
    # When the domain-minimal effect_threshold sets the bar (not noise), an
    # effect below it is domain-insignificant NOT_INFLUENCED, not UNDECIDABLE:
    # UNDECIDABLE is reserved for noise-driven ambiguity.
    seq = iter([0.0, 1.0, 2.0, 5.0, 6.0, 7.0])  # base mean 1, perturbed mean 6
    run = lambda x: next(seq)
    res = perturbation_oracle(
        run, 0.0, lambda x: x + 1, repeats=3,
        effect_threshold=10.0, noise_multiplier=3.0,
    )
    assert res.influence is OracleInfluence.NOT_INFLUENCED


# -- deterministic target: measured-zero, the modal case -------------------

def test_deterministic_pipeline_does_not_read_influenced_on_float_noise():
    # The defect this release corrects: with the noise floor measured at 0 the
    # threshold was 0.0, so any nonzero move read INFLUENCED. A deterministic
    # pipeline whose finding is ~1000 and moves by 0.0001 (1e-7 relative, deep
    # inside the float-equality band) must read UNDECIDABLE, never INFLUENCED.
    base = 1000.0
    seq = iter([base, base, base, base, base,
                base + 0.0001, base + 0.0001, base + 0.0001,
                base + 0.0001, base + 0.0001])
    run = lambda x: next(seq)
    res = perturbation_oracle(run, 0.0, lambda x: x + 1.0, repeats=5)
    assert res.influence is OracleInfluence.UNDECIDABLE
    assert res.deterministic is True
    assert res.noise_floor == 0.0
    assert res.noise_measured is True


def test_deterministic_pipeline_real_move_is_influenced():
    # A deterministic pipeline whose finding genuinely tracks the input still
    # reads INFLUENCED: the float-equality band is tiny relative to the finding,
    # so a real dependency clears it. Guards against the band swallowing signal.
    res = perturbation_oracle(lambda x: x * 2.0, 10.0, lambda x: x + 5.0, repeats=5)
    assert res.influence is OracleInfluence.INFLUENCED
    assert res.deterministic is True


def test_deterministic_invariant_reads_not_influenced_not_hollow_undecidable():
    # A provable invariant: the finding does not move at all under the null. On a
    # deterministic pipeline that is an exact 0 effect, which is NOT_INFLUENCED
    # (a genuine flat), distinct from a tiny nonzero move (UNDECIDABLE).
    res = perturbation_oracle(lambda x: 42.0, 10.0, lambda x: x + 5.0, repeats=5)
    assert res.influence is OracleInfluence.NOT_INFLUENCED
    assert res.effect_size == 0.0
    assert res.deterministic is True


def test_near_zero_finding_gets_a_band_from_the_absolute_floor():
    # A finding whose magnitude is ~0 has no relative float-equality band, so a
    # summation-order artifact would read INFLUENCED. An absolute floor gives it
    # a band: a 1e-9 move inside determinism_atol=1e-6 reads UNDECIDABLE.
    base = 0.0
    seq = iter([base, base, base, base, base,
                1e-9, 1e-9, 1e-9, 1e-9, 1e-9])
    run = lambda x: next(seq)
    res = perturbation_oracle(run, 0.0, lambda x: x + 1.0, repeats=5,
                              determinism_atol=1e-6)
    assert res.influence is OracleInfluence.UNDECIDABLE
    # Without the absolute floor the relative band is 0, so the same tiny move
    # reads INFLUENCED: the knob is what closes the near-zero gap.
    seq2 = iter([base, base, base, base, base,
                 1e-9, 1e-9, 1e-9, 1e-9, 1e-9])
    bare = perturbation_oracle(lambda x: next(seq2), 0.0, lambda x: x + 1.0,
                               repeats=5)
    assert bare.influence is OracleInfluence.INFLUENCED


def test_measured_zero_is_distinct_from_never_measured():
    # Measured-zero (repeats>1, floor sampled and found 0) carries
    # deterministic=True; a single unmeasured base run does not, even though both
    # have noise_floor 0. A consumer must be able to tell the two apart.
    measured = perturbation_oracle(lambda x: x * 2.0, 10.0, lambda x: x + 5.0,
                                   repeats=5)
    never = perturbation_oracle(lambda x: x * 2.0, 10.0, lambda x: x + 5.0)
    assert measured.deterministic is True
    assert measured.noise_measured is True
    assert never.deterministic is False
    assert never.noise_measured is False
    assert measured.noise_floor == never.noise_floor == 0.0
    assert "deterministic pipeline" in measured.reason
    assert "no noise estimate" in never.reason


# -- NOT_TESTED: the oracle did not run -------------------------------------

def test_not_tested_row_has_none_measurements_and_typed_reason():
    # A never-run row carries None on all three measurement numbers (the only
    # legal value there) so no consumer reads a zero as a measurement, and a
    # typed reason a consumer branches on without parsing the English sentence.
    res = OracleResult.not_tested(
        NotTestedReason.CRASHED_UNDER_NULL, traceback="Traceback...\nValueError"
    )
    assert res.influence is OracleInfluence.NOT_TESTED
    assert res.effect_size is None
    assert res.noise_floor is None
    assert res.decision_threshold is None
    assert res.not_tested_reason is NotTestedReason.CRASHED_UNDER_NULL
    assert res.traceback is not None
    assert res.not_tested_reason.value in res.reason


def test_every_not_tested_reason_has_a_path_that_produces_it():
    # The enum is exactly the set of ways this module declines to measure, and
    # every member is reachable. A member no code path produces reads as coverage
    # of a state that cannot occur, and a consumer branching on it is never
    # exercised, so the set is pinned against the paths rather than restated.
    assert {r.value for r in NotTestedReason} == {
        "unsupported-shape", "null-construction-failed", "target-failed",
        "crashed-under-null", "unreducible-value", "non-finite-value",
    }
    produced = {
        # no scramble family fits a string
        perturbation_oracle(lambda x: 1.0, "a path").not_tested_reason,
        # building the null raises
        perturbation_oracle(
            lambda x: 1.0, [1.0, 2.0], perturb=_raises
        ).not_tested_reason,
        # the unperturbed run raises
        perturbation_oracle(_raises, [1.0, 2.0]).not_tested_reason,
        # the target survives the base and dies on a null
        perturbation_oracle(_dies_on_zeroed, [1.0, 2.0]).not_tested_reason,
        # the reducer cannot reduce the finding
        perturbation_oracle(lambda x: object(), [1.0, 2.0]).not_tested_reason,
        # the run reduces to NaN
        perturbation_oracle(lambda x: float("nan"), [1.0, 2.0]).not_tested_reason,
    }
    assert produced == set(NotTestedReason)


def _raises(x):
    raise RuntimeError("boom")


def _dies_on_zeroed(x):
    if all(v == 0.0 for v in x):
        raise RuntimeError("the null killed the target")
    return sum(x)


# -- profile routing over the auto-derived null family ----------------------

def test_profile_hollow_finding_is_not_influenced():
    # A hardcoded fallback holds still under every null in the derived family, so
    # the profile is flat everywhere: NOT_INFLUENCED (hollow).
    res = perturbation_oracle(lambda x: 42.0, [1.0, 2.0, 3.0, 4.0], repeats=5)
    assert res.influence is OracleInfluence.NOT_INFLUENCED
    assert res.scramble_names == ("zeroed", "constant", "permuted", "reversed")


def test_profile_position_dependent_finding_is_influenced():
    # A finding that reads specific positions moves under every null (content and
    # order both change it), so the profile is moves-everywhere: INFLUENCED.
    res = perturbation_oracle(
        lambda x: float(x[0]) * 10.0 + float(x[-1]), [1.0, 2.0, 3.0, 4.0],
        repeats=5,
    )
    assert res.influence is OracleInfluence.INFLUENCED


def test_profile_marginal_invariant_is_undecidable_not_hollow():
    # The false-hollow discipline: a genuine mean is invariant under the
    # marginal-preserving nulls (permute, reverse) and moves under the destroying
    # ones. Moves under some, flat under others, so it reads UNDECIDABLE, never
    # NOT_INFLUENCED. This is the trap the whole instrument rests on.
    res = perturbation_oracle(
        lambda x: sum(x) / len(x), [1.0, 2.0, 3.0, 4.0], repeats=5,
    )
    assert res.influence is OracleInfluence.UNDECIDABLE
    assert "permuted" in res.reason and "reversed" in res.reason


def test_profile_routing_is_the_default_no_caller_supplies_a_null():
    # perturb defaults to None: the family is derived from the finding's shape,
    # so a caller cannot choose (and cannot fish for) a null.
    res = perturbation_oracle(lambda x: 42.0, 10.0)
    assert res.influence is OracleInfluence.NOT_INFLUENCED
    assert res.scramble_names  # a family was derived


def test_profile_routing_governs_caller_supplied_sequences_too():
    # The profile rule replaces the old max-based logic for EVERY family, not
    # only the auto-derived one: a caller-supplied sequence where the finding
    # moves under one null and holds invariant under another reads UNDECIDABLE,
    # where the retired max-move logic would have called it INFLUENCED. The
    # invariant under a valid null is the honest-hard case, not a clean pass.
    res = perturbation_oracle(lambda x: x, 0.0, [5.0, 0.0], repeats=3)
    assert res.perturbation_effects == (5.0, 0.0)
    assert res.influence is OracleInfluence.UNDECIDABLE


def test_unsupported_shape_is_not_tested_never_a_verdict():
    # A shape the scramble library has no family for yields NOT_TESTED, and the
    # pipeline is never run: no base value, no verdict invented.
    ran = []
    def run(x):
        ran.append(x)
        return len(x)
    res = perturbation_oracle(run, "a prose finding")
    assert res.influence is OracleInfluence.NOT_TESTED
    assert res.not_tested_reason is NotTestedReason.UNSUPPORTED_SHAPE
    assert res.effect_size is None
    assert ran == []  # nothing was run


# -- crash guard and progress -----------------------------------------------

def test_crash_under_a_null_reads_not_tested_with_the_traceback():
    # A null that kills the target must not abort the measurement: it reads
    # NOT_TESTED(crashed-under-null), naming the null and carrying the traceback,
    # never a raised exception.
    def fragile(x):
        if any(v == 0.0 for v in x):  # the zeroed null kills it
            raise ZeroDivisionError("cannot divide by the zeroed input")
        return sum(x)
    res = perturbation_oracle(fragile, [1.0, 2.0, 3.0], repeats=3)
    assert res.influence is OracleInfluence.NOT_TESTED
    assert res.not_tested_reason is NotTestedReason.CRASHED_UNDER_NULL
    assert res.traceback and "ZeroDivisionError" in res.traceback
    assert res.effect_size is None


def test_keyboard_interrupt_escapes_the_crash_guard():
    # The guard catches Exception, not BaseException, so an abort the operator
    # asked for ends the run rather than turning into a NOT_TESTED row.
    def aborts(x):
        raise KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        perturbation_oracle(aborts, [1.0, 2.0], repeats=2)


def test_a_value_the_reducer_cannot_reduce_is_not_tested():
    # A finding the declared reducer cannot turn into a scalar is a different
    # failure from a crash: NOT_TESTED(unreducible-value), not crashed-under-null.
    res = perturbation_oracle(lambda x: object(), 1.0, perturb=[2.0])
    assert res.influence is OracleInfluence.NOT_TESTED
    assert res.not_tested_reason is NotTestedReason.UNREDUCIBLE_VALUE


def test_on_progress_reports_the_documented_run_count():
    # total = repeats * (1 + number of nulls). For [1,2,3] the derived family is
    # zeroed, constant, permuted, reversed (4 nulls), so 2 * (1 + 4) = 10 runs.
    calls = []
    perturbation_oracle(lambda x: 1.0, [1.0, 2.0, 3.0], repeats=2,
                        on_progress=lambda done, total: calls.append((done, total)))
    assert len(calls) == 10
    assert calls[-1] == (10, 10)
    assert [d for d, _ in calls] == list(range(1, 11))


# -- corpus sweep: multiplicity computed, not left to the caller ------------

def test_influence_sweep_sets_multiplicity_from_the_corpus_size():
    # Leaving multiplicity at 1 over a corpus silently overcounts influence; the
    # sweep computes it from the count so the caller never has to.
    findings = [
        (lambda x: x[0], [1.0, 2.0, 3.0]),
        (lambda x: 42.0, [1.0, 2.0, 3.0]),
        (lambda x: sum(x) / len(x), [1.0, 2.0, 3.0]),
    ]
    results = influence_sweep(findings, repeats=5)
    assert [r.multiplicity for r in results] == [3, 3, 3]
    assert [r.influence for r in results] == [
        OracleInfluence.INFLUENCED,
        OracleInfluence.NOT_INFLUENCED,
        OracleInfluence.UNDECIDABLE,
    ]


def test_influence_sweep_refuses_a_caller_supplied_multiplicity():
    # The sweep owns multiplicity; passing it is a mistake, not a silent override.
    with pytest.raises(TypeError):
        influence_sweep([(lambda x: x, 1.0)], multiplicity=2)


# -- per-verdict blind-spot line + threat model -----------------------------

def test_blind_spot_line_names_the_invariant_nulls_and_the_threat_bound():
    from mareforma.observe.oracle import THREAT_MODEL_STATEMENT

    data = [1.0, 2.0, 3.0, 4.0]
    # A genuine mean is invariant under the marginal-preserving nulls: those are
    # the verdict's blind spots and the line names them.
    mean = perturbation_oracle(lambda x: sum(x) / len(x), data, repeats=5)
    assert set(mean.flat_nulls) == {"permuted", "reversed"}
    assert "permuted" in mean.blind_spot_line()
    assert THREAT_MODEL_STATEMENT in mean.blind_spot_line()

    # An influenced finding moved under every null: no blind spot, but still the
    # threat-model statement.
    influenced = perturbation_oracle(lambda x: x[0], data, repeats=5)
    assert influenced.flat_nulls == ()
    assert "every null" in influenced.blind_spot_line()
    assert THREAT_MODEL_STATEMENT in influenced.blind_spot_line()


def test_not_tested_row_carries_the_threat_statement_without_blind_spots():
    from mareforma.observe.oracle import THREAT_MODEL_STATEMENT

    res = OracleResult.not_tested(NotTestedReason.UNSUPPORTED_SHAPE)
    assert res.flat_nulls == ()
    assert THREAT_MODEL_STATEMENT in res.blind_spot_line()


# -- reconcile: flow vs influence -------------------------------------------

def test_reconcile_agreement():
    assert reconcile(OG.GROUNDED, OracleInfluence.INFLUENCED).relation is (
        Reconciliation.AGREE
    )
    assert reconcile(OG.UNGROUNDED, OracleInfluence.NOT_INFLUENCED).relation is (
        Reconciliation.AGREE
    )


def test_reconcile_flow_without_influence_is_construct_difference():
    # Read the data, then ignored it: a construct difference, not a bug.
    assert reconcile(OG.GROUNDED, OracleInfluence.NOT_INFLUENCED).relation is (
        Reconciliation.CONSTRUCT_DIFFERENCE
    )


def test_reconcile_influence_without_flow_is_tension():
    assert reconcile(OG.UNGROUNDED, OracleInfluence.INFLUENCED).relation is (
        Reconciliation.TENSION
    )


def test_reconcile_opaque_is_observer_blind():
    assert reconcile(OG.OPAQUE, OracleInfluence.INFLUENCED).relation is (
        Reconciliation.OBSERVER_BLIND
    )


def test_reconcile_opaque_undecidable_keeps_its_opacity():
    # REGRESSION (mandatory): the dispatch used to check UNDECIDABLE before
    # OPAQUE, so an OPAQUE row whose oracle could not decide collapsed to
    # INCONCLUSIVE, losing the opacity and folding two of the three coverage
    # states into one. OPAQUE must win over UNDECIDABLE: the observer's blindness
    # is the fact to report, not the oracle's inability to decide.
    assert reconcile(OG.OPAQUE, OracleInfluence.UNDECIDABLE).relation is (
        Reconciliation.OBSERVER_BLIND
    )


def test_reconcile_not_tested_is_guarded_first_and_never_accuses():
    # NOT_TESTED is guarded before every other branch. In particular GROUNDED +
    # NOT_TESTED must NOT fall through to CONSTRUCT_DIFFERENCE, the cited-but-
    # hollow accusation, for a row the oracle never ran on.
    assert reconcile(OG.GROUNDED, OracleInfluence.NOT_TESTED).relation is (
        Reconciliation.NOT_TESTED
    )
    assert reconcile(OG.UNGROUNDED, OracleInfluence.NOT_TESTED).relation is (
        Reconciliation.NOT_TESTED
    )
    assert reconcile(OG.OPAQUE, OracleInfluence.NOT_TESTED).relation is (
        Reconciliation.NOT_TESTED
    )


def test_reconcile_is_exhaustive_over_the_full_grounding_by_influence_product():
    # Every cell of the 3 grounding x 4 influence product has an asserted
    # relation, so a fifth influence state (or a reordered dispatch) cannot ship
    # a silent gap. Three grounding states, four influence states, twelve cells.
    expected = {
        (OG.GROUNDED, OracleInfluence.INFLUENCED): Reconciliation.AGREE,
        (OG.GROUNDED, OracleInfluence.NOT_INFLUENCED): Reconciliation.CONSTRUCT_DIFFERENCE,
        (OG.GROUNDED, OracleInfluence.UNDECIDABLE): Reconciliation.INCONCLUSIVE,
        (OG.GROUNDED, OracleInfluence.NOT_TESTED): Reconciliation.NOT_TESTED,
        (OG.UNGROUNDED, OracleInfluence.INFLUENCED): Reconciliation.TENSION,
        (OG.UNGROUNDED, OracleInfluence.NOT_INFLUENCED): Reconciliation.AGREE,
        (OG.UNGROUNDED, OracleInfluence.UNDECIDABLE): Reconciliation.INCONCLUSIVE,
        (OG.UNGROUNDED, OracleInfluence.NOT_TESTED): Reconciliation.NOT_TESTED,
        (OG.OPAQUE, OracleInfluence.INFLUENCED): Reconciliation.OBSERVER_BLIND,
        (OG.OPAQUE, OracleInfluence.NOT_INFLUENCED): Reconciliation.OBSERVER_BLIND,
        (OG.OPAQUE, OracleInfluence.UNDECIDABLE): Reconciliation.OBSERVER_BLIND,
        (OG.OPAQUE, OracleInfluence.NOT_TESTED): Reconciliation.NOT_TESTED,
    }
    grounding_states = list(OG)
    influence_states = list(OracleInfluence)
    # The product the table claims to cover really is the whole enum product.
    assert set(expected) == {
        (g, i) for g in grounding_states for i in influence_states
    }
    for (grounding, influence), relation in expected.items():
        assert reconcile(grounding, influence).relation is relation, (
            f"reconcile({grounding}, {influence}) should be {relation}"
        )


def test_reconcile_result_type_is_on_the_package_surface():
    # Callers annotate what reconcile hands back without reaching into oracle.
    assert "ReconcileResult" in obs.__all__
    assert isinstance(
        reconcile(OG.GROUNDED, OracleInfluence.INFLUENCED), obs.ReconcileResult
    )


# -- aggregate measurement ---------------------------------------------------

def test_summarize_reports_the_split_and_opaque_trigger(tmp_path):
    data = tmp_path / "d.csv"
    data.write_text("x\n1\n")
    cfg = tmp_path / "c.yaml"
    cfg.write_text("k: v\n")
    verdicts = []
    with obs.observe(cites=str(data)) as h:
        open(str(data)).read()
    verdicts.append(h.verdict)  # GROUNDED
    with obs.observe(cites=str(data)) as h:
        open(str(cfg)).read()  # incidental
    verdicts.append(h.verdict)  # UNGROUNDED, incidental read present

    report = summarize(verdicts)
    assert report.total == 2
    assert report.grounded == 1
    assert report.ungrounded == 1
    assert report.incidental_read_rate == 0.5
    assert report.opaque_dominates(threshold=0.5) is False


def test_incidental_rate_does_not_depend_on_the_summarizing_cwd(tmp_path,
                                                               monkeypatch):
    # A receipt is written on the producing host and summarized anywhere: by a
    # later run, by `mareforma measure`, or off-host after --redact-home. The
    # read identifier is resolved once, where the filesystem is authoritative,
    # so the same receipt reports the same number from any directory.
    from mareforma.observe import summarize_receipts

    data = tmp_path / "trial.csv"
    data.write_text("x\n1\n")
    monkeypatch.chdir(tmp_path)
    with obs.observe(cites=str(data)) as h:
        open("trial.csv").read()  # relative read of the cited source
    assert h.verdict.grounding is OG.GROUNDED
    receipt = h.verdict.receipt()

    assert summarize_receipts([receipt]).incidental_read_rate == 0.0
    monkeypatch.chdir(tmp_path.parent)
    assert summarize_receipts([receipt]).incidental_read_rate == 0.0


def test_summarize_receipts_refuses_an_older_grounding_axis():
    # The older axis stored the loader's RAW read identifier and normalized both
    # sides at compare time. This one normalizes at record time and compares by
    # plain string, so an older receipt's relative identifier no longer equals
    # its normalized cited source and would be counted as an incidental read.
    # Two definitions of the number cannot share one report.
    from mareforma.observe import GROUNDING_AXIS_VERSION, summarize_receipts
    from mareforma.observe.measure import GroundingAxisMismatchError

    older = {
        "version": "v0.3.9",
        "grounding": "GROUNDED",
        "reason": "a read matching the cited source returned non-empty data",
        "cited_sources": ["/data/trial.csv"],
        "grounded_sources": ["/data/trial.csv"],
        "reads": [{"kind": "pandas", "identifier": "trial.csv", "nonempty": True,
                   "content_address": None}],
        "coverage": {"reads_seen": 1, "opens_detected": 1},
    }
    assert older["version"] != GROUNDING_AXIS_VERSION

    current = {**older, "version": GROUNDING_AXIS_VERSION,
               "reads": [{"kind": "pandas", "identifier": "/data/trial.csv",
                          "nonempty": True, "content_address": None}]}
    assert summarize_receipts([current]).incidental_read_rate == 0.0

    with pytest.raises(GroundingAxisMismatchError) as exc:
        summarize_receipts([older])
    assert "v0.3.9" in str(exc.value)
    assert GROUNDING_AXIS_VERSION in str(exc.value)


def test_summarize_flags_opaque_dominance():
    from mareforma.observe import GroundingVerdict

    verdicts = [GroundingVerdict(OG.OPAQUE, "seam", ()) for _ in range(3)]
    verdicts.append(GroundingVerdict(OG.GROUNDED, "", ()))
    report = summarize(verdicts)
    assert report.opaque_fraction == 0.75
    assert report.opaque_dominates() is True


# -- known-bound guards (documented limits, pinned as intended behavior) -----

def test_bound_load_once_reuse_is_ungrounded(tmp_path):
    # KNOWN BOUND (documented false-positive for UNGROUNDED): data loaded ONCE
    # before the scope and reused inside without a re-read is invisible to the
    # observer, so a genuinely-grounded finding reads as UNGROUNDED. This is an
    # intended limit of flow observation, not a bug; pinned so a change is loud.
    data = tmp_path / "d.csv"
    data.write_text("x\n1\n")
    preloaded = open(str(data)).read()  # read OUTSIDE the scope
    with obs.observe(cites=str(data)) as h:
        _ = preloaded.upper()  # reuse, no read inside the scope
    assert h.verdict.grounding is OG.UNGROUNDED


def test_bound_wrong_read_stale_cache_is_grounded(tmp_path):
    # KNOWN BOUND (documented false-negative for GROUNDED): the observer sees a
    # non-empty read of the cited path and calls GROUNDED. It cannot tell the
    # bytes are stale or wrong — flow is not correctness. Intended limit; pinned.
    data = tmp_path / "d.csv"
    data.write_text("stale-but-nonempty\n")
    with obs.observe(cites=str(data)) as h:
        open(str(data)).read()
    assert h.verdict.grounding is OG.GROUNDED


def test_bound_incidental_read_is_ungrounded(tmp_path):
    # KNOWN BOUND: an incidental read (config/tokenizer/cache) must never ground
    # a finding. Citation binding is what enforces this; pinned as intended.
    data = tmp_path / "d.csv"
    data.write_text("x\n1\n")
    cfg = tmp_path / "c.yaml"
    cfg.write_text("k: v\n")
    with obs.observe(cites=str(data)) as h:
        open(str(cfg)).read()
    assert h.verdict.grounding is OG.UNGROUNDED


def test_bound_seam_is_opaque(tmp_path):
    # KNOWN BOUND: a read the observer cannot see across a seam is OPAQUE, never
    # a confident UNGROUNDED. Pinned as intended behavior.
    import subprocess

    data = tmp_path / "d.csv"
    data.write_text("x\n1\n")
    with obs.observe(cites=str(data)) as h:
        subprocess.run(["true"], capture_output=True)
    assert h.verdict.grounding is OG.OPAQUE


# -- declared metric reducer -------------------------------------------------

def test_oracle_records_the_default_scalar_reducer():
    from mareforma.observe.oracle import scalar_reducer

    result = perturbation_oracle(lambda x: x, 1.0, perturb=[5.0])
    assert result.reducer is scalar_reducer
    assert result.reducer.reinserts_model is False


def test_module_offers_no_bare_metric_function():
    """Every reduction the oracle offers is wrapped in a declared reducer.

    A bare metric function would reduce a finding without a declaration, so the
    result would carry no record of which reduction produced the number."""
    import inspect

    from mareforma.observe import oracle

    bare = [name for name, member in vars(oracle).items()
            if inspect.isfunction(member) and "metric" in name.lower()]
    assert bare == []


def test_oracle_records_a_declared_prose_reducer():
    from mareforma.observe.oracle import declared_reducer

    # A prose finding reduced by a declared model-based extraction: the result
    # must record the reducer AND that it reinserts a model into the ground truth.
    reducer = declared_reducer(
        "answer_length", lambda text: float(len(text)),
        reinserts_model=True, description="LLM-judge stand-in",
    )
    result = perturbation_oracle(lambda text: text, "short",
                                 perturb=["a much longer answer"], metric=reducer)
    assert result.reducer is reducer
    assert result.reducer.declaration()["reinserts_model"] is True


def test_bare_callable_metric_is_wrapped_as_a_reducer():
    result = perturbation_oracle(lambda x: x, 1.0, perturb=[9.0],
                                 metric=lambda f: float(f))
    assert result.reducer is not None
    assert result.reducer.name == "custom"


# -- measure: per-seam buckets, receipts, closing sentence -------------------

def test_summarize_buckets_opaque_by_seam():
    from mareforma.observe import GroundingVerdict, SeamEvent

    verdicts = [
        GroundingVerdict(OG.OPAQUE, "r", seams=(SeamEvent("subprocess", "x"),)),
        GroundingVerdict(OG.OPAQUE, "r", seams=(SeamEvent("socket", "y"),)),
        GroundingVerdict(OG.OPAQUE, "r", seams=(SeamEvent("subprocess", "z"),)),
    ]
    report = summarize(verdicts)
    assert report.opaque_by_seam == {"socket": 1, "subprocess": 2}


def test_summarize_receipts_matches_live_verdicts():
    from mareforma.observe import GroundingVerdict, ReadRecord, SeamEvent
    from mareforma.observe.measure import summarize_receipts

    verdicts = [
        GroundingVerdict(OG.GROUNDED, "g", cited_sources=("/x",),
                         reads=(ReadRecord("file", "/x", True),)),
        GroundingVerdict(OG.OPAQUE, "o", seams=(SeamEvent("thread", "t"),)),
    ]
    live = summarize(verdicts).to_dict()
    from_receipts = summarize_receipts([v.receipt() for v in verdicts]).to_dict()
    assert live == from_receipts


def test_summarize_receipts_degrades_a_malformed_record_to_opaque():
    """One bad record must not deny the whole report (it buckets OPAQUE)."""
    from mareforma.observe.measure import summarize_receipts

    report = summarize_receipts([
        {"grounding": "GROUNDED", "coverage": {"reads_seen": 1, "opens_detected": 1}},
        {"grounding": "PARTIAL"},
        {"grounding": None},
        {"grounding": "GROUNDED", "coverage": {"reads_seen": "x", "opens_detected": 2}},
    ])
    assert report.total == 4
    assert report.grounded == 2
    assert report.opaque == 2
    # The unreadable coverage field degrades to 0 reads over 2 detected opens.
    assert report.mean_read_coverage == 0.5


def test_from_receipt_names_the_unparsable_grounding_in_the_reason():
    from mareforma.observe import GroundingVerdict

    verdict = GroundingVerdict.from_receipt({"grounding": "PARTIAL", "reason": "why"})
    assert verdict.grounding is OG.OPAQUE
    assert "PARTIAL" in verdict.reason
    assert "why" in verdict.reason


def test_closing_sentence_names_the_dominant_seam():
    from mareforma.observe import GroundingVerdict, SeamEvent

    verdicts = [GroundingVerdict(OG.OPAQUE, "r", seams=(SeamEvent("subprocess", "x"),))
                for _ in range(3)]
    sentence = summarize(verdicts).closing_sentence()
    assert "subprocess" in sentence
    assert "OPAQUE dominates" in sentence


# -- a verdict never rests on a number that was never measured --------------
#
# NOT_INFLUENCED is the instrument's accusation: this finding does not depend on
# its data. These pin the paths that reached that accusation without a
# measurement behind it, each of which read as "the finding held still" because
# a non-finite number or a null identical to the input compares False against
# every threshold.

def test_a_null_that_reduces_to_nan_is_not_tested_never_hollow():
    # A ratio whose denominator a null zeroes returns NaN. NaN > threshold and
    # NaN <= threshold are both False, so an unguarded NaN falls through to
    # "flat under every null" and earns the finding the hollow verdict off a
    # measurement that never produced a number.
    def coefficient_of_variation(xs):
        mean = sum(xs) / len(xs)
        spread = (sum((v - mean) ** 2 for v in xs) / len(xs)) ** 0.5
        return spread / mean if mean else float("nan")

    res = perturbation_oracle(coefficient_of_variation, [1.0, 2.0, 3.0, 4.0])
    assert res.influence is OracleInfluence.NOT_TESTED
    assert res.not_tested_reason is NotTestedReason.NON_FINITE_VALUE
    assert res.influence is not OracleInfluence.NOT_INFLUENCED
    # A never-run row carries no measurement numbers to be misread as zeros.
    assert res.effect_size is None and res.decision_threshold is None


def test_a_non_finite_base_run_is_not_tested_not_a_crash():
    # The base runs feed statistics.pstdev, which raises an opaque AttributeError
    # on a NaN. That escaped the guard the docstring promises, so a target that
    # produced NaN crashed the measurement instead of being recorded.
    res = perturbation_oracle(lambda x: float("nan"), [1.0, 2.0], repeats=3)
    assert res.influence is OracleInfluence.NOT_TESTED
    assert res.not_tested_reason is NotTestedReason.NON_FINITE_VALUE
    assert "base run" in res.reason


def test_an_infinite_effect_is_not_tested_not_a_large_move():
    # Every value can be finite while the arithmetic between them overflows. An
    # infinite effect is the absence of a comparable number, not a huge move, and
    # it must not reach the router where it would count as INFLUENCED.
    big = 1.0e308
    res = perturbation_oracle(lambda x: big if x[0] == 0.0 else -big, [1.0, 2.0])
    assert res.influence is OracleInfluence.NOT_TESTED
    assert res.not_tested_reason is NotTestedReason.NON_FINITE_VALUE


def test_a_broken_target_is_not_reported_as_a_null_breaking_it():
    # The base is not a null. Bucketing a target that never ran under
    # "crashed-under-null" counts a broken target as evidence about the null
    # family's reach, and routes the reader to the wrong fix.
    res = perturbation_oracle(_raises, [1.0, 2.0])
    assert res.not_tested_reason is NotTestedReason.TARGET_FAILED
    assert "base run" in res.reason and "null" not in res.reason.split("(")[1]
    # A target that survives the base and dies on a null is the other reason.
    assert (
        perturbation_oracle(_dies_on_zeroed, [1.0, 2.0]).not_tested_reason
        is NotTestedReason.CRASHED_UNDER_NULL
    )


def test_a_null_that_cannot_be_built_is_not_charged_to_the_target():
    res = perturbation_oracle(lambda x: 1.0, [1.0, 2.0], perturb=_raises)
    assert res.not_tested_reason is NotTestedReason.NULL_CONSTRUCTION_FAILED
    assert res.traceback is not None


def test_an_empty_perturb_sequence_raises_rather_than_reading_as_not_tested():
    # A caller handing the oracle nothing to measure is a bug in the call. Turning
    # it into NOT_TESTED would file it as a measurement outcome a report counts.
    with pytest.raises(obs.NoPerturbationsError):
        perturbation_oracle(lambda x: 1.0, [1.0, 2.0], perturb=[])


# -- the result cannot contradict itself ------------------------------------

def test_flat_nulls_reads_the_routers_classification_not_the_effects():
    # flat_nulls used to be re-derived by comparing effects to the threshold, so a
    # null the router had counted as ambiguous was described as held invariant,
    # and a NaN effect vanished from the line while driving the verdict.
    res = perturbation_oracle(lambda x: sum(x), [1.0, 2.0, 3.0, 4.0], repeats=3)
    assert len(res.null_outcomes) == len(res.perturbation_effects)
    named = dict(zip(res.scramble_names, res.null_outcomes))
    assert named["zeroed"] is obs.NullOutcome.MOVED
    # A sum is invariant under a reordering: flat, and named as such.
    assert named["permuted"] is obs.NullOutcome.FLAT
    assert "permuted" in res.flat_nulls
    assert "zeroed" not in res.flat_nulls
    assert res.blind_spot_line().startswith("Held invariant under")


def test_an_ambiguous_null_is_never_described_as_held_invariant():
    # A move inside the band moved. Calling it invariant claims the finding held
    # still under a null it did not hold still under.
    base = 1000.0
    seq = iter([base] * 5 + [base + 1e-4] * 5)
    res = perturbation_oracle(
        lambda x: next(seq), 0.0, lambda x: x + 1.0, repeats=5
    )
    assert res.influence is OracleInfluence.UNDECIDABLE
    assert res.ambiguous_nulls == ("perturbation",)
    assert res.flat_nulls == ()
    assert "less than the decision threshold" in res.blind_spot_line()


# -- what the verdict did not try, said on the verdict ----------------------

def test_a_family_the_input_narrowed_says_which_nulls_it_lost():
    # Permuting a constant sequence changes nothing, so both marginal-preserving
    # nulls are identical to the base and cannot run. The verdict then rests on
    # the content-destroying nulls alone, which is a narrower claim than the same
    # verdict over data that supports the whole family.
    res = perturbation_oracle(lambda x: sum(x) / len(x), [5.0, 5.0, 5.0], repeats=3)
    assert res.dropped_nulls == ("permuted", "reversed")
    assert "ruled out" in res.reason
    assert "permuted, reversed" in res.blind_spot_line()
    # The same statistic over data that supports the family reaches the mixed
    # profile instead, which is the verdict the narrowed family cannot produce.
    full = perturbation_oracle(lambda x: sum(x) / len(x), [1.0, 2.0, 3.0, 4.0],
                               repeats=3)
    assert full.dropped_nulls == ()
    assert full.influence is OracleInfluence.UNDECIDABLE


def test_a_caller_chosen_null_says_so_on_the_verdict():
    # The derived family exists because a chosen null is a place to fish: pick the
    # one a mean is provably invariant to and it reads NOT_INFLUENCED however
    # honest the pipeline is. The oracle still measures what it is asked to, but
    # the result records who chose, so the row is not read as the family's verdict.
    res = perturbation_oracle(
        lambda x: sum(x) / len(x), [1.0, 2.0, 3.0],
        perturb=lambda x: list(reversed(x)), repeats=3,
    )
    assert res.influence is OracleInfluence.NOT_INFLUENCED
    assert res.caller_chose_nulls is True
    assert "chosen by the caller" in res.reason
    assert "chosen by the caller" in res.blind_spot_line()
    # The derived family on the same finding does not reach that verdict.
    derived = perturbation_oracle(lambda x: sum(x) / len(x), [1.0, 2.0, 3.0],
                                  repeats=3)
    assert derived.influence is OracleInfluence.UNDECIDABLE
    assert derived.caller_chose_nulls is False


def test_multiplicity_records_whether_the_widening_actually_applied():
    # On a pipeline with no measurable noise there is no sigma to widen, so the
    # correction is computed and discarded. Recording only the multiplicity would
    # read as "this was corrected for" on the modal deterministic target.
    det = perturbation_oracle(lambda x: x[0] * 2, [1.0, 2.0, 3.0],
                              repeats=3, multiplicity=10_000)
    assert det.multiplicity == 10_000
    assert det.multiplicity_applied is False
    noisy = iter([1.0, 1.4, 0.7, 1.2, 0.9] * 20)
    stochastic = perturbation_oracle(lambda x: next(noisy), [1.0, 2.0, 3.0],
                                     repeats=5, multiplicity=10)
    assert stochastic.multiplicity_applied is True


def test_the_sub_sigma_band_is_flat_not_ambiguous():
    # Pins the pre-existing boundary the profile rule inherited: on a stochastic
    # pipeline a move at or below one sigma is FLAT, not AMBIGUOUS, so a finding
    # whose every null moves it less than the run-to-run spread reads
    # NOT_INFLUENCED. Documented here so a change to that edge fails loudly.
    base = iter([0.0, 1.0, 2.0] * 40)
    res = perturbation_oracle(lambda x: next(base), [1.0, 2.0, 3.0], repeats=3)
    assert res.noise_floor > 0
    for effect, outcome in zip(res.perturbation_effects, res.null_outcomes):
        if effect <= res.noise_floor:
            assert outcome is obs.NullOutcome.FLAT


def test_influence_sweep_takes_a_per_finding_metric_from_the_tuple():
    doubled = obs.declared_reducer("doubled", lambda f: float(f) * 2)
    results = influence_sweep([
        (lambda x: sum(x), [1.0, 2.0], doubled),
        (lambda x: sum(x), [1.0, 2.0]),
    ], repeats=2)
    assert results[0].reducer is doubled
    assert results[1].reducer.name == "scalar"
    assert all(r.multiplicity == 2 for r in results)
