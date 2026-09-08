"""The oracle calibration bench: the profile rule proved on planted pipelines.

The result that licenses shipping the influence oracle is a calibration bench:
finding classes whose ground truth is known by construction, crossed with the null
family, asserting the oracle routes each to the right verdict. The earlier bench was
a table-verification exercise whose entries and pipelines were written by one hand,
so a green run proved only internal consistency. This bench asserts the PROFILE rule
directly: it plants pipelines whose relationship to their data is known, runs the
whole derived null family, and checks both the per-null effect profile and the
verdict the profile routes to.

Three finding classes, each a pipeline over the same sequence input:

- HOLLOW, a silent fallback that ignores its input and returns a constant. Flat
  under every null, so the verdict is NOT_INFLUENCED. This is the read-but-ignored
  case flow cannot catch.
- POSITIONAL, an honest computation that reads specific positions of the data.
  Moves under every null (content and order both change it), so the verdict is
  INFLUENCED.
- MARGINAL, a genuine mean. It is a provable invariant of the marginal-preserving
  nulls (permute, reverse) and moves under the destroying ones. The profile is
  "moves under some, flat under others", so the verdict is UNDECIDABLE, never
  NOT_INFLUENCED. This is the false-hollow trap the whole instrument rests on: a
  real computation must never be called hollow just because a valid null cannot
  see it.

Plus the deterministic cell: the modal target has no run-to-run noise, where the
oracle used to degenerate to exact float equality. A tiny float-scale move must
read UNDECIDABLE, and measured-zero must be recorded as its own state.
"""
from __future__ import annotations

import pytest

from mareforma.observe.oracle import (
    OracleInfluence,
    QuantityClass,
    perturbation_oracle,
)

# A deterministic sequence finding, run at repeats>1 so the noise floor is
# measured (and comes out 0), the modal real case.
_DATA = [1.0, 2.0, 3.0, 4.0]
_REPEATS = 5


def _hollow(x):
    """A silent fallback: reads nothing, returns a constant."""
    return 42.0


def _boom(x):
    """A target a run kills, which is expected input rather than a bug."""
    raise RuntimeError("the target crashed")


def _positional(x):
    """Honest: reads specific positions, so content and order both matter."""
    return float(x[0]) * 10.0 + float(x[-1])


def _marginal(x):
    """A genuine mean: invariant under the marginal-preserving nulls."""
    return sum(x) / len(x)


def _sample_size(x):
    """A reported sample size: a function of length, not of content.

    The case the bench was missing. Every null in the family preserves length,
    so this is flat under all of them for a reason that has nothing to do with
    the pipeline, and the bench only ever protected the mean, which the
    marginal-preserving nulls already separate.
    """
    return float(len(x))


def _by_null(result):
    """Map null name -> effect for a result from the derived family."""
    return dict(zip(result.scramble_names, result.perturbation_effects))


# -- the three verdicts the profile routes to -------------------------------

def test_hollow_finding_is_not_influenced():
    res = perturbation_oracle(_hollow, _DATA, repeats=_REPEATS)
    assert res.influence is OracleInfluence.NOT_INFLUENCED
    # Flat under every null: that is what "hollow" means.
    assert all(e == 0.0 for e in res.perturbation_effects)


def test_positional_finding_is_influenced():
    res = perturbation_oracle(_positional, _DATA, repeats=_REPEATS)
    assert res.influence is OracleInfluence.INFLUENCED
    # Moved under every null.
    assert all(e > res.decision_threshold for e in res.perturbation_effects)


def test_marginal_finding_is_undecidable_never_hollow():
    res = perturbation_oracle(_marginal, _DATA, repeats=_REPEATS)
    # The load-bearing assertion: a genuine mean must NOT read as hollow.
    assert res.influence is not OracleInfluence.NOT_INFLUENCED
    assert res.influence is OracleInfluence.UNDECIDABLE


def test_a_length_shaped_quantity_is_flat_under_the_whole_family():
    """The false positive, shown before it is excluded.

    Nothing about this is ambiguous in the data: the family cannot move a
    length, so it comes out flat whatever the pipeline does. Undeclared, that
    reads as the same verdict a silent fallback earns.
    """
    res = perturbation_oracle(_sample_size, _DATA, repeats=_REPEATS)
    assert all(e == 0.0 for e in res.perturbation_effects)
    assert res.influence is OracleInfluence.NOT_INFLUENCED


def test_a_declared_length_invariant_quantity_is_never_hollow():
    """The exclusion, and the assertion that fails without it.

    Sample sizes, cell counts and degrees of freedom are among the most common
    numbers in any write-up. Calling them hollow is a systematic false positive,
    not an edge case, so the class routes to undecidable by construction.
    """
    res = perturbation_oracle(
        _sample_size, _DATA, repeats=_REPEATS,
        quantity_class=QuantityClass.LENGTH_INVARIANT,
    )
    assert res.influence is not OracleInfluence.NOT_INFLUENCED
    assert res.influence is OracleInfluence.UNDECIDABLE
    assert "preserves the input's length" in res.reason


def test_declaring_a_class_cannot_launder_a_real_dependence():
    """A finding the family moves is INFLUENCED whatever class it declared.

    Weak on its own: the family moves this target, so control never reaches the
    branch the exclusion lives in and this passes with the exclusion deleted.
    Kept because it pins the other half of the grid, and paired with the test
    below, which is the one that has teeth.
    """
    res = perturbation_oracle(
        _positional, _DATA, repeats=_REPEATS,
        quantity_class=QuantityClass.LENGTH_INVARIANT,
    )
    assert res.influence is OracleInfluence.INFLUENCED


def test_the_exclusion_does_suppress_a_hollow_finding_when_declared():
    """The cost of the exclusion, pinned rather than left in a docstring.

    NOT_INFLUENCED is the only accusation this instrument makes, and a
    declaration the instrument cannot check suppresses it. A pipeline that
    ignores its input entirely, the canonical silent fallback, reads hollow
    undeclared and undecidable declared, and undecidable leaves the report.

    That is a real hole and naming it is the honest thing to do: the class is a
    caller's word, on the one axis the product otherwise refuses to take on the
    producer's word. This test is the record that it was chosen, not overlooked.
    """
    undeclared = perturbation_oracle(_hollow, _DATA, repeats=_REPEATS)
    declared = perturbation_oracle(
        _hollow, _DATA, repeats=_REPEATS,
        quantity_class=QuantityClass.LENGTH_INVARIANT,
    )
    assert undeclared.influence is OracleInfluence.NOT_INFLUENCED
    assert declared.influence is OracleInfluence.UNDECIDABLE
    assert declared.declared_exclusion is True
    assert undeclared.declared_exclusion is False


@pytest.mark.parametrize("shape, kwargs", [
    ("the target crashed", dict(run_fn=_boom, base_input=_DATA)),
    ("no family fits the shape", dict(run_fn=len, base_input=object())),
])
def test_a_row_that_never_ran_still_records_what_was_declared(shape, kwargs):
    """The class is known before anything runs, so a never-run row carries it.

    It was dropped on every NOT_TESTED path, so the one field that exists to
    record what the caller declared reported ``CONTENT_DEPENDENT`` for a caller
    who declared otherwise. An audit weighing declarations across a corpus would
    have counted every crashed and every unsupported row on the wrong side, and
    the free-text reason it replaced said nothing about the class either.
    """
    res = perturbation_oracle(
        quantity_class=QuantityClass.LENGTH_INVARIANT, **kwargs,
    )
    assert res.influence is OracleInfluence.NOT_TESTED, shape
    assert res.quantity_class == QuantityClass.LENGTH_INVARIANT.value, shape


def test_building_a_never_run_row_by_hand_refuses_an_unreadable_class():
    """The field records a declaration, so it takes declarations only.

    The oracle converts before it runs, so its own paths cannot reach here with
    a bad value. This constructor is reachable on its own, and a row carrying a
    class no reader can branch on is worse than one carrying the default: the
    default is at least a value the enum defines.
    """
    from mareforma.observe.oracle import NotTestedReason, OracleResult

    with pytest.raises(ValueError, match="not a quantity class"):
        OracleResult.not_tested(
            NotTestedReason.UNSUPPORTED_SHAPE,
            quantity_class="LENGTH_INVARIENT",
        )


def test_an_unreadable_class_is_refused_before_the_target_runs():
    """A typo used to read as the default and reinstate the false positive.

    Refused up front, because the check used to sit past every re-run of the
    target: an expensive pipeline paid for the whole measurement and then found
    out.
    """
    runs = []

    def counting(x):
        runs.append(1)
        return float(len(x))

    with pytest.raises(ValueError, match="not a quantity class"):
        perturbation_oracle(
            counting, _DATA, repeats=_REPEATS,
            quantity_class="LENGTH_INVARIENT",
        )
    assert runs == [], "the target ran before the class was checked"


# -- the 3x3 cross-tab: finding class x null, the per-null effect profile ----

def test_bench_cross_tab_reproduces_the_calibration():
    hollow = _by_null(perturbation_oracle(_hollow, _DATA, repeats=_REPEATS))
    positional = _by_null(perturbation_oracle(_positional, _DATA, repeats=_REPEATS))
    marginal = _by_null(perturbation_oracle(_marginal, _DATA, repeats=_REPEATS))

    # A destroying null (zeroed) and a marginal-preserving null (permuted) are
    # both present, or the cross-tab cannot separate the classes.
    for row in (hollow, positional, marginal):
        assert "zeroed" in row and "permuted" in row

    # HOLLOW: flat everywhere.
    assert hollow["zeroed"] == 0.0
    assert hollow["permuted"] == 0.0

    # POSITIONAL: moves under the destroying null AND the marginal-preserving one
    # (a reordering changes which element sits at position 0).
    assert positional["zeroed"] > 0.0
    assert positional["permuted"] > 0.0

    # MARGINAL: moves under the destroying null, INVARIANT under the
    # marginal-preserving one. This single cell is the false-hollow trap.
    assert marginal["zeroed"] > 0.0
    assert marginal["permuted"] == 0.0


# -- the deterministic cell -------------------------------------------------

def test_deterministic_target_does_not_read_influenced_on_a_float_move():
    # The modal target: no run-to-run noise. A move at the float-equality scale
    # must read UNDECIDABLE, not INFLUENCED, and the pipeline is recorded as
    # measured-deterministic (its own state, distinct from an unmeasured run).
    base = 1000.0
    seq = iter([base] * _REPEATS + [base + 1e-4] * _REPEATS)
    res = perturbation_oracle(lambda x: next(seq), 0.0, lambda x: x + 1.0,
                              repeats=_REPEATS)
    assert res.influence is OracleInfluence.UNDECIDABLE
    assert res.deterministic is True
    assert res.noise_floor == 0.0


def test_deterministic_target_still_catches_a_real_dependence():
    # The float-equality band must not swallow a real effect: a deterministic
    # pipeline that genuinely tracks its input still reads INFLUENCED.
    res = perturbation_oracle(_positional, _DATA, repeats=_REPEATS)
    assert res.influence is OracleInfluence.INFLUENCED
    assert res.deterministic is True


# -- the cell where the data itself narrows the family ----------------------

def test_a_constant_input_cannot_run_the_marginal_preserving_nulls():
    # The bench's three classes all assume the input supports the whole family.
    # Constant data does not: permuting or reversing it is the identity, so the
    # two nulls that separate a genuine mean from a hollow finding cannot run.
    # The verdict is still reported, and it must say what it never tried, or the
    # same statistic reads INFLUENCED here and UNDECIDABLE over ordinary data
    # with nothing on the row to explain the difference.
    narrow = perturbation_oracle(_marginal, [5.0, 5.0, 5.0], repeats=_REPEATS)
    assert narrow.scramble_names == ("zeroed", "constant")
    assert narrow.dropped_nulls == ("permuted", "reversed")
    assert "ruled out" in narrow.reason

    full = perturbation_oracle(_marginal, _DATA, repeats=_REPEATS)
    assert full.influence is OracleInfluence.UNDECIDABLE
    assert full.dropped_nulls == ()
