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

from mareforma.observe.oracle import OracleInfluence, perturbation_oracle

# A deterministic sequence finding, run at repeats>1 so the noise floor is
# measured (and comes out 0), the modal real case.
_DATA = [1.0, 2.0, 3.0, 4.0]
_REPEATS = 5


def _hollow(x):
    """A silent fallback: reads nothing, returns a constant."""
    return 42.0


def _positional(x):
    """Honest: reads specific positions, so content and order both matter."""
    return float(x[0]) * 10.0 + float(x[-1])


def _marginal(x):
    """A genuine mean: invariant under the marginal-preserving nulls."""
    return sum(x) / len(x)


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
