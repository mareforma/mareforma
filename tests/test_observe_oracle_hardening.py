"""Oracle hardening: prose reducers, thin-sigma guard, and multiplicity control.

The scalar A/B/C path and its noise handling are pinned in
``tests/test_observe_oracle.py``; these tests cover the additions and, by pinning
the defaults, guard that the scalar path stays byte-identical: multiplicity 1 and
the thin-sigma guard off must not move a verdict the older tests fixed.
"""
from __future__ import annotations

import math

import pytest

from mareforma.observe.oracle import (
    OracleInfluence,
    numeric_extraction_reducer,
    perturbation_oracle,
    scalar_reducer,
)


# -- prose reducer path ------------------------------------------------------

def test_prose_reducer_declared():
    # A prose pipeline reports the effect in text; the model-free extraction
    # reduces it to a number, and the result records the declared reducer without
    # reinserting a model into the ground truth.
    reducer = numeric_extraction_reducer()
    run = lambda answer: answer  # the pipeline "returns" its prose answer
    res = perturbation_oracle(
        run, "the estimated effect was 0.10",
        perturb=["the estimated effect was 0.90"], metric=reducer,
    )
    assert res.reducer is reducer
    assert res.reinserts_model is False
    assert res.declaration()["name"] == "numeric_extraction"
    assert res.declaration()["reinserts_model"] is False
    # 0.90 vs 0.10 is a real move on the deterministic single run.
    assert res.influence is OracleInfluence.INFLUENCED


def test_numeric_extraction_reduces_prose_and_scalars():
    r = numeric_extraction_reducer()
    assert r("the effect was 0.42") == 0.42
    assert r("a decrease of -3 units") == -3.0
    assert r("rate 1.2e-3 per hour") == pytest.approx(1.2e-3)
    assert r(0.5) == 0.5  # a bare scalar still reduces
    # A number-free answer raises rather than inventing a silent zero.
    with pytest.raises(ValueError):
        r("no number here at all")
    # A boolean is not a numeric finding.
    with pytest.raises(TypeError):
        r(True)


def test_reinserts_model_flag_recorded():
    # An embedding / LLM-judge reduction runs a model, so it must declare
    # reinserts_model=True, and the measurement records that its ground truth is
    # no longer model-independent.
    from mareforma.observe.oracle import declared_reducer

    judge = declared_reducer(
        "llm_judge", lambda text: float(len(text)),
        reinserts_model=True, description="LLM-judge stand-in",
    )
    res = perturbation_oracle(
        lambda t: t, "short", perturb=["a considerably longer answer"], metric=judge,
    )
    assert res.reinserts_model is True
    assert res.declaration()["reinserts_model"] is True


# -- multiplicity control ----------------------------------------------------

def test_multiplicity_control():
    # An effect that clears the single-finding bar must not clear it once the
    # threshold is corrected for a large family: the noisiest of many findings
    # would otherwise cross by chance. base [0,1,2] (std ~0.816), perturbed
    # [3,4,5] (effect 3). At multiplicity 1 the margin is 3*0.816=2.449 →
    # INFLUENCED; at 100 the multiplier gains sqrt(2 ln100)~3.03 → margin ~4.9 →
    # the effect no longer clears it (UNDECIDABLE, never a silent INFLUENCED).
    def _run():
        seq = iter([0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
        return lambda x: next(seq)

    single = perturbation_oracle(_run(), 0.0, lambda x: x + 1, repeats=3)
    family = perturbation_oracle(_run(), 0.0, lambda x: x + 1, repeats=3,
                                 multiplicity=100)
    assert single.influence is OracleInfluence.INFLUENCED
    assert family.decision_threshold > single.decision_threshold
    assert family.influence is OracleInfluence.UNDECIDABLE
    assert family.multiplicity == 100


def test_multiplicity_one_is_unchanged():
    # The default (multiplicity 1) adds nothing to the threshold: sqrt(2 ln 1)=0.
    def _run():
        seq = iter([0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
        return lambda x: next(seq)

    base = perturbation_oracle(_run(), 0.0, lambda x: x + 1, repeats=3)
    one = perturbation_oracle(_run(), 0.0, lambda x: x + 1, repeats=3, multiplicity=1)
    assert one.decision_threshold == base.decision_threshold


def test_multiplicity_rejects_zero():
    with pytest.raises(ValueError):
        perturbation_oracle(lambda x: x, 1.0, lambda x: x + 1, multiplicity=0)


# -- thin-sigma guard --------------------------------------------------------

def test_thin_sigma_guarded():
    # A noise floor from only 3 repeats is thin. An effect just past the
    # un-guarded margin (2.449) but under the guarded one is INFLUENCED without
    # the guard and UNDECIDABLE with it: base [0,1,2] (std ~0.816), perturbed
    # mean 3.6 (effect 2.6). Guarded multiplier 3*sqrt(5/3)~3.87 → margin ~3.16.
    def _run():
        seq = iter([0.0, 1.0, 2.0, 2.6, 3.6, 4.6])
        return lambda x: next(seq)

    off = perturbation_oracle(_run(), 0.0, lambda x: x + 1, repeats=3)
    on = perturbation_oracle(_run(), 0.0, lambda x: x + 1, repeats=3,
                             thin_sigma_guard=True)
    assert off.influence is OracleInfluence.INFLUENCED
    assert on.noise_is_thin is True
    assert on.decision_threshold > off.decision_threshold
    assert on.influence is OracleInfluence.UNDECIDABLE


def test_noise_is_thin_recorded_but_verdict_unchanged_by_default():
    # noise_is_thin is informational: it is recorded at 3 repeats but, with the
    # guard off (the default), it does not move the verdict. This pins that the
    # scalar path stays as tests/test_observe_oracle.py fixed it.
    def _run():
        seq = iter([0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
        return lambda x: next(seq)

    res = perturbation_oracle(_run(), 0.0, lambda x: x + 1, repeats=3)
    assert res.noise_is_thin is True
    assert res.influence is OracleInfluence.INFLUENCED  # unchanged by the flag


def test_thick_sigma_is_not_thin():
    # At >= _THIN_REPEATS repeats the floor is not thin, so the guard is a no-op.
    def _run():
        seq = iter([0.0, 1.0, 2.0, 3.0, 4.0] + [10.0, 11.0, 12.0, 13.0, 14.0])
        return lambda x: next(seq)

    res = perturbation_oracle(_run(), 0.0, lambda x: x + 1, repeats=5,
                              thin_sigma_guard=True)
    assert res.noise_is_thin is False


# -- default scalar reducer unchanged ---------------------------------------

def test_default_reducer_is_scalar_and_model_free():
    res = perturbation_oracle(lambda x: x, 1.0, perturb=[9.0])
    assert res.reducer is scalar_reducer
    assert res.reinserts_model is False
    assert res.multiplicity == 1
