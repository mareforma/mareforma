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
    OracleInfluence,
    Reconciliation,
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
