"""Kill-switch A (all six caught) and the slim natural-prevalence pilot harness.

The kill-switch fixtures are the cheap go/no-go pre-check: a correct instrument
must catch all six seeded failures before any spend on a natural-corpus run. The
pilot harness runs a slim receipts file through both measurement arms and reports
the honest OPAQUE-coverage bound, so a run with a blind observer reads as a
bounded lower bound, not a false prevalence number.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from mareforma.observe import GROUNDING_AXIS_VERSION, summarize_pilot
from mareforma.observe import _loaders, _scope
from tests.fixtures.killswitch import (
    KILL_SWITCHES,
    decoy_incidental_read,
    excluded_partition,
    register_model_responses,
    run_all,
    same_model_corroboration,
    silent_zero_row_fallback,
    unrecognized_host_model,
)


# -- kill-switch A: all six caught ------------------------------------------

def test_killswitch_all_six_caught(tmp_path: Path, httpx_mock):
    outcomes = run_all(tmp_path, httpx_mock)
    assert len(outcomes) == 6, "there are exactly six kill-switch fixtures"
    missed = [o.name for o in outcomes if not o.caught]
    assert not missed, (
        "the instrument missed a kill-switch it must catch: "
        + ", ".join(f"{o.name} (observed {o.observed})" for o in outcomes
                    if not o.caught)
    )
    # Every fixture reports the concrete observation behind its verdict, so a
    # future regression names what it broke rather than a bare False.
    for o in outcomes:
        assert o.observed
        assert o.expectation


def test_each_killswitch_is_distinct():
    names = [c.__name__ for c in KILL_SWITCHES]
    assert len(set(names)) == len(names) == 6


def test_model_axis_killswitches_break_when_provider_derivation_breaks(
    tmp_path: Path, monkeypatch, httpx_mock
):
    """The two model-axis fixtures must route through the observer's socket seam.

    If a fixture hand-feeds the provider instead of driving ``observe()``, an
    observer-side regression in host recognition leaves the kill-switch green and
    the pre-spend gate greenlights a blind observer. Break ``_provider_of`` in
    each direction and the fixtures must stop reporting ``caught`` -- proof they
    exercise ``_provider_of`` rather than a hardcoded provider.
    """
    register_model_responses(httpx_mock)
    # Break Anthropic recognition: the same-model calls now derive no provider, so
    # the seam tiers each UNVERIFIABLE (soft) instead of COMPUTED. Soft lineage
    # drops the naive hard-line count to 0, so the naive-2/number-1 same-model
    # collapse never forms and the kill-switch stops firing.
    monkeypatch.setattr(_loaders, "_provider_of", lambda url: None)
    assert same_model_corroboration(tmp_path).caught is False

    # Recognize an arbitrary host as a provider: the producer-controlled endpoint
    # now mints COMPUTED, so the "unrecognized host is UNVERIFIABLE" guard no
    # longer holds and the kill-switch stops firing.
    monkeypatch.setattr(_loaders, "_provider_of", lambda url: "anthropic")
    assert unrecognized_host_model(tmp_path).caught is False


def test_read_axis_killswitches_break_when_the_observer_goes_blind(
    tmp_path: Path, monkeypatch
):
    """The read-axis fixtures must route through the observer's read seam.

    A fixture whose ``caught`` rests only on fields a blind observer also
    produces greenlights the pre-spend gate on an instrument that saw
    nothing. Break the seam in each direction and every fixture that
    depends on it must stop reporting ``caught``.

    ``number_with_no_execution`` is absent by construction: its scope
    contains no read, so a blind observer and an honest one report the
    same empty provenance. The legs below are what makes its UNGROUNDED
    worth reading, they prove the seam was live for the run.
    """
    # Leg 1: the binder can never bind a read to a citation, so nothing is ever
    # grounded. Only the fixture that claims a partition WAS grounded can see
    # this; the zero-row and decoy catches do not rest on a successful bind.
    with monkeypatch.context() as m:
        m.setattr(_scope, "read_norm_matches", lambda *a, **k: False)
        blind_binder = tmp_path / "blind_binder"
        blind_binder.mkdir()
        assert excluded_partition(blind_binder).caught is False

    # Leg 2: no read reaches the scope. Every fixture whose catch rests on an
    # observed read must stop firing.
    with monkeypatch.context() as m:
        m.setattr(_scope, "record_read", lambda *a, **k: None)
        blind_seam = tmp_path / "blind_seam"
        blind_seam.mkdir()
        assert silent_zero_row_fallback(blind_seam).caught is False
        assert excluded_partition(blind_seam).caught is False
        assert decoy_incidental_read(blind_seam).caught is False


# -- the slim natural-prevalence pilot --------------------------------------

def _ground_receipt(state: str, indep: dict | None = None) -> dict:
    r = {
        "version": "v0.3.9",
        "grounding": state,
        "reason": state.lower(),
        "cited_sources": ["/data/x"],
        "grounded_sources": ["/data/x"] if state == "GROUNDED" else [],
        "reads": (
            [{"kind": "file", "identifier": "/data/x", "nonempty": True,
              "content_address": None}] if state == "GROUNDED" else []
        ),
        "seams": (
            [{"kind": "subprocess", "detail": "spawn"}] if state == "OPAQUE" else []
        ),
        "coverage": {"reads_seen": 1 if state == "GROUNDED" else 0,
                     "opens_detected": 1 if state == "GROUNDED" else 0},
    }
    if indep is not None:
        r["independence"] = indep
    return r


def test_pilot_harness_emits_bounded_report():
    # A slim ~30-finding pilot: a healthy split (observer sees most of it) plus an
    # independence arm. Both arms report, and the OPAQUE-coverage bound is stated.
    receipts = []
    for _ in range(20):
        receipts.append(_ground_receipt("GROUNDED", {"number": 2, "naive": 2, "soft": False}))
    for _ in range(7):
        receipts.append(_ground_receipt("UNGROUNDED", {"number": 1, "naive": 2, "soft": False}))
    for _ in range(3):
        receipts.append(_ground_receipt("OPAQUE", {"number": 1, "naive": 1, "soft": True}))

    report = summarize_pilot(receipts)
    d = report.to_dict()
    assert d["n"] == 30
    # Both arms present.
    assert d["grounding"]["counts"]["GROUNDED"] == 20
    assert d["independence"] is not None
    assert d["independence"]["total"] == 30
    # The independence arm computed the collapse and the unverifiable fraction.
    # 20 GROUNDED naive 2 / collapse 0, 7 UNGROUNDED naive 2 / collapse 1; the 3
    # OPAQUE naive 1 are single lines, not corroborations, excluded from the
    # denominator. The collapse rate is over CORROBORATIONS (naive>=2): 7 of 54.
    assert d["independence"]["collapsed_total"] == 7
    assert d["independence"]["naive_total"] == 54
    assert d["independence"]["same_model_collapse_rate"] == pytest.approx(7 / 54)
    assert d["independence"]["unverifiable"] == 3
    # OPAQUE is a minority here: the bound is stated but does not dominate.
    assert report.opaque_dominates() is False
    assert "lower bound" in d["coverage_bound"]
    assert "OPAQUE" in report.closing_sentence()


def test_pilot_honesty_gate_fires_when_opaque_dominates():
    # When the observer is blind on most of the pipeline, the pilot refuses to
    # read the split as a prevalence number: the honesty gate fires.
    receipts = [_ground_receipt("OPAQUE") for _ in range(18)]
    receipts += [_ground_receipt("GROUNDED") for _ in range(12)]
    report = summarize_pilot(receipts)
    assert report.opaque_dominates() is True
    bound = report.coverage_bound()
    assert "not yet a trustworthy prevalence number" in bound
    assert "attach deeper" in bound


def test_pilot_without_independence_still_reports_grounding():
    # A grounding-only receipts file (no independence records) still yields a
    # pilot report; the independence arm is simply absent, never fabricated.
    receipts = [_ground_receipt("GROUNDED") for _ in range(5)]
    report = summarize_pilot(receipts)
    assert report.independence.total == 0
    assert report.to_dict()["independence"] is None
    assert report.grounding.total == 5
