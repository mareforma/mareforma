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

from mareforma.observe import summarize_pilot
from tests.fixtures.killswitch import KILL_SWITCHES, run_all


# -- kill-switch A: all six caught ------------------------------------------

def test_killswitch_all_six_caught(tmp_path: Path):
    outcomes = run_all(tmp_path)
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
    # 20 GROUNDED contribute naive 2 / collapse 0, 7 UNGROUNDED naive 2 / collapse 1,
    # 3 OPAQUE naive 1 / collapse 0 → 7 collapsed of 57 naive lines.
    assert d["independence"]["collapsed_total"] == 7
    assert d["independence"]["naive_total"] == 57
    assert d["independence"]["same_model_collapse_rate"] == pytest.approx(7 / 57)
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
