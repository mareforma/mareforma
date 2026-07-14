"""The independence arm of the measurement: aggregate effective independence.

A run's per-finding effective-independence records aggregate into the paper's
independence arm: the distribution of the effective number (a single line vs
genuine corroboration), the UNVERIFIABLE fraction (soft lineage), and the same-
model-collapse rate (corroborations that were one COMPUTED model counted twice).
These tests pin the aggregation on hand-authored records AND the per-finding
record producer on a real graph, so the two never drift.
"""
from __future__ import annotations

from pathlib import Path

import mareforma
from mareforma.observe.measure import (
    IndependenceReport,
    independence_records,
    summarize_independence,
    summarize_independence_receipts,
)
from mareforma.trust._store import effective_independence_receipt
from tests._helpers import (
    _bootstrap_key, _enroll_key, _est, _pred, _prop, _verdict,
)


_CLAUDE = "claude-3-5-sonnet-20241022"  # COMPUTED root: claude-3-5-sonnet
_GPT = "gpt-4o-2024-08-06"              # COMPUTED root: gpt-4o


def _rec(number: int, naive: int, soft: bool = False) -> dict:
    return {"number": number, "naive": naive, "soft": soft}


# -- aggregation over hand-authored records ---------------------------------

def test_independence_report_distribution():
    # Findings at 0, 1, 1, and >=2: the distribution counts each bucket and the
    # fractions are over the total.
    records = [_rec(0, 0), _rec(1, 1), _rec(1, 1), _rec(3, 3)]
    report = summarize_independence(records)
    assert isinstance(report, IndependenceReport)
    assert report.total == 4
    assert report.at_zero == 1
    assert report.at_one == 2
    assert report.at_two_plus == 1
    assert report.fraction_at_one() == 0.5
    assert report.fraction_two_plus() == 0.25
    assert report.distribution() == {"0": 0.25, "1": 0.5, ">=2": 0.25}


def test_same_model_collapse_rate():
    # Two findings each corroborated by a naive counter as 2 lines, but each is
    # one COMPUTED model counted twice (effective 1). Of 4 naive corroborations,
    # 2 collapsed → rate 0.5.
    records = [_rec(1, 2), _rec(1, 2)]
    report = summarize_independence(records)
    assert report.naive_total == 4
    assert report.collapsed_total == 2
    assert report.same_model_collapse_rate == 0.5


def test_same_model_collapse_rate_zero_when_no_corroboration():
    # All single lines (naive < 2): a single line is not a corroboration, so the
    # collapse denominator is 0 and the rate is 0 — never a divide error.
    report = summarize_independence([_rec(1, 1), _rec(1, 1)])
    assert report.naive_total == 0
    assert report.collapsed_total == 0
    assert report.same_model_collapse_rate == 0.0


def test_unverifiable_fraction():
    # Two of four findings rest on soft lineage → UNVERIFIABLE fraction 0.5.
    records = [_rec(2, 2), _rec(1, 0, soft=True), _rec(1, 0, soft=True), _rec(2, 2)]
    report = summarize_independence(records)
    assert report.unverifiable == 2
    assert report.unverifiable_fraction == 0.5


def test_empty_independence_report_is_all_zero():
    report = summarize_independence([])
    assert report.total == 0
    assert report.same_model_collapse_rate == 0.0
    assert report.unverifiable_fraction == 0.0
    assert report.closing_sentence() == "No independence records to measure."


def test_malformed_record_degrades_to_conservative_default():
    # A record missing every field, or carrying a non-numeric one, aggregates as
    # a zero-independence finding rather than raising, and naive < number never
    # yields a negative collapse.
    report = summarize_independence(
        [{}, {"number": 2, "naive": 1}, {"number": "oops", "naive": None}]
    )
    assert report.total == 3
    assert report.at_zero == 2  # the empty record and the non-numeric one
    assert report.collapsed_total == 0  # max(0, 1 - 2) clamped


# -- extraction from combined receipts --------------------------------------

def test_independence_records_pulls_the_sub_records():
    receipts = [
        {"grounding": "GROUNDED", "independence": _rec(2, 2)},
        {"grounding": "OPAQUE"},  # no independence arm on this finding
        {"grounding": "UNGROUNDED", "independence": _rec(1, 1)},
    ]
    assert independence_records(receipts) == [_rec(2, 2), _rec(1, 1)]
    report = summarize_independence_receipts(receipts)
    assert report.total == 2


# -- the per-finding record producer on a real graph ------------------------

def test_receipt_same_model_reads_naive_two_number_one(tmp_path: Path):
    # Two same-model COMPUTED checks (distinct signer + data): a naive signer
    # counter sees 2, the model-aware effective number is 1 — one collapse.
    ka = _bootstrap_key(tmp_path, "ka.key")
    kb = _bootstrap_key(tmp_path, "kb.key")
    _enroll_key(tmp_path, ka, kb)
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=ka) as g:
        g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1",
                         grounding=_verdict(_CLAUDE))
    with mareforma.open(tmp_path, key_path=kb) as g:
        g.assert_finding(prop, pred, _est(), data_id="ds2", generated_by="run2",
                         grounding=_verdict(_CLAUDE))
        rec = effective_independence_receipt(g._conn, prop.content_id())
    assert rec == {"number": 1, "naive": 2, "soft": False}


def test_receipt_distinct_model_does_not_collapse(tmp_path: Path):
    ka = _bootstrap_key(tmp_path, "ka.key")
    kb = _bootstrap_key(tmp_path, "kb.key")
    _enroll_key(tmp_path, ka, kb)
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=ka) as g:
        g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1",
                         grounding=_verdict(_CLAUDE))
    with mareforma.open(tmp_path, key_path=kb) as g:
        g.assert_finding(prop, pred, _est(), data_id="ds2", generated_by="run2",
                         grounding=_verdict(_GPT))
        rec = effective_independence_receipt(g._conn, prop.content_id())
    assert rec == {"number": 2, "naive": 2, "soft": False}


def test_receipt_soft_lineage_is_unverifiable_not_collapse(tmp_path: Path):
    # A PROXY (declared) line makes the pair UNVERIFIABLE for independence: the
    # effective number rests on soft lineage (soft=True), the naive-hard count
    # excludes the soft line, and it is not counted as a same-model collapse.
    ka = _bootstrap_key(tmp_path, "ka.key")
    kb = _bootstrap_key(tmp_path, "kb.key")
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=ka) as g:
        g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1",
                         grounding=_verdict(_CLAUDE))
    with mareforma.open(tmp_path, key_path=kb) as g:
        g.assert_finding(prop, pred, _est(), data_id="ds2", generated_by="run2",
                         grounding=_verdict(_GPT, source="declared"))
        rec = effective_independence_receipt(g._conn, prop.content_id())
    assert rec["soft"] is True
    # naive counts only the one hard (COMPUTED) line; the effective number rests
    # on it, so naive - number is a non-negative, non-collapse difference.
    assert rec["number"] >= 1
    assert max(0, rec["naive"] - rec["number"]) == 0
