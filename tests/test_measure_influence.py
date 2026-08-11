"""The influence arm of the measurement: aggregate per-edge influence verdicts.

A run's receipts carry per-cited-source influence records; the arm flattens them
1:N into edges and aggregates by verdict, with NOT_TESTED excluded from the rate
denominator but counted and bucketed by reason. These tests pin the aggregation on
hand-authored records AND the end-to-end path (audit writes the arm, measure reads
it), so the reader and the writer never drift apart, the way the independence arm
drifted when nothing outside the tests wrote its records.
"""
from __future__ import annotations

import json
from pathlib import Path

from mareforma.observe.measure import (
    InfluenceReport,
    influence_records,
    summarize_influence,
    summarize_influence_receipts,
)


def _edge(verdict: str, *, source: str = "s", reason=None, runs=None) -> dict:
    rec = {"cited_source": source, "influence": verdict, "not_tested_reason": reason}
    if runs is not None:
        rec["distinct_runs"] = runs
    return rec


# -- aggregation over hand-authored records ---------------------------------

def test_influence_report_counts_by_verdict():
    records = [
        _edge("INFLUENCED", runs=2),
        _edge("INFLUENCED", runs=2),
        _edge("NOT_INFLUENCED", runs=2),
        _edge("UNDECIDABLE", runs=2),
        _edge("NOT_TESTED", reason="unsupported-shape"),
    ]
    report = summarize_influence(records)
    assert isinstance(report, InfluenceReport)
    assert report.total == 5
    assert report.influenced == 2
    assert report.not_influenced == 1
    assert report.undecidable == 1
    assert report.not_tested == 1
    assert report.not_tested_by_reason == {"unsupported-shape": 1}


def test_not_tested_is_excluded_from_the_rate_denominator():
    # The rate is over RESOLVED edges, never the NOT_TESTED-inflated total, so a
    # pile of never-run edges cannot dilute or inflate the influenced fraction.
    records = [_edge("INFLUENCED", runs=1)] + [
        _edge("NOT_TESTED", reason="unsupported-shape") for _ in range(9)
    ]
    report = summarize_influence(records)
    assert report.total == 10
    assert report.resolved == 1
    assert report.influenced_fraction == 1.0
    assert report.not_tested_dominates() is True


def test_distinct_runs_are_summed_so_a_rate_carries_its_run_count():
    records = [_edge("INFLUENCED", runs=3), _edge("NOT_INFLUENCED", runs=4)]
    assert summarize_influence(records).distinct_runs == 7


def test_all_not_tested_says_so_and_claims_no_prevalence():
    # A report where every edge is NOT_TESTED must not print what a resolved
    # report prints: it names the non-measurement and claims no prevalence.
    records = [_edge("NOT_TESTED", reason=None) for _ in range(3)]
    report = summarize_influence(records)
    sentence = report.closing_sentence()
    assert "not tested" in sentence.lower()
    assert "no influence prevalence" in sentence
    assert report.not_tested_by_reason == {"not-run": 3}


def test_empty_arm_is_reported_as_absent_never_fabricated():
    report = summarize_influence([])
    assert report.total == 0
    assert report.closing_sentence() == "No influence records to measure."


def test_unrecognized_verdict_degrades_to_not_tested_unknown():
    report = summarize_influence([_edge("GARBAGE")])
    assert report.not_tested == 1
    assert report.not_tested_by_reason == {"unknown": 1}


# -- flattening 1:N over cited sources --------------------------------------

def test_influence_records_flatten_one_finding_to_many_edges():
    # A finding citing three sources contributes three edges: the 1:N difference
    # from the independence arm, which is 1:1 per finding.
    receipts = [
        {"finding_id": "f1", "influence": [
            _edge("NOT_TESTED", source="a"),
            _edge("NOT_TESTED", source="b"),
            _edge("NOT_TESTED", source="c"),
        ]},
        {"finding_id": "f2", "influence": [_edge("INFLUENCED", source="d", runs=1)]},
    ]
    edges = influence_records(receipts)
    assert len(edges) == 4
    assert summarize_influence_receipts(receipts).total == 4


def test_receipts_without_the_influence_key_report_no_arm():
    # An old receipt written before the arm existed has no influence key; the arm
    # is simply absent, never fabricated.
    receipts = [{"finding_id": "f1", "grounding": "GROUNDED"}]
    assert influence_records(receipts) == []
    assert summarize_influence_receipts(receipts).total == 0


# -- end to end: the writer and the reader meet -----------------------------

def test_audit_writes_the_arm_and_measure_reads_it(tmp_path: Path):
    # The path the release exists for: audit attaches the influence record, and
    # measure over those receipts actually reports the arm. The independence arm
    # never had a live writer; this one must.
    from click.testing import CliRunner

    from mareforma import signing
    from mareforma.cli import cli

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        Path("target.py").write_text(
            "with open('data.csv') as f:\n    f.read()\n", encoding="utf-8"
        )
        Path("data.csv").write_text("a,b\n1,2\n", encoding="utf-8")
        Path("findings.json").write_text(
            json.dumps({"finding-1": "data.csv"}), encoding="utf-8"
        )
        key = Path("auditor.key")
        signing.bootstrap_key(key)
        res = runner.invoke(
            cli,
            ["audit", "--findings", "findings.json", "--out", "out",
             "--key", str(key), "--", "python", "target.py"],
            catch_exceptions=False,
        )
        assert res.exit_code == 0, res.output
        receipts = [
            json.loads(line)
            for line in Path("out/receipts.jsonl").read_text().splitlines()
        ]

    assert receipts and "influence" in receipts[0]
    report = summarize_influence_receipts(receipts)
    assert report.total >= 1
    # The audit path never perturbs, so every edge is NOT_TESTED: the honest
    # state, not a fabricated influence number.
    assert report.not_tested == report.total
    assert report.resolved == 0
