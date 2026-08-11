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
    assert report.decided == 1
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
    assert report.decided == 0
    assert report.tested == 0


# -- the reported line names every state it came from -----------------------

def test_undecidable_is_not_counted_as_an_edge_the_oracle_decided():
    # UNDECIDABLE is the oracle saying it could not decide, and it is the EXPECTED
    # verdict for any statistic that is a provable invariant of a valid null (a
    # mean under a permutation). Counting it as decided put the non-answer in the
    # denominator of an answer, so a corpus the oracle refused to call and a
    # corpus it called hollow printed the same sentence.
    undecided = summarize_influence([_edge("UNDECIDABLE", runs=5) for _ in range(8)])
    hollow = summarize_influence([_edge("NOT_INFLUENCED", runs=5) for _ in range(8)])
    assert undecided.decided == 0
    assert undecided.tested == 8
    assert hollow.decided == 8
    assert undecided.closing_sentence() != hollow.closing_sentence()
    assert "decided none of them" in undecided.closing_sentence()
    assert "no influence prevalence is claimed" in undecided.closing_sentence()


def test_a_rate_names_the_undecided_edges_it_excluded():
    records = [_edge("INFLUENCED", runs=2), _edge("NOT_INFLUENCED", runs=2),
               _edge("UNDECIDABLE", runs=2)]
    sentence = summarize_influence(records).closing_sentence()
    assert "50% INFLUENCED" in sentence and "50% NOT_INFLUENCED" in sentence
    assert "1 edges UNDECIDABLE" in sentence
    assert "excluded from the rate" in sentence


def test_a_rate_with_no_run_count_says_the_count_is_missing():
    # The arm's stated guarantee is that a rate never prints without the run
    # count behind it. Printing "over 0 distinct runs" claims a rate stands on
    # nothing, which is a stronger and falser statement than not knowing.
    sentence = summarize_influence([_edge("INFLUENCED")]).closing_sentence()
    assert "over 0 distinct runs" not in sentence
    assert "run count not recorded" in sentence


def test_a_malformed_verdict_cannot_launder_itself_into_a_real_reason():
    # The verdict is judged before the record's own reason is trusted. Otherwise a
    # record with an unrecognized verdict and a well-formed reason string was
    # filed under that reason and read as a legitimate not-tested state.
    report = summarize_influence([
        {"influence": "GARBAGE", "not_tested_reason": "unsupported-shape"},
        {"influence": "NOT_TESTED", "not_tested_reason": "invented-by-a-writer"},
        {"influence": "NOT_TESTED", "not_tested_reason": "unsupported-shape"},
    ])
    assert report.not_tested_by_reason == {"unknown": 2, "unsupported-shape": 1}


def test_to_dict_carries_the_keys_the_cli_reads():
    # The CLI indexes these by name, so a rename ships as a KeyError in
    # `mareforma measure` rather than a test failure.
    payload = summarize_influence([_edge("INFLUENCED", runs=2)]).to_dict()
    for key in ("total", "decided", "tested", "counts", "not_tested_by_reason",
                "not_tested_dominates", "distinct_runs", "influenced_fraction"):
        assert key in payload
    assert set(payload["counts"]) == {
        "INFLUENCED", "NOT_INFLUENCED", "UNDECIDABLE", "NOT_TESTED"
    }


def test_the_pilot_carries_the_influence_arm_when_receipts_have_one():
    from mareforma.observe.measure import summarize_pilot

    receipts = [{
        "grounding": "GROUNDED", "reads": [], "cited_sources": [],
        "influence": [_edge("INFLUENCED", runs=2)],
    }]
    pilot = summarize_pilot(receipts)
    assert pilot.influence.total == 1
    assert pilot.to_dict()["influence"] is not None
    assert "INFLUENCED" in pilot.closing_sentence()
    # And a pilot without the arm reports exactly as it did before it existed.
    bare = summarize_pilot([{"grounding": "GROUNDED", "reads": [],
                             "cited_sources": []}])
    assert bare.influence.total == 0
    assert bare.to_dict()["influence"] is None


def test_audit_writes_one_edge_per_cited_source(tmp_path: Path):
    # The arm's unit is the EDGE, so a finding citing two sources must contribute
    # two records. A writer regressing to one record per finding would pass every
    # hand-authored flattening test in this file and silently halve the arm.
    from click.testing import CliRunner

    from mareforma import signing
    from mareforma.cli import cli
    from mareforma.observe.oracle import THREAT_MODEL_STATEMENT

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        Path("target.py").write_text(
            "with open('a.csv') as f:\n    f.read()\n"
            "with open('b.csv') as f:\n    f.read()\n",
            encoding="utf-8",
        )
        Path("a.csv").write_text("a\n1\n", encoding="utf-8")
        Path("b.csv").write_text("b\n2\n", encoding="utf-8")
        Path("findings.json").write_text(
            json.dumps({"finding-1": ["a.csv", "b.csv"]}), encoding="utf-8"
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

    edges = influence_records(receipts)
    assert len(edges) == 2
    assert len({e["cited_source"] for e in edges}) == 2
    # The bound the record makes its claim under travels with the record.
    assert all(THREAT_MODEL_STATEMENT in e["reason"] for e in edges)
