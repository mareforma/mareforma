"""Multi-line evidence tree + run-distinct independence.

A finding may carry N evidence lines; each line's bearing is recomputed on read;
Status counts independence by distinct run (``generated_by``) with a dataset
guard, so one run cannot self-certify. The single-line path stays byte-compatible.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from mareforma.trust import (
    Contrast,
    ControlType,
    EvidenceLine,
    FindingPlanForkError,
    InconsistentEstimateError,
    Status,
)
from mareforma.trust._store import _count_run_distinct

from ._builders import _prop, _smd, _superiority, open_graph


def _lines(*specs):
    """(value, data_id[, p]) tuples -> EvidenceLine list, default p=0.003."""
    out = []
    for spec in specs:
        value, data_id = spec[0], spec[1]
        p = spec[2] if len(spec) > 2 else 0.003
        out.append(EvidenceLine(_smd(value, p=p), data_id))
    return out


def _read_health(root: Path) -> list[dict]:
    path = root / ".mareforma" / "health.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


class TestMultiLineWrite:
    def test_two_lines_round_trip(self, tmp_path: Path) -> None:
        h = _prop()
        with open_graph(tmp_path) as g:
            r = g.assert_finding(
                h, _superiority(), lines=_lines((-2.6, "dA"), (-2.4, "dB")),
                generated_by="run1",
            )
            rows = g._conn.execute(
                "SELECT data_id FROM evidence_lines WHERE finding_id = ? ORDER BY data_id",
                (r["finding_id"],),
            ).fetchall()
        assert [row["data_id"] for row in rows] == ["dA", "dB"]
        assert r["bearings"] == [
            {"direction": "supports", "significant": True},
            {"direction": "supports", "significant": True},
        ]

    def test_estimate_and_lines_are_mutually_exclusive(self, tmp_path: Path) -> None:
        h = _prop()
        with open_graph(tmp_path) as g:
            with pytest.raises(ValueError, match="not both"):
                g.assert_finding(
                    h, _superiority(), _smd(-2.6, p=0.003),
                    data_id="dA", lines=_lines((-2.4, "dB")), generated_by="run1",
                )

    def test_scalar_line_attrs_rejected_in_multiline(self, tmp_path: Path) -> None:
        h = _prop()
        with open_graph(tmp_path) as g:
            # control_type belongs on each EvidenceLine in multi-line mode.
            with pytest.raises(ValueError, match="per-line attributes"):
                g.assert_finding(
                    h, _superiority(), lines=_lines((-2.6, "dA")),
                    control_type=ControlType.VEHICLE, generated_by="run1",
                )


class TestF2WriteContract:
    def test_empty_line_list_rejected(self, tmp_path: Path) -> None:
        h = _prop()
        with open_graph(tmp_path) as g:
            with pytest.raises(ValueError, match="at least one evidence line"):
                g.assert_finding(h, _superiority(), lines=[], generated_by="run1")

    def test_one_bad_line_rolls_back_whole_finding(self, tmp_path: Path) -> None:
        """A single un-gateable line leaves no claim, finding, or evidence row."""
        h = _prop()
        good = EvidenceLine(_smd(-2.6, p=0.003), "dA")
        # ci_level must equal 1 - 2*alpha (=0.90); 0.50 fails the gate.
        bad = EvidenceLine(
            _smd(-2.0, ci=(-3.5, -0.5), ci_level=0.50), "dB"
        )
        with open_graph(tmp_path) as g:
            before = g._conn.execute("SELECT COUNT(*) AS c FROM claims").fetchone()["c"]
            with pytest.raises(InconsistentEstimateError):
                g.assert_finding(
                    h, _superiority(), lines=[good, bad], generated_by="run1",
                )
            after = g._conn.execute("SELECT COUNT(*) AS c FROM claims").fetchone()["c"]
            findings = g._conn.execute("SELECT COUNT(*) AS c FROM findings").fetchone()["c"]
            lines = g._conn.execute("SELECT COUNT(*) AS c FROM evidence_lines").fetchone()["c"]
        assert after == before
        assert findings == 0
        assert lines == 0

    def test_duplicate_data_id_allowed_counts_once(self, tmp_path: Path) -> None:
        """Two lines on the same dataset (e.g. distinct contrasts) are stored
        but count as one independent unit."""
        h = _prop()
        lines = [
            EvidenceLine(_smd(-2.6, p=0.003), "dA", contrast=Contrast(ControlType.NEGATIVE)),
            EvidenceLine(_smd(-2.5, p=0.004), "dA", contrast=Contrast(ControlType.VEHICLE)),
        ]
        with open_graph(tmp_path) as g:
            r = g.assert_finding(h, _superiority(), lines=lines, generated_by="run1")
            stored = g._conn.execute(
                "SELECT COUNT(*) AS c FROM evidence_lines WHERE finding_id = ?",
                (r["finding_id"],),
            ).fetchone()["c"]
            status = g.proposition_status(h)
        assert stored == 2  # both rows persisted
        assert status["independent_support"] == 1  # one dataset, one unit
        assert status["status"] == Status.PRELIMINARY.value


class TestPerLineBearing:
    def test_disagreeing_lines_count_per_line(self, tmp_path: Path) -> None:
        """A finding whose lines disagree is CONTESTED off per-line bearing, not
        the finding's denormalised cache."""
        h = _prop()
        # line A: significant decrease (supports); line B: significant increase
        # (refutes the predicted decrease).
        lines = [
            EvidenceLine(_smd(-2.6, p=0.003), "dA"),
            EvidenceLine(_smd(2.6, p=0.003), "dB"),
        ]
        with open_graph(tmp_path) as g:
            g.assert_finding(h, _superiority(), lines=lines, generated_by="run1")
            status = g.proposition_status(h)
        assert status["independent_support"] == 1
        assert status["independent_refute"] == 1
        assert status["status"] == Status.CONTESTED.value


class TestRunDistinct:
    def test_same_run_two_findings_no_self_corroborate(self, tmp_path: Path) -> None:
        h = _prop()
        with open_graph(tmp_path) as g:
            g.assert_finding(h, _superiority(), _smd(-2.6, p=0.003), data_id="dA", generated_by="run1")
            g.assert_finding(h, _superiority(), _smd(-2.4, p=0.01), data_id="dB", generated_by="run1")
            status = g.proposition_status(h)
        assert status["independent_support"] == 1
        assert status["status"] == Status.PRELIMINARY.value

    def test_cross_run_corroborates(self, tmp_path: Path) -> None:
        h = _prop()
        with open_graph(tmp_path) as g:
            g.assert_finding(h, _superiority(), _smd(-2.6, p=0.003), data_id="dA", generated_by="run1")
            g.assert_finding(h, _superiority(), _smd(-2.4, p=0.01), data_id="dB", generated_by="run2")
            status = g.proposition_status(h)
        assert status["independent_support"] == 2
        assert status["status"] == Status.CORROBORATED.value

    def test_single_multiline_finding_no_self_corroborate(self, tmp_path: Path) -> None:
        h = _prop()
        with open_graph(tmp_path) as g:
            g.assert_finding(
                h, _superiority(), lines=_lines((-2.6, "dA"), (-2.4, "dB")),
                generated_by="run1",
            )
            status = g.proposition_status(h)
        assert status["independent_support"] == 1
        assert status["status"] == Status.PRELIMINARY.value

    def test_single_run_cannot_self_contest(self, tmp_path: Path) -> None:
        """One run with a supporting and a refuting line on distinct datasets is
        1/1 (CONTESTED), not 2/2: each side caps at one per run."""
        h = _prop()
        lines = [
            EvidenceLine(_smd(-2.6, p=0.003), "dA"),
            EvidenceLine(_smd(2.6, p=0.003), "dB"),
        ]
        with open_graph(tmp_path) as g:
            g.assert_finding(h, _superiority(), lines=lines, generated_by="run1")
            status = g.proposition_status(h)
        assert (status["independent_support"], status["independent_refute"]) == (1, 1)


class TestCountRunDistinct:
    """Unit tests for the count, including the dataset-guard path the write-time
    fork-guard prevents the public API from reaching."""

    def test_same_dataset_distinct_runs_counts_once(self) -> None:
        # The data_id guard: one dataset re-run under two tokens is one unit.
        assert _count_run_distinct([("r1", "dA"), ("r2", "dA")]) == 1

    def test_distinct_runs_distinct_data_counts_each(self) -> None:
        assert _count_run_distinct([("r1", "dA"), ("r2", "dB")]) == 2

    def test_same_run_many_datasets_counts_once(self) -> None:
        assert _count_run_distinct([("r1", "dA"), ("r1", "dB"), ("r1", "dC")]) == 1

    def test_order_and_label_independent(self) -> None:
        pairs = [("zeta", "d1"), ("zeta", "d2"), ("alpha", "d1")]
        assert _count_run_distinct(pairs) == _count_run_distinct(list(reversed(pairs)))


class TestReadPathSurvivesUngateableRow:
    def test_non_numeric_stored_estimate_does_not_deny_reads(self, tmp_path: Path) -> None:
        """A row that no longer reconstructs into a gateable bearing (here a
        non-numeric estimate_value smuggled past SQLite's weak REAL affinity) is
        skipped on read, not raised: it must not deny status reads for the
        proposition or roll back unrelated writes via the frame contest."""
        h = _prop()
        with open_graph(tmp_path) as g:
            g.assert_finding(h, _superiority(), _smd(-2.6, p=0.003), data_id="dA", generated_by="run1")
            g.assert_finding(h, _superiority(), _smd(-2.4, p=0.01), data_id="dB", generated_by="run2")
            # Corrupt only run1's stored estimate to a non-numeric value (direct
            # SQL, the "foreign/direct writer" case the read-path guard exists
            # for). math.isfinite on it raises TypeError on recompute.
            g._conn.execute(
                "UPDATE effect_estimates SET estimate_value = 'CORRUPT' "
                "WHERE contrast_id IN ("
                "  SELECT c.contrast_id FROM contrasts c "
                "  JOIN evidence_lines el ON el.line_id = c.line_id "
                "  WHERE el.data_id = 'dA')"
            )
            g._conn.commit()
            status = g.proposition_status(h)  # must not raise
        assert status is not None
        # The poisoned row is skipped; the valid run2/dB line still counts.
        assert status["independent_support"] == 1
        assert status["status"] == Status.PRELIMINARY.value


class TestGeneratedByPrecondition:
    def test_blank_token_rejected(self, tmp_path: Path) -> None:
        h = _prop()
        with open_graph(tmp_path) as g:
            with pytest.raises(ValueError, match="non-empty run token"):
                g.assert_finding(h, _superiority(), _smd(-2.6, p=0.003), data_id="dA", generated_by="   ")

    def test_default_token_emits_health_event(self, tmp_path: Path) -> None:
        h = _prop()
        with open_graph(tmp_path) as g:
            g.assert_finding(h, _superiority(), _smd(-2.6, p=0.003), data_id="dA")
        events = _read_health(tmp_path)
        assert any(e.get("generated_by_default") for e in events)

    def test_real_token_no_default_event(self, tmp_path: Path) -> None:
        h = _prop()
        with open_graph(tmp_path) as g:
            g.assert_finding(h, _superiority(), _smd(-2.6, p=0.003), data_id="dA", generated_by="run1")
        events = _read_health(tmp_path)
        assert not any(e.get("generated_by_default") for e in events)


class TestMultiLineIdempotencyAndFork:
    def test_same_set_same_plan_is_idempotent(self, tmp_path: Path) -> None:
        h = _prop()
        with open_graph(tmp_path) as g:
            r1 = g.assert_finding(h, _superiority(), lines=_lines((-2.6, "dA"), (-2.4, "dB")), generated_by="run1")
            r2 = g.assert_finding(h, _superiority(), lines=_lines((-2.6, "dA"), (-2.4, "dB")), generated_by="run1")
        assert r2["idempotent"] is True
        assert r2["finding_id"] == r1["finding_id"]

    def test_partial_overlap_forks(self, tmp_path: Path) -> None:
        h = _prop()
        with open_graph(tmp_path) as g:
            g.assert_finding(h, _superiority(), lines=_lines((-2.6, "dA"), (-2.4, "dB")), generated_by="run1")
            with pytest.raises(FindingPlanForkError):
                g.assert_finding(h, _superiority(), lines=_lines((-2.6, "dA"), (-2.0, "dC")), generated_by="run1")


class TestSingleLineParity:
    def test_distinct_run_single_lines_still_corroborate(self, tmp_path: Path) -> None:
        """Findings from distinct runs still corroborate (prior behaviour kept)."""
        h = _prop()
        with open_graph(tmp_path) as g:
            g.assert_finding(h, _superiority(), _smd(-2.6, p=0.003), data_id="dataA", generated_by="lab_a")
            g.assert_finding(h, _superiority(), _smd(-2.4, p=0.01), data_id="dataB", generated_by="lab_b")
            status = g.proposition_status(h)
        assert status["status"] == Status.CORROBORATED.value
