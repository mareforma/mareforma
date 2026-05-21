"""Tests for the additional predicate URIs, health.jsonl event log,
and `mareforma stats` CLI."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

import mareforma
from mareforma import health as _health
from mareforma import predicate_types as _pt
from mareforma.cli import cli as _cli


# ----------------------------------------------------------------------------
# Additional predicate URIs
# ----------------------------------------------------------------------------


class TestExpandedBuiltinUris:
    def test_seventeen_additional_uris_registered(self) -> None:
        # 3 core + 17 reserved adapter URIs = 20 total.
        assert len(_pt.BUILTIN_URIS) == 20
        for uri in _pt.BUILTIN_URIS:
            assert mareforma.is_registered(uri)

    def test_wet_lab_assay_subhierarchy_present(self) -> None:
        expected_subhierarchy = {
            "flow-cytometry", "sequencing", "imaging",
            "proteomics", "electrophysiology",
        }
        seen = {
            uri.split(":")[-2].split("/")[-1]
            for uri in _pt.BUILTIN_URIS
            if "wet-lab-assay" in uri
        }
        assert expected_subhierarchy <= seen

    def test_builtin_uris_cannot_be_re_registered(self) -> None:
        for uri in _pt.BUILTIN_URIS[3:]:  # skip the core three
            with pytest.raises(mareforma.PredicateTypeError):
                mareforma.register_predicate(uri, owner="adapter-x")


# ----------------------------------------------------------------------------
# health.jsonl append + compute_rolling_stats
# ----------------------------------------------------------------------------


class TestHealthEventLog:
    def test_append_creates_file_and_writes_line(self, tmp_path: Path) -> None:
        _health.append_health_event(tmp_path, "test_op", outcome="ok", n=1)
        path = tmp_path / ".mareforma" / "health.jsonl"
        assert path.exists()
        lines = path.read_text().splitlines()
        assert len(lines) == 1
        event = json.loads(lines[0])
        assert event["op"] == "test_op"
        assert event["outcome"] == "ok"
        assert event["n"] == 1
        assert "ts" in event

    def test_append_is_additive(self, tmp_path: Path) -> None:
        for i in range(5):
            _health.append_health_event(tmp_path, "tick", i=i)
        path = tmp_path / ".mareforma" / "health.jsonl"
        lines = path.read_text().splitlines()
        assert len(lines) == 5

    def test_compute_rolling_stats_empty(self, tmp_path: Path) -> None:
        stats = _health.compute_rolling_stats(tmp_path)
        assert stats == {"events_total": 0, "ops": {}}

    def test_compute_rolling_stats_aggregates(self, tmp_path: Path) -> None:
        _health.append_health_event(tmp_path, "grounding_verdict", score=0.9)
        _health.append_health_event(tmp_path, "grounding_verdict", score=0.4)
        _health.append_health_event(tmp_path, "grounding_verdict", score=0.8)
        _health.append_health_event(tmp_path, "provenance_query", depth=4)
        _health.append_health_event(tmp_path, "doi_drift_scan", drifted=2)
        stats = _health.compute_rolling_stats(tmp_path)
        assert stats["events_total"] == 5
        gv = stats["ops"]["grounding_verdict"]
        assert gv["count"] == 3
        assert gv["avg_score"] == round((0.9 + 0.4 + 0.8) / 3, 3)
        assert gv["pass_rate"] == round(2 / 3, 3)
        assert stats["ops"]["provenance_query"]["avg_depth"] == 4.0
        assert stats["ops"]["doi_drift_scan"]["avg_drifted"] == 2.0

    def test_compute_rolling_stats_last_n(self, tmp_path: Path) -> None:
        for i in range(10):
            _health.append_health_event(
                tmp_path, "tick", outcome="ok",
            )
        stats = _health.compute_rolling_stats(tmp_path, last_n=3)
        assert stats["events_total"] == 3

    def test_malformed_lines_skipped(self, tmp_path: Path) -> None:
        path = tmp_path / ".mareforma" / "health.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            '{"op":"good","outcome":"ok"}\n'
            "not json at all\n"
            '{"op":"alsogood","outcome":"ok"}\n'
        )
        stats = _health.compute_rolling_stats(tmp_path)
        assert stats["events_total"] == 2

    def test_write_failure_does_not_raise(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        # Patch Path.open to raise OSError on the health log; the
        # substrate must swallow via RuntimeWarning rather than
        # crashing the upstream op.
        real_open = Path.open

        def _failing(self, *args, **kwargs):
            if self.name == "health.jsonl":
                raise OSError("disk full")
            return real_open(self, *args, **kwargs)

        monkeypatch.setattr(Path, "open", _failing)
        with pytest.warns(RuntimeWarning, match="health log"):
            _health.append_health_event(tmp_path, "op")


# ----------------------------------------------------------------------------
# Graph operations emit events
# ----------------------------------------------------------------------------


class TestGraphEmitsHealthEvents:
    def test_provenance_query_emits_event(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            graph.query_provenance(a, depth=2)
        stats = _health.compute_rolling_stats(tmp_path)
        assert stats["ops"]["provenance_query"]["count"] == 1
        assert stats["ops"]["provenance_query"]["avg_depth"] == 2.0

    def test_grounding_verdict_emits_event(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            graph.assert_claim(
                "x",
                grounding_sensor=mareforma.MockNLIVerifier(
                    score=0.85, rationale="ok",
                ),
            )
        stats = _health.compute_rolling_stats(tmp_path)
        assert stats["ops"]["grounding_verdict"]["count"] == 1
        assert stats["ops"]["grounding_verdict"]["avg_score"] == 0.85
        assert stats["ops"]["grounding_verdict"]["pass_rate"] == 1.0

    def test_grounding_sensor_failure_does_not_emit_event(
        self, tmp_path: Path,
    ) -> None:
        class _Broken:
            def grounding_score(self, c, s):
                raise OSError("model missing")

        with mareforma.open(tmp_path) as graph:
            with pytest.warns(RuntimeWarning):
                graph.assert_claim("x", grounding_sensor=_Broken())
        stats = _health.compute_rolling_stats(tmp_path)
        # No event written because the sensor never produced a score.
        assert "grounding_verdict" not in stats["ops"]


# ----------------------------------------------------------------------------
# `mareforma stats` CLI
# ----------------------------------------------------------------------------


class TestStatsCommand:
    def test_stats_empty_project(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(_cli, ["status"])
            # status_cmd needs a bootstrapped project — skip the
            # initialisation by going straight to stats.
            result = runner.invoke(_cli, ["stats"])
        assert result.exit_code == 0
        assert "Events scanned: 0" in result.output

    def test_stats_after_provenance_query(self, tmp_path: Path) -> None:
        # Build a project with events, then run the CLI.
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            graph.query_provenance(a)
        runner = CliRunner()
        result = runner.invoke(
            _cli, ["stats"], env={"MAREFORMA_PROJECT": str(tmp_path)},
        )
        # The CLI's _root() uses cwd by default; chdir.
        import os
        os.chdir(tmp_path)
        result = runner.invoke(_cli, ["stats"])
        assert result.exit_code == 0
        assert "provenance_query" in result.output

    def test_stats_json_mode(self, tmp_path: Path) -> None:
        import os
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            graph.query_provenance(a)
        os.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(_cli, ["stats", "--json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["events_total"] >= 1
        assert "provenance_query" in parsed["ops"]
