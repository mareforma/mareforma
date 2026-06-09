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
    def test_additional_uris_registered(self) -> None:
        # 3 core + 18 reserved (17 adapter slots + the wet-lab-assay
        # umbrella parent) = 21 total. Adapter ecosystem coverage
        # rather than a fixed count is the durable invariant.
        assert len(_pt.BUILTIN_URIS) >= 20
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

    def test_core_builtin_uris_cannot_be_re_registered(self) -> None:
        # Core mareforma-owned URIs always raise.
        for uri in _pt._CORE_BUILTIN_URIS:
            with pytest.raises(mareforma.PredicateTypeError):
                mareforma.register_predicate(uri, owner="adapter-x")

    def test_newly_reserved_uris_emit_deprecation_warning(self) -> None:
        # Reserved adapter URIs (the 17 added beyond the core three)
        # downgrade re-registration from raise to DeprecationWarning
        # so adapters that registered them pre-promotion don't
        # silently break on pip-install -U.
        non_core = [
            u for u in _pt.BUILTIN_URIS if u not in _pt._CORE_BUILTIN_URIS
        ]
        for uri in non_core[:3]:  # spot-check; behaviour is uniform
            with pytest.warns(DeprecationWarning, match="core-reserved"):
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
        # The torn / corrupt line is now counted explicitly so the
        # operator's stats CLI surfaces drift instead of swallowing
        # it silently.
        assert stats["malformed_lines"] == 1

    def test_write_failure_does_not_raise(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        # Patch Path.open to raise OSError on the health log; the
        # graph must swallow via RuntimeWarning rather than
        # crashing the upstream op.
        real_open = Path.open

        def _failing(self, *args, **kwargs):
            if self.name == "health.jsonl":
                raise OSError("disk full")
            return real_open(self, *args, **kwargs)

        monkeypatch.setattr(Path, "open", _failing)
        with pytest.warns(RuntimeWarning, match="health log"):
            _health.append_health_event(tmp_path, "op")

    def test_non_json_encodable_counter_does_not_propagate(
        self, tmp_path: Path,
    ) -> None:
        # A counter value like a set / datetime / bytes raises
        # TypeError from json.dumps. The graph must catch and
        # warn instead of letting the TypeError bubble into the
        # upstream operation's grounding-sensor handler.
        import datetime
        with pytest.warns(RuntimeWarning, match="health log"):
            _health.append_health_event(
                tmp_path, "test",
                bad_counter=datetime.datetime.now(),
            )
        # No file written because dumps failed.
        path = tmp_path / ".mareforma" / "health.jsonl"
        assert not path.exists() or path.read_text() == ""

    def test_nan_counter_does_not_produce_nonportable_jsonl(
        self, tmp_path: Path,
    ) -> None:
        # NaN counters MUST NOT land in the file (json.dumps default
        # would write `NaN` which is invalid JSON for jq / browsers).
        with pytest.warns(RuntimeWarning, match="health log"):
            _health.append_health_event(
                tmp_path, "test", score=float("nan"),
            )
        path = tmp_path / ".mareforma" / "health.jsonl"
        assert not path.exists() or "NaN" not in path.read_text()

    def test_compute_rolling_stats_bounded_memory_with_last_n(
        self, tmp_path: Path,
    ) -> None:
        # A 10k-event log read with last_n=5 should produce 5 events.
        # The deque-based bounded-read keeps memory O(last_n).
        for i in range(10000):
            _health.append_health_event(tmp_path, "tick", i=i)
        stats = _health.compute_rolling_stats(tmp_path, last_n=5)
        assert stats["events_total"] == 5


class TestStatsCliReadError:
    def test_stats_exits_nonzero_on_read_error(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        # Force compute_rolling_stats to return read_error=True by
        # making the log file unreadable via Path.open monkeypatch.
        (tmp_path / ".mareforma").mkdir()
        (tmp_path / ".mareforma" / "health.jsonl").write_text(
            '{"op":"x","outcome":"ok"}\n'
        )
        real_open = Path.open

        def _failing(self, *args, **kwargs):
            if self.name == "health.jsonl":
                raise OSError("permission denied")
            return real_open(self, *args, **kwargs)

        monkeypatch.setattr(Path, "open", _failing)
        runner = CliRunner()
        result = runner.invoke(_cli, ["stats"])
        assert result.exit_code == 1


class TestDoiDriftEmitsTotalInspected:
    def test_emission_carries_total_inspected_counter(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from mareforma import doi_resolver as _doi
        with mareforma.open(tmp_path) as graph:
            conn = graph._conn
            for i in range(3):
                conn.execute(
                    "INSERT INTO doi_cache (doi, resolved, registry, "
                    "last_checked_at, content_digest) VALUES "
                    "(?, 1, 'crossref', ?, ?)",
                    (f"10.1234/em-{i}", "2026-01-01T00:00:00+00:00", "old"),
                )
            conn.commit()
            monkeypatch.setattr(
                _doi, "fetch_doi_metadata",
                lambda doi, timeout=5.0, registry=None: (
                    {"title": ["X"]}, "crossref", False,
                ),
            )
            graph.find_drifted_dois()
        stats = _health.compute_rolling_stats(tmp_path)
        drift = stats["ops"]["doi_drift_scan"]
        assert drift["count"] == 1
        # total_inspected is now a real aggregated stat, not just
        # documented.
        assert drift["total_inspected"] == 3


class TestPredicateRegistryBackwardsCompat:
    def setup_method(self) -> None:
        self._snapshot = dict(_pt._registry)

    def teardown_method(self) -> None:
        _pt._registry.clear()
        _pt._registry.update(self._snapshot)

    def test_core_builtin_re_register_still_raises(self) -> None:
        with pytest.raises(mareforma.PredicateTypeError):
            mareforma.register_predicate(
                "urn:mareforma:predicate:claim:v1",
                owner="rogue",
            )

    def test_newly_reserved_builtin_re_register_deprecation(self) -> None:
        # Adapter that registered tool-call:v1 before promotion gets
        # a DeprecationWarning, not a hard break, so pip-install -U
        # doesn't surprise downstream users mid-release.
        with pytest.warns(DeprecationWarning, match="core-reserved"):
            mareforma.register_predicate(
                "urn:mareforma:predicate:tool-call:v1",
                owner="legacy-adapter",
            )


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

    def test_grounding_sensor_failure_emits_fail_event(
        self, tmp_path: Path,
    ) -> None:
        # A failed sensor emits an outcome=fail event so rolling
        # stats can compute availability = ok/(ok+fail) alongside
        # pass_rate; otherwise the operator never sees flaky sensors.
        class _Broken:
            def grounding_score(self, c, s):
                raise OSError("model missing")

        with mareforma.open(tmp_path) as graph:
            with pytest.warns(RuntimeWarning):
                graph.assert_claim("x", grounding_sensor=_Broken())
        stats = _health.compute_rolling_stats(tmp_path)
        gv = stats["ops"]["grounding_verdict"]
        assert gv["count"] == 1
        assert gv["fail"] == 1
        assert gv["ok"] == 0
        # No avg_score/pass_rate because no score sample landed.
        assert "avg_score" not in gv
        assert "pass_rate" not in gv


# ----------------------------------------------------------------------------
# `mareforma stats` CLI
# ----------------------------------------------------------------------------


class TestActivityCommand:
    def test_activity_empty_project(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(_cli, ["activity"])
        assert result.exit_code == 0
        assert "Events scanned: 0" in result.output

    def test_activity_after_provenance_query(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        # The CLI's _root() reads Path.cwd(); chdir to the project.
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            graph.query_provenance(a)
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(_cli, ["activity"])
        assert result.exit_code == 0
        assert "provenance_query" in result.output

    def test_activity_json_mode(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            graph.query_provenance(a)
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(_cli, ["activity", "--json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["events_total"] >= 1
        assert "provenance_query" in parsed["ops"]


class TestStatsDeprecationAlias:
    """`mareforma stats` is kept as a deprecation alias for one
    release. It must delegate to `mareforma activity` and emit a
    DeprecationWarning. v0.4 will remove the alias entirely."""

    def test_stats_alias_still_works(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            graph.query_provenance(a)
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(_cli, ["stats"])
        # The alias still produces the same output as the renamed cmd.
        assert result.exit_code == 0
        assert "provenance_query" in result.output

    def test_stats_alias_emits_deprecation_warning(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        import warnings
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        # CliRunner swallows warnings by default; catch them around the
        # invoke so we can assert on the deprecation.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            runner.invoke(_cli, ["stats"])
        dep = [
            w for w in caught
            if issubclass(w.category, DeprecationWarning)
            and "renamed to `mareforma activity`" in str(w.message)
        ]
        assert dep, (
            "expected DeprecationWarning about the stats→activity "
            f"rename; got {[str(w.message) for w in caught]}"
        )

    def test_stats_hidden_from_top_level_help(self) -> None:
        runner = CliRunner()
        result = runner.invoke(_cli, ["--help"])
        # The renamed `activity` command must appear; the deprecated
        # `stats` alias must NOT (it's hidden so first-time users
        # discover the right name).
        assert "activity" in result.output
        # `stats` appearing as a substring of "Statistics" or similar
        # would be a false positive; check the discrete word boundary.
        # Click's --help formats commands one-per-line as "  name  Description"
        assert "\n  stats " not in result.output
