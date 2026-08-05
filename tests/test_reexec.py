"""Tests for the re-execution faithfulness proxy (``mareforma reexec``).

The proxy re-runs a recorded pipeline and checks the reported number reproduces,
three-valued: REPRODUCED / DIVERGED / COULD_NOT_REEXECUTE. These pin the load-
bearing honesty rule, a run that cannot be re-executed (declared
non-reexecutable, unresolvable, raising, or returning a non-number) is
COULD_NOT_REEXECUTE, never a false REPRODUCED and never a spurious DIVERGED, and
the deterministic reproduce / perturb-to-diverge / tolerance paths.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

import mareforma
from mareforma.cli import cli
from mareforma.reexec import (
    DEFAULT_ABS_TOLERANCE,
    FaithfulnessVerdict,
    MalformedRunError,
    ReexecResult,
    reexec,
)

# A deterministic pipeline resolvable by dotted path (``module:attr``), so the
# real import-based resolution path is exercised, not only the injected registry.
_RECORDED_NUMBER = 0.4200


def deterministic_pipeline() -> float:
    """A pure, deterministic pipeline: always returns the recorded number."""
    return _RECORDED_NUMBER


def _run(**overrides) -> dict:
    """A minimal well-formed run record with sane defaults."""
    base = {
        "run_id": "run-0001",
        "reported_value": 0.5,
        "pipeline": {"target": "pipe"},
    }
    base.update(overrides)
    return base


class TestDeterministicReproduces:
    def test_reexec_deterministic_reproduces(self) -> None:
        # A deterministic recorded pipeline, re-run, matches the recorded number.
        result = reexec(
            _run(reported_value=0.5), registry={"pipe": lambda: 0.5}
        )
        assert result.verdict is FaithfulnessVerdict.REPRODUCED
        assert result.reproduced is True
        assert result.reproduced_value == 0.5
        assert result.run_id == "run-0001"

    def test_reproduces_via_dotted_path(self) -> None:
        # The real import-based resolution path, not the injected registry.
        result = reexec(
            _run(
                reported_value=_RECORDED_NUMBER,
                pipeline={"target": "tests.test_reexec:deterministic_pipeline"},
            )
        )
        assert result.verdict is FaithfulnessVerdict.REPRODUCED

    def test_reproduces_within_declared_tolerance(self) -> None:
        # A near-but-not-exact number reproduces only within the DECLARED slack.
        run = _run(reported_value=1.0, tolerance=0.01)
        assert reexec(run, registry={"pipe": lambda: 1.005}).verdict is (
            FaithfulnessVerdict.REPRODUCED
        )
        # Just outside the declared tolerance diverges, the bound is honored.
        assert reexec(run, registry={"pipe": lambda: 1.05}).verdict is (
            FaithfulnessVerdict.DIVERGED
        )

    def test_default_tolerance_is_exact(self) -> None:
        # With no declared tolerance the match is exact, nothing silently slack.
        assert DEFAULT_ABS_TOLERANCE == 0.0
        run = _run(reported_value=1.0)
        assert reexec(run, registry={"pipe": lambda: 1.0}).verdict is (
            FaithfulnessVerdict.REPRODUCED
        )
        assert reexec(run, registry={"pipe": lambda: 1.0 + 1e-9}).verdict is (
            FaithfulnessVerdict.DIVERGED
        )


class TestDiverged:
    def test_reexec_diverged(self) -> None:
        # Perturb the recorded output: the re-run produces a different number.
        result = reexec(
            _run(reported_value=0.5), registry={"pipe": lambda: 0.7}
        )
        assert result.verdict is FaithfulnessVerdict.DIVERGED
        assert result.reproduced is False
        assert result.recorded_value == 0.5
        assert result.reproduced_value == 0.7


class TestCouldNotReexecute:
    @pytest.mark.parametrize(
        "reason", ["world_contact", "private_data", "expensive_compute"]
    )
    def test_reexec_could_not_reexecute(self, reason: str) -> None:
        # A run declared non-reexecutable is NEVER re-run and NEVER REPRODUCED.
        result = reexec(
            _run(reexecutable=False, not_reexecutable_reason=reason, pipeline=None)
        )
        assert result.verdict is FaithfulnessVerdict.COULD_NOT_REEXECUTE
        assert result.reproduced is False
        assert result.reproduced_value is None
        assert reason in result.residual

    def test_raise_is_could_not_not_diverged(self) -> None:
        # A re-execution that RAISES is could-not, never a spurious DIVERGED, a
        # failed re-run is not evidence that the number changed.
        def boom() -> float:
            raise RuntimeError("pipeline blew up")

        result = reexec(_run(reported_value=0.5), registry={"pipe": boom})
        assert result.verdict is FaithfulnessVerdict.COULD_NOT_REEXECUTE
        assert "raised" in result.residual

    def test_unresolvable_target_is_could_not(self) -> None:
        # A dotted path that does not import is could-not, not a crash.
        result = reexec(
            _run(pipeline={"target": "no.such.module:fn"})
        )
        assert result.verdict is FaithfulnessVerdict.COULD_NOT_REEXECUTE
        assert "could not be resolved" in result.residual

    def test_import_time_raise_is_could_not(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        # A target module whose top-level code raises an exception outside the
        # narrow resolution tuple (here RuntimeError) is could-not, not a crash
        # that escapes the never-raises contract.
        (tmp_path / "badmod_probe.py").write_text(
            'raise RuntimeError("boom at import")\n'
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        result = reexec(
            {"reported_value": 1.0, "pipeline": {"target": "badmod_probe:fn"}}
        )
        assert result.verdict is FaithfulnessVerdict.COULD_NOT_REEXECUTE
        assert "could not be resolved" in result.residual
        assert "boom at import" in result.residual

    def test_non_numeric_result_is_could_not(self) -> None:
        # A pipeline that returns a non-number gives no number to compare.
        result = reexec(
            _run(reported_value=0.5), registry={"pipe": lambda: "not a number"}
        )
        assert result.verdict is FaithfulnessVerdict.COULD_NOT_REEXECUTE
        assert "non-numeric" in result.residual

    def test_missing_pipeline_is_could_not(self) -> None:
        # Re-executable but with nothing recorded to run: could-not, not a crash.
        result = reexec(_run(pipeline=None))
        assert result.verdict is FaithfulnessVerdict.COULD_NOT_REEXECUTE

    def test_bool_result_is_not_a_number(self) -> None:
        # bool is int in Python; a True must not compare equal to a recorded 1.0.
        result = reexec(
            _run(reported_value=1.0), registry={"pipe": lambda: True}
        )
        assert result.verdict is FaithfulnessVerdict.COULD_NOT_REEXECUTE


class TestMalformedRecord:
    def test_missing_reported_value_raises(self) -> None:
        with pytest.raises(MalformedRunError):
            reexec({"pipeline": {"target": "pipe"}})

    def test_non_numeric_reported_value_raises(self) -> None:
        with pytest.raises(MalformedRunError):
            reexec({"reported_value": "high", "pipeline": {"target": "pipe"}})

    def test_negative_tolerance_raises(self) -> None:
        with pytest.raises(MalformedRunError):
            reexec(_run(tolerance=-0.1))

    def test_unknown_non_reexecutable_reason_raises(self) -> None:
        with pytest.raises(MalformedRunError):
            reexec(_run(reexecutable=False, not_reexecutable_reason="just because"))

    def test_non_record_input_raises(self) -> None:
        with pytest.raises(MalformedRunError):
            reexec(42)


class TestLoadFromFile:
    def test_reexec_reads_a_json_run_record(self, tmp_path: Path) -> None:
        run = _run(
            reported_value=_RECORDED_NUMBER,
            pipeline={"target": "tests.test_reexec:deterministic_pipeline"},
        )
        path = tmp_path / "run.json"
        path.write_text(json.dumps(run), encoding="utf-8")
        result = reexec(path)
        assert result.verdict is FaithfulnessVerdict.REPRODUCED

    def test_bad_json_file_raises_malformed(self, tmp_path: Path) -> None:
        path = tmp_path / "run.json"
        path.write_text("{not json", encoding="utf-8")
        with pytest.raises(MalformedRunError):
            reexec(path)

    def test_non_object_json_file_raises_malformed(self, tmp_path: Path) -> None:
        # A file whose JSON parses to a list, string, or number is well-formed
        # JSON but not a run record. It must reach the same malformed path as
        # unparseable text, not a raw AttributeError from a later .get().
        for body in ("[1, 2, 3]", '"hello"', "42", "null"):
            path = tmp_path / "run.json"
            path.write_text(body, encoding="utf-8")
            with pytest.raises(MalformedRunError):
                reexec(path)

    def test_cli_non_object_json_exits_malformed(self, tmp_path: Path) -> None:
        path = tmp_path / "run.json"
        path.write_text("[1, 2, 3]", encoding="utf-8")
        result = CliRunner().invoke(cli, ["reexec", str(path)])
        assert result.exit_code == 3


class TestResultShape:
    def test_to_dict_and_map_record_round_trip(self) -> None:
        result = reexec(_run(reported_value=0.5), registry={"pipe": lambda: 0.5})
        assert isinstance(result, ReexecResult)
        d = result.to_dict()
        assert d["verdict"] == "REPRODUCED"
        assert d["recorded_value"] == 0.5
        rec = result.to_map_record()
        assert rec["verdict"] == "REPRODUCED"
        # The residual names both bounds the proxy does not cross.
        assert "not correct" in rec["residual"]
        assert "independent" in rec["residual"]

    def test_residual_names_the_tolerance(self) -> None:
        # A conclusive residual states the tolerance the comparison used, so a
        # match obtained via a generous tolerance is never silent.
        result = reexec(
            _run(reported_value=0.5, tolerance=0.02),
            registry={"pipe": lambda: 0.5},
        )
        assert "abs=0.02" in result.residual


class TestWideToleranceIsFlagged:
    def test_wide_absolute_tolerance_flags_a_weak_match(self) -> None:
        # A tolerance as large as the recorded magnitude makes almost any number
        # "reproduce"; the verdict stays REPRODUCED (the recorder declared it) but
        # is flagged, so a wide tolerance can never fake a clean match silently.
        result = reexec(
            _run(reported_value=5.0, tolerance=1e308),
            registry={"pipe": lambda: 999999.0},
        )
        assert result.verdict is FaithfulnessVerdict.REPRODUCED
        assert "WARNING" in result.residual
        assert "wide" in result.residual

    def test_wide_absolute_tolerance_around_zero_is_flagged(self) -> None:
        # A recorded zero (no effect, null result) is where a generous absolute
        # tolerance makes every number "reproduce", so it has to be flagged too.
        result = reexec(
            _run(reported_value=0.0, tolerance=1e6),
            registry={"pipe": lambda: 999999.0},
        )
        assert result.verdict is FaithfulnessVerdict.REPRODUCED
        assert "WARNING" in result.residual

    def test_slack_over_the_recorded_magnitude_is_flagged(self) -> None:
        # Wideness is judged against the recorded magnitude, not against the
        # number the re-run happened to produce: a slack larger than what was
        # recorded is wide even when the re-run lands far above it.
        result = reexec(
            _run(reported_value=5.0, tolerance=6.0),
            registry={"pipe": lambda: 11.0},
        )
        assert result.verdict is FaithfulnessVerdict.REPRODUCED
        assert "WARNING" in result.residual

    def test_narrow_tolerance_is_not_flagged(self) -> None:
        result = reexec(
            _run(reported_value=5.0, tolerance=0.001),
            registry={"pipe": lambda: 5.0},
        )
        assert result.verdict is FaithfulnessVerdict.REPRODUCED
        assert "WARNING" not in result.residual

    def test_relative_tolerance_over_one_is_flagged(self) -> None:
        result = reexec(
            _run(reported_value=5.0, rel_tolerance=1.5),
            registry={"pipe": lambda: 12.0},
        )
        assert result.verdict is FaithfulnessVerdict.REPRODUCED
        assert "WARNING" in result.residual


class TestCli:
    def test_reproduced_exits_zero(self, tmp_path: Path) -> None:
        run = _run(
            reported_value=_RECORDED_NUMBER,
            pipeline={"target": "tests.test_reexec:deterministic_pipeline"},
        )
        path = tmp_path / "run.json"
        path.write_text(json.dumps(run), encoding="utf-8")
        res = CliRunner().invoke(cli, ["reexec", str(path)])
        assert res.exit_code == 0, res.output
        assert "REPRODUCED" in res.output

    def test_diverged_exits_one(self, tmp_path: Path) -> None:
        # A recorded number the deterministic pipeline will not reproduce.
        run = _run(
            reported_value=999.0,
            pipeline={"target": "tests.test_reexec:deterministic_pipeline"},
        )
        path = tmp_path / "run.json"
        path.write_text(json.dumps(run), encoding="utf-8")
        res = CliRunner().invoke(cli, ["reexec", str(path)])
        assert res.exit_code == 1, res.output
        assert "DIVERGED" in res.output

    def test_could_not_reexecute_exits_two(self, tmp_path: Path) -> None:
        run = _run(
            reexecutable=False, not_reexecutable_reason="private_data", pipeline=None
        )
        path = tmp_path / "run.json"
        path.write_text(json.dumps(run), encoding="utf-8")
        res = CliRunner().invoke(cli, ["reexec", str(path)])
        assert res.exit_code == 2, res.output
        assert "COULD_NOT_REEXECUTE" in res.output
        # No reproduced number exists; say so with the same placeholder every
        # other renderer uses.
        assert "reproduced: n/a" in res.output

    def test_json_output(self, tmp_path: Path) -> None:
        run = _run(
            reported_value=_RECORDED_NUMBER,
            pipeline={"target": "tests.test_reexec:deterministic_pipeline"},
        )
        path = tmp_path / "run.json"
        path.write_text(json.dumps(run), encoding="utf-8")
        res = CliRunner().invoke(cli, ["reexec", str(path), "--json"])
        assert res.exit_code == 0, res.output
        doc = json.loads(res.output)
        assert doc["verdict"] == "REPRODUCED"
        assert doc["recorded_value"] == _RECORDED_NUMBER

    def test_malformed_record_exits_three_with_message(self, tmp_path: Path) -> None:
        # A malformed record is a usage error (exit 3), distinct from an honest
        # COULD_NOT_REEXECUTE (exit 2), so a script can tell them apart.
        path = tmp_path / "run.json"
        path.write_text(json.dumps({"pipeline": {"target": "pipe"}}), encoding="utf-8")
        res = CliRunner().invoke(cli, ["reexec", str(path)])
        assert res.exit_code == 3
        assert "Malformed" in res.output

    def test_tolerance_shown_in_human_output(self, tmp_path: Path) -> None:
        # The tolerance that enabled a match is visible, so a REPRODUCED reached
        # via a generous tolerance is never displayed as an exact match.
        run = _run(
            reported_value=_RECORDED_NUMBER,
            pipeline={"target": "tests.test_reexec:deterministic_pipeline"},
            tolerance=0.01,
        )
        path = tmp_path / "run.json"
        path.write_text(json.dumps(run), encoding="utf-8")
        res = CliRunner().invoke(cli, ["reexec", str(path)])
        assert res.exit_code == 0, res.output
        assert "tolerance:" in res.output
        assert "abs=0.01" in res.output

    def test_map_overlay_places_the_verdict(self, tmp_path: Path) -> None:
        # --map renders the claim's trust map with the faithfulness verdict on
        # its PROXY axis, as a read-side overlay (never stored).
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            with mareforma.open(".") as g:
                cid = g.assert_claim("f", classification="ANALYTICAL")
            run = _run(
                reported_value=_RECORDED_NUMBER,
                pipeline={"target": "tests.test_reexec:deterministic_pipeline"},
            )
            Path("run.json").write_text(json.dumps(run), encoding="utf-8")
            res = r.invoke(cli, ["reexec", "run.json", "--map", cid])
            assert res.exit_code == 0, res.output
            assert "TRUST MAP" in res.output
            assert "faithfulness" in res.output
            assert "REPRODUCED" in res.output
            assert "PROXIED" in res.output

    def test_map_overlay_json_is_one_document(self, tmp_path: Path) -> None:
        # --json --map is the only programmatic path to the overlaid map, so it
        # must emit a single parseable object carrying both the verdict and the
        # map, not two concatenated top-level documents.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            with mareforma.open(".") as g:
                cid = g.assert_claim("f", classification="ANALYTICAL")
            run = _run(
                reported_value=_RECORDED_NUMBER,
                pipeline={"target": "tests.test_reexec:deterministic_pipeline"},
            )
            Path("run.json").write_text(json.dumps(run), encoding="utf-8")
            res = r.invoke(cli, ["reexec", "run.json", "--json", "--map", cid])
            assert res.exit_code == 0, res.output
            doc = json.loads(res.output)
            assert doc["verdict"] == "REPRODUCED"
            assert doc["trust_map"]["subject_id"] == cid

    def test_map_overlay_json_keeps_the_verdict_on_a_map_failure(
        self, tmp_path: Path,
    ) -> None:
        # A --map that cannot be rendered is a usage error, but the verdict is
        # already known: it stays on stdout as a parseable document, with no
        # trust_map key to claim an overlay that was never rendered.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            with mareforma.open(".") as g:
                g.assert_claim("f", classification="ANALYTICAL")
            run = _run(
                reported_value=_RECORDED_NUMBER,
                pipeline={"target": "tests.test_reexec:deterministic_pipeline"},
            )
            Path("run.json").write_text(json.dumps(run), encoding="utf-8")
            res = r.invoke(
                cli, ["reexec", "run.json", "--json", "--map", "does-not-exist"],
            )
            assert res.exit_code == 3, res.output
            doc = json.loads(res.stdout)
            assert doc["verdict"] == "REPRODUCED"
            assert "trust_map" not in doc

    def test_map_overlay_unknown_claim_exits_usage_error(
        self, tmp_path: Path,
    ) -> None:
        # A mistyped --map id on an otherwise-REPRODUCED run is a usage error,
        # not a divergence: it must exit 3 (the usage/malformed code), never 1
        # (DIVERGED), so a CI gate does not misread a typo as a failed re-run.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            with mareforma.open(".") as g:
                g.assert_claim("f", classification="ANALYTICAL")
            run = _run(
                reported_value=_RECORDED_NUMBER,
                pipeline={"target": "tests.test_reexec:deterministic_pipeline"},
            )
            Path("run.json").write_text(json.dumps(run), encoding="utf-8")
            res = r.invoke(cli, ["reexec", "run.json", "--map", "does-not-exist"])
            assert res.exit_code == 3
            assert "not found" in res.output
            # The faithfulness verdict still prints before the lookup error.
            assert "REPRODUCED" in res.output

    def test_map_overlay_without_a_project_exits_usage_error(
        self, tmp_path: Path,
    ) -> None:
        # No project at or above cwd is an environment error of the overlay,
        # not a divergence: exit 3, so a gate never reads it as a failed re-run.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            run = _run(
                reported_value=_RECORDED_NUMBER,
                pipeline={"target": "tests.test_reexec:deterministic_pipeline"},
            )
            Path("run.json").write_text(json.dumps(run), encoding="utf-8")
            res = r.invoke(cli, ["reexec", "run.json", "--map", "any-id"])
            assert res.exit_code == 3, res.output
            assert "No mareforma project" in res.output

    def test_map_overlay_unreadable_graph_exits_usage_error(
        self, tmp_path: Path,
    ) -> None:
        # A damaged graph.db is likewise an overlay failure, not a divergence.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            db = Path(".mareforma") / "graph.db"
            db.parent.mkdir()
            db.write_bytes(b"not a database")
            run = _run(
                reported_value=_RECORDED_NUMBER,
                pipeline={"target": "tests.test_reexec:deterministic_pipeline"},
            )
            Path("run.json").write_text(json.dumps(run), encoding="utf-8")
            res = r.invoke(cli, ["reexec", "run.json", "--map", "any-id"])
            assert res.exit_code == 3, res.output
            assert "Could not read graph.db" in res.output
