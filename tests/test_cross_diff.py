"""
tests/test_cross_diff.py — tests for ``mareforma cross-diff``.

Covers:
  - same artifact suffix, same content → status "same"
  - same artifact suffix, different content → status "changed"
  - disjoint artifact sets → "only_in_a" / "only_in_b"
  - transform with no recorded runs → exit 1 with error message
  - --json flag produces valid JSON with expected keys
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from mareforma.cli import cli
from mareforma.db import open_db
from mareforma.initializer import initialize
from mareforma.pipeline.runner import TransformRunner
from mareforma.transforms import TransformRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(name: str, fn, depends_on=None) -> TransformRecord:
    return TransformRecord(
        name=name,
        fn=fn,
        depends_on=depends_on or [],
        source_file="<test>",
        source_code="def fn(ctx): pass",
    )


def _runner(root: Path) -> TransformRunner:
    from mareforma.registry import load as load_toml
    return TransformRunner(root=root, registry_data=load_toml(root))


@pytest.fixture()
def proj(tmp_path: Path) -> Path:
    """Initialised project with one source registered."""
    initialize(tmp_path)
    from mareforma.registry import add_source
    raw = tmp_path / "data" / "src" / "raw"
    raw.mkdir(parents=True)
    add_source(tmp_path, "src", str(raw), "test source")
    original = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(original)


# ---------------------------------------------------------------------------
# Artifact comparison
# ---------------------------------------------------------------------------

class TestArtifactDelta:
    def test_same_content_reports_same(self, proj: Path) -> None:
        """Two transforms saving identical content → artifact status 'same'."""
        def fork_a(ctx):
            ctx.save("result", {"target": "STAT4"}, fmt="json")

        def fork_b(ctx):
            ctx.save("result", {"target": "STAT4"}, fmt="json")

        _runner(proj).run([_make_record("ra_cd4.analysis", fork_a)])
        _runner(proj).run([_make_record("sle_cd4.analysis", fork_b)])

        runner = CliRunner()
        result = runner.invoke(cli, ["cross-diff", "ra_cd4.analysis", "sle_cd4.analysis", "--json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)

        delta = {e["suffix"]: e["status"] for e in data["artifact_delta"]}
        assert delta["result"] == "same"

    def test_different_content_reports_changed(self, proj: Path) -> None:
        """Two transforms saving different content → artifact status 'changed'."""
        def fork_a(ctx):
            ctx.save("result", {"target": "STAT4"}, fmt="json")

        def fork_b(ctx):
            ctx.save("result", {"target": "PTPN22"}, fmt="json")

        _runner(proj).run([_make_record("ra_cd4.analysis", fork_a)])
        _runner(proj).run([_make_record("sle_cd4.analysis", fork_b)])

        runner = CliRunner()
        result = runner.invoke(cli, ["cross-diff", "ra_cd4.analysis", "sle_cd4.analysis", "--json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)

        delta = {e["suffix"]: e["status"] for e in data["artifact_delta"]}
        assert delta["result"] == "changed"

    def test_disjoint_artifacts(self, proj: Path) -> None:
        """Transforms with different artifact names → only_in_a / only_in_b."""
        def fork_a(ctx):
            ctx.save("generated_code", "SELECT * FROM ra", fmt="json")

        def fork_b(ctx):
            ctx.save("hypothesis", "PTPN22 is the target", fmt="json")

        _runner(proj).run([_make_record("ra_cd4.analysis", fork_a)])
        _runner(proj).run([_make_record("sle_cd4.analysis", fork_b)])

        runner = CliRunner()
        result = runner.invoke(cli, ["cross-diff", "ra_cd4.analysis", "sle_cd4.analysis", "--json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)

        delta = {e["suffix"]: e["status"] for e in data["artifact_delta"]}
        assert delta["generated_code"] == "only_in_a"
        assert delta["hypothesis"] == "only_in_b"


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

class TestErrorPaths:
    def test_missing_transform_a_exits_nonzero(self, proj: Path) -> None:
        """If transform_a has no runs, exit code 1 with error message."""
        def fork_b(ctx):
            ctx.save("result", "x", fmt="json")

        _runner(proj).run([_make_record("sle_cd4.analysis", fork_b)])

        runner = CliRunner()
        result = runner.invoke(cli, ["cross-diff", "ra_cd4.analysis", "sle_cd4.analysis"])
        assert result.exit_code == 1

    def test_missing_transform_b_exits_nonzero(self, proj: Path) -> None:
        """If transform_b has no runs, exit code 1 with error message."""
        def fork_a(ctx):
            ctx.save("result", "x", fmt="json")

        _runner(proj).run([_make_record("ra_cd4.analysis", fork_a)])

        runner = CliRunner()
        result = runner.invoke(cli, ["cross-diff", "ra_cd4.analysis", "sle_cd4.analysis"])
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

class TestJsonOutput:
    def test_json_output_has_expected_keys(self, proj: Path) -> None:
        """--json output contains transform_a, transform_b, run_a, run_b, artifact_delta."""
        def fork_a(ctx):
            ctx.save("result", {"target": "STAT4"}, fmt="json")

        def fork_b(ctx):
            ctx.save("result", {"target": "PTPN22"}, fmt="json")

        _runner(proj).run([_make_record("ra_cd4.analysis", fork_a)])
        _runner(proj).run([_make_record("sle_cd4.analysis", fork_b)])

        runner = CliRunner()
        result = runner.invoke(cli, ["cross-diff", "ra_cd4.analysis", "sle_cd4.analysis", "--json"])
        assert result.exit_code == 0, result.output

        data = json.loads(result.output)
        assert data["transform_a"] == "ra_cd4.analysis"
        assert data["transform_b"] == "sle_cd4.analysis"
        assert "run_a" in data
        assert "run_b" in data
        assert isinstance(data["artifact_delta"], list)
        assert len(data["artifact_delta"]) == 1
        assert data["artifact_delta"][0]["suffix"] == "result"
