"""
tests/test_cli.py — smoke tests for all CLI commands.

Uses click's CliRunner to invoke commands in an isolated temp directory.

Additional coverage (v0.3):
  - trace: linear pipeline shows all transforms in order
  - trace: single-node pipeline (root only)
  - trace: transform never ran → exit 1 with helpful message
  - trace: --json flag emits valid JSON with chain
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from mareforma.cli import cli


@pytest.fixture()
def project_dir(tmp_path: Path) -> Path:
    """Return a temp dir with a fully initialised mareforma project."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["init"])
        assert result.exit_code == 0, result.output
        yield Path(os.getcwd())


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------

class TestInit:
    def test_fresh_project_creates_structure(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init"])
            assert result.exit_code == 0
            cwd = Path(os.getcwd())
            assert (cwd / ".mareforma" / "commits").is_dir()
            assert (cwd / "data").is_dir()
            assert (cwd / "mareforma.project.toml").is_file()
            assert (cwd / ".gitignore").is_file()

    def test_idempotent_on_existing_project(self, project_dir: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=project_dir.parent):
            os.chdir(project_dir)
            result = runner.invoke(cli, ["init"])
            assert result.exit_code == 0
            assert "already initialised" in result.output

    def test_does_not_overwrite_existing_toml(self, project_dir: Path) -> None:
        toml = project_dir / "mareforma.project.toml"
        original = toml.read_text()
        toml.write_text(original + '\n# custom comment\n')
        runner = CliRunner()
        os.chdir(project_dir)
        runner.invoke(cli, ["init"])
        assert "custom comment" in toml.read_text()


# ---------------------------------------------------------------------------
# add-source
# ---------------------------------------------------------------------------

class TestAddSource:
    def test_creates_dirs_and_registers_in_toml(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        result = runner.invoke(cli, ["add-source", "morphology", "--description", "Morphology data"])
        assert result.exit_code == 0
        base = project_dir / "data" / "morphology"
        for d in ["raw", "processed", "protocols", "preprocessing"]:
            assert (base / d).is_dir(), f"Missing: {d}"
        toml_text = (project_dir / "mareforma.project.toml").read_text()
        assert "morphology" in toml_text
        assert "Morphology data" in toml_text

    def test_duplicate_fails_without_force(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        runner.invoke(cli, ["add-source", "morphology"])
        result = runner.invoke(cli, ["add-source", "morphology"])
        assert result.exit_code == 1
        assert "already registered" in result.output.lower() or "already registered" in (result.stderr or "")

    def test_duplicate_succeeds_with_force(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        runner.invoke(cli, ["add-source", "morphology", "--description", "old"])
        result = runner.invoke(cli, ["add-source", "morphology", "--description", "new", "--force"])
        assert result.exit_code == 0
        toml_text = (project_dir / "mareforma.project.toml").read_text()
        assert "new" in toml_text

    def test_nonexistent_path_warns_but_registers(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        result = runner.invoke(cli, ["add-source", "ghost", "--path", "/nonexistent/path"])
        # Should warn but exit 0
        assert result.exit_code == 0
        assert "does not exist" in result.output or "Warning" in result.output

    def test_creates_build_transform_template(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        runner.invoke(cli, ["add-source", "imaging"])
        bt = project_dir / "data" / "imaging" / "preprocessing" / "build_transform.py"
        assert bt.is_file()
        assert "imaging" in bt.read_text()


# ---------------------------------------------------------------------------
# explain
# ---------------------------------------------------------------------------

class TestExplain:
    def test_explain_project_no_sources(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        result = runner.invoke(cli, ["explain"])
        assert result.exit_code == 0
        assert "PROJECT" in result.output

    def test_explain_project_lists_sources(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        runner.invoke(cli, ["add-source", "morphology"])
        runner.invoke(cli, ["add-source", "ephys"])
        result = runner.invoke(cli, ["explain"])
        assert result.exit_code == 0
        assert "morphology" in result.output
        assert "ephys" in result.output

    def test_explain_source(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        runner.invoke(cli, ["add-source", "morphology", "--description", "Skeleton data"])
        result = runner.invoke(cli, ["explain", "morphology"])
        assert result.exit_code == 0
        assert "morphology" in result.output
        assert "Skeleton data" in result.output

    def test_explain_source_json(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        runner.invoke(cli, ["add-source", "morphology"])
        result = runner.invoke(cli, ["explain", "morphology", "--json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert "morphology" in parsed

    def test_explain_unknown_source_exits_1(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        result = runner.invoke(cli, ["explain", "does_not_exist"])
        assert result.exit_code == 1

    def test_explain_project_json(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        result = runner.invoke(cli, ["explain", "--json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert "project" in parsed
        assert "sources" in parsed


# ---------------------------------------------------------------------------
# check
# ---------------------------------------------------------------------------

class TestCheck:
    def test_empty_project_warns(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        result = runner.invoke(cli, ["check"])
        # No sources registered → warning → exit 1
        assert result.exit_code == 1

    def test_clean_project_exits_0(self, project_dir: Path, tmp_path: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)

        # Create an actual raw dir so path check passes
        raw = project_dir / "data" / "morphology" / "raw"
        raw.mkdir(parents=True, exist_ok=True)

        runner.invoke(cli, [
            "add-source", "morphology",
            "--path", str(raw),
            "--description", "Skeleton reconstructions",
        ])

        # Manually fill format field
        toml_path = project_dir / "mareforma.project.toml"
        text = toml_path.read_text()
        text = text.replace('format = ""', 'format = "SWC"')
        # Also fill project description
        text = text.replace('description = ""', 'description = "Test project"', 1)
        toml_path.write_text(text)

        result = runner.invoke(cli, ["check"])
        assert result.exit_code == 0, result.output

    def test_missing_path_warns(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        runner.invoke(cli, ["add-source", "ghost", "--path", "/nonexistent/path"])
        result = runner.invoke(cli, ["check"])
        assert result.exit_code == 1
        assert "does not exist" in result.output or "does not exist" in (result.stderr or "")

    def test_no_project_exits_1(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["check"])
            assert result.exit_code == 1


# ---------------------------------------------------------------------------
# trace (v0.3)
# ---------------------------------------------------------------------------

class TestTrace:
    """Smoke tests for `mareforma trace <transform_name>`."""

    def _seed_runs(self, project_dir: Path) -> None:
        """Populate graph.db with a small linear pipeline: load → filter → features."""
        import uuid
        import sqlite3
        from mareforma.db import open_db, begin_run, end_run, record_deps, write_transform_class

        conn = open_db(project_dir)
        try:
            for name, deps, cls in [
                ("src.load",     [],              "raw"),
                ("src.filter",   ["src.load"],    "processed"),
                ("src.features", ["src.filter"],  "analysed"),
            ]:
                run_id = str(uuid.uuid4())
                begin_run(conn, run_id, name, "ih", "sh")
                record_deps(conn, name, deps)
                end_run(conn, run_id, status="success", output_hash=f"h_{name}")
                write_transform_class(
                    conn, run_id,
                    transform_class=cls,
                    class_confidence=0.9,
                    class_method="heuristic",
                    class_reason="test",
                )
        finally:
            conn.close()

    def test_trace_linear_pipeline(self, project_dir: Path) -> None:
        self._seed_runs(project_dir)
        runner = CliRunner()
        os.chdir(project_dir)
        result = runner.invoke(cli, ["trace", "src.features"])
        assert result.exit_code == 0, result.output
        # All three transforms should appear
        assert "src.load" in result.output
        assert "src.filter" in result.output
        assert "src.features" in result.output

    def test_trace_single_node(self, project_dir: Path) -> None:
        """A root-only transform traces cleanly."""
        self._seed_runs(project_dir)
        runner = CliRunner()
        os.chdir(project_dir)
        result = runner.invoke(cli, ["trace", "src.load"])
        assert result.exit_code == 0, result.output
        assert "src.load" in result.output

    def test_trace_unknown_transform_exits_1(self, project_dir: Path) -> None:
        runner = CliRunner()
        os.chdir(project_dir)
        result = runner.invoke(cli, ["trace", "nonexistent.transform"])
        assert result.exit_code == 1
        assert "nonexistent.transform" in result.output or "No runs" in result.output

    def test_trace_json_flag_emits_valid_json(self, project_dir: Path) -> None:
        self._seed_runs(project_dir)
        runner = CliRunner()
        os.chdir(project_dir)
        result = runner.invoke(cli, ["trace", "src.features", "--json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["transform"] == "src.features"
        assert isinstance(data["chain"], list)
        assert len(data["chain"]) >= 1
        # Each chain entry has required keys
        for entry in data["chain"]:
            assert "transform_name" in entry
            assert "class" in entry
            assert "support" in entry

    def test_trace_shows_class_labels(self, project_dir: Path) -> None:
        self._seed_runs(project_dir)
        runner = CliRunner()
        os.chdir(project_dir)
        result = runner.invoke(cli, ["trace", "src.features"])
        assert result.exit_code == 0
        output = result.output.upper()
        assert "RAW" in output
        assert "PROCESSED" in output
        assert "ANALYSED" in output
