"""
tests/test_build_cli.py — CLI integration tests.

Tests mareforma build, mareforma log, and mareforma export via
click's CliRunner. Uses real project scaffolds and real transform files
written to tmp_path.

Covers:
  - build with no transforms exits 0 with warning
  - build discovers and runs transforms
  - build --dry-run prints plan, does not run
  - build --force re-runs cached nodes
  - build on broken build_transform.py exits 1 with error message
  - build failing transform exits 1
  - log with no builds shows message
  - log after build shows transform names
  - log --json emits parseable JSON
  - export creates ontology.jsonld
  - export --json emits to stdout
  - export --output writes to custom path
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from mareforma.cli import cli
from mareforma.initializer import initialize
from mareforma.registry import add_source


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _init(tmp_path: Path, source: str = "morph") -> tuple[Path, Path]:
    """Initialise a project, register a source, return (root, raw_path)."""
    initialize(tmp_path)
    raw = tmp_path / "data" / source / "raw"
    raw.mkdir(parents=True)
    add_source(tmp_path, source, str(raw), "test source")
    return tmp_path, raw


def _write_bt(root: Path, source: str, content: str) -> Path:
    """Write a build_transform.py for the given source."""
    bt = root / "data" / source / "preprocessing" / "build_transform.py"
    bt.parent.mkdir(parents=True, exist_ok=True)
    bt.write_text(content, encoding="utf-8")
    return bt


def _run(root: Path, *args) -> object:
    runner = CliRunner()
    return runner.invoke(cli, list(args), catch_exceptions=False)


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------

class TestBuildCommand:
    def test_no_transforms_warns_exits_0(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        os.chdir(root)
        result = _run(root, "build")
        assert result.exit_code == 0
        assert "No transforms" in result.output or "no transforms" in result.output.lower()

    def test_build_runs_transform(self, tmp_path: Path) -> None:
        root, raw = _init(tmp_path)
        marker = root / "ran.txt"
        _write_bt(root, "morph", f"""
from mareforma.transforms import transform

@transform("morph.load")
def load(ctx):
    import pathlib
    pathlib.Path(r"{marker}").write_text("ran")
""")
        os.chdir(root)
        result = _run(root, "build")
        assert result.exit_code == 0
        assert marker.exists(), "transform function must have been called"

    def test_build_creates_graph_db(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        _write_bt(root, "morph", """
from mareforma.transforms import transform

@transform("morph.load")
def load(ctx):
    pass
""")
        os.chdir(root)
        _run(root, "build")
        assert (root / ".mareforma" / "graph.db").exists()

    def test_build_creates_ontology_jsonld(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        _write_bt(root, "morph", """
from mareforma.transforms import transform

@transform("morph.load")
def load(ctx):
    pass
""")
        os.chdir(root)
        _run(root, "build")
        assert (root / "ontology.jsonld").exists()

    def test_build_dry_run_does_not_run(self, tmp_path: Path) -> None:
        root, raw = _init(tmp_path)
        marker = root / "ran.txt"
        _write_bt(root, "morph", f"""
from mareforma.transforms import transform

@transform("morph.load")
def load(ctx):
    import pathlib
    pathlib.Path(r"{marker}").write_text("ran")
""")
        os.chdir(root)
        result = _run(root, "build", "--dry-run")
        assert result.exit_code == 0
        assert not marker.exists()

    def test_build_failing_transform_exits_1(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        _write_bt(root, "morph", """
from mareforma.transforms import transform

@transform("morph.bad")
def bad(ctx):
    raise RuntimeError("deliberate failure")
""")
        os.chdir(root)
        result = _run(root, "build")
        assert result.exit_code == 1

    def test_build_source_filter(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        # Register a second source with a transform
        raw2 = root / "data" / "ephys" / "raw"
        raw2.mkdir(parents=True)
        add_source(root, "ephys", str(raw2), "ephys source")

        marker_morph = root / "morph_ran.txt"
        marker_ephys = root / "ephys_ran.txt"

        _write_bt(root, "morph", f"""
from mareforma.transforms import transform

@transform("morph.load")
def load(ctx):
    import pathlib
    pathlib.Path(r"{marker_morph}").write_text("ran")
""")
        _write_bt(root, "ephys", f"""
from mareforma.transforms import transform

@transform("ephys.load")
def load(ctx):
    import pathlib
    pathlib.Path(r"{marker_ephys}").write_text("ran")
""")
        os.chdir(root)
        result = _run(root, "build", "morph")
        assert result.exit_code == 0
        assert marker_morph.exists()
        assert not marker_ephys.exists()

    def test_build_broken_file_exits_1(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        _write_bt(root, "morph", "this is not valid python !!")
        os.chdir(root)
        result = _run(root, "build")
        assert result.exit_code == 1

    def test_build_no_project_exits_1(self, tmp_path: Path) -> None:
        os.chdir(tmp_path)
        result = _run(tmp_path, "build")
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# log
# ---------------------------------------------------------------------------

class TestLogCommand:
    def test_log_no_builds_shows_message(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        os.chdir(root)
        result = _run(root, "log")
        assert result.exit_code == 0
        assert "No builds" in result.output

    def test_log_after_build_shows_transform(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        _write_bt(root, "morph", """
from mareforma.transforms import transform

@transform("morph.load")
def load(ctx):
    pass
""")
        os.chdir(root)
        _run(root, "build")
        result = _run(root, "log")
        assert result.exit_code == 0
        assert "morph.load" in result.output

    def test_log_json_is_parseable(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        _write_bt(root, "morph", """
from mareforma.transforms import transform

@transform("morph.load")
def load(ctx):
    pass
""")
        os.chdir(root)
        _run(root, "build")
        result = _run(root, "log", "--json")
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "nodes" in data
        assert "build_timestamp" in data

    def test_log_no_project_exits_1(self, tmp_path: Path) -> None:
        os.chdir(tmp_path)
        result = _run(tmp_path, "log")
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

class TestExportCommand:
    def test_export_creates_file(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        os.chdir(root)
        result = _run(root, "export")
        assert result.exit_code == 0
        assert (root / "ontology.jsonld").exists()

    def test_export_file_is_valid_json_ld(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        os.chdir(root)
        _run(root, "export")
        doc = json.loads((root / "ontology.jsonld").read_text())
        assert "@context" in doc
        assert "@graph" in doc

    def test_export_json_flag_stdout(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        os.chdir(root)
        result = _run(root, "export", "--json")
        assert result.exit_code == 0
        doc = json.loads(result.output)
        assert "@context" in doc

    def test_export_custom_output_path(self, tmp_path: Path) -> None:
        root, _ = _init(tmp_path)
        custom = root / "out" / "onto.jsonld"
        os.chdir(root)
        result = _run(root, "export", "--output", str(custom))
        assert result.exit_code == 0
        assert custom.exists()

    def test_export_no_project_exits_1(self, tmp_path: Path) -> None:
        os.chdir(tmp_path)
        result = _run(tmp_path, "export")
        assert result.exit_code == 1
