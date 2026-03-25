"""
tests/test_runner.py — integration tests for the pipeline runner.

Strategy: build minimal TransformRecords with real Python functions,
run them through TransformRunner with a tmp project root, and assert on
BuildResult. No real data files needed — transforms operate on simple
Python values via ctx.save/ctx.load.

Covers:
  - all nodes run on first build (empty db)
  - cached nodes are skipped when hashes unchanged (second run reads db)
  - force=True re-runs cached nodes
  - source code change invalidates cache
  - failed transform → result.failed populated, error recorded in graph.db
  - failed node: downstream skipped, independent still runs
  - dry_run shows planned execution without writing to db
  - BuildResult.success reflects failure correctly
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mareforma.db import all_transform_runs, open_db
from mareforma.initializer import initialize
from mareforma.pipeline.runner import TransformRunner
from mareforma.transforms import TransformRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(
    name: str,
    fn,
    depends_on: list[str] | None = None,
    source_code: str = "def fn(): pass",
) -> TransformRecord:
    return TransformRecord(
        name=name,
        fn=fn,
        depends_on=depends_on or [],
        source_file="<test>",
        source_code=source_code,
    )


def _runner(root: Path, **kwargs) -> TransformRunner:
    from mareforma.registry import load as load_toml
    registry_data = load_toml(root)
    return TransformRunner(root=root, registry_data=registry_data, **kwargs)


@pytest.fixture()
def proj(tmp_path: Path) -> Path:
    """Initialised project with one source registered."""
    initialize(tmp_path)
    from mareforma.registry import add_source
    raw = tmp_path / "data" / "morph" / "raw"
    raw.mkdir(parents=True)
    add_source(tmp_path, "morph", str(raw), "test source")
    return tmp_path


# ---------------------------------------------------------------------------
# Basic execution
# ---------------------------------------------------------------------------

class TestBasicExecution:
    def test_single_node_runs(self, proj: Path) -> None:
        ran = []

        def load(ctx):
            ran.append("load")

        rec = _make_record("morph.load", load)
        result = _runner(proj).run([rec])

        assert result.success
        assert "morph.load" in result.ran
        assert ran == ["load"]

    def test_two_nodes_run_in_order(self, proj: Path) -> None:
        order = []

        def load(ctx):
            order.append("load")

        def proc(ctx):
            order.append("proc")

        r1 = _make_record("morph.load", load)
        r2 = _make_record("morph.proc", proc, depends_on=["morph.load"])

        result = _runner(proj).run([r1, r2])

        assert result.success
        assert order == ["load", "proc"]

    def test_empty_records_succeed(self, proj: Path) -> None:
        result = _runner(proj).run([])
        assert result.success
        assert result.ran == []

    def test_build_result_duration_set(self, proj: Path) -> None:
        def fn(ctx):
            pass

        rec = _make_record("morph.fn", fn)
        result = _runner(proj).run([rec])
        assert result.duration_ms >= 0


# ---------------------------------------------------------------------------
# Caching (incremental builds)
# ---------------------------------------------------------------------------

class TestCaching:
    def test_second_run_caches_unchanged(self, proj: Path) -> None:
        ran = []

        def load(ctx):
            ran.append("load")

        rec = _make_record("morph.load", load, source_code="def load(ctx): pass")

        # First build — writes to graph.db
        r1 = _runner(proj).run([rec])
        assert "morph.load" in r1.ran

        # Second build — same source code, same raw dir → cached
        r2 = _runner(proj).run([rec])

        assert "morph.load" in r2.cached
        assert r2.ran == []
        assert len(ran) == 1  # fn called only once

    def test_force_reruns_cached(self, proj: Path) -> None:
        ran = []

        def load(ctx):
            ran.append("load")

        rec = _make_record("morph.load", load, source_code="def load(ctx): pass")

        _runner(proj).run([rec])
        r2 = _runner(proj, force=True).run([rec])

        assert "morph.load" in r2.ran
        assert len(ran) == 2  # ran both times

    def test_source_code_change_invalidates_cache(self, proj: Path) -> None:
        ran = []

        def load(ctx):
            ran.append("load")

        r_v1 = _make_record("morph.load", load, source_code="def load(ctx): pass  # v1")
        _runner(proj).run([r_v1])

        # Same function, different source code hash
        r_v2 = _make_record("morph.load", load, source_code="def load(ctx): pass  # v2")
        r2 = _runner(proj).run([r_v2])

        assert "morph.load" in r2.ran
        assert len(ran) == 2


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_failing_node_recorded_in_result(self, proj: Path) -> None:
        def bad(ctx):
            raise RuntimeError("deliberate failure")

        rec = _make_record("morph.bad", bad)
        result = _runner(proj).run([rec])

        assert not result.success
        assert "morph.bad" in result.failed
        assert "RuntimeError" in result.errors["morph.bad"]
        assert "deliberate failure" in result.errors["morph.bad"]

    def test_failing_node_does_not_propagate_exception(self, proj: Path) -> None:
        """Runner must catch exceptions — never re-raise them."""
        def bad(ctx):
            raise ValueError("boom")

        rec = _make_record("morph.bad2", bad)
        result = _runner(proj).run([rec])
        assert not result.success

    def test_failure_skips_downstream_but_not_independent(self, proj: Path) -> None:
        """Failed node: dependent skipped; unrelated independent still runs."""
        ran = []

        def bad(ctx):
            raise RuntimeError("fail")

        def downstream(ctx):
            ran.append("downstream")

        def independent(ctx):
            ran.append("independent")

        r_bad  = _make_record("morph.bad",         bad)
        r_down = _make_record("morph.downstream",  downstream, depends_on=["morph.bad"])
        r_ind  = _make_record("other.independent", independent)

        result = _runner(proj).run([r_bad, r_down, r_ind])

        assert "morph.bad"        in result.failed
        assert "morph.downstream" in result.skipped
        assert "other.independent" in result.ran
        assert ran == ["independent"]  # downstream never ran

    def test_failed_node_written_to_db(self, proj: Path) -> None:
        def bad(ctx):
            raise RuntimeError("fail")

        rec = _make_record("morph.failnode", bad)
        _runner(proj).run([rec])

        conn = open_db(proj)
        try:
            runs = all_transform_runs(conn)
        finally:
            conn.close()

        assert "morph.failnode" in runs
        assert runs["morph.failnode"]["status"] == "failed"


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_dry_run_does_not_call_function(self, proj: Path) -> None:
        ran = []

        def fn(ctx):
            ran.append("ran")

        rec = _make_record("morph.drytest", fn)
        result = _runner(proj, dry_run=True).run([rec])

        assert ran == []
        assert result.ran  # node is listed as would-run

    def test_dry_run_does_not_write_to_db(self, proj: Path) -> None:
        def fn(ctx):
            pass

        rec = _make_record("morph.drynopersist", fn)
        _runner(proj, dry_run=True).run([rec])

        conn = open_db(proj)
        try:
            runs = all_transform_runs(conn)
        finally:
            conn.close()

        assert "morph.drynopersist" not in runs


# ---------------------------------------------------------------------------
# BuildResult
# ---------------------------------------------------------------------------

class TestBuildResult:
    def test_success_reflects_failure(self, proj: Path) -> None:
        def ok(ctx):
            pass

        def bad(ctx):
            raise RuntimeError()

        assert _runner(proj).run([_make_record("morph.ok", ok)]).success is True
        assert _runner(proj).run([_make_record("morph.bad3", bad)]).success is False
