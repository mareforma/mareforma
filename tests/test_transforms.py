"""
tests/test_transforms.py — tests for the @transform decorator and TransformRegistry.

Covers:
  - JSONL logging: success, failure, multiple calls, timestamp format, no-project graceful
  - Decorator contract: kwargs pass-through, empty name error, return value
  - depends_on parameter storage
  - TransformRegistry: register, get, overwrite, clear
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import pytest

from mareforma.transforms import transform, registry


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def commits_dir(tmp_path: Path):
    """Create .mareforma/commits/ and chdir into the project root."""
    d = tmp_path / ".mareforma" / "commits"
    d.mkdir(parents=True)
    original = os.getcwd()
    os.chdir(tmp_path)
    yield d
    os.chdir(original)


def _read_log(commits_dir: Path) -> list[dict]:
    log = commits_dir / "transforms.jsonl"
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text().splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# JSONL logging
# ---------------------------------------------------------------------------

class TestLogging:
    def test_logs_success(self, commits_dir: Path) -> None:
        @transform("log.success")
        def fn():
            return 42

        assert fn() == 42
        entries = _read_log(commits_dir)
        assert len(entries) == 1
        assert entries[0]["name"] == "log.success"
        assert entries[0]["status"] == "success"
        assert entries[0]["error"] is None
        assert isinstance(entries[0]["duration_ms"], int)

    def test_logs_failure_and_reraises(self, commits_dir: Path) -> None:
        @transform("log.failure")
        def bad():
            raise ValueError("something went wrong")

        with pytest.raises(ValueError, match="something went wrong"):
            bad()

        entries = _read_log(commits_dir)
        assert len(entries) == 1
        assert entries[0]["status"] == "failed"
        assert "ValueError" in entries[0]["error"]
        assert "something went wrong" in entries[0]["error"]

    def test_multiple_calls_append(self, commits_dir: Path) -> None:
        @transform("log.repeated")
        def step():
            pass

        step()
        step()
        step()
        assert len(_read_log(commits_dir)) == 3

    def test_timestamp_is_iso_format(self, commits_dir: Path) -> None:
        @transform("log.timestamp")
        def fn():
            pass

        fn()
        ts = _read_log(commits_dir)[0]["timestamp"]
        datetime.fromisoformat(ts)  # raises if not valid ISO

    def test_silent_outside_project(self, tmp_path: Path) -> None:
        """Decorator must not crash when .mareforma/commits/ does not exist."""
        original = os.getcwd()
        os.chdir(tmp_path)
        try:
            @transform("log.no_project")
            def fn():
                return "ok"

            assert fn() == "ok"
            assert not (tmp_path / ".mareforma").exists()
        finally:
            os.chdir(original)


# ---------------------------------------------------------------------------
# Decorator contract
# ---------------------------------------------------------------------------

class TestDecoratorContract:
    def test_kwargs_passed_through(self, commits_dir: Path) -> None:
        @transform("meta.kwargs")
        def fn(x, y=10):
            return x + y

        assert fn(1, y=5) == 6

    def test_empty_name_raises_at_decoration_time(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            @transform("")
            def fn():
                pass

    def test_return_value_preserved(self, commits_dir: Path) -> None:
        @transform("meta.return")
        def fn():
            return {"key": "value"}

        assert fn() == {"key": "value"}


# ---------------------------------------------------------------------------
# depends_on (v0.2)
# ---------------------------------------------------------------------------

class TestDependsOn:
    def test_depends_on_empty_list(self) -> None:
        @transform("dep.root", depends_on=[])
        def root():
            pass

        assert registry.get("dep.root").depends_on == []

    def test_depends_on_single(self) -> None:
        @transform("dep.step", depends_on=["dep.root"])
        def step():
            pass

        assert registry.get("dep.step").depends_on == ["dep.root"]

    def test_depends_on_multiple(self) -> None:
        @transform("dep.merge", depends_on=["dep.a", "dep.b"])
        def merge():
            pass

        assert set(registry.get("dep.merge").depends_on) == {"dep.a", "dep.b"}

    def test_depends_on_omitted_defaults_to_empty(self) -> None:
        @transform("dep.implicit_root")
        def fn():
            pass

        assert registry.get("dep.implicit_root").depends_on == []

    def test_depends_on_does_not_affect_call(self) -> None:
        results = []

        @transform("dep.callable", depends_on=["dep.upstream"])
        def fn(x):
            results.append(x)
            return x * 2

        assert fn(5) == 10
        assert results == [5]


# ---------------------------------------------------------------------------
# TransformRegistry (v0.2)
# ---------------------------------------------------------------------------

class TestTransformRegistry:
    def test_register_and_get(self) -> None:
        @transform("reg.one")
        def one():
            pass

        rec = registry.get("reg.one")
        assert rec is not None
        assert rec.name == "reg.one"

    def test_get_unknown_returns_none(self) -> None:
        assert registry.get("does.not.exist") is None

    def test_names_lists_all_registered(self) -> None:
        @transform("reg.a")
        def a():
            pass

        @transform("reg.b")
        def b():
            pass

        assert "reg.a" in registry.names()
        assert "reg.b" in registry.names()

    def test_overwrite_same_name_replaces_fn(self) -> None:
        @transform("reg.ow")
        def v1():
            return 1

        @transform("reg.ow")
        def v2():
            return 2

        assert registry.get("reg.ow").fn() == 2

    def test_clear_empties_registry(self) -> None:
        @transform("reg.clr")
        def fn():
            pass

        assert registry.get("reg.clr") is not None
        registry.clear()
        assert registry.get("reg.clr") is None

