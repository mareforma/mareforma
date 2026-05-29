"""Conformance tests for :mod:`mareforma.tools`."""

from __future__ import annotations

import pytest

from mareforma.tools import (
    PredicateBoundaryError,
    ReplayResult,
    Tool,
    ToolCallError,
    ToolResult,
)


def test_tool_runtime_checkable_positive():
    class GoodTool:
        name = "demo"
        version = "1.0.0"

        def call(self, **kwargs):
            return {"data": kwargs}

    assert isinstance(GoodTool(), Tool)


def test_tool_runtime_checkable_negative_missing_version():
    class NoVersion:
        name = "x"

        def call(self, **kwargs):
            return {"data": None}

    assert not isinstance(NoVersion(), Tool)


def test_tool_runtime_checkable_negative_missing_call():
    class NoCall:
        name = "x"
        version = "0"

    assert not isinstance(NoCall(), Tool)


def test_tool_result_typed_dict_acceptance():
    r: ToolResult = {"data": [1, 2, 3], "metadata": {"cached": True}}
    assert r["data"] == [1, 2, 3]
    assert r.get("source_version") is None


def test_replay_result_immutable_and_diff_default_empty():
    r = ReplayResult(
        ok=True,
        observed_result_digest="abc",
        expected_result_digest="abc",
    )
    assert r.ok is True
    assert r.diff_fields == ()
    with pytest.raises((AttributeError, TypeError)):
        r.ok = False  # frozen


def test_replay_result_with_diff_fields():
    r = ReplayResult(
        ok=False,
        observed_result_digest="abc",
        expected_result_digest="def",
        diff_fields=("result_digest", "tool_version"),
    )
    assert r.ok is False
    assert r.diff_fields == ("result_digest", "tool_version")


def test_typed_exceptions_are_distinguishable():
    assert issubclass(ToolCallError, Exception)
    assert issubclass(PredicateBoundaryError, ValueError)
    # Catchable via parent class.
    with pytest.raises(ValueError):
        raise PredicateBoundaryError("smuggled tag")
