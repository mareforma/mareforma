"""Conformance tests for :mod:`mareforma.tools`.

Conceptual clusters:

- :class:`TestToolProtocol` — runtime_checkable structural shape.
- :class:`TestToolResult` — TypedDict acceptance shape.
- :class:`TestReplayResult` — dataclass immutability + defaults.
- :class:`TestTypedExceptions` — exception parent-class wiring.
"""

from __future__ import annotations

import pytest

from mareforma.tools import (
    PredicateBoundaryError,
    ReplayResult,
    Tool,
    ToolCallError,
    ToolResult,
)


class TestToolProtocol:
    def test_runtime_checkable_positive(self):
        class GoodTool:
            name = "demo"
            version = "1.0.0"

            def call(self, **kwargs):
                return {"data": kwargs}

        assert isinstance(GoodTool(), Tool)

    def test_runtime_checkable_negative_missing_version(self):
        class NoVersion:
            name = "x"

            def call(self, **kwargs):
                return {"data": None}

        assert not isinstance(NoVersion(), Tool)

    def test_runtime_checkable_negative_missing_call(self):
        class NoCall:
            name = "x"
            version = "0"

        assert not isinstance(NoCall(), Tool)


class TestToolResult:
    def test_typed_dict_acceptance(self):
        r: ToolResult = {"data": [1, 2, 3], "metadata": {"cached": True}}
        assert r["data"] == [1, 2, 3]
        assert r.get("source_version") is None


class TestReplayResult:
    def test_immutable_and_diff_default_empty(self):
        r = ReplayResult(
            ok=True,
            observed_result_digest="abc",
            expected_result_digest="abc",
        )
        assert r.ok is True
        assert r.diff_fields == ()
        with pytest.raises((AttributeError, TypeError)):
            r.ok = False  # frozen

    def test_with_diff_fields(self):
        r = ReplayResult(
            ok=False,
            observed_result_digest="abc",
            expected_result_digest="def",
            diff_fields=("result_digest", "tool_version"),
        )
        assert r.ok is False
        assert r.diff_fields == ("result_digest", "tool_version")


class TestTypedExceptions:
    def test_distinguishable_via_parent_classes(self):
        assert issubclass(ToolCallError, Exception)
        assert issubclass(PredicateBoundaryError, ValueError)
        # Catchable via parent class.
        with pytest.raises(ValueError):
            raise PredicateBoundaryError("smuggled tag")
