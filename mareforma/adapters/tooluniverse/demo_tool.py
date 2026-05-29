"""Deterministic Open Targets stand-in for the demo CLI.

The CLI's ``demo`` subcommand and the wedge test both need a tool that
behaves like ToolUniverse's `OpenTargets_search_targets` but returns a
pinned response. This module ships that tool so the CLI is callable
without the test fixtures at runtime.

The pinned response payload is duplicated from
``tests/conftest.py::MOCK_OPEN_TARGETS_PAYLOAD`` deliberately — the
test fixture is a test-only artifact, while this demo tool ships with
the package for runtime use.
"""

from __future__ import annotations

from typing import Any


__all__ = ["OpenTargetsSearchTargetsMock", "MOCK_OPEN_TARGETS_PAYLOAD"]


MOCK_OPEN_TARGETS_PAYLOAD: dict[str, Any] = {
    "search": {
        "hits": [
            {
                "id": "ENSG00000146648",
                "name": "EGFR",
                "entity": "target",
                "score": 0.989,
            },
            {
                "id": "ENSG00000148848",
                "name": "ADAM12",
                "entity": "target",
                "score": 0.412,
            },
        ],
        "total": 2,
    },
    "_source_version": "Open Targets 25.06",
}


class OpenTargetsSearchTargetsMock:
    """Deterministic stand-in for ToolUniverse's OpenTargets_search_targets."""

    name = "OpenTargets_search_targets"
    version = "1.1.11"
    category = "pharmacology"

    def __init__(self) -> None:
        self.tool_config: dict[str, Any] = {
            "name": self.name,
            "type": "OpenTargets",
            "category": self.category,
            "parameter": {
                "type": "object",
                "properties": {
                    "target": {"type": "string"},
                    "size": {"type": "integer", "default": 10},
                },
                "required": ["target"],
            },
        }

    def call(self, **kwargs: Any) -> dict[str, Any]:
        target = kwargs.get("target")
        if not isinstance(target, str) or not target:
            raise ValueError("target is required and must be non-empty")
        payload = dict(MOCK_OPEN_TARGETS_PAYLOAD)
        return {
            "data": {
                "search": payload["search"],
                "args_echo": dict(sorted(kwargs.items())),
            },
            "metadata": {"observed_at_call_time": True},
            "source_version": payload["_source_version"],
        }
