"""Deterministic Open Targets stand-in for the adapter tests.

:class:`ToolCallRecorder` and the adapter tests need a tool that behaves
like ToolUniverse's `OpenTargets_search_targets` but returns a pinned
response. This module ships that tool so the adapter family can be
exercised without ToolUniverse installed.
"""

from __future__ import annotations

from copy import deepcopy
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
        return {
            "data": {
                # Deep copy: a caller that post-processes the result in
                # place must not edit the pinned payload every later
                # call reads from.
                "search": deepcopy(MOCK_OPEN_TARGETS_PAYLOAD["search"]),
                "args_echo": dict(sorted(kwargs.items())),
            },
            "metadata": {"observed_at_call_time": True},
            "source_version": MOCK_OPEN_TARGETS_PAYLOAD["_source_version"],
        }
