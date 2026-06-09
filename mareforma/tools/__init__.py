"""Tool Protocol — the integration shape for tool-call adapters.

Any object satisfying :class:`Tool` can be wrapped by an adapter that
emits ``tool-call/v1`` claims. The Protocol is structural; tools do
not subclass anything to satisfy it. The core ships the contract
here; adapters in :mod:`mareforma.adapters` ship the bindings.

Three concepts:

- :class:`Tool` — the wrappable callable shape: ``name``, ``version``,
  ``call(**kwargs) -> ToolResult``.
- :class:`ToolResult` — structured result with optional metadata + a
  source-version hint that gets recorded in the predicate.
- :class:`ReplayResult` — the result of a deterministic re-execution
  check against a recorded claim: ``ok`` plus the ordered list of
  fields that drifted.
"""

from __future__ import annotations

from mareforma.tools.protocol import (
    PredicateBoundaryError,
    ReplayResult,
    Tool,
    ToolCallError,
    ToolResult,
)


__all__ = [
    "PredicateBoundaryError",
    "ReplayResult",
    "Tool",
    "ToolCallError",
    "ToolResult",
]
