"""ToolUniverse adapter: wrap any Tool so each call emits a signed claim.

ToolUniverse exposes 600+ biomedical tools as callables; this adapter
wraps any object satisfying :class:`mareforma.tools.Tool` so each
``call(**kwargs)`` records a ``urn:mareforma:predicate:tool-call:v1``
claim with the arguments digest, result digest, tool config
fingerprint, timing, and optional role.

Public surface:

- :class:`ProvenanceToolAdapter`: the load-bearing wrapper. Wrap a
  Tool with this and use it as you would the underlying tool;
  ``.call(**kwargs)`` returns the same ``ToolResult`` shape while
  asserting a signed claim as a side effect.
- :func:`build_tool_call_predicate` / :func:`encode_predicate_into_text` /
  :func:`decode_predicate_from_text`: for callers constructing or
  verifying the embedded predicate by hand.
- :func:`verify_tool_call_envelope`: re-verify a recorded tool-call
  claim's DSSE envelope and assert the predicate is well-formed.
- :class:`ToolCallRecorder`: minimal shim around ProvenanceToolAdapter
  for the cross-adapter coexistence test (uses an in-memory demo Tool).

Install: ``pip install mareforma[tooluniverse]``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mareforma.adapters.tooluniverse.adapter import (
    MAX_RESULT_BYTES_DEFAULT,
    MissingToolVersionWarning,
    ProvenanceToolAdapter,
    ResultTooLargeError,
)
from mareforma.adapters.tooluniverse.predicate import (
    PREDICATE_TAG_CLOSE,
    PREDICATE_TAG_OPEN,
    PREDICATE_TYPE_V1,
    build_tool_call_predicate,
    decode_predicate_from_text,
    encode_predicate_into_text,
    verify_tool_call_envelope,
)
from mareforma.adapters.tooluniverse.demo_tool import OpenTargetsSearchTargetsMock


if TYPE_CHECKING:
    from mareforma._graph import EpistemicGraph


__all__ = [
    "MAX_RESULT_BYTES_DEFAULT",
    "MissingToolVersionWarning",
    "PREDICATE_TAG_CLOSE",
    "PREDICATE_TAG_OPEN",
    "PREDICATE_TYPE_V1",
    "ProvenanceToolAdapter",
    "ResultTooLargeError",
    "ToolCallRecorder",
    "build_tool_call_predicate",
    "decode_predicate_from_text",
    "encode_predicate_into_text",
    "verify_tool_call_envelope",
]


class ToolCallRecorder:
    """Convention wrapper used by the cross-adapter coexistence test.

    Real callers should use :class:`ProvenanceToolAdapter` directly,
    passing their own :class:`mareforma.tools.Tool` instance. This
    shim constructs a Demo tool internally so the adapter family can
    be tested without external dependencies.
    """

    def __init__(self, *, graph: "EpistemicGraph | None" = None) -> None:
        self._graph = graph
        self._adapter: ProvenanceToolAdapter | None = None
        if graph is not None:
            self._adapter = ProvenanceToolAdapter(
                tool=OpenTargetsSearchTargetsMock(), graph=graph,
            )

    def predicate_uris(self) -> tuple[str, ...]:
        return (PREDICATE_TYPE_V1,)

    def emit_sample(self) -> str:
        if self._adapter is None:
            raise RuntimeError(
                "emit_sample() needs the ToolCallRecorder to be "
                "constructed with a graph=... argument"
            )
        result = self._adapter.call(target="EGFR")
        return result["metadata"]["mareforma_claim_id"]
