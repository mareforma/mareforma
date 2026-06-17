"""Structural types for the Tool integration contract."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, TypedDict, runtime_checkable


__all__ = [
    "PredicateBoundaryError",
    "ReplayResult",
    "Tool",
    "ToolCallError",
    "ToolResult",
]


class ToolCallError(Exception):
    """Raised when a wrapped tool's ``call(**kwargs)`` raises.

    Adapters that wrap user code re-raise the underlying error with
    this typed parent so a caller can pattern-match on a stable class
    while still inspecting ``__cause__`` for the original error.
    """


class PredicateBoundaryError(ValueError):
    """Raised when canonical predicate JSON would contain a tag-boundary marker.

    Tag-bracketed predicate encodings (e.g. ``<predicate tool-call v1>…
    </predicate>``) refuse to write a payload that contains the close
    marker rather than risk a smuggled-tag ambiguity at decode.
    """


@runtime_checkable
class Tool(Protocol):
    """Structural shape every wrappable tool callable must satisfy.

    Minimum contract:

    - ``name``: stable identifier for this tool.
    - ``version``: version string (typically the source package's
      ``__version__``).
    - ``call(**kwargs) -> ToolResult``: synchronous invocation.

    A tool that has these attributes/methods satisfies the Protocol
    without inheriting from it (``@runtime_checkable`` makes
    ``isinstance`` work).
    """

    name: str
    version: str

    def call(self, **kwargs: Any) -> "ToolResult": ...


class ToolResult(TypedDict, total=False):
    """Structured tool-call result.

    ``data``
        The call output, already canonicalizable. Tools that produce
        non-canonical types (numpy arrays, custom dataclasses) must
        convert before returning.

    ``metadata``
        Opaque per-call info that does NOT contribute to the result
        digest (cache state, timing, observed source version).

    ``source_version``
        When the underlying data source surfaces a version (e.g. an
        external database release tag), the tool records it. ``None``
        is the honest absence signal; the predicate carries ``null``,
        not a fabricated placeholder.
    """

    data: Any
    metadata: dict[str, Any]
    source_version: str | None


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Outcome of a replay check against a recorded tool-call claim.

    ``ok`` is True iff every checked field (arguments digest, result
    digest, tool_config_fingerprint, tool_version) matched the
    predicate's pinned values byte-for-byte.

    ``diff_fields`` is the ordered list of field names that did not
    match (empty when ``ok=True``). The order matches the field check
    order so a downstream consumer can route on the first mismatch.
    """

    ok: bool
    observed_result_digest: str
    expected_result_digest: str
    diff_fields: tuple[str, ...] = field(default_factory=tuple)
