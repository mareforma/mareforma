"""Typed Protocol contract for adapter event sources.

An adapter that wants to translate external events into mareforma
claims implements :class:`EventSource` and accepts an
:class:`EventHandler`. The handler is invoked once per inbound event
and returns a :class:`ClaimResult` describing whether a claim was
emitted (and, if so, its id).

The contract is intentionally minimal: payloads are typed dicts so
adapters don't need a shared object model, and ``ClaimResult``
distinguishes ``emitted=False`` from an error so an adapter can
deliberately skip events without raising.
"""

from __future__ import annotations

from mareforma.events.protocol import (
    ClaimResult,
    EventHandler,
    EventPayload,
    EventSource,
)


# Canonical source-name constants. EventPayload.source is a string for
# wire-format compatibility, but adapters and handlers MUST use these
# constants instead of string literals — a typo on a literal silently
# routes events to the wrong handler with no type-check or runtime
# error. Mirrors the predicate-URI constants pattern.
SOURCE_CLAWINSTITUTE = "clawinstitute"
SOURCE_TOOLUNIVERSE = "tooluniverse"
SOURCE_GEMINI = "gemini"
SOURCE_CLAUDE_CODE_PRETOOLUSE = "claude-code-pretooluse"


KNOWN_SOURCES: frozenset[str] = frozenset({
    SOURCE_CLAWINSTITUTE,
    SOURCE_TOOLUNIVERSE,
    SOURCE_GEMINI,
    SOURCE_CLAUDE_CODE_PRETOOLUSE,
})


__all__ = [
    "ClaimResult",
    "EventHandler",
    "EventPayload",
    "EventSource",
    "KNOWN_SOURCES",
    "SOURCE_CLAWINSTITUTE",
    "SOURCE_TOOLUNIVERSE",
    "SOURCE_GEMINI",
    "SOURCE_CLAUDE_CODE_PRETOOLUSE",
]
