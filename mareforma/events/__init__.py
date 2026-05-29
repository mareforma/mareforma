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

__all__ = [
    "ClaimResult",
    "EventHandler",
    "EventPayload",
    "EventSource",
]
