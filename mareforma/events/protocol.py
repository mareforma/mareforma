"""Event Protocol types — see :mod:`mareforma.events` for the contract."""

from __future__ import annotations

from typing import Any, Protocol, TypedDict, runtime_checkable


class EventPayload(TypedDict):
    """One inbound event from an adapter's upstream source.

    ``source`` identifies the adapter (e.g. ``"clawinstitute"``,
    ``"tooluniverse"``, ``"gemini"``). ``event_type`` is
    adapter-defined and routes the payload to the right handler logic
    (e.g. ``"post.created"`` for a ClawInstitute post).
    """

    source: str
    event_type: str
    data: dict[str, Any]
    timestamp: str  # ISO 8601 UTC


class ClaimResult(TypedDict):
    """Outcome of handling one event.

    ``emitted=False`` with ``error=None`` is the deliberate skip case
    (event was understood but did not warrant a claim). ``error`` is a
    short human-readable string when the handler raised but the
    adapter wanted to continue the stream.
    """

    claim_id: str | None
    emitted: bool
    error: str | None


@runtime_checkable
class EventHandler(Protocol):
    """Callable that turns one event into zero or one claim."""

    def handle_event(self, payload: EventPayload) -> ClaimResult:
        ...


@runtime_checkable
class EventSource(Protocol):
    """An adapter that emits events to one or more handlers.

    Subscription is push-style: the adapter calls every subscribed
    handler for each event. Adapters MAY buffer or batch internally;
    subscribers SHOULD treat ``handle_event`` as the unit of work.
    """

    def subscribe(self, handler: EventHandler) -> None:
        ...

    def unsubscribe(self, handler: EventHandler) -> None:
        ...
