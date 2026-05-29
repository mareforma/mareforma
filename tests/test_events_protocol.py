"""Conformance tests for :mod:`mareforma.events`."""

from __future__ import annotations

from mareforma.events import (
    ClaimResult,
    EventHandler,
    EventPayload,
    EventSource,
)


def test_handler_runtime_checkable():
    """Anything implementing ``handle_event`` satisfies EventHandler."""

    class SilentHandler:
        def handle_event(self, payload):
            return {"claim_id": None, "emitted": False, "error": None}

    assert isinstance(SilentHandler(), EventHandler)


def test_handler_runtime_checkable_negative():
    class NotAHandler:
        pass

    assert not isinstance(NotAHandler(), EventHandler)


def test_source_runtime_checkable():
    class StubSource:
        def __init__(self):
            self._handlers = []

        def subscribe(self, handler):
            self._handlers.append(handler)

        def unsubscribe(self, handler):
            self._handlers.remove(handler)

    assert isinstance(StubSource(), EventSource)


def test_source_partial_implementation_rejected():
    class HalfSource:
        def subscribe(self, handler):
            pass
        # Missing unsubscribe.

    assert not isinstance(HalfSource(), EventSource)


def test_payload_and_result_shapes_are_typed_dicts():
    payload: EventPayload = {
        "source": "stub",
        "event_type": "test.event",
        "data": {"k": "v"},
        "timestamp": "2026-05-30T00:00:00Z",
    }
    result: ClaimResult = {
        "claim_id": "abc123",
        "emitted": True,
        "error": None,
    }

    assert payload["source"] == "stub"
    assert result["emitted"] is True


def test_end_to_end_dispatch():
    """Wire one handler through one source and assert delivery."""
    received: list[EventPayload] = []

    class CollectHandler:
        def handle_event(self, payload: EventPayload) -> ClaimResult:
            received.append(payload)
            return {"claim_id": "x", "emitted": True, "error": None}

    class Source:
        def __init__(self):
            self._handlers: list[EventHandler] = []

        def subscribe(self, handler):
            self._handlers.append(handler)

        def unsubscribe(self, handler):
            self._handlers.remove(handler)

        def fire(self, payload):
            results = []
            for h in self._handlers:
                results.append(h.handle_event(payload))
            return results

    src = Source()
    handler = CollectHandler()
    src.subscribe(handler)

    payload: EventPayload = {
        "source": "test",
        "event_type": "fire",
        "data": {"n": 1},
        "timestamp": "2026-05-30T00:00:00Z",
    }
    results = src.fire(payload)

    assert len(received) == 1
    assert received[0]["data"] == {"n": 1}
    assert results == [{"claim_id": "x", "emitted": True, "error": None}]

    src.unsubscribe(handler)
    src.fire(payload)
    assert len(received) == 1  # unsubscribe took effect
