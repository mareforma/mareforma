"""Conformance tests for :mod:`mareforma.events`.

Three conceptual clusters:

- :class:`TestEventHandlerProtocol` — runtime-checkable EventHandler.
- :class:`TestEventSourceProtocol` — runtime-checkable EventSource +
  partial-implementation rejection.
- :class:`TestTypedShapes` — EventPayload + ClaimResult TypedDicts.
- :class:`TestEndToEndDispatch` — wire one handler through one source.
- :class:`TestSourceNameConstants` — adapters dispatch on constants,
  never string literals (mirrors the predicate-URI constants pattern).
"""

from __future__ import annotations

from mareforma.events import (
    ClaimResult,
    EventHandler,
    EventPayload,
    EventSource,
)


class TestEventHandlerProtocol:
    def test_runtime_checkable_positive(self):
        """Anything implementing ``handle_event`` satisfies EventHandler."""

        class SilentHandler:
            def handle_event(self, payload):
                return {"claim_id": None, "emitted": False, "error": None}

        assert isinstance(SilentHandler(), EventHandler)

    def test_runtime_checkable_negative(self):
        class NotAHandler:
            pass

        assert not isinstance(NotAHandler(), EventHandler)


class TestEventSourceProtocol:
    def test_runtime_checkable_positive(self):
        class StubSource:
            def __init__(self):
                self._handlers = []

            def subscribe(self, handler):
                self._handlers.append(handler)

            def unsubscribe(self, handler):
                self._handlers.remove(handler)

        assert isinstance(StubSource(), EventSource)

    def test_partial_implementation_rejected(self):
        class HalfSource:
            def subscribe(self, handler):
                pass
            # Missing unsubscribe.

        assert not isinstance(HalfSource(), EventSource)


class TestTypedShapes:
    def test_payload_and_result_shapes_are_typed_dicts(self):
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


class TestEndToEndDispatch:
    def test_wire_one_handler_through_one_source(self):
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
                return [h.handle_event(payload) for h in self._handlers]

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


class TestSourceNameConstants:
    def test_constants_distinct_and_known(self):
        """Adapters must use constants, not literals, for source identifiers."""
        from mareforma.events import (
            KNOWN_SOURCES,
            SOURCE_CLAWINSTITUTE,
            SOURCE_CLAUDE_CODE_PRETOOLUSE,
            SOURCE_GEMINI,
            SOURCE_TOOLUNIVERSE,
        )
        sources = {
            SOURCE_CLAWINSTITUTE, SOURCE_TOOLUNIVERSE,
            SOURCE_GEMINI, SOURCE_CLAUDE_CODE_PRETOOLUSE,
        }
        assert len(sources) == 4
        assert sources == set(KNOWN_SOURCES)
        for s in sources:
            assert isinstance(s, str) and s == s.lower()
