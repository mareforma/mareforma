"""
agent/adapters/langchain.py — LangChain adapter for MareformaObserver.

Translates LangChain callback events into AgentEvent and forwards them
to a MareformaObserver. Pass a LangChainAdapter to any LangChain chain,
agent, or tool that accepts a callbacks= argument.

Usage
-----
    from mareforma.agent import MareformaObserver
    from mareforma.agent.adapters.langchain import LangChainAdapter

    @transform("medea.run")
    def run_medea(ctx: BuildContext) -> None:
        with MareformaObserver(ctx) as observer:
            adapter = LangChainAdapter(observer)
            medea.run(query, callbacks=[adapter])

LangChain is an optional dependency. Importing this module succeeds even
without langchain installed; instantiating LangChainAdapter raises
ImportError with a clear install message.

Hooks implemented
-----------------
    on_llm_start    — record LLM call start (in_progress)
    on_llm_end      — update with response (success)
    on_llm_error    — update with error (failed)
    on_tool_start   — record tool call start (in_progress)
    on_tool_end     — update with output (success)
    on_tool_error   — update with error (failed)
    on_chain_end    — record chain completion (success)
    on_chain_error  — record chain error (failed)
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Union

try:
    from langchain_core.callbacks.base import BaseCallbackHandler
    _HAS_LANGCHAIN = True
except ImportError:
    _HAS_LANGCHAIN = False
    BaseCallbackHandler = object  # type: ignore[assignment,misc]

from mareforma.agent._observer import AgentEvent, MareformaObserver


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class LangChainAdapter(BaseCallbackHandler):  # type: ignore[misc]
    """LangChain BaseCallbackHandler that records events to MareformaObserver.

    Parameters
    ----------
    observer:
        An active MareformaObserver (must be used inside its context manager).

    Raises
    ------
    ImportError
        If langchain is not installed.
    """

    def __init__(self, observer: MareformaObserver) -> None:
        if not _HAS_LANGCHAIN:
            raise ImportError(
                "langchain is required to use LangChainAdapter. "
                "Install it with: pip install langchain"
            )
        super().__init__()
        self._observer = observer
        # Track in-progress calls: run_id (LC) → (event_id, start_time)
        self._pending: dict[str, tuple[str, float]] = {}

    # ------------------------------------------------------------------
    # LLM hooks
    # ------------------------------------------------------------------

    def on_llm_start(
        self,
        serialized: dict[str, Any],
        prompts: list[str],
        *,
        run_id: uuid.UUID,
        **kwargs: Any,
    ) -> None:
        event_id = str(uuid.uuid4())
        self._pending[str(run_id)] = (event_id, time.monotonic())
        self._observer.on_event(AgentEvent(
            event_id=event_id,
            event_type="llm_call",
            name=serialized.get("name") or serialized.get("id", ["unknown"])[-1],
            run_id=self._observer.run_id,
            status="in_progress",
            timestamp=_now(),
            input={"prompts": prompts, "serialized": serialized},
            metadata={"lc_run_id": str(run_id), **kwargs},
        ))

    def on_llm_end(
        self,
        response: Any,
        *,
        run_id: uuid.UUID,
        **kwargs: Any,
    ) -> None:
        key = str(run_id)
        event_id, start = self._pending.pop(key, (str(uuid.uuid4()), time.monotonic()))
        duration_ms = round((time.monotonic() - start) * 1000)

        try:
            output = response.dict() if hasattr(response, "dict") else {"text": str(response)}
        except Exception:  # noqa: BLE001
            output = {"text": str(response)}

        self._observer.on_event(AgentEvent(
            event_id=event_id,
            event_type="llm_call",
            name=kwargs.get("name", "llm"),
            run_id=self._observer.run_id,
            status="success",
            timestamp=_now(),
            duration_ms=duration_ms,
            output=output,
            metadata={"lc_run_id": str(run_id)},
        ))

    def on_llm_error(
        self,
        error: Union[Exception, KeyboardInterrupt],
        *,
        run_id: uuid.UUID,
        **kwargs: Any,
    ) -> None:
        key = str(run_id)
        event_id, start = self._pending.pop(key, (str(uuid.uuid4()), time.monotonic()))
        duration_ms = round((time.monotonic() - start) * 1000)

        self._observer.on_event(AgentEvent(
            event_id=event_id,
            event_type="llm_call",
            name=kwargs.get("name", "llm"),
            run_id=self._observer.run_id,
            status="failed",
            timestamp=_now(),
            duration_ms=duration_ms,
            metadata={"lc_run_id": str(run_id), "error": str(error)},
        ))

    # ------------------------------------------------------------------
    # Tool hooks
    # ------------------------------------------------------------------

    def on_tool_start(
        self,
        serialized: dict[str, Any],
        input_str: str,
        *,
        run_id: uuid.UUID,
        **kwargs: Any,
    ) -> None:
        event_id = str(uuid.uuid4())
        self._pending[str(run_id)] = (event_id, time.monotonic())
        self._observer.on_event(AgentEvent(
            event_id=event_id,
            event_type="tool_call",
            name=serialized.get("name") or serialized.get("id", ["unknown"])[-1],
            run_id=self._observer.run_id,
            status="in_progress",
            timestamp=_now(),
            input={"input": input_str, "serialized": serialized},
            metadata={"lc_run_id": str(run_id)},
        ))

    def on_tool_end(
        self,
        output: str,
        *,
        run_id: uuid.UUID,
        **kwargs: Any,
    ) -> None:
        key = str(run_id)
        event_id, start = self._pending.pop(key, (str(uuid.uuid4()), time.monotonic()))
        duration_ms = round((time.monotonic() - start) * 1000)

        self._observer.on_event(AgentEvent(
            event_id=event_id,
            event_type="tool_call",
            name=kwargs.get("name", "tool"),
            run_id=self._observer.run_id,
            status="success",
            timestamp=_now(),
            duration_ms=duration_ms,
            output={"output": output},
            metadata={"lc_run_id": str(run_id)},
        ))

    def on_tool_error(
        self,
        error: Union[Exception, KeyboardInterrupt],
        *,
        run_id: uuid.UUID,
        **kwargs: Any,
    ) -> None:
        key = str(run_id)
        event_id, start = self._pending.pop(key, (str(uuid.uuid4()), time.monotonic()))
        duration_ms = round((time.monotonic() - start) * 1000)

        self._observer.on_event(AgentEvent(
            event_id=event_id,
            event_type="tool_call",
            name=kwargs.get("name", "tool"),
            run_id=self._observer.run_id,
            status="failed",
            timestamp=_now(),
            duration_ms=duration_ms,
            metadata={"lc_run_id": str(run_id), "error": str(error)},
        ))

    # ------------------------------------------------------------------
    # Chain hooks
    # ------------------------------------------------------------------

    def on_chain_end(
        self,
        outputs: dict[str, Any],
        *,
        run_id: uuid.UUID,
        **kwargs: Any,
    ) -> None:
        self._observer.on_event(AgentEvent(
            event_type="chain_step",
            name=kwargs.get("name", "chain"),
            run_id=self._observer.run_id,
            status="success",
            timestamp=_now(),
            output=outputs,
            metadata={"lc_run_id": str(run_id)},
        ))

    def on_chain_error(
        self,
        error: Union[Exception, KeyboardInterrupt],
        *,
        run_id: uuid.UUID,
        **kwargs: Any,
    ) -> None:
        self._observer.on_event(AgentEvent(
            event_type="chain_step",
            name=kwargs.get("name", "chain"),
            run_id=self._observer.run_id,
            status="failed",
            timestamp=_now(),
            metadata={"lc_run_id": str(run_id), "error": str(error)},
        ))
