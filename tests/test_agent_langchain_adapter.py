"""
tests/test_agent_langchain_adapter.py — LangChainAdapter unit tests.

Tests translation of LangChain callback events into AgentEvent:
  - on_llm_end calls observer with event_type='llm_call' and status='success'
  - on_tool_end calls observer with event_type='tool_call' and status='success'
  - on_llm_error calls observer with status='failed'
  - on_tool_error calls observer with status='failed'
  - on_chain_end calls observer with event_type='chain_step'
  - raises ImportError with clear message when langchain is not installed

LangChain is not required to be installed. All tests mock the LangChain
import so the test suite runs without langchain in the environment.
"""

from __future__ import annotations

import sys
import types
import uuid
from unittest.mock import MagicMock, patch

import pytest

from mareforma.agent._observer import AgentEvent


# ---------------------------------------------------------------------------
# LangChain mock setup
#
# We create a minimal mock of the langchain.callbacks.base module so tests
# run without langchain installed. The adapter gates the import with
# try/except, so we patch sys.modules before importing the adapter.
# ---------------------------------------------------------------------------

def _install_langchain_mock():
    """Install a minimal langchain mock into sys.modules."""
    # Base class that LangChainAdapter inherits from
    class BaseCallbackHandler:
        def __init__(self): pass

    lc_mod = types.ModuleType("langchain")
    lc_callbacks = types.ModuleType("langchain.callbacks")
    lc_base = types.ModuleType("langchain.callbacks.base")
    lc_schema = types.ModuleType("langchain.schema")

    lc_base.BaseCallbackHandler = BaseCallbackHandler
    lc_schema.LLMResult = object

    lc_mod.callbacks = lc_callbacks
    lc_callbacks.base = lc_base

    sys.modules.setdefault("langchain", lc_mod)
    sys.modules.setdefault("langchain.callbacks", lc_callbacks)
    sys.modules.setdefault("langchain.callbacks.base", lc_base)
    sys.modules.setdefault("langchain.schema", lc_schema)

    return BaseCallbackHandler


_install_langchain_mock()

# Now safe to import
from mareforma.agent.adapters.langchain import LangChainAdapter  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_observer(run_id: str | None = None) -> MagicMock:
    observer = MagicMock()
    observer._run_id = run_id or str(uuid.uuid4())
    return observer


def _make_run_id() -> uuid.UUID:
    return uuid.uuid4()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_langchain_adapter_on_llm_end_calls_observer() -> None:
    observer = _make_observer()
    adapter = LangChainAdapter(observer)
    lc_run_id = _make_run_id()

    # Simulate on_llm_start to register pending state
    adapter.on_llm_start(
        {"name": "gpt-4o", "id": ["gpt-4o"]},
        ["What is the drug target?"],
        run_id=lc_run_id,
    )

    response = MagicMock()
    response.dict.return_value = {"generations": [["BRCA2"]]}
    adapter.on_llm_end(response, run_id=lc_run_id)

    assert observer.on_event.call_count == 2  # start + end
    end_event: AgentEvent = observer.on_event.call_args_list[1][0][0]
    assert end_event.event_type == "llm_call"
    assert end_event.status == "success"
    assert end_event.output is not None


def test_langchain_adapter_on_tool_end_calls_observer() -> None:
    observer = _make_observer()
    adapter = LangChainAdapter(observer)
    lc_run_id = _make_run_id()

    adapter.on_tool_start(
        {"name": "depmap_query", "id": ["depmap_query"]},
        "BRCA2",
        run_id=lc_run_id,
    )
    adapter.on_tool_end("dependency score: 0.92", run_id=lc_run_id)

    assert observer.on_event.call_count == 2
    end_event: AgentEvent = observer.on_event.call_args_list[1][0][0]
    assert end_event.event_type == "tool_call"
    assert end_event.status == "success"
    assert end_event.output == {"output": "dependency score: 0.92"}


def test_langchain_adapter_on_llm_error_emits_failed_event() -> None:
    observer = _make_observer()
    adapter = LangChainAdapter(observer)
    lc_run_id = _make_run_id()

    adapter.on_llm_start(
        {"name": "gpt-4o", "id": ["gpt-4o"]},
        ["prompt"],
        run_id=lc_run_id,
    )
    adapter.on_llm_error(ValueError("rate limit"), run_id=lc_run_id)

    end_event: AgentEvent = observer.on_event.call_args_list[1][0][0]
    assert end_event.status == "failed"
    assert "rate limit" in end_event.metadata.get("error", "")


def test_langchain_adapter_on_tool_error_emits_failed_event() -> None:
    observer = _make_observer()
    adapter = LangChainAdapter(observer)
    lc_run_id = _make_run_id()

    adapter.on_tool_start(
        {"name": "pubmed_search", "id": ["pubmed_search"]},
        "BRCA2 cancer",
        run_id=lc_run_id,
    )
    adapter.on_tool_error(ConnectionError("timeout"), run_id=lc_run_id)

    end_event: AgentEvent = observer.on_event.call_args_list[1][0][0]
    assert end_event.status == "failed"
    assert end_event.event_type == "tool_call"


def test_langchain_adapter_on_chain_end_calls_observer() -> None:
    observer = _make_observer()
    adapter = LangChainAdapter(observer)
    lc_run_id = _make_run_id()

    adapter.on_chain_end({"result": "done"}, run_id=lc_run_id)

    event: AgentEvent = observer.on_event.call_args_list[0][0][0]
    assert event.event_type == "chain_step"
    assert event.status == "success"
    assert event.output == {"result": "done"}


def test_langchain_adapter_raises_import_error_without_langchain() -> None:
    """LangChainAdapter raises ImportError with a clear message if langchain absent."""
    observer = _make_observer()

    # Temporarily remove our mock from _HAS_LANGCHAIN by patching the module flag
    with patch("mareforma.agent.adapters.langchain._HAS_LANGCHAIN", False):
        with pytest.raises(ImportError, match="pip install langchain"):
            LangChainAdapter(observer)
