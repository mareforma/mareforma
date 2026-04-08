"""mareforma.agent — framework-agnostic agent provenance instrumentation.

Public API
----------
    AgentEvent        — canonical provenance event dataclass
    MareformaObserver — context manager that records events to graph.db

Adapters
--------
    mareforma.agent.adapters.langchain.LangChainAdapter
"""

from mareforma.agent._observer import AgentEvent, MareformaObserver

__all__ = ["AgentEvent", "MareformaObserver"]
