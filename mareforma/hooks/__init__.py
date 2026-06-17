"""Hooks for ambient agent-tool-call provenance recording.

Provides a Claude Code PreToolUse hook (``mareforma.hooks.agent_hook``)
that records every tool invocation as a ``prov:Activity`` row in the
project's ``.mareforma/graph.db``. Opt-in via ``.claude/settings.json``
(snippet in ``mareforma.hooks.agent_hook`` docstring).

Recorded activities live in the ``agent_activities`` table, separate
from the signed ``claims`` table because per-call activities are
high-volume, low-semantic-density data that most never escalate into
signed claims.
"""

from __future__ import annotations

from mareforma.hooks.agent_hook import find_graph_db, main, parse_event
from mareforma.hooks.db_activities import (
    create_activities_table,
    record_activity,
)


__all__ = [
    "create_activities_table",
    "find_graph_db",
    "main",
    "parse_event",
    "record_activity",
]
