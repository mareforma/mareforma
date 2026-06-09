"""``agent_activities`` table — PROV-O Activity rows for tool calls.

A separate table from the main ``claims`` table because tool-call
activities are high-volume, low-semantic-density data: every Claude
Code (or other agent) tool invocation produces a row, but most rows
never escalate into signed claims. Keeping them out of the signed
graph lets mareforma stay lean.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any


def create_activities_table(conn: sqlite3.Connection) -> None:
    """Create the ``agent_activities`` table if missing."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_activities (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id  TEXT,
            tool_name   TEXT NOT NULL,
            tool_input  TEXT NOT NULL,
            started_at  TEXT NOT NULL,
            prov_type   TEXT NOT NULL DEFAULT 'prov:Activity'
        )
        """
    )


def record_activity(
    conn: sqlite3.Connection,
    tool_name: str,
    tool_input: Any,
    session_id: str | None,
    started_at: str,
) -> None:
    """Insert one PROV-O Activity row for a tool call."""
    conn.execute(
        """
        INSERT INTO agent_activities
            (session_id, tool_name, tool_input, started_at, prov_type)
        VALUES (?, ?, ?, ?, 'prov:Activity')
        """,
        (session_id, tool_name, json.dumps(tool_input), started_at),
    )
