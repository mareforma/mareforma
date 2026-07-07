"""``agent_activities`` table: PROV-O Activity rows for tool calls.

A separate table from the main ``claims`` table because tool-call
activities are high-volume, low-semantic-density data: every Claude
Code (or other agent) tool invocation produces a row, but most rows
never escalate into signed claims. Keeping them out of the signed
graph lets mareforma stay lean.

The table's DDL lives once, in the canonical schema
(``mareforma.db._schema_sql``), and is created by ``open_db``. This
module only inserts rows; it does not carry a second copy of the CREATE
statement to drift out of sync with the canonical schema.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any


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
