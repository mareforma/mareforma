"""Tests for :mod:`mareforma.hooks` — Claude Code PreToolUse hook.

Conceptual clusters:

- :class:`TestParseEvent` — JSON parsing of stdin payloads.
- :class:`TestFindGraphDb` — walk-up resolution of .mareforma/graph.db.
- :class:`TestAgentActivitiesTable` — DDL + INSERT contract for the
  ``agent_activities`` table.
"""

from __future__ import annotations

import io
import json
import sqlite3
from pathlib import Path

from mareforma.hooks import (
    create_activities_table,
    find_graph_db,
    parse_event,
    record_activity,
)


class TestParseEvent:
    def test_valid_json(self):
        event = parse_event(io.StringIO(
            '{"tool_name": "Bash", "tool_input": {"command": "ls"}}'
        ))
        assert event == {"tool_name": "Bash", "tool_input": {"command": "ls"}}

    def test_empty(self):
        assert parse_event(io.StringIO("")) is None

    def test_whitespace_only(self):
        assert parse_event(io.StringIO("   \n  ")) is None

    def test_invalid_json(self):
        assert parse_event(io.StringIO("not json")) is None

    def test_non_dict(self):
        assert parse_event(io.StringIO("[1, 2, 3]")) is None


class TestFindGraphDb:
    def test_walks_up_directory_tree(self, tmp_path: Path):
        project = tmp_path / "project"
        nested = project / "src" / "deep"
        nested.mkdir(parents=True)
        db_dir = project / ".mareforma"
        db_dir.mkdir()
        db_file = db_dir / "graph.db"
        db_file.touch()

        found = find_graph_db(nested)
        assert found == db_file.resolve()

    def test_returns_none_when_absent(self, tmp_path: Path):
        assert find_graph_db(tmp_path) is None


class TestAgentActivitiesTable:
    def test_create_and_record(self, tmp_path: Path):
        db_path = tmp_path / "graph.db"
        conn = sqlite3.connect(str(db_path))
        try:
            create_activities_table(conn)
            record_activity(
                conn,
                tool_name="Bash",
                tool_input={"command": "ls"},
                session_id="sess-1",
                started_at="2026-05-30T00:00:00Z",
            )
            conn.commit()

            rows = list(conn.execute(
                "SELECT tool_name, tool_input, session_id, started_at, prov_type "
                "FROM agent_activities"
            ))
            assert len(rows) == 1
            tool_name, tool_input, session_id, started_at, prov_type = rows[0]
            assert tool_name == "Bash"
            assert json.loads(tool_input) == {"command": "ls"}
            assert session_id == "sess-1"
            assert started_at == "2026-05-30T00:00:00Z"
            assert prov_type == "prov:Activity"
        finally:
            conn.close()

    def test_create_is_idempotent(self, tmp_path: Path):
        db_path = tmp_path / "graph.db"
        conn = sqlite3.connect(str(db_path))
        try:
            create_activities_table(conn)
            create_activities_table(conn)  # second call must not raise
            conn.commit()
        finally:
            conn.close()
