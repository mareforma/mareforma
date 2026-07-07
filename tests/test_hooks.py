"""Tests for :mod:`mareforma.hooks` — Claude Code PreToolUse hook.

Conceptual clusters:

- :class:`TestParseEvent` — JSON parsing of stdin payloads.
- :class:`TestFindGraphDb` — walk-up resolution of .mareforma/graph.db.
- :class:`TestAgentActivitiesTable` — the record path against the real
  ``open_db`` schema, exercised end-to-end through ``python -m
  mareforma.hooks``.
"""

from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

from mareforma.db import open_db
from mareforma.hooks import (
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
    def test_record_activity_uses_canonical_schema(self, tmp_path: Path):
        """record_activity inserts into the agent_activities table that
        open_db creates from the canonical schema — no hand-rolled DDL."""
        conn = open_db(tmp_path)
        try:
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


class TestHookMainEndToEnd:
    def test_python_m_mareforma_hooks_records_a_row(self, tmp_path: Path):
        """Run the hook exactly as Claude Code does — `python -m
        mareforma.hooks`, event on stdin — and read the row back through a
        real open_db connection. This exercises main()'s open_db path and
        proves the canonical schema stands on its own without any
        hand-duplicated CREATE TABLE."""
        open_db(tmp_path).close()  # bootstrap .mareforma/graph.db

        event = json.dumps({
            "tool_name": "Bash",
            "tool_input": {"command": "ls -la"},
            "session_id": "sess-e2e",
        })
        proc = subprocess.run(
            [sys.executable, "-m", "mareforma.hooks"],
            input=event, text=True, capture_output=True, cwd=str(tmp_path),
        )
        assert proc.returncode == 0, proc.stderr

        conn = open_db(tmp_path)
        try:
            rows = list(conn.execute(
                "SELECT tool_name, tool_input, session_id FROM agent_activities"
            ))
        finally:
            conn.close()
        assert len(rows) == 1
        assert rows[0][0] == "Bash"
        assert json.loads(rows[0][1]) == {"command": "ls -la"}
        assert rows[0][2] == "sess-e2e"

    def test_hook_is_a_noop_without_a_graph_db(self, tmp_path: Path):
        """No .mareforma/graph.db in the tree → the hook exits 0 and writes
        nothing, never interrupting the host tool call."""
        event = json.dumps({"tool_name": "Bash", "tool_input": {}})
        proc = subprocess.run(
            [sys.executable, "-m", "mareforma.hooks"],
            input=event, text=True, capture_output=True, cwd=str(tmp_path),
        )
        assert proc.returncode == 0
        assert not (tmp_path / ".mareforma").exists()
