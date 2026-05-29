"""Tests for :mod:`mareforma.hooks` — Claude Code PreToolUse hook."""

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


def test_parse_event_valid_json():
    event = parse_event(io.StringIO(
        '{"tool_name": "Bash", "tool_input": {"command": "ls"}}'
    ))
    assert event == {"tool_name": "Bash", "tool_input": {"command": "ls"}}


def test_parse_event_empty():
    assert parse_event(io.StringIO("")) is None


def test_parse_event_whitespace_only():
    assert parse_event(io.StringIO("   \n  ")) is None


def test_parse_event_invalid_json():
    assert parse_event(io.StringIO("not json")) is None


def test_parse_event_non_dict():
    assert parse_event(io.StringIO("[1, 2, 3]")) is None


def test_find_graph_db_walks_up(tmp_path: Path):
    project = tmp_path / "project"
    nested = project / "src" / "deep"
    nested.mkdir(parents=True)
    db_dir = project / ".mareforma"
    db_dir.mkdir()
    db_file = db_dir / "graph.db"
    db_file.touch()

    found = find_graph_db(nested)
    assert found == db_file.resolve()


def test_find_graph_db_returns_none_when_absent(tmp_path: Path):
    assert find_graph_db(tmp_path) is None


def test_create_and_record_activity(tmp_path: Path):
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


def test_create_activities_table_is_idempotent(tmp_path: Path):
    db_path = tmp_path / "graph.db"
    conn = sqlite3.connect(str(db_path))
    try:
        create_activities_table(conn)
        create_activities_table(conn)  # second call must not raise
        conn.commit()
    finally:
        conn.close()
