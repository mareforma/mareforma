"""
tests/test_agent_observer.py — MareformaObserver unit tests.

Tests AgentEvent construction and MareformaObserver behaviour:
  - agent_events table created on first use
  - event row written to graph.db
  - input payload written to artifact file
  - output payload written to artifact file
  - None output stores no output_hash
  - raises ValueError on missing run_id
  - raises FileNotFoundError outside a project directory
  - usable as context manager
  - connection closed on __exit__
  - db write failure warns and continues
"""

from __future__ import annotations

import json
import sqlite3
import uuid
import warnings
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mareforma.agent import AgentEvent, MareformaObserver
from mareforma.initializer import initialize


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ctx(root: Path, run_id: str = "") -> MagicMock:
    ctx = MagicMock()
    ctx.root = root
    ctx.run_id = run_id or str(uuid.uuid4())
    return ctx


def _make_event(run_id: str, **kwargs) -> AgentEvent:
    defaults = dict(
        event_type="llm_call",
        name="gpt-4o",
        run_id=run_id,
        status="success",
        input={"prompts": ["What is the drug target?"]},
        output={"text": "BRCA2"},
    )
    defaults.update(kwargs)
    return AgentEvent(**defaults)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_observer_creates_agent_events_table_on_first_use(tmp_path: Path) -> None:
    initialize(tmp_path)
    ctx = _make_ctx(tmp_path)

    with MareformaObserver(ctx) as observer:
        pass  # entering is sufficient — table is created in __enter__

    conn = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    assert "agent_events" in tables


def test_observer_writes_event_row_to_db(tmp_path: Path) -> None:
    initialize(tmp_path)
    ctx = _make_ctx(tmp_path)

    event = _make_event(ctx.run_id)
    with MareformaObserver(ctx) as observer:
        observer.on_event(event)

    conn = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
    row = conn.execute(
        "SELECT event_id, run_id, event_type, name, status FROM agent_events WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == event.event_id
    assert row[1] == ctx.run_id
    assert row[2] == "llm_call"
    assert row[3] == "gpt-4o"
    assert row[4] == "success"


def test_observer_writes_input_payload_to_artifact_file(tmp_path: Path) -> None:
    initialize(tmp_path)
    ctx = _make_ctx(tmp_path)

    event = _make_event(ctx.run_id, input={"prompts": ["test prompt"]})
    with MareformaObserver(ctx) as observer:
        observer.on_event(event)

    conn = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
    row = conn.execute(
        "SELECT input_hash FROM agent_events WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    conn.close()

    input_hash = row[0]
    assert input_hash is not None

    payload_path = tmp_path / ".mareforma" / "artifacts" / "agent_payloads" / f"{input_hash}.json"
    assert payload_path.exists()
    stored = json.loads(payload_path.read_bytes())
    assert stored == {"prompts": ["test prompt"]}


def test_observer_writes_output_payload_to_artifact_file(tmp_path: Path) -> None:
    initialize(tmp_path)
    ctx = _make_ctx(tmp_path)

    event = _make_event(ctx.run_id, output={"text": "BRCA2"})
    with MareformaObserver(ctx) as observer:
        observer.on_event(event)

    conn = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
    row = conn.execute(
        "SELECT output_hash FROM agent_events WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    conn.close()

    output_hash = row[0]
    assert output_hash is not None

    payload_path = tmp_path / ".mareforma" / "artifacts" / "agent_payloads" / f"{output_hash}.json"
    assert payload_path.exists()
    stored = json.loads(payload_path.read_bytes())
    assert stored == {"text": "BRCA2"}


def test_observer_stores_no_output_hash_when_output_is_none(tmp_path: Path) -> None:
    initialize(tmp_path)
    ctx = _make_ctx(tmp_path)

    event = _make_event(ctx.run_id, output=None, status="in_progress")
    with MareformaObserver(ctx) as observer:
        observer.on_event(event)

    conn = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
    row = conn.execute(
        "SELECT output_hash FROM agent_events WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    conn.close()

    assert row[0] is None


def test_observer_raises_on_missing_run_id(tmp_path: Path) -> None:
    initialize(tmp_path)

    class _FakeCtx:
        root = tmp_path
        run_id = ""

    with pytest.raises(ValueError, match="started transform run"):
        MareformaObserver(_FakeCtx())


def test_observer_raises_outside_project_directory(tmp_path: Path) -> None:
    # tmp_path has no .mareforma/ directory
    ctx = _make_ctx(tmp_path)

    with pytest.raises(FileNotFoundError, match=".mareforma"):
        MareformaObserver(ctx)


def test_observer_is_usable_as_context_manager(tmp_path: Path) -> None:
    initialize(tmp_path)
    ctx = _make_ctx(tmp_path)

    with MareformaObserver(ctx) as observer:
        assert observer is not None
        assert observer._conn is not None


def test_observer_closes_connection_on_exit(tmp_path: Path) -> None:
    initialize(tmp_path)
    ctx = _make_ctx(tmp_path)

    with MareformaObserver(ctx) as observer:
        conn = observer._conn

    # After __exit__ the connection is closed and _conn is None
    assert observer._conn is None


def test_observer_on_event_db_failure_warns_and_continues(tmp_path: Path) -> None:
    initialize(tmp_path)
    ctx = _make_ctx(tmp_path)

    event = _make_event(ctx.run_id)

    with MareformaObserver(ctx) as observer:
        # Force a db error by closing the connection manually
        observer._conn.close()
        observer._conn = None  # trigger the "called outside context manager" path

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            observer.on_event(event)

    assert len(caught) == 1
    assert "not recorded" in str(caught[0].message).lower()
