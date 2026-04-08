"""
agent/_observer.py — MareformaObserver and AgentEvent.

MareformaObserver is the framework-agnostic sink for agent provenance events.
Framework-specific adapters (e.g. LangChainAdapter) translate their native
callback payloads into AgentEvent and call observer.on_event().

Usage
-----
    from mareforma.agent import MareformaObserver

    @transform("medea.run")
    def run_medea(ctx: BuildContext) -> None:
        with MareformaObserver(ctx) as observer:
            adapter = LangChainAdapter(observer)
            medea.run(query, callbacks=[adapter])

Connection lifecycle
---------------------
MareformaObserver opens its own SQLite connection on __enter__ and closes it
on __exit__. The connection uses check_same_thread=False because LangChain
dispatches some callbacks on background threads. WAL mode (already enabled
by open_db) makes this safe for single-writer use.

Payload storage
---------------
Full LLM/tool call payloads are written to
  .mareforma/artifacts/agent_payloads/<sha256>.json
graph.db stores only the SHA-256 hash. This keeps graph.db lean regardless
of context window size.

Failure behaviour
-----------------
on_event() db/file write failures: swallow + warn. The AI scientist run is
never interrupted by provenance failures. Consistent with ctx.save().
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import sqlite3
import uuid
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mareforma.agent._schema import AGENT_EVENTS_DDL


# ---------------------------------------------------------------------------
# AgentEvent — canonical provenance event (framework-agnostic)
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class AgentEvent:
    """One provenance event emitted by an AI scientist step.

    Parameters
    ----------
    event_type:
        One of: 'llm_call', 'tool_call', 'chain_step', 'custom'.
    name:
        Model name, tool name, chain name, or custom label.
    run_id:
        UUID of the parent @transform run. Links to transform_runs.run_id.
    status:
        'success' | 'failed' | 'in_progress'
    timestamp:
        UTC ISO 8601. Generated automatically if not provided.
    duration_ms:
        Wall-clock duration in milliseconds, or None if not measured.
    input:
        Serialisable dict representing the call input. Written to artifact
        file; only its SHA-256 is stored in graph.db.
    output:
        Serialisable dict representing the call output, or None if the call
        failed or is still in progress.
    metadata:
        Framework-specific extras not part of the canonical schema.
    event_id:
        UUID for this event. Auto-generated if not provided.
    """
    event_type: str
    name: str
    run_id: str
    status: str
    timestamp: str = dataclasses.field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    duration_ms: int | None = None
    input: dict = dataclasses.field(default_factory=dict)
    output: dict | None = None
    metadata: dict = dataclasses.field(default_factory=dict)
    event_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))


# ---------------------------------------------------------------------------
# MareformaObserver
# ---------------------------------------------------------------------------

class MareformaObserver:
    """Framework-agnostic sink for agent provenance events.

    Must be used as a context manager to ensure the SQLite connection is
    closed even if the AI scientist run raises.

    Parameters
    ----------
    ctx:
        The BuildContext for the enclosing @transform run.
        ctx.run_id and ctx.root must be set before constructing the observer.

    Raises
    ------
    ValueError
        If ctx.run_id is empty (transform run not yet started).
    FileNotFoundError
        If the project's .mareforma/ directory does not exist.
    """

    def __init__(self, ctx: Any) -> None:
        run_id: str = ctx.run_id
        root: Path = ctx.root

        if not run_id:
            raise ValueError(
                "MareformaObserver requires a started transform run. "
                "Construct it inside a @transform function."
            )

        mare_dir = root / ".mareforma"
        if not mare_dir.exists():
            raise FileNotFoundError(
                f"No .mareforma/ directory found at {root}. "
                "Run 'mareforma init' first."
            )

        self._run_id = run_id
        self._root = root
        self._payloads_dir = mare_dir / "artifacts" / "agent_payloads"
        self._conn: sqlite3.Connection | None = None

    @property
    def run_id(self) -> str:
        """The transform run_id this observer is recording events for."""
        return self._run_id

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "MareformaObserver":
        from mareforma.db import open_db
        self._conn = open_db(self._root)
        self._conn.executescript(AGENT_EVENTS_DDL)
        self._conn.commit()
        self._payloads_dir.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, *_: Any) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def on_event(self, event: AgentEvent) -> None:
        """Record one agent provenance event.

        Writes full payloads to .mareforma/artifacts/agent_payloads/ and
        inserts a row into agent_events with the SHA-256 hashes.

        Failures are non-fatal: a warning is emitted and the AI scientist
        run continues uninterrupted.
        """
        try:
            input_hash = self._write_payload(event.input) if event.input else None
            output_hash = self._write_payload(event.output) if event.output is not None else None

            if self._conn is None:
                warnings.warn(
                    "MareformaObserver.on_event() called outside context manager — "
                    "event not recorded.",
                    stacklevel=2,
                )
                return

            self._conn.execute(
                """
                INSERT OR IGNORE INTO agent_events
                    (event_id, run_id, event_type, name, timestamp,
                     status, duration_ms, input_hash, output_hash, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.run_id,
                    event.event_type,
                    event.name,
                    event.timestamp,
                    event.status,
                    event.duration_ms,
                    input_hash,
                    output_hash,
                    json.dumps(event.metadata) if event.metadata else None,
                ),
            )
            self._conn.commit()

        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                f"MareformaObserver: agent event not recorded ({exc})",
                stacklevel=2,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_payload(self, data: Any) -> str:
        """Serialise *data* to JSON, write to agent_payloads/, return SHA-256."""
        raw = json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")
        digest = hashlib.sha256(raw).hexdigest()
        path = self._payloads_dir / f"{digest}.json"
        if not path.exists():
            path.write_bytes(raw)
        return digest
