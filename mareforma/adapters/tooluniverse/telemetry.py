"""Telemetry: append-only health.jsonl writer.

Each completed tool call appends one line documenting the call's
shape. The sandboxed-execution dependency writes an aggregate
`health.jsonl`; the adapter's per-call telemetry is a finer-grain
stream living next to it. The writer records every call so tests
can verify nothing is dropped.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


__all__ = ["append_event"]


def append_event(path: Path, event: dict[str, Any]) -> None:
    """Append one JSON-encoded line to ``path``.

    Adds a ``ts`` field (UTC ISO 8601) if absent. Creates the parent
    directory if needed. Never raises on telemetry write failure;
    telemetry is observational, not load-bearing.
    """

    event = dict(event)
    event.setdefault("ts", datetime.now(timezone.utc).isoformat())
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, sort_keys=True))
            fh.write("\n")
    except OSError:
        # Best-effort; mareforma has the canonical record via the claim.
        pass
