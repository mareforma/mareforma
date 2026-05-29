"""Claude Code PreToolUse hook — record agent tool calls as PROV-O activities.

Reads a JSON event from stdin, walks up from CWD to find
``.mareforma/graph.db``, writes one ``prov:Activity`` row via
:mod:`mareforma.hooks.db_activities`.

Invocation contract — Claude Code wires this hook in via
``.claude/settings.json``:

    {
      "hooks": {
        "PreToolUse": [
          {
            "matcher": "",
            "hooks": [
              {
                "type": "command",
                "command": "python -m mareforma.hooks"
              }
            ]
          }
        ]
      }
    }

Exit behaviour: always exits 0. Failures are logged to stderr but
never propagated — a non-zero exit would interrupt Claude Code's tool
call.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any

from mareforma.hooks.db_activities import record_activity


__all__ = ["find_graph_db", "main", "parse_event"]


def parse_event(fp: IO[str]) -> dict[str, Any] | None:
    """Read and parse a JSON event from a file-like object.

    Returns the parsed dict, or ``None`` on any malformed input.
    """
    try:
        text = fp.read()
        if not text.strip():
            return None
        data = json.loads(text)
        if not isinstance(data, dict):
            return None
        return data
    except Exception:
        return None


def find_graph_db(start: Path) -> Path | None:
    """Walk up the directory tree from ``start`` to find ``.mareforma/graph.db``."""
    current = start.resolve()
    while True:
        candidate = current / ".mareforma" / "graph.db"
        if candidate.exists():
            return candidate
        parent = current.parent
        if parent == current:
            return None
        current = parent


def main() -> None:
    try:
        event = parse_event(sys.stdin)
        if event is None:
            print(
                "mareforma.hooks: could not parse event from stdin",
                file=sys.stderr,
            )
            return

        db_path = find_graph_db(Path.cwd())
        if db_path is None:
            print(
                "mareforma.hooks: no .mareforma/graph.db found — "
                "skipping provenance record",
                file=sys.stderr,
            )
            return

        tool_name = event.get("tool_name", "unknown")
        tool_input = event.get("tool_input", {})
        session_id = event.get("session_id")
        started_at = datetime.now(timezone.utc).isoformat()

        # Route through the canonical mareforma.db.open_db so this
        # hook respects schema_version validation, foreign_keys
        # PRAGMA, and additive migrations. agent_activities is now
        # part of the canonical schema (_ADDITIVE_TABLES_SQL), so no
        # CREATE-on-demand is needed here.
        from mareforma.db import open_db
        project_root = db_path.parent.parent
        conn = open_db(project_root)
        try:
            record_activity(
                conn,
                tool_name=tool_name,
                tool_input=tool_input,
                session_id=session_id,
                started_at=started_at,
            )
            conn.commit()
        finally:
            conn.close()

    except Exception as exc:
        print(f"mareforma.hooks: unexpected error — {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
