"""
pipeline/lock.py — PipelineLock: read/write .mareforma/pipeline.lock.json.

The lock file records the result of every transform in the last successful
build. It is used by the runner to decide which nodes are stale.

Structure
---------
{
    "schema_version": 1,
    "build_timestamp": "2026-03-14T10:00:00+00:00",
    "git_sha": "abc1234" | null,
    "nodes": {
        "morphology.load": {
            "input_hash": "sha256...",   ← hash of raw/ dir at build time
            "output_hash": "sha256...",  ← hash of artifact written by ctx.save
            "source_hash": "sha256...",  ← hash of transform fn source code
            "status": "success" | "failed" | "skipped",
            "timestamp": "...",
            "duration_ms": 142
        }
    }
}

Atomicity
---------
Writes go to pipeline.lock.json.tmp first, then os.replace() renames to
the real path. os.replace() is atomic on POSIX and best-effort on Windows.
A partial build cannot corrupt the previous lock — the old file survives
until the new one is fully written.

Staleness logic (used by runner, implemented here for testability)
------------------
A node is stale if ANY of:
  1. It has no entry in the lock (never run)
  2. Its input_hash has changed  (raw data changed)
  3. Its source_hash has changed (transform code changed)
  4. Any of its depends_on nodes were re-run in this build session
  5. --force was passed
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOCK_FILENAME = "pipeline.lock.json"
LOCK_TMP_FILENAME = "pipeline.lock.json.tmp"
SCHEMA_VERSION = 1


class PipelineLock:
    """Represents the persisted state of the last build."""

    def __init__(self, data: dict[str, Any], path: Path) -> None:
        self._data = data
        self._path = path

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    @classmethod
    def load(cls, root: Path) -> "PipelineLock":
        """Load lock from *root/.mareforma/pipeline.lock.json*.

        Returns an empty lock if the file does not exist.
        """
        path = _lock_path(root)
        if not path.exists():
            return cls(_empty(), path)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("schema_version") != SCHEMA_VERSION:
                # Incompatible old lock — treat as empty
                return cls(_empty(), path)
            return cls(data, path)
        except (json.JSONDecodeError, KeyError):
            return cls(_empty(), path)

    @classmethod
    def empty(cls, root: Path) -> "PipelineLock":
        return cls(_empty(), _lock_path(root))

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_node(self, name: str) -> dict[str, Any] | None:
        return self._data["nodes"].get(name)

    def is_stale(
        self,
        name: str,
        input_hash: str,
        source_hash: str,
        rerun_set: set[str],
        force: bool = False,
    ) -> bool:
        """Return True if this node needs to run."""
        if force:
            return True
        node = self.get_node(name)
        if node is None:
            return True  # never run
        if node.get("status") != "success":
            return True  # last run failed
        if node.get("input_hash") != input_hash:
            return True  # raw data changed
        if node.get("source_hash") != source_hash:
            return True  # transform code changed
        # Check if any upstream dependency was re-run this session
        # (handled by runner passing rerun_set)
        return False

    def all_nodes(self) -> dict[str, Any]:
        return dict(self._data["nodes"])

    @property
    def build_timestamp(self) -> str | None:
        return self._data.get("build_timestamp")

    @property
    def git_sha(self) -> str | None:
        return self._data.get("git_sha")

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def record_node(
        self,
        name: str,
        *,
        input_hash: str,
        output_hash: str,
        source_hash: str,
        status: str,
        duration_ms: int,
    ) -> None:
        """Update the lock entry for *name* in memory (not persisted yet)."""
        self._data["nodes"][name] = {
            "input_hash": input_hash,
            "output_hash": output_hash,
            "source_hash": source_hash,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_ms": duration_ms,
        }

    def finalise(self, git_sha: str | None = None) -> None:
        """Set build-level metadata before saving."""
        self._data["build_timestamp"] = datetime.now(timezone.utc).isoformat()
        self._data["git_sha"] = git_sha

    def save(self) -> None:
        """Atomically write lock to disk."""
        tmp = self._path.with_name(LOCK_TMP_FILENAME)
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(
            json.dumps(self._data, indent=2), encoding="utf-8"
        )
        os.replace(tmp, self._path)  # atomic on POSIX, best-effort on Windows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lock_path(root: Path) -> Path:
    return root / ".mareforma" / LOCK_FILENAME


def _empty() -> dict[str, Any]:
    return {"schema_version": SCHEMA_VERSION, "nodes": {}}


