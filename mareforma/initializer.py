"""
initializer.py — Initialize a mareforma project in a directory.

Handles two cases:
    1. Fresh project  — no .mareforma/ directory present.
    2. Existing project — .mareforma/ present; patch any missing dirs/files.
"""

from __future__ import annotations

from pathlib import Path

from mareforma import __version__
from mareforma.scaffold import scaffold_project


def _is_initialized(root: Path) -> bool:
    return (root / ".mareforma").is_dir()


def _ensure_graph_db(root: Path) -> None:
    """Create graph.db if it does not exist. Safe to call on existing projects."""
    from mareforma.db import open_db
    conn = open_db(root)
    conn.close()


def initialize(root: Path | None = None) -> list[str]:
    """Initialize (or patch) a mareforma project at *root*.

    If *root* is None, uses the current working directory.
    Returns a list of human-readable status messages.

    This function is safe to call on an existing project:
    it adds any missing pieces without touching existing files.
    """
    if root is None:
        root = Path.cwd()
    root = root.resolve()

    project_name = root.name

    if _is_initialized(root):
        # Patch mode: run scaffold with force=False so nothing is overwritten.
        msgs = ["Project already initialised. Checking for missing pieces..."]
        patch_msgs = scaffold_project(root, project_name, __version__)
        created = [m for m in patch_msgs if "created" in m]
        msgs.extend(created)
        _ensure_graph_db(root)
        if not created:
            msgs.append("  Everything looks good. Nothing to add.")
        return msgs

    msgs = [f"Initialising mareforma project '{project_name}' at {root}"]
    msgs.extend(scaffold_project(root, project_name, __version__))
    _ensure_graph_db(root)
    msgs.append("")
    msgs.append("Next steps:")
    msgs.append("  1. Edit mareforma.project.toml — fill in project.description")
    msgs.append("  2. mareforma add-source <name> --path <path/to/raw/>")
    msgs.append("  3. mareforma check")
    return msgs
