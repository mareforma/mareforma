"""
git.py — Git integration for mareforma build.

All operations are best-effort: if the project is not in a git repo,
or gitpython is not installed, everything degrades to a warning and
the build continues. Git integration never blocks a build.

Operations
----------
get_current_sha()     → str | None   current HEAD sha (short)
is_git_repo(root)     → bool         is root inside a git repo?
tag_build(root, ts)   → str | None   create tag mare/build/<timestamp>
snapshot_lock(root)   → bool         stage + commit pipeline.lock.json
                                     if working tree is otherwise clean

Tag format
----------
    mare/build/2026-03-14T10-00-00Z

Using slashes in the tag name groups tags in git GUIs under a
'mare/build/' namespace, just like GitHub Actions build tags.

Snapshot policy
---------------
snapshot_lock() only commits pipeline.lock.json. It never touches user
files. It will NOT commit if there are other staged/unstaged changes —
it doesn't want to accidentally bundle user work into a pipeline commit.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


class GitIntegrationError(Exception):
    """Raised for unrecoverable git errors. Build should warn, not fail."""


def get_user_config() -> dict[str, str]:
    """Return git user config as {name, email}, falling back to empty strings."""
    try:
        import git
        reader = git.GitConfigParser()
        return {
            "name": reader.get_value("user", "name", "") or "",
            "email": reader.get_value("user", "email", "") or "",
        }
    except Exception:
        pass
    # subprocess fallback (when gitpython not installed)
    import subprocess
    result: dict[str, str] = {"name": "", "email": ""}
    for key in ("name", "email"):
        try:
            out = subprocess.run(
                ["git", "config", f"user.{key}"],
                capture_output=True, text=True, encoding="utf-8", timeout=5,
            )
            if out.returncode == 0:
                result[key] = out.stdout.strip()
        except Exception:
            pass
    return result


def is_git_repo(root: Path) -> bool:
    """Return True if *root* is inside a git repository."""
    try:
        import git
        try:
            git.Repo(root, search_parent_directories=True)
            return True
        except git.InvalidGitRepositoryError:
            return False
    except ImportError:
        return _subprocess_is_git_repo(root)


def get_current_sha(root: Path) -> str | None:
    """Return the short SHA of HEAD, or None if not in a repo."""
    try:
        import git
        repo = git.Repo(root, search_parent_directories=True)
        return repo.head.commit.hexsha[:8]
    except Exception:
        return _subprocess_sha(root)


def tag_build(root: Path, timestamp: str | None = None) -> str | None:
    """Create a git tag for the current build.

    Parameters
    ----------
    root:
        Project root.
    timestamp:
        ISO timestamp string. If None, uses current UTC time.

    Returns
    -------
    str | None
        The tag name if successful, None if git is unavailable or tag failed.
    """
    ts = timestamp or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    tag_name = f"mare/build/{ts}"

    try:
        import git
        repo = git.Repo(root, search_parent_directories=True)
        repo.create_tag(tag_name, message=f"mareforma build {ts}")
        return tag_name
    except ImportError:
        return _subprocess_tag(root, tag_name, ts)
    except Exception:
        return None  # degrade silently


def snapshot_lock(root: Path) -> bool:
    """Stage and commit pipeline.lock.json if the working tree is otherwise clean.

    Only commits .mareforma/pipeline.lock.json. Refuses to commit if there
    are other changes in the working tree (staged or unstaged) to avoid
    accidentally bundling user edits.

    Returns True if a commit was made, False otherwise.
    """
    lock_path = root / ".mareforma" / "pipeline.lock.json"
    if not lock_path.exists():
        return False

    try:
        import git
        repo = git.Repo(root, search_parent_directories=True)

        # Check for other changes
        other_changes = [
            item for item in repo.index.diff(None)  # unstaged
            if "pipeline.lock.json" not in item.a_path
        ] + [
            item for item in repo.index.diff("HEAD")  # staged
            if "pipeline.lock.json" not in item.a_path
        ]

        if other_changes:
            return False  # Don't commit alongside user changes

        rel_lock = lock_path.relative_to(
            Path(repo.working_tree_dir)  # type: ignore[arg-type]
        )
        repo.index.add([str(rel_lock)])

        if not repo.index.diff("HEAD"):
            return False  # Nothing new to commit

        repo.index.commit(
            "chore(mareforma): update pipeline.lock.json",
            author=repo.config_reader().get_value("user", "name", "mareforma"),
        )
        return True

    except ImportError:
        return False  # gitpython not installed, skip silently
    except Exception:
        return False  # any git error — degrade silently


# ---------------------------------------------------------------------------
# subprocess fallbacks (when gitpython is not installed)
# ---------------------------------------------------------------------------

def _subprocess_is_git_repo(root: Path) -> bool:
    import subprocess
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--git-dir"],
            cwd=root,
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def _subprocess_sha(root: Path) -> str | None:
    import subprocess
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None


def _subprocess_tag(root: Path, tag_name: str, message: str) -> str | None:
    import subprocess
    try:
        result = subprocess.run(
            ["git", "tag", "-a", tag_name, "-m", message],
            cwd=root,
            capture_output=True,
            timeout=5,
        )
        return tag_name if result.returncode == 0 else None
    except Exception:
        return None