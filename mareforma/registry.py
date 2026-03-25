"""
registry.py — Read and write mareforma.project.toml.

Uses stdlib tomllib (Python 3.11+) or tomli (Python 3.10) for reading and tomli-w for writing.
All public functions operate on a *root* Path (the project root directory).

Data model (simplified):
    {
        "project": {
            "name": ...,
            "description": ...,
            "created": ...,          # ISO 8601 datetime (UTC)
            "mareforma_version": ...,
            "author": {
                "name": ...,
                "email": ...,
                "institution": ...
            }
        },
        "sources": {
            "source_name": {
                "path": ...,
                "description": ...,
                "format": ...,       # file format(s), e.g. "HDF5", "CSV"
                "version": ...,      # dataset version, e.g. "mat_v1078", "2024-03"
                "added": ...,        # ISO 8601 datetime (UTC)
                "added_by": ...,     # "Name <email>" from git config
                "status": ...,       # raw | processed | archived
                "acquisition": {
                    "protocol_file": ...
                }
            }
        }
    }

Public API
----------
  load(root)                        → dict
  save(root, data)
  add_source(root, name, path, ...) → dict
  get_source(root, name)            → dict
  list_sources(root)                → list[str]
  get_project(root)                 → dict
  validate(root)                    → list[dict]
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ImportError as e:
        raise ImportError(
            "Python < 3.11 requires 'tomli': pip install tomli"
        ) from e

try:
    import tomli_w as _tomli_w_mod
    def _dumps(data: dict) -> str:  # type: ignore[misc]
        return _tomli_w_mod.dumps(data)
except ImportError:
    from mareforma._toml_writer import dumps as _dumps  # type: ignore[assignment]


TOML_FILENAME = "mareforma.project.toml"


class MareformaError(Exception):
    """Base exception for all mareforma errors."""


class ProjectNotFoundError(MareformaError):
    """Raised when no mareforma project is found at the given root."""


class SourceAlreadyExistsError(MareformaError):
    """Raised when a source with the given name is already registered."""


class SourceNotFoundError(MareformaError):
    """Raised when a requested source is not in the registry."""


class TOMLParseError(MareformaError):
    """Raised when the project TOML cannot be parsed."""


# ---------------------------------------------------------------------------
# Low-level I/O
# ---------------------------------------------------------------------------

def _toml_path(root: Path) -> Path:
    return root / TOML_FILENAME


def _require_project(root: Path) -> None:
    if not _toml_path(root).exists():
        raise ProjectNotFoundError(
            f"No mareforma project found at '{root}'.\n"
            "Run 'mareforma init' to initialise one."
        )


def load(root: Path) -> dict[str, Any]:
    """Load and return the parsed project TOML as a plain dict.

    Raises TOMLParseError with a helpful message on syntax errors.
    """
    _require_project(root)
    path = _toml_path(root)
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise TOMLParseError(
            f"Could not parse {TOML_FILENAME}: {exc}\n"
            "Fix the syntax error above, then re-run."
        ) from exc


def save(root: Path, data: dict[str, Any]) -> None:
    """Write *data* back to the project TOML file."""
    path = _toml_path(root)
    path.write_bytes(_dumps(data).encode("utf-8"))


# ---------------------------------------------------------------------------
# Source management
# ---------------------------------------------------------------------------

def _default_source_entry(
    name: str,
    path: str,
    description: str,
    added_by: str = "",
) -> dict[str, Any]:
    return {
        "path": path,
        "description": description,
        "format": "",
        "version": "",
        "added": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "added_by": added_by,
        "status": "raw",
        "acquisition": {
            "protocol_file": f"data/{name}/protocols/",
        },
    }


def add_source(
    root: Path,
    name: str,
    path: str,
    description: str = "",
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Register a new data source in the project TOML.

    Returns the data dict for the new source.
    Raises SourceAlreadyExistsError if *name* exists and force=False.
    """
    data = load(root)
    sources: dict[str, Any] = data.setdefault("sources", {})

    if name in sources and not force:
        raise SourceAlreadyExistsError(
            f"Source '{name}' is already registered.\n"
            "Use --force to overwrite."
        )

    from mareforma.git import get_user_config
    git_user = get_user_config()
    git_name = git_user.get("name", "")
    git_email = git_user.get("email", "")
    added_by = f"{git_name} <{git_email}>" if git_name and git_email else git_name or git_email
    entry = _default_source_entry(name, path, description, added_by=added_by)
    sources[name] = entry
    save(root, data)
    return entry


def get_source(root: Path, name: str) -> dict[str, Any]:
    """Return the TOML entry for *name*.

    Raises SourceNotFoundError if not present.
    """
    data = load(root)
    sources = data.get("sources", {})
    if name not in sources:
        registered = list(sources.keys())
        hint = (
            f"Registered sources: {', '.join(registered)}"
            if registered
            else "No sources registered yet. Use 'mareforma add-source <name>'."
        )
        raise SourceNotFoundError(
            f"Source '{name}' not found.\n{hint}"
        )
    return sources[name]


def list_sources(root: Path) -> list[str]:
    """Return sorted list of registered source names."""
    data = load(root)
    return sorted(data.get("sources", {}).keys())


def get_project(root: Path) -> dict[str, Any]:
    """Return the [project] block."""
    data = load(root)
    return data.get("project", {})


# ---------------------------------------------------------------------------
# Validation helpers (used by `mareforma check`)
# ---------------------------------------------------------------------------

def validate(root: Path) -> list[dict[str, str]]:
    """Validate the project TOML. Returns a list of issue dicts.

    Each issue has keys: level ('warning'|'error'), source (or 'project'), message.
    """
    issues: list[dict[str, str]] = []

    # Will raise TOMLParseError if unparseable — let it propagate.
    data = load(root)

    project = data.get("project", {})
    if not project.get("description", "").strip():
        issues.append({
            "level": "warning",
            "source": "project",
            "message": "project.description is empty.",
        })

    sources = data.get("sources", {})
    if not sources:
        issues.append({
            "level": "warning",
            "source": "project",
            "message": "No sources registered. Use 'mareforma add-source <name>'.",
        })

    for name, src in sources.items():
        src_path = Path(src.get("path", ""))
        if not src_path.is_absolute():
            src_path = root / src_path
        if not src_path.exists():
            issues.append({
                "level": "warning",
                "source": name,
                "message": f"path '{src.get('path')}' does not exist on disk.",
            })

        if not src.get("description", "").strip():
            issues.append({
                "level": "warning",
                "source": name,
                "message": "description is empty.",
            })

        if not src.get("format", "").strip():
            issues.append({
                "level": "warning",
                "source": name,
                "message": "format is empty.",
            })

    return issues
