"""Re-export guard for the ``mareforma.db`` subpackage.

Same discipline as ``test_signing_reexports.py``: walks each
submodule source file for every module-level name (functions, classes,
constants) and asserts each is importable from ``mareforma.db`` AND
accessible via ``getattr``. Fails CI if ``db/__init__.py`` is missing
a re-export.
"""

from __future__ import annotations

import ast
import importlib
import inspect
from pathlib import Path

import pytest

import mareforma.db as db_pkg


def _module_level_names(source_path: Path) -> list[str]:
    """Return every top-level name defined in *source_path*."""
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.append(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.append(node.target.id)
    return [n for n in names if not n.startswith("__")]


_CORE = Path(inspect.getfile(importlib.import_module("mareforma.db.core")))
_SCHEMA = Path(inspect.getfile(importlib.import_module("mareforma.db._schema_sql")))
_ERRORS = Path(inspect.getfile(importlib.import_module("mareforma.db.errors")))
_RESTORE = Path(inspect.getfile(importlib.import_module("mareforma.db.restore")))


@pytest.mark.parametrize("name", _module_level_names(_CORE))
def test_core_name_reexported(name: str) -> None:
    assert hasattr(db_pkg, name), (
        f"mareforma/db/__init__.py missing re-export for {name!r} (from core.py)"
    )


@pytest.mark.parametrize("name", _module_level_names(_SCHEMA))
def test_schema_name_reexported(name: str) -> None:
    assert hasattr(db_pkg, name), (
        f"mareforma/db/__init__.py missing re-export for {name!r} (from _schema_sql.py)"
    )


@pytest.mark.parametrize("name", _module_level_names(_ERRORS))
def test_errors_name_reexported(name: str) -> None:
    assert hasattr(db_pkg, name), (
        f"mareforma/db/__init__.py missing re-export for {name!r} (from errors.py)"
    )


@pytest.mark.parametrize("name", _module_level_names(_RESTORE))
def test_restore_name_reexported(name: str) -> None:
    assert hasattr(db_pkg, name), (
        f"mareforma/db/__init__.py missing re-export for {name!r} (from restore.py)"
    )


def test_all_lists_only_public_names() -> None:
    underscore_in_all = [n for n in db_pkg.__all__ if n.startswith("_")]
    assert all(n in ("_SCHEMA_SQL", "_ADDITIVE_TABLES_SQL",
                      "_CLAIM_COLUMNS", "_CLAIM_SELECT",
                      "_backup_claims_toml", "_now")
               for n in underscore_in_all), (
        f"__all__ contains unexpected underscore names: {underscore_in_all}"
    )


def test_all_lists_match_actually_defined() -> None:
    missing = [n for n in db_pkg.__all__ if not hasattr(db_pkg, n)]
    assert missing == [], (
        f"__all__ lists names not bound in mareforma.db: {missing}"
    )
