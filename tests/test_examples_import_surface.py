"""Every example file must compile and import only names the package exports.

Examples are read as instruction, so one that names a removed API teaches an
API that no longer exists. The walk is static: the examples import optional
third-party packages this suite does not install, so nothing here executes
them.
"""
from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest

from tests._helpers import _example_files, _requires_repo_checkout

# examples/ is not in the sdist, so the shipped suite skips this module.
pytestmark = _requires_repo_checkout


def _mareforma_imports(tree: ast.AST) -> list[tuple[str, str]]:
    """Return the (module, name) pairs each ``from mareforma... import`` asks for."""
    pairs = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            if node.module == "mareforma" or node.module.startswith("mareforma."):
                pairs += [(node.module, alias.name) for alias in node.names]
    return pairs


@pytest.mark.parametrize("path", _example_files(), ids=lambda p: p.name)
def test_example_compiles_and_imports_live_names(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    compile(tree, str(path), "exec")

    for module, name in _mareforma_imports(tree):
        assert hasattr(importlib.import_module(module), name), (
            f"{path} imports {module}.{name}, which the package does not export"
        )
