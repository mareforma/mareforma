"""Regression guard: the core-derived classification engine is gone.

Closes #19 (defeated keyword classifier), #30 (defect in it), and #38
(missing coverage) by removal, not repair. The keyword-derived
ANALYTICAL/INFERRED classifier contradicted the "grounding is computed"
story; the execution-observed grounding axis replaced it. This guard
fails on the pre-removal tree (where ``mareforma.derivation`` still
imports) and pins that the package, its public symbols, and every
residual in-tree import stay gone.
"""

from __future__ import annotations

import ast
import importlib
import pathlib

import pytest

import mareforma


def test_derivation_package_is_gone():
    """The whole subpackage no longer imports — removal, not deprecation."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("mareforma.derivation")


@pytest.mark.parametrize(
    "symbol",
    [
        "derive_classification",
        "verify_classification",
        "extract_source_profile",
        "extract_directory_profile",
        "extract_templates",
        "DERIVATION_VERSION",
    ],
)
def test_derivation_symbols_not_reexported(symbol):
    """None of the derivation surface leaks back through the top level."""
    assert not hasattr(mareforma, symbol)


def test_no_residual_derivation_imports_in_package():
    """No shipped module imports the deleted package (import-time landmine)."""
    pkg_root = pathlib.Path(mareforma.__file__).parent
    offenders = []
    for py in pkg_root.rglob("*.py"):
        tree = ast.parse(py.read_text(encoding="utf-8"), filename=str(py))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                if any(a.name.split(".")[0:2] == ["mareforma", "derivation"] for a in node.names):
                    offenders.append(str(py.relative_to(pkg_root)))
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                if mod == "mareforma.derivation" or mod.startswith("mareforma.derivation."):
                    offenders.append(str(py.relative_to(pkg_root)))
    assert offenders == [], f"residual mareforma.derivation imports: {offenders}"
