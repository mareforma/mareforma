"""
tests/conftest.py — shared pytest fixtures for mareforma tests.

Fixtures
--------
project(tmp_path)
    Fully initialised mareforma project root. Returns Path.

project_with_source(project)
    Project with one source 'morphology' registered and raw/ dir created.
    Returns (root, raw_path).

make_record(...)
    Factory for TransformRecord instances without needing @transform decoration.

registry_cleared
    Auto-use fixture that clears the global TransformRegistry before and after
    each test. Critical: without this, decorated functions from one test leak
    into the next via the module-level singleton.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

from mareforma.initializer import initialize
from mareforma.registry import add_source
from mareforma.transforms import TransformRecord, registry as _registry


# ---------------------------------------------------------------------------
# Auto-use: clear the global TransformRegistry around every test
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def registry_cleared():
    """Clear the global TransformRegistry before and after every test.

    This prevents @transform decorations in one test from bleeding into another
    via the module-level singleton.
    """
    _registry.clear()
    yield
    _registry.clear()

    # Also evict any _mareforma_build_* modules imported during discovery tests
    # so they get freshly imported next time.
    to_remove = [k for k in sys.modules if k.startswith("_mareforma_build_")]
    for k in to_remove:
        del sys.modules[k]


# ---------------------------------------------------------------------------
# Project fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def project(tmp_path: Path) -> Path:
    """Return a fully initialised mareforma project root."""
    initialize(tmp_path)
    return tmp_path


@pytest.fixture()
def project_with_source(project: Path):
    """Return (root, raw_path) with source 'morphology' registered.

    The raw/ directory is created on disk so path-existence checks pass.
    """
    raw = project / "data" / "morphology" / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    add_source(project, "morphology", str(raw), "Test skeleton data")

    # Fill required fields so mareforma check passes
    toml = project / "mareforma.project.toml"
    text = toml.read_text()
    text = text.replace('description = ""', 'description = "Test project"', 1)
    text = text.replace('format = ""', 'format = "SWC"')
    toml.write_text(text)

    return project, raw


# ---------------------------------------------------------------------------
# TransformRecord factory
# ---------------------------------------------------------------------------

@pytest.fixture()
def make_record():
    """Factory for TransformRecord instances.

    Usage:
        rec = make_record("morphology.load")
        rec = make_record("morphology.register", depends_on=["morphology.load"])
        rec = make_record("morphology.features", fn=my_fn)
    """
    def _make(
        name: str,
        depends_on: list[str] | None = None,
        fn: Any = None,
        source_code: str = "",
    ) -> TransformRecord:
        if fn is None:
            def _noop(ctx=None):
                pass
            fn = _noop
        return TransformRecord(
            name=name,
            fn=fn,
            depends_on=depends_on or [],
            source_file="<test>",
            source_code=source_code or f"def {name.replace('.','_')}(): pass",
        )
    return _make