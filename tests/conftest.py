"""tests/conftest.py — shared pytest fixtures for mareforma tests."""

from __future__ import annotations

from pathlib import Path

import pytest

import mareforma


@pytest.fixture()
def open_graph(tmp_path: Path):
    """Open an EpistemicGraph in a temp directory and close it after the test."""
    with mareforma.open(tmp_path) as graph:
        yield graph
