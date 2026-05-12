"""tests/conftest.py — shared pytest fixtures for mareforma tests."""

from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from mareforma import doi_resolver


@pytest.fixture(autouse=True)
def _reset_doi_client():
    """Drop the module-level httpx.Client around every test.

    The DOI resolver pools a Client across calls. Tests using pytest-httpx
    patch httpx's transport per test; a Client constructed during test N
    must not leak into test N+1 with stale mock state.
    """
    doi_resolver._reset_client_for_testing()
    yield
    doi_resolver._reset_client_for_testing()


@pytest.fixture(autouse=True)
def _isolate_xdg_config(tmp_path_factory, monkeypatch):
    """Scope XDG_CONFIG_HOME to a per-session tmpdir so tests never observe
    (or write to) the real user's ~/.config/mareforma/key.

    Without this, ``mareforma.open(tmp_path)`` on a developer machine that
    has run ``mareforma bootstrap`` would auto-sign every test claim with
    the developer's key, while CI would not — flaky difference.
    """
    sandbox = tmp_path_factory.mktemp("xdg")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(sandbox))
    yield


@pytest.fixture()
def open_graph(tmp_path: Path):
    """Open an EpistemicGraph in a temp directory and close it after the test."""
    with mareforma.open(tmp_path) as graph:
        yield graph
