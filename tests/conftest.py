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
def _isolate_xdg_config(tmp_path, monkeypatch):
    """Scope XDG_CONFIG_HOME to a per-TEST tmpdir so tests never observe
    (or write to) the real user's ~/.config/mareforma/key.

    Function-scoped tmp_path (not session-scoped tmp_path_factory) so two
    tests that both bootstrap the default key path don't collide on the
    second run — bootstrap_key now uses O_CREAT|O_EXCL and would fail the
    loser with a SigningError.
    """
    sandbox = tmp_path / "xdg"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(sandbox))
    yield


@pytest.fixture()
def open_graph(tmp_path: Path):
    """Open an EpistemicGraph in a temp directory, with a bootstrapped
    signing key so seed=True works for ESTABLISHED-upstream bootstrap.

    P1.7 made the ESTABLISHED-upstream rule strict by default, so most
    tests that exercise REPLICATED detection need a seeded upstream.
    A signing key is bootstrapped automatically; tests that don't
    want one can use ``mareforma.open(tmp_path)`` directly without
    the fixture."""
    from mareforma import signing as _signing
    key_path = tmp_path / "mareforma.key"
    if not key_path.exists():
        _signing.bootstrap_key(key_path)
    with mareforma.open(tmp_path, key_path=key_path) as graph:
        yield graph
