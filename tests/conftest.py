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


@pytest.fixture(autouse=True)
def _isolate_predicate_registry():
    """Snapshot + restore the process-global predicate-type registry.

    ``mareforma.predicate_types`` keeps registered URIs in a module-
    level dict. Tests that exercise ``register_predicate`` would
    otherwise leak entries between tests — green-alone, red-in-suite.
    """
    from mareforma import predicate_types as _pt
    snapshot = dict(_pt._registry)
    yield
    _pt._registry.clear()
    _pt._registry.update(snapshot)


_INGEST_FIXTURES = Path(__file__).parent / "ingest_fixtures"


@pytest.fixture()
def db(tmp_path):
    """Fresh mareforma graph.db with all DDL applied — for ingest/ask tests."""
    from mareforma.db import open_db
    conn = open_db(tmp_path)
    yield conn
    conn.close()


@pytest.fixture()
def sample_abstract_a():
    return _INGEST_FIXTURES / "abstract_a.txt"


@pytest.fixture()
def sample_abstract_b():
    return _INGEST_FIXTURES / "abstract_b.txt"


@pytest.fixture()
def populated_db(db, sample_abstract_a, sample_abstract_b):
    """DB with two sample abstracts ingested."""
    from mareforma.ingest_command import ingest_file
    ingest_file(sample_abstract_a, db, extracted_by="ingest:mock")
    ingest_file(sample_abstract_b, db, extracted_by="ingest:mock")
    return db


@pytest.fixture()
def open_graph(tmp_path: Path):
    """Open an EpistemicGraph in a temp directory, with a bootstrapped
    signing key so seed=True works for ESTABLISHED-upstream bootstrap.

    REPLICATED detection requires an ESTABLISHED upstream by default, so
    most tests that exercise it need a seeded upstream. A signing key is
    bootstrapped automatically; tests that don't want one can use
    ``mareforma.open(tmp_path)`` directly without the fixture."""
    from mareforma import signing as _signing
    key_path = tmp_path / "mareforma.key"
    if not key_path.exists():
        _signing.bootstrap_key(key_path)
    with mareforma.open(tmp_path, key_path=key_path) as graph:
        yield graph


@pytest.fixture()
def graph(open_graph):
    """Alias for ``open_graph``.

    The adapter and federation tests request the graph under the name
    ``graph``; each used to define a byte-identical local fixture. They
    all share this one now.
    """
    return open_graph
