"""the per-connection chain-verification cache is real.

Stdlib ``sqlite3.Connection`` rejects attribute assignment, so the old
``setattr(conn, ...)`` cache silently fell back to a fresh empty set on
every call and ``is_enrolled`` re-walked the validator chain each time.
Graph connections are now the ``_GraphConnection`` subclass that accepts
the attribute, so the cache persists for the connection's life. This pins
that a repeat ``is_enrolled`` hits the cache instead of re-walking.
"""

from __future__ import annotations

from pathlib import Path

import mareforma
from mareforma import signing as _signing
from mareforma import validators as _validators
from mareforma.db.core import _GraphConnection


def _root_keyid(conn) -> str:
    row = conn.execute("SELECT keyid FROM validators LIMIT 1").fetchone()
    assert row is not None, "expected an auto-enrolled root validator"
    return row["keyid"]


def test_graph_connection_accepts_the_cache_attribute(tmp_path: Path) -> None:
    key = tmp_path / "root.key"
    _signing.save_private_key(_signing.generate_keypair(), key)
    with mareforma.open(tmp_path, key_path=key) as graph:
        conn = graph._conn
        assert isinstance(conn, _GraphConnection)
        keyid = _root_keyid(conn)
        assert _validators.is_enrolled(conn, keyid) is True
        # The cache actually persisted the verified keyid (not the dead
        # fresh-set-every-call fallback).
        assert keyid in _validators._conn_cache(conn)


def _count_chain_walk(monkeypatch):
    """Count get_validator lookups (the chain walk) inside is_enrolled."""
    calls = {"n": 0}
    real = _validators.get_validator

    def counting(conn, keyid):
        calls["n"] += 1
        return real(conn, keyid)

    monkeypatch.setattr(_validators, "get_validator", counting)
    return calls


def test_repeat_is_enrolled_skips_the_chain_walk(tmp_path: Path, monkeypatch) -> None:
    key = tmp_path / "root.key"
    _signing.save_private_key(_signing.generate_keypair(), key)
    with mareforma.open(tmp_path, key_path=key) as graph:
        conn = graph._conn
        keyid = _root_keyid(conn)
        calls = _count_chain_walk(monkeypatch)

        # Graph open already warmed the cache; clear it for a true cold call.
        _validators.invalidate_conn_cache(conn)
        calls["n"] = 0
        _validators.is_enrolled(conn, keyid)
        cold = calls["n"]
        calls["n"] = 0
        _validators.is_enrolled(conn, keyid)
        warm = calls["n"]

        assert cold >= 1, "cold is_enrolled must walk the chain at least once"
        assert warm == 0, "warm is_enrolled must serve from cache, no re-walk"


def test_invalidation_forces_a_rewalk(tmp_path: Path, monkeypatch) -> None:
    key = tmp_path / "root.key"
    _signing.save_private_key(_signing.generate_keypair(), key)
    with mareforma.open(tmp_path, key_path=key) as graph:
        conn = graph._conn
        keyid = _root_keyid(conn)
        _validators.is_enrolled(conn, keyid)  # populate cache
        _validators.invalidate_conn_cache(conn)
        assert keyid not in _validators._conn_cache(conn)
        calls = _count_chain_walk(monkeypatch)
        calls["n"] = 0
        _validators.is_enrolled(conn, keyid)
        assert calls["n"] >= 1, "after invalidation the chain must be re-walked"
