"""Backup and restore integrity: the claims.toml DR artifact and its restore.

claims.toml is the source of truth for ``mareforma restore`` after loss of
graph.db. It must survive a crash during its own rewrite, and restore must
reconstruct the state promotion depends on rather than trust an unsigned field.
"""
from __future__ import annotations

import os

import mareforma
from tests._helpers import _bootstrap_key


def test_backup_survives_a_crash_at_the_atomic_rename(tmp_path, monkeypatch):
    """A crash during the backup rewrite must leave the previous good
    claims.toml intact, never a truncated or empty file."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"

    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("first claim alpha", generated_by="x")
        first = out.read_bytes()
        assert b"first claim alpha" in first

        def _boom_replace(src, dst, *a, **k):
            raise OSError("simulated crash before the rename completes")

        monkeypatch.setattr(os, "replace", _boom_replace)
        # The next mutation's backup fails at the atomic-commit step. The backup
        # is non-fatal, so the claim write itself still succeeds.
        g.assert_claim("second claim beta", generated_by="x")

    after = out.read_bytes()
    assert after == first
    assert b"first claim alpha" in after
    assert b"second claim beta" not in after
    # No half-written temp file lingers in the project root.
    assert not list(tmp_path.glob(".claims.toml.*.tmp"))


def test_backup_does_not_capture_a_rolled_back_claim(tmp_path):
    """A claim written inside a caller's open transaction that then rolls back
    must not persist in claims.toml. The backup snapshots committed state only,
    so it runs only when add_claim owns the transaction, not when it joined a
    caller's open one."""
    import mareforma.db.core as _core

    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"

    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("committed alpha claim", generated_by="x")
        conn = g._conn
        # A caller owns the transaction; add_claim joins it (own_transaction is
        # False), writes the claim, then the caller rolls the whole thing back.
        conn.execute("BEGIN IMMEDIATE")
        _core.add_claim(
            conn, tmp_path, "phantom rolled-back claim", generated_by="x",
        )
        conn.rollback()

    toml = out.read_text()
    assert "committed alpha claim" in toml
    assert "phantom rolled-back claim" not in toml


def _count_claims_toml_writes(monkeypatch, out):
    """Count real claims.toml writes by intercepting the atomic rename."""
    counter = {"n": 0}
    real = os.replace

    def _counting(src, dst, *a, **k):
        if str(dst) == str(out):
            counter["n"] += 1
        return real(src, dst, *a, **k)

    monkeypatch.setattr(os, "replace", _counting)
    return counter


def test_refresh_convergence_writes_the_backup_once_not_per_row(
    tmp_path, monkeypatch,
):
    """A refresh pass over many flagged rows writes claims.toml once, not once
    per row."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"
    with mareforma.open(tmp_path, key_path=root_key) as g:
        for i in range(4):
            g.assert_claim(f"claim number {i}", generated_by="x")
        g._conn.execute("UPDATE claims SET convergence_retry_needed = 1")
        g._conn.commit()

        writes = _count_claims_toml_writes(monkeypatch, out)
        g.refresh_convergence()

    assert writes["n"] == 1


def test_defer_backup_batches_writes_and_backup_forces_one(tmp_path, monkeypatch):
    """defer_backup groups mutations under one write and leaves claims.toml
    current on exit; backup() forces a write on demand."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"
    with mareforma.open(tmp_path, key_path=root_key) as g:
        writes = _count_claims_toml_writes(monkeypatch, out)
        with g.defer_backup():
            for i in range(5):
                g.assert_claim(f"deferred claim {i}", generated_by="x")
            assert writes["n"] == 0  # nothing written inside the window
        assert writes["n"] == 1  # one write when the window closes

        toml = out.read_text()
        for i in range(5):
            assert f"deferred claim {i}" in toml

        g.backup()
        assert writes["n"] == 2


def test_nested_defer_backup_keeps_batching_until_the_outer_window_closes(
    tmp_path, monkeypatch,
):
    """A defer_backup window nested inside another writes claims.toml once, when
    the outermost window closes, not when the inner one exits."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"
    with mareforma.open(tmp_path, key_path=root_key) as g:
        writes = _count_claims_toml_writes(monkeypatch, out)
        with g.defer_backup():
            g.assert_claim("outer claim one", generated_by="x")
            with g.defer_backup():
                g.assert_claim("inner claim two", generated_by="x")
                assert writes["n"] == 0  # inner window still batching
            # Inner window closed, but the outer one is still open: no write yet.
            assert writes["n"] == 0
            g.assert_claim("outer claim three", generated_by="x")
        assert writes["n"] == 1  # single write when the outermost window closes

        toml = out.read_text()
        assert "outer claim one" in toml
        assert "inner claim two" in toml
        assert "outer claim three" in toml


def test_close_inside_a_nested_defer_backup_still_flushes_the_batch(
    tmp_path, monkeypatch,
):
    """Closing the graph mid-batch (inside nested defer_backup windows) flushes
    the pending write once and leaves no window keyed on the connection id. The
    unwinding context managers then resume on a drained connection as no-ops."""
    import mareforma.db.core as _core

    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"
    g = mareforma.open(tmp_path, key_path=root_key)
    conn_id = id(g._conn)
    writes = _count_claims_toml_writes(monkeypatch, out)
    with g.defer_backup():
        with g.defer_backup():
            g.assert_claim("claim written mid batch", generated_by="x")
            assert writes["n"] == 0  # nothing written while windows are open
            g.close()  # drains every level at once, before the conn closes
            assert writes["n"] == 1  # the batched write happened on close
            assert conn_id not in _core._backup_suspended  # no leaked key

    assert "claim written mid batch" in out.read_text()
