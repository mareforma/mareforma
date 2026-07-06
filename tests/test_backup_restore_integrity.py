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
