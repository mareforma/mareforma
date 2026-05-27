"""Shared test helpers for the mareforma test suite."""

from __future__ import annotations

from pathlib import Path


def _bootstrap_key(tmp_path: Path, name: str = "mareforma.key") -> Path:
    """Generate a signing key at ``tmp_path / name`` and return the path.

    Shared helper replacing the per-file copies that were duplicated
    across 10+ test files with the same 3-line body.
    """
    from mareforma import signing as _signing
    key_path = tmp_path / name
    _signing.bootstrap_key(key_path)
    return key_path
