"""Shared test helpers for the mareforma test suite."""

from __future__ import annotations

import ast
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


def _load_signer(key_path: Path):
    """Load and return the Ed25519 private key object at *key_path*."""
    from mareforma import signing as _signing
    return _signing.load_private_key(key_path)


def _two_signers(tmp_path: Path):
    """Bootstrap two distinct signing keys and return loaded signer objects.

    Under the v0.3.7 model, REPLICATED convergence keys on two distinct,
    non-NULL ``asserter_keyid`` values (the per-claim signer keyid), not on
    distinct ``generated_by``. Tests that want two converging claims to
    promote must sign each with a distinct key. This returns ``(sa, sb)``,
    two loaded private-key objects to thread through ``assert_claim(signer=...)``.
    """
    from mareforma import signing as _signing
    ka = tmp_path / "_signer_a.key"
    kb = tmp_path / "_signer_b.key"
    if not ka.exists():
        _signing.bootstrap_key(ka)
    if not kb.exists():
        _signing.bootstrap_key(kb)
    return _signing.load_private_key(ka), _signing.load_private_key(kb)


def _pem_of(key_path: Path) -> bytes:
    """Return the PEM-encoded public key for the private key at ``key_path``.

    Shared helper replacing the byte-identical ``_pem_of`` /
    ``_validator_pubkey_pem`` copies that several enrollment tests each
    defined locally.
    """
    from mareforma import signing as _signing
    return _signing.public_key_to_pem(
        _signing.load_private_key(key_path).public_key(),
    )


def _wipe_db(tmp_path: Path) -> None:
    """Delete ``graph.db`` and its WAL/SHM sidecars under ``tmp_path/.mareforma``.

    The shared first half of the "wipe, then ``mareforma.restore``"
    round-trip setup that several restore tests perform identically.
    """
    for fname in ("graph.db", "graph.db-wal", "graph.db-shm"):
        p = tmp_path / ".mareforma" / fname
        if p.exists():
            p.unlink()


def _module_level_names(source_path: Path) -> list[str]:
    """Return every top-level name defined in *source_path*.

    Captures ``def``, ``async def``, ``class`` definitions, and module-
    level assignments (both annotated and unannotated). Does NOT
    capture imported names — those are explicitly excluded so the test
    only enforces re-export of names that originate in this submodule.
    """
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.append(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.append(node.target.id)
    return [n for n in names if not n.startswith("__")]
