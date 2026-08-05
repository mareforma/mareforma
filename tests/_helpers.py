"""Shared test helpers for the mareforma test suite."""

from __future__ import annotations

import ast
import base64
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

# Fixtures that simulate a legacy schema drop a column, which SQLite only
# learned in 3.35. ``open_db`` accepts down to 3.30, where the statement is a
# syntax error, so those fixtures skip on a build inside the supported window
# instead of failing the suite.
_requires_drop_column = pytest.mark.skipif(
    sqlite3.sqlite_version_info < (3, 35),
    reason="ALTER TABLE ... DROP COLUMN needs SQLite >= 3.35",
)

# The sdist ships the suite for packagers but not the repo trees some tests
# read (docs/, examples/, .github/, AGENTS.md), so those tests skip when the
# suite runs from an unpacked archive. The predicate is PKG-INFO, a file only
# an unpacked sdist carries, not the absence of the tree a test reads: keyed
# on absence, the guard would go quiet in CI the day a path moved.
_requires_repo_checkout = pytest.mark.skipif(
    (Path(__file__).resolve().parent.parent / "PKG-INFO").exists(),
    reason="reads a repo tree the sdist does not ship",
)

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _example_files(suffix: str = ".py", root: Path = _REPO_ROOT) -> list[Path]:
    """Return the example files ending in *suffix* that *root* tracks in git.

    Example 05 installs into its own directory: its ``--install`` clones a
    repository and builds a virtualenv beside the script, and both are
    gitignored. A walk of the working tree hands every vendored source in
    that virtualenv to the guards that read the example tree, so the
    listing comes from git, and falls back to the walk outside a checkout.
    """
    try:
        tracked = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z", "--", "examples"],
            capture_output=True,
            check=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return sorted((root / "examples").rglob(f"*{suffix}"))
    return sorted(root / name for name in tracked.split("\0") if name.endswith(suffix))


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


def _enroll_key(tmp_path: Path, root_key: Path, new_key: Path,
                identity: str = "second@lab.example") -> None:
    """Enroll ``new_key`` as a validator under the project's root.

    The root (``root_key``, the first key opened) signs an enrollment for
    ``new_key``'s public half, so a finding signed by ``new_key`` verifies on
    read. Without this, ``new_key`` is a non-enrolled signer whose model lineage
    reads soft (fail closed) and never counts as a distinct model.
    """
    import mareforma
    from mareforma import validators as _validators
    new_pem = _pem_of(new_key)
    with mareforma.open(tmp_path, key_path=root_key) as graph:
        _validators.enroll_validator(
            graph._conn, graph._signer, new_pem, identity=identity)


def _wipe_db(tmp_path: Path) -> None:
    """Delete ``graph.db`` and its WAL/SHM sidecars under ``tmp_path/.mareforma``.

    The shared first half of the "wipe, then ``mareforma.restore``"
    round-trip setup that several restore tests perform identically.
    """
    for fname in ("graph.db", "graph.db-wal", "graph.db-shm"):
        p = tmp_path / ".mareforma" / fname
        if p.exists():
            p.unlink()


def _rekor_response_for(
    *,
    payload_hash: str,
    sig_b64: str,
    uuid: str = "abc01deadbeef02",
    log_index: int = 42,
    integrated_time: int = 1700000000,
) -> dict:
    """Build a realistic Rekor 201 body whose `body` field actually
    records the submitted hash + signature.

    submit_to_rekor verifies the response, a generic mock without a
    matching body fails the equality check. Shared builder for every
    rekor test module, so that mirror contract has one definition to
    move when it changes.
    """
    record = {
        "apiVersion": "0.0.1",
        "kind": "hashedrekord",
        "spec": {
            "data": {"hash": {"algorithm": "sha256", "value": payload_hash}},
            "signature": {
                "content": sig_b64,
                "publicKey": {"content": "<not-checked>"},
            },
        },
    }
    encoded = base64.standard_b64encode(
        json.dumps(record, separators=(",", ":")).encode("utf-8"),
    ).decode("ascii")
    return {
        uuid: {
            "body": encoded,
            "integratedTime": integrated_time,
            "logIndex": log_index,
        }
    }


def _prop():
    """Build the canonical BRCA1 proposition the finding tests assert on.

    Shared helper replacing the byte-identical copies that the model-lineage
    and model-independence tests each defined locally.
    """
    from mareforma.trust import Direction, Proposition

    return Proposition(
        subject="BRCA1", relation="affects", object="tumour growth",
        direction=Direction.DECREASES,
        scope={"population": "TNBC", "condition": "in vitro"},
    )


def _pred():
    """Build the canonical one-sided superiority prediction (alpha 0.05)."""
    from mareforma.trust import DirectionOfInterest, Prediction, TestType

    return Prediction(
        TestType.SUPERIORITY,
        direction_of_interest=DirectionOfInterest.DECREASE,
        alpha=0.05,
    )


def _est():
    """Build the canonical SMD effect estimate (-0.8, p=0.001)."""
    from mareforma.trust import EffectEstimate, EffectType

    return EffectEstimate(-0.8, EffectType.SMD, p_value=0.001)


def _claim(**overrides) -> dict:
    """A minimal claim row dict for ``trust_map._assemble``, with sane defaults.

    Shared builder for the trust-map tests; ``**overrides`` set individual
    fields. Replaces the byte-identical copies test_trust_map and
    test_trust_map_single_root each defined locally.
    """
    base = {
        "claim_id": "11111111-2222-3333-4444-555555555555",
        "text": "a finding",
        "classification": "ANALYTICAL",
        "support_level": "PRELIMINARY",
        "status": "open",
        "supports_json": "[]",
        "contradicts_json": "[]",
        "asserter_keyid": "abcdef0123456789",
        "signature_bundle": "{}",
        "transparency_logged": 1,
        "verified": True,
        "observed_grounding": None,
        "t_invalid": None,
    }
    base.update(overrides)
    return base


def _verdict(model_id: str, *, source: str = "socket"):
    """A grounding verdict carrying a model lineage of the requested tier.

    ``source="socket"`` to a recognized provider host earns COMPUTED, ``declared``
    earns PROXY; a fine-tune / alias string is UNVERIFIABLE regardless. The
    verdict is OPAQUE (the finding path only reads its ``model_lineage``; grounding
    state is irrelevant to the independence count). Shared helper replacing the
    code-identical copies the model-independence and measure-independence tests
    each defined locally.
    """
    from mareforma.observe import GroundingVerdict, ObservedGrounding
    from mareforma.observe._lineage import resolve_lineage

    lower = model_id.lower()
    provider = (
        "anthropic" if lower.startswith("claude")
        else "openai" if lower.startswith(("gpt", "o1", "o3", "o4", "chatgpt"))
        else None
    )
    lineage = resolve_lineage(
        model_id, source=source, method="m",
        decoding={"temperature": None, "top_p": None, "seed": None},
        provider=provider,
    )
    return GroundingVerdict(
        grounding=ObservedGrounding.OPAQUE, reason="test lineage",
        model_lineage=lineage,
    )


def _import_registry_delta(package: str) -> int:
    """Import *package* in a clean interpreter and return how many
    predicate URIs that import registered.

    Snapshotting the registry around an in-process ``import`` measures
    nothing: pytest has already imported the adapters during collection,
    so the statement is a dict lookup that runs no module code, and any
    URI the body would seize sits in both snapshots. A fresh interpreter
    runs the body for the first time, submodule side effects included.
    """
    probe = (
        "import importlib\n"
        "from mareforma.predicate_types import predicates\n"
        "before = len(predicates())\n"
        f"importlib.import_module({package!r})\n"
        "print(len(predicates()) - before)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"importing {package} in a clean interpreter failed:\n"
            f"{proc.stderr}"
        )
    return int(proc.stdout.strip())


def _module_level_names(source_path: Path) -> list[str]:
    """Return every top-level name defined in *source_path*.

    Captures ``def``, ``async def``, ``class`` definitions, and module-
    level assignments (both annotated and unannotated). Does NOT
    capture imported names, those are explicitly excluded so the test
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
