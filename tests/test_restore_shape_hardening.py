"""restore() must turn malformed section shapes into a typed RestoreError.

restore's threat model is a hand-edited or tampered claims.toml, and it
documents that it raises RestoreError with a .kind. But only top-level TOML
syntax was guarded: once parsing succeeded the sort helpers called .items() on
each section value and .get() on each entry, so a scalar in place of a table
leaked a raw AttributeError. An unreadable path (a directory at claims.toml)
leaked a raw OSError. Both broke the documented contract for the exact
disaster-recovery user the module is written for.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
import mareforma.db as _db


def _write_claims_toml(tmp_path: Path, body: str) -> None:
    (tmp_path / "claims.toml").write_text(body, encoding="utf-8")


@pytest.mark.parametrize("section", [
    "validators",
    "claims",
    "replication_verdicts",
    "contradiction_verdicts",
])
def test_scalar_section_value_raises_toml_malformed(
    tmp_path: Path, section: str,
) -> None:
    _write_claims_toml(tmp_path, f'{section} = "tampered"\n')
    with pytest.raises(_db.RestoreError) as exc_info:
        mareforma.restore(tmp_path)
    assert exc_info.value.kind == "toml_malformed"
    assert section in str(exc_info.value)


@pytest.mark.parametrize("section", [
    "validators",
    "claims",
    "replication_verdicts",
    "contradiction_verdicts",
])
def test_scalar_section_entry_raises_toml_malformed(
    tmp_path: Path, section: str,
) -> None:
    _write_claims_toml(tmp_path, f"[{section}]\nfoo = \"bar\"\n")
    with pytest.raises(_db.RestoreError) as exc_info:
        mareforma.restore(tmp_path)
    assert exc_info.value.kind == "toml_malformed"


def test_unreadable_claims_toml_raises_restore_error(tmp_path: Path) -> None:
    """A directory at the claims.toml path passes the exists() check but the
    read raises OSError. It must surface as a typed RestoreError, not a raw
    IsADirectoryError traceback."""
    (tmp_path / "claims.toml").mkdir()
    with pytest.raises(_db.RestoreError) as exc_info:
        mareforma.restore(tmp_path)
    assert exc_info.value.kind == "toml_unreadable"
