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

import json
from pathlib import Path

import pytest

import mareforma
import mareforma.db as _db


def _write_claims_toml(tmp_path: Path, body: str) -> None:
    (tmp_path / "claims.toml").write_text(body, encoding="utf-8")


def _write_unsigned_claim(tmp_path: Path, evidence_json: object) -> None:
    """Write a claims.toml holding one unsigned claim with a chosen
    ``evidence_json`` (a string, or any scalar a tamper could put there).
    No validators section means unsigned mode, which is
    the default (signing is opt-in) and the only mode that reaches the
    evidence denormalization without a signature check first."""
    _write_claims_toml(tmp_path, f"""\
[claims.c1]
text = "alpha"
classification = "INFERRED"
support_level = "PRELIMINARY"
generated_by = "agent"
status = "open"
supports = []
contradicts = []
created_at = "2026-01-01T00:00:00+00:00"
updated_at = "2026-01-01T00:00:00+00:00"
evidence_json = {json.dumps(evidence_json)}
""")


# Every section restore() reads out of claims.toml, and the subset that is a
# table of tables (project_policy is a table of scalars). Parametrizing over
# these keeps a newly added section from missing the guard silently.
ALL_SECTIONS = [
    "validators",
    "claims",
    "replication_verdicts",
    "contradiction_verdicts",
    "rekor_inclusions",
    "project_policy",
]
TABLE_OF_TABLE_SECTIONS = [s for s in ALL_SECTIONS if s != "project_policy"]


@pytest.mark.parametrize("section", ALL_SECTIONS)
def test_scalar_section_value_raises_toml_malformed(
    tmp_path: Path, section: str,
) -> None:
    _write_claims_toml(tmp_path, f'{section} = "tampered"\n')
    with pytest.raises(_db.RestoreError) as exc_info:
        mareforma.restore(tmp_path)
    assert exc_info.value.kind == "toml_malformed"
    assert section in str(exc_info.value)


@pytest.mark.parametrize("section", ALL_SECTIONS)
def test_integer_section_value_raises_toml_malformed(
    tmp_path: Path, section: str,
) -> None:
    _write_claims_toml(tmp_path, f"{section} = 5\n")
    with pytest.raises(_db.RestoreError) as exc_info:
        mareforma.restore(tmp_path)
    assert exc_info.value.kind == "toml_malformed"
    assert section in str(exc_info.value)


@pytest.mark.parametrize("section", TABLE_OF_TABLE_SECTIONS)
def test_scalar_section_entry_raises_toml_malformed(
    tmp_path: Path, section: str,
) -> None:
    _write_claims_toml(tmp_path, f"[{section}]\nfoo = \"bar\"\n")
    with pytest.raises(_db.RestoreError) as exc_info:
        mareforma.restore(tmp_path)
    assert exc_info.value.kind == "toml_malformed"


@pytest.mark.parametrize("evidence_json", [
    '{"risk_of_bias": "oops"}',   # bare int() raised ValueError
    '{"risk_of_bias": [1]}',      # bare int() raised TypeError
    '[1, 2]',                     # .get() on a list raised AttributeError
])
def test_hostile_evidence_value_raises_restore_error(
    tmp_path: Path, evidence_json: str,
) -> None:
    """Well-formed JSON carrying a value the GRADE denormalization cannot
    coerce must abort as a typed RestoreError naming the claim, not leak the
    coercion's own exception past restore's documented error surface."""
    _write_unsigned_claim(tmp_path, evidence_json)
    with pytest.raises(_db.RestoreError) as exc_info:
        mareforma.restore(tmp_path)
    assert "c1" in str(exc_info.value)


@pytest.mark.parametrize("evidence_json", ["not json at all", 5])
def test_unparseable_evidence_json_refuses_restore(
    tmp_path: Path, evidence_json: object,
) -> None:
    """An evidence vector that will not parse used to be swallowed: the five
    ev_* columns landed as zeros, the unparsed blob was written back into
    evidence_json, and restore reported the claim restored. The signed path
    refuses the same failure, so the unsigned path must too."""
    _write_unsigned_claim(tmp_path, evidence_json)
    with pytest.raises(_db.RestoreError) as exc_info:
        mareforma.restore(tmp_path)
    assert exc_info.value.kind == "toml_malformed"
    assert "c1" in str(exc_info.value)
    assert not (tmp_path / ".mareforma" / "graph.db").exists()


def test_unreadable_claims_toml_raises_restore_error(tmp_path: Path) -> None:
    """A directory at the claims.toml path passes the exists() check but the
    read raises OSError. It must surface as a typed RestoreError, not a raw
    IsADirectoryError traceback."""
    (tmp_path / "claims.toml").mkdir()
    with pytest.raises(_db.RestoreError) as exc_info:
        mareforma.restore(tmp_path)
    assert exc_info.value.kind == "toml_unreadable"
