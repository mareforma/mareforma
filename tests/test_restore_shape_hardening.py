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

import datetime as _dt
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


# Every section restore() runs _validate_section_shape over, and the subset that
# is a table of tables. project_policy and graph_meta are tables of scalars
# (fields, not rows), so they carry the section-shape guard but not the per-entry
# one. Parametrizing over the full set keeps a newly added section from missing
# the guard silently; test_section_list_matches_restore below fails if this list
# drifts from what restore actually validates.
SCALAR_FIELD_SECTIONS = ["project_policy", "graph_meta"]
ALL_SECTIONS = [
    "validators",
    "claims",
    "replication_verdicts",
    "contradiction_verdicts",
    "rekor_inclusions",
    *SCALAR_FIELD_SECTIONS,
]
TABLE_OF_TABLE_SECTIONS = [s for s in ALL_SECTIONS if s not in SCALAR_FIELD_SECTIONS]


def test_section_list_matches_restore() -> None:
    """The parametrized list must be exactly the set restore validates. restore
    calls ``_validate_section_shape`` once per section it reads out of
    claims.toml; if a release adds a section there and not here, the new section
    ships with no shape guard and a scalar planted in it leaks a raw
    ``AttributeError`` past the documented ``RestoreError`` contract. Reading the
    real call sites off the source keeps this list honest without a hand-sync."""
    import ast
    import importlib
    from pathlib import Path as _Path

    restore_module = importlib.import_module("mareforma.db.restore")
    tree = ast.parse(
        _Path(restore_module.__file__).read_text(encoding="utf-8")
    )

    def _calls_validate(node: ast.AST) -> bool:
        return any(
            isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "_validate_section_shape"
            for n in ast.walk(node)
        )

    def _strings(node: ast.AST) -> set[str]:
        return {
            n.value
            for n in ast.walk(node)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)
        }

    validated: set[str] = set()
    for node in ast.walk(tree):
        # A literal call: _validate_section_shape(data.get("<section>"), ...).
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_validate_section_shape"
            and node.args
        ):
            first = node.args[0]
            if (
                isinstance(first, ast.Call)
                and isinstance(first.func, ast.Attribute)
                and first.func.attr == "get"
                and first.args
                and isinstance(first.args[0], ast.Constant)
            ):
                validated.add(first.args[0].value)
        # The loop that sweeps a tuple of section names through the same check.
        if isinstance(node, ast.For) and _calls_validate(node):
            validated |= _strings(node.iter)
    assert validated == set(ALL_SECTIONS), (
        f"restore validates {sorted(validated)} but the test covers "
        f"{sorted(ALL_SECTIONS)}; a section drifted out of the shape sweep"
    )


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


@pytest.mark.parametrize("section", ALL_SECTIONS)
def test_array_section_value_raises_toml_malformed(
    tmp_path: Path, section: str,
) -> None:
    """A section set to a TOML array (a list, not a table) exercises the type
    guard with a non-dict that is not a scalar: ``.items()`` on a list would
    leak a raw ``AttributeError``. It must surface as the documented
    ``RestoreError`` naming the section, the same as a scalar."""
    _write_claims_toml(tmp_path, f"{section} = [1, 2]\n")
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


@pytest.mark.parametrize(
    "label,bad",
    [
        ("array", [1]),
        ("inline_table", {"a": 1}),
        ("local_time", _dt.time(1, 2, 3)),
        ("oversized_int", 10 ** 32),
    ],
)
def test_non_scalar_trust_row_value_raises_trust_row_rejected(
    tmp_path: Path, label: str, bad: object,
) -> None:
    """Shape validation proves table-of-tables; it says nothing about VALUES.

    TOML expresses arrays, inline tables, local times and integers wider than 64
    bits. sqlite3 binds none of them, and the replay loop caught only
    ``sqlite3.IntegrityError``, so a hand-edited backup raised ``ProgrammingError``
    (and ``OverflowError``, which is not even a sqlite3 exception) straight out
    of ``mareforma.restore``, past the documented ``RestoreError`` contract.

    Checked on ``propositions`` because every trust table is replayed through
    the one loop, so the guard belongs there rather than per-section.
    """
    conn = _db.open_db(tmp_path)
    data = {
        "propositions": {
            "pid1": {
                "subject": bad, "relation": "affects", "object": "x",
                "direction": "DECREASES", "scope_json": "{}",
                "content_id": "c", "frame_id": "f",
            },
        },
    }
    with pytest.raises(_db.RestoreError) as exc_info:
        _db._restore_trust_tables(conn, data)
    assert exc_info.value.kind == "trust_row_rejected"
    assert "pid1" in str(exc_info.value)
