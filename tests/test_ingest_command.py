"""
test_ingest_cmd.py — tests for the ingest command logic.

Tests ingest_file() directly (no Click runner needed for unit tests).
CLI integration tested via CliRunner.
"""

import sys

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

import pytest
from click.testing import CliRunner


def test_ingest_parses_structured_file(db, sample_abstract_a):
    from mareforma.ingest_command import ingest_file
    claims = ingest_file(sample_abstract_a, db)
    assert len(claims) == 3


def test_ingest_writes_to_db(db, sample_abstract_a):
    from mareforma.ingest_command import ingest_file
    ingest_file(sample_abstract_a, db)
    count = db.execute("SELECT COUNT(*) FROM literature_claims").fetchone()[0]
    assert count == 3


def test_ingest_is_idempotent(db, sample_abstract_a):
    from mareforma.ingest_command import ingest_file
    ingest_file(sample_abstract_a, db)
    ingest_file(sample_abstract_a, db)
    count = db.execute("SELECT COUNT(*) FROM literature_claims").fetchone()[0]
    assert count == 3


def test_ingest_sets_extracted_by_mock(db, sample_abstract_a):
    from mareforma.ingest_command import ingest_file
    claims = ingest_file(sample_abstract_a, db)
    assert all(c["extracted_by"] == "ingest:mock" for c in claims)


def test_ingest_sets_ingested_at_utc(db, sample_abstract_a):
    from mareforma.ingest_command import ingest_file
    claims = ingest_file(sample_abstract_a, db)
    for c in claims:
        assert "+00:00" in c["ingested_at"]


def test_ingest_sets_source_doc_id(db, sample_abstract_a):
    from mareforma.ingest_command import ingest_file
    claims = ingest_file(sample_abstract_a, db)
    assert all(len(c["source_doc_id"]) == 16 for c in claims)


def test_ingest_confidence_parsed(db, sample_abstract_a):
    from mareforma.ingest_command import ingest_file
    claims = ingest_file(sample_abstract_a, db)
    confs = {c["claim_text"]: c["confidence"] for c in claims}
    crp_claim = next(t for t in confs if "CRP" in t and "30%" in t)
    assert confs[crp_claim] == pytest.approx(0.90)


def test_ingest_toml_output_parseable(db, sample_abstract_a):
    from mareforma.ingest_command import ingest_file, claims_to_toml
    claims = ingest_file(sample_abstract_a, db)
    toml_str = claims_to_toml(claims)
    parsed = tomllib.loads(toml_str)
    assert "claim" in parsed
    assert len(parsed["claim"]) == 3


def test_ingest_toml_has_required_fields(db, sample_abstract_a):
    from mareforma.ingest_command import ingest_file, claims_to_toml
    claims = ingest_file(sample_abstract_a, db)
    parsed = tomllib.loads(claims_to_toml(claims))
    for cid, data in parsed["claim"].items():
        for field in ("text", "confidence", "source_doc_id", "doi",
                      "extracted_by", "ingested_at"):
            assert field in data, f"Missing TOML field: {field}"


def test_ingest_llm_flag_fails_gracefully_when_anthropic_missing(
    tmp_path, sample_abstract_a
):
    """--llm should print an error and exit 1 when anthropic is not installed."""
    import sys
    # Temporarily block import of anthropic
    orig = sys.modules.get("anthropic", None)
    sys.modules["anthropic"] = None  # type: ignore

    from mareforma.ingest_command import ingest_cli
    runner = CliRunner()
    db_path = tmp_path / "g.db"

    # Pre-create the db so --db arg is valid
    from mareforma.db import open_db_from_db_path
    open_db_from_db_path(db_path).close()

    result = runner.invoke(
        ingest_cli, [str(sample_abstract_a), "--db", str(db_path), "--llm"]
    )

    # Restore
    if orig is None:
        del sys.modules["anthropic"]
    else:
        sys.modules["anthropic"] = orig

    assert result.exit_code == 1
    assert "anthropic" in result.output.lower() or "not installed" in result.output.lower()


def test_ingest_missing_file_exits_1(tmp_path):
    from mareforma.ingest_command import ingest_cli
    from mareforma.db import open_db_from_db_path
    db_path = tmp_path / "g.db"
    open_db_from_db_path(db_path).close()
    runner = CliRunner()
    result = runner.invoke(
        ingest_cli, [str(tmp_path / "nonexistent.txt"), "--db", str(db_path)]
    )
    assert result.exit_code == 1


def test_ingest_respects_custom_db_path(tmp_path, sample_abstract_a):
    """`--db /path/file.db` writes claims to file.db, NOT to <parent>/.mareforma/graph.db.

    Regression: prior implementation re-derived a project_root and silently
    opened <root>/.mareforma/graph.db, so the user-supplied filename was
    ignored and claims landed in a different file.
    """
    from mareforma.ingest_command import ingest_cli

    custom_db = tmp_path / "custom.db"
    rewritten_path = tmp_path / ".mareforma" / "graph.db"

    runner = CliRunner()
    result = runner.invoke(
        ingest_cli, [str(sample_abstract_a), "--db", str(custom_db)]
    )
    assert result.exit_code == 0, result.output

    # The file the user asked for must exist with non-zero size.
    assert custom_db.exists(), "ingest did not write to the --db path the user supplied"
    assert custom_db.stat().st_size > 0

    # The previously-buggy rewrite path must NOT have been created.
    assert not rewritten_path.exists(), (
        "ingest still rewrote --db under .mareforma/graph.db — "
        "open_db_from_db_path is not honouring the user-supplied filename"
    )

    # And the rows are queryable from the user-supplied file.
    import sqlite3
    conn = sqlite3.connect(str(custom_db))
    try:
        n = conn.execute("SELECT COUNT(*) FROM literature_claims").fetchone()[0]
        assert n > 0, "no claims written to the --db path"
    finally:
        conn.close()
