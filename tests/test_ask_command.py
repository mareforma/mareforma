"""
test_ask_cmd.py — tests for the ask command logic.
"""

import pytest


def test_ask_returns_results_for_known_term(populated_db):
    from mareforma.ask_command import ask
    results = ask("CRP", populated_db)
    assert len(results) > 0


def test_ask_empty_question_returns_empty(populated_db):
    from mareforma.ask_command import ask
    results = ask("", populated_db)
    assert results == []


def test_ask_unknown_term_returns_empty(populated_db):
    from mareforma.ask_command import ask
    results = ask("xylophone", populated_db)
    assert results == []


def test_ask_limit_respected(populated_db):
    from mareforma.ask_command import ask
    results = ask("Drug", populated_db, limit=2)
    assert len(results) <= 2


def test_ask_result_has_doi_confidence_text(populated_db):
    from mareforma.ask_command import ask
    results = ask("CRP", populated_db)
    for r in results:
        assert len(r.claim_text) > 0
        assert 0.0 <= r.confidence <= 1.0
        assert len(r.doi) > 0


def test_ask_score_positive(populated_db):
    from mareforma.ask_command import ask
    results = ask("CRP", populated_db)
    assert all(r.score > 0 for r in results)


def test_ask_il6_returns_relevant(populated_db):
    from mareforma.ask_command import ask
    results = ask("IL-6", populated_db)
    texts = [r.claim_text for r in results]
    assert any("IL-6" in t for t in texts)


def test_ask_cli_outputs_table(populated_db, tmp_path):
    """CliRunner smoke test: the ask CLI prints something useful."""
    from mareforma.ask_command import ask_cli
    from click.testing import CliRunner
    import sqlite3

    from mareforma.db import open_db_from_db_path
    from mareforma.ingest_command import ingest_file
    from pathlib import Path

    db_path = tmp_path / "literature.db"
    conn = open_db_from_db_path(db_path)
    sample_dir = Path(__file__).parent / "ingest_fixtures"
    ingest_file(sample_dir / "abstract_a.txt", conn)
    conn.close()

    runner = CliRunner()
    result = runner.invoke(ask_cli, ["CRP", "--db", str(db_path)])
    assert result.exit_code == 0
    assert len(result.output) > 0


def test_ask_handles_embedded_double_quotes(populated_db):
    """Regression: input containing `"` must not produce invalid FTS5 syntax.

    Before the fix: `mareforma ask 'word "quoted"'` produced
    `"word" ""quoted""` which raised sqlite3.OperationalError.
    """
    from mareforma.ask_command import ask
    # Should not raise — the embedded quote must be escaped per FTS5.
    results = ask('mutations of "BRCA1"', populated_db, limit=5)
    assert isinstance(results, list)


def test_ask_handles_fts5_special_characters(populated_db):
    """Regression: parens / asterisks / hyphens must be safe inside tokens."""
    from mareforma.ask_command import ask
    # Each of these would otherwise be interpreted as FTS5 syntax.
    for q in (
        "study (n=42)",
        "drug-X*y",
        "title: subtitle",
        "OR AND NOT",  # FTS5 reserved-ish operators
    ):
        results = ask(q, populated_db, limit=5)
        assert isinstance(results, list), f"failed on query: {q!r}"
