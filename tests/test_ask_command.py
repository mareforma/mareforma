"""Tests for ``mareforma ask`` — FTS5 BM25 search over ingested claims.

Conceptual clusters:

- :class:`TestSearchResults` — hits, empty queries, unknown terms,
  limit honouring, result-row shape.
- :class:`TestRanking` — BM25 score sign + relevance ordering.
- :class:`TestFts5Sanitization` — embedded double quotes + FTS5
  special characters must not produce invalid MATCH syntax
  (regression).
- :class:`TestCli` — CliRunner smoke check.
"""

from __future__ import annotations


class TestSearchResults:
    def test_returns_results_for_known_term(self, populated_db):
        from mareforma.ask_command import ask
        results = ask("CRP", populated_db)
        assert len(results) > 0

    def test_empty_question_returns_empty(self, populated_db):
        from mareforma.ask_command import ask
        results = ask("", populated_db)
        assert results == []

    def test_unknown_term_returns_empty(self, populated_db):
        from mareforma.ask_command import ask
        results = ask("xylophone", populated_db)
        assert results == []

    def test_limit_respected(self, populated_db):
        from mareforma.ask_command import ask
        results = ask("Drug", populated_db, limit=2)
        assert len(results) <= 2

    def test_result_has_doi_confidence_text(self, populated_db):
        from mareforma.ask_command import ask
        results = ask("CRP", populated_db)
        for r in results:
            assert len(r.claim_text) > 0
            assert 0.0 <= r.confidence <= 1.0
            assert len(r.doi) > 0


class TestRanking:
    def test_score_positive(self, populated_db):
        from mareforma.ask_command import ask
        results = ask("CRP", populated_db)
        assert all(r.score > 0 for r in results)

    def test_il6_returns_relevant(self, populated_db):
        from mareforma.ask_command import ask
        results = ask("IL-6", populated_db)
        texts = [r.claim_text for r in results]
        assert any("IL-6" in t for t in texts)


class TestFts5Sanitization:
    def test_handles_embedded_double_quotes(self, populated_db):
        """Regression: input containing ``"`` must not produce invalid
        FTS5 syntax. Before the fix: ``mareforma ask 'word "quoted"'``
        produced ``"word" ""quoted""`` which raised
        sqlite3.OperationalError.
        """
        from mareforma.ask_command import ask
        results = ask('mutations of "BRCA1"', populated_db, limit=5)
        assert isinstance(results, list)

    def test_handles_fts5_special_characters(self, populated_db):
        """Regression: parens / asterisks / hyphens must be safe inside tokens."""
        from mareforma.ask_command import ask
        for q in (
            "study (n=42)",
            "drug-X*y",
            "title: subtitle",
            "OR AND NOT",  # FTS5 reserved-ish operators
        ):
            results = ask(q, populated_db, limit=5)
            assert isinstance(results, list), f"failed on query: {q!r}"


class TestCli:
    def test_cli_outputs_table(self, populated_db, tmp_path):
        """CliRunner smoke test: the ask CLI prints something useful."""
        from pathlib import Path

        from click.testing import CliRunner

        from mareforma.ask_command import ask_cli
        from mareforma.db import open_db_from_db_path
        from mareforma.ingest_command import ingest_file

        db_path = tmp_path / "literature.db"
        conn = open_db_from_db_path(db_path)
        sample_dir = Path(__file__).parent / "ingest_fixtures"
        ingest_file(sample_dir / "abstract_a.txt", conn)
        conn.close()

        runner = CliRunner()
        result = runner.invoke(ask_cli, ["CRP", "--db", str(db_path)])
        assert result.exit_code == 0
        assert len(result.output) > 0
