"""The retired surfaces are gone and nothing imports them.

Asserts the removed public surface can no longer be imported, the removed
CLI commands are absent from ``mareforma --help``, and the removed tables
are no longer created on a fresh graph.
"""
from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from click.testing import CliRunner

import mareforma
from mareforma.cli import cli


class TestRemovedModulesUnimportable:
    @pytest.mark.parametrize(
        "module",
        [
            "mareforma._evidence",
            "mareforma.ingest_command",
            "mareforma.ask_command",
            "mareforma.exporters.narrative",
            "mareforma._literature_health",
            "mareforma.hooks",
        ],
    )
    def test_module_gone(self, module: str) -> None:
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(module)


class TestRemovedPublicNames:
    @pytest.mark.parametrize("name", ["EvidenceVector", "EvidenceVectorError",
                                      "VALID_STUDY_DESIGNS"])
    def test_evidence_names_gone(self, name: str) -> None:
        assert not hasattr(mareforma, name)
        assert name not in mareforma.__all__

    @pytest.mark.parametrize(
        "name",
        ["resolve_doi", "resolve_dois_with_cache", "find_drifted_dois",
         "fetch_doi_metadata", "clear_unresolved_cache",
         "_reset_client_for_testing"],
    )
    def test_doi_head_check_surface_gone(self, name: str) -> None:
        from mareforma import doi_resolver
        assert not hasattr(doi_resolver, name)

    def test_doi_list_filter_gone(self) -> None:
        # The cut took every caller of extract_dois; is_doi is what survives,
        # and the graph reaches it directly at module scope.
        from mareforma.db import core
        from mareforma import doi_resolver
        assert not hasattr(doi_resolver, "extract_dois")
        assert core.is_doi is doi_resolver.is_doi

    @pytest.mark.parametrize(
        "method",
        ["refresh_unresolved", "refresh_all_dois", "find_drifted_dois"],
    )
    def test_graph_doi_methods_gone(self, method: str, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            assert not hasattr(graph, method)


class TestRemovedCliCommands:
    @pytest.mark.parametrize("command", ["ingest", "ask", "narrative"])
    def test_command_absent_from_help(self, command: str) -> None:
        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert command not in result.output

    @pytest.mark.parametrize("command", ["ingest", "ask", "narrative"])
    def test_command_not_invocable(self, command: str) -> None:
        result = CliRunner().invoke(cli, [command, "--help"])
        assert result.exit_code != 0


class TestRemovedTablesNotCreated:
    @pytest.mark.parametrize(
        "table", ["doi_cache", "literature_claims", "agent_activities"],
    )
    def test_table_absent_on_fresh_graph(self, table: str, tmp_path: Path) -> None:
        from mareforma.db import open_db

        conn = open_db(tmp_path)
        try:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
        finally:
            conn.close()
        assert row is None
