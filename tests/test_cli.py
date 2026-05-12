"""tests/test_cli.py — smoke tests for the mareforma CLI (agent-native commands)."""

from __future__ import annotations

import json
import os
from pathlib import Path

from click.testing import CliRunner

from mareforma.cli import cli


# ---------------------------------------------------------------------------
# claim add
# ---------------------------------------------------------------------------

class TestClaimAdd:
    def test_add_exits_0(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["claim", "add", "Target T is elevated"],
                                   catch_exceptions=False)
        assert result.exit_code == 0

    def test_add_prints_claim_id(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["claim", "add", "Some finding"],
                                   catch_exceptions=False)
        assert "ID:" in result.output

    def test_add_with_classification(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli,
                ["claim", "add", "Analytical finding", "--classification", "ANALYTICAL"],
                catch_exceptions=False,
            )
        assert result.exit_code == 0
        assert "ANALYTICAL" in result.output

    def test_add_empty_text_exits_1(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["claim", "add", "   "])
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# claim list
# ---------------------------------------------------------------------------

class TestClaimList:
    def test_list_empty_exits_0(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["claim", "list"], catch_exceptions=False)
        assert result.exit_code == 0
        assert "No claims" in result.output

    def test_list_shows_added_claim(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["claim", "add", "Unique claim text XYZ"],
                          catch_exceptions=False)
            result = runner.invoke(cli, ["claim", "list"], catch_exceptions=False)
        assert "Unique claim text XYZ" in result.output

    def test_list_json_flag_emits_valid_json(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["claim", "add", "Claim for JSON test"],
                          catch_exceptions=False)
            result = runner.invoke(cli, ["claim", "list", "--json"],
                                   catch_exceptions=False)
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) == 1

    def test_list_filter_by_status(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["claim", "add", "Open claim"], catch_exceptions=False)
            runner.invoke(cli, ["claim", "add", "Contested claim",
                                "--status", "contested"], catch_exceptions=False)
            result = runner.invoke(cli, ["claim", "list", "--status", "open"],
                                   catch_exceptions=False)
        assert "Open claim" in result.output
        assert "Contested claim" not in result.output


# ---------------------------------------------------------------------------
# claim show
# ---------------------------------------------------------------------------

class TestClaimShow:
    def _add_and_get_id(self, runner: CliRunner, text: str) -> str:
        result = runner.invoke(cli, ["claim", "add", text], catch_exceptions=False)
        return next(
            line.split("ID:")[-1].strip()
            for line in result.output.splitlines()
            if "ID:" in line
        )

    def test_show_exits_0_for_existing_claim(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            claim_id = self._add_and_get_id(runner, "Show test claim")
            result = runner.invoke(cli, ["claim", "show", claim_id],
                                   catch_exceptions=False)
        assert result.exit_code == 0
        assert "Show test claim" in result.output

    def test_show_missing_id_exits_1(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["claim", "show", "nonexistent-id"])
        assert result.exit_code == 1

    def test_show_json_flag(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            claim_id = self._add_and_get_id(runner, "JSON show test")
            result = runner.invoke(cli, ["claim", "show", claim_id, "--json"],
                                   catch_exceptions=False)
        data = json.loads(result.output)
        assert data["text"] == "JSON show test"


# ---------------------------------------------------------------------------
# claim update
# ---------------------------------------------------------------------------

class TestClaimUpdate:
    def _add_and_get_id(self, runner: CliRunner, text: str) -> str:
        result = runner.invoke(cli, ["claim", "add", text], catch_exceptions=False)
        return next(
            line.split("ID:")[-1].strip()
            for line in result.output.splitlines()
            if "ID:" in line
        )

    def test_update_status_exits_0(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            claim_id = self._add_and_get_id(runner, "Update test claim")
            result = runner.invoke(
                cli, ["claim", "update", claim_id, "--status", "contested"],
                catch_exceptions=False,
            )
        assert result.exit_code == 0

    def test_update_missing_id_exits_1(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli, ["claim", "update", "no-such-id", "--status", "contested"]
            )
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

class TestStatus:
    def test_status_red_on_empty_graph(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["status"], catch_exceptions=False)
        assert result.exit_code == 0
        assert "RED" in result.output

    def test_status_json_flag_emits_valid_json(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["status", "--json"], catch_exceptions=False)
        data = json.loads(result.output)
        assert "traffic_light" in data
        assert data["traffic_light"] == "red"

    def test_status_yellow_after_preliminary_claim(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["claim", "add", "Single agent finding"],
                          catch_exceptions=False)
            result = runner.invoke(cli, ["status"], catch_exceptions=False)
        assert "YELLOW" in result.output


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

class TestExport:
    def test_export_json_flag_emits_valid_json(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["export", "--json"], catch_exceptions=False)
        data = json.loads(result.output)
        assert "@context" in data
        assert "@graph" in data

    def test_export_includes_claims(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["claim", "add", "Exported claim ABC"],
                          catch_exceptions=False)
            result = runner.invoke(cli, ["export", "--json"], catch_exceptions=False)
        data = json.loads(result.output)
        claims = [n for n in data["@graph"] if n.get("@type") == "mare:Claim"]
        assert len(claims) == 1
        assert "Exported claim ABC" in claims[0]["claimText"]

    def test_export_creates_file(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["export"], catch_exceptions=False)
            written = Path(os.getcwd()) / "ontology.jsonld"
        assert result.exit_code == 0
        assert written.exists()


# ---------------------------------------------------------------------------
# claim validate
# ---------------------------------------------------------------------------

class TestClaimValidate:
    def _make_replicated_claim_id(self) -> tuple[str, str]:
        """Return (prior_id, replicated_id) using the Python API in cwd."""
        import mareforma
        with mareforma.open() as g:
            prior = g.assert_claim("upstream reference", generated_by="seed")
            rep_id = g.assert_claim("finding A", supports=[prior], generated_by="agent-A")
            g.assert_claim("finding B", supports=[prior], generated_by="agent-B")
        return prior, rep_id

    def test_validate_success(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _, rep_id = self._make_replicated_claim_id()
            result = runner.invoke(cli, ["claim", "validate", rep_id],
                                   catch_exceptions=False)
        assert result.exit_code == 0
        assert "ESTABLISHED" in result.output

    def test_validate_not_found_exits_1(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["claim", "validate", "nonexistent-id"])
        assert result.exit_code == 1

    def test_validate_preliminary_claim_exits_1(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            add = runner.invoke(cli, ["claim", "add", "only one agent"],
                                catch_exceptions=False)
            claim_id = next(
                line.split("ID:")[-1].strip()
                for line in add.output.splitlines()
                if "ID:" in line
            )
            result = runner.invoke(cli, ["claim", "validate", claim_id])
        assert result.exit_code == 1
        assert "REPLICATED" in result.output

    def test_validate_with_validated_by(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _, rep_id = self._make_replicated_claim_id()
            runner.invoke(
                cli,
                ["claim", "validate", rep_id, "--validated-by", "reviewer@example.org"],
                catch_exceptions=False,
            )
            result = runner.invoke(cli, ["claim", "show", rep_id, "--json"],
                                   catch_exceptions=False)
        data = json.loads(result.output)
        assert data["validated_by"] == "reviewer@example.org"
