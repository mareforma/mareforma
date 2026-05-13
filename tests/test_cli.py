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
    def _ensure_xdg_key(self) -> None:
        """Bootstrap the XDG signing key the CLI's validate path needs.

        ``mareforma claim validate`` now routes through ``graph.validate()``
        which requires an enrolled validator. Bootstrapping the default
        XDG key auto-enrolls it as root on first project open.
        """
        from mareforma import signing as _signing
        key_path = _signing.default_key_path()
        if not key_path.exists():
            _signing.bootstrap_key(key_path)

    def _make_replicated_claim_id(self) -> tuple[str, str]:
        """Return (prior_id, replicated_id).

        Claims are asserted under a generator key distinct from the XDG
        validator key, then XDG is enrolled as a second validator. The
        substrate refuses self-validation; the CLI must run as a
        different signer from the one that signed the claim.
        """
        import mareforma
        from mareforma import signing as _signing

        gen_key_path = Path("generator.key")
        if not gen_key_path.exists():
            _signing.bootstrap_key(gen_key_path)

        with mareforma.open(key_path=gen_key_path) as g:
            prior = g.assert_claim("upstream reference", generated_by="seed", seed=True)
            rep_id = g.assert_claim("finding A", supports=[prior], generated_by="agent-A")
            g.assert_claim("finding B", supports=[prior], generated_by="agent-B")
            xdg_pem = _signing.public_key_to_pem(
                _signing.load_private_key(_signing.default_key_path()).public_key(),
            )
            g.enroll_validator(xdg_pem, identity="xdg-validator")
        return prior, rep_id

    def test_validate_success(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            self._ensure_xdg_key()
            _, rep_id = self._make_replicated_claim_id()
            result = runner.invoke(cli, ["claim", "validate", rep_id],
                                   catch_exceptions=False)
        assert result.exit_code == 0
        assert "ESTABLISHED" in result.output

    def test_validate_not_found_exits_1(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            self._ensure_xdg_key()
            result = runner.invoke(cli, ["claim", "validate", "nonexistent-id"])
        assert result.exit_code == 1

    def test_validate_preliminary_claim_exits_1(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            self._ensure_xdg_key()
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
            self._ensure_xdg_key()
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
        # CLI now produces a signed envelope persisted to the row.
        assert data["validation_signature"] is not None


# ---------------------------------------------------------------------------
# key show
# ---------------------------------------------------------------------------

class TestKeyShow:
    def test_key_show_no_key_exits_1(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["key", "show"])
        assert result.exit_code == 1
        assert "No signing key" in result.output
        assert "mareforma bootstrap" in result.output

    def test_key_show_default_emits_keyid_and_pem(self, tmp_path: Path) -> None:
        from mareforma import signing as _signing
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _signing.bootstrap_key(_signing.default_key_path())
            result = runner.invoke(cli, ["key", "show"],
                                   catch_exceptions=False)
        assert result.exit_code == 0
        assert "keyid:" in result.output
        assert "-----BEGIN PUBLIC KEY-----" in result.output
        assert "-----END PUBLIC KEY-----" in result.output

    def test_key_show_pem_flag_emits_only_pem(self, tmp_path: Path) -> None:
        from mareforma import signing as _signing
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _signing.bootstrap_key(_signing.default_key_path())
            result = runner.invoke(cli, ["key", "show", "--pem"],
                                   catch_exceptions=False)
        assert result.exit_code == 0
        # First non-empty line must be the PEM header — pipe-able to a file.
        first_nonempty = next(
            line for line in result.output.splitlines() if line.strip()
        )
        assert first_nonempty == "-----BEGIN PUBLIC KEY-----"
        assert "-----END PUBLIC KEY-----" in result.output
        # No human-readable framing leaked in.
        assert "keyid:" not in result.output
        assert "Signing key at" not in result.output

    def test_key_show_keyid_flag_emits_only_hex(self, tmp_path: Path) -> None:
        from mareforma import signing as _signing
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _signing.bootstrap_key(_signing.default_key_path())
            result = runner.invoke(cli, ["key", "show", "--keyid"],
                                   catch_exceptions=False)
        assert result.exit_code == 0
        line = result.output.strip()
        # SHA-256 hex digest: 64 lowercase hex chars, nothing else.
        assert len(line) == 64
        assert all(c in "0123456789abcdef" for c in line)

    def test_key_show_pem_and_keyid_mutually_exclusive(
        self, tmp_path: Path,
    ) -> None:
        from mareforma import signing as _signing
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _signing.bootstrap_key(_signing.default_key_path())
            result = runner.invoke(cli, ["key", "show", "--pem", "--keyid"])
        assert result.exit_code == 1
        assert "mutually exclusive" in result.output

    def test_key_show_explicit_path(self, tmp_path: Path) -> None:
        from mareforma import signing as _signing
        custom = tmp_path / "custom.key"
        _signing.bootstrap_key(custom)
        runner = CliRunner()
        result = runner.invoke(
            cli, ["key", "show", "--key-path", str(custom), "--keyid"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        assert len(result.output.strip()) == 64

    def test_key_show_pem_matches_loaded_key(self, tmp_path: Path) -> None:
        """The CLI must emit the SAME PEM the substrate computes for the
        same private key. Without this, `key show --pem | validator add
        --pubkey -` would silently enroll the wrong identity."""
        from mareforma import signing as _signing
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _signing.bootstrap_key(_signing.default_key_path())
            result = runner.invoke(cli, ["key", "show", "--pem"],
                                   catch_exceptions=False)
            expected = _signing.public_key_to_pem(
                _signing.load_private_key(
                    _signing.default_key_path(),
                ).public_key(),
            ).decode("ascii")
        assert result.output == expected


# ---------------------------------------------------------------------------
# bootstrap output — next-step hint
# ---------------------------------------------------------------------------

class TestBootstrapHint:
    def test_bootstrap_prints_enrollment_next_steps(
        self, tmp_path: Path,
    ) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["bootstrap"],
                                   catch_exceptions=False)
        assert result.exit_code == 0
        assert "Next steps:" in result.output
        # Names the rule that traps first-time users.
        assert "self-validation" in result.output
        # Points at the exact commands needed to unblock it.
        assert "mareforma key show" in result.output
        assert "mareforma validator add" in result.output


# ---------------------------------------------------------------------------
# claim validate — error-translation paths
# ---------------------------------------------------------------------------

class TestClaimValidateErrors:
    def test_self_validation_surfaces_resolution_hint(
        self, tmp_path: Path,
    ) -> None:
        """When the loaded validator key is the same key that signed the
        claim, the CLI must surface SelfValidationError text + a
        concrete resolution pointing at validator add / key show.
        Without it, a first-time user gets a Python traceback because
        SelfValidationError doesn't inherit from ValueError."""
        from mareforma import signing as _signing
        import mareforma
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _signing.bootstrap_key(_signing.default_key_path())
            # Build a REPLICATED claim signed by the XDG key (the same
            # key the CLI will load when we invoke `claim validate`).
            with mareforma.open() as g:
                prior = g.assert_claim(
                    "upstream", generated_by="seed", seed=True,
                )
                a = g.assert_claim(
                    "shared finding", supports=[prior],
                    generated_by="agent-A",
                )
                g.assert_claim(
                    "shared finding restated", supports=[prior],
                    generated_by="agent-B",
                )
                assert g.get_claim(a)["support_level"] == "REPLICATED"
            result = runner.invoke(cli, ["claim", "validate", a])
        assert result.exit_code == 1
        # Substrate's own message comes through (no traceback).
        assert "self-promotion is refused" in result.output
        # CLI adds the resolution pointing at the relevant commands.
        assert "Resolution:" in result.output
        assert "mareforma validator add" in result.output
        assert "mareforma key show" in result.output
