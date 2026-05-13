"""Restore-from-claims.toml tests (spec.md #99).

``mareforma.restore(project_root)`` rebuilds a fresh graph.db from the
TOML state file written by every mutation. The rebuild is fresh-only
(refuses non-empty graph.db) and fail-all-or-nothing on signature
verification. The adversarial test class is the load-bearing one — it
documents what tampering the restore path must catch.
"""

from __future__ import annotations

import base64
import json
import sqlite3
from pathlib import Path

import pytest

import mareforma
from mareforma import db as _db
from mareforma import signing as _signing


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bootstrap_key(tmp_path: Path, name: str) -> Path:
    key_path = tmp_path / name
    _signing.bootstrap_key(key_path)
    return key_path


def _pem_of(key_path: Path) -> bytes:
    return _signing.public_key_to_pem(
        _signing.load_private_key(key_path).public_key(),
    )


def _build_full_graph(tmp_path: Path) -> dict:
    """Populate a project with the full v0.3.0 substrate: root validator,
    second validator, seed claim, REPLICATED pair, ESTABLISHED claim,
    one unsigned PRELIMINARY (in a separate unsigned-mode project).

    Returns identifiers used by tests for verification.
    """
    root_key = _bootstrap_key(tmp_path, "root.key")
    val_key = _bootstrap_key(tmp_path, "val.key")

    with mareforma.open(tmp_path, key_path=root_key) as g:
        seed_id = g.assert_claim("anchor", generated_by="seed", seed=True)
        rep_id = g.assert_claim("converged", supports=[seed_id], generated_by="A")
        g.assert_claim("converged", supports=[seed_id], generated_by="B")
        g.enroll_validator(_pem_of(val_key), identity="v")

    with mareforma.open(tmp_path, key_path=val_key) as g:
        g.validate(rep_id)
        assert g.get_claim(rep_id)["support_level"] == "ESTABLISHED"

    return {
        "root_key": root_key,
        "val_key": val_key,
        "seed_id": seed_id,
        "rep_id": rep_id,
    }


def _wipe_graph_db(tmp_path: Path) -> None:
    db_dir = tmp_path / ".mareforma"
    for f in db_dir.iterdir():
        f.unlink()
    db_dir.rmdir()


# ---------------------------------------------------------------------------
# Happy path: full round-trip
# ---------------------------------------------------------------------------

class TestRestoreHappyPath:
    def test_round_trip_preserves_claims_and_validators(
        self, tmp_path: Path,
    ) -> None:
        ctx = _build_full_graph(tmp_path)

        # Capture pre-state via the live graph.
        with mareforma.open(tmp_path, key_path=ctx["root_key"]) as g:
            pre_claims = sorted(
                g.query(include_unverified=True, limit=99),
                key=lambda c: c["created_at"],
            )
            from mareforma import validators as _validators
            pre_validators = _validators.list_validators(g._conn)
            pre_count = len(pre_claims)

        # Wipe graph.db; claims.toml survives.
        _wipe_graph_db(tmp_path)
        assert not (tmp_path / ".mareforma" / "graph.db").exists()

        result = mareforma.restore(tmp_path)
        assert result == {
            "validators_restored": len(pre_validators),
            "claims_restored": pre_count,
        }

        # Re-open the restored graph and confirm shape.
        with mareforma.open(tmp_path, key_path=ctx["root_key"]) as g:
            post_claims = sorted(
                g.query(include_unverified=True, limit=99),
                key=lambda c: c["created_at"],
            )
            post_validators = _validators.list_validators(g._conn)

        assert len(post_claims) == pre_count
        for pre, post in zip(pre_claims, post_claims):
            assert pre["claim_id"] == post["claim_id"]
            assert pre["text"] == post["text"]
            assert pre["support_level"] == post["support_level"]
            assert pre["signature_bundle"] == post["signature_bundle"]
            assert pre["validation_signature"] == post["validation_signature"]
            assert pre["validator_keyid"] == post["validator_keyid"]
            # prev_hash recomputed; must match because inputs and order
            # are identical and SHA256 is deterministic.
            assert pre["prev_hash"] == post["prev_hash"]
        assert {v["keyid"] for v in post_validators} == {
            v["keyid"] for v in pre_validators
        }

    def test_restore_rebuilds_fts_index(self, tmp_path: Path) -> None:
        """The INSERT triggers fire during restore, populating
        claims_fts. Search must work on the restored graph."""
        ctx = _build_full_graph(tmp_path)
        _wipe_graph_db(tmp_path)
        mareforma.restore(tmp_path)
        with mareforma.open(tmp_path, key_path=ctx["root_key"]) as g:
            results = g.search("converged")
        # Two REPLICATED claims share the text "converged".
        assert len(results) >= 1
        # And one carries the validator_reputation projection.
        ranked = [r for r in results if r["support_level"] == "ESTABLISHED"]
        if ranked:
            assert ranked[0]["validator_reputation"] >= 1

    def test_restore_returns_zeros_on_unsigned_empty_project(
        self, tmp_path: Path,
    ) -> None:
        """An unsigned project with claims still round-trips: no
        validators, claim signatures are NULL — that's mode-consistent."""
        with mareforma.open(tmp_path) as g:
            g.assert_claim("alpha")
            g.assert_claim("beta")
        _wipe_graph_db(tmp_path)
        result = mareforma.restore(tmp_path)
        assert result == {"validators_restored": 0, "claims_restored": 2}


# ---------------------------------------------------------------------------
# Refuse non-empty graph
# ---------------------------------------------------------------------------

class TestRestoreRefusesNonEmptyGraph:
    def test_refuses_when_graph_has_claims(self, tmp_path: Path) -> None:
        _build_full_graph(tmp_path)
        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "graph_not_empty"

    def test_accepts_when_graph_db_empty(self, tmp_path: Path) -> None:
        """Empty .mareforma/graph.db (claims table exists but has 0
        rows) is accepted — restore() proceeds normally."""
        ctx = _build_full_graph(tmp_path)
        # Wipe ROWS but keep the file. Re-open the live graph and delete
        # rows would trip the retracted-terminal trigger; easier: drop
        # the file entirely.
        _wipe_graph_db(tmp_path)
        # Calling open_db creates a fresh empty graph.db.
        conn = _db.open_db(tmp_path)
        conn.close()
        result = mareforma.restore(tmp_path)
        assert result["validators_restored"] >= 1


# ---------------------------------------------------------------------------
# Missing claims.toml
# ---------------------------------------------------------------------------

class TestRestoreMissingTOML:
    def test_missing_toml_raises(self, tmp_path: Path) -> None:
        # No graph, no TOML.
        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "toml_not_found"

    def test_explicit_toml_path_honored(self, tmp_path: Path) -> None:
        ctx = _build_full_graph(tmp_path)
        # Move claims.toml to a non-default location.
        moved = tmp_path / "backup" / "state.toml"
        moved.parent.mkdir()
        (tmp_path / "claims.toml").rename(moved)
        _wipe_graph_db(tmp_path)
        result = mareforma.restore(tmp_path, claims_toml=moved)
        assert result["claims_restored"] >= 1


# ---------------------------------------------------------------------------
# Adversarial: tampering is caught
# ---------------------------------------------------------------------------

class TestRestoreAdversarial:
    def _setup_and_wipe(self, tmp_path: Path) -> dict:
        ctx = _build_full_graph(tmp_path)
        _wipe_graph_db(tmp_path)
        return ctx

    def _read_toml(self, tmp_path: Path) -> dict:
        import tomli
        return tomli.loads(
            (tmp_path / "claims.toml").read_text(encoding="utf-8"),
        )

    def _write_toml(self, tmp_path: Path, data: dict) -> None:
        import tomli_w
        (tmp_path / "claims.toml").write_bytes(
            tomli_w.dumps(data).encode("utf-8"),
        )

    def test_tampered_claim_text_fails_verify(self, tmp_path: Path) -> None:
        """Edit a signed claim's text in claims.toml without re-signing.
        The signature_bundle remains the original bytes; restore must
        detect the field divergence and refuse."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        # Pick a signed claim (the seed has a bundle).
        signed_ids = [
            cid for cid, c in data["claims"].items()
            if c.get("signature_bundle")
        ]
        assert signed_ids
        victim = signed_ids[0]
        data["claims"][victim]["text"] = "TAMPERED — drug X causes effect Y"
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "claim_unverified"

    def test_tampered_signature_bytes_fail_verify(
        self, tmp_path: Path,
    ) -> None:
        """Mutate the base64 signature bytes; verify must fail."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        signed_ids = [
            cid for cid, c in data["claims"].items()
            if c.get("signature_bundle")
        ]
        victim = signed_ids[0]
        bundle = json.loads(data["claims"][victim]["signature_bundle"])
        # Flip a byte in the base64 signature.
        sig_bytes = bytearray(
            base64.standard_b64decode(bundle["signatures"][0]["sig"])
        )
        sig_bytes[0] ^= 0xFF
        bundle["signatures"][0]["sig"] = base64.standard_b64encode(
            bytes(sig_bytes)
        ).decode("ascii")
        data["claims"][victim]["signature_bundle"] = json.dumps(
            bundle, sort_keys=True, separators=(",", ":"),
        )
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "claim_unverified"

    def test_missing_signature_in_signed_mode_refused(
        self, tmp_path: Path,
    ) -> None:
        """Strip a signature_bundle from a signed-mode TOML — restore
        must refuse the mode-inconsistent graph."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        # Strip the bundle from one signed claim.
        for cid, c in data["claims"].items():
            if c.get("signature_bundle"):
                del c["signature_bundle"]
                break
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "mode_inconsistent"

    def test_orphan_signer_refused(self, tmp_path: Path) -> None:
        """A signature_bundle's keyid doesn't appear in the validators
        section — restore refuses the orphan signer."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        # Strip the validators section while leaving signed claims.
        # The signed claims now have signers not in the (empty)
        # validators set.
        del data["validators"]
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        # Either mode_inconsistent (the bundle exists but no validators)
        # or orphan_signer — both indicate the tampering.
        # The actual code path: signed_mode is False (no validators),
        # so claims with signature_bundle hit the orphan_signer check.
        assert exc_info.value.kind in ("orphan_signer", "mode_inconsistent")

    def test_tampered_validator_envelope_refused(
        self, tmp_path: Path,
    ) -> None:
        """Tamper with a validator's identity field in claims.toml —
        the enrollment envelope's signed payload no longer matches."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        non_root_keyids = [
            keyid for keyid, v in data["validators"].items()
            if v["enrolled_by_keyid"] != keyid
        ]
        assert non_root_keyids
        victim = non_root_keyids[0]
        data["validators"][victim]["identity"] = "TAMPERED-IDENTITY"
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "enrollment_unverified"

    def test_adversarial_text_round_trips(self, tmp_path: Path) -> None:
        """Newlines, quotes, control-like chars in source_name and text
        must round-trip through TOML and reload identically."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        adversarial_text = (
            'multi\nline\twith "quotes" and a backslash\\here'
        )
        adversarial_source = 'src "with quotes"'
        with mareforma.open(tmp_path, key_path=root_key) as g:
            cid = g.assert_claim(
                adversarial_text, source_name=adversarial_source,
            )

        # Round-trip.
        _wipe_graph_db(tmp_path)
        mareforma.restore(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            restored = g.get_claim(cid)
        assert restored["text"] == adversarial_text
        assert restored["source_name"] == adversarial_source


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

class TestRestoreCLI:
    def test_cli_restore_happy_path(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from click.testing import CliRunner
        from mareforma.cli import cli as mareforma_cli

        monkeypatch.chdir(tmp_path)
        ctx = _build_full_graph(tmp_path)
        _wipe_graph_db(tmp_path)

        runner = CliRunner()
        result = runner.invoke(mareforma_cli, ["restore"])
        assert result.exit_code == 0, result.output
        assert "validators_restored" in result.output
        assert "claims_restored" in result.output

    def test_cli_restore_refuses_non_empty(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from click.testing import CliRunner
        from mareforma.cli import cli as mareforma_cli

        monkeypatch.chdir(tmp_path)
        _build_full_graph(tmp_path)

        runner = CliRunner()
        result = runner.invoke(mareforma_cli, ["restore"])
        assert result.exit_code == 1
        assert "refuses to merge" in result.output
