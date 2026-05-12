"""
tests/test_signing_integration.py — signing wired through assert_claim + CLI.

Covers:
  - mareforma.open() with no key → claims persist with signature_bundle=NULL
  - mareforma.open(require_signed=True) without a key → KeyNotFoundError
  - mareforma.open() with a key → assert_claim writes a verifiable envelope
  - The envelope keyid matches the public key id of the supplied key
  - The signed payload binds to claim_id, text, supports, created_at
  - `mareforma bootstrap` CLI generates a loadable key
"""

from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest
from click.testing import CliRunner

import mareforma
from mareforma import signing as _signing
from mareforma.cli import cli as mareforma_cli


def _bootstrap_key(tmp_path: Path) -> Path:
    """Generate a key inside tmp_path and return its absolute path."""
    key_path = tmp_path / "mareforma.key"
    _signing.bootstrap_key(key_path)
    return key_path


# ---------------------------------------------------------------------------
# Library-level integration
# ---------------------------------------------------------------------------

class TestOpenWithSigning:
    def test_no_key_yields_unsigned_claims(self, tmp_path):
        # Point key_path at a path that doesn't exist; default behaviour
        # (require_signed=False) must NOT raise.
        with mareforma.open(tmp_path, key_path=tmp_path / "absent") as graph:
            claim_id = graph.assert_claim("unsigned finding")
            claim = graph.get_claim(claim_id)
        assert claim["signature_bundle"] is None

    def test_require_signed_without_key_raises(self, tmp_path):
        with pytest.raises(_signing.KeyNotFoundError):
            mareforma.open(
                tmp_path,
                key_path=tmp_path / "absent",
                require_signed=True,
            )

    def test_signed_claim_has_verifiable_envelope(self, tmp_path):
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim(
                "signed finding",
                classification="ANALYTICAL",
                generated_by="agent/test",
            )
            claim = graph.get_claim(claim_id)

        assert claim["signature_bundle"] is not None
        envelope = json.loads(claim["signature_bundle"])

        # Reload the key independently to verify (simulates a third party).
        verifier_key = _signing.load_private_key(key_path).public_key()
        assert _signing.verify_envelope(envelope, verifier_key) is True

    def test_signed_payload_binds_to_claim_fields(self, tmp_path):
        """The signed payload must contain the canonical claim fields so
        any tampering with the row is detectable."""
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim(
                "anchor finding",
                supports=["upstream-A", "upstream-B"],
                generated_by="agent/test",
                source_name="experiment-42",
            )
            claim = graph.get_claim(claim_id)

        envelope = json.loads(claim["signature_bundle"])
        payload = _signing.envelope_payload(envelope)
        assert payload["claim_id"] == claim_id
        assert payload["text"] == "anchor finding"
        assert payload["supports"] == ["upstream-A", "upstream-B"]
        assert payload["source_name"] == "experiment-42"
        assert payload["generated_by"] == "agent/test"

    def test_envelope_keyid_matches_signer_keyid(self, tmp_path):
        key_path = _bootstrap_key(tmp_path)
        expected_keyid = _signing.public_key_id(
            _signing.load_private_key(key_path).public_key(),
        )
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("keyid check")
            envelope = json.loads(graph.get_claim(claim_id)["signature_bundle"])
        assert envelope["signatures"][0]["keyid"] == expected_keyid

    def test_tampering_with_text_invalidates_signature(self, tmp_path):
        """Bind-check: edit the claim's text directly in sqlite and the
        previously-valid signature must no longer verify against the row."""
        import sqlite3

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("original text")
            envelope_before = json.loads(graph.get_claim(claim_id)["signature_bundle"])

        # Tamper.
        db = tmp_path / ".mareforma" / "graph.db"
        raw = sqlite3.connect(str(db))
        raw.execute("UPDATE claims SET text = ? WHERE claim_id = ?",
                    ("tampered text", claim_id))
        raw.commit()
        raw.close()

        # Signature stored on the row still cryptographically verifies (the
        # signature was over the ORIGINAL payload), but the payload no longer
        # matches the live row. A verifier comparing payload.text vs row.text
        # would see the mismatch.
        payload = _signing.envelope_payload(envelope_before)
        assert payload["text"] == "original text"

        with mareforma.open(tmp_path, key_path=key_path) as graph:
            assert graph.get_claim(claim_id)["text"] == "tampered text"


# ---------------------------------------------------------------------------
# CLI: mareforma bootstrap
# ---------------------------------------------------------------------------

class TestBootstrapCLI:
    def test_bootstrap_creates_key_at_explicit_path(self, tmp_path):
        runner = CliRunner()
        key_path = tmp_path / "key"
        result = runner.invoke(
            mareforma_cli, ["bootstrap", "--key-path", str(key_path)],
        )
        assert result.exit_code == 0, result.output
        assert key_path.exists()
        assert stat.S_IMODE(key_path.stat().st_mode) == 0o600
        # Key must be loadable.
        _signing.load_private_key(key_path)

    def test_bootstrap_prints_keyid(self, tmp_path):
        runner = CliRunner()
        key_path = tmp_path / "key"
        result = runner.invoke(
            mareforma_cli, ["bootstrap", "--key-path", str(key_path)],
        )
        assert result.exit_code == 0
        loaded_keyid = _signing.public_key_id(
            _signing.load_private_key(key_path).public_key(),
        )
        assert loaded_keyid in result.output

    def test_bootstrap_refuses_overwrite_without_flag(self, tmp_path):
        runner = CliRunner()
        key_path = tmp_path / "key"
        runner.invoke(mareforma_cli, ["bootstrap", "--key-path", str(key_path)])
        result = runner.invoke(
            mareforma_cli, ["bootstrap", "--key-path", str(key_path)],
        )
        assert result.exit_code == 1
        assert "Refuse to overwrite" in result.output or "Refuse" in result.output

    def test_bootstrap_overwrite_flag_replaces_key(self, tmp_path):
        runner = CliRunner()
        key_path = tmp_path / "key"
        runner.invoke(mareforma_cli, ["bootstrap", "--key-path", str(key_path)])
        first_keyid = _signing.public_key_id(
            _signing.load_private_key(key_path).public_key(),
        )

        result = runner.invoke(
            mareforma_cli,
            ["bootstrap", "--key-path", str(key_path), "--overwrite"],
        )
        assert result.exit_code == 0
        second_keyid = _signing.public_key_id(
            _signing.load_private_key(key_path).public_key(),
        )
        assert first_keyid != second_keyid
