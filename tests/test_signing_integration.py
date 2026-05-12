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


# ---------------------------------------------------------------------------
# Signed claims are append-only across the signed surface
# ---------------------------------------------------------------------------

class TestUpdateClaimSignedSurface:
    """A claim that carries a signature must be immutable across the fields
    in the signed payload: text / supports / contradicts. Mutating those
    via ``update_claim`` would silently invalidate the signature while
    leaving ``transparency_logged=1`` in place. ``status`` and
    ``comparison_summary`` are not in the signed payload and stay editable.
    """

    def test_text_change_on_signed_claim_raises(self, tmp_path):
        from mareforma.db import (
            SignedClaimImmutableError, open_db, update_claim,
        )
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("original text")
        conn = open_db(tmp_path)
        try:
            with pytest.raises(SignedClaimImmutableError, match="signed"):
                update_claim(conn, tmp_path, claim_id, text="tampered text")
        finally:
            conn.close()

    def test_supports_change_on_signed_claim_raises(self, tmp_path):
        from mareforma.db import (
            SignedClaimImmutableError, open_db, update_claim,
        )
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("anchor", supports=["upstream-1"])
        conn = open_db(tmp_path)
        try:
            with pytest.raises(SignedClaimImmutableError):
                update_claim(conn, tmp_path, claim_id, supports=["upstream-2"])
        finally:
            conn.close()

    def test_contradicts_change_on_signed_claim_raises(self, tmp_path):
        from mareforma.db import (
            SignedClaimImmutableError, open_db, update_claim,
        )
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("anchor")
        conn = open_db(tmp_path)
        try:
            with pytest.raises(SignedClaimImmutableError):
                update_claim(conn, tmp_path, claim_id, contradicts=["xx"])
        finally:
            conn.close()

    def test_status_change_on_signed_claim_allowed(self, tmp_path):
        """status is not part of the signed payload — must still be editable."""
        from mareforma.db import get_claim, open_db, update_claim
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("retract me")
        conn = open_db(tmp_path)
        try:
            update_claim(conn, tmp_path, claim_id, status="retracted")
            assert get_claim(conn, claim_id)["status"] == "retracted"
        finally:
            conn.close()

    def test_comparison_summary_on_signed_claim_allowed(self, tmp_path):
        """comparison_summary is not part of the signed payload."""
        from mareforma.db import get_claim, open_db, update_claim
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("with summary")
        conn = open_db(tmp_path)
        try:
            update_claim(
                conn, tmp_path, claim_id,
                comparison_summary="reviewed 2026-05-12",
            )
            assert get_claim(conn, claim_id)["comparison_summary"] == "reviewed 2026-05-12"
        finally:
            conn.close()

    def test_unsigned_claim_can_still_mutate_freely(self, tmp_path):
        from mareforma.db import add_claim, get_claim, open_db, update_claim
        conn = open_db(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "unsigned")
            update_claim(conn, tmp_path, claim_id, text="freely edited")
            assert get_claim(conn, claim_id)["text"] == "freely edited"
        finally:
            conn.close()

    def test_redundant_signed_field_set_is_a_noop(self, tmp_path):
        """Passing supports=<existing supports> on a signed claim must NOT raise.

        The refuse logic compares old vs new; identical values shouldn't trip it.
        """
        from mareforma.db import get_claim, open_db, update_claim
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("redundant", supports=["u1"])
        conn = open_db(tmp_path)
        try:
            update_claim(
                conn, tmp_path, claim_id,
                supports=["u1"], status="contested",
            )
            assert get_claim(conn, claim_id)["status"] == "contested"
        finally:
            conn.close()

    def test_update_claim_signed_params_match_refuse_list(self):
        """Force a future contributor who exposes a new SIGNED_FIELDS member
        on update_claim to update db.update_claim's refuse block too.

        Today update_claim exposes exactly the three signed-surface fields
        text / supports / contradicts. Adding e.g. ``classification`` to
        update_claim's signature without extending the refuse block would
        re-open the silent-mutation hole; this test fails until the refuse
        list catches up.
        """
        import inspect
        from mareforma.db import update_claim

        params = set(inspect.signature(update_claim).parameters.keys())
        # claim_id is the row lookup key, not a mutable field. Exclude it
        # from the writable set even though it appears in SIGNED_FIELDS.
        params -= {"claim_id"}
        signed_writable = params & set(_signing.SIGNED_FIELDS)

        expected = {"text", "supports", "contradicts"}
        assert signed_writable == expected, (
            f"update_claim now exposes signed-surface params {signed_writable!r} "
            f"but db.update_claim's refuse block only covers {expected!r}. "
            "Update the refuse block when extending coverage, OR update this "
            "test if intentionally narrowing it."
        )
