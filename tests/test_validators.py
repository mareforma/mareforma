"""
tests/test_validators.py — Per-project validator enrollment.

Covers:
  - auto_enroll_root self-enrolls on a fresh graph; idempotent on re-open
  - count_validators / get_validator / list_validators reflect inserts
  - enroll_validator refuses when the parent signer is not enrolled
  - enroll_validator refuses to re-enroll an existing keyid
  - enroll_validator chain: root signs validator B; B signs validator C
  - enrollment envelopes verify against the parent's pubkey
  - graph.validate() requires loaded signer + enrolled keyid
  - graph.validate() persists a signed validation envelope that verifies
  - CLI: mareforma validator add (file path + inline PEM)
  - CLI: mareforma validator list (text + json)
"""

from __future__ import annotations

import base64
import json
import sqlite3
from pathlib import Path

import pytest
from click.testing import CliRunner

import mareforma
from mareforma import signing as _signing
from mareforma import validators as _validators
from mareforma.cli import cli as mareforma_cli
from mareforma.db import open_db


def _bootstrap_key(tmp_path: Path, name: str = "mareforma.key") -> Path:
    key_path = tmp_path / name
    _signing.bootstrap_key(key_path)
    return key_path


# ---------------------------------------------------------------------------
# auto_enroll_root
# ---------------------------------------------------------------------------

class TestAutoEnrollRoot:
    def test_root_enrolls_on_first_open(self, tmp_path: Path) -> None:
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            keyid = _signing.public_key_id(graph._signer.public_key())
            assert _validators.is_enrolled(graph._conn, keyid)
            assert _validators.count_validators(graph._conn) == 1

            row = _validators.get_validator(graph._conn, keyid)
            assert row is not None
            assert row["keyid"] == keyid
            assert row["enrolled_by_keyid"] == keyid  # self-signed root
            assert row["identity"] == "root"

    def test_reopen_with_same_key_is_idempotent(self, tmp_path: Path) -> None:
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            keyid = _signing.public_key_id(graph._signer.public_key())
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            assert _validators.count_validators(graph._conn) == 1
            assert _validators.is_enrolled(graph._conn, keyid)

    def test_no_signer_no_enrollment(self, tmp_path: Path) -> None:
        # Open without a key — no validators table populated.
        with mareforma.open(tmp_path, key_path=tmp_path / "absent") as graph:
            assert _validators.count_validators(graph._conn) == 0

    def test_root_enrollment_envelope_verifies(self, tmp_path: Path) -> None:
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            keyid = _signing.public_key_id(graph._signer.public_key())
            row = _validators.get_validator(graph._conn, keyid)
        pubkey_pem = base64.standard_b64decode(row["pubkey_pem"])
        assert _validators.verify_enrollment(row, pubkey_pem) is True


# ---------------------------------------------------------------------------
# enroll_validator (root signs B)
# ---------------------------------------------------------------------------

class TestEnrollValidator:
    def test_enroll_new_validator_under_root(self, tmp_path: Path) -> None:
        key_path = _bootstrap_key(tmp_path)
        new_key = _signing.generate_keypair()
        new_pem = _signing.public_key_to_pem(new_key.public_key())

        with mareforma.open(tmp_path, key_path=key_path) as graph:
            row = _validators.enroll_validator(
                graph._conn, graph._signer, new_pem,
                identity="alice@lab.example",
            )

        assert row["identity"] == "alice@lab.example"
        new_keyid = _signing.public_key_id(new_key.public_key())
        assert row["keyid"] == new_keyid
        # enrolled_by is the root signer, not self.
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            root_keyid = _signing.public_key_id(graph._signer.public_key())
        assert row["enrolled_by_keyid"] == root_keyid
        assert row["enrolled_by_keyid"] != row["keyid"]

    def test_enroll_chain_root_then_b_then_c(self, tmp_path: Path) -> None:
        """root → enrolls B → B then enrolls C."""
        root_key_path = _bootstrap_key(tmp_path, "root.key")

        b_key = _signing.generate_keypair()
        b_pem = _signing.public_key_to_pem(b_key.public_key())
        b_keyid = _signing.public_key_id(b_key.public_key())

        c_key = _signing.generate_keypair()
        c_pem = _signing.public_key_to_pem(c_key.public_key())

        # Root enrolls B.
        with mareforma.open(tmp_path, key_path=root_key_path) as graph:
            _validators.enroll_validator(
                graph._conn, graph._signer, b_pem, identity="B",
            )
            # B is now an enrolled validator. enroll C, signed by B.
            _validators.enroll_validator(
                graph._conn, b_key, c_pem, identity="C",
            )

        # All three live; chain follows enrolled_by.
        with mareforma.open(tmp_path, key_path=root_key_path) as graph:
            all_rows = _validators.list_validators(graph._conn)
        assert len(all_rows) == 3
        identities = {r["identity"] for r in all_rows}
        assert identities == {"root", "B", "C"}

        rows_by_id = {r["identity"]: r for r in all_rows}
        assert rows_by_id["B"]["enrolled_by_keyid"] == rows_by_id["root"]["keyid"]
        assert rows_by_id["C"]["enrolled_by_keyid"] == b_keyid

    def test_enroll_by_unenrolled_signer_raises(self, tmp_path: Path) -> None:
        root_key_path = _bootstrap_key(tmp_path, "root.key")
        # A signer that has NOT been enrolled tries to add C.
        outsider = _signing.generate_keypair()
        c_pem = _signing.public_key_to_pem(_signing.generate_keypair().public_key())

        with mareforma.open(tmp_path, key_path=root_key_path) as graph:
            with pytest.raises(_validators.ValidatorNotEnrolledError):
                _validators.enroll_validator(
                    graph._conn, outsider, c_pem, identity="C",
                )

    def test_enroll_existing_keyid_raises(self, tmp_path: Path) -> None:
        root_key_path = _bootstrap_key(tmp_path, "root.key")
        # The root tries to re-enroll itself with a different identity.
        with mareforma.open(tmp_path, key_path=root_key_path) as graph:
            root_pem = _signing.public_key_to_pem(graph._signer.public_key())
            with pytest.raises(_validators.ValidatorAlreadyEnrolledError):
                _validators.enroll_validator(
                    graph._conn, graph._signer, root_pem, identity="root-clone",
                )

    def test_enrolled_validator_envelope_verifies_under_parent(
        self, tmp_path: Path,
    ) -> None:
        root_key_path = _bootstrap_key(tmp_path, "root.key")
        new_key = _signing.generate_keypair()
        new_pem = _signing.public_key_to_pem(new_key.public_key())

        with mareforma.open(tmp_path, key_path=root_key_path) as graph:
            new_row = _validators.enroll_validator(
                graph._conn, graph._signer, new_pem, identity="alice",
            )
            root_pem = _signing.public_key_to_pem(graph._signer.public_key())

        # The new validator's envelope was signed by root → must verify
        # against root's pubkey.
        assert _validators.verify_enrollment(new_row, root_pem) is True
        # And NOT against the new key's own pubkey (wrong signer).
        assert _validators.verify_enrollment(new_row, new_pem) is False


# ---------------------------------------------------------------------------
# graph.validate() — identity check + signed envelope
# ---------------------------------------------------------------------------

class TestValidateIdentityCheck:
    def _setup_replicated(self, graph) -> str:
        """Helper: assert seed + 2 agents to produce a REPLICATED claim."""
        seed = graph.assert_claim("seed", generated_by="seed")
        id_a = graph.assert_claim(
            "finding", supports=[seed], generated_by="agent-A",
        )
        graph.assert_claim(
            "finding", supports=[seed], generated_by="agent-B",
        )
        assert graph.get_claim(id_a)["support_level"] == "REPLICATED"
        return id_a

    def test_validate_requires_loaded_signer(self, tmp_path: Path) -> None:
        # Open WITHOUT a key — validate() must refuse.
        with mareforma.open(tmp_path, key_path=tmp_path / "absent") as graph:
            id_a = self._setup_replicated(graph)
            with pytest.raises(ValueError, match="loaded signing key"):
                graph.validate(id_a)

    def test_validate_requires_enrolled_validator(self, tmp_path: Path) -> None:
        """A signer that exists but is not in the validators table is refused.

        We construct this by:
          1) Bootstrapping key A, opening (auto-enrolls A as root)
          2) Bootstrapping key B in a different path
          3) Opening with key B → B sees a non-empty validators table
             (A is the root), so B does NOT auto-enroll
          4) validate() with B's signer raises because B isn't enrolled
        """
        key_a = _bootstrap_key(tmp_path, "a.key")
        with mareforma.open(tmp_path, key_path=key_a) as graph:
            id_a = self._setup_replicated(graph)

        key_b = _bootstrap_key(tmp_path, "b.key")
        with mareforma.open(tmp_path, key_path=key_b) as graph:
            # B sees the validators table is non-empty; auto_enroll_root
            # is a no-op for B.
            assert _validators.count_validators(graph._conn) == 1
            b_keyid = _signing.public_key_id(graph._signer.public_key())
            assert not _validators.is_enrolled(graph._conn, b_keyid)

            with pytest.raises(ValueError, match="not an enrolled validator"):
                graph.validate(id_a)

    def test_validate_persists_signed_envelope(self, tmp_path: Path) -> None:
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            id_a = self._setup_replicated(graph)
            graph.validate(id_a, validated_by="display@lab.example")
            claim = graph.get_claim(id_a)

        assert claim["support_level"] == "ESTABLISHED"
        assert claim["validated_by"] == "display@lab.example"
        assert claim["validation_signature"] is not None

        envelope = json.loads(claim["validation_signature"])
        # The envelope verifies against the loaded key.
        verifier_key = _signing.load_private_key(key_path).public_key()
        assert _signing.verify_envelope(
            envelope, verifier_key,
            expected_payload_type=_signing._PAYLOAD_TYPE_VALIDATION,
        ) is True

        # And the payload binds the claim_id + validator's real keyid.
        payload = _signing.envelope_payload(envelope)
        assert payload["claim_id"] == id_a
        assert payload["validator_keyid"] == _signing.public_key_id(verifier_key)


# ---------------------------------------------------------------------------
# CLI: mareforma validator add / list
# ---------------------------------------------------------------------------

class TestValidatorCLI:
    def test_validator_add_with_pem_file(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        root_key_path = _bootstrap_key(tmp_path, "root.key")
        # Promote into XDG default location so the CLI's mareforma.open()
        # picks it up.
        xdg_key = _signing.default_key_path()
        xdg_key.parent.mkdir(parents=True, exist_ok=True)
        xdg_key.write_bytes(root_key_path.read_bytes())
        import os
        os.chmod(xdg_key, 0o600)

        # Open once via library to auto-enroll root.
        with mareforma.open(tmp_path):
            pass

        new_key = _signing.generate_keypair()
        new_pem_path = tmp_path / "alice.pub.pem"
        new_pem_path.write_bytes(_signing.public_key_to_pem(new_key.public_key()))

        runner = CliRunner()
        result = runner.invoke(
            mareforma_cli,
            ["validator", "add", "--pubkey", str(new_pem_path), "--identity", "alice"],
        )
        assert result.exit_code == 0, result.output
        assert "Enrolled validator alice" in result.output

        # The row is in the project graph.
        conn = open_db(tmp_path)
        try:
            new_keyid = _signing.public_key_id(new_key.public_key())
            assert _validators.is_enrolled(conn, new_keyid)
        finally:
            conn.close()

    def test_validator_list_shows_root_and_extras(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        root_key_path = _bootstrap_key(tmp_path, "root.key")
        xdg_key = _signing.default_key_path()
        xdg_key.parent.mkdir(parents=True, exist_ok=True)
        xdg_key.write_bytes(root_key_path.read_bytes())
        import os
        os.chmod(xdg_key, 0o600)

        # Open + auto-enroll root.
        with mareforma.open(tmp_path):
            pass

        # Enroll a second validator.
        new_key = _signing.generate_keypair()
        new_pem_path = tmp_path / "bob.pub.pem"
        new_pem_path.write_bytes(_signing.public_key_to_pem(new_key.public_key()))

        runner = CliRunner()
        result = runner.invoke(
            mareforma_cli,
            ["validator", "add", "--pubkey", str(new_pem_path), "--identity", "bob"],
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke(mareforma_cli, ["validator", "list"])
        assert result.exit_code == 0, result.output
        assert "root" in result.output
        assert "(root)" in result.output  # marker for self-enrolled
        assert "bob" in result.output

    def test_validator_list_json(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        root_key_path = _bootstrap_key(tmp_path, "root.key")
        xdg_key = _signing.default_key_path()
        xdg_key.parent.mkdir(parents=True, exist_ok=True)
        xdg_key.write_bytes(root_key_path.read_bytes())
        import os
        os.chmod(xdg_key, 0o600)

        with mareforma.open(tmp_path):
            pass

        runner = CliRunner()
        result = runner.invoke(mareforma_cli, ["validator", "list", "--json"])
        assert result.exit_code == 0
        rows = json.loads(result.output)
        assert len(rows) == 1
        assert rows[0]["identity"] == "root"

    def test_validator_add_rejects_invalid_pem(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        root_key_path = _bootstrap_key(tmp_path, "root.key")
        xdg_key = _signing.default_key_path()
        xdg_key.parent.mkdir(parents=True, exist_ok=True)
        xdg_key.write_bytes(root_key_path.read_bytes())
        import os
        os.chmod(xdg_key, 0o600)

        with mareforma.open(tmp_path):
            pass

        runner = CliRunner()
        result = runner.invoke(
            mareforma_cli,
            ["validator", "add", "--pubkey", "not a real pem", "--identity", "x"],
        )
        assert result.exit_code == 1
        assert "Invalid public key" in result.output
