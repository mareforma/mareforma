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
        seed = graph.assert_claim("seed", generated_by="seed", seed=True)
        id_a = graph.assert_claim(
            "finding", supports=[seed], generated_by="agent-A",
        )
        graph.assert_claim(
            "finding", supports=[seed], generated_by="agent-B",
        )
        assert graph.get_claim(id_a)["support_level"] == "REPLICATED"
        return id_a

    def test_validate_requires_loaded_signer(self, tmp_path: Path) -> None:
        # Bootstrap a key, build the REPLICATED chain via the seeded-
        # upstream path. Then re-open without a key and confirm
        # validate() refuses on the loaded-signer gate.
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            id_a = self._setup_replicated(graph)
        with mareforma.open(tmp_path, key_path=tmp_path / "absent") as graph:
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
        # Generator key signs the REPLICATED claim; a separately-enrolled
        # validator key is the only one allowed to promote it. Same-key
        # validation is refused by the substrate as self-promotion.
        root_key_path = _bootstrap_key(tmp_path, "root.key")
        validator_key_path = _bootstrap_key(tmp_path, "validator.key")
        validator_pubkey_pem = _signing.public_key_to_pem(
            _signing.load_private_key(validator_key_path).public_key(),
        )

        with mareforma.open(tmp_path, key_path=root_key_path) as graph:
            id_a = self._setup_replicated(graph)
            graph.enroll_validator(
                validator_pubkey_pem, identity="validator@lab.example",
            )

        with mareforma.open(tmp_path, key_path=validator_key_path) as graph:
            graph.validate(id_a, validated_by="display@lab.example")
            claim = graph.get_claim(id_a)

        assert claim["support_level"] == "ESTABLISHED"
        assert claim["validated_by"] == "display@lab.example"
        assert claim["validation_signature"] is not None

        envelope = json.loads(claim["validation_signature"])
        # The envelope verifies against the VALIDATOR key (not the root).
        verifier_key = _signing.load_private_key(validator_key_path).public_key()
        assert _signing.verify_envelope(
            envelope, verifier_key,
            expected_payload_type=_signing.PAYLOAD_TYPE_VALIDATION,
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


# ---------------------------------------------------------------------------
# Bootstrap race: BEGIN IMMEDIATE prevents two roots from coexisting
# ---------------------------------------------------------------------------

class TestBootstrapRace:
    def test_only_one_root_under_concurrent_first_open(self, tmp_path: Path) -> None:
        """Two threads each opening with a different key against the same
        fresh graph.db must result in EXACTLY ONE root validator. Without
        BEGIN IMMEDIATE, both threads could pass the count==0 check and
        each insert a self-signed root."""
        import threading

        from mareforma.db import open_db

        # Create the graph + schema once.
        open_db(tmp_path).close()

        key_a = _signing.generate_keypair()
        key_b = _signing.generate_keypair()
        keys = [key_a, key_b]
        results: list[object] = []
        barrier = threading.Barrier(2)

        def runner(key):
            barrier.wait()  # release both threads simultaneously
            conn = open_db(tmp_path)
            try:
                row = _validators.auto_enroll_root(conn, key, identity="thread")
                results.append(row)
            finally:
                conn.close()

        threads = [threading.Thread(target=runner, args=(k,)) for k in keys]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Inspect the final table.
        conn = open_db(tmp_path)
        try:
            all_rows = _validators.list_validators(conn)
        finally:
            conn.close()

        assert len(all_rows) == 1, (
            f"expected exactly one root, got {len(all_rows)} — "
            "BEGIN IMMEDIATE is supposed to serialize the racing writers"
        )

    def test_root_self_enrollment_emits_warning(self, tmp_path: Path) -> None:
        """A fresh-graph root enrollment must fire a UserWarning so the
        operator notices if they opened the project with the wrong key."""
        key_path = _bootstrap_key(tmp_path)
        with pytest.warns(UserWarning, match="root validator"):
            with mareforma.open(tmp_path, key_path=key_path):
                pass

    def test_reopen_with_same_key_does_not_warn(self, tmp_path: Path) -> None:
        """Auto-enrollment fires once. Re-opening with the same key does
        NOT re-warn (the enrollment already exists)."""
        import warnings as _warnings

        key_path = _bootstrap_key(tmp_path)
        # First open — warns. Drain it.
        with _warnings.catch_warnings():
            _warnings.simplefilter("ignore")
            with mareforma.open(tmp_path, key_path=key_path):
                pass

        # Second open — must not warn.
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            with mareforma.open(tmp_path, key_path=key_path):
                pass
        root_warnings = [w for w in caught if "root validator" in str(w.message)]
        assert root_warnings == [], (
            f"reopen warned about root enrollment: {root_warnings}"
        )


# ---------------------------------------------------------------------------
# verify_envelope: strict default now refuses cross-type envelopes
# ---------------------------------------------------------------------------

class TestVerifyEnvelopeStrictDefault:
    def test_default_accepts_claim_envelope(self) -> None:
        key = _signing.generate_keypair()
        envelope = _signing.sign_claim(
            {"claim_id": "c", "text": "x", "classification": "INFERRED",
             "generated_by": "a", "supports": [], "contradicts": [],
             "source_name": None, "created_at": "2026-05-12T00:00:00+00:00"},
            key,
        )
        assert _signing.verify_envelope(envelope, key.public_key()) is True

    def test_default_rejects_validation_envelope(self) -> None:
        """A validation envelope swapped into a claim verifier MUST fail.

        Without this guard, an attacker with sqlite write access could
        replace signature_bundle with a validation envelope signed by
        the same key and have third-party 'verify this signed claim'
        code return True.
        """
        key = _signing.generate_keypair()
        validation_env = _signing.sign_validation(
            {"claim_id": "c", "validator_keyid": "x", "validated_at": "t"},
            key,
        )
        with pytest.raises(_signing.InvalidEnvelopeError, match="payloadType"):
            _signing.verify_envelope(validation_env, key.public_key())

    def test_default_rejects_enrollment_envelope(self) -> None:
        key = _signing.generate_keypair()
        enrollment_env = _signing.sign_validator_enrollment(
            {"keyid": "k", "pubkey_pem": "b", "identity": "i",
             "enrolled_at": "t", "enrolled_by_keyid": "k"},
            key,
        )
        with pytest.raises(_signing.InvalidEnvelopeError, match="payloadType"):
            _signing.verify_envelope(enrollment_env, key.public_key())

    def test_explicit_type_works_for_other_kinds(self) -> None:
        """Callers verifying enrollment/validation pass the explicit type."""
        key = _signing.generate_keypair()
        validation_env = _signing.sign_validation(
            {"claim_id": "c", "validator_keyid": "x", "validated_at": "t"},
            key,
        )
        assert _signing.verify_envelope(
            validation_env, key.public_key(),
            expected_payload_type=_signing.PAYLOAD_TYPE_VALIDATION,
        ) is True


# ---------------------------------------------------------------------------
# Timestamp parity: the validation envelope's validated_at MUST equal the row's
# ---------------------------------------------------------------------------

class TestValidationTimestampParity:
    def test_envelope_validated_at_matches_row(self, tmp_path: Path) -> None:
        """The signed envelope and the row's validated_at must be the
        SAME ISO string. Computing _now() twice would diverge by
        microseconds and defeat the tamper-evidence claim."""
        root_key_path = _bootstrap_key(tmp_path, "root.key")
        validator_key_path = _bootstrap_key(tmp_path, "validator.key")
        validator_pubkey_pem = _signing.public_key_to_pem(
            _signing.load_private_key(validator_key_path).public_key(),
        )

        with mareforma.open(tmp_path, key_path=root_key_path) as graph:
            upstream = graph.assert_claim("u", generated_by="seed", seed=True)
            id_a = graph.assert_claim("f", supports=[upstream], generated_by="A")
            graph.assert_claim("f", supports=[upstream], generated_by="B")
            graph.enroll_validator(validator_pubkey_pem, identity="v@lab")

        with mareforma.open(tmp_path, key_path=validator_key_path) as graph:
            graph.validate(id_a)
            claim = graph.get_claim(id_a)

        envelope = json.loads(claim["validation_signature"])
        payload = _signing.envelope_payload(envelope)
        assert payload["validated_at"] == claim["validated_at"], (
            f"envelope validated_at {payload['validated_at']!r} differs "
            f"from row validated_at {claim['validated_at']!r} — "
            "tamper-evidence broken"
        )


# ---------------------------------------------------------------------------
# Chain integrity: is_enrolled walks the chain back to a self-signed root
# ---------------------------------------------------------------------------

class TestChainIntegrity:
    def test_tampered_row_with_fabricated_parent_fails_is_enrolled(
        self, tmp_path: Path,
    ) -> None:
        """A row inserted directly via sqlite with a fabricated parent
        and a forged envelope must NOT pass is_enrolled."""
        import sqlite3

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            root_keyid = _signing.public_key_id(graph._signer.public_key())

        # Forge a row: a brand new key, claims to be enrolled by root,
        # but the envelope is signed by ITSELF (not root). Direct sqlite
        # INSERT bypasses enroll_validator's parent-signature check.
        attacker_key = _signing.generate_keypair()
        attacker_keyid = _signing.public_key_id(attacker_key.public_key())
        attacker_pem = _signing.public_key_to_pem(attacker_key.public_key())
        pem_b64 = base64.standard_b64encode(attacker_pem).decode("ascii")
        now = "2026-05-12T00:00:00+00:00"
        forged_envelope = _signing.sign_validator_enrollment(
            {
                "keyid": attacker_keyid,
                "pubkey_pem": pem_b64,
                "identity": "attacker",
                "enrolled_at": now,
                # Lies: claims root enrolled it, but envelope is self-signed.
                "enrolled_by_keyid": root_keyid,
            },
            attacker_key,  # signed by SELF, not by root
        )

        raw = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        raw.execute(
            "INSERT INTO validators "
            "(keyid, pubkey_pem, identity, enrolled_at, "
            " enrolled_by_keyid, enrollment_envelope) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                attacker_keyid, pem_b64, "attacker", now, root_keyid,
                json.dumps(forged_envelope, sort_keys=True, separators=(",", ":")),
            ),
        )
        raw.commit()
        raw.close()

        # is_enrolled must walk the chain and notice the envelope was
        # signed by attacker, not by root.
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            assert _validators.is_enrolled(graph._conn, attacker_keyid) is False
            # And the root still verifies.
            assert _validators.is_enrolled(graph._conn, root_keyid) is True


# ---------------------------------------------------------------------------
# Identity sanitization
# ---------------------------------------------------------------------------

class TestIdentitySanitization:
    def test_empty_identity_rejected(self, tmp_path: Path) -> None:
        from mareforma.validators import InvalidIdentityError
        key_path = _bootstrap_key(tmp_path)
        new_pem = _signing.public_key_to_pem(_signing.generate_keypair().public_key())
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            with pytest.raises(InvalidIdentityError, match="non-empty"):
                _validators.enroll_validator(
                    graph._conn, graph._signer, new_pem, identity="",
                )

    def test_oversized_identity_rejected(self, tmp_path: Path) -> None:
        from mareforma.validators import InvalidIdentityError
        key_path = _bootstrap_key(tmp_path)
        new_pem = _signing.public_key_to_pem(_signing.generate_keypair().public_key())
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            with pytest.raises(InvalidIdentityError, match="character cap"):
                _validators.enroll_validator(
                    graph._conn, graph._signer, new_pem, identity="x" * 1024,
                )

    def test_control_chars_rejected(self, tmp_path: Path) -> None:
        from mareforma.validators import InvalidIdentityError
        key_path = _bootstrap_key(tmp_path)
        new_pem = _signing.public_key_to_pem(_signing.generate_keypair().public_key())
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            # ANSI escape — would spoof the (root) marker in list output.
            with pytest.raises(InvalidIdentityError, match="control character"):
                _validators.enroll_validator(
                    graph._conn, graph._signer, new_pem,
                    identity="alice\x1b[31m(root)",
                )

    def test_nul_byte_rejected(self, tmp_path: Path) -> None:
        from mareforma.validators import InvalidIdentityError
        key_path = _bootstrap_key(tmp_path)
        new_pem = _signing.public_key_to_pem(_signing.generate_keypair().public_key())
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            with pytest.raises(InvalidIdentityError, match="control character"):
                _validators.enroll_validator(
                    graph._conn, graph._signer, new_pem,
                    identity="alice\x00bob",
                )

    def test_legitimate_identity_accepted(self, tmp_path: Path) -> None:
        key_path = _bootstrap_key(tmp_path)
        new_pem = _signing.public_key_to_pem(_signing.generate_keypair().public_key())
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            row = _validators.enroll_validator(
                graph._conn, graph._signer, new_pem,
                identity="alice@lab.example",
            )
        assert row["identity"] == "alice@lab.example"


# ---------------------------------------------------------------------------
# Public EpistemicGraph API for enrollment (no _conn / _signer leak in docs)
# ---------------------------------------------------------------------------

class TestPublicEnrollmentAPI:
    def test_graph_enroll_validator_public_method(self, tmp_path: Path) -> None:
        key_path = _bootstrap_key(tmp_path)
        new_pem = _signing.public_key_to_pem(_signing.generate_keypair().public_key())
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            row = graph.enroll_validator(new_pem, identity="alice")
            assert row["identity"] == "alice"
            rows = graph.list_validators()
        assert len(rows) == 2  # root + alice

    def test_graph_enroll_without_signer_raises(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path, key_path=tmp_path / "absent") as graph:
            new_pem = _signing.public_key_to_pem(
                _signing.generate_keypair().public_key(),
            )
            with pytest.raises(ValueError, match="loaded signing key"):
                graph.enroll_validator(new_pem, identity="alice")


# ---------------------------------------------------------------------------
# CLI: oversized --pubkey file is rejected before PEM parsing
# ---------------------------------------------------------------------------

class TestPemFileSizeCap:
    def test_oversized_pem_file_rejected(
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

        # 128 KB of garbage past the 64 KB cap.
        big_path = tmp_path / "huge.pem"
        big_path.write_bytes(b"X" * (128 * 1024))

        runner = CliRunner()
        result = runner.invoke(
            mareforma_cli,
            ["validator", "add", "--pubkey", str(big_path), "--identity", "x"],
        )
        assert result.exit_code == 1
        assert "exceeds" in result.output


# ---------------------------------------------------------------------------
# Singleton-root invariant: an attacker-planted alternate root breaks trust
# ---------------------------------------------------------------------------

class TestSingletonRoot:
    def test_alternate_self_signed_root_rejects_all_keyids(
        self, tmp_path: Path,
    ) -> None:
        """An attacker with sqlite write access plants a fresh self-signed
        row with their own key. The chain walk must refuse to trust ANY
        keyid in the table (including the legitimate root) because two
        self-signed rows violate the singleton-root invariant."""
        import sqlite3

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            root_keyid = _signing.public_key_id(graph._signer.public_key())
            assert _validators.is_enrolled(graph._conn, root_keyid)

        # Forge a fresh self-signed root with attacker's own key.
        attacker_key = _signing.generate_keypair()
        attacker_keyid = _signing.public_key_id(attacker_key.public_key())
        attacker_pem = _signing.public_key_to_pem(attacker_key.public_key())
        pem_b64 = base64.standard_b64encode(attacker_pem).decode("ascii")
        now = "2026-05-12T00:00:00+00:00"
        envelope = _signing.sign_validator_enrollment(
            {
                "keyid": attacker_keyid,
                "pubkey_pem": pem_b64,
                "identity": "attacker",
                "enrolled_at": now,
                "enrolled_by_keyid": attacker_keyid,  # self-signed
            },
            attacker_key,
        )

        raw = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        raw.execute(
            "INSERT INTO validators "
            "(keyid, pubkey_pem, identity, enrolled_at, "
            " enrolled_by_keyid, enrollment_envelope) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                attacker_keyid, pem_b64, "attacker", now, attacker_keyid,
                json.dumps(envelope, sort_keys=True, separators=(",", ":")),
            ),
        )
        raw.commit()
        raw.close()

        # Open a fresh connection so the cache doesn't mask the issue.
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            # Both keyids must be refused — neither attacker nor legit root
            # passes because the singleton-root invariant is broken.
            assert _validators.is_enrolled(graph._conn, attacker_keyid) is False
            assert _validators.is_enrolled(graph._conn, root_keyid) is False


# ---------------------------------------------------------------------------
# Chain-walk depth cap
# ---------------------------------------------------------------------------

class TestChainDepthCap:
    def test_pathological_chain_depth_rejected(self, tmp_path: Path) -> None:
        """A maliciously long chain (no cycle) must be rejected past the
        depth cap rather than walked to completion."""
        import sqlite3
        from mareforma.validators import _MAX_CHAIN_DEPTH

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path):
            pass

        # Plant a long chain of rows, each pointing at the next, terminating
        # at a self-signed fake root. The chain walk should hit the depth
        # cap before reaching the (would-be) terminator.
        keys = [_signing.generate_keypair() for _ in range(_MAX_CHAIN_DEPTH + 5)]
        keyids = [_signing.public_key_id(k.public_key()) for k in keys]
        now = "2026-05-12T00:00:00+00:00"

        raw = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        # Forge each row's envelope as signed by its declared parent.
        for i, key in enumerate(keys):
            keyid = keyids[i]
            parent_idx = i + 1 if i + 1 < len(keys) else i  # last is self-signed
            parent_key = keys[parent_idx]
            parent_keyid = keyids[parent_idx]
            pem = _signing.public_key_to_pem(key.public_key())
            pem_b64 = base64.standard_b64encode(pem).decode("ascii")
            envelope = _signing.sign_validator_enrollment(
                {
                    "keyid": keyid,
                    "pubkey_pem": pem_b64,
                    "identity": f"fake-{i}",
                    "enrolled_at": now,
                    "enrolled_by_keyid": parent_keyid,
                },
                parent_key,
            )
            raw.execute(
                "INSERT INTO validators "
                "(keyid, pubkey_pem, identity, enrolled_at, "
                " enrolled_by_keyid, enrollment_envelope) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    keyid, pem_b64, f"fake-{i}", now, parent_keyid,
                    json.dumps(envelope, sort_keys=True, separators=(",", ":")),
                ),
            )
        raw.commit()
        raw.close()

        # Now there are TWO self-signed rows (the legitimate root plus the
        # fake terminator at the end of the chain). Singleton-root alone
        # already rejects everything; this also exercises the depth cap.
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            for kid in keyids:
                assert _validators.is_enrolled(graph._conn, kid) is False


# ---------------------------------------------------------------------------
# CLI claim validate: now signs the validation envelope
# ---------------------------------------------------------------------------

class TestCLIValidateProducesSignedEnvelope:
    def test_cli_validate_persists_signed_envelope(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """The CLI `mareforma claim validate` previously bypassed the
        identity check and signature. It now routes through
        graph.validate(), which signs the validation event."""
        monkeypatch.chdir(tmp_path)

        # Root key signs the REPLICATED claim. Validator key (which lands
        # in XDG so the CLI picks it up) is enrolled separately and is
        # the one allowed to promote — same-key validation is refused by
        # the substrate as self-promotion.
        root_key_path = _bootstrap_key(tmp_path, "root.key")
        validator_key_path = _bootstrap_key(tmp_path, "validator.key")
        validator_pubkey_pem = _signing.public_key_to_pem(
            _signing.load_private_key(validator_key_path).public_key(),
        )

        # Build the REPLICATED claim using the root key, then enroll the
        # validator key under root.
        with mareforma.open(tmp_path, key_path=root_key_path) as graph:
            upstream = graph.assert_claim("u", generated_by="seed", seed=True)
            rep_id = graph.assert_claim("f", supports=[upstream], generated_by="A")
            graph.assert_claim("f", supports=[upstream], generated_by="B")
            graph.enroll_validator(validator_pubkey_pem, identity="cli-validator")

        # Stage the validator key as the XDG default so the CLI finds it.
        xdg_key = _signing.default_key_path()
        xdg_key.parent.mkdir(parents=True, exist_ok=True)
        xdg_key.write_bytes(validator_key_path.read_bytes())
        import os
        os.chmod(xdg_key, 0o600)

        # Validate via CLI.
        runner = CliRunner()
        result = runner.invoke(
            mareforma_cli, ["claim", "validate", rep_id, "--validated-by", "ops"],
        )
        assert result.exit_code == 0, result.output

        # The row now carries a signed envelope.
        with mareforma.open(tmp_path, key_path=root_key_path) as graph:
            claim = graph.get_claim(rep_id)
        assert claim["validation_signature"] is not None

        envelope = json.loads(claim["validation_signature"])
        verifier_key = _signing.load_private_key(xdg_key).public_key()
        assert _signing.verify_envelope(
            envelope, verifier_key,
            expected_payload_type=_signing.PAYLOAD_TYPE_VALIDATION,
        ) is True


# ---------------------------------------------------------------------------
# Identity sanitizer: Unicode bidi / zero-width display-spoofing
# ---------------------------------------------------------------------------

class TestIdentityUnicodeSpoofing:
    def test_rtl_override_rejected(self, tmp_path: Path) -> None:
        """RTL override (U+202E) can visually disguise (root) on a
        different row in `mareforma validator list` output."""
        from mareforma.validators import InvalidIdentityError
        key_path = _bootstrap_key(tmp_path)
        new_pem = _signing.public_key_to_pem(_signing.generate_keypair().public_key())
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            with pytest.raises(InvalidIdentityError, match="display-spoofing"):
                _validators.enroll_validator(
                    graph._conn, graph._signer, new_pem,
                    identity="alice\u202E(root)",
                )

    def test_zero_width_space_rejected(self, tmp_path: Path) -> None:
        from mareforma.validators import InvalidIdentityError
        key_path = _bootstrap_key(tmp_path)
        new_pem = _signing.public_key_to_pem(_signing.generate_keypair().public_key())
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            with pytest.raises(InvalidIdentityError, match="display-spoofing"):
                _validators.enroll_validator(
                    graph._conn, graph._signer, new_pem,
                    identity="alice​@lab.example",
                )

    def test_bom_zwnbsp_rejected(self, tmp_path: Path) -> None:
        from mareforma.validators import InvalidIdentityError
        key_path = _bootstrap_key(tmp_path)
        new_pem = _signing.public_key_to_pem(_signing.generate_keypair().public_key())
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            with pytest.raises(InvalidIdentityError, match="display-spoofing"):
                _validators.enroll_validator(
                    graph._conn, graph._signer, new_pem,
                    identity="﻿alice",
                )


# ---------------------------------------------------------------------------
# Loser of the bootstrap race gets a warning
# ---------------------------------------------------------------------------

class TestRaceLoserWarns:
    def test_second_key_opening_existing_project_warns(
        self, tmp_path: Path,
    ) -> None:
        """Open with key A (auto-enrolls as root). Open with key B → B is
        not enrolled. EpistemicGraph.__init__ must warn so the operator
        notices before any validate() call."""
        import warnings as _warnings

        key_a_path = _bootstrap_key(tmp_path, "a.key")
        with _warnings.catch_warnings():
            _warnings.simplefilter("ignore")
            with mareforma.open(tmp_path, key_path=key_a_path):
                pass

        key_b_path = _bootstrap_key(tmp_path, "b.key")
        with pytest.warns(UserWarning, match="not an enrolled validator"):
            with mareforma.open(tmp_path, key_path=key_b_path):
                pass


# ---------------------------------------------------------------------------
# verify_enrollment now binds ALL payload fields, not just keyid
# ---------------------------------------------------------------------------

class TestVerifyEnrollmentFullBinding:
    def test_tampered_identity_in_row_breaks_verify(self, tmp_path: Path) -> None:
        """Swapping identity in the row (envelope's signed identity still
        says 'root') must break verify_enrollment."""
        import sqlite3

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            root_keyid = _signing.public_key_id(graph._signer.public_key())
            row_before = _validators.get_validator(graph._conn, root_keyid)
            pubkey_pem = base64.standard_b64decode(row_before["pubkey_pem"])
            # Before tamper: verifies.
            assert _validators.verify_enrollment(row_before, pubkey_pem) is True

        # Tamper: change identity in the row but not in the envelope.
        raw = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        raw.execute(
            "UPDATE validators SET identity = ? WHERE keyid = ?",
            ("attacker-renamed-the-root", root_keyid),
        )
        raw.commit()
        raw.close()

        with mareforma.open(tmp_path, key_path=key_path) as graph:
            row_after = _validators.get_validator(graph._conn, root_keyid)
            pubkey_pem = base64.standard_b64decode(row_after["pubkey_pem"])
            # After tamper: the row's identity diverges from the envelope's
            # signed identity → verify must fail.
            assert _validators.verify_enrollment(row_after, pubkey_pem) is False


# ---------------------------------------------------------------------------
# _conn_cache invalidation on validator writes
# ---------------------------------------------------------------------------

class TestConnCacheInvalidation:
    """The per-connection chain-verification cache is dropped whenever a
    validator-mutation path runs through our Python API. Without this,
    on Connection wrappers that DO accept arbitrary attributes (apsw,
    certain SQLAlchemy adapters, future subclasses), an
    ``is_enrolled(K) → True`` call caches the keyid; a subsequent
    mutation would leave the cache pointing at a True that no longer
    reflects ground truth until the connection is reopened.

    Stdlib ``sqlite3.Connection`` refuses ``setattr``, so on stdlib
    ``_conn_cache`` falls into its per-call fresh-set safe branch and
    the cache never persists. To exercise the invalidation logic
    independently of stdlib behavior, the tests below use a tiny
    ``_AttrConn`` wrapper that allows attribute writes — same surface
    the cache code actually targets.

    Raw-SQL mutations from outside our Python paths are explicitly out
    of scope for this gate — see ``invalidate_conn_cache`` docstring.
    """

    class _AttrConn:
        """Minimal sqlite3.Connection-like shim that accepts arbitrary
        attribute writes (which stdlib sqlite3.Connection refuses).
        Lets us exercise the cache + invalidation logic in isolation
        from whether the running stdlib is one of the wrappers that
        happens to accept attrs."""

        def __init__(self, real_conn):
            self._real = real_conn

        def __getattr__(self, name):
            return getattr(self._real, name)

    def test_cache_persists_when_attrs_allowed(
        self, tmp_path: Path,
    ) -> None:
        """On a Connection wrapper that accepts attrs, _conn_cache
        actually persists across calls — establishing the baseline the
        invalidation has to clear."""
        conn = open_db(tmp_path)
        try:
            attr_conn = self._AttrConn(conn)
            cache_a = _validators._conn_cache(attr_conn)
            cache_a.add("test-keyid-deadbeef")
            cache_b = _validators._conn_cache(attr_conn)
            # Same set object returned across calls — cache really does
            # persist on this wrapper.
            assert cache_b is cache_a
            assert "test-keyid-deadbeef" in cache_b
        finally:
            conn.close()

    def test_invalidate_clears_persistent_cache(
        self, tmp_path: Path,
    ) -> None:
        """invalidate_conn_cache must drop the persisted cache so the
        next _conn_cache call returns a fresh empty set."""
        conn = open_db(tmp_path)
        try:
            attr_conn = self._AttrConn(conn)
            cache = _validators._conn_cache(attr_conn)
            cache.add("test-keyid-cafebabe")
            assert "test-keyid-cafebabe" in _validators._conn_cache(attr_conn)

            _validators.invalidate_conn_cache(attr_conn)

            after = _validators._conn_cache(attr_conn)
            assert "test-keyid-cafebabe" not in after
            assert after == set()
        finally:
            conn.close()

    def test_invalidate_on_unattributed_conn_is_noop(
        self, tmp_path: Path,
    ) -> None:
        """On a connection that never built a cache (e.g. stdlib
        sqlite3.Connection where setattr is refused), invalidation
        must not raise. Idempotent."""
        conn = open_db(tmp_path)
        try:
            _validators.invalidate_conn_cache(conn)  # never cached → no-op
            _validators.invalidate_conn_cache(conn)  # twice still no-op
        finally:
            conn.close()

    def test_enroll_validator_calls_invalidate(self, tmp_path: Path) -> None:
        """The enroll_validator path must invoke invalidate_conn_cache
        on exit. We monkey-patch the helper to count calls — this is
        the load-bearing assertion: any future refactor that drops the
        invalidation call regresses this test."""
        from mareforma import validators as _v
        calls: list[None] = []
        original = _v.invalidate_conn_cache

        def counting(conn) -> None:
            calls.append(None)
            original(conn)

        _v.invalidate_conn_cache = counting
        try:
            key_path = _bootstrap_key(tmp_path, "root.key")
            child_key = _bootstrap_key(tmp_path, "child.key")
            with mareforma.open(tmp_path, key_path=key_path) as g:
                child_pem = _signing.public_key_to_pem(
                    _signing.load_private_key(child_key).public_key(),
                )
                calls_before = len(calls)
                g.enroll_validator(child_pem, identity="child")
                calls_after = len(calls)
            # At least one call landed during enroll_validator. (The
            # auto_enroll_root + restore paths also call invalidate;
            # this test is about the enroll_validator path specifically.)
            assert calls_after > calls_before
        finally:
            _v.invalidate_conn_cache = original
