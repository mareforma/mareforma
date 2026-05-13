"""
tests/test_signing.py — Ed25519 keypair lifecycle + DSSE-style envelope.

Covers:
  - default_key_path honours XDG_CONFIG_HOME
  - keypair gen + save + load roundtrip preserves identity
  - private key on disk is mode 0600; loose perms raise
  - missing key raises KeyNotFoundError
  - sign_claim / verify_envelope happy path
  - tampering with payload bytes invalidates the signature
  - signature with a foreign public key fails verification
  - envelope round-trips through JSON (db persistence shape)
  - bootstrap_key refuses to overwrite an existing key
  - bootstrap_key returns the keyid we can later compare to envelope keyid
"""

from __future__ import annotations

import base64
import json
import os
import stat
from pathlib import Path

import pytest

from mareforma.signing import (
    InvalidEnvelopeError,
    KeyNotFoundError,
    KeyPermissionError,
    SigningError,
    bootstrap_key,
    default_key_path,
    envelope_payload,
    generate_keypair,
    load_private_key,
    public_key_from_pem,
    public_key_id,
    public_key_to_pem,
    save_private_key,
    sign_claim,
    verify_envelope,
)


def _claim_fields(**overrides):
    base = {
        "claim_id": "11111111-1111-1111-1111-111111111111",
        "text": "the gradient explodes at step 1024",
        "classification": "ANALYTICAL",
        "generated_by": "agent/test",
        "supports": ["upstream-id-1"],
        "contradicts": [],
        "source_name": "experiment-2026-05",
        "created_at": "2026-05-12T10:00:00+00:00",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# default_key_path
# ---------------------------------------------------------------------------

class TestDefaultKeyPath:
    def test_uses_xdg_config_home_when_set(self, tmp_path, monkeypatch):
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
        assert default_key_path() == tmp_path / "xdg" / "mareforma" / "key"

    def test_falls_back_to_dot_config(self, tmp_path, monkeypatch):
        monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
        monkeypatch.setenv("HOME", str(tmp_path))
        # Path.home() reads HOME on POSIX.
        assert default_key_path() == tmp_path / ".config" / "mareforma" / "key"


# ---------------------------------------------------------------------------
# Key lifecycle
# ---------------------------------------------------------------------------

class TestKeyLifecycle:
    def test_generate_save_load_roundtrip_preserves_keyid(self, tmp_path):
        key = generate_keypair()
        keyid_before = public_key_id(key.public_key())

        key_path = tmp_path / "key"
        save_private_key(key, key_path)
        loaded = load_private_key(key_path)
        keyid_after = public_key_id(loaded.public_key())

        assert keyid_before == keyid_after

    def test_save_creates_file_with_mode_0600(self, tmp_path):
        key = generate_keypair()
        key_path = tmp_path / "secrets" / "key"
        save_private_key(key, key_path)
        assert key_path.exists()
        mode = stat.S_IMODE(key_path.stat().st_mode)
        assert mode == 0o600, f"expected 0o600, got {oct(mode)}"

    def test_load_rejects_world_readable_key(self, tmp_path):
        key = generate_keypair()
        key_path = tmp_path / "key"
        save_private_key(key, key_path)
        # Open it up to group/world.
        os.chmod(key_path, 0o644)
        with pytest.raises(KeyPermissionError):
            load_private_key(key_path)

    def test_load_missing_key_raises_KeyNotFoundError(self, tmp_path):
        with pytest.raises(KeyNotFoundError, match="mareforma bootstrap"):
            load_private_key(tmp_path / "absent")

    def test_load_rejects_non_ed25519_key(self, tmp_path):
        # RSA key from the cryptography stdlib — not Ed25519, must fail loudly.
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization

        rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = rsa_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        path = tmp_path / "key"
        path.write_bytes(pem)
        os.chmod(path, 0o600)
        with pytest.raises(SigningError, match="not an Ed25519 key"):
            load_private_key(path)


# ---------------------------------------------------------------------------
# bootstrap_key
# ---------------------------------------------------------------------------

class TestBootstrap:
    def test_bootstrap_writes_a_loadable_key(self, tmp_path):
        path = tmp_path / "key"
        returned_path, keyid = bootstrap_key(path)
        assert returned_path == path
        assert path.exists()
        loaded = load_private_key(path)
        assert public_key_id(loaded.public_key()) == keyid

    def test_bootstrap_refuses_to_overwrite(self, tmp_path):
        path = tmp_path / "key"
        bootstrap_key(path)
        with pytest.raises(SigningError, match="Refuse to overwrite"):
            bootstrap_key(path)

    def test_bootstrap_overwrites_with_flag(self, tmp_path):
        path = tmp_path / "key"
        _, keyid_first = bootstrap_key(path)
        _, keyid_second = bootstrap_key(path, overwrite=True)
        assert keyid_first != keyid_second, "new key must have a different id"


# ---------------------------------------------------------------------------
# Sign / verify
# ---------------------------------------------------------------------------

class TestSignVerify:
    def test_sign_then_verify_passes(self):
        key = generate_keypair()
        envelope = sign_claim(_claim_fields(), key)
        assert verify_envelope(envelope, key.public_key()) is True

    def test_envelope_payload_decodes_back_to_claim_fields(self):
        from mareforma.signing import claim_predicate_from_envelope
        key = generate_keypair()
        fields = _claim_fields()
        envelope = sign_claim(fields, key)
        # envelope_payload now returns the Statement v1 dict; claim
        # fields live one level deeper under predicate.
        stmt = envelope_payload(envelope)
        assert stmt["_type"] == "https://in-toto.io/Statement/v1"
        assert stmt["predicateType"] == "https://mareforma.dev/claim/v1"
        predicate = claim_predicate_from_envelope(envelope)
        # Field-by-field check — extras like updated_at must NOT appear in
        # the signed predicate (only the canonical _SIGNED_FIELDS + evidence).
        assert predicate["claim_id"] == fields["claim_id"]
        assert predicate["text"] == fields["text"]
        assert predicate["classification"] == fields["classification"]
        assert predicate["supports"] == fields["supports"]
        assert predicate["created_at"] == fields["created_at"]
        assert "updated_at" not in predicate
        # evidence is now part of the signed predicate (defaults to empty)
        assert "evidence" in predicate

    def test_keyid_in_envelope_matches_public_key(self):
        key = generate_keypair()
        envelope = sign_claim(_claim_fields(), key)
        assert envelope["signatures"][0]["keyid"] == public_key_id(key.public_key())

    def test_tampered_payload_fails_verification(self):
        key = generate_keypair()
        envelope = sign_claim(_claim_fields(), key)
        # Flip one byte in the payload base64.
        raw = base64.standard_b64decode(envelope["payload"])
        tampered = raw.replace(b"gradient", b"GRADIENT")
        envelope["payload"] = base64.standard_b64encode(tampered).decode("ascii")
        assert verify_envelope(envelope, key.public_key()) is False

    def test_wrong_public_key_fails_verification(self):
        signing_key = generate_keypair()
        envelope = sign_claim(_claim_fields(), signing_key)
        other_key = generate_keypair()
        # Different keyid → verify returns False (not InvalidSignature).
        assert verify_envelope(envelope, other_key.public_key()) is False

    def test_malformed_envelope_raises(self):
        with pytest.raises(InvalidEnvelopeError):
            verify_envelope({"payloadType": "wrong/type"}, generate_keypair().public_key())
        with pytest.raises(InvalidEnvelopeError):
            verify_envelope(
                {
                    "payloadType": "application/vnd.mareforma.claim+json",
                    "payload": "not-base64!!!",
                    "signatures": [{"keyid": "x", "sig": "y"}],
                },
                generate_keypair().public_key(),
            )

    def test_envelope_survives_json_roundtrip(self):
        """The envelope is persisted as JSON in signature_bundle; that
        round-trip must not break verification."""
        key = generate_keypair()
        envelope = sign_claim(_claim_fields(), key)
        encoded = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
        decoded = json.loads(encoded)
        assert verify_envelope(decoded, key.public_key()) is True

    def test_same_claim_signs_deterministically(self):
        """Ed25519 signatures over identical bytes are themselves identical
        (no random nonce). Two signs of the same claim with the same key
        must produce byte-identical envelopes — important for cache and
        idempotency reasoning."""
        key = generate_keypair()
        env_a = sign_claim(_claim_fields(), key)
        env_b = sign_claim(_claim_fields(), key)
        assert env_a == env_b


# ---------------------------------------------------------------------------
# Public-key PEM helpers (used by validators table later)
# ---------------------------------------------------------------------------

class TestPublicKeyPEM:
    def test_pem_roundtrip_preserves_keyid(self):
        key = generate_keypair()
        pem = public_key_to_pem(key.public_key())
        reloaded = public_key_from_pem(pem)
        assert public_key_id(reloaded) == public_key_id(key.public_key())

    def test_pem_rejects_non_ed25519(self):
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
        rsa_pub = rsa.generate_private_key(public_exponent=65537, key_size=2048).public_key()
        pem = rsa_pub.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        with pytest.raises(SigningError, match="not an Ed25519 public key"):
            public_key_from_pem(pem)


# ---------------------------------------------------------------------------
# Private key on-disk storage — parent dir perms, race-safe creation
# ---------------------------------------------------------------------------

class TestPrivateKeyStorage:
    def test_parent_dir_is_0o700_on_posix(self, tmp_path):
        import os
        from mareforma.signing import bootstrap_key
        key_path = tmp_path / "deep" / "nest" / "mareforma" / "key"
        bootstrap_key(key_path)
        if os.name == "posix":
            mode = stat.S_IMODE(key_path.parent.stat().st_mode)
            assert mode == 0o700, f"expected 0o700, got {oct(mode)}"

    def test_bootstrap_concurrent_calls_only_one_wins(self, tmp_path):
        """Two threads racing on the same path: O_CREAT|O_EXCL must let
        exactly one succeed and the others raise SigningError. Closes the
        TOCTOU between exists() and the on-disk write."""
        import threading
        from mareforma.signing import bootstrap_key

        key_path = tmp_path / "racy.key"
        results: list[object] = []

        def runner():
            try:
                bootstrap_key(key_path)
                results.append("ok")
            except SigningError as exc:
                results.append(exc)

        threads = [threading.Thread(target=runner) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        oks = [r for r in results if r == "ok"]
        errs = [r for r in results if isinstance(r, SigningError)]
        assert len(oks) == 1, f"expected exactly one winner, got {oks}"
        assert len(errs) == 3, f"expected three losers, got {errs}"

    def test_save_private_key_exclusive_refuses_to_replace(self, tmp_path):
        key_a = generate_keypair()
        key_b = generate_keypair()
        path = tmp_path / "key"
        save_private_key(key_a, path, exclusive=True)
        with pytest.raises(FileExistsError):
            save_private_key(key_b, path, exclusive=True)
        # First key still on disk, untouched.
        loaded = load_private_key(path)
        assert public_key_id(loaded.public_key()) == public_key_id(key_a.public_key())


# ---------------------------------------------------------------------------
# save_private_key cleanup on mid-write failure
# ---------------------------------------------------------------------------

class TestSavePrivateKeyCleanup:
    def test_write_failure_in_exclusive_mode_unlinks_the_file(self, tmp_path, monkeypatch):
        """If os.write raises mid-write, the O_EXCL'd file is unlinked so
        the next bootstrap can re-attempt without hitting a misleading
        FileExistsError on a zero-byte leftover."""
        import os as _os
        key = generate_keypair()
        path = tmp_path / "key"

        def flaky_write(fd, data):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(_os, "write", flaky_write)
        with pytest.raises(OSError):
            save_private_key(key, path, exclusive=True)

        assert not path.exists(), (
            "save_private_key(exclusive=True) left a zero-byte file behind "
            "after write failure; next bootstrap would hit FileExistsError."
        )

    def test_after_failed_exclusive_save_next_attempt_succeeds(self, tmp_path, monkeypatch):
        """The cleanup must restore the precondition for a retry."""
        import os as _os
        key_a = generate_keypair()
        path = tmp_path / "key"

        monkeypatch.setattr(
            _os, "write",
            lambda fd, data: (_ for _ in ()).throw(OSError(28, "ENOSPC")),
        )
        with pytest.raises(OSError):
            save_private_key(key_a, path, exclusive=True)

        monkeypatch.undo()
        key_b = generate_keypair()
        save_private_key(key_b, path, exclusive=True)
        assert path.exists()
        loaded = load_private_key(path)
        assert public_key_id(loaded.public_key()) == public_key_id(key_b.public_key())


# ---------------------------------------------------------------------------
# envelope_payload dict-only contract
# ---------------------------------------------------------------------------

class TestEnvelopePayloadDictContract:
    """envelope_payload must reject payloads that decode to non-dict JSON.

    Downstream callers (e.g. mark_claim_logged) do ``payload.get(...)``
    which would otherwise AttributeError on a bare string/list/number.
    """

    def test_payload_string_raises_InvalidEnvelopeError(self):
        import base64
        bad_payload_b64 = base64.standard_b64encode(b'"just a string"').decode("ascii")
        envelope = {
            "payloadType": "application/vnd.mareforma.claim+json",
            "payload": bad_payload_b64,
            "signatures": [{"keyid": "x", "sig": "y"}],
        }
        with pytest.raises(InvalidEnvelopeError, match="JSON object"):
            envelope_payload(envelope)

    def test_payload_list_raises_InvalidEnvelopeError(self):
        import base64
        bad_payload_b64 = base64.standard_b64encode(b'[1, 2, 3]').decode("ascii")
        envelope = {
            "payloadType": "application/vnd.mareforma.claim+json",
            "payload": bad_payload_b64,
            "signatures": [{"keyid": "x", "sig": "y"}],
        }
        with pytest.raises(InvalidEnvelopeError, match="JSON object"):
            envelope_payload(envelope)
