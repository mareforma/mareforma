"""Tests for role-attestation signing and verification."""
from __future__ import annotations

import base64

import pytest

from mareforma import signing as _signing
from mareforma.adapters.tooluniverse.roles import (
    ROLE_ATTESTATION_PAYLOAD_TYPE,
    ROLE_TOOL,
    InvalidRoleAttestationError,
    _dsse_pae,
    sign_role_attestation,
    verify_role_attestation,
)


class TestVerifyRoleAttestation:
    def test_round_trip(self) -> None:
        signer = _signing.generate_keypair()
        att = sign_role_attestation(
            role=ROLE_TOOL, payload={"name": "demo"}, signer=signer,
        )
        assert verify_role_attestation(att, signer.public_key()) == {"name": "demo"}

    def test_rewritten_keyid_raises_typed_error(self) -> None:
        # keyid is outside the signed bytes, so any intermediary can
        # rewrite it on an otherwise genuine attestation. A verifier
        # that accepts it hands the caller the wrong attester.
        signer = _signing.generate_keypair()
        other = _signing.generate_keypair()
        att = sign_role_attestation(
            role=ROLE_TOOL, payload={"name": "demo"}, signer=signer,
        )
        att["keyid"] = _signing.public_key_id(other.public_key())
        with pytest.raises(InvalidRoleAttestationError, match="keyid"):
            verify_role_attestation(att, signer.public_key())

    def test_non_json_payload_raises_typed_error(self) -> None:
        # A valid signature covering non-JSON bytes (a signer that bypasses
        # sign_role_attestation and signs raw bytes) must surface the
        # documented typed failure, not a raw json.JSONDecodeError.
        signer = _signing.generate_keypair()
        payload_bytes = b"not json at all"
        pae = _dsse_pae(ROLE_ATTESTATION_PAYLOAD_TYPE, payload_bytes)
        att = {
            "role": ROLE_TOOL,
            "payload_b64": base64.b64encode(payload_bytes).decode("ascii"),
            "signature_b64": base64.b64encode(signer.sign(pae)).decode("ascii"),
            "keyid": _signing.public_key_id(signer.public_key()),
        }
        with pytest.raises(InvalidRoleAttestationError):
            verify_role_attestation(att, signer.public_key())

    def test_non_utf8_payload_raises_typed_error(self) -> None:
        # A valid signature over bytes that are not UTF-8 must also surface
        # the typed error rather than a raw UnicodeDecodeError.
        signer = _signing.generate_keypair()
        payload_bytes = b"\xff\xfe\x00"
        pae = _dsse_pae(ROLE_ATTESTATION_PAYLOAD_TYPE, payload_bytes)
        att = {
            "role": ROLE_TOOL,
            "payload_b64": base64.b64encode(payload_bytes).decode("ascii"),
            "signature_b64": base64.b64encode(signer.sign(pae)).decode("ascii"),
            "keyid": _signing.public_key_id(signer.public_key()),
        }
        with pytest.raises(InvalidRoleAttestationError):
            verify_role_attestation(att, signer.public_key())
