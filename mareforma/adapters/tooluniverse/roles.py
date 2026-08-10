"""Role attestations: per-actor signed sub-attestations for a tool call.

Each tool-call envelope carries a primary DSSE signature from the
calling identity. A role attestation is a SECONDARY `(role, payload,
signature)` triple signed by the role's own key. This module signs and
verifies those triples; where a caller carries them is the caller's
choice, and mareforma's own read path reports the roles recorded in the
envelope's ``signatures[]`` entries instead.

The signed bytes for a role attestation are::

    DSSE-PAE("application/x-mareforma-role+json", sorted-key-json(payload))

so the signing covers the role+payload pair, not raw payload bytes,
preventing role-confusion across attestation types. The PAE wrap is
:func:`mareforma.signing.dsse_pae`, the same one the claim envelopes
use. The payload bytes are a local sorted-key JSON form, not the JCS
canonicaliser behind :func:`mareforma.signing.canonical_statement`.

This module defines four roles, mirroring the provenance-aware
execution package's ``claim-with-roles/v1`` predicate variant:

- ``tool``       : the tool itself attests its own identity (name,
                   version, config_fingerprint) at call time.
- ``executor``   : the adapter signs the call envelope (args_digest,
                   result_digest, completed_at, cache_hit).
- ``summarizer`` : when a hook reduces output for the LLM context,
                   the hook signs (original_digest, summary_digest).
- ``reviewer``   : a critic agent's verdict signature; multiple
                   reviewer attestations per claim are allowed.
"""

from __future__ import annotations

import base64
import json
from typing import Any

from mareforma import signing as mf_signing


__all__ = [
    "ROLE_TOOL",
    "ROLE_EXECUTOR",
    "ROLE_SUMMARIZER",
    "ROLE_REVIEWER",
    "ROLE_ATTESTATION_PAYLOAD_TYPE",
    "InvalidRoleAttestationError",
    "sign_role_attestation",
    "verify_role_attestation",
]


ROLE_TOOL = "tool"
ROLE_EXECUTOR = "executor"
ROLE_SUMMARIZER = "summarizer"
ROLE_REVIEWER = "reviewer"

ROLE_ATTESTATION_PAYLOAD_TYPE = "application/x-mareforma-role+json"

_ALLOWED_ROLES = frozenset({
    ROLE_TOOL, ROLE_EXECUTOR, ROLE_SUMMARIZER, ROLE_REVIEWER,
})


class InvalidRoleAttestationError(ValueError):
    """Raised when role-attestation signature or shape is invalid."""


def _canonical_payload_bytes(role: str, payload: dict[str, Any]) -> bytes:
    """Canonicalise the role-attestation payload to deterministic bytes.

    A local JSON-with-sorted-keys form, deliberately not the RFC 8785
    JCS canonicaliser behind ``signing.canonical_statement``: the two
    emit different bytes for the same input, and changing these bytes
    would invalidate every attestation already signed. NaN/Inf
    rejected: we want the same finiteness guarantees as the rest of
    the package.
    """

    body = {"role": role, "payload": payload}
    return json.dumps(
        body, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")


def sign_role_attestation(
    *, role: str, payload: dict[str, Any], signer: Any
) -> dict[str, Any]:
    """Sign a role attestation with `signer` (Ed25519PrivateKey).

    Returns ``{"role", "payload_b64", "signature_b64", "keyid"}``.
    Verifier must reverse via :func:`verify_role_attestation`.
    """

    if role not in _ALLOWED_ROLES:
        raise ValueError(
            "unknown role %r (allowed: %s)" % (
                role, ", ".join(sorted(_ALLOWED_ROLES)),
            )
        )
    payload_bytes = _canonical_payload_bytes(role, payload)
    pae = mf_signing.dsse_pae(ROLE_ATTESTATION_PAYLOAD_TYPE, payload_bytes)
    sig = signer.sign(pae)
    return {
        "role": role,
        "payload_b64": base64.b64encode(payload_bytes).decode("ascii"),
        "signature_b64": base64.b64encode(sig).decode("ascii"),
        "keyid": mf_signing.public_key_id(signer.public_key()),
    }


def verify_role_attestation(attestation: dict[str, Any], public_key: Any) -> dict[str, Any]:
    """Verify a role attestation against ``public_key``; return payload on success.

    Raises :class:`InvalidRoleAttestationError` on any failure: bad
    shape, bad base64, bad signature, role not in the allowed enum,
    keyid naming a key other than the verifying one, payload not a
    JSON object.
    """

    for key in ("role", "payload_b64", "signature_b64", "keyid"):
        if key not in attestation:
            raise InvalidRoleAttestationError(
                f"role attestation missing field {key!r}"
            )
    role = attestation["role"]
    if role not in _ALLOWED_ROLES:
        raise InvalidRoleAttestationError(f"unknown role {role!r}")
    try:
        payload_bytes = base64.b64decode(attestation["payload_b64"], validate=True)
        sig = base64.b64decode(attestation["signature_b64"], validate=True)
    except Exception as exc:
        raise InvalidRoleAttestationError(
            f"role attestation has invalid base64: {exc}"
        ) from exc
    pae = mf_signing.dsse_pae(ROLE_ATTESTATION_PAYLOAD_TYPE, payload_bytes)
    try:
        public_key.verify(sig, pae)
    except Exception as exc:
        raise InvalidRoleAttestationError(
            "role attestation signature did not verify"
        ) from exc
    # keyid is outside the signed bytes, so a genuine attestation can
    # reach a verifier advertising someone else's key. Bind it to the
    # key that actually verified rather than let it be recorded as-is.
    if attestation["keyid"] != mf_signing.public_key_id(public_key):
        raise InvalidRoleAttestationError(
            "role attestation keyid does not match the verifying key"
        )
    try:
        body = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise InvalidRoleAttestationError(
            f"role attestation payload is not a JSON object: {exc}"
        ) from exc
    if not isinstance(body, dict) or "payload" not in body:
        raise InvalidRoleAttestationError(
            "role attestation payload must decode to {role, payload} object"
        )
    if body.get("role") != role:
        raise InvalidRoleAttestationError(
            "role attestation payload role mismatch"
        )
    return body["payload"]
