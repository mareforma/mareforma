"""Ed25519 claim signing, DSSE envelopes, and Sigstore-Rekor integration.

Two submodules:

- :mod:`mareforma.signing.core` — key management, canonical Statement v1
  build, DSSE v1 envelope sign/verify (claim, validator-enrollment,
  validation, seed-claim payload types), and the ``bootstrap_key``
  entry point used by ``mareforma bootstrap``.
- :mod:`mareforma.signing.rekor` — transparency-log submission, RFC 6962
  Merkle inclusion-proof verification, signed-checkpoint parsing, log
  pubkey fetch, and SSRF defense on the Rekor URL.

Every name documented as ``mareforma.signing.X`` continues to be
importable at this level after the split — the submodule layout is an
internal organisation, not a public API change.
"""

from .core import (
    # Payload-type constants.
    PAYLOAD_TYPE_CLAIM,
    PAYLOAD_TYPE_VALIDATOR_ENROLLMENT,
    PAYLOAD_TYPE_VALIDATION,
    PAYLOAD_TYPE_SEED,
    # Signed-field contracts.
    SIGNED_FIELDS,
    _ENROLLMENT_FIELDS,
    _VALIDATION_FIELDS,
    _SEED_FIELDS,
    VALID_CLAIM_ROLES,
    # Exceptions.
    SigningError,
    KeyNotFoundError,
    KeyPermissionError,
    InvalidEnvelopeError,
    # Key management.
    default_key_path,
    generate_keypair,
    save_private_key,
    load_private_key,
    public_key_id,
    public_key_to_pem,
    public_key_from_pem,
    # Envelope build / verify.
    dsse_pae,
    canonical_statement,
    sign_claim,
    sign_claim_with_roles,
    sign_validator_enrollment,
    sign_validation,
    sign_seed_claim,
    verify_envelope,
    verify_envelope_multi,
    envelope_payload,
    claim_predicate_from_envelope,
    _canonical_record,
    _build_envelope,
    # Bootstrap.
    bootstrap_key,
)
from .rekor import (
    # Constants.
    PUBLIC_REKOR_URL,
    _REKOR_TIMEOUT,
    _REKOR_USER_AGENT,
    _MAX_REKOR_RESPONSE_SIZE,
    _NUMERIC_HOSTNAME_RE,
    _LOOPBACK_DNS_NAMES,
    _RFC6962_LEAF_PREFIX,
    _RFC6962_NODE_PREFIX,
    _UUID_HEX_RE,
    _SIGNED_NOTE_DASH,
    # SSRF defense + helpers.
    validate_rekor_url,
    _b64_decode_tolerant,
    # Exception.
    RekorInclusionError,
    # Submit + attach.
    submit_to_rekor,
    attach_rekor_entry,
    # Inclusion-proof verification.
    verify_merkle_inclusion_proof,
    compute_rekor_leaf_hash,
    parse_rekor_checkpoint,
    verify_rekor_checkpoint,
    verify_rekor_inclusion,
    _verify_with_pubkey,
    # Fetchers.
    fetch_inclusion_proof,
    fetch_log_pubkey,
)


__all__ = [
    # Payload-type constants.
    "PAYLOAD_TYPE_CLAIM",
    "PAYLOAD_TYPE_VALIDATOR_ENROLLMENT",
    "PAYLOAD_TYPE_VALIDATION",
    "PAYLOAD_TYPE_SEED",
    # Signed-field contracts.
    "SIGNED_FIELDS",
    "VALID_CLAIM_ROLES",
    # Exceptions.
    "SigningError",
    "KeyNotFoundError",
    "KeyPermissionError",
    "InvalidEnvelopeError",
    "RekorInclusionError",
    # Key management.
    "default_key_path",
    "generate_keypair",
    "save_private_key",
    "load_private_key",
    "public_key_id",
    "public_key_to_pem",
    "public_key_from_pem",
    # Envelope build / verify.
    "dsse_pae",
    "canonical_statement",
    "sign_claim",
    "sign_claim_with_roles",
    "sign_validator_enrollment",
    "sign_validation",
    "sign_seed_claim",
    "verify_envelope",
    "verify_envelope_multi",
    "envelope_payload",
    "claim_predicate_from_envelope",
    # Bootstrap.
    "bootstrap_key",
    # Rekor constants.
    "PUBLIC_REKOR_URL",
    # Rekor SSRF defense.
    "validate_rekor_url",
    # Rekor submit + attach.
    "submit_to_rekor",
    "attach_rekor_entry",
    # Inclusion-proof verification.
    "verify_merkle_inclusion_proof",
    "compute_rekor_leaf_hash",
    "parse_rekor_checkpoint",
    "verify_rekor_checkpoint",
    "verify_rekor_inclusion",
    # Fetchers.
    "fetch_inclusion_proof",
    "fetch_log_pubkey",
]
