"""
signing.py — Ed25519 claim signing and verification.

Every claim in the epistemic graph is signed locally before INSERT. The
signature is stored alongside the claim as a DSSE-style JSON envelope so
that future Rekor (transparency-log) integration can ingest the same
bundle format without schema changes.

Key lifecycle
-------------
- One Ed25519 keypair per user. Private key at ``~/.config/mareforma/key``
  (XDG-compliant, mode 0600 — POSIX). On Windows, file-mode bits are mostly
  advisory; mareforma issues a warning when loading a key on a non-POSIX
  platform because the on-disk perm guarantees do not hold.
- ``mareforma bootstrap`` generates the key once at install time, atomically
  (``O_CREAT|O_EXCL``) so concurrent invocations cannot race to overwrite
  each other.
- The library never auto-creates a key. Missing key + ``require_signed=False``
  → claims are inserted with ``signature_bundle=NULL`` (unsigned).
- Missing key + ``require_signed=True`` → :class:`KeyNotFoundError`.

Timestamps
----------
The signed payload includes ``created_at`` (microsecond ISO 8601 UTC) so the
signature binds an authorial timestamp. The Rekor entry then contributes an
independent witnessed time (``integratedTime``). Downstream verifiers should
treat ``created_at`` as the agent's claim about when the assertion was made
and ``integratedTime`` as a third party's claim about when the log first
observed it.

Rekor submission is synchronous and blocks ``assert_claim`` for up to
``_REKOR_TIMEOUT`` seconds. For batch workflows where this matters, run
unsigned (no key) or signed-without-Rekor (``rekor_url=None``) and call
``EpistemicGraph.refresh_unsigned()`` later.

Envelope format — DSSE v1 with in-toto Statement v1
----------------------------------------------------
Every claim envelope is a DSSE v1 envelope whose payload is a
canonical in-toto Statement v1::

    {
      "payloadType": "application/vnd.in-toto+json",
      "payload":     "<base64 of canonicalize(Statement v1)>",
      "signatures":  [
        {"keyid": "<hex sha256 of pubkey bytes>", "sig": "<base64 sig>"}
      ],
      "rekor":       {"uuid": ..., "logIndex": ..., "integratedTime": ...}
    }

The Statement v1 payload is::

    {
      "_type":         "https://in-toto.io/Statement/v1",
      "subject":       [{"name": "mareforma:claim:<id>",
                         "digest": {"sha256": "<text_sha256>"}}],
      "predicateType": "urn:mareforma:predicate:claim:v1",
      "predicate":     { <claim fields + GRADE EvidenceVector> }
    }

The signature covers the DSSE Pre-Authentication Encoding (PAE) of
the payload, not the payload bytes alone. PAE is::

    b"DSSEv1 " + len(payloadType) + b" " + payloadType
              + b" " + len(body)  + b" " + body

so a signature on (typeA, payload) cannot be replayed as a signature
on (typeB, payload) even when the bytes are otherwise identical.

The signed predicate carries exactly: ``claim_id``, ``text``,
``classification``, ``generated_by``, ``supports``, ``contradicts``,
``source_name``, ``artifact_hash``, ``created_at`` (the contract in
:data:`SIGNED_FIELDS`) plus ``evidence`` (a GRADE EvidenceVector
serialized via :meth:`mareforma._evidence.EvidenceVector.to_dict`).

The ``rekor`` block is added by :func:`attach_rekor_entry` after a
successful transparency-log submission; it does not affect signature
verification. Including ``created_at`` binds the signature to an
authorial timestamp; the Rekor entry contributes an independent
witnessed time.

Auxiliary envelopes (validator enrollment, validation events, seed
attestations) reuse the DSSE PAE envelope but with mareforma-specific
payload types and flat record payloads — they are not in-toto Statements.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import ipaddress
import json
import os
import re
import warnings
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes as _hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec as _ec
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


PUBLIC_REKOR_URL = "https://rekor.sigstore.dev/api/v1/log/entries"
_REKOR_TIMEOUT = 10.0
_REKOR_USER_AGENT = (
    "mareforma/0.3.0 (+https://github.com/mareforma/mareforma; "
    "mailto:hello@mareforma.com)"
)

# 64 KB: a Rekor entry should be under a few KB in practice. A malicious or
# buggy server returning multi-MB JSON would otherwise land in graph.db and
# then be re-encoded into claims.toml on every backup.
_MAX_REKOR_RESPONSE_SIZE = 64 * 1024


# Claim envelopes carry an in-toto Statement v1 as the signed payload.
# payloadType is the IANA-style media type used by Sigstore / SLSA /
# GUAC, so off-the-shelf in-toto tooling can introspect a mareforma
# claim envelope without a mareforma-specific verifier.
PAYLOAD_TYPE_CLAIM = "application/vnd.in-toto+json"
PAYLOAD_TYPE_VALIDATOR_ENROLLMENT = "application/vnd.mareforma.validator-enrollment+json"
PAYLOAD_TYPE_VALIDATION = "application/vnd.mareforma.validation+json"
PAYLOAD_TYPE_SEED = "application/vnd.mareforma.seed+json"

# Predicate fields bound by a claim signature. After Statement v1 these
# live inside ``statement.predicate``; the tuple is the contract restore
# uses to cross-check signature-vs-row consistency. Public so callers
# (db.update_claim, restore, exporters) all share one source of truth.
SIGNED_FIELDS = (
    "claim_id",
    "text",
    "classification",
    "generated_by",
    "supports",
    "contradicts",
    "source_name",
    "artifact_hash",
    "created_at",
)

# Fields included in the signed payload of a validator enrollment.
# ``validator_type`` is bound here so a verifier can detect post-hoc
# tampering of a row from 'llm' to 'human' (or vice versa) — the value
# is part of what the parent signed off on at enroll time.
_ENROLLMENT_FIELDS = (
    "keyid",
    "pubkey_pem",
    "identity",
    "validator_type",
    "enrolled_at",
    "enrolled_by_keyid",
)

# Fields included in the signed payload of a validation event.
#
# ``evidence_seen`` is a list of claim_ids the validator declares to have
# reviewed before signing the promotion. The field is ALWAYS present in
# the signed payload — an empty list is a positive statement that the
# validator reviewed nothing, which is then visible in the audit trail
# rather than hidden by absence. The validator's enumeration is
# self-declared (the substrate cannot prove the validator actually
# opened the cited claims) but every cited entry must exist in the
# graph and predate the validation timestamp — that part the substrate
# DOES verify at write and at restore.
_VALIDATION_FIELDS = (
    "claim_id",
    "validator_keyid",
    "validated_at",
    "evidence_seen",
)

# Fields included in the signed payload of a seed-claim attestation.
# A seed envelope establishes the bootstrap of trust for a fresh graph:
# the validator who asserts the seed claim signs (claim_id,
# validator_keyid, seeded_at) so a verifier can confirm the seed
# came from an enrolled validator at the time of assertion.
_SEED_FIELDS = (
    "claim_id",
    "validator_keyid",
    "seeded_at",
)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class SigningError(Exception):
    """Base exception for all signing-related errors."""


class KeyNotFoundError(SigningError):
    """Raised when require_signed=True but no key exists at the expected path."""


class KeyPermissionError(SigningError):
    """Raised when the private key file has world- or group-readable perms."""


class InvalidEnvelopeError(SigningError):
    """Raised when a signature envelope is malformed."""


# ---------------------------------------------------------------------------
# Key paths
# ---------------------------------------------------------------------------

_NUMERIC_HOSTNAME_RE = re.compile(r"^[0-9.]+$")


def _b64_decode_tolerant(s: str) -> Optional[bytes]:
    """Decode a base64 string accepting both standard and URL-safe alphabets,
    with or without padding.

    Returns ``None`` on failure. Used for signature equality where a
    third-party server (Rekor) may canonicalize the encoding differently
    than we sent; the raw bytes are the real signature.

    Internals: ``urlsafe_b64decode`` translates ``_``→``/`` and ``-``→``+``
    before delegating to the standard decoder, so it transparently accepts
    inputs in either alphabet. The standard decoder is permissive of
    non-alphabet bytes by default, which is intentional here — garbage
    inputs decode to wrong bytes and the downstream equality check
    rejects them.
    """
    if not isinstance(s, str):
        return None
    padded = s + "=" * (-len(s) % 4)
    try:
        return base64.urlsafe_b64decode(padded)
    except (ValueError, binascii.Error):
        return None
_LOOPBACK_DNS_NAMES = frozenset({
    "localhost",
    "localhost.localdomain",
    "ip6-localhost",
    "ip6-loopback",
})


def validate_rekor_url(url: str, *, allow_insecure: bool = False) -> None:
    """Reject Rekor URLs that look like SSRF probes.

    Enforces ``https://`` and rejects:

    - Loopback / private / link-local / multicast / unspecified (``0.0.0.0``,
      ``::``) IP literals.
    - DNS shortcuts that resolve to loopback at connect time:
      ``localhost`` and friends, plus numeric-only hostnames like
      ``127.1`` and ``2130706433`` (decimal IPv4 form). These bypass
      :func:`ipaddress.ip_address` because Python rejects the shortcut
      form, but ``socket.getaddrinfo`` happily resolves them to loopback.

    DNS hostnames that don't look like loopback shortcuts are accepted;
    defending against a DNS rebind at connect-time would need ahead-of-time
    resolution which is fragile — TLS at the registry host is the actual
    authentication boundary.

    Pass ``allow_insecure=True`` to skip all checks (only useful for
    internal testing against a private Rekor instance on a non-public
    address).

    Raises
    ------
    SigningError
        If the URL fails any check and ``allow_insecure`` is False.
    """
    if allow_insecure:
        return
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise SigningError(
            f"rekor_url must use https:// (got {parsed.scheme!r}). "
            "Pass trust_insecure_rekor=True to bypass for private/test instances."
        )
    hostname = parsed.hostname
    if hostname is None:
        raise SigningError(f"rekor_url is missing a hostname: {url!r}")
    try:
        ip = ipaddress.ip_address(hostname)
    except ValueError:
        # Not a strict IP literal — apply the DNS-shortcut bypass guards.
        hl = hostname.lower()
        if (
            hl in _LOOPBACK_DNS_NAMES
            or hl.endswith(".localhost")
            or hl.startswith("localhost.")
        ):
            raise SigningError(
                f"rekor_url hostname {hostname!r} resolves to loopback. "
                "Pass trust_insecure_rekor=True if this is intentional."
            )
        if _NUMERIC_HOSTNAME_RE.fullmatch(hostname):
            # 127.1, 2130706433, 0177.0.0.1 etc. — ipaddress rejects these
            # but socket.getaddrinfo resolves them to private addresses on
            # most kernels. Numeric-only labels are not valid public DNS.
            raise SigningError(
                f"rekor_url hostname {hostname!r} is a numeric IP shortcut. "
                "Pass trust_insecure_rekor=True if this is intentional."
            )
        return
    if (
        ip.is_loopback or ip.is_private or ip.is_link_local
        or ip.is_multicast or ip.is_unspecified
    ):
        raise SigningError(
            f"rekor_url resolves to a non-public address ({ip}). "
            "Pass trust_insecure_rekor=True if this is intentional "
            "(e.g. a private Rekor instance on an internal network)."
        )


def default_key_path() -> Path:
    """Return the XDG-compliant default path for the user's private key.

    ``$XDG_CONFIG_HOME/mareforma/key`` if XDG_CONFIG_HOME is set,
    otherwise ``~/.config/mareforma/key``.
    """
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else Path.home() / ".config"
    return base / "mareforma" / "key"


# ---------------------------------------------------------------------------
# Key generation / IO
# ---------------------------------------------------------------------------

def generate_keypair() -> Ed25519PrivateKey:
    """Generate a fresh Ed25519 keypair."""
    return Ed25519PrivateKey.generate()


def save_private_key(
    key: Ed25519PrivateKey,
    path: Path,
    *,
    exclusive: bool = False,
) -> None:
    """Write a private key to *path* as PEM with mode 0600.

    Creates parent directories as needed. The leaf parent directory is
    chmodded to 0o700 on POSIX so its contents are not enumerable by
    other local users; ``~/.config`` itself is left alone since it is
    conventionally shared by many tools.

    Parameters
    ----------
    exclusive:
        If True, open *path* itself with ``O_CREAT|O_EXCL`` — the call
        raises ``FileExistsError`` if *path* already exists. Use this for
        first-time bootstrap to close the TOCTOU race between an
        ``exists()`` check and the rename. If False (default), the write
        uses an atomic tmp + rename so a crash mid-write cannot leave a
        truncated key on disk; an existing key is silently replaced.
    """
    path = Path(path)
    leaf_dir = path.parent
    leaf_dir.mkdir(parents=True, exist_ok=True)
    if os.name == "posix":
        try:
            os.chmod(leaf_dir, 0o700)
        except OSError:
            pass  # Non-fatal; the file itself still gets 0o600.

    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    if exclusive:
        # No tmp+rename: O_EXCL is the no-overwrite contract.
        fd = os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600,
        )
        try:
            os.write(fd, pem)
        except OSError:
            # If the write failed (disk full, IO error), the O_EXCL'd file
            # is on disk but empty. Without cleanup, the next bootstrap
            # hits FileExistsError and reports "key already exists" — a
            # misleading message that strands the user behind a zero-byte
            # file they don't know to delete. Unlink before re-raising.
            os.close(fd)
            try:
                os.unlink(path)
            except OSError:
                pass
            raise
        os.close(fd)
        return

    tmp = path.with_suffix(path.suffix + ".tmp")
    # Create with 0600 from the start so the key never exists with looser perms.
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, pem)
    finally:
        os.close(fd)
    os.replace(tmp, path)


def load_private_key(path: Path) -> Ed25519PrivateKey:
    """Read a PEM-encoded Ed25519 private key from disk.

    Raises
    ------
    KeyNotFoundError
        If ``path`` does not exist.
    KeyPermissionError
        On POSIX, if the file is readable by group or world.
    SigningError
        If the file is not a parseable Ed25519 private key.
    """
    path = Path(path)
    if not path.exists():
        raise KeyNotFoundError(
            f"No private key at {path}. Run `mareforma bootstrap` to create one."
        )
    if os.name == "posix":
        mode = path.stat().st_mode & 0o777
        if mode & 0o077:
            raise KeyPermissionError(
                f"Private key {path} has mode {oct(mode)}; must be 0600. "
                f"Fix with: chmod 600 {path}"
            )
    else:
        # Windows / other non-POSIX: file-mode bits are largely advisory.
        # Operators relying on filesystem perms for key confidentiality
        # need ACL-based controls (NTFS ACEs); mareforma does not set them.
        warnings.warn(
            f"Loaded {path} on a non-POSIX platform ({os.name!r}). "
            "File-mode perm check is skipped; key confidentiality depends "
            "on filesystem ACLs, which mareforma does not configure.",
            stacklevel=2,
        )
    try:
        pem = path.read_bytes()
        key = serialization.load_pem_private_key(pem, password=None)
    except Exception as exc:  # noqa: BLE001 — propagate as SigningError
        raise SigningError(f"Failed to load private key at {path}: {exc}") from exc
    if not isinstance(key, Ed25519PrivateKey):
        raise SigningError(
            f"Key at {path} is not an Ed25519 key (got {type(key).__name__}). "
            "mareforma only signs with Ed25519 currently."
        )
    return key


def public_key_id(public_key: Ed25519PublicKey) -> str:
    """Return a short, stable identifier for a public key.

    SHA-256 of the raw 32-byte public key, hex-encoded. Used as the
    ``keyid`` in the envelope so verifiers can index a validator pubkey
    set by id.
    """
    raw = public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return hashlib.sha256(raw).hexdigest()


def public_key_to_pem(public_key: Ed25519PublicKey) -> bytes:
    """Serialize an Ed25519 public key as PEM (SubjectPublicKeyInfo)."""
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def public_key_from_pem(pem: bytes) -> Ed25519PublicKey:
    """Parse a PEM-encoded Ed25519 public key.

    Raises
    ------
    SigningError
        If the PEM is malformed or names a non-Ed25519 algorithm.
    """
    try:
        key = serialization.load_pem_public_key(pem)
    except Exception as exc:  # noqa: BLE001
        raise SigningError(f"Failed to parse PEM public key: {exc}") from exc
    if not isinstance(key, Ed25519PublicKey):
        raise SigningError(
            f"PEM is not an Ed25519 public key (got {type(key).__name__})."
        )
    return key


# ---------------------------------------------------------------------------
# Envelope build / verify
# ---------------------------------------------------------------------------

def dsse_pae(payload_type: str, body: bytes) -> bytes:
    """Pre-Authentication Encoding per DSSE v1 spec.

    Returns ``b"DSSEv1 <len(type)> <type> <len(body)> <body>"``. The
    signature covers these bytes — never the payload alone — so an
    attacker cannot take a valid signature on (typeA, payload) and
    re-attribute it as a signature on (typeB, payload).

    Reference: https://github.com/secure-systems-lab/dsse/blob/master/protocol.md
    """
    pt_bytes = payload_type.encode("utf-8")
    return (
        b"DSSEv1 "
        + str(len(pt_bytes)).encode("ascii") + b" " + pt_bytes
        + b" " + str(len(body)).encode("ascii") + b" " + body
    )


def canonical_statement(
    claim_fields: dict[str, Any],
    evidence: dict[str, Any],
) -> bytes:
    """Canonical bytes of the in-toto Statement v1 for a claim.

    These bytes are what gets signed (after DSSE PAE wrap) and what
    chain_hash binds. Same input → same bytes — across Python versions,
    dict orderings, and Unicode normalization forms.

    Callers: ``sign_claim`` for envelope construction; ``db._chain_input_for_claim``
    for chain integrity; restore for adversarial re-derivation.
    """
    from . import _statement
    stmt = _statement.build_statement(
        claim_id=claim_fields["claim_id"],
        text=claim_fields["text"],
        classification=claim_fields["classification"],
        generated_by=claim_fields["generated_by"],
        supports=claim_fields.get("supports") or [],
        contradicts=claim_fields.get("contradicts") or [],
        source_name=claim_fields.get("source_name"),
        artifact_hash=claim_fields.get("artifact_hash"),
        created_at=claim_fields["created_at"],
        evidence=evidence,
    )
    from ._canonical import canonicalize
    return canonicalize(stmt)




def sign_claim(
    claim_fields: dict[str, Any],
    private_key: Ed25519PrivateKey,
    *,
    evidence: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Build a DSSE-signed in-toto Statement v1 envelope for a claim.

    Returns a DSSE envelope dict::

        {"payloadType": "application/vnd.in-toto+json",
         "payload":     "<base64 of canonicalize(statement)>",
         "signatures":  [{"keyid": "...", "sig": "<base64 of sign(PAE)>"}]}

    where ``statement`` is the in-toto Statement v1 produced by
    :func:`mareforma._statement.build_statement` and the signature
    covers the DSSE Pre-Authentication Encoding (not the payload alone).

    Parameters
    ----------
    claim_fields
        Must contain every key in :data:`SIGNED_FIELDS`.
    private_key
        Ed25519 private key.
    evidence
        Optional GRADE EvidenceVector serialized via ``EvidenceVector.to_dict()``.
        Defaults to ``{}`` (an empty vector that decodes back into the
        all-zeros default).
    """
    body = canonical_statement(claim_fields, evidence or {})
    return _build_envelope(body, private_key, payload_type=PAYLOAD_TYPE_CLAIM)


def _canonical_record(fields: tuple[str, ...], record: dict[str, Any]) -> bytes:
    """Canonicalise an arbitrary record using a fixed field list.

    Sorted keys, no whitespace. Used for non-claim envelope payloads
    (validator enrollment, validation, seed) which are not in-toto
    Statements but still get DSSE-PAE-signed.
    """
    payload = {name: record.get(name) for name in fields}
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def _build_envelope(
    payload_bytes: bytes,
    private_key: Ed25519PrivateKey,
    *,
    payload_type: str,
) -> dict[str, Any]:
    """Sign *payload_bytes* via DSSE PAE and wrap into the standard envelope.

    The signature covers ``dsse_pae(payload_type, payload_bytes)``, NOT
    the payload bytes alone. This is the type-safety property of DSSE:
    a signature on (typeA, payload) cannot be reused as a signature on
    (typeB, payload) even when the payload bytes are identical.
    """
    pae = dsse_pae(payload_type, payload_bytes)
    sig = private_key.sign(pae)
    keyid = public_key_id(private_key.public_key())
    return {
        "payloadType": payload_type,
        "payload": base64.standard_b64encode(payload_bytes).decode("ascii"),
        "signatures": [
            {
                "keyid": keyid,
                "sig": base64.standard_b64encode(sig).decode("ascii"),
            }
        ],
    }


def sign_validator_enrollment(
    enrollment: dict[str, Any],
    private_key: Ed25519PrivateKey,
) -> dict[str, Any]:
    """Sign a validator-enrollment record.

    The record must contain ``keyid`` (sha256-hex of the NEW validator's
    raw public key), ``pubkey_pem`` (base64 of the new validator's PEM),
    ``identity``, ``enrolled_at``, and ``enrolled_by_keyid``.
    *private_key* is the parent validator's key (equal to the new key for
    the root self-enrollment).
    """
    payload = _canonical_record(_ENROLLMENT_FIELDS, enrollment)
    return _build_envelope(
        payload, private_key,
        payload_type=PAYLOAD_TYPE_VALIDATOR_ENROLLMENT,
    )


def sign_validation(
    validation: dict[str, Any],
    private_key: Ed25519PrivateKey,
) -> dict[str, Any]:
    """Sign a validation event for a claim.

    The record must contain ``claim_id`` (the claim being promoted to
    ESTABLISHED), ``validator_keyid`` (the signing validator), and
    ``validated_at`` (ISO 8601 UTC). The envelope is persisted to the
    claim's ``validation_signature`` column so the promotion event is
    independently verifiable.
    """
    payload = _canonical_record(_VALIDATION_FIELDS, validation)
    return _build_envelope(
        payload, private_key,
        payload_type=PAYLOAD_TYPE_VALIDATION,
    )


def sign_seed_claim(
    seed: dict[str, Any],
    private_key: Ed25519PrivateKey,
) -> dict[str, Any]:
    """Sign a seed-claim attestation.

    The record must contain ``claim_id``, ``validator_keyid``
    (the asserting validator's keyid), and ``seeded_at``
    (ISO 8601 UTC). The envelope establishes the bootstrap of trust
    for a fresh graph: only enrolled validators can produce seed
    envelopes, and the envelope binds the moment of assertion so a
    verifier can detect post-hoc tampering with the ``validated_at``
    timestamp or the validator identity.

    The envelope is persisted to the claim's ``validation_signature``
    column, satisfying the CHECK constraint that requires ESTABLISHED
    rows to carry a signed validation envelope. The payload type
    ``application/vnd.mareforma.seed+json`` is distinct from the
    regular validation payload type so cross-type envelope
    substitution is detectable.
    """
    payload = _canonical_record(_SEED_FIELDS, seed)
    return _build_envelope(
        payload, private_key,
        payload_type=PAYLOAD_TYPE_SEED,
    )


def verify_envelope(
    envelope: dict[str, Any],
    public_key: Ed25519PublicKey,
    *,
    expected_payload_type: Optional[str] = None,
) -> bool:
    """Verify a signature envelope against a public key.

    Returns True iff the envelope is well-formed, names this public key
    (by keyid), and the signature matches the payload bytes.

    Does NOT decode the payload or re-validate semantic fields — those are
    the caller's concern. The contract here is purely cryptographic.

    Parameters
    ----------
    expected_payload_type:
        The envelope's ``payloadType`` must match this exact value.
        Defaults to the claim payload type — the most common case —
        so callers that omit the kwarg get type-safe behavior. Pass
        :data:`PAYLOAD_TYPE_VALIDATOR_ENROLLMENT` or
        :data:`PAYLOAD_TYPE_VALIDATION` explicitly when verifying
        those envelopes. There is no "accept any type" mode by design:
        cross-type acceptance lets an attacker pass a validation or
        enrollment envelope through a verifier expecting a claim.
    """
    if not isinstance(envelope, dict):
        raise InvalidEnvelopeError("envelope must be a dict")
    if expected_payload_type is None:
        expected_payload_type = PAYLOAD_TYPE_CLAIM
    declared = envelope.get("payloadType")
    if declared != expected_payload_type:
        raise InvalidEnvelopeError(
            f"unexpected payloadType: {declared!r} "
            f"(expected {expected_payload_type!r})"
        )
    try:
        payload_bytes = base64.standard_b64decode(envelope["payload"])
        signatures = envelope["signatures"]
        if not signatures:
            raise InvalidEnvelopeError("envelope has no signatures")
        sig_entry = signatures[0]
        keyid = sig_entry["keyid"]
        sig_bytes = base64.standard_b64decode(sig_entry["sig"])
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidEnvelopeError(f"malformed envelope: {exc}") from exc

    if keyid != public_key_id(public_key):
        return False

    # DSSE v1: signature covers PAE(payload_type, payload_bytes), not
    # payload_bytes alone. Using the wrong message during verify would
    # accept malformed envelopes that signed the raw payload.
    pae = dsse_pae(declared, payload_bytes)
    try:
        public_key.verify(sig_bytes, pae)
    except InvalidSignature:
        return False
    return True


def envelope_payload(envelope: dict[str, Any]) -> dict[str, Any]:
    """Decode an envelope's payload into a dict.

    For claim envelopes (``payloadType == application/vnd.in-toto+json``)
    the result is an in-toto Statement v1 dict with ``_type``, ``subject``,
    ``predicateType``, and ``predicate`` keys. Use
    :func:`claim_predicate_from_envelope` to extract the predicate
    (which contains the claim fields) in one call.

    For validation / enrollment / seed envelopes the result is the flat
    record (claim_id, validator_keyid, ...).

    Does NOT verify the signature — that is :func:`verify_envelope`'s job.
    Use after a successful verify, or for structural inspection only.

    Raises
    ------
    InvalidEnvelopeError
        If the envelope shape is wrong, the payload cannot be decoded,
        or the decoded JSON is not a top-level object.
    """
    if not isinstance(envelope, dict) or "payload" not in envelope:
        raise InvalidEnvelopeError("envelope is missing 'payload'")
    try:
        raw = base64.standard_b64decode(envelope["payload"])
        parsed = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidEnvelopeError(f"payload could not be decoded: {exc}") from exc
    if not isinstance(parsed, dict):
        raise InvalidEnvelopeError(
            f"payload must decode to a JSON object, got {type(parsed).__name__}"
        )
    return parsed


def claim_predicate_from_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    """Decode a claim envelope's Statement v1 payload and return the predicate.

    The predicate carries the SIGNED_FIELDS values + ``evidence``. Use
    this whenever a caller previously did ``envelope_payload(env)[key]``
    on a claim envelope — after Statement v1 the keys live one level
    deeper.

    Also enforces subject-vs-predicate consistency:

    - ``subject[0].name`` MUST equal ``"mareforma:claim:" + predicate.claim_id``
    - ``subject[0].digest.sha256`` MUST equal :func:`text_sha256(predicate.text)`

    Without these checks, a signer could issue an envelope whose
    in-toto ``subject`` (the part standard tooling like ``cosign``
    and GUAC keys off) names a different claim than the predicate
    asserts — the bytes verify but the two halves of the envelope
    disagree about what is being attested. Catching it here makes
    every claim envelope structurally honest before any downstream
    consumer sees it.

    Raises
    ------
    InvalidEnvelopeError
        If the envelope is not a claim envelope, or the Statement v1
        shape is malformed, or subject and predicate disagree.
    """
    from . import _statement as _stmt

    payload = envelope_payload(envelope)
    pt = envelope.get("payloadType")
    if pt != PAYLOAD_TYPE_CLAIM:
        raise InvalidEnvelopeError(
            f"not a claim envelope: payloadType={pt!r} "
            f"(expected {PAYLOAD_TYPE_CLAIM!r})"
        )
    if payload.get("_type") != _stmt.STATEMENT_TYPE:
        raise InvalidEnvelopeError(
            f"unexpected _type: {payload.get('_type')!r}"
        )
    if payload.get("predicateType") != _stmt.PREDICATE_TYPE:
        raise InvalidEnvelopeError(
            f"unexpected predicateType: {payload.get('predicateType')!r}"
        )
    predicate = payload.get("predicate")
    if not isinstance(predicate, dict):
        raise InvalidEnvelopeError(
            "Statement v1 predicate missing or not a JSON object"
        )
    subjects = payload.get("subject")
    if not isinstance(subjects, list) or len(subjects) != 1:
        raise InvalidEnvelopeError(
            "Statement v1 subject must be a single-entry list"
        )
    subj = subjects[0]
    if not isinstance(subj, dict):
        raise InvalidEnvelopeError(
            "Statement v1 subject[0] must be a JSON object"
        )
    expected_name = f"{_stmt.SUBJECT_NAME_PREFIX}{predicate.get('claim_id')}"
    if subj.get("name") != expected_name:
        raise InvalidEnvelopeError(
            f"subject.name {subj.get('name')!r} does not match "
            f"predicate.claim_id (expected {expected_name!r})"
        )
    digest = subj.get("digest")
    if not isinstance(digest, dict) or "sha256" not in digest:
        raise InvalidEnvelopeError("subject.digest.sha256 missing")
    expected_digest = _stmt.text_sha256(predicate.get("text") or "")
    if digest["sha256"] != expected_digest:
        raise InvalidEnvelopeError(
            "subject.digest.sha256 does not match predicate.text — "
            "envelope subject and predicate disagree"
        )
    return predicate


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Rekor transparency log
# ---------------------------------------------------------------------------

def submit_to_rekor(
    envelope: dict[str, Any],
    public_key: Ed25519PublicKey,
    *,
    rekor_url: str,
    timeout: float = _REKOR_TIMEOUT,
) -> tuple[bool, Optional[dict[str, Any]]]:
    """Submit a signed envelope to a Rekor transparency log.

    Uses the ``hashedrekord`` entry kind: Rekor receives the SHA-256 of the
    signed payload bytes, the raw signature, and the PEM public key. Rekor
    re-verifies the signature server-side and appends an immutable entry to
    the Merkle log, returning an inclusion proof.

    The call is synchronous and blocks up to ``timeout`` seconds. Callers
    that batch many claims should consider running unsigned or signed-
    without-Rekor (``rekor_url=None``) and flushing with
    ``EpistemicGraph.refresh_unsigned()`` rather than blocking each
    ``assert_claim``.

    Returns
    -------
    (logged, log_entry)
        ``logged`` is True iff Rekor returned 2xx, the response body
        decoded as JSON within the size cap, AND the body's encoded entry
        verifies against our submission (same payload hash and same
        signature). ``log_entry`` carries the uuid + integratedTime +
        logIndex on success, ``None`` on failure.

    Failure modes
    -------------
    Network errors, timeouts, non-2xx, oversized responses, and Rekor
    responses that fail body-matches-submission verification all return
    ``(False, None)`` — never raise. Caller persists the claim with
    ``transparency_logged=0`` and retries later via ``refresh_unsigned()``.
    """
    try:
        payload_bytes = base64.standard_b64decode(envelope["payload"])
        sig_b64 = envelope["signatures"][0]["sig"]
    except (KeyError, IndexError, TypeError, ValueError):
        return (False, None)

    pem = public_key_to_pem(public_key)
    expected_hash = hashlib.sha256(payload_bytes).hexdigest()
    proposed_entry = {
        "apiVersion": "0.0.1",
        "kind": "hashedrekord",
        "spec": {
            "data": {
                "hash": {
                    "algorithm": "sha256",
                    "value": expected_hash,
                },
            },
            "signature": {
                "content": sig_b64,
                "publicKey": {
                    "content": base64.standard_b64encode(pem).decode("ascii"),
                },
            },
        },
    }

    # Stream the response so a multi-MB body never lands fully in memory.
    # httpx.post() reads the full body before returning; switch to
    # httpx.stream() with a running-byte accumulator that aborts at
    # _MAX_REKOR_RESPONSE_SIZE.
    body_bytes: Optional[bytes] = None
    try:
        with httpx.stream(
            "POST",
            rekor_url,
            json=proposed_entry,
            headers={"User-Agent": _REKOR_USER_AGENT},
            timeout=timeout,
            follow_redirects=False,
        ) as r:
            if not (200 <= r.status_code < 300):
                return (False, None)
            content_length = r.headers.get("content-length")
            if content_length is not None:
                try:
                    if int(content_length) > _MAX_REKOR_RESPONSE_SIZE:
                        return (False, None)
                except ValueError:
                    return (False, None)
            received = bytearray()
            for chunk in r.iter_bytes():
                received.extend(chunk)
                if len(received) > _MAX_REKOR_RESPONSE_SIZE:
                    return (False, None)
            body_bytes = bytes(received)
    except (httpx.HTTPError, httpx.InvalidURL, OSError):
        return (False, None)

    if body_bytes is None:
        return (False, None)

    try:
        body = json.loads(body_bytes.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return (False, None)

    # Rekor returns {<uuid>: {body, integratedTime, logIndex, ...}}.
    if not isinstance(body, dict) or not body:
        return (False, None)

    try:
        uuid_key = next(iter(body))
        entry = body[uuid_key]
    except (StopIteration, TypeError):
        return (False, None)

    # Verify the returned entry actually records OUR submission. Without
    # this, a hostile or buggy server can hand back any uuid/logIndex and
    # mareforma would attach it to the bundle as proof of inclusion.
    encoded_body = entry.get("body") if isinstance(entry, dict) else None
    if not isinstance(encoded_body, str):
        return (False, None)
    try:
        decoded = base64.standard_b64decode(encoded_body)
        record = json.loads(decoded.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return (False, None)
    try:
        spec = record["spec"]
        rec_hash = spec["data"]["hash"]["value"]
        rec_sig = spec["signature"]["content"]
    except (KeyError, TypeError):
        return (False, None)
    if rec_hash.lower() != expected_hash.lower():
        return (False, None)
    # Byte-level signature comparison. Real Rekor instances may canonicalize
    # the entry body's base64 differently than what we POSTed (URL-safe vs
    # standard alphabet, padding variants); literal string equality would
    # false-reject those wire-equivalent representations. Decode both sides
    # tolerantly to raw bytes and compare.
    rec_sig_bytes = _b64_decode_tolerant(rec_sig)
    expected_sig_bytes = _b64_decode_tolerant(sig_b64)
    if rec_sig_bytes is None or expected_sig_bytes is None:
        return (False, None)
    if rec_sig_bytes != expected_sig_bytes:
        return (False, None)

    return (
        True,
        {
            "uuid": uuid_key,
            "integratedTime": entry.get("integratedTime"),
            "logIndex": entry.get("logIndex"),
        },
    )


def attach_rekor_entry(
    envelope: dict[str, Any],
    log_entry: dict[str, Any],
) -> dict[str, Any]:
    """Return a copy of *envelope* with a ``rekor`` block attached.

    The block is a future-compatible carrier for the transparency-log
    coordinates. It does NOT replace or modify the original payload or
    signatures — the envelope still verifies via :func:`verify_envelope`.
    """
    augmented = dict(envelope)
    augmented["rekor"] = {
        "uuid": log_entry.get("uuid"),
        "integratedTime": log_entry.get("integratedTime"),
        "logIndex": log_entry.get("logIndex"),
    }
    return augmented


# ---------------------------------------------------------------------------
# Rekor inclusion-proof verification (RFC 6962-style Merkle audit path)
# ---------------------------------------------------------------------------
#
# A Rekor entry includes a ``verification.inclusionProof`` block carrying:
#
#   - ``logIndex``     — 0-indexed position of the entry's leaf in the tree
#   - ``treeSize``     — total number of leaves in the tree at proof time
#   - ``hashes``       — list of hex-encoded sibling hashes from the leaf
#                        up to the root, in audit-path order
#   - ``rootHash``     — hex of the tree's Merkle root at proof time
#   - ``checkpoint``   — a signed-note text whose third line is the same
#                        rootHash (base64), bound to the log's public key
#                        via the signature on the note
#
# Verification has two independent parts:
#
#   1. **Merkle inclusion** — recompute the leaf hash from the entry body,
#      walk the audit path applying the RFC 6962 rule at each step, and
#      compare against ``rootHash``. This catches "the log returned a
#      tampered or fabricated entry".
#
#   2. **Checkpoint signature** — verify the signed-note over the
#      checkpoint text against the log's known public key. This catches
#      "the log unilaterally chose a root hash to cover for tampering".
#      Without this step, Merkle inclusion alone proves nothing — the
#      log operator could supply any root that hashes its forgery.
#
# Both parts are necessary; mareforma refuses inclusions that fail
# either. Callers supply the log's public key as PEM bytes; mareforma
# does NOT hardcode the public Sigstore Rekor key (TUF-based key
# distribution is out of scope), so users must fetch the key
# themselves and pass it to the verification entry points.

# RFC 6962 §2.1: leaf nodes are prefixed with 0x00, inner nodes with 0x01.
_RFC6962_LEAF_PREFIX = b"\x00"
_RFC6962_NODE_PREFIX = b"\x01"

# Rekor entry uuids are hex strings: either a 64-char SHA-256 digest
# alone or a tree-id-prefixed form ``<treehex>-<entryhex>`` (Rekor
# emits the latter for shard-aware deployments). The regex permits
# either: one or two lowercase-hex groups joined by a single hyphen.
_UUID_HEX_RE = re.compile(r"^[0-9a-f]+(?:-[0-9a-f]+)?$")


class RekorInclusionError(SigningError):
    """Raised when a Rekor inclusion proof cannot be verified.

    The ``reason`` attribute carries a short stable token so callers can
    pattern-match without parsing English messages:

      - ``"missing_proof"``        — entry body lacks ``verification.inclusionProof``
      - ``"malformed_proof"``      — proof block is not the expected shape
      - ``"bad_root_hex"``         — rootHash is not parseable hex
      - ``"bad_proof_hex"``        — one of the sibling hashes is unparseable
      - ``"merkle_root_mismatch"`` — recomputed root != claimed root
      - ``"checkpoint_missing"``   — signed-note text not supplied
      - ``"checkpoint_malformed"`` — signed-note doesn't match the format
      - ``"checkpoint_root_mismatch"`` — checkpoint's root != proof's root
      - ``"checkpoint_unsigned"``  — no signature lines in the note
      - ``"checkpoint_bad_sig"``   — ECDSA/Ed25519 verify failed
      - ``"unsupported_key"``      — log pubkey is neither Ed25519 nor ECDSA P-256
    """

    def __init__(self, message: str, *, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


def verify_merkle_inclusion_proof(
    leaf_hash: bytes,
    leaf_index: int,
    tree_size: int,
    proof_hashes: list[bytes],
    root_hash: bytes,
) -> bool:
    """Verify an RFC 6962 inclusion proof.

    Returns ``True`` iff hashing *leaf_hash* up the audit path produces
    *root_hash*. Pure function: no I/O, no parsing.

    Parameters
    ----------
    leaf_hash:
        SHA-256 of ``0x00 || canonical_entry_bytes``. Use
        :func:`compute_rekor_leaf_hash` to derive this from a Rekor
        entry's base64 ``body`` field.
    leaf_index:
        0-indexed position of the leaf in the tree.
    tree_size:
        Total number of leaves in the tree at proof time. Must be > 0
        and > leaf_index.
    proof_hashes:
        Sibling hashes along the audit path from the leaf to the root,
        in the order Rekor returns them.
    root_hash:
        The claimed tree root the proof should hash up to.

    The algorithm is RFC 6962 §2.1.1 (a.k.a. the Trillian
    ``proof.VerifyInclusion`` recipe). Even on a perfectly-balanced
    tree, the path may include "fold-up" steps near tree boundaries —
    the algorithm handles both balanced and unbalanced subtrees.
    """
    if tree_size <= 0:
        return False
    if leaf_index < 0 or leaf_index >= tree_size:
        return False
    if not isinstance(leaf_hash, (bytes, bytearray)) or len(leaf_hash) != 32:
        return False
    if not isinstance(root_hash, (bytes, bytearray)) or len(root_hash) != 32:
        return False

    fn = leaf_index
    sn = tree_size - 1
    r = bytes(leaf_hash)
    for sibling in proof_hashes:
        if not isinstance(sibling, (bytes, bytearray)) or len(sibling) != 32:
            return False
        if sn == 0:
            # Past the right edge of the tree — proof has more hashes
            # than the audit path actually needs. Reject as malformed
            # rather than silently accept.
            return False
        if fn % 2 == 1 or fn == sn:
            # Sibling is on the LEFT of the current node.
            r = hashlib.sha256(
                _RFC6962_NODE_PREFIX + bytes(sibling) + r,
            ).digest()
            # Skip the consecutive right-edge steps where this node's
            # subtree was the right child.
            while fn % 2 == 0 and fn != 0:
                fn >>= 1
                sn >>= 1
        else:
            # Sibling is on the RIGHT of the current node.
            r = hashlib.sha256(
                _RFC6962_NODE_PREFIX + r + bytes(sibling),
            ).digest()
        fn >>= 1
        sn >>= 1

    # All audit-path hashes consumed and we should be at the root.
    if sn != 0:
        # We stopped before reaching the root — proof was too short.
        return False
    return r == bytes(root_hash)


def compute_rekor_leaf_hash(entry_body_b64: str) -> bytes:
    """Return the RFC 6962 leaf hash for a Rekor entry.

    The Rekor entry's ``body`` field is a base64-encoded canonical JSON
    record. The Merkle leaf bytes are the DECODED record bytes; the
    leaf hash is ``SHA-256(0x00 || decoded_bytes)`` per RFC 6962 §2.1.

    Raises
    ------
    RekorInclusionError
        If ``entry_body_b64`` is not valid base64.
    """
    try:
        leaf_bytes = base64.standard_b64decode(entry_body_b64)
    except (ValueError, binascii.Error, TypeError) as exc:
        raise RekorInclusionError(
            f"entry body is not valid base64: {exc}",
            reason="malformed_proof",
        ) from exc
    return hashlib.sha256(_RFC6962_LEAF_PREFIX + leaf_bytes).digest()


# Signed-note format (used by Trillian / Sigstore-Rekor checkpoints):
#
#     <origin>\n
#     <tree size>\n
#     <root hash base64>\n
#     [optional extra body lines]\n
#     \n                              <- blank separator line
#     — <key name> <signature base64>\n
#     [more signatures]\n
#
# The signature covers every byte before the blank line, INCLUDING the
# trailing newline on the line that precedes the blank. The signature
# line itself uses U+2014 EM DASH as the delimiter (not ASCII hyphen).
# See https://github.com/transparency-dev/formats/blob/main/log/README.md

_SIGNED_NOTE_DASH = "—"  # U+2014 EM DASH


def parse_rekor_checkpoint(checkpoint_text: str) -> dict[str, Any]:
    """Parse a Rekor checkpoint in the signed-note format.

    Returns a dict with:

      - ``origin`` (str) — log identity
      - ``tree_size`` (int)
      - ``root_hash`` (bytes, 32 bytes)
      - ``signed_body`` (bytes) — the bytes the signature covers
      - ``signatures`` (list[(name, key_hash, sig_bytes)]) — every
        signature line in the note. ``key_hash`` is a 4-byte prefix
        derived by Trillian as ``SHA-256("<name>\\nA<pubkey-bytes>")[:4]``;
        we don't use it for verification but expose it for inspection.

    Raises
    ------
    RekorInclusionError(reason="checkpoint_malformed")
        Format does not match a signed note.
    """
    if not isinstance(checkpoint_text, str) or not checkpoint_text:
        raise RekorInclusionError(
            "checkpoint text is empty or not a string",
            reason="checkpoint_malformed",
        )

    # Split body / signatures on the first blank line.
    sep = "\n\n"
    sep_idx = checkpoint_text.find(sep)
    if sep_idx < 0:
        raise RekorInclusionError(
            "checkpoint missing blank-line separator between body and signatures",
            reason="checkpoint_malformed",
        )
    body_text = checkpoint_text[: sep_idx + 1]  # include trailing \n
    sig_text = checkpoint_text[sep_idx + 2 :]

    # Reject CR characters in the body section. A proxy that rewrote
    # LF→CRLF would corrupt the bytes the log operator signed, and
    # signature verification would then fail-closed with
    # ``checkpoint_bad_sig`` — accurate-but-misleading: the bytes
    # never were what the operator signed, they were mangled in
    # transit. Surface that distinction up-front with a clearer
    # ``checkpoint_malformed`` reason.
    if "\r" in body_text:
        raise RekorInclusionError(
            "checkpoint body contains carriage-return characters; "
            "the signed-note byte stream must be LF-only (a proxy "
            "rewriting LF to CRLF will break signature verification)",
            reason="checkpoint_malformed",
        )

    body_lines = body_text.rstrip("\n").split("\n")
    if len(body_lines) < 3:
        raise RekorInclusionError(
            f"checkpoint body has {len(body_lines)} lines, expected at least 3",
            reason="checkpoint_malformed",
        )
    origin = body_lines[0]
    try:
        tree_size = int(body_lines[1])
    except ValueError as exc:
        raise RekorInclusionError(
            f"checkpoint tree size {body_lines[1]!r} is not an integer",
            reason="checkpoint_malformed",
        ) from exc
    try:
        root_hash = base64.standard_b64decode(body_lines[2])
    except (ValueError, binascii.Error) as exc:
        raise RekorInclusionError(
            f"checkpoint root hash {body_lines[2]!r} is not valid base64",
            reason="checkpoint_malformed",
        ) from exc
    if len(root_hash) != 32:
        raise RekorInclusionError(
            f"checkpoint root hash is {len(root_hash)} bytes, expected 32",
            reason="checkpoint_malformed",
        )

    signatures: list[tuple[str, bytes, bytes]] = []
    for raw_line in sig_text.split("\n"):
        line = raw_line.rstrip("\r")
        if not line:
            continue
        # Each line: "— <name> <base64-encoded-sig-with-prefix>"
        prefix = _SIGNED_NOTE_DASH + " "
        if not line.startswith(prefix):
            raise RekorInclusionError(
                f"signature line {line!r} does not start with EM DASH",
                reason="checkpoint_malformed",
            )
        rest = line[len(prefix) :]
        parts = rest.rsplit(" ", 1)
        if len(parts) != 2:
            raise RekorInclusionError(
                f"signature line {line!r} missing 'name signature' split",
                reason="checkpoint_malformed",
            )
        name, sig_b64 = parts
        try:
            sig_blob = base64.standard_b64decode(sig_b64)
        except (ValueError, binascii.Error) as exc:
            raise RekorInclusionError(
                f"signature line {line!r} has invalid base64",
                reason="checkpoint_malformed",
            ) from exc
        if len(sig_blob) < 4:
            raise RekorInclusionError(
                f"signature blob too short ({len(sig_blob)} bytes); "
                "expected 4-byte keyhash prefix + signature bytes",
                reason="checkpoint_malformed",
            )
        key_hash = sig_blob[:4]
        sig_bytes = sig_blob[4:]
        signatures.append((name, key_hash, sig_bytes))

    if not signatures:
        raise RekorInclusionError(
            "checkpoint has no signature lines",
            reason="checkpoint_unsigned",
        )

    return {
        "origin": origin,
        "tree_size": tree_size,
        "root_hash": root_hash,
        "signed_body": body_text.encode("utf-8"),
        "signatures": signatures,
    }


def _verify_with_pubkey(public_key: Any, signed_body: bytes, sig: bytes) -> bool:
    """Verify *signed_body* with *public_key* (Ed25519 or ECDSA P-256).

    Returns False on any verify failure or unsupported key type rather
    than raising — callers wrap the False return in their own typed
    error if they want a raise contract.
    """
    if isinstance(public_key, Ed25519PublicKey):
        try:
            public_key.verify(sig, signed_body)
            return True
        except InvalidSignature:
            return False
    if isinstance(public_key, _ec.EllipticCurvePublicKey):
        # Sigstore Rekor v1 signs over SHA-256(body) with ECDSA P-256
        # (secp256r1) and ASN.1-DER-encoded signatures. Refuse other
        # curves explicitly so a swapped-curve key (P-384, P-521,
        # secp256k1) surfaces as ``unsupported_key`` rather than as
        # a generic ``checkpoint_bad_sig`` after the verify call
        # quietly fails.
        if not isinstance(public_key.curve, _ec.SECP256R1):
            return False
        try:
            public_key.verify(
                sig, signed_body, _ec.ECDSA(_hashes.SHA256()),
            )
            return True
        except InvalidSignature:
            return False
    return False


def verify_rekor_checkpoint(
    checkpoint_text: str,
    log_pubkey_pem: bytes,
    *,
    expected_root_hash: Optional[bytes] = None,
    expected_tree_size: Optional[int] = None,
) -> bool:
    """Verify the signed-note signature on a Rekor checkpoint.

    Returns ``True`` iff at least one signature line in the note
    verifies against *log_pubkey_pem* AND (when supplied) the
    checkpoint's root hash + tree size match the expected values.

    Parameters
    ----------
    checkpoint_text:
        The checkpoint string as returned by Rekor (typically inside
        ``verification.inclusionProof.checkpoint``, sometimes itself
        base64-encoded — caller decodes first).
    log_pubkey_pem:
        PEM-encoded Ed25519 or ECDSA P-256 public key of the log
        operator. mareforma does not hardcode the public Sigstore
        Rekor key currently; callers fetch it from a trusted source
        (Sigstore TUF, cosign root, etc.) and pass it here.
    expected_root_hash, expected_tree_size:
        Optional cross-check. When supplied, both must equal the
        values in the checkpoint body; this binds the signed note to
        the proof it accompanies. Without these, an attacker who
        controlled the proof block could swap in a SIGNED note from a
        DIFFERENT moment in the log's history (so the signature
        verifies) whose root happens to match a forged proof.

    Raises
    ------
    RekorInclusionError
        With ``reason`` in ``{"checkpoint_malformed", "checkpoint_unsigned",
        "checkpoint_root_mismatch", "unsupported_key", "checkpoint_bad_sig"}``.
    """
    try:
        public_key = serialization.load_pem_public_key(log_pubkey_pem)
    except (ValueError, TypeError) as exc:
        raise RekorInclusionError(
            f"log_pubkey_pem is not a valid PEM public key: {exc}",
            reason="unsupported_key",
        ) from exc
    if isinstance(public_key, Ed25519PublicKey):
        pass
    elif isinstance(public_key, _ec.EllipticCurvePublicKey):
        # P-256 only — Sigstore Rekor v1 uses secp256r1. Reject other
        # curves at type-check time with a precise reason instead of
        # letting them fall through to a generic ``checkpoint_bad_sig``.
        if not isinstance(public_key.curve, _ec.SECP256R1):
            raise RekorInclusionError(
                f"log pubkey curve {public_key.curve.name!r} unsupported; "
                "Sigstore Rekor v1 uses ECDSA secp256r1 (P-256)",
                reason="unsupported_key",
            )
    else:
        raise RekorInclusionError(
            f"log pubkey type {type(public_key).__name__} unsupported; "
            "Ed25519 or ECDSA P-256 expected",
            reason="unsupported_key",
        )

    parsed = parse_rekor_checkpoint(checkpoint_text)

    if expected_root_hash is not None and parsed["root_hash"] != expected_root_hash:
        raise RekorInclusionError(
            "checkpoint's root hash does not match the inclusion proof's "
            "root — possible signature-vs-proof splicing attack",
            reason="checkpoint_root_mismatch",
        )
    if expected_tree_size is not None and parsed["tree_size"] != expected_tree_size:
        raise RekorInclusionError(
            f"checkpoint's tree size ({parsed['tree_size']}) does not "
            f"match the inclusion proof's ({expected_tree_size})",
            reason="checkpoint_root_mismatch",
        )

    signed_body = parsed["signed_body"]
    for _name, _key_hash, sig_bytes in parsed["signatures"]:
        if _verify_with_pubkey(public_key, signed_body, sig_bytes):
            return True

    raise RekorInclusionError(
        "no signature line on the checkpoint verified against the supplied "
        "log public key",
        reason="checkpoint_bad_sig",
    )


def verify_rekor_inclusion(
    rekor_body: dict[str, Any],
    log_pubkey_pem: bytes,
) -> bool:
    """Verify a Rekor entry's full inclusion proof end-to-end.

    *rekor_body* is the FULL Rekor entry dict — the value side of the
    ``{uuid: entry}`` map Rekor returns. It must contain a ``body``
    field (base64-encoded canonical record) AND a
    ``verification.inclusionProof`` block with ``logIndex``,
    ``treeSize``, ``hashes``, ``rootHash``, and ``checkpoint``.

    The function:

      1. Extracts the inclusion proof.
      2. Computes the Merkle leaf hash from ``body``.
      3. Walks the audit path; refuses on root mismatch.
      4. Verifies the checkpoint's signature with *log_pubkey_pem*,
         cross-checking root + tree size between the proof and the
         signed note.

    Raises :class:`RekorInclusionError` (with a specific ``reason``)
    on any failure. Returns ``True`` only when both Merkle inclusion
    AND checkpoint signature succeed.
    """
    if not isinstance(rekor_body, dict):
        raise RekorInclusionError(
            "rekor_body must be a dict", reason="malformed_proof",
        )

    body_b64 = rekor_body.get("body")
    if not isinstance(body_b64, str):
        raise RekorInclusionError(
            "rekor_body missing 'body' string", reason="malformed_proof",
        )

    verification = rekor_body.get("verification")
    if not isinstance(verification, dict):
        raise RekorInclusionError(
            "rekor_body missing 'verification' block; this entry was "
            "returned without an inclusion proof and cannot be verified "
            "without re-fetching from the log",
            reason="missing_proof",
        )
    proof = verification.get("inclusionProof")
    if not isinstance(proof, dict):
        raise RekorInclusionError(
            "rekor_body missing 'verification.inclusionProof'",
            reason="missing_proof",
        )

    try:
        raw_log_index = proof["logIndex"]
        raw_tree_size = proof["treeSize"]
        hashes_hex = proof["hashes"]
        root_hash_hex = proof["rootHash"]
        checkpoint = proof["checkpoint"]
    except (KeyError, TypeError) as exc:
        raise RekorInclusionError(
            f"inclusionProof missing required field: {exc}",
            reason="malformed_proof",
        ) from exc
    # Reject floats and bools: ``int(42.9)`` truncates silently to 42,
    # and ``int(True)`` is 1. A hostile Rekor returning ``42.5`` would
    # otherwise surface as ``merkle_root_mismatch`` rather than the
    # accurate ``malformed_proof``. ``bool`` is a subclass of ``int``,
    # so the order of checks matters — bool first.
    for name, raw in (("logIndex", raw_log_index), ("treeSize", raw_tree_size)):
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise RekorInclusionError(
                f"inclusionProof.{name} must be an integer, got "
                f"{type(raw).__name__} ({raw!r})",
                reason="malformed_proof",
            )
    leaf_index = raw_log_index
    tree_size = raw_tree_size

    if not isinstance(hashes_hex, list) or not all(
        isinstance(h, str) for h in hashes_hex
    ):
        raise RekorInclusionError(
            "inclusionProof.hashes must be a list of hex strings",
            reason="malformed_proof",
        )

    try:
        proof_hashes = [bytes.fromhex(h) for h in hashes_hex]
    except ValueError as exc:
        raise RekorInclusionError(
            f"inclusionProof.hashes contains non-hex entry: {exc}",
            reason="bad_proof_hex",
        ) from exc

    try:
        claimed_root = bytes.fromhex(root_hash_hex)
    except (TypeError, ValueError) as exc:
        raise RekorInclusionError(
            f"inclusionProof.rootHash is not valid hex: {exc}",
            reason="bad_root_hex",
        ) from exc

    leaf_hash = compute_rekor_leaf_hash(body_b64)

    if not verify_merkle_inclusion_proof(
        leaf_hash, leaf_index, tree_size, proof_hashes, claimed_root,
    ):
        raise RekorInclusionError(
            f"Merkle inclusion proof failed: recomputed root for leaf "
            f"{leaf_index} in tree of size {tree_size} does not match the "
            f"claimed root {root_hash_hex!r}",
            reason="merkle_root_mismatch",
        )

    # The checkpoint may itself be base64-encoded on some Rekor versions.
    # Try as-is first; fall back to base64-decode if the as-is parse fails.
    if isinstance(checkpoint, str):
        checkpoint_text = checkpoint
        try:
            verify_rekor_checkpoint(
                checkpoint_text,
                log_pubkey_pem,
                expected_root_hash=claimed_root,
                expected_tree_size=tree_size,
            )
            return True
        except RekorInclusionError as exc:
            if exc.reason != "checkpoint_malformed":
                raise
            # Fallback: some Rekor versions return the checkpoint
            # itself base64-encoded. Decode and retry. If the decode
            # ALSO fails, re-raise the ORIGINAL ``checkpoint_malformed``
            # — not the raw ValueError/binascii.Error — so callers
            # relying on the documented RekorInclusionError-only
            # contract never see a leaked decode exception.
            try:
                checkpoint_text = base64.standard_b64decode(checkpoint).decode("utf-8")
            except (ValueError, binascii.Error, UnicodeDecodeError):
                raise exc
            verify_rekor_checkpoint(
                checkpoint_text,
                log_pubkey_pem,
                expected_root_hash=claimed_root,
                expected_tree_size=tree_size,
            )
            return True

    raise RekorInclusionError(
        "inclusionProof.checkpoint is missing or not a string",
        reason="checkpoint_missing",
    )


def fetch_inclusion_proof(
    uuid: str, rekor_url: str, *, timeout: float = _REKOR_TIMEOUT,
) -> dict[str, Any]:
    """Re-fetch a Rekor entry by uuid and return its full body.

    Used when the locally-stored sidecar row predates the
    inclusion-proof capture (existing rows from a pre-Merkle-verify
    version) or when callers want to re-verify against the log's
    current signed checkpoint.

    Parameters
    ----------
    uuid:
        Rekor entry uuid (from ``rekor_inclusions.uuid``).
    rekor_url:
        Base Rekor API URL — typically the same value passed to
        ``mareforma.open(rekor_url=...)``. The GET URL is constructed
        by appending ``/<uuid>`` to this.
    timeout:
        Per-request timeout in seconds.

    Returns the FULL entry dict (with ``body`` + ``verification``).

    Raises
    ------
    RekorInclusionError
        On invalid uuid, malformed rekor_url, network errors, non-2xx
        responses, oversized bodies, or malformed JSON.
    """
    # uuid validation. Rekor entry uuids are hex-encoded SHA-256
    # digests, optionally prefixed with a tree-id (also hex). A uuid
    # containing ``?``, ``#``, ``/``, or path-traversal segments
    # would shift the GET URL — even with a constant host, that's a
    # query-string smuggling primitive a hostile Rekor could exploit
    # by returning a crafted uuid in the submit response. Validate
    # before substitution.
    if not isinstance(uuid, str) or not _UUID_HEX_RE.match(uuid):
        raise RekorInclusionError(
            f"Rekor entry uuid must be a hex string (optionally with a "
            f"hex tree-id prefix separated by '-'); got {uuid!r}",
            reason="malformed_proof",
        )
    # Re-validate the rekor_url even though mareforma.open() already
    # did so at session start. Direct callers of this function (tests,
    # ad-hoc verifier scripts) could otherwise bypass the SSRF /
    # scheme defenses. Idempotent and cheap.
    try:
        validate_rekor_url(rekor_url)
    except SigningError as exc:
        raise RekorInclusionError(
            f"rekor_url failed SSRF / scheme validation: {exc}",
            reason="malformed_proof",
        ) from exc
    fetch_url = rekor_url.rstrip("/") + "/" + uuid
    try:
        with httpx.stream(
            "GET",
            fetch_url,
            headers={"User-Agent": _REKOR_USER_AGENT},
            timeout=timeout,
            follow_redirects=False,
        ) as r:
            if not (200 <= r.status_code < 300):
                raise RekorInclusionError(
                    f"Rekor GET {fetch_url} returned HTTP {r.status_code}",
                    reason="missing_proof",
                )
            content_length = r.headers.get("content-length")
            if content_length is not None:
                try:
                    if int(content_length) > _MAX_REKOR_RESPONSE_SIZE:
                        raise RekorInclusionError(
                            f"Rekor response content-length "
                            f"{content_length} exceeds cap "
                            f"{_MAX_REKOR_RESPONSE_SIZE}",
                            reason="malformed_proof",
                        )
                except ValueError as exc:
                    raise RekorInclusionError(
                        f"Rekor response content-length not an integer: "
                        f"{content_length!r}",
                        reason="malformed_proof",
                    ) from exc
            received = bytearray()
            for chunk in r.iter_bytes():
                received.extend(chunk)
                if len(received) > _MAX_REKOR_RESPONSE_SIZE:
                    raise RekorInclusionError(
                        f"Rekor response body exceeds cap "
                        f"{_MAX_REKOR_RESPONSE_SIZE}",
                        reason="malformed_proof",
                    )
            body_bytes = bytes(received)
    except (httpx.HTTPError, httpx.InvalidURL, OSError) as exc:
        raise RekorInclusionError(
            f"Rekor GET {fetch_url} failed: {exc}",
            reason="missing_proof",
        ) from exc

    try:
        parsed = json.loads(body_bytes.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise RekorInclusionError(
            f"Rekor GET {fetch_url} returned non-JSON: {exc}",
            reason="malformed_proof",
        ) from exc
    if not isinstance(parsed, dict) or not parsed:
        raise RekorInclusionError(
            f"Rekor GET {fetch_url} returned empty or non-object body",
            reason="malformed_proof",
        )
    # Body shape: {uuid: entry}. Return the entry side.
    try:
        return next(iter(parsed.values()))
    except StopIteration as exc:
        raise RekorInclusionError(
            f"Rekor GET {fetch_url} returned an empty map",
            reason="malformed_proof",
        ) from exc


def fetch_log_pubkey(
    rekor_url: str, *, timeout: float = _REKOR_TIMEOUT,
) -> bytes:
    """Fetch the log operator's public key from a Rekor instance.

    Used by :func:`mareforma.open` to implement TOFU pinning of the
    log key: the first connection to a Rekor URL fetches the key over
    HTTPS and persists it to ``.mareforma/rekor_log_pubkey.pem``;
    every subsequent connection loads from disk and the substrate
    refuses silent rotation (a mismatched key triggers a verification
    failure on the next inclusion proof).

    URL transformation: ``rekor_url`` is typically the entries
    endpoint (``…/api/v1/log/entries``); the pubkey endpoint sits at
    ``…/api/v1/log/publicKey``. The implementation strips the
    trailing ``/entries`` (if present) and appends ``/publicKey``.

    Parameters
    ----------
    rekor_url:
        The same value passed to ``mareforma.open(rekor_url=...)``.
    timeout:
        Per-request timeout in seconds.

    Returns
    -------
    bytes
        The PEM-encoded log operator public key.

    Raises
    ------
    SigningError
        On malformed rekor_url, network errors, non-2xx response,
        oversized body, or a response body that does not parse as PEM.
    """
    # Re-validate the rekor_url so the SSRF / scheme defense is a
    # property of this function rather than the call graph that
    # leads to it. mareforma.open() already validates; direct callers
    # (tests, scripts) might not. Idempotent and cheap.
    validate_rekor_url(rekor_url)
    base = rekor_url.rstrip("/")
    # Best-effort: if the URL ends in `/entries`, replace that segment;
    # otherwise just append `/publicKey`.
    if base.endswith("/entries"):
        pubkey_url = base[: -len("/entries")] + "/publicKey"
    else:
        pubkey_url = base + "/publicKey"
    try:
        with httpx.stream(
            "GET",
            pubkey_url,
            headers={"User-Agent": _REKOR_USER_AGENT},
            timeout=timeout,
            follow_redirects=False,
        ) as r:
            if not (200 <= r.status_code < 300):
                raise SigningError(
                    f"Rekor GET {pubkey_url} returned HTTP {r.status_code}"
                )
            content_length = r.headers.get("content-length")
            if content_length is not None:
                try:
                    if int(content_length) > _MAX_REKOR_RESPONSE_SIZE:
                        raise SigningError(
                            f"Rekor publicKey response content-length "
                            f"{content_length} exceeds cap "
                            f"{_MAX_REKOR_RESPONSE_SIZE}"
                        )
                except ValueError as exc:
                    raise SigningError(
                        f"Rekor publicKey content-length not an integer: "
                        f"{content_length!r}"
                    ) from exc
            received = bytearray()
            for chunk in r.iter_bytes():
                received.extend(chunk)
                if len(received) > _MAX_REKOR_RESPONSE_SIZE:
                    raise SigningError(
                        f"Rekor publicKey response body exceeds cap "
                        f"{_MAX_REKOR_RESPONSE_SIZE}"
                    )
            body = bytes(received)
    except (httpx.HTTPError, httpx.InvalidURL, OSError) as exc:
        raise SigningError(
            f"Rekor GET {pubkey_url} failed: {exc}"
        ) from exc

    # PEM check — must contain a BEGIN/END public-key block. Wider
    # check than load_pem_public_key alone so the operator sees a
    # clear error when the log served HTML / JSON / nothing useful.
    if b"-----BEGIN PUBLIC KEY-----" not in body:
        raise SigningError(
            f"Rekor GET {pubkey_url} did not return a PEM public key "
            f"(first 60 bytes: {body[:60]!r})"
        )
    # Smoke-test parse so callers get a clean SigningError up front
    # rather than a confusing crypto error at first use.
    try:
        serialization.load_pem_public_key(body)
    except (ValueError, TypeError) as exc:
        raise SigningError(
            f"Rekor publicKey response failed PEM parse: {exc}"
        ) from exc
    return body


def bootstrap_key(
    path: Optional[Path] = None,
    *,
    overwrite: bool = False,
) -> tuple[Path, str]:
    """Generate and persist a fresh keypair at *path*.

    Returns ``(path, public_key_id)``. Refuses to overwrite an existing key
    unless ``overwrite=True``.

    No-overwrite mode uses ``O_CREAT|O_EXCL`` so two concurrent bootstraps
    cannot both pass the existence check and race-write conflicting keys.
    The loser of the race raises :class:`SigningError`.

    Overwrite mode is destructive in two ways
    -----------------------------------------
    1. **Verification:** every claim signed with the prior key becomes
       unverifiable from this machine — the old public key is gone, so
       :func:`verify_envelope` will see ``keyid`` mismatches forever.
    2. **Rekor stranding:** any signed claim that has not yet been
       submitted to Rekor (``transparency_logged=0``) becomes permanently
       un-loggable. :meth:`EpistemicGraph.refresh_unsigned` checks the
       envelope's keyid against the current signer's keyid and skips
       mismatches; without the old key on disk, those claims cannot
       advance to ``transparency_logged=1`` and will never reach
       ``REPLICATED``.

    If you must rotate, back up the prior key first, run
    ``refresh_unsigned()`` with the old key to drain the pending queue,
    then bootstrap the new one.
    """
    target = Path(path) if path is not None else default_key_path()
    key = generate_keypair()

    if overwrite:
        save_private_key(key, target)
        return target, public_key_id(key.public_key())

    try:
        save_private_key(key, target, exclusive=True)
    except FileExistsError as exc:
        raise SigningError(
            f"Key already exists at {target}. Refuse to overwrite — every "
            "claim signed by the existing key would become unverifiable. "
            "Pass overwrite=True if this is intentional."
        ) from exc
    return target, public_key_id(key.public_key())
