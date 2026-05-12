"""
signing.py — Ed25519 claim signing and verification.

Every claim in the epistemic graph is signed locally before INSERT. The
signature is stored alongside the claim as a DSSE-style JSON envelope so
that future Rekor (transparency-log) integration can ingest the same
bundle format without schema changes.

Key lifecycle
-------------
- One Ed25519 keypair per user. Private key at ``~/.config/mareforma/key``
  (XDG-compliant, mode 0600). Public key derived on the fly.
- ``mareforma bootstrap`` generates the key once at install time.
- The library never auto-creates a key. Missing key + ``require_signed=False``
  → claims are inserted with ``signature_bundle=NULL`` (unsigned).
- Missing key + ``require_signed=True`` → :class:`KeyNotFoundError`.

Envelope format
---------------
A simplified DSSE shape. The payload is canonical JSON of the signed claim
fields; the signature covers the payload bytes directly (no PAE wrapping
yet — Rekor integration in Phase B will switch to PAE for sigstore
compatibility).

::

    {
      "payloadType": "application/vnd.mareforma.claim+json",
      "payload":     "<base64 of canonical JSON>",
      "signatures": [
        {"keyid": "<hex sha256 of pubkey bytes>", "sig": "<base64 sig>"}
      ]
    }

The signed payload always contains exactly these fields (sorted, no nulls):
``claim_id``, ``text``, ``classification``, ``generated_by``, ``supports``,
``contradicts``, ``source_name``, ``created_at``. Including ``created_at``
binds the signature to an authorial timestamp; once Rekor is wired in
Phase B, the transparency log entry adds an independent witnessed time.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Optional

import httpx
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
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


_PAYLOAD_TYPE = "application/vnd.mareforma.claim+json"

# Fields included in the signed payload. Sorted at envelope build time so the
# signature is order-stable across writers.
_SIGNED_FIELDS = (
    "claim_id",
    "text",
    "classification",
    "generated_by",
    "supports",
    "contradicts",
    "source_name",
    "created_at",
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


def save_private_key(key: Ed25519PrivateKey, path: Path) -> None:
    """Write a private key to *path* as PEM with mode 0600.

    Creates parent directories as needed. Overwrites any existing file
    atomically (write-temp-then-rename) so a crash mid-write cannot leave
    a truncated key on disk.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
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
    try:
        pem = path.read_bytes()
        key = serialization.load_pem_private_key(pem, password=None)
    except Exception as exc:  # noqa: BLE001 — propagate as SigningError
        raise SigningError(f"Failed to load private key at {path}: {exc}") from exc
    if not isinstance(key, Ed25519PrivateKey):
        raise SigningError(
            f"Key at {path} is not an Ed25519 key (got {type(key).__name__}). "
            "mareforma only signs with Ed25519 in v0.3.0."
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

def _canonical_payload(claim_fields: dict[str, Any]) -> bytes:
    """Canonicalise a claim into the bytes that get signed.

    Only keys in ``_SIGNED_FIELDS`` are included. ``supports``/``contradicts``
    default to ``[]``; ``source_name`` defaults to ``None``. Output is JSON
    with sorted keys and no whitespace — same input → same bytes → same
    signature.
    """
    payload = {
        "claim_id": claim_fields["claim_id"],
        "text": claim_fields["text"],
        "classification": claim_fields["classification"],
        "generated_by": claim_fields["generated_by"],
        "supports": list(claim_fields.get("supports") or []),
        "contradicts": list(claim_fields.get("contradicts") or []),
        "source_name": claim_fields.get("source_name"),
        "created_at": claim_fields["created_at"],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign_claim(
    claim_fields: dict[str, Any],
    private_key: Ed25519PrivateKey,
) -> dict[str, Any]:
    """Build a signed envelope for a claim. Returns the envelope dict.

    The caller is responsible for JSON-encoding the returned dict before
    persisting to the ``signature_bundle`` column.
    """
    payload_bytes = _canonical_payload(claim_fields)
    sig = private_key.sign(payload_bytes)
    keyid = public_key_id(private_key.public_key())
    return {
        "payloadType": _PAYLOAD_TYPE,
        "payload": base64.standard_b64encode(payload_bytes).decode("ascii"),
        "signatures": [
            {
                "keyid": keyid,
                "sig": base64.standard_b64encode(sig).decode("ascii"),
            }
        ],
    }


def verify_envelope(
    envelope: dict[str, Any],
    public_key: Ed25519PublicKey,
) -> bool:
    """Verify a signature envelope against a public key.

    Returns True iff the envelope is well-formed, names this public key
    (by keyid), and the signature matches the payload bytes.

    Does NOT decode the payload or re-validate claim fields — those are the
    caller's concern. The contract here is purely cryptographic.
    """
    if not isinstance(envelope, dict):
        raise InvalidEnvelopeError("envelope must be a dict")
    if envelope.get("payloadType") != _PAYLOAD_TYPE:
        raise InvalidEnvelopeError(
            f"unexpected payloadType: {envelope.get('payloadType')!r}"
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

    try:
        public_key.verify(sig_bytes, payload_bytes)
    except InvalidSignature:
        return False
    return True


def envelope_payload(envelope: dict[str, Any]) -> dict[str, Any]:
    """Decode an envelope's payload back into the claim-fields dict.

    Does NOT verify the signature — that is :func:`verify_envelope`'s job.
    Use this only after a successful verify, or when you only need to
    inspect the payload structure.
    """
    if not isinstance(envelope, dict) or "payload" not in envelope:
        raise InvalidEnvelopeError("envelope is missing 'payload'")
    try:
        raw = base64.standard_b64decode(envelope["payload"])
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidEnvelopeError(f"payload could not be decoded: {exc}") from exc


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

    Returns
    -------
    (logged, log_entry)
        ``logged`` is True iff Rekor returned 2xx and the response parsed.
        ``log_entry`` carries the uuid + integratedTime + logIndex on
        success, ``None`` on failure.

    Failure modes
    -------------
    Network errors, timeouts, non-2xx, and malformed responses all return
    ``(False, None)`` — never raise. Caller persists the claim with
    ``transparency_logged=0`` and retries later via ``refresh_unsigned()``.
    """
    try:
        payload_bytes = base64.standard_b64decode(envelope["payload"])
        sig_b64 = envelope["signatures"][0]["sig"]
    except (KeyError, IndexError, TypeError, ValueError):
        return (False, None)

    pem = public_key_to_pem(public_key)
    proposed_entry = {
        "apiVersion": "0.0.1",
        "kind": "hashedrekord",
        "spec": {
            "data": {
                "hash": {
                    "algorithm": "sha256",
                    "value": hashlib.sha256(payload_bytes).hexdigest(),
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

    try:
        r = httpx.post(
            rekor_url,
            json=proposed_entry,
            headers={"User-Agent": _REKOR_USER_AGENT},
            timeout=timeout,
            follow_redirects=False,
        )
    except (httpx.HTTPError, httpx.InvalidURL, OSError):
        return (False, None)

    if not (200 <= r.status_code < 300):
        return (False, None)

    try:
        body = r.json()
    except ValueError:
        return (False, None)

    # Rekor returns {<uuid>: {body, integratedTime, logIndex, ...}}.
    if not isinstance(body, dict) or not body:
        return (False, None)
    try:
        uuid_key = next(iter(body))
        entry = body[uuid_key]
        return (
            True,
            {
                "uuid": uuid_key,
                "integratedTime": entry.get("integratedTime"),
                "logIndex": entry.get("logIndex"),
            },
        )
    except (StopIteration, TypeError):
        return (False, None)


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


def bootstrap_key(
    path: Optional[Path] = None,
    *,
    overwrite: bool = False,
) -> tuple[Path, str]:
    """Generate and persist a fresh keypair at *path*.

    Returns ``(path, public_key_id)``. Refuses to overwrite an existing key
    unless ``overwrite=True`` — accidental regeneration would orphan every
    claim ever signed with the prior key.
    """
    target = Path(path) if path is not None else default_key_path()
    if target.exists() and not overwrite:
        raise SigningError(
            f"Key already exists at {target}. Refuse to overwrite — every "
            "claim signed by the existing key would become unverifiable. "
            "Pass overwrite=True if this is intentional."
        )
    key = generate_keypair()
    save_private_key(key, target)
    return target, public_key_id(key.public_key())
