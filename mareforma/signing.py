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

Envelope format
---------------
A simplified DSSE shape. The payload is canonical JSON of the signed claim
fields; the signature covers the payload bytes directly. (No PAE wrapping
yet — that switch lands when full sigstore-bundle compatibility ships.)

::

    {
      "payloadType": "application/vnd.mareforma.claim+json",
      "payload":     "<base64 of canonical JSON>",
      "signatures": [
        {"keyid": "<hex sha256 of pubkey bytes>", "sig": "<base64 sig>"}
      ],
      "rekor": {"uuid": ..., "logIndex": ..., "integratedTime": ...}
    }

The ``rekor`` block is added by :func:`attach_rekor_entry` after a successful
transparency-log submission; it does not affect signature verification.

The signed payload always contains exactly these fields (sorted, no nulls):
``claim_id``, ``text``, ``classification``, ``generated_by``, ``supports``,
``contradicts``, ``source_name``, ``created_at``. Including ``created_at``
binds the signature to an authorial timestamp; the Rekor entry contributes
an independent witnessed time.
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

# 64 KB: a Rekor entry should be under a few KB in practice. A malicious or
# buggy server returning multi-MB JSON would otherwise land in graph.db and
# then be re-encoded into claims.toml on every backup.
_MAX_REKOR_RESPONSE_SIZE = 64 * 1024


_PAYLOAD_TYPE = "application/vnd.mareforma.claim+json"

# Fields included in the signed payload. Sorted at envelope build time so the
# signature is order-stable across writers. Public so db.update_claim can
# refuse mutations on these fields when the row already carries a signature.
SIGNED_FIELDS = (
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

def canonical_payload(claim_fields: dict[str, Any]) -> bytes:
    """Canonicalise a claim into the bytes that get signed.

    Only keys in :data:`SIGNED_FIELDS` are included. ``supports``/``contradicts``
    default to ``[]``; ``source_name`` defaults to ``None``. Output is JSON
    with sorted keys and no whitespace — same input → same bytes → same
    signature. Public so verifiers can independently re-derive the bytes
    that should be signed.
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


def _canonical_payload(claim_fields: dict[str, Any]) -> bytes:
    """Legacy private alias retained for internal callers; use canonical_payload."""
    return canonical_payload(claim_fields)


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

    Raises
    ------
    InvalidEnvelopeError
        If the envelope shape is wrong, the payload cannot be decoded,
        or the decoded JSON is not a top-level object. The dict-only
        contract matters: callers downstream do ``payload.get(...)`` and
        a bare JSON string would otherwise raise AttributeError.
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
