"""
rekor.py: Sigstore-Rekor transparency-log integration.

Mareforma submits each signed claim envelope to a Rekor instance for
public, tamper-evident witnessing. The submit path verifies the response
binds OUR hash + OUR signature; the opt-in inclusion-proof path re-fetches
the entry and cryptographically verifies the RFC 6962 Merkle audit path
against the log's signed checkpoint.

Two independent guarantees
--------------------------
1. **Submit-time response binding.** :func:`submit_to_rekor` rejects a
   Rekor response whose recorded ``entry.body`` does not encode OUR
   payload hash AND OUR raw signature. A hostile or buggy registry
   cannot launder an arbitrary ``uuid``/``logIndex`` into the bundle
   as proof of inclusion.

2. **Merkle inclusion + checkpoint signature** (opt-in via
   ``rekor_log_pubkey_pem`` at :func:`mareforma.open`).
   :func:`verify_rekor_inclusion` re-derives the leaf hash, walks the
   audit path, and verifies the signed checkpoint over the resulting
   root. Closes the gap where submit-time response binding alone proves
   "Rekor returned an entry recording OUR hash + sig" but NOT "the log
   committed the entry and didn't mutate / remove / reposition it after."

SSRF defense
------------
:func:`validate_rekor_url` enforces HTTPS and rejects loopback /
private / link-local / multicast / unspecified IP literals, plus DNS
shortcuts that resolve to loopback at connect time (``localhost``
and friends, numeric-only hostnames like ``127.1`` /
``2130706433``). :func:`fetch_inclusion_proof` and
:func:`fetch_log_pubkey` both re-validate the URL at function entry
so direct callers (tests, scripts) cannot bypass the defense by
skipping :func:`mareforma.open`.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import ipaddress
import json
import re
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes as _hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec as _ec
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from .core import SigningError, public_key_to_pem


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
    non-alphabet bytes by default, which is intentional here: garbage
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
    resolution which is fragile: TLS at the registry host is the actual
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


# ---------------------------------------------------------------------------
# Rekor submission
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
    ``(False, None)``, never raise. Caller persists the claim with
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
    signatures: the envelope still verifies via :func:`verify_envelope`.
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

      - ``"missing_proof"``:        entry body lacks ``verification.inclusionProof``
      - ``"malformed_proof"``:      proof block is not the expected shape
      - ``"bad_root_hex"``:         rootHash is not parseable hex
      - ``"bad_proof_hex"``:        one of the sibling hashes is unparseable
      - ``"merkle_root_mismatch"``: recomputed root != claimed root
      - ``"checkpoint_missing"``:   signed-note text not supplied
      - ``"checkpoint_malformed"``: signed-note doesn't match the format
      - ``"checkpoint_root_mismatch"``: checkpoint's root != proof's root
      - ``"checkpoint_unsigned"``:  no signature lines in the note
      - ``"checkpoint_bad_sig"``:   ECDSA/Ed25519 verify failed
      - ``"unsupported_key"``:      log pubkey is neither Ed25519 nor ECDSA P-256
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
    tree, the path may include "fold-up" steps near tree boundaries:
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

      - ``origin`` (str): log identity
      - ``tree_size`` (int)
      - ``root_hash`` (bytes, 32 bytes)
      - ``signed_body`` (bytes): the bytes the signature covers
      - ``signatures`` (list[(name, key_hash, sig_bytes)]): every
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
    than raising: callers wrap the False return in their own typed
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
        base64-encoded, caller decodes first).
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

    *rekor_body* is the FULL Rekor entry dict, the value side of the
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
        Base Rekor API URL, typically the same value passed to
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
    every subsequent connection loads from disk and mareforma
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
