"""
validators.py — Per-project validator enrollment.

A validator is a public key permitted to call ``graph.validate()`` and
promote a REPLICATED claim to ESTABLISHED. The set of permitted keys is
stored in the ``validators`` table of the project's ``graph.db``.
Mareforma is a local epistemic graph; validators here are local-trust
entries, not cross-org PKI.

Root of trust
-------------
The first key opened against a fresh project ``graph.db`` auto-enrolls
as the root validator with a self-signed enrollment envelope.
Subsequent validators are added by an already-enrolled validator via
``enroll_validator()`` or the ``mareforma validator add`` CLI. Removal
is intentionally not supported in v0.3.0 — append-only validator
history mirrors the append-only claim history.

Enrollment payload
------------------
The signed payload of an enrollment binds:

  - ``keyid``              sha256-hex of the new validator's raw public key
  - ``pubkey_pem``         base64 of the new validator's PEM SubjectPublicKeyInfo
  - ``identity``           free-form display name (email, lab tag, etc.)
  - ``enrolled_at``        ISO 8601 UTC
  - ``enrolled_by_keyid``  parent validator's keyid;
                           equals ``keyid`` for the root self-enrollment

The signature is produced by the parent validator's private key. Verifiers
walk the chain back to a self-signed row at the root.
"""

from __future__ import annotations

import base64
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from mareforma import signing as _signing


class ValidatorError(Exception):
    """Base exception for validator-enrollment errors."""


class ValidatorNotEnrolledError(ValidatorError):
    """Raised when a caller tries to act as a validator without being enrolled."""


class ValidatorAlreadyEnrolledError(ValidatorError):
    """Raised when enrolling a key that is already in the validators table."""


class InvalidIdentityError(ValidatorError):
    """Raised when an enrollment identity fails the format check."""


_MAX_IDENTITY_LEN = 256

# Unicode display-spoofing characters we refuse on top of C0/C1 controls.
# RTL/LTR overrides and zero-width chars are technically printable but
# let an attacker visually disguise the (root) marker in
# `mareforma validator list` output. Operators rarely need these in
# legitimate identity labels.
_FORBIDDEN_DISPLAY_CHARS = frozenset({
    "​",  # ZERO WIDTH SPACE
    "‌",  # ZERO WIDTH NON-JOINER
    "‍",  # ZERO WIDTH JOINER
    "\u200E",  # LEFT-TO-RIGHT MARK
    "\u200F",  # RIGHT-TO-LEFT MARK
    "\u202A",  # LEFT-TO-RIGHT EMBEDDING
    "\u202B",  # RIGHT-TO-LEFT EMBEDDING
    "\u202C",  # POP DIRECTIONAL FORMATTING
    "\u202D",  # LEFT-TO-RIGHT OVERRIDE
    "\u202E",  # RIGHT-TO-LEFT OVERRIDE
    "\u2066",  # LEFT-TO-RIGHT ISOLATE
    "\u2067",  # RIGHT-TO-LEFT ISOLATE
    "\u2068",  # FIRST STRONG ISOLATE
    "\u2069",  # POP DIRECTIONAL ISOLATE
    "﻿",  # ZERO WIDTH NO-BREAK SPACE / BOM
})


def _validate_identity(identity: str) -> str:
    """Reject identities with control characters, NULs, excessive length,
    or Unicode display-spoofing characters.

    The identity string is signed into the enrollment envelope AND
    displayed by ``mareforma validator list``. Without sanitization an
    operator pasting a malicious identity could plant ANSI escapes that
    spoof the ``(root)`` marker on a different row, or fill the table
    with arbitrary-length blobs that bloat the project on every backup.
    """
    if not isinstance(identity, str):
        raise InvalidIdentityError(
            f"identity must be a string, got {type(identity).__name__}"
        )
    if not identity:
        raise InvalidIdentityError("identity must be non-empty")
    if len(identity) > _MAX_IDENTITY_LEN:
        raise InvalidIdentityError(
            f"identity exceeds {_MAX_IDENTITY_LEN}-character cap "
            f"(got {len(identity)})"
        )
    for ch in identity:
        if ch == " ":
            continue
        if ord(ch) < 0x20 or ord(ch) == 0x7f:
            raise InvalidIdentityError(
                f"identity contains a control character (codepoint "
                f"U+{ord(ch):04X}); use printable characters only"
            )
        if ch in _FORBIDDEN_DISPLAY_CHARS:
            raise InvalidIdentityError(
                f"identity contains a display-spoofing character "
                f"(codepoint U+{ord(ch):04X}); RTL overrides and "
                "zero-width characters are refused to prevent visual "
                "spoofing of the (root) marker"
            )
    return identity


# ---------------------------------------------------------------------------
# Read paths
# ---------------------------------------------------------------------------

_CACHE_ATTR = "_mareforma_verified_keyids"

# Cap chain-walk depth. Any realistic local-trust hierarchy is single-digit
# deep; longer chains in the validators table indicate either a malicious
# attempt to DoS the verifier (the chain walk fires on every is_enrolled
# call) or a misuse worth surfacing as a verification failure.
_MAX_CHAIN_DEPTH = 64


def _conn_cache(conn: sqlite3.Connection) -> set[str]:
    """Per-connection cache of keyids whose chain has been verified back to
    a self-signed root within this session.

    Stored as an attribute on the connection object so it dies with the
    connection — avoids the stale-cache hazard of an id()-keyed module
    dict where a recycled object id picks up a previous conn's set.
    """
    cache = getattr(conn, _CACHE_ATTR, None)
    if cache is None:
        cache = set()
        try:
            setattr(conn, _CACHE_ATTR, cache)
        except (AttributeError, TypeError):
            # Some sqlite3.Connection wrappers refuse arbitrary attributes
            # (rare). Fall back to a fresh set every call — slow but safe.
            return set()
    return cache


def _count_self_signed_rows(conn: sqlite3.Connection) -> int:
    """Return how many rows have ``keyid == enrolled_by_keyid``.

    The validators table is bootstrapped by exactly one self-signed
    root row. If there are 2+ such rows, somebody planted an alternate
    root (or two simultaneous bootstraps slipped past the BEGIN
    IMMEDIATE guard) and trust in the table is forfeit.
    """
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM validators "
        "WHERE keyid = enrolled_by_keyid"
    ).fetchone()
    return int(row["n"] if row else 0)


def _verify_chain(conn: sqlite3.Connection, keyid: str) -> bool:
    """Walk ``enrolled_by_keyid`` back to a self-signed root, verifying
    each signature against the parent's persisted pubkey.

    Returns True iff every link in the chain (a) exists in the validators
    table, (b) has a well-formed enrollment envelope, (c) whose signature
    matches the parent's pubkey, and (d) terminates in a row where
    ``keyid == enrolled_by_keyid`` (the self-signed root), AND (e) that
    self-signed root is the unique such row in the table (singleton-root
    invariant — see :func:`_count_self_signed_rows`).

    A tampered row (manual sqlite INSERT with a fabricated parent, a
    bogus envelope, or an alternate self-signed root) fails one of these
    checks and breaks the chain.

    The walk is capped at :data:`_MAX_CHAIN_DEPTH` to defend against
    DoS from a planted long fake chain.
    """
    # Singleton-root: if two self-signed rows exist, the table has been
    # tampered (or two bootstraps raced past BEGIN IMMEDIATE, which
    # shouldn't happen but isn't strictly enforced by SQLite). Refuse
    # to trust any chain.
    if _count_self_signed_rows(conn) != 1:
        return False

    cache = _conn_cache(conn)
    if keyid in cache:
        return True

    path: list[str] = []
    current = keyid
    seen: set[str] = set()
    depth = 0
    while True:
        if depth >= _MAX_CHAIN_DEPTH:
            return False  # depth cap — refuse to walk pathological chains
        depth += 1
        if current in seen:
            return False  # cycle — not a tree rooted at a self-signed entry
        seen.add(current)
        path.append(current)
        row = get_validator(conn, current)
        if row is None:
            return False
        try:
            parent_pem = base64.standard_b64decode(row["pubkey_pem"])
        except (ValueError, TypeError):
            return False
        # Self-signed root terminates the walk.
        if row["enrolled_by_keyid"] == row["keyid"]:
            # The root's envelope must verify under its OWN pubkey.
            if not verify_enrollment(row, parent_pem):
                return False
            break
        # Otherwise the row's envelope must verify under the PARENT's pubkey.
        parent_row = get_validator(conn, row["enrolled_by_keyid"])
        if parent_row is None:
            return False
        try:
            parent_pubkey_pem = base64.standard_b64decode(parent_row["pubkey_pem"])
        except (ValueError, TypeError):
            return False
        if not verify_enrollment(row, parent_pubkey_pem):
            return False
        current = row["enrolled_by_keyid"]

    # Whole chain verified — cache every link.
    cache.update(path)
    return True


def is_enrolled(conn: sqlite3.Connection, keyid: str) -> bool:
    """Return True if *keyid* is enrolled AND its chain verifies back to a
    self-signed root.

    A row whose envelope doesn't verify against its parent (e.g. manual
    sqlite INSERT with a fabricated parent or a bogus envelope) is NOT
    considered enrolled. SQLite doesn't enforce the logical foreign key
    on ``enrolled_by_keyid`` — the chain walk is what gives the
    validators table its trust property.

    Results are cached per-connection so repeated calls during a session
    don't re-walk.
    """
    row = conn.execute(
        "SELECT 1 FROM validators WHERE keyid = ? LIMIT 1", (keyid,),
    ).fetchone()
    if row is None:
        return False
    return _verify_chain(conn, keyid)


def get_validator(conn: sqlite3.Connection, keyid: str) -> Optional[dict]:
    """Return the validator row for *keyid*, or None if not enrolled."""
    row = conn.execute(
        "SELECT keyid, pubkey_pem, identity, enrolled_at, "
        "enrolled_by_keyid, enrollment_envelope "
        "FROM validators WHERE keyid = ?",
        (keyid,),
    ).fetchone()
    return dict(row) if row else None


def list_validators(conn: sqlite3.Connection) -> list[dict]:
    """Return all enrolled validators ordered by enrollment time (asc)."""
    rows = conn.execute(
        "SELECT keyid, pubkey_pem, identity, enrolled_at, "
        "enrolled_by_keyid, enrollment_envelope "
        "FROM validators ORDER BY enrolled_at"
    ).fetchall()
    return [dict(r) for r in rows]


def count_validators(conn: sqlite3.Connection) -> int:
    """Return the number of enrolled validators."""
    row = conn.execute("SELECT COUNT(*) AS n FROM validators").fetchone()
    return int(row["n"] if row else 0)


# ---------------------------------------------------------------------------
# Write paths
# ---------------------------------------------------------------------------

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def auto_enroll_root(
    conn: sqlite3.Connection,
    signer,
    identity: str,
) -> Optional[dict]:
    """Enroll *signer* as the root validator if no validators exist yet.

    Idempotent: returns the existing root row (or any row for the
    signer's keyid) without modification if either is already present.
    Returns the newly-inserted row on first call.

    The root enrollment is self-signed: ``enrolled_by_keyid == keyid``.

    Concurrency
    -----------
    Two simultaneous processes opening a fresh ``graph.db`` with
    DIFFERENT keys could each pass a naive ``count == 0`` check and
    both insert a self-signed root — keyid is the PK, so distinct keys
    don't collide and you'd end up with two roots. The check + insert
    therefore runs inside ``BEGIN IMMEDIATE``: SQLite blocks the second
    writer until the first commits, after which the second's re-check
    sees count == 1 and bails out.

    First-time root enrollment also emits a ``UserWarning`` with the
    keyid fingerprint so an operator who opened the project with the
    wrong key has a chance to notice before the now-immutable root is
    cemented.
    """
    if signer is None:
        return None

    identity = _validate_identity(identity)
    keyid = _signing.public_key_id(signer.public_key())

    # Fast path: already enrolled or root exists under a different keyid.
    existing = get_validator(conn, keyid)
    if existing is not None:
        return existing
    if count_validators(conn) > 0:
        return None

    pubkey_pem = _signing.public_key_to_pem(signer.public_key())
    pem_b64 = base64.standard_b64encode(pubkey_pem).decode("ascii")
    now = _utcnow_iso()

    enrollment = {
        "keyid": keyid,
        "pubkey_pem": pem_b64,
        "identity": identity,
        "enrolled_at": now,
        "enrolled_by_keyid": keyid,  # self-signed root
    }
    envelope = _signing.sign_validator_enrollment(enrollment, signer)
    envelope_json = json.dumps(envelope, sort_keys=True, separators=(",", ":"))

    # Atomic check + insert. BEGIN IMMEDIATE acquires the write lock
    # immediately, so two processes that both raced past the fast-path
    # check above serialize here.
    try:
        conn.execute("BEGIN IMMEDIATE")
        # Re-check inside the locked transaction.
        existing_in_txn = get_validator(conn, keyid)
        if existing_in_txn is not None:
            conn.execute("COMMIT")
            return existing_in_txn
        if count_validators(conn) > 0:
            conn.execute("COMMIT")
            return None
        conn.execute(
            "INSERT INTO validators "
            "(keyid, pubkey_pem, identity, enrolled_at, "
            " enrolled_by_keyid, enrollment_envelope) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (keyid, pem_b64, identity, now, keyid, envelope_json),
        )
        conn.execute("COMMIT")
    except sqlite3.IntegrityError:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        return get_validator(conn, keyid)
    except sqlite3.OperationalError:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        return get_validator(conn, keyid)

    import warnings
    # stacklevel=4 points at the user's mareforma.open(...) call site.
    # Chain: warn → auto_enroll_root → EpistemicGraph.__init__ →
    # mareforma.open → user (1 → 2 → 3 → 4).
    warnings.warn(
        f"Enrolled key {keyid[:12]}… ({identity!r}) as root validator on "
        f"this project. This is silent and irrevocable in v0.3.0 — if you "
        "opened the graph with the wrong key, fix it before any further "
        "validate() calls.",
        stacklevel=4,
    )

    return get_validator(conn, keyid)


def enroll_validator(
    conn: sqlite3.Connection,
    parent_signer,
    new_pubkey_pem: bytes,
    identity: str,
) -> dict:
    """Add a new validator, signed by *parent_signer*.

    The parent must already be enrolled. Returns the new validator row.

    Raises
    ------
    ValidatorNotEnrolledError
        If the parent signer is not currently enrolled as a validator.
    ValidatorAlreadyEnrolledError
        If the new key is already enrolled.
    """
    identity = _validate_identity(identity)
    parent_keyid = _signing.public_key_id(parent_signer.public_key())
    if not is_enrolled(conn, parent_keyid):
        raise ValidatorNotEnrolledError(
            f"Parent key {parent_keyid[:12]}… is not enrolled. Only an "
            "already-enrolled validator can enroll new ones. The first "
            "key opened against a fresh graph.db auto-enrolls as the root."
        )

    new_pub = _signing.public_key_from_pem(new_pubkey_pem)
    new_keyid = _signing.public_key_id(new_pub)
    if is_enrolled(conn, new_keyid):
        raise ValidatorAlreadyEnrolledError(
            f"Key {new_keyid[:12]}… is already enrolled."
        )
    # is_enrolled returned False but the row might still exist with a
    # broken chain (e.g. tampered envelope or singleton-root violated).
    # Catching that here surfaces the inconsistency as a typed error
    # instead of a raw sqlite3.IntegrityError from the INSERT below.
    if get_validator(conn, new_keyid) is not None:
        raise ValidatorAlreadyEnrolledError(
            f"Key {new_keyid[:12]}… already has a row in the validators "
            "table, but its chain does not verify back to a self-signed "
            "root. The table appears tampered — investigate before "
            "enrolling further keys."
        )

    pem_b64 = base64.standard_b64encode(new_pubkey_pem).decode("ascii")
    now = _utcnow_iso()
    enrollment = {
        "keyid": new_keyid,
        "pubkey_pem": pem_b64,
        "identity": identity,
        "enrolled_at": now,
        "enrolled_by_keyid": parent_keyid,
    }
    envelope = _signing.sign_validator_enrollment(enrollment, parent_signer)
    envelope_json = json.dumps(envelope, sort_keys=True, separators=(",", ":"))

    conn.execute(
        "INSERT INTO validators "
        "(keyid, pubkey_pem, identity, enrolled_at, "
        " enrolled_by_keyid, enrollment_envelope) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (new_keyid, pem_b64, identity, now, parent_keyid, envelope_json),
    )
    conn.commit()

    row = get_validator(conn, new_keyid)
    assert row is not None  # we just inserted it
    return row


# ---------------------------------------------------------------------------
# Verification (post-insert)
# ---------------------------------------------------------------------------

def verify_enrollment(validator_row: dict, parent_pubkey_pem: bytes) -> bool:
    """Verify a single enrollment's signature against the parent's pubkey
    AND that every signed-payload field matches the persisted row.

    Returns True iff the row's ``enrollment_envelope`` is a well-formed
    DSSE-style envelope, the signature matches the parent's public key,
    AND every field in the signed payload (keyid, pubkey_pem, identity,
    enrolled_at, enrolled_by_keyid) equals the corresponding row column.

    Binding all fields — not just ``keyid`` — gives defense in depth:
    an attacker who swaps ``identity`` or ``pubkey_pem`` in the row
    breaks verification even if they could somehow reuse a legitimate
    envelope.
    """
    try:
        envelope = json.loads(validator_row["enrollment_envelope"])
    except (json.JSONDecodeError, TypeError, KeyError):
        return False
    try:
        parent_pub = _signing.public_key_from_pem(parent_pubkey_pem)
    except _signing.SigningError:
        return False
    try:
        if not _signing.verify_envelope(
            envelope, parent_pub,
            expected_payload_type=_signing._PAYLOAD_TYPE_VALIDATOR_ENROLLMENT,
        ):
            return False
        payload = _signing.envelope_payload(envelope)
    except _signing.InvalidEnvelopeError:
        return False

    for field in ("keyid", "pubkey_pem", "identity",
                  "enrolled_at", "enrolled_by_keyid"):
        if payload.get(field) != validator_row.get(field):
            return False
    return True
