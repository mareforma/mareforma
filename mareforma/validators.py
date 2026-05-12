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


# ---------------------------------------------------------------------------
# Read paths
# ---------------------------------------------------------------------------

def is_enrolled(conn: sqlite3.Connection, keyid: str) -> bool:
    """Return True if *keyid* is enrolled as a validator on this project."""
    row = conn.execute(
        "SELECT 1 FROM validators WHERE keyid = ? LIMIT 1", (keyid,),
    ).fetchone()
    return row is not None


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
    """
    if signer is None:
        return None

    keyid = _signing.public_key_id(signer.public_key())
    existing = get_validator(conn, keyid)
    if existing is not None:
        return existing
    if count_validators(conn) > 0:
        # A different key is already enrolled; this signer isn't the root.
        # Caller should use enroll_validator() with that parent's signer.
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

    try:
        conn.execute(
            "INSERT INTO validators "
            "(keyid, pubkey_pem, identity, enrolled_at, "
            " enrolled_by_keyid, enrollment_envelope) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (keyid, pem_b64, identity, now, keyid, envelope_json),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        # Concurrent first-open race; another process won. Re-read.
        return get_validator(conn, keyid)

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
    """Verify a single enrollment's signature against the parent's pubkey.

    Returns True iff the row's ``enrollment_envelope`` is a well-formed
    DSSE-style envelope, the signature matches the parent's public key,
    and the decoded payload's ``keyid`` equals the row's ``keyid``.
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
    return payload.get("keyid") == validator_row["keyid"]
