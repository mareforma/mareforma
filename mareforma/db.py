"""
db.py — SQLite-backed epistemic graph for mareforma.

Tables
------
  claims : explicit scientific assertions with provenance

Schema version
--------------
  Stored in PRAGMA user_version. Current: 1.
  Version 0 → fresh db, full schema applied, user_version=1.
  Version 1 → ready to use.
  Any other version → DatabaseError — delete graph.db to start fresh.

Connection lifecycle
--------------------
  Use open_db(root) to get a connection. Close when done.

Support levels — graph-derived trust signal
-------------------------------------------
  PRELIMINARY  : one agent claimed it
  REPLICATED   : ≥2 agents with different generated_by share the same
                 upstream claim in supports[] (auto-detected at INSERT)
  ESTABLISHED  : explicit human validation via validate_claim() only
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_FILENAME = "graph.db"
_SCHEMA_VERSION = 1

VALID_STATUSES = ("open", "contested", "retracted")

VALID_CLASSIFICATIONS = ("INFERRED", "ANALYTICAL", "DERIVED")

VALID_SUPPORT_LEVELS = ("PRELIMINARY", "REPLICATED", "ESTABLISHED")

# Maps min_support value to the set of levels that satisfy it.
_SUPPORT_LEVEL_TIERS: dict[str, tuple[str, ...]] = {
    "PRELIMINARY": ("PRELIMINARY", "REPLICATED", "ESTABLISHED"),
    "REPLICATED":  ("REPLICATED", "ESTABLISHED"),
    "ESTABLISHED": ("ESTABLISHED",),
}

_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS claims (
    claim_id        TEXT PRIMARY KEY,
    text            TEXT NOT NULL,
    classification  TEXT NOT NULL DEFAULT 'INFERRED'
                        CHECK (classification IN ('INFERRED', 'ANALYTICAL', 'DERIVED')),
    support_level   TEXT NOT NULL DEFAULT 'PRELIMINARY'
                        CHECK (support_level IN ('PRELIMINARY', 'REPLICATED', 'ESTABLISHED')),
    idempotency_key TEXT,
    validated_by    TEXT,
    validated_at    TEXT,
    status          TEXT NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open', 'contested', 'retracted')),
    source_name     TEXT,
    generated_by    TEXT NOT NULL DEFAULT 'agent',
    supports_json   TEXT NOT NULL DEFAULT '[]',
    contradicts_json TEXT NOT NULL DEFAULT '[]',
    comparison_summary TEXT,
    branch_id       TEXT NOT NULL DEFAULT 'main',
    unresolved      INTEGER NOT NULL DEFAULT 0
                        CHECK (unresolved IN (0, 1)),
    signature_bundle TEXT,
    transparency_logged INTEGER NOT NULL DEFAULT 1
                        CHECK (transparency_logged IN (0, 1)),
    validation_signature TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_claims_status
    ON claims(status);
CREATE INDEX IF NOT EXISTS idx_claims_source
    ON claims(source_name);
CREATE INDEX IF NOT EXISTS idx_claims_generated_by
    ON claims(generated_by);
CREATE INDEX IF NOT EXISTS idx_claims_support_level
    ON claims(support_level);
CREATE INDEX IF NOT EXISTS idx_claims_unresolved
    ON claims(unresolved);
CREATE INDEX IF NOT EXISTS idx_claims_transparency_logged
    ON claims(transparency_logged);
CREATE UNIQUE INDEX IF NOT EXISTS idx_claims_idempotency_key
    ON claims(idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS doi_cache (
    doi              TEXT PRIMARY KEY,
    resolved         INTEGER NOT NULL CHECK (resolved IN (0, 1)),
    registry         TEXT,
    last_checked_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS validators (
    keyid                TEXT PRIMARY KEY,
    pubkey_pem           TEXT NOT NULL,
    identity             TEXT NOT NULL,
    enrolled_at          TEXT NOT NULL,
    enrolled_by_keyid    TEXT NOT NULL,
    enrollment_envelope  TEXT NOT NULL
);
"""


# Explicit column list — avoids SELECT * coupling to schema changes.
# Source of truth for the column-presence check in open_db().
_CLAIM_COLUMNS = (
    "claim_id", "text", "classification", "support_level",
    "idempotency_key", "validated_by", "validated_at",
    "status", "source_name", "generated_by",
    "supports_json", "contradicts_json",
    "comparison_summary", "branch_id", "unresolved",
    "signature_bundle", "transparency_logged",
    "validation_signature",
    "created_at", "updated_at",
)
_CLAIM_SELECT = ", ".join(_CLAIM_COLUMNS)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class MareformaError(Exception):
    """Base exception for all mareforma errors."""


class DatabaseError(MareformaError):
    """Raised when a graph.db operation fails."""


class ClaimNotFoundError(MareformaError):
    """Raised when a claim lookup finds no matching record."""


class SignedClaimImmutableError(MareformaError):
    """Raised when `update_claim` is asked to mutate a signed-surface field.

    Once a claim has a signature attached, mutating any field that was part
    of the signed payload (``text``, ``supports``, ``contradicts``,
    ``classification``, ``generated_by``, ``source_name``) would invalidate
    the signature without surfacing the change. To revise a signed claim,
    retract the old one (``status='retracted'``) and assert a new one that
    cites the old via ``contradicts=[<old_claim_id>]``.
    """


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def _db_path(root: Path) -> Path:
    return root / ".mareforma" / DB_FILENAME


def open_db(root: Path) -> sqlite3.Connection:
    """Open (and initialise if needed) the graph database.

    Returns an open sqlite3.Connection with row_factory set to
    sqlite3.Row for dict-like access.

    Schema validation
    -----------------
    Fresh db (user_version=0): full schema applied, user_version set to 1.

    Initialised db (user_version=1): claims table must have every column
    in ``_CLAIM_COLUMNS``. Missing columns raise DatabaseError instructing
    the user to delete graph.db. ``_CLAIM_COLUMNS`` is the source of truth
    for what the schema must contain.

    Raises
    ------
    DatabaseError
        On SQLite errors or schema drift (missing columns).
    """
    path = _db_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        conn = sqlite3.connect(str(path), check_same_thread=False)
        conn.row_factory = sqlite3.Row

        version = conn.execute("PRAGMA user_version").fetchone()[0]

        if version == 0:
            conn.executescript(_SCHEMA_SQL)
            conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            conn.commit()
            return conn

        # Initialised db — validate the schema by exact column-set match.
        # Catching extras as well as missing columns means a partially-migrated
        # or hand-edited claims table fails loudly instead of silently passing
        # through code that assumes _CLAIM_COLUMNS is exhaustive.
        existing_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(claims)").fetchall()
        }
        expected_cols = set(_CLAIM_COLUMNS)
        if existing_cols != expected_cols:
            missing = expected_cols - existing_cols
            extra = existing_cols - expected_cols
            conn.close()

            # Extras-only is the downgrade case: the db was written by a
            # newer mareforma. Direct the user to upgrade rather than to
            # delete — claims.toml may not be a faithful backup for columns
            # the older version does not understand.
            if extra and not missing:
                raise DatabaseError(
                    f"graph.db was created by a newer mareforma version "
                    f"(extra columns: {sorted(extra)}). Upgrade the mareforma "
                    "package or back up claims.toml before downgrading."
                )

            parts: list[str] = []
            if missing:
                parts.append(f"missing: {sorted(missing)}")
            if extra:
                parts.append(f"unexpected: {sorted(extra)}")
            raise DatabaseError(
                f"graph.db schema mismatch ({'; '.join(parts)}). "
                "Delete .mareforma/graph.db to start fresh — "
                "your claims are backed up in claims.toml."
            )
        return conn

    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Could not open database at {path}: {exc}") from exc


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_status(status: str) -> None:
    """Raise ValueError if *status* is not a recognised claim status."""
    if status not in VALID_STATUSES:
        allowed = ", ".join(VALID_STATUSES)
        raise ValueError(
            f"Unknown claim status '{status}'. Use one of: {allowed}"
        )


# ---------------------------------------------------------------------------
# Claims
# ---------------------------------------------------------------------------

def add_claim(
    conn: sqlite3.Connection,
    root: Path,
    text: str,
    *,
    classification: str = "INFERRED",
    idempotency_key: str | None = None,
    supports: list[str] | None = None,
    contradicts: list[str] | None = None,
    generated_by: str = "agent",
    source_name: str | None = None,
    status: str = "open",
    unresolved: bool = False,
    signer: "object | None" = None,
    rekor_url: str | None = None,
    require_rekor: bool = False,
) -> str:
    """Insert a new claim and return its claim_id.

    Returns the existing claim_id without inserting if idempotency_key
    already exists. After insert, checks for REPLICATED: if ≥2 claims share
    the same upstream claim_id in supports[] with different generated_by,
    all are promoted to support_level='REPLICATED'.

    Parameters
    ----------
    classification:
        'INFERRED' | 'ANALYTICAL' | 'DERIVED'
    idempotency_key:
        Retry-safe writes — same key returns the same claim_id.
    supports:
        Upstream claim_ids or DOIs this claim is grounded in.
    contradicts:
        Claim_ids or DOIs this claim contests.
    generated_by:
        Agent or human identifier.
    source_name:
        Data source this claim derives from.
    status:
        Editorial status: 'open' | 'contested' | 'retracted'
    unresolved:
        True if any DOI in supports[]/contradicts[] failed to resolve.
        Unresolved claims are ineligible for REPLICATED promotion.
    signer:
        Optional Ed25519 private key. When provided, the claim is signed
        before INSERT and the signature envelope is persisted to the
        ``signature_bundle`` column. ``None`` skips signing.
    rekor_url:
        When set, every signed claim is submitted to the Rekor
        transparency log at this URL. Success augments the signature
        bundle with the log entry coordinates and sets
        ``transparency_logged=1``. Failure persists ``transparency_logged=0``,
        blocking REPLICATED promotion until
        :meth:`EpistemicGraph.refresh_unsigned` retries.
    require_rekor:
        When True, raise :class:`SigningError` if the initial Rekor
        submission fails. Use for production high-assurance flows.

    Raises
    ------
    ValueError
        If classification or status are invalid.
    SigningError
        If ``require_rekor=True`` and the Rekor submission fails.
    """
    if not text or not text.strip():
        raise ValueError("Claim text cannot be empty.")
    if classification not in VALID_CLASSIFICATIONS:
        raise ValueError(
            f"Unknown classification '{classification}'. "
            f"Use one of: {', '.join(VALID_CLASSIFICATIONS)}"
        )
    validate_status(status)

    # Idempotency check — return existing claim_id if key already present.
    if idempotency_key is not None:
        try:
            row = conn.execute(
                "SELECT claim_id FROM claims WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row:
                return row["claim_id"]
        except sqlite3.OperationalError as exc:
            raise DatabaseError(f"Idempotency check failed: {exc}") from exc

    claim_id = str(uuid.uuid4())
    now = _now()
    supports_json = json.dumps(supports or [])
    contradicts_json = json.dumps(contradicts or [])

    # Sign the claim if a signer was supplied. The signature is bound to the
    # claim_id + canonical fields + created_at, so any later tamper (text edit,
    # support reattribution) breaks verification.
    signature_bundle: str | None = None
    envelope: dict | None = None
    if signer is not None:
        from mareforma import signing as _signing
        envelope = _signing.sign_claim(
            {
                "claim_id": claim_id,
                "text": text.strip(),
                "classification": classification,
                "generated_by": generated_by,
                "supports": supports or [],
                "contradicts": contradicts or [],
                "source_name": source_name,
                "created_at": now,
            },
            signer,
        )
        signature_bundle = json.dumps(envelope, sort_keys=True, separators=(",", ":"))

    # ``transparency_logged`` defaults to 1 (ready). We flip it to 0 only when
    # Rekor is enabled AND we have something to submit — the row then waits
    # for either a successful submission below or a refresh_unsigned() retry.
    rekor_enabled = rekor_url is not None and signer is not None and envelope is not None
    transparency_logged = 0 if rekor_enabled else 1

    try:
        conn.execute(
            """
            INSERT INTO claims
                (claim_id, text, classification, support_level, idempotency_key,
                 status, source_name, generated_by,
                 supports_json, contradicts_json, unresolved,
                 signature_bundle, transparency_logged,
                 created_at, updated_at)
            VALUES (?, ?, ?, 'PRELIMINARY', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim_id, text.strip(), classification, idempotency_key,
                status, source_name, generated_by,
                supports_json, contradicts_json, 1 if unresolved else 0,
                signature_bundle, transparency_logged,
                now, now,
            ),
        )
        conn.commit()
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        raise DatabaseError(f"Failed to add claim: {exc}") from exc

    # Attempt Rekor submission. On success, augment the envelope with the
    # log entry and flip transparency_logged → 1. On failure, leave the row
    # at transparency_logged=0 — REPLICATED is blocked until refresh_unsigned
    # succeeds.
    if rekor_enabled:
        from mareforma import signing as _signing
        logged, entry = _signing.submit_to_rekor(
            envelope, signer.public_key(), rekor_url=rekor_url,
        )
        if logged and entry is not None:
            augmented = _signing.attach_rekor_entry(envelope, entry)
            new_bundle = json.dumps(augmented, sort_keys=True, separators=(",", ":"))
            try:
                conn.execute(
                    "UPDATE claims SET signature_bundle = ?, "
                    "transparency_logged = 1, updated_at = ? "
                    "WHERE claim_id = ?",
                    (new_bundle, _now(), claim_id),
                )
                conn.commit()
                transparency_logged = 1
            except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
                # Rekor succeeded but the local UPDATE failed. The row stays
                # at transparency_logged=0; refresh_unsigned will re-submit
                # to Rekor and overwrite the bundle then. Warn loudly so
                # operators see the inconsistency.
                import warnings as _warnings
                _warnings.warn(
                    f"Claim {claim_id} was accepted by Rekor but the local "
                    f"UPDATE failed ({exc}). transparency_logged remains 0; "
                    "run EpistemicGraph.refresh_unsigned() to reconcile.",
                    stacklevel=2,
                )
        elif require_rekor:
            raise _signing.SigningError(
                f"Rekor submission to {rekor_url} failed and require_rekor=True. "
                "Claim was persisted with transparency_logged=0; call "
                "EpistemicGraph.refresh_unsigned() to retry."
            )

    # Check whether this claim triggers REPLICATED status on shared upstreams.
    # Unresolved DOIs OR pending transparency-log inclusion block eligibility.
    if not unresolved and transparency_logged == 1:
        _maybe_update_replicated(conn, claim_id, supports or [], generated_by)

    _backup_claims_toml(conn, root)
    return claim_id


def _maybe_update_replicated_unlocked(
    conn: sqlite3.Connection,
    new_claim_id: str,
    supports: list[str],
    generated_by: str,
) -> None:
    """REPLICATED-detection SQL without a commit — caller controls the txn.

    Used by ``mark_claim_resolved`` so the unresolved-flag clear and the
    REPLICATED promotion land in the same SQLite transaction.
    """
    if not supports:
        return
    placeholders = ",".join("?" * len(supports))
    rows = conn.execute(
        f"""
        SELECT DISTINCT c.claim_id, c.generated_by
        FROM claims c, json_each(c.supports_json) j
        WHERE j.value IN ({placeholders})
          AND c.claim_id != ?
          AND c.generated_by != ?
          AND c.support_level != 'ESTABLISHED'
          AND c.unresolved = 0
          AND c.transparency_logged = 1
        """,
        (*supports, new_claim_id, generated_by),
    ).fetchall()

    if not rows:
        return

    peer_ids = [r["claim_id"] for r in rows] + [new_claim_id]
    peer_placeholders = ",".join("?" * len(peer_ids))
    conn.execute(
        f"UPDATE claims SET support_level = 'REPLICATED', updated_at = ? "
        f"WHERE claim_id IN ({peer_placeholders})",
        (_now(), *peer_ids),
    )


def _maybe_update_replicated(
    conn: sqlite3.Connection,
    new_claim_id: str,
    supports: list[str],
    generated_by: str,
) -> None:
    """Promote claims to REPLICATED when convergence is detected.

    Convergence: ≥2 claims share the same upstream claim_id in their
    supports[] and have different generated_by values. Uses json_each()
    for correct JSON array element extraction (no fragile LIKE).

    Called immediately after a successful INSERT in add_claim().
    Failures are swallowed — convergence detection must not crash writes.
    """
    try:
        _maybe_update_replicated_unlocked(conn, new_claim_id, supports, generated_by)
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Convergence detection is best-effort — never crash a write.


def validate_claim(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
    *,
    validated_by: str | None = None,
    validation_signature: str | None = None,
) -> None:
    """Promote a REPLICATED claim to ESTABLISHED (human validation).

    Parameters
    ----------
    validation_signature:
        Optional JSON-encoded DSSE-style envelope binding (claim_id,
        validator_keyid, validated_at). Produced by
        :func:`mareforma.signing.sign_validation` and stored verbatim
        on the row so the validation event itself is independently
        verifiable (tampering with ``validated_by``/``validated_at``
        post-hoc is detectable).

    Raises
    ------
    ClaimNotFoundError
        If no claim with claim_id exists.
    ValueError
        If the claim's support_level is not 'REPLICATED'.
    """
    row = conn.execute(
        "SELECT support_level FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")
    if row["support_level"] != "REPLICATED":
        raise ValueError(
            f"Claim '{claim_id}' has support_level='{row['support_level']}'. "
            "Only REPLICATED claims can be promoted to ESTABLISHED."
        )
    now = _now()
    try:
        conn.execute(
            """
            UPDATE claims
            SET support_level = 'ESTABLISHED',
                validated_by = ?,
                validated_at = ?,
                validation_signature = ?,
                updated_at   = ?
            WHERE claim_id = ?
            """,
            (validated_by, now, validation_signature, now, claim_id),
        )
        conn.commit()
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        raise DatabaseError(f"Failed to validate claim '{claim_id}': {exc}") from exc
    _backup_claims_toml(conn, root)


def list_unresolved_claims(conn: sqlite3.Connection) -> list[dict]:
    """Return all claims currently marked unresolved=True."""
    rows = conn.execute(
        f"SELECT {_CLAIM_SELECT} FROM claims WHERE unresolved = 1 ORDER BY created_at"
    ).fetchall()
    return [dict(r) for r in rows]


def list_unlogged_claims(conn: sqlite3.Connection) -> list[dict]:
    """Return signed claims still awaiting Rekor inclusion.

    A claim is "unlogged" when ``signature_bundle`` is non-NULL but
    ``transparency_logged`` is 0. Unsigned claims are excluded — they have
    no envelope to submit.
    """
    rows = conn.execute(
        f"SELECT {_CLAIM_SELECT} FROM claims "
        "WHERE signature_bundle IS NOT NULL AND transparency_logged = 0 "
        "ORDER BY created_at"
    ).fetchall()
    return [dict(r) for r in rows]


def mark_claim_logged(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
    new_signature_bundle: str,
) -> None:
    """Mark a claim as transparency-log included and update its bundle.

    The bundle is rewritten with the Rekor entry attached (uuid + logIndex +
    integratedTime). The flag-flip and REPLICATED re-evaluation happen in a
    single transaction so a crash between them cannot strand a claim at
    PRELIMINARY despite ``transparency_logged=1``.

    Verification
    ------------
    Before writing, the supplied bundle is decoded and its payload's
    ``claim_id`` is checked against the row's ``claim_id``. A buggy caller
    that mixes up claim ids cannot silently write Alice's bundle onto
    Bob's row.

    Raises
    ------
    ClaimNotFoundError
        If no claim with claim_id exists.
    DatabaseError
        If the supplied bundle is malformed or its payload's claim_id does
        not match.
    """
    row = conn.execute(
        "SELECT supports_json, generated_by, unresolved "
        "FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")

    # Sanity-check that the supplied bundle actually belongs to this claim.
    from mareforma import signing as _signing
    try:
        envelope = json.loads(new_signature_bundle)
        payload = _signing.envelope_payload(envelope)
    except (json.JSONDecodeError, _signing.InvalidEnvelopeError) as exc:
        raise DatabaseError(
            f"mark_claim_logged given malformed bundle for {claim_id}: {exc}"
        ) from exc
    if payload.get("claim_id") != claim_id:
        raise DatabaseError(
            f"mark_claim_logged bundle's payload.claim_id "
            f"({payload.get('claim_id')!r}) does not match row {claim_id!r}."
        )

    supports = json.loads(row["supports_json"] or "[]")
    generated_by = row["generated_by"]
    unresolved = int(row["unresolved"] or 0)
    now = _now()

    try:
        with conn:
            conn.execute(
                "UPDATE claims SET signature_bundle = ?, "
                "transparency_logged = 1, updated_at = ? "
                "WHERE claim_id = ?",
                (new_signature_bundle, now, claim_id),
            )
            # Convergence detection is best-effort by design: a transient
            # lock error during the REPLICATED check must not roll back
            # the flag flip the operator just committed.
            if not unresolved:
                try:
                    _maybe_update_replicated_unlocked(
                        conn, claim_id, supports, generated_by,
                    )
                except sqlite3.OperationalError:
                    pass
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        raise DatabaseError(f"Failed to mark claim logged: {exc}") from exc

    _backup_claims_toml(conn, root)


def mark_claim_resolved(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
) -> None:
    """Clear the unresolved flag on a claim and re-check REPLICATED eligibility.

    The flag-clear and the REPLICATED promotion happen in the same SQLite
    transaction. A crash between them would otherwise leave the claim with
    ``unresolved=0`` but stuck at PRELIMINARY, even though a sibling claim
    is waiting on it for convergence.

    Raises
    ------
    ClaimNotFoundError
        If no claim with claim_id exists.
    """
    row = conn.execute(
        "SELECT supports_json, generated_by FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")

    supports = json.loads(row["supports_json"] or "[]")
    generated_by = row["generated_by"]
    now = _now()

    try:
        # ``with conn`` opens a transaction and commits on exit; on exception
        # it rolls back, leaving the claim in its prior unresolved=1 state.
        with conn:
            conn.execute(
                "UPDATE claims SET unresolved = 0, updated_at = ? WHERE claim_id = ?",
                (now, claim_id),
            )
            # Convergence detection is best-effort by design: a transient
            # lock or convergence-query failure must not roll back the
            # flag-clear (which is the actual user intent).
            try:
                _maybe_update_replicated_unlocked(
                    conn, claim_id, supports, generated_by,
                )
            except sqlite3.OperationalError:
                pass
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        raise DatabaseError(f"Failed to mark claim resolved: {exc}") from exc

    _backup_claims_toml(conn, root)


def update_claim(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
    *,
    status: str | None = None,
    text: str | None = None,
    supports: list[str] | None = None,
    contradicts: list[str] | None = None,
    comparison_summary: str | None = None,
) -> None:
    """Update fields on an existing claim.

    Signed claims are append-only across the signed surface. If the row
    carries a non-NULL ``signature_bundle``, this call refuses to mutate
    ``text`` / ``supports`` / ``contradicts`` — those fields are part of
    the signed payload and editing them would silently invalidate the
    signature while leaving ``transparency_logged=1`` and the Rekor entry
    in place. ``status`` and ``comparison_summary`` remain editable since
    they are not part of the signed payload.

    To revise a signed claim, retract it (``status='retracted'``) and
    assert a new one with ``contradicts=[<old_claim_id>]``.

    Raises
    ------
    ClaimNotFoundError
        If no claim with *claim_id* exists.
    ValueError
        If status is invalid.
    SignedClaimImmutableError
        If the claim is signed and the caller tries to mutate a signed-
        surface field.
    """
    existing = get_claim(conn, claim_id)
    if existing is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")

    # Refuse signed-surface mutations on signed claims. text/supports/
    # contradicts are the only signed-surface fields currently exposed by
    # update_claim's parameter list.
    if existing.get("signature_bundle") is not None:
        signed_field_changes: list[str] = []
        if text is not None and text.strip() != existing.get("text"):
            signed_field_changes.append("text")
        if supports is not None:
            old_supports = json.loads(existing.get("supports_json") or "[]")
            if list(supports) != old_supports:
                signed_field_changes.append("supports")
        if contradicts is not None:
            old_contradicts = json.loads(existing.get("contradicts_json") or "[]")
            if list(contradicts) != old_contradicts:
                signed_field_changes.append("contradicts")
        if signed_field_changes:
            raise SignedClaimImmutableError(
                f"Claim '{claim_id}' is signed; refused to mutate "
                f"{signed_field_changes!r}. To revise, retract this claim "
                "(status='retracted') and assert a new one with "
                "contradicts=[<this_id>]."
            )

    new_status = existing["status"]
    new_text = existing["text"]
    new_supports_json = existing.get("supports_json", "[]")
    new_contradicts_json = existing.get("contradicts_json", "[]")
    new_comparison_summary = existing.get("comparison_summary")
    new_unresolved = int(existing.get("unresolved") or 0)

    if status is not None:
        validate_status(status)
        new_status = status
    if text is not None:
        if not text.strip():
            raise ValueError("Claim text cannot be empty.")
        new_text = text.strip()
    if supports is not None:
        new_supports_json = json.dumps(supports)
    if contradicts is not None:
        new_contradicts_json = json.dumps(contradicts)
    if comparison_summary is not None:
        new_comparison_summary = comparison_summary

    # Re-resolve DOIs only when supports/contradicts actually change. Stale
    # `unresolved` flags would let a claim with a newly-added fake DOI reach
    # REPLICATED, or pin a claim as unresolved after its bad DOI is removed.
    # Diff-check against the prior JSON skips the hot path when callers pass
    # identical lists (e.g. when only `text` or `status` is being edited).
    old_supports_json = existing.get("supports_json") or "[]"
    old_contradicts_json = existing.get("contradicts_json") or "[]"
    old_unresolved = int(existing.get("unresolved") or 0)
    supports_changed = supports is not None and new_supports_json != old_supports_json
    contradicts_changed = (
        contradicts is not None and new_contradicts_json != old_contradicts_json
    )
    if supports_changed or contradicts_changed:
        from mareforma import doi_resolver as _doi
        all_refs = json.loads(new_supports_json) + json.loads(new_contradicts_json)
        dois = _doi.extract_dois(all_refs)
        if dois:
            results = _doi.resolve_dois_with_cache(conn, dois)
            new_unresolved = 0 if all(results.values()) else 1
        else:
            new_unresolved = 0

    # If the claim just became resolved (or supports changed while resolved),
    # we MUST re-evaluate REPLICATED. Otherwise a claim cured via update_claim
    # stays at PRELIMINARY even when a peer is already waiting for convergence.
    needs_replicated_check = (
        supports_changed
        and new_unresolved == 0
        and existing.get("support_level") != "ESTABLISHED"
    ) or (old_unresolved == 1 and new_unresolved == 0)

    try:
        # Wrap the UPDATE and (optional) convergence check in one txn so the
        # unresolved-flag transition and the REPLICATED promotion are atomic.
        with conn:
            conn.execute(
                """
                UPDATE claims
                SET text = ?, status = ?, supports_json = ?, contradicts_json = ?,
                    comparison_summary = ?, unresolved = ?, updated_at = ?
                WHERE claim_id = ?
                """,
                (
                    new_text, new_status,
                    new_supports_json, new_contradicts_json,
                    new_comparison_summary, new_unresolved, _now(), claim_id,
                ),
            )
            if needs_replicated_check:
                try:
                    new_supports = json.loads(new_supports_json)
                    _maybe_update_replicated_unlocked(
                        conn, claim_id, new_supports, existing["generated_by"],
                    )
                except sqlite3.OperationalError:
                    # Convergence detection is best-effort — never crash an update.
                    pass
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        raise DatabaseError(f"Failed to update claim '{claim_id}': {exc}") from exc

    _backup_claims_toml(conn, root)


def delete_claim(conn: sqlite3.Connection, root: Path, claim_id: str) -> None:
    """Delete a claim.

    Raises
    ------
    ClaimNotFoundError
        If no claim with *claim_id* exists.
    """
    if get_claim(conn, claim_id) is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")
    try:
        conn.execute("DELETE FROM claims WHERE claim_id = ?", (claim_id,))
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to delete claim '{claim_id}': {exc}") from exc

    _backup_claims_toml(conn, root)


def get_claim(conn: sqlite3.Connection, claim_id: str) -> dict | None:
    """Return a claim dict or None if not found."""
    try:
        row = conn.execute(
            f"SELECT {_CLAIM_SELECT} FROM claims WHERE claim_id = ?",
            (claim_id,),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to fetch claim '{claim_id}': {exc}") from exc
    return dict(row) if row else None


def list_claims(
    conn: sqlite3.Connection,
    *,
    status: str | None = None,
    source_name: str | None = None,
    generated_by: str | None = None,
) -> list[dict]:
    """Return all claims, optionally filtered.

    Uses an explicit column list (not SELECT *) to avoid coupling to schema changes.
    """
    conditions: list[str] = []
    params: list[Any] = []
    if status is not None:
        conditions.append("status = ?")
        params.append(status)
    if source_name is not None:
        conditions.append("source_name = ?")
        params.append(source_name)
    if generated_by is not None:
        conditions.append("generated_by = ?")
        params.append(generated_by)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    try:
        rows = conn.execute(
            f"SELECT {_CLAIM_SELECT} FROM claims {where} ORDER BY created_at DESC",
            params,
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to list claims: {exc}") from exc
    return [dict(row) for row in rows]


def delete_claims_by_generated_by(
    conn: sqlite3.Connection,
    root: Path,
    generated_by: str,
) -> int:
    """Delete all claims with the given generated_by tag.

    Returns the number of claims deleted.
    """
    try:
        rows = conn.execute(
            "SELECT claim_id FROM claims WHERE generated_by = ?",
            (generated_by,),
        ).fetchall()
        claim_ids = [r[0] for r in rows]
        if not claim_ids:
            return 0
        placeholders = ",".join("?" * len(claim_ids))
        conn.execute(
            f"DELETE FROM claims WHERE claim_id IN ({placeholders})", claim_ids
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to delete claims: {exc}") from exc

    _backup_claims_toml(conn, root)
    return len(claim_ids)


def query_claims(
    conn: sqlite3.Connection,
    *,
    limit: int = 10,
    text: str | None = None,
    min_support: str | None = None,
    classification: str | None = None,
) -> list[dict]:
    """Return claims ordered by support_level (desc) then recency (desc).

    Parameters
    ----------
    limit:
        Maximum number of claims to return. Default 10.
    text:
        Optional substring filter — case-insensitive LIKE match on claim text.
    min_support:
        Minimum support level: 'PRELIMINARY' | 'REPLICATED' | 'ESTABLISHED'.
    classification:
        Filter by classification: 'INFERRED' | 'ANALYTICAL' | 'DERIVED'.
    """
    conditions: list[str] = []
    params: list = []

    if text is not None:
        conditions.append("text LIKE ?")
        params.append(f"%{text}%")

    if min_support is not None:
        if min_support not in VALID_SUPPORT_LEVELS:
            raise ValueError(
                f"Unknown min_support '{min_support}'. "
                f"Use one of: {', '.join(VALID_SUPPORT_LEVELS)}"
            )
        tiers = _SUPPORT_LEVEL_TIERS[min_support]
        tier_placeholders = ",".join("?" * len(tiers))
        conditions.append(f"support_level IN ({tier_placeholders})")
        params.extend(tiers)

    if classification is not None:
        if classification not in VALID_CLASSIFICATIONS:
            raise ValueError(
                f"Unknown classification '{classification}'. "
                f"Use one of: {', '.join(VALID_CLASSIFICATIONS)}"
            )
        conditions.append("classification = ?")
        params.append(classification)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    try:
        rows = conn.execute(
            f"SELECT {_CLAIM_SELECT} FROM claims {where} "
            f"ORDER BY CASE support_level "
            f"WHEN 'ESTABLISHED' THEN 3 WHEN 'REPLICATED' THEN 2 ELSE 1 END DESC, "
            f"created_at DESC LIMIT ?",
            params,
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to query claims: {exc}") from exc
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _backup_claims_toml(conn: sqlite3.Connection, root: Path) -> None:
    """Write all claims to claims.toml in the project root.

    Called after every claim mutation (add, update, delete).
    Failure is non-fatal: a warning is printed but the exception is not raised.
    """
    try:
        import tomli_w

        claims = list_claims(conn)
        data: dict[str, Any] = {"claims": {}}
        for c in claims:
            supports = json.loads(c.get("supports_json", "[]") or "[]")
            contradicts = json.loads(c.get("contradicts_json", "[]") or "[]")
            entry: dict[str, Any] = {
                "text": c["text"],
                "classification": c.get("classification") or "INFERRED",
                "support_level": c.get("support_level") or "PRELIMINARY",
                "generated_by": c.get("generated_by", "agent"),
                "status": c["status"],
                "supports": supports,
                "contradicts": contradicts,
                "comparison_summary": c.get("comparison_summary") or "",
                "created_at": c["created_at"],
                "updated_at": c["updated_at"],
            }
            if c.get("source_name"):
                entry["source_name"] = c["source_name"]
            if c.get("validated_by"):
                entry["validated_by"] = c["validated_by"]
            if c.get("validated_at"):
                entry["validated_at"] = c["validated_at"]
            if c.get("unresolved"):
                entry["unresolved"] = True
            if c.get("signature_bundle"):
                entry["signature_bundle"] = c["signature_bundle"]
            # transparency_logged: only record when it deviates from the
            # default (1). A 0 means "signed but awaiting Rekor inclusion".
            if c.get("transparency_logged") == 0:
                entry["transparency_logged"] = False
            data["claims"][c["claim_id"]] = entry

        out = root / "claims.toml"
        out.write_bytes(tomli_w.dumps(data).encode("utf-8"))

    except Exception as exc:  # noqa: BLE001
        import warnings
        warnings.warn(f"claims.toml backup failed (claim is saved in graph.db): {exc}")
