"""
core.py: live-write path, queries, verdicts, and TOML backup.

Schema DDL in ``_schema_sql.py``; exceptions in ``errors.py``;
``restore()`` in ``restore.py``. Everything else stays here because
the threat-model locality is load-bearing: every defensive measure
names the threat it blocks, and the callers that must hold
``BEGIN IMMEDIATE`` together live in one buffer.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import sqlite3
import uuid
import warnings
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from .._atomic import atomic_write_bytes
from .._canonical import signed_value_matches
from ..doi_resolver import is_doi
from ._schema_sql import (  # noqa: F401
    _ADDITIVE_TABLES_SQL,
    _EXPECTED_TRIGGER_TABLES,
    _SCHEMA_CENSUS_SQL,
    _CLAIM_COLUMNS,
    _CLAIM_SELECT,
    _MANAGED_TRIGGERS,
    _POLICY_MARKER_TABLE,
    _SCHEMA_SQL,
    _UPGRADE_MARKER_TABLE,
    claims_rebuild_sql,
    _SIGNED_FIELDS_TRIGGER_NAME,
    _SIGNED_FIELDS_TRIGGER_SQL,
)
from .errors import (  # noqa: F401
    MareformaError,
    DatabaseError,
    ScanCeilingReached,
    ClaimNotFoundError,
    UnverifiedClaimError,
    SignedClaimImmutableError,
    IdempotencyConflictError,
    IllegalStateTransitionError,
    ChainIntegrityError,
    LLMValidatorPromotionError,
    SelfValidationError,
    EvidenceCitationError,
    InvalidValidationEnvelopeError,
    RestoreError,
    CycleDetectedError,
    GraphTooLargeError,
    ProjectPolicyError,
    VerdictIssuerError,
    FormatArtifactError,
)

_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_FILENAME = "graph.db"
_SCHEMA_VERSION = 2

# Hard cap on a single claim's ``text`` field. 100k chars covers any
# realistic scientific finding (≈ a 15k-word paragraph) and matches the
# truncation point in ``prompt_safety._MAX_FIELD_LEN`` so claim text
# never silently degrades when consumed by an LLM. A multi-MB claim is
# either a bug or a write-side DoS attempt; rejecting is the simpler
# defence than silently truncating.
_MAX_CLAIM_TEXT_LEN = 100_000

VALID_STATUSES = ("open", "contested", "retracted")

VALID_CLASSIFICATIONS = ("INFERRED", "ANALYTICAL", "DERIVED")




def _serialize_predicate_payload(payload: dict | None) -> str:
    """Serialize an adapter's structured predicate_payload for storage.

    Canonical JSON (sorted keys, NFC Unicode, no whitespace, ``allow_nan=False``)
    so the column round-trips byte-stably across writers. ``None`` becomes
    the empty string to match the column's ``DEFAULT ''`` so callers that
    never pass this kwarg write the same bytes they did before. The active
    signed envelope is the authoritative copy of the predicate body; this
    column is the queryable denormalisation.

    Raises :class:`TypeError` if payload is non-dict. Adapters MUST pass
    a JSON-object-shaped dict (the typed predicate body); passing a
    string, list, int, or other non-object JSON value would serialize
    successfully but break mareforma's "predicate body is a dict"
    contract that downstream consumers (eg. PROV-O exporter,
    role-attestation walker) assume.
    """
    if payload is None:
        return ""
    if not isinstance(payload, dict):
        raise TypeError(
            f"predicate_payload must be a dict (the typed predicate body), "
            f"got {type(payload).__name__}. Wrap non-dict values in a dict "
            "with a documented key, e.g. {'value': <your value>}."
        )
    from .._canonical import canonicalize
    return canonicalize(payload).decode("utf-8")


def _serialize_observed_grounding(record: dict | None) -> str | None:
    """Serialize the observed-grounding record for its queryable column.

    Canonical JSON so the column round-trips byte-stably and matches the same
    record bound into the signed predicate. ``None`` stays NULL, the column
    default, so a claim asserted without the observer writes exactly the bytes
    it did before this field existed. The signed envelope is authoritative; this
    column is the denormalisation the split measurement and the promotion gate
    read.
    """
    if record is None:
        return None
    if not isinstance(record, dict):
        raise TypeError(
            f"observed_grounding must be a dict (the computed verdict record), "
            f"got {type(record).__name__}."
        )
    from .._canonical import canonicalize
    return canonicalize(record).decode("utf-8")


_GROUNDING_ATTESTATION_FIELDS = (
    "claim_id",
    "statement_cid",
    "receipt_digest",
    "grounding",
)


def _grounding_attestation_pae(record: dict) -> bytes:
    """The DSSE PAE a grounding attestation is made and checked over.

    Its own payload type, so an attestation can never be read as the claim
    envelope it names, nor a claim envelope as an attestation.
    """
    from mareforma import signing as _signing
    return _signing.dsse_pae(
        _signing.PAYLOAD_TYPE_GROUNDING_ATTESTATION,
        _verdict_canonical_payload(_GROUNDING_ATTESTATION_FIELDS, record),
    )


def _observer_minted(record: "dict | None") -> bool:
    """True iff *record* is the observer's own, rather than a declaration.

    The two are told apart by what the write path leaves on them: a declaration
    is marked ``provenance: DECLARED`` and has its receipt digest stripped, so a
    record carrying a digest and no such mark is one the observer minted. Both
    halves are checked rather than either alone, because each is a field.
    """
    from mareforma.observe._verdict import DECLARED_PROVENANCE

    if not isinstance(record, dict):
        return False
    digest = record.get("receipt_digest")
    return (
        isinstance(digest, str)
        and bool(digest)
        and record.get("provenance") != DECLARED_PROVENANCE
    )


def _write_grounding_attestation(
    conn: sqlite3.Connection,
    *,
    claim_id: str,
    statement_cid: str,
    record: "dict | None",
    signer: "object | None",
    asserter_keyid: "str | None",
    created_at: str,
) -> None:
    """Record that the observer computed this claim's grounding verdict.

    Written only where the write path kept the observer's own record. A declared
    verdict gets none, and that absence is the signal a reader looks for.

    The axis is the one signal on a claim meant not to be the producer's word,
    and ``_attest_grounding`` is where the write path enforces that. Restore
    never passes through it: it takes ``observed_grounding`` straight from
    ``claims.toml``, so a neutralised record could be exported, edited, re-signed
    by the producer's own key and restored as GROUNDED. Re-running the check on
    restore is not available, because the register it reads is in-process and
    keyed on a receipt digest, so a fresh restore would strip the axis off every
    honest claim too. This carries what the write path knew into the file
    instead.

    What it buys is parity, not prevention. The observer runs inside the
    producer's process and the producer holds the key, so a producer determined
    enough to re-sign a claim can build one of these as well. It closes the
    ordinary act, editing the axis and nothing else, and every surface that
    reports it says so.

    Silent when there is no signer: an unsigned claim carries no signature worth
    attesting beside.
    """
    if signer is None or not asserter_keyid or not _observer_minted(record):
        return
    payload = {
        "claim_id": claim_id,
        "statement_cid": statement_cid,
        "receipt_digest": record["receipt_digest"],
        "grounding": record.get("grounding"),
    }
    conn.execute(
        "INSERT INTO grounding_attestations(claim_id, statement_cid, "
        "receipt_digest, grounding, signer_keyid, signature, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            claim_id, statement_cid, payload["receipt_digest"],
            payload["grounding"], asserter_keyid,
            signer.sign(_grounding_attestation_pae(payload)), created_at,
        ),
    )


def grounding_attestation_state(
    conn: sqlite3.Connection, claim_id: str,
) -> str:
    """Whether this claim's grounding axis is attested, in one word.

    ``"attested"`` when a row is present, binds this claim's current statement,
    names the axis the claim stores, and verifies under the key that asserted
    it. ``"unattested"`` when no row is there, which is the honest state for a
    declared verdict and for every claim written before the table existed.
    ``"broken"`` when a row is present and fails any of those, which is a
    stronger signal than absence and must never be folded into it.

    Never raises. A graph too damaged to answer from reports ``"broken"``
    rather than taking a read down, and never ``"attested"``.
    """
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    try:
        row = conn.execute(
            "SELECT statement_cid, receipt_digest, grounding, signer_keyid, "
            "signature FROM grounding_attestations WHERE claim_id = ?",
            (claim_id,),
        ).fetchone()
        if row is None:
            return "unattested"
        claim = conn.execute(
            "SELECT statement_cid, observed_grounding, asserter_keyid FROM "
            "claims WHERE claim_id = ?", (claim_id,),
        ).fetchone()
        if claim is None or claim["statement_cid"] != row["statement_cid"]:
            return "broken"
        stored = _json_object(claim["observed_grounding"]) or {}
        if stored.get("grounding") != row["grounding"]:
            return "broken"
        if stored.get("receipt_digest") != row["receipt_digest"]:
            return "broken"
        # The asserting key, and only that one. An attestation is the producer's
        # word that their own observer computed this axis, so a signature from
        # any other enrolled key attests nothing about it. Without this the read
        # surfaces printed "the observer that computed this verdict attested it
        # under the asserting key" for an attestation re-signed by a peer that
        # asserted nothing: a false attribution rather than a false axis, since
        # a peer cannot create the axis, but the sentence was still untrue.
        if row["signer_keyid"] != claim["asserter_keyid"]:
            return "broken"
        # The enrolment chain walk, the same bar the verdict chain applies to
        # its own signers. A bare row lookup accepted a validator row that does
        # not chain back to the root, so one check refused a key the other
        # accepted, in the same file, two functions apart.
        signer_row = _validators.get_validator(conn, row["signer_keyid"])
        if signer_row is None or not _validators.is_enrolled(
            conn, row["signer_keyid"],
        ):
            return "broken"
        pem = base64.standard_b64decode(signer_row["pubkey_pem"])
        _signing.public_key_from_pem(pem).verify(
            row["signature"],
            _grounding_attestation_pae({
                "claim_id": claim_id,
                "statement_cid": row["statement_cid"],
                "receipt_digest": row["receipt_digest"],
                "grounding": row["grounding"],
            }),
        )
    except Exception:
        return "broken"
    return "attested"


def _observed_grounding_promotes(stored: str | None) -> bool:
    """Whether a stored observed-grounding column permits support-level promotion.

    A NULL column (every claim asserted without the observer) is unaffected and
    promotes as before. A recorded verdict promotes only when it is GROUNDED;
    UNGROUNDED and OPAQUE never count toward promotion. Any non-NULL value that
    is not GROUNDED JSON (including an empty string) is non-promoting
    (fail-closed): a verdict we cannot read is not a GROUNDED one. This matches
    the peer-promotion SQL guard, which excludes a non-``json_valid`` column.
    """
    if stored is None:
        return True
    try:
        record = json.loads(stored)
        return record.get("grounding") == "GROUNDED"
    except (ValueError, TypeError, AttributeError):
        return False


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

class _GraphConnection(sqlite3.Connection):
    """A ``sqlite3.Connection`` that permits attribute storage.

    Stdlib ``sqlite3.Connection`` rejects arbitrary attribute assignment,
    which silently disabled the per-connection chain-verification cache in
    :func:`mareforma.validators._conn_cache`: ``setattr`` raised, the cache
    fell through to a fresh empty set on every call, and every
    ``is_enrolled`` re-walked the validator chain. A trivial subclass gains
    ``__dict__`` so the cache actually persists for the life of the
    connection and dies with it, no module-level ``id()``-keyed dict (which
    aliases recycled object ids) and no weakref (sqlite3 connections are not
    weak-referenceable).
    """


@contextmanager

def _policy_window(conn: sqlite3.Connection):
    """Open the project-policy marker for the statements inside the block.

    The one write that replaces the singleton policy row runs inside one, for
    the reason the promotion window exists: the guard on the table refuses an
    UPDATE that no mareforma writer opened, and the marker is a temp table, so
    it lives on this connection and dies with it. Nesting is not expected (only
    :func:`set_project_policy` opens one) so the block simply creates and drops.
    """
    conn.execute(
        f"CREATE TEMP TABLE IF NOT EXISTS {_POLICY_MARKER_TABLE} (id INTEGER)"
    )
    try:
        yield
    finally:
        # IF EXISTS: a ROLLBACK inside the block takes the temp table with it,
        # and the close still has to be idempotent.
        conn.execute(f"DROP TABLE IF EXISTS temp.{_POLICY_MARKER_TABLE}")


def _db_path(root: Path) -> Path:
    return root / ".mareforma" / DB_FILENAME


def _open_failure(
    path: Path, exc: sqlite3.Error, corrupt_remedy: str,
) -> DatabaseError:
    """Wrap a failed open, choosing the remedy that fits the cause.

    A file the process may not write and a file whose bytes are damaged both
    arrive here as ``sqlite3.Error``. Answering both with the corruption
    remedy tells an operator whose graph is intact to delete their only copy,
    so the permission case gets its own sentence and names no deletion.
    """
    if "readonly database" in str(exc):
        remedy = (
            "That is a file permission problem, not damage to the graph. "
            "Make the file writable, or copy the project somewhere writable "
            "and open it there."
        )
    else:
        remedy = corrupt_remedy
    return DatabaseError(f"Could not open database at {path}: {exc}. {remedy}")


def _ensure_supports_revision_row(conn: sqlite3.Connection) -> None:
    """Seed the supports_revision singleton when it is missing.

    ``INSERT OR IGNORE`` takes a write lock even when the row is already
    there, so running it on every open makes a read-only graph.db
    unopenable. The row is written once, on the fresh db and on the first
    open of a graph that predates the table.
    """
    if conn.execute("SELECT 1 FROM supports_revision WHERE id = 1").fetchone():
        return
    conn.execute("INSERT INTO supports_revision (id, revision) VALUES (1, 0)")
    conn.commit()


def _guards_seen(conn: sqlite3.Connection) -> "set[str]":
    """Every guard this graph is known to have carried, or an empty set.

    Tolerates the table being absent, which is not an error condition but the
    state of every file written before the store existed, and the state the
    census has to read from before it is allowed to create anything.
    """
    try:
        return {
            row[0] for row in conn.execute("SELECT name FROM schema_guards_seen")
        }
    except sqlite3.OperationalError:
        return set()


def _note_guards_seen(conn: sqlite3.Connection) -> None:
    """Add every expected guard the graph currently carries to its seen set.

    The seen set is what makes "this table was never built here" separable from
    "somebody took this table away". A guard is expected while its table is
    present, which is what keeps an older graph.db off the tamper report, but on
    its own that rule hands an attacker a way out: drop the table and its guards
    leave the expected set along with it, and the additive script rebuilds the
    table empty on the same open. Once a guard is in this set it stays expected
    whatever happens to its table.

    Called at the END of an open, after the repairs, and that is not the same
    moment as the census. The census has to look before anything heals or it
    sees a mended schema; this has to look after, or the guards an open just
    built are not recorded until the next one and a table dropped in between
    walks out through the gap it was closing.

    Monotone, and only ever written when it grows. It records what is present,
    never what is expected. A guard absent when this looks is not written down
    however much the census wanted it, so a drop cannot enrol itself as normal,
    and nothing here forgets: a graph cannot un-know a guard it once had.
    """
    conn.executescript(_SCHEMA_CENSUS_SQL)
    present = {
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        )
    }
    known = _guards_seen(conn)
    fresh = sorted((present & set(_EXPECTED_TRIGGER_TABLES)) - known)
    if not fresh:
        return
    now = _now()
    conn.executemany(
        "INSERT OR IGNORE INTO schema_guards_seen (name, first_seen) VALUES (?, ?)",
        [(name, now) for name in fresh],
    )
    conn.commit()


# Guards this release adds to a table that ALREADY existed. Held back from the
# expected set for one release, because the census looks before the reconciler
# creates them and every graph written before this release would otherwise
# report them missing on its first open, permanently.
#
# This release puts one guard on `claims`, a table every existing graph already
# has, so table-presence does not keep it off their reports. Declared here and
# removed in the release after, once every opened graph carries it in its seen
# set.
#
# Verified rather than assumed: adding one guard to `claims` and opening an
# existing graph once was measured turning a verified claim into UNVERIFIABLE,
# with no migration involved.
_GUARDS_INTRODUCED_THIS_RELEASE: "frozenset[str]" = frozenset({
    "claims_validation_is_terminal",
})


def _record_schema_census(conn: sqlite3.Connection) -> "tuple[str, ...]":
    """Record which write guards are absent, BEFORE anything recreates them.

    A dropped trigger is the one tamper the read path cannot infer afterwards,
    because two different repairs run on the way in and both are silent. The
    managed set is reconciled against ``sqlite_master`` on every open, and
    ``_ADDITIVE_TABLES_SQL`` re-runs its own ``CREATE TRIGGER IF NOT EXISTS``
    statements on every open as well. By the time a caller reads a claim, a
    guard that was missing when the file was opened is back, with nothing
    anywhere saying it had gone, and the deletes it permitted while it was down
    are already indistinguishable from rows that were never written.

    So this runs first and writes down what it saw. Ordering is the whole
    mechanism: called after the repair, it observes a healed schema and reports
    clean forever.

    Only a non-empty result is recorded, and only when it differs from the last
    record: an unconditional row per open would grow without bound on a
    long-lived process and bury the one observation that matters.

    A guard is expected when its table is here and this release did not just
    introduce it, or when this graph has carried that guard before. Three parts,
    and each closes something the others open.

    Table-presence keeps an older graph.db off the report:
    :data:`_EXPECTED_TRIGGER_TABLES` explains that the additive script builds
    nine tables on the way in, a file written before they existed has none of
    them, and measured against the flat set it would show every guard on those
    tables as absent on the very open that creates them.

    The seen set closes what table-presence would otherwise open, because a
    guard cannot outlive its table: dropping the table would take its guards out
    of the expected set, and the additive script rebuilds the table empty on the
    same open, so the rows would be gone with nothing said. A guard this graph
    has carried stays expected however its table is treated. Deleting every row
    of the seen set does not lower what is expected of a table that is still
    present, and the store's own guards refuse the delete anyway.

    What it does not close, and no single-file scheme can: the census table
    itself is droppable. ``DROP TABLE schema_census`` takes its own guards with
    it, the additive script rebuilds both empty on the next open, and every
    observation ever recorded is gone with the seen store and the guarded table
    untouched. That is cheaper than it was once described here, which said all
    three had to go; one does. The union over every record defends against a
    later open burying an earlier one, and not against the store being
    replaced. The record lives in the file the attacker is holding.
    What raises the cost is the second copy: the census rides in the backup, so
    a graph emptied this way disagrees with a claims.toml the attacker has to
    find and edit too.

    :data:`_GUARDS_INTRODUCED_THIS_RELEASE` closes the case both of the others
    miss, which is the ordinary shape of a schema release. A guard added to a
    table that ALREADY exists is expected of every graph written before it, on
    the first open, because the census looks before the reconciler creates it.
    The record is a union over every open and nothing here forgets, so one added
    guard would brand every claim in every existing graph as tampered,
    permanently, with no way to clear it. Declaring it holds it back for exactly
    one release, after which every graph that has been opened carries it in its
    seen set and the declaration can go.

    Returns the missing names so the caller can act on the open it happened on.
    """
    # Look before creating anything, including the census store itself. The
    # store's own no-delete guards are created by _SCHEMA_CENSUS_SQL, so running
    # that script first would heal a dropped store guard and then report a
    # healthy schema: exactly the blindness the ordering above exists to avoid,
    # aimed at the one table that is the only record left. Four statements
    # emptied the whole report that way, dropping the two store guards to get
    # past them and deleting the rows behind.
    live = conn.execute(
        "SELECT type, name, sql FROM sqlite_master "
        "WHERE type IN ('table', 'trigger')"
    ).fetchall()
    tables = {name for kind, name, _ in live if kind == "table"}
    # Present AND intact. A guard whose body has been replaced by a no-op is
    # not a guard, and comparing names alone reported it as healthy: the
    # reconciler further down this same open then repairs the text silently, so
    # the tamper healed with nothing written down, which is the one thing this
    # function exists to prevent. The comparison is the reconciler's own, so
    # the two cannot disagree about what a guard is.
    _wanted = dict(_MANAGED_TRIGGERS)
    triggers = {
        name for kind, name, sql in live
        if kind == "trigger" and (name not in _wanted or sql == _wanted[name])
    }
    seen = _guards_seen(conn)
    expected = {
        name for name, table in _EXPECTED_TRIGGER_TABLES.items()
        if (table in tables and name not in _GUARDS_INTRODUCED_THIS_RELEASE)
        or name in seen
    }
    missing = tuple(sorted(expected - triggers))

    conn.executescript(_SCHEMA_CENSUS_SQL)
    if not missing:
        return ()

    payload = json.dumps(list(missing))
    last = conn.execute(
        "SELECT missing FROM schema_census "
        "ORDER BY observed_at DESC, rowid DESC LIMIT 1"
    ).fetchone()
    if last is None or last[0] != payload:
        conn.execute(
            "INSERT INTO schema_census (observed_at, missing) VALUES (?, ?)",
            (_now(), payload),
        )
        # Commit here rather than leaning on the caller. The record would
        # otherwise survive only because the additive executescript further down
        # the open commits the transaction on its way past, and this function's
        # whole contract is that it can be moved to stay ahead of the repairs.
        # Moved, it would go on returning the right names and silently stop
        # writing them down.
        conn.commit()
    return missing


def schema_census_missing(conn: sqlite3.Connection) -> "tuple[str, ...]":
    """Every write guard any open has found absent, or ``()``.

    Read surfaces must consult this rather than re-deriving from
    ``sqlite_master``: the repairs described in :func:`_record_schema_census`
    have already run by then, so a live re-derivation answers "nothing is
    missing" on exactly the graph that was tampered with.

    The union of every record, not the most recent one. A guard that came back
    is not a guard that was never gone: the rows it let someone delete while it
    was down are still gone, and no later open can see that. Reporting only the
    latest census would let one subsequent open bury the observation, which is
    the same disappearance this function exists to prevent, one level up.
    """
    try:
        rows = conn.execute("SELECT missing FROM schema_census").fetchall()
    except sqlite3.OperationalError:
        return ()          # no census table: nothing was ever missing
    seen: set[str] = set()
    for row in rows:
        try:
            names = json.loads(row[0])
        except (ValueError, TypeError):
            continue
        # A list of names, and nothing else. `update` iterates whatever it is
        # given, so a stored JSON string became one reported guard per
        # character and a stored object became one per key. The writer only
        # ever stores a list, and since the census travels in the backup the
        # value can also arrive from a file, so the reader checks rather than
        # assumes.
        if not isinstance(names, list):
            continue
        seen.update(n for n in names if isinstance(n, str))
    return tuple(sorted(seen))


def _ensure_managed_triggers(conn: sqlite3.Connection) -> None:
    """Reconcile every trigger in the schema with its wanted text.

    _SCHEMA_SQL never runs again on an initialised db, so a trigger whose
    definition changed shape reaches an existing graph only from here, and a
    trigger somebody dropped comes back only from here. Doing that as an
    unconditional drop-and-recreate would open a window on every single open()
    in which another connection sees a table with no guard on it, which is
    exactly the substitution the triggers exist to refuse. Compare against
    sqlite_master instead: the steady-state open is a pure read, and a genuine
    rewrite runs inside one transaction so the absence is never observable.

    One read for the whole set rather than a lookup per name. The set is now
    every trigger the schema defines rather than the seventeen with authored
    text, and a query each would put a statement per guard on the hot path of
    every open to answer a question one scan of sqlite_master answers.
    """
    live = conn.execute(
        "SELECT type, name, sql FROM sqlite_master "
        "WHERE type IN ('table', 'trigger')"
    ).fetchall()
    tables = {name for kind, name, _ in live if kind == "table"}
    stored = {name: sql for kind, name, sql in live if kind == "trigger"}
    for name, wanted in _MANAGED_TRIGGERS:
        if stored.get(name) == wanted:
            continue
        # A guard whose table is not here cannot be created, and its absence is
        # not this function's to report. The same rule the census uses: a table
        # that was never built here is an older file, and one that was built and
        # taken away is already on the census under the guard's own name.
        if _EXPECTED_TRIGGER_TABLES[name] not in tables:
            continue
        own_transaction = not conn.in_transaction
        if own_transaction:
            conn.execute("BEGIN IMMEDIATE")
        conn.execute(f"DROP TRIGGER IF EXISTS {name}")
        conn.execute(wanted)
        if own_transaction:
            conn.commit()


def _open_existing_db(
    conn: sqlite3.Connection, root: Path, version: int,
) -> None:
    """Enforce and migrate the on-disk contract of an already-initialised db.

    Shared by :func:`open_db` and :func:`open_db_from_db_path` so both entry
    points refuse the same files and migrate the same ones: a db reached by a
    literal path must meet the contract a db reached by project root meets.
    Closes *conn* and raises :class:`DatabaseError` when the file cannot be
    served. *root* is the project root used for the grandfather event and the
    claims.toml remediation hint.
    """
    # A graph from a later release is refused before anything touches it. No
    # census, no ALTER, no migration: this code does not know what it is looking
    # at, and the census store forgets nothing, so a guard this build expects
    # and a newer one retired would be written into that graph as a permanent
    # tamper record by the build least able to judge it.
    if version > _SCHEMA_VERSION:
        conn.close()
        raise DatabaseError(
            f"graph.db has user_version={version}, which is ahead of the "
            f"user_version={_SCHEMA_VERSION} this mareforma understands. It "
            "was written by a newer release and may carry schema this one "
            "does not know how to read, so opening it could report on a graph "
            "it is misreading. Nothing is wrong with the file. Upgrade "
            "mareforma to the version that wrote it. Do not delete graph.db: "
            "it holds the chain and every signature, and claims.toml cannot "
            "reconstruct them."
        )

    existing_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(claims)").fetchall()
    }

    # Census ahead of every repair below, and only into a file that is a graph.
    # A migration's rebuild calls the trigger reconciler, and the reconciler
    # restores the whole managed set rather than the guards the rebuild dropped,
    # on tables it never touched. Censused after that, one migration reports a
    # clean schema on a graph somebody had taken guards off, which is the
    # observation the census exists to keep. The claims check comes first
    # because pointing this at an unrelated SQLite file should refuse it, not
    # write mareforma's own tables into it on the way to refusing it.
    # A version with no route is refused before any ALTER commits AND before the
    # census writes. The ALTERs below commit one at a time, so refusing after
    # them leaves a file this release will not open and the release that wrote it
    # now rejects for carrying columns it does not know. And the census store
    # forgets nothing, so a guard this build expects that an unroutable old file
    # never had would be written into it as a permanent tamper record, by a
    # build that has just said it cannot interpret the file. The release that
    # wrote it then reads that record and brands every claim in it. Both
    # directions of an unrecognised version are now refused before anything is
    # written, which is the ordering this had before the route check arrived.
    if version < _SCHEMA_VERSION:
        try:
            _plan_migration(version)
        except MigrationError:
            conn.close()
            raise

    if existing_cols:
        _record_schema_census(conn)

    # Auto-migrate the two columns added between v0.3.0 and v0.3.1.
    # Both are non-signed, non-CHECK'd query-side denormalisations with
    # safe defaults, so ALTER ADD COLUMN is a non-disruptive in-place
    # additive migration that preserves every existing row's signed
    # bytes. Concurrent first-opens hit a "duplicate column name" race
    # we treat as benign.
    _ensure_claims_columns_for_upgrade(conn, existing_cols)
    # Migrate AFTER the column ALTERs and before the exact-set check. A step
    # copies the column list this release knows, and an older file is missing
    # some of those columns until the ALTERs above have run, so migrating first
    # fails on exactly the graphs migrations exist for. The exact-set check has
    # to come after, because changing that set is what a migration is for.
    if version < _SCHEMA_VERSION:
        try:
            version = _migrate_to_current(conn, version)
        except BaseException:
            conn.close()
            raise
        # Re-gate. The version above was read before any lock was taken, so a
        # concurrent opener can have moved the file past this release while this
        # one waited, and the future check has already run.
        if version != _SCHEMA_VERSION:
            conn.close()
            raise MigrationError(
                f"graph.db reached user_version={version} while this open was "
                f"waiting, which this mareforma does not understand "
                f"(it expects {_SCHEMA_VERSION}). Another process upgraded it. "
                "Do not delete graph.db. Upgrade mareforma to the version that "
                "did, and open it again."
            )

    existing_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(claims)").fetchall()
    }
    # Validate the schema by exact column-set match. Catching extras as well as
    # missing columns means a partially-migrated or hand-edited claims table
    # fails loudly instead of silently passing through code that assumes
    # _CLAIM_COLUMNS is exhaustive.
    expected_cols = set(_CLAIM_COLUMNS)
    if existing_cols != expected_cols:
        missing = expected_cols - existing_cols
        extra = existing_cols - expected_cols
        conn.close()

        # Extras-only means the db was written by a newer mareforma. Upgrade
        # is the whole of the advice: an older release refuses a migrated graph
        # and refuses the backup it writes, so there is no downgrade to prepare
        # for. Saying otherwise sent an operator to take a backup that the
        # release they were downgrading to would not read.
        if extra and not missing:
            raise DatabaseError(
                f"graph.db was created by a newer mareforma version "
                f"(extra columns: {sorted(extra)}). Upgrade the mareforma "
                "package. Downgrading is not a route: an older release refuses "
                "this graph and refuses the claims.toml it writes."
            )

        parts: list[str] = []
        if missing:
            parts.append(f"missing: {sorted(missing)}")
        if extra:
            parts.append(f"unexpected: {sorted(extra)}")
        # Only claim a backup after looking for one. Recovery is two
        # steps, and restore refuses to run while graph.db still holds
        # claims, so naming the deletion alone leaves the operator with
        # a TOML file and no stated way to use it.
        if (root / "claims.toml").exists():
            remedy = (
                "Delete .mareforma/graph.db, then run `mareforma "
                "restore` to rebuild it from claims.toml with signature "
                "verification."
            )
        else:
            remedy = (
                "No claims.toml is present, so deleting "
                ".mareforma/graph.db discards the only copy of these "
                "claims. Copy graph.db elsewhere first."
            )
        raise DatabaseError(
            f"graph.db schema mismatch ({'; '.join(parts)}). {remedy}"
        )

    # Additive tables (project_policy, the trust layer) must be
    # present on every initialised db, not just fresh ones ,
    # otherwise an existing legacy graph.db lacks them and the first
    # trust-layer write raises 'no such table'.
    conn.executescript(_ADDITIVE_TABLES_SQL)
    _ensure_supports_revision_row(conn)
    _ensure_evidence_lines_columns(conn)
    _ensure_project_policy_columns(conn)


def open_db(root: Path) -> sqlite3.Connection:
    """Open (and initialise if needed) the graph database.

    Returns an open sqlite3.Connection with row_factory set to
    sqlite3.Row for dict-like access.

    Schema validation
    -----------------
    Fresh db (user_version=0): full schema applied, user_version set to
    ``_SCHEMA_VERSION``.

    Initialised db (user_version equals ``_SCHEMA_VERSION``): claims
    table must have every column in ``_CLAIM_COLUMNS``. Missing columns
    raise DatabaseError instructing the user to delete graph.db.
    ``_CLAIM_COLUMNS`` is the source of truth for what the schema must
    contain.

    Raises
    ------
    DatabaseError
        On SQLite errors or schema drift (missing columns).
    """
    path = _db_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Minimum SQLite version. FTS5 with `remove_diacritics 2` (used by
    # claims_fts) requires ≥ 3.27 (released 2019-02). We pick 3.30 as a
    # comfortable floor that gives us window functions + UPSERT + the
    # `||` operator parsing fixes that have shaken out over the years.
    # Common LTS distros that ship below this floor (Ubuntu 18.04 EOL,
    # CentOS 7 EOL) are well outside the support window. Fail loudly
    # with a concrete remediation rather than a cryptic SQL syntax
    # error deep in trigger creation.
    _MIN_SQLITE = (3, 30, 0)
    _have = tuple(int(p) for p in sqlite3.sqlite_version.split("."))
    if _have < _MIN_SQLITE:
        raise DatabaseError(
            f"mareforma requires SQLite >= "
            f"{'.'.join(str(p) for p in _MIN_SQLITE)}, "
            f"this Python build links {sqlite3.sqlite_version}. "
            "Upgrade your system SQLite (apt / brew / etc.) or install "
            "`pysqlite3-binary` and import it as the `sqlite3` module."
        )

    try:
        conn = sqlite3.connect(
            str(path), check_same_thread=False, factory=_GraphConnection
        )
        conn.row_factory = sqlite3.Row
        # SQLite default is foreign_keys = OFF. Every REFERENCES clause
        # in the schema is advisory without this PRAGMA. Verdict-issuer
        # tables FK to validators(keyid) and claims(claim_id); without
        # this set on every connection the FK is unenforced and direct-
        # SQL INSERTs with fabricated keyids would succeed.
        conn.execute("PRAGMA foreign_keys = ON")

        version = conn.execute("PRAGMA user_version").fetchone()[0]

        if version == 0 and conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='claims'"
        ).fetchone() is not None:
            # A fresh database has no tables. This one has claims, so whatever
            # the pragma says, it is not fresh: `user_version` is a plain write
            # no trigger can refuse, and zeroing it on a populated graph asks
            # for the branch below, which heals every guard and records
            # nothing. That is the census bypassed by one PRAGMA, on the graph
            # the census exists for. Look before the repairs run, the same
            # ordering the existing-graph path uses and for the same reason.
            _record_schema_census(conn)
            # And then send it down the existing-graph path rather than the
            # fresh one. The fresh branch ends by stamping the current version,
            # so a zeroed graph came out marked as having had every migration,
            # with none of them run. That was harmless only while the one
            # registered step changed nothing, and the stamp is now the sole
            # evidence a step ever ran. Normalised to the earliest version this
            # release routes from: zero is not a state any release wrote on a
            # populated file, and a graph already past that version meets a step
            # that rebuilds the table it already has, which costs a rebuild and
            # changes nothing.
            version = 1
            conn.execute(f"PRAGMA user_version = {version}")
            conn.commit()

        if version == 0:
            conn.executescript(_SCHEMA_SQL)
            conn.executescript(_ADDITIVE_TABLES_SQL)
            # With the census store here too, since its guards are reconciled
            # like the rest and the reconciler cannot build a trigger on a
            # table that does not exist yet.
            conn.executescript(_SCHEMA_CENSUS_SQL)
            _ensure_supports_revision_row(conn)
            _ensure_managed_triggers(conn)
            # Seed the seen set while the graph is provably whole, so a graph
            # this build creates never carries an empty baseline. Without it a
            # brand-new file could have a table taken away before its first
            # reopen, and with nothing seen yet the guards on that table would
            # never have been expected.
            _note_guards_seen(conn)
            conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            conn.commit()
            _attach_supports_cache(conn, root)
            return conn

        _open_existing_db(conn, root, version)
        _attach_supports_cache(conn, root)
        _ensure_managed_triggers(conn)
        # After the repairs, so an upgrade records the trust layer it just
        # built rather than leaving it unseen until the next open.
        _note_guards_seen(conn)
        conn.commit()
        return conn

    except sqlite3.Error as exc:
        # sqlite3.DatabaseError ('file is not a database') is the PARENT of
        # OperationalError, so a corrupt or truncated graph.db raised it at the
        # PRAGMA user_version read and sailed past a narrow OperationalError
        # catch as a bare traceback. Catch the whole sqlite3.Error family so the
        # documented "Raises DatabaseError on SQLite errors" contract holds and
        # the corruption case reaches the claims.toml remediation.
        #
        # Contention is told apart first, because it is not a fault in the file
        # and the remedy below is the worst possible advice for it. Several
        # writes happen on the way in before any migration guard, the census and
        # the column additions among them, and a lock held by another opener
        # surfaced there as "delete graph.db and start fresh" on a file with
        # every byte intact. Measured. This release makes it reachable in
        # ordinary use, because the migration holds the lock for seconds on a
        # large graph's first open.
        if isinstance(exc, sqlite3.OperationalError) and (
            "locked" in str(exc) or "busy" in str(exc)
        ):
            raise _open_failure(
                path, exc,
                "Another process is using this graph and is holding it while "
                "it writes. Nothing here has been changed. Do not delete "
                "graph.db; open it again in a moment.",
            ) from exc
        raise _open_failure(
            path, exc,
            "If graph.db is corrupt or truncated, delete .mareforma/graph.db "
            "and start fresh; claims.toml is a human-readable record of the "
            "prior state.",
        ) from exc


def open_db_from_db_path(db_path: "str | Path") -> sqlite3.Connection:
    """Open the graph DB from a direct path to ``graph.db`` (not a project root).

    ``open_db`` takes the project root and re-derives the file path. This
    helper reverses that for a caller who already holds the file path, so the
    path is honoured instead of silently rewritten. An existing db goes through
    the same version guard and column migration either entry point applies.

    Accepted shapes:
      - ``<root>/.mareforma/graph.db``: opens ``<root>`` as project root.
      - any other path: opens the DB file directly; the user supplied
        a non-conventional location and we honour it. The parent
        directory becomes the "project root" for cache lookups, and the
        DB lives at the supplied path (NOT at ``<parent>/.mareforma/``).
    """
    db_file = Path(db_path).resolve()
    if db_file.parent.name == ".mareforma":
        return open_db(db_file.parent.parent)

    # Non-conventional path: connect to db_file directly and apply the
    # schema script (idempotent). This preserves the user-supplied
    # filename instead of silently rewriting it under .mareforma/.
    db_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = sqlite3.connect(
            str(db_file), check_same_thread=False, factory=_GraphConnection
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version == 0 and conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='claims'"
        ).fetchone() is not None:
            # The same normalisation the project-root path applies, for the
            # same reason and on the same evidence: a file holding claims is
            # not fresh whatever the pragma says, and the branch below stamps
            # the current version, so it would come out marked as having had
            # every migration with none of them run. That is worse here than
            # there, because a literal path is what an operator reaches for
            # after `sqlite3 .dump`, which does not carry `user_version` at
            # all. Left alone, the recovery route ends in a graph no release
            # will open: this one refuses the columns the migration would have
            # dropped, and the release that wrote it refuses the stamp.
            _record_schema_census(conn)
            version = 1
            conn.execute(f"PRAGMA user_version = {version}")
            conn.commit()
        if version == 0:
            conn.executescript(_SCHEMA_SQL)
            conn.executescript(_ADDITIVE_TABLES_SQL)
            conn.executescript(_SCHEMA_CENSUS_SQL)
            _ensure_supports_revision_row(conn)
            conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
        else:
            _open_existing_db(conn, db_file.parent, version)
        _ensure_managed_triggers(conn)
        # Same seeding as the conventional path: a db reached by a literal path
        # meets the contract a db reached by project root meets.
        _note_guards_seen(conn)
        conn.commit()
        # Attach the rebuildable supports cache just like open_db does. Without
        # it add_claim's unconditional supports-edge maintenance hits
        # 'no such table: supports_cache.cache_meta' and every write fails,
        # even a claim with no supports. The parent directory is the "project
        # root" for cache lookups per this function's own docstring, so the
        # sidecar lands at <parent>/.mareforma/claim_supports_cache.db.
        _attach_supports_cache(conn, db_file.parent)
        return conn
    except sqlite3.Error as exc:
        # Same contract as open_db: a corrupt or truncated file raises
        # sqlite3.DatabaseError at the PRAGMA read, which is NOT an
        # OperationalError. Wrap the whole sqlite3.Error family so a literal
        # path never leaks a raw sqlite3 exception.
        raise _open_failure(
            db_file, exc,
            "If the file is corrupt or truncated, delete it and restore "
            "from claims.toml.",
        ) from exc


class MigrationError(DatabaseError):
    """A schema migration could not complete, and nothing was changed.

    Distinct from :class:`DatabaseError` so the remedy can be too. The generic
    one tells an operator to delete graph.db and start from claims.toml, which
    is right for a file this code cannot recognise and wrong for one it failed
    to migrate: the migration is a single transaction, so a failure leaves the
    graph exactly as it was, and deleting it would throw away a chain that is
    still intact over a fault that changed nothing.
    """


@contextmanager
def _upgrade_window(conn: sqlite3.Connection):
    """Open the marker a table rebuild is only allowed to run inside.

    The sibling windows mark a write so a trigger will permit it. This one
    marks nothing in SQL, because no trigger can refuse a ``DROP TABLE``: the
    rebuild takes every guard on the table away with the table itself, and runs
    with foreign keys off. So the gate is in Python and this is the only thing
    that opens it, from the versioned upgrade path.

    A temp table, so it lives on this connection and cannot be left open for
    another one, and it is dropped in a ``finally`` so a failed migration does
    not leave the door open behind it.
    """
    conn.execute(
        f"CREATE TEMP TABLE IF NOT EXISTS {_UPGRADE_MARKER_TABLE} (id INTEGER)"
    )
    try:
        yield
    finally:
        try:
            conn.execute(f"DROP TABLE IF EXISTS temp.{_UPGRADE_MARKER_TABLE}")
        except sqlite3.Error:
            # Closing the window can only fail on a connection that is already
            # failing every statement, which is what an interrupted migration
            # leaves. Raising here would replace the migration's own error with
            # the noise that followed it. The marker is a temp table, so it
            # cannot outlive this connection however this ends.
            pass


def _upgrade_window_open(conn: sqlite3.Connection) -> bool:
    """True while :func:`_upgrade_window` is open on this connection."""
    row = conn.execute(
        "SELECT 1 FROM temp.sqlite_master WHERE type = 'table' AND name = ?",
        (_UPGRADE_MARKER_TABLE,),
    ).fetchone()
    return row is not None


def _table_index_sql(
    conn: sqlite3.Connection, table: str,
) -> "tuple[str, ...]":
    """The CREATE INDEX statements a rebuild of *table* has to put back.

    Read from ``sqlite_master`` rather than from a constant, for the same reason
    the trigger reconciler reads the DDL rather than a hand-copied list, and
    with one more reason on top: an upgraded graph carries indexes this code
    never authored, and a rebuild that replayed only the authored set would
    drop them silently.

    Implicit indexes are skipped. SQLite gives ``sqlite_autoindex_*`` rows a
    NULL ``sql`` because the PRIMARY KEY and UNIQUE clauses in the table's own
    DDL create them, so the new table already has them and replaying is neither
    possible nor needed.
    """
    return tuple(
        row[0] for row in conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' "
            "AND tbl_name = ? AND sql IS NOT NULL ORDER BY name",
            (table,),
        )
    )


def _unmanaged_trigger_sql(
    conn: sqlite3.Connection, table: str,
) -> "tuple[str, ...]":
    """Triggers on *table* the reconciler will not put back after a rebuild.

    The reconciler recreates the managed set, which is the set this release
    names. A graph can carry a guard an earlier release wrote and this one no
    longer lists, and dropping the table takes it away with everything else. The
    reconciler would then not miss it, the census would not report it, and a
    write guard on an append-only store would be gone with no record.

    So they are captured and replayed, for the reason
    :func:`_table_index_sql` captures indexes rather than replaying an authored
    list. A rebuild is not the place to decide which guards a graph is allowed
    to have; its job is to leave the table as it found it.
    """
    managed = {name for name, _ in _MANAGED_TRIGGERS}
    return tuple(
        row[1] for row in conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' "
            "AND tbl_name = ? AND sql IS NOT NULL ORDER BY name",
            (table,),
        )
        if row[0] not in managed
    )


# The name a rebuild renames the table to and back, to make SQLite reparse.
_REPARSE_PROBE = "_mareforma_reparse_probe"


def _canary_targets(conn: sqlite3.Connection, table: str) -> "tuple[str, ...]":
    """The tables a rebuild of *table* could have left unable to take a write.

    The rebuilt table, and every ordinary table carrying a trigger. A trigger
    body is the only place a reference to a dropped column can hide where a
    write will find it: a CHECK constraint cannot name another table, and a
    foreign key is :func:`_require_references_resolve`'s business.

    Virtual tables are left out. Their write path belongs to the module that
    implements them, and preparing a statement against a full-text index proves
    nothing about the schema this migration changed.
    """
    owners = {
        row[0] for row in conn.execute(
            "SELECT DISTINCT tbl_name FROM sqlite_master WHERE type = 'trigger'"
        )
    }
    ordinary = {
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%' "
            "AND sql NOT LIKE 'CREATE VIRTUAL%'"
        )
    }
    return tuple(sorted(ordinary & (owners | {table})))


def _uncompilable_writes(
    conn: sqlite3.Connection, table: str,
) -> "dict[str, str]":
    """Which tables will not accept a write, without writing to any of them.

    SQLite compiles a statement's whole trigger program when it prepares it, so
    a trigger body naming a column that is gone fails at prepare time. Preparing
    one insert, one update and one delete per table therefore reaches every
    trigger a write on it could fire, including one fired by another trigger.

    This is what closes the shape no check that matches names can see. A trigger
    doing ``INSERT INTO archive SELECT * FROM claims`` never spells the dropped
    column, and the reparse resolves it happily because every name in it still
    exists; only the arity changed. Measured: the migration committed, reported
    success, and the next write died with "table claims_archive has 36 columns
    but 35 values were supplied". Preparing the same write raises that inside
    the transaction, where a failure rolls the rebuild back.

    Preparing rather than writing is what makes it safe to run on a real graph.
    There is no synthetic row to build, so no CHECK constraint to satisfy and no
    unique key to collide with; no write guard fires, so a table that refuses
    deletes by design is not read as a broken one; and nothing has to be undone,
    so a crash here cannot leave a canary row behind.

    The update names every column. A trigger declared ``AFTER UPDATE OF`` one
    column is compiled only by a statement that writes that column, so an update
    touching one column reaches one trigger and says nothing about the rest.

    All three statements are tried even after one fails, because the caller
    compares this against the same probe run before the rebuild. Stopping at the
    first failure would report the same first error on a table that arrived
    broken and that the rebuild then broke a second way, and the comparison would
    read the two as equal and let the new one through.
    """
    broken: "dict[str, str]" = {}
    for target in _canary_targets(conn, table):
        columns = [
            row[1] for row in conn.execute(f'PRAGMA table_info("{target}")')
        ]
        if not columns:
            continue
        assignments = ", ".join(f'"{c}" = "{c}"' for c in columns)
        failures = []
        for kind, statement in (
            ("insert", f'INSERT INTO "{target}" DEFAULT VALUES'),
            ("update", f'UPDATE "{target}" SET {assignments}'),
            ("delete", f'DELETE FROM "{target}"'),
        ):
            try:
                conn.execute(f"EXPLAIN {statement}")
            except sqlite3.Error as exc:
                failures.append(f"{kind}: {exc}")
        if failures:
            broken[target] = "; ".join(failures)
    return broken


def _require_writes_still_compile(
    conn: sqlite3.Connection, table: str, before: "dict[str, str]",
) -> None:
    """Refuse a rebuild that left a table unable to take a write.

    Compared against the same probe run before the rebuild, so a graph that
    arrived carrying a broken trigger is neither refused for it nor told the
    migration did it. A migration answers for what it changed.
    """
    for target, detail in sorted(_uncompilable_writes(conn, table).items()):
        if before.get(target) == detail:
            continue
        raise MigrationError(
            f"after rebuilding {table!r} a write to {target!r} no longer "
            f"compiles: {detail}. Something in this graph reaches what the "
            "migration removed, and this release does not manage it so it "
            "cannot rewrite it. Left in place it would commit a graph whose "
            "next write fails. Nothing has been changed."
        )


def _unresolved_references(
    conn: sqlite3.Connection, table: str,
) -> "dict[str, str]":
    """Which foreign keys onto *table* no longer resolve.

    ``PRAGMA foreign_key_check`` raises "foreign key mismatch" for a REFERENCES
    clause whose parent no longer has the column, and it has to be asked about
    the CHILD. Measured: the same pragma scoped to the rebuilt parent returns no
    rows on exactly that graph, so the check that shipped here first was looking
    in the wrong place and could not have caught the case its own comment
    claimed it caught.

    Scoped to the rebuilt table and its children rather than the whole database,
    so a mismatch elsewhere in a graph that arrived with one is neither blamed on
    this migration nor able to mask one this migration made.

    Ordinary orphan rows come back as rows rather than an exception and are
    deliberately not reported: a graph that already had them is not this
    migration's to refuse.
    """
    children = [
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    broken: "dict[str, str]" = {}
    for child in children:
        parents = {
            row[2] for row in conn.execute(
                f'PRAGMA foreign_key_list("{child}")'
            )
        }
        if child != table and table not in parents:
            continue
        try:
            conn.execute(f'PRAGMA foreign_key_check("{child}")').fetchall()
        except sqlite3.Error as exc:
            broken[child] = str(exc)
    return broken


def _require_references_resolve(
    conn: sqlite3.Connection, table: str, before: "dict[str, str]",
) -> None:
    """Refuse a rebuild that left a foreign key naming a column that is gone.

    Compared against the same probe run before the rebuild, on the same rule the
    write probe follows: a graph that arrived with a dangling reference is not
    refused for it and, worse, not told the migration did it.
    """
    for child, detail in sorted(_unresolved_references(conn, table).items()):
        if before.get(child) == detail:
            continue
        raise MigrationError(
            f"after rebuilding {table!r} a foreign key on {child!r} no longer "
            f"resolves: {detail}. A REFERENCES clause names what the migration "
            "removed, and this release does not manage it so it cannot rewrite "
            "it. Nothing has been changed."
        )


def _require_schema_resolves(conn: sqlite3.Connection, table: str) -> None:
    """Refuse a rebuild that left any object naming something that is gone.

    SQLite resolves a trigger or view body when it runs, not when it is created,
    so a narrowing rebuild can commit a schema whose next write dies on a column
    the migration removed. Atomicity is no help: nothing failed inside the
    transaction, and the graph is left unwritable by a migration that reported
    success.

    A rename with ``legacy_alter_table`` off reparses the WHOLE schema and fails
    if anything in it no longer resolves, so renaming the table aside and back
    is a full check with an exact error naming the object. It runs inside the
    caller's transaction, so a failure rolls the rebuild back.

    Pattern-matching column names against trigger text was tried first and was
    wrong twice over. It missed anything the check did not scan or spell the
    same way, since SQLite identifiers are case-insensitive and objects on other
    tables were never looked at, and it fired on the word appearing inside an
    error string, blocking a legitimate migration with advice to delete a write
    guard. The reparse has neither failure: it asks SQLite the question instead
    of guessing at it.
    """
    squatter = conn.execute(
        "SELECT name FROM sqlite_master WHERE name = ?", (_REPARSE_PROBE,),
    ).fetchone()
    if squatter is not None:
        raise MigrationError(
            f"an object named {_REPARSE_PROBE!r} is already in this graph, and "
            "the rebuild needs that name free to make SQLite recheck the "
            "schema. Nothing has been changed. Rename or drop it, then migrate."
        )
    # Set the pragma here rather than inheriting it. The check IS the reference
    # rewriting, so a connection that already had the legacy behaviour on turned
    # the whole thing into a no-op that passed everything.
    was_legacy = conn.execute("PRAGMA legacy_alter_table").fetchone()[0]
    conn.execute("PRAGMA legacy_alter_table = OFF")
    try:
        conn.execute(f"ALTER TABLE {table} RENAME TO {_REPARSE_PROBE}")
        conn.execute(f"ALTER TABLE {_REPARSE_PROBE} RENAME TO {table}")
    except sqlite3.Error as exc:
        raise MigrationError(
            f"after rebuilding {table!r} the schema no longer resolves: {exc}. "
            "Something in this graph, a trigger or a view, names what the "
            "migration removed, and this release does not manage it so it "
            "cannot rewrite it. Left alone it would commit a graph whose next "
            "write fails. Nothing has been changed."
        ) from exc
    finally:
        if was_legacy:
            try:
                conn.execute("PRAGMA legacy_alter_table = ON")
            except sqlite3.Error:
                # Same rule as the other three restores. On an interrupted
                # connection this raises too, and it would replace the exact
                # "schema no longer resolves" message, which names the offending
                # object, with a bare "interrupted".
                pass


def _rebuild_table(
    conn: sqlite3.Connection,
    *,
    table: str,
    create_sql: str,
    columns: "tuple[str, ...]",
    drops: "tuple[str, ...]" = (),
) -> None:
    """Rebuild *table* under a new definition, carrying *columns* across.

    The seven steps SQLite's own ALTER procedure prescribes, in the order that
    survives them: build the replacement, copy, drop the original, rename,
    then put the triggers and indexes back.

    **The column list is a parameter, and both sides of the copy name it.**
    A same-schema rebuild could get away with ``INSERT INTO new SELECT * FROM
    old``, and a column-dropping one cannot: it needs an explicit list against a
    wider source, and a positional copy is then the one way this can corrupt a
    graph in silence. Naming the columns on both sides makes the ordering of
    either table irrelevant, so the shape that ships here is the shape a
    column-dropping migration reuses without a rewrite.

    *create_sql* builds the replacement under a temporary name and is supplied
    rather than derived. Filtering a column out of authored DDL means parsing
    it, and a parser that gets a CHECK clause wrong writes a table that accepts
    what the old one refused.

    Two pragmas, and they behave differently, which is why neither is left to
    the caller's memory. ``foreign_keys`` must already be off, and it cannot be
    turned off here: it is a silent no-op inside a transaction, so the runner
    sets it outside one. ``legacy_alter_table`` does take effect inside a
    transaction and is set here, around the rename alone. Without it step four
    fails: ``contradiction_invalidates_older`` is a trigger on another table
    whose body names ``claims``, and a modern rename reparses the whole schema,
    which cannot resolve that name in the window where the table is gone. It is
    connection-scoped, so it is restored immediately: left on, every later
    rename would quietly stop rewriting references, which is the laundering
    primitive this whole path is gated to prevent.

    **A narrowing rebuild is checked three ways, and each sees what the others
    cannot.** The reparse resolves view bodies and ``NEW``/``OLD`` references.
    The write probe compiles a write against the rebuilt table and every table
    carrying a trigger, which reaches everything a statement compiles, including
    a body that gets at the dropped column through a star and never spells its
    name. The reference check reads a ``REFERENCES`` clause, which neither of
    the others looks at. A name-matching scan of the stored SQL sat here first
    and is gone: every shape it caught, one of these three catches by asking
    SQLite rather than by guessing, and the shape it could never catch is the
    one the write probe exists for.

    Assumes an open transaction. The caller owns it, because the version bump
    has to commit with the rebuild or not at all.
    """
    if not _upgrade_window_open(conn):
        raise MigrationError(
            f"the rebuild of {table!r} was called outside the upgrade path. It "
            "drops every write guard on the table and runs with foreign keys "
            "off, so it is reachable from a versioned migration and from "
            "nowhere else."
        )
    live = tuple(
        row[1] for row in conn.execute(f"PRAGMA table_info({table})")
    )
    # A rebuild copies the columns it is given and the rest are gone with the
    # old table. The exact column-set check on the open path is what catches a
    # hand-edited claims table, and it runs after this, so an undeclared
    # narrowing would leave that check comparing against the laundered result
    # and finding nothing to report. Every column that goes has to be named.
    unnamed = set(live) - set(columns) - set(drops)
    if unnamed:
        raise MigrationError(
            f"the rebuild of {table!r} would drop {sorted(unnamed)}, which the "
            "step did not declare. A column this migration does not know about "
            "is a column somebody else put there, and dropping it here would "
            "erase it and the check that would have reported it. Nothing has "
            "been changed. The usual cause is a graph a newer mareforma wrote, "
            "so upgrade the package; downgrading is not a route, because an "
            "older release refuses both this graph and the claims.toml it "
            "writes. This used to be said by the column-set check, which now "
            "runs after the migration and no longer gets the chance."
        )
    absent = set(drops) - set(live)
    if absent:
        raise MigrationError(
            f"the rebuild of {table!r} declares it drops {sorted(absent)}, "
            f"which {table!r} does not have. The step and the table disagree "
            "about what is there, so the rest of what it declares cannot be "
            "trusted either. Nothing has been changed."
        )

    # Before anything changes, so the checks afterwards can tell what this
    # rebuild broke from what arrived broken.
    already_broken = _uncompilable_writes(conn, table)
    already_dangling = _unresolved_references(conn, table)
    indexes = _table_index_sql(conn, table)
    unmanaged = _unmanaged_trigger_sql(conn, table)
    names = ", ".join(columns)
    temp = f"{table}_new"
    # The scratch name has to be free before anything is built under it. A graph
    # that merely holds a table by that name opened without complaint on every
    # release before this one, and would now fail its upgrade on "table
    # claims_new already exists", which names no remedy and leaves the operator
    # with a graph no release will open. The cure is one statement, so say it.
    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (temp,),
    ).fetchone():
        raise MigrationError(
            f"this graph already has a table named {temp!r}, which is the name "
            f"the rebuild of {table!r} builds its replacement under. Nothing "
            f"has been changed. Rename or drop {temp!r} and open the graph "
            "again; it holds nothing mareforma wrote."
        )
    was_legacy = conn.execute("PRAGMA legacy_alter_table").fetchone()[0]

    conn.execute(create_sql)                                        # 1
    # The replacement has to agree with what the step said it was doing. A
    # declared drop the definition still carries is the dangerous shape: the
    # column survives, the copy does not carry it, every row silently loses its
    # value or takes a DEFAULT, and the exact column-set check on the open path
    # compares set against set, sees no difference and reports nothing.
    built = {row[1] for row in conn.execute(f"PRAGMA table_info({temp})")}
    if not set(drops).isdisjoint(built):
        raise MigrationError(
            f"the rebuild of {table!r} declares it drops "
            f"{sorted(set(drops) & built)}, and the replacement definition "
            "still has those columns. The copy would leave them empty on every "
            "row and nothing downstream would notice. Nothing has been changed."
        )
    if not set(columns) <= built:
        raise MigrationError(
            f"the rebuild of {table!r} copies "
            f"{sorted(set(columns) - built)}, which the replacement definition "
            "does not have. Nothing has been changed."
        )
    # rowid travels with the rows. The claim chain's tip is read in rowid order,
    # so a copy that let SQLite choose its own order could move the tip with
    # nothing raising. Naming it keeps the order the chain is defined by.
    conn.execute(                                                   # 2
        f"INSERT INTO {temp} (rowid, {names}) SELECT rowid, {names} FROM {table}"
    )
    conn.execute(f"DROP TABLE {table}")                             # 3
    conn.execute("PRAGMA legacy_alter_table = ON")
    try:
        conn.execute(f"ALTER TABLE {temp} RENAME TO {table}")       # 4
    finally:
        try:
            conn.execute(
                f"PRAGMA legacy_alter_table = {'ON' if was_legacy else 'OFF'}"
            )
        except sqlite3.Error:
            # Same rule as the other two restores: on a connection that is
            # already failing, do not replace the real error with this one. The
            # transaction is about to roll back and the connection is closed by
            # every caller that gets a migration failure, so the pragma cannot
            # outlive the fault and go on suppressing reference rewriting.
            pass
    _ensure_managed_triggers(conn)                                  # 5
    for statement in unmanaged:
        conn.execute(statement)
    for statement in indexes:                                       # 6
        conn.execute(statement)
    # Three checks, because each sees something the other two do not. The
    # reparse resolves view bodies and NEW/OLD references. The write probe
    # reaches everything a statement compiles, including a trigger body that
    # never spells the dropped column's name. The reference check reads a
    # REFERENCES clause, which neither of the others looks at.
    _require_schema_resolves(conn, table)
    _require_writes_still_compile(conn, table, already_broken)
    _require_references_resolve(conn, table, already_dangling)


def _run_migration(
    conn: sqlite3.Connection,
    *,
    to_version: int,
    steps: "Callable[[sqlite3.Connection], None]",
    from_version: "int | None" = None,
) -> None:
    """Apply *steps* and bump ``user_version``, in one transaction or none.

    Atomicity is the whole guarantee. Verified rather than assumed: no statement
    in the rebuild forces an implicit commit, ``sqlite_master`` rolls back to
    exactly what it was, and ``user_version`` rolls back with it. So a crash at
    any step leaves a graph that opens on the old code, with its rows, its
    guards and its version untouched. There is no half-migrated state to
    recover from, which is why nothing here tells an operator to delete
    anything.

    ``foreign_keys`` is toggled outside the transaction because inside one the
    pragma is a silent no-op, and the rebuild needs it off: dropping the old
    table would otherwise fail against the six tables that reference it.
    Restored in a ``finally``, since it is connection-scoped and every later
    write on this connection depends on it.
    """
    # Both pragmas are inside the guarded block for the same reason the BEGIN
    # is. They were outside it, and a failure on either escaped as a raw sqlite
    # error into the generic open handler, whose remedy is to delete graph.db.
    # Measured, by denying the pragma and by interrupting on it: a file with
    # every byte intact was met with the delete advice this path exists to stop
    # giving, before the migration had touched anything.
    ok = False
    was_on = False
    try:
        try:
            was_on = conn.execute("PRAGMA foreign_keys").fetchone()[0]
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute("BEGIN IMMEDIATE")
            # Re-read the version under the lock. It was read before the lock
            # was taken, so two openers can both have seen the old one and both
            # decide to run this step. The rebuild that ships here happens to be
            # idempotent, which hides it; a step that backfills a column or
            # inserts a row is not, and would apply twice with no error and a
            # correct-looking version afterwards.
            if from_version is not None:
                current = conn.execute("PRAGMA user_version").fetchone()[0]
                if current != from_version:
                    conn.execute("ROLLBACK")
                    # Not a failure: somebody else did this step. The connection
                    # goes back to a caller either way, so the pragma restore
                    # below is held to the success rule, not the unwinding one.
                    ok = True
                    return
            steps(conn)
            conn.execute(f"PRAGMA user_version = {int(to_version)}")
            conn.execute("COMMIT")
            ok = True
        except BaseException as exc:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            # Contention is not a fault in the file, and the sentence for it
            # says the one thing that helps. Without this the loser of a race
            # waits out the busy timeout and is handed a failure that reads
            # like a defect and never mentions trying again.
            if isinstance(exc, sqlite3.OperationalError) and (
                "locked" in str(exc) or "busy" in str(exc)
            ):
                raise MigrationError(
                    "another process is upgrading this graph and holds it while "
                    f"it works: {exc}. Nothing has been changed here, and that "
                    "upgrade is still running or has already finished. Do not "
                    "delete graph.db. Open it again in a moment."
                ) from exc
            raise MigrationError(
                f"the schema migration to version {to_version} failed and was "
                f"rolled back: {exc}. This step changed nothing: the graph is "
                "at the version it was at when the step began, with every "
                "claim, signature and chain link intact. Do not delete it."
            ) from exc
    finally:
        if was_on:
            try:
                conn.execute("PRAGMA foreign_keys = ON")
            except sqlite3.Error as exc:
                # Swallowed only while unwinding. A connection that cannot run
                # this statement is failing every statement, which is how an
                # interrupt behaves, and raising would replace the migration's
                # own error with the noise that followed it. On the success path
                # it is not swallowed: the connection is about to be handed back
                # to a caller, and handing back one with foreign keys off is an
                # unenforced schema for every write that follows.
                #
                # Raised as a MigrationError rather than the sqlite error it
                # came from. open_db wraps any sqlite3.Error in the generic
                # open failure, whose remedy is to delete graph.db and start
                # fresh, and the migration has already committed by this point.
                if ok:
                    raise MigrationError(
                        "the schema migration committed, and foreign-key "
                        f"enforcement could not be turned back on: {exc}. "
                        "graph.db is migrated and intact; do not delete it. "
                        "Close this connection and open the graph again."
                    ) from exc


def _drop_support_level(conn: sqlite3.Connection) -> None:
    """Rebuild ``claims`` without the support ladder's column.

    The narrowing step the rebuild was built for. It authors nothing: the
    column is gone from the definition a fresh database gets and from the
    column list, so the same two inputs the unchanged rebuild used now describe
    a narrower table, and the copy carries every remaining column across by
    name. Filtering a column out of authored DDL would mean parsing it, and a
    parser that mishandles a CHECK writes a table accepting what the old one
    refused.

    The triggers and the index that named the column go with the old table
    rather than being dropped one by one: the rebuild recreates the current
    schema's guards against the new table, and the current schema no longer has
    them. What a graph loses is the stored word. What it keeps is every claim,
    every signature, and the signed validation envelopes, which outlive the
    ladder because a human attestation was never the same thing as a level.
    """
    # The guards the ladder had are retired here, by name, before the rebuild
    # reaches them. A rebuild replays a trigger or index this release does not
    # manage exactly as it found it, and refuses when one names a column the
    # step is dropping, because a trigger it cannot rewrite is one it must not
    # silently discard. That refusal is right for a trigger somebody else
    # wrote and wrong for the ones this project shipped and is now removing:
    # left to the general rule, no graph written by an older release could
    # upgrade at all, for carrying exactly what that release was supposed to
    # give it. Naming them keeps the refusal intact for everything else.
    for guard in (
        "claims_insert_state_check",
        "claims_update_state_check",
        "claims_signed_promotion_backed",
    ):
        conn.execute(f"DROP TRIGGER IF EXISTS {guard}")
    for index in ("idx_claims_support_level", "idx_claims_convergence_retry"):
        conn.execute(f"DROP INDEX IF EXISTS {index}")
    # The read-order index sorted on the ladder tier before recency. It is
    # recreated from the current schema, on recency alone.
    conn.execute("DROP INDEX IF EXISTS idx_claims_read_order")

    # Named, but only the ones this file actually has. Both columns arrived in
    # different releases, so a graph old enough to predate one of them carries
    # the other alone, and the runner refuses a step that claims to drop a
    # column the table does not hold as readily as one that drops a column it
    # did not name. Anything else unexpected still trips the undeclared check.
    # The new table asks that a row naming a validator carry the envelope that
    # proves one, and the old one never asked it on UPDATE: the check it had
    # fired on support_level alone, and the validation columns are not on the
    # laundering trigger's list either. So a row naming a validator with nothing
    # signed is a legal thing for an older graph to hold and an illegal thing
    # for this one, and it meets the CHECK inside the rebuild, where the only
    # report is the constraint's own text and the only outcome is a rollback.
    # Every later open tries again and fails the same way, so the graph never
    # opens again, and the generic remedy the runner offers, which is that
    # nothing changed and the file should be kept, is true and useless.
    #
    # Found first, named here, and the file left alone. Restore already refuses
    # this row shape with a sentence that says what to do; a migration that ends
    # a graph's life should not say less.
    unsigned = [
        row[0] for row in conn.execute(
            "SELECT claim_id FROM claims WHERE validation_signature IS NULL "
            "AND (validated_by IS NOT NULL OR validated_at IS NOT NULL) "
            "ORDER BY rowid"
        )
    ]
    if unsigned:
        shown = ", ".join(unsigned[:5])
        more = f" and {len(unsigned) - 5} more" if len(unsigned) > 5 else ""
        raise MigrationError(
            f"{len(unsigned)} claim(s) in this graph say a human validated "
            f"them and carry no validation envelope to prove one did: {shown}"
            f"{more}. A validation nobody signed is not a validation, and the "
            "current schema will not store one, so the upgrade stops here "
            "rather than at a constraint inside the rebuild. Nothing has been "
            "changed and graph.db is exactly as it was; do not delete it. "
            "Clear validated_by and validated_at on those claims, or put the "
            "envelope back beside them, then open the graph again."
        )

    live = {row[1] for row in conn.execute("PRAGMA table_info(claims)")}
    _rebuild_table(
        conn, table="claims",
        create_sql=claims_rebuild_sql("claims_new"),
        columns=_CLAIM_COLUMNS,
        drops=tuple(
            name for name in ("support_level", "convergence_retry_needed")
            if name in live
        ),
    )


# from-version -> (to-version, the steps that get there).
#
# One step, because one is all any graph needs. The previous release built the
# runner and registered a rebuild that changed nothing, so the machinery would
# be proven on real graphs before a step that narrows the table depended on it.
# That release was never published, so no graph ever reached the version it
# would have written, and that version describes a table no file on disk has.
# Keeping it would mean carrying a frozen copy of the old definition forever to
# describe a shape nobody holds.
#
# The runner still walks a chain and still refuses a route with a gap before it
# commits anything. A second step, when one is needed, registers here.
_MIGRATIONS: "dict[int, tuple[int, Callable[[sqlite3.Connection], None]]]" = {
    1: (2, _drop_support_level),
}


def _plan_migration(version: int) -> "tuple[int, ...]":
    """The whole route from *version* to current, or refuse before anything runs.

    Every link is checked, not just the first. Checking only the first asks "is
    there a step from here", and a chain with a gap two links along passes that,
    commits the steps before the gap, and then refuses with a message saying
    nothing was changed. The file is left at a version this release will not
    open and the release that wrote it now rejects as too new, which is the
    bricked-forward outcome the pre-check exists to prevent, reached one link
    later.

    Also refuses a route that does not advance, which would loop forever
    committing a rebuild each pass inside an open, and one that overshoots the
    current version, which commits a version this release then treats as from
    the future. Both are one typo in a registry entry.
    """
    seen = []
    at = version
    while at < _SCHEMA_VERSION:
        route = _MIGRATIONS.get(at)
        if route is None:
            raise MigrationError(
                f"graph.db is at user_version={version} and this mareforma "
                f"cannot route it to user_version={_SCHEMA_VERSION}: there is "
                f"no migration from {at}. Nothing has been changed. Open it "
                "with the release that wrote it, or with one that names this "
                "version as a supported upgrade source. Do not delete graph.db."
            )
        to_version = route[0]
        if not at < to_version <= _SCHEMA_VERSION:
            raise MigrationError(
                f"the migration registry routes user_version={at} to "
                f"{to_version}, which does not move forward toward "
                f"{_SCHEMA_VERSION}. Nothing has been changed. This is a "
                "defect in mareforma, not in graph.db; please report it."
            )
        seen.append(to_version)
        at = to_version
    return tuple(seen)


def _migrate_to_current(conn: sqlite3.Connection, version: int) -> int:
    """Walk *version* up to :data:`_SCHEMA_VERSION`, one registered step at a
    time, and return where it got to.

    Each step is its own transaction, so a chain of them stops at the first
    failure with every earlier step committed and the version recording exactly
    that. Refuses a version with no route rather than guessing: a file this code
    has no step for is a file it does not know the shape of.
    """
    started_at = version
    _plan_migration(version)
    while version < _SCHEMA_VERSION:
        to_version, steps = _MIGRATIONS[version]
        try:
            with _upgrade_window(conn):
                _run_migration(
                    conn, to_version=to_version, steps=steps,
                    from_version=version,
                )
        except MigrationError as exc:
            if version == started_at:
                raise
            # Some steps already committed. The step's own message says the
            # graph is at the version it was at when that step began, which is
            # true of the step and false of the graph, so quote the underlying
            # cause rather than the sentence built around it. Saying "unchanged"
            # here would also point at a remedy that cannot work: the release
            # that wrote this file refuses the version the chain has reached.
            cause = exc.__cause__ if exc.__cause__ is not None else exc
            raise MigrationError(
                f"the schema migration reached user_version={version} of "
                f"{_SCHEMA_VERSION} and then failed: {cause}. The steps that "
                f"completed are committed, and graph.db opens at "
                f"{version} with this release. Do not delete it. Re-run the "
                "open to continue from here."
            ) from exc
        # Re-read rather than trusting to_version, because the step may have
        # found another process had already done it. Guard the re-read the same
        # way the registry entry is guarded: a version that did not advance
        # loops here forever, committing a full table rebuild each pass inside
        # an open(), and one past this release is a graph this code must not go
        # on repairing.
        seen = conn.execute("PRAGMA user_version").fetchone()[0]
        if seen > _SCHEMA_VERSION:
            raise MigrationError(
                f"graph.db moved to user_version={seen} while this migration "
                f"was running, past the {_SCHEMA_VERSION} this mareforma "
                "understands. Another process upgraded it. Nothing further has "
                "been changed here. Do not delete graph.db."
            )
        if seen <= version:
            raise MigrationError(
                f"the migration to user_version={to_version} committed and "
                f"graph.db still reads {seen}, so it did not advance. Stopping "
                "rather than repeating the step. Do not delete graph.db; "
                "report this."
            )
        version = seen
    return version


def _ensure_claims_columns_for_upgrade(
    conn: sqlite3.Connection, existing_cols: set[str],
) -> set[str]:
    """Auto-add the claims-table columns introduced in this release.

    Returns the set of columns this call actually added (empty when the db
    is already current, or when a concurrent opener won every ALTER race).
    The caller uses an ``asserter_keyid`` entry to fire the one-time
    legacy-promotion grandfather event exactly once.

    ``predicate_payload``, ``original_signature_bundle``, and
    ``asserter_keyid`` are query-side fields that are NOT part of the
    signed envelope or the chain hash. ALTER TABLE ADD COLUMN with the
    documented defaults leaves every existing row's signed bytes
    byte-identical, so the migration is safe to run on any legacy graph.db.

    Concurrent first-opens race the ALTER: SQLite serialises writes;
    the loser's ALTER fails with ``duplicate column name`` and we
    re-check + return. Same posture as ``_ensure_evidence_lines_columns``.
    """
    # If the claims table itself is missing, there's nothing to ALTER , 
    # let the column-set validation below surface the schema-mismatch
    # error with its actionable message.
    if not existing_cols:
        return set()
    added: set[str] = set()
    upgrades = [
        ("predicate_payload",
         "ALTER TABLE claims ADD COLUMN predicate_payload "
         "TEXT NOT NULL DEFAULT ''"),
        ("original_signature_bundle",
         "ALTER TABLE claims ADD COLUMN original_signature_bundle TEXT"),
        # Denormalized asserter keyid. New rows populate it at write from the
        # signature_bundle; legacy rows stay NULL (not backfilled) so the
        # promotion query's NULL guard treats them as "not a distinct signer."
        ("asserter_keyid",
         "ALTER TABLE claims ADD COLUMN asserter_keyid TEXT"),
        # Observed grounding verdict (computed axis). Added NULL on every
        # existing row; a NULL verdict is omitted from the signed predicate, so
        # this ALTER leaves every existing row's signed bytes byte-identical,
        # exactly like the query-side columns above.
        ("observed_grounding",
         "ALTER TABLE claims ADD COLUMN observed_grounding TEXT"),
    ]
    for col, alter_sql in upgrades:
        if col in existing_cols:
            continue
        try:
            conn.execute(alter_sql)
            conn.commit()
            added.add(col)
        except sqlite3.OperationalError as exc:
            # Re-check: a concurrent process may have won the ALTER
            # race. Duplicate-column-name is benign; any other failure
            # is real. The race-loser did NOT add the column, so it is
            # left out of ``added`` and never fires the grandfather event.
            cols_after = {
                row[1]
                for row in conn.execute("PRAGMA table_info(claims)").fetchall()
            }
            if col in cols_after:
                continue
            raise DatabaseError(
                f"Could not add claims.{col} column: {exc}"
            ) from exc

    # The partial index on asserter_keyid lives in _SCHEMA_SQL, which only runs
    # on a fresh db. An upgraded db gets the column via the ALTER above but
    # never re-runs _SCHEMA_SQL, so create the index here too. asserter_keyid is
    # always present by this point (in existing_cols, or added by the loop
    # above), so this runs unconditionally; IF NOT EXISTS keeps it a no-op once
    # present.
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_claims_asserter_keyid "
            "ON claims(asserter_keyid) WHERE asserter_keyid IS NOT NULL"
        )
        conn.commit()
    except sqlite3.OperationalError:
        # A concurrent opener may be mid-ALTER; the index is a perf
        # optimisation, not a correctness gate, so a transient failure
        # here must not block the open. The next open retries.
        pass

    return added


def _ensure_evidence_lines_columns(conn: sqlite3.Connection) -> None:
    """Add the ``model_lineage`` column to legacy evidence_lines tables.

    The trust-layer evidence tree is additive and not part of the signed
    claim-envelope integrity surface, so in-place ALTER is safe: a NULL
    ``model_lineage`` on an existing line is exactly a line authored without an
    observed model call. CREATE TABLE IF NOT EXISTS on a fresh DB already creates
    the column; this fills the gap on DBs created by older mareforma builds. Runs
    after the additive-tables script, so the table itself is guaranteed present.
    """
    cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(evidence_lines)").fetchall()
    }
    if "model_lineage" not in cols:
        try:
            conn.execute("ALTER TABLE evidence_lines ADD COLUMN model_lineage TEXT")
            conn.commit()
        except sqlite3.OperationalError as exc:
            # Concurrent open: another process won the ALTER race. Re-check
            # before raising, "duplicate column name" is benign.
            cols2 = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(evidence_lines)"
                ).fetchall()
            }
            if "model_lineage" in cols2:
                return
            raise DatabaseError(
                f"Could not add evidence_lines.model_lineage column: {exc}"
            ) from exc


# Policy columns added after the table shipped, with their declarations.
_PROJECT_POLICY_ADDED_COLUMNS = (
    ("strict_promotion_required", "INTEGER NOT NULL DEFAULT 0"),
    ("rekor_declared_at", "TEXT"),
    ("strict_promotion_declared_at", "TEXT"),
)


def _ensure_project_policy_columns(conn: sqlite3.Connection) -> None:
    """Add the columns a legacy policy table predates.

    The flat columns are a read cache over the signed envelope, so the ALTERs
    touch no signed bytes: a policy declared before ``strict_promotion_required``
    existed is a v1 envelope that says nothing about strict promotion, which is
    exactly what the ``DEFAULT 0`` records, and one declared before the
    ``*_declared_at`` columns says nothing about when each flag was declared,
    which is what NULL records. Runs after the additive-tables script, so the
    table itself is guaranteed present. Same posture as
    ``_ensure_evidence_lines_columns``.
    """
    def _columns() -> set[str]:
        return {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(project_policy)"
            ).fetchall()
        }

    cols = _columns()
    for name, decl in _PROJECT_POLICY_ADDED_COLUMNS:
        if name in cols:
            continue
        try:
            conn.execute(
                f"ALTER TABLE project_policy ADD COLUMN {name} {decl}"
            )
            conn.commit()
        except sqlite3.OperationalError as exc:
            # Concurrent open: another process won the ALTER race. Re-check
            # before raising, "duplicate column name" is benign.
            if name in _columns():
                continue
            raise DatabaseError(
                f"Could not add project_policy.{name} column: {exc}"
            ) from exc


def _attach_supports_cache(conn: sqlite3.Connection, root: Path) -> None:
    """Attach the rebuildable claim_supports cache.

    The cache lives outside the versioned schema (separate DB file) so
    the file can be deleted with no consequence beyond a one-time
    rebuild on next open. Errors during attach are surfaced as
    :class:`DatabaseError` so the operator sees a clear remediation
    message rather than a deferred sqlite3 error on the first
    provenance query.
    """
    from mareforma import _supports
    try:
        _supports.attach_cache(conn, root)
    except sqlite3.Error as exc:
        raise DatabaseError(
            f"Could not attach claim_supports cache: {exc}. "
            "Delete .mareforma/claim_supports_cache.db and re-open "
            "the project (the cache is rebuildable from claims.toml "
            "and graph.db; this file is not part of the signed graph)."
        ) from exc


# ---------------------------------------------------------------------------
# Append-only hash chain
# ---------------------------------------------------------------------------

def _chain_input_for_claim(
    claim_fields: dict, evidence: dict | None = None,
) -> bytes:
    """Canonical bytes for the chain hash on a single claim row.

    Uses the in-toto Statement v1 canonical bytes: the exact same
    bytes that get signed (after DSSE PAE wrap). Chain integrity and
    signature integrity bind to one authoritative byte sequence. The
    evidence vector is part of the Statement, so it is part of the
    chain input.
    """
    from mareforma import signing as _signing
    return _signing.canonical_statement(claim_fields, evidence or {})


# Evidence-vector field defaults. The signed predicate carries a plain
# dict; a claim asserted without an explicit vector binds this all-zeros
# shape so the canonical bytes stay stable across releases.
_EVIDENCE_DOWNGRADE_DOMAINS = (
    "risk_of_bias",
    "inconsistency",
    "indirectness",
    "imprecision",
    "publication_bias",
)
_EVIDENCE_UPGRADE_FLAGS = (
    "large_effect",
    "dose_response",
    "opposing_confounding",
)


def _normalize_evidence(evidence: dict | None) -> dict:
    """Project an evidence dict onto the canonical signed-predicate shape.

    Fills every downgrade domain (0), upgrade flag (False), the rationale
    dict, and the reporting_compliance list. ``study_design`` and the
    grounding snapshot are carried only when present, so a claim without
    them produces byte-identical canonical bytes to a legacy claim.

    Every field is type-checked here, because every field signs into the
    immutable predicate and nothing downstream re-reads it: a wrong type is
    refused with a ValueError rather than coerced into a permanent record.
    """
    src = evidence or {}
    out: dict = {}
    for domain in _EVIDENCE_DOWNGRADE_DOMAINS:
        val = src.get(domain, 0)
        if isinstance(val, bool) or not isinstance(val, int) or not -2 <= val <= 0:
            raise ValueError(
                f"evidence downgrade domain {domain!r} must be an integer in "
                f"[-2, 0], got {val!r}: an out-of-range value must not sign into "
                "the immutable predicate"
            )
        out[domain] = val
    for flag in _EVIDENCE_UPGRADE_FLAGS:
        val = src.get(flag, False)
        if not isinstance(val, bool):
            raise ValueError(
                f"evidence upgrade flag {flag!r} must be a bool, got {val!r}: "
                "a non-flag value must not sign into the immutable predicate"
            )
        out[flag] = val
    rationale = src.get("rationale") or {}
    if not isinstance(rationale, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in rationale.items()
    ):
        raise ValueError(
            f"evidence rationale must be a dict of str to str, got "
            f"{rationale!r}: an unreadable justification must not sign into "
            "the immutable predicate"
        )
    out["rationale"] = dict(rationale)
    compliance = src.get("reporting_compliance") or ()
    # A bare str is the silent case: list("CONSORT") splats into seven
    # single-letter guidelines and signs compliance with every one of them.
    if not isinstance(compliance, (list, tuple)) or not all(
        isinstance(item, str) for item in compliance
    ):
        raise ValueError(
            f"evidence reporting_compliance must be a list of guideline "
            f"names, got {compliance!r}: pass ['CONSORT'], not 'CONSORT', so "
            "a permanent record does not claim compliance the asserter never "
            "meant"
        )
    out["reporting_compliance"] = list(compliance)
    study_design = src.get("study_design")
    if study_design is not None:
        if not isinstance(study_design, str):
            raise ValueError(
                f"evidence study_design must be a str, got {study_design!r}: "
                "a non-string value must not sign into the immutable predicate"
            )
        out["study_design"] = study_design
    if src.get("grounding_score") is not None:
        raw = src["grounding_score"]
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise ValueError(
                f"grounding_score must be a number in [0, 1], got {raw!r}: a "
                "flag is not a score and must not sign into the predicate"
            )
        score = float(raw)
        if not 0.0 <= score <= 1.0:
            raise ValueError(
                f"grounding_score must be in [0, 1], got {score!r}: an evidence "
                "value out of range must not sign into the immutable predicate"
            )
        rationale = src.get("grounding_rationale")
        if not (isinstance(rationale, str) and rationale.strip()):
            raise ValueError(
                "grounding_score requires a non-empty grounding_rationale: a "
                "scored claim must say why, not carry a bare number in the record"
            )
        out["grounding_score"] = score
        out["grounding_rationale"] = rationale
    return out


def _compute_prev_hash(
    conn: sqlite3.Connection,
    claim_fields: dict,
    evidence: dict | None = None,
) -> str:
    """Compute the new ``prev_hash`` value for a claim about to be inserted.

    The new chain link is ``sha256(prev_chain_link || canonical_statement_bytes)``.
    For the genesis row (no prior rows), the prior link is empty bytes.

    MUST be called inside ``BEGIN IMMEDIATE``: the SELECT-then-INSERT
    pattern depends on the write lock to prevent two writers from
    branching the chain on the same predecessor.
    """
    row = conn.execute(
        "SELECT prev_hash FROM claims ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    prev = (row["prev_hash"] or "").encode("ascii") if row else b""
    chain_input = _chain_input_for_claim(claim_fields, evidence)
    return hashlib.sha256(prev + chain_input).hexdigest()


# ---------------------------------------------------------------------------
# Cycle / self-loop detection
# ---------------------------------------------------------------------------

# Pattern for the UUID format we generate via uuid.uuid4(). Strict
# UUIDv4, version nibble is exactly ``4`` and variant nibble is one
# of {8, 9, a, b} (RFC 4122 §4.1.1, "10xx" binary variant). Tightening
# from the looser "any hex-shape UUID" rejects v1/v3/v5/zero UUIDs in
# ``supports[]`` as non-graph-nodes, which makes the shape-vs-version
# check explicit instead of accidental. Strings in ``supports[]`` that
# DON'T match are external references (DOIs etc.) and do not
# participate in cycle checking, they are not graph nodes.
_CLAIM_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)

# Cap on the number of DISTINCT nodes the acyclicity walk may reach before it
# gives up. It is the LIMIT on the walk's recursive CTE, so the walk stops here
# rather than being measured after the fact. With node-dedup the walk always
# terminates, so this is not a correctness bound, it is a runaway guard against
# an absurdly large reachable set. A node is a claim_id or an external
# reference carried in a reached claim's supports_json. Generous: a legitimate
# fan-out in a mature graph can reach many thousands of claims at shallow
# depth, and exceeding the cap is reported as GraphTooLargeError, never as a
# cycle.
_REACHABLE_CLAIM_CAP = 100_000


def _is_claim_id(value: str) -> bool:
    return bool(_CLAIM_ID_RE.match(value))


def _validate_claim_text(text: str) -> str:
    """Enforce the write-side text invariants and return the clean text.

    Shared by ``add_claim`` and ``update_claim`` so the two write paths cannot
    drift: both reject empty text, cap the length at ``_MAX_CLAIM_TEXT_LEN``, and
    run sanitize-on-write (stripping zero-width / bidi / tag-plane codepoints)
    BEFORE the text is stored or signed. Any consumer reading ``text`` directly
    then sees a clean, bounded string.
    """
    if not text or not text.strip():
        raise ValueError("Claim text cannot be empty.")
    if len(text) > _MAX_CLAIM_TEXT_LEN:
        raise ValueError(
            f"Claim text exceeds {_MAX_CLAIM_TEXT_LEN}-char cap "
            f"(got {len(text)}). Split the finding into smaller claims "
            "and link them via supports=[]."
        )
    from mareforma import prompt_safety as _ps
    # Strip AFTER sanitizing too: none of the codepoints the sanitizer deletes
    # are Python whitespace, so the first strip cannot reach the whitespace they
    # hide. Re-stripping keeps one canonical string flowing to the column, the
    # signature and every comparison.
    cleaned = _ps.sanitize_for_llm(text.strip()).strip()
    if not cleaned:
        raise ValueError(
            "Claim text became empty after stripping zero-width / control "
            "characters. The input contained no visible content."
        )
    return cleaned


def _refuse_supports_contradicts_overlap(
    supports: "list | None", contradicts: "list | None",
) -> None:
    """Refuse a claim that supports AND contradicts the same upstream UUID.

    A row that simultaneously builds on and refutes the same upstream is
    logically incoherent (a reader cannot tell which relation is real). Only
    UUID-shaped refs are compared; DOI / external string refs are out of scope.
    Shared by ``add_claim`` and ``update_claim`` (an edit to either side can
    create the overlap) so the gate cannot drift between the two write paths.
    """
    if supports and contradicts:
        sup_ids = {s for s in supports if isinstance(s, str) and _is_claim_id(s)}
        con_ids = {c for c in contradicts if isinstance(c, str) and _is_claim_id(c)}
        overlap = sup_ids & con_ids
        if overlap:
            raise ValueError(
                f"supports[] and contradicts[] reference the same "
                f"upstream claim(s): {sorted(overlap)}. A claim that "
                "simultaneously builds on and refutes the same upstream "
                "is logically incoherent; pick one relation."
            )


# Three-way classification of ``supports[]`` and ``contradicts[]`` entries.
# The flat string API stays, mareforma auto-classifies each entry so
# JSON-LD export, audit helpers, and future query surfaces can distinguish
# the three semantic types without forcing callers to wrap strings.
SUPPORT_TYPE_CLAIM = "claim"
SUPPORT_TYPE_DOI = "doi"
SUPPORT_TYPE_EXTERNAL = "external"

_VALID_SUPPORT_TYPES = (
    SUPPORT_TYPE_CLAIM,
    SUPPORT_TYPE_DOI,
    SUPPORT_TYPE_EXTERNAL,
)


def classify_support(value: str) -> str:
    """Return the type tag for a single ``supports[]`` entry.

    Three buckets:

      * ``"claim"``: strict UUIDv4 shape, candidate graph-node edge.
        Independence counting and cycle detection walk these.
      * ``"doi"``: DOI form (``10.<registrant>/<suffix>``) per Crossref +
        DataCite syntax; not a shared anchor two claims can converge on
        (the upstream is not a local claim).
      * ``"external"``: anything else. Free-form strings (URLs, ORCID
        ids, lab-internal references). Stored verbatim, not walked, not
        resolved.

    Classification is deterministic and regex-only: no network, no
    database lookup. The same string always yields the same tag.
    """
    if not isinstance(value, str):
        return SUPPORT_TYPE_EXTERNAL
    if _is_claim_id(value):
        return SUPPORT_TYPE_CLAIM
    if is_doi(value):
        return SUPPORT_TYPE_DOI
    return SUPPORT_TYPE_EXTERNAL


def classify_supports(values: list[str]) -> list[dict[str, str]]:
    """Classify every entry in a ``supports[]`` / ``contradicts[]`` list.

    Returns ``[{"value": <original>, "type": <one of SUPPORT_TYPE_*>}, ...]``
    in input order. Empty list → empty list.

    Used by:

      * the JSON-LD exporter, which emits each entry under a typed
        predicate (``mare:supportsClaim``, ``mare:supportsDoi``,
        ``mare:supportsReference``) so consumers can distinguish a
        local graph edge from an external citation;
      * operator audits: pair with :func:`find_dangling_supports` for a
        complete view of which entries are graph nodes, which are
        external references, and which are dangling claim_ids that point
        nowhere.
    """
    return [{"value": v, "type": classify_support(v)} for v in values]


def _check_no_cycle(
    conn: sqlite3.Connection,
    new_claim_id: str,
    supports: list[str],
) -> None:
    """Raise :class:`CycleDetectedError` if extending the graph with
    ``new_claim_id → supports`` would create a cycle.

    Algorithm: one recursive-CTE reachability walk. Seed the walk with
    the new claim's ``supports[]`` and follow each reached claim's own
    ``supports[]`` forward; if the walk ever reaches ``new_claim_id`` the
    new edge closes a cycle. A single query replaces the former
    per-node DFS (one ``SELECT`` per visited claim), so the cost no longer
    scales with the depth of the ancestral chain.

    The walk reads ``claims.supports_json`` directly (the authoritative
    edge source), not the ``supports_cache`` sidecar: cycle detection must
    stay correct on connections where that cache is not attached
    (``open_db_from_db_path`` on a non-conventional path).

    Why reachability (not Tarjan's SCC): the existing graph is acyclic by
    induction (we reject cycles on every write). A new claim has no
    incoming edges at INSERT time, so the only cycle it can create is one
    that goes ``new → supports → ... → new``. A forward walk from each
    support entry is sufficient. For ``update_claim``, the new edge is the
    changed ``supports[]``; same algorithm applies.

    DOI strings in ``supports[]`` are external references, they are kept
    as seeds but never match a ``claim_id``, so they drop out of the walk.
    ``UNION`` dedupes the reachable set by ``node``, so each claim is
    visited once: the walk is O(reachable claims), it terminates even if a
    stored cycle exists among other claims (a DB-write adversary could
    plant one), and such a stored cycle never turns into a spurious verdict
    here, only ``new_claim_id`` being reachable from the seeds is a cycle.
    The walk itself stops at ``_REACHABLE_CLAIM_CAP`` nodes: the ``reach``
    body carries a ``LIMIT`` of one past the cap, which short-circuits row
    generation, so a runaway graph costs the cap rather than its full size.
    Coming back with that extra row means the walk was truncated and raises
    :class:`GraphTooLargeError` (a distinct condition, never a cycle), so a
    legitimate wide fan-out is not mislabeled. ``hit`` is checked first, so a
    cycle inside the truncated prefix is still reported as a cycle; one whose
    target sits past the truncation point reads as GraphTooLargeError. Either
    way the write is refused.
    """
    seeds = [s for s in supports if _is_claim_id(s)]
    if not seeds:
        return
    if new_claim_id in seeds:
        raise CycleDetectedError(
            f"Claim {new_claim_id!r} cannot support itself "
            f"(self-loop in supports[])."
        )

    row = conn.execute(
        """
        WITH RECURSIVE reach(node) AS (
            SELECT value FROM json_each(?)
            UNION
            SELECT je.value
              FROM reach r
              JOIN claims c ON c.claim_id = r.node
              JOIN json_each(c.supports_json) je
             WHERE c.supports_json IS NOT NULL
               AND json_valid(c.supports_json)
            LIMIT ?
        )
        SELECT
            MAX(CASE WHEN node = ? THEN 1 ELSE 0 END) AS hit,
            COUNT(*) AS visited
        FROM reach
        """,
        (json.dumps(seeds), _REACHABLE_CLAIM_CAP + 1, new_claim_id),
    ).fetchone()

    if row is not None and row["hit"]:
        raise CycleDetectedError(
            f"Inserting/updating {new_claim_id!r} with the given "
            "supports[] would create a cycle."
        )
    if row is not None and (row["visited"] or 0) > _REACHABLE_CLAIM_CAP:
        raise GraphTooLargeError(
            f"supports[] reaches more than {_REACHABLE_CLAIM_CAP} distinct "
            "upstream nodes; the acyclicity walk stopped at the cap. This is "
            "not a cycle the walk found, the reachable graph is "
            "extraordinarily large and nothing past the cap was walked. "
            "Investigate before relaxing the cap."
        )


def _signed_delete_error(
    exc: sqlite3.IntegrityError, claim_id: str | None = None,
) -> "MareformaError":
    """Translate a delete-blocked IntegrityError into a typed error.

    The ``claims_signed_no_delete`` trigger raises
    ``mareforma:append_only:signed_claim_no_delete`` when a caller tries to
    delete a signed claim (its signature + Rekor entry + chain hash attest
    the assertion; a delete would let a DB-write process forget it). Map
    that marker to the documented :class:`SignedClaimImmutableError` so the
    delete path surfaces the same typed failure as the update path instead
    of a raw sqlite3.IntegrityError. Any other IntegrityError is a genuine
    DB fault and becomes :class:`DatabaseError`.
    """
    msg = str(exc)
    if "mareforma:append_only:signed_claim_no_delete" in msg:
        target = f" '{claim_id}'" if claim_id else ""
        return SignedClaimImmutableError(
            f"Signed claim{target} cannot be deleted: its signature commits "
            "the assertion to the append-only chain. To withdraw it, assert "
            "a retraction (status='retracted') that cites the claim via "
            "contradicts=[...]."
        )
    detail = f" claim '{claim_id}'" if claim_id else "s"
    return DatabaseError(f"Failed to delete claim{detail}: {exc}")


def _state_error_from_integrity(
    exc: sqlite3.IntegrityError,
) -> "MareformaError | None":
    """Translate trigger / UNIQUE violations into mareforma exceptions.

    Returns ``None`` if the IntegrityError is not one of the patterns
    we own; callers should re-raise as ``DatabaseError`` then.
    """
    msg = str(exc)
    if "mareforma:state:" in msg:
        # Extract the suffix after the prefix for callers that want to
        # pattern-match. The full SQLite message looks like:
        #   IntegrityError: mareforma:state:illegal_transition:from_preliminary
        # (Static suffixes only, SQLite < 3.46 rejects `'prefix:' || NEW.x`
        # in RAISE() as a syntax error. See the schema preamble.)
        marker = "mareforma:state:"
        suffix = msg[msg.index(marker) + len(marker):]
        return IllegalStateTransitionError(f"State transition refused: {suffix}")
    if "idx_claims_prev_hash" in msg or (
        "UNIQUE constraint failed" in msg and "prev_hash" in msg
    ):
        return ChainIntegrityError(
            "prev_hash UNIQUE violation, two writers raced past BEGIN "
            "IMMEDIATE, or a manual SQL tamper re-used an existing chain "
            "link. Treat as corruption, not a retry."
        )
    return None


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


def normalize_artifact_hash(value: str | None) -> str | None:
    """Validate and lowercase a SHA256 hex digest. Returns None for None.

    A claim's ``artifact_hash`` is the SHA256 of the output bytes that
    backed the claim (a figure, a CSV, a pickled model). It is signed
    into the claim envelope and read as a secondary collapse check: two
    peers citing the same upstream that both supply an EQUAL hash are
    one line of evidence (a byte-identical rerun is not corroboration)
    and do not promote on their own. Distinct hashes, or an absent hash
    on either side, never block the distinct-signer axis.

    Accepts canonical hex digests only: no ``sha256:`` prefix, no
    base64, no whitespace. Case is normalised to lowercase so two
    spellings of the same digest compare equal when two peers are compared.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(
            f"artifact_hash must be a string or None, got {type(value).__name__}."
        )
    candidate = value.strip().lower()
    if not _SHA256_HEX_RE.match(candidate):
        raise ValueError(
            f"artifact_hash {value!r} is not a 64-character lowercase SHA256 "
            "hex digest. Compute with hashlib.sha256(bytes).hexdigest()."
        )
    return candidate


# ---------------------------------------------------------------------------
# Claims
# ---------------------------------------------------------------------------

def _reconcile_idempotency_row(
    row: sqlite3.Row,
    idempotency_key: str,
    text: str,
    classification: str,
    generated_by: str | None,
    supports: list[str] | None,
    contradicts: list[str] | None,
    source_name: str | None,
    artifact_hash: str | None,
    evidence_dict: dict,
    observed_grounding: dict | None,
    original_signature_bundle: str | None = None,
) -> str:
    """Compare a found row against the current call's semantic fields.

    Same key + every semantic field matching → return the existing
    ``claim_id`` (true retry). Any divergence → raise
    :class:`IdempotencyConflictError` listing every mismatched field.

    Called from two places:

    1. The pre-INSERT idempotency SELECT, the happy path. Catches the
       common case where a deterministic agent retries an in-flight
       assertion after a crash.
    2. The post-INSERT race-recovery path. The pre-SELECT runs outside
       BEGIN IMMEDIATE, so two concurrent writers with the same key
       both see "no existing row" and both proceed to INSERT. SQLite's
       ``idx_claims_idempotency_key`` UNIQUE index makes the second
       INSERT fail; the loser re-SELECTs and routes through this
       helper to deliver the same epistemic error as the happy path,
       not a bare ``sqlite3.IntegrityError``.
    """
    expected_supports = json.dumps(supports or [])
    expected_contradicts = json.dumps(contradicts or [])
    mismatches: list[str] = []
    if row["text"] != text:
        mismatches.append("text")
    if row["classification"] != classification:
        mismatches.append("classification")
    if row["generated_by"] != generated_by:
        mismatches.append("generated_by")
    if row["supports_json"] != expected_supports:
        mismatches.append("supports")
    if row["contradicts_json"] != expected_contradicts:
        mismatches.append("contradicts")
    if row["source_name"] != source_name:
        mismatches.append("source_name")
    if row["artifact_hash"] != artifact_hash:
        mismatches.append("artifact_hash")
    # The evidence vector and the observed-grounding verdict sign into the
    # envelope and into the chain hash, so they are semantic identity, not
    # denormalisation: a retry that carries a different verdict is a different
    # claim, and returning the first row's id would hand the caller a grounding
    # it did not compute. Both columns hold canonical JSON, so compare the
    # parsed records and key ordering cannot fake a mismatch on a true retry.
    if _json_object(row["evidence_json"], {}) != evidence_dict:
        mismatches.append("evidence")
    if _json_object(row["observed_grounding"]) != observed_grounding:
        mismatches.append("observed_grounding")
    # predicate_payload is intentionally NOT compared. It is a query-
    # side denormalisation that does not enter the signed envelope or
    # the chain hash; treating it as a semantic field for idempotency
    # would mean federation exports (which drop predicate_payload)
    # round-trip differently than direct asserts. The signed bytes
    # are the only authoritative semantic identity.
    expected_original = _canonical_envelope(original_signature_bundle)
    stored_original = _canonical_envelope(row["original_signature_bundle"])
    if stored_original != expected_original:
        mismatches.append("original_signature_bundle")
    if mismatches:
        raise IdempotencyConflictError(
            f"idempotency_key={idempotency_key!r} already exists "
            f"with different {', '.join(mismatches)}. Use a "
            "different idempotency_key: silently merging two "
            "different claims into one row would discard the "
            "second author's content. To record that two groups "
            "found the same thing, assert two separate claims "
            "signed by distinct keys that cite the same upstream "
            "claim in supports[]."
        )
    return row["claim_id"]


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
    artifact_hash: str | None = None,
    evidence: "object | None" = None,
    signer: "object | None" = None,
    rekor_url: str | None = None,
    require_rekor: bool = False,
    trust_insecure_rekor: bool = False,
    on_convergence_error: "Callable[[Exception], None] | None" = None,
    rekor_log_pubkey_pem: bytes | None = None,
    predicate_payload: dict | None = None,
    original_signature_bundle: str | None = None,
    observed_grounding: dict | None = None,
    finding_record: dict | None = None,
) -> str:
    """Insert a new claim and return its claim_id.

    Returns the existing claim_id without inserting if idempotency_key
    already exists. The insert is the whole of it: nothing is derived from the
    new row and written back to it, and nothing on any other row changes. Two
    claims citing a shared upstream under distinct asserter_keyid values are
    what convergence looks like, and a reader counts that off the rows rather
    than being told it by them.

    Parameters
    ----------
    classification:
        'INFERRED' | 'ANALYTICAL' | 'DERIVED'
    idempotency_key:
        Retry-safe writes: same key returns the same claim_id.
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
        True if any DOI in supports[]/contradicts[] failed to resolve. The
        claim is stored and served; the flag says its citations did not all
        resolve, which a reader weighs for itself.
    artifact_hash:
        Optional SHA256 hex digest of the artifact bytes (figure, CSV,
        model) backing this claim. When supplied it is included in the
        signed payload and read as a secondary collapse check: peers
        sharing an upstream that both supply an EQUAL hash are one line of
        evidence, whatever keys signed them, and the independence count says
        so. Distinct hashes, or ``None`` on either side, leave the
        distinct-signer reading alone.
    signer:
        Optional Ed25519 private key. When provided, the claim is signed
        before INSERT and the signature envelope is persisted to the
        ``signature_bundle`` column. ``None`` skips signing.
    rekor_url:
        When set, every signed claim is submitted to the Rekor
        transparency log at this URL. Success augments the signature
        bundle with the log entry coordinates and sets
        ``transparency_logged=1``. Failure persists ``transparency_logged=0``,
        so the claim carries no public log entry until
        :meth:`EpistemicGraph.refresh_unsigned` retries.
    require_rekor:
        When True, raise :class:`SigningError` if the initial Rekor
        submission fails. Use for production high-assurance flows.
    trust_insecure_rekor:
        The session opt-in that lets ``rekor_url`` point at a private
        Rekor on a non-public address. Forwarded to the SSRF / scheme
        re-validation the submit and fetch calls run.

    Raises
    ------
    ValueError
        If classification or status are invalid.
    SigningError
        If ``require_rekor=True`` and the Rekor submission fails.
    """
    # Enforce the empty / cap / sanitize-on-write invariants. Shared with
    # update_claim so the two write paths cannot drift.
    text = _validate_claim_text(text)
    if classification not in VALID_CLASSIFICATIONS:
        raise ValueError(
            f"Unknown classification '{classification}'. "
            f"Use one of: {', '.join(VALID_CLASSIFICATIONS)}"
        )
    validate_status(status)
    artifact_hash = normalize_artifact_hash(artifact_hash)

    # Callers can supply a populated evidence-vector dict via the ``evidence``
    # parameter, the asserter's confidence in the evidence backing this claim.
    # Default all-zeros means the asserter flagged no quality concerns;
    # downstream readers should interpret a default-zero vector as "asserter
    # made no claim about quality," not as "evidence is high-quality."
    # Normalised here, ahead of the idempotency check, because the normalised
    # vector is what that check compares and what the INSERT stores.
    if evidence is not None and not isinstance(evidence, dict):
        raise TypeError(
            f"evidence must be a dict or None; got {type(evidence).__name__}"
        )
    evidence_dict = _normalize_evidence(evidence)
    evidence_json = json.dumps(
        evidence_dict, sort_keys=True, separators=(",", ":"),
    )

    # Idempotency check, return existing claim_id if key already present.
    # Strict contract: same key MUST match on every semantic field. True
    # retries pass silently; anything else raises IdempotencyConflictError.
    #
    # Prior behavior, match on artifact_hash only and silently return the
    # existing claim_id, was anti-epistemic: a second caller's text and
    # generated_by were discarded into the first caller's row, collapsing
    # what should have been two independent claims into one. The
    # "convergence convention" documented around this primitive actively
    # destroyed the thing a reader counts (distinct signer identities
    # converging on a shared upstream). The correct path for cross-lab
    # convergence is two separate claims that share an entry in supports[]
    # with distinct asserter_keyid values. Idempotency_key is retry-safety
    # only.
    if idempotency_key is not None:
        try:
            row = conn.execute(
                "SELECT claim_id, text, classification, generated_by, "
                "supports_json, contradicts_json, source_name, artifact_hash, "
                "evidence_json, observed_grounding, "
                "predicate_payload, original_signature_bundle "
                "FROM claims WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row:
                existing_id = _reconcile_idempotency_row(
                    row, idempotency_key, text, classification, generated_by,
                    supports, contradicts, source_name, artifact_hash,
                    evidence_dict, observed_grounding,
                    original_signature_bundle=original_signature_bundle,
                )
                return existing_id
        except sqlite3.OperationalError as exc:
            raise DatabaseError(f"Idempotency check failed: {exc}") from exc

    claim_id = str(uuid.uuid4())
    now = _now()
    supports_json = json.dumps(supports or [])
    contradicts_json = json.dumps(contradicts or [])

    # Refuse a claim that simultaneously supports AND contradicts the same
    # upstream, the row would be logically incoherent (downstream readers
    # cannot tell which interpretation is "real"). Shared with update_claim.
    _refuse_supports_contradicts_overlap(supports, contradicts)

    # Cycle / self-loop check on supports[]. DOI entries are external
    # references and not graph nodes, _check_no_cycle filters them
    # out. The walk runs before signing and INSERT so we don't strand
    # half-built state on rejection.
    _check_no_cycle(conn, claim_id, supports or [])

    # Sign the claim if a signer was supplied. The signature is bound to the
    # in-toto Statement v1 wrapping claim fields + the evidence vector, so
    # any later tamper (text edit, support reattribution, evidence override)
    # breaks verification.
    signature_bundle: str | None = None
    envelope: dict | None = None
    statement_cid: str | None = None
    if signer is not None:
        from mareforma import signing as _signing
        from mareforma import _statement as _stmt
        claim_fields = {
            "claim_id": claim_id,
            "text": text,
            "classification": classification,
            "generated_by": generated_by,
            "supports": supports or [],
            "contradicts": contradicts or [],
            "source_name": source_name,
            "artifact_hash": artifact_hash,
            "created_at": now,
        }
        # Bind the observed verdict into the signed bytes only when one was
        # recorded. Absent → the key never enters claim_fields, so the signed
        # statement and its cid are byte-identical to a pre-observer claim.
        if observed_grounding is not None:
            claim_fields["observed_grounding"] = observed_grounding
        # Bind a finding's verdict inputs into the signed bytes only when the
        # caller recorded them. Absent → the key never enters claim_fields, so a
        # non-finding claim and a legacy finding sign to byte-identical bytes.
        if finding_record is not None:
            claim_fields["finding_record"] = finding_record
        envelope = _signing.sign_claim(
            claim_fields, signer, evidence=evidence_dict,
        )
        signature_bundle = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
        statement_cid = _stmt.statement_cid(
            _stmt.build_statement(
                claim_id=claim_fields["claim_id"],
                text=claim_fields["text"],
                classification=claim_fields["classification"],
                generated_by=claim_fields["generated_by"],
                supports=claim_fields["supports"],
                contradicts=claim_fields["contradicts"],
                source_name=claim_fields["source_name"],
                artifact_hash=claim_fields["artifact_hash"],
                created_at=claim_fields["created_at"],
                evidence=evidence_dict,
                observed_grounding=observed_grounding,
                finding_record=finding_record,
            )
        )

    # ``transparency_logged`` defaults to 1 (ready). We flip it to 0 only when
    # Rekor is enabled AND we have something to submit, the row then waits
    # for either a successful submission below or a refresh_unsigned() retry.
    rekor_enabled = rekor_url is not None and signer is not None and envelope is not None
    transparency_logged = 0 if rekor_enabled else 1

    # BEGIN IMMEDIATE: serialize the read-latest-chain-link + INSERT so
    # two writers cannot branch the append-only hash chain. Defaults
    # would let them race past the SELECT and both insert with the same
    # prev_hash, splitting the chain silently, the UNIQUE index on
    # prev_hash catches that case as a backstop, but BEGIN IMMEDIATE is
    # the primary defense.
    chain_fields = {
        "claim_id": claim_id,
        "text": text,
        "classification": classification,
        "generated_by": generated_by,
        "supports": supports or [],
        "contradicts": contradicts or [],
        "source_name": source_name,
        "artifact_hash": artifact_hash,
        "created_at": now,
    }
    # The chain hash binds the same optional field the signature does, so the
    # verdict is tamper-evident on the append-only chain as well. Absent when no
    # verdict was recorded, keeping the chain link identical for pre-observer
    # claims (the chain input is the canonical statement, which omits the key).
    if observed_grounding is not None:
        chain_fields["observed_grounding"] = observed_grounding
    # The chain binds the finding record the same way, so a finding's verdict
    # inputs are tamper-evident on the append-only chain too. Absent when there
    # is none, keeping the chain link identical for non-finding and legacy rows.
    if finding_record is not None:
        chain_fields["finding_record"] = finding_record
    # BEGIN IMMEDIATE is only valid when no transaction is currently
    # open. Python's default sqlite3 isolation_level='' auto-starts a
    # transaction before DML, so callers that already wrote within the
    # same connection will be in-transaction when they reach us. In
    # that case the caller's transaction supplies the serialization;
    # our SELECT runs inside their snapshot and the chain stays linear.
    _own_transaction = not conn.in_transaction
    # A claim is born with no validation. The one path that used to write one
    # at insert time was the seed anchor, and it is gone: it existed to place a
    # claim above the first rung so the rung below it was reachable, and there
    # are no rungs. A validation arrives later, through validate_claim, or not
    # at all.
    initial_validation_signature = None
    initial_validated_at = None
    initial_validator_keyid = None
    # Denormalize the asserter keyid from the signed envelope so the
    # trust-layer independence count reads an indexed column rather than
    # walking the bundle JSON. The
    # signature_bundle stays authoritative. NULL on unsigned claims.
    asserter_keyid = _extract_signature_bundle_keyid(signature_bundle)
    try:
        if _own_transaction:
            conn.execute("BEGIN IMMEDIATE")
        prev_hash = _compute_prev_hash(conn, chain_fields, evidence_dict)
        conn.execute(
            """
            INSERT INTO claims
                (claim_id, text, classification, idempotency_key,
                 status, source_name, generated_by,
                 supports_json, contradicts_json, unresolved,
                 signature_bundle, transparency_logged,
                 validation_signature, validator_keyid, asserter_keyid,
                 validated_at,
                 artifact_hash, prev_hash,
                 ev_risk_of_bias, ev_inconsistency, ev_indirectness,
                 ev_imprecision, ev_pub_bias,
                 evidence_json, statement_cid,
                 predicate_payload, original_signature_bundle,
                 observed_grounding,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim_id, text, classification, idempotency_key,
                status, source_name, generated_by,
                supports_json, contradicts_json, 1 if unresolved else 0,
                signature_bundle, transparency_logged,
                initial_validation_signature, initial_validator_keyid,
                asserter_keyid,
                initial_validated_at,
                artifact_hash, prev_hash,
                evidence_dict["risk_of_bias"], evidence_dict["inconsistency"],
                evidence_dict["indirectness"], evidence_dict["imprecision"],
                evidence_dict["publication_bias"],
                evidence_json, statement_cid,
                _serialize_predicate_payload(predicate_payload),
                _canonical_envelope(original_signature_bundle),
                _serialize_observed_grounding(observed_grounding),
                now, now,
            ),
        )
        # Maintain claim_supports rebuildable cache inside the same
        # transaction so the edge rows and the main-claim INSERT commit
        # together on the normal path. The cache is an attached WAL
        # database, so SQLite commits it separately and a crash between
        # the two commits can write one without the other. That torn
        # write moves the claim count out of step with the stamped one,
        # and the next open rebuilds the cache.
        from mareforma import _supports
        _supports.record_supports_edges(conn, claim_id, supports)
        # Inside the claim's own transaction: an attestation without its claim
        # attests nothing, and a claim whose attestation did not land would read
        # as a declared verdict on the next open.
        _write_grounding_attestation(
            conn, claim_id=claim_id, statement_cid=statement_cid,
            record=observed_grounding, signer=signer,
            asserter_keyid=asserter_keyid, created_at=now,
        )
        if _own_transaction:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        if _own_transaction:
            conn.rollback()
        # Race-loss recovery: two concurrent writers with the same
        # idempotency_key both passed the pre-INSERT SELECT (it runs
        # outside BEGIN IMMEDIATE), and the second INSERT tripped the
        # UNIQUE index on claims.idempotency_key. Re-SELECT and route
        # through the same comparison helper as the happy path so the
        # loser gets IdempotencyConflictError-with-field-list (true
        # retry) or a clean return (everything matched), not a bare
        # IntegrityError. SQLite reports the failure as
        # "UNIQUE constraint failed: claims.idempotency_key", match on
        # the qualified column name rather than the index name.
        exc_msg = str(exc)
        if (
            idempotency_key is not None
            and "UNIQUE constraint failed" in exc_msg
            and "claims.idempotency_key" in exc_msg
        ):
            try:
                row = conn.execute(
                    "SELECT claim_id, text, classification, generated_by, "
                    "supports_json, contradicts_json, source_name, "
                    "artifact_hash, evidence_json, observed_grounding, "
                    "predicate_payload, original_signature_bundle "
                    "FROM claims WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
            except sqlite3.OperationalError as fetch_exc:
                raise DatabaseError(
                    f"Idempotency race recovery failed: {fetch_exc}",
                ) from fetch_exc
            if row is not None:
                return _reconcile_idempotency_row(
                    row, idempotency_key, text, classification, generated_by,
                    supports, contradicts, source_name, artifact_hash,
                    evidence_dict, observed_grounding,
                    original_signature_bundle=original_signature_bundle,
                )
        translated = _state_error_from_integrity(exc)
        if translated is not None:
            raise translated from exc
        raise DatabaseError(f"Failed to add claim: {exc}") from exc
    except sqlite3.OperationalError as exc:
        if _own_transaction:
            conn.rollback()
        raise DatabaseError(f"Failed to add claim: {exc}") from exc

    # Attempt Rekor submission. The saga (submit → sidecar → row UPDATE)
    # is its own concern; the helper returns the new transparency_logged
    # value the INSERT below stores.
    if rekor_enabled:
        transparency_logged = _attempt_rekor_saga(
            conn,
            root,
            claim_id=claim_id,
            envelope=envelope,
            signer=signer,
            rekor_url=rekor_url,
            require_rekor=require_rekor,
            trust_insecure_rekor=trust_insecure_rekor,
            rekor_log_pubkey_pem=rekor_log_pubkey_pem,
            own_transaction=_own_transaction,
        )

    # Snapshot committed state only. When this call joined a caller's open
    # transaction the rows are not committed yet, so backing up here would put a
    # claim in the DR artifact that a caller rollback then erases from the DB.
    # The owning caller runs the backup after it commits (submit_finding does).
    if _own_transaction:
        _backup_claims_toml(conn, root)
    return claim_id


def _claim_model_lineage(
    conn: sqlite3.Connection, claim_id: str,
) -> "dict | None":
    """The model/method lineage recorded on a claim's finding, or None.

    A finding-derived claim carries its authoring scope's lineage on its evidence
    lines (written identically on every line), so the first non-NULL value
    represents it. A plain claim with no finding, every converging peer in the
    claims graph, has none, which reads as absent (no model constraint). Any missing
    table (a schema without the evidence tree) or unparseable value also reads as
    absent, never as a fabricated distinct model.
    """
    try:
        row = conn.execute(
            "SELECT el.model_lineage FROM findings f "
            "JOIN evidence_lines el ON el.finding_id = f.finding_id "
            "WHERE f.claim_id = ? AND el.model_lineage IS NOT NULL LIMIT 1",
            (claim_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None or row["model_lineage"] is None:
        return None
    try:
        return json.loads(row["model_lineage"])
    except (ValueError, TypeError):
        return None


def find_dangling_supports(conn: sqlite3.Connection) -> list[dict]:
    """Return UUID-shaped ``supports[]`` entries that point to no local claim.

    A ``supports`` entry can be:

      * a UUID-shaped string: interpreted by mareforma as a claim_id;
      * a DOI like ``10.1234/abc``: an external reference;
      * any other free-form string: also treated as external.

    Only UUID-shaped entries can plausibly point at a local claim and so
    only those are checked. A dangling reference is not necessarily a
    bug: it could legitimately reference a claim from another project,
    a not-yet-asserted upstream, or a DOI mistyped as a UUID. But
    operators auditing graph integrity want a single query that surfaces
    every such hanging arrow, so they can decide case by case.

    Returns a list of ``{"claim_id", "dangling_ref"}`` dicts, sorted by
    ``claim_id`` then ``dangling_ref`` for deterministic output. Returns
    an empty list when nothing is dangling.

    A dangling reference points at no claim, so nothing counts it as a shared
    anchor and it cannot make two claims look convergent. This helper is for
    auditing, not enforcement.
    """
    rows = conn.execute(
        "SELECT c.claim_id, j.value AS ref "
        "FROM claims c, json_each(c.supports_json) j"
    ).fetchall()

    if not rows:
        return []

    candidates = [
        (row["claim_id"], row["ref"])
        for row in rows
        if isinstance(row["ref"], str) and _CLAIM_ID_RE.match(row["ref"])
    ]
    if not candidates:
        return []

    # One JSON-array variable, not one per ref: the distinct-citation count
    # scales with the graph and would cross SQLite's per-statement variable cap.
    refs = sorted({ref for (_cid, ref) in candidates})
    existing = {
        r["claim_id"]
        for r in conn.execute(
            "SELECT claim_id FROM claims "
            "WHERE claim_id IN (SELECT value FROM json_each(?))",
            (json.dumps(refs),),
        ).fetchall()
    }

    dangling = [
        {"claim_id": cid, "dangling_ref": ref}
        for (cid, ref) in candidates
        if ref not in existing
    ]
    dangling.sort(key=lambda r: (r["claim_id"], r["dangling_ref"]))
    return dangling


def _extract_validation_signer_keyid(validation_signature: str) -> str | None:
    """Return the signing keyid from a validation envelope, or None if the
    envelope is malformed.

    The envelope's ``signatures[0].keyid`` is the authoritative signer.
    Malformed envelopes return None: the gates short-circuit
    rather than failing closed on top of the (already-failing) signing
    layer; the underlying UPDATE will then proceed via the legacy path
    and the row's ``validation_signature`` column will carry the broken
    envelope for later forensic inspection.
    """
    try:
        envelope = json.loads(validation_signature)
        return envelope["signatures"][0]["keyid"]
    except (json.JSONDecodeError, KeyError, IndexError, TypeError):
        return None


def _refuse_llm_validator(conn: sqlite3.Connection, validator_keyid: str) -> None:
    """Raise :class:`LLMValidatorPromotionError` if *validator_keyid* is an
    enrolled validator whose ``validator_type`` is ``'llm'``.

    A keyid that is not enrolled (no row in validators) does not trip
    this gate; that case is the enrollment check in
    ``_graph.validate`` and need not be re-litigated here.
    """
    row = conn.execute(
        "SELECT validator_type FROM validators WHERE keyid = ?",
        (validator_keyid,),
    ).fetchone()
    if row is None:
        return
    if row["validator_type"] == "llm":
        raise LLMValidatorPromotionError(
            f"Validator {validator_keyid[:12]}… is enrolled with "
            "validator_type='llm'. LLM validators may sign validation "
            "envelopes, and recording one on a claim is refused. "
            "Have a human-typed validator sign it instead."
        )


def _refuse_llm_contradiction_issuer(
    conn: sqlite3.Connection, validator_keyid: str,
) -> None:
    """Raise :class:`LLMValidatorPromotionError` if *validator_keyid* is an
    enrolled LLM-typed validator attempting to issue a contradiction.

    Symmetric to :func:`_refuse_llm_validator`. A signed contradiction
    sets ``t_invalid`` on the older of two claims via the
    ``contradiction_invalidates_older`` trigger; that is equivalent in
    blast radius to overturning a human-validated claim (it drops from
    default ``query()`` results). The human-only rule must apply in both
    directions: humans only to sign off, humans only to invalidate. Without
    this gate an enrolled LLM key could mark down any validated claim by
    signing a contradiction, breaking the same rule from the other side.

    A keyid that is not enrolled (no row in validators) does not trip
    this gate; the enrollment check in :func:`_require_enrolled_issuer`
    handles that case.
    """
    row = conn.execute(
        "SELECT validator_type FROM validators WHERE keyid = ?",
        (validator_keyid,),
    ).fetchone()
    if row is None:
        return
    if row["validator_type"] == "llm":
        raise LLMValidatorPromotionError(
            f"Validator {validator_keyid[:12]}… is enrolled with "
            "validator_type='llm'. LLM validators may sign validation "
            "envelopes but cannot issue contradictions that invalidate "
            "human-validated claims, the human-only rule applies to "
            "both promotion AND demotion. Have a human-typed validator "
            "sign the contradiction instead."
        )


def _canonical_envelope(envelope_str: str | None) -> str | None:
    """Canonicalise a JSON envelope so byte-level comparison is stable.

    Two semantically identical envelopes that differ only in JSON key
    order or whitespace should compare equal during idempotency
    reconciliation. Refuses non-JSON input and refuses JSON that
    isn't shaped like a DSSE envelope (top-level object with a
    ``signatures`` list of objects carrying ``keyid`` + ``sig``);
    mareforma stores this on the federation-import path and
    callers who passed garbage previously got a silent fallback.

    Mareforma does NOT cross-verify the envelope against any
    source-graph validator set; that is an adapter responsibility.
    Shape validation alone keeps tamperers from poisoning the
    column with non-DSSE content.
    """
    if envelope_str is None:
        return None
    if not isinstance(envelope_str, str):
        raise ValueError(
            f"original_signature_bundle must be a JSON string; got "
            f"{type(envelope_str).__name__}"
        )
    try:
        parsed = json.loads(envelope_str)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError(
            f"original_signature_bundle is not valid JSON: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError(
            "original_signature_bundle must decode to a JSON object"
        )
    sigs = parsed.get("signatures")
    if not isinstance(sigs, list) or not sigs:
        raise ValueError(
            "original_signature_bundle: signatures must be a non-empty list"
        )
    for s in sigs:
        if not isinstance(s, dict):
            raise ValueError(
                "original_signature_bundle: signature entries must be objects"
            )
        if not isinstance(s.get("keyid"), str) or not s.get("keyid"):
            raise ValueError(
                "original_signature_bundle: every signature entry needs a keyid"
            )
        if not isinstance(s.get("sig"), str) or not s.get("sig"):
            raise ValueError(
                "original_signature_bundle: every signature entry needs a sig"
            )
    return json.dumps(parsed, sort_keys=True, separators=(",", ":"))



def _refuse_self_verdict(
    conn: sqlite3.Connection,
    issuer_keyid: str,
    claim_id: str,
    *,
    relation: str,
    verdict_kind: str,
) -> None:
    """Raise :class:`VerdictIssuerError` if *issuer_keyid* signed ANY
    role on *claim_id*'s envelope.

    Walks every keyid in the claim's ``signature_bundle.signatures[*]``
    so a planner / executor / reviewer / validator on a
    ``claim-with-roles:v1`` envelope cannot also issue a replication
    or contradiction verdict on the same claim.

    Unsigned claims (``signature_bundle IS NULL``) pass the gate: same
    posture as :func:`_refuse_self_validation`. The gate is layered
    AFTER the enrollment check, so a non-enrolled key was already
    rejected; here the issuer is enrolled and we just check role
    overlap.
    """
    row = conn.execute(
        "SELECT signature_bundle FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None or row["signature_bundle"] is None:
        return
    # Reject malformed / empty-array envelopes outright, they would
    # slip through the keyid-match check and let the issuer bypass
    # the self-verdict gate either by corrupting the JSON or by
    # writing a structurally-empty signatures array. Fail closed.
    try:
        _bundle = json.loads(row["signature_bundle"])
    except (json.JSONDecodeError, TypeError):
        raise VerdictIssuerError(
            f"{verdict_kind} verdict refused: claim '{claim_id}' "
            "(relation=" + relation + ") has a signature_bundle that "
            "is not valid JSON. Refusing to gate against an unknowable "
            "identity set."
        )
    if not isinstance(_bundle, dict):
        raise VerdictIssuerError(
            f"{verdict_kind} verdict refused: claim '{claim_id}' "
            "(relation=" + relation + ") signature_bundle did not "
            "decode to a JSON object."
        )
    _sigs = _bundle.get("signatures")
    if _sigs is not None and (
        not isinstance(_sigs, list) or len(_sigs) == 0
    ):
        raise VerdictIssuerError(
            f"{verdict_kind} verdict refused: claim '{claim_id}' "
            "(relation=" + relation + ") has a signature_bundle "
            "whose signatures field is empty or non-list. Refusing "
            "to gate against an empty identity set."
        )
    keyids = _claim_signer_keyids(row["signature_bundle"])
    if issuer_keyid in keyids:
        try:
            bundle = json.loads(row["signature_bundle"])
            role = next(
                (s.get("role") for s in bundle.get("signatures") or []
                 if isinstance(s, dict) and s.get("keyid") == issuer_keyid),
                None,
            ) or "asserter"
        except (json.JSONDecodeError, TypeError, AttributeError):
            role = "asserter"
        raise VerdictIssuerError(
            f"{verdict_kind} verdict issuer {issuer_keyid[:12]}… signed "
            f"claim '{claim_id}' (relation={relation}) as {role!r}; "
            "self-verdicts are refused. The issuer must be an external "
            "witness whose keyid does not appear on the claim envelope."
        )


def _claim_asserting_keyid(claim_signature_bundle: "str | None") -> "str | None":
    """The one keyid that asserted the claim, or None.

    The FIRST signature on the envelope, which is the asserter's: a
    ``claim-with-roles:v1`` envelope carries the role actors after it. Kept
    apart from :func:`_claim_signer_keyids` because the two answer opposite
    questions. That one asks who is disqualified from judging this claim, and a
    wider answer is a safer one. This asks who is entitled to attest it, and a
    wider answer is a hole: role signatures cover the same PAE bytes, the role
    label is the asserter's own metadata, and the bundle can be rewritten from
    non-NULL to non-NULL, so any enrolled key can append itself to the signer
    set. A capability must not rest on a set that grows.

    Read out of the envelope rather than off ``claims.asserter_keyid``: no
    signature covers that column.
    """
    keyids = _claim_signer_keyids(claim_signature_bundle)
    return keyids[0] if keyids else None


def _claim_signer_keyids(claim_signature_bundle: str | None) -> list[str]:
    """Return every keyid that signed the claim envelope.

    For ``claim:v1`` envelopes (single signature) the result has one
    entry, the asserter. For ``claim-with-roles:v1`` envelopes (multi-
    signature) the result has one entry per role-actor (planner /
    executor / reviewer / validator).

    Malformed bundles return an empty list: mareforma cannot
    decide identity against a corrupted envelope, so downstream gates
    short-circuit and the row falls through to whatever pre-existing
    layer handles unsigned data. Same conservative posture as the
    earlier single-sig code path.
    """
    if claim_signature_bundle is None:
        return []
    try:
        bundle = json.loads(claim_signature_bundle)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(bundle, dict):
        return []
    sigs = bundle.get("signatures")
    # Structurally invalid envelopes, empty signatures list or a non-
    # list signatures field, must NOT silently collapse to []. Callers
    # use that empty result to mean "unsigned claim, gates pass". An
    # empty-array envelope would slip through every keyid match.
    # _refuse_self_validation / _refuse_self_verdict layer the explicit
    # rejection on top; this helper preserves the "no envelope" return
    # only for the genuine no-envelope case (signatures key absent).
    if sigs is None:
        return []
    if not isinstance(sigs, list):
        return []
    return [
        s["keyid"] for s in sigs
        if isinstance(s, dict) and isinstance(s.get("keyid"), str)
    ]


def _refuse_self_validation(
    claim_id: str,
    claim_signature_bundle: str | None,
    validator_keyid: str,
) -> None:
    """Raise :class:`SelfValidationError` if the validator signed ANY
    role on the claim envelope.

    Walks every keyid in ``signature_bundle.signatures[*].keyid``:
    the primary asserter AND any role-attestation signer (planner /
    executor / reviewer / validator on a ``claim-with-roles:v1``
    envelope). A validation has to come from a key that does not appear on
    the envelope at all.

    Unsigned claims (``signature_bundle IS NULL``) carry no signer
    identity to compare against and pass this gate. A malformed bundle
    is treated as absent (conservative posture).
    """
    matched_role: str | None = None
    if claim_signature_bundle is not None:
        try:
            bundle = json.loads(claim_signature_bundle)
        except (json.JSONDecodeError, TypeError):
            # Fail closed: a non-NULL bundle that doesn't parse means
            # the keyid set is unknowable, and the safe posture for a
            # self-loop refusal gate is to refuse the validation
            # rather than pass it.
            raise SelfValidationError(
                f"Claim '{claim_id}' has a signature_bundle that is "
                "not valid JSON. Refusing to gate self-validation "
                "against an unknowable identity set; investigate the "
                "row's signature_bundle column before retrying."
            )
        if not isinstance(bundle, dict):
            raise SelfValidationError(
                f"Claim '{claim_id}' signature_bundle did not decode "
                "to a JSON object; refusing to validate."
            )
        signatures = bundle.get("signatures")
        # A signed claim with an empty / non-list signatures field is a
        # structurally invalid envelope. Treating it as "absent" would
        # let a tamperer drop their keyid from the gate and self-promote
        #, refuse the operation rather than silently pass.
        if signatures is None:
            return  # no signature_bundle subfield at all → unsigned
        if not isinstance(signatures, list) or not signatures:
            raise SelfValidationError(
                f"Claim '{claim_id}' has a signature_bundle whose "
                "signatures field is empty or non-list. Refusing to "
                "gate self-validation against an empty identity set."
            )
        for sig in signatures:
            if not isinstance(sig, dict):
                continue
            if sig.get("keyid") == validator_keyid:
                matched_role = sig.get("role") or "asserter"
                break
    if matched_role is not None:
        raise SelfValidationError(
            f"Validator {validator_keyid[:12]}… signed claim "
            f"'{claim_id}' as {matched_role!r}; self-promotion is "
            "refused. A validation has to come from a key that does not "
            "appear on the claim envelope. "
            "Have a different enrolled key call graph.validate(...)."
        )


def _refuse_self_validation_across_set(
    conn: sqlite3.Connection, claim_id: str, validator_keyid: str,
) -> None:
    """Refuse a validator that asserted ANY claim in the converging set.

    A claim's converging set is the peers citing the same upstream anchors it
    cites. A validator whose keyid equals the ``asserter_keyid`` of any claim
    in that set is signing off on a line it took part in, so the validation is
    refused. :func:`_refuse_self_validation` already covers the claim's own
    signers; this extends the refusal to the peers beside it.
    """
    sup_row = conn.execute(
        "SELECT supports_json FROM claims WHERE claim_id = ?", (claim_id,),
    ).fetchone()
    if sup_row is None:
        return
    try:
        supports = json.loads(sup_row["supports_json"] or "[]")
    except (json.JSONDecodeError, TypeError) as exc:
        # Fail closed: a malformed supports_json on the row being promoted is a
        # tamper signal, not a normal state (the system always writes valid
        # JSON). We cannot enumerate the converging peers to clear the across-set
        # refusal, so refuse rather than fall through to validation. Mirrors
        # _refuse_self_validation failing closed on an unparseable bundle.
        raise SelfValidationError(
            f"Cannot verify the converging set behind '{claim_id}': its "
            f"supports_json does not parse ({exc}). Refusing validation."
        ) from exc
    if not supports:
        return
    sup_placeholders = ",".join("?" * len(supports))
    anchors = [
        r["claim_id"] for r in conn.execute(
            f"SELECT claim_id FROM claims "
            f"WHERE claim_id IN ({sup_placeholders}) "
            f"AND status = 'open'",
            supports,
        ).fetchall()
    ]
    if not anchors:
        return
    placeholders = ",".join("?" * len(anchors))
    # The gate is a membership test, not an enumeration: all it needs is whether
    # THIS validator asserted a converging peer. Leading on asserter_keyid lets
    # idx_claims_asserter_keyid pick the validator's own rows, so json_each
    # expands those and nothing else. Matching the anchors first left the
    # planner no better path than every claim in the graph, and every promotion
    # paid for the whole subset.
    # Membership is the supports edge and nothing else. A peer that somebody
    # has since validated is still a line its asserter took part in, so
    # narrowing the set by anything but the edge drops peers and clears a
    # refusal that should have stood.
    # The peer set is read from claims.supports_json, not from the reverse-edge
    # cache the insert path narrows with. The cache is unsigned and its
    # staleness check only counts claims, so a dropped edge is invisible; there
    # it would block a promotion (fail closed), here it would drop a peer and
    # clear a refusal (fail open on a trust gate).
    peer = conn.execute(
        f"SELECT 1 FROM claims c, json_each(c.supports_json) j "
        f"WHERE c.asserter_keyid = ? "
        f"AND c.status = 'open' "
        f"AND j.value IN ({placeholders}) "
        f"LIMIT 1",
        (validator_keyid, *anchors),
    ).fetchone()
    if peer is not None:
        raise SelfValidationError(
            f"Validator {validator_keyid[:12]}… asserted a claim in the "
            f"converging set behind '{claim_id}'; a participant cannot "
            "sign off on a line it took part in. Have an "
            "independent enrolled key call graph.validate(...)."
        )


def _verify_evidence_seen(
    conn: sqlite3.Connection,
    promoted_claim_id: str,
    evidence_seen: list[str],
    validated_at: str,
) -> None:
    """Verify every entry in ``evidence_seen`` is a valid citation.

    Each entry must be:
      * a string,
      * a strict-v4 UUID (``_is_claim_id``),
      * the id of a claim that exists in this graph,
      * a claim whose ``created_at`` is no later than ``validated_at``.

    Raises :class:`EvidenceCitationError` naming the first failing entry.
    An empty list is the explicit "I reviewed nothing" admission and
    passes the gate without inspection.

    The validator's enumeration is self-declared: this gate cannot
    prove the validator actually opened those claims, only that the
    claims they cited exist and predate validation. That's the
    strongest property mareforma can enforce; everything else
    rests on the validator's honesty.
    """
    if not evidence_seen:
        return
    for entry in evidence_seen:
        if not isinstance(entry, str):
            raise EvidenceCitationError(
                f"evidence_seen entry {entry!r} is not a string."
            )
        if not _is_claim_id(entry):
            raise EvidenceCitationError(
                f"evidence_seen entry '{entry}' is not a strict-v4 UUID; "
                "only local claim_ids can be cited as reviewed evidence."
            )
        if entry == promoted_claim_id:
            raise EvidenceCitationError(
                f"evidence_seen cites the claim being promoted "
                f"('{promoted_claim_id}'); the validator cannot count "
                "the promotion target as evidence for itself."
            )
        row = conn.execute(
            "SELECT created_at FROM claims WHERE claim_id = ?",
            (entry,),
        ).fetchone()
        if row is None:
            raise EvidenceCitationError(
                f"evidence_seen entry '{entry}' does not exist in the "
                "graph; cite only claims the validator actually reviewed."
            )
        cited_created_at = row["created_at"]
        if cited_created_at > validated_at:
            raise EvidenceCitationError(
                f"evidence_seen entry '{entry}' was created at "
                f"{cited_created_at} which is after validated_at "
                f"{validated_at}; the validator could not have reviewed "
                "a claim that didn't exist yet."
            )


def validate_claim(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
    *,
    validated_by: str | None = None,
    validation_signature: str | None = None,
    validated_at: str | None = None,
    evidence_seen: list[str] | None = None,
) -> None:
    """Record a human validator's signed sign-off on a claim.

    Writes a signed envelope onto the row and nothing else: no level, no
    ranking, no change to any other claim. A validation is terminal, so a row
    already carrying one is refused rather than overwritten.

    Parameters
    ----------
    validation_signature:
        Required JSON-encoded DSSE-style envelope binding
        ``(claim_id, validator_keyid, validated_at, evidence_seen)``.
        Produced by :func:`mareforma.signing.sign_validation` and stored
        verbatim on the row so the validation event itself is
        independently verifiable (tampering with
        ``validated_by``/``validated_at``/``evidence_seen`` post-hoc is
        detectable). There is no unsigned path: an unsigned call raises
        ``ValueError`` up front.
    validated_at:
        Optional ISO 8601 UTC timestamp to write to the row. The caller
        signs a validation envelope binding a timestamp, so the SAME
        timestamp must be threaded through here for the envelope's
        ``validated_at`` to match the row's ``validated_at``
        byte-for-byte; a divergent value is rejected by the
        envelope-agreement gate. If ``None``, a fresh timestamp is
        generated (only self-consistent when the envelope was signed
        against that exact value).
    evidence_seen:
        Optional list of claim_ids the validator declares to have
        reviewed before signing. ``None`` is normalized
        to ``[]`` and bound into the signed envelope: a positive
        statement that the validator reviewed nothing, which is then
        visible in the audit trail rather than hidden by absence. Each
        cited entry must be a strict-v4 UUID matching an existing
        claim with ``created_at <= validated_at``. The validator's
        enumeration is self-declared; mareforma cannot prove they
        actually opened the cited claims, but it CAN verify the cited
        claims exist and predate validation.

    Verification gates
    ------------------
    When ``validation_signature`` is supplied, mareforma fires the
    following defense-in-depth gates before the row is updated. All
    consult mareforma directly, calling :func:`validate_claim`
    bypassing :meth:`EpistemicGraph.validate` does not relax any of
    them, so a hostile in-process caller cannot route around them:

    1. The envelope must parse as JSON and carry a ``payloadType`` in
       ``{PAYLOAD_TYPE_VALIDATION, PAYLOAD_TYPE_SEED}`` (raises
       :class:`InvalidValidationEnvelopeError` on either failure).
    2. The envelope's signing keyid must be an enrolled validator
       (raises :class:`InvalidValidationEnvelopeError`).
    3. The envelope must verify cryptographically against the claimed
       signer's public key via :func:`signing.verify_envelope` (raises
       :class:`InvalidValidationEnvelopeError`).
    4. The signing validator's ``validator_type`` must be ``'human'``.
       An ``'llm'``-typed validator can sign a validation envelope, and
       recording it is refused (raises
       :class:`LLMValidatorPromotionError`).
    5. The validator's keyid must NOT match the claim's
       ``signature_bundle`` signing keyid. Self-validation is the
       trivial-loop attack (raises :class:`SelfValidationError`).
    6. The envelope's signed payload must agree on ``claim_id``,
       ``validator_keyid``, and the timestamp (``validated_at`` for
       validation envelopes, ``seeded_at`` for seed envelopes) with the
       row being written and the kwargs being written (raises
       :class:`InvalidValidationEnvelopeError`).
    7. The envelope's ``evidence_seen`` field must equal the
       ``evidence_seen`` kwarg, and every cited entry must be a
       strict-v4 UUID matching an existing claim with
       ``created_at <= validated_at`` (raises
       :class:`EvidenceCitationError`).

    Raises
    ------
    ClaimNotFoundError
        If no claim with claim_id exists.
    ValueError
        If ``validation_signature`` is ``None`` (recording a validation
        requires a signed envelope; there is no unsigned path); if the
        claim's status is not 'open' (contested/retracted claims are
        editorially tainted and must not be signed off on; revisit the
        editorial flag via update_claim before validating); if the claim
        already carries a validation, since the row holds one envelope and a
        second would erase the first; or if a signed contradiction invalidated
        the claim inside the check-to-write window.
    InvalidValidationEnvelopeError
        If the validation envelope is malformed, wrong-typed, signed
        by a non-enrolled key, fails cryptographic verification, or
        its payload disagrees with the row or kwargs on ``claim_id``,
        ``validator_keyid``, or the timestamp.
    LLMValidatorPromotionError
        If the validation envelope is signed by an LLM-typed validator.
    SelfValidationError
        If the validation envelope's signing keyid equals the claim's
        ``signature_bundle`` signing keyid.
    EvidenceCitationError
        If any entry in ``evidence_seen`` is not a strict-v4 UUID, does
        not point to an existing claim, or points to a claim with
        ``created_at > validated_at``.
    """
    if validation_signature is None:
        # The write below puts this kwarg straight into
        # validation_signature, and a row carrying a validator with no
        # envelope is refused by the table CHECK, so an unsigned call could
        # only ever surface as an integrity error that reads like row
        # corruption. Reject it here with a message that names the real
        # requirement.
        raise ValueError(
            f"validate_claim for claim '{claim_id}' requires a signed "
            "validation envelope; recording a validation has no unsigned "
            "path. Build the envelope with mareforma.signing.sign_validation "
            "or call graph.validate() from an enrolled session."
        )
    row = conn.execute(
        "SELECT status, signature_bundle, t_invalid "
        "FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")
    # There is no level to require a claim to have reached first. What is left
    # is the part that was never about levels: a person cannot sign off on a
    # claim the graph has already withdrawn or contradicted.
    if row["status"] != "open":
        raise ValueError(
            f"Claim '{claim_id}' has status='{row['status']}'. "
            "Only claims with status='open' can be validated. "
            "Reset the status via update_claim if the editorial flag no "
            "longer applies."
        )
    if row["t_invalid"] is not None:
        # A signed contradiction verdict from an enrolled validator has
        # marked this claim invalid. Promotion would ride past the
        # terminal evidence and let validate() lift an already-refuted
        # claim back into the trust ladder.
        raise ValueError(
            f"Claim '{claim_id}' was invalidated by a signed contradiction "
            f"verdict at t_invalid={row['t_invalid']!r}. Refuse to record a "
            "validation on an invalidated claim."
        )

    # Verification gates over the validation envelope.
    #
    # validate_claim is a public-by-convention function (no leading
    # underscore) and is callable directly by any in-process code path , 
    # not only :meth:`EpistemicGraph.validate`. The wrapper builds the
    # envelope with the graph's loaded signer, so the wrapper path is
    # safe by construction; this function is the defense-in-depth layer
    # that must also be safe when called with a caller-supplied envelope.
    #
    # Without cryptographic verification here, an enrolled LLM-typed
    # validator (or any in-process caller) could hand-craft an envelope
    # JSON claiming a human validator's keyid + a garbage signature,
    # then call ``db.validate_claim`` directly. Mareforma would
    # consult the CLAIMED keyid to enforce the validation gates
    # (LLM-type, self-validation), find them satisfied, and persist a
    # fraudulently validated row anchored by an envelope that does not
    # verify against the impersonated signer's public key. Restore would
    # eventually catch it, but the live DB would already have shipped
    # bad data to whoever queried in the meantime.
    #
    # Order of operations:
    #   1. Decode the envelope structure (refuse malformed JSON).
    #   2. Restrict ``payloadType`` to validation or seed, same set the
    #      restore path accepts on this column.
    #   3. Look up the claimed signer in the validators table.
    #   4. Cryptographically verify the envelope with the signer's
    #      pubkey via :func:`signing.verify_envelope`.
    #   5. Apply the trust-ladder gates (LLM-type ceiling, self-
    #      validation refusal). These can now safely consult the
    #      validator_keyid because step 4 proved the signer actually
    #      holds the private key.
    #   6. Compare the envelope's payload fields against the row + the
    #      kwargs mareforma is about to write, claim_id, the
    #      timestamp, validator_keyid, and evidence_seen all must
    #      agree byte-for-byte.
    #
    # An unsigned call was rejected up front, so validation_signature is
    # always present here; the ``is not None`` guard below is a defensive
    # restatement of that contract, not a live unsigned branch.
    validator_keyid: str | None = None
    env: dict | None = None
    declared_type: str | None = None
    if validation_signature is not None:
        from mareforma import signing as _signing
        from mareforma import validators as _validators

        try:
            env = json.loads(validation_signature)
            validator_keyid = env["signatures"][0]["keyid"]
            declared_type = env["payloadType"]
        except (json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
            raise InvalidValidationEnvelopeError(
                f"validation_signature for claim '{claim_id}' is malformed "
                f"({exc}); cannot extract signer or payloadType."
            ) from exc

        # The validation_signature column carries either a validation
        # envelope or a seed envelope. Anything else is a type confusion
        # attempt: cross-type acceptance lets an attacker pass an enrollment or
        # claim envelope through a verifier expecting a validation
        # event. verify_envelope's expected_payload_type is the formal
        # guard; the early-rejection here gives a clear error message.
        if declared_type not in (
            _signing.PAYLOAD_TYPE_VALIDATION,
            _signing.PAYLOAD_TYPE_SEED,
        ):
            raise InvalidValidationEnvelopeError(
                f"validation_signature payloadType {declared_type!r} for "
                f"claim '{claim_id}' is neither validation nor seed; "
                "refusing to persist a wrong-typed envelope as validation."
            )

        # A row in the validators table is not an enrolment: the table takes a
        # direct INSERT, so a real pubkey under a junk envelope reads as a
        # validator on presence alone. is_enrolled walks the chain back to the
        # self-signed root, the bar the CLI and the verdict path already apply.
        signer_row = _validators.get_validator(conn, validator_keyid)
        if signer_row is None or not _validators.is_enrolled(
            conn, validator_keyid,
        ):
            raise InvalidValidationEnvelopeError(
                f"validation_signature for claim '{claim_id}' is signed by "
                f"keyid {validator_keyid[:12]}… which is not an enrolled "
                "validator on this graph. Enroll the signer first via "
                "graph.enroll_validator() or call graph.validate() from a "
                "session whose loaded signer is already enrolled."
            )

        try:
            signer_pem = base64.standard_b64decode(signer_row["pubkey_pem"])
            signer_pub = _signing.public_key_from_pem(signer_pem)
            sig_ok = _signing.verify_envelope(
                env, signer_pub, expected_payload_type=declared_type,
            )
        except (ValueError, TypeError, _signing.SigningError) as exc:
            raise InvalidValidationEnvelopeError(
                f"validation_signature for claim '{claim_id}' did not verify "
                f"cryptographically against keyid {validator_keyid[:12]}…: "
                f"{exc}"
            ) from exc
        if not sig_ok:
            raise InvalidValidationEnvelopeError(
                f"validation_signature for claim '{claim_id}' failed Ed25519 "
                f"verification against keyid {validator_keyid[:12]}…. The "
                "envelope is not authorized by the claimed signer."
            )

        # Trust-ladder gates run AFTER signature verification, so the
        # validator_keyid is now known to be authentic, not just claimed.
        _refuse_llm_validator(conn, validator_keyid)
        _refuse_self_validation(
            claim_id, row["signature_bundle"], validator_keyid,
        )
        _refuse_self_validation_across_set(conn, claim_id, validator_keyid)

    now = validated_at if validated_at is not None else _now()

    # Envelope/kwarg/row payload-field agreement. verify_envelope above
    # proved the signer signed THESE BYTES, but it does NOT prove the
    # signed payload describes the row being updated. Without these
    # equality checks a caller could replay a legitimate validation
    # envelope from claim A onto row B (matching signer + matching
    # cryptography), so B reads as validated under an envelope that
    # binds a different claim_id and timestamp. Restore would catch the
    # divergence; this is the live-DB equivalent of the restore-path
    # checks at ``_verify_claim_signatures_on_restore``.
    if validation_signature is not None and env is not None:
        # envelope_payload raises InvalidEnvelopeError when the signed
        # payload bytes fail to base64-decode or do not parse as a JSON
        # object. verify_envelope only checks the DSSE PAE signature;
        # it does NOT enforce that the payload bytes are well-formed.
        # An enrolled validator with a real key could (intentionally or
        # by bug) sign non-JSON bytes; without this try/except the
        # InvalidEnvelopeError would propagate past mareforma's
        # documented contract.
        try:
            env_payload = _signing.envelope_payload(env)
        except _signing.InvalidEnvelopeError as exc:
            raise InvalidValidationEnvelopeError(
                f"validation envelope's signed payload is not a JSON "
                f"object ({exc}); refusing to persist an envelope whose "
                "payload contract is malformed."
            ) from exc
        if env_payload.get("claim_id") != claim_id:
            raise InvalidValidationEnvelopeError(
                f"validation envelope binds claim_id "
                f"{env_payload.get('claim_id')!r} but the row being promoted "
                f"is {claim_id!r}; envelope replay across claims refused."
            )
        if env_payload.get("validator_keyid") != validator_keyid:
            raise InvalidValidationEnvelopeError(
                "validation envelope's payload.validator_keyid does not "
                "match the signing keyid; envelope is internally "
                "inconsistent and refused."
            )
        # Seed envelopes bind ``seeded_at``; validation envelopes bind
        # ``validated_at``. The row's ``validated_at`` is being written
        # from ``now`` either way, so the comparison key is uniform on
        # the row side and varies only on the envelope side.
        timestamp_field = (
            "validated_at"
            if declared_type == _signing.PAYLOAD_TYPE_VALIDATION
            else "seeded_at"
        )
        if env_payload.get(timestamp_field) != now:
            raise InvalidValidationEnvelopeError(
                f"validation envelope's {timestamp_field} "
                f"({env_payload.get(timestamp_field)!r}) does not match the "
                f"validated_at value being written ({now!r}); envelope "
                "timestamp must agree with mareforma's write."
            )
        # evidence_seen is bound only on validation envelopes; seed
        # envelopes have no analog. Skip the comparison for seeds.
        if declared_type == _signing.PAYLOAD_TYPE_VALIDATION:
            env_evidence = env_payload.get("evidence_seen")
            kwarg_evidence = evidence_seen if evidence_seen is not None else []
            if env_evidence != kwarg_evidence:
                raise EvidenceCitationError(
                    "validation envelope's evidence_seen "
                    f"({env_evidence!r}) does not match the evidence_seen "
                    f"kwarg ({kwarg_evidence!r}); mareforma validates "
                    "what the caller passed, and the signed envelope must "
                    "bind the same list, refusing to persist a divergent "
                    "envelope."
                )

    # Evidence-citation gate. Every entry in evidence_seen must be a
    # strict-v4 UUID pointing at an existing claim that predates the
    # validation timestamp. An empty list is the "I reviewed nothing"
    # admission and passes the gate. None is normalized to [].
    _verify_evidence_seen(
        conn, claim_id, evidence_seen or [], now,
    )
    # The early gate above ran in an autocommit SELECT, then the crypto and
    # evidence-citation checks ran with no transaction open. Wrap the write in
    # BEGIN IMMEDIATE and re-assert the gate on the UPDATE itself so a signed
    # contradiction (t_invalid) or retraction (status) that lands in the
    # check-to-write window cannot land under a signed sign-off. Mirrors
    # record_replication_verdict's guarded write; when the caller already
    # holds a transaction its outer commit flushes this write.
    _own_txn = not conn.in_transaction
    try:
        if _own_txn:
            conn.execute("BEGIN IMMEDIATE")
        # ``validation_signature IS NULL`` is what makes a validation
        # terminal, and it has to be said here. The column holds one envelope,
        # so without this a second validator's sign-off overwrites the first
        # and the first is gone from the graph with nothing recording that it
        # was ever there. The read path cannot notice, because the envelope
        # that survives verifies.
        #
        # On the UPDATE rather than only in the gate above, so a concurrent
        # validation landing in the check-to-write window loses the race
        # instead of quietly replacing the winner.
        cur = conn.execute(
            """
            UPDATE claims
            SET validated_by = ?,
                validated_at = ?,
                validation_signature = ?,
                validator_keyid = COALESCE(?, validator_keyid),
                updated_at   = ?
            WHERE claim_id = ?
              AND status = 'open'
              AND t_invalid IS NULL
              AND validation_signature IS NULL
            """,
            (validated_by, now, validation_signature, validator_keyid,
             now, claim_id),
        )
        if cur.rowcount == 0:
            # The guarded UPDATE matched nothing. Say which of the three
            # reasons it was, because they call for different things: a claim
            # somebody already signed off on is not the same problem as one a
            # verdict invalidated while this call was working.
            already = conn.execute(
                "SELECT validation_signature IS NOT NULL AS validated, status, "
                "t_invalid FROM claims WHERE claim_id = ?",
                (claim_id,),
            ).fetchone()
            if _own_txn:
                conn.rollback()
            if already is not None and already["validated"]:
                raise ValueError(
                    f"Claim '{claim_id}' already carries a validation. The row "
                    "holds one signed envelope, so recording a second would "
                    "erase the first and the graph would keep no record that "
                    "it was ever there. A validation is the statement of the "
                    "person who made it; to add another reviewer's, record it "
                    "as its own claim rather than over the top of theirs."
                )
            raise ValueError(
                f"Claim '{claim_id}' was invalidated by a signed contradiction "
                "verdict during validation (the check-to-write window closed), "
                "or its status changed. Refuse to record a validation over it."
            )
        if _own_txn:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        if _own_txn:
            conn.rollback()
        translated = _state_error_from_integrity(exc)
        if translated is not None:
            raise translated from exc
        raise DatabaseError(f"Failed to validate claim '{claim_id}': {exc}") from exc
    except sqlite3.OperationalError as exc:
        if _own_txn:
            conn.rollback()
        raise DatabaseError(f"Failed to validate claim '{claim_id}': {exc}") from exc
    _backup_claims_toml(conn, root)


def list_unresolved_claims(conn: sqlite3.Connection) -> list[dict]:
    """Return all claims currently marked unresolved=True."""
    rows = conn.execute(
        f"SELECT {_CLAIM_SELECT} FROM claims WHERE unresolved = 1 ORDER BY created_at"
    ).fetchall()
    return [dict(r) for r in rows]


def _attempt_rekor_saga(
    conn: sqlite3.Connection,
    root: Path,
    *,
    claim_id: str,
    envelope: dict,
    signer: "object",
    rekor_url: str,
    require_rekor: bool,
    trust_insecure_rekor: bool = False,
    rekor_log_pubkey_pem: bytes | None = None,
    own_transaction: bool = True,
) -> int:
    """Run the Rekor 4-step saga on a freshly-INSERTed signed claim.

    Returns the new ``transparency_logged`` value to write back to the
    caller's local variable (0 if the saga did not complete, 1 if the
    row UPDATE succeeded).

    Saga steps
    ----------
    1. The claim is already INSERTed with ``transparency_logged=0``
       (the caller's responsibility, before this helper runs).
    2. Submit the envelope to Rekor. On failure, warn and append a
       ``rekor_submit`` fail event to ``health.jsonl`` (so an outage is
       visible in ``mareforma activity``, not only in a per-claim trust
       map), then return 0.
    3. **(opt-in) Verify the inclusion proof.** If the caller supplied
       ``rekor_log_pubkey_pem``, re-fetch the entry via
       :func:`signing.fetch_inclusion_proof` and pass the full body to
       :func:`signing.verify_rekor_inclusion`. A verification failure
       refuses the saga (the row stays at ``transparency_logged=0``);
       a future ``refresh_unsigned`` will retry. When
       ``rekor_log_pubkey_pem`` is None, the residual gap is the trust
       posture documented in README "Limits of the Rekor integration".
    4. Persist the (uuid, logIndex, integratedTime) coords to the
       ``rekor_inclusions`` sidecar. The sidecar's append-only triggers
       guarantee no replay can rewrite this row.
    5. UPDATE the claim row's ``signature_bundle`` with the augmented
       envelope (Rekor block attached) and set ``transparency_logged=1``.

    If step 5 fails after step 4 succeeded, the sidecar holds the durable
    record. :meth:`EpistemicGraph.refresh_unsigned` reads the sidecar and
    replays step 5 instead of double-submitting to Rekor.

    Extracting this helper out of :func:`add_claim` keeps the
    happy-path read concise: ``add_claim`` is about claim insertion +
    chain integrity; the saga is a separate concern that lives next to
    its sidecar helper :func:`_record_rekor_inclusion`.

    Raises
    ------
    SigningError
        If the initial Rekor submission fails and ``require_rekor=True``,
        OR if Merkle inclusion-proof verification fails and
        ``require_rekor=True``.
    """
    from mareforma import signing as _signing

    logged, entry = _signing.submit_to_rekor(
        envelope, signer.public_key(), rekor_url=rekor_url,
        allow_insecure=trust_insecure_rekor,
    )
    if not logged or entry is None:
        if require_rekor:
            raise _signing.SigningError(
                f"Rekor submission to {rekor_url} failed and "
                "require_rekor=True. Claim was persisted with "
                "transparency_logged=0; call "
                "EpistemicGraph.refresh_unsigned() to retry."
            )
        warnings.warn(
            f"Rekor submission to {rekor_url} failed for claim {claim_id}. "
            "The claim is stored and signed, but transparency_logged stays 0, "
            "so nothing outside this machine witnesses the signature until "
            "EpistemicGraph.refresh_unsigned() logs it.",
            stacklevel=2,
        )
        from mareforma.health import append_health_event
        append_health_event(
            root, "rekor_submit", outcome="fail", claim_id=claim_id,
        )
        return 0

    # Step 3 (opt-in): cryptographic inclusion-proof verification. The
    # submit-time response binding (OUR hash + OUR signature inside the
    # returned entry) is checked by submit_to_rekor; what's left to
    # close is "the log committed our entry and didn't tamper with it
    # afterward." That requires the signed checkpoint + Merkle audit
    # path, which submit_to_rekor's stripped response doesn't carry.
    # Re-fetch the entry by uuid (one extra GET) and run the full
    # verifier. Skipped when the caller hasn't supplied a log pubkey
    #, current trust posture is "trust submit-time response."
    proof_entry = None
    if rekor_log_pubkey_pem is not None:
        uuid = entry.get("uuid")
        if not isinstance(uuid, str) or not uuid:
            if require_rekor:
                raise _signing.SigningError(
                    "Rekor inclusion-proof verification requested but "
                    "the submit response had no uuid; cannot re-fetch "
                    "to obtain the inclusion proof."
                )
            return 0
        try:
            full_body = _signing.fetch_inclusion_proof(
                uuid, rekor_url, allow_insecure=trust_insecure_rekor,
            )
            _signing.verify_rekor_inclusion(
                full_body, rekor_log_pubkey_pem, envelope,
            )
            proof_entry = full_body
        except _signing.RekorInclusionError as exc:
            if require_rekor:
                raise _signing.SigningError(
                    f"Rekor inclusion-proof verification failed for "
                    f"claim {claim_id} (uuid {uuid}): {exc} "
                    f"[reason={exc.reason}]. require_rekor=True; refusing "
                    "to advance transparency_logged to 1."
                ) from exc
            warnings.warn(
                f"Rekor inclusion-proof verification failed for claim "
                f"{claim_id} (uuid {uuid}, reason={exc.reason}). The "
                "submit response itself was bound to OUR hash + sig, but "
                "the log's signed Merkle path did not verify. "
                "transparency_logged stays 0; refresh_unsigned() will "
                "retry.",
                stacklevel=2,
            )
            return 0

    # Step 4: durable sidecar write. Failure here means Rekor saw the
    # entry but we lost the record locally, the next refresh_unsigned
    # will re-submit and create a duplicate, which is the only recovery
    # path when no sidecar exists. _record_rekor_inclusion emits a
    # warning on that path; we honor its return value.
    if not _record_rekor_inclusion(
        conn, claim_id, entry,
        proof_entry=proof_entry, own_transaction=own_transaction,
    ):
        return 0

    # Step 4: augment the row's bundle with the Rekor coords and flip
    # the transparency flag. Failure here is benign: the sidecar holds
    # the truth, refresh_unsigned will replay this UPDATE from the
    # stored coords without re-submitting to Rekor.
    augmented = _signing.attach_rekor_entry(envelope, entry)
    new_bundle = json.dumps(
        augmented, sort_keys=True, separators=(",", ":"),
    )
    try:
        conn.execute(
            "UPDATE claims SET signature_bundle = ?, "
            "transparency_logged = 1, updated_at = ? "
            "WHERE claim_id = ?",
            (new_bundle, _now(), claim_id),
        )
        # Commit only when this call owns the transaction. When add_claim joined
        # a caller's open transaction (submit_finding's BEGIN IMMEDIATE),
        # committing here would flush the caller's in-flight writes early and
        # void its atomicity: a later fork or raise could no longer roll the
        # signed claim back, stranding an orphan. The caller's commit flushes
        # this UPDATE with the rest of its transaction.
        if own_transaction:
            conn.commit()
        return 1
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        warnings.warn(
            f"Claim {claim_id} accepted by Rekor (coords saved to "
            f"rekor_inclusions sidecar) but the local UPDATE failed "
            f"({exc}). transparency_logged remains 0; run "
            "EpistemicGraph.refresh_unsigned() to reconcile without "
            "re-submitting.",
            stacklevel=2,
        )
        return 0


def _record_rekor_inclusion(
    conn: sqlite3.Connection,
    claim_id: str,
    entry: dict,
    proof_entry: dict | None = None,
    own_transaction: bool = True,
) -> bool:
    """Step 3 of the Rekor saga: persist a successful inclusion.

    Called after Rekor returns a `(logged=True, entry)` response and
    before the claims-row UPDATE. The sidecar is the durable record of
    "Rekor witnessed this claim"; when the row UPDATE later fails,
    :meth:`refresh_unsigned` consults this table to replay the UPDATE
    from the coordinate columns instead of re-submitting.

    *entry* carries the submit-response coordinates (uuid, logIndex,
    integratedTime) that populate those columns. *proof_entry* is the
    re-fetched full entry, with ``body`` and ``verification``, verified
    against the pinned log key; when present it is what lands in
    ``raw_response_b64``, in the ``{uuid: entry}`` shape Rekor returns
    and :func:`mareforma.restore` re-verifies. Without a pinned log key
    there is no proof to store and the coordinates are stored instead,
    so such a row cannot be re-verified at restore time.

    Returns ``True`` on success. On failure, emits a WARNING and returns
    ``False``: the caller skips the subsequent UPDATE so we don't end
    up with `transparency_logged=1` but no sidecar record (the inverse
    of the gap this saga closes). The Rekor entry exists publicly; the
    operator must run :meth:`refresh_unsigned` which will detect the
    missing-sidecar-but-unflagged state and re-submit (creating a
    duplicate entry, the only recovery available when we have no
    record of the original inclusion).
    """
    try:
        uuid = entry.get("uuid")
        stored = (
            {uuid: proof_entry}
            if proof_entry is not None and isinstance(uuid, str)
            else entry
        )
        raw_json = json.dumps(stored, sort_keys=True, separators=(",", ":"))
        raw_b64 = base64.standard_b64encode(
            raw_json.encode("utf-8"),
        ).decode("ascii")
        # Defensive numeric parsing. Rekor returns ``logIndex`` and
        # ``integratedTime`` as JSON numbers, but a buggy or hostile
        # registry could return strings (``"42"``), floats, or non-
        # numeric tokens. Without this guard, an int() ValueError would
        # propagate out of add_claim AFTER the claim has been committed
        #, the user would see a stack trace instead of the documented
        # (False, None) sidecar-failure flow. Treat any parse failure
        # as a sidecar miss; the recovery path then re-submits.
        try:
            log_index_int = int(entry.get("logIndex") or 0)
        except (TypeError, ValueError):
            warnings.warn(
                f"Rekor returned a non-integer logIndex "
                f"({entry.get('logIndex')!r}) for claim {claim_id}. "
                "Treating as a sidecar miss; refresh_unsigned() will "
                "re-submit and create a duplicate Rekor entry, the "
                "only recovery available without a parseable record.",
                stacklevel=2,
            )
            return False
        try:
            integrated_time_int = (
                int(entry.get("integratedTime") or 0) or None
            )
        except (TypeError, ValueError):
            # integratedTime is informational. A malformed value gets
            # stored as NULL rather than failing the whole sidecar
            # write, the uuid and logIndex are sufficient to replay
            # the saga's step 4.
            integrated_time_int = None
        # ON CONFLICT DO NOTHING: a successful Rekor inclusion is
        # immutable. If a caller retries the saga and lands here twice
        # for the same claim_id, the original row stays, the
        # append-only trigger refuses overwrite anyway, but the explicit
        # conflict clause keeps the path crash-free. The PRIMARY KEY on
        # claim_id is the conflict target.
        conn.execute(
            "INSERT INTO rekor_inclusions "
            "(claim_id, uuid, log_index, integrated_time, "
            " raw_response_b64, recorded_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(claim_id) DO NOTHING",
            (
                claim_id,
                uuid,
                log_index_int,
                integrated_time_int,
                raw_b64,
                _now(),
            ),
        )
        # Same rule as the claims-row UPDATE one step later: commit only
        # when the saga owns the transaction. Committing here while joined
        # to a caller's open transaction would flush its in-flight writes,
        # so a later rollback could no longer discard the signed claim.
        if own_transaction:
            conn.commit()
        return True
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        warnings.warn(
            f"Claim {claim_id} accepted by Rekor but the sidecar INSERT "
            f"into rekor_inclusions failed ({exc}). The local row stays "
            "unflagged AND there is no recovery hint, refresh_unsigned() "
            "will RE-SUBMIT, creating a duplicate Rekor entry. This is "
            "the only recovery path when no record of the original "
            "submission exists.",
            stacklevel=2,
        )
        return False


def get_rekor_inclusion(
    conn: sqlite3.Connection,
    claim_id: str,
) -> dict | None:
    """Return the stored Rekor coordinates for a claim, if any.

    Used by the recovery path in :meth:`refresh_unsigned` to detect
    "Rekor ACK persisted, claims-row UPDATE pending" and replay the
    UPDATE from stored coords instead of re-submitting.

    Returns the (uuid, integratedTime, logIndex) dict in the shape
    :func:`signing.submit_to_rekor` returns, so the replayed bundle
    matches what the original UPDATE would have written. The coords
    come from the sidecar's own columns; ``raw_response_b64`` holds the
    inclusion proof for restore-time verification, not these values.
    Returns ``None`` when no sidecar row exists for this claim.
    """
    row = conn.execute(
        "SELECT uuid, log_index, integrated_time FROM rekor_inclusions "
        "WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "uuid": row["uuid"],
        "integratedTime": row["integrated_time"],
        "logIndex": row["log_index"],
    }


def list_unlogged_claims(conn: sqlite3.Connection) -> list[dict]:
    """Return signed claims still awaiting Rekor inclusion.

    A claim is "unlogged" when ``signature_bundle`` is non-NULL but
    ``transparency_logged`` is 0. Unsigned claims are excluded: they have
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
    integratedTime). The flag-flip and the bundle rewrite happen in a single
    transaction, so a crash between them cannot leave a row claiming a log
    entry its envelope does not carry.

    Verification
    ------------
    Before writing, FOUR gates apply:

    1. The row must already carry a non-NULL ``signature_bundle``.
       mark_claim_logged attaches a Rekor block to an existing
       envelope; it is not a path to sign an unsigned claim.
    2. The supplied bundle must be JSON.
    3. The bundle must be a structurally-valid claim envelope and its
       ``predicate.claim_id`` must equal the row's ``claim_id``. A buggy
       caller that mixes up claim ids cannot silently write Alice's
       bundle onto Bob's row.
    4. The supplied bundle's ``payload``, ``payloadType``, and
       ``signatures`` fields must be byte-identical to the row's
       existing ``signature_bundle``. The trigger
       ``claims_signed_fields_no_laundering`` refuses only a de-signing
       write to ``signature_bundle`` (non-NULL to NULL) and leaves the
       non-NULL rewrite legal, because the Rekor attachment needs it, so
       this function is the sole defense against a caller substituting a
       different envelope wholesale (different signer, different
       payload, different keyid). Only the optional
       top-level ``rekor`` block may differ between the existing and
       new bundles.

    Raises
    ------
    ClaimNotFoundError
        If no claim with claim_id exists.
    DatabaseError
        If the row has no existing signature_bundle, the supplied
        bundle is malformed, its payload's claim_id does not match,
        or it substantively differs from the existing bundle.
    """
    row = conn.execute(
        "SELECT supports_json, generated_by, unresolved, artifact_hash, "
        "signature_bundle "
        "FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")

    existing_bundle_raw = row["signature_bundle"]
    if existing_bundle_raw is None:
        raise DatabaseError(
            f"mark_claim_logged refused for claim '{claim_id}': the row "
            "carries no existing signature_bundle. Rekor inclusion attaches "
            "a transparency-log block to an already-signed envelope; an "
            "unsigned claim cannot be log-stamped retroactively. Sign the "
            "claim at assert time via mareforma.open(key_path=...)."
        )

    # Sanity-check that the supplied bundle actually belongs to this claim.
    # After Statement v1, claim_id lives inside the predicate.
    from mareforma import signing as _signing
    try:
        envelope = json.loads(new_signature_bundle)
        predicate = _signing.claim_predicate_from_envelope(envelope)
    except (json.JSONDecodeError, _signing.InvalidEnvelopeError) as exc:
        raise DatabaseError(
            f"mark_claim_logged given malformed bundle for {claim_id}: {exc}"
        ) from exc
    if predicate.get("claim_id") != claim_id:
        raise DatabaseError(
            f"mark_claim_logged bundle's predicate.claim_id "
            f"({predicate.get('claim_id')!r}) does not match row {claim_id!r}."
        )

    # Substitution gate. mark_claim_logged exists to attach a Rekor
    # inclusion block to the envelope that was already produced + signed
    # by add_claim. The new bundle must preserve the existing payload
    # bytes, signatures array, and payloadType, only the optional
    # top-level ``rekor`` block may differ. Without this check, a caller
    # could pass any DSSE-shaped envelope (different signer, freshly
    # forged signatures, same predicate.claim_id) and mareforma
    # would persist it: the claims_signed_fields_no_laundering trigger
    # refuses only a de-signing write, not a non-NULL substitution.
    try:
        existing_envelope = json.loads(existing_bundle_raw)
    except json.JSONDecodeError as exc:
        # Row's bundle column is corrupt, separate failure mode from
        # caller error. Surface so the operator can investigate.
        raise DatabaseError(
            f"mark_claim_logged refused for claim '{claim_id}': the "
            f"existing signature_bundle on the row is malformed ({exc}). "
            "Run `mareforma restore` (or `mareforma.restore(project_root)`) "
            "to surface and recover from the corruption."
        ) from exc
    if (
        envelope.get("payload") != existing_envelope.get("payload")
        or envelope.get("payloadType") != existing_envelope.get("payloadType")
        or envelope.get("signatures") != existing_envelope.get("signatures")
    ):
        raise DatabaseError(
            f"mark_claim_logged refused for claim '{claim_id}': the new "
            "bundle's payload, payloadType, or signatures differ from the "
            "existing row's signature_bundle. This function attaches a "
            "Rekor inclusion block to an existing envelope; it does not "
            "substitute one envelope for another. To re-sign, retract the "
            "claim (status='retracted') and assert a new one citing the "
            "retracted via contradicts=[<old_claim_id>]."
        )

    # Whitelist of allowed top-level envelope keys. The field-equality
    # check above only compares the cryptographically meaningful trio
    # (payload, payloadType, signatures); extra keys would slip through
    # and get persisted to signature_bundle. The only legitimate addition
    # mark_claim_logged exists to enable is the ``rekor`` block. Anything
    # else is a smuggling vector for opaque metadata that downstream
    # consumers (jsonld exporter, restore) would have to defend against
    # individually.
    _ALLOWED_BUNDLE_KEYS = frozenset(
        {"payload", "payloadType", "signatures", "rekor"}
    )
    extra_keys = set(envelope.keys()) - _ALLOWED_BUNDLE_KEYS
    if extra_keys:
        raise DatabaseError(
            f"mark_claim_logged refused for claim '{claim_id}': the new "
            f"bundle carries unexpected top-level keys {sorted(extra_keys)!r}. "
            "Only payload, payloadType, signatures, and rekor are allowed; "
            "smuggling additional metadata into signature_bundle is refused."
        )

    supports = json.loads(row["supports_json"] or "[]")
    generated_by = row["generated_by"]
    unresolved = int(row["unresolved"] or 0)
    artifact_hash = row["artifact_hash"]
    now = _now()

    try:
        with conn:
            conn.execute(
                "UPDATE claims SET signature_bundle = ?, "
                "transparency_logged = 1, updated_at = ? "
                "WHERE claim_id = ?",
                (new_signature_bundle, now, claim_id),
            )
    except (sqlite3.OperationalError, sqlite3.IntegrityError) as exc:
        raise DatabaseError(f"Failed to mark claim logged: {exc}") from exc

    _backup_claims_toml(conn, root)


def mark_claim_resolved(
    conn: sqlite3.Connection,
    root: Path,
    claim_id: str,
) -> None:
    """Clear the unresolved flag on a claim.

    Every DOI in supports[] and contradicts[] resolved, so the flag that said
    otherwise comes off and the claim reads as fully cited.

    Raises
    ------
    ClaimNotFoundError
        If no claim with claim_id exists.
    """
    row = conn.execute(
        "SELECT supports_json, generated_by, artifact_hash "
        "FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    if row is None:
        raise ClaimNotFoundError(f"Claim '{claim_id}' not found.")

    supports = json.loads(row["supports_json"] or "[]")
    generated_by = row["generated_by"]
    artifact_hash = row["artifact_hash"]
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
            # flag-clear (the actual user intent), but it must stay retryable
            # (retry flag + health event) rather than strand the claim.
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
    ``text`` / ``supports`` / ``contradicts``: those fields are part of
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

    # Same empty / cap / sanitize-on-write gate add_claim applies, so an edit
    # cannot re-introduce an injection payload, blow past the cap, or leak an
    # unsanitized string into the FTS index via the update trigger. Runs before
    # the signed-surface diff so both compare the canonical stored string.
    clean_text = _validate_claim_text(text) if text is not None else None

    # Refuse signed-surface mutations on signed claims. text/supports/
    # contradicts are the only signed-surface fields currently exposed by
    # update_claim's parameter list.
    if existing.get("signature_bundle") is not None:
        signed_field_changes: list[str] = []
        if clean_text is not None and clean_text != existing.get("text"):
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
    if clean_text is not None:
        new_text = clean_text
    if supports is not None:
        new_supports_json = json.dumps(supports)
    if contradicts is not None:
        new_contradicts_json = json.dumps(contradicts)
    if comparison_summary is not None:
        new_comparison_summary = comparison_summary

    # Refuse a support+contradict on the same upstream, the same incoherent
    # state add_claim rejects. Check the EFFECTIVE post-update lists (the new
    # side where provided, else the existing one) since an edit to either side
    # can create the overlap.
    _refuse_supports_contradicts_overlap(
        json.loads(new_supports_json or "[]"),
        json.loads(new_contradicts_json or "[]"),
    )

    # DOIs are no longer network-resolved, so a supports/contradicts edit
    # clears any legacy `unresolved` quarantine rather than re-checking.
    # Diff-check against the prior JSON skips the hot path when callers pass
    # identical lists (e.g. when only `text` or `status` is being edited).
    old_supports_json = existing.get("supports_json") or "[]"
    old_contradicts_json = existing.get("contradicts_json") or "[]"
    old_unresolved = int(existing.get("unresolved") or 0)
    supports_changed = supports is not None and new_supports_json != old_supports_json
    contradicts_changed = (
        contradicts is not None and new_contradicts_json != old_contradicts_json
    )

    # Cycle / self-loop check on the NEW supports[] if it changed. Signed
    # claims refuse supports mutation upstream (SignedClaimImmutableError
    # raised earlier in this function), so reaching here implies an
    # unsigned claim, the cycle-introduction window the acyclicity check covers.
    if supports_changed:
        new_supports_list = json.loads(new_supports_json)
        _check_no_cycle(conn, claim_id, new_supports_list)

    if supports_changed or contradicts_changed:
        new_unresolved = 0

    try:
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
            if supports_changed:
                # Keep the supports cache in step with the edited edges. The
                # count-only staleness heuristic never trips on an in-place
                # UPDATE, so the edges must be maintained here or
                # query_provenance serves the pre-edit lineage forever.
                from mareforma import _supports
                _supports.replace_supports_edges(
                    conn, claim_id, json.loads(new_supports_json))
    except sqlite3.IntegrityError as exc:
        translated = _state_error_from_integrity(exc)
        if translated is not None:
            raise translated from exc
        raise DatabaseError(f"Failed to update claim '{claim_id}': {exc}") from exc
    except sqlite3.OperationalError as exc:
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
    # Own the transaction the same way add_claim does. The append-only trigger's
    # RAISE(ABORT) backs out the statement but leaves the transaction open, and
    # every write helper reads conn.in_transaction to decide who commits, so a
    # caller that catches the documented refusal and keeps writing would lose
    # every later claim without an error.
    _own_transaction = not conn.in_transaction
    try:
        if _own_transaction:
            conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM claims WHERE claim_id = ?", (claim_id,))
        # Drop the row's cache edges in the same transaction so downstream and
        # upstream walks stop surfacing a dangling claim before the next open.
        from mareforma import _supports
        _supports.remove_claim_edges(conn, claim_id)
        if _own_transaction:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        if _own_transaction:
            conn.rollback()
        raise _signed_delete_error(exc, claim_id) from exc
    except sqlite3.OperationalError as exc:
        if _own_transaction:
            conn.rollback()
        raise DatabaseError(f"Failed to delete claim '{claim_id}': {exc}") from exc

    _backup_claims_toml(conn, root)


def _cached_validator(conn: sqlite3.Connection, cache: dict, keyid: str):
    """The validators row for *keyid*, read once per caller's read.

    Every pubkey the read path needs comes through here. The lookup is keyed on
    the keyid alone, unlike the signature entries, because the answer does not
    depend on the row being checked; it is still scoped to the caller's cache,
    so an enrollment written after this read is picked up by the next one.
    Without it a bulk read costs one validators SELECT per row checked, and the
    corroboration index costs one per peer it verifies.
    """
    ck = ("K", keyid)
    if ck not in cache:
        from mareforma import validators as _validators
        cache[ck] = _validators.get_validator(conn, keyid)
    return cache[ck]

def _row_verified_on_read(
    conn: sqlite3.Connection, row: dict, cache: dict,
) -> bool:
    """True iff *row*'s signed material still backs what the row says.

    This used to be gated on the stored level: only promoted rows were checked,
    because the level was the unsigned word an attacker could raise, and the
    corroboration behind it was what had to be re-proved on every read. There
    is no level now, so that whole question is gone, and with it the reason to
    check some rows and not others.

    What is left applies to every row, which is a wider net than the one it
    replaces. A row carrying a validation envelope has it verified, whether the
    envelope was written by ``validate`` or is a seed attestation on a graph
    old enough to have one. Every row has its asserter bundle re-verified
    against its own signed fields; a row with no envelope carries nothing to
    check and passes, the same exemption it always had.
    """
    if row.get("validation_signature") and not _verify_validation_on_read(
        conn, row, cache,
    ):
        return False
    return _verify_participant_bundle_on_read(conn, row, cache)


def count_unverified_rows(conn: sqlite3.Connection) -> int:
    """Count rows carrying signed material that does not re-verify on read.

    This counted promoted rows, because a stored level was the thing a direct
    writer could raise without touching a signature, and the health surface had
    to be unable to read green over a graph where one had been. The level is
    gone; the tampering it stood for is not. A signature can still be swapped,
    an envelope stapled onto another row, a signed field rewritten underneath.

    So the set is the rows where verification can fail at all: those carrying an
    asserter bundle or a validation envelope. Rows with neither are exempt for
    the reason they always were, they have nothing to check, and counting them
    would turn every legacy graph amber for no finding.

    Every row pays its own signature check. The cache is keyed by claim id as
    well as by bundle, deliberately, so two rows carrying one envelope cannot
    share a verdict: that pair is the copied-bundle attack, and letting the
    first row's answer stand for the second is what the key exists to stop. The
    cost is therefore linear in the rows carrying signed material, with no scan
    ceiling, which is the price of the surface reading honestly.
    """
    rows = conn.execute(
        f"SELECT {_CLAIM_SELECT} FROM claims "
        "WHERE signature_bundle IS NOT NULL OR validation_signature IS NOT NULL"
    ).fetchall()
    cache: dict = {}
    return sum(
        1 for row in rows if not _row_verified_on_read(conn, dict(row), cache)
    )


def _trust_domain_disclosure(conn: sqlite3.Connection) -> tuple[bool, str | None]:
    """(single_trust_domain, trust_domain_root) for this graph's validators.

    A graph-global property of the validator topology, attached per row so a
    consumer reading one claim sees whether all
    validators trace to one root of trust. It discloses trust-domain
    concentration; it is NOT a Sybil guard over the participant topology.
    """
    from mareforma import validators as _validators
    return (
        _validators.single_trust_domain(conn),
        _validators.trust_domain_root(conn),
    )


def _verify_validation_on_read(
    conn: sqlite3.Connection, row: dict, cache: dict,
) -> bool:
    """Re-verify a row's validation envelope (validator side)."""
    vs = row.get("validation_signature")
    if not vs:
        # The caller only reaches here for a row that carries an envelope,
        # and a row naming a validator without one is refused by the schema
        # CHECK; so this means a direct tamper. Refuse to serve it.
        return False
    try:
        env = json.loads(vs)
        keyid = env["signatures"][0]["keyid"]
        declared = env["payloadType"]
    except (json.JSONDecodeError, KeyError, IndexError, TypeError):
        return False
    # The cache value depends on the per-row claim_id binding check below, so
    # claim_id MUST be part of the key. Without it, two rows that carry the same
    # validation_signature bytes (an attacker copies a genuine envelope onto a
    # second row) would share a cache entry: the row evaluated first, sorted by
    # created_at, which the attacker controls, caches its result and poisons
    # the second, so a forged row could censor the legitimate validated claim.
    ck = (
        "V", keyid, row.get("claim_id"),
        hashlib.sha256(vs.encode("utf-8")).hexdigest(),
    )
    if ck in cache:
        return cache[ck]
    from mareforma import signing as _signing
    from mareforma import validators as _validators
    ok = False
    if declared in (_signing.PAYLOAD_TYPE_VALIDATION, _signing.PAYLOAD_TYPE_SEED):
        signer_row = _cached_validator(conn, cache, keyid)
        # Presence in the table is not enrolment. The validators table has no
        # INSERT guard, so one INSERT carrying a real pubkey and a junk
        # enrollment envelope makes any key look like a validator; is_enrolled
        # walks the chain back to the self-signed root, the same bar
        # _verdict_verifies and the CLI apply. Without it, that one INSERT
        # makes a claim read as validated under a signature that verifies
        # against a key the project never enrolled.
        if signer_row is not None and _validators.is_enrolled(conn, keyid):
            try:
                pem = base64.standard_b64decode(signer_row["pubkey_pem"])
                pub = _signing.public_key_from_pem(pem)
                if _signing.verify_envelope(
                    env, pub, expected_payload_type=declared,
                ):
                    # The signature is genuine; confirm the signed payload
                    # binds THIS claim so a valid envelope from another claim
                    # cannot be replayed onto this row.
                    payload = json.loads(
                        base64.standard_b64decode(env["payload"])
                    )
                    ok = payload.get("claim_id") == row.get("claim_id")
                    # And that it names the validator the row names.
                    # ``validator_keyid`` is an unsigned denormalisation of the
                    # signed payload, and the reputation count groups by that
                    # column while its docstring says it asks the envelope. A
                    # row disagreeing with its own envelope about who signed off
                    # is refused rather than read either way round.
                    if ok and row.get("validator_keyid") is not None:
                        ok = payload.get("validator_keyid") == row.get(
                            "validator_keyid"
                        )
                    if ok:
                        # The envelope is genuine, binds this claim, and comes
                        # from an enrolled key. Whether that key was entitled to
                        # sign off is a separate question, and it is the one the
                        # write path asks. Skipping it here served a row the
                        # same graph refuses to write.
                        #
                        # The llm rule applies whatever the envelope calls
                        # itself, because the write path applies it to both: the
                        # seed path refuses an llm signer in as many words, so
                        # that an llm validator cannot route around the rule by
                        # seeding instead of validating.
                        #
                        # The self-validation rule is where the two types differ,
                        # and the difference is not an exemption to assume. A
                        # seeded claim is attested by its own asserter,
                        # so for a seed that is the thing to REQUIRE. The signer
                        # picks the payloadType, and taking the word for it let
                        # any enrolled key sign off on any claim by calling
                        # its envelope a seed.
                        try:
                            _refuse_llm_validator(conn, keyid)
                            if declared == _signing.PAYLOAD_TYPE_VALIDATION:
                                _refuse_self_validation(
                                    row.get("claim_id"),
                                    row.get("signature_bundle"),
                                    keyid,
                                )
                            elif row.get("signature_bundle") and keyid \
                                    != _claim_asserting_keyid(
                                        row["signature_bundle"]):
                                ok = False
                        except Exception:
                            ok = False
            except Exception:
                ok = False
        # No row, or a row whose chain does not walk back to the root -> the
        # validator keyid is not enrolled. A validation can only
        # come from an enrolled validator, so this is a forged row: leave ok
        # False (excluded).
    cache[ck] = ok
    return ok


def _signed_field_mismatch(pred: dict, row: dict) -> str | None:
    """Name the first SIGNED_FIELDS value the row and its predicate disagree on.

    Returns None when the signed predicate binds this row's content exactly.
    Binding the claim_id alone leaves the signature decorative: the row's text,
    classification, provenance and links can all be rewritten under an envelope
    that still verifies. Shared by the read-path gate and the audit path so the
    two cannot drift apart.

    Every comparison runs through :func:`signed_value_matches`, not ``!=``. The
    signature covers canonical bytes, which NFC-normalize every string, while
    the row keeps the bytes the caller passed, so text that arrives decomposed
    (a macOS filename, a PDF extract, most text typed in Vietnamese or Korean)
    is signed composed and stored decomposed. A bare ``!=`` reports those as a
    mismatch, which drops an honest claim on read and makes restore refuse the
    whole backup naming it as tampered. Comparing up to NFC form is what the
    signature already means and narrows nothing: a value that still differs
    after normalization differs here too.
    """
    from mareforma import signing as _signing
    expected = {
        "claim_id": row.get("claim_id"),
        "text": row.get("text"),
        "classification": row.get("classification"),
        "generated_by": row.get("generated_by"),
        "supports": _json_list(row.get("supports_json")),
        "contradicts": _json_list(row.get("contradicts_json")),
        "source_name": row.get("source_name"),
        "artifact_hash": row.get("artifact_hash"),
        "created_at": row.get("created_at"),
    }
    for field in _signing.SIGNED_FIELDS:
        if not signed_value_matches(pred.get(field), expected[field]):
            return field
    # The evidence vector and the observed-grounding verdict are signed and
    # chained too, but they live outside SIGNED_FIELDS (one is a nested dict,
    # the other is optional), so restore checks them by hand. Do the same here
    # or a rewritten verdict reads clean and unlocks the promotion the real
    # verdict blocked. Both sides are parsed so key ordering cannot fake a
    # mismatch; absent on both sides is the pre-observer case and passes.
    if not signed_value_matches(
        pred.get("evidence"), _json_object(row.get("evidence_json"), {}),
    ):
        return "evidence"
    if not signed_value_matches(
        pred.get("observed_grounding"), _json_object(row.get("observed_grounding")),
    ):
        return "observed_grounding"
    return None


def _legacy_unsigned_row(conn: sqlite3.Connection, row, cache: dict) -> bool:
    """True when a row carrying no signature is legacy, not de-signed.

    The participant check has no envelope to hold an unsigned row to, so it
    exempts one. That exemption was keyed on the claims row alone, and every
    column it reads is one a writer with SQL access is already assigning:
    ``UPDATE claims SET asserter_keyid = NULL, signature_bundle = NULL`` turns
    any claim into a legacy one, and a bare INSERT mints one from nothing. The
    exemption then serves a fabricated row as though it predated signing.

    So the grandfather asks two questions instead. ``statement_cid`` is written
    at signing time and no honest path clears it, so it separates "unsigned from
    birth" from "signed once and stripped". And the PROJECT is asked whether it
    signs at all, through the validators table, which is a different table that
    no UPDATE against ``claims`` reaches: a project that enrols a validator does
    not serve a claim carrying no signature. This is the rule
    ``trust._gate._signer_identity`` already applies to the same claims, so the
    read path and the gate speak for one graph.

    *cache* is the caller's verify cache, so the validators probe runs once per
    read. A caller whose SELECT omitted ``statement_cid`` (restore reads a
    narrow column list) has it looked up, so both paths apply the same rule
    rather than a laxer one on the recovery side.
    """
    ck = ("PS",)
    if ck not in cache:
        from mareforma.trust._gate import _project_signs
        cache[ck] = _project_signs(conn)
    if cache[ck]:
        return False
    try:
        cid = row["statement_cid"]
    except (KeyError, IndexError):
        found = conn.execute(
            "SELECT statement_cid FROM claims WHERE claim_id = ?",
            (row["claim_id"],),
        ).fetchone()
        cid = found["statement_cid"] if found is not None else None
    return cid is None



def _verify_participant_bundle_on_read(
    conn: sqlite3.Connection, row: dict, cache: dict,
) -> bool:
    """Re-verify a row's asserter bundle (participant side).

    Legacy (no bundle, no keyid) rows are verify-exempt, they carry no envelope
    to check. A row with a keyid but no bundle is not legacy: the keyid is
    derived from the bundle on the only honest write path, so that pair means
    the bundle was cleared by direct SQL, and the row is refused. Otherwise the
    bundle's signed predicate MUST match THIS row on every signed field and be
    subject/predicate-consistent. That binding holds even when the asserter is
    not an enrolled validator, so a genuine bundle copied off another claim
    cannot be stapled onto this row, a rewritten field cannot hide under a valid
    envelope, and a junk bundle is rejected, with no pubkey needed. The signer is
    read out of the bundle, never off the row: ``asserter_keyid`` is an unsigned
    denormalisation, so a row that disagrees with its own envelope is refused
    rather than trusted. When the signer IS enrolled the bundle signature is
    also verified against that pubkey; a forged or tampered signature
    excludes the row. When it is not enrolled there is no pubkey in the lean
    model, so a claim-bound bundle is served (exempt on authenticity, never on
    the claim binding).
    """
    ak = row.get("asserter_keyid")
    bundle_json = row.get("signature_bundle")
    if not bundle_json:
        # No envelope to check. That is the legacy case only when the claim was
        # never signed AND the project does not sign at all; otherwise the row
        # was de-signed or INSERTed, and an exemption would serve a promoted
        # claim that carries no attribution whatsoever.
        return ak is None and _legacy_unsigned_row(conn, row, cache)
    # claim_id is part of the key: the binding check below depends on the row,
    # so two rows sharing one bundle (a copy attack) must not share a cache
    # entry or the first-evaluated row poisons the second (same reasoning as
    # the validator path).
    ck = (
        "P", ak, row.get("claim_id"),
        hashlib.sha256(bundle_json.encode("utf-8")).hexdigest(),
    )
    if ck in cache:
        return cache[ck]
    from mareforma import signing as _signing
    ok = False
    try:
        env = json.loads(bundle_json)
        # The signer the bundle itself names. A missing or malformed signatures
        # array raises and is excluded.
        signer = env["signatures"][0]["keyid"]
        # asserter_keyid is an unsigned denormalisation of that signer. A row
        # that contradicts its own envelope was written outside the signing
        # path, so refuse it rather than let the column pick the pubkey.
        keyid_agrees = ak is None or ak == signer
        # Content binding (no pubkey needed): the signed predicate must match
        # THIS row on every signed field, not just its claim_id.
        # claim_predicate_from_envelope also enforces subject-vs-predicate
        # consistency, so a junk or internally-inconsistent bundle raises and is
        # excluded.
        pred = _signing.claim_predicate_from_envelope(env)
        if keyid_agrees and _signed_field_mismatch(pred, row) is None:
            signer_row = _cached_validator(conn, cache, signer)
            if signer_row is None:
                # Non-enrolled asserter: no pubkey in the lean model. The
                # binding above is the integrity we can offer; serve it.
                ok = True
            else:
                pem = base64.standard_b64decode(signer_row["pubkey_pem"])
                pub = _signing.public_key_from_pem(pem)
                ok = bool(_signing.verify_envelope(env, pub))
                # Multi-role parity: a claim-with-roles:v1 bundle carries role
                # attestations in signatures[1:]. verify_envelope only checked
                # signatures[0] (the asserter); walk the rest so a forged role
                # signature is caught on the LIVE read path, not only at restore.
                if ok:
                    ok = _verify_role_signatures(conn, env)
    except Exception:
        ok = False
    cache[ck] = ok
    return ok


def _verify_role_signatures(conn: sqlite3.Connection, env: dict) -> bool:
    """Verify the role attestations in signatures[1:] of a claim-with-roles bundle.

    The shared read-path routine for multi-signature envelopes. Applies the same
    contract restore enforces (:func:`db.restore._verify_claim_signatures_on_restore`):
    every signature beyond the asserter must carry a role in
    :data:`signing.VALID_CLAIM_ROLES`, roles are unique, each signer's keyid must
    be enrolled, and each signature must verify against that keyid's pubkey over
    the DSSE PAE. Any deviation → ``False``. A single-signature (asserter-only)
    bundle has nothing extra to check and passes.
    """
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    sigs = env.get("signatures") or []
    if len(sigs) <= 1:
        return True
    try:
        payload_bytes = base64.standard_b64decode(env["payload"])
    except (KeyError, TypeError, ValueError):
        return False
    pae = _signing.dsse_pae(_signing.PAYLOAD_TYPE_CLAIM, payload_bytes)
    seen_roles: set[str] = set()
    for entry in sigs[1:]:
        if not isinstance(entry, dict):
            return False
        role = entry.get("role")
        keyid = entry.get("keyid")
        if not isinstance(role, str) or role not in _signing.VALID_CLAIM_ROLES:
            return False
        if role in seen_roles:
            return False
        seen_roles.add(role)
        if not isinstance(keyid, str):
            return False
        signer_row = _validators.get_validator(conn, keyid)
        # A row in the table is not an enrolment: the chain has to walk back to
        # the self-signed root, or one direct INSERT of a real pubkey under a
        # junk envelope buys a role attestation the project never granted.
        if signer_row is None or not _validators.is_enrolled(conn, keyid):
            return False  # orphan or unchained signer, not enrolled
        try:
            pem = base64.standard_b64decode(signer_row["pubkey_pem"])
            pub = _signing.public_key_from_pem(pem)
            sig_bytes = base64.standard_b64decode(entry["sig"])
            pub.verify(sig_bytes, pae)
        except Exception:
            return False
    return True


def verify_claim_signatures(
    conn: sqlite3.Connection, row: dict,
) -> tuple[bool, str]:
    """Audit-grade, tier-independent re-verification of a claim's signatures.

    Where :func:`_row_verified_on_read` answers a listing surface, this is the
    explicit ``mareforma verify`` audit and re-checks the whole bundle. It confirms
    the signed predicate binds THIS row (claim_id + every signed field matches,
    catching a hand-edited row), verifies the asserter signature when the
    asserter is enrolled, and verifies all role attestations. Returns
    ``(ok, reason)``; ``reason`` is empty on success.

    An unsigned claim returns ``(True, "")``, there is no signature to break;
    its lack of attribution is reported by the trust map, not failed here. A row
    that kept its ``asserter_keyid`` but lost its bundle is not unsigned, it was
    de-signed by direct SQL, and fails. A non-enrolled asserter cannot have its
    signature checked against a pubkey in the lean model, so the claim-binding
    (predicate names this row, signed fields match) is the integrity floor
    offered.
    """
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    bundle_json = row.get("signature_bundle")
    if not bundle_json:
        if row.get("asserter_keyid") is not None:
            return (False, "signature bundle was removed from a signed claim")
        return (True, "")
    try:
        env = json.loads(bundle_json)
    except (ValueError, TypeError):
        return (False, "signature bundle is not valid JSON")
    try:
        pred = _signing.claim_predicate_from_envelope(env)
    except Exception:
        return (False, "signature bundle envelope is structurally invalid")

    if not signed_value_matches(pred.get("claim_id"), row.get("claim_id")):
        return (False, "signed predicate does not bind this claim id")

    mismatch = _signed_field_mismatch(pred, row)
    if mismatch is not None:
        return (False, f"signed field {mismatch!r} does not match the row (tampered)")

    # The signer the bundle itself names. ``asserter_keyid`` is an unsigned
    # denormalisation of it, written from the bundle on the only honest path
    # (see :func:`assert_claim`). A row whose column contradicts its own
    # envelope was written outside that path, so refuse it rather than let the
    # column pick the pubkey to check against. ``ak is None`` is the legacy row
    # (bundle present, keyid never denormalised) and is not a disagreement. This
    # mirrors the participant-side guard in _verify_participant_bundle_on_read,
    # and keeps this surface reading the same keyid the CLI enrollment
    # disclosure reads.
    bundle_keyid = _extract_signature_bundle_keyid(bundle_json)
    # A bundle that names no signer is not the legacy row. ``_extract`` answers
    # None for two different things: a row that never denormalised its keyid
    # (legacy, honest) and a bundle whose ``signatures`` array is empty or
    # malformed (nothing to verify). Conflating them let an empty array satisfy
    # the agreement check below, skip the pubkey block entirely, and fall
    # through to verified: exit 0 over a bundle carrying no signature at all.
    # The legacy row is bundle-with-a-signer plus a NULL column, which the
    # ``ak is None`` arm below still admits.
    if bundle_keyid is None:
        return (False, "signature bundle names no signer, so nothing can be verified")
    ak = row.get("asserter_keyid")
    if not (ak is None or ak == bundle_keyid):
        return (False, "row asserter keyid disagrees with its signature bundle")
    keyid = bundle_keyid
    signer_row = _validators.get_validator(conn, keyid)
    if signer_row is not None:
        try:
            pem = base64.standard_b64decode(signer_row["pubkey_pem"])
            pub = _signing.public_key_from_pem(pem)
            if not _signing.verify_envelope(env, pub):
                return (False, "asserter signature failed verification")
        except Exception:
            return (False, "asserter signature could not be verified")
        # A row in the validators table is not an enrolment. The table has
        # no INSERT guard, so one INSERT with a real pubkey and a junk
        # envelope puts a key there; ``is_enrolled`` walks the chain back to
        # the self-signed root, which is what "registered" means everywhere
        # else. A signer whose chain does not walk back is not a stranger
        # the lean model has no pubkey for, it is a forged enrolment, and an
        # audit that answered "verified" over it would report the forgery as
        # attribution.
        if not _validators.is_enrolled(conn, keyid):
            return (
                False,
                "asserter key has a validators row whose enrollment does "
                "not chain back to the project root",
            )

    if not _verify_role_signatures(conn, env):
        return (False, "a role signature failed verification")
    return (True, "")


def _json_object(value, empty=None):
    """Parse a JSON-object column into a dict; ``None``/empty → *empty*.

    A malformed value is returned as-is, so it can never compare equal to a
    signed predicate value and the row reads as tampered rather than clean.
    """
    if value is None or value == "":
        return empty
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except (ValueError, TypeError):
        return value
    return parsed if isinstance(parsed, dict) else value


def _json_list(value) -> list:
    """Parse a JSON-array column into a list; ``None``/malformed → ``[]``."""
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (ValueError, TypeError):
        return []
    return parsed if isinstance(parsed, list) else []


def get_claim(
    conn: sqlite3.Connection,
    claim_id: str,
    *,
    verify_cache: dict | None = None,
) -> dict | None:
    """Return a claim dict or None if not found.

    Every row carries a ``verified`` boolean: the read path re-verifies its
    signatures and flags the result rather than excluding the row, so an auditor
    can still see a tampered row and know it failed. A row with no envelope
    carries nothing to check and reads ``verified=True``.

    It also carries ``generator_enrolled``, for the reason the enumerating reads
    do. A claim whose signer the project never enrolled used to be held back
    from the default read; it is served now, so every surface that serves it has
    to say what it is rather than leave the caller to assume somebody vouched
    for it.

    *verify_cache* lets a caller reading several claims share one cache, the
    way :func:`list_claims` shares one across its rows: the peer evidence
    behind a promoted row is verified once for the caller's whole read instead
    of once per claim. A fresh cache when it is omitted.
    """
    try:
        row = conn.execute(
            f"SELECT {_CLAIM_SELECT} FROM claims WHERE claim_id = ?",
            (claim_id,),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to fetch claim '{claim_id}': {exc}") from exc
    if not row:
        return None
    d = dict(row)
    d["verified"] = _row_verified_on_read(
        conn, d, {} if verify_cache is None else verify_cache,
    )
    # A graph-global property of the validator topology. It used to ride only
    # on promoted rows, because those were the ones a consumer would act on;
    # every row is that row now.
    std, root_kid = _trust_domain_disclosure(conn)
    d["single_trust_domain"] = std
    d["trust_domain_root"] = root_kid
    gen_keyid = _extract_signature_bundle_keyid(d.get("signature_bundle"))
    d["generator_enrolled"] = (
        gen_keyid is not None
        and gen_keyid in _enrolled_validator_keyids(conn)
    )
    return d


def list_claims(
    conn: sqlite3.Connection,
    *,
    status: str | None = None,
    source_name: str | None = None,
    generated_by: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    """Return all claims, optionally filtered, each carrying ``verified``.

    Uses an explicit column list (not SELECT *) to avoid coupling to schema changes.

    Like :func:`get_claim`, and unlike ``query`` / ``search``, this flags a
    high-trust row whose signature does not re-verify instead of dropping it: a
    bulk dump feeds auditors and exports, and a row that vanished would be
    indistinguishable from one that was never written. Consumers that publish
    the row must act on the flag; :func:`refuse_unverified_claims` is the shared
    gate for that. One verify cache per call, so a large graph pays one
    verification per distinct signature.

    ``limit`` caps the number of rows returned (the newest by ``created_at``),
    so a caller can bound the verify-on-read work on a large graph the way
    ``query`` and ``search`` already do. Omitted, every matching row is returned
    as before. A negative limit is a caller error and is refused; zero returns
    no rows.
    """
    if limit is not None:
        _require_non_negative_limit(limit, "list_claims")
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
    limit_clause = ""
    if limit is not None:
        limit_clause = " LIMIT ?"
        params.append(limit)
    try:
        rows = conn.execute(
            f"SELECT {_CLAIM_SELECT} FROM claims {where} "
            f"ORDER BY created_at DESC{limit_clause}",
            params,
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to list claims: {exc}") from exc

    verify_cache: dict = {}
    claims = []
    # Read once for the whole listing rather than per row: the set does not
    # change under a single read, and this is the export feed.
    enrolled = _enrolled_validator_keyids(conn)
    for row in rows:
        d = dict(row)
        d["verified"] = _row_verified_on_read(conn, d, verify_cache)
        gen_keyid = _extract_signature_bundle_keyid(d.get("signature_bundle"))
        d["generator_enrolled"] = (
            gen_keyid is not None and gen_keyid in enrolled
        )
        claims.append(d)
    return claims


def refuse_unverified_claims(claims: "Iterable[dict]") -> None:
    """Raise :class:`UnverifiedClaimError` if any claim failed verify-on-read.

    The gate every export shares. A claim dict comes from :func:`list_claims` or
    :func:`get_claim`, both of which flag rather than drop, so a publishing
    surface has to refuse the document itself, or a row whose signed material
    does not check out leaves the machine with nothing marking it.

    A row carrying no ``verified`` key is refused too. It never went through
    verify-on-read, so testing the flag alone would read it as clean: a caller
    handing over rows from a plain select would pass this gate having checked
    nothing.
    """
    claims = list(claims)
    unchecked = sorted(c["claim_id"] for c in claims if "verified" not in c)
    if unchecked:
        raise UnverifiedClaimError(
            f"Refusing to export {len(unchecked)} claim(s) that carry no "
            f"verify-on-read result: {', '.join(unchecked)}. Fetch rows with "
            "list_claims or get_claim, which flag every row they return."
        )
    forged = sorted(c["claim_id"] for c in claims if c["verified"] is False)
    if forged:
        raise UnverifiedClaimError(
            f"Refusing to export {len(forged)} claim(s) whose signature did not "
            f"re-verify: {', '.join(forged)}. Run `mareforma verify <claim_id>` "
            "for the detail, then retract or repair the row."
        )


def delete_claims_by_generated_by(
    conn: sqlite3.Connection,
    root: Path,
    generated_by: str,
) -> int:
    """Delete all claims with the given generated_by tag.

    Returns the number of claims deleted.
    """
    # Same transaction ownership as delete_claim: a refusal must not leave the
    # connection in-transaction for the next writer to inherit.
    _own_transaction = not conn.in_transaction
    try:
        rows = conn.execute(
            "SELECT claim_id FROM claims WHERE generated_by = ?",
            (generated_by,),
        ).fetchall()
        claim_ids = [r[0] for r in rows]
        if not claim_ids:
            return 0
        if _own_transaction:
            conn.execute("BEGIN IMMEDIATE")
        # The f-string interpolates only ``?`` placeholders, one per id, never
        # data, so this is the standard parameterised-IN idiom, not injection.
        # The real bound is SQLite's host-parameter cap
        # (SQLITE_LIMIT_VARIABLE_NUMBER): a cohort larger than the cap would need
        # chunking, the same limit tests/test_sql_variable_limit.py pins for the
        # convergence and dangling-support queries.
        placeholders = ",".join("?" * len(claim_ids))
        conn.execute(
            f"DELETE FROM claims WHERE claim_id IN ({placeholders})", claim_ids
        )
        # Drop every deleted claim's cache edges in the same transaction, and
        # decrement the staleness counter once per removed claim.
        from mareforma import _supports
        for deleted_id in claim_ids:
            _supports.remove_claim_edges(conn, deleted_id, count_delta=0)
        _supports._bump_source_count(conn, delta=-len(claim_ids))
        if _own_transaction:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        if _own_transaction:
            conn.rollback()
        raise _signed_delete_error(exc) from exc
    except sqlite3.OperationalError as exc:
        if _own_transaction:
            conn.rollback()
        raise DatabaseError(f"Failed to delete claims: {exc}") from exc

    _backup_claims_toml(conn, root)
    return len(claim_ids)


_VALID_REPLICATION_METHODS = (
    "hash-match",
    "semantic-cluster",
    "shared-resolved-upstream",
    "cross-method",
    # Signed-bracket tournament replay: an external verdict-issuer (e.g.
    # mareforma_elo) has independently replayed a signed Elo tournament
    # bracket and confirms two claims converge under that bracket's
    # outcome. The replay itself produces a signed
    # ``elo-bracket-snapshot/v1`` predicate; this verdict method records
    # the convergence it attests to.
    "signed-elo-bracket-replay",
)


_REPLICATION_VERDICT_PAYLOAD_TYPE = (
    "application/vnd.mareforma.replication-verdict+json"
)

_REPLICATION_VERDICT_FIELDS = (
    "verdict_id",
    "cluster_id",
    "member_claim_id",
    "other_claim_id",
    "method",
    "confidence",
)

_CONTRADICTION_VERDICT_PAYLOAD_TYPE = (
    "application/vnd.mareforma.contradiction-verdict+json"
)

_CONTRADICTION_VERDICT_FIELDS = (
    "verdict_id",
    "member_claim_id",
    "other_claim_id",
    "confidence",
)


def _verdict_canonical_payload(
    fields: tuple[str, ...], record: dict,
) -> bytes:
    """Canonical JSON of a verdict record under a fixed field set.

    Uses :func:`mareforma._canonical.canonicalize` so verdicts and
    claims share one canonicalization contract (sorted keys, NFC
    Unicode normalization, no whitespace, ``allow_nan=False``).
    A third-party verdict-issuer implementing against the same
    canonical-JSON contract produces signatures the OSS core
    verifies; a confidence dict containing NaN / Inf is rejected at
    sign time rather than producing a payload some verifiers refuse.
    """
    from .._canonical import canonicalize
    payload = {name: record.get(name) for name in fields}
    return canonicalize(payload)


def _replication_verdict_pae(record: dict) -> bytes:
    """The DSSE PAE a replication verdict's signature is made and checked over.

    Signing on the live path, restore's verify-before-INSERT and the read
    path's re-verification all build the signed bytes here, so the canonical
    form cannot drift into one version per caller.
    """
    from mareforma import signing as _signing
    return _signing.dsse_pae(
        _REPLICATION_VERDICT_PAYLOAD_TYPE,
        _verdict_canonical_payload(_REPLICATION_VERDICT_FIELDS, record),
    )


def _contradiction_verdict_pae(record: dict) -> bytes:
    """The DSSE PAE a contradiction verdict's signature is made and checked over.

    The signing path and restore's verify-before-INSERT both build the signed
    bytes here, so the canonical form cannot drift into one version per caller,
    the same discipline :func:`_replication_verdict_pae` holds for its sibling.
    """
    from mareforma import signing as _signing
    return _signing.dsse_pae(
        _CONTRADICTION_VERDICT_PAYLOAD_TYPE,
        _verdict_canonical_payload(_CONTRADICTION_VERDICT_FIELDS, record),
    )


_VERDICT_CHAIN_LINK_FIELDS = (
    "seq",
    "prev_tip",
    "verdict_kind",
    "verdict_id",
    "verdict_digest",
    # The author, inside the bytes. Left out, the link's signed payload and its
    # tip were identical whoever signed it, so the column naming the author was
    # free for an attacker to set and the tip did not notice. Safe to add here
    # and nowhere else: this payload type ships for the first time in this
    # release, so no reader anywhere rebuilds these bytes from a shorter list.
    "issuer_keyid",
)

# The tip a chain starts from. Empty rather than a hash of nothing, so the
# first link is recognisable as the first by reading it.
_VERDICT_CHAIN_GENESIS = ""


def _verdict_chain_link_pae(record: dict) -> bytes:
    """The DSSE PAE a chain link's signature is made and checked over.

    Its own payload type, so the signature cannot be confused with the one over
    the verdict the link covers. Both are made by the same issuer key over
    bytes naming the same ``verdict_id``, and only the type separates them.
    """
    from mareforma import signing as _signing
    return _signing.dsse_pae(
        _signing.PAYLOAD_TYPE_VERDICT_CHAIN_LINK,
        _verdict_canonical_payload(_VERDICT_CHAIN_LINK_FIELDS, record),
    )


def _verdict_chain_tip(record: dict) -> str:
    """The tip a link's own contents produce.

    Over the canonical payload rather than the signature: the tip has to be
    recomputable by anyone holding the file, including a reader with no key at
    all, or the chain could only be checked by its own signers.
    """
    return hashlib.sha256(
        _verdict_canonical_payload(_VERDICT_CHAIN_LINK_FIELDS, record)
    ).hexdigest()


def _verdict_chain_head(conn: sqlite3.Connection) -> "tuple[int, str]":
    """The last link's ``(seq, tip)``, or ``(0, genesis)`` on an empty chain.

    Read inside the caller's write transaction. Two writers that read the same
    head would build two links claiming the same ``prev_tip``, and the chain
    would fork with both halves verifying; the ``BEGIN IMMEDIATE`` around the
    verdict write is what stops that, and the PRIMARY KEY on ``seq`` refuses
    the loser if it ever does not.
    """
    row = conn.execute(
        "SELECT seq, tip FROM verdict_chain ORDER BY seq DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return (0, _VERDICT_CHAIN_GENESIS)
    return (row["seq"], row["tip"])


def _append_verdict_chain_link(
    conn: sqlite3.Connection,
    *,
    verdict_kind: str,
    verdict_id: str,
    signature: bytes,
    signer: "object",
    issuer_keyid: str,
    created_at: str,
) -> None:
    """Append the link covering one verdict, signed by that verdict's issuer.

    Called from inside the verdict's own write transaction, never from the
    backup writer. A verdict that committed without its link would break the
    chain for a reason that is not tamper, and the reader cannot tell the two
    apart, so the two writes are one write or neither.

    The link binds to the verdict's signature rather than to its row. The
    signature is the one field only the issuer could have produced, so a
    re-signed lookalike carrying the same ids does not satisfy the link.
    """
    seq, prev_tip = _verdict_chain_head(conn)
    record = {
        "seq": seq + 1,
        "prev_tip": prev_tip,
        "verdict_kind": verdict_kind,
        "verdict_id": verdict_id,
        "verdict_digest": hashlib.sha256(signature).hexdigest(),
        "issuer_keyid": issuer_keyid,
    }
    conn.execute(
        """
        INSERT INTO verdict_chain(
            seq, prev_tip, tip, verdict_kind, verdict_id, verdict_digest,
            issuer_keyid, signature, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record["seq"], record["prev_tip"], _verdict_chain_tip(record),
            verdict_kind, verdict_id, record["verdict_digest"],
            issuer_keyid, signer.sign(_verdict_chain_link_pae(record)),
            created_at,
        ),
    )


def verdict_chain_tip(conn: sqlite3.Connection) -> str:
    """The current tip of the verdict-set chain, ``""`` when it is empty."""
    return _verdict_chain_head(conn)[1]


def verdict_chain_coverage(conn: sqlite3.Connection) -> "tuple[int, int]":
    """``(covered, total)`` verdicts, counting both verdict tables.

    They differ on any graph that recorded verdicts before this version, and
    the difference is the point: those verdicts have no link and the chain says
    nothing about them. Reporting the pair keeps that a number an operator can
    read rather than a silence in the middle of an artifact about absence.
    """
    total = conn.execute(
        "SELECT (SELECT COUNT(*) FROM contradiction_verdicts) "
        "     + (SELECT COUNT(*) FROM replication_verdicts)"
    ).fetchone()[0]
    covered = conn.execute("SELECT COUNT(*) FROM verdict_chain").fetchone()[0]
    return (covered, total)


def verify_verdict_chain(conn: sqlite3.Connection) -> "tuple[str, ...]":
    """Every way the stored chain fails to account for the verdicts it covers.

    Empty means all of: the links run 1..n with no gap, each carries the tip its
    own contents produce, each names the tip of the link before it, each
    signature verifies against an enrolled issuer, and each covers a verdict
    that is still present carrying the signature the link was made over.

    What a clean result rules out, stated as narrowly as it holds: no verdict
    has been taken out of the middle of the chain by anyone holding no enrolled
    key. Removing a verdict means removing its link, and the next link then has
    to be re-signed over the gap, its tip recomputed over the new contents, and
    the verdict it covers made to verify under the key the link names. An
    outside attacker with file access and the project operator are held out by
    that.

    **An enrolled peer is not held out, and this used to say it was.** The
    verdict's own signed payload carries no issuer, so a peer can put its keyid
    on a surviving verdict, re-sign the verdict under its own key, and re-sign
    the link to match. Every check here then passes, on a chain that peer just
    shortened. Reproduced against this code, not reasoned about. Closing it
    needs the issuer inside the verdict's signed bytes, which changes bytes an
    already-released reader rebuilds, so it waits for a release that can pay
    for that.

    The issuer of the verdicts is not held out either, and cannot be: a key can
    always restate its own view of its own verdicts, and on a graph where one
    issuer signed everything that is the whole chain.

    Two further things it does not say. A removed suffix leaves a shorter chain
    that verifies, so length is reported by :func:`verdict_chain_coverage`
    rather than checked here. And verdicts recorded before the chain existed
    carry no link, which the same coverage pair is what makes visible.

    Never raises. A graph too damaged to read the chain from reports that as a
    problem rather than taking the caller down.
    """
    try:
        links = conn.execute(
            "SELECT seq, prev_tip, tip, verdict_kind, verdict_id, "
            "verdict_digest, issuer_keyid, signature "
            "FROM verdict_chain ORDER BY seq"
        ).fetchall()
    except sqlite3.Error as exc:
        return (f"the verdict chain could not be read: {exc}",)

    problems: list[str] = []
    cache: dict = {}
    expected_prev = _VERDICT_CHAIN_GENESIS
    expected_seq = 1
    for link in links:
        name = f"link {link['seq']} (verdict {link['verdict_id']!r})"
        try:
            _check_verdict_chain_link(
                conn, cache, link, name, expected_seq, expected_prev, problems,
            )
        except Exception as exc:      # noqa: BLE001, see the contract above
            # Every exception, not only sqlite3's. The enrolment walk and the
            # covered-verdict lookup read tables this function does not own, and
            # on a graph somebody has taken apart they raise whatever they
            # raise: a signature column holding TEXT where the schema says BLOB
            # reached hashlib as a str and came out a TypeError, which walked
            # straight past a handler scoped to sqlite3.Error and took the
            # caller down. That column is one of the things an attacker edits,
            # so the shape most likely to arrive here was the one shape this did
            # not catch. A link that cannot be checked is reported as unchecked,
            # never skipped: the damage is the finding.
            problems.append(
                f"{name} could not be checked, the graph is not readable "
                f"here: {exc}"
            )
        expected_prev = link["tip"]
        expected_seq = link["seq"] + 1
    return tuple(problems)


def _verdict_verifies(
    conn: sqlite3.Connection, cache: dict, v: sqlite3.Row,
) -> bool:
    """True iff verdict *v*'s issuer is enrolled and its signature checks out.

    Restore verifies each verdict before inserting it, and that precondition
    does not travel to the live read path, where ``replication_verdicts`` is
    whatever a process with SQL access wrote. Each verdict is therefore held
    against its issuer here: the keyid must be an enrolled validator whose chain
    verifies, the bar the recording path applies, and the signature must verify
    over the DSSE PAE rebuilt from the stored columns. A verdict that fails
    names nobody.

    Never raises: a forged or unparseable verdict is not evidence, and a read
    must degrade rather than crash. *cache* is the caller's verify cache, so one
    issuer's pubkey is read once however many verdicts it signed.
    """
    from mareforma import signing as _signing
    from mareforma import validators as _validators
    signer_row = _cached_validator(conn, cache, v["issuer_keyid"])
    if signer_row is None or not _validators.is_enrolled(conn, v["issuer_keyid"]):
        return False
    record = {
        "verdict_id": v["verdict_id"],
        "cluster_id": v["cluster_id"],
        "member_claim_id": v["member_claim_id"],
        "other_claim_id": v["other_claim_id"],
        "method": v["method"],
    }
    try:
        record["confidence"] = json.loads(v["confidence_json"] or "{}")
        pem = base64.standard_b64decode(signer_row["pubkey_pem"])
        _signing.public_key_from_pem(pem).verify(
            v["signature"], _replication_verdict_pae(record),
        )
    except Exception:
        return False
    return _issuer_was_entitled(
        conn, v["issuer_keyid"],
        ((v["member_claim_id"], "member_claim_id"),
         (v["other_claim_id"], "other_claim_id")),
        verdict_kind="replication",
    )


def _issuer_was_entitled(
    conn: sqlite3.Connection,
    issuer_keyid: str,
    claims: "tuple[tuple[str, str], ...]",
    *,
    verdict_kind: str,
    refuse_llm_issuer: bool = False,
) -> bool:
    """Whether *issuer_keyid* was entitled to issue this verdict.

    A signature proves who signed. Entitlement is the separate question of
    whether that signer was allowed to, and the recording path and
    :mod:`mareforma.db.restore` both ask it: an issuer may not verdict a claim
    whose envelope it signed any role on, and a contradiction, which invalidates
    the older claim through the insert trigger, may not come from an llm-typed
    validator. A read that verified the signature and skipped these served a
    level the same graph refuses to restore, which is one file disagreeing with
    itself about whether a claim is corroborated.

    False rather than raising. Every caller is a read, and a read degrades
    rather than crashes; the write path keeps the exceptions, where refusing is
    the whole point.
    """
    try:
        if refuse_llm_issuer:
            _refuse_llm_contradiction_issuer(conn, issuer_keyid)
        for claim_id, relation in claims:
            if claim_id is None:
                continue
            _refuse_self_verdict(
                conn, issuer_keyid, claim_id,
                relation=relation, verdict_kind=verdict_kind,
            )
    except Exception:
        return False
    return True



def _check_verdict_chain_link(
    conn: sqlite3.Connection,
    cache: dict,
    link: sqlite3.Row,
    name: str,
    expected_seq: int,
    expected_prev: str,
    problems: "list[str]",
) -> None:
    """Append every way one link fails. See :func:`verify_verdict_chain`.

    Split out so the caller can catch a database error per link. Raises
    :class:`sqlite3.Error` when a table it reads has been taken away, which the
    caller turns into a reported problem.
    """
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    if link["seq"] != expected_seq:
        problems.append(
            f"{name} is out of sequence, expected seq {expected_seq}: "
            "links are numbered without gaps, so a jump is a link that was "
            "removed"
        )
    if link["prev_tip"] != expected_prev:
        problems.append(
            f"{name} names a previous tip no surviving link produced; the "
            "chain is broken here and mending it needs this link's issuer "
            "key"
        )
    record = {
        "seq": link["seq"],
        "prev_tip": link["prev_tip"],
        "verdict_kind": link["verdict_kind"],
        "verdict_id": link["verdict_id"],
        "verdict_digest": link["verdict_digest"],
        "issuer_keyid": link["issuer_keyid"],
    }
    if _verdict_chain_tip(record) != link["tip"]:
        problems.append(
            f"{name} stores a tip its own contents do not produce, so the "
            "row was edited after it was written"
        )
    signer_row = _cached_validator(conn, cache, link["issuer_keyid"])
    if signer_row is None or not _validators.is_enrolled(
        conn, link["issuer_keyid"],
    ):
        problems.append(
            f"{name} names an issuer that is not an enrolled validator"
        )
    else:
        try:
            pem = base64.standard_b64decode(signer_row["pubkey_pem"])
            _signing.public_key_from_pem(pem).verify(
                link["signature"], _verdict_chain_link_pae(record),
            )
        except Exception:
            problems.append(
                f"{name} does not verify against its issuer's key"
            )
    table = (
        "contradiction_verdicts"
        if link["verdict_kind"] == "contradiction"
        else "replication_verdicts"
    )
    row = conn.execute(
        f"SELECT signature, issuer_keyid FROM {table} WHERE verdict_id = ?",
        (link["verdict_id"],),
    ).fetchone()
    if row is None:
        problems.append(
            f"{name} covers a verdict that is no longer in the graph"
        )
    else:
        if hashlib.sha256(row["signature"]).hexdigest() != link["verdict_digest"]:
            problems.append(
                f"{name} covers a verdict whose signature is not the one "
                "the link was made over"
            )
        # The link must be signed by the key that issued the verdict it covers,
        # not by whoever happens to be enrolled. Otherwise an enrolled peer
        # deletes a verdict, re-signs the following link over the gap, and the
        # chain recomputes and reads clean: a valid signature over a set the
        # signer had just emptied.
        #
        # Asked of the verdict's own signature, not of its issuer_keyid column.
        # Both columns are outside their signed payloads, so comparing them was
        # comparing two things an attacker sets together: put your own keyid on
        # the verdict and on the link, sign both, and the two agreed. Verifying
        # the verdict under the key the link names is a question only that key's
        # holder can answer.
        #
        # What it does NOT close, and the docstring says so: the verdict payload
        # carries no issuer, so an enrolled peer can re-sign the verdict itself
        # under their own key and satisfy this. Binding the issuer inside the
        # verdict's signed bytes is what closes that, and it changes bytes a
        # released reader rebuilds.
        if row["issuer_keyid"] != link["issuer_keyid"]:
            problems.append(
                f"{name} is signed by a key that did not issue the verdict "
                "it covers, so the link was rewritten by somebody else"
            )
        else:
            verifies = (
                _contradiction_verdict_verifies
                if link["verdict_kind"] == "contradiction"
                else _verdict_verifies
            )
            verdict_row = conn.execute(
                f"SELECT * FROM {table} WHERE verdict_id = ?",
                (link["verdict_id"],),
            ).fetchone()
            if not verifies(conn, cache, verdict_row):
                problems.append(
                    f"{name} covers a verdict that does not verify under the "
                    "key the link names as its issuer"
                )


def _require_enrolled_issuer(
    conn: sqlite3.Connection, issuer_keyid: str,
) -> None:
    """Refuse the verdict if issuer_keyid is not an enrolled validator.

    Walks the enrollment chain back to a self-signed root via
    ``validators.is_enrolled``: same gate the seed-claim path and
    ``graph.validate()`` use. A row that exists in the validators
    table but whose enrollment_envelope does not verify against its
    parent (e.g. a tampered DB or a partial restore) is rejected.
    Without the chain walk, the verdict path would be strictly more
    permissive than every other trust-bearing path.
    """
    from mareforma import validators as _validators
    if not _validators.is_enrolled(conn, issuer_keyid):
        raise VerdictIssuerError(
            f"Verdict-issuer keyid {issuer_keyid!r} is not enrolled "
            "(or its enrollment chain does not verify). Issuers must "
            "be in the validators table with a verifiable chain, "
            "call graph.enroll_validator() under a verified parent."
        )


def _require_claim_exists(
    conn: sqlite3.Connection, claim_id: str, role: str,
) -> None:
    row = conn.execute(
        "SELECT 1 FROM claims WHERE claim_id = ?", (claim_id,),
    ).fetchone()
    if row is None:
        raise VerdictIssuerError(
            f"Verdict references missing claim_id {claim_id!r} ({role})."
        )


def record_replication_verdict(
    conn: sqlite3.Connection,
    root: Path,
    *,
    verdict_id: str,
    cluster_id: str,
    member_claim_id: str,
    other_claim_id: str | None,
    method: str,
    confidence: dict[str, Any] | None,
    signer: "object",
) -> None:
    """Insert a signed replication verdict written by an enrolled validator.

    *signer* is an Ed25519 private key (the verdict-issuer's key).
    The issuer_keyid (sha256-hex of the signer's public key) must be
    present in the ``validators`` table; otherwise the call raises
    :class:`VerdictIssuerError`.

    The DSSE-PAE signature covers the canonical JSON of
    ``(verdict_id, cluster_id, member_claim_id, other_claim_id,
    method, confidence)``. Restore re-derives this binding to catch
    TOML tampering of verdict rows.

    The OSS core doesn't fire replication predicates itself:
    third-party verdict-issuers call this method after running their
    predicate logic. Mareforma just accepts the signed verdict and
    records the verdict.
    """
    from mareforma import signing as _signing

    if method not in _VALID_REPLICATION_METHODS:
        raise VerdictIssuerError(
            f"Unknown verdict method {method!r}. "
            f"Use one of: {', '.join(_VALID_REPLICATION_METHODS)}"
        )
    issuer_keyid = _signing.public_key_id(signer.public_key())
    _require_enrolled_issuer(conn, issuer_keyid)
    _require_claim_exists(conn, member_claim_id, "member_claim_id")
    if other_claim_id is not None:
        _require_claim_exists(conn, other_claim_id, "other_claim_id")
    # Defense-in-depth: a verdict issuer cannot be a role-actor on
    # either claim under verdict. Walks ALL signatures on each
    # claim's signature_bundle so a planner / executor / reviewer /
    # validator on a claim-with-roles:v1 envelope cannot also issue
    # the verdict.
    _refuse_self_verdict(
        conn, issuer_keyid, member_claim_id,
        relation="member_claim_id", verdict_kind="replication",
    )
    if other_claim_id is not None:
        _refuse_self_verdict(
            conn, issuer_keyid, other_claim_id,
            relation="other_claim_id", verdict_kind="replication",
        )

    confidence_dict = confidence or {}
    # canonicalize() (NFC + sorted keys + no whitespace + allow_nan=False)
    # for stored confidence_json so restore round-trips byte-equally
    # AND callers can't sneak a NaN/Inf into a signed payload.
    from .._canonical import canonicalize as _canonicalize
    confidence_json = _canonicalize(confidence_dict).decode("utf-8")
    record = {
        "verdict_id": verdict_id,
        "cluster_id": cluster_id,
        "member_claim_id": member_claim_id,
        "other_claim_id": other_claim_id,
        "method": method,
        "confidence": confidence_dict,
    }
    signature = signer.sign(_replication_verdict_pae(record))
    created_at = _now()
    # Verdict INSERT and the guarded UPDATE run in one BEGIN IMMEDIATE
    # transaction so a concurrent contradiction verdict cannot land between
    # the two commits and leave the claim reading as corroborated and
    # invalidated at once.
    members = [member_claim_id]
    if other_claim_id is not None:
        members.append(other_claim_id)
    placeholders = ",".join("?" * len(members))
    _own_txn = not conn.in_transaction
    try:
        if _own_txn:
            conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO replication_verdicts(
                verdict_id, cluster_id, member_claim_id, other_claim_id,
                method, confidence_json, issuer_keyid, signature, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict_id, cluster_id, member_claim_id, other_claim_id,
                method, confidence_json, issuer_keyid, signature, created_at,
            ),
        )
        # Inside the same transaction as the verdict: see
        # _append_verdict_chain_link on why the two are one write.
        _append_verdict_chain_link(
            conn, verdict_kind="replication", verdict_id=verdict_id,
            signature=signature, signer=signer, issuer_keyid=issuer_keyid,
            created_at=created_at,
        )
        if _own_txn:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        if _own_txn:
            conn.rollback()
        # The INSERT itself failing is a verdict-issuer error; a
        # promotion-trigger refusal would surface here too but at this
        # point everything either committed atomically or rolled back.
        raise VerdictIssuerError(
            f"Replication verdict {verdict_id!r} INSERT refused: {exc}"
        ) from exc
    except BaseException:
        # See the sibling handler in record_contradiction_verdict. Same two
        # writes, same open transaction, same silent loss of whatever is
        # written next on this connection.
        if _own_txn:
            conn.rollback()
        raise

    _backup_claims_toml(conn, root)


def record_contradiction_verdict(
    conn: sqlite3.Connection,
    root: Path,
    *,
    verdict_id: str,
    member_claim_id: str,
    other_claim_id: str,
    confidence: dict[str, Any] | None,
    signer: "object",
) -> None:
    """Insert a signed contradiction verdict from an enrolled validator.

    Sets ``claims.t_invalid`` on the older of the two referenced
    claims via the ``contradiction_invalidates_older`` AFTER INSERT
    trigger. ``include_invalidated=False`` queries (the default) then
    exclude the invalidated claim from results.

    Same enrollment / claim-existence / signature-binding contract as
    :func:`record_replication_verdict`.
    """
    from mareforma import signing as _signing

    if member_claim_id == other_claim_id:
        # Self-contradiction is meaningless and would let a single
        # validator invalidate any claim unilaterally. The table CHECK
        # also blocks it, but raising here gives a clean Python error.
        raise VerdictIssuerError(
            f"Contradiction verdict {verdict_id!r} references the same "
            f"claim_id on both sides ({member_claim_id!r}), self-"
            "contradiction is not a valid verdict."
        )
    issuer_keyid = _signing.public_key_id(signer.public_key())
    _require_enrolled_issuer(conn, issuer_keyid)
    # Symmetric to validate_claim's LLM-validator gate: an LLM-typed
    # validator cannot issue a contradiction, because a contradiction
    # invalidates the older claim and drops it from default query()
    # results. Sign-off-requires-human and invalidation-requires-human must
    # move together; otherwise an enrolled LLM key can mark down any
    # validated claim with a signed contradiction.
    _refuse_llm_contradiction_issuer(conn, issuer_keyid)
    _require_claim_exists(conn, member_claim_id, "member_claim_id")
    _require_claim_exists(conn, other_claim_id, "other_claim_id")
    # Defense-in-depth: the contradiction issuer cannot be a role-
    # actor on either claim.
    _refuse_self_verdict(
        conn, issuer_keyid, member_claim_id,
        relation="member_claim_id", verdict_kind="contradiction",
    )
    _refuse_self_verdict(
        conn, issuer_keyid, other_claim_id,
        relation="other_claim_id", verdict_kind="contradiction",
    )

    confidence_dict = confidence or {}
    # canonicalize() (NFC + sorted keys + no whitespace + allow_nan=False)
    # for stored confidence_json so restore round-trips byte-equally
    # AND callers can't sneak a NaN/Inf into a signed payload.
    from .._canonical import canonicalize as _canonicalize
    confidence_json = _canonicalize(confidence_dict).decode("utf-8")
    record = {
        "verdict_id": verdict_id,
        "member_claim_id": member_claim_id,
        "other_claim_id": other_claim_id,
        "confidence": confidence_dict,
    }
    pae = _contradiction_verdict_pae(record)
    signature = signer.sign(pae)
    created_at = _now()
    # One transaction, like its sibling. This used to be a bare INSERT on the
    # grounds that the AFTER-INSERT trigger fires inside the same auto-statement
    # transaction and no second write follows. A second write follows now: the
    # chain link has to land with the verdict or not at all, and reading the
    # chain head under BEGIN IMMEDIATE is what stops two writers building two
    # links from the same tip.
    _own_txn = not conn.in_transaction
    try:
        if _own_txn:
            conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO contradiction_verdicts(
                verdict_id, member_claim_id, other_claim_id,
                confidence_json, issuer_keyid, signature, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict_id, member_claim_id, other_claim_id,
                confidence_json, issuer_keyid, signature, created_at,
            ),
        )
        _append_verdict_chain_link(
            conn, verdict_kind="contradiction", verdict_id=verdict_id,
            signature=signature, signer=signer, issuer_keyid=issuer_keyid,
            created_at=created_at,
        )
        if _own_txn:
            conn.commit()
    except sqlite3.IntegrityError as exc:
        if _own_txn:
            conn.rollback()
        raise VerdictIssuerError(
            f"Contradiction verdict {verdict_id!r} INSERT refused: {exc}"
        ) from exc
    except BaseException:
        # Every other way this block can fail, and the transaction has to close
        # on all of them. There are two writes in here now, and the second one
        # can fail for reasons the first never could: a schema the graph no
        # longer has, or a signer that goes away between the two. Left open, the
        # transaction makes the NEXT write on this connection a silent no-op:
        # add_claim sees in_transaction and does not commit, returns a claim id,
        # raises nothing, and the row is discarded when the connection closes.
        # Measured both ways before this handler existed.
        if _own_txn:
            conn.rollback()
        raise

    _backup_claims_toml(conn, root)


def list_replication_verdicts(
    conn: sqlite3.Connection,
    *,
    member_claim_id: str | None = None,
    cluster_id: str | None = None,
    include_invalidated: bool = False,
) -> list[dict]:
    """List signed replication verdicts, optionally filtered.

    By default, verdicts whose member or other claim has been
    invalidated (``claims.t_invalid IS NOT NULL``) are excluded: same
    surface as :func:`query_claims`. Pass ``include_invalidated=True``
    for audit-mode listings.
    """
    conditions: list[str] = []
    params: list[Any] = []
    if member_claim_id is not None:
        conditions.append("(v.member_claim_id = ? OR v.other_claim_id = ?)")
        params.extend([member_claim_id, member_claim_id])
    if cluster_id is not None:
        conditions.append("v.cluster_id = ?")
        params.append(cluster_id)
    if not include_invalidated:
        conditions.append(
            "NOT EXISTS ("
            "SELECT 1 FROM claims c "
            "WHERE (c.claim_id = v.member_claim_id OR c.claim_id = v.other_claim_id) "
            "AND c.t_invalid IS NOT NULL"
            ")"
        )
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    rows = conn.execute(
        f"SELECT v.verdict_id, v.cluster_id, v.member_claim_id, "
        f"v.other_claim_id, v.method, v.confidence_json, v.issuer_keyid, "
        f"v.signature, v.created_at "
        f"FROM replication_verdicts v {where} "
        f"ORDER BY v.created_at ASC, v.verdict_id ASC",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def list_contradiction_verdicts(
    conn: sqlite3.Connection,
    *,
    claim_id: str | None = None,
    include_invalidated: bool = False,
) -> list[dict]:
    """List signed contradiction verdicts, optionally filtered.

    By default, contradiction verdicts whose claims have been
    invalidated are excluded. Pass ``include_invalidated=True`` for
    audit-mode listings (the typical use, since a contradiction verdict
    is the EVIDENCE for invalidation, so callers inspecting "why was
    this invalidated" need audit mode).
    """
    conditions: list[str] = []
    params: list[Any] = []
    if claim_id is not None:
        conditions.append("(v.member_claim_id = ? OR v.other_claim_id = ?)")
        params.extend([claim_id, claim_id])
    if not include_invalidated:
        conditions.append(
            "NOT EXISTS ("
            "SELECT 1 FROM claims c "
            "WHERE (c.claim_id = v.member_claim_id OR c.claim_id = v.other_claim_id) "
            "AND c.t_invalid IS NOT NULL"
            ")"
        )
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    rows = conn.execute(
        f"SELECT v.verdict_id, v.member_claim_id, v.other_claim_id, "
        f"v.confidence_json, v.issuer_keyid, v.signature, v.created_at "
        f"FROM contradiction_verdicts v {where} "
        f"ORDER BY v.created_at ASC, v.verdict_id ASC",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


# Refutation taxonomy, the four states a claim can be in with respect
# to active refutations against it. Surfaced to callers via
# :func:`refutation_status` and as a filter argument to query_claims.
#   clean       , no signed contradiction, status='open', not retracted
#   contradicted, t_invalid IS NOT NULL (a signed contradiction
#                  verdict from an enrolled validator marked the older
#                  claim invalid)
#   contested   , status='contested' (an editorial-level flag set by
#                  update_claim; non-cryptographic, weaker than a
#                  contradiction verdict but visible to consumers)
#   retracted   , status='retracted' (the asserter withdrew the claim
#                  themselves; terminal state in the status state
#                  machine)
REFUTATION_STATES: tuple[str, ...] = (
    "clean", "contradicted", "contested", "retracted",
)
# Filter values that query_claims accepts on refutation_filter=. ``None``
# preserves the legacy behaviour gated by include_invalidated.
VALID_REFUTATION_FILTERS: tuple[str, ...] = (
    "clean", "contradicted", "contested", "retracted", "any",
)


# Signals that mean the column and the signed evidence disagree. Grouped here
# rather than tested one by one at each call site, because every reader of a
# replayed status has to make the same split: a verdict that was checked and a
# verdict that was asserted are different claims about the world, and a caller
# that treats them alike is back where the column left it.
REPLAY_TAMPER_SIGNALS: tuple[str, ...] = (
    "unbacked-invalidation",
    "suppressed-verdict",
    "unverifiable-verdict",
    "replay-unavailable",
)


_CONTRADICTION_VERDICT_SELECT = (
    "SELECT verdict_id, member_claim_id, other_claim_id, confidence_json, "
    "issuer_keyid, signature, created_at FROM contradiction_verdicts"
)


def _contradiction_verdicts_naming(
    conn: sqlite3.Connection, claim_id: str,
) -> "list[sqlite3.Row]":
    """Every contradiction verdict that names *claim_id* on either side."""
    return conn.execute(
        _CONTRADICTION_VERDICT_SELECT
        + " WHERE member_claim_id = ? OR other_claim_id = ?",
        (claim_id, claim_id),
    ).fetchall()


def _gather_contradictions_by_claim(
    conn: sqlite3.Connection,
) -> "dict[str, list[sqlite3.Row]]":
    """Every contradiction verdict grouped by the claim ids it names.

    One scan, no signature work, the same shape
    The same shape holds for the replication table and for
    the same reason. A bulk read that replays per row spends a statement per row
    to ask a question one pass answers, and it spends it hardest on the ordinary
    graph where the table is empty and every one of those statements returns
    nothing. Measured on a 400-claim graph with no verdicts, grouping saves
    about 0.1 ms on a 20-row page and grows with the page.

    Modest, and worth stating at its real size, because the obvious reading of
    the numbers around it is wrong: a clean-filtered page costs roughly 1.3 ms
    more than an unfiltered one on that graph, all of which is the SQL the
    filter already emitted (it plans as SCAN claims) and none of which is the
    replay.

    The trade is real and worth naming too: this reads the whole verdict table,
    so a graph that argues with itself far more than it is read pays a scan
    where indexed lookups would have been cheaper. Verdicts are rare next to
    claims, which is what makes the scan the right default rather than a safe
    one.
    """
    by_claim: "dict[str, list[sqlite3.Row]]" = {}
    for v in conn.execute(_CONTRADICTION_VERDICT_SELECT):
        for cid in (v["member_claim_id"], v["other_claim_id"]):
            if cid is not None:
                by_claim.setdefault(cid, []).append(v)
    return by_claim


def _contradiction_verdict_verifies(
    conn: sqlite3.Connection, cache: dict, v: "sqlite3.Row",
) -> bool:
    """True iff *v*'s issuer is enrolled and its signature checks out.

    The contradiction sibling of :func:`_verdict_verifies`, over the
    contradiction PAE. Same reasoning: the recording path checks the issuer, and
    that precondition does not travel to a read, where the table is whatever a
    process with SQL access wrote. Never raises; an unparseable verdict is not
    evidence and a read must degrade rather than crash.
    """
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    signer_row = _cached_validator(conn, cache, v["issuer_keyid"])
    if signer_row is None or not _validators.is_enrolled(conn, v["issuer_keyid"]):
        return False
    record = {
        "verdict_id": v["verdict_id"],
        "member_claim_id": v["member_claim_id"],
        "other_claim_id": v["other_claim_id"],
    }
    try:
        record["confidence"] = json.loads(v["confidence_json"] or "{}")
        pem = base64.standard_b64decode(signer_row["pubkey_pem"])
        _signing.public_key_from_pem(pem).verify(
            v["signature"], _contradiction_verdict_pae(record),
        )
    except Exception:
        return False
    return _issuer_was_entitled(
        conn, v["issuer_keyid"],
        ((v["member_claim_id"], "member_claim_id"),
         (v["other_claim_id"], "other_claim_id")),
        verdict_kind="contradiction", refuse_llm_issuer=True,
    )


def _verdict_invalidates(
    conn: sqlite3.Connection, v: "sqlite3.Row",
) -> "str | None":
    """Which claim ``contradiction_invalidates_older`` picks for verdict *v*.

    The rule is the trigger's, restated in Python so a read can ask what the
    write path would have done: the older claim by ``created_at``, tie-broken on
    the lexicographically smaller id so the verdict's argument order does not
    decide it. Restating is a duplication and it is the only way to check the
    column against anything; the pairing is pinned by test.

    ``None`` when either claim is gone, because then the question has no answer
    rather than a negative one.
    """
    rows = {
        r["claim_id"]: r["created_at"] for r in conn.execute(
            "SELECT claim_id, created_at FROM claims WHERE claim_id IN (?, ?)",
            (v["member_claim_id"], v["other_claim_id"]),
        )
    }
    a, b = v["member_claim_id"], v["other_claim_id"]
    if a not in rows or b not in rows:
        return None
    if rows[a] != rows[b]:
        return a if rows[a] < rows[b] else b
    return min(a, b)


def replay_contradictions(
    conn: sqlite3.Connection, claim_id: str, *,
    verdicts: "list | None" = None, cache: "dict | None" = None,
) -> dict:
    """What the signed contradiction verdicts say about *claim_id*.

    The read path cannot take ``t_invalid`` at its word. No trigger guards that
    column, so one UPDATE either fabricates a contradiction with no verdict
    behind it or erases a real one from every read surface, and a presenter over
    the row alone reports the edit as though it were the evidence.

    So the verdicts naming the claim are fetched and held against their issuers:
    enrolled validator, signature verifying over the DSSE PAE rebuilt from the
    stored columns, the same bar the recording path applies. A verdict that
    fails is not weaker evidence, it is a row somebody planted, and it is
    reported rather than skipped.

    Returns ``backed`` (a verifying verdict makes this claim the invalidated
    one), ``unverifiable`` (verdict ids naming it that did not check out), and
    ``checked`` (how many were examined), so a caller can tell "no verdict" from
    "a verdict nobody can authenticate".
    """
    cache = {} if cache is None else cache
    backed = False
    unverifiable: list[str] = []
    if verdicts is None:
        verdicts = _contradiction_verdicts_naming(conn, claim_id)
    for v in verdicts:
        if not _contradiction_verdict_verifies(conn, cache, v):
            unverifiable.append(v["verdict_id"])
            continue
        if _verdict_invalidates(conn, v) == claim_id:
            backed = True
    return {
        "backed": backed,
        "unverifiable": tuple(sorted(unverifiable)),
        "checked": len(verdicts),
    }


def _replayed_refutation(
    conn: sqlite3.Connection, row: dict, flagged: bool, *,
    verdicts: "list | None" = None, cache: "dict | None" = None,
) -> "dict | None":
    """The contradiction answer with the verdicts replayed, or ``None``.

    ``None`` means the replay had nothing to say and the caller should fall
    through to the status flags: no verdict names this claim and its column is
    clear, which is the ordinary case and must stay cheap to report.

    Never raises, and that has to hold for more than a database error. The
    replay compares two ``created_at`` values, and that column has TEXT
    affinity, so a value written around the write path stays whatever type it
    was put in and the comparison raises a ``TypeError`` rather than a
    ``sqlite3.Error``. A read that cannot reach the verdict tables, or cannot
    make sense of what it found there, degrades to the column rather than taking
    a claim down with it, and says which it did.
    """
    try:
        replay = replay_contradictions(
            conn, row["claim_id"], verdicts=verdicts, cache=cache,
        )
    except Exception as exc:
        # Reported, not None. None here means "the replay had nothing to say",
        # and both callers then fall through to ``t_invalid``, the column with
        # no trigger that this replay exists to distrust. So a graph where the
        # replay cannot run served a suppressed contradiction as a clean row,
        # which is worse than the crash this handler replaced: the crash was at
        # least visible. A replay that could not run is a fact about the graph,
        # not the absence of one, and it belongs with the other three signals
        # for the same reason they are grouped: every reader has to make the
        # same split, and a caller asking for clean claims must not be handed
        # this one.
        return {
            "state": "contradicted" if flagged else "clean",
            "reason": (
                "the contradiction verdicts behind this claim could not be "
                f"replayed ({type(exc).__name__}), so nothing signed stands "
                "behind the invalidation column either way"
            ),
            "signal": "replay-unavailable",
        }
    if replay["unverifiable"]:
        return {
            "state": "contradicted",
            "reason": (
                f"{len(replay['unverifiable'])} contradiction verdict(s) name "
                "this claim and do not verify against an enrolled issuer: "
                + ", ".join(replay["unverifiable"])
                + ". A verdict that fails its own signature is a planted row, "
                "not weak evidence"
            ),
            "signal": "unverifiable-verdict",
        }
    if replay["backed"] and not flagged:
        return {
            "state": "contradicted",
            "reason": (
                "a contradiction verdict that verifies invalidates this claim, "
                "and its invalidation timestamp is clear; the column was "
                "cleared under signed evidence that is still here"
            ),
            "signal": "suppressed-verdict",
        }
    if flagged and not replay["backed"]:
        return {
            "state": "contradicted",
            "reason": (
                "this claim's invalidation timestamp is set "
                f"(t_invalid={row['t_invalid']}) and no contradiction verdict "
                f"that verifies invalidates it ({replay['checked']} named it); "
                "the column was written by something that left no evidence"
            ),
            "signal": "unbacked-invalidation",
        }
    if flagged and replay["backed"]:
        return {
            "state": "contradicted",
            "reason": (
                "a contradiction verdict that verifies against an enrolled "
                "issuer invalidates this claim, and the invalidation timestamp "
                f"agrees (t_invalid={row['t_invalid']})"
            ),
            "signal": "signed-verdict",
        }
    return None


def refutation_status(row: dict, conn: "sqlite3.Connection | None" = None) -> dict:
    """Classify a claim row's refutation state.

    Returns a dict with three fields:

      * ``state``: one of :data:`REFUTATION_STATES`
      * ``reason``: short human-readable explanation
      * ``signal``: how the state was established, see below

    Pass *conn* and the contradiction verdicts are replayed
    (:func:`replay_contradictions`) instead of the ``t_invalid`` column being
    taken at its word. Without it the answer is the column, said plainly, which
    is what every caller got before and is still the honest report when there is
    no graph to check against.

    The signals, and what each is worth:

      * ``signed-verdict``: the column is set and a verdict that verifies backs
        it. The only reading that survives a hostile writer.
      * ``invalidation-recorded``: the column is set and nothing replayed it.
      * ``editorial``: a status flag, which is an assertion by the asserter.
      * ``none``: nothing to report.
      * ``unbacked-invalidation``: the column is set and no verifying verdict
        names this claim. Somebody wrote the column.
      * ``suppressed-verdict``: a verifying verdict invalidates this claim and
        the column is clear. Somebody erased it from every read surface.
      * ``unverifiable-verdict``: verdicts naming this claim exist and do not
        check out. Planted rows, not weak evidence.
      * ``replay-unavailable``: the replay itself could not run, so nothing
        signed stands behind the column either way.

    Those last four are :data:`REPLAY_TAMPER_SIGNALS`. For the first three the
    state stays ``contradicted``, deliberately: an unbacked column is not
    grounds to hand a suppressed claim back as clean, and a suppressed verdict
    is not grounds to keep calling it clean either. Refusing to un-flag in both
    directions is the only choice that does not do an attacker's work in one of
    them. ``replay-unavailable`` is the exception and follows the column,
    because a replay that did not run is not evidence that one would have
    found something.

    Without *conn* the presenter is a pure function over the row's queryable
    columns and does NOT walk verdict tables (callers wanting the underlying
    verdicts use :meth:`EpistemicGraph.contradiction_verdicts`).

    Raises :class:`ValueError` when *row* lacks the required
    ``status`` field: a hand-crafted partial dict would otherwise
    fall through to a falsely-confident ``"clean"`` verdict.
    """
    if not isinstance(row, dict):
        raise ValueError(
            f"refutation_status: row must be a dict, got {type(row).__name__}"
        )
    if "status" not in row:
        raise ValueError(
            "refutation_status: row missing 'status' field; pass a row "
            "fetched via list_claims / get_claim, not a partial dict."
        )
    if conn is None:
        from .._deprecation import warn_refutation_status_without_conn

        warn_refutation_status_without_conn()
        return refutation_from_column(row)
    if row.get("claim_id"):
        replayed = _replayed_refutation(
            conn, row, row.get("t_invalid") is not None,
        )
        if replayed is not None:
            return replayed
    return refutation_from_column(row)


def refutation_from_column(row: dict) -> dict:
    """The refutation state as the row's own columns record it, no replay.

    The honest floor: what a reader can say with the row in front of them and
    nothing else. ``refutation_status`` falls through to it when the replay has
    nothing to add, and :func:`mareforma.trust_map._assemble` calls it directly
    because that function is pure by contract and holds no graph, which is a
    legitimate absence rather than a caller who should have passed one.

    Carries no deprecation warning for that reason. The warning belongs on
    ``refutation_status(row)``, where a connection was available and was not
    handed over.
    """
    flagged = row.get("t_invalid") is not None
    if flagged:
        # States what was read, not what was proved. With no connection this is
        # a pure function over one row: it sees `t_invalid` and nothing else.
        # The signed evidence sits untouched in contradiction_verdicts, and no
        # trigger guards this column, so one UPDATE either fabricates a
        # contradiction with zero verdicts present or erases a real one from
        # every read surface. Claiming a signed verdict had been checked is not
        # something this branch can know, so it says what it saw.
        return {
            "state": "contradicted",
            "reason": (
                "this claim's invalidation timestamp is set "
                f"(t_invalid={row['t_invalid']}); the contradiction verdicts "
                "behind it were not replayed on this call"
            ),
            "signal": "invalidation-recorded",
        }
    status = row.get("status")
    if status == "retracted":
        return {
            "state": "retracted",
            "reason": "the asserter retracted this claim",
            "signal": "editorial",
        }
    if status == "contested":
        return {
            "state": "contested",
            "reason": "this claim was editorially flagged as contested",
            "signal": "editorial",
        }
    return {
        "state": "clean",
        "reason": "no refutation signal on this claim",
        "signal": "none",
    }


def _read_scan_ceiling(limit: int) -> int:
    """Max rows a read surface materialises before returning the survivors it
    has. Bounds the adversarial worst case: a flood of rows that fail
    verify-on-read (mass tamper, or a flood of unsigned rows) must not turn
    a cheap insert into a whole-table read amplifier. Generous enough that
    legitimate signature-heavy projects are unaffected."""
    return max(limit * 50, 5000)


def _require_non_negative_limit(limit: int, surface: str) -> None:
    """Refuse a negative limit on a read surface.

    Zero is a legitimate boundary (a pager or budget loop that has drained) and
    returns no rows. A negative limit has no reading: it is a caller's
    arithmetic mistake, and quietly returning nothing would hide it."""
    if limit < 0:
        raise ValueError(
            f"{surface} limit must be zero or greater, got {limit}."
        )


def _count_unbacked_invalidations(
    conn: sqlite3.Connection,
    from_sql: str,
    where: str,
    params: list,
    *,
    ceiling: int,
    prefix: str = "",
) -> "tuple[int, bool]":
    """How many rows this read hid on an invalidation no signed verdict backs.

    The filter runs in SQL, so the drained rows never enter the scan and
    nothing downstream can see what was dropped. The filter is
    ``t_invalid IS NULL``, and that column carries no trigger, so one UPDATE
    hides a claim from every listing while the per-claim surfaces go on
    reporting the disagreement to nobody who is looking.

    The negated condition alone is not the answer, because a claim invalidated
    by a verdict that verifies is honestly hidden. So each hidden row is
    replayed against the signed verdicts, and only the ones no verdict backs are
    counted. Bounded by the same scan ceiling for the same reason: a disclosure
    must not cost more than the read it describes, and a saturated count reads
    as "at least this many".

    Cheap on an ordinary graph, which invalidates nothing: the bounded id query
    comes back empty and no replay runs.
    """
    # The read's own WHERE with the invalidation condition NEGATED, which is
    # how the sibling counter reaches its drained rows too. The literal is
    # already treated as a token where the filter is assembled, and the assert
    # says so out loud rather than silently counting nothing if it ever moves.
    negated = where.replace("t_invalid IS NULL", "t_invalid IS NOT NULL")
    if negated == where:
        return 0, False
    hidden = conn.execute(
        f"SELECT {prefix}claim_id AS claim_id FROM {from_sql} {negated} "
        f"LIMIT ?",
        (*params, ceiling + 1),
    ).fetchall()
    if not hidden:
        return 0, False
    saturated = len(hidden) > ceiling
    verdicts = _gather_contradictions_by_claim(conn)
    cache: dict = {}
    unbacked = 0
    for row in hidden[:ceiling]:
        claim_id = row[0]
        try:
            replay = replay_contradictions(
                conn, claim_id, verdicts=verdicts.get(claim_id, []),
                cache=cache,
            )
        except Exception:
            continue
        if not replay["backed"]:
            unbacked += 1
    return unbacked, saturated


def _disclose_invalidation_gaps(
    conn: sqlite3.Connection,
    from_sql: str,
    where: str,
    params: list,
    *,
    ceiling: int,
    prefix: str = "",
    contested: int = 0,
    on_contested: "Callable[[int], None] | None" = None,
    include_invalidated: bool = True,
) -> None:
    """Report where ``t_invalid`` and the signed verdicts disagree about a read.

    Two facts, one on each side of the column. A contested row was SERVED with
    ``t_invalid`` empty while a signed verdict says it is invalid. A hidden row
    was WITHHELD on a ``t_invalid`` no signed verdict backs. Neither shows in
    the list the caller gets: the first arrives looking clean, the second does
    not arrive at all.
    """
    # Counted apart, because a served row filed under an exclusion is a false
    # sentence in the health record and inflates a count that answers a
    # different question. The contested count goes first: it is a property of
    # the rows served, so it holds whatever the page length.
    if contested and on_contested is not None:
        on_contested(contested)
    if not include_invalidated:
        unbacked, unbacked_saturated = _count_unbacked_invalidations(
            conn, from_sql, where, params, ceiling=ceiling, prefix=prefix,
        )
        if unbacked:
            import logging
            logging.getLogger("mareforma").warning(
                "Read hid %s claim(s)%s behind an invalidation timestamp that "
                "no signed verdict supports; that column carries no trigger, "
                "so one UPDATE hides a claim from every listing. Call "
                "`mareforma verify` on the project, or pass "
                "include_invalidated=True to see them.",
                unbacked, " (at least)" if unbacked_saturated else "",
            )


def _scan_ceiling_error(surface: str, ceiling: int, found: int, limit: int):
    """The ScanCeilingReached a read surface raises when its scan ran out."""
    return ScanCeilingReached(
        f"{surface} stopped at the {ceiling}-row scan ceiling with {found} of "
        f"{limit} claims collected; rows past the ceiling were not read, so "
        f"this result would be short without saying so. Narrow the query or "
        f"lower limit."
    )


# Returned instead of None when a row is dropped because its signature did not
# re-verify. A failed re-verification is a tamper signal, not ordinary
# filtering, and the caller counts the two apart.
_VERIFY_EXCLUDED = object()


def _read_path_row(
    conn: sqlite3.Connection,
    row,
    *,
    reputation: dict,
    enrolled_keyids: set,
    trust_domain: tuple,
    verify_cache: dict,
) -> dict | None | object:
    """Project one claims row for a read surface, or exclude it.

    Shared by :func:`query_claims` and :func:`search_claims` so the read-path
    verification cannot drift between the two surfaces. Attaches
    ``generator_enrolled`` and ``validator_reputation``; drops a row whose
    signature does not re-verify (returns :data:`_VERIFY_EXCLUDED`); and
    attaches the trust-domain disclosure.

    It no longer drops a row for being signed by a key the project never
    enrolled. That filter only ever applied below the top of the support
    ladder, and converging lifted a row out of it; with nothing to converge
    into, it would have hidden every claim in a project that enrols no
    validator. ``generator_enrolled`` still rides on every row, so a caller
    that wants the old set can ask for it and see why.
    """
    d = dict(row)
    gen_keyid = _extract_signature_bundle_keyid(d.get("signature_bundle"))
    d["generator_enrolled"] = (
        gen_keyid is not None and gen_keyid in enrolled_keyids
    )
    validator_kid = d.get("validator_keyid")
    d["validator_reputation"] = (
        reputation.get(validator_kid, 0) if validator_kid else 0
    )
    # A claim whose signer the project never enrolled used to be held back from
    # the default read, and converging lifted it into view. Nothing lifts
    # anything now, so applying that rule to every row made a project which
    # never enrols a validator read as empty, which looks like data loss and is
    # not what the rest of this release says: trust is derived and disclosed,
    # not gated by a word. The row is served, carrying ``generator_enrolled``
    # and ``verified`` so a caller reads what backs it instead of being handed
    # a shorter list and no reason.
    if not _row_verified_on_read(conn, d, verify_cache):
        return _VERIFY_EXCLUDED
    d["single_trust_domain"], d["trust_domain_root"] = trust_domain
    return d


def _project_verified_rows(
    conn: sqlite3.Connection,
    rows: "Iterable",
    *,
    limit: int,
    on_verify_excluded: Callable[[int], None] | None = None,
    clean_only: bool = False,
) -> tuple[list[dict], int, int]:
    """Filter and project rows for a read surface, stopping at ``limit`` survivors.

    Computes the per-call reputation, enrolled set, and trust-domain disclosure
    once, then applies :func:`_read_path_row` in sorted order until ``limit``
    survivors are collected. The single ordered fetch happens in the caller, so
    the table is sorted once, not re-sorted per batch. ``rows`` may be a live
    cursor: the early break then stops fetching, so the common path pulls a
    handful of rows rather than the whole scan ceiling.

    Rows dropped by verify-on-read are counted and reported: a WARNING names
    the count, and *on_verify_excluded* (when given) receives it, so a tampered
    row registers as a signal instead of as a shorter list. Without this the
    only trace of a tamper on an enumerating surface is a row that is not
    there, indistinguishable from a claim that never existed.

    A row withheld on a ``t_invalid`` no signed verdict backs does not pass
    through here at all: that filter runs in SQL, so the row never reaches this
    loop to be counted. :func:`_disclose_invalidation_gaps` takes its count
    separately, off the read's own WHERE.

    Returns ``(survivors, scanned, contested)``. ``scanned`` is how many rows
    were pulled, which the caller compares against the scan ceiling to tell
    "that is all there is" from "the scan ran out before the survivors did".
    ``contested`` counts rows whose contradiction record the signed verdicts do
    not support, and it is counted whatever the caller asked for: a clean-only
    caller has those rows withheld, an ordinary caller is served them and has to
    be told. It is kept apart from the excluded count because a row that fails
    to re-verify and a row whose contradiction record does not hold up are
    different news.
    """
    if limit <= 0:
        # The loop appends a survivor before testing the stop condition, so it
        # would hand back one row for a limit of zero. Nothing was asked for:
        # return nothing, and skip the per-call reputation and trust-domain work.
        return [], 0, 0
    reputation = _compute_validator_reputation(conn)
    enrolled_keyids = _enrolled_validator_keyids(conn)
    trust_domain = _trust_domain_disclosure(conn)
    # Grouped once for the page, whether or not the filter will act on the
    # answer, because the count is disclosed either way: a caller who did not
    # ask for clean claims still has to be told that one of the rows it was
    # handed carries a contradiction record the signed verdicts do not support.
    # Per row this would be a statement each to ask what one pass answers, and
    # the ordinary graph has no verdicts at all, so every one of those
    # statements returns nothing. It cannot move into the SQL filter either:
    # that filter can only read t_invalid, which carries no trigger, so a real
    # contradiction erased from that column reads as clean to every statement
    # in this file.
    contradictions = _gather_contradictions_by_claim(conn)
    contested = 0
    verify_cache: dict = {}
    results: list[dict] = []
    scanned = 0
    excluded = 0
    withheld = 0
    for row in rows:
        scanned += 1
        d = _read_path_row(
            conn, row,
            reputation=reputation, enrolled_keyids=enrolled_keyids,
            trust_domain=trust_domain,
            verify_cache=verify_cache,
        )
        if d is _VERIFY_EXCLUDED:
            excluded += 1
        elif d is not None:
            # One replay per served row, doing both jobs. The disagreement is
            # counted whatever the caller asked for, and only a caller who asked
            # for clean claims has the row withheld: dropping it from an
            # unfiltered listing would be this function deciding what the caller
            # meant, and counting it nowhere is the silence this exists to end.
            replayed = _replayed_refutation(
                conn, d, d.get("t_invalid") is not None,
                verdicts=contradictions.get(d["claim_id"], []),
                cache=verify_cache,
            )
            if replayed is not None and replayed["signal"] in REPLAY_TAMPER_SIGNALS:
                contested += 1
                if clean_only:
                    # Counted apart from the verify-on-read exclusions. Both
                    # withhold a row, and they are different news: one row's
                    # signature did not re-verify, this one's contradiction
                    # record does not hold up. Folding them told the operator
                    # the wrong thing about which claim to go and look at.
                    withheld += 1
                    continue
            results.append(d)
            if len(results) >= limit:
                break
    if excluded:
        import logging
        logging.getLogger("mareforma").warning(
            "Read excluded %s claim(s) whose signature did not re-verify; "
            "call get_claim() or `mareforma verify` on the affected claim_id "
            "for the detail.",
            excluded,
        )
        if on_verify_excluded is not None:
            on_verify_excluded(excluded)
    if withheld:
        import logging
        logging.getLogger("mareforma").warning(
            "Read withheld %s claim(s) whose contradiction record the signed "
            "verdicts do not support; call `mareforma verify` on the affected "
            "claim_id for the detail.",
            withheld,
        )
    if contested - withheld:
        # The other direction, and the one that reads as a clean answer. A
        # claim whose invalidation was cleared passes the SQL filter and is
        # SERVED, so the caller is handed a row the signed verdicts say is
        # contradicted. Counting it in the health record is not telling the
        # person reading the list, and the disagreement has to reach them the
        # same way the withheld one does.
        import logging
        logging.getLogger("mareforma").warning(
            "Read served %s claim(s) whose contradiction record the signed "
            "verdicts contradict; the invalidation column carries no trigger, "
            "so one UPDATE clears a real contradiction from every listing. "
            "Call `mareforma verify` on the affected claim_id for the detail.",
            contested - withheld,
        )
    return results, scanned, contested


def query_claims(
    conn: sqlite3.Connection,
    *,
    limit: int = 10,
    text: str | None = None,
    classification: str | None = None,
    include_invalidated: bool = False,
    refutation_filter: str | None = None,
    on_verify_excluded: Callable[[int], None] | None = None,
    on_contested: Callable[[int], None] | None = None,
) -> list[dict]:
    """Return claims ordered by recency (desc).

    Parameters
    ----------
    limit:
        Maximum number of claims to return. Default 10. Zero returns no
        claims; a negative limit raises ``ValueError``.
    text:
        Optional substring filter: case-insensitive LIKE match on claim text.
    classification:
        Filter by classification: 'INFERRED' | 'ANALYTICAL' | 'DERIVED'.
    include_invalidated:
        When False (default), claims with non-NULL ``t_invalid`` are
        excluded: a contradiction_verdicts row from an enrolled
        validator has marked them invalid. Pass ``True`` for audit /
        history queries where you want to see contradicted claims too.
    on_verify_excluded:
        Optional callback receiving the number of rows this call dropped
        because their signature did not re-verify. No flag surfaces those
        rows, so this is how a caller learns a result is short because the
        graph was tampered with rather than because it is empty.

    Each returned dict carries the standard claim columns plus two
    reputation projections computed at query time:

      - ``validator_reputation`` (int): for a row carrying a validation, the
        number of claims the same validator has signed off on (≥ 1). For
        other rows, ``0``.
      - ``generator_enrolled`` (bool): True iff the key on the claim's
        ``signature_bundle`` has a row in the ``validators`` table. False
        for unsigned claims and for keys that table does not name.

        Membership, not enrolment. The table has no INSERT guard, so one
        INSERT carrying a real pubkey and a junk enrollment envelope makes a
        key a member without its chain walking back to the root. The walk is
        what ``validators.is_enrolled`` does and what verify-on-read and
        ``mareforma verify`` apply; a caller wanting that answer asks them,
        and this field is the cheap listing-side filter it was built as.
    """
    _require_non_negative_limit(limit, "query")

    conditions: list[str] = []
    params: list = []

    if text is not None:
        # SQLite treats % and _ as LIKE wildcards; a caller-supplied
        # text containing those metacharacters (or an empty string)
        # would otherwise behave as a wildcard match against every
        # row. Escape with \ + ESCAPE clause so the substring filter
        # is literal as documented.
        escaped = (
            text.replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
        )
        conditions.append("text LIKE ? ESCAPE '\\'")
        params.append(f"%{escaped}%")

    if not include_invalidated:
        conditions.append("t_invalid IS NULL")

    if classification is not None:
        if classification not in VALID_CLASSIFICATIONS:
            raise ValueError(
                f"Unknown classification '{classification}'. "
                f"Use one of: {', '.join(VALID_CLASSIFICATIONS)}"
            )
        conditions.append("classification = ?")
        params.append(classification)

    # Refutation filter is composable with include_invalidated:
    #   refutation_filter="clean"       , restrict to clean rows
    #   refutation_filter="contradicted", restrict to t_invalid IS NOT NULL
    #   refutation_filter="contested"   , restrict to status='contested'
    #   refutation_filter="retracted"   , restrict to status='retracted'
    #   refutation_filter="any"         , include every refutation state
    #                                      (implies include_invalidated=True)
    if refutation_filter is not None:
        if refutation_filter not in VALID_REFUTATION_FILTERS:
            raise ValueError(
                f"Unknown refutation_filter '{refutation_filter}'. "
                f"Use one of: {', '.join(VALID_REFUTATION_FILTERS)}"
            )
        if refutation_filter == "clean":
            # Guard against double-adding t_invalid IS NULL when
            # include_invalidated=False already pushed the same
            # predicate above. SQL is idempotent on AND-of-equals
            # today, but the conditions.remove() pattern below only
            # strips the first occurrence, if a future refactor
            # expects exactly-once semantics, the duplicate could
            # silently widen results.
            if "t_invalid IS NULL" not in conditions:
                conditions.append("t_invalid IS NULL")
            conditions.append("status = 'open'")
        elif refutation_filter == "contradicted":
            # Override include_invalidated=False so we can SELECT
            # contradicted rows even when the caller forgot to flip
            # the include flag.
            if "t_invalid IS NULL" in conditions:
                conditions.remove("t_invalid IS NULL")
            conditions.append("t_invalid IS NOT NULL")
        elif refutation_filter == "contested":
            # A row can be both contested AND contradicted; the
            # caller asking for "contested" wants every contested
            # row regardless of t_invalid, so override the default
            # invalidation gate.
            if "t_invalid IS NULL" in conditions:
                conditions.remove("t_invalid IS NULL")
            conditions.append("status = 'contested'")
        elif refutation_filter == "retracted":
            # Same posture: retracted-and-contradicted should still
            # surface under "retracted".
            if "t_invalid IS NULL" in conditions:
                conditions.remove("t_invalid IS NULL")
            conditions.append("status = 'retracted'")
        elif refutation_filter == "any":
            # Surface every refutation kind, implies include_invalidated.
            if "t_invalid IS NULL" in conditions:
                conditions.remove("t_invalid IS NULL")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    # The signature re-verification runs in Python after the fetch, so a flat
    # `LIMIT limit` could under-return when the top rows are all drained. Order
    # the table once and materialise up to the scan ceiling of sorted rows in a
    # single statement (no growing OFFSET, so no per-batch re-scan and re-sort),
    # then filter for survivors.
    base_sql = (
        f"SELECT {_CLAIM_SELECT} FROM claims {where} "
        f"ORDER BY created_at DESC LIMIT ?"
    )
    ceiling = _read_scan_ceiling(limit)
    try:
        cursor = conn.execute(base_sql, params + [ceiling])
    except sqlite3.OperationalError as exc:
        raise DatabaseError(f"Failed to query claims: {exc}") from exc
    # Step the live cursor rather than .fetchall(): _project_verified_rows breaks
    # at `limit` survivors, and on the common path (the first `limit` rows all
    # survive) that break stops fetching too, so the ceiling stays the worst-case
    # bound for the adversarial drain path instead of the per-call materialisation.
    results, scanned, contested = _project_verified_rows(
        conn, cursor, limit=limit, on_verify_excluded=on_verify_excluded,
        clean_only=refutation_filter == "clean",
    )
    if scanned >= ceiling and len(results) < limit:
        raise _scan_ceiling_error("query", ceiling, len(results), limit)
    _disclose_invalidation_gaps(
        conn, "claims", where, params,
        ceiling=ceiling, contested=contested, on_contested=on_contested,
        include_invalidated=include_invalidated,
    )
    return results


def _extract_signature_bundle_keyid(bundle_json: str | None) -> str | None:
    """Return the signing keyid embedded in a claim's signature_bundle,
    or None if the bundle is absent or malformed."""
    if bundle_json is None:
        return None
    try:
        bundle = json.loads(bundle_json)
        return bundle["signatures"][0]["keyid"]
    except (json.JSONDecodeError, KeyError, IndexError, TypeError):
        return None


def _enrolled_validator_keyids(conn: sqlite3.Connection) -> set[str]:
    """Return the set of keyids currently in the validators table.

    Membership only: does NOT walk the enrollment chain. The chain
    walk in :func:`mareforma.validators.is_enrolled` is the
    authoritative check for individual validations; this set is a
    cheap pre-filter, reported per row as ``generator_enrolled`` so a reader
    can tell a signer this project named from one it has not.
    """
    rows = conn.execute("SELECT keyid FROM validators").fetchall()
    return {r["keyid"] for r in rows}


def _compute_validator_reputation(
    conn: sqlite3.Connection,
) -> dict[str, int]:
    """Return ``{validator_keyid: count}`` for claims a validator signed off on.

    Count is the number of rows carrying a validation envelope signed by the
    key. Validators with none are omitted from the dict (caller defaults to 0).
    Derived state, recomputed on every call, never cached.

    Grouped by the signer named INSIDE the envelope, not by the
    ``validator_keyid`` column beside it. That column is an unsigned
    denormalisation, and grouping on it credited a validator for a row it never
    signed: a row carrying somebody else's envelope under its own name is
    refused by the read path, and the count is a separate SQL statement that
    never consulted the read path. Reading the signed thing is the only way the
    two agree.

    ``json_valid`` guards the extract, so a malformed envelope contributes to
    nobody rather than failing the statement for everybody.

    One statement over a column, so it counts envelopes that are PRESENT, not
    envelopes that bind: a row carrying a copy of a genuine envelope is counted
    for the key that signed it, though every read surface refuses to serve that
    row. The same shape as ``generator_enrolled``, and the same rule applies,
    a caller who needs the stronger answer asks the read path or
    ``mareforma verify``.
    """
    rows = conn.execute(
        "SELECT json_extract(validation_signature, '$.signatures[0].keyid') "
        "         AS signer, COUNT(*) AS n "
        "FROM claims "
        "WHERE validation_signature IS NOT NULL "
        "  AND json_valid(validation_signature) "
        "GROUP BY signer"
    ).fetchall()
    return {r["signer"]: int(r["n"]) for r in rows if r["signer"] is not None}


def _validate_fts5_query(query: str) -> str:
    """Sanity-check an FTS5 MATCH expression.

    Refuses empty strings and queries consisting entirely of wildcards
    (e.g. ``"*"``, ``"* **"``). FTS5 prefix syntax is ``term*`` and the
    leading-``*`` form is not valid syntax anyway, but a user who
    expects shell-glob semantics deserves a clear error instead of
    SQLite's terse ``fts5: syntax error near "*"``.
    """
    stripped = query.strip()
    if not stripped:
        raise ValueError(
            "Empty search query. Pass at least one term, optionally "
            "with FTS5 prefix syntax: graph.search('gene*')."
        )
    tokens = stripped.split()
    if all(t.strip("*") == "" for t in tokens):
        raise ValueError(
            f"Search query {query!r} is just wildcards. FTS5 prefix "
            "search requires at least one term (e.g. 'gene*'). A pure "
            "wildcard would scan the whole table and is refused."
        )
    return stripped


def search_claims(
    conn: sqlite3.Connection,
    query: str,
    *,
    limit: int = 20,
    classification: str | None = None,
    include_invalidated: bool = False,
    on_verify_excluded: Callable[[int], None] | None = None,
    on_contested: Callable[[int], None] | None = None,
) -> list[dict]:
    """FTS5-ranked search over claim text.

    Returns claim dicts ordered by FTS5 rank (best match first). Each
    dict carries the same projection as :func:`query_claims`:
    ``validator_reputation`` and ``generator_enrolled`` are attached
    per row, and ``include_invalidated`` /
    ``on_verify_excluded`` behave identically.

    The ``query`` string is passed through to SQLite's FTS5 MATCH
    operator. FTS5 syntax (phrase matching with double quotes, prefix
    search with trailing ``*``, ``AND``/``OR``/``NOT`` operators, and
    parentheses) works as documented in SQLite. Pure-wildcard queries
    are refused (see :func:`_validate_fts5_query`). ``limit`` follows
    :func:`query_claims`: zero returns no claims, a negative limit raises
    ``ValueError``.
    """
    _require_non_negative_limit(limit, "search")
    fts_query = _validate_fts5_query(query)

    if classification is not None and classification not in VALID_CLASSIFICATIONS:
        raise ValueError(
            f"Unknown classification '{classification}'. "
            f"Use one of: {', '.join(VALID_CLASSIFICATIONS)}"
        )

    conditions: list[str] = ["claims_fts MATCH ?"]
    params: list = [fts_query]

    if not include_invalidated:
        conditions.append("c.t_invalid IS NULL")

    if classification is not None:
        conditions.append("c.classification = ?")
        params.append(classification)
    where = " AND ".join(conditions)
    select_cols = ", ".join(f"c.{col}" for col in _CLAIM_COLUMNS)
    # Rank once and materialise up to the scan ceiling in a single statement,
    # then project through the SAME read-path filter as query_claims. Routing
    # both surfaces through _project_verified_rows re-verifies every row here
    # too, so search cannot serve a row that query correctly excludes, and the
    # two projections cannot drift apart.
    base_sql = (
        f"SELECT {select_cols} FROM claims_fts f "
        f"JOIN claims c ON c.claim_id = f.claim_id "
        f"WHERE {where} "
        f"ORDER BY rank LIMIT ?"
    )
    ceiling = _read_scan_ceiling(limit)
    try:
        cursor = conn.execute(base_sql, params + [ceiling])
    except sqlite3.OperationalError as exc:
        # FTS5 raises OperationalError on malformed MATCH syntax.
        # Wrap so callers don't have to import sqlite3 to pattern-match.
        msg = str(exc)
        if "fts5" in msg or "syntax error" in msg:
            raise ValueError(
                f"Search query {query!r} is not valid FTS5 syntax: {msg}"
            ) from exc
        raise DatabaseError(f"Failed to search claims: {exc}") from exc
    # Step the ranked cursor lazily: _project_verified_rows stops at `limit`
    # survivors, so the common path fetches a handful, not the whole ceiling.
    results, scanned, contested = _project_verified_rows(
        conn, cursor, limit=limit, on_verify_excluded=on_verify_excluded,
    )
    if scanned >= ceiling and len(results) < limit:
        raise _scan_ceiling_error("search", ceiling, len(results), limit)
    _disclose_invalidation_gaps(
        conn,
        "claims_fts f JOIN claims c ON c.claim_id = f.claim_id",
        "WHERE " + where, params, ceiling=ceiling, prefix="c.",
        # The contested count reaches the caller here as it does from query.
        # The shared projection replays the signed verdicts for both surfaces
        # and hands the count back to both, and this one bound it to a local
        # and dropped it, leaving `on_contested` in the signature with nothing
        # to call it. So a claim whose contradiction verdict is signed and
        # whose t_invalid somebody erased was served by search in silence and
        # by query with a disclosure, on the same graph, in the same process.
        contested=contested, on_contested=on_contested,
        include_invalidated=include_invalidated,
    )
    return results


def get_validator_reputation(conn: sqlite3.Connection) -> dict[str, int]:
    """Public wrapper around :func:`_compute_validator_reputation`.

    Returns a dict mapping every enrolled validator keyid to the number of
    claims it has signed off on. Validators with zero validations are
    included with ``count=0`` (the bulk map use case wants the full
    enrollment list, not just the active validators).
    """
    counts = _compute_validator_reputation(conn)
    enrolled = _enrolled_validator_keyids(conn)
    return {keyid: counts.get(keyid, 0) for keyid in enrolled}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# Backup deferral state, keyed by id(conn). A connection with an open deferral
# window records that a rewrite is due rather than writing claims.toml; the
# window writes once when the outermost window closes. The connection stays
# alive for the whole window, so its id is stable and unique for it. The value
# is ``[depth, dirty]``: nesting depth so an inner window does not end the outer
# one, and the pending-dirty flag a mutation sets.
_backup_suspended: dict[int, list] = {}


def suspend_backup(conn: sqlite3.Connection) -> None:
    """Open a backup-deferral window on *conn*. While open, _backup_claims_toml
    marks the backup dirty rather than writing claims.toml. Windows nest: a
    mutation writes once when the outermost window closes."""
    state = _backup_suspended.get(id(conn))
    if state is None:
        _backup_suspended[id(conn)] = [1, False]
    else:
        state[0] += 1


def resume_backup(conn: sqlite3.Connection, root: Path) -> None:
    """Close one deferral window on *conn*. The outermost close writes
    claims.toml once if a mutation marked it dirty; an inner close keeps the
    window open. A no-op when no window is open."""
    state = _backup_suspended.get(id(conn))
    if state is None:
        return
    state[0] -= 1
    if state[0] > 0:
        return
    dirty = state[1]
    del _backup_suspended[id(conn)]
    if dirty:
        _backup_claims_toml(conn, root)


def _drain_backup_window(conn: sqlite3.Connection, root: Path) -> None:
    """Close every open deferral level on *conn* at once and flush a pending
    write. For teardown: a graph closed mid-batch still leaves claims.toml
    current, and no window keyed on a soon-reused id() is left behind."""
    state = _backup_suspended.pop(id(conn), None)
    if state is not None and state[1]:
        _backup_claims_toml(conn, root)


def get_project_policy(conn: sqlite3.Connection) -> dict | None:
    """Return the singleton project-policy row as a dict, or None if unset.

    The signed ``envelope`` is the authority; the flat columns are a read
    cache. restore verifies the envelope against the enrolled root before
    trusting either.
    """
    row = conn.execute(
        "SELECT rekor_required, strict_promotion_required, signer_keyid, "
        "envelope, created_at, rekor_declared_at, strict_promotion_declared_at "
        "FROM project_policy WHERE id = 1"
    ).fetchone()
    return dict(row) if row is not None else None


def project_policy_flags(policy: dict | None) -> tuple[bool, bool]:
    """The ``(rekor_required, strict_promotion_required)`` pair of *policy*.

    An absent policy declares neither. Callers compare flag tuples rather
    than rows so the one-way rule is stated in one place.
    """
    if policy is None:
        return (False, False)
    return (
        bool(policy["rekor_required"]),
        bool(policy["strict_promotion_required"]),
    )


def project_policy_declared_at(
    policy: dict | None,
) -> tuple[str | None, str | None]:
    """When each of *policy*'s flags was first declared, in flag order.

    ``created_at`` is when the row was last signed, so extending the policy
    with a second rule moves it forward and it cannot date the first rule. A
    declaration signed before the per-flag times existed carries neither, and
    for it ``created_at`` is the best evidence there is: fall back to it for
    the flags such a policy does declare, which is what those projects were
    already held to. An undeclared flag has no declaration time.
    """
    if policy is None:
        return (None, None)
    rekor, strict = project_policy_flags(policy)
    fallback = policy["created_at"]
    return (
        (policy["rekor_declared_at"] or fallback) if rekor else None,
        (policy["strict_promotion_declared_at"] or fallback) if strict else None,
    )


# The reading a policy row gets when its envelope does not back it. Both rules
# read as declared, from before every claim in the graph, which is the strictest
# reading available and the only safe one: a policy that cannot be
# authenticated has been tampered with, and every way of tampering with it is a
# way of switching a rule OFF. Answering "no policy" instead would hand the
# attacker exactly what the edit was for.
_UNVERIFIED_POLICY: dict = {
    "rekor_required": 1,
    "strict_promotion_required": 1,
    "signer_keyid": None,
    "envelope": None,
    "created_at": "",
    "rekor_declared_at": "",
    "strict_promotion_declared_at": "",
}


def _policy_envelope_binds(
    conn: sqlite3.Connection, policy: dict,
) -> bool:
    """True iff the root's signature covers exactly this policy row.

    The same check restore runs before it enforces a ``[project_policy]``
    section (``restore._verify_and_insert_project_policy``), applied to the live
    row so recovery and the running graph hold the policy to one standard: the
    signer must be the project's single enrolled root, the envelope must verify
    under that root's pubkey at the project-policy payload type, the payload's
    own version fixes which fields it is allowed to carry, and every flat column
    must match what the payload says. Never raises: an unreadable envelope is
    not a verified one.
    """
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    signer_keyid = policy["signer_keyid"]
    root_keyid = _validators.trust_domain_root(conn)
    if (
        not signer_keyid
        or root_keyid is None
        or signer_keyid != root_keyid
        or not _validators.is_enrolled(conn, signer_keyid)
    ):
        return False
    signer_row = _validators.get_validator(conn, signer_keyid)
    if signer_row is None:
        return False
    try:
        env = json.loads(policy["envelope"] or "")
        pem = base64.standard_b64decode(signer_row["pubkey_pem"])
        pub = _signing.public_key_from_pem(pem)
        if not _signing.verify_envelope(
            env, pub,
            expected_payload_type=_signing.PAYLOAD_TYPE_PROJECT_POLICY,
        ):
            return False
        payload = _signing.envelope_payload(env)
        signed_fields = _signing._project_policy_fields(
            payload.get("version", 1)
        )
    except Exception:
        return False
    if set(payload) != set(signed_fields):
        return False
    return (
        bool(payload.get("rekor_required")) == bool(policy["rekor_required"])
        and bool(payload.get("strict_promotion_required"))
        == bool(policy["strict_promotion_required"])
        and payload.get("created_at") == policy["created_at"]
        and payload.get("rekor_declared_at") == policy["rekor_declared_at"]
        and payload.get("strict_promotion_declared_at")
        == policy["strict_promotion_declared_at"]
    )


def _verified_project_policy(conn: sqlite3.Connection) -> dict | None:
    """The stored policy, but only as far as its root signature backs it.

    Every enforcement of the policy reads it through here. The envelope is the
    authority and the flat columns are a denormalized cache, so a rule that
    binds every writer cannot be read off columns any writer can edit: one
    ``UPDATE project_policy SET strict_promotion_required = 0`` retires the rule
    for the whole graph, and moving a ``*_declared_at`` forward grandfathers
    every claim written in between.

    No policy row answers None, the honest "nothing declared". A row whose
    envelope does not bind it answers :data:`_UNVERIFIED_POLICY`, the strictest
    reading, because tampering here can only ever be an attempt to switch a rule
    off. ``get_project_policy`` stays the raw reader for the backup writer and
    for the one-way check in :func:`set_project_policy`, which have to see the
    row as stored.
    """
    policy = get_project_policy(conn)
    if policy is None:
        return None
    return policy if _policy_envelope_binds(conn, policy) else _UNVERIFIED_POLICY


def project_policy_unverified(conn: sqlite3.Connection) -> bool:
    """True when a stored project policy exists but its root signature does not
    back it, so every enforcement reads the fail-closed :data:`_UNVERIFIED_POLICY`.

    The stalled state: the row is present, so the project declared a rule, but
    the signed envelope no longer binds it. :func:`_verified_project_policy` then
    hands every caller the strictest possible policy (both rules on, declared
    before every claim), which is correct as a defence but silent as a signal.
    An operator meets it as a promotion held closed or a restore refusing the
    backup, with nothing on ``mareforma status`` to say why. This predicate is
    what ``health()`` and ``status`` read to name it.
    """
    policy = get_project_policy(conn)
    if policy is None:
        return False
    return not _policy_envelope_binds(conn, policy)


def set_project_policy(
    conn: sqlite3.Connection,
    root: Path,
    *,
    envelope: str,
    signer_keyid: str,
    rekor_required: bool,
    strict_promotion_required: bool,
    created_at: str,
    rekor_declared_at: str | None,
    strict_promotion_declared_at: str | None,
) -> dict:
    """Persist the singleton project policy and refresh the backup.

    One-way: a flag already declared stays declared, at the time it was first
    declared. A declaration that adds a flag replaces the row with its newly
    signed envelope; one that adds nothing returns the stored policy unchanged.
    Locks with BEGIN IMMEDIATE so a racing writer serializes on the singleton.
    Returns the effective policy.

    Raises
    ------
    ProjectPolicyError
        If the stored policy carries a flag this envelope does not, or dates a
        flag earlier than this envelope does: the signed material cannot speak
        for that flag, so the write is refused rather than dropping the rule or
        moving its start forward. Re-read the policy and sign the union.
    """
    wanted = (rekor_required, strict_promotion_required)
    wanted_since = (rekor_declared_at, strict_promotion_declared_at)
    existing = get_project_policy(conn)
    if existing is not None and project_policy_flags(existing) == wanted:
        return existing
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = get_project_policy(conn)
        stored = project_policy_flags(existing)
        if existing is not None and stored == wanted:
            conn.execute("COMMIT")
            return existing
        if any(was and not now for was, now in zip(stored, wanted)):
            conn.execute("ROLLBACK")
            raise ProjectPolicyError(
                "The stored project policy declares a rule this envelope "
                "does not carry, so persisting it would revoke that rule. "
                "Re-read the policy and sign the union."
            )
        if any(
            was is not None and (now is None or now > was)
            for was, now in zip(project_policy_declared_at(existing), wanted_since)
        ):
            conn.execute("ROLLBACK")
            raise ProjectPolicyError(
                "The stored project policy declares a rule earlier than this "
                "envelope dates it, so persisting it would move the rule's "
                "start forward. Re-read the policy and sign the union."
            )
        # Upsert, not delete-then-insert: the row records a one-way rule and the
        # table's no-delete guard refuses to let it go, so the replacement
        # rewrites the singleton in place inside the policy window (the marker
        # the append-only guard looks for).
        with _policy_window(conn):
            conn.execute(
                "INSERT INTO project_policy "
                "(id, rekor_required, strict_promotion_required, signer_keyid, "
                "envelope, created_at, rekor_declared_at, "
                "strict_promotion_declared_at) VALUES (1, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "rekor_required = excluded.rekor_required, "
                "strict_promotion_required = "
                "excluded.strict_promotion_required, "
                "signer_keyid = excluded.signer_keyid, "
                "envelope = excluded.envelope, "
                "created_at = excluded.created_at, "
                "rekor_declared_at = excluded.rekor_declared_at, "
                "strict_promotion_declared_at = "
                "excluded.strict_promotion_declared_at",
                (
                    1 if rekor_required else 0,
                    1 if strict_promotion_required else 0,
                    signer_keyid, envelope, created_at,
                    rekor_declared_at, strict_promotion_declared_at,
                ),
            )
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise
    _backup_claims_toml(conn, root)
    return get_project_policy(conn)


# Trust-layer tables round-tripped through claims.toml, in foreign-key order
# (parents before children) so restore can replay them without FK violations:
# predictions -> propositions, findings -> propositions/predictions/claims,
# evidence_lines -> findings, contrasts -> evidence_lines,
# effect_estimates -> contrasts. Each tuple is
# (section_name, table, primary_key, ordered non-PK columns). The explicit
# column list avoids SELECT * coupling and is the single source of truth shared
# by the backup reader and the restore writer.
_TRUST_TABLE_BACKUP: tuple = (
    ("propositions", "propositions", "content_id", (
        "frame_id", "subject", "relation", "object", "direction",
        "scope_json", "magnitude", "content_id_policy", "schema_version",
        "created_at",
    )),
    ("predictions", "predictions", "plan_id", (
        "content_id", "inference_regime", "test_type", "direction_of_interest",
        "equivalence_lower", "equivalence_upper", "alpha", "preregistered",
        "registered_at",
    )),
    ("findings", "findings", "finding_id", (
        "content_id", "plan_id", "claim_id", "bearing_direction", "created_at",
    )),
    ("evidence_lines", "evidence_lines", "line_id", (
        "finding_id", "modality", "provenance_id", "design_type", "data_id",
        "model_lineage", "created_at",
    )),
    ("contrasts", "contrasts", "contrast_id", (
        "line_id", "control_type",
    )),
    ("effect_estimates", "effect_estimates", "estimate_id", (
        "contrast_id", "estimate_value", "effect_type", "scale", "p_value",
        "ci_lower", "ci_upper", "ci_level", "n_total",
    )),
    # Last: a retirement references two predictions rows and a claim, so both
    # parents are already replayed by the time it lands. The triple it carries
    # is re-derived from its signed attestation after the replay (see
    # restore._verify_plan_retirement_binding).
    ("plan_retirements", "plan_retirements", "plan_id", (
        "superseded_by", "reason", "claim_id", "retired_at",
    )),
)


def _backup_trust_tables(conn: sqlite3.Connection, data: dict) -> None:
    """Add the populated trust-layer tables to the backup ``data`` dict.

    Each table becomes a TOML section keyed by its primary key, with NULL-valued
    columns omitted (TOML cannot serialize None, and restore reads each column
    with a NULL default). The finding tree is reconstructable on restore from
    these rows, which hang off the finding's own signed attestation claim.
    """
    for section, table, pk, cols in _TRUST_TABLE_BACKUP:
        rows = conn.execute(
            f"SELECT {pk}, {', '.join(cols)} FROM {table}"
        ).fetchall()
        if not rows:
            continue
        section_data: dict[str, Any] = {}
        for r in rows:
            entry: dict[str, Any] = {}
            for col in cols:
                value = r[col]
                if value is not None:
                    entry[col] = value
            section_data[r[pk]] = entry
        data[section] = section_data


def _backup_grounding_attestations(
    conn: sqlite3.Connection, data: dict,
) -> None:
    """Add the observer's grounding attestations to the backup ``data`` dict.

    This is the section the whole artifact is for. The axis travels in the
    claim's signed statement already; what did not travel was any record that an
    observer, rather than the producer's typing, put it there. Round-tripping
    these is what lets a restored graph be held to the standard the write path
    holds.

    Emitted only when populated, like every other optional section.
    """
    rows = conn.execute(
        "SELECT claim_id, statement_cid, receipt_digest, grounding, "
        "signer_keyid, signature, created_at FROM grounding_attestations "
        "ORDER BY created_at, claim_id"
    ).fetchall()
    if not rows:
        return
    data["grounding_attestations"] = {
        r["claim_id"]: {
            "statement_cid": r["statement_cid"],
            "receipt_digest": r["receipt_digest"],
            "grounding": r["grounding"],
            "signer_keyid": r["signer_keyid"],
            "signature": base64.b64encode(r["signature"]).decode("ascii"),
            "created_at": r["created_at"],
        }
        for r in rows
    }


def _backup_verdict_chain(conn: sqlite3.Connection, data: dict) -> None:
    """Add the verdict-set chain to the backup ``data`` dict.

    Keyed by sequence number as a string, because TOML table keys are strings.
    Round-tripping it is what carries the chain through the recovery the file
    exists for: a restore that dropped the chain would rebuild a graph whose
    verdicts are all uncovered, which reads exactly like a graph somebody
    stripped.

    Emitted only when populated, so a graph that has recorded no verdict under
    this version writes no section, the same rule every other optional section
    follows.
    """
    rows = conn.execute(
        "SELECT seq, prev_tip, tip, verdict_kind, verdict_id, verdict_digest, "
        "issuer_keyid, signature, created_at FROM verdict_chain ORDER BY seq"
    ).fetchall()
    rows = _chain_prefix_that_forms_a_chain(conn, rows, data)
    if not rows:
        return
    data["verdict_chain"] = {
        str(r["seq"]): {
            "prev_tip": r["prev_tip"],
            "tip": r["tip"],
            "verdict_kind": r["verdict_kind"],
            "verdict_id": r["verdict_id"],
            "verdict_digest": r["verdict_digest"],
            "issuer_keyid": r["issuer_keyid"],
            "signature": base64.b64encode(r["signature"]).decode("ascii"),
            "created_at": r["created_at"],
        }
        for r in rows
    }


def _chain_prefix_that_forms_a_chain(
    conn: sqlite3.Connection, rows: list, data: dict,
) -> list:
    """The leading run of links that still forms a chain, and what stopped it.

    A backup carrying links that do not link rebuilds a graph broken the same
    way, so the one artifact meant to recover from tampering hands the
    tampering back. Stopping at the first link that does not follow the one
    before it gives an operator something they can open.

    Structure only: the sequence numbering, the previous tip each link names,
    and whether a link's stored tip is the one its own contents produce. Not
    signatures, not enrolment, not the verdict a link covers. Those are the
    read path's answer and the restore's, and asking them here cost a full
    chain verification on every mutation, which is per-write work that grows
    with the graph. What it leaves for the restore is the case where the file
    is a chain and the chain is forged, and the restore refuses that.

    The count and the reason go in the file, because leaving quietly is the
    silence the chain exists to close. The links stay in the graph this was
    read from, which still holds them and still reports them on every read.
    """
    if not rows:
        return rows
    kept: list = []
    expected_prev = ""
    stopped = ""
    for link in rows:
        try:
            stopped = _why_a_link_does_not_follow(link, len(kept) + 1, expected_prev)
        except Exception as exc:  # noqa: BLE001
            # Every exception, not only sqlite3's, for the reason the read
            # path's copy of this gives: on a graph somebody has taken apart
            # these rows hold whatever they hold, and a column typed against
            # the schema reaches the hashing with the wrong type. Narrower than
            # this, the error escaped into the backup writer, and the backup
            # writer runs inside every mutation, so a single planted row made
            # the graph permanently unwritable. The row cannot be deleted
            # either, the chain is append-only. Withholding is the recoverable
            # answer; raising is not.
            stopped = f"link {len(kept) + 1} could not be read: {type(exc).__name__}"
        if stopped:
            break
        kept.append(link)
        expected_prev = link["tip"]
    dropped = len(rows) - len(kept)
    if dropped:
        data["verdict_chain_withheld"] = dropped
        data["verdict_chain_withheld_because"] = stopped
    return kept


def _why_a_link_does_not_follow(
    link: sqlite3.Row, expected_seq: int, expected_prev: str,
) -> str:
    """Empty when the link follows the one before it, else why it does not."""
    if link["seq"] != expected_seq:
        return (
            f"link {expected_seq} is out of sequence, the row here is numbered "
            f"{link['seq']}: links are numbered without gaps, so a jump is a "
            "link that was removed or one that was put in"
        )
    if link["prev_tip"] != expected_prev:
        return (
            f"link {expected_seq} names a previous tip no earlier link "
            "produced, so the chain is broken at this point"
        )
    record = {
        "seq": link["seq"],
        "prev_tip": link["prev_tip"],
        "verdict_kind": link["verdict_kind"],
        "verdict_id": link["verdict_id"],
        "verdict_digest": link["verdict_digest"],
        "issuer_keyid": link["issuer_keyid"],
    }
    if _verdict_chain_tip(record) != link["tip"]:
        return (
            f"link {expected_seq} stores a tip its own contents do not "
            "produce, so the row was edited after it was written"
        )
    return ""


def _backup_schema_census(conn: sqlite3.Connection, data: dict) -> None:
    """Add the write-guard census to the backup ``data`` dict.

    The census is the graph's memory that a guard was found missing, and a
    guard that came back is not a guard that was never gone: the rows it let
    somebody delete while it was down are gone, and no later open can see that.
    Leaving the census out of the backup made a round trip erase exactly that
    memory, so a graph could be tampered with, backed up and restored, and read
    clean on every surface that had just called it tampered.

    It is observation rather than evidence, which is why it rides as its own
    section and not as a claim: the file records that something was seen
    missing, and the restored graph goes on saying so.

    Emitted only when populated, the rule every optional section follows.
    """
    try:
        rows = conn.execute(
            "SELECT observed_at, missing FROM schema_census "
            "ORDER BY observed_at, missing"
        ).fetchall()
    except sqlite3.OperationalError:
        return          # no census table on this schema: nothing observed
    if not rows:
        return
    data["schema_census"] = {
        str(n): {"observed_at": r["observed_at"], "missing": r["missing"]}
        for n, r in enumerate(rows, start=1)
    }


# The line that separates the backup's body from its completeness table. The
# digest below covers every byte before it, so both the writer and
# :func:`verify_completeness_digest` locate the split on this exact string.
def _verdict_chain_completeness(data: dict) -> dict:
    """What the verdict chain in *data* says about itself, measured from *data*.

    The writer records it, the restore reader holds the file to it, and the test
    helper that rebuilds a table after an edit reproduces it. One function, so a
    file can never disagree with its own table because two places computed it
    differently.

    Measured from the FILE, never from the graph. Read off the graph, a backup
    the writer could not write in full advertised a chain longer than the one it
    carried and named a tip no link in it produces.
    """
    links = data.get("verdict_chain")
    links = links if isinstance(links, dict) else {}
    # The writer keys links by their sequence number, so the tip is the highest.
    # A hand-edited file can carry anything, and this runs on the RECOVERY path
    # over exactly that input, so a key that is not a number is skipped rather
    # than converted: raising here would surface as a bare ValueError out of
    # restore, past its documented RestoreError contract, and a disclosure must
    # never be the thing that fails a recovery. A file whose keys are not the
    # shape the writer produces disagrees with its own table anyway, which the
    # caller reports.
    ordered = []
    for seq in links:
        try:
            ordered.append((int(seq), seq))
        except (TypeError, ValueError):
            continue
    last = max(ordered)[1] if ordered else None
    tip = links[last] if last is not None else None
    return {
        "verdict_chain_tip": tip.get("tip", "") if isinstance(tip, dict) else "",
        "verdict_chain_covered": len(links),
        "verdicts_total": sum(
            len(data[name])
            for name in ("contradiction_verdicts", "replication_verdicts")
            if isinstance(data.get(name), dict)
        ),
    }


_COMPLETENESS_HEADER = "[completeness]\n"

# Which shape of claims.toml this is, stamped at the top of every backup.
#
# The completeness table lets a reader ask whether a backup accounts for itself,
# and a reader can only hold a file to that question if the file says it owes an
# answer. Without this number it cannot: a backup of a healthy graph carries only
# [validators], [claims] and [graph_meta] beside the table, because every other
# section is written only when it has rows. Delete the table from such a file and
# what is left has the same sections, and the same keys, as a backup written
# before the table existed. Measured against the released trees, not reasoned
# about. So absence could not be read as tamper, and the silence a stripped file
# keeps was the same silence an honest older file keeps.
#
# It is a top-level key rather than a field inside a section, and that is the
# whole of its truncation value. Inside [graph_meta] it sat seventeen bytes above
# the table on a seven-kilobyte file, so all but one cut that took the table took
# the stamp with it. On line one it survives every cut that leaves a parseable
# file. Measured both ways.
#
# What it does not do: beat an editor who removes it along with the table. This
# is a claim the file makes about itself and nothing signs it, so that edit puts
# the file back where it was. It closes deleting the completeness table whole and
# leaving the rest, which is one shape, not the class.
#
# The number rises when the set of sections a reader may rely on changes. A
# reader that meets a number above its own cannot say what that file owes, and
# says so rather than guessing in either direction.
_BACKUP_FORMAT = 1


def _backup_completeness_tail(
    conn: sqlite3.Connection, data: dict, body: str,
) -> str:
    """The ``[completeness]`` table: what this file says it contains.

    Row counts per emitted section, the verdict-chain tip, the covered-versus-
    total verdict pair, and a SHA-256 over *body*, which is every byte of the
    file that precedes this table.

    **The digest is not a signature and must never be described as one.**
    Anyone editing the file recomputes it in a line. What it does is make
    truncation, corruption and casual editing detectable, and make a deliberate
    attacker be deliberate. Nothing signs at backup time because nothing holds a
    key at backup time, which is the constraint the verdict chain works around
    by signing where a key genuinely is, and which this table cannot.

    It digests the serialized body rather than a second canonical form of the
    same data, for two reasons. The writer has already paid for those bytes, and
    hashing them again costs nothing, where canonicalizing the whole dict a
    second time measured at about a third of the cost of writing a claim. And a
    reader checks it by hashing the file it is holding, with no need to
    reproduce a serialization byte for byte before it can agree.

    Every count describes the file rather than the graph, so a file truncated
    after it was written disagrees with itself. The verdict pair included: it
    used to be read off the graph, which meant a backup the writer could not
    write in full advertised a chain longer than the one it carried, and named
    a tip no link in it produces. A table whose job is to say what the file
    holds cannot describe something else. The gap between covered and total is
    still how a reader sees which verdicts predate the chain, and now the gap
    also opens when the writer withheld links, which is the same question a
    reader is asking.
    """
    import tomli_w

    table = {
        "completeness": {
            "sections": {
                name: len(entries)
                for name, entries in sorted(data.items())
                if isinstance(entries, dict)
            },
            **_verdict_chain_completeness(data),
            "digest": hashlib.sha256(body.encode("utf-8")).hexdigest(),
        }
    }
    tail = tomli_w.dumps(table)
    if not tail.startswith(_COMPLETENESS_HEADER):
        # tomli_w puts the table header first for a single-table document. If
        # that ever stops being true the split point moves and every stored
        # digest silently stops reproducing, so refuse rather than write one.
        raise FormatArtifactError(
            "the completeness table did not serialize with its own header "
            f"first, so the digest boundary is not where readers look: {tail[:60]!r}"
        )
    return tail


def verify_completeness_digest(claims_toml: "str | Path") -> bool:
    """True iff the file's stored digest matches the bytes above it.

    Splits on the last :data:`_COMPLETENESS_HEADER` and hashes everything
    before it. No TOML is re-serialized, so the answer does not depend on
    agreeing with the writer's formatting, only on the bytes on disk.

    False for a file with no completeness table, which is every backup written
    before the table existed, and for one whose digest does not reproduce.
    Truncation, corruption and hand-editing all land here. A deliberate
    attacker recomputes it, which is why this is not a signature and the
    verdict chain exists beside it.
    """
    # Line endings normalised before the boundary is looked for, the same way
    # and for the same reason as the reader that asks what follows the table.
    # When only one of the two normalised, the pair could be made to locate
    # different boundaries in one file: a decoy header written with carriage
    # returns was invisible here and visible there, and a forged section between
    # them was checked by neither while this still returned True. So the rule is
    # that both find the boundary the same way, and the cost of that is that a
    # line-ending conversion no longer reads as an edit, which it never was.
    raw = Path(claims_toml).read_bytes().replace(b"\r\n", b"\n")
    marker = ("\n" + _COMPLETENESS_HEADER).encode("utf-8")
    cut = raw.rfind(marker)
    if cut == -1:
        return False
    body = raw[: cut + 1]
    try:
        import tomllib          # 3.11+ stdlib
    except ModuleNotFoundError:  # 3.10, where it is the tomli backport
        import tomli as tomllib  # type: ignore[no-redef]

    try:
        stored = tomllib.loads(raw.decode("utf-8"))["completeness"]["digest"]
    except (ValueError, KeyError, UnicodeDecodeError):
        return False
    return hashlib.sha256(body).hexdigest() == stored


def tables_below_completeness(claims_toml: "str | Path") -> "tuple[str, ...]":
    """The names of any tables written below the ``[completeness]`` table.

    Empty for every file this writer produces, because the completeness table is
    the last thing it writes.

    This is the blind spot the digest cannot cover by construction. The digest
    is taken over the bytes ABOVE the header, so bytes below it are outside what
    it attests, and the row counts only walk the section names the table itself
    declares, so a section the file never had is counted by neither. Measured
    rather than reasoned about: a well-formed transparency-log entry appended
    under the table restored into the graph, flipped a real claim to logged, and
    the digest still verified with nothing said.

    **The tail is parsed, not scanned.** Reading it line by line for something
    that opens with a bracket and closes with one was the first version, and a
    single trailing comment walked straight past it: ``[rekor_inclusions."..."]
    # note`` is a table to TOML and was not one to that check, so the same
    forged entry landed again in silence. Measured before and after. Every other
    shape a header can take, whitespace, an array of tables, a quoted key with a
    bracket in it, is the same class of mistake waiting, and the parser already
    knows all of them.

    Raises :class:`ValueError` when the tail cannot be parsed, rather than
    reporting nothing found. The two are different answers and collapsing them
    hands a caller "there is nothing below the table" for a file where there
    demonstrably is: a multi-line string carrying the marker moves the boundary,
    the tail then starts mid-string, and the parse fails. Restore has already
    parsed the whole file by the time it asks, so it does not meet this; a
    caller reaching the function directly does, and should hear about it.
    """
    try:
        import tomllib          # 3.11+ stdlib
    except ModuleNotFoundError:  # 3.10, where it is the tomli backport
        import tomli as tomllib  # type: ignore[no-redef]

    # Line endings are normalised before the boundary is looked for. The file is
    # read as text everywhere else, which is newline-agnostic, so a backup that
    # went through a Windows editor or a checkout that rewrites endings is an
    # ordinary honest file. Searching it for a byte pattern that requires a bare
    # newline found nothing, which made every such file look like it had no
    # table below and let a forged section hide behind the digest complaint the
    # conversion produced on its own.
    raw = Path(claims_toml).read_bytes().replace(b"\r\n", b"\n")
    marker = ("\n" + _COMPLETENESS_HEADER).encode("utf-8")
    cut = raw.rfind(marker)
    if cut == -1:
        return ()
    # From the header itself, so the tail is a document in its own right and
    # its own table is named rather than inferred from what follows it.
    try:
        tail = tomllib.loads(raw[cut + 1:].decode("utf-8"))
    except (ValueError, UnicodeDecodeError, RecursionError) as exc:
        raise ValueError(
            f"the bytes below the completeness table of {claims_toml} are not "
            f"a table this format writes: {exc}"
        ) from exc
    return tuple(name for name in tail if name != "completeness")


def _format_artifact(build, *args):
    """Run a format writer, raising rather than degrading to absent.

    Every failure inside one becomes a :class:`FormatArtifactError`, which
    :func:`_backup_claims_toml` re-raises instead of printing, for the reason on
    the class.

    In ordinary operation this cannot fire: the writers read one table and hash
    bytes that are already in hand. The raise is reserved for a graph that is
    already broken, and on such a graph a caller finding out is the point.
    """
    try:
        return build(*args)
    except FormatArtifactError:
        raise
    except Exception as exc:
        raise FormatArtifactError(
            f"a claims.toml format section could not be built: {exc}. "
            "Whatever mutation triggered this backup is already committed to "
            "graph.db, which stays authoritative; what did not happen is the "
            "backup, and claims.toml still holds the previous good copy. This "
            "refuses instead of printing because a completeness section that "
            "is merely absent cannot be told apart from one that was never "
            "written."
        ) from exc


def _backup_claims_toml(conn: sqlite3.Connection, root: Path) -> None:
    """Write all claims AND validators to claims.toml in the project root.

    Called after every claim or validator mutation. The TOML file is
    the source of truth for ``mareforma restore`` after catastrophic
    loss of ``graph.db``.

    **Most failures are non-fatal.** An error line goes to stderr and the
    exception is not raised: graph.db is still authoritative and the next
    successful mutation rewrites the file. Stderr-ERROR rather than
    ``warnings.warn``, which production callers often suppress, so divergence
    is visible by default.

    **The completeness sections are the exception, and they raise.** Every
    other section degrades to a stale backup that the next mutation repairs. An
    absent completeness section cannot be told apart from a backup written
    before the section existed, so its silence has the shape of the tamper it
    exists to detect, and a writer whose job is detecting silence cannot fail
    silently. :class:`~mareforma.db.errors.FormatArtifactError` therefore
    reaches the caller, which means **every mutating call can raise it, after
    the mutation itself has already committed**. The row is in graph.db; what
    did not happen is the backup.
    """
    state = _backup_suspended.get(id(conn))
    if state is not None:
        # A deferral window is open: mark a rewrite due; resume_backup writes it.
        state[1] = True
        return
    try:
        import tomli_w

        # First, and unconditionally. A stamp that is sometimes absent says
        # nothing when it is absent, and one written low in the file goes with
        # the bytes a truncation takes. tomli_w emits top-level keys ahead of
        # every table, so this lands on line one whatever else the graph holds.
        data: dict[str, Any] = {"backup_format": _BACKUP_FORMAT}

        # Validators first so a restore pass can verify enrollment
        # signatures before trying to verify the claims that reference
        # those keys.
        from mareforma import validators as _validators
        validator_rows = _validators.list_validators(conn)
        if validator_rows:
            data["validators"] = {}
            for v in validator_rows:
                data["validators"][v["keyid"]] = {
                    "pubkey_pem": v["pubkey_pem"],
                    "identity": v["identity"],
                    "validator_type": v["validator_type"],
                    "enrolled_at": v["enrolled_at"],
                    "enrolled_by_keyid": v["enrolled_by_keyid"],
                    "enrollment_envelope": v["enrollment_envelope"],
                }

        # Rows are mirrored verbatim; the mirror never reads verify-on-read, so
        # it fetches them directly rather than through list_claims. That flag
        # costs a signature re-verification and a corroboration pass per
        # high-trust row, and this runs after every write.
        try:
            claims = [
                dict(r) for r in conn.execute(
                    f"SELECT {_CLAIM_SELECT} FROM claims ORDER BY created_at DESC"
                ).fetchall()
            ]
        except sqlite3.OperationalError as exc:
            raise DatabaseError(f"Failed to list claims: {exc}") from exc
        data["claims"] = {}
        for c in claims:
            supports = json.loads(c.get("supports_json", "[]") or "[]")
            contradicts = json.loads(c.get("contradicts_json", "[]") or "[]")
            entry: dict[str, Any] = {
                "text": c["text"],
                "classification": c.get("classification") or "INFERRED",
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
            if c.get("idempotency_key"):
                # Not signed material, but dropping it breaks the retry-safe
                # write contract on a restored graph: the replay of a step
                # misses the key lookup and inserts a signed near-duplicate.
                entry["idempotency_key"] = c["idempotency_key"]
            if c.get("signature_bundle"):
                entry["signature_bundle"] = c["signature_bundle"]
            if c.get("validation_signature"):
                entry["validation_signature"] = c["validation_signature"]
            # transparency_logged: only record when it deviates from the
            # default (1). A 0 means "signed but awaiting Rekor inclusion".
            if c.get("transparency_logged") == 0:
                entry["transparency_logged"] = False
            if c.get("artifact_hash"):
                entry["artifact_hash"] = c["artifact_hash"]
            # Evidence vector: always present in the current schema.
            # Round-trip the full JSON so restore can rebuild the
            # canonical Statement v1 bytes, chain_hash + signature both
            # bind these values. statement_cid is the cross-check anchor
            # restore uses to detect envelope-vs-row drift.
            entry["evidence_json"] = c.get("evidence_json") or "{}"
            if c.get("statement_cid"):
                entry["statement_cid"] = c["statement_cid"]
            # t_invalid is derived (set by the contradiction trigger
            # on signed verdict INSERT). Restore replays the verdict
            # table; the trigger fires again and re-sets t_invalid.
            # We do NOT round-trip the column directly, that would
            # accept a TOML-tampered t_invalid value without verifying
            # it against a signed contradiction envelope.
            # Adapter-specific predicate_payload + federation-imported
            # original_signature_bundle: round-trip only when populated.
            # Empty/NULL defaults stay omitted from the TOML so backups
            # don't grow new fields uselessly.
            if c.get("predicate_payload"):
                entry["predicate_payload"] = c["predicate_payload"]
            if c.get("original_signature_bundle"):
                entry["original_signature_bundle"] = c["original_signature_bundle"]
            # Observed grounding verdict: round-trip only when populated, so a
            # backup of a graph that never used the observer grows no new field.
            # It is bound into the signed statement, so restore rebuilds the
            # canonical bytes from it and statement_cid catches any TOML tamper.
            if c.get("observed_grounding"):
                entry["observed_grounding"] = c["observed_grounding"]
            data["claims"][c["claim_id"]] = entry

        # Verdict tables. Each verdict carries its own signature
        # binding (issuer_keyid, payload bytes) so restore can
        # cryptographically verify before re-INSERT. The trigger that
        # sets t_invalid fires on the re-INSERT, restoring the
        # invalidation state without needing a separate t_invalid
        # round-trip.
        #
        # include_invalidated=True because backup MUST capture every
        # signed verdict regardless of whether its referenced claim
        # has been invalidated. The default-filter is for user-facing
        # query semantics; backup is audit-mode by definition.
        rep_rows = list_replication_verdicts(conn, include_invalidated=True)
        if rep_rows:
            data["replication_verdicts"] = {}
            for v in rep_rows:
                vid = v["verdict_id"]
                # other_claim_id is NULL for a single-row cross-method verdict.
                # TOML cannot serialize None, so emit the key only when present,
                # mirroring the conditional-key pattern in the claims section.
                # restore reads it with .get(), so an absent key yields None.
                verdict_entry: dict[str, Any] = {
                    "cluster_id": v["cluster_id"],
                    "member_claim_id": v["member_claim_id"],
                    "method": v["method"],
                    "confidence_json": v["confidence_json"],
                    "issuer_keyid": v["issuer_keyid"],
                    "signature": base64.b64encode(v["signature"]).decode("ascii"),
                    "created_at": v["created_at"],
                }
                if v["other_claim_id"] is not None:
                    verdict_entry["other_claim_id"] = v["other_claim_id"]
                data["replication_verdicts"][vid] = verdict_entry
        con_rows = list_contradiction_verdicts(conn, include_invalidated=True)
        if con_rows:
            data["contradiction_verdicts"] = {}
            for v in con_rows:
                vid = v["verdict_id"]
                data["contradiction_verdicts"][vid] = {
                    "member_claim_id": v["member_claim_id"],
                    "other_claim_id": v["other_claim_id"],
                    "confidence_json": v["confidence_json"],
                    "issuer_keyid": v["issuer_keyid"],
                    "signature": base64.b64encode(v["signature"]).decode("ascii"),
                    "created_at": v["created_at"],
                }

        # Rekor inclusion sidecar. Every successful submit is recorded
        # here independently of whether the claims-row UPDATE succeeded.
        # Round-tripping through TOML lets restore() re-verify inclusion
        # proofs against a pinned log pubkey.
        rekor_rows = conn.execute(
            "SELECT claim_id, uuid, log_index, integrated_time, "
            "raw_response_b64, recorded_at "
            "FROM rekor_inclusions ORDER BY recorded_at"
        ).fetchall()
        if rekor_rows:
            data["rekor_inclusions"] = {}
            for r in rekor_rows:
                # integrated_time is NULL when the log response carried a
                # malformed integratedTime. Emit the key only when present so
                # None is never handed to the TOML serializer; restore reads it
                # with .get() and tolerates its absence.
                rekor_entry: dict[str, Any] = {
                    "uuid": r["uuid"],
                    "log_index": r["log_index"],
                    "raw_response_b64": r["raw_response_b64"],
                    "recorded_at": r["recorded_at"],
                }
                if r["integrated_time"] is not None:
                    rekor_entry["integrated_time"] = r["integrated_time"]
                data["rekor_inclusions"][r["claim_id"]] = rekor_entry

        # Project policy: a root-signed, project-wide trust declaration. The
        # signed envelope is the authority restore verifies; the flat fields
        # are the read cache. Emitted only when set.
        policy_row = get_project_policy(conn)
        if policy_row is not None:
            data["project_policy"] = {
                "rekor_required": bool(policy_row["rekor_required"]),
                "strict_promotion_required": bool(
                    policy_row["strict_promotion_required"]
                ),
                "signer_keyid": policy_row["signer_keyid"],
                "envelope": policy_row["envelope"],
                "created_at": policy_row["created_at"],
            }
            # Absent rather than null for an undeclared flag: TOML has no null,
            # and restore reads an absent field as "the envelope does not date
            # this flag", which is what a pre-v3 declaration means.
            for column in ("rekor_declared_at", "strict_promotion_declared_at"):
                if policy_row[column] is not None:
                    data["project_policy"][column] = policy_row[column]

        # Trust layer (propositions, predictions, findings, evidence_lines,
        # contrasts, effect_estimates). Round-trip these query-side tables so
        # the documented delete-and-restore recovery rebuilds the finding tree,
        # not just the surviving finding claims. Emitted only when populated.
        _backup_trust_tables(conn, data)

        # Supports-edge revision counter. Not signed material: the supports
        # cache compares itself against it to decide whether to rebuild. It
        # round-trips so a restored graph resumes the count instead of
        # climbing back through values a surviving cache file already stamped.
        from mareforma import _supports
        data["graph_meta"] = {
            "supports_revision": _supports.supports_revision(conn),
        }

        # Last section in the dict, so the completeness digest below covers it
        # along with everything above. Raises where the sections above degrade:
        # see _format_artifact.
        _format_artifact(_backup_verdict_chain, conn, data)
        _format_artifact(_backup_grounding_attestations, conn, data)
        _format_artifact(_backup_schema_census, conn, data)

        # Rotate the previous backup aside before overwriting it. graph.db is
        # authoritative, so the threat this addresses is not a torn write (the
        # atomic replace below already rules that out) but the loss of the only
        # recovery copy: a bug in the serialisation above, or a graph.db and
        # claims.toml lost together, leaves nothing to restore from. Keeping one
        # generation behind means a bad rewrite still has a recovery point behind
        # it. Constant work relative to the backup already being written (one
        # file copy, no per-row signature verification), and the .prev write is
        # atomic too, so a crash mid-rotation cannot corrupt either file.
        toml_path = root / "claims.toml"
        try:
            prior = toml_path.read_bytes()
        except OSError:
            prior = None
        if prior is not None:
            atomic_write_bytes(root / "claims.toml.prev", prior)

        # Serialize once. The completeness table is appended as text rather than
        # added to the dict and dumped with it, so the digest can cover the body
        # bytes without a second pass over every claim.
        body = tomli_w.dumps(data)
        tail = _format_artifact(_backup_completeness_tail, conn, data, body)

        # Atomic write: a crash during the rewrite must not destroy the sole DR
        # artifact on the exact crash class it exists for. A failure anywhere
        # before the rename leaves the previous good claims.toml untouched,
        # never truncated or empty.
        atomic_write_bytes(
            toml_path, (body + tail).encode("utf-8"),
        )

    except FormatArtifactError:
        # The one failure this writer does not absorb. Printing it would leave a
        # claims.toml with no completeness section, which reads the same as a
        # file written before the section existed. See FormatArtifactError.
        raise
    except Exception as exc:  # noqa: BLE001
        import sys
        # stderr at an ERROR-line prefix is harder for production to
        # silently swallow than warnings.warn (which downstream code
        # routinely filters out). graph.db remains authoritative;
        # this line surfaces the divergence so an operator notices.
        print(
            f"ERROR: claims.toml backup failed; graph.db is "
            f"authoritative, {exc}",
            file=sys.stderr,
        )
