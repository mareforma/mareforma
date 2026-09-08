"""tests/test_state_machine.py, DB-layer state-machine + prev_hash chain.

Covers:
  - SQLite triggers reject illegal state transitions with translated
    `IllegalStateTransitionError`
  - CHECK constraint enforces validation_signature on ESTABLISHED rows
  - ``prev_hash`` chain is built linearly across claims
  - ``prev_hash`` UNIQUE catches branched chains
  - Status-only edits on signed claims still work (status transition
    legal on a signed row)
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

import mareforma
from mareforma import db as _db
from mareforma.db import (
    IllegalStateTransitionError,
    SignedClaimImmutableError,
    _MANAGED_TRIGGERS,
    _SIGNED_FIELDS_TRIGGER_SQL,
    add_claim,
    open_db,
    update_claim,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_UPDATE_OF_RE = re.compile(r"UPDATE\s+OF\s+(.+?)\s+ON\s", re.IGNORECASE | re.DOTALL)
_UPDATE_ALL_RE = re.compile(
    r"(?:BEFORE|AFTER)\s+UPDATE\s+ON\s+(\w+)", re.IGNORECASE)
_DELETE_RE = re.compile(
    r"(?:BEFORE|AFTER)\s+DELETE\s+ON\s+(\w+)", re.IGNORECASE)
_INSERT_RE = re.compile(
    r"(?:BEFORE|AFTER)\s+INSERT\s+ON\s+(\w+)", re.IGNORECASE)
_UPDATE_OF_TABLE_RE = re.compile(
    r"UPDATE\s+OF\s+.+?\s+ON\s+(\w+)", re.IGNORECASE | re.DOTALL
)


def _watched_columns(trigger_sql: str) -> list[str]:
    """The columns a ``BEFORE UPDATE OF ...`` trigger fires on."""
    match = _UPDATE_OF_RE.search(trigger_sql)
    assert match is not None, trigger_sql
    return [col.strip() for col in match.group(1).split(",")]


def _noop_dml_for_trigger(conn, trigger_sql: str) -> list[str]:
    """No-op DML statements that attach a managed trigger's subprogram.

    SQLite compiles a trigger's body when it compiles a DML statement on the
    trigger's table and event, so exercising each managed trigger means running
    the matching statement in a form that touches no row. ``WHERE 0`` does that
    for UPDATE and DELETE; an INSERT drawing from ``SELECT ... WHERE 0`` does it
    for INSERT.

    Every event shape in the schema, because the reconciled set is now every
    trigger rather than the seventeen with authored text: the claims
    state-machine checks fire BEFORE INSERT, and the FTS sync triggers fire
    AFTER all three.
    """
    of_table = _UPDATE_OF_TABLE_RE.search(trigger_sql)
    if of_table is not None:
        table = of_table.group(1)
        return [
            f"UPDATE {table} SET {col} = {col} WHERE 0"
            for col in _watched_columns(trigger_sql)
        ]
    update_all = _UPDATE_ALL_RE.search(trigger_sql)
    if update_all is not None:
        table = update_all.group(1)
        col = _any_column(conn, table)
        return [f"UPDATE {table} SET {col} = {col} WHERE 0"]
    delete = _DELETE_RE.search(trigger_sql)
    if delete is not None:
        return [f"DELETE FROM {delete.group(1)} WHERE 0"]
    insert = _INSERT_RE.search(trigger_sql)
    assert insert is not None, trigger_sql
    table = insert.group(1)
    return [f"INSERT INTO {table} SELECT * FROM {table} WHERE 0"]


def _any_column(conn, table: str) -> str:
    """One column name of *table*, for a whole-table no-op UPDATE.

    Read off the live schema rather than from a table kept here by hand. The
    hand-kept version had six entries and covered the six tables that happened
    to be reconciled at the time, so adding a guard elsewhere failed with a
    KeyError in the helper rather than a verdict about the guard.
    """
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
    assert cols, f"no such table: {table}"
    return cols[0]


# ---------------------------------------------------------------------------
# State-transition triggers
# ---------------------------------------------------------------------------


class TestStatusEditsOnSignedRows:
    """What survives the state machine the ladder used to drive.

    The insert and update triggers checked one thing each: that a level was
    a legal one to be born at, and that a change from one to another was a
    step the machine allowed. There are no levels, so both went. Editing a
    signed row's status was never about levels and still holds.
    """

    def test_status_only_edit_on_signed_claim_allowed(
        self, tmp_path: Path,
    ) -> None:
        """A status-only edit must pass even on a
        signed (and otherwise immutable) claim."""
        from mareforma import signing as _sig
        if not (tmp_path / "k").exists():
            _sig.bootstrap_key(tmp_path / "k")
        with mareforma.open(tmp_path, key_path=tmp_path / "k") as g:
            cid = g.assert_claim("retract me", generated_by="agent")
            update_claim(g._conn, tmp_path, cid, status="retracted")
            assert g.get_claim(cid)["status"] == "retracted"


# ---------------------------------------------------------------------------
# CHECK constraint
# ---------------------------------------------------------------------------


class TestAValidationNobodySigned:
    """The CHECK that outlived the ladder, from the other side.

    It used to say an ESTABLISHED row must carry a validation envelope, which
    was a claim about a level. The claim underneath had nothing to do with
    levels: a row cannot say a human validated it without the envelope that
    proves one did. ``validated_by`` and ``validated_at`` are display fields
    denormalised out of the signed payload, so either of them standing alone is
    a row asserting a validation nobody signed.
    """

    def _validated_claim(self, tmp_path: Path) -> tuple[Path, str]:
        from mareforma import signing as _sig

        gen_key = tmp_path / "gen.key"
        val_key = tmp_path / "val.key"
        _sig.bootstrap_key(gen_key)
        _sig.bootstrap_key(val_key)
        val_pem = _sig.public_key_to_pem(
            _sig.load_private_key(val_key).public_key(),
        )
        with mareforma.open(tmp_path, key_path=gen_key) as g:
            claim_id = g.assert_claim("a finding", generated_by="A")
            g.enroll_validator(val_pem, identity="v")
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.validate(claim_id)
        return val_key, claim_id

    def test_the_envelope_cannot_be_cleared_off_a_validated_row(
        self, tmp_path: Path,
    ) -> None:
        val_key, claim_id = self._validated_claim(tmp_path)
        with mareforma.open(tmp_path, key_path=val_key) as g:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
                g._conn.execute(
                    "UPDATE claims SET validation_signature = NULL "
                    "WHERE claim_id = ?",
                    (claim_id,),
                )

    @pytest.mark.parametrize("column", ["validated_by", "validated_at"])
    def test_a_display_field_cannot_stand_without_the_envelope(
        self, tmp_path: Path, column: str,
    ) -> None:
        """The direction the ladder's version never covered.

        The old CHECK asked whether a promoted row had an envelope. It never
        asked the reverse, because a row could not carry a validator's name
        without having been promoted to carry it. Nothing enforces that now
        except this.
        """
        with mareforma.open(tmp_path) as g:
            claim_id = g.assert_claim("a finding", generated_by="A")
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
                g._conn.execute(
                    f"UPDATE claims SET {column} = ? WHERE claim_id = ?",
                    ("someone who never signed", claim_id),
                )


# ---------------------------------------------------------------------------
# Append-only prev_hash chain
# ---------------------------------------------------------------------------


class TestPrevHashChain:
    def test_chain_populated_on_every_claim(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as g:
            g.assert_claim("claim a")
            g.assert_claim("claim b")
            g.assert_claim("claim c")
        conn = open_db(tmp_path)
        try:
            rows = conn.execute(
                "SELECT claim_id, prev_hash FROM claims ORDER BY rowid"
            ).fetchall()
        finally:
            conn.close()
        prevs = [r["prev_hash"] for r in rows]
        assert all(p is not None for p in prevs)
        assert len(set(prevs)) == 3  # all distinct

    def test_chain_is_linear_and_verifiable(self, tmp_path: Path) -> None:
        """Recompute the chain locally and verify each row matches.

        After Statement v1, chain_input includes the EvidenceVector so
        the row's stored evidence_json must be threaded through too.
        """
        with mareforma.open(tmp_path) as g:
            [g.assert_claim(f"claim {i}") for i in range(5)]
        conn = open_db(tmp_path)
        try:
            rows = conn.execute(
                "SELECT * FROM claims ORDER BY rowid"
            ).fetchall()
        finally:
            conn.close()
        prev = b""
        for row in rows:
            evidence_dict = json.loads(row["evidence_json"] or "{}")
            chain_input = _db._chain_input_for_claim({
                "claim_id": row["claim_id"],
                "text": row["text"],
                "classification": row["classification"],
                "generated_by": row["generated_by"],
                "supports": json.loads(row["supports_json"] or "[]"),
                "contradicts": json.loads(row["contradicts_json"] or "[]"),
                "source_name": row["source_name"],
                "artifact_hash": row["artifact_hash"],
                "created_at": row["created_at"],
            }, evidence_dict)
            expected = hashlib.sha256(prev + chain_input).hexdigest()
            assert row["prev_hash"] == expected
            prev = expected.encode("ascii")

    def test_prev_hash_unique_catches_duplicate(self, tmp_path: Path) -> None:
        """A manual INSERT that re-uses an existing prev_hash hits the
        UNIQUE index. UNIQUE is the backstop to BEGIN IMMEDIATE, if
        someone bypasses the Python write path, the index catches them."""
        with mareforma.open(tmp_path) as g:
            cid = g.assert_claim("first")
        conn = open_db(tmp_path)
        try:
            existing = conn.execute(
                "SELECT prev_hash FROM claims WHERE claim_id = ?", (cid,)
            ).fetchone()["prev_hash"]
            with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
                conn.execute(
                    """
                    INSERT INTO claims
                        (claim_id, text, classification,
                         status, generated_by, supports_json, contradicts_json,
                         prev_hash, created_at, updated_at)
                    VALUES (?, ?, 'INFERRED', 'open', 'agent',
                            '[]', '[]', ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        "duplicate prev_hash",
                        existing,
                        _now_iso(),
                        _now_iso(),
                    ),
                )
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Triggers don't fire on status-only edits
# ---------------------------------------------------------------------------


class TestStatusOnlyEditsBypassTrigger:
    def test_retraction_of_replicated_claim(self, tmp_path: Path) -> None:
        """A REPLICATED claim's status can be set to retracted without
        a state-machine trigger firing."""
        from mareforma import signing as _sig
        from tests._helpers import _two_signers
        key = tmp_path / "k"
        _sig.bootstrap_key(key)
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            up = g.assert_claim("up", generated_by="seed")
            a = g.assert_claim("a", supports=[up], generated_by="A", signer=sa)
            g.assert_claim("b", supports=[up], generated_by="B", signer=sb)
            update_claim(g._conn, tmp_path, a, status="retracted")
            row = g.get_claim(a)
            assert row["status"] == "retracted"


# ---------------------------------------------------------------------------
# Append-only signed-fields trigger
# ---------------------------------------------------------------------------


class TestSignedFieldsAppendOnly:
    """claims_signed_fields_no_laundering refuses direct-SQL mutation
    of any signed predicate column on a signed claim. The envelope is
    the canonical source; the row must always match what was signed.
    """

    def _signed_claim(self, tmp_path: Path) -> tuple[str, "object"]:
        from mareforma import signing as _sig
        key_path = tmp_path / "key"
        _sig.bootstrap_key(key_path)
        g = mareforma.open(tmp_path, key_path=key_path)
        cid = g.assert_claim("anchor", artifact_hash="a" * 64)
        return cid, g

    def test_direct_text_update_blocked(self, tmp_path: Path) -> None:
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(sqlite3.IntegrityError, match="signed_field_locked"):
                g._conn.execute(
                    "UPDATE claims SET text = ? WHERE claim_id = ?",
                    ("tampered", cid),
                )
        finally:
            g.close()

    def test_direct_evidence_update_blocked(self, tmp_path: Path) -> None:
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(sqlite3.IntegrityError, match="signed_field_locked"):
                g._conn.execute(
                    "UPDATE claims SET ev_risk_of_bias = -1 WHERE claim_id = ?",
                    (cid,),
                )
        finally:
            g.close()

    def test_direct_statement_cid_update_blocked(self, tmp_path: Path) -> None:
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(sqlite3.IntegrityError, match="signed_field_locked"):
                g._conn.execute(
                    "UPDATE claims SET statement_cid = ? WHERE claim_id = ?",
                    ("0" * 64, cid),
                )
        finally:
            g.close()

    def test_unsigned_row_allows_text_update(self, tmp_path: Path) -> None:
        """Unsigned claims (no key configured) are not under append-only
        protection, the trigger gates on OLD.signature_bundle IS NOT NULL."""
        with mareforma.open(tmp_path) as g:
            cid = g.assert_claim("draft")
            # No signature → trigger does not fire.
            g._conn.execute(
                "UPDATE claims SET text = ? WHERE claim_id = ?",
                ("revised", cid),
            )
            g._conn.commit()

    def test_de_signing_update_blocked(self, tmp_path: Path) -> None:
        """Nulling signature_bundle on a signed row would disarm both the
        laundering trigger and claims_signed_no_delete, so the trigger
        watches its own guard column."""
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(sqlite3.IntegrityError, match="signed_field_locked"):
                g._conn.execute(
                    "UPDATE claims SET signature_bundle = NULL WHERE claim_id = ?",
                    (cid,),
                )
        finally:
            g.close()

    def test_asserter_keyid_update_blocked(self, tmp_path: Path) -> None:
        """asserter_keyid is the independence axis of REPLICATED. It is a
        denormalisation of the bundle's signer, so the row may not contradict
        the envelope it was derived from."""
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(sqlite3.IntegrityError, match="signed_field_locked"):
                g._conn.execute(
                    "UPDATE claims SET asserter_keyid = ? WHERE claim_id = ?",
                    ("0123456789abcdef", cid),
                )
        finally:
            g.close()

    def test_bundle_rewrite_still_allowed(self, tmp_path: Path) -> None:
        """Rekor inclusion-proof attachment rewrites signature_bundle in
        place. Non-NULL to non-NULL stays legal."""
        cid, g = self._signed_claim(tmp_path)
        try:
            bundle = g._conn.execute(
                "SELECT signature_bundle FROM claims WHERE claim_id = ?", (cid,),
            ).fetchone()[0]
            rewritten = json.dumps({**json.loads(bundle), "rekor": {"logIndex": 1}})
            g._conn.execute(
                "UPDATE claims SET signature_bundle = ? WHERE claim_id = ?",
                (rewritten, cid),
            )
            g._conn.commit()
            after = g._conn.execute(
                "SELECT signature_bundle FROM claims WHERE claim_id = ?", (cid,),
            ).fetchone()[0]
            assert json.loads(after)["rekor"] == {"logIndex": 1}
        finally:
            g.close()

    def test_delete_still_refused_after_attempted_de_signing(
        self, tmp_path: Path,
    ) -> None:
        """The de-signing UPDATE is the disarm step of the delete attack:
        once it is refused, claims_signed_no_delete stays armed."""
        from mareforma.db import delete_claim as _delete
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(sqlite3.IntegrityError):
                g._conn.execute(
                    "UPDATE claims SET signature_bundle = NULL WHERE claim_id = ?",
                    (cid,),
                )
            with pytest.raises(SignedClaimImmutableError, match="cannot be deleted"):
                _delete(g._conn, tmp_path, cid)
            assert g.get_claim(cid) is not None
        finally:
            g.close()

    def test_status_only_update_passes_on_signed_row(
        self, tmp_path: Path,
    ) -> None:
        """update_claim writes the full SET clause (text/supports/etc.)
        but with unchanged values when only status is being changed.
        The trigger's value-comparison clause lets this pass."""
        cid, g = self._signed_claim(tmp_path)
        try:
            update_claim(g._conn, tmp_path, cid, status="retracted")
            assert g.get_claim(cid)["status"] == "retracted"
        finally:
            g.close()

    def _trigger_sql(self, conn: sqlite3.Connection) -> "str | None":
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
            "AND name = 'claims_signed_fields_no_laundering'",
        ).fetchone()
        return None if row is None else row[0]

    def test_open_on_a_current_graph_leaves_the_trigger_untouched(
        self, tmp_path: Path,
    ) -> None:
        """Dropping and recreating the guard on every open would let any
        other connection write a signed row while it is absent. On a graph
        whose trigger already matches, open() must not write at all."""
        cid, g = self._signed_claim(tmp_path)
        g.close()
        observer = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        try:
            before_sql = self._trigger_sql(observer)
            before_version = observer.execute("PRAGMA data_version").fetchone()[0]
            open_db(tmp_path).close()
            assert self._trigger_sql(observer) == before_sql
            assert (
                observer.execute("PRAGMA data_version").fetchone()[0]
                == before_version
            )
            with pytest.raises(sqlite3.IntegrityError, match="signed_field_locked"):
                observer.execute(
                    "UPDATE claims SET text = ? WHERE claim_id = ?",
                    ("laundered", cid),
                )
        finally:
            observer.close()

    def test_open_rewrites_a_trigger_whose_definition_drifted(
        self, tmp_path: Path,
    ) -> None:
        """A graph written by an older release carries a narrower watch
        list. The rewrite path still has to reach it."""
        cid, g = self._signed_claim(tmp_path)
        g._conn.executescript(
            """
            DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering;
            CREATE TRIGGER claims_signed_fields_no_laundering
            BEFORE UPDATE OF text ON claims
            WHEN OLD.signature_bundle IS NOT NULL AND OLD.text IS NOT NEW.text
            BEGIN
                SELECT RAISE(ABORT, 'mareforma:append_only:signed_field_locked');
            END;
            """
        )
        g.close()
        with open_db(tmp_path) as conn:
            assert self._trigger_sql(conn) == _SIGNED_FIELDS_TRIGGER_SQL
            with pytest.raises(sqlite3.IntegrityError, match="signed_field_locked"):
                conn.execute(
                    "UPDATE claims SET asserter_keyid = ? WHERE claim_id = ?",
                    ("0123456789abcdef", cid),
                )


# ---------------------------------------------------------------------------
# Promotion of a signed row goes through the promotion paths
# ---------------------------------------------------------------------------


class TestSignedDeleteAppendOnly:
    """claims_signed_no_delete refuses DELETE on a signed claim.

    Without this trigger, a process with DB access could wipe a Rekor-
    logged ESTABLISHED claim, _backup_claims_toml would rewrite the
    TOML as if the claim never existed, and the entire "append-only
    over the signed predicate" framing would be half-implemented
    (UPDATE-of-signed-fields was already locked; DELETE was not).
    Unsigned claims remain deletable, they carry no cryptographic
    commitment and the trust ladder does not extend to them.
    """

    def _signed_claim(self, tmp_path: Path) -> tuple[str, "object"]:
        from mareforma import signing as _sig
        key_path = tmp_path / "key"
        _sig.bootstrap_key(key_path)
        g = mareforma.open(tmp_path, key_path=key_path)
        cid = g.assert_claim("signed anchor", artifact_hash="a" * 64)
        return cid, g

    def test_direct_delete_of_signed_claim_blocked(
        self, tmp_path: Path,
    ) -> None:
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(
                sqlite3.IntegrityError, match="signed_claim_no_delete",
            ):
                g._conn.execute("DELETE FROM claims WHERE claim_id = ?", (cid,))
        finally:
            g.close()

    def test_delete_claim_helper_blocked_on_signed_row(
        self, tmp_path: Path,
    ) -> None:
        """The user-facing ``db.delete_claim`` helper must surface the
        trigger's refusal as the documented typed error, not a raw
        sqlite3.IntegrityError a public-API caller cannot reasonably
        catch."""
        from mareforma.db import delete_claim as _delete
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(SignedClaimImmutableError, match="cannot be deleted"):
                _delete(g._conn, tmp_path, cid)
            # The row survives the refused delete.
            assert g.get_claim(cid) is not None
        finally:
            g.close()

    def test_unsigned_claim_remains_deletable(self, tmp_path: Path) -> None:
        """Unsigned mode (no key, no signature_bundle) is not under
        append-only protection. The trigger gates on
        OLD.signature_bundle IS NOT NULL, unsigned rows pass through."""
        from mareforma.db import delete_claim as _delete
        with mareforma.open(tmp_path) as g:
            cid = g.assert_claim("draft unsigned")
            assert g.get_claim(cid) is not None
            _delete(g._conn, tmp_path, cid)
            assert g.get_claim(cid) is None

    def test_delete_claims_by_generated_by_blocked_on_signed_rows(
        self, tmp_path: Path,
    ) -> None:
        """The bulk-delete helper must also refuse when any matched row
        is signed. Without this gate, an adversary could wipe an entire
        agent's signed history by ``delete_claims_by_generated_by``."""
        from mareforma.db import delete_claims_by_generated_by as _bulk
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(
                SignedClaimImmutableError, match="cannot be deleted",
            ):
                _bulk(g._conn, tmp_path, generated_by=g.get_claim(cid)["generated_by"])
            # Row still present after the failed bulk delete.
            assert g.get_claim(cid) is not None
        finally:
            g.close()

    def test_refused_delete_leaves_no_open_transaction(
        self, tmp_path: Path,
    ) -> None:
        """The refusal must release the transaction it opened.

        RAISE(ABORT) backs the statement out but leaves the transaction open,
        and every write helper reads ``conn.in_transaction`` to decide who owns
        the commit. On a poisoned connection they all skip BEGIN IMMEDIATE, the
        commit and the claims.toml backup, so the documented "catch the typed
        error and keep writing" path silently discards every later claim.
        """
        from mareforma.db import delete_claim as _delete
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(SignedClaimImmutableError):
                _delete(g._conn, tmp_path, cid)
            assert g._conn.in_transaction is False
            later = g.assert_claim("a finding asserted after the refusal")
            other = open_db(tmp_path)
            try:
                assert _db.get_claim(other, later) is not None
            finally:
                other.close()
            assert later in (tmp_path / "claims.toml").read_text()
        finally:
            g.close()

    def test_refused_bulk_delete_leaves_no_open_transaction(
        self, tmp_path: Path,
    ) -> None:
        """The bulk helper has the same hole and the same contract."""
        from mareforma.db import delete_claims_by_generated_by as _bulk
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(SignedClaimImmutableError):
                _bulk(g._conn, tmp_path, generated_by=g.get_claim(cid)["generated_by"])
            assert g._conn.in_transaction is False
        finally:
            g.close()
