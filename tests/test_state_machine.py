"""tests/test_state_machine.py, DB-layer state-machine + prev_hash chain.

Covers:
  - SQLite triggers reject illegal state transitions with translated
    `IllegalStateTransitionError`
  - CHECK constraint enforces validation_signature on ESTABLISHED rows
  - ``prev_hash`` chain is built linearly across claims
  - ``prev_hash`` UNIQUE catches branched chains
  - Status-only edits on signed claims still work (status transition
    legal without support_level change)
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
_UPDATE_ALL_RE = re.compile(r"BEFORE\s+UPDATE\s+ON\s+(\w+)", re.IGNORECASE)
_DELETE_RE = re.compile(r"BEFORE\s+DELETE\s+ON\s+(\w+)", re.IGNORECASE)
_UPDATE_OF_TABLE_RE = re.compile(
    r"UPDATE\s+OF\s+.+?\s+ON\s+(\w+)", re.IGNORECASE | re.DOTALL
)


def _watched_columns(trigger_sql: str) -> list[str]:
    """The columns a ``BEFORE UPDATE OF ...`` trigger fires on."""
    match = _UPDATE_OF_RE.search(trigger_sql)
    assert match is not None, trigger_sql
    return [col.strip() for col in match.group(1).split(",")]


def _noop_dml_for_trigger(trigger_sql: str) -> list[str]:
    """No-op DML statements that attach a managed trigger's subprogram.

    SQLite compiles a trigger's body when it compiles a DML statement on the
    trigger's table and event, so exercising each managed trigger means running
    the matching statement with ``WHERE 0`` (no row touched, the body still
    compiles). Covers the three managed-trigger shapes: ``BEFORE UPDATE OF
    <cols>`` (one statement per watched column), a whole-table ``BEFORE UPDATE``
    (append-only guards), and ``BEFORE DELETE`` (no-delete guards).
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
        col = _any_column(table)
        return [f"UPDATE {table} SET {col} = {col} WHERE 0"]
    delete = _DELETE_RE.search(trigger_sql)
    assert delete is not None, trigger_sql
    return [f"DELETE FROM {delete.group(1)} WHERE 0"]


def _any_column(table: str) -> str:
    """One column name of *table*, for a whole-table no-op UPDATE."""
    return {
        "findings": "content_id",
        "evidence_lines": "data_id",
    }[table]


# ---------------------------------------------------------------------------
# State-transition triggers
# ---------------------------------------------------------------------------


class TestInsertTrigger:
    def test_preliminary_insert_allowed(self, tmp_path: Path) -> None:
        # The standard add_claim path, sanity check the trigger doesn't
        # reject the legal case.
        conn = open_db(tmp_path)
        try:
            cid = add_claim(conn, tmp_path, "ok", generated_by="agent")
            row = conn.execute(
                "SELECT support_level FROM claims WHERE claim_id = ?", (cid,)
            ).fetchone()
            assert row["support_level"] == "PRELIMINARY"
        finally:
            conn.close()

    def test_direct_established_without_validation_rejected(
        self, tmp_path: Path,
    ) -> None:
        conn = open_db(tmp_path)
        try:
            with pytest.raises(sqlite3.IntegrityError, match="established_without_validation"):
                conn.execute(
                    """
                    INSERT INTO claims
                        (claim_id, text, classification, support_level,
                         status, generated_by, supports_json, contradicts_json,
                         created_at, updated_at)
                    VALUES (?, ?, 'INFERRED', 'ESTABLISHED', 'open', 'agent',
                            '[]', '[]', ?, ?)
                    """,
                    (str(uuid.uuid4()), "rogue ESTABLISHED", _now_iso(), _now_iso()),
                )
        finally:
            conn.close()

    def test_preliminary_with_validation_rejected(self, tmp_path: Path) -> None:
        """A PRELIMINARY row that carries validated_by is incoherent, reject."""
        conn = open_db(tmp_path)
        try:
            with pytest.raises(sqlite3.IntegrityError, match="preliminary_with_validation"):
                conn.execute(
                    """
                    INSERT INTO claims
                        (claim_id, text, classification, support_level,
                         status, generated_by, validated_by, supports_json,
                         contradicts_json, created_at, updated_at)
                    VALUES (?, ?, 'INFERRED', 'PRELIMINARY', 'open', 'agent',
                            'someone@lab', '[]', '[]', ?, ?)
                    """,
                    (str(uuid.uuid4()), "weird", _now_iso(), _now_iso()),
                )
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# UPDATE trigger, transitions
# ---------------------------------------------------------------------------


class TestUpdateTrigger:
    def test_preliminary_to_replicated_allowed(self, tmp_path: Path) -> None:
        # The auto-promotion path that _maybe_update_replicated takes.
        from mareforma import signing as _sig
        from tests._helpers import _two_signers
        key = tmp_path / "k"
        _sig.bootstrap_key(key)
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            upstream = g.assert_claim("upstream", generated_by="seed", seed=True)
            a = g.assert_claim("a", supports=[upstream], generated_by="A", signer=sa)
            b = g.assert_claim("b", supports=[upstream], generated_by="B", signer=sb)
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"

    def test_preliminary_directly_to_established_rejected(
        self, tmp_path: Path,
    ) -> None:
        """Raw UPDATE attempting PRELIMINARY → ESTABLISHED hits the trigger.

        ``validate_claim`` has a Python-layer guard that blocks this
        path with a ValueError before reaching the DB. We bypass it
        here to exercise the trigger directly, which is the actual
        defense-in-depth layer the DB trigger provides."""
        conn = open_db(tmp_path)
        try:
            cid = add_claim(conn, tmp_path, "x", generated_by="agent")
            with pytest.raises(sqlite3.IntegrityError, match="illegal_transition:from_preliminary"):
                conn.execute(
                    "UPDATE claims SET support_level = 'ESTABLISHED', "
                    "validation_signature = ? WHERE claim_id = ?",
                    ('{"sig":"x"}', cid),
                )
        finally:
            conn.close()

    def test_established_downgrade_rejected(self, tmp_path: Path) -> None:
        from mareforma import signing as _sig
        from tests._helpers import _two_signers
        gen_key = tmp_path / "gen.key"
        val_key = tmp_path / "val.key"
        if not gen_key.exists():
            _sig.bootstrap_key(gen_key)
        if not val_key.exists():
            _sig.bootstrap_key(val_key)
        val_pem = _sig.public_key_to_pem(
            _sig.load_private_key(val_key).public_key(),
        )
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=gen_key) as g:
            upstream = g.assert_claim("upstream", generated_by="seed", seed=True)
            id_a = g.assert_claim("a", supports=[upstream], generated_by="A", signer=sa)
            g.assert_claim("b", supports=[upstream], generated_by="B", signer=sb)
            g.enroll_validator(val_pem, identity="v")
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.validate(id_a)
            # Now id_a is ESTABLISHED. Attempt a direct UPDATE to PRELIMINARY.
            conn = g._conn
            with pytest.raises(IllegalStateTransitionError, match="from_established"):
                try:
                    conn.execute(
                        "UPDATE claims SET support_level = 'PRELIMINARY' "
                        "WHERE claim_id = ?",
                        (id_a,),
                    )
                except sqlite3.IntegrityError as exc:
                    translated = _db._state_error_from_integrity(exc)
                    if translated is not None:
                        raise translated from exc
                    raise

    def test_status_only_edit_on_signed_claim_allowed(
        self, tmp_path: Path,
    ) -> None:
        """The trigger fires on UPDATE OF support_level. A status-only edit
        does NOT change support_level and must therefore pass even on a
        signed (and otherwise immutable) claim."""
        from mareforma import signing as _sig
        if not (tmp_path / "k").exists():
            _sig.bootstrap_key(tmp_path / "k")
        with mareforma.open(tmp_path, key_path=tmp_path / "k") as g:
            cid = g.assert_claim("retract me", generated_by="agent")
            update_claim(g._conn, tmp_path, cid, status="retracted")
            assert g.get_claim(cid)["status"] == "retracted"
            assert g.get_claim(cid)["support_level"] == "PRELIMINARY"


# ---------------------------------------------------------------------------
# CHECK constraint
# ---------------------------------------------------------------------------


class TestCheckConstraint:
    def test_check_blocks_established_with_null_validation_signature(
        self, tmp_path: Path,
    ) -> None:
        """The CHECK is the row-level belt to the trigger's transition-level
        suspenders. A direct UPDATE that tries to NULL validation_signature
        on an ESTABLISHED row violates CHECK."""
        from mareforma import signing as _sig
        gen_key = tmp_path / "gen.key"
        val_key = tmp_path / "val.key"
        if not gen_key.exists():
            _sig.bootstrap_key(gen_key)
        if not val_key.exists():
            _sig.bootstrap_key(val_key)
        val_pem = _sig.public_key_to_pem(
            _sig.load_private_key(val_key).public_key(),
        )
        from tests._helpers import _two_signers
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=gen_key) as g:
            upstream = g.assert_claim("upstream", generated_by="seed", seed=True)
            id_a = g.assert_claim("a", supports=[upstream], generated_by="A", signer=sa)
            g.assert_claim("b", supports=[upstream], generated_by="B", signer=sb)
            g.enroll_validator(val_pem, identity="v")
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.validate(id_a)
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
                g._conn.execute(
                    "UPDATE claims SET validation_signature = NULL "
                    "WHERE claim_id = ?",
                    (id_a,),
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
                        (claim_id, text, classification, support_level,
                         status, generated_by, supports_json, contradicts_json,
                         prev_hash, created_at, updated_at)
                    VALUES (?, ?, 'INFERRED', 'PRELIMINARY', 'open', 'agent',
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
        the state-machine trigger firing (it fires on OF support_level)."""
        from mareforma import signing as _sig
        from tests._helpers import _two_signers
        key = tmp_path / "k"
        _sig.bootstrap_key(key)
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            up = g.assert_claim("up", generated_by="seed", seed=True)
            a = g.assert_claim("a", supports=[up], generated_by="A", signer=sa)
            g.assert_claim("b", supports=[up], generated_by="B", signer=sb)
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            update_claim(g._conn, tmp_path, a, status="retracted")
            row = g.get_claim(a)
            assert row["status"] == "retracted"
            assert row["support_level"] == "REPLICATED"


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


class TestSignedPromotionBacked:
    """claims_signed_promotion_backed refuses a raw promotion of a signed row.

    ``support_level`` is the trust ladder and it is not a signed field, so
    without this trigger one ``UPDATE claims SET support_level='REPLICATED'``
    lifts a lone claim a rung. The transition is legal to the state machine, so
    the guard is the promotion marker: only the library's promotion paths open
    it, and a statement from anywhere else is refused.
    """

    def _signed_claim(self, tmp_path: Path) -> tuple[str, "object"]:
        from mareforma import signing as _sig
        key_path = tmp_path / "key"
        _sig.bootstrap_key(key_path)
        g = mareforma.open(tmp_path, key_path=key_path)
        return g.assert_claim("signed anchor"), g

    def test_direct_promotion_of_signed_row_refused(self, tmp_path: Path) -> None:
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(sqlite3.IntegrityError, match="promotion_unmarked"):
                g._conn.execute(
                    "UPDATE claims SET support_level = 'REPLICATED' "
                    "WHERE claim_id = ?",
                    (cid,),
                )
            assert g.get_claim(cid)["support_level"] == "PRELIMINARY"
        finally:
            g.close()

    def test_direct_promotion_from_a_foreign_connection_refused(
        self, tmp_path: Path,
    ) -> None:
        """A co-resident process opens graph.db with plain sqlite3. It never
        opens the marker, so the promotion is refused by the trigger's own
        message rather than by a name the connection cannot resolve."""
        cid, g = self._signed_claim(tmp_path)
        g.close()
        observer = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        try:
            with pytest.raises(sqlite3.IntegrityError, match="promotion_unmarked"):
                observer.execute(
                    "UPDATE claims SET support_level = 'REPLICATED' "
                    "WHERE claim_id = ?",
                    (cid,),
                )
        finally:
            observer.close()
        with open_db(tmp_path) as conn:
            row = conn.execute(
                "SELECT support_level FROM claims WHERE claim_id = ?", (cid,),
            ).fetchone()
            assert row["support_level"] == "PRELIMINARY"

    def test_managed_triggers_compile_on_an_unregistered_connection(
        self, tmp_path: Path,
    ) -> None:
        """Trigger text is durable schema, so every connection that opens the
        file has to be able to compile it, including an older release of
        mareforma and any co-resident reader. A name only this release puts on
        its connections (a per-connection SQL function) breaks that: SQLite
        resolves it when it compiles the statement, so the whole watched column
        becomes unwritable rather than the guarded transition being refused.

        ``WHERE 0`` matches no row, so nothing here depends on the trigger
        firing; the statement still has to compile with the trigger's
        subprogram attached.
        """
        self._signed_claim(tmp_path)[1].close()
        observer = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        try:
            for _, sql in _MANAGED_TRIGGERS:
                for statement in _noop_dml_for_trigger(sql):
                    observer.execute(statement)
        finally:
            observer.close()

    def test_non_promoting_level_write_from_a_foreign_connection_passes(
        self, tmp_path: Path,
    ) -> None:
        """The guard covers two transitions, not the column. A write that
        leaves the level where it is has no rung to steal and must go
        through, whoever holds the connection."""
        cid, g = self._signed_claim(tmp_path)
        g.close()
        observer = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        try:
            observer.execute(
                "UPDATE claims SET support_level = 'PRELIMINARY' "
                "WHERE claim_id = ?",
                (cid,),
            )
            observer.commit()
        finally:
            observer.close()

    def test_an_open_window_does_not_reach_another_connection(
        self, tmp_path: Path,
    ) -> None:
        """The marker is what stands between a stray UPDATE and the trust
        ladder, so it has to be state one connection cannot read off another.
        A marker kept in the graph itself would hand every co-resident writer
        the window this one opened."""
        from mareforma.db import _promotion_window
        cid, g = self._signed_claim(tmp_path)
        observer = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        try:
            with _promotion_window(g._conn):
                with pytest.raises(
                    sqlite3.IntegrityError, match="promotion_unmarked",
                ):
                    observer.execute(
                        "UPDATE claims SET support_level = 'REPLICATED' "
                        "WHERE claim_id = ?",
                        (cid,),
                    )
        finally:
            observer.close()
            g.close()

    def test_the_window_closes_when_the_block_raises(
        self, tmp_path: Path,
    ) -> None:
        """A window left open by a failed promotion would leave the connection
        promoting freely for the rest of its life."""
        from mareforma.db import _promotion_window
        cid, g = self._signed_claim(tmp_path)
        try:
            with pytest.raises(RuntimeError):
                with _promotion_window(g._conn):
                    raise RuntimeError("promotion path blew up")
            with pytest.raises(sqlite3.IntegrityError, match="promotion_unmarked"):
                g._conn.execute(
                    "UPDATE claims SET support_level = 'REPLICATED' "
                    "WHERE claim_id = ?",
                    (cid,),
                )
        finally:
            g.close()

    def test_unsigned_row_promotion_passes(self, tmp_path: Path) -> None:
        """The trigger gates on OLD.signature_bundle IS NOT NULL, like the
        laundering guard: an unsigned row carries no commitment to defend."""
        conn = open_db(tmp_path)
        try:
            cid = add_claim(conn, tmp_path, "draft", generated_by="agent")
            conn.execute(
                "UPDATE claims SET support_level = 'REPLICATED' "
                "WHERE claim_id = ?",
                (cid,),
            )
            conn.commit()
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Append-only, signed rows refuse DELETE
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
