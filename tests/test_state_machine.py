"""tests/test_state_machine.py — DB-layer state-machine + prev_hash chain.

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
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

import mareforma
from mareforma import db as _db
from mareforma.db import (
    ChainIntegrityError,
    IllegalStateTransitionError,
    _CLAIM_COLUMNS,
    _compute_prev_hash,
    add_claim,
    open_db,
    update_claim,
    validate_claim,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# State-transition triggers
# ---------------------------------------------------------------------------


class TestInsertTrigger:
    def test_preliminary_insert_allowed(self, tmp_path: Path) -> None:
        # The standard add_claim path — sanity check the trigger doesn't
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
        """A PRELIMINARY row that carries validated_by is incoherent — reject."""
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


class TestUpdateTrigger:
    def test_preliminary_to_replicated_allowed(self, tmp_path: Path) -> None:
        # The auto-promotion path that _maybe_update_replicated takes.
        from mareforma import signing as _sig
        key = tmp_path / "k"
        _sig.bootstrap_key(key)
        with mareforma.open(tmp_path, key_path=key) as g:
            upstream = g.assert_claim("upstream", generated_by="seed", seed=True)
            a = g.assert_claim("a", supports=[upstream], generated_by="A")
            b = g.assert_claim("b", supports=[upstream], generated_by="B")
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"

    def test_preliminary_directly_to_established_rejected(
        self, tmp_path: Path,
    ) -> None:
        """Raw UPDATE attempting PRELIMINARY → ESTABLISHED hits the trigger.

        ``validate_claim`` has a Python-layer guard that blocks this
        path with a ValueError before reaching the DB. We bypass it
        here to exercise the trigger directly, which is the actual
        defense-in-depth layer P1.5 adds."""
        conn = open_db(tmp_path)
        try:
            cid = add_claim(conn, tmp_path, "x", generated_by="agent")
            with pytest.raises(sqlite3.IntegrityError, match="PRELIMINARY->ESTABLISHED"):
                conn.execute(
                    "UPDATE claims SET support_level = 'ESTABLISHED', "
                    "validation_signature = ? WHERE claim_id = ?",
                    ('{"sig":"x"}', cid),
                )
        finally:
            conn.close()

    def test_established_downgrade_rejected(self, tmp_path: Path) -> None:
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
        with mareforma.open(tmp_path, key_path=gen_key) as g:
            upstream = g.assert_claim("upstream", generated_by="seed", seed=True)
            id_a = g.assert_claim("a", supports=[upstream], generated_by="A")
            g.assert_claim("b", supports=[upstream], generated_by="B")
            g.enroll_validator(val_pem, identity="v")
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.validate(id_a)
            # Now id_a is ESTABLISHED. Attempt a direct UPDATE to PRELIMINARY.
            conn = g._conn
            with pytest.raises(IllegalStateTransitionError, match="ESTABLISHED"):
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
        with mareforma.open(tmp_path, key_path=gen_key) as g:
            upstream = g.assert_claim("upstream", generated_by="seed", seed=True)
            id_a = g.assert_claim("a", supports=[upstream], generated_by="A")
            g.assert_claim("b", supports=[upstream], generated_by="B")
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
            a = g.assert_claim("claim a")
            b = g.assert_claim("claim b")
            c = g.assert_claim("claim c")
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
            ids = [g.assert_claim(f"claim {i}") for i in range(5)]
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
        UNIQUE index. UNIQUE is the backstop to BEGIN IMMEDIATE — if
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
        key = tmp_path / "k"
        _sig.bootstrap_key(key)
        with mareforma.open(tmp_path, key_path=key) as g:
            up = g.assert_claim("up", generated_by="seed", seed=True)
            a = g.assert_claim("a", supports=[up], generated_by="A")
            g.assert_claim("b", supports=[up], generated_by="B")
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            update_claim(g._conn, tmp_path, a, status="retracted")
            row = g.get_claim(a)
            assert row["status"] == "retracted"
            assert row["support_level"] == "REPLICATED"
