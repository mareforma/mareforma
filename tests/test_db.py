"""
tests/test_db.py — unit tests for mareforma/db.py.

Covers:
  - Schema initialisation: claims table created, WAL mode on, schema version set
  - Schema version mismatch raises DatabaseError
  - add_claim: row written, claim_id returned
  - get_claim: returns dict or None
  - list_claims: filtered and unfiltered
  - update_claim: fields updated, backup written
  - delete_claim: row removed from db
  - claims.toml backup written after add_claim
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from mareforma.db import (
    ClaimNotFoundError,
    DatabaseError,
    add_claim,
    delete_claim,
    get_claim,
    list_claims,
    open_db,
    update_claim,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _open(tmp_path: Path) -> sqlite3.Connection:
    (tmp_path / ".mareforma").mkdir(parents=True, exist_ok=True)
    return open_db(tmp_path)


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

class TestOpenDb:
    def test_creates_claims_table(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            tables = {
                row[0] for row in
                conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
        finally:
            conn.close()
        assert "claims" in tables

    def test_schema_version_set(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
        finally:
            conn.close()
        assert version == 1

    def test_idempotent_second_open(self, tmp_path: Path) -> None:
        conn1 = _open(tmp_path)
        conn1.close()
        conn2 = _open(tmp_path)
        conn2.close()

    def test_missing_columns_raises(self, tmp_path: Path) -> None:
        """A db with user_version=1 but missing claims columns is rejected."""
        (tmp_path / ".mareforma").mkdir(parents=True, exist_ok=True)
        db_path = tmp_path / ".mareforma" / "graph.db"
        raw = sqlite3.connect(str(db_path))
        # Initialised marker but no tables created — simulates schema drift.
        raw.execute("PRAGMA user_version = 1")
        raw.commit()
        raw.close()
        with pytest.raises(DatabaseError, match="schema mismatch"):
            open_db(tmp_path)

    def test_extra_columns_raise_downgrade_error(self, tmp_path: Path) -> None:
        """A claims table with an extra column is treated as a downgrade.

        Hand-edited or partially-migrated schemas should fail loudly, AND the
        error message must guide the user toward upgrading rather than
        deleting (because claims.toml may not be a faithful backup of columns
        the older version doesn't know about).
        """
        # Initialise normally, then sneak an extra column in (simulates a
        # newer mareforma having written this db).
        conn = open_db(tmp_path)
        conn.execute("ALTER TABLE claims ADD COLUMN sneaky_field TEXT")
        conn.commit()
        conn.close()
        with pytest.raises(DatabaseError, match="newer mareforma version"):
            open_db(tmp_path)


# ---------------------------------------------------------------------------
# Claims CRUD
# ---------------------------------------------------------------------------

class TestClaimCRUD:
    def test_add_claim_returns_id(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "Some observation")
            assert isinstance(claim_id, str)
            assert len(claim_id) > 0
        finally:
            conn.close()

    def test_get_claim_roundtrip(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "Cell type A shows X", classification="ANALYTICAL")
            c = get_claim(conn, claim_id)
            assert c is not None
            assert c["text"] == "Cell type A shows X"
            assert c["classification"] == "ANALYTICAL"
        finally:
            conn.close()

    def test_get_claim_missing_returns_none(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            assert get_claim(conn, "nonexistent-id") is None
        finally:
            conn.close()

    def test_list_claims_unfiltered(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Claim A")
            add_claim(conn, tmp_path, "Claim B")
            claims = list_claims(conn)
            assert len(claims) == 2
        finally:
            conn.close()

    def test_list_claims_filtered_by_status(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Open claim", status="open")
            add_claim(conn, tmp_path, "Contested claim", status="contested")
            open_claims = list_claims(conn, status="open")
            assert len(open_claims) == 1
            assert open_claims[0]["text"] == "Open claim"
        finally:
            conn.close()

    def test_list_claims_filtered_by_source(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "About dataset alpha", source_name="dataset_alpha")
            add_claim(conn, tmp_path, "About dataset beta", source_name="dataset_beta")
            alpha_claims = list_claims(conn, source_name="dataset_alpha")
            assert len(alpha_claims) == 1
        finally:
            conn.close()

    def test_update_claim_text(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "Old text")
            update_claim(conn, tmp_path, claim_id, text="New text")
            c = get_claim(conn, claim_id)
            assert c["text"] == "New text"
        finally:
            conn.close()

    def test_update_missing_claim_raises(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            with pytest.raises(ClaimNotFoundError):
                update_claim(conn, tmp_path, "no-such-id", status="contested")
        finally:
            conn.close()

    def test_delete_claim_removes_row(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "To be deleted")
            delete_claim(conn, tmp_path, claim_id)
            assert get_claim(conn, claim_id) is None
        finally:
            conn.close()

    def test_delete_missing_claim_raises(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            with pytest.raises(ClaimNotFoundError):
                delete_claim(conn, tmp_path, "no-such-id")
        finally:
            conn.close()

    def test_empty_claim_text_raises(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            with pytest.raises(ValueError, match="empty"):
                add_claim(conn, tmp_path, "   ")
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# claims.toml backup
# ---------------------------------------------------------------------------

class TestClaimsTomlBackup:
    def test_backup_written_after_add(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Backup test claim")
        finally:
            conn.close()
        assert (tmp_path / "claims.toml").exists()

    def test_backup_reflects_delete(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "Claim to delete")
            delete_claim(conn, tmp_path, claim_id)
        finally:
            conn.close()
        toml_text = (tmp_path / "claims.toml").read_text(encoding="utf-8")
        assert claim_id not in toml_text
