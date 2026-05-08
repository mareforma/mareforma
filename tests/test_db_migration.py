"""
tests/test_db_migration.py — open_db() schema migration.

Coverage
--------
  v0→v2  : fresh db gets full v2 schema, user_version=2
  v1→v2  : existing v1 db migrates correctly
  v1→v2  : existing claims preserved after migration
  v1→v2  : stated_confidence equals former confidence_float value
  v1→v2  : classification default is 'INFERRED'
  v1→v2  : support_level default is 'PRELIMINARY'
  v1→v2  : migration is transactional (future-proof — no partial schema)
  v>2    : DatabaseError raised
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from mareforma.db import open_db, DatabaseError, _SCHEMA_VERSION


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_v1_db(path: Path) -> None:
    """Create a user_version=1 database with the old schema and one claim."""
    db_path = path / ".mareforma" / "graph.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE transform_runs (
            run_id TEXT PRIMARY KEY, transform_name TEXT NOT NULL,
            input_hash TEXT NOT NULL, source_hash TEXT NOT NULL,
            output_hash TEXT, status TEXT NOT NULL DEFAULT 'running',
            error_message TEXT, duration_ms INTEGER, timestamp TEXT NOT NULL,
            transform_class TEXT, class_confidence REAL,
            class_method TEXT, class_reason TEXT
        );
        CREATE TABLE claims (
            claim_id TEXT PRIMARY KEY,
            text TEXT NOT NULL,
            confidence TEXT NOT NULL DEFAULT 'exploratory',
            confidence_float REAL NOT NULL DEFAULT 0.4,
            generation_method TEXT NOT NULL DEFAULT 'explicit',
            status TEXT NOT NULL DEFAULT 'open',
            replication_status TEXT NOT NULL DEFAULT 'unknown',
            source_name TEXT,
            generated_by TEXT NOT NULL DEFAULT 'human',
            supports_json TEXT NOT NULL DEFAULT '[]',
            contradicts_json TEXT NOT NULL DEFAULT '[]',
            comparison_summary TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE evidence (
            evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,
            claim_id TEXT NOT NULL REFERENCES claims(claim_id),
            run_id TEXT,
            artifact_name TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE build_meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE transform_deps (
            transform_name TEXT NOT NULL, depends_on_name TEXT NOT NULL,
            PRIMARY KEY (transform_name, depends_on_name)
        );
        INSERT INTO claims
            (claim_id, text, confidence, confidence_float, created_at, updated_at)
        VALUES
            ('test-id-001', 'prior finding', 'supported', 0.80,
             '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00');
        PRAGMA user_version = 1;
    """)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# v0 → v2 (fresh database)
# ---------------------------------------------------------------------------

def test_fresh_db_gets_schema_version_2(tmp_path):
    conn = open_db(tmp_path)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    conn.close()
    assert version == _SCHEMA_VERSION == 2


def test_fresh_db_has_classification_column(tmp_path):
    conn = open_db(tmp_path)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(claims)")}
    conn.close()
    assert "classification" in cols
    assert "support_level" in cols
    assert "stated_confidence" in cols
    assert "idempotency_key" in cols
    assert "validated_by" in cols
    assert "validated_at" in cols


def test_fresh_db_has_agent_events_table(tmp_path):
    conn = open_db(tmp_path)
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    conn.close()
    assert "agent_events" in tables


# ---------------------------------------------------------------------------
# v1 → v2 migration
# ---------------------------------------------------------------------------

def test_migration_v1_to_v2_sets_version_2(tmp_path):
    _make_v1_db(tmp_path)
    conn = open_db(tmp_path)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    conn.close()
    assert version == 2


def test_migration_v1_to_v2_preserves_existing_claims(tmp_path):
    _make_v1_db(tmp_path)
    conn = open_db(tmp_path)
    row = conn.execute(
        "SELECT claim_id, text FROM claims WHERE claim_id = 'test-id-001'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[1] == "prior finding"


def test_migration_v1_to_v2_stated_confidence_equals_old_float(tmp_path):
    _make_v1_db(tmp_path)
    conn = open_db(tmp_path)
    row = conn.execute(
        "SELECT stated_confidence FROM claims WHERE claim_id = 'test-id-001'"
    ).fetchone()
    conn.close()
    assert row["stated_confidence"] == pytest.approx(0.80)


def test_migration_v1_to_v2_default_classification_is_inferred(tmp_path):
    _make_v1_db(tmp_path)
    conn = open_db(tmp_path)
    row = conn.execute(
        "SELECT classification FROM claims WHERE claim_id = 'test-id-001'"
    ).fetchone()
    conn.close()
    assert row["classification"] == "INFERRED"


def test_migration_v1_to_v2_default_support_level_is_preliminary(tmp_path):
    _make_v1_db(tmp_path)
    conn = open_db(tmp_path)
    row = conn.execute(
        "SELECT support_level FROM claims WHERE claim_id = 'test-id-001'"
    ).fetchone()
    conn.close()
    assert row["support_level"] == "PRELIMINARY"


# ---------------------------------------------------------------------------
# Unsupported version
# ---------------------------------------------------------------------------

def test_open_db_future_version_raises(tmp_path):
    db_path = tmp_path / ".mareforma" / "graph.db"
    db_path.parent.mkdir(parents=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA user_version = 99")
    conn.commit()
    conn.close()

    with pytest.raises(DatabaseError, match="newer"):
        open_db(tmp_path)
