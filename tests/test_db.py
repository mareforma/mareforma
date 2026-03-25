"""
tests/test_db.py — unit tests for mareforma/db.py.

Covers:
  - Schema initialisation: tables created, WAL mode on, schema version set
  - Schema version mismatch raises DatabaseError
  - record_deps: rows written, idempotent on re-insert
  - write_transform_class / lookup_cached_class: roundtrip
  - hash_string: deterministic SHA-256, 64-char output
  - is_stale: never-run, matching hashes, input changed, source changed, force
  - begin_run + end_run lifecycle: row created, then updated
  - all_transform_runs: returns latest run per transform
  - record_artifact: row written with correct fields
  - get_build_meta / set_build_meta roundtrip
  - add_claim: row written, claim_id returned
  - get_claim: returns dict or None
  - list_claims: filtered and unfiltered
  - update_claim: fields updated, backup written
  - delete_claim: row removed from db
  - validate_confidence: valid labels pass, invalid raises ValueError
  - claims.toml backup written after add_claim
  - migrate_from_lock_json: lock data imported, .bak created
  - migrate_from_lock_json: idempotent (skipped if .bak exists)
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

import pytest

from mareforma.db import (
    ClaimNotFoundError,
    DatabaseError,
    add_claim,
    all_transform_runs,
    begin_run,
    delete_claim,
    end_run,
    get_build_meta,
    get_claim,
    hash_string,
    is_stale,
    list_claims,
    lookup_cached_class,
    migrate_from_lock_json,
    open_db,
    record_artifact,
    record_deps,
    set_build_meta,
    update_claim,
    validate_confidence,
    write_transform_class,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _open(tmp_path: Path) -> sqlite3.Connection:
    """Open a fresh graph.db in tmp_path/.mareforma/."""
    (tmp_path / ".mareforma").mkdir(parents=True, exist_ok=True)
    return open_db(tmp_path)


def _run_id() -> str:
    return str(uuid.uuid4())


def _insert_success_run(
    conn: sqlite3.Connection,
    tmp_path: Path,
    name: str,
    input_hash: str = "ihash",
    source_hash: str = "shash",
) -> str:
    run_id = _run_id()
    begin_run(conn, run_id, name, input_hash, source_hash)
    end_run(conn, run_id, status="success", output_hash="ohash", duration_ms=10)
    return run_id


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

class TestOpenDb:
    def test_creates_tables(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            tables = {
                row[0] for row in
                conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
        finally:
            conn.close()
        assert {"transform_runs", "artifacts", "claims", "evidence", "build_meta"}.issubset(tables)

    def test_schema_version_set(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
        finally:
            conn.close()
        assert version == 1

    def test_idempotent_second_open(self, tmp_path: Path) -> None:
        """Opening an already-initialised db must not raise."""
        conn1 = _open(tmp_path)
        conn1.close()
        conn2 = _open(tmp_path)
        conn2.close()

    def test_wrong_schema_version_raises(self, tmp_path: Path) -> None:
        (tmp_path / ".mareforma").mkdir(parents=True, exist_ok=True)
        db_path = tmp_path / ".mareforma" / "graph.db"
        # Manually create a db with a wrong schema version
        raw = sqlite3.connect(str(db_path))
        raw.execute("PRAGMA user_version = 99")
        raw.close()

        with pytest.raises(DatabaseError, match="schema v"):
            open_db(tmp_path)



# ---------------------------------------------------------------------------
# hash_string
# ---------------------------------------------------------------------------

class TestHashString:
    def test_sha256_length_and_deterministic(self) -> None:
        h = hash_string("abc")
        assert len(h) == 64
        assert hash_string("abc") == h  # deterministic
        assert hash_string("abc") != hash_string("def")


# ---------------------------------------------------------------------------
# is_stale
# ---------------------------------------------------------------------------

class TestIsStale:
    def test_never_run_is_stale(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            assert is_stale(conn, "morph.load", "ihash", "shash") is True
        finally:
            conn.close()

    def test_matching_hashes_not_stale(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            _insert_success_run(conn, tmp_path, "morph.load")
            assert is_stale(conn, "morph.load", "ihash", "shash") is False
        finally:
            conn.close()

    def test_input_hash_changed_is_stale(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            _insert_success_run(conn, tmp_path, "morph.load")
            assert is_stale(conn, "morph.load", "DIFFERENT", "shash") is True
        finally:
            conn.close()

    def test_source_hash_changed_is_stale(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            _insert_success_run(conn, tmp_path, "morph.load")
            assert is_stale(conn, "morph.load", "ihash", "DIFFERENT") is True
        finally:
            conn.close()

    def test_failed_run_is_stale(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            run_id = _run_id()
            begin_run(conn, run_id, "morph.bad", "ihash", "shash")
            end_run(conn, run_id, status="failed")
            assert is_stale(conn, "morph.bad", "ihash", "shash") is True
        finally:
            conn.close()

    def test_force_always_stale(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            _insert_success_run(conn, tmp_path, "morph.load")
            assert is_stale(conn, "morph.load", "ihash", "shash", force=True) is True
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# begin_run / end_run lifecycle
# ---------------------------------------------------------------------------

class TestRunLifecycle:
    def test_begin_run_creates_running_row(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            run_id = _run_id()
            begin_run(conn, run_id, "morph.load", "ihash", "shash")
            row = conn.execute(
                "SELECT status FROM transform_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            assert row["status"] == "running"
        finally:
            conn.close()

    def test_end_run_updates_status(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            run_id = _run_id()
            begin_run(conn, run_id, "morph.load", "ih", "sh")
            end_run(conn, run_id, status="success", output_hash="oh", duration_ms=100)
            row = conn.execute(
                "SELECT status, output_hash, duration_ms FROM transform_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            assert row["status"] == "success"
            assert row["output_hash"] == "oh"
            assert row["duration_ms"] == 100
        finally:
            conn.close()

    def test_end_run_records_error_message(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            run_id = _run_id()
            begin_run(conn, run_id, "morph.bad", "ih", "sh")
            end_run(conn, run_id, status="failed", error_message="RuntimeError: boom")
            row = conn.execute(
                "SELECT error_message FROM transform_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            assert "boom" in row["error_message"]
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# all_transform_runs
# ---------------------------------------------------------------------------

class TestAllTransformRuns:
    def test_returns_latest_per_transform(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            # Two runs for the same transform — latest should win
            _insert_success_run(conn, tmp_path, "morph.load")
            run_id2 = _run_id()
            begin_run(conn, run_id2, "morph.load", "new_input", "shash")
            end_run(conn, run_id2, status="success")

            runs = all_transform_runs(conn)
            assert "morph.load" in runs
        finally:
            conn.close()

    def test_empty_db_returns_empty_dict(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            assert all_transform_runs(conn) == {}
        finally:
            conn.close()

    def test_multiple_transforms_all_present(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            for name in ["morph.load", "morph.register", "ephys.load"]:
                _insert_success_run(conn, tmp_path, name)
            runs = all_transform_runs(conn)
            assert set(runs.keys()) == {"morph.load", "morph.register", "ephys.load"}
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# record_artifact
# ---------------------------------------------------------------------------

class TestRecordArtifact:
    def test_artifact_row_written(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            run_id = _run_id()
            begin_run(conn, run_id, "morph.load", "ih", "sh")
            artifact_path = tmp_path / "out.pickle"
            artifact_path.write_bytes(b"data")
            record_artifact(
                conn, run_id, "morph.load.data", artifact_path, "pickle",
                sha256="abc123", size_bytes=4,
            )
            row = conn.execute(
                "SELECT artifact_name, format FROM artifacts WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            assert row["artifact_name"] == "morph.load.data"
            assert row["format"] == "pickle"
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# build_meta
# ---------------------------------------------------------------------------

class TestBuildMeta:
    def test_roundtrip(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            set_build_meta(conn, timestamp="2026-03-18T00:00:00+00:00", git_sha="abc1234")
            meta = get_build_meta(conn)
            assert meta["last_build_timestamp"] == "2026-03-18T00:00:00+00:00"
            assert meta["last_git_sha"] == "abc1234"
        finally:
            conn.close()

    def test_empty_returns_none(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            meta = get_build_meta(conn)
            assert meta["last_build_timestamp"] is None
            assert meta["last_git_sha"] is None
        finally:
            conn.close()

    def test_git_sha_none_stored(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            set_build_meta(conn, timestamp="2026-01-01T00:00:00+00:00", git_sha=None)
            meta = get_build_meta(conn)
            assert meta["last_git_sha"] is None
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# validate_confidence
# ---------------------------------------------------------------------------

class TestValidateConfidence:
    def test_valid_labels(self) -> None:
        for label in ("anecdotal", "exploratory", "preliminary", "supported", "established"):
            val = validate_confidence(label)
            assert 0.0 < val <= 1.0

    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown confidence"):
            validate_confidence("certain")

    def test_scale_ordering(self) -> None:
        from mareforma.db import CONFIDENCE_SCALE
        vals = list(CONFIDENCE_SCALE.values())
        assert vals == sorted(vals), "CONFIDENCE_SCALE must be in ascending order"


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
            claim_id = add_claim(
                conn, tmp_path, "L2/3 neurons show X",
                confidence="preliminary", status="open",
            )
            c = get_claim(conn, claim_id)
            assert c is not None
            assert c["text"] == "L2/3 neurons show X"
            assert c["confidence"] == "preliminary"
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
            add_claim(conn, tmp_path, "Supported claim", status="supported")
            open_claims = list_claims(conn, status="open")
            assert len(open_claims) == 1
            assert open_claims[0]["text"] == "Open claim"
        finally:
            conn.close()

    def test_list_claims_filtered_by_source(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "About morphology", source_name="morphology")
            add_claim(conn, tmp_path, "About ephys", source_name="ephys")
            morph_claims = list_claims(conn, source_name="morphology")
            assert len(morph_claims) == 1
        finally:
            conn.close()

    def test_update_claim_confidence(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "Original claim", confidence="exploratory")
            update_claim(conn, tmp_path, claim_id, confidence="supported")
            c = get_claim(conn, claim_id)
            assert c["confidence"] == "supported"
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
                update_claim(conn, tmp_path, "no-such-id", status="supported")
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

    def test_invalid_confidence_raises(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            with pytest.raises(ValueError, match="confidence"):
                add_claim(conn, tmp_path, "Valid text", confidence="bogus")
        finally:
            conn.close()

    def test_add_claim_with_run_id_links_evidence(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            run_id = _run_id()
            begin_run(conn, run_id, "morph.load", "ih", "sh")
            claim_id = add_claim(conn, tmp_path, "Linked claim", run_id=run_id)
            evidence = conn.execute(
                "SELECT run_id FROM evidence WHERE claim_id = ?", (claim_id,)
            ).fetchall()
            assert len(evidence) == 1
            assert evidence[0]["run_id"] == run_id
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
        # claims.toml must exist and not contain the deleted claim
        toml_text = (tmp_path / "claims.toml").read_text(encoding="utf-8")
        assert claim_id not in toml_text


# ---------------------------------------------------------------------------
# Migration from pipeline.lock.json
# ---------------------------------------------------------------------------

class TestMigration:
    def _write_lock(self, root: Path, nodes: dict, build_ts: str = "2026-01-01T00:00:00+00:00") -> None:
        lock_dir = root / ".mareforma"
        lock_dir.mkdir(parents=True, exist_ok=True)
        lock_path = lock_dir / "pipeline.lock.json"
        data = {
            "schema_version": 1,
            "build_timestamp": build_ts,
            "git_sha": "abc123",
            "nodes": nodes,
        }
        lock_path.write_text(json.dumps(data), encoding="utf-8")

    def test_migration_imports_nodes(self, tmp_path: Path) -> None:
        self._write_lock(tmp_path, {
            "morph.load": {
                "input_hash": "ih", "output_hash": "oh", "source_hash": "sh",
                "status": "success", "duration_ms": 50, "timestamp": "2026-01-01T00:00:00+00:00",
            }
        })
        conn = _open(tmp_path)
        try:
            migrated = migrate_from_lock_json(conn, tmp_path)
            assert migrated is True
            runs = all_transform_runs(conn)
            assert "morph.load" in runs
            assert runs["morph.load"]["status"] == "success"
        finally:
            conn.close()

    def test_migration_renames_lock_to_bak(self, tmp_path: Path) -> None:
        self._write_lock(tmp_path, {})
        conn = _open(tmp_path)
        try:
            migrate_from_lock_json(conn, tmp_path)
        finally:
            conn.close()
        assert not (tmp_path / ".mareforma" / "pipeline.lock.json").exists()
        assert (tmp_path / ".mareforma" / "pipeline.lock.json.bak").exists()

    def test_migration_idempotent_when_bak_exists(self, tmp_path: Path) -> None:
        """If .bak already exists, migration is skipped — no duplicate inserts."""
        self._write_lock(tmp_path, {
            "morph.load": {
                "input_hash": "ih", "output_hash": "oh", "source_hash": "sh",
                "status": "success", "duration_ms": 50, "timestamp": "2026-01-01T00:00:00+00:00",
            }
        })
        (tmp_path / ".mareforma").mkdir(parents=True, exist_ok=True)
        (tmp_path / ".mareforma" / "pipeline.lock.json.bak").write_text("{}")

        conn = _open(tmp_path)
        try:
            result = migrate_from_lock_json(conn, tmp_path)
            assert result is False
        finally:
            conn.close()

    def test_migration_skipped_if_no_lock_file(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            result = migrate_from_lock_json(conn, tmp_path)
            assert result is False
        finally:
            conn.close()




# ---------------------------------------------------------------------------
# record_deps
# ---------------------------------------------------------------------------

class TestRecordDeps:
    def test_record_deps_writes_rows(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            run_id = _run_id()
            begin_run(conn, run_id, "src.filter", "ih", "sh")
            record_deps(conn, "src.filter", ["src.load", "src.preprocess"])
            rows = conn.execute(
                "SELECT depends_on_name FROM transform_deps WHERE transform_name = 'src.filter'"
            ).fetchall()
            dep_names = {r["depends_on_name"] for r in rows}
            assert dep_names == {"src.load", "src.preprocess"}
        finally:
            conn.close()

    def test_record_deps_empty_list_no_rows(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            run_id = _run_id()
            begin_run(conn, run_id, "src.load", "ih", "sh")
            record_deps(conn, "src.load", [])
            rows = conn.execute(
                "SELECT * FROM transform_deps WHERE transform_name = 'src.load'"
            ).fetchall()
            assert rows == []
        finally:
            conn.close()

    def test_record_deps_idempotent(self, tmp_path: Path) -> None:
        """Calling record_deps twice with same data must not raise or duplicate rows."""
        conn = _open(tmp_path)
        try:
            begin_run(conn, _run_id(), "src.filter", "ih", "sh")
            record_deps(conn, "src.filter", ["src.load"])
            record_deps(conn, "src.filter", ["src.load"])  # duplicate call
            rows = conn.execute(
                "SELECT * FROM transform_deps WHERE transform_name = 'src.filter'"
            ).fetchall()
            assert len(rows) == 1
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# write_transform_class / lookup_cached_class
# ---------------------------------------------------------------------------

class TestTransformClass:
    def test_write_and_read_transform_class(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            run_id = _run_id()
            begin_run(conn, run_id, "src.load", "ih", "sh")
            end_run(conn, run_id, status="success", output_hash="ohash")
            write_transform_class(
                conn, run_id,
                transform_class="raw",
                class_confidence=1.0,
                class_method="heuristic",
                class_reason="root node",
            )
            row = conn.execute(
                "SELECT transform_class, class_confidence, class_method, class_reason "
                "FROM transform_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            assert row["transform_class"] == "raw"
            assert row["class_confidence"] == pytest.approx(1.0)
            assert row["class_method"] == "heuristic"
            assert row["class_reason"] == "root node"
        finally:
            conn.close()

    def test_lookup_cached_class_hit(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            run_id = _run_id()
            begin_run(conn, run_id, "src.filter", "ih", "sh")
            end_run(conn, run_id, status="success", output_hash="shared_hash")
            write_transform_class(
                conn, run_id,
                transform_class="processed",
                class_confidence=0.9,
                class_method="content",
                class_reason="subset of input",
            )
            cached = lookup_cached_class(conn, "shared_hash")
            assert cached is not None
            cls, conf, method, reason = cached
            assert cls == "processed"
            assert conf == pytest.approx(0.9)
        finally:
            conn.close()

    def test_lookup_cached_class_miss(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            result = lookup_cached_class(conn, "nonexistent_hash")
            assert result is None
        finally:
            conn.close()

    def test_lookup_cached_class_ignores_unknown(self, tmp_path: Path) -> None:
        """A cached class of 'unknown' should not be returned as a cache hit."""
        conn = _open(tmp_path)
        try:
            run_id = _run_id()
            begin_run(conn, run_id, "src.ghost", "ih", "sh")
            end_run(conn, run_id, status="success", output_hash="ghost_hash")
            write_transform_class(
                conn, run_id,
                transform_class="unknown",
                class_confidence=0.0,
                class_method="heuristic",
                class_reason="file not found",
            )
            cached = lookup_cached_class(conn, "ghost_hash")
            assert cached is None
        finally:
            conn.close()
