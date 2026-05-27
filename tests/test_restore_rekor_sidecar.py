"""Round-trip and adversarial tests for the rekor_inclusions sidecar
through claims.toml.

Tests the v0.3.2 feature: _backup_claims_toml emits a [rekor_inclusions]
section; restore() replays entries into the sidecar table; drift warnings
distinguish "section absent (upgrade)" from "entry missing (suspicious)".
"""

from __future__ import annotations

import json
import shutil
import warnings
from pathlib import Path

import pytest

import mareforma
from mareforma.db import RestoreError
from mareforma.db.errors import (
    RekorSidecarEntryMissingWarning,
    RekorSidecarSectionAbsentWarning,
)


def _bootstrap_key(tmp_path: Path) -> Path:
    from mareforma.signing import bootstrap_key
    key_path = tmp_path / "keys" / "key"
    bootstrap_key(key_path)
    return key_path


class TestSidecarRoundTrip:
    """Happy-path: backup emits [rekor_inclusions], restore replays it."""

    def test_section_present_in_toml_when_sidecar_populated(self, tmp_path):
        """After a claim with a faked sidecar entry, claims.toml has the section."""
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim("test claim", generated_by="agent/test")

        # Manually insert a sidecar row (we can't submit to real Rekor in tests)
        from mareforma.db import open_db, _backup_claims_toml
        conn = open_db(tmp_path)
        conn.execute(
            "INSERT INTO rekor_inclusions "
            "(claim_id, uuid, log_index, integrated_time, raw_response_b64, recorded_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (cid, "abc123", 42, 1716800000, "eyJ0ZXN0IjogdHJ1ZX0=", "2026-05-27T00:00:00Z"),
        )
        conn.commit()
        _backup_claims_toml(conn, tmp_path)
        conn.close()

        toml_path = tmp_path / "claims.toml"
        content = toml_path.read_text()
        assert "[rekor_inclusions." in content
        assert "abc123" in content

    def test_round_trip_preserves_sidecar_entries(self, tmp_path):
        """Backup then restore preserves sidecar data byte-for-byte."""
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim("round trip test", generated_by="agent/test")

        from mareforma.db import open_db, _backup_claims_toml
        conn = open_db(tmp_path)
        conn.execute(
            "INSERT INTO rekor_inclusions "
            "(claim_id, uuid, log_index, integrated_time, raw_response_b64, recorded_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (cid, "def456", 99, 1716800001, "eyJyb3VuZCI6ICJ0cmlwIn0=", "2026-05-27T01:00:00Z"),
        )
        conn.commit()
        _backup_claims_toml(conn, tmp_path)
        conn.close()

        # Wipe graph.db + sidecar
        db_path = tmp_path / ".mareforma" / "graph.db"
        cache_path = tmp_path / ".mareforma" / "claim_supports_cache.db"
        db_path.unlink()
        if cache_path.exists():
            cache_path.unlink()

        # Restore
        result = mareforma.restore(tmp_path)
        assert result["claims_restored"] == 1

        # Verify sidecar row was replayed
        conn2 = open_db(tmp_path)
        row = conn2.execute(
            "SELECT uuid, log_index, raw_response_b64 FROM rekor_inclusions WHERE claim_id = ?",
            (cid,),
        ).fetchone()
        conn2.close()
        assert row is not None
        assert row["uuid"] == "def456"
        assert row["log_index"] == 99
        assert row["raw_response_b64"] == "eyJyb3VuZCI6ICJ0cmlwIn0="


class TestDriftWarnings:
    """Drift detection: section-absent vs entry-missing."""

    def test_section_absent_warns_on_upgrade(self, tmp_path):
        """A pre-v0.3.2 TOML (no [rekor_inclusions]) triggers the section warning."""
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim("upgrade test", generated_by="agent/test")

        from mareforma.db import open_db, _backup_claims_toml
        conn = open_db(tmp_path)
        # Add rekor coords to the signature_bundle but DON'T add a sidecar row
        row = conn.execute(
            "SELECT signature_bundle FROM claims WHERE claim_id = ?", (cid,),
        ).fetchone()
        if row and row["signature_bundle"]:
            bundle = json.loads(row["signature_bundle"])
            bundle["rekor"] = {"uuid": "fake", "logIndex": 1, "integratedTime": 0}
            conn.execute(
                "UPDATE claims SET signature_bundle = ? WHERE claim_id = ?",
                (json.dumps(bundle, separators=(",", ":")), cid),
            )
            conn.commit()
        _backup_claims_toml(conn, tmp_path)
        conn.close()

        # Strip [rekor_inclusions] from TOML to simulate pre-v0.3.2
        toml_path = tmp_path / "claims.toml"
        import tomli
        data = tomli.loads(toml_path.read_text())
        data.pop("rekor_inclusions", None)
        import tomli_w
        toml_path.write_bytes(tomli_w.dumps(data).encode("utf-8"))

        # Wipe and restore
        db_path = tmp_path / ".mareforma" / "graph.db"
        cache_path = tmp_path / ".mareforma" / "claim_supports_cache.db"
        db_path.unlink()
        if cache_path.exists():
            cache_path.unlink()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            mareforma.restore(tmp_path)
            section_warns = [x for x in w if issubclass(x.category, RekorSidecarSectionAbsentWarning)]
            assert len(section_warns) == 1
            assert "no [rekor_inclusions] section" in str(section_warns[0].message)

    def test_entry_missing_warns_when_section_exists(self, tmp_path):
        """A present [rekor_inclusions] missing an entry triggers the entry warning."""
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim("entry missing test", generated_by="agent/test")

        from mareforma.db import open_db, _backup_claims_toml
        conn = open_db(tmp_path)
        # Add rekor coords to bundle
        row = conn.execute(
            "SELECT signature_bundle FROM claims WHERE claim_id = ?", (cid,),
        ).fetchone()
        if row and row["signature_bundle"]:
            bundle = json.loads(row["signature_bundle"])
            bundle["rekor"] = {"uuid": "real", "logIndex": 5, "integratedTime": 0}
            conn.execute(
                "UPDATE claims SET signature_bundle = ? WHERE claim_id = ?",
                (json.dumps(bundle, separators=(",", ":")), cid),
            )
            conn.commit()
        # Add sidecar row, then backup, then REMOVE the entry from TOML
        conn.execute(
            "INSERT INTO rekor_inclusions "
            "(claim_id, uuid, log_index, integrated_time, raw_response_b64, recorded_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (cid, "real", 5, 0, "e30=", "2026-05-27T00:00:00Z"),
        )
        conn.commit()
        _backup_claims_toml(conn, tmp_path)
        conn.close()

        # Edit TOML: keep [rekor_inclusions] section but remove this claim's entry
        toml_path = tmp_path / "claims.toml"
        import tomli
        data = tomli.loads(toml_path.read_text())
        data["rekor_inclusions"] = {}  # section present, but empty
        import tomli_w
        toml_path.write_bytes(tomli_w.dumps(data).encode("utf-8"))

        db_path = tmp_path / ".mareforma" / "graph.db"
        cache_path = tmp_path / ".mareforma" / "claim_supports_cache.db"
        db_path.unlink()
        if cache_path.exists():
            cache_path.unlink()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            mareforma.restore(tmp_path)
            entry_warns = [x for x in w if issubclass(x.category, RekorSidecarEntryMissingWarning)]
            assert len(entry_warns) == 1
            assert "no matching entry" in str(entry_warns[0].message)


class TestAdversarial:
    """Tampered sidecar entries in TOML."""

    def test_orphan_entry_raises(self, tmp_path):
        """A sidecar entry referencing a non-existent claim raises RestoreError."""
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("anchor", generated_by="agent/test")

        from mareforma.db import open_db, _backup_claims_toml
        conn = open_db(tmp_path)
        _backup_claims_toml(conn, tmp_path)
        conn.close()

        # Inject a sidecar entry for a non-existent claim
        toml_path = tmp_path / "claims.toml"
        import tomli
        data = tomli.loads(toml_path.read_text())
        data["rekor_inclusions"] = {
            "00000000-0000-4000-8000-000000000000": {
                "uuid": "ghost",
                "log_index": 1,
                "integrated_time": 0,
                "raw_response_b64": "e30=",
                "recorded_at": "2026-01-01T00:00:00Z",
            }
        }
        import tomli_w
        toml_path.write_bytes(tomli_w.dumps(data).encode("utf-8"))

        db_path = tmp_path / ".mareforma" / "graph.db"
        cache_path = tmp_path / ".mareforma" / "claim_supports_cache.db"
        db_path.unlink()
        if cache_path.exists():
            cache_path.unlink()

        with pytest.raises(RestoreError, match="not in the .claims. section"):
            mareforma.restore(tmp_path)

    def test_missing_required_fields_raises(self, tmp_path):
        """A sidecar entry missing uuid or raw_response_b64 raises RestoreError."""
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim("fields test", generated_by="agent/test")

        from mareforma.db import open_db, _backup_claims_toml
        conn = open_db(tmp_path)
        _backup_claims_toml(conn, tmp_path)
        conn.close()

        toml_path = tmp_path / "claims.toml"
        import tomli
        data = tomli.loads(toml_path.read_text())
        data["rekor_inclusions"] = {
            cid: {"log_index": 1}  # missing uuid and raw_response_b64
        }
        import tomli_w
        toml_path.write_bytes(tomli_w.dumps(data).encode("utf-8"))

        db_path = tmp_path / ".mareforma" / "graph.db"
        cache_path = tmp_path / ".mareforma" / "claim_supports_cache.db"
        db_path.unlink()
        if cache_path.exists():
            cache_path.unlink()

        with pytest.raises(RestoreError, match="missing required fields"):
            mareforma.restore(tmp_path)

    def test_empty_rekor_inclusions_section_succeeds(self, tmp_path):
        """An empty [rekor_inclusions] section is valid (no logged claims)."""
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("no rekor", generated_by="agent/test")

        from mareforma.db import open_db, _backup_claims_toml
        conn = open_db(tmp_path)
        _backup_claims_toml(conn, tmp_path)
        conn.close()

        toml_path = tmp_path / "claims.toml"
        import tomli
        data = tomli.loads(toml_path.read_text())
        data["rekor_inclusions"] = {}
        import tomli_w
        toml_path.write_bytes(tomli_w.dumps(data).encode("utf-8"))

        db_path = tmp_path / ".mareforma" / "graph.db"
        cache_path = tmp_path / ".mareforma" / "claim_supports_cache.db"
        db_path.unlink()
        if cache_path.exists():
            cache_path.unlink()

        result = mareforma.restore(tmp_path)
        assert result["claims_restored"] == 1
