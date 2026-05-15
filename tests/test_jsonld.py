"""tests/test_jsonld.py — unit tests for exporters/jsonld.py (claims-only)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from mareforma.db import add_claim, open_db
from mareforma.exporters.jsonld import JSONLDExporter


def _open(tmp_path: Path) -> sqlite3.Connection:
    (tmp_path / ".mareforma").mkdir(parents=True, exist_ok=True)
    return open_db(tmp_path)


# ---------------------------------------------------------------------------
# @context + top-level structure
# ---------------------------------------------------------------------------


class TestContextAndStructure:
    def test_context_present(self, tmp_path: Path) -> None:
        doc = JSONLDExporter(tmp_path).export()
        assert "@context" in doc
        ctx = doc["@context"]
        assert "schema" in ctx
        assert "mare" in ctx
        # PROV-O references removed in v0.3.0: the prior export
        # name-dropped prov: without populating the full PROV graph.
        # Honest scoping — the export is mareforma-native, not PROV-O.
        assert "prov" not in ctx

    def test_export_media_type(self, tmp_path: Path) -> None:
        from mareforma.exporters.jsonld import EXPORT_MEDIA_TYPE
        doc = JSONLDExporter(tmp_path).export()
        assert doc["@type"] == "mare:Graph"
        assert doc["mare:mediaType"] == EXPORT_MEDIA_TYPE
        assert EXPORT_MEDIA_TYPE == "application/x-mareforma-graph+json"

    def test_context_has_claim_vocabulary(self, tmp_path: Path) -> None:
        doc = JSONLDExporter(tmp_path).export()
        ctx = doc["@context"]
        assert "claimText" in ctx
        assert "classification" in ctx
        assert "supportLevel" in ctx
        assert "claimStatus" in ctx

    def test_graph_present(self, tmp_path: Path) -> None:
        doc = JSONLDExporter(tmp_path).export()
        assert "@graph" in doc
        assert isinstance(doc["@graph"], list)

    def test_empty_graph_when_no_claims(self, tmp_path: Path) -> None:
        doc = JSONLDExporter(tmp_path).export()
        assert doc["@graph"] == []


# ---------------------------------------------------------------------------
# Claim node serialization
# ---------------------------------------------------------------------------


class TestClaimNodes:
    def test_claim_appears_in_graph(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Target T is elevated in condition C")
        finally:
            conn.close()
        doc = JSONLDExporter(tmp_path).export()
        claims = [n for n in doc["@graph"] if n.get("@type") == "mare:Claim"]
        assert len(claims) == 1

    def test_claim_node_has_text_and_classification(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Test claim text", classification="ANALYTICAL")
        finally:
            conn.close()
        doc = JSONLDExporter(tmp_path).export()
        node = next(n for n in doc["@graph"] if n.get("@type") == "mare:Claim")
        assert node["claimText"] == "Test claim text"
        assert node["classification"] == "ANALYTICAL"

    def test_claim_id_format(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "Some claim")
        finally:
            conn.close()
        doc = JSONLDExporter(tmp_path).export()
        node = next(n for n in doc["@graph"] if n.get("@type") == "mare:Claim")
        assert node["@id"] == f"mare:claim/{claim_id}"

    def test_claim_with_source_has_usedsource_link(self, tmp_path: Path) -> None:
        # ``used`` (formerly aliased to prov:used) was renamed to
        # ``usedSource`` (now aliased to mare:usedSource) so the export
        # stays inside the mareforma-native vocabulary.
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Finding about dataset", source_name="dataset_alpha")
        finally:
            conn.close()
        doc = JSONLDExporter(tmp_path).export()
        node = next(n for n in doc["@graph"] if n.get("@type") == "mare:Claim")
        assert node.get("usedSource") == "mare:source/dataset_alpha"
        assert "used" not in node  # the PROV-flavored key is gone

    def test_multiple_claims_all_present(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Claim one")
            add_claim(conn, tmp_path, "Claim two")
            add_claim(conn, tmp_path, "Claim three")
        finally:
            conn.close()
        doc = JSONLDExporter(tmp_path).export()
        claims = [n for n in doc["@graph"] if n.get("@type") == "mare:Claim"]
        assert len(claims) == 3


# ---------------------------------------------------------------------------
# File output / write()
# ---------------------------------------------------------------------------


class TestFileOutput:
    def test_write_creates_file(self, tmp_path: Path) -> None:
        path = JSONLDExporter(tmp_path).write()
        assert path.exists()
        assert path.name == "ontology.jsonld"

    def test_written_file_is_valid_json(self, tmp_path: Path) -> None:
        path = JSONLDExporter(tmp_path).write()
        doc = json.loads(path.read_text(encoding="utf-8"))
        assert "@context" in doc
        assert "@graph" in doc

    def test_custom_output_path(self, tmp_path: Path) -> None:
        custom = tmp_path / "exports" / "my_ontology.jsonld"
        path = JSONLDExporter(tmp_path).write(custom)
        assert path == custom
        assert custom.exists()

    def test_write_returns_path(self, tmp_path: Path) -> None:
        result = JSONLDExporter(tmp_path).write()
        assert isinstance(result, Path)
