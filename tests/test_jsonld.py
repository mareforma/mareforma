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
    def test_missing_graph_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="No epistemic graph found"):
            JSONLDExporter(tmp_path).export()
        assert (tmp_path / ".mareforma").exists() is False

    def test_context_present(self, tmp_path: Path) -> None:
        _open(tmp_path).close()
        doc = JSONLDExporter(tmp_path).export()
        assert "@context" in doc
        ctx = doc["@context"]
        assert "schema" in ctx
        assert "mare" in ctx
        # PROV-O references are intentionally absent here: this export
        # is mareforma-native. A real PROV-O view ships separately via
        # ``mareforma export --format=prov-o``.
        assert "prov" not in ctx

    def test_export_media_type(self, tmp_path: Path) -> None:
        from mareforma.exporters.jsonld import EXPORT_MEDIA_TYPE
        _open(tmp_path).close()
        doc = JSONLDExporter(tmp_path).export()
        assert doc["@type"] == "mare:Graph"
        assert doc["mare:mediaType"] == EXPORT_MEDIA_TYPE
        assert EXPORT_MEDIA_TYPE == "application/x-mareforma-graph+json"

    def test_context_has_claim_vocabulary(self, tmp_path: Path) -> None:
        _open(tmp_path).close()
        doc = JSONLDExporter(tmp_path).export()
        ctx = doc["@context"]
        assert "claimText" in ctx
        assert "classification" in ctx
        assert "supportLevel" in ctx
        assert "claimStatus" in ctx

    def test_every_claim_node_key_survives_expansion(self, tmp_path: Path) -> None:
        # A key that is neither @-prefixed, defined in @context, nor an IRI
        # with a prefix is dropped by a JSON-LD processor.
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Target T is elevated in condition C")
        finally:
            conn.close()
        doc = JSONLDExporter(tmp_path).export()
        ctx = doc["@context"]
        node = doc["@graph"][0]
        undefined = [
            k for k in node
            if not k.startswith("@") and k not in ctx and ":" not in k
        ]
        assert undefined == []

    def test_evidence_is_typed_json(self, tmp_path: Path) -> None:
        # Without @json the object survives but every key inside it that
        # the context does not define is dropped on expansion.
        _open(tmp_path).close()
        doc = JSONLDExporter(tmp_path).export()
        ctx = doc["@context"]
        assert ctx["@version"] == 1.1
        assert ctx["evidence"]["@type"] == "@json"

    def test_graph_present(self, tmp_path: Path) -> None:
        _open(tmp_path).close()
        doc = JSONLDExporter(tmp_path).export()
        assert "@graph" in doc
        assert isinstance(doc["@graph"], list)

    def test_empty_graph_when_no_claims(self, tmp_path: Path) -> None:
        _open(tmp_path).close()
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
        _open(tmp_path).close()
        path = JSONLDExporter(tmp_path).write()
        assert path.exists()
        assert path.name == "ontology.jsonld"

    def test_written_file_is_valid_json(self, tmp_path: Path) -> None:
        _open(tmp_path).close()
        path = JSONLDExporter(tmp_path).write()
        doc = json.loads(path.read_text(encoding="utf-8"))
        assert "@context" in doc
        assert "@graph" in doc

    def test_custom_output_path(self, tmp_path: Path) -> None:
        _open(tmp_path).close()
        custom = tmp_path / "exports" / "my_ontology.jsonld"
        path = JSONLDExporter(tmp_path).write(custom)
        assert path == custom
        assert custom.exists()

    def test_write_returns_path(self, tmp_path: Path) -> None:
        _open(tmp_path).close()
        result = JSONLDExporter(tmp_path).write()
        assert isinstance(result, Path)


class TestCallerSuppliedRowsRefuseSubstitution:
    """A caller-supplied row that omits a field is refused, not fabricated.

    Rows from ``list_claims`` always carry ``support_level`` and
    ``classification`` (both columns are NOT NULL), so the normal export is
    unaffected. A caller that hands over its own row missing one of them used to
    get a fabricated ``PRELIMINARY`` / ``INFERRED`` the record never carried;
    now the export refuses, matching the sibling fields that hard-index.
    """

    def _one_row(self, tmp_path: Path) -> dict:
        from mareforma.db import list_claims

        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "a claim")
            rows = list_claims(conn)
        finally:
            conn.close()
        assert rows, "expected one claim to export"
        return dict(rows[0])

    def test_export_refuses_row_missing_support_level(self, tmp_path: Path) -> None:
        row = self._one_row(tmp_path)
        del row["support_level"]
        with pytest.raises(KeyError, match="support_level"):
            JSONLDExporter(tmp_path).export(claims=[row])

    def test_export_refuses_row_missing_classification(self, tmp_path: Path) -> None:
        row = self._one_row(tmp_path)
        del row["classification"]
        with pytest.raises(KeyError, match="classification"):
            JSONLDExporter(tmp_path).export(claims=[row])
