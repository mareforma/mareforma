"""
tests/test_jsonld.py — unit tests for exporters/jsonld.py.

Covers:
  - @context keys present (including claim vocabulary)
  - @graph contains project node
  - @graph contains source nodes with correct fields
  - transform nodes from graph.db included in graph
  - artifact nodes created for successful transforms
  - claim nodes included for recorded claims
  - write() creates ontology.jsonld at correct path
  - write(output_path=...) writes to custom path
  - empty project (no sources, no db runs) produces valid doc
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mareforma.db import add_claim, begin_run, end_run, open_db
from mareforma.exporters.jsonld import JSONLDExporter
from mareforma.initializer import initialize
from mareforma.registry import add_source


def _init_project(root: Path, name: str = "test_project") -> Path:
    initialize(root)
    toml = root / "mareforma.project.toml"
    text = toml.read_text()
    text = text.replace('description = ""', 'description = "A test project"', 1)
    toml.write_text(text)
    return root


def _write_db_run(root: Path, name: str, status: str = "success") -> None:
    """Write a transform run directly into graph.db for test setup."""
    conn = open_db(root)
    try:
        run_id = "test-run-" + name.replace(".", "-")
        begin_run(conn, run_id, name, "ihash", "shash")
        end_run(conn, run_id, status=status, output_hash="ohash", duration_ms=42)
    finally:
        conn.close()


class TestContextAndStructure:
    def test_context_present(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        doc = JSONLDExporter(tmp_path).export()
        assert "@context" in doc
        ctx = doc["@context"]
        assert "schema" in ctx
        assert "prov" in ctx
        assert "mare" in ctx

    def test_context_has_claim_vocabulary(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        doc = JSONLDExporter(tmp_path).export()
        ctx = doc["@context"]
        assert "claimText" in ctx
        assert "confidence" in ctx
        assert "claimStatus" in ctx

    def test_graph_present(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        doc = JSONLDExporter(tmp_path).export()
        assert "@graph" in doc
        assert isinstance(doc["@graph"], list)

    def test_graph_has_project_node(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        doc = JSONLDExporter(tmp_path).export()
        project_nodes = [
            n for n in doc["@graph"]
            if n.get("@type") == "schema:ResearchProject"
        ]
        assert len(project_nodes) == 1
        assert project_nodes[0].get("name") is not None


class TestSourceNodes:
    def test_source_appears_in_graph(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        raw = tmp_path / "data" / "morph" / "raw"
        raw.mkdir(parents=True)
        add_source(tmp_path, "morph", str(raw), "Neuron skeletons")

        doc = JSONLDExporter(tmp_path).export()
        datasets = [n for n in doc["@graph"] if n.get("@type") == "schema:Dataset"]
        assert any(n.get("name") == "morph" for n in datasets)

    def test_source_description_in_node(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        raw = tmp_path / "data" / "morph" / "raw"
        raw.mkdir(parents=True)
        add_source(tmp_path, "morph", str(raw), "My description")

        doc = JSONLDExporter(tmp_path).export()
        node = next(
            n for n in doc["@graph"]
            if n.get("@type") == "schema:Dataset" and n.get("name") == "morph"
        )
        assert node["description"] == "My description"

    def test_multiple_sources_all_present(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        for src in ["src_a", "src_b", "src_c"]:
            raw = tmp_path / "data" / src / "raw"
            raw.mkdir(parents=True)
            add_source(tmp_path, src, str(raw), src)

        doc = JSONLDExporter(tmp_path).export()
        dataset_names = {
            n["name"] for n in doc["@graph"]
            if n.get("@type") == "schema:Dataset"
        }
        assert {"src_a", "src_b", "src_c"}.issubset(dataset_names)

    def test_source_id_format(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        raw = tmp_path / "data" / "morph" / "raw"
        raw.mkdir(parents=True)
        add_source(tmp_path, "morph", str(raw), "test")

        doc = JSONLDExporter(tmp_path).export()
        node = next(
            n for n in doc["@graph"]
            if n.get("@type") == "schema:Dataset" and n.get("name") == "morph"
        )
        assert node["@id"] == "mare:source/morph"


class TestTransformNodes:
    def test_transform_node_in_graph(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        _write_db_run(tmp_path, "morph.load")

        doc = JSONLDExporter(tmp_path).export()
        activities = [n for n in doc["@graph"] if n.get("@type") == "prov:Activity"]
        assert any(n.get("name") == "morph.load" for n in activities)

    def test_transform_node_id_format(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        _write_db_run(tmp_path, "morph.load")

        doc = JSONLDExporter(tmp_path).export()
        node = next(
            n for n in doc["@graph"]
            if n.get("@type") == "prov:Activity" and n.get("name") == "morph.load"
        )
        assert node["@id"] == "mare:transform/morph.load"

    def test_artifact_node_created_for_success(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        _write_db_run(tmp_path, "morph.load", status="success")

        doc = JSONLDExporter(tmp_path).export()
        entities = [n for n in doc["@graph"] if n.get("@type") == "prov:Entity"]
        assert any("morph.load" in n.get("@id", "") for n in entities)

    def test_no_artifact_node_for_failed(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        _write_db_run(tmp_path, "morph.bad", status="failed")

        doc = JSONLDExporter(tmp_path).export()
        entities = [n for n in doc["@graph"] if n.get("@type") == "prov:Entity"]
        assert not any("morph.bad" in n.get("@id", "") for n in entities)

    def test_artifact_was_generated_by_link(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        _write_db_run(tmp_path, "morph.load")

        doc = JSONLDExporter(tmp_path).export()
        entity = next(
            n for n in doc["@graph"]
            if n.get("@type") == "prov:Entity"
        )
        assert entity.get("wasGeneratedBy") == "mare:transform/morph.load"


class TestClaimNodes:
    def test_claim_appears_in_graph(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        conn = open_db(tmp_path)
        try:
            add_claim(conn, tmp_path, "L2/3 neurons have a mean axon extent of 0.7 mm (n=312)")
        finally:
            conn.close()

        doc = JSONLDExporter(tmp_path).export()
        claims = [n for n in doc["@graph"] if n.get("@type") == "mare:Claim"]
        assert len(claims) == 1

    def test_claim_node_has_text(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        conn = open_db(tmp_path)
        try:
            add_claim(conn, tmp_path, "Test claim text", confidence="preliminary")
        finally:
            conn.close()

        doc = JSONLDExporter(tmp_path).export()
        claim_node = next(
            n for n in doc["@graph"] if n.get("@type") == "mare:Claim"
        )
        assert claim_node["claimText"] == "Test claim text"
        assert claim_node["confidence"] == "preliminary"

    def test_claim_id_format(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        conn = open_db(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "Some claim")
        finally:
            conn.close()

        doc = JSONLDExporter(tmp_path).export()
        claim_node = next(
            n for n in doc["@graph"] if n.get("@type") == "mare:Claim"
        )
        assert claim_node["@id"] == f"mare:claim/{claim_id}"

    def test_claim_with_source_has_used_link(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        raw = tmp_path / "data" / "morph" / "raw"
        raw.mkdir(parents=True)
        add_source(tmp_path, "morph", str(raw), "test")

        conn = open_db(tmp_path)
        try:
            add_claim(conn, tmp_path, "Claim about morphology", source_name="morph")
        finally:
            conn.close()

        doc = JSONLDExporter(tmp_path).export()
        claim_node = next(
            n for n in doc["@graph"] if n.get("@type") == "mare:Claim"
        )
        assert claim_node.get("used") == "mare:source/morph"

    def test_multiple_claims_all_present(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        conn = open_db(tmp_path)
        try:
            add_claim(conn, tmp_path, "Claim one")
            add_claim(conn, tmp_path, "Claim two")
            add_claim(conn, tmp_path, "Claim three")
        finally:
            conn.close()

        doc = JSONLDExporter(tmp_path).export()
        claims = [n for n in doc["@graph"] if n.get("@type") == "mare:Claim"]
        assert len(claims) == 3


class TestFileOutput:
    def test_write_creates_file(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        exporter = JSONLDExporter(tmp_path)
        path = exporter.write()
        assert path.exists()
        assert path.name == "ontology.jsonld"

    def test_written_file_is_valid_json(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        path = JSONLDExporter(tmp_path).write()
        doc = json.loads(path.read_text(encoding="utf-8"))
        assert "@context" in doc
        assert "@graph" in doc

    def test_custom_output_path(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        custom = tmp_path / "exports" / "my_ontology.jsonld"
        path = JSONLDExporter(tmp_path).write(custom)
        assert path == custom
        assert custom.exists()

    def test_write_returns_path(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        result = JSONLDExporter(tmp_path).write()
        assert isinstance(result, Path)

    def test_empty_project_still_valid(self, tmp_path: Path) -> None:
        _init_project(tmp_path)
        doc = JSONLDExporter(tmp_path).export()
        assert "@context" in doc
        assert "@graph" in doc
        assert len(doc["@graph"]) >= 1  # at least the project node
