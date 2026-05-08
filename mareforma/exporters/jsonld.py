"""
exporters/jsonld.py — Export the project ontology as a JSON-LD document.

Vocabulary
----------
Uses a pragmatic mix:
  - schema.org/Dataset  for data sources
  - PROV-O (W3C)        for provenance (wasGeneratedBy, used, Activity)
  - mare: prefix        for mareforma-specific concepts (acquisitionProtocol, Claim)

The result is valid JSON-LD 1.1, readable by any linked-data tool, and
designed to be attached to a preprint or shared as a project URL.

Output structure
----------------
{
  "@context": { ... },
  "@graph": [
    { "@type": "schema:Dataset",  "@id": "mare:source/morphology", ... },
    { "@type": "prov:Activity",   "@id": "mare:transform/morphology.register", ... },
    { "@type": "prov:Entity",     "@id": "mare:artifact/morphology.register", ... },
    { "@type": "mare:Claim",      "@id": "mare:claim/<uuid>", ... }
  ]
}

Data sources
------------
  - mareforma.project.toml  : project metadata and sources
  - graph.db                : transform run provenance (all_transform_runs)
                              and explicit scientific claims (list_claims)

Updated automatically after every successful build and every add-source.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mareforma import __version__


_CONTEXT = {
    "schema": "https://schema.org/",
    "prov": "http://www.w3.org/ns/prov#",
    "mare": "https://mareforma.dev/ns#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    # Shorthand aliases — source / transform / artifact
    "name": "schema:name",
    "description": "schema:description",
    "dateCreated": "schema:dateCreated",
    "format": "schema:encodingFormat",
    "path": "schema:contentUrl",
    "status": "mare:status",
    "protocolFile": "mare:acquisitionProtocol",
    "wasGeneratedBy": "prov:wasGeneratedBy",
    "used": "prov:used",
    "startedAtTime": "prov:startedAtTime",
    "hadDuration": "mare:durationMs",
    "dependsOn": "mare:dependsOn",
    "inputHash": "mare:inputHash",
    "outputHash": "mare:outputHash",
    "sourceHash": "mare:sourceHash",
    "columns": "mare:schemaColumns",
    "shape": "mare:schemaShape",
    # Shorthand aliases — claims
    "claimText": "mare:claimText",
    "classification": "mare:classification",
    "supportLevel": "mare:supportLevel",
    "claimStatus": "mare:claimStatus",
    "sourceName": "mare:sourceName",
    "generatedBy": "mare:generatedBy",
    "supports": "mare:supports",
    "contradicts": "mare:contradicts",
    "comparisonSummary": "mare:comparisonSummary",
    # Shorthand aliases — literature
    "doi": "schema:identifier",
    "datePublished": "schema:datePublished",
    "author": "schema:author",
}


class JSONLDExporter:
    """Converts mareforma.project.toml + graph.db → JSON-LD graph.

    Parameters
    ----------
    root:
        Project root directory.
    """

    def __init__(self, root: Path) -> None:
        self._root = root

    def export(self) -> dict[str, Any]:
        """Build and return the full JSON-LD document as a Python dict."""
        from mareforma.registry import load as load_toml
        from mareforma.db import open_db, all_transform_runs, list_claims

        toml_data = load_toml(self._root)

        conn = open_db(self._root)
        try:
            runs = all_transform_runs(conn)
            claims = list_claims(conn)
        finally:
            conn.close()

        graph: list[dict[str, Any]] = []

        # Project node
        project = toml_data.get("project", {})
        graph.append(self._project_node(project))

        # Source nodes
        sources = toml_data.get("sources", {})
        for source_name, source_cfg in sources.items():
            graph.append(self._source_node(source_name, source_cfg))

        # Transform + artifact nodes from graph.db
        for transform_name, run_data in runs.items():
            graph.append(self._transform_node(transform_name, run_data, sources))
            if run_data.get("status") == "success":
                graph.append(self._artifact_node(transform_name, run_data))

        # Literature nodes from TOML
        literature = toml_data.get("literature", {})
        for doi_key, lit_cfg in literature.items():
            graph.append(self._literature_node(doi_key, lit_cfg))

        # Claim nodes from graph.db
        for claim in claims:
            graph.append(self._claim_node(claim))

        return {
            "@context": _CONTEXT,
            "@graph": graph,
        }

    def write(self, output_path: Path | None = None) -> Path:
        """Write JSON-LD to *output_path* (default: project root/ontology.jsonld).

        Returns the path written.
        """
        if output_path is None:
            output_path = self._root / "ontology.jsonld"

        output_path.parent.mkdir(parents=True, exist_ok=True)
        doc = self.export()
        output_path.write_text(
            json.dumps(doc, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return output_path

    # ------------------------------------------------------------------
    # Node builders
    # ------------------------------------------------------------------

    def _project_node(self, project: dict) -> dict:
        return {
            "@type": "schema:ResearchProject",
            "@id": f"mare:project/{_slug(project.get('name', 'unknown'))}",
            "name": project.get("name", ""),
            "description": project.get("description", ""),
            "dateCreated": project.get("created", ""),
            "mare:mareformaVersion": __version__,
            "mare:exportedAt": datetime.now(timezone.utc).isoformat(),
        }

    def _source_node(self, name: str, cfg: dict) -> dict:
        node: dict[str, Any] = {
            "@type": "schema:Dataset",
            "@id": f"mare:source/{name}",
            "name": name,
            "description": cfg.get("description", ""),
            "format": cfg.get("format", ""),
            "path": cfg.get("path", ""),
            "status": cfg.get("status", "raw"),
            "dateCreated": cfg.get("added", ""),
        }

        acq = cfg.get("acquisition", {})
        if acq:
            node["protocolFile"] = acq.get("protocol_file", "")

        return node

    def _transform_node(
        self,
        name: str,
        run_data: dict,
        sources: dict,
    ) -> dict:
        source_name = name.split(".")[0]
        node: dict[str, Any] = {
            "@type": "prov:Activity",
            "@id": f"mare:transform/{name}",
            "name": name,
            "startedAtTime": run_data.get("timestamp", ""),
            "hadDuration": run_data.get("duration_ms"),
            "status": run_data.get("status", ""),
        }
        if source_name in sources:
            node["used"] = f"mare:source/{source_name}"
        return node

    def _artifact_node(self, transform_name: str, _run_data: dict) -> dict:
        return {
            "@type": "prov:Entity",
            "@id": f"mare:artifact/{transform_name}",
            "name": f"{transform_name} output",
            "wasGeneratedBy": f"mare:transform/{transform_name}",
        }

    def _literature_node(self, doi_key: str, cfg: dict) -> dict:
        return {
            "@type": "schema:ScholarlyArticle",
            "@id": f"mare:literature/{doi_key}",
            "doi": cfg.get("doi", ""),
            "name": cfg.get("title", ""),
            "author": cfg.get("authors", []),
            "datePublished": cfg.get("year", 0),
            "schema:isPartOf": cfg.get("journal", ""),
        }

    def _claim_node(self, claim: dict) -> dict:
        node: dict[str, Any] = {
            "@type": "mare:Claim",
            "@id": f"mare:claim/{claim['claim_id']}",
            "claimText": claim["text"],
            "classification": claim.get("classification", "INFERRED"),
            "supportLevel": claim.get("support_level", "PRELIMINARY"),
            "claimStatus": claim["status"],
            "generatedBy": claim.get("generated_by", "human"),
            "dateCreated": claim["created_at"],
        }
        supports = json.loads(claim.get("supports_json", "[]") or "[]")
        contradicts = json.loads(claim.get("contradicts_json", "[]") or "[]")
        if supports:
            node["supports"] = supports
        if contradicts:
            node["contradicts"] = contradicts
        if claim.get("comparison_summary"):
            node["comparisonSummary"] = claim["comparison_summary"]
        if claim.get("source_name"):
            node["sourceName"] = claim["source_name"]
            node["used"] = f"mare:source/{claim['source_name']}"
        return node


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slug(name: str) -> str:
    return name.lower().replace(" ", "_").replace("/", "_")
