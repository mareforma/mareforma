"""
exporters/jsonld.py — Export claims as a JSON-LD document.

Output structure
----------------
{
  "@context": { ... },
  "@graph": [
    { "@type": "mare:Claim", "@id": "mare:claim/<uuid>", ... }
  ]
}

Vocabulary
----------
  mare: prefix for mareforma-specific concepts
  prov: W3C PROV-O for provenance (wasGeneratedBy, used)
  schema: schema.org for interoperability
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mareforma import __version__


_CONTEXT = {
    "schema": "https://schema.org/",
    "prov":   "http://www.w3.org/ns/prov#",
    "mare":   "https://mareforma.dev/ns#",
    "xsd":    "http://www.w3.org/2001/XMLSchema#",
    "name":            "schema:name",
    "dateCreated":     "schema:dateCreated",
    "used":            "prov:used",
    "wasGeneratedBy":  "prov:wasGeneratedBy",
    "claimText":       "mare:claimText",
    "classification":  "mare:classification",
    "supportLevel":    "mare:supportLevel",
    "claimStatus":     "mare:claimStatus",
    "sourceName":      "mare:sourceName",
    "generatedBy":     "mare:generatedBy",
    "supports":        "mare:supports",
    "contradicts":     "mare:contradicts",
    "comparisonSummary": "mare:comparisonSummary",
    "validatedBy":     "mare:validatedBy",
}


class JSONLDExporter:
    """Export claims from graph.db as a JSON-LD document.

    Parameters
    ----------
    root:
        Project root directory containing .mareforma/graph.db.
    """

    def __init__(self, root: Path) -> None:
        self._root = root

    def export(self) -> dict[str, Any]:
        """Build and return the full JSON-LD document as a Python dict."""
        from mareforma.db import open_db, list_claims

        conn = open_db(self._root)
        try:
            claims = list_claims(conn)
        finally:
            conn.close()

        graph: list[dict[str, Any]] = [
            self._claim_node(c) for c in claims
        ]

        return {
            "@context": _CONTEXT,
            "@graph": graph,
            "mare:exportedAt": datetime.now(timezone.utc).isoformat(),
            "mare:mareformaVersion": __version__,
        }

    def write(self, output_path: Path | None = None) -> Path:
        """Write JSON-LD to *output_path* (default: <root>/ontology.jsonld).

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
        if claim.get("validated_by"):
            node["validatedBy"] = claim["validated_by"]
        return node
