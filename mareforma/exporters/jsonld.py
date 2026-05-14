"""
exporters/jsonld.py — Export claims as a mareforma-native JSON-LD document.

Output structure
----------------
{
  "@context": { ... },
  "@graph": [
    { "@type": "mare:Claim", "@id": "mare:claim/<uuid>", ... }
  ],
  "@type": "mare:Graph",
  "mare:mediaType": "application/x-mareforma-graph+json"
}

Vocabulary
----------
The export uses mareforma's own ``mare:`` vocabulary plus schema.org
for cross-tool friendliness. PROV-O references were removed in v0.3.0
— the previous JSON-LD context name-dropped ``prov:wasGeneratedBy``
and ``prov:used`` without populating the full PROV-O graph (no
prov:Activity, no prov:wasAssociatedWith, no model identity, no
prompt/response hashes). Consumers integrating against the export
should treat it as a mareforma-native format with media type
``application/x-mareforma-graph+json``, not as a standards-compliant
PROV-O graph. See ``docs/reference/export-format.md`` for the schema.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mareforma import __version__


# Media type for the exported graph. Distinct from PROV-O — see module
# docstring for the scoping rationale.
EXPORT_MEDIA_TYPE = "application/x-mareforma-graph+json"


_CONTEXT = {
    "schema": "https://schema.org/",
    "mare":   "https://mareforma.dev/ns#",
    "xsd":    "http://www.w3.org/2001/XMLSchema#",
    "name":            "schema:name",
    "dateCreated":     "schema:dateCreated",
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
    "usedSource":      "mare:usedSource",
    "artifactHash":    "mare:artifactHash",
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
            "@type": "mare:Graph",
            "@graph": graph,
            "mare:mediaType": EXPORT_MEDIA_TYPE,
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
        # Always include every SIGNED_FIELDS member + the GRADE
        # EvidenceVector so a downstream consumer (e.g. SCITT bundle
        # verification) can re-derive the canonical Statement v1 bytes
        # from the node alone. Optional fields use null/[] defaults to
        # match canonical_statement's expected shape.
        supports = json.loads(claim.get("supports_json", "[]") or "[]")
        contradicts = json.loads(claim.get("contradicts_json", "[]") or "[]")
        try:
            evidence_dict = json.loads(claim.get("evidence_json") or "{}")
        except (ValueError, TypeError):
            evidence_dict = {}
        node: dict[str, Any] = {
            "@type": "mare:Claim",
            "@id": f"mare:claim/{claim['claim_id']}",
            "claimText": claim["text"],
            "classification": claim.get("classification", "INFERRED"),
            "supportLevel": claim.get("support_level", "PRELIMINARY"),
            "claimStatus": claim["status"],
            "generatedBy": claim.get("generated_by", "agent"),
            "dateCreated": claim["created_at"],
            "supports": supports,
            "contradicts": contradicts,
            "sourceName": claim.get("source_name"),
            "artifactHash": claim.get("artifact_hash"),
            "evidence": evidence_dict,
        }
        if claim.get("comparison_summary"):
            node["comparisonSummary"] = claim["comparison_summary"]
        if claim.get("source_name"):
            node["usedSource"] = f"mare:source/{claim['source_name']}"
        if claim.get("validated_by"):
            node["validatedBy"] = claim["validated_by"]
        return node
