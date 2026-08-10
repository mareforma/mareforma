"""
exporters/jsonld.py: Export claims as a mareforma-native JSON-LD document.

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
for cross-tool friendliness. PROV-O references were removed currently.
The previous JSON-LD context name-dropped ``prov:wasGeneratedBy``
and ``prov:used`` without populating the full PROV-O graph (no
prov:Activity, no prov:wasAssociatedWith, no model identity, no
prompt/response hashes). Consumers integrating against the export
should treat it as a mareforma-native format with media type
``application/x-mareforma-graph+json``, not as a standards-compliant
PROV-O graph. See ``docs/for-agents/agents.mdx`` under "Export and
signed bundles" for the shape.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mareforma import __version__
from mareforma._atomic import atomic_write_text


# Media type for the exported graph. Distinct from PROV-O, see module
# docstring for the scoping rationale.
EXPORT_MEDIA_TYPE = "application/x-mareforma-graph+json"


_CONTEXT = {
    # 1.1 is required for the ``@json`` type used by ``evidence`` below.
    "@version": 1.1,
    "schema": "https://schema.org/",
    "mare":   "urn:mareforma:ns:",
    "xsd":    "http://www.w3.org/2001/XMLSchema#",
    "name":            "schema:name",
    "dateCreated":     "schema:dateCreated",
    "claimText":       "mare:claimText",
    "classification":  "mare:classification",
    "supportLevel":    "mare:supportLevel",
    "claimStatus":     "mare:claimStatus",
    "sourceName":      "mare:sourceName",
    "generatedBy":     "mare:generatedBy",
    # Flat list of strings (back-compat): exactly what's stored in
    # supports_json / contradicts_json. Mixed types, claim_ids, DOIs,
    # external refs, appear in arbitrary order.
    "supports":        "mare:supports",
    "contradicts":     "mare:contradicts",
    # Typed buckets: every entry from supports/contradicts also appears
    # under the matching typed predicate, so a downstream consumer can
    # distinguish "graph-node edges" from "external citations" without
    # re-running the classification regex.
    "supportsClaim":      "mare:supportsClaim",
    "supportsDoi":        "mare:supportsDoi",
    "supportsReference":  "mare:supportsReference",
    "contradictsClaim":      "mare:contradictsClaim",
    "contradictsDoi":        "mare:contradictsDoi",
    "contradictsReference":  "mare:contradictsReference",
    "comparisonSummary": "mare:comparisonSummary",
    "validatedBy":     "mare:validatedBy",
    "usedSource":      "mare:usedSource",
    "artifactHash":    "mare:artifactHash",
    # The evidence vector is an open object: GRADE domains, upgrade flags
    # and grounding fields. ``@json`` keeps it whole through expansion,
    # where a plain term mapping would drop every key inside it.
    "evidence":        {"@id": "mare:evidence", "@type": "@json"},
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

    def export(self, claims: "list[dict] | None" = None) -> dict[str, Any]:
        """Build and return the full JSON-LD document as a Python dict.

        ``claims`` lets a caller that already holds the graph's rows hand them
        over instead of paying a second open and a second verify-on-read pass
        over every row (that flag costs a signature re-verification and a
        corroboration probe per high-trust claim). The rows must come from
        :func:`mareforma.db.list_claims`; ``refuse_unverified_claims`` rejects
        any that carry no verify-on-read result, so this cannot become a way
        around the gate below. Defaults to reading them here.

        Raises ``FileNotFoundError`` if *root* holds no graph: ``open_db``
        would otherwise create one and return an empty export as success.
        Raises ``UnverifiedClaimError`` if any claim failed verify-on-read: an
        export carries the support level off the machine, so it must not
        publish a level the signature no longer backs.
        """
        from mareforma.db import open_db, list_claims, refuse_unverified_claims

        if claims is None:
            db_path = self._root / ".mareforma" / "graph.db"
            if not db_path.exists():
                raise FileNotFoundError(
                    f"No epistemic graph found at {db_path}. "
                    "Run `mareforma bootstrap` to initialize one."
                )

            conn = open_db(self._root)
            try:
                claims = list_claims(conn)
            finally:
                conn.close()
        refuse_unverified_claims(claims)

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
        atomic_write_text(
            output_path, json.dumps(doc, indent=2, ensure_ascii=False),
        )
        return output_path

    # ------------------------------------------------------------------
    # Node builders
    # ------------------------------------------------------------------

    def _claim_node(self, claim: dict) -> dict:
        # Always include every SIGNED_FIELDS member + the evidence
        # vector so a downstream consumer (e.g. export bundle
        # verification) can re-derive the canonical Statement v1 bytes
        # from the node alone. Optional fields use null/[] defaults to
        # match canonical_statement's expected shape.
        supports = json.loads(claim.get("supports_json", "[]") or "[]")
        contradicts = json.loads(claim.get("contradicts_json", "[]") or "[]")
        try:
            evidence_dict = json.loads(claim.get("evidence_json") or "{}")
        except (ValueError, TypeError):
            evidence_dict = {}

        # 215: emit typed buckets alongside the flat list. The flat
        # ``supports`` / ``contradicts`` arrays stay byte-identical to
        # what was signed (and to claims.toml's round-tripped copy), so
        # the canonical_statement digest still matches. The typed
        # arrays are derived view: a consumer that knows about the
        # typed predicates can route on them; a consumer that doesn't
        # falls back to the flat list and re-classifies if it cares.
        from mareforma.db import (
            SUPPORT_TYPE_CLAIM,
            SUPPORT_TYPE_DOI,
            SUPPORT_TYPE_EXTERNAL,
            classify_supports,
        )

        def _split(entries: list[str]) -> tuple[list[str], list[str], list[str]]:
            claims_b: list[str] = []
            dois_b: list[str] = []
            refs_b: list[str] = []
            for typed in classify_supports(entries):
                t = typed["type"]
                v = typed["value"]
                if t == SUPPORT_TYPE_CLAIM:
                    claims_b.append(v)
                elif t == SUPPORT_TYPE_DOI:
                    dois_b.append(v)
                else:
                    assert t == SUPPORT_TYPE_EXTERNAL
                    refs_b.append(v)
            return claims_b, dois_b, refs_b

        sup_claims, sup_dois, sup_refs = _split(supports)
        con_claims, con_dois, con_refs = _split(contradicts)

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
            "supportsClaim": sup_claims,
            "supportsDoi": sup_dois,
            "supportsReference": sup_refs,
            "contradictsClaim": con_claims,
            "contradictsDoi": con_dois,
            "contradictsReference": con_refs,
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
