"""RO-Crate 1.2 export of the epistemic graph (Process Run Crate profile).

Substrate-level adapter. Produces an ``ro-crate-metadata.json`` JSON-LD
document describing the whole graph as a Dataset of CreateAction
entities (one per claim assertion). Each claim's signature envelope is
attached to the CreateAction's ``signature`` property so signatures
travel with the package.

Downstream consumers: Galaxy, EuroScienceGateway, FAIR-EASE, any
RO-Crate-aware FAIR-research tooling.

This is a minimal-viable v0.3.1 implementation. Polish targets for
follow-on work (Phase 6 / v0.3.2):

* RO-Crate zip writer (bundle the JSON-LD + claim text payload files +
  signed-envelope sidecars into a single ``.crate.zip``).
* Software / SoftwareApplication entities for the agents (one per
  distinct ``generated_by`` value) so downstream tools can audit the
  toolchain.
* Embedding the GRADE EvidenceVector as ``additionalProperty``
  PropertyValue rows for quality-of-evidence downstream filters.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


__all__ = [
    "build_crate",
    "RO_CRATE_PROFILE",
    "RO_CRATE_CONTEXT",
    "PROCESS_RUN_CRATE_PROFILE",
]


RO_CRATE_PROFILE = "https://w3id.org/ro/crate/1.2"
RO_CRATE_CONTEXT = "https://w3id.org/ro/crate/1.2/context"
PROCESS_RUN_CRATE_PROFILE = "https://w3id.org/ro/wfrun/process/0.5"


def _claim_to_create_action(claim: dict) -> dict[str, Any]:
    """Map a mareforma claim row to an RO-Crate CreateAction entity.

    The CreateAction's ``instrument`` references the asserting agent
    (``generated_by``); ``result`` references the claim text as a
    MediaObject; ``signature`` carries the DSSE envelope if signed.
    """
    claim_id = claim["claim_id"]
    action: dict[str, Any] = {
        "@id": f"urn:mareforma:claim:{claim_id}",
        "@type": "CreateAction",
        "name": f"claim assertion {claim_id}",
        "agent": {"@id": f"#agent/{claim.get('generated_by', 'agent')}"},
        "result": [{"@id": f"#claim-text/{claim_id}"}],
        "actionStatus": {"@id": "http://schema.org/CompletedActionStatus"},
        "startTime": claim.get("created_at"),
        "endTime": claim.get("updated_at"),
    }
    extra: list[dict[str, str]] = []
    if claim.get("classification"):
        extra.append({
            "@type": "PropertyValue",
            "name": "classification",
            "value": claim["classification"],
        })
    if claim.get("support_level"):
        extra.append({
            "@type": "PropertyValue",
            "name": "support_level",
            "value": claim["support_level"],
        })
    if claim.get("status"):
        extra.append({
            "@type": "PropertyValue",
            "name": "status",
            "value": claim["status"],
        })
    if claim.get("source_name"):
        extra.append({
            "@type": "PropertyValue",
            "name": "source_name",
            "value": claim["source_name"],
        })
    if claim.get("artifact_hash"):
        extra.append({
            "@type": "PropertyValue",
            "name": "artifact_hash",
            "value": claim["artifact_hash"],
        })
    if extra:
        action["additionalProperty"] = extra

    # supports[] / contradicts[] → object / result links to the
    # referenced claims (RO-Crate references; consumers walk @id).
    object_refs: list[dict[str, str]] = []
    supports = claim.get("supports_json")
    if supports:
        try:
            for ref in json.loads(supports):
                if isinstance(ref, str) and ref:
                    object_refs.append(
                        {"@id": f"urn:mareforma:claim:{ref}"}
                    )
        except (json.JSONDecodeError, TypeError):
            pass
    if object_refs:
        action["object"] = object_refs

    # Attach the signed envelope so signatures travel with the package.
    if claim.get("signature_bundle"):
        action["signature"] = claim["signature_bundle"]
    return action


def _claim_to_media_object(claim: dict) -> dict[str, Any]:
    """Map the claim text to a MediaObject entity."""
    claim_id = claim["claim_id"]
    return {
        "@id": f"#claim-text/{claim_id}",
        "@type": "MediaObject",
        "name": f"claim text {claim_id}",
        "encodingFormat": "text/plain",
        "text": claim.get("text", ""),
    }


def _agent_entities(claims: list[dict]) -> list[dict[str, Any]]:
    """Distinct asserting agents → SoftwareApplication entities."""
    seen: set[str] = set()
    entities: list[dict[str, Any]] = []
    for c in claims:
        agent = c.get("generated_by", "agent")
        if agent in seen:
            continue
        seen.add(agent)
        entities.append({
            "@id": f"#agent/{agent}",
            "@type": "SoftwareApplication",
            "name": agent,
        })
    return entities


def build_crate(root: Path) -> dict[str, Any]:
    """Build an ``ro-crate-metadata.json`` dict from the local graph.

    Parameters
    ----------
    root
        Project root (the directory holding ``.mareforma/graph.db``).

    Returns
    -------
    dict
        An RO-Crate 1.2 metadata document, ready for ``json.dumps`` or
        for writing to ``ro-crate-metadata.json`` inside an RO-Crate
        directory.
    """
    from mareforma.db import open_db, list_claims

    db_path = root / ".mareforma" / "graph.db"
    if not db_path.exists():
        raise FileNotFoundError(
            f"No epistemic graph found at {db_path}. "
            "Run `mareforma bootstrap` to initialize one."
        )

    conn = open_db(root)
    try:
        claims = list_claims(conn)
    finally:
        conn.close()

    graph: list[dict[str, Any]] = [
        {
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "conformsTo": [
                {"@id": RO_CRATE_PROFILE},
                {"@id": PROCESS_RUN_CRATE_PROFILE},
            ],
            "about": {"@id": "./"},
        },
        {
            "@id": "./",
            "@type": "Dataset",
            "name": "Mareforma epistemic graph",
            "description": (
                "Signed epistemic graph exported from a mareforma "
                "project. Each claim is a CreateAction; the signed "
                "envelope is attached as the action's signature."
            ),
            "datePublished": claims[-1]["updated_at"] if claims else None,
            "hasPart": [],
        },
    ]

    # Agent entities first (referenced by CreateAction.agent).
    graph.extend(_agent_entities(claims))

    has_part: list[dict[str, str]] = []
    for c in claims:
        graph.append(_claim_to_media_object(c))
        graph.append(_claim_to_create_action(c))
        has_part.append({"@id": f"urn:mareforma:claim:{c['claim_id']}"})

    # Root dataset hasPart references each CreateAction.
    root_entity = next(e for e in graph if e["@id"] == "./")
    root_entity["hasPart"] = has_part

    return {
        "@context": RO_CRATE_CONTEXT,
        "@graph": graph,
    }
