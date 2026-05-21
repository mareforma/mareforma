"""RO-Crate 1.2 export of the epistemic graph (Process Run Crate profile).

Substrate-level adapter. Produces an ``ro-crate-metadata.json`` JSON-LD
document describing the whole graph as a Dataset of CreateAction
entities (one per claim assertion). Each claim's signature envelope is
attached to the CreateAction's ``signature`` property so signatures
travel with the package.

Downstream consumers: Galaxy, EuroScienceGateway, FAIR-EASE, any
RO-Crate-aware FAIR-research tooling.

This is a minimal-viable implementation. Polish targets for follow-on
work:

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
import re
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


# UUID-shape claim_ids only. Federation imports preserve foreign IDs in
# the substrate; this exporter refuses to splice non-UUID values into
# `urn:mareforma:claim:<id>` URIs because they would silently break
# downstream URN parsing and JSON-LD @id resolution.
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
# generated_by is allowed slashes ("model/version/context") and dashes,
# but `#`, whitespace, or shell-meta chars would break JSON-LD @id.
_AGENT_SAFE_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")


def _safe_agent_id(agent: str) -> str:
    """Coerce an agent identifier into a JSON-LD-@id-safe form.

    Replaces any character outside ``[A-Za-z0-9._/-]`` with ``_`` so the
    resulting ``#agent/<sanitised>`` fragment parses correctly in every
    JSON-LD consumer. Idempotent.
    """
    if _AGENT_SAFE_RE.match(agent):
        return agent
    return re.sub(r"[^A-Za-z0-9._/\-]", "_", agent)


def _claim_to_create_action(claim: dict) -> dict[str, Any]:
    """Map a mareforma claim row to an RO-Crate CreateAction entity.

    The CreateAction's ``instrument`` references the asserting agent
    (``generated_by``); ``result`` references the claim text as a
    MediaObject; ``signature`` carries the DSSE envelope if signed.

    Raises :class:`ValueError` on non-UUID claim_id — federation imports
    can land foreign IDs in the substrate and splicing them unvalidated
    into ``urn:mareforma:claim:<id>`` URIs would silently break URN /
    JSON-LD @id parsing downstream.
    """
    claim_id = claim["claim_id"]
    if not isinstance(claim_id, str) or not _UUID_RE.match(claim_id):
        raise ValueError(
            f"RO-Crate export refuses non-UUID claim_id: {claim_id!r}. "
            "Federation-imported foreign IDs must be remapped to UUIDs "
            "before export."
        )
    agent = _safe_agent_id(claim.get("generated_by", "agent") or "agent")
    action: dict[str, Any] = {
        "@id": f"urn:mareforma:claim:{claim_id}",
        "@type": "CreateAction",
        "name": f"claim assertion {claim_id}",
        "agent": {"@id": f"#agent/{agent}"},
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
    # Only UUID-shaped refs are spliced; DOI / arXiv / free-text refs
    # in supports[] are external and intentionally omitted from the
    # JSON-LD @id graph (they have no `urn:mareforma:claim:` identity).
    object_refs: list[dict[str, str]] = []
    supports = claim.get("supports_json")
    if supports:
        try:
            decoded = json.loads(supports)
        except (json.JSONDecodeError, TypeError, ValueError):
            decoded = None
        if isinstance(decoded, list):
            for ref in decoded:
                if isinstance(ref, str) and _UUID_RE.match(ref):
                    object_refs.append(
                        {"@id": f"urn:mareforma:claim:{ref}"}
                    )
    if object_refs:
        action["object"] = object_refs

    # Attach the signed envelope so signatures travel with the package.
    if claim.get("signature_bundle"):
        action["signature"] = claim["signature_bundle"]
    return action


def _claim_to_media_object(claim: dict) -> dict[str, Any]:
    """Map the claim text to a MediaObject entity.

    Claim text is asserter-controlled and passes through mareforma's
    ``sanitize_for_llm`` filter on write (zero-width / RTL / control-
    char stripping). The text is still embedded raw in the JSON-LD
    payload here — RO-Crate consumers that render this surface in a
    browser UI MUST treat it as untrusted (Galaxy / Workbench /
    EuroScienceGateway viewers all do safe-render by default).
    """
    claim_id = claim["claim_id"]
    return {
        "@id": f"#claim-text/{claim_id}",
        "@type": "MediaObject",
        "name": f"claim text {claim_id}",
        "encodingFormat": "text/plain",
        "text": claim.get("text", ""),
    }


def _agent_entities(claims: list[dict]) -> list[dict[str, Any]]:
    """Distinct asserting agents → SoftwareApplication entities.

    Agent identifiers are sanitised via :func:`_safe_agent_id` so a
    foreign or malformed ``generated_by`` value can't poison the
    JSON-LD @id space.
    """
    seen: set[str] = set()
    entities: list[dict[str, Any]] = []
    for c in claims:
        raw = c.get("generated_by", "agent") or "agent"
        agent = _safe_agent_id(raw)
        if agent in seen:
            continue
        seen.add(agent)
        entities.append({
            "@id": f"#agent/{agent}",
            "@type": "SoftwareApplication",
            "name": raw,
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
