"""PROV-O JSON-LD export of the epistemic graph.

W3C PROV Ontology (https://www.w3.org/TR/prov-o/) view of the local
graph. Each claim becomes a ``prov:Entity``; its assertion is a
``prov:Activity`` with ``prov:wasGeneratedBy`` from entity → activity
and ``prov:wasAssociatedWith`` from activity → agent. ``supports[]``
links materialise as ``prov:wasDerivedFrom`` edges (entity → entity).
Validation events become a second activity that
``prov:wasAssociatedWith`` the validator agent. The whole document is
PROV-O JSON-LD with the standard ``prov:`` namespace prefix.

The exporter is *one-way*. Re-importing PROV-O back into mareforma is
out of scope — the PROV model carries less than the signed substrate
(no DSSE envelopes, no GRADE evidence vector, no hash chain), so a
round-trip through PROV-O would drop integrity surface.

The four-invariant validator :func:`validate_prov_o` runs over the
exported document and raises :class:`ProvOValidationError` on
structural violations. It is a hand-rolled schema check, not a full
SHACL or OWL validator — the four invariants are the minimum a
consumer needs to walk the document without nil-pointer surprises.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


__all__ = [
    "build_prov_o",
    "validate_prov_o",
    "ProvOValidationError",
    "PROV_CONTEXT",
]


PROV_CONTEXT = {
    "prov": "http://www.w3.org/ns/prov#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "mareforma": "urn:mareforma:",
}


_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_AGENT_SAFE_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")


class ProvOValidationError(ValueError):
    """Raised when a PROV-O document violates the four export invariants."""

    def __init__(self, message: str, *, invariant: str | None = None) -> None:
        super().__init__(message)
        self.invariant = invariant


def _safe_agent_id(agent: str) -> str:
    if _AGENT_SAFE_RE.match(agent):
        return agent
    return re.sub(r"[^A-Za-z0-9._/\-]", "_", agent)


def _entity_id(claim_id: str) -> str:
    return f"mareforma:claim:{claim_id}"


def _activity_id(claim_id: str, kind: str) -> str:
    return f"mareforma:activity:{kind}:{claim_id}"


def _agent_id(generated_by: str) -> str:
    return f"mareforma:agent:{_safe_agent_id(generated_by)}"


def _validator_id(keyid: str) -> str:
    return f"mareforma:validator:{keyid}"


def build_prov_o(root: Path, claim_id: str | None = None) -> dict[str, Any]:
    """Build a PROV-O JSON-LD document from the local graph.

    Parameters
    ----------
    root
        Project root (the directory holding ``.mareforma/graph.db``).
    claim_id
        If given, restricts the document to *claim_id* and its
        transitive ``supports[]`` ancestors. If ``None``, exports the
        whole graph.

    Returns
    -------
    dict
        A PROV-O JSON-LD document with ``@context``, ``@graph``.

    Raises
    ------
    FileNotFoundError
        If no graph database exists at the expected path.
    ValueError
        If *claim_id* is supplied but does not exist in the graph.
    """
    from mareforma.db import open_db, list_claims, get_claim

    db_path = root / ".mareforma" / "graph.db"
    if not db_path.exists():
        raise FileNotFoundError(
            f"No epistemic graph found at {db_path}. "
            "Run `mareforma bootstrap` to initialize one."
        )

    conn = open_db(root)
    try:
        if claim_id is None:
            claims = list_claims(conn)
        else:
            focal = get_claim(conn, claim_id)
            if focal is None:
                raise ValueError(
                    f"claim_id {claim_id!r} not found in graph"
                )
            # Walk transitive supports[] to collect ancestors.
            claims_by_id: dict[str, dict] = {focal["claim_id"]: focal}
            frontier: list[str] = [focal["claim_id"]]
            while frontier:
                current = frontier.pop()
                row = claims_by_id[current]
                try:
                    refs = json.loads(row.get("supports_json") or "[]")
                except (json.JSONDecodeError, TypeError):
                    refs = []
                if not isinstance(refs, list):
                    refs = []
                for ref in refs:
                    if (
                        isinstance(ref, str)
                        and _UUID_RE.match(ref)
                        and ref not in claims_by_id
                    ):
                        ancestor = get_claim(conn, ref)
                        if ancestor is not None:
                            claims_by_id[ref] = ancestor
                            frontier.append(ref)
            claims = sorted(
                claims_by_id.values(),
                key=lambda c: c.get("created_at") or "",
            )
    finally:
        conn.close()

    graph: list[dict[str, Any]] = []
    seen_agents: set[str] = set()
    seen_validators: set[str] = set()

    for claim in claims:
        cid = claim["claim_id"]
        agent = claim.get("generated_by") or "agent"
        agent_safe = _safe_agent_id(agent)

        if agent_safe not in seen_agents:
            graph.append({
                "@id": _agent_id(agent),
                "@type": "prov:Agent",
                "prov:label": agent,
            })
            seen_agents.add(agent_safe)

        # Entity: the claim itself.
        entity: dict[str, Any] = {
            "@id": _entity_id(cid),
            "@type": "prov:Entity",
            "prov:label": (claim.get("text") or "")[:120],
            "prov:wasGeneratedBy": {
                "@id": _activity_id(cid, "assertion"),
            },
            "prov:wasAttributedTo": {"@id": _agent_id(agent)},
        }
        # Derivation: each UUID-shaped supports[] entry → wasDerivedFrom.
        try:
            refs = json.loads(claim.get("supports_json") or "[]")
        except (json.JSONDecodeError, TypeError):
            refs = []
        if isinstance(refs, list):
            derivations = [
                {"@id": _entity_id(r)}
                for r in refs
                if isinstance(r, str) and _UUID_RE.match(r)
            ]
            if derivations:
                entity["prov:wasDerivedFrom"] = derivations
        graph.append(entity)

        # Activity: the assertion event.
        graph.append({
            "@id": _activity_id(cid, "assertion"),
            "@type": "prov:Activity",
            "prov:startedAtTime": claim.get("created_at"),
            "prov:endedAtTime": claim.get("updated_at"),
            "prov:wasAssociatedWith": {"@id": _agent_id(agent)},
        })

        # Validation activity + validator agent (when ESTABLISHED).
        if claim.get("validator_keyid"):
            vkeyid = claim["validator_keyid"]
            if vkeyid not in seen_validators:
                graph.append({
                    "@id": _validator_id(vkeyid),
                    "@type": "prov:Agent",
                    "prov:label": claim.get("validated_by") or vkeyid,
                })
                seen_validators.add(vkeyid)
            graph.append({
                "@id": _activity_id(cid, "validation"),
                "@type": "prov:Activity",
                "prov:startedAtTime": claim.get("validated_at"),
                "prov:wasAssociatedWith": {"@id": _validator_id(vkeyid)},
                "prov:used": {"@id": _entity_id(cid)},
            })

    return {
        "@context": PROV_CONTEXT,
        "@graph": graph,
    }


def validate_prov_o(doc: dict[str, Any]) -> None:
    """Hand-rolled four-invariant validator for a PROV-O document.

    The invariants checked:

    1. Every ``prov:Entity`` carries at least one ``prov:wasGeneratedBy``
       reference that resolves to a ``prov:Activity`` in the graph.
    2. Every ``prov:Activity`` carries ``prov:wasAssociatedWith``
       referencing a ``prov:Agent`` in the graph.
    3. Every ``prov:wasDerivedFrom`` link runs Entity → Entity (no
       Agent or Activity endpoints).
    4. Every ``prov:wasAttributedTo`` link runs Entity → Agent (no
       Activity endpoints).

    Raises :class:`ProvOValidationError` on the first failing
    invariant, tagged via the ``invariant`` attribute so machine
    consumers can route the error.
    """
    if not isinstance(doc, dict) or "@graph" not in doc:
        raise ProvOValidationError(
            "PROV-O document must have a top-level @graph", invariant="shape",
        )
    graph = doc["@graph"]
    if not isinstance(graph, list):
        raise ProvOValidationError(
            "@graph must be a list", invariant="shape",
        )
    by_id: dict[str, dict] = {
        n["@id"]: n for n in graph if isinstance(n, dict) and "@id" in n
    }
    for node in graph:
        if not isinstance(node, dict):
            continue
        ntype = node.get("@type")
        nid = node.get("@id", "<missing>")

        # Invariant 1: Entity → Activity via wasGeneratedBy.
        if ntype == "prov:Entity":
            gen = node.get("prov:wasGeneratedBy")
            if gen is None:
                raise ProvOValidationError(
                    f"Entity {nid!r} has no prov:wasGeneratedBy",
                    invariant="entity-needs-activity",
                )
            gen_id = gen.get("@id") if isinstance(gen, dict) else None
            referenced = by_id.get(gen_id) if gen_id else None
            if not referenced or referenced.get("@type") != "prov:Activity":
                raise ProvOValidationError(
                    f"Entity {nid!r} prov:wasGeneratedBy points to "
                    f"{gen_id!r} which is not a prov:Activity",
                    invariant="entity-needs-activity",
                )

            # Invariant 4: Entity → Agent via wasAttributedTo.
            attr = node.get("prov:wasAttributedTo")
            if attr is not None:
                attr_id = attr.get("@id") if isinstance(attr, dict) else None
                referenced = by_id.get(attr_id) if attr_id else None
                if not referenced or referenced.get("@type") != "prov:Agent":
                    raise ProvOValidationError(
                        f"Entity {nid!r} prov:wasAttributedTo points to "
                        f"{attr_id!r} which is not a prov:Agent",
                        invariant="attribution-targets-agent",
                    )

            # Invariant 3: wasDerivedFrom Entity → Entity only.
            derived = node.get("prov:wasDerivedFrom") or []
            if isinstance(derived, dict):
                derived = [derived]
            for link in derived:
                if not isinstance(link, dict):
                    continue
                target = link.get("@id")
                referenced = by_id.get(target) if target else None
                # External references (no @id in our graph) are allowed
                # — PROV-O does not require the target be in-document.
                if referenced is not None and referenced.get(
                    "@type"
                ) != "prov:Entity":
                    raise ProvOValidationError(
                        f"Entity {nid!r} prov:wasDerivedFrom points to "
                        f"{target!r} which is not a prov:Entity",
                        invariant="derivation-targets-entity",
                    )

        # Invariant 2: Activity → Agent via wasAssociatedWith.
        if ntype == "prov:Activity":
            assoc = node.get("prov:wasAssociatedWith")
            if assoc is None:
                raise ProvOValidationError(
                    f"Activity {nid!r} has no prov:wasAssociatedWith",
                    invariant="activity-needs-agent",
                )
            assoc_id = assoc.get("@id") if isinstance(assoc, dict) else None
            referenced = by_id.get(assoc_id) if assoc_id else None
            if not referenced or referenced.get("@type") != "prov:Agent":
                raise ProvOValidationError(
                    f"Activity {nid!r} prov:wasAssociatedWith points to "
                    f"{assoc_id!r} which is not a prov:Agent",
                    invariant="activity-needs-agent",
                )
