"""Exporter profile conformance: RO-Crate root entity and PROV-O labels.

- #29  the RO-Crate root data entity must carry a ``license`` and a
       non-null ``datePublished``, and it must separate data entities
       (``hasPart``) from provenance actions (``mentions``) per the
       Process Run Crate profile.
- #48  PROV-O labels must use ``rdfs:label`` (with an ``rdfs`` context
       mapping), not the non-existent ``prov:label`` a strict consumer
       rejects.

Both fail on the pre-fix tree.

Scope note: these check the SHAPE of the exported entities (key presence,
in-graph @id resolution, label vocabulary) — not full profile conformance.
They do not run an RO-Crate validator, a JSON-LD processor, or SHACL/OWL over
PROV-O, so a genuine profile violation (e.g. a ``hasPart`` member that is a
contextual entity rather than a data entity) would not be caught here. Treat
"conformant" as shape/label conformance until a real validator is wired in.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from mareforma.exporters.prov_o import PROV_CONTEXT, build_prov_o, validate_prov_o
from mareforma.exporters.ro_crate import build_crate


def _seed(tmp_path: Path) -> tuple[str, str]:
    from mareforma import signing as _signing

    key_path = tmp_path / "asserter.key"
    _signing.save_private_key(_signing.generate_keypair(), key_path)
    with mareforma.open(tmp_path, key_path=key_path) as graph:
        a = graph.assert_claim("upstream finding", generated_by="lab-A", seed=True)
        b = graph.assert_claim(
            "downstream conclusion", supports=[a], generated_by="lab-B"
        )
    return a, b


def _root(crate: dict) -> dict:
    return next(e for e in crate["@graph"] if e["@id"] == "./")


def test_ro_crate_root_has_license_resolving_to_entity(tmp_path: Path) -> None:
    """root license references a contextual entity in the graph."""
    _seed(tmp_path)
    crate = build_crate(tmp_path)
    root = _root(crate)
    assert "license" in root, "RO-Crate root data entity must declare a license"
    lic = root["license"]
    lic_id = lic.get("@id") if isinstance(lic, dict) else lic
    assert lic_id, "license must reference a URI / entity @id"
    by_id = {e.get("@id"): e for e in crate["@graph"]}
    assert lic_id in by_id, "license @id must resolve to a contextual entity"


def test_ro_crate_license_override_propagates(tmp_path: Path) -> None:
    """The build_crate(license_id=, license_name=) override reaches the root
    entity and its contextual license entity — a producer who knows their data's
    license can pin it instead of the CC-BY-4.0 default."""
    _seed(tmp_path)
    custom_id = "https://opensource.org/licenses/MIT"
    custom_name = "MIT License"
    crate = build_crate(tmp_path, license_id=custom_id, license_name=custom_name)
    root = _root(crate)
    lic = root["license"]
    lic_id = lic.get("@id") if isinstance(lic, dict) else lic
    assert lic_id == custom_id, "root license must use the overridden id"
    by_id = {e.get("@id"): e for e in crate["@graph"]}
    assert custom_id in by_id, "overridden license @id must resolve to an entity"
    assert by_id[custom_id].get("name") == custom_name, (
        "the license entity must carry the overridden name"
    )


def test_ro_crate_rejects_empty_license_override(tmp_path: Path) -> None:
    """An empty license override would emit a dangling @id (a malformed crate);
    build_crate refuses it rather than ship a broken license reference."""
    _seed(tmp_path)
    with pytest.raises(ValueError, match="license_id"):
        build_crate(tmp_path, license_id="")
    with pytest.raises(ValueError, match="license_name"):
        build_crate(tmp_path, license_name="  ")


def test_ro_crate_date_published_never_null(tmp_path: Path) -> None:
    """datePublished is present even for an empty graph."""
    with mareforma.open(tmp_path):
        pass  # bootstrap only — zero claims
    crate = build_crate(tmp_path)
    assert _root(crate).get("datePublished"), "datePublished must be non-null"


def test_ro_crate_mentions_actions_haspart_data(tmp_path: Path) -> None:
    """CreateActions ride under mentions; data entities under hasPart."""
    a, b = _seed(tmp_path)
    crate = build_crate(tmp_path)
    root = _root(crate)
    mentions = {m["@id"] for m in root.get("mentions", [])}
    has_part = {p["@id"] for p in root.get("hasPart", [])}
    for cid in (a, b):
        action_id = f"urn:mareforma:claim:{cid}"
        text_id = f"#claim-text/{cid}"
        assert action_id in mentions, "CreateActions belong under mentions"
        assert action_id not in has_part, "CreateActions must not sit in hasPart"
        assert text_id in has_part, "claim-text data entities belong under hasPart"


def test_prov_o_uses_rdfs_label(tmp_path: Path) -> None:
    """rdfs:label with an rdfs context, never prov:label."""
    _seed(tmp_path)
    doc = build_prov_o(tmp_path)
    assert "rdfs" in doc["@context"], "@context must map the rdfs prefix"
    assert doc["@context"]["rdfs"] == "http://www.w3.org/2000/01/rdf-schema#"
    labeled = [n for n in doc["@graph"] if isinstance(n, dict)]
    assert any("rdfs:label" in n for n in labeled), "labels must use rdfs:label"
    for n in labeled:
        assert "prov:label" not in n, f"{n.get('@id')} still uses prov:label"
    validate_prov_o(doc)  # the four structural invariants still hold


def test_prov_context_declares_rdfs() -> None:
    """the module-level context constant carries the rdfs mapping."""
    assert PROV_CONTEXT.get("rdfs") == "http://www.w3.org/2000/01/rdf-schema#"
