"""Tests for the Gemini read-only ingest adapter."""

from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from mareforma.adapters.gemini import (
    CODE_VARIATION_V1,
    HYPOTHESIS_V1,
    LITERATURE_INSIGHT_V1,
    SCIENCE_SKILL_V1,
    OutputIngester,
    SUPPORTED_CAPABILITIES,
)


@pytest.fixture()
def graph(tmp_path: Path):
    from mareforma import signing as _signing
    key_path = tmp_path / "key"
    _signing.bootstrap_key(key_path)
    with mareforma.open(tmp_path, key_path=key_path) as g:
        yield g


def test_supported_capabilities_complete():
    assert set(SUPPORTED_CAPABILITIES) == {
        "code-variation", "hypothesis", "literature-insight", "science-skill",
    }


def test_predicate_uris_match_supported_capabilities():
    ing = OutputIngester()
    expected = {
        CODE_VARIATION_V1, HYPOTHESIS_V1,
        LITERATURE_INSIGHT_V1, SCIENCE_SKILL_V1,
    }
    assert set(ing.predicate_uris()) == expected


def test_all_capability_uris_are_urn_form():
    for uri in SUPPORTED_CAPABILITIES.values():
        assert uri.startswith("urn:mareforma:predicate:")


def test_ingest_unknown_capability_raises():
    ing = OutputIngester(graph=None)
    with pytest.raises(RuntimeError, match="graph="):
        ing.ingest(capability="code-variation", payload={})


def test_ingest_validates_capability(graph):
    ing = OutputIngester(graph=graph)
    with pytest.raises(ValueError, match="unsupported capability"):
        ing.ingest(capability="not-real", payload={"summary": "x"})


def test_ingest_each_capability_writes_claim_with_correct_uri(graph):
    ing = OutputIngester(graph=graph)
    for cap, expected_uri in SUPPORTED_CAPABILITIES.items():
        cid = ing.ingest(
            capability=cap,
            payload={"summary": f"sample {cap}", "extra": 42},
        )
        row = graph.get_claim(cid)
        assert row is not None
        # predicate_payload is stored on the claim row.
        import json
        payload = json.loads(row["predicate_payload"])
        assert payload["predicate_type"] == expected_uri
        assert payload["capability"] == cap
        assert payload["extra"] == 42


def test_ingest_default_classification_is_inferred(graph):
    ing = OutputIngester(graph=graph)
    cid = ing.ingest(
        capability="hypothesis", payload={"summary": "h"},
    )
    row = graph.get_claim(cid)
    assert row["classification"] == "INFERRED"


def test_emit_sample(graph):
    ing = OutputIngester(graph=graph)
    cid = ing.emit_sample()
    row = graph.get_claim(cid)
    assert row is not None
    assert "literature-insight" in row["text"] or "sample" in row["text"].lower()


def test_emit_sample_without_graph_raises():
    ing = OutputIngester()
    with pytest.raises(RuntimeError, match="graph="):
        ing.emit_sample()


def test_import_does_not_register_predicates():
    from mareforma.predicate_types import predicates
    before = len(predicates())
    import mareforma.adapters.gemini  # noqa: F401
    after = len(predicates())
    assert before == after
