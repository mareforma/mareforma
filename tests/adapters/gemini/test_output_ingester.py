"""Tests for the Gemini read-only ingest adapter."""

from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from mareforma.adapters.gemini import (
    CODE_VARIATION_V1,
    HYPOTHESIS_V1,
    LITERATURE_INSIGHT_V1,
    REQUIRED_FIELDS,
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


# Reusable per-capability payload fixtures that satisfy REQUIRED_FIELDS.
_VALID_PAYLOADS = {
    "code-variation": {
        "summary": "sample code-variation",
        "input_problem_digest": "sha256:" + "1" * 64,
        "code_variation_source_digest": "sha256:" + "2" * 64,
        "score": 0.87,
        "model_version": "gemini-2.0-2026-05",
    },
    "hypothesis": {
        "summary": "sample hypothesis",
        "final_hypothesis_text_digest": "sha256:" + "3" * 64,
        "model_version": "gemini-2.0-2026-05",
    },
    "literature-insight": {
        "summary": "sample literature-insight",
        "cell_value_digest": "sha256:" + "4" * 64,
        "cited_paper_dois": ["10.1234/example"],
        "model_version": "gemini-2.0-2026-05",
    },
    "science-skill": {
        "summary": "sample science-skill",
        "db_name": "UniProt",
        "query_digest": "sha256:" + "5" * 64,
        "result_digest": "sha256:" + "6" * 64,
        "result_canonical_form": "json-c14n-v1",
        "provider": "google-antigravity",
    },
}


def test_supported_capabilities_complete():
    assert set(SUPPORTED_CAPABILITIES) == {
        "code-variation", "hypothesis", "literature-insight", "science-skill",
    }


def test_supported_capabilities_is_frozen():
    """SUPPORTED_CAPABILITIES must be a MappingProxyType — no mutation."""
    with pytest.raises(TypeError):
        SUPPORTED_CAPABILITIES["x"] = "y"  # type: ignore[index]


def test_required_fields_cover_every_capability():
    """REQUIRED_FIELDS schema MUST exist for every supported capability."""
    assert set(REQUIRED_FIELDS) == set(SUPPORTED_CAPABILITIES)


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


def test_ingest_without_graph_raises():
    ing = OutputIngester(graph=None)
    with pytest.raises(RuntimeError, match="graph="):
        ing.ingest(capability="code-variation", payload={})


def test_ingest_validates_capability(graph):
    ing = OutputIngester(graph=graph)
    with pytest.raises(ValueError, match="unsupported capability"):
        ing.ingest(capability="not-real", payload={"summary": "x"})


def test_ingest_rejects_reserved_key_predicate_type(graph):
    """Caller MUST NOT be able to override predicate_type via payload."""
    ing = OutputIngester(graph=graph)
    payload = dict(_VALID_PAYLOADS["hypothesis"])
    payload["predicate_type"] = "urn:attacker:fake:v1"
    with pytest.raises(ValueError, match="reserved keys"):
        ing.ingest(capability="hypothesis", payload=payload)


def test_ingest_rejects_reserved_key_capability(graph):
    ing = OutputIngester(graph=graph)
    payload = dict(_VALID_PAYLOADS["hypothesis"])
    payload["capability"] = "other"
    with pytest.raises(ValueError, match="reserved keys"):
        ing.ingest(capability="hypothesis", payload=payload)


def test_ingest_requires_capability_fields(graph):
    ing = OutputIngester(graph=graph)
    with pytest.raises(ValueError, match="missing"):
        ing.ingest(capability="hypothesis", payload={"summary": "x"})


def test_ingest_each_capability_writes_claim_with_correct_uri(graph):
    ing = OutputIngester(graph=graph)
    for cap, expected_uri in SUPPORTED_CAPABILITIES.items():
        cid = ing.ingest(
            capability=cap, payload=dict(_VALID_PAYLOADS[cap]),
        )
        row = graph.get_claim(cid)
        assert row is not None
        import json
        payload = json.loads(row["predicate_payload"])
        assert payload["predicate_type"] == expected_uri
        assert payload["capability"] == cap


def test_ingest_sanitises_string_payload_values(graph):
    """String payload values flow through sanitize_for_llm."""
    ing = OutputIngester(graph=graph)
    payload = dict(_VALID_PAYLOADS["hypothesis"])
    payload["summary"] = "abc\x00def"  # NUL stripped by sanitize_for_llm
    cid = ing.ingest(capability="hypothesis", payload=payload)
    import json
    stored = json.loads(graph.get_claim(cid)["predicate_payload"])
    assert "\x00" not in stored["summary"]


def test_ingest_default_classification_is_inferred(graph):
    ing = OutputIngester(graph=graph)
    cid = ing.ingest(
        capability="hypothesis", payload=dict(_VALID_PAYLOADS["hypothesis"]),
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
