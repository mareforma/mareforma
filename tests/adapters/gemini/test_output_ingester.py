"""Tests for the Gemini read-only ingest adapter.

Conceptual clusters:

- :class:`TestSupportedCapabilities` — capability registry shape and
  immutability (MappingProxyType).
- :class:`TestRequiredFields` — schema-completeness invariant.
- :class:`TestPredicateUris` — adapter exposes URN-form URIs matching
  the capability set.
- :class:`TestReservedKeyRejection` — payload may not override
  ``predicate_type`` / ``capability``.
- :class:`TestPayloadValidation` — per-capability required-field check.
- :class:`TestSanitization` — string payload values flow through
  sanitize_for_llm.
- :class:`TestClaimEmission` — each capability writes one INFERRED
  claim under the correct URI.
- :class:`TestEmitSample` — coexistence convention surface.
- :class:`TestImportHygiene` — registry pollution check.
"""

from __future__ import annotations

import json

import pytest

from mareforma.adapters.gemini import (
    CODE_VARIATION_V1,
    HYPOTHESIS_V1,
    LITERATURE_INSIGHT_V1,
    REQUIRED_FIELDS,
    SCIENCE_SKILL_V1,
    OutputIngester,
    SUPPORTED_CAPABILITIES,
)


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


class TestSupportedCapabilities:
    def test_complete_set(self):
        assert set(SUPPORTED_CAPABILITIES) == {
            "code-variation", "hypothesis",
            "literature-insight", "science-skill",
        }

    def test_is_frozen(self):
        """SUPPORTED_CAPABILITIES must be a MappingProxyType — no mutation."""
        with pytest.raises(TypeError):
            SUPPORTED_CAPABILITIES["x"] = "y"  # type: ignore[index]


class TestRequiredFields:
    def test_covers_every_capability(self):
        """REQUIRED_FIELDS schema MUST exist for every supported capability."""
        assert set(REQUIRED_FIELDS) == set(SUPPORTED_CAPABILITIES)


class TestPredicateUris:
    def test_match_supported_capabilities(self):
        ing = OutputIngester()
        expected = {
            CODE_VARIATION_V1, HYPOTHESIS_V1,
            LITERATURE_INSIGHT_V1, SCIENCE_SKILL_V1,
        }
        assert set(ing.predicate_uris()) == expected

    def test_all_urn_form(self):
        for uri in SUPPORTED_CAPABILITIES.values():
            assert uri.startswith("urn:mareforma:predicate:")


class TestReservedKeyRejection:
    def test_rejects_predicate_type_override(self, graph):
        """Caller MUST NOT be able to override predicate_type via payload."""
        ing = OutputIngester(graph=graph)
        payload = dict(_VALID_PAYLOADS["hypothesis"])
        payload["predicate_type"] = "urn:attacker:fake:v1"
        with pytest.raises(ValueError, match="reserved keys"):
            ing.ingest(capability="hypothesis", payload=payload)

    def test_rejects_capability_override(self, graph):
        ing = OutputIngester(graph=graph)
        payload = dict(_VALID_PAYLOADS["hypothesis"])
        payload["capability"] = "other"
        with pytest.raises(ValueError, match="reserved keys"):
            ing.ingest(capability="hypothesis", payload=payload)


class TestPayloadValidation:
    def test_requires_graph(self):
        ing = OutputIngester(graph=None)
        with pytest.raises(RuntimeError, match="graph="):
            ing.ingest(capability="code-variation", payload={})

    def test_validates_capability(self, graph):
        ing = OutputIngester(graph=graph)
        with pytest.raises(ValueError, match="unsupported capability"):
            ing.ingest(capability="not-real", payload={"summary": "x"})

    def test_requires_capability_fields(self, graph):
        ing = OutputIngester(graph=graph)
        with pytest.raises(ValueError, match="missing"):
            ing.ingest(capability="hypothesis", payload={"summary": "x"})


class TestSanitization:
    def test_string_payload_values_flow_through_sanitize(self, graph):
        """String payload values flow through sanitize_for_llm."""
        ing = OutputIngester(graph=graph)
        payload = dict(_VALID_PAYLOADS["hypothesis"])
        payload["summary"] = "abc\x00def"  # NUL stripped by sanitize_for_llm
        cid = ing.ingest(capability="hypothesis", payload=payload)
        stored = json.loads(graph.get_claim(cid)["predicate_payload"])
        assert "\x00" not in stored["summary"]


class TestClaimEmission:
    def test_each_capability_writes_correct_uri(self, graph):
        ing = OutputIngester(graph=graph)
        for cap, expected_uri in SUPPORTED_CAPABILITIES.items():
            cid = ing.ingest(
                capability=cap, payload=dict(_VALID_PAYLOADS[cap]),
            )
            row = graph.get_claim(cid)
            assert row is not None
            payload = json.loads(row["predicate_payload"])
            assert payload["predicate_type"] == expected_uri
            assert payload["capability"] == cap

    def test_default_classification_is_inferred(self, graph):
        ing = OutputIngester(graph=graph)
        cid = ing.ingest(
            capability="hypothesis",
            payload=dict(_VALID_PAYLOADS["hypothesis"]),
        )
        row = graph.get_claim(cid)
        assert row["classification"] == "INFERRED"


class TestEmitSample:
    def test_writes_sample_claim(self, graph):
        ing = OutputIngester(graph=graph)
        cid = ing.emit_sample()
        row = graph.get_claim(cid)
        assert row is not None
        assert (
            "literature-insight" in row["text"]
            or "sample" in row["text"].lower()
        )

    def test_without_graph_raises(self):
        ing = OutputIngester()
        with pytest.raises(RuntimeError, match="graph="):
            ing.emit_sample()


class TestImportHygiene:
    def test_import_does_not_register_predicates(self):
        from mareforma.predicate_types import predicates
        before = len(predicates())
        import mareforma.adapters.gemini  # noqa: F401
        after = len(predicates())
        assert before == after
