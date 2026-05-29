"""Capability-shaped URI constants live in :mod:`mareforma.predicate_types`
and re-export at the top level. Drift between constants, BUILTIN_URIS,
and the registered set is a contract violation tested here.
"""

from __future__ import annotations

import mareforma
from mareforma import predicate_types as pt


_CONSTANT_NAMES = (
    "CLAIM_V1",
    "EPISTEMIC_GRAPH_V1",
    "CLAIM_WITH_ROLES_V1",
    "TOOL_CALL_V1",
    "CONTAINER_EXEC_V1",
    "CODE_VARIATION_V1",
    "HYPOTHESIS_V1",
    "LITERATURE_INSIGHT_V1",
    "SCIENCE_SKILL_V1",
    "META_CLAIM_V1",
    "WORKSHOP_EVENT_V1",
    "AGENT_TRACE_V1",
    "INGESTED_TRACE_V1",
    "LLM_OUTPUT_V1",
    "REVIEW_V1",
    "PEER_REVIEW_V1",
    "ELO_MATCH_V1",
    "TOURNAMENT_BRACKET_V1",
    "WET_LAB_ASSAY_V1",
    "WET_LAB_ASSAY_FLOW_CYTOMETRY_V1",
    "WET_LAB_ASSAY_SEQUENCING_V1",
    "WET_LAB_ASSAY_IMAGING_V1",
    "WET_LAB_ASSAY_PROTEOMICS_V1",
    "WET_LAB_ASSAY_ELECTROPHYSIOLOGY_V1",
    "REPLICATION_ATTESTATION_V1",
    "COMPOUNDING_ATTESTATION_V1",
    "SEMANTIC_GROUNDING_V1",
    "DOI_RESOLUTION_V1",
)


def test_every_constant_is_in_builtin_uris():
    for name in _CONSTANT_NAMES:
        assert getattr(pt, name) in pt.BUILTIN_URIS, (
            f"{name}={getattr(pt, name)!r} not in BUILTIN_URIS"
        )


def test_top_level_re_export_matches_predicate_types():
    for name in _CONSTANT_NAMES:
        assert getattr(mareforma, name) == getattr(pt, name)


def test_capability_uris_registered_at_import():
    registered = set(mareforma.predicates())
    for name in (
        "CODE_VARIATION_V1", "HYPOTHESIS_V1", "LITERATURE_INSIGHT_V1",
        "SCIENCE_SKILL_V1", "CONTAINER_EXEC_V1", "META_CLAIM_V1",
        "WORKSHOP_EVENT_V1",
    ):
        uri = getattr(mareforma, name)
        assert uri in registered, f"{name}={uri} not registered at import"


def test_no_dns_form_in_builtin_uris():
    for uri in pt.BUILTIN_URIS:
        assert not uri.startswith("https://"), (
            f"BUILTIN_URIS has DNS-form URI: {uri}"
        )


def test_all_constants_urn_form():
    for name in _CONSTANT_NAMES:
        uri = getattr(pt, name)
        assert uri.startswith("urn:mareforma:predicate:"), (
            f"{name}={uri!r} is not URN-form"
        )
