"""End-to-end: multiple adapters writing into the same graph, then
asserting we can read back claims from each by predicate URI."""

from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from mareforma.adapters.clawinstitute import EventHook
from mareforma.adapters.gemini import OutputIngester
from mareforma.adapters.tooluniverse import ToolCallRecorder
from mareforma.predicate_types import (
    CODE_VARIATION_V1,
    HYPOTHESIS_V1,
    LITERATURE_INSIGHT_V1,
    SCIENCE_SKILL_V1,
    TOOL_CALL_V1,
    WORKSHOP_EVENT_V1,
)


@pytest.fixture()
def graph(tmp_path: Path):
    from mareforma import signing as _signing
    key_path = tmp_path / "key"
    _signing.bootstrap_key(key_path)
    with mareforma.open(tmp_path, key_path=key_path) as g:
        yield g


def _predicate_uri(graph, claim_id: str) -> str:
    """Read the predicate URI out of a claim, accepting either
    storage shape: ``predicate_payload`` column (clawinstitute,
    gemini) OR a tagged-JSON predicate embedded in the claim text
    (tooluniverse). Production verifiers handle both."""
    import json
    row = graph.get_claim(claim_id)
    payload_str = row["predicate_payload"]
    if payload_str:
        try:
            return json.loads(payload_str)["predicate_type"]
        except (json.JSONDecodeError, KeyError, TypeError):
            pass
    # tooluniverse embeds the predicate inside the claim text as
    # <predicate tool-call v1>{...}</predicate>.
    text = row["text"]
    open_tag, close_tag = "<predicate tool-call v1>", "</predicate>"
    if text.startswith(open_tag):
        end = text.find(close_tag, len(open_tag))
        if end > 0:
            inner = json.loads(text[len(open_tag):end])
            return inner["predicate_type"]
    raise AssertionError(
        f"could not extract predicate_type from claim {claim_id}"
    )


def test_each_adapter_emits_distinct_predicate_uri(graph):
    claw_id = EventHook(graph=graph).emit_sample()
    tu_id = ToolCallRecorder(graph=graph).emit_sample()
    gem_id = OutputIngester(graph=graph).emit_sample()

    assert _predicate_uri(graph, claw_id) == WORKSHOP_EVENT_V1
    assert _predicate_uri(graph, tu_id) == TOOL_CALL_V1
    assert _predicate_uri(graph, gem_id) == LITERATURE_INSIGHT_V1


def test_gemini_ingester_covers_all_four_capabilities(graph):
    """Each Gemini capability writes a claim under the right URI."""
    ing = OutputIngester(graph=graph)
    expected_pairs = {
        "code-variation": CODE_VARIATION_V1,
        "hypothesis": HYPOTHESIS_V1,
        "literature-insight": LITERATURE_INSIGHT_V1,
        "science-skill": SCIENCE_SKILL_V1,
    }
    for cap, uri in expected_pairs.items():
        cid = ing.ingest(capability=cap, payload={"summary": f"sample {cap}"})
        assert _predicate_uri(graph, cid) == uri


def test_two_hosts_converge_on_a_replicated_finding(tmp_path: Path):
    """Simulated cross-host replication: two independent graphs each
    record a Gemini ``hypothesis`` claim with the same artifact_hash;
    the merge target should see ≥2 INFERRED claims supporting the
    same finding (the substrate's REPLICATED-promotion path that
    downstream agents act on)."""
    from mareforma import signing as _signing

    host_a = tmp_path / "host-a"
    host_b = tmp_path / "host-b"
    merge = tmp_path / "merge"
    host_a.mkdir(); host_b.mkdir(); merge.mkdir()

    # Two independent signing keys (different agents on different hosts).
    key_a = host_a / "k"; _signing.bootstrap_key(key_a)
    key_b = host_b / "k"; _signing.bootstrap_key(key_b)
    key_merge = merge / "k"; _signing.bootstrap_key(key_merge)

    # Each host independently emits the same finding via Gemini ingest.
    summary = "Compound X inhibits target Y at IC50=15nM"
    with mareforma.open(host_a, key_path=key_a) as ga:
        cid_a = OutputIngester(graph=ga).ingest(
            capability="hypothesis",
            payload={"summary": summary, "ic50_nM": 15},
            generated_by="adapter:gemini@host-a",
        )
        row_a = ga.get_claim(cid_a)

    with mareforma.open(host_b, key_path=key_b) as gb:
        cid_b = OutputIngester(graph=gb).ingest(
            capability="hypothesis",
            payload={"summary": summary, "ic50_nM": 15},
            generated_by="adapter:gemini@host-b",
        )
        row_b = gb.get_claim(cid_b)

    # The two independently-emitted claims must agree on the
    # human-readable text — this is the convergence signal a merge
    # agent looks for. (Cryptographic envelope cross-host replay is
    # exercised by the broader signing/restore suite; here we only
    # verify that two independent adapter calls produce comparable
    # claims when given the same input.)
    assert row_a["text"] == row_b["text"]
    assert row_a["classification"] == row_b["classification"] == "INFERRED"
    # Distinct claim IDs (they were signed by different keys on
    # different hosts) — that distinctness is exactly what allows
    # REPLICATED promotion on a downstream merge.
    assert cid_a != cid_b
