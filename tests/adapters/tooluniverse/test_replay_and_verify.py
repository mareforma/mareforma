"""#37: behavioral coverage for the "did this tool call really happen" surface.

replay_from_claim, verify_tool_call_envelope, and decode_predicate_from_text
carried the tool-call replay/verify guarantee but sat at 0% test coverage.
These exercise the happy path and the failure modes each promises.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mareforma import signing as _signing
from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
from mareforma.adapters.tooluniverse.demo_tool import OpenTargetsSearchTargetsMock
from mareforma.adapters.tooluniverse.predicate import (
    PREDICATE_TAG_CLOSE,
    PREDICATE_TAG_OPEN,
    PREDICATE_TYPE_V1,
    build_tool_call_predicate,
    decode_predicate_from_text,
    encode_predicate_into_text,
    verify_tool_call_envelope,
)
from mareforma.adapters.tooluniverse.replay import (
    MalformedClaimError,
    MissingToolError,
    replay_from_claim,
)


def _predicate() -> dict:
    return build_tool_call_predicate(
        tool_namespace="tu",
        tool_name="demo",
        tool_version="1.0.0",
        tool_config_fingerprint="fp",
        arguments_canonical={"x": 1},
        arguments_digest="sha256:aaa",
        result_canonical_form="json-c14n-v1",
        result_digest="sha256:bbb",
        result_bytes_size=3,
        started_at="2026-01-01T00:00:00+00:00",
        completed_at="2026-01-01T00:00:01+00:00",
        cache_hit=False,
        tool_call_id="call-1",
    )


# --- decode_predicate_from_text ------------------------------------------

def test_decode_round_trips_encode():
    text = encode_predicate_into_text(_predicate(), "a summary line")
    decoded = decode_predicate_from_text(text)
    assert decoded["predicate_type"] == PREDICATE_TYPE_V1
    assert decoded["tool_name"] == "demo"


def test_decode_rejects_missing_open_tag():
    with pytest.raises(ValueError, match="predicate tag header"):
        decode_predicate_from_text("no tags here")


def test_decode_rejects_missing_close_tag():
    with pytest.raises(ValueError, match="close tag"):
        decode_predicate_from_text(PREDICATE_TAG_OPEN + '{"a":1}')


def test_decode_rejects_non_object():
    with pytest.raises(ValueError, match="JSON object"):
        decode_predicate_from_text(PREDICATE_TAG_OPEN + "[1,2]" + PREDICATE_TAG_CLOSE)


def test_decode_rejects_missing_required_field():
    p = _predicate()
    del p["tool_name"]
    text = PREDICATE_TAG_OPEN + json.dumps(p) + PREDICATE_TAG_CLOSE
    with pytest.raises(ValueError, match="missing required fields"):
        decode_predicate_from_text(text)


# --- replay_from_claim ---------------------------------------------------

def _record_call(graph):
    pta = ProvenanceToolAdapter(tool=OpenTargetsSearchTargetsMock(), graph=graph)
    res = pta.call(target="EGFR")
    return res["metadata"]["mareforma_claim_id"]


def _registry_key(graph, cid):
    predicate = decode_predicate_from_text(graph.get_claim(cid)["text"])
    return f"{predicate['tool_namespace']}/{predicate['tool_name']}", predicate


def test_replay_reproduces_the_recorded_call(graph):
    cid = _record_call(graph)
    key, _ = _registry_key(graph, cid)
    result = replay_from_claim(graph, cid, {key: OpenTargetsSearchTargetsMock()})
    assert result.ok is True
    assert result.observed_result_digest == result.expected_result_digest
    assert result.diff_fields == ()


def test_replay_missing_tool_raises(graph):
    cid = _record_call(graph)
    with pytest.raises(MissingToolError):
        replay_from_claim(graph, cid, {})


def test_replay_malformed_claim_raises(graph):
    cid = graph.assert_claim("a plain claim with no predicate block")
    with pytest.raises(MalformedClaimError):
        replay_from_claim(graph, cid, {})


def test_replay_flags_tool_version_drift(graph):
    cid = _record_call(graph)
    key, _ = _registry_key(graph, cid)

    class _Drifted(OpenTargetsSearchTargetsMock):
        version = "9.9.9"

    result = replay_from_claim(graph, cid, {key: _Drifted()})
    assert result.ok is False
    assert "tool_version" in result.diff_fields


# --- verify_tool_call_envelope -------------------------------------------

def test_verify_envelope_extracts_predicate(graph, tmp_path: Path):
    cid = _record_call(graph)
    envelope = json.loads(graph.get_claim(cid)["signature_bundle"])
    pub = _signing.load_private_key(tmp_path / "mareforma.key").public_key()
    predicate = verify_tool_call_envelope(envelope, pub)
    assert predicate["predicate_type"] == PREDICATE_TYPE_V1


def test_verify_envelope_rejects_wrong_key(graph, tmp_path: Path):
    cid = _record_call(graph)
    envelope = json.loads(graph.get_claim(cid)["signature_bundle"])
    wrong_pub = _signing.generate_keypair().public_key()
    with pytest.raises(ValueError, match="signature failed"):
        verify_tool_call_envelope(envelope, wrong_pub)
