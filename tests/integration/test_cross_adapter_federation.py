"""End-to-end: multiple adapters writing into the same graph, then
asserting we can read back claims from each by predicate URI.

Conceptual clusters:

- :class:`TestDistinctPredicateUris` — three adapters share one
  graph; each predicate URI is what its adapter advertises.
- :class:`TestGeminiCapabilityCoverage` — every Gemini capability
  writes a claim under the right URI.
- :class:`TestCrossHostConvergence` — two independent graphs each
  record the same Gemini hypothesis claim; verify the
  preconditions a downstream REPLICATED-promotion path requires.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

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


def _predicate_uri(graph, claim_id: str) -> str:
    """Read the predicate URI out of a claim, accepting either
    storage shape: ``predicate_payload`` column (clawinstitute,
    gemini) OR a tagged-JSON predicate embedded in the claim text
    (tooluniverse). Production verifiers handle both.

    The tagged-text branch parses on ``</predicate>`` close tag so
    it works for any ``<predicate X v1>`` family member, not just
    tool-call (container-exec routing emits ``<predicate
    container-exec v1>`` from the same wrapper).
    """
    row = graph.get_claim(claim_id)
    payload_str = row["predicate_payload"]
    if payload_str:
        try:
            return json.loads(payload_str)["predicate_type"]
        except (json.JSONDecodeError, KeyError, TypeError):
            pass
    # Generic <predicate <name> v<N>>{...}</predicate> parser.
    match = re.match(
        r"^<predicate\s+[a-z0-9._/\-]+\s+v\d+>(\{.*?\})</predicate>",
        row["text"], re.DOTALL,
    )
    if match:
        inner = json.loads(match.group(1))
        return inner["predicate_type"]
    raise AssertionError(
        f"could not extract predicate_type from claim {claim_id}"
    )


class TestDistinctPredicateUris:
    def test_each_adapter_emits_its_advertised_uri(self, graph):
        claw_id = EventHook(graph=graph).emit_sample()
        tu_id = ToolCallRecorder(graph=graph).emit_sample()
        gem_id = OutputIngester(graph=graph).emit_sample()

        assert _predicate_uri(graph, claw_id) == WORKSHOP_EVENT_V1
        assert _predicate_uri(graph, tu_id) == TOOL_CALL_V1
        assert _predicate_uri(graph, gem_id) == LITERATURE_INSIGHT_V1


class TestGeminiCapabilityCoverage:
    def test_each_capability_writes_correct_uri(self, graph):
        """Each Gemini capability writes a claim under the right URI."""
        ing = OutputIngester(graph=graph)
        expected_pairs = {
            "code-variation": (CODE_VARIATION_V1, {
                "input_problem_digest": "sha256:" + "1" * 64,
                "code_variation_source_digest": "sha256:" + "2" * 64,
                "score": 0.9, "model_version": "g",
            }),
            "hypothesis": (HYPOTHESIS_V1, {
                "final_hypothesis_text_digest": "sha256:" + "3" * 64,
                "model_version": "g",
            }),
            "literature-insight": (LITERATURE_INSIGHT_V1, {
                "cell_value_digest": "sha256:" + "4" * 64,
                "cited_paper_dois": [], "model_version": "g",
            }),
            "science-skill": (SCIENCE_SKILL_V1, {
                "db_name": "UniProt", "query_digest": "sha256:" + "5" * 64,
                "result_digest": "sha256:" + "6" * 64,
                "result_canonical_form": "json-c14n-v1", "provider": "g",
            }),
        }
        for cap, (uri, extra) in expected_pairs.items():
            payload = {"summary": f"sample {cap}", **extra}
            cid = ing.ingest(capability=cap, payload=payload)
            assert _predicate_uri(graph, cid) == uri


class TestCrossHostConvergence:
    def test_two_hosts_emit_convergent_findings(self, tmp_path: Path):
        """Two independent graphs each record a Gemini hypothesis claim
        with the same hypothesis content; verify both produce INFERRED
        claims with matching text and matching predicate payload (the
        convergence signal a downstream merge agent looks for).

        Cryptographic envelope cross-host replay is exercised by the
        signing/restore suite; this test only verifies that two
        independent adapter calls produce comparable claims when given
        the same input — the precondition the REPLICATED-promotion
        path requires.
        """
        from mareforma import signing as _signing

        host_a = tmp_path / "host-a"
        host_b = tmp_path / "host-b"
        host_a.mkdir(); host_b.mkdir()

        # Two independent signing keys (different agents on different hosts).
        key_a = host_a / "k"; _signing.bootstrap_key(key_a)
        key_b = host_b / "k"; _signing.bootstrap_key(key_b)

        summary = "Compound X inhibits target Y at IC50=15nM"
        payload = {
            "summary": summary,
            "final_hypothesis_text_digest": "sha256:" + "a" * 64,
            "model_version": "gemini-2.0-2026-05",
        }
        with mareforma.open(host_a, key_path=key_a) as ga:
            cid_a = OutputIngester(graph=ga).ingest(
                capability="hypothesis", payload=dict(payload),
                generated_by="adapter:gemini@host-a",
            )
            row_a = ga.get_claim(cid_a)

        with mareforma.open(host_b, key_path=key_b) as gb:
            cid_b = OutputIngester(graph=gb).ingest(
                capability="hypothesis", payload=dict(payload),
                generated_by="adapter:gemini@host-b",
            )
            row_b = gb.get_claim(cid_b)

        # Convergence: same human-readable text, both INFERRED.
        assert row_a["text"] == row_b["text"]
        assert row_a["classification"] == row_b["classification"] == "INFERRED"
        # Distinct claim IDs (different keys, different hosts) — the
        # distinctness is what allows REPLICATED promotion on a
        # downstream merge that imports both.
        assert cid_a != cid_b
        # Predicate payloads agree on the load-bearing hypothesis
        # digest (the bytes a merge agent compares against).
        pa = json.loads(row_a["predicate_payload"])
        pb = json.loads(row_b["predicate_payload"])
        assert (
            pa["final_hypothesis_text_digest"]
            == pb["final_hypothesis_text_digest"]
        )
        assert pa["predicate_type"] == pb["predicate_type"]
