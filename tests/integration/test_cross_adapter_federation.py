"""End-to-end: multiple adapters writing into the same graph, then
asserting we can read back claims from each by predicate URI.

Conceptual clusters:

- :class:`TestDistinctPredicateUris` — three adapters share one
  graph; each predicate URI is what its adapter advertises.
- :class:`TestGeminiCapabilityCoverage` — every Gemini capability
  writes a claim under the right URI.
- :class:`TestCrossHostConvergence` — two independent graphs each
  record the same Gemini hypothesis claim; verify the two hosts sign
  under distinct keyids, the axis a downstream merge counts on, with
  a shared-key negative control.
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


def _ingest_on_two_hosts(tmp_path: Path, *, shared_key: bool) -> tuple:
    """Record the same Gemini hypothesis on two host graphs; return both rows.

    With *shared_key* the two hosts run under one signing key, the collapsed
    case a merge cannot count twice.
    """
    from mareforma import signing as _signing

    host_a = tmp_path / "host-a"
    host_b = tmp_path / "host-b"
    host_a.mkdir(); host_b.mkdir()

    key_a = host_a / "k"; _signing.bootstrap_key(key_a)
    if shared_key:
        key_b = key_a
    else:
        key_b = host_b / "k"; _signing.bootstrap_key(key_b)

    payload = {
        "summary": "Compound X inhibits target Y at IC50=15nM",
        "final_hypothesis_text_digest": "sha256:" + "a" * 64,
        "model_version": "gemini-2.0-2026-05",
    }
    rows = []
    for host, key, run in (
        (host_a, key_a, "adapter:gemini@host-a"),
        (host_b, key_b, "adapter:gemini@host-b"),
    ):
        with mareforma.open(host, key_path=key) as g:
            cid = OutputIngester(graph=g).ingest(
                capability="hypothesis", payload=dict(payload),
                generated_by=run,
            )
            rows.append(g.get_claim(cid))
    return tuple(rows)


class TestCrossHostConvergence:
    def test_two_hosts_emit_convergent_findings(self, tmp_path: Path):
        """Two independent graphs each record a Gemini hypothesis claim with
        the same content; verify both produce INFERRED claims with matching
        text and matching predicate payload (the convergence signal a
        downstream merge agent looks for), and that the two hosts sign under
        distinct non-NULL asserter keyids.

        The keyid axis is the one a merge counts on: promotion turns on a
        shared ESTABLISHED anchor plus distinct non-NULL ``asserter_keyid``,
        never on equal text or an equal predicate payload. Cryptographic
        envelope cross-host replay is exercised by the signing/restore suite.
        """
        row_a, row_b = _ingest_on_two_hosts(tmp_path, shared_key=False)

        # Two hosts, two keys: two lines a merge may count separately.
        assert row_a["asserter_keyid"] and row_b["asserter_keyid"]
        assert row_a["asserter_keyid"] != row_b["asserter_keyid"]
        # Convergence: same human-readable text, both INFERRED.
        assert row_a["text"] == row_b["text"]
        assert row_a["classification"] == row_b["classification"] == "INFERRED"
        # Predicate payloads agree on the load-bearing hypothesis
        # digest (the bytes a merge agent compares against).
        pa = json.loads(row_a["predicate_payload"])
        pb = json.loads(row_b["predicate_payload"])
        assert (
            pa["final_hypothesis_text_digest"]
            == pb["final_hypothesis_text_digest"]
        )
        assert pa["predicate_type"] == pb["predicate_type"]

    def test_one_key_on_both_hosts_stays_one_signer(self, tmp_path: Path):
        """Negative control: run the same two-host body under one key. The
        content assertions above still hold, so they say nothing about
        independence; the keyid collapses, which is what blocks promotion."""
        row_a, row_b = _ingest_on_two_hosts(tmp_path, shared_key=True)

        assert row_a["text"] == row_b["text"]
        assert row_a["asserter_keyid"] == row_b["asserter_keyid"]
