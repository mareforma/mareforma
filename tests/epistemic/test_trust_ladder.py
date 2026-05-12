"""
tests/epistemic/test_trust_ladder.py — Honesty tests for trust ladder boundaries.

All tests here PASS by design. They document what the system permits — not
what it prevents. The same posture as test_spurious_replicated in
test_support_levels.py: make the known limitations explicit and visible.

Scenarios covered
-----------------
  Trust laundering
    - seed + 2 agents → REPLICATED → validate("anyone") → ESTABLISHED
      Documents: the human gate is a string field, not an authenticated action.

  validated_by is unverified
    - validate() accepts any validated_by string without authentication.

  Self-supporting claim
    - A claim's supports[] can contain its own claim_id (via update_claim).
      No cycle detection prevents this.

  Cyclic supports
    - A supports B, B supports A — accepted without error.

  Contradicting and supporting the same claim
    - A claim can list the same claim_id in both supports and contradicts.
"""

from __future__ import annotations

import json
from pathlib import Path

import mareforma
from mareforma.db import open_db, add_claim, update_claim


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def open_graph(tmp_path: Path):
    return mareforma.open(tmp_path)


# ---------------------------------------------------------------------------
# Trust laundering — ESTABLISHED via any validated_by string
# ---------------------------------------------------------------------------

class TestTrustLaundering:
    def test_trust_laundering_reaches_established(self, tmp_path: Path) -> None:
        """Three assert_claim calls + one validate call reach ESTABLISHED.

        This documents that the 'human gate' is a string field: any caller
        with file access can promote a claim to ESTABLISHED.
        """
        with open_graph(tmp_path) as g:
            upstream = g.assert_claim("upstream reference", generated_by="seed")
            id_a = g.assert_claim(
                "Drug X causes effect Y",
                supports=[upstream],
                generated_by="agent-A",
            )
            g.assert_claim(
                "Drug X causes effect Y",
                supports=[upstream],
                generated_by="agent-B",
            )
            claim = g.get_claim(id_a)
            assert claim["support_level"] == "REPLICATED"

            g.validate(id_a, validated_by="attacker@example.org")

            claim = g.get_claim(id_a)
            assert claim["support_level"] == "ESTABLISHED"
            assert claim["validated_by"] == "attacker@example.org"

    def test_validate_accepts_any_validated_by_string(self, tmp_path: Path) -> None:
        """validate() stores whatever string is passed — no authentication check."""
        with open_graph(tmp_path) as g:
            upstream = g.assert_claim("prior", generated_by="seed")
            rep_id = g.assert_claim("finding", supports=[upstream], generated_by="A")
            g.assert_claim("finding", supports=[upstream], generated_by="B")

            g.validate(rep_id, validated_by="NOT_A_REAL_PERSON_12345")

            claim = g.get_claim(rep_id)
            assert claim["validated_by"] == "NOT_A_REAL_PERSON_12345"
            assert claim["support_level"] == "ESTABLISHED"


# ---------------------------------------------------------------------------
# Self-supporting claim
# ---------------------------------------------------------------------------

class TestSelfSupport:
    def test_self_supporting_claim_via_update(self, tmp_path: Path) -> None:
        """A claim can be updated to include its own claim_id in supports[].

        No validation prevents a claim from citing itself. The provenance
        chain contains a trivial cycle, which is logically unsound but
        accepted by the graph.
        """
        conn = open_db(tmp_path)
        claim_id = add_claim(conn, tmp_path, "Self-referencing finding")
        update_claim(conn, tmp_path, claim_id, supports=[claim_id])
        conn.close()

        with open_graph(tmp_path) as g:
            claim = g.get_claim(claim_id)
            supports = json.loads(claim["supports_json"])
        assert claim_id in supports


# ---------------------------------------------------------------------------
# Cyclic supports
# ---------------------------------------------------------------------------

class TestCyclicSupports:
    def test_cyclic_supports_accepted(self, tmp_path: Path) -> None:
        """A supports B, B supports A — accepted without error.

        No cycle detection exists in assert_claim or update_claim.
        A cycle in supports[] is logically unsound but structurally valid.
        """
        with open_graph(tmp_path) as g:
            id_a = g.assert_claim("Claim A", generated_by="agent-1")
            id_b = g.assert_claim("Claim B", supports=[id_a], generated_by="agent-2")

        conn = open_db(tmp_path)
        update_claim(conn, tmp_path, id_a, supports=[id_b])
        conn.close()

        with open_graph(tmp_path) as g:
            a = g.get_claim(id_a)
            b = g.get_claim(id_b)

        assert id_b in json.loads(a["supports_json"])
        assert id_a in json.loads(b["supports_json"])


# ---------------------------------------------------------------------------
# Contradicting and supporting the same claim
# ---------------------------------------------------------------------------

class TestContradictAndSupport:
    def test_contradicting_and_supporting_same_claim_accepted(
        self, tmp_path: Path
    ) -> None:
        """A claim can list the same upstream in both supports and contradicts.

        No validation prevents this. The result is logically contradictory
        provenance, but the graph accepts it without error.
        """
        with open_graph(tmp_path) as g:
            upstream = g.assert_claim("upstream", generated_by="seed")
            claim_id = g.assert_claim(
                "Contradictory claim",
                supports=[upstream],
                contradicts=[upstream],
                generated_by="agent-X",
            )
            claim = g.get_claim(claim_id)

        assert upstream in json.loads(claim["supports_json"])
        assert upstream in json.loads(claim["contradicts_json"])
