"""
tests/epistemic/test_trust_ladder.py — Honesty tests for trust ladder boundaries.

All tests here PASS by design. They document what the system permits — not
what it prevents. The same posture as test_spurious_replicated in
test_support_levels.py: make the known limitations explicit and visible.

Scenarios covered
-----------------
  validated_by is a cosmetic label
    - The validator's authenticated identity is the keyid embedded in the
      signed validation envelope; ``validated_by`` is a free-form display
      string and ``validate()`` stores whatever the caller passes.

  Local-key trust footprint
    - Anyone with read access to the project's signing key can act as the
      enrolled root validator. Mareforma is local-trust, not cross-org PKI.

  Self-supporting claim (closed in P1.6)
    - Updating a claim to include its own claim_id in supports[] is
      rejected with CycleDetectedError.

  Cyclic supports (closed in P1.6)
    - A supports B, B supports A — the second edge is rejected with
      CycleDetectedError on update_claim.

  Contradicting and supporting the same claim
    - A claim can list the same claim_id in both supports and contradicts.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import mareforma
from mareforma.db import open_db, add_claim, update_claim


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def open_graph(tmp_path: Path):
    return mareforma.open(tmp_path)


def open_signed_graph(tmp_path: Path):
    """Open a graph with a bootstrapped signing key (auto-enrolled as root)."""
    from mareforma import signing as _signing
    key_path = tmp_path / "mareforma.key"
    _signing.bootstrap_key(key_path)
    return mareforma.open(tmp_path, key_path=key_path)


# ---------------------------------------------------------------------------
# validated_by is a cosmetic label (the keyid in validation_signature is truth)
# ---------------------------------------------------------------------------

class TestTrustLaundering:
    def test_validated_by_is_a_cosmetic_label(self, tmp_path: Path) -> None:
        """validate() requires an enrolled validator (the loaded key auto-
        enrolls as root on first open), but the ``validated_by`` parameter
        is a free-form display string — the authenticated identity is the
        keyid embedded in the signed validation envelope.

        A caller can pass any label they like; downstream consumers MUST
        check ``validation_signature``'s keyid against the validators
        table if they want to know who actually validated.
        """
        with open_signed_graph(tmp_path) as g:
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

            # The label "attacker@example.org" is stored verbatim in
            # validated_by, but the validator's real keyid lives in
            # validation_signature.
            g.validate(id_a, validated_by="attacker@example.org")

            claim = g.get_claim(id_a)
            assert claim["support_level"] == "ESTABLISHED"
            assert claim["validated_by"] == "attacker@example.org"
            assert claim["validation_signature"] is not None

    def test_validate_accepts_any_validated_by_string(self, tmp_path: Path) -> None:
        """validate() stores whatever display string is passed."""
        with open_signed_graph(tmp_path) as g:
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
    def test_self_supporting_claim_via_update_rejected(self, tmp_path: Path) -> None:
        """A claim cannot be updated to include its own claim_id in
        ``supports[]``.

        Closed by v0.3.0 P1.6: cycle detection on ``update_claim``
        rejects the self-loop. Provenance chains are required to be
        acyclic.
        """
        from mareforma.db import CycleDetectedError
        conn = open_db(tmp_path)
        claim_id = add_claim(conn, tmp_path, "Self-referencing finding")
        with pytest.raises(CycleDetectedError, match="self-loop"):
            update_claim(conn, tmp_path, claim_id, supports=[claim_id])
        conn.close()

        with open_graph(tmp_path) as g:
            claim = g.get_claim(claim_id)
            supports = json.loads(claim["supports_json"])
        assert claim_id not in supports


# ---------------------------------------------------------------------------
# Cyclic supports
# ---------------------------------------------------------------------------

class TestCyclicSupports:
    def test_cyclic_supports_rejected(self, tmp_path: Path) -> None:
        """A supports B, B supports A — the second edge is rejected.

        Closed by v0.3.0 P1.6: cycle detection on ``update_claim``
        walks the supports[] graph forward from the proposed edge and
        rejects any closure back to the updated claim.
        """
        from mareforma.db import CycleDetectedError
        with open_graph(tmp_path) as g:
            id_a = g.assert_claim("Claim A", generated_by="agent-1")
            id_b = g.assert_claim("Claim B", supports=[id_a], generated_by="agent-2")

        conn = open_db(tmp_path)
        with pytest.raises(CycleDetectedError, match="cycle"):
            update_claim(conn, tmp_path, id_a, supports=[id_b])
        conn.close()

        with open_graph(tmp_path) as g:
            a = g.get_claim(id_a)
            b = g.get_claim(id_b)

        # a.supports unchanged; b.supports still contains id_a.
        assert id_b not in json.loads(a["supports_json"])
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
