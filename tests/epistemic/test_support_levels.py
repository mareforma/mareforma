"""
tests/epistemic/test_support_levels.py — Epistemic correctness tests.

Unlike unit tests (which verify function behaviour), these tests validate
the thesis: that the graph's trust signals are honest under hostile inputs
and edge-case conditions.

Scenarios covered
-----------------
  REPLICATED
    - fires when two independent agents share an upstream
    - does not fire when the same agent makes two claims
    - does not fire when agents have no shared upstream
    - fires from a contaminated upstream (spurious — detectable by classification)

  Fragmentation
    - two agents assert the same semantic claim without idempotency_key
      → two PRELIMINARY claims, REPLICATED never fires
    - same agents use idempotency_key → single claim, no fragmentation

  DERIVED chain
    - DERIVED with valid supports= is traceable to upstream
    - DERIVED without supports= is recorded but chain is broken

  ESTABLISHED gate
    - validate() on PRELIMINARY raises ValueError
    - validate() on REPLICATED succeeds → ESTABLISHED
    - ESTABLISHED is not reachable in a single assert_claim() call
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import mareforma
from mareforma.db import ClaimNotFoundError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def open_graph(tmp_path: Path):
    """Open a graph with a bootstrapped key so seed=True works.

    P1.7 made ESTABLISHED-upstream the default rule for REPLICATED
    promotion, so REPLICATED tests need seed=True on the upstream —
    which in turn requires a loaded signing key. The local helper
    bootstraps one transparently."""
    from mareforma import signing as _signing
    key_path = tmp_path / "_test_key"
    if not key_path.exists():
        _signing.bootstrap_key(key_path)
    return mareforma.open(tmp_path, key_path=key_path)


def open_signed_graph(tmp_path: Path):
    """Open a graph with a bootstrapped signing key.

    Required for tests that exercise ``graph.validate()`` — the loaded
    key auto-enrolls as the root validator, which is the prerequisite
    for promoting a claim to ESTABLISHED.
    """
    from mareforma import signing as _signing
    key_path = tmp_path / "mareforma.key"
    _signing.bootstrap_key(key_path)
    return mareforma.open(tmp_path, key_path=key_path)


# ---------------------------------------------------------------------------
# REPLICATED — genuine independent convergence
# ---------------------------------------------------------------------------

class TestReplicatedGenuine:
    def test_replicated_fires_when_two_independent_agents_share_upstream(
        self, tmp_path: Path
    ) -> None:
        with open_graph(tmp_path) as graph:
            upstream = graph.assert_claim("upstream finding", generated_by="seed", seed=True)

            id_a = graph.assert_claim(
                "finding from agent A",
                classification="ANALYTICAL",
                generated_by="agent/model-a/lab_a",
                supports=[upstream],
            )
            id_b = graph.assert_claim(
                "finding from agent B",
                classification="ANALYTICAL",
                generated_by="agent/model-b/lab_b",
                supports=[upstream],
            )

            c_a = graph.get_claim(id_a)
            c_b = graph.get_claim(id_b)

        assert c_a["support_level"] == "REPLICATED"
        assert c_b["support_level"] == "REPLICATED"

    def test_replicated_requires_different_generated_by(
        self, tmp_path: Path
    ) -> None:
        """Same agent making two claims on the same upstream is not independent."""
        with open_graph(tmp_path) as graph:
            upstream = graph.assert_claim("upstream finding", generated_by="seed", seed=True)

            id_a = graph.assert_claim(
                "first claim from agent A",
                generated_by="agent/model-a/lab_a",
                supports=[upstream],
            )
            id_b = graph.assert_claim(
                "second claim from agent A",
                generated_by="agent/model-a/lab_a",   # same agent
                supports=[upstream],
            )

            c_a = graph.get_claim(id_a)
            c_b = graph.get_claim(id_b)

        assert c_a["support_level"] == "PRELIMINARY"
        assert c_b["support_level"] == "PRELIMINARY"

    def test_replicated_requires_shared_upstream(
        self, tmp_path: Path
    ) -> None:
        """Two independent agents with no shared upstream do not trigger REPLICATED."""
        with open_graph(tmp_path) as graph:
            upstream_a = graph.assert_claim("upstream A", generated_by="seed", seed=True)
            upstream_b = graph.assert_claim("upstream B", generated_by="seed", seed=True)

            id_a = graph.assert_claim(
                "finding from agent A",
                generated_by="agent/model-a/lab_a",
                supports=[upstream_a],
            )
            id_b = graph.assert_claim(
                "finding from agent B",
                generated_by="agent/model-b/lab_b",
                supports=[upstream_b],         # different upstream
            )

            c_a = graph.get_claim(id_a)
            c_b = graph.get_claim(id_b)

        assert c_a["support_level"] == "PRELIMINARY"
        assert c_b["support_level"] == "PRELIMINARY"

    def test_replicated_fires_on_third_independent_agent(
        self, tmp_path: Path
    ) -> None:
        """REPLICATED fires as soon as the second independent agent asserts."""
        with open_graph(tmp_path) as graph:
            upstream = graph.assert_claim("upstream", generated_by="seed", seed=True)

            id_a = graph.assert_claim(
                "claim A", generated_by="agent/model-a/lab_a", supports=[upstream]
            )
            # After one claim: still PRELIMINARY
            assert graph.get_claim(id_a)["support_level"] == "PRELIMINARY"

            id_b = graph.assert_claim(
                "claim B", generated_by="agent/model-b/lab_b", supports=[upstream]
            )
            # After second independent agent: REPLICATED
            assert graph.get_claim(id_a)["support_level"] == "REPLICATED"
            assert graph.get_claim(id_b)["support_level"] == "REPLICATED"


# ---------------------------------------------------------------------------
# REPLICATED — spurious convergence (detectable via classification)
# ---------------------------------------------------------------------------

class TestReplicatedSpurious:
    def test_replicated_fires_from_inferred_claims_without_data(
        self, tmp_path: Path
    ) -> None:
        """Two agents repeating LLM prior knowledge trigger REPLICATED.

        This is the false replication failure mode. REPLICATED fires because
        the graph topology is satisfied — but both claims are INFERRED with
        no source_name. The graph makes this detectable.
        """
        with open_graph(tmp_path) as graph:
            upstream = graph.assert_claim("prior literature ref", generated_by="seed", seed=True)

            id_a = graph.assert_claim(
                "target T is likely relevant (LLM prior)",
                classification="INFERRED",      # no data pipeline ran
                generated_by="agent/model-a/lab_a",
                supports=[upstream],
                source_name=None,               # no source
            )
            id_b = graph.assert_claim(
                "target T is likely relevant (LLM prior)",
                classification="INFERRED",
                generated_by="agent/model-b/lab_b",
                supports=[upstream],
                source_name=None,
            )

            c_a = graph.get_claim(id_a)
            c_b = graph.get_claim(id_b)

        # REPLICATED fires — topology is satisfied
        assert c_a["support_level"] == "REPLICATED"
        assert c_b["support_level"] == "REPLICATED"

        # But the signal is spurious: both are INFERRED with no source
        assert c_a["classification"] == "INFERRED"
        assert c_b["classification"] == "INFERRED"
        assert not c_a["source_name"]
        assert not c_b["source_name"]

    def test_genuine_replicated_is_distinguishable_from_spurious(
        self, tmp_path: Path
    ) -> None:
        """ANALYTICAL + source_name distinguishes genuine from spurious REPLICATED."""
        with open_graph(tmp_path) as graph:
            upstream = graph.assert_claim("upstream", generated_by="seed", seed=True)

            # Genuine: data-driven
            genuine_a = graph.assert_claim(
                "finding A (data-driven)",
                classification="ANALYTICAL",
                generated_by="agent/model-a/lab_a",
                supports=[upstream],
                source_name="dataset_alpha",
            )
            genuine_b = graph.assert_claim(
                "finding B (data-driven)",
                classification="ANALYTICAL",
                generated_by="agent/model-b/lab_b",
                supports=[upstream],
                source_name="dataset_beta",
            )

            # Spurious: LLM prior
            spurious_a = graph.assert_claim(
                "finding A (LLM prior)",
                classification="INFERRED",
                generated_by="agent/model-a/lab_a",
                supports=[upstream],
            )
            spurious_b = graph.assert_claim(
                "finding B (LLM prior)",
                classification="INFERRED",
                generated_by="agent/model-b/lab_b",
                supports=[upstream],
            )

            all_replicated = graph.query(min_support="REPLICATED")

        # All four downstream peers REPLICATE plus the ESTABLISHED
        # seeded upstream (min_support='REPLICATED' is inclusive of
        # ESTABLISHED). Topology alone does not distinguish trustworthy
        # from spurious.
        assert len(all_replicated) == 5

        # Filter for trustworthy: ANALYTICAL + source present
        trustworthy = [
            c for c in all_replicated
            if c["classification"] == "ANALYTICAL" and c.get("source_name")
        ]
        assert len(trustworthy) == 2
        assert all(c["classification"] == "ANALYTICAL" for c in trustworthy)


# ---------------------------------------------------------------------------
# Graph fragmentation
# ---------------------------------------------------------------------------

class TestGraphFragmentation:
    def test_two_agents_without_idempotency_key_produce_two_claims(
        self, tmp_path: Path
    ) -> None:
        """Without a shared idempotency_key, two agents create two PRELIMINARY claims.

        The graph fragments: the same semantic finding exists twice with no
        connection between them. REPLICATED never fires because there is no
        shared upstream link.
        """
        with open_graph(tmp_path) as graph:
            id_a = graph.assert_claim(
                "Target T is elevated in condition C",
                generated_by="agent/model-a/lab_a",
            )
            id_b = graph.assert_claim(
                "Target T shows increased expression under condition C",
                generated_by="agent/model-b/lab_b",
            )

            all_claims = graph.query("Target T")
            c_a = graph.get_claim(id_a)
            c_b = graph.get_claim(id_b)

        # Two separate claims — the graph has fragmented
        assert id_a != id_b
        assert len(all_claims) == 2
        assert c_a["support_level"] == "PRELIMINARY"
        assert c_b["support_level"] == "PRELIMINARY"

    def test_shared_idempotency_key_prevents_fragmentation(
        self, tmp_path: Path
    ) -> None:
        """A shared structured key routes both agents to the same claim_id."""
        KEY = "target_T_elevated_condition_C"

        with open_graph(tmp_path) as graph:
            id_a = graph.assert_claim(
                "Target T is elevated in condition C",
                generated_by="agent/model-a/lab_a",
                idempotency_key=KEY,
            )
            id_b = graph.assert_claim(
                "Target T shows increased expression under condition C",
                generated_by="agent/model-b/lab_b",
                idempotency_key=KEY,            # same key → same claim
            )

            all_claims = graph.query("Target T")

        assert id_a == id_b
        assert len(all_claims) == 1


# ---------------------------------------------------------------------------
# DERIVED chain integrity
# ---------------------------------------------------------------------------

class TestDerivedChain:
    def test_derived_with_supports_is_traceable_to_upstream(
        self, tmp_path: Path
    ) -> None:
        with open_graph(tmp_path) as graph:
            upstream = graph.assert_claim(
                "upstream ANALYTICAL finding",
                classification="ANALYTICAL",
                generated_by="agent/model-a/lab_a",
            )
            derived = graph.assert_claim(
                "derived synthesis built on upstream",
                classification="DERIVED",
                generated_by="agent/model-b/lab_b",
                supports=[upstream],
            )

            c_derived = graph.get_claim(derived)

        supports = json.loads(c_derived["supports_json"])
        assert upstream in supports
        assert c_derived["classification"] == "DERIVED"

    def test_derived_without_supports_is_recorded_but_chain_is_broken(
        self, tmp_path: Path
    ) -> None:
        """DERIVED with no supports= is accepted but the chain is unverifiable.

        The graph records the claim honestly. A reviewer querying supports_json
        will find an empty list — the provenance is missing.
        """
        with open_graph(tmp_path) as graph:
            broken = graph.assert_claim(
                "derived claim with no upstream",
                classification="DERIVED",
                generated_by="agent/model-a/lab_a",
                # no supports= — broken chain
            )
            c_broken = graph.get_claim(broken)

        import json
        supports = json.loads(c_broken["supports_json"])
        assert c_broken["classification"] == "DERIVED"
        assert supports == []   # chain is broken — detectable but not prevented


# ---------------------------------------------------------------------------
# ESTABLISHED gate
# ---------------------------------------------------------------------------

class TestEstablishedGate:
    def test_validate_on_preliminary_raises(self, tmp_path: Path) -> None:
        with open_signed_graph(tmp_path) as graph:
            claim_id = graph.assert_claim(
                "single agent claim",
                generated_by="agent/model-a/lab_a",
            )
            with pytest.raises(ValueError, match="REPLICATED"):
                graph.validate(claim_id)

    def test_validate_on_replicated_succeeds(self, tmp_path: Path) -> None:
        with open_signed_graph(tmp_path) as graph:
            upstream = graph.assert_claim("upstream", generated_by="seed", seed=True)
            id_a = graph.assert_claim(
                "claim A", generated_by="agent/model-a/lab_a", supports=[upstream]
            )
            graph.assert_claim(
                "claim B", generated_by="agent/model-b/lab_b", supports=[upstream]
            )
            # id_a is now REPLICATED
            graph.validate(id_a, validated_by="reviewer@example.org")
            c = graph.get_claim(id_a)

        assert c["support_level"] == "ESTABLISHED"
        assert c["validated_by"] == "reviewer@example.org"
        assert c["validated_at"] is not None

    def test_assert_claim_cannot_produce_established(
        self, tmp_path: Path
    ) -> None:
        """No combination of assert_claim() arguments reaches ESTABLISHED.

        ESTABLISHED is only reachable via validate(). This test ensures
        the gate holds — a single agent cannot self-promote.
        """
        with open_graph(tmp_path) as graph:
            claim_id = graph.assert_claim(
                "an agent trying to self-promote",
                classification="ANALYTICAL",
                generated_by="agent/model-a/lab_a",
            )
            c = graph.get_claim(claim_id)

        assert c["support_level"] != "ESTABLISHED"

    def test_validate_on_nonexistent_claim_raises(
        self, tmp_path: Path
    ) -> None:
        with open_signed_graph(tmp_path) as graph:
            with pytest.raises(ClaimNotFoundError):
                graph.validate("no-such-uuid")
