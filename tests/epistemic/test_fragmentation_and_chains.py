"""
tests/epistemic/test_fragmentation_and_chains.py: epistemic correctness tests.

Unlike unit tests (which verify function behaviour), these tests validate
the thesis: that the graph's trust signals are honest under hostile inputs
and edge-case conditions.

Scenarios covered
-----------------
  Fragmentation
    - two agents assert the same semantic claim without idempotency_key
      and the graph records two claims, which is one finding read as two
    - the same key on divergent fields is refused rather than merged

  DERIVED chain
    - DERIVED with valid supports= is traceable to upstream
    - DERIVED without supports= is recorded but the chain is broken
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

from tests._helpers import _pem_of, _two_signers
from tests.epistemic._builders import (
    _bootstrap_validator_key,
    open_graph,
)


# ---------------------------------------------------------------------------
# Fragmentation
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

        # Two separate claims, the graph has fragmented
        assert id_a != id_b
        assert len(all_claims) == 2

    def test_shared_idempotency_key_with_conflicting_fields_refused(
        self, tmp_path: Path
    ) -> None:
        """Same idempotency_key with different text + generated_by raises.

        The "convergence convention" historically documented around this
        primitive was anti-epistemic: collapsing two labs' content into
        one row destroyed the second author's text + generated_by and
        broke REPLICATED detection (REPLICATED requires two distinct
        rows with different generated_by). The graph now refuses
        the silent merge. The legitimate cross-lab convergence path is
        two separate claims that share an entry in ``supports[]``,
        that fires REPLICATED honestly. See ``TestCrossLabConvergence``
        below for that pattern.
        """
        from mareforma.db import IdempotencyConflictError
        KEY = "target_T_elevated_condition_C"

        with open_graph(tmp_path) as graph:
            graph.assert_claim(
                "Target T is elevated in condition C",
                generated_by="agent/model-a/lab_a",
                idempotency_key=KEY,
            )
            with pytest.raises(
                IdempotencyConflictError,
                match="text|generated_by",
            ):
                graph.assert_claim(
                    "Target T shows increased expression under condition C",
                    generated_by="agent/model-b/lab_b",
                    idempotency_key=KEY,
                )


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
        will find an empty list, because the provenance is missing.
        """
        with open_graph(tmp_path) as graph:
            broken = graph.assert_claim(
                "derived claim with no upstream",
                classification="DERIVED",
                generated_by="agent/model-a/lab_a",
                # no supports=, broken chain
            )
            c_broken = graph.get_claim(broken)

        supports = json.loads(c_broken["supports_json"])
        assert c_broken["classification"] == "DERIVED"
        assert supports == []   # chain is broken, detectable but not prevented

