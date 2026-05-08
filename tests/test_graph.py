"""
tests/test_graph.py — EpistemicGraph: mareforma.open(), assert_claim(),
query(), get_claim(), validate().

Coverage
--------
  open()          : default path, creates db, context manager closes connection
  assert_claim()  : default INFERRED, ANALYTICAL, DERIVED, invalid raises,
                    idempotency no-op, idempotency same id returned,
                    REPLICATED triggers (independent agents, shared upstream),
                    REPLICATED not triggered (same agent),
                    REPLICATED not triggered (no shared upstream)
  query()         : text=None returns all, substring match, no match,
                    min_support filter, classification filter, limit
  get_claim()     : found, not found
  validate()      : REPLICATED→ESTABLISHED, validated_by stored,
                    PRELIMINARY raises, nonexistent raises
"""

from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from mareforma.db import DatabaseError, ClaimNotFoundError


# ---------------------------------------------------------------------------
# open()
# ---------------------------------------------------------------------------

def test_open_returns_epistemic_graph(tmp_path):
    graph = mareforma.open(tmp_path)
    try:
        assert repr(graph).startswith("EpistemicGraph(")
    finally:
        graph.close()


def test_open_creates_db_if_missing(tmp_path):
    db_path = tmp_path / ".mareforma" / "graph.db"
    assert not db_path.exists()
    graph = mareforma.open(tmp_path)
    graph.close()
    assert db_path.exists()


def test_open_context_manager_closes_connection(tmp_path):
    with mareforma.open(tmp_path) as graph:
        claim_id = graph.assert_claim("test claim")
    # Connection is closed — further use should raise
    with pytest.raises(Exception):
        graph.query()


def test_open_default_path_uses_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with mareforma.open() as graph:
        assert graph._root == tmp_path


# ---------------------------------------------------------------------------
# assert_claim() — classification
# ---------------------------------------------------------------------------

def test_assert_claim_default_classification_is_inferred(tmp_path):
    with mareforma.open(tmp_path) as graph:
        claim_id = graph.assert_claim("some finding")
        claim = graph.get_claim(claim_id)
    assert claim["classification"] == "INFERRED"


def test_assert_claim_analytical_stored(tmp_path):
    with mareforma.open(tmp_path) as graph:
        claim_id = graph.assert_claim("count is 42", classification="ANALYTICAL")
        claim = graph.get_claim(claim_id)
    assert claim["classification"] == "ANALYTICAL"


def test_assert_claim_derived_stored(tmp_path):
    with mareforma.open(tmp_path) as graph:
        prior = graph.assert_claim("prior finding", classification="ANALYTICAL")
        claim_id = graph.assert_claim(
            "derived finding", classification="DERIVED", supports=[prior]
        )
        claim = graph.get_claim(claim_id)
    assert claim["classification"] == "DERIVED"


def test_assert_claim_invalid_classification_raises(tmp_path):
    with mareforma.open(tmp_path) as graph:
        with pytest.raises(ValueError, match="classification"):
            graph.assert_claim("bad", classification="MADE_UP")


# ---------------------------------------------------------------------------
# assert_claim() — idempotency
# ---------------------------------------------------------------------------

def test_assert_claim_idempotency_key_no_duplicate(tmp_path):
    with mareforma.open(tmp_path) as graph:
        id1 = graph.assert_claim("finding A", idempotency_key="run1_claim0")
        id2 = graph.assert_claim("finding A", idempotency_key="run1_claim0")
        all_claims = graph.query()
    assert id1 == id2
    assert len(all_claims) == 1


def test_assert_claim_different_keys_creates_two(tmp_path):
    with mareforma.open(tmp_path) as graph:
        graph.assert_claim("finding A", idempotency_key="run1_claim0")
        graph.assert_claim("finding B", idempotency_key="run1_claim1")
        all_claims = graph.query()
    assert len(all_claims) == 2


# ---------------------------------------------------------------------------
# assert_claim() — REPLICATED trigger
# ---------------------------------------------------------------------------

def test_assert_claim_replicated_triggers_on_independent_agents(tmp_path):
    with mareforma.open(tmp_path) as graph:
        prior = graph.assert_claim("prior finding", generated_by="agent_seed")
        # Two independent agents both support the same prior
        id1 = graph.assert_claim(
            "agent A finding", supports=[prior], generated_by="agent_A"
        )
        id2 = graph.assert_claim(
            "agent B finding", supports=[prior], generated_by="agent_B"
        )
        c1 = graph.get_claim(id1)
        c2 = graph.get_claim(id2)
    assert c1["support_level"] == "REPLICATED"
    assert c2["support_level"] == "REPLICATED"


def test_assert_claim_replicated_not_triggered_same_agent(tmp_path):
    with mareforma.open(tmp_path) as graph:
        prior = graph.assert_claim("prior finding", generated_by="agent_seed")
        id1 = graph.assert_claim(
            "first claim", supports=[prior], generated_by="agent_A"
        )
        id2 = graph.assert_claim(
            "second claim", supports=[prior], generated_by="agent_A"
        )
        c1 = graph.get_claim(id1)
        c2 = graph.get_claim(id2)
    assert c1["support_level"] == "PRELIMINARY"
    assert c2["support_level"] == "PRELIMINARY"


def test_assert_claim_replicated_not_triggered_no_shared_upstream(tmp_path):
    with mareforma.open(tmp_path) as graph:
        prior_a = graph.assert_claim("prior A", generated_by="seed")
        prior_b = graph.assert_claim("prior B", generated_by="seed")
        id1 = graph.assert_claim(
            "claim 1", supports=[prior_a], generated_by="agent_A"
        )
        id2 = graph.assert_claim(
            "claim 2", supports=[prior_b], generated_by="agent_B"
        )
        c1 = graph.get_claim(id1)
        c2 = graph.get_claim(id2)
    assert c1["support_level"] == "PRELIMINARY"
    assert c2["support_level"] == "PRELIMINARY"


# ---------------------------------------------------------------------------
# query()
# ---------------------------------------------------------------------------

def test_query_text_none_returns_all(tmp_path):
    with mareforma.open(tmp_path) as graph:
        graph.assert_claim("alpha finding")
        graph.assert_claim("beta finding")
        results = graph.query()
    assert len(results) == 2


def test_query_text_substring_match(tmp_path):
    with mareforma.open(tmp_path) as graph:
        graph.assert_claim("inhibitory neurons are special")
        graph.assert_claim("excitatory neurons are different")
        results = graph.query("inhibitory")
    assert len(results) == 1
    assert "inhibitory" in results[0]["text"]


def test_query_text_no_match_returns_empty(tmp_path):
    with mareforma.open(tmp_path) as graph:
        graph.assert_claim("some finding about neurons")
        results = graph.query("zzz_no_match")
    assert results == []


def test_query_min_support_filters_correctly(tmp_path):
    with mareforma.open(tmp_path) as graph:
        prior = graph.assert_claim("prior", generated_by="seed")
        # Create a REPLICATED pair
        rep1 = graph.assert_claim("rep claim", supports=[prior], generated_by="A")
        rep2 = graph.assert_claim("rep claim", supports=[prior], generated_by="B")
        # One PRELIMINARY
        pre = graph.assert_claim("preliminary only", generated_by="C")

        replicated_results = graph.query(min_support="REPLICATED")
        preliminary_results = graph.query(min_support="PRELIMINARY")

    replicated_ids = {r["claim_id"] for r in replicated_results}
    assert rep1 in replicated_ids
    assert rep2 in replicated_ids
    assert pre not in replicated_ids

    # PRELIMINARY returns everything
    all_ids = {r["claim_id"] for r in preliminary_results}
    assert pre in all_ids
    assert rep1 in all_ids


def test_query_classification_filter(tmp_path):
    with mareforma.open(tmp_path) as graph:
        graph.assert_claim("inferred claim", classification="INFERRED")
        graph.assert_claim("analytical claim", classification="ANALYTICAL")
        results = graph.query(classification="ANALYTICAL")
    assert len(results) == 1
    assert results[0]["classification"] == "ANALYTICAL"


def test_query_limit_respected(tmp_path):
    with mareforma.open(tmp_path) as graph:
        for i in range(5):
            graph.assert_claim(f"finding {i}")
        results = graph.query(limit=3)
    assert len(results) == 3


# ---------------------------------------------------------------------------
# get_claim()
# ---------------------------------------------------------------------------

def test_get_claim_returns_dict(tmp_path):
    with mareforma.open(tmp_path) as graph:
        claim_id = graph.assert_claim("a finding")
        claim = graph.get_claim(claim_id)
    assert claim is not None
    assert claim["claim_id"] == claim_id
    assert claim["text"] == "a finding"


def test_get_claim_nonexistent_returns_none(tmp_path):
    with mareforma.open(tmp_path) as graph:
        result = graph.get_claim("nonexistent-uuid")
    assert result is None


# ---------------------------------------------------------------------------
# validate()
# ---------------------------------------------------------------------------

def test_validate_replicated_to_established(tmp_path):
    with mareforma.open(tmp_path) as graph:
        prior = graph.assert_claim("prior", generated_by="seed")
        id1 = graph.assert_claim("finding", supports=[prior], generated_by="A")
        id2 = graph.assert_claim("finding", supports=[prior], generated_by="B")
        graph.validate(id1)
        claim = graph.get_claim(id1)
    assert claim["support_level"] == "ESTABLISHED"


def test_validate_stores_validated_by(tmp_path):
    with mareforma.open(tmp_path) as graph:
        prior = graph.assert_claim("prior", generated_by="seed")
        id1 = graph.assert_claim("finding", supports=[prior], generated_by="A")
        graph.assert_claim("finding", supports=[prior], generated_by="B")
        graph.validate(id1, validated_by="jane@lab.org")
        claim = graph.get_claim(id1)
    assert claim["validated_by"] == "jane@lab.org"
    assert claim["validated_at"] is not None


def test_validate_preliminary_raises(tmp_path):
    with mareforma.open(tmp_path) as graph:
        claim_id = graph.assert_claim("single agent claim")
        with pytest.raises(ValueError, match="REPLICATED"):
            graph.validate(claim_id)


def test_validate_nonexistent_claim_raises(tmp_path):
    with mareforma.open(tmp_path) as graph:
        with pytest.raises(ClaimNotFoundError):
            graph.validate("no-such-uuid")
