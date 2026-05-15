"""tests/test_status_attack_chain.py — status-tainted claims must not
participate in the trust ladder.

The scenario these tests defend against:

  1. Adversary asserts a claim with status='retracted' (or 'contested')
     pointing at an ESTABLISHED upstream.
  2. An honest peer asserts the same upstream with a different
     generated_by, expecting their claim to REPLICATE off the upstream.
  3. Without a status filter, the substrate would also promote the
     adversary's tainted claim to REPLICATED.
  4. validate() (or another adversary path) then promotes the tainted
     row to ESTABLISHED — usable as a fake upstream for further chains.

The defenses: REPLICATED detection skips status != 'open' peers;
validate() refuses non-open rows; seed=True refuses non-open status;
the LLM-facing query_graph tool surfaces status so consumers can see
editorial taint on otherwise-REPLICATED rows.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import mareforma
from mareforma import signing as _signing


def _key(tmp_path: Path) -> Path:
    key_path = tmp_path / "_status_key"
    if not key_path.exists():
        _signing.bootstrap_key(key_path)
    return key_path


def _validator_key(tmp_path: Path) -> Path:
    """Second key used for validation — the substrate refuses self-validation,
    so promotion tests need a key distinct from the one signing claims."""
    key_path = tmp_path / "_status_validator_key"
    if not key_path.exists():
        _signing.bootstrap_key(key_path)
    return key_path


def _enroll_validator(graph, validator_key_path: Path, identity: str = "v") -> None:
    pem = _signing.public_key_to_pem(
        _signing.load_private_key(validator_key_path).public_key(),
    )
    graph.enroll_validator(pem, identity=identity)


def _seeded_upstream(graph) -> str:
    return graph.assert_claim(
        "seeded prior literature",
        classification="DERIVED",
        generated_by="agent/seed",
        seed=True,
    )


# ---------------------------------------------------------------------------
# REPLICATED detection filters by status
# ---------------------------------------------------------------------------


class TestReplicatedFiltersStatus:
    def test_retracted_peer_does_not_trigger_replicated(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            tainted = g.assert_claim(
                "X is true",
                supports=[seed],
                generated_by="agent/adversary",
                status="retracted",
            )
            honest = g.assert_claim(
                "X is true",
                supports=[seed],
                generated_by="agent/honest",
            )
            assert g.get_claim(tainted)["support_level"] == "PRELIMINARY"
            assert g.get_claim(honest)["support_level"] == "PRELIMINARY"

    def test_contested_peer_does_not_trigger_replicated(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            tainted = g.assert_claim(
                "X is true",
                supports=[seed],
                generated_by="agent/a",
                status="contested",
            )
            honest = g.assert_claim(
                "X is true",
                supports=[seed],
                generated_by="agent/b",
            )
            assert g.get_claim(tainted)["support_level"] == "PRELIMINARY"
            assert g.get_claim(honest)["support_level"] == "PRELIMINARY"

    def test_two_open_peers_with_third_retracted_still_replicate(
        self, tmp_path: Path,
    ) -> None:
        """The retracted row is skipped but honest peers still find each other."""
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            g.assert_claim(
                "Y", supports=[seed], generated_by="agent/x", status="retracted",
            )
            a = g.assert_claim("Y", supports=[seed], generated_by="agent/a")
            b = g.assert_claim("Y", supports=[seed], generated_by="agent/b")
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"

    def test_tainted_new_claim_does_not_replicate_off_existing_honest_peer(
        self, tmp_path: Path,
    ) -> None:
        """Reverse-order attack: honest peer is inserted FIRST (and sits at
        PRELIMINARY since it has no partner yet), then an adversary INSERTs a
        retracted claim citing the same upstream. Without the new-claim
        status guard, the adversary's INSERT would find the honest peer in
        the SELECT and the UPDATE (which appends new_claim_id to peer_ids
        unconditionally) would co-promote BOTH rows to REPLICATED."""
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            honest = g.assert_claim(
                "Z", supports=[seed], generated_by="agent/honest",
            )
            tainted = g.assert_claim(
                "Z",
                supports=[seed],
                generated_by="agent/adversary",
                status="retracted",
            )
            assert g.get_claim(honest)["support_level"] == "PRELIMINARY"
            assert g.get_claim(tainted)["support_level"] == "PRELIMINARY"


# ---------------------------------------------------------------------------
# validate() refuses non-open claims
# ---------------------------------------------------------------------------


class TestValidateRefusesNonOpen:
    def test_validate_refused_on_contested(self, tmp_path: Path) -> None:
        """Build a REPLICATED row by normal means, then flip status, then
        confirm validate() refuses the promotion."""
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            a = g.assert_claim("Z", supports=[seed], generated_by="agent/a")
            b = g.assert_claim("Z", supports=[seed], generated_by="agent/b")
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            # Flip a to contested via the editorial update path.
            from mareforma.db import update_claim
            update_claim(g._conn, g._root, a, status="contested")
            _enroll_validator(g, _validator_key(tmp_path))

        # Validator re-opens and tries to promote.
        with mareforma.open(tmp_path, key_path=_validator_key(tmp_path)) as g:
            with pytest.raises(ValueError, match="status='contested'"):
                g.validate(a, validated_by="reviewer")
            # b is still open and still validatable.
            g.validate(b, validated_by="reviewer")
            assert g.get_claim(b)["support_level"] == "ESTABLISHED"


# ---------------------------------------------------------------------------
# seed=True refuses non-open status
# ---------------------------------------------------------------------------


class TestSeedRefusesNonOpen:
    def test_seed_with_retracted_status_refused(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            with pytest.raises(ValueError, match="seed.*status='retracted'"):
                g.assert_claim(
                    "anchor",
                    classification="DERIVED",
                    generated_by="agent/seed",
                    seed=True,
                    status="retracted",
                )

    def test_seed_with_contested_status_refused(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            with pytest.raises(ValueError, match="seed.*status='contested'"):
                g.assert_claim(
                    "anchor",
                    classification="DERIVED",
                    generated_by="agent/seed",
                    seed=True,
                    status="contested",
                )


# ---------------------------------------------------------------------------
# Retracted status is terminal at the storage layer
# ---------------------------------------------------------------------------


class TestRetractedIsTerminal:
    """A BEFORE UPDATE trigger refuses any transition out of
    status='retracted'. Without this, an adversary could insert a
    born-retracted claim and then flip it back to 'open' via
    update_claim (a pure status mutation doesn't trigger a REPLICATED
    re-check). The flipped row would then ride an honest peer's INSERT
    into REPLICATED, with no audit trail since the signed envelope
    doesn't bind status."""

    def test_retracted_to_open_refused(self, tmp_path: Path) -> None:
        from mareforma.db import update_claim, IllegalStateTransitionError
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            c = g.assert_claim(
                "X", supports=[seed], generated_by="agent/a", status="retracted",
            )
            with pytest.raises(IllegalStateTransitionError, match="retracted_is_terminal"):
                update_claim(g._conn, g._root, c, status="open")

    def test_retracted_to_contested_refused(self, tmp_path: Path) -> None:
        from mareforma.db import update_claim, IllegalStateTransitionError
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            c = g.assert_claim(
                "X", supports=[seed], generated_by="agent/a", status="retracted",
            )
            with pytest.raises(IllegalStateTransitionError, match="retracted_is_terminal"):
                update_claim(g._conn, g._root, c, status="contested")

    def test_open_to_retracted_still_allowed(self, tmp_path: Path) -> None:
        from mareforma.db import update_claim
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            c = g.assert_claim("X", supports=[seed], generated_by="agent/a")
            update_claim(g._conn, g._root, c, status="retracted")
            assert g.get_claim(c)["status"] == "retracted"

    def test_open_contested_open_round_trip(self, tmp_path: Path) -> None:
        """Non-terminal transitions still work freely."""
        from mareforma.db import update_claim
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            c = g.assert_claim("X", supports=[seed], generated_by="agent/a")
            update_claim(g._conn, g._root, c, status="contested")
            assert g.get_claim(c)["status"] == "contested"
            update_claim(g._conn, g._root, c, status="open")
            assert g.get_claim(c)["status"] == "open"

    def test_full_flip_back_attack_chain_blocked(self, tmp_path: Path) -> None:
        """The full attack chain Q5 surfaced: born-retracted, flip to open,
        ride an honest peer's INSERT into REPLICATED. The trigger refuses
        the flip, so the chain stops at step 2."""
        from mareforma.db import update_claim, IllegalStateTransitionError
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            adv = g.assert_claim(
                "Z", supports=[seed], generated_by="agent/adversary",
                status="retracted",
            )
            with pytest.raises(IllegalStateTransitionError):
                update_claim(g._conn, g._root, adv, status="open")
            # Honest peer can still REPLICATE with another honest peer —
            # the adversary's retracted claim is invisible to convergence.
            honest_a = g.assert_claim("Z", supports=[seed], generated_by="agent/h1")
            honest_b = g.assert_claim("Z", supports=[seed], generated_by="agent/h2")
            assert g.get_claim(adv)["support_level"] == "PRELIMINARY"
            assert g.get_claim(adv)["status"] == "retracted"
            assert g.get_claim(honest_a)["support_level"] == "REPLICATED"
            assert g.get_claim(honest_b)["support_level"] == "REPLICATED"


# ---------------------------------------------------------------------------
# LLM tool surfaces reflect status
# ---------------------------------------------------------------------------


class TestLLMToolSurfacesStatus:
    def test_query_graph_returns_status_field(self, tmp_path: Path) -> None:
        """An LLM consumer of the agent tool must be able to see editorial
        taint, even on a REPLICATED row whose peers happen to be open."""
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            a = g.assert_claim("W", supports=[seed], generated_by="agent/a")
            b = g.assert_claim("W", supports=[seed], generated_by="agent/b")
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            # Flip a to contested editorially — it remains REPLICATED but
            # the LLM must see the taint.
            from mareforma.db import update_claim
            update_claim(g._conn, g._root, a, status="contested")
            query_graph, _ = g.get_tools(generated_by="agent/llm")
            results = json.loads(query_graph("W", min_support="REPLICATED"))
            statuses = {r["claim_id"]: r["status"] for r in results}
            assert statuses[a] == "contested"
            assert statuses[b] == "open"
