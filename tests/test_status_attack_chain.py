"""tests/test_status_attack_chain.py: a retracted or contested claim must
not read as a line of evidence.

The scenario these tests defend against:

  1. Adversary asserts a claim with status='retracted' (or 'contested')
     citing a shared upstream.
  2. An honest peer cites the same upstream under a different signing key.
  3. Without a status filter, a reader counting distinct signers on that
     upstream counts the adversary's tainted claim as the second line.
  4. validate() (or another adversary path) then puts a human's sign-off on
     the tainted row, and it serves as a fake upstream for further chains.

The defenses: a withdrawn claim is not counted as a peer; validate() refuses
non-open rows; and the LLM-facing query_graph tool surfaces status so
consumers can see editorial taint on a row that otherwise looks corroborated.
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
    """Second key used for validation — mareforma refuses self-validation,
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
    )


# ---------------------------------------------------------------------------
# Validation refuses a claim the graph has withdrawn
# ---------------------------------------------------------------------------


class TestValidateRefusesNonOpen:
    def test_validate_refused_on_contested(self, tmp_path: Path) -> None:
        """Build a claim by normal means, flip its status, then confirm
        validate() refuses to record a sign-off over it."""
        from tests._helpers import _two_signers
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            a = g.assert_claim("Z", supports=[seed], generated_by="agent/a", signer=sa)
            b = g.assert_claim("Z", supports=[seed], generated_by="agent/b", signer=sb)
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


# ---------------------------------------------------------------------------
# seed=True refuses non-open status
# ---------------------------------------------------------------------------


class TestRetractedIsTerminal:
    """A BEFORE UPDATE trigger refuses any transition out of
    status='retracted'. Without this, an adversary could insert a
    born-retracted claim and then flip it back to 'open' via update_claim,
    and a withdrawn finding would be back in every default read counting as
    a line of evidence, with no audit trail since the signed envelope does
    not bind status."""

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
        """The full chain: born-retracted, flip to open, then ride an honest
        peer's citation and count as a second line. The trigger refuses the
        flip, so the chain stops at step 2."""
        from mareforma.db import update_claim, IllegalStateTransitionError
        from tests._helpers import _two_signers
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            adv = g.assert_claim(
                "Z", supports=[seed], generated_by="agent/adversary",
                status="retracted",
            )
            with pytest.raises(IllegalStateTransitionError):
                update_claim(g._conn, g._root, adv, status="open")
            # Honest peer can still REPLICATE with another honest peer,             # the adversary's retracted claim is invisible to convergence.
            honest_a = g.assert_claim("Z", supports=[seed], generated_by="agent/h1", signer=sa)
            honest_b = g.assert_claim("Z", supports=[seed], generated_by="agent/h2", signer=sb)
            assert g.get_claim(adv)["status"] == "retracted"


# ---------------------------------------------------------------------------
# LLM tool surfaces reflect status
# ---------------------------------------------------------------------------


class TestLLMToolSurfacesStatus:
    def test_query_graph_returns_status_field(self, tmp_path: Path) -> None:
        """An LLM consumer of the agent tool must be able to see editorial
        taint, even on a converged row whose peers happen to be open."""
        from tests._helpers import _two_signers
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            seed = _seeded_upstream(g)
            a = g.assert_claim("W", supports=[seed], generated_by="agent/a", signer=sa)
            b = g.assert_claim("W", supports=[seed], generated_by="agent/b", signer=sb)
            # Flip a to contested editorially. Its peers are unchanged, but
            # the LLM must see the taint.
            from mareforma.db import update_claim
            update_claim(g._conn, g._root, a, status="contested")
            query_graph, _ = g.get_tools(generated_by="agent/llm")
            results = json.loads(query_graph("W"))
            statuses = {r["claim_id"]: r["status"] for r in results}
            assert statuses[a] == "contested"
            assert statuses[b] == "open"
