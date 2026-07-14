"""Convergence must not jam when a seed claim cites a shared anchor.

A seed born ESTABLISHED clears every new-claim gate in
``_maybe_update_replicated_unlocked`` except support_level. When a second seed
cites the same ESTABLISHED anchor an honest peer also cites, the promotion
folded the seed into the target set and attempted an illegal
ESTABLISHED -> REPLICATED transition, aborting the whole UPDATE and stranding
the peer with a retry flag that no retry could ever clear.
"""
from __future__ import annotations

import mareforma
from tests._helpers import _bootstrap_key, _load_signer


def test_second_seed_on_shared_anchor_does_not_jam_convergence(tmp_path):
    root_key = _bootstrap_key(tmp_path, "root.key")
    peer_key = _bootstrap_key(tmp_path, "peer.key")
    peer_signer = _load_signer(peer_key)

    with mareforma.open(tmp_path, key_path=root_key) as g:
        root = g.assert_claim("anchor", generated_by="seed", seed=True)
        # An honest distinct-signer peer citing the anchor. An ESTABLISHED
        # claim is never a convergence peer, so this stays PRELIMINARY.
        peer = g.assert_claim(
            "honest peer", supports=[root], generated_by="lab_a",
            signer=peer_signer,
        )
        # A SECOND seed citing the same anchor (signed by the enrolled root, the
        # only key that can seed). Born ESTABLISHED, it must not ride its own
        # promotion nor abort the honest peer's.
        second = g.assert_claim(
            "second seed", supports=[root], generated_by="seed", seed=True,
        )

        assert g.get_claim(second)["support_level"] == "ESTABLISHED"
        # The seed must not be flagged for a convergence retry that can never
        # succeed.
        flag = g._conn.execute(
            "SELECT convergence_retry_needed FROM claims WHERE claim_id = ?",
            (second,),
        ).fetchone()["convergence_retry_needed"]
        assert flag == 0
        # The honest peer stays PRELIMINARY (a seed is not a valid peer), but it
        # is not errored into a permanent stall.
        assert g.get_claim(peer)["support_level"] == "PRELIMINARY"

    # refresh_convergence must report no permanently-pending work.
    with mareforma.open(tmp_path, key_path=root_key) as g:
        result = g.refresh_convergence()
        assert result["still_pending"] == 0
