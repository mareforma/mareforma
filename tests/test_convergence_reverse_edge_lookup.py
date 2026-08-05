"""the REPLICATED convergence candidate lookup must not full-scan.

The candidate-peer query joined ``claims`` against ``json_each(supports_json)``,
which no index can serve, so every converging insert scanned and JSON-parsed the
whole claims table (O(N) per insert, O(N^2) to build a graph). The indexed
reverse-edge cache (``idx_supports_reverse``) already answers "what claims cite
anchor X"; the convergence check must use it instead of ``json_each``, and must
re-read each candidate's authoritative ``supports_json`` so a stale cache row
cannot promote a claim that does not actually cite the anchor.
"""
from __future__ import annotations

import mareforma
from tests._helpers import _bootstrap_key, _two_signers


def _seed_and_peer(g, signer):
    up = g.assert_claim("anchor", generated_by="seed", seed=True)
    x = g.assert_claim("X", supports=[up], generated_by="lab_a", signer=signer)
    return up, x


def test_convergence_insert_does_not_json_each_scan(tmp_path):
    """A converging insert must not run the candidate-peer json_each scan over
    the whole claims table; it finds candidates through the reverse-edge cache.

    The bounded acyclicity walk keeps its own json_each (it follows the new
    claim's upstream by primary key, not a full scan), so the discriminator is
    the ``claims c, json_each(c.supports_json)`` cross join the old candidate
    query used.
    """
    sa, sb = _two_signers(tmp_path)
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        up, _x = _seed_and_peer(g, sa)
        seen: list[str] = []
        g._conn.set_trace_callback(seen.append)
        try:
            g.assert_claim("Y", supports=[up], generated_by="lab_b", signer=sb)
        finally:
            g._conn.set_trace_callback(None)
    scanned = [s for s in seen if "claims c, json_each" in s]
    assert not scanned, f"convergence still cross-joins json_each: {scanned}"
    # The reverse-edge cache is what serves the candidate lookup now.
    used_cache = [
        s for s in seen
        if "supports_cache.claim_supports" in s and "supports_claim_id IN" in s
    ]
    assert used_cache, "convergence did not use the reverse-edge cache"


def test_convergence_still_promotes_over_reverse_edge(tmp_path):
    """Two clean distinct-signer claims on a shared ESTABLISHED anchor still
    promote to REPLICATED through the reverse-edge candidate lookup."""
    sa, sb = _two_signers(tmp_path)
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        up, x = _seed_and_peer(g, sa)
        y = g.assert_claim("Y", supports=[up], generated_by="lab_b", signer=sb)
        assert g.get_claim(x)["support_level"] == "REPLICATED"
        assert g.get_claim(y)["support_level"] == "REPLICATED"


def test_stale_cache_edge_cannot_promote_a_non_citing_claim(tmp_path):
    """The reverse-edge cache is rebuildable, so a drifted edge that names an
    anchor the claim does not actually cite must not smuggle it into a
    promotion. The authoritative supports_json is the source of truth."""
    sa, sb = _two_signers(tmp_path)
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        up = g.assert_claim("anchor", generated_by="seed", seed=True)
        # z is a clean distinct-signer PRELIMINARY claim that cites NOTHING.
        z = g.assert_claim("Z", generated_by="lab_z", signer=sb)
        # Plant a stale reverse edge claiming z cites the anchor.
        g._conn.execute(
            "INSERT INTO supports_cache.claim_supports "
            "(claim_id, supports_claim_id, position) VALUES (?, ?, 0)",
            (z, up),
        )
        g._conn.commit()
        # A genuine peer citing the anchor triggers convergence. The candidate
        # lookup surfaces z through the stale edge, but its own supports_json
        # does not cite the anchor, so it must not promote.
        y = g.assert_claim("Y", supports=[up], generated_by="lab_y", signer=sa)
        assert g.get_claim(z)["support_level"] == "PRELIMINARY"
        # y has no valid peer (z is filtered out), so it stays PRELIMINARY too.
        assert g.get_claim(y)["support_level"] == "PRELIMINARY"


def test_stale_cache_edge_is_not_reported_as_lineage(tmp_path):
    """query_provenance reads the same rebuildable cache, so it owes the same
    re-check: an edge no signed supports_json attests is not lineage."""
    _sa, sb = _two_signers(tmp_path)
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        up = g.assert_claim("anchor", generated_by="seed", seed=True)
        z = g.assert_claim("Z", generated_by="lab_z", signer=sb)
        g._conn.execute(
            "INSERT INTO supports_cache.claim_supports "
            "(claim_id, supports_claim_id, position) VALUES (?, ?, 0)",
            (z, up),
        )
        g._conn.commit()

        assert g.get_claim(z)["supports_json"] == "[]"
        assert [e["claim_id"] for e in g.query_provenance(z)["upstream"]] == []
        anchor_prov = g.query_provenance(up)
        assert [e["claim_id"] for e in anchor_prov["downstream"]] == []
