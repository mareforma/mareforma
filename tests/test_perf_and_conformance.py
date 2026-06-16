"""Perf-pin + cross-conformance tests."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

import mareforma
from mareforma import _supports


# ----------------------------------------------------------------------------
# Perf pin: claim_supports cache walk at scale
# ----------------------------------------------------------------------------
#
# The cache exists so REPLICATED queries don't degrade to a full
# table scan as the graph grows. The pin documents the graph's
# scaling promise:
#
#   10k claims  → p99 < 100ms
#   50k claims  → p99 < 300ms
#
# The 50k case is opt-in via -k or marker because building the graph
# costs ~20s on a laptop and we don't want it in the default suite.


def _build_wide_dag(
    graph: "mareforma.EpistemicGraph",
    n: int,
    *,
    fanout: int = 2,
    seed_pool: int = 256,
) -> list[str]:
    """Build a wide-but-shallow DAG of *n* claims.

    Each new claim supports up to *fanout* randomly-chosen claims
    from a sliding window of size *seed_pool* (avoiding the
    deep-chain pattern that trips the 1024-hop cycle-detection cap).
    Returns the list of claim_ids in insertion order so the caller
    can pick a leaf to walk from.
    """
    import random as _random
    rng = _random.Random(42)  # reproducible
    ids: list[str] = []
    seed = graph.assert_claim("seed")
    ids.append(seed)
    for i in range(1, n):
        # Pool of recently-seen ids to support; sliding window keeps
        # the per-walk recursion depth bounded.
        pool = ids[max(0, len(ids) - seed_pool):]
        k = min(fanout, len(pool))
        supports = rng.sample(pool, k=k) if k else []
        cid = graph.assert_claim(f"claim-{i}", supports=supports)
        ids.append(cid)
    return ids


@pytest.mark.slow
def test_supports_cache_50k_walk_under_300ms(tmp_path: Path) -> None:
    """50k claims, walk_upstream p99 < 300ms.

    DAG is wide-but-shallow (fanout=2, seed-pool=256) so the cycle-
    detection depth cap (1024 hops) isn't hit while still exercising
    a substantial recursive walk.
    """
    n = 50_000
    with mareforma.open(tmp_path) as graph:
        ids = _build_wide_dag(graph, n)

    with mareforma.open(tmp_path) as graph:
        # Walk from the most recently created claim (densest cache
        # coverage). depth=4 mirrors the query_provenance default.
        leaf = ids[-1]
        latencies: list[float] = []
        for _ in range(100):
            t0 = time.perf_counter()
            _supports.walk_upstream(graph._conn, leaf, depth=4)
            latencies.append(time.perf_counter() - t0)
        latencies.sort()
        p99 = latencies[int(0.99 * len(latencies))]
        assert p99 < 0.3, (
            f"50k-claim p99 walk_upstream = {p99*1000:.1f}ms exceeds "
            "300ms pin"
        )


def _percentile(samples: list[float], q: float) -> float:
    """Linear-interp percentile in [0, 1]. ``q=0.99`` → p99."""
    if not samples:
        raise ValueError("samples must be non-empty")
    s = sorted(samples)
    if len(s) == 1:
        return s[0]
    rank = q * (len(s) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(s) - 1)
    frac = rank - lo
    return s[lo] + frac * (s[hi] - s[lo])


def test_supports_cache_1k_walk_stays_subgraph_bounded(tmp_path: Path) -> None:
    """1k-scale pin runs in the default suite so the regression is
    caught on every CI invocation.

    ``walk_upstream(depth=4)`` is a recursive CTE over the *indexed*
    supports cache, so its cost is bounded by the reachable subgraph
    (~fanout**depth), not the total claim count. The guard against a
    regression to an O(N) scan is expressed *relative* to a same-machine
    primary-key lookup rather than as an absolute wall-clock: a PK lookup
    rides a different index, so it won't share a supports-cache
    regression, yet it tracks the machine's general speed. Under a slow
    or contended runner both measurements rise together and the ratio
    holds — which is what lets this run in the default suite without
    flaking. The healthy ratio is ~10x; an O(N) regression at n=1000
    would push it into the hundreds.
    """
    n = 1_000
    with mareforma.open(tmp_path) as graph:
        ids = _build_wide_dag(graph, n)

    with mareforma.open(tmp_path) as graph:
        leaf = ids[-1]
        conn = graph._conn
        warmup = 50
        samples = 1000
        walk_latencies: list[float] = []
        ref_latencies: list[float] = []
        for i in range(warmup + samples):
            t0 = time.perf_counter()
            _supports.walk_upstream(conn, leaf, depth=4)
            walk = time.perf_counter() - t0
            t0 = time.perf_counter()
            conn.execute(
                "SELECT 1 FROM claims WHERE claim_id = ?", (leaf,)
            ).fetchone()
            ref = time.perf_counter() - t0
            if i >= warmup:
                walk_latencies.append(walk)
                ref_latencies.append(ref)
        walk_p99 = _percentile(walk_latencies, 0.99)
        ref_p99 = _percentile(ref_latencies, 0.99)
        # 40x the PK-lookup floor (~4x over the healthy ~10x ratio) absorbs
        # jitter; the additive 0.5ms cushion guards the degenerate case
        # where the floor is too small to measure. An O(N) walk regression
        # blows far past this.
        ceiling = ref_p99 * 40 + 0.0005
        assert walk_p99 < ceiling, (
            f"walk_upstream p99={walk_p99*1000:.3f}ms exceeds "
            f"{ceiling*1000:.3f}ms (PK-lookup p99={ref_p99*1000:.3f}ms × 40 "
            f"+ 0.5ms) — walk is no longer subgraph-bounded"
        )


# ----------------------------------------------------------------------------
# Cross-exporter conformance
# ----------------------------------------------------------------------------
#
# mareforma ships four exporters: jsonld (mareforma-native), in-
# toto-v1 (unsigned Statement v1), ro-crate-1.2 (RO-Crate Process Run
# Crate), prov-o (W3C PROV-O). All four MUST encode the same claim_id
# in the same URN shape so downstream consumers can join across
# formats without ambiguity.


class TestCrossExporterConformance:
    def _seed(self, tmp_path: Path) -> tuple[str, str]:
        from mareforma import signing as _signing
        key_path = tmp_path / "asserter.key"
        _signing.save_private_key(_signing.generate_keypair(), key_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            a = graph.assert_claim(
                "upstream finding",
                classification="DERIVED",
                generated_by="lab-A",
                seed=True,
            )
            b = graph.assert_claim(
                "downstream conclusion",
                classification="ANALYTICAL",
                supports=[a],
                generated_by="lab-B",
            )
        return a, b

    def test_claim_id_consistent_across_formats(self, tmp_path: Path) -> None:
        from mareforma.exporters.in_toto import build_statement
        from mareforma.exporters.ro_crate import build_crate
        from mareforma.exporters.prov_o import build_prov_o
        a, b = self._seed(tmp_path)

        statement = build_statement(tmp_path)
        crate = build_crate(tmp_path)
        prov = build_prov_o(tmp_path)

        # in-toto: subject[].name is "urn:mareforma:claim:<uuid>"
        intoto_subjects = {s["name"] for s in statement["subject"]}
        assert f"urn:mareforma:claim:{a}" in intoto_subjects
        assert f"urn:mareforma:claim:{b}" in intoto_subjects

        # RO-Crate: each claim has a CreateAction @id of the same URN
        crate_ids = {n.get("@id") for n in crate["@graph"]}
        assert f"urn:mareforma:claim:{a}" in crate_ids
        assert f"urn:mareforma:claim:{b}" in crate_ids

        # PROV-O: each claim is a prov:Entity with "mareforma:claim:<uuid>"
        # @id (uses the prefixed form via the @context mapping)
        prov_ids = {n.get("@id") for n in prov["@graph"]}
        assert f"mareforma:claim:{a}" in prov_ids
        assert f"mareforma:claim:{b}" in prov_ids

    def test_text_consistent_across_formats(self, tmp_path: Path) -> None:
        from mareforma.exporters.ro_crate import build_crate
        from mareforma.exporters.prov_o import build_prov_o
        a, b = self._seed(tmp_path)

        crate = build_crate(tmp_path)
        prov = build_prov_o(tmp_path)

        # RO-Crate carries claim text as MediaObject.text
        text_obj_a = next(
            n for n in crate["@graph"]
            if n.get("@id") == f"#claim-text/{a}"
        )
        assert text_obj_a["text"] == "upstream finding"

        # PROV-O carries (truncated) claim text as prov:label on the
        # Entity. Compare prefix to handle the 120-char truncation.
        entity_a = next(
            n for n in prov["@graph"]
            if n.get("@id") == f"mareforma:claim:{a}"
        )
        assert "upstream finding" in entity_a["prov:label"]

    def test_signed_bundle_anchors_same_claim_id(
        self, tmp_path: Path,
    ) -> None:
        # The signed in-toto bundle (via mareforma.export_bundle) must
        # anchor the same urn:mareforma:claim:<uuid> identifiers as the
        # unsigned in-toto exporter and the RO-Crate exporter.
        from mareforma.export_bundle import build_statement
        a, _b = self._seed(tmp_path)
        signed_statement = build_statement(tmp_path)
        signed_subjects = {s["name"] for s in signed_statement["subject"]}
        assert f"urn:mareforma:claim:{a}" in signed_subjects

    def test_supports_chain_consistent_across_formats(
        self, tmp_path: Path,
    ) -> None:
        from mareforma.exporters.ro_crate import build_crate
        from mareforma.exporters.prov_o import build_prov_o
        a, b = self._seed(tmp_path)

        crate = build_crate(tmp_path)
        prov = build_prov_o(tmp_path)

        # RO-Crate: CreateAction.object[] references upstream claim_ids.
        action_b = next(
            n for n in crate["@graph"]
            if n.get("@id") == f"urn:mareforma:claim:{b}"
        )
        crate_upstream = {o["@id"] for o in action_b.get("object", [])}
        assert f"urn:mareforma:claim:{a}" in crate_upstream

        # PROV-O: Entity.wasDerivedFrom references upstream Entity @id.
        entity_b = next(
            n for n in prov["@graph"]
            if n.get("@id") == f"mareforma:claim:{b}"
        )
        derived = entity_b.get("prov:wasDerivedFrom") or []
        if isinstance(derived, dict):
            derived = [derived]
        prov_upstream = {d["@id"] for d in derived}
        assert f"mareforma:claim:{a}" in prov_upstream
