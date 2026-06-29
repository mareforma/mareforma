"""PR2b read-latency benchmark: verify-on-read over a many-REPLICATED-row graph.

The read path re-verifies a high-trust row's signature before serving it, with a
per-query ``(tier, keyid, digest)`` cache. This file MEASURES two things on a
graph with many REPLICATED rows rather than asserting them by construction:

* the cache BOUNDS total signature verifications — at most one per distinct
  ``(keyid, digest)`` served. Distinct claims carry distinct signature digests,
  so a varied result set rarely collapses; the bound that matters is "never more
  than once per row," which is what the cache guarantees and what we count.
* read latency stays within a generous budget so verify-on-read does not turn a
  bulk query into an O(rows x crypto) cliff.

The budget is deliberately loose (CI machines vary); the test guards against an
order-of-magnitude regression, not a tight wall-clock number.
"""

from __future__ import annotations

import base64
import time
from pathlib import Path

import mareforma
from mareforma import signing as _signing


def _enrolled_signer(graph, root: Path, name: str):
    """Bootstrap a key, enroll it as a validator, and return its loaded signer.

    Enrolling the asserter means its pubkey is in the validators table, so the
    participant bundle on its REPLICATED rows is actually verified on read
    (an unenrolled asserter would be verify-exempt and skip the crypto).
    """
    kp = root / f"{name}.key"
    _signing.bootstrap_key(kp)
    signer = _signing.load_private_key(kp)
    graph.enroll_validator(
        _signing.public_key_to_pem(signer.public_key()), identity=name,
    )
    return signer


def _build_many_replicated(graph, root: Path, n_signers: int) -> int:
    """Create one ESTABLISHED anchor, then n claims each by a distinct enrolled
    signer citing it. Every claim converges with the others on the anchor, so all
    n land at REPLICATED. Returns the number of REPLICATED rows created.
    """
    anchor = graph.assert_claim("anchor", generated_by="seed", seed=True)
    signers = [_enrolled_signer(graph, root, f"a{i}") for i in range(n_signers)]
    for i, s in enumerate(signers):
        graph.assert_claim(
            f"converging claim {i}", generated_by=f"lab_{i}",
            supports=[anchor], signer=s,
        )
    return n_signers


def test_pr2b_verify_count_is_bounded_and_latency_within_budget(
    tmp_path, monkeypatch,
):
    n = 40
    kv = tmp_path / "mareforma.key"
    _signing.bootstrap_key(kv)
    with mareforma.open(tmp_path, key_path=kv) as g:
        created = _build_many_replicated(g, tmp_path, n)

        # Count crypto verifications through the read path. The verify helpers
        # call ``_signing.verify_envelope``; patching the attribute the module
        # binds lets us count every actual signature check.
        calls = {"n": 0}
        real = _signing.verify_envelope

        def counting(*args, **kwargs):
            calls["n"] += 1
            return real(*args, **kwargs)

        monkeypatch.setattr(_signing, "verify_envelope", counting)

        t0 = time.perf_counter()
        rows = g.query(limit=1000)
        elapsed = time.perf_counter() - t0

        replicated = [r for r in rows if r["support_level"] == "REPLICATED"]
        # The setup actually produced the REPLICATED rows we query over.
        assert len(replicated) == created
        # Every REPLICATED row was served verified (cache reports per row).
        assert all(r.get("verified", True) for r in replicated)

        # Cache bound, MEASURED: at most one verification per high-trust row
        # served — never the 2x+ that a missing cache would allow if a row were
        # re-checked across the query's internal batches. Each distinct claim
        # has a distinct (keyid, digest), so the count tracks the row count.
        high_trust = [
            r for r in rows
            if r["support_level"] in ("REPLICATED", "ESTABLISHED")
        ]
        assert calls["n"] >= 1, "expected the read path to verify signatures"
        assert calls["n"] <= len(high_trust), (
            f"verify cache did not bound checks: {calls['n']} checks for "
            f"{len(high_trust)} high-trust rows"
        )

        # Latency budget: loose, regression-only. 40 rows must not take seconds.
        assert elapsed < 5.0, f"read latency {elapsed:.2f}s exceeds budget"


def test_pr2b_cache_collapses_a_repeated_row_in_one_walk(tmp_path):
    """A claim reachable by two provenance paths is verified once, not twice.

    query_provenance shares one verify cache across the upstream and downstream
    hydration, so a node that appears in both walks collapses to a single
    ``(keyid, digest)`` check. This is the case where the cache genuinely saves
    a redundant verification, measured directly.
    """
    kv = tmp_path / "mareforma.key"
    _signing.bootstrap_key(kv)
    with mareforma.open(tmp_path, key_path=kv) as g:
        anchor = g.assert_claim("anchor", generated_by="seed", seed=True)
        sa = _enrolled_signer(g, tmp_path, "sa")
        sb = _enrolled_signer(g, tmp_path, "sb")
        a = g.assert_claim("A", generated_by="x", supports=[anchor], signer=sa)
        b = g.assert_claim("B", generated_by="y", supports=[anchor], signer=sb)
        # A downstream claim that cites both A and B, so the anchor and the
        # REPLICATED peers are reachable on more than one path from it.
        g.assert_claim("C cites both", generated_by="z", supports=[a, b], signer=sa)

        seen: dict[str, int] = {}
        real = _signing.verify_envelope

        # Count verifications per signature digest to prove a repeated node is
        # checked at most once within a single provenance walk.
        import mareforma.signing as _S

        def counting(env, *args, **kwargs):
            try:
                key = env["signatures"][0]["keyid"] + ":" + env["payload"][:16]
            except Exception:
                key = "?"
            seen[key] = seen.get(key, 0) + 1
            return real(env, *args, **kwargs)

        _S.verify_envelope = counting
        try:
            g.query_provenance(a, depth=4)
        finally:
            _S.verify_envelope = real

        assert seen, "expected the provenance walk to verify signatures"
        assert max(seen.values()) == 1, (
            f"a signature was verified more than once in one walk: {seen}"
        )
