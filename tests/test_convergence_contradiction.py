"""Convergence must respect a signed contradiction verdict.

A claim a validator has marked invalid (``t_invalid`` set) must not climb the
support ladder through convergence, and must not pull an honest peer up with it.
``record_replication_verdict`` already refuses to promote an invalidated claim;
the convergence path in ``_maybe_update_replicated_unlocked`` must agree.
"""
from __future__ import annotations

import json
import sqlite3

import mareforma
import mareforma.db.core as _core
from mareforma.health import _health_log_path
from tests._helpers import _bootstrap_key, _pem_of, _two_signers


def _seed_anchor_and_claim(g, *, up_text="anchor", x_text="X", signer=None):
    up = g.assert_claim(up_text, generated_by="seed", seed=True)
    x = g.assert_claim(x_text, supports=[up], generated_by="lab_a", signer=signer)
    return up, x


def test_invalidated_claim_does_not_ride_convergence_into_replicated(tmp_path):
    sa, sb = _two_signers(tmp_path)
    root_key = _bootstrap_key(tmp_path, "root.key")
    val_key = _bootstrap_key(tmp_path, "val.key")

    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.enroll_validator(_pem_of(val_key), identity="v")
        up, x = _seed_anchor_and_claim(g, signer=sa)
        # A counter-claim asserted after X, so X is the older of the pair.
        w = g.assert_claim("counter", generated_by="lab_w", signer=sb)

    # A signed contradiction marks the older claim (X) invalid; X stays
    # PRELIMINARY and status='open', so nothing but t_invalid blocks it.
    with mareforma.open(tmp_path, key_path=val_key) as g:
        g.record_contradiction_verdict(
            verdict_id="cv_1", member_claim_id=x, other_claim_id=w,
            confidence={"stance": "refutes"},
        )

    with mareforma.open(tmp_path, key_path=root_key) as g:
        cx = g.get_claim(x)
        assert cx["t_invalid"] is not None
        assert cx["support_level"] == "PRELIMINARY"
        # A distinct-signer peer converges on the same ESTABLISHED anchor.
        y = g.assert_claim("Y", supports=[up], generated_by="lab_b", signer=sb)

    with mareforma.open(tmp_path, key_path=root_key) as g:
        # The invalidated claim must not promote, and with no valid peer the
        # honest new claim stays PRELIMINARY too.
        assert g.get_claim(x)["support_level"] == "PRELIMINARY"
        assert g.get_claim(y)["support_level"] == "PRELIMINARY"


def test_convergence_still_promotes_two_clean_distinct_signers(tmp_path):
    """The gate must not over-block: two clean distinct-signer claims on a
    shared ESTABLISHED anchor still converge to REPLICATED."""
    sa, sb = _two_signers(tmp_path)
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        up, x = _seed_anchor_and_claim(g, signer=sa)
        y = g.assert_claim("Y", supports=[up], generated_by="lab_b", signer=sb)
        assert g.get_claim(x)["support_level"] == "REPLICATED"
        assert g.get_claim(y)["support_level"] == "REPLICATED"


def test_transient_convergence_failure_preserves_retry_and_records_health(
    tmp_path, monkeypatch,
):
    """A transient lock during the convergence re-check inside a flag flip must
    not silently strand the claim: the retry flag stays set (so
    refresh_convergence can retry) and a health event records the strand."""
    root_key = _bootstrap_key(tmp_path, "root.key")

    def _busy(*_a, **_k):
        raise sqlite3.OperationalError("database is locked")

    with mareforma.open(tmp_path, key_path=root_key) as g:
        cid = g.assert_claim("a resolved claim", generated_by="x")
        g._conn.execute(
            "UPDATE claims SET unresolved = 1 WHERE claim_id = ?", (cid,),
        )
        g._conn.commit()
        # Force the convergence re-check that mark_claim_resolved runs to fail
        # with a transient lock error.
        monkeypatch.setattr(_core, "_maybe_update_replicated_unlocked", _busy)
        _core.mark_claim_resolved(g._conn, tmp_path, cid)
        retry = g._conn.execute(
            "SELECT convergence_retry_needed FROM claims WHERE claim_id = ?",
            (cid,),
        ).fetchone()["convergence_retry_needed"]

    assert retry == 1, "a transient convergence failure must leave the claim retryable"
    events = [
        json.loads(line)
        for line in _health_log_path(tmp_path).read_text().splitlines()
        if line.strip()
    ]
    assert any(e["op"] == "convergence_retry" for e in events), (
        "a health event must record the stranded convergence re-check"
    )
