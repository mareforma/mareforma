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
    up = g.assert_claim(up_text, generated_by="seed")
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
        # A distinct-signer peer converges on the same ESTABLISHED anchor.
        y = g.assert_claim("Y", supports=[up], generated_by="lab_b", signer=sb)


