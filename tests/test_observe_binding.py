"""Signed binding, verify-on-read, promotion gate, and the additive-axis rules.

Covers the verdict bound into the signed envelope, the backward-compatible
absence of the field on pre-observer claims, restore tamper detection, the
sign-after-author invariant, the promotion gate that keeps non-GROUNDED
findings off the support ladder, and the regression that the declared
classification axis is untouched.
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest

import mareforma
import mareforma.observe as obs
from mareforma import signing as S
from mareforma.observe import GroundingVerdict, ObservedGrounding as OG

sys.path.insert(0, str(Path(__file__).parent))
from epistemic._builders import (  # noqa: E402
    _bootstrap_validator_key,
    _prop,
    _smd,
    _superiority,
    open_graph,
)


def _grounded(reason="cited read returned data"):
    return GroundingVerdict(OG.GROUNDED, reason, cited_sources=("/data/trial.csv",))


def _ungrounded():
    return GroundingVerdict(OG.UNGROUNDED, "no cited read", cited_sources=("/d.csv",))


def _finding(graph, grounding=None, data_id="sha256:" + "a" * 64, generated_by="run/1"):
    return graph.assert_finding(
        _prop(), _superiority(), _smd(-0.8, p=0.001),
        data_id=data_id, grounding=grounding, generated_by=generated_by,
    )


# -- envelope binding --------------------------------------------------------

def test_verdict_bound_into_signed_envelope(tmp_path):
    with open_graph(tmp_path) as g:
        res = _finding(g, _grounded())
        row = g._conn.execute(
            "SELECT observed_grounding, signature_bundle FROM claims "
            "WHERE claim_id = ?",
            (res["claim_id"],),
        ).fetchone()
    assert row["observed_grounding"]
    env = json.loads(row["signature_bundle"])
    predicate = S.claim_predicate_from_envelope(env)
    # The signed predicate carries the same record as the queryable column.
    assert predicate["observed_grounding"] == json.loads(row["observed_grounding"])
    assert predicate["observed_grounding"]["grounding"] == "GROUNDED"


def test_no_verdict_leaves_signed_bytes_unchanged(tmp_path):
    # A claim asserted without the observer must omit the field entirely, so a
    # pre-observer claim and a no-verdict claim are byte-identical.
    with open_graph(tmp_path) as g:
        res = _finding(g, grounding=None)
        row = g._conn.execute(
            "SELECT observed_grounding, signature_bundle FROM claims "
            "WHERE claim_id = ?",
            (res["claim_id"],),
        ).fetchone()
    assert row["observed_grounding"] is None
    predicate = S.claim_predicate_from_envelope(json.loads(row["signature_bundle"]))
    assert "observed_grounding" not in predicate


def test_absent_field_reads_as_not_present_not_tampered(tmp_path):
    # The optional/versioned contract: an envelope without the field verifies
    # cleanly (it is a valid pre-observer claim), never a tamper failure.
    with open_graph(tmp_path) as g:
        res = _finding(g, grounding=None)
        env = json.loads(
            g._conn.execute(
                "SELECT signature_bundle FROM claims WHERE claim_id = ?",
                (res["claim_id"],),
            ).fetchone()["signature_bundle"]
        )
        keyid = env["signatures"][0]["keyid"]
        pub = _pubkey_for(g, keyid)
    assert S.verify_envelope(env, pub) is True


def _pubkey_for(graph, keyid):
    from mareforma import validators as V

    row = V.get_validator(graph._conn, keyid)
    import base64

    return S.public_key_from_pem(base64.standard_b64decode(row["pubkey_pem"]))


# -- verify-on-read via restore ---------------------------------------------

def test_restore_round_trips_the_verdict(tmp_path):
    with open_graph(tmp_path) as g:
        res = _finding(g, _grounded())
        cid = res["claim_id"]
    shutil.rmtree(tmp_path / ".mareforma", ignore_errors=True)
    report = mareforma.restore(tmp_path)
    assert report["claims_restored"] >= 1
    with open_graph(tmp_path) as g:
        row = g._conn.execute(
            "SELECT observed_grounding FROM claims WHERE claim_id = ?", (cid,)
        ).fetchone()
    assert row["observed_grounding"], "verdict lost across restore"


def test_restore_round_trips_a_non_grounded_verdict(tmp_path):
    # Only GROUNDED was exercised across restore. A non-promoting verdict
    # (UNGROUNDED here) must survive verify-on-read with its state intact, not
    # merely as "some verdict present."
    with open_graph(tmp_path) as g:
        res = _finding(g, _ungrounded())
        cid = res["claim_id"]
    shutil.rmtree(tmp_path / ".mareforma", ignore_errors=True)
    report = mareforma.restore(tmp_path)
    assert report["claims_restored"] >= 1
    with open_graph(tmp_path) as g:
        row = g._conn.execute(
            "SELECT observed_grounding FROM claims WHERE claim_id = ?", (cid,)
        ).fetchone()
    assert json.loads(row["observed_grounding"])["grounding"] == "UNGROUNDED"


def test_restore_rejects_a_tampered_verdict(tmp_path):
    with open_graph(tmp_path) as g:
        _finding(g, _grounded())
    toml_path = tmp_path / "claims.toml"
    # The signed predicate rides inside the base64 DSSE bundle, so replacing the
    # literal "GROUNDED" flips only the denormalized observed_grounding column —
    # exactly the row-vs-envelope binding check. Pin that message so an unrelated
    # restore failure cannot make this test pass by accident.
    toml_path.write_text(toml_path.read_text().replace("GROUNDED", "UNGROUNDED"))
    shutil.rmtree(tmp_path / ".mareforma", ignore_errors=True)
    with pytest.raises(mareforma.RestoreError, match="observed-grounding"):
        mareforma.restore(tmp_path)


# -- sign-after-author invariant --------------------------------------------

def test_asserting_inside_open_scope_is_refused(tmp_path):
    with open_graph(tmp_path) as g:
        with pytest.raises(RuntimeError, match="observe"):
            with obs.observe(cites="/x"):
                g.assert_claim("authored and signed inside an open scope")


# -- promotion gate ----------------------------------------------------------

def test_ungrounded_finding_does_not_promote(tmp_path):
    # A finding whose execution shows it is not grounded must never ride into
    # REPLICATED, even when a distinct-signer peer would otherwise converge.
    key_b = _bootstrap_validator_key(tmp_path)
    with open_graph(tmp_path) as g:
        g.assert_claim("established anchor", seed=True)
        prop, pred = _prop(), _superiority()
        g.register_plan(prop, pred)
        g.submit_finding(
            prop, pred, _smd(-0.8, p=0.001), data_id="dsA", generated_by="lab_a",
            grounding=_ungrounded(),
        )
        # Peer from a distinct signer, sharing the ESTABLISHED anchor.
        with mareforma.open(tmp_path, key_path=key_b) as g2:
            prop2 = _prop()
            pred2 = _superiority()
            g2.register_plan(prop2, pred2)
            g2.submit_finding(
                prop2, pred2, _smd(-0.8, p=0.001), data_id="dsB",
                generated_by="lab_b", grounding=_ungrounded(),
            )
        levels = [
            r["support_level"]
            for r in g._conn.execute(
                "SELECT support_level FROM claims WHERE observed_grounding IS NOT NULL"
            ).fetchall()
        ]
    assert levels, "expected finding claims with a recorded verdict"
    assert all(lvl == "PRELIMINARY" for lvl in levels), levels


def test_idempotent_replay_reports_the_stored_verdict(tmp_path):
    # An idempotent replay reuses the first write's signed claim and does not
    # re-record grounding, so the return must report the STORED verdict, not a
    # different one the replay happened to pass.
    with open_graph(tmp_path) as g:
        prop, pred = _prop(), _superiority()
        g.register_plan(prop, pred)
        first = g.submit_finding(
            prop, pred, _smd(-0.8, p=0.001), data_id="dsA", generated_by="a",
            grounding=_grounded(),
        )
        assert first["grounding"]["grounding"] == "GROUNDED"
        replay = g.submit_finding(
            prop, pred, _smd(-0.8, p=0.001), data_id="dsA", generated_by="a",
            grounding=_ungrounded(),  # a different verdict on the replay
        )
    assert replay["idempotent"] is True
    assert replay["grounding"]["grounding"] == "GROUNDED"  # stored, not the new one


def test_grounding_promotes_helper():
    from mareforma.db import _observed_grounding_promotes

    assert _observed_grounding_promotes(None) is True  # pre-observer: unaffected
    assert _observed_grounding_promotes('{"grounding":"GROUNDED"}') is True
    assert _observed_grounding_promotes('{"grounding":"UNGROUNDED"}') is False
    assert _observed_grounding_promotes('{"grounding":"OPAQUE"}') is False
    assert _observed_grounding_promotes("not json") is False  # fail-closed
    assert _observed_grounding_promotes("") is False  # matches the SQL gate


def test_sql_promotion_guard_fails_closed_on_malformed_column():
    # The promotion query's grounding guard must fail closed on a malformed or
    # empty observed_grounding column, matching the Python helper — NOT raise.
    # SQLite does not short-circuit `json_valid(x) AND json_extract(x, ...)`, so
    # json_extract is still evaluated and throws "malformed JSON"; the guard
    # must use CASE. A single corrupt row would otherwise abort the whole
    # convergence scan. Exercised as a WHERE filter, the way the real query uses
    # it: only NULL (pre-observer) and GROUNDED rows survive.
    import sqlite3

    guard = (
        "col IS NULL OR ("
        "CASE WHEN json_valid(col) "
        "THEN json_extract(col, '$.grounding') ELSE NULL END) = 'GROUNDED'"
    )
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE t(id INTEGER, col TEXT)")
    con.executemany(
        "INSERT INTO t(id, col) VALUES (?, ?)",
        [(1, None), (2, ""), (3, "not json"),
         (4, '{"grounding":"GROUNDED"}'), (5, '{"grounding":"UNGROUNDED"}')],
    )
    survivors = [r[0] for r in con.execute(f"SELECT id FROM t WHERE {guard}")]
    con.close()
    # NULL (pre-observer) and GROUNDED promote; '', malformed, UNGROUNDED excluded.
    assert survivors == [1, 4]


# -- additive axis regression ------------------------------------------------

def test_declared_classification_untouched_by_observed_axis(tmp_path):
    # The observed axis is additive: asserting a claim with a grounding verdict
    # leaves the declared classification exactly as passed, in its own column
    # and its own value space.
    with open_graph(tmp_path) as g:
        cid = g.assert_claim(
            "a declared-analytical claim",
            classification="ANALYTICAL",
            observed_grounding=_grounded().to_signed_dict(),
        )
        row = g._conn.execute(
            "SELECT classification, observed_grounding FROM claims "
            "WHERE claim_id = ?",
            (cid,),
        ).fetchone()
    assert row["classification"] == "ANALYTICAL"
    assert json.loads(row["observed_grounding"])["grounding"] == "GROUNDED"
    # The two axes never share a value space.
    assert row["classification"] not in ("GROUNDED", "UNGROUNDED", "OPAQUE")
