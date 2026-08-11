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
import sqlite3
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
from tests._helpers import _two_signers  # noqa: E402


from mareforma.observe._citation import normalize_identifier

# Every verdict here is one the OBSERVER computed, from a real scope over a real
# file. Only a minted verdict writes the observed axis; a hand-built one is a
# declaration and is stored as DECLARED, neutralised out of GROUNDED.
# So these tests cannot construct their premise, and must not try to: minting a
# hand-built verdict is the attack the write path exists to refuse, not a way to
# set up a test. Where a GROUNDED verdict is wanted, a scope reads a file.
#
# The modal honest workflow cites a PATH in observe(cites=...) while the finding's
# data_id is a content address over the bytes. They are disjoint by construction,
# so the finding must also carry data_source= naming the same path for the verdict
# to bind. ``_dataset`` is that shared path.

_DEFAULT = object()


def _dataset(tmp_path, name="trial.csv"):
    """A real file to cite and read, and the path both sides normalize."""
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("arm,outcome\ntreat,0.42\ncontrol,0.71\n")
    return path


def _cited_path(tmp_path, name="trial.csv"):
    """The normalized identifier for :func:`_dataset`, as both sides store it."""
    return normalize_identifier(str(_dataset(tmp_path, name)))


def _grounded(tmp_path, cites=None, reads=None):
    """A GROUNDED verdict the observer computed, from reads it watched happen.

    ``cites`` is what the scope declares, ``reads`` what actually gets read
    (defaulting to all of it). The decoy case passes a ``reads`` subset, which is
    how a genuine verdict comes to name a source in ``cited_sources`` but not in
    ``grounded_sources``: the observer saw a read for one and not the other.
    """
    cites = [_dataset(tmp_path)] if cites is None else list(cites)
    reads = cites if reads is None else list(reads)
    with obs.observe(cites=[str(c) for c in cites]) as handle:
        for path in reads:
            Path(path).read_text()
    assert handle.verdict.grounding is OG.GROUNDED, handle.verdict.reason
    return handle.verdict


def _ungrounded(tmp_path):
    """An UNGROUNDED verdict the observer computed: cited, never read."""
    with obs.observe(cites=str(_dataset(tmp_path, "unread.csv"))) as handle:
        pass  # the step that would read the dataset never ran
    assert handle.verdict.grounding is OG.UNGROUNDED
    return handle.verdict


def _finding(
    graph, tmp_path, grounding=None, data_id="sha256:" + "a" * 64,
    generated_by="run/1", data_source=_DEFAULT,
):
    # data_source defaults to the modal cited path so a GROUNDED verdict binds; a
    # test exercising the disjoint attack passes data_source=None.
    if data_source is _DEFAULT:
        data_source = str(_dataset(tmp_path))
    return graph.assert_finding(
        _prop(), _superiority(), _smd(-0.8, p=0.001),
        data_id=data_id, data_source=data_source, grounding=grounding,
        generated_by=generated_by,
    )


# -- envelope binding --------------------------------------------------------

def test_verdict_bound_into_signed_envelope(tmp_path):
    with open_graph(tmp_path) as g:
        res = _finding(g, tmp_path, _grounded(tmp_path))
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
        res = _finding(g, tmp_path, grounding=None)
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
        res = _finding(g, tmp_path, grounding=None)
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
        res = _finding(g, tmp_path, _grounded(tmp_path))
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
        res = _finding(g, tmp_path, _ungrounded(tmp_path))
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
        _finding(g, tmp_path, _grounded(tmp_path))
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

def _converge_on_anchor(tmp_path, subject_grounding):
    """Return the support level of a claim carrying *subject_grounding*.

    The fixture is eligible to promote by construction: an ESTABLISHED anchor,
    a GROUNDED peer from a distinct signer citing it, and the claim under test
    citing the same anchor. So the only thing that can hold the level down is
    the grounding gate on the convergence path. The peer is GROUNDED on purpose:
    a non-GROUNDED peer is refused by the candidate SELECT's own clause, which
    would mask whether the gate on the new claim runs at all.
    """
    sa, sb = _two_signers(tmp_path)
    with open_graph(tmp_path) as g:
        anchor = g.assert_claim("established anchor", seed=True)
        g.assert_claim(
            "peer from a distinct signer", supports=[anchor],
            generated_by="lab_b", signer=sb,
            observed_grounding=_grounded(tmp_path).to_signed_dict(),
        )
        subject = g.assert_claim(
            "claim under test", supports=[anchor], generated_by="lab_a",
            signer=sa, observed_grounding=subject_grounding.to_signed_dict(),
        )
        return g.get_claim(subject)["support_level"]


def test_grounded_finding_promotes_on_convergence(tmp_path):
    # Positive control: the fixture really is eligible, so the PRELIMINARY
    # results below are the gate talking and not a precondition that never held.
    assert _converge_on_anchor(tmp_path, _grounded(tmp_path)) == "REPLICATED"


@pytest.mark.parametrize("verdict", [OG.UNGROUNDED, OG.OPAQUE])
def test_non_grounded_finding_does_not_promote(tmp_path, verdict):
    # A finding whose execution shows it is not grounded must never ride into
    # REPLICATED, even when a distinct-signer peer would otherwise converge.
    grounding = GroundingVerdict(
        verdict, "no cited read", cited_sources=(_cited_path(tmp_path),),
    )
    assert _converge_on_anchor(tmp_path, grounding) == "PRELIMINARY"


def test_idempotent_replay_reports_the_stored_verdict(tmp_path):
    # An idempotent replay reuses the first write's signed claim and does not
    # re-record grounding, so the return must report the STORED verdict, not a
    # different one the replay happened to pass.
    with open_graph(tmp_path) as g:
        prop, pred = _prop(), _superiority()
        g.register_plan(prop, pred)
        src = str(_dataset(tmp_path))
        first = g.submit_finding(
            prop, pred, _smd(-0.8, p=0.001), data_id="dsA",
            data_source=src, generated_by="a",
            grounding=_grounded(tmp_path),
        )
        assert first["grounding"]["grounding"] == "GROUNDED"
        replay = g.submit_finding(
            prop, pred, _smd(-0.8, p=0.001), data_id="dsA",
            data_source=src, generated_by="a",
            grounding=_ungrounded(tmp_path),  # a different verdict on the replay
        )
    assert replay["idempotent"] is True
    assert replay["grounding"]["grounding"] == "GROUNDED"  # stored, not the new one


def test_idempotent_replay_of_disjoint_verdict_fires_no_event_and_no_raise(tmp_path):
    # An idempotent replay reuses the stored verdict and discards the passed one,
    # so a disjoint GROUNDED on the replay must NOT fire a downgrade health event
    # (nothing is downgraded) nor raise in strict mode (nothing is written).
    with open_graph(tmp_path) as g:
        prop, pred = _prop(), _superiority()
        g.register_plan(prop, pred)
        src = str(_dataset(tmp_path))
        g.submit_finding(
            prop, pred, _smd(-0.8, p=0.001), data_id="sha256:" + "a" * 64,
            data_source=src, generated_by="a", grounding=_grounded(tmp_path),
        )
        # Replay with a disjoint GROUNDED and strict mode on: must not raise.
        # The disjoint verdict is earned on a different real file, so it is a
        # genuine observation of something the finding does not cite.
        replay = g.submit_finding(
            prop, pred, _smd(-0.8, p=0.001), data_id="sha256:" + "a" * 64,
            data_source=src, generated_by="a",
            grounding=_grounded(tmp_path, cites=[_dataset(tmp_path, "other.csv")]),
            grounding_strict=True,
        )
    assert replay["idempotent"] is True
    # The stored verdict is the first write's GROUNDED, untouched — the disjoint
    # replay was discarded, not applied. Without this the test would pass even if
    # the replay overwrote the stored verdict.
    assert replay["grounding"]["grounding"] == "GROUNDED"
    ops = [e.get("op") for e in _health_ops(tmp_path)]
    assert "grounding_citation_mismatch" not in ops


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


# -- verdict-citation binding -----------------------------------------------

def test_disjoint_verdict_downgrades_to_opaque(tmp_path):
    # The v0.3.8 attack, inverted: a GROUNDED verdict earned on /data/trial.csv,
    # bound onto a finding that cites only sha256:<other> with no matching
    # data_source, must NOT be stored as GROUNDED. Default mode downgrades to
    # OPAQUE with the disjoint reason.
    with open_graph(tmp_path) as g:
        res = _finding(g, tmp_path, _grounded(tmp_path), data_source=None)
    assert res["grounding"]["grounding"] == "OPAQUE"
    assert res["grounding"]["reason"] == (
        "verdict cited-set disjoint from finding citation"
    )


def test_disjoint_verdict_raises_in_strict_mode(tmp_path):
    from mareforma.observe import GroundingCitationMismatchError

    with open_graph(tmp_path) as g:
        with pytest.raises(GroundingCitationMismatchError):
            g.assert_finding(
                _prop(), _superiority(), _smd(-0.8, p=0.001),
                data_id="sha256:" + "a" * 64, grounding=_grounded(tmp_path),
                generated_by="run/1", grounding_strict=True,
            )


def test_modal_workflow_stays_grounded_through_round_trip(tmp_path):
    # Modal acceptance: cite the path in the verdict, pass data_source=same path, and
    # a content-addressed data_id. The finding must stay GROUNDED through
    # store -> verify-on-read -> restore. This is the test the release fails
    # without: binding must not punish the honest producer.
    with open_graph(tmp_path) as g:
        res = _finding(g, tmp_path, _grounded(tmp_path))
        cid = res["claim_id"]
    assert res["grounding"]["grounding"] == "GROUNDED"
    shutil.rmtree(tmp_path / ".mareforma", ignore_errors=True)
    report = mareforma.restore(tmp_path)
    assert report["claims_restored"] >= 1
    with open_graph(tmp_path) as g:
        row = g._conn.execute(
            "SELECT observed_grounding FROM claims WHERE claim_id = ?", (cid,)
        ).fetchone()
    assert json.loads(row["observed_grounding"])["grounding"] == "GROUNDED"


def test_content_address_citation_binds(tmp_path):
    # A finding that cites a sha256: data_id, with a verdict whose cited set is
    # that same content address, binds without any data_source.
    from mareforma.observe import _scope
    from mareforma.trust._store import content_address_data_id

    # A content address is matched on the DIGEST of the bytes that arrived, so it
    # needs a read the observer can hash, which the plain file-open path cannot
    # (it sees the open, not the bytes). The read is recorded through the same
    # entry point a wrapped transport calls, so the verdict below is still one
    # the observer's classifier computed from a read it was told about, not a
    # conclusion written by hand.
    ca = content_address_data_id(b"payload-bytes")
    with obs.observe(cites=ca, content_address=True) as handle:
        _scope.record_read("http", "https://api.example/x", True, content_address=ca)
    assert handle.verdict.grounding is OG.GROUNDED, handle.verdict.reason
    with open_graph(tmp_path) as g:
        res = _finding(
            g, tmp_path, handle.verdict, data_id=ca, data_source=None,
        )
    assert res["grounding"]["grounding"] == "GROUNDED"


def test_finding_without_matchable_citation_is_not_applicable(tmp_path):
    # A finding whose only citation is a string-fallback data_id (no data_source,
    # not content-addressed) has no matchable identifier to bind against — a bare
    # token is not a path or content address, and normalizing it would need a
    # realpath the read side cannot reproduce. So the binding is not-applicable:
    # the verdict is kept and annotated, never silently downgraded or trusted.
    with open_graph(tmp_path) as g:
        prop, pred = _prop(), _superiority()
        g.register_plan(prop, pred)
        res = g.submit_finding(
            prop, pred, _smd(-0.8, p=0.001), data_id="dsA", generated_by="a",
            grounding=_grounded(tmp_path),
        )
    assert res["grounding"]["grounding"] == "GROUNDED"
    assert "no finding citation to bind" in res["grounding"]["reason"]


def test_empty_verdict_cited_set_downgrades(tmp_path):
    # A GROUNDED verdict that names no cited source cannot demonstrate binding to
    # a finding that cites data; it downgrades.
    with open_graph(tmp_path) as g:
        res = _finding(
            g, tmp_path,
            GroundingVerdict(OG.GROUNDED, "hand-built", cited_sources=()),
        )
    assert res["grounding"]["grounding"] == "OPAQUE"


def test_source_name_never_binds(tmp_path):
    # Collision guard: the free-text source_name must NEVER participate in
    # binding. Even a verdict that GROUNDED on the source_name string (it rides in
    # grounded_sources), with no data_source and a content-addressed data_id,
    # stays disjoint — the finding's content address is never that free text.
    with open_graph(tmp_path) as g:
        res = g.assert_finding(
            _prop(), _superiority(), _smd(-0.8, p=0.001),
            data_id="sha256:" + "a" * 64, data_source=None,
            grounding=GroundingVerdict(
                OG.GROUNDED, "r",
                cited_sources=("my-free-text-source",),
                grounded_sources=("my-free-text-source",),
            ),
            generated_by="run/1",
        )
    assert res["grounding"]["grounding"] == "OPAQUE"


def test_decoy_cite_does_not_bind_finding(tmp_path):
    # The decoy-in-cite-set bypass. A verdict DECLARES both the finding's dataset
    # and an incidental decoy in its cited set, but a read was observed only for
    # the decoy (grounded_sources=(decoy,)). Binding checks the read-observed set,
    # not the declared cites, so the finding whose OWN data was never read does
    # not earn GROUNDED — it downgrades to OPAQUE. If binding checked the declared
    # cited set (the v0.3.9 pre-fix bug) this would wrongly MATCH and promote.
    decoy = _dataset(tmp_path, "decoy.csv")
    with open_graph(tmp_path) as g:
        res = _finding(
            g, tmp_path,
            _grounded(
                tmp_path, cites=[_dataset(tmp_path), decoy], reads=[decoy],
            ),
        )
    assert res["grounding"]["grounding"] == "OPAQUE"


def test_assert_claim_verdict_is_marked_unbound(tmp_path):
    # assert_claim carries no finding citation: source_name is free text and the
    # claim has no data_id / data_source, so the verdict-to-citation check has
    # nothing to compare. A verdict earned on a decoy read must therefore not
    # store as a clean GROUNDED; it keeps the not-applicable annotation the
    # finding path already emits, so a reader can tell binding was never
    # exercised rather than passed.
    decoy = _dataset(tmp_path, "decoy.csv")
    with open_graph(tmp_path) as g:
        claim_id = g.assert_claim(
            "IL-21 elevated in SLE CD4+ T cells",
            classification="ANALYTICAL",
            source_name="medeadb",
            observed_grounding=_grounded(
                tmp_path, cites=[decoy],
            ).to_signed_dict(),
        )
        stored = g._stored_grounding(claim_id)
    assert "no finding citation to bind" in stored["reason"]


def test_assert_finding_annotation_is_not_doubled(tmp_path):
    # The finding path binds before it calls assert_claim, so the claim path must
    # not append the not-applicable marker a second time.
    with open_graph(tmp_path) as g:
        prop, pred = _prop(), _superiority()
        g.register_plan(prop, pred)
        res = g.submit_finding(
            prop, pred, _smd(-0.8, p=0.001), data_id="dsA", generated_by="a",
            grounding=_grounded(tmp_path),
        )
        stored = g._stored_grounding(res["claim_id"])
    assert stored["reason"].count("no finding citation to bind") == 1


def test_read_side_rejects_disjoint_grounded_record():
    # The read-side re-check (verify-on-read / restore / audit) must reject a
    # stored GROUNDED record whose grounded set is disjoint from the finding's
    # citation — the defense against a hand-edited-then-re-signed row. A matching
    # record passes untouched; a v0.3.8 record (no grounded_sources) is skipped.
    from mareforma.db.restore import _verify_grounding_binding_on_read, RestoreError

    predicate = {"data_sources": [normalize_identifier("/data/trial.csv")],
                 "data_ids": []}
    disjoint = {"grounding": "GROUNDED",
                "grounded_sources": [normalize_identifier("/etc/hostname")]}
    with pytest.raises(RestoreError, match="binding violation"):
        _verify_grounding_binding_on_read("claim-x", disjoint, predicate)

    matched = {"grounding": "GROUNDED",
               "grounded_sources": [normalize_identifier("/data/trial.csv")]}
    _verify_grounding_binding_on_read("claim-x", matched, predicate)  # no raise
    legacy = {"grounding": "GROUNDED"}  # pre-v0.3.9: not checkable, skipped
    _verify_grounding_binding_on_read("claim-x", legacy, predicate)  # no raise

    # A GROUNDED record with an empty-but-present grounded set (grounded_sources
    # == []) against a finding that DOES cite data is the hand-edited case the
    # write side would have downgraded to OPAQUE: empty is checkable (not None),
    # disjoint from the citation, and must be rejected — not skipped like legacy.
    empty = {"grounding": "GROUNDED", "grounded_sources": []}
    with pytest.raises(RestoreError, match="binding violation"):
        _verify_grounding_binding_on_read("claim-x", empty, predicate)
    # Empty grounded set against a finding that cites nothing is not disjoint.
    _verify_grounding_binding_on_read(
        "claim-x", empty, {"data_sources": [], "data_ids": []}
    )  # no raise


def test_write_gate_and_both_read_checks_read_one_citation_rule():
    # Which identifiers a predicate binds is one rule: the write gate decides
    # whether a GROUNDED verdict is storable, the audit CLI and restore decide
    # whether a stored one is still honest. A non-string data_source is not a
    # bindable identifier, so all three drop it; restore counted it, which
    # turned a finding with nothing to bind into a binding violation on read.
    from mareforma._verify import claim_bound_sources
    from mareforma.db.restore import _verify_grounding_binding_on_read
    from mareforma.observe._binding import predicate_citation_sources

    cited = normalize_identifier("/data/trial.csv")
    predicate = {"data_sources": [cited, 42], "data_ids": []}
    assert predicate_citation_sources(predicate) == (cited,)
    assert claim_bound_sources(
        {"predicate_payload": json.dumps(predicate)}
    ) == (cited,)

    unbindable = {"data_sources": [42], "data_ids": []}
    assert predicate_citation_sources(unbindable) == ()
    assert claim_bound_sources(
        {"predicate_payload": json.dumps(unbindable)}
    ) == ()
    record = {"grounding": "GROUNDED", "grounded_sources": [cited]}
    _verify_grounding_binding_on_read("claim-x", record, unbindable)  # no raise


def _health_ops(root):
    path = root / ".mareforma" / "health.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def test_bind_downgrade_fires_health_event(tmp_path):
    # Every bind-time downgrade appends a grounding_citation_mismatch health
    # event, so a misconfigured producer is visible when drift starts.
    with open_graph(tmp_path) as g:
        _finding(g, tmp_path, _grounded(tmp_path), data_source=None)
    ops = [e.get("op") for e in _health_ops(tmp_path)]
    assert "grounding_citation_mismatch" in ops


def test_matched_bind_fires_no_mismatch_event(tmp_path):
    with open_graph(tmp_path) as g:
        _finding(g, tmp_path, _grounded(tmp_path))
    ops = [e.get("op") for e in _health_ops(tmp_path)]
    assert "grounding_citation_mismatch" not in ops


def test_string_data_id_with_data_source_survives_restore(tmp_path):
    # A finding matched via data_source, carrying a string-fallback data_id whose
    # normalized form the read side cannot reproduce, must NOT false-flag on
    # restore: the write and read sides bind against the same identifiers
    # (content-addressed data_ids + data_sources), so a legitimately signed
    # GROUNDED round-trips.
    src = str(_dataset(tmp_path))
    with open_graph(tmp_path) as g:
        prop, pred = _prop(), _superiority()
        g.register_plan(prop, pred)
        res = g.submit_finding(
            prop, pred, _smd(-0.8, p=0.001), data_id="dsA", data_source=src,
            generated_by="a", grounding=_grounded(tmp_path),
        )
        cid = res["claim_id"]
    assert res["grounding"]["grounding"] == "GROUNDED"
    shutil.rmtree(tmp_path / ".mareforma", ignore_errors=True)
    report = mareforma.restore(tmp_path)  # must not raise a binding violation
    assert report["claims_restored"] >= 1
    with open_graph(tmp_path) as g:
        row = g._conn.execute(
            "SELECT observed_grounding FROM claims WHERE claim_id = ?", (cid,)
        ).fetchone()
    assert json.loads(row["observed_grounding"])["grounding"] == "GROUNDED"


def test_read_side_binding_is_pure_string_no_filesystem(tmp_path):
    # Verify-on-read compares STORED normalized identifiers with no
    # filesystem access, so an honest claim whose cited path does not exist on
    # the verifier's host (a cross-host bundle) still restores clean. Simulate by
    # citing a path under a tmpdir that we delete before restore.
    missing = _dataset(tmp_path / "gone", "trial.csv")
    with open_graph(tmp_path) as g:
        res = _finding(
            g, tmp_path, _grounded(tmp_path, cites=[missing]),
            data_source=str(missing),
        )
        cid = res["claim_id"]
    assert res["grounding"]["grounding"] == "GROUNDED"
    # The cited path is gone by the time the verifier looks, which is the point.
    shutil.rmtree(tmp_path / "gone")
    shutil.rmtree(tmp_path / ".mareforma", ignore_errors=True)
    # The path never existed; restore must not touch the filesystem to check it.
    report = mareforma.restore(tmp_path)
    assert report["claims_restored"] >= 1
    with open_graph(tmp_path) as g:
        row = g._conn.execute(
            "SELECT observed_grounding FROM claims WHERE claim_id = ?", (cid,)
        ).fetchone()
    assert json.loads(row["observed_grounding"])["grounding"] == "GROUNDED"


# -- additive axis regression ------------------------------------------------

def test_declared_classification_untouched_by_observed_axis(tmp_path):
    # The observed axis is additive: asserting a claim with a grounding verdict
    # leaves the declared classification exactly as passed, in its own column
    # and its own value space.
    with open_graph(tmp_path) as g:
        cid = g.assert_claim(
            "a declared-analytical claim",
            classification="ANALYTICAL",
            observed_grounding=_grounded(tmp_path).to_signed_dict(),
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


# -- row-vs-envelope binding of the verdict ----------------------------------

FORGED = '{"grounding":"GROUNDED","reason":"forged"}'


def _ungrounded_claim(g, tmp_path):
    return g.assert_claim(
        "a finding the observer refused to ground",
        observed_grounding=_ungrounded(tmp_path).to_signed_dict(),
    )


def test_direct_verdict_update_is_refused(tmp_path):
    # The verdict is signed, chained and gates promotion, so a signed row may
    # not have it rewritten by direct SQL any more than its evidence vector.
    with open_graph(tmp_path) as g:
        cid = _ungrounded_claim(g, tmp_path)
        with pytest.raises(sqlite3.IntegrityError, match="signed_field_locked"):
            g._conn.execute(
                "UPDATE claims SET observed_grounding = ? WHERE claim_id = ?",
                (FORGED, cid),
            )


def test_forged_verdict_fails_the_audit(tmp_path):
    # A database tampered while the trigger was disarmed must still fail
    # `mareforma verify`: the envelope is canonical, the column must match it.
    from mareforma.db import verify_claim_signatures

    with open_graph(tmp_path) as g:
        cid = _ungrounded_claim(g, tmp_path)
        g._conn.execute(
            "DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering"
        )
        g._conn.execute(
            "UPDATE claims SET observed_grounding = ? WHERE claim_id = ?",
            (FORGED, cid),
        )
        g._conn.commit()
        row = dict(
            g._conn.execute(
                "SELECT * FROM claims WHERE claim_id = ?", (cid,),
            ).fetchone()
        )
        ok, reason = verify_claim_signatures(g._conn, row)
    assert ok is False
    assert "observed_grounding" in reason


def test_forged_verdict_is_unverified_on_the_replicated_read_path(tmp_path):
    # The ordinary read path, not only an explicit verify, must refuse a row
    # whose verdict disagrees with the envelope it was signed under.
    key_b = _bootstrap_validator_key(tmp_path)
    verdict = _grounded(tmp_path).to_signed_dict()
    with open_graph(tmp_path) as g:
        anchor = g.assert_claim("established anchor", seed=True)
        cid = g.assert_claim(
            "converged", supports=[anchor], observed_grounding=verdict,
        )
    with mareforma.open(tmp_path, key_path=key_b) as g2:
        g2.assert_claim(
            "converged", supports=[anchor], observed_grounding=verdict,
        )
    with open_graph(tmp_path) as g:
        assert g.get_claim(cid)["support_level"] == "REPLICATED"
        g._conn.execute(
            "DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering"
        )
        g._conn.execute(
            "UPDATE claims SET observed_grounding = ? WHERE claim_id = ?",
            (FORGED, cid),
        )
        g._conn.commit()
        assert g.get_claim(cid)["verified"] is False


def test_the_restore_gate_reads_a_predicate_that_carries_no_citation(tmp_path):
    """Pin WHERE the citation lives, which the unit test above does not.

    ``test_read_side_rejects_disjoint_grounded_record`` hands the gate a
    hand-built ``{"data_sources": [...]}`` and proves the comparison works. It
    does not prove the restore path ever supplies such a predicate, and it does
    not: ``restore`` passes the SIGNED claim predicate, which carries
    ``finding_record`` and no ``data_sources``, so the citation set comes back
    empty, the comparison reads NOT_APPLICABLE, and the gate cannot raise on a
    real row. The citation lives in the ``predicate_payload`` column, which the
    live audit path reads (``mareforma._verify.claim_bound_sources``).

    This test states the gap rather than hiding it. It is characterization, not
    approval: the restore-side re-check is inert on the real path, and closing it
    changes restore semantics, which is trust-core work, not a docs-and-surface
    change. If a future release binds the citation into the signed predicate,
    this test fails and should be replaced by one asserting the gate fires.
    """
    import json

    from mareforma import signing as _signing
    from mareforma.observe._binding import (
        BindingState, check_grounding_binding, predicate_citation_sources,
    )
    from tests.epistemic._builders import _prop, _smd, _superiority, open_graph

    data = tmp_path / "trial.csv"
    data.write_text("a\n1\n", encoding="utf-8")
    with open_graph(tmp_path) as g:
        prop, pred = _prop(), _superiority()
        g.register_plan(prop, pred)
        res = g.submit_finding(
            prop, pred, _smd(-0.8, p=0.001), data_id="dsA",
            data_source=str(data), generated_by="a",
        )
        row = g.get_claim(res["claim_id"])

    signed = _signing.claim_predicate_from_envelope(
        json.loads(row["signature_bundle"])
    )
    assert "finding_record" in signed
    assert "data_sources" not in signed
    assert predicate_citation_sources(signed) == ()

    # Which makes the comparison inert: no citation to be disjoint from.
    verdict = check_grounding_binding(("/etc/hostname",),
                                      predicate_citation_sources(signed))
    assert verdict.state is BindingState.NOT_APPLICABLE

    # The citation the gate would need is in the unsigned column beside it.
    payload = json.loads(row["predicate_payload"])
    assert predicate_citation_sources(payload) != ()
