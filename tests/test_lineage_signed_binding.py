"""Model lineage is bound into the signed finding and re-authenticated on read.

The model-distinct axis reads ``evidence_lines.model_lineage``, a denormalised,
unsigned column. Binding the lineage into the signed observed record (the same
carrier as the grounding verdict) and rerouting the independence read to that
signed copy closes the forge: a column edited out of band no longer moves the
count. This mirrors the WHO-axis re-authentication ``_authentic_signer_keyid``
already applies to the signer column.
"""
from __future__ import annotations

import json
from pathlib import Path

import mareforma
from mareforma.trust._store import effective_independence, independence_counts
from tests._helpers import (
    _bootstrap_key, _enroll_key, _est, _pred, _prop, _verdict, _wipe_db,
)

_CLAUDE = "claude-3-5-sonnet-20241022"   # COMPUTED root: claude-3-5-sonnet
_GPT = "gpt-4o-2024-08-06"               # COMPUTED root: gpt-4o

_FORGED_COMPUTED = (
    '{"tier":"COMPUTED","model_id":"gpt-4o-2024-08-06",'
    '"family_root":"gpt-4o","provider":"openai","version":"2024-08-06",'
    '"method":"m","decoding":{},"attestor":"provider-host","digest":null}'
)


def _bypass_write_guard(conn) -> None:
    """Model an adversary with raw SQL access to graph.db.

    ``evidence_lines`` carries an append-only trigger, but that guard is durable
    schema a writer with database access can drop outright, so a tamper test
    drops it before rewriting the unsigned column. The read-path re-derivation,
    not the write trigger, is what every test here asserts still catches the
    forgery: the trigger raises the cost of writing the bad row, it is not the
    guarantee.
    """
    conn.execute("DROP TRIGGER IF EXISTS evidence_lines_append_only")
    conn.execute("DROP TRIGGER IF EXISTS evidence_lines_no_delete")


def _tamper_one_line(conn, root: str) -> None:
    """Rewrite exactly one evidence line's unsigned model_lineage column."""
    _bypass_write_guard(conn)
    line_id = conn.execute("SELECT line_id FROM evidence_lines LIMIT 1").fetchone()[0]
    conn.execute(
        "UPDATE evidence_lines SET model_lineage = ? WHERE line_id = ?",
        (root, line_id),
    )
    conn.commit()


def _donor_prop():
    """A proposition distinct from ``_prop()``, for the cross-claim staple."""
    from mareforma.trust import Direction, Proposition

    return Proposition(
        subject="TP53", relation="affects", object="apoptosis",
        direction=Direction.INCREASES,
        scope={"population": "TNBC", "condition": "in vitro"},
    )


def _finding_row(conn, data_id: str):
    """The signed claim row behind the finding that carries *data_id*."""
    return conn.execute(
        "SELECT f.claim_id, f.finding_id, c.signature_bundle, c.asserter_keyid, "
        " el.model_lineage "
        "FROM findings f JOIN claims c ON c.claim_id = f.claim_id "
        "JOIN evidence_lines el ON el.finding_id = f.finding_id "
        "WHERE el.data_id = ? AND c.signature_bundle IS NOT NULL LIMIT 1",
        (data_id,),
    ).fetchone()


def _forge_bundle(conn, data_id: str, lineage_json: str,
                  *, keyid: str | None = None) -> None:
    """Re-point one finding's SIGNED lineage to a distinct model, leaving the
    signature stale.

    The producer forge: rewrite the bundle payload's ``model_lineage`` and the
    denormalized column so the two agree. ``keyid`` stamps a fabricated,
    non-enrolled signer id (the outsider, whose bundle cannot be verified at
    all); left None the genuine signer id stays in place, so an enrolled peer
    editing its own payload is stopped by the signature check alone. Either way
    the read must treat the lineage as soft, never a counted distinct model.
    """
    import base64 as _b64
    _bypass_write_guard(conn)
    row = _finding_row(conn, data_id)
    env = json.loads(row["signature_bundle"])
    payload = json.loads(_b64.standard_b64decode(env["payload"]))
    payload["predicate"]["observed_grounding"]["model_lineage"] = json.loads(
        lineage_json)
    env["payload"] = _b64.standard_b64encode(
        json.dumps(payload).encode()).decode()
    if keyid is not None:
        env["signatures"][0]["keyid"] = keyid
    conn.execute("UPDATE claims SET signature_bundle = ? WHERE claim_id = ?",
                 (json.dumps(env), row["claim_id"]))
    conn.execute("UPDATE evidence_lines SET model_lineage = ? WHERE finding_id = ?",
                 (lineage_json, row["finding_id"]))
    conn.commit()


def _staple_bundle(conn, data_id: str, donor_data_id: str) -> None:
    """Staple the donor finding's genuine, verifying bundle onto this finding's
    claim, keyid and lineage column included.

    The cross-claim staple: every part of the stapled material is authentic, it
    was just issued for another claim. Only the claim-binding check stands
    between the donor's signed lineage and this finding's model axis.
    """
    _bypass_write_guard(conn)
    donor = _finding_row(conn, donor_data_id)
    target = _finding_row(conn, data_id)
    conn.execute(
        "UPDATE claims SET signature_bundle = ?, asserter_keyid = ? "
        "WHERE claim_id = ?",
        (donor["signature_bundle"], donor["asserter_keyid"], target["claim_id"]),
    )
    conn.execute("UPDATE evidence_lines SET model_lineage = ? WHERE finding_id = ?",
                 (donor["model_lineage"], target["finding_id"]))
    conn.commit()


class TestSignedLineageBinding:
    def test_forged_column_does_not_break_same_model_collapse(
        self, tmp_path: Path,
    ) -> None:
        """Two same-model checks whose one line's model_lineage column is forged
        to a distinct COMPUTED root still collapse to a single independent line:
        the read authenticates the column against the SIGNED lineage, so the
        unsigned column cannot inflate independence."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        cid = prop.content_id()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_CLAUDE),
            )
            # Forge one line's unsigned column to a distinct COMPUTED model.
            _tamper_one_line(g._conn, _FORGED_COMPUTED)
            eff = effective_independence(g._conn, cid)
        # The signed copy still roots both lines to claude, so the collapse holds.
        assert eff["number"] == 1

    def test_genuine_distinct_models_still_corroborate(self, tmp_path: Path) -> None:
        """The binding does not suppress a genuine cross-model pair: two distinct
        COMPUTED models each signed by an ENROLLED validator still read as
        effective 2. The second signer is enrolled so its bundle verifies; only
        an authenticated distinct model counts (see the fail-closed forge test)."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        prop, pred = _prop(), _pred()
        cid = prop.content_id()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_GPT),
            )
            eff = effective_independence(g._conn, cid)
        assert eff["number"] == 2
        assert eff["soft"] is False

    def test_nonenrolled_signed_forge_does_not_count(self, tmp_path: Path) -> None:
        """The strongest producer forge is blocked: re-pointing a finding's
        SIGNED lineage to a distinct model under a non-enrolled, unverifiable
        signature must NOT inflate the count. An unverifiable bundle reads soft
        (fail closed), never a trusted distinct model, so two same-model
        findings stay collapsed even when one bundle is forged."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        cid = prop.content_id()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_CLAUDE),
            )
            _forge_bundle(g._conn, "ds2", _FORGED_COMPUTED,
                          keyid="deadbeef-not-enrolled-fabricated")
            eff = effective_independence(g._conn, cid)
        # The forged bundle does not verify (non-enrolled signer), so its
        # lineage reads soft and cannot mint a distinct model. Count stays 1.
        assert eff["number"] == 1
        assert eff["soft"] is True

    def test_enrolled_signer_forge_does_not_count(self, tmp_path: Path) -> None:
        """The peer forge: a second lab already ENROLLED as a validator rewrites
        its own bundle payload's model lineage to a distinct model and leaves its
        real keyid in place, so the signer resolves to a real pubkey and the read
        runs the crypto check. The stale signature no longer covers the edited
        payload, so the lineage is unauthenticated and reads soft: the same-model
        pair stays collapsed at one."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        prop, pred = _prop(), _pred()
        cid = prop.content_id()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_CLAUDE),
            )
            _forge_bundle(g._conn, "ds2", _FORGED_COMPUTED)
            eff = effective_independence(g._conn, cid)
        assert eff == {"number": 1, "soft": True}

    def test_cross_claim_staple_does_not_count(self, tmp_path: Path) -> None:
        """A genuine, verifying bundle lifted from ANOTHER claim authenticates
        nothing here: the envelope binds a different claim_id, so its distinct
        model belongs to that claim, not this one. The stapled line reads soft
        and the same-model pair stays collapsed at one."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        prop, pred = _prop(), _pred()
        cid = prop.content_id()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_CLAUDE),
            )
            # The donor: a genuine gpt-4o finding on an unrelated proposition,
            # signed by the same enrolled key, so only the claim binding differs.
            g.assert_finding(
                _donor_prop(), pred, _est(), data_id="ds3", generated_by="run3",
                grounding=_verdict(_GPT),
            )
            _staple_bundle(g._conn, "ds2", "ds3")
            eff = effective_independence(g._conn, cid)
        assert eff == {"number": 1, "soft": True}


def _same_model_pair(tmp_path: Path) -> str:
    """Two same-model findings, distinct ENROLLED signers, distinct datasets.

    The collapsed baseline the erasure tests attack: counts (1, 0), effective 1.
    Returns the proposition's content id.
    """
    ka = _bootstrap_key(tmp_path, "ka.key")
    kb = _bootstrap_key(tmp_path, "kb.key")
    _enroll_key(tmp_path, ka, kb)
    prop, pred = _prop(), _pred()
    for key, data_id, run in ((ka, "ds1", "run1"), (kb, "ds2", "run2")):
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_finding(
                prop, pred, _est(), data_id=data_id, generated_by=run,
                grounding=_verdict(_CLAUDE),
            )
    return prop.content_id()


class TestErasedColumn:
    """Erasing the unsigned column must not read as "no model call" either.

    The forge direction (column rewritten to a distinct root) is covered above.
    The absence direction is the same bypass mirrored: a NULL column would key
    the line ``("absent",)``, which the signer axis counts per signer and, under
    an enrolled human validator, re-keys to the human axis. The signed lineage
    the claim still carries is the authority in both directions.
    """

    def test_stripping_the_column_from_claims_toml_does_not_inflate(
        self, tmp_path: Path,
    ) -> None:
        """Deleting the ``model_lineage`` lines from claims.toml and restoring
        must not promote a same-model pair to two independent lines. The
        restored bundles still bind the signed lineage, so the collapse holds."""
        cid = _same_model_pair(tmp_path)
        toml = tmp_path / "claims.toml"
        kept = [
            line for line in toml.read_text().splitlines()
            if not line.startswith("model_lineage")
        ]
        toml.write_text("\n".join(kept) + "\n")
        _wipe_db(tmp_path)
        mareforma.restore(tmp_path)

        with mareforma.open(tmp_path, key_path=tmp_path / "ka.key") as g:
            assert independence_counts(g._conn, cid) == (1, 0)
            assert effective_independence(g._conn, cid)["number"] == 1

    def test_nulling_the_column_does_not_inflate(self, tmp_path: Path) -> None:
        """The direct-SQL form of the same erasure: wiping the denormalized
        column leaves the signed lineage as the authority, so the same-model
        pair stays collapsed."""
        cid = _same_model_pair(tmp_path)
        with mareforma.open(tmp_path, key_path=tmp_path / "ka.key") as g:
            _bypass_write_guard(g._conn)
            g._conn.execute("UPDATE evidence_lines SET model_lineage = NULL")
            g._conn.commit()
            assert independence_counts(g._conn, cid) == (1, 0)
            assert effective_independence(g._conn, cid)["number"] == 1


class TestV1DowngradeGuard:
    def test_v1_finding_column_is_soft_not_a_counted_model(
        self, tmp_path: Path,
    ) -> None:
        """A finding whose SIGNED record carries no model lineage (a legacy v1
        mint) cannot forge a counted distinct model by setting the unsigned
        column: the line reads soft, so two such forged columns do not inflate
        independence to 2. Downgrading to the v1 path gains the adversary
        nothing."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        cid = prop.content_id()
        # Both findings are minted with a grounding verdict that carries NO model
        # lineage (model_lineage=None), so their signed record is v1-shaped.
        from mareforma.observe import GroundingVerdict, ObservedGrounding

        v1 = GroundingVerdict(grounding=ObservedGrounding.OPAQUE, reason="v1")
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=v1,
            )
            # Forge this line's unsigned column to a distinct COMPUTED model.
            _tamper_one_line(g._conn, _FORGED_COMPUTED)
        forged_claude = _FORGED_COMPUTED.replace("gpt-4o-2024-08-06", _CLAUDE)
        forged_claude = forged_claude.replace('"gpt-4o"', '"claude-3-5-sonnet"')
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=v1,
            )
            line_id = g._conn.execute(
                "SELECT el.line_id FROM evidence_lines el "
                "JOIN findings f ON f.finding_id = el.finding_id "
                "JOIN evidence_lines el2 ON el2.finding_id = f.finding_id "
                "WHERE el.data_id = 'ds2' LIMIT 1"
            ).fetchone()[0]
            _bypass_write_guard(g._conn)
            g._conn.execute(
                "UPDATE evidence_lines SET model_lineage = ? WHERE line_id = ?",
                (forged_claude, line_id),
            )
            g._conn.commit()
            eff = effective_independence(g._conn, cid)
        # Neither forged column authenticates against a signed lineage, so both
        # lines are soft: the count rests at the single-line floor, never 2.
        assert eff["number"] == 1
        assert eff["soft"] is True


class TestNoUnauthenticatedKeyHelper:
    def test_raw_column_model_key_helper_is_gone(self) -> None:
        """No helper may key independence off the raw column. One that does is
        the forge this module closes, and it is the shorter call a future
        caller reaches for first."""
        from mareforma.trust import _store

        assert not hasattr(_store, "_line_model_key")
