"""Every read-path check must derive from the SIGNED record, not its twin column.

The read path re-derives a line's plan, the proposition it attests, and its
signer before counting it. Each of those values exists twice in the database:
once inside the claim's signed envelope, and once in an unsigned column beside
it (``findings.plan_id`` next to ``claims.predicate_payload``, the
``propositions`` text columns next to ``claims.text``, ``asserter_keyid`` next
to the bundle). A check that compares one mutable column against another
catches only a writer who rewrites one of the pair. The threat model here is a
process with raw SQL access, and that process rewrites both.

Each class below carries one such forgery, performed by direct SQL with the
durable write guards dropped first, because the guards are a speed bump a SQL
writer removes and the read-path re-derivation is the guarantee. Every attack is
paired with a benign control on the same surface, so a check that passed by
refusing everything would fail its own control.

The assertions are on the user-visible verdict (``proposition_status``): the
status, the two counts, and ``lines_skipped``. A drop that is not disclosed is
the one outcome the trust model cannot tolerate, so ``lines_skipped`` is part of
every verdict tuple rather than an afterthought.

| Attack | Caught by |
|---|---|
| Plant a bundle nobody enrolled the signer of | the predicate is verified, not parsed |
| Rewrite ``findings.plan_id`` and ``predicate_payload`` together | the signed ``finding_record.plan_id`` |
| Corrupt one evidence row, edit its siblings | the per-row digest rebuild |
| Re-point ``findings.content_id``, or rewrite the ``propositions`` row | identity, on both ends of the edge |
| Null ``asserter_keyid``, ``signature_bundle`` AND ``statement_cid`` | the project's own signing posture |
| INSERT an unsigned claim tree into a signed project | the same posture |
| INSERT a ``validators`` row | ``is_enrolled`` walks the chain |

Three classes record something other than a defence, and each says so in its own
docstring: :class:`TestScopeKeyCaseVariantStillCounts` and
:class:`TestScopeValuesThatJsonDoesNotRoundTrip` pin honest graphs an earlier
binding dropped with no adversary present, and
:meth:`TestStrippedSignatureCannotReachTheGrandfather.test_an_unsigned_project_is_the_named_residual`
pins a boundary as NOT caught, so it is visible rather than assumed.
"""
from __future__ import annotations

import base64
import json
import sqlite3
import uuid
from pathlib import Path

import pytest

import mareforma
from mareforma.trust import (
    Contrast,
    ControlType,
    DirectionOfInterest,
    EffectEstimate,
    EffectType,
    EvidenceLine,
    Prediction,
    Proposition,
    TestType,
)
from mareforma.trust._gate import (
    GateCache,
    GateInputRefused,
    verify_gate_inputs_or_refuse,
)

from tests._helpers import (
    _bootstrap_key,
    _enroll_key,
    _est,
    _pem_of,
    _pred,
    _prop,
    _verdict,
    _wipe_db,
)

_CLAUDE = "claude-3-5-sonnet-20241022"
_GPT = "gpt-4o-2024-08-06"

# The mirror image of ``_est()`` (-0.8): significant on the opposite side, so it
# REFUTES the same DECREASES proposition under the same plan. Every attack here
# is a way of making this line stop refuting.
_REFUTING = EffectEstimate(0.8, EffectType.SMD, p_value=0.001)

# Every append-only / no-delete guard standing between a direct writer and the
# rows these attacks touch. The threat model's writer drops them first, so each
# attack drops them too and proves the re-derivation, not the trigger, is what
# catches the forgery.
_WRITE_GUARDS = (
    "findings_append_only", "findings_no_delete",
    "evidence_lines_append_only", "evidence_lines_no_delete",
    "effect_estimates_append_only", "effect_estimates_no_delete",
    "contrasts_append_only", "contrasts_no_delete",
    "propositions_append_only", "propositions_no_delete",
    "predictions_append_only", "predictions_no_delete",
    "validators_append_only", "validators_no_delete",
    "claims_signed_fields_no_laundering",
)


def _adversary(tmp_path: Path) -> sqlite3.Connection:
    """A raw connection to graph.db with foreign keys off, guards dropped.

    The threat model's attacker: a co-resident process opening the file
    directly. Foreign keys default to OFF, so nothing the attacker never turned
    on can be credited with catching the write.
    """
    conn = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
    conn.execute("PRAGMA foreign_keys = OFF")
    for name in _WRITE_GUARDS:
        conn.execute(f"DROP TRIGGER IF EXISTS {name}")
    return conn


def _plant_bundle(
    conn: sqlite3.Connection,
    claim_id: str,
    key_path: Path,
    *,
    text: "str | None" = None,
    finding_record: "dict | None" = None,
) -> None:
    """Write a fabricated signature bundle onto *claim_id*.

    The envelope is well formed and its signature is genuine: it is produced by
    :func:`mareforma.signing.sign_claim` over the claim's own row, so it parses,
    its subject matches its predicate, and it binds this claim id. What it is
    not is a signature the project can authenticate, because *key_path* is a key
    nobody enrolled. Anything read out of it is an attacker-chosen string.

    ``text`` and ``finding_record`` are the forgeries the caller wants inside the
    signed half of the envelope, next to the unsigned columns the read path used
    to compare them against.
    """
    from mareforma import signing as _signing

    row = conn.execute(
        "SELECT text, classification, generated_by, supports_json, "
        " contradicts_json, source_name, artifact_hash, created_at, "
        " evidence_json FROM claims WHERE claim_id = ?",
        (claim_id,),
    ).fetchone()
    fields = {
        "claim_id": claim_id,
        "text": row[0] if text is None else text,
        "classification": row[1],
        "generated_by": row[2],
        "supports": json.loads(row[3]),
        "contradicts": json.loads(row[4]),
        "source_name": row[5],
        "artifact_hash": row[6],
        "created_at": row[7],
    }
    if finding_record is not None:
        fields["finding_record"] = finding_record
    envelope = _signing.sign_claim(
        fields, _signing.load_private_key(key_path),
        evidence=json.loads(row[8] or "{}"),
    )
    conn.execute(
        "UPDATE claims SET signature_bundle = ? WHERE claim_id = ?",
        (json.dumps(envelope, sort_keys=True, separators=(",", ":")), claim_id),
    )


def _health_ops(tmp_path: Path) -> list[str]:
    """The op names recorded on the health channel so far."""
    log = tmp_path / ".mareforma" / "health.jsonl"
    if not log.exists():
        return []
    return [
        json.loads(line)["op"]
        for line in log.read_text().splitlines()
        if line.strip()
    ]


def _status_tuple(view: dict) -> tuple:
    """(status, support, refute, lines_skipped) from a proposition view.

    ``lines_skipped`` rides along with the counts because a count is only
    readable next to what was dropped to produce it: a silently dropped
    refutation and a proposition nobody contested report the same two counts.
    """
    return (
        view["status"],
        view["independent_support"],
        view["independent_refute"],
        view["lines_skipped"],
    )


def _contested_graph(tmp_path: Path) -> dict:
    """One proposition, one SUPPORTING and one REFUTING line, distinct signers.

    Reads CONTESTED 1/1 with nothing skipped, so any silent flip or drop of
    either line moves a number a reader can see. Both signers are enrolled and
    each ran a different model, so both lines count on the distinct-model axis.
    """
    ka = _bootstrap_key(tmp_path, "ka.key")
    kb = _bootstrap_key(tmp_path, "kb.key")
    _enroll_key(tmp_path, ka, kb)
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=ka) as g:
        sup = g.assert_finding(
            prop, pred, _est(), data_id="ds_sup", generated_by="run_sup",
            grounding=_verdict(_CLAUDE),
        )
    with mareforma.open(tmp_path, key_path=kb) as g:
        ref = g.assert_finding(
            prop, pred, _REFUTING, data_id="ds_ref", generated_by="run_ref",
            grounding=_verdict(_GPT),
        )
    return {
        "root": ka, "second": kb, "prop": prop, "cid": prop.content_id(),
        "sup": sup, "ref": ref,
    }


# ---------------------------------------------------------------------------
# "Signed" means verified. A parsed envelope is an attacker-chosen string
# ---------------------------------------------------------------------------


def _unsigned_contested_graph(tmp_path: Path) -> dict:
    """One proposition, one SUPPORTING and one REFUTING line, nothing signed.

    A project opened with no key writes claims with a NULL ``asserter_keyid``,
    no bundle and no ``statement_cid``: the genuinely unsigned graph the run-axis
    grandfather exists for. It reads CONTESTED 1/1 with nothing skipped, and it
    is the setting that isolates this check, because no signer axis, no
    ``statement_cid`` and no enrolment can be credited with catching the forgery
    below. The only thing standing between the attacker and the verdict is
    whether the read path verifies the envelope it reads or merely parses it.
    """
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path) as g:
        sup = g.assert_finding(
            prop, pred, _est(), data_id="ds_sup", generated_by="run_sup",
        )
        ref = g.assert_finding(
            prop, pred, _REFUTING, data_id="ds_ref", generated_by="run_ref",
        )
    return {"prop": prop, "cid": prop.content_id(), "sup": sup, "ref": ref}


class TestAPlantedBundleIsNotSignedMaterial:
    """Both re-derivations above read a value out of the claim's envelope, and
    an envelope is only worth reading once its signature has been checked
    against an enrolled validator's public key. Parsing one and trusting the
    result hands the attacker the very column the check was moved away from:
    they write the envelope themselves. Measured on an untampered unsigned
    graph, planting one envelope turned a refutation into a second supporting
    line and read CONVERGENT (2, 0, 0) with nothing skipped and restore
    accepting.

    A key nobody enrolled produces a REAL signature here, not a broken one, so
    the check that catches it has to be enrolment plus verification. Neither
    well-formedness nor a valid signature is standing.

    What this pins is that a forged envelope buys the attacker NOTHING, not that
    the graph is safe. On an unsigned graph it is not: the same verdict is
    reachable with no envelope at all, because there is no signed material to
    re-derive anything from. That boundary is pinned in
    :meth:`TestStrippedSignatureCannotReachTheGrandfather.test_an_unsigned_project_is_the_named_residual`.
    The setting is used here because it is the only one where nothing ELSE can be
    credited with catching the forgery.
    """

    def test_a_planted_envelope_cannot_supply_the_plan_a_line_is_gated_under(
        self, tmp_path: Path,
    ) -> None:
        state = _unsigned_contested_graph(tmp_path)
        cid, ref_claim = state["cid"], state["ref"]["claim_id"]
        # The same second registered plan the signed-record attack uses: the
        # +0.8 estimate that refutes under the filed plan supports under this
        # one, and its rule genuinely hashes to its own plan_id.
        flip = Prediction(
            TestType.SUPERIORITY,
            direction_of_interest=DirectionOfInterest.INCREASE, alpha=0.05,
        )
        with mareforma.open(tmp_path) as g:
            flip_plan = g.register_plan(state["prop"], flip)
            before = _status_tuple(g.proposition_status(cid))
        assert before == ("CONTESTED", 1, 1, 0)

        conn = _adversary(tmp_path)
        conn.execute(
            "UPDATE findings SET plan_id = ? WHERE claim_id = ?",
            (flip_plan, ref_claim),
        )
        _plant_bundle(
            conn, ref_claim, _bootstrap_key(tmp_path, "planted.key"),
            finding_record={"plan_id": flip_plan},
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path) as g:
            after = _status_tuple(g.proposition_status(cid))
            # The planted record vouches for nothing: the plan falls back to the
            # claim's own unsigned payload, which still names the filed plan, so
            # the repointed line is dropped and disclosed.
            assert after == ("PRELIMINARY", 1, 0, 1)
            with pytest.raises(GateInputRefused, match="plan_id column"):
                verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())
        assert "plan_rebind_skipped" in _health_ops(tmp_path)

    def test_a_planted_envelope_cannot_supply_the_proposition_a_finding_attests(
        self, tmp_path: Path,
    ) -> None:
        """The same lever against the other re-derivation. Moving
        ``findings.content_id`` onto another proposition is caught because the
        finding's own record names the one it was filed against; planting an
        envelope whose ``finding_record.content_id`` names the destination makes
        the move look like the finding's own word."""
        state = _unsigned_contested_graph(tmp_path)
        other = Proposition.from_dict(
            {**state["prop"].to_dict(), "subject": "TP53"},
        )
        other_cid = other.content_id()
        with mareforma.open(tmp_path) as g:
            g.register_plan(other, _pred())
            assert _status_tuple(g.proposition_status(other_cid)) == (
                "UNTESTED", 0, 0, 0
            )

        conn = _adversary(tmp_path)
        planted = _bootstrap_key(tmp_path, "planted.key")
        for rec in (state["sup"], state["ref"]):
            conn.execute(
                "UPDATE findings SET content_id = ? WHERE finding_id = ?",
                (other_cid, rec["finding_id"]),
            )
            _plant_bundle(
                conn, rec["claim_id"], planted,
                finding_record={"content_id": other_cid},
            )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path) as g:
            after = _status_tuple(g.proposition_status(other_cid))
            # The planted record names nothing: the binding falls back to each
            # claim's own text, which renders the proposition the finding was
            # actually filed against, so both lines drop and are disclosed.
            assert after == ("UNTESTED", 0, 0, 2)
            with pytest.raises(GateInputRefused, match="does not attest it"):
                verify_gate_inputs_or_refuse(
                    g._conn, other_cid, cache=GateCache(),
                )
        assert "proposition_rebind_skipped" in _health_ops(tmp_path)

    def test_an_untouched_unsigned_graph_still_counts(
        self, tmp_path: Path,
    ) -> None:
        """The benign control. A graph with no key, no validators and no
        envelopes anywhere is exactly what the grandfather exists for: both
        lines count on the run axis, nothing is skipped, and the recovery is not
        refused. A check that refused every claim it could not verify would take
        this whole graph out."""
        state = _unsigned_contested_graph(tmp_path)
        with mareforma.open(tmp_path) as g:
            after = _status_tuple(g.proposition_status(state["cid"]))
            verify_gate_inputs_or_refuse(g._conn, state["cid"], cache=GateCache())
        assert after == ("CONTESTED", 1, 1, 0)
        assert _health_ops(tmp_path) == [
            op for op in _health_ops(tmp_path) if not op.endswith("_skipped")
        ]


# ---------------------------------------------------------------------------
# The plan a line is gated under comes from the signed finding record
# ---------------------------------------------------------------------------


class TestPlanRepointUnderTheSignedRecord:
    """``findings.plan_id`` decides which rule reads a line's estimate, and the
    claim's ``predicate_payload`` is the unsigned copy sitting beside it.
    Rewriting only the column was already caught. Rewriting BOTH re-pointed a
    refuting line at a rule that reads the same estimate as support, and the
    proposition read CONVERGENT with nothing disclosed. The plan now comes from
    the signed ``finding_record``, which no UPDATE reaches."""

    def test_repointing_the_column_and_the_unsigned_payload_together_is_caught(
        self, tmp_path: Path,
    ) -> None:
        state = _contested_graph(tmp_path)
        cid, ref_claim = state["cid"], state["ref"]["claim_id"]
        # A second, genuinely registered plan on the same proposition whose rule
        # predicts the other direction: the +0.8 estimate that refutes under the
        # filed plan SUPPORTS under this one. Registered through the API, so the
        # rule still hashes to the plan_id keying it and the plan-rule binding
        # cannot be the thing that catches the repoint.
        flip = Prediction(
            TestType.SUPERIORITY,
            direction_of_interest=DirectionOfInterest.INCREASE, alpha=0.05,
        )
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            flip_plan = g.register_plan(state["prop"], flip)
            before = _status_tuple(g.proposition_status(cid))
        assert before == ("CONTESTED", 1, 1, 0)

        conn = _adversary(tmp_path)
        payload = json.loads(
            conn.execute(
                "SELECT predicate_payload FROM claims WHERE claim_id = ?",
                (ref_claim,),
            ).fetchone()[0]
        )
        payload["plan_id"] = flip_plan
        conn.execute(
            "UPDATE findings SET plan_id = ? WHERE claim_id = ?",
            (flip_plan, ref_claim),
        )
        conn.execute(
            "UPDATE claims SET predicate_payload = ? WHERE claim_id = ?",
            (json.dumps(payload), ref_claim),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(cid))
            # The refutation does not reappear as support: the repointed line
            # counts on no axis and the drop is disclosed.
            assert after == ("PRELIMINARY", 1, 0, 1)
            # Caught on the read path and on restore, by the same verifier.
            with pytest.raises(GateInputRefused, match="plan_id column"):
                verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())
        assert "plan_rebind_skipped" in _health_ops(tmp_path)

    def test_two_findings_filed_under_two_real_plans_both_still_count(
        self, tmp_path: Path,
    ) -> None:
        """The benign control: two findings on one proposition, each filed under
        its own genuinely registered plan. Their ``plan_id`` columns legitimately
        differ, and each matches the plan its own claim signed, so both count. A
        check that refused any line whose plan_id differs from its neighbour's
        would fail here."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        prop = _prop()
        tighter = Prediction(
            TestType.SUPERIORITY,
            direction_of_interest=DirectionOfInterest.DECREASE, alpha=0.01,
        )
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, _pred(), _est(), data_id="ds_sup",
                generated_by="run_sup", grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.register_plan(prop, tighter)
            g.submit_finding(
                prop, tighter, _REFUTING, data_id="ds_ref",
                generated_by="run_ref", grounding=_verdict(_GPT),
            )
            after = _status_tuple(g.proposition_status(prop.content_id()))
        assert after == ("CONTESTED", 1, 1, 0)


# ---------------------------------------------------------------------------
# One unrebuildable evidence row does not exempt its siblings
# ---------------------------------------------------------------------------


class TestSiblingRowsAreNotExemptedByABrokenRow:
    """The finding's signed digest covers its whole line set, and it used to be
    compared against one digest recomputed over every live row at once. A row a
    writer corrupted past rebuilding made that recompute raise, which read as
    "no live digest" and abandoned the comparison for the WHOLE finding: on a
    three-line refutation, corrupting one line bought a free edit of the other
    two, the refutation vanished, and the only disclosure named the corrupted
    line. Each row is now digested on its own, so the reader is told both which
    row broke and that the finding no longer matches what was signed."""

    def _three_line_refutation(self, tmp_path: Path) -> dict:
        """A supporting single-line finding plus a three-line refuting one.

        Reads CONTESTED 1/1 with nothing skipped. The refuting finding is
        multi-line precisely so one of its rows can be broken while the others
        are edited.
        """
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds_sup", generated_by="run_sup",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            ref = g.assert_finding(
                prop, pred,
                lines=[
                    EvidenceLine(
                        estimate=_REFUTING, data_id=f"ds_ref{i}",
                        contrast=Contrast(ControlType.NEGATIVE),
                    )
                    for i in (1, 2, 3)
                ],
                generated_by="run_ref", grounding=_verdict(_GPT),
            )
        return {"root": ka, "cid": prop.content_id(), "ref": ref}

    def _estimate_ids(
        self, conn: sqlite3.Connection, finding_id: str,
    ) -> list[str]:
        """The finding's estimate ids, ordered by data_id so the pick is stable."""
        return [
            r[0]
            for r in conn.execute(
                "SELECT est.estimate_id FROM evidence_lines el "
                "JOIN contrasts c ON c.line_id = el.line_id "
                "JOIN effect_estimates est ON est.contrast_id = c.contrast_id "
                "WHERE el.finding_id = ? ORDER BY el.data_id",
                (finding_id,),
            ).fetchall()
        ]

    def test_a_broken_row_no_longer_buys_a_free_edit_of_its_siblings(
        self, tmp_path: Path,
    ) -> None:
        state = self._three_line_refutation(tmp_path)
        cid = state["cid"]
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            assert _status_tuple(g.proposition_status(cid)) == (
                "CONTESTED", 1, 1, 0
            )

        conn = _adversary(tmp_path)
        estimates = self._estimate_ids(conn, state["ref"]["finding_id"])
        # Row one is corrupted past rebuilding: REAL affinity keeps a
        # non-numeric string as text, and math.isfinite raises on it, so both
        # the bearing and the digest recompute fail for this row.
        conn.execute(
            "UPDATE effect_estimates SET estimate_value = 'not-a-number' "
            "WHERE estimate_id = ?",
            (estimates[0],),
        )
        # Rows two and three are flipped from refuting to supporting. These are
        # the rows the broken sibling used to shield.
        for estimate_id in estimates[1:]:
            conn.execute(
                "UPDATE effect_estimates SET estimate_value = -0.8 "
                "WHERE estimate_id = ?",
                (estimate_id,),
            )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(cid))
            # The edited rows do not manufacture consensus, and all three lines
            # of the finding are accounted for rather than one.
            assert after == ("PRELIMINARY", 1, 0, 3)
            with pytest.raises(GateInputRefused, match="digest"):
                verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())
        ops = _health_ops(tmp_path)
        # Both halves of the disclosure: which row broke, and that the finding
        # as a whole no longer matches its signed digest.
        assert "bearing_recompute_skipped" in ops
        assert "estimates_digest_skipped" in ops

    def test_an_untouched_multi_line_finding_still_counts(
        self, tmp_path: Path,
    ) -> None:
        """The benign control: the same three-line refutation, untampered. The
        per-row digest pass must not invent a mismatch on an honest finding, and
        restore must not refuse it."""
        state = self._three_line_refutation(tmp_path)
        cid = state["cid"]
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(cid))
            verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())
        assert after == ("CONTESTED", 1, 1, 0)
        assert "estimates_digest_skipped" not in _health_ops(tmp_path)


# ---------------------------------------------------------------------------
# The proposition binding compares the SIGNED claim text
# ---------------------------------------------------------------------------


class TestARewrittenPropositionRowIsCaught:
    """Rewriting the ``propositions`` row re-points every finding filed against
    that ``content_id`` at a sentence none of them made. It used to be caught by
    comparing the row's rendering against ``claims.text``, another unsigned
    column, so rewriting both together left the verdict intact while the evidence
    stood under a different sentence. It is caught by identity now: the row has
    to hash to the content_id it is stored under, and no rewrite of a column
    beside it changes that."""

    def _one_supporting_finding(self, tmp_path: Path) -> dict:
        ka = _bootstrap_key(tmp_path, "ka.key")
        prop = _prop()
        with mareforma.open(tmp_path, key_path=ka) as g:
            rec = g.assert_finding(
                prop, _pred(), _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        return {"root": ka, "prop": prop, "cid": prop.content_id(), "rec": rec}

    def test_rewriting_the_proposition_row_and_the_claim_text_together_is_caught(
        self, tmp_path: Path,
    ) -> None:
        state = self._one_supporting_finding(tmp_path)
        cid = state["cid"]
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            assert _status_tuple(g.proposition_status(cid)) == (
                "PRELIMINARY", 1, 0, 0
            )
        # The sentence the evidence would stand under after the rewrite, and the
        # claim text an attacker writes so the old comparison still balances.
        forged = Proposition.from_dict({**state["prop"].to_dict(), "subject": "TP53"})

        conn = _adversary(tmp_path)
        conn.execute(
            "UPDATE propositions SET subject = 'TP53' WHERE content_id = ?",
            (cid,),
        )
        conn.execute(
            "UPDATE claims SET text = ? WHERE claim_id = ?",
            (forged.text(), state["rec"]["claim_id"]),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(cid))
            # The evidence does not stay attached to a sentence its claim never
            # made: the whole finding drops and the drop is disclosed.
            assert after == ("UNTESTED", 0, 0, 1)
            with pytest.raises(GateInputRefused, match="does not attest it"):
                verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())
        assert "proposition_rebind_skipped" in _health_ops(tmp_path)

    def test_a_case_variant_of_the_proposition_row_still_counts(
        self, tmp_path: Path,
    ) -> None:
        """The benign control on the same surface. Identity is content-addressed
        through casefold, so a proposition row whose subject differs from the
        claim text only in capitalisation is the same proposition, not a
        rewritten edge. Both sides are normalised, so the line keeps counting
        and the recovery is not refused."""
        state = self._one_supporting_finding(tmp_path)
        cid = state["cid"]
        conn = _adversary(tmp_path)
        conn.execute(
            "UPDATE propositions SET subject = 'brca1' WHERE content_id = ?",
            (cid,),
        )
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(cid))
            verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())
        assert after == ("PRELIMINARY", 1, 0, 0)
        assert "proposition_rebind_skipped" not in _health_ops(tmp_path)


# ---------------------------------------------------------------------------
# Stripping a signature does not reach the legacy grandfather
# ---------------------------------------------------------------------------


class TestStrippedSignatureCannotReachTheGrandfather:
    """A claim with a NULL ``asserter_keyid`` is grandfathered onto the retired
    run axis, because a graph written before claim signing, or in unsigned mode,
    must keep reading. Two UPDATEs put any signed claim into that state, and
    once there every check that reads signed material had nothing to read and
    exempted the line. ``statement_cid`` is written at signing time and survives
    both UPDATEs, so it now separates "unsigned from birth" from "de-signed just
    now"."""

    def test_nulling_the_keyid_and_bundle_no_longer_exempts_the_claim(
        self, tmp_path: Path,
    ) -> None:
        state = _contested_graph(tmp_path)
        cid, ref = state["cid"], state["ref"]
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            assert _status_tuple(g.proposition_status(cid)) == (
                "CONTESTED", 1, 1, 0
            )

        conn = _adversary(tmp_path)
        # The whole attack in one statement: the claim is now indistinguishable
        # from a legacy one by keyid and bundle alone.
        conn.execute(
            "UPDATE claims SET asserter_keyid = NULL, signature_bundle = NULL "
            "WHERE claim_id = ?",
            (ref["claim_id"],),
        )
        # What that exemption then buys, and why the line must not count at all:
        # with no signed digest to check the estimate against, the refutation is
        # rewritten into support, and with no signed lineage to key on, erasing
        # the model column moves the line onto the signer axis where it counts
        # as a second independent source.
        conn.execute(
            "UPDATE effect_estimates SET estimate_value = -0.8 "
            "WHERE contrast_id IN ("
            "  SELECT c.contrast_id FROM contrasts c "
            "  JOIN evidence_lines el ON el.line_id = c.line_id "
            "  WHERE el.finding_id = ?)",
            (ref["finding_id"],),
        )
        conn.execute(
            "UPDATE evidence_lines SET model_lineage = NULL WHERE finding_id = ?",
            (ref["finding_id"],),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(cid))
        # Not a clean second supporting source: the de-signed line counts on no
        # axis, and its removal is disclosed rather than read as consensus.
        assert after == ("PRELIMINARY", 1, 0, 1)
        assert "unregistered_signer_skipped" in _health_ops(tmp_path)

    def test_nulling_the_statement_cid_as_well_does_not_reach_it_either(
        self, tmp_path: Path,
    ) -> None:
        """``statement_cid`` is a plain nullable column, and the writer in this
        threat model is already issuing an UPDATE against the row. Appending one
        more assignment to the same statement restored the bypass in full:
        CONVERGENT (2, 0, 0), nothing skipped, restore accepting. A column cannot
        be the whole of the defence, so the grandfather also asks the PROJECT
        whether it signs at all: a project with an enrolled validator does not
        hand the run axis to a claim carrying no signature, and no UPDATE against
        ``claims`` reaches the ``validators`` table."""
        state = _contested_graph(tmp_path)
        cid, ref = state["cid"], state["ref"]
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            assert _status_tuple(g.proposition_status(cid)) == (
                "CONTESTED", 1, 1, 0
            )

        conn = _adversary(tmp_path)
        conn.execute(
            "UPDATE claims SET asserter_keyid = NULL, signature_bundle = NULL, "
            "statement_cid = NULL WHERE claim_id = ?",
            (ref["claim_id"],),
        )
        # Same payoff as the two-column version: with nothing signed left to
        # check the estimate or the model against, the refutation is rewritten
        # into support and the line moves onto the run axis as a second source.
        conn.execute(
            "UPDATE effect_estimates SET estimate_value = -0.8 "
            "WHERE contrast_id IN ("
            "  SELECT c.contrast_id FROM contrasts c "
            "  JOIN evidence_lines el ON el.line_id = c.line_id "
            "  WHERE el.finding_id = ?)",
            (ref["finding_id"],),
        )
        conn.execute(
            "UPDATE evidence_lines SET model_lineage = NULL WHERE finding_id = ?",
            (ref["finding_id"],),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(cid))
        assert after == ("PRELIMINARY", 1, 0, 1)
        assert "unregistered_signer_skipped" in _health_ops(tmp_path)

    def test_an_inserted_unsigned_finding_does_not_count_in_a_signed_project(
        self, tmp_path: Path,
    ) -> None:
        """The grandfather used to be a property of the CLAIM, so a writer did
        not have to de-sign anything: it could INSERT a whole new claim, finding
        and evidence tree carrying no signature at all, into a fully signed
        project, and every check fell back to the columns it had just written.
        Measured: one INSERT sequence took a proposition from PRELIMINARY to
        CONVERGENT (2, 0, 0) with nothing disclosed. Asking the project rather
        than the row closes it."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            sup = g.assert_finding(
                prop, pred, _est(), data_id="ds_sup", generated_by="run_sup",
                grounding=_verdict(_CLAUDE),
            )
            cid = prop.content_id()
            assert _status_tuple(g.proposition_status(cid)) == (
                "PRELIMINARY", 1, 0, 0
            )

        conn = _adversary(tmp_path)
        conn.row_factory = sqlite3.Row
        claim = conn.execute(
            "SELECT * FROM claims WHERE claim_id = ?", (sup["claim_id"],),
        ).fetchone()
        finding = conn.execute(
            "SELECT * FROM findings WHERE claim_id = ?", (sup["claim_id"],),
        ).fetchone()
        payload = json.loads(claim["predicate_payload"])
        payload["data_id"] = "ds_forged"
        payload["data_ids"] = ["ds_forged"]
        new_claim = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO claims (claim_id, text, classification, support_level, "
            " status, generated_by, supports_json, contradicts_json, branch_id, "
            " signature_bundle, asserter_keyid, statement_cid, evidence_json, "
            " predicate_payload, created_at, updated_at, transparency_logged) "
            "VALUES (?, ?, ?, 'PRELIMINARY', 'open', 'run_forged', '[]', '[]', "
            " 'main', NULL, NULL, NULL, '{}', ?, ?, ?, 1)",
            (
                new_claim, claim["text"], claim["classification"],
                json.dumps(payload), claim["created_at"], claim["created_at"],
            ),
        )
        new_finding = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO findings (finding_id, content_id, plan_id, claim_id, "
            " bearing_direction, created_at) VALUES (?, ?, ?, ?, 'supports', ?)",
            (
                new_finding, cid, finding["plan_id"], new_claim,
                finding["created_at"],
            ),
        )
        new_line = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO evidence_lines (line_id, finding_id, data_id, "
            " model_lineage, created_at) VALUES (?, ?, 'ds_forged', NULL, ?)",
            (new_line, new_finding, finding["created_at"]),
        )
        new_contrast = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO contrasts (contrast_id, line_id, control_type) "
            "VALUES (?, ?, 'negative')",
            (new_contrast, new_line),
        )
        conn.execute(
            "INSERT INTO effect_estimates (estimate_id, contrast_id, "
            " estimate_value, effect_type, scale, p_value) "
            "VALUES (?, ?, -0.8, 'SMD', 'raw', 0.001)",
            (str(uuid.uuid4()), new_contrast),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=ka) as g:
            after = _status_tuple(g.proposition_status(cid))
        # The manufactured line counts on no axis, and the drop is disclosed.
        assert after == ("PRELIMINARY", 1, 0, 1)
        assert "unregistered_signer_skipped" in _health_ops(tmp_path)

    def test_a_graph_that_was_never_signed_still_counts(
        self, tmp_path: Path,
    ) -> None:
        """The benign control: a graph opened with no key at all writes findings
        with a NULL keyid, no bundle, and no ``statement_cid``, and its project
        enrols no validator. Those are the genuinely unsigned claims the
        grandfather exists for, and they must keep counting on the run axis. A
        guard keyed on the missing bundle, or one that refused every unsigned
        claim outright, would drop this whole graph."""
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
            )
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
            )
            after = _status_tuple(g.proposition_status(prop.content_id()))
        assert after == ("CONVERGENT", 2, 0, 0)

    def test_enrolling_a_root_costs_the_claims_that_predate_it(
        self, tmp_path: Path,
    ) -> None:
        """The price of the two fixes above, pinned so it is not a surprise.

        Asking the PROJECT rather than the row is what closes them, and it
        cannot distinguish a claim written honestly before the project had a key
        from one a writer manufactured afterwards. So a project that starts
        unsigned and later enrols loses every finding it wrote first, and there
        is no un-enrol.

        This is asserted, not worked around, because it is the current shipped
        behaviour and an operator hitting it deserves to find it written down
        rather than discover it. The repair is a retroactive signature over
        those claims; it was built and withdrawn before release after an
        adversary turned it into a laundering path, and it needs its own pass.

        Until then the release notes have to carry this, and so does this test.
        """
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
            )
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
            )
            assert _status_tuple(g.proposition_status(prop.content_id())) == (
                "CONVERGENT", 2, 0, 0
            )

        key = _bootstrap_key(tmp_path, "later.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            after = _status_tuple(g.proposition_status(prop.content_id()))
        # Both lines are dropped, and the drop IS disclosed: lines_skipped is 2,
        # not a silent zero. That is the one part of this that is not a loss.
        assert after == ("UNTESTED", 0, 0, 2)
        assert "unregistered_signer_skipped" in _health_ops(tmp_path)
















    def test_an_unsigned_project_is_the_named_residual(
        self, tmp_path: Path,
    ) -> None:
        """The boundary, pinned as NOT caught so it is visible rather than
        assumed.

        Every re-derivation on the read path compares a column against signed
        material. A project that enrols no validator has no signed material, so
        there is nothing to compare against and a writer with SQL access moves
        the verdict freely: rewriting ``findings.plan_id`` and the matching
        ``predicate_payload`` re-points a refutation at a supporting rule and
        reads CONVERGENT (2, 0, 0) with nothing skipped and restore accepting.

        No check in this module changes that, and none can: the guarantee here is
        the signature, and an unsigned project has none. This is what the
        grandfather costs, stated plainly. It is also why the planted-envelope
        class above pins that a forged envelope buys NOTHING rather than that the
        graph is safe: on this graph the attacker never needed one.
        """
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds_sup", generated_by="run_sup",
            )
            ref = g.assert_finding(
                prop, pred, _REFUTING, data_id="ds_ref",
                generated_by="run_ref",
            )
            flip_plan = g.register_plan(prop, Prediction(
                TestType.SUPERIORITY,
                direction_of_interest=DirectionOfInterest.INCREASE, alpha=0.05,
            ))
            cid = prop.content_id()
            assert _status_tuple(g.proposition_status(cid)) == (
                "CONTESTED", 1, 1, 0
            )

        conn = _adversary(tmp_path)
        payload = json.loads(
            conn.execute(
                "SELECT predicate_payload FROM claims WHERE claim_id = ?",
                (ref["claim_id"],),
            ).fetchone()[0]
        )
        payload["plan_id"] = flip_plan
        conn.execute(
            "UPDATE findings SET plan_id = ? WHERE claim_id = ?",
            (flip_plan, ref["claim_id"]),
        )
        conn.execute(
            "UPDATE claims SET predicate_payload = ? WHERE claim_id = ?",
            (json.dumps(payload), ref["claim_id"]),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path) as g:
            after = _status_tuple(g.proposition_status(cid))
            # Asserted as NOT caught. On a signed project the same two UPDATEs
            # are refused (see TestPlanRepointUnderTheSignedRecord); here there
            # is no signed record to refuse them with.
            assert after == ("CONVERGENT", 2, 0, 0)
            verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())
        assert "plan_rebind_skipped" not in _health_ops(tmp_path)


# ---------------------------------------------------------------------------
# A signer must be enrolled, not merely present in the validators table
# ---------------------------------------------------------------------------


class TestSignerMustBeEnrolledNotMerelyPresent:
    """The signer axis used to accept any keyid the ``validators`` table carried
    a row for. That table's trust property is the enrolment chain, not the row:
    SQLite enforces no foreign key on ``enrolled_by_keyid``, so one INSERT with
    a real public key and a junk envelope authenticated a signer that had never
    been enrolled, and took a proposition from PRELIMINARY to CONVERGENT without
    forging a signature. ``is_enrolled`` walks the chain back to the self-signed
    root, which is what registration means on every other surface here."""

    def _unenrolled_second_signer(self, tmp_path: Path) -> dict:
        """Two supporting findings under two models; only the first signer is
        enrolled. The second line therefore counts on no axis, and the honest
        read is PRELIMINARY 1 with one disclosed skip."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            second = g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_GPT),
            )
        return {
            "root": ka, "second": kb, "prop": prop, "cid": prop.content_id(),
            "second_claim": second["claim_id"],
        }

    def test_a_planted_validators_row_does_not_authenticate_its_signer(
        self, tmp_path: Path,
    ) -> None:
        state = self._unenrolled_second_signer(tmp_path)
        cid = state["cid"]
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            before = _status_tuple(g.proposition_status(cid))
        assert before == ("PRELIMINARY", 1, 0, 1)

        conn = _adversary(tmp_path)
        keyid = conn.execute(
            "SELECT asserter_keyid FROM claims WHERE claim_id = ?",
            (state["second_claim"],),
        ).fetchone()[0]
        root_keyid = conn.execute("SELECT keyid FROM validators").fetchone()[0]
        # The row carries the signer's REAL public key, so every signature on
        # its claims verifies; only the enrolment envelope is junk. Nothing but
        # the chain walk separates this from an enrolment.
        conn.execute(
            "INSERT INTO validators (keyid, pubkey_pem, identity, "
            " validator_type, enrolled_at, enrolled_by_keyid, "
            " enrollment_envelope) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                keyid,
                base64.standard_b64encode(_pem_of(state["second"])).decode("ascii"),
                "planted@lab.example",
                "human",
                "2026-01-01T00:00:00Z",
                root_keyid,
                "{}",
            ),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(cid))
        # The INSERT bought nothing: the count is what it was before it landed.
        assert after == before
        assert "unregistered_signer_skipped" in _health_ops(tmp_path)

    def test_a_properly_enrolled_second_signer_does_count(
        self, tmp_path: Path,
    ) -> None:
        """The benign control: the same two findings, with the second signer
        enrolled through the signed chain instead of planted. The line counts,
        nothing is skipped, and the proposition converges. A signer check that
        refused every keyid would fail here."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        prop, pred = _prop(), _pred()
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
            after = _status_tuple(g.proposition_status(prop.content_id()))
        assert after == ("CONVERGENT", 2, 0, 0)


# ---------------------------------------------------------------------------
# A retirement is resolved only when its own attestation says so
# ---------------------------------------------------------------------------


class TestAPlantedRetirementCannotRegateALine:
    """``plan_retirements`` decides which rule a stranded line is gated under,
    and none of its columns is signed. The read path resolved it straight off the
    raw row, on the reasoning that resolution is reached only from a plan that
    cannot be run, so it could only ever recover a line counting zero.

    A writer supplies the premise. Rewriting a live plan's alpha to a value no
    gate can discriminate at makes its rule un-runnable, which sends the line
    into resolution; a planted replacement at a stricter alpha then re-gates the
    refutation to NEUTRAL. NEUTRAL is COUNTED, not skipped, so the refutation
    disappears with ``lines_skipped`` still zero, nothing on the health channel,
    and restore accepting. Measured: CONTESTED (1, 1, 0) to PRELIMINARY (1, 0, 0)
    on a fully signed graph with every signature valid.

    ``retire_plan`` writes a signed attestation whose text renders the plan, the
    replacement and the reason, and restore has always re-derived the row from
    it. The read path re-derives it too now, so a retirement nobody attested
    resolves nothing.
    """

    def _refutation_under_its_own_plan(self, tmp_path: Path) -> dict:
        """One supporting line, and one refuting line under a separate plan.

        The refuting finding needs its own plan so the attack can make THAT
        plan un-runnable without touching the supporting line's.
        """
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        prop = _prop()
        tighter = Prediction(
            TestType.SUPERIORITY,
            direction_of_interest=DirectionOfInterest.DECREASE, alpha=0.01,
        )
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, _pred(), _est(), data_id="ds_sup",
                generated_by="run_sup", grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            ref_plan = g.register_plan(prop, tighter)
            g.submit_finding(
                prop, tighter, _REFUTING, data_id="ds_ref",
                generated_by="run_ref", grounding=_verdict(_GPT),
            )
        return {
            "root": ka, "prop": prop, "cid": prop.content_id(),
            "ref_plan": ref_plan,
        }

    def test_a_planted_retirement_does_not_regate_a_counted_line(
        self, tmp_path: Path,
    ) -> None:
        from mareforma.trust._store import compute_plan_id

        state = self._refutation_under_its_own_plan(tmp_path)
        cid = state["cid"]
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            assert _status_tuple(g.proposition_status(cid)) == (
                "CONTESTED", 1, 1, 0
            )

        # The replacement: the same rule at an alpha the refutation's p-value
        # misses, registered under the plan_id its own rule hashes to, so the
        # rule-to-plan_id binding re-derives cleanly.
        replacement = Prediction(
            TestType.SUPERIORITY,
            direction_of_interest=DirectionOfInterest.DECREASE, alpha=0.0005,
        )
        new_plan = compute_plan_id(cid, replacement)

        conn = _adversary(tmp_path)
        conn.execute(
            "UPDATE predictions SET alpha = 0.6 WHERE plan_id = ?",
            (state["ref_plan"],),
        )
        conn.execute(
            "INSERT INTO predictions (plan_id, content_id, inference_regime, "
            " test_type, direction_of_interest, equivalence_lower, "
            " equivalence_upper, alpha, preregistered, registered_at) "
            "VALUES (?, ?, 'frequentist', 'superiority', 'decrease', NULL, "
            " NULL, 0.0005, 0, '2026-01-01T00:00:00Z')",
            (new_plan, cid),
        )
        any_claim = conn.execute("SELECT claim_id FROM claims").fetchone()[0]
        conn.execute(
            "INSERT INTO plan_retirements (plan_id, superseded_by, reason, "
            " claim_id, retired_at) VALUES (?, ?, 'operator repair', ?, "
            " '2026-01-01T00:00:00Z')",
            (state["ref_plan"], new_plan, any_claim),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(cid))
        # The refutation does not quietly become a NEUTRAL that counts. Its plan
        # was rewritten past running and nothing legitimately supersedes it, so
        # the line drops and the drop is on the record.
        assert after == ("PRELIMINARY", 1, 0, 1)
        assert "ungateable_plan_skipped" in _health_ops(tmp_path)

    def test_a_rewritten_retired_rule_is_not_a_legacy_plan(
        self, tmp_path: Path,
    ) -> None:
        """The premise the attack supplies, refused on its own.

        Retirement exists for a plan a WIDER-BOUND release registered: its rule
        cannot be run today, but the plan_id keying it is the hash of that rule,
        so the row still attests itself. A writer who rewrites a live plan's
        alpha to reach the same un-runnable state does not get that: the row no
        longer hashes to its own plan_id, so it is corruption rather than a
        legacy registration, and no retirement resolves it.

        Nothing re-derived this once a line was superseded, which is what let a
        rewritten rule ride the operator's own repair into a signed flip.

        The honest path, a genuine legacy plan retired through the API, is
        covered by tests/epistemic/test_plan_retirement.py, which builds the
        wide-alpha registration rather than faking it by tampering.
        """
        state = self._refutation_under_its_own_plan(tmp_path)
        cid = state["cid"]
        conn = _adversary(tmp_path)
        conn.execute(
            "UPDATE predictions SET alpha = 0.6 WHERE plan_id = ?",
            (state["ref_plan"],),
        )
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(cid))
        # Dropped and disclosed, not silently re-gated under some other rule.
        assert after == ("PRELIMINARY", 1, 0, 1)
        assert "ungateable_plan_skipped" in _health_ops(tmp_path)


# ---------------------------------------------------------------------------
# The proposition a finding attests is bound by content_id, not by prose
# ---------------------------------------------------------------------------


class TestTheEdgeIsBoundByContentId:
    """``findings.content_id`` decides which proposition a finding's evidence
    counts toward, and nothing signs it. The finding's own signed record names
    the content_id it was filed against, and the proposition row has to hash to
    the key it sits under, so both ends of the edge are re-derivable from
    identity rather than from a rendered sentence."""

    def _two_propositions(self, tmp_path: Path) -> dict:
        """One finding on BRCA1, one registered plan on a second proposition.

        The second proposition is the one the attacker wants the evidence moved
        onto: it is real, registered, and has no evidence of its own.
        """
        ka = _bootstrap_key(tmp_path, "ka.key")
        mine = _prop()
        other = Proposition.from_dict({**mine.to_dict(), "subject": "TP53"})
        with mareforma.open(tmp_path, key_path=ka) as g:
            rec = g.assert_finding(
                mine, _pred(), _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
            g.register_plan(other, _pred())
        return {
            "root": ka, "mine": mine.content_id(), "other": other.content_id(),
            "rec": rec,
        }

    def test_repointing_the_finding_at_another_proposition_is_caught(
        self, tmp_path: Path,
    ) -> None:
        state = self._two_propositions(tmp_path)
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            assert _status_tuple(g.proposition_status(state["other"])) == (
                "UNTESTED", 0, 0, 0
            )

        conn = _adversary(tmp_path)
        conn.execute(
            "UPDATE findings SET content_id = ? WHERE finding_id = ?",
            (state["other"], state["rec"]["finding_id"]),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(state["other"]))
            # The moved evidence does not count for a sentence its own signed
            # record does not name, and the drop is disclosed.
            assert after == ("UNTESTED", 0, 0, 1)
            with pytest.raises(GateInputRefused, match="does not attest it"):
                verify_gate_inputs_or_refuse(
                    g._conn, state["other"], cache=GateCache(),
                )
        assert "proposition_rebind_skipped" in _health_ops(tmp_path)

    def test_a_proposition_row_that_no_longer_hashes_to_its_key_is_caught(
        self, tmp_path: Path,
    ) -> None:
        """The other end of the same edge. Rewriting the ``propositions`` row
        leaves its ``content_id`` key naming a sentence the row no longer states,
        so every finding filed against that key is now attached to prose none of
        them wrote. The row has to re-derive its own identity."""
        state = self._two_propositions(tmp_path)
        conn = _adversary(tmp_path)
        conn.execute(
            "UPDATE propositions SET object = 'apoptosis' WHERE content_id = ?",
            (state["mine"],),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(state["mine"]))
            assert after == ("UNTESTED", 0, 0, 1)
            with pytest.raises(GateInputRefused, match="does not attest it"):
                verify_gate_inputs_or_refuse(
                    g._conn, state["mine"], cache=GateCache(),
                )
        assert "proposition_rebind_skipped" in _health_ops(tmp_path)

    def test_an_untouched_edge_still_counts(self, tmp_path: Path) -> None:
        """The benign control: neither end rewritten, the finding counts and the
        recovery is not refused."""
        state = self._two_propositions(tmp_path)
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = _status_tuple(g.proposition_status(state["mine"]))
            verify_gate_inputs_or_refuse(
                g._conn, state["mine"], cache=GateCache(),
            )
        assert after == ("PRELIMINARY", 1, 0, 0)
        assert "proposition_rebind_skipped" not in _health_ops(tmp_path)


# ---------------------------------------------------------------------------
# Scope keys that differ only in case (a regression, not an attack)
# ---------------------------------------------------------------------------

# One proposition, named by two agents who capitalised a scope key differently.
# Identity casefolds scope keys, so these are the SAME proposition and share a
# content_id. What differs is where each key sorts: ``Population`` sorts before
# ``condition`` on raw bytes (every uppercase letter precedes every lowercase
# one) and after it once casefolded, so the two agents render their scope pairs
# in opposite orders.
_SCOPE_MIXED_CASE = {"Population": "TNBC", "condition": "in vitro"}
_SCOPE_LOWER_CASE = {"population": "TNBC", "condition": "in vitro"}


def _prop_with_scope(scope: dict) -> Proposition:
    return Proposition.from_dict({**_prop().to_dict(), "scope": scope})


class TestScopeValuesThatJsonDoesNotRoundTrip:
    """Identity hashes ``str(value)`` and ``text`` renders ``str(value)``, but the
    row stored the caller's Python value through ``json.dumps``, which does not
    preserve every type it accepts: a tuple comes back a list, a non-string key
    comes back a string. Such a row could not re-derive its own ``content_id``
    and did not render its own claim text, so on an untampered graph with no
    adversary anywhere the finding was dropped and the operator's own backup was
    refused. The row now stores the token identity was computed over."""

    @pytest.mark.parametrize(
        "scope",
        [
            pytest.param(
                {"cell_lines": ("MCF7", "MDA-MB-231"), "population": "TNBC"},
                id="tuple-value",
            ),
            pytest.param(
                {"passages": {1: "early", 2: "late"}, "population": "TNBC"},
                id="nested-dict-with-int-keys",
            ),
            # All keys one type: ``text`` sorts the raw scope items, so a scope
            # MIXING str and int keys raises a TypeError out of ``sorted`` at
            # write time. That is loud, immediate, and nothing is stored, so it
            # is not this defect.
            pytest.param({1: "first-pass", 2: "TNBC"}, id="non-string-keys"),
            pytest.param(
                {"replicates": 3, "population": "TNBC"}, id="int-value",
            ),
        ],
    )
    def test_the_finding_counts_and_the_graph_restores(
        self, tmp_path: Path, scope: dict,
    ) -> None:
        ka = _bootstrap_key(tmp_path, "ka.key")
        prop = _prop_with_scope(scope)
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, _pred(), _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
            after = _status_tuple(g.proposition_status(prop.content_id()))
        assert after == ("PRELIMINARY", 1, 0, 0)
        assert "proposition_rebind_skipped" not in _health_ops(tmp_path)
        _wipe_db(tmp_path)
        mareforma.restore(tmp_path)
        with mareforma.open(tmp_path, key_path=ka) as g:
            assert _status_tuple(g.proposition_status(prop.content_id())) == (
                "PRELIMINARY", 1, 0, 0
            )


class TestScopeKeyCaseVariantStillCounts:
    """The binding is on identity, so a rendering cannot false-drop a finding.

    ``content_id`` casefolds scope keys; ``Proposition.text`` orders them by the
    RAW key, and every uppercase byte sorts before every lowercase one. So two
    propositions that ARE the same proposition can render their scope pairs in
    opposite orders, and no normalisation applied afterwards undoes an ordering.
    A binding built on comparing two renderings therefore fails on honest,
    adversary-free graphs: the first test below read UNTESTED (0, 0, 1) and its
    own backup would not restore, and the second read PRELIMINARY (1, 0, 1)
    instead of counting both agents.

    Binding on the ``content_id`` the finding's own signed record names is exact
    in both directions: it is what identity is, so it catches a re-point and
    never drops a case variant.
    """

    def test_one_honest_writer_with_mixed_case_scope_keys_still_counts(
        self, tmp_path: Path,
    ) -> None:
        """No second agent, no tampering, no adversary connection: one key, one
        finding, scope keys the caller happened to capitalise unevenly. The line
        must count and the graph must restore from its own backup."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        prop = _prop_with_scope(_SCOPE_MIXED_CASE)
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, _pred(), _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
            after = _status_tuple(g.proposition_status(prop.content_id()))
        assert after == ("PRELIMINARY", 1, 0, 0)
        _wipe_db(tmp_path)
        mareforma.restore(tmp_path)

    def test_one_unsigned_writer_naming_it_twice_still_counts(
        self, tmp_path: Path,
    ) -> None:
        """The same case on the FALLBACK, where there is no signed record to bind
        on and the rendering comparison is all there is. One writer, no key, no
        adversary: the plan is registered under one capitalisation of a scope key
        and the finding filed under the other. They are one proposition with one
        content_id and one stored row, so the finding must count and the graph
        must restore. The row is compared against every order a writer of it
        could have emitted, not against a single string."""
        mixed = _prop_with_scope(_SCOPE_MIXED_CASE)
        lower = _prop_with_scope(_SCOPE_LOWER_CASE)
        assert mixed.content_id() == lower.content_id()
        with mareforma.open(tmp_path) as g:
            g.register_plan(mixed, _pred())
            g.submit_finding(
                lower, _pred(), _est(), data_id="ds1", generated_by="run1",
            )
            after = _status_tuple(g.proposition_status(lower.content_id()))
        assert after == ("PRELIMINARY", 1, 0, 0)
        assert "proposition_rebind_skipped" not in _health_ops(tmp_path)
        _wipe_db(tmp_path)
        mareforma.restore(tmp_path)

    def test_two_unsigned_agents_naming_one_proposition_both_count(
        self, tmp_path: Path,
    ) -> None:
        """The two-writer case on the fallback. Neither claim carries an envelope
        to bind on, so both sides are renderings, and the two agents render their
        scope pairs in opposite orders."""
        mixed = _prop_with_scope(_SCOPE_MIXED_CASE)
        lower = _prop_with_scope(_SCOPE_LOWER_CASE)
        with mareforma.open(tmp_path) as g:
            g.assert_finding(
                mixed, _pred(), _est(), data_id="ds1", generated_by="run1",
            )
            g.assert_finding(
                lower, _pred(), _est(), data_id="ds2", generated_by="run2",
            )
            after = _status_tuple(g.proposition_status(lower.content_id()))
        assert after == ("CONVERGENT", 2, 0, 0)
        assert "proposition_rebind_skipped" not in _health_ops(tmp_path)

    def test_two_agents_naming_one_proposition_both_count(
        self, tmp_path: Path,
    ) -> None:
        """The stated requirement. Two agents name one proposition, differing
        only in the capitalisation of a scope key, and converge on a single row
        whose stored strings are the first writer's. Both findings attest that
        one proposition, so both must count."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        mixed = _prop_with_scope(_SCOPE_MIXED_CASE)
        lower = _prop_with_scope(_SCOPE_LOWER_CASE)
        assert mixed.content_id() == lower.content_id()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                mixed, _pred(), _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                lower, _pred(), _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_GPT),
            )
            after = _status_tuple(g.proposition_status(lower.content_id()))
        assert after == ("CONVERGENT", 2, 0, 0)
