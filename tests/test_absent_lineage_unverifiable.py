"""Absent model lineage is an honesty gap, not a confident independent line.

The v0.3.10 model-distinctness gate holds only for observed model calls. When
the observer never saw a model call (a subprocess, an unsupported SDK, or
``grounding=None``), the per-finding effective-independence disclosure must not
revert to the pre-v0.3.10 signer axis and print a confident number: it cannot
tell the models apart, so the trust map reads UNVERIFIABLE. The legacy status
ladder (independence_counts) still counts distinct signers, this narrows only
the per-finding certification the map surfaces.
"""
from __future__ import annotations

from pathlib import Path

import mareforma
from mareforma.trust._store import (
    effective_independence,
    effective_independence_receipt,
    independence_counts,
)
from tests._helpers import (
    _bootstrap_key, _enroll_key, _est, _pred, _prop, _verdict,
)

_CLAUDE = "claude-3-5-sonnet-20241022"
_GPT = "gpt-4o-2024-08-06"


class TestAbsentLineageIsUnverifiable:
    def test_two_absent_lines_read_unverifiable_not_a_confident_two(
        self, tmp_path: Path,
    ) -> None:
        """Two supporting lines with no observed model call, distinct signers,
        distinct data: the effective number does not read a confident 2, the
        models were never observed, so it is soft / UNVERIFIABLE."""
        # Unsigned graph: neither line is an enrolled human validator, so both
        # carry absent (not human) lineage.
        prop, pred = _prop(), _pred()
        cid = prop.content_id()
        with mareforma.open(tmp_path) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1")
            r = g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
            )
            eff = effective_independence(g._conn, cid)
            tmap = g.trust_map(r["claim_id"])
        assert not (eff["number"] == 2 and eff["soft"] is False)
        assert eff["soft"] is True
        assert tmap.get("independence").value == "UNVERIFIABLE"

    def test_absent_does_not_block_a_genuine_distinct_model_pair(
        self, tmp_path: Path,
    ) -> None:
        """An absent line alongside two genuinely distinct COMPUTED models does
        not suppress the corroboration: the clean pair still reads 2."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        kc = _bootstrap_key(tmp_path, "kc.key")
        _enroll_key(tmp_path, ka, kb)  # so kb's distinct model authenticates
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
        with mareforma.open(tmp_path, key_path=kc) as g:
            g.assert_finding(  # no grounding -> absent lineage
                prop, pred, _est(), data_id="ds3", generated_by="run3",
            )
            eff = effective_independence(g._conn, cid)
        assert eff["number"] == 2

    def test_status_ladder_still_counts_distinct_signers(
        self, tmp_path: Path,
    ) -> None:
        """The legacy status ladder is unchanged: absent distinct-signer findings
        still corroborate to CONVERGENT. Only the per-finding map disclosure
        narrows."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1")
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds2", generated_by="run2")
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 2
        assert status["status"] == "CONVERGENT"

    def test_receipt_absent_reads_unverifiable(self, tmp_path: Path) -> None:
        """The measurement receipt reports the absent body as soft, not a
        same-model collapse."""
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1")
            g.assert_finding(prop, pred, _est(), data_id="ds2", generated_by="run2")
            rec = effective_independence_receipt(g._conn, prop.content_id())
        assert rec["soft"] is True
        assert rec["number"] == 1

    def test_counters_do_not_promise_they_always_agree(self) -> None:
        """The two counters answer different questions and routinely differ.

        The tests above pin the split on the commonest shape in the wild: a
        finding with no observed model call reads 2 on the ladder and 1 on the
        map. A docstring promising they can never disagree invites a caller to
        quote either number for either question.
        """
        for fn in (independence_counts, effective_independence):
            text = " ".join((fn.__doc__ or "").split())
            assert "never disagree" not in text, (
                f"{fn.__name__} promises an agreement the counters do not hold"
            )

    def test_declared_human_signer_does_not_certify_independence(
        self, tmp_path: Path,
    ) -> None:
        """A human signer is self-declared, never observed, so it cannot lift an
        unobserved line to a confident unit in the per-finding disclosure.

        ``validator_type`` defaults to ``'human'`` and the root's type cannot be
        chosen at all, so every fresh graph would otherwise print a confident
        count for a body where no model call was observed and no person attested
        to anything.
        """
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        prop, pred = _prop(), _pred()
        cid = prop.content_id()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1")
        with mareforma.open(tmp_path, key_path=kb) as g:
            r = g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
            )
            eff = effective_independence(g._conn, cid)
            tmap = g.trust_map(r["claim_id"])
        assert eff["soft"] is True
        assert eff["number"] == 1
        assert tmap.get("independence").value == "UNVERIFIABLE"
