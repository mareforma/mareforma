"""Model lineage is bound into the signed finding and re-authenticated on read.

The model-distinct axis reads ``evidence_lines.model_lineage``, a denormalised,
unsigned column. Binding the lineage into the signed observed record (the same
carrier as the grounding verdict) and rerouting the independence read to that
signed copy closes the forge: a column edited out of band no longer moves the
count. This mirrors the WHO-axis re-authentication ``_authentic_signer_keyid``
already applies to the signer column.
"""
from __future__ import annotations

from pathlib import Path

import mareforma
from mareforma.trust._store import effective_independence
from tests._helpers import _bootstrap_key, _est, _pred, _prop, _verdict

_CLAUDE = "claude-3-5-sonnet-20241022"   # COMPUTED root: claude-3-5-sonnet
_GPT = "gpt-4o-2024-08-06"               # COMPUTED root: gpt-4o

_FORGED_COMPUTED = (
    '{"tier":"COMPUTED","model_id":"gpt-4o-2024-08-06",'
    '"family_root":"gpt-4o","provider":"openai","version":"2024-08-06",'
    '"method":"m","decoding":{},"attestor":"provider-host","digest":null}'
)


def _tamper_one_line(conn, root: str) -> None:
    """Rewrite exactly one evidence line's unsigned model_lineage column."""
    line_id = conn.execute("SELECT line_id FROM evidence_lines LIMIT 1").fetchone()[0]
    conn.execute(
        "UPDATE evidence_lines SET model_lineage = ? WHERE line_id = ?",
        (root, line_id),
    )
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
        COMPUTED models signed on their findings still read as effective 2."""
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
                grounding=_verdict(_GPT),
            )
            eff = effective_independence(g._conn, cid)
        assert eff["number"] == 2
        assert eff["soft"] is False


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
