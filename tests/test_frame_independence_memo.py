"""a frame read must verify each claim's signature at most once.

Independence counts drive an Ed25519 verify per evidence line, and one
``query_frame`` call recomputes the same per-proposition counts several times:
``proposition_status`` counts a proposition once for itself and again for every
sibling whose ``_frame_status`` walks it as a contrary. Without a per-call memo
each of those recomputations re-verifies the same signatures. A per-call memo
keyed on content_id (counts) and claim_id (signer authentication) collapses the
repeats so a claim's signature is authenticated once per read call.
"""
from __future__ import annotations

import collections
from pathlib import Path

import mareforma
import mareforma.trust._gate as gt
import mareforma.trust._store as st
from mareforma.trust import Direction, Proposition
from tests._helpers import _bootstrap_key, _est, _pred, _verdict

_CLAUDE = "claude-3-5-sonnet-20241022"


def _prop(direction: Direction) -> Proposition:
    return Proposition(
        subject="BRCA1", relation="affects", object="tumour growth",
        direction=direction,
        scope={"population": "TNBC", "condition": "in vitro"},
    )


def test_query_frame_authenticates_each_signature_once(tmp_path: Path) -> None:
    """Three contrary propositions in one frame, each with a finding: one
    query_frame call must authenticate each claim's signature at most once,
    not once per contrary recomputation."""
    ka = _bootstrap_key(tmp_path, "ka.key")
    props = [
        _prop(Direction.INCREASES),
        _prop(Direction.DECREASES),
        _prop(Direction.NO_EFFECT),
    ]
    pred = _pred()
    with mareforma.open(tmp_path, key_path=ka) as g:
        for i, p in enumerate(props):
            g.assert_finding(
                p, pred, _est(), data_id=f"ds{i}", generated_by=f"run{i}",
                grounding=_verdict(_CLAUDE),
            )
        frame_id = g._conn.execute(
            "SELECT frame_id FROM propositions LIMIT 1"
        ).fetchone()["frame_id"]

        counts: collections.Counter = collections.Counter()
        orig = gt._authentic_signer_keyid

        def counting(conn, claim_id, *args, **kwargs):
            counts[claim_id] += 1
            return orig(conn, claim_id, *args, **kwargs)

        gt._authentic_signer_keyid = counting
        try:
            views = st.query_frame(g._conn, frame_id)
        finally:
            gt._authentic_signer_keyid = orig

    assert len(views) == 3
    assert counts, "no signatures were authenticated at all"
    worst = max(counts.values())
    assert worst == 1, (
        f"a claim's signature was authenticated {worst} times in one "
        f"query_frame call: {dict(counts)}"
    )
