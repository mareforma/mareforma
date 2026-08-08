"""The observed axis is written by the observer, never by the caller.

The product's sentence is "computed, not declared": the grounding verdict on a
claim is what execution showed, not what the producing agent says. The write
path used to store the record a caller handed to
``assert_claim(observed_grounding=...)`` or ``assert_finding(grounding=...)``
verbatim, so a hand-built GROUNDED dict was signed, stored, gated promotion and
rendered on the computed axis exactly like one an ``observe()`` scope computed.

These tests hold the write path to the sentence from both sides: a verdict the
observer did not compute can never occupy GROUNDED on any read surface, and the
verdict an observer DID compute still arrives untouched through both documented
call shapes.
"""
from __future__ import annotations

import json
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from mareforma.observe import GroundingVerdict, ObservedGrounding as OG, observe

sys.path.insert(0, str(Path(__file__).parent))
from epistemic._builders import (  # noqa: E402
    _prop,
    _smd,
    _superiority,
    open_graph,
)


def _hand_built(path: str) -> dict:
    """The record a caller can type out: a GROUNDED conclusion, no observation."""
    return GroundingVerdict(
        OG.GROUNDED,
        "the caller says the data was read",
        cited_sources=(path,),
        grounded_sources=(path,),
    ).to_signed_dict()


def _dataset(tmp_path: Path) -> Path:
    csv = tmp_path / "trial.csv"
    csv.write_text("arm,outcome\ntreat,1\ncontrol,0\n")
    return csv


def _stored(graph, claim_id: str) -> dict:
    row = graph._conn.execute(
        "SELECT observed_grounding FROM claims WHERE claim_id = ?", (claim_id,)
    ).fetchone()
    return json.loads(row["observed_grounding"])


# -- a declared verdict never reads as a computed one ------------------------

def test_hand_built_grounded_dict_is_not_stored_as_grounded(tmp_path):
    csv = _dataset(tmp_path)
    with open_graph(tmp_path) as g:
        cid = g.assert_claim(
            "the treatment lowers the outcome",
            observed_grounding=_hand_built(str(csv)),
        )
        record = _stored(g, cid)
    assert record["grounding"] != "GROUNDED"
    assert record["provenance"] == "DECLARED"


def test_hand_built_grounded_dict_does_not_promote(tmp_path):
    # The promotion gate reads the stored column, so the neutralised state has
    # to be what the gate sees, not a separate flag a read surface might miss.
    from mareforma.db import _observed_grounding_promotes

    csv = _dataset(tmp_path)
    with open_graph(tmp_path) as g:
        cid = g.assert_claim(
            "a declared finding", observed_grounding=_hand_built(str(csv)),
        )
        row = g._conn.execute(
            "SELECT observed_grounding FROM claims WHERE claim_id = ?", (cid,)
        ).fetchone()
    assert _observed_grounding_promotes(row["observed_grounding"]) is False


def test_hand_built_verdict_object_is_not_stored_as_grounded(tmp_path):
    # The finding path takes the observer's own type, so the type is not the
    # provenance: an instance a caller constructed is still a declaration.
    csv = _dataset(tmp_path)
    verdict = GroundingVerdict(
        OG.GROUNDED, "hand-built", cited_sources=(str(csv),),
        grounded_sources=(str(csv),),
    )
    with open_graph(tmp_path) as g:
        res = g.assert_finding(
            _prop(), _superiority(), _smd(-0.8, p=0.001),
            data_id="sha256:" + "a" * 64, data_source=str(csv),
            grounding=verdict, generated_by="run/1",
        )
    assert res["grounding"]["grounding"] != "GROUNDED"
    assert res["grounding"]["provenance"] == "DECLARED"


def test_trust_map_never_renders_a_declared_verdict_as_grounded(tmp_path):
    # The read surface an auditor looks at: the value must not say GROUNDED and
    # the residual must say where the verdict came from.
    csv = _dataset(tmp_path)
    with open_graph(tmp_path) as g:
        cid = g.assert_claim(
            "a declared finding", observed_grounding=_hand_built(str(csv)),
        )
        prop = g.trust_map(cid).get("grounding")
    assert prop.value != "GROUNDED"
    assert "declared" in prop.residual


def test_a_declared_non_grounded_verdict_keeps_its_state_but_is_marked(tmp_path):
    # UNGROUNDED and OPAQUE promote nothing, so a declaration buys nothing with
    # them and they are left standing. The record still says where it came from,
    # in the reason every read surface already renders.
    record = GroundingVerdict(
        OG.UNGROUNDED, "no cited read", cited_sources=("/data/trial.csv",),
    ).to_signed_dict()
    with open_graph(tmp_path) as g:
        cid = g.assert_claim("a declared absence", observed_grounding=record)
        stored = _stored(g, cid)
    assert stored["grounding"] == "UNGROUNDED"
    assert stored["provenance"] == "DECLARED"
    assert "no verdict mareforma observed" in stored["reason"]


def test_a_minted_receipt_digest_cannot_carry_a_flipped_state(tmp_path):
    # The observer's own digest, with the state rewritten. The write path stores
    # what the observer recorded against that digest, not the caller's copy.
    csv = _dataset(tmp_path)
    with observe(cites=str(csv)) as handle:
        pass  # the step that would read the dataset never ran
    assert handle.verdict.grounding is OG.UNGROUNDED
    forged = handle.verdict.to_signed_dict()
    forged["grounding"] = "GROUNDED"
    forged["grounded_sources"] = [str(csv)]
    with open_graph(tmp_path) as g:
        cid = g.assert_claim("a flipped verdict", observed_grounding=forged)
        record = _stored(g, cid)
    assert record["grounding"] == "UNGROUNDED"


# -- the standing is the observer's register, not anything on the object -----

def test_a_subclass_cannot_declare_itself_observed(tmp_path):
    # The standing used to be a flag on the instance, so a subclass could
    # default it True and every hand-built instance of that subclass was stored
    # as an execution mareforma watched. Membership of a module-private register
    # cannot be claimed by subclassing.
    class Observed(GroundingVerdict):
        _observed = True

    csv = _dataset(tmp_path)
    verdict = Observed(
        OG.GROUNDED, "the subclass says so", cited_sources=(str(csv),),
        grounded_sources=(str(csv),),
    )
    with open_graph(tmp_path) as g:
        res = g.assert_finding(
            _prop(), _superiority(), _smd(-0.8, p=0.001),
            data_id="sha256:" + "a" * 64, data_source=str(csv),
            grounding=verdict, generated_by="run/1",
        )
    assert res["grounding"]["grounding"] == "OPAQUE"
    assert res["grounding"]["provenance"] == "DECLARED"


def test_a_duck_type_cannot_declare_itself_observed(tmp_path):
    # Same lever without the subclass: any object that answers to the flag was
    # taken at its word, whatever its type.
    csv = _dataset(tmp_path)

    class NotAVerdict:
        _observed = True

        def to_signed_dict(self):
            return {
                "version": "v0.3.11", "grounding": "GROUNDED",
                "reason": "the duck says so", "cited_sources": [str(csv)],
                "grounded_sources": [str(csv)],
                "receipt_digest": "sha256:" + "d" * 64,
            }

    with open_graph(tmp_path) as g:
        res = g.assert_finding(
            _prop(), _superiority(), _smd(-0.8, p=0.001),
            data_id="sha256:" + "a" * 64, data_source=str(csv),
            grounding=NotAVerdict(), generated_by="run/1",
        )
    assert res["grounding"]["grounding"] == "OPAQUE"
    assert res["grounding"]["provenance"] == "DECLARED"


def test_the_verdict_is_frozen(tmp_path):
    # The cheapest attack in the issue: run a real scope, then edit the object it
    # handed back. Ordinary assignment is an error at the line that writes it.
    csv = _dataset(tmp_path)
    with observe(cites=str(csv)) as handle:
        pass  # the step that would read the dataset never ran
    assert handle.verdict.grounding is OG.UNGROUNDED
    with pytest.raises(FrozenInstanceError):
        handle.verdict.grounding = OG.GROUNDED


def test_an_edit_that_reaches_past_frozen_is_still_discarded(tmp_path):
    # Freezing is the cheap half. ``object.__setattr__`` reaches through a frozen
    # dataclass, so what actually holds is that the write path stores the
    # SNAPSHOT taken when the observer minted the verdict, not a re-serialization
    # of whatever the object says by the time it is written.
    csv = _dataset(tmp_path)
    with observe(cites=str(csv)) as handle:
        pass
    verdict = handle.verdict
    assert verdict.grounding is OG.UNGROUNDED
    object.__setattr__(verdict, "grounding", OG.GROUNDED)
    object.__setattr__(verdict, "grounded_sources", (str(csv),))
    assert verdict.grounding is OG.GROUNDED  # the object really was edited
    with open_graph(tmp_path) as g:
        res = g.assert_finding(
            _prop(), _superiority(), _smd(-0.8, p=0.001),
            data_id="sha256:" + "a" * 64, data_source=str(csv),
            grounding=verdict, generated_by="run/1",
        )
        stored = _stored(g, res["claim_id"])
    assert stored["grounding"] == "UNGROUNDED"


def test_an_evicted_digest_does_not_lose_a_live_verdict(tmp_path):
    # The honest-caller failure the earlier design shipped: the digest table is
    # capped, and an eviction rewrote a genuine observed GROUNDED to OPAQUE. A
    # caller still holding the verdict must never lose to that, so a miss falls
    # back to the live verdicts before giving up.
    from mareforma.observe import _verdict as _v

    csv = _dataset(tmp_path)
    with observe(cites=str(csv)) as handle:
        csv.read_text()
    assert handle.verdict.grounding is OG.GROUNDED
    record = handle.verdict.to_signed_dict()
    _v._BY_DIGEST.clear()  # simulate the cap evicting this entry
    with open_graph(tmp_path) as g:
        cid = g.assert_claim("an observed claim", observed_grounding=record)
        stored = _stored(g, cid)
    assert stored["grounding"] == "GROUNDED"
    assert "provenance" not in stored


# -- the observer's own verdict is untouched ---------------------------------

def test_observed_verdict_still_reads_grounded_through_assert_claim(tmp_path):
    # The README call shape, unchanged: observe, then sign the computed record.
    csv = _dataset(tmp_path)
    with observe(cites=str(csv)) as handle:
        csv.read_text()
    assert handle.verdict.grounding is OG.GROUNDED
    with open_graph(tmp_path) as g:
        cid = g.assert_claim(
            "the treatment lowers the outcome",
            observed_grounding=handle.verdict.to_signed_dict(),
        )
        record = _stored(g, cid)
    assert record["grounding"] == "GROUNDED"
    assert "provenance" not in record


def test_observed_verdict_still_reads_grounded_through_assert_finding(tmp_path):
    # The shipped examples' call shape: hand the verdict object to the finding.
    csv = _dataset(tmp_path)
    with observe(cites=str(csv)) as handle:
        csv.read_text()
    with open_graph(tmp_path) as g:
        res = g.assert_finding(
            _prop(), _superiority(), _smd(-0.8, p=0.001),
            data_id="sha256:" + "a" * 64, data_source=str(csv),
            grounding=handle.verdict, generated_by="run/1",
        )
        cid = res["claim_id"]
        record = _stored(g, cid)
        prop = g.trust_map(cid).get("grounding")
    assert res["grounding"]["grounding"] == "GROUNDED"
    assert record["grounding"] == "GROUNDED"
    assert prop.value == "GROUNDED"
