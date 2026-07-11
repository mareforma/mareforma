"""Independence counted by distinct model/method, not signer + data alone.

A corroboration counts only when the two lines differ in model/method as well as
signer and dataset. Two same-model checks (distinct signer, distinct data) are a
single line of evidence, not independent corroboration, so they stay below
CORROBORATED. Where the model lineage is soft (PROXY / UNVERIFIABLE) on either
side, the pair is UNVERIFIABLE for independence — never a silent pass. A finding
with no observed model call carries no model constraint, so the pre-observer /
legacy behaviour (distinct signer + data corroborate) is unchanged.

The trust map surfaces the same axis as an effective-independence number, with an
explicit UNVERIFIABLE state where the lineage is soft.
"""
from __future__ import annotations

from pathlib import Path

import mareforma
from mareforma.observe import GroundingVerdict, ObservedGrounding
from mareforma.observe._lineage import resolve_lineage
from mareforma.trust._store import effective_independence
from tests._helpers import _bootstrap_key, _est, _pred, _prop


def _verdict(model_id: str, *, source: str = "socket") -> GroundingVerdict:
    """A grounding verdict carrying a model lineage of the requested tier.

    ``source="socket"`` to a RECOGNIZED provider host earns COMPUTED (a
    body-parse at the seam), ``"declared"`` earns PROXY; a fine-tune / alias
    string is UNVERIFIABLE regardless. The verdict itself is OPAQUE (the finding
    path only reads ``model_lineage`` off it); grounding state is irrelevant to
    the independence count.
    """
    lower = model_id.lower()
    provider = (
        "anthropic" if lower.startswith("claude")
        else "openai" if lower.startswith(("gpt", "o1", "o3", "o4", "chatgpt"))
        else None
    )
    lineage = resolve_lineage(
        model_id, source=source, method="m",
        decoding={"temperature": None, "top_p": None, "seed": None},
        provider=provider,
    )
    return GroundingVerdict(
        grounding=ObservedGrounding.OPAQUE,
        reason="test lineage",
        model_lineage=lineage,
    )


_CLAUDE = "claude-3-5-sonnet-20241022"   # COMPUTED root: claude-3-5-sonnet
_GPT = "gpt-4o-2024-08-06"               # COMPUTED root: gpt-4o
_FINETUNE = "ft:gpt-4o-2024-08-06:acme::rExAbC12"  # UNVERIFIABLE (no base)


# ---------------------------------------------------------------------------
# Trust core (red-first)
# ---------------------------------------------------------------------------

class TestModelDistinctness:
    def test_same_model_does_not_promote(self, tmp_path: Path) -> None:
        """Two checks — distinct signer, distinct data, SAME COMPUTED model —
        are one line of evidence, not two: they stay below CORROBORATED."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
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
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 1
        assert status["status"] == "PRELIMINARY"

    def test_distinct_model_promotes(self, tmp_path: Path) -> None:
        """Distinct signer + distinct data + distinct COMPUTED model still
        corroborate — the model axis does not block a genuinely different one."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
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
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 2
        assert status["status"] == "CORROBORATED"

    def test_soft_lineage_is_unverifiable(self, tmp_path: Path) -> None:
        """A PROXY (declared) lineage on one side makes the pair UNVERIFIABLE
        for independence: it does not silently corroborate."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_GPT, source="declared"),  # PROXY, soft
            )
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 1
        assert status["status"] == "PRELIMINARY"

    def test_finetune_string_is_not_a_distinct_model(self, tmp_path: Path) -> None:
        """A distinct model STRING whose base is not declarable is UNVERIFIABLE,
        not a counted distinct model — so it cannot manufacture corroboration."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_FINETUNE),  # UNVERIFIABLE
            )
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 1
        assert status["status"] == "PRELIMINARY"

    def test_absent_lineage_keeps_legacy_corroboration(self, tmp_path: Path) -> None:
        """A finding with no observed model call carries no model constraint:
        two distinct-signer, distinct-data findings corroborate as before."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1")
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds2", generated_by="run2")
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 2
        assert status["status"] == "CORROBORATED"


# ---------------------------------------------------------------------------
# Trust map: the effective-independence number
# ---------------------------------------------------------------------------

class TestEffectiveIndependenceNumber:
    def test_effective_independence_number(self, tmp_path: Path) -> None:
        """N pairwise-distinct (model, data, signer) checks → effective N;
        same-model duplicates do not inflate it; soft lineage surfaces as
        UNVERIFIABLE on the trust map's independence axis."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        kc = _bootstrap_key(tmp_path, "kc.key")
        prop, pred = _prop(), _pred()
        cid = prop.content_id()

        # Two distinct COMPUTED models, distinct signers + data → effective 2.
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            r = g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_GPT),
            )
            eff = effective_independence(g._conn, cid)
            assert eff["number"] == 2
            assert eff["soft"] is False
            # The trust map surfaces the number on the independence axis.
            tmap = g.trust_map(r["claim_id"])
            assert tmap.get("independence").value == "2"

        # A third check on the SAME model as an existing one does not inflate.
        with mareforma.open(tmp_path, key_path=kc) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds3", generated_by="run3",
                grounding=_verdict(_CLAUDE),  # same root as ds1
            )
            assert effective_independence(g._conn, cid)["number"] == 2

    def test_soft_lineage_surfaces_unverifiable_on_map(self, tmp_path: Path) -> None:
        """When a supporting line's lineage is soft, the map's independence axis
        reads UNVERIFIABLE rather than a confident number."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            r = g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_GPT, source="declared"),  # PROXY, soft
            )
            eff = effective_independence(g._conn, prop.content_id())
            assert eff["soft"] is True
            assert eff["number"] == 1
            tmap = g.trust_map(r["claim_id"])
            assert tmap.get("independence").value == "UNVERIFIABLE"


# ---------------------------------------------------------------------------
# Human check: the highest-value independent source
# ---------------------------------------------------------------------------

class TestHumanIndependence:
    def test_human_independence_counts(self, tmp_path: Path) -> None:
        """A human check (no observed model call, signed by an enrolled human
        validator) is the highest-value independent source: it counts on its own
        axis and is never collapsed away as a same-model duplicate.

        A human check plus a model check yields effective 2, where two same-model
        checks yield 1 — so the human axis lifts effective independence above the
        same-model floor.
        """
        # Scenario A: human check + model check on one proposition. The human
        # signer opens the project first, so it auto-enrolls as the human root.
        a = tmp_path / "a"
        a.mkdir()
        ka = _bootstrap_key(a, "human.key")   # enrolled human root
        kb = _bootstrap_key(a, "model.key")
        prop, pred = _prop(), _pred()
        cid = prop.content_id()
        with mareforma.open(a, key_path=ka) as g:
            # No grounding -> no observed model call: a human check.
            g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1")
        with mareforma.open(a, key_path=kb) as g:
            r = g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_CLAUDE),  # a model check
            )
            assert effective_independence(g._conn, cid)["number"] == 2
        # A second, same-model check does NOT collapse the human check away: the
        # duplicate model folds to one, the human unit still stands -> still 2.
        kc = _bootstrap_key(a, "model2.key")
        with mareforma.open(a, key_path=kc) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds3", generated_by="run3",
                grounding=_verdict(_CLAUDE),  # same root as the kb model check
            )
            eff_a = effective_independence(g._conn, cid)
            tmap = g.trust_map(r["claim_id"])
        assert eff_a["number"] == 2
        assert eff_a["soft"] is False
        assert tmap.get("independence").value == "2"

        # Scenario B: two same-model checks, no human check -> effective 1.
        b = tmp_path / "b"
        b.mkdir()
        kd = _bootstrap_key(b, "d.key")
        ke = _bootstrap_key(b, "e.key")
        prop2, pred2 = _prop(), _pred()
        with mareforma.open(b, key_path=kd) as g:
            g.assert_finding(
                prop2, pred2, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(b, key_path=ke) as g:
            g.assert_finding(
                prop2, pred2, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_CLAUDE),
            )
            eff_b = effective_independence(g._conn, prop2.content_id())
        assert eff_b["number"] == 1

        # The human axis lifts effective independence above the same-model floor.
        assert eff_a["number"] > eff_b["number"]


class TestForgedComputedIsRejected:
    """COMPUTED requires a body-parse to a RECOGNIZED provider host.

    A "model" field in a POST the producer sent to an arbitrary endpoint is
    producer-controlled and must never mint COMPUTED, or a producer could forge
    two distinct models from two request bodies and fake independence.
    """

    def test_socket_to_unrecognized_host_is_not_computed(self) -> None:
        from mareforma.observe._lineage import ModelLineageTier

        forged = resolve_lineage(
            _GPT, source="socket", method="/v1/x", decoding={}, provider=None
        )
        assert forged.tier is ModelLineageTier.UNVERIFIABLE
        real = resolve_lineage(
            _GPT, source="socket", method="/v1/x", decoding={}, provider="openai"
        )
        assert real.tier is ModelLineageTier.COMPUTED

    def test_two_forged_models_are_not_a_distinct_pair(self) -> None:
        # Same real run, two forged distinct strings to arbitrary hosts: both are
        # soft, so the pair is never model-distinct and can never promote.
        from mareforma.observe._lineage import model_distinct_pair

        a = resolve_lineage(_CLAUDE, source="socket", method="m", decoding={}, provider=None)
        b = resolve_lineage(_GPT, source="socket", method="m", decoding={}, provider=None)
        assert model_distinct_pair(a.to_dict(), b.to_dict()) is False
