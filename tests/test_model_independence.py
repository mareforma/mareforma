"""Independence counted by distinct model/method, not signer + data alone.

A corroboration counts only when the two lines differ in model/method as well as
signer and dataset. Two same-model checks (distinct signer, distinct data) are a
single line of evidence, not independent corroboration, so they stay below
CONVERGENT. Where the model lineage is soft (PROXY / UNVERIFIABLE) on either
side, the pair is UNVERIFIABLE for independence, never a silent pass. A finding
with no observed model call carries no model constraint, so the pre-observer /
legacy behaviour (distinct signer + data corroborate) is unchanged.

The trust map surfaces the same axis as an effective-independence number, with an
explicit UNVERIFIABLE state where the lineage is soft.
"""
from __future__ import annotations

from pathlib import Path

import mareforma
from mareforma.observe._lineage import resolve_lineage
from mareforma.trust._store import effective_independence, independence_counts
from tests._helpers import (
    _bootstrap_key, _enroll_key, _est, _pred, _prop, _verdict,
)


_CLAUDE = "claude-3-5-sonnet-20241022"   # COMPUTED root: claude-3-5-sonnet
_GPT = "gpt-4o-2024-08-06"               # COMPUTED root: gpt-4o
_FINETUNE = "ft:gpt-4o-2024-08-06:acme::rExAbC12"  # UNVERIFIABLE (no base)


# ---------------------------------------------------------------------------
# Trust core (red-first)
# ---------------------------------------------------------------------------

class TestModelDistinctness:
    def test_same_model_does_not_promote(self, tmp_path: Path) -> None:
        """Two checks, distinct signer, distinct data, SAME COMPUTED model , 
        are one line of evidence, not two: they stay below CONVERGENT."""
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
        corroborate, the model axis does not block a genuinely different one.
        The second signer is enrolled so its distinct model authenticates on
        read; only a verified distinct model counts (fail closed)."""
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
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 2
        assert status["status"] == "CONVERGENT"

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
        not a counted distinct model, so it cannot manufacture corroboration."""
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
        assert status["status"] == "CONVERGENT"


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
        _enroll_key(tmp_path, ka, kb, identity="b@lab")
        _enroll_key(tmp_path, ka, kc, identity="c@lab")
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
# Human check: counted by the status ladder, not certified by the map
# ---------------------------------------------------------------------------

class TestHumanIndependence:
    def test_human_independence_counts(self, tmp_path: Path) -> None:
        """A human check (no observed model call, signed by an enrolled human
        validator) counts on its own axis in the status ladder: it needs no
        distinct model, so a human check plus a model check reads as two where
        two same-model checks read as one.

        The per-finding map disclosure does not follow. ``validator_type`` is
        self-declared and defaults to 'human', so an unobserved line under a
        human signer is soft there and the map reads UNVERIFIABLE.
        """
        # Scenario A: human check + model check on one proposition. The human
        # signer opens the project first, so it auto-enrolls as the human root.
        a = tmp_path / "a"
        a.mkdir()
        ka = _bootstrap_key(a, "human.key")   # enrolled human root
        kb = _bootstrap_key(a, "model.key")
        _enroll_key(a, ka, kb, identity="model@lab")
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
            assert independence_counts(g._conn, cid)[0] == 2
        # A second, same-model check does NOT collapse the human check away: the
        # duplicate model folds to one, the human unit still stands -> still 2.
        kc = _bootstrap_key(a, "model2.key")
        _enroll_key(a, ka, kc, identity="model2@lab")
        with mareforma.open(a, key_path=kc) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds3", generated_by="run3",
                grounding=_verdict(_CLAUDE),  # same root as the kb model check
            )
            ladder_a = independence_counts(g._conn, cid)[0]
            eff_a = effective_independence(g._conn, cid)
            tmap = g.trust_map(r["claim_id"])
        assert ladder_a == 2
        # The map does not certify the human line: nothing was observed and the
        # 'human' type is self-declared, so the disclosure reads UNVERIFIABLE.
        assert eff_a["number"] == 1
        assert eff_a["soft"] is True
        assert tmap.get("independence").value == "UNVERIFIABLE"

        # Scenario B: two same-model checks, no human check -> ladder 1.
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
            ladder_b = independence_counts(g._conn, prop2.content_id())[0]
        assert ladder_b == 1

        # The human axis lifts the ladder above the same-model floor.
        assert ladder_a > ladder_b


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

    def test_provider_gate_matches_host_not_substring(self) -> None:
        # COMPUTED is gated on this, so it must match the parsed HOST, not a
        # substring of the producer-controlled URL. A producer must not earn a
        # provider by naming one anywhere in a URL they own.
        from mareforma.observe._loaders import _provider_of

        assert _provider_of("https://api.anthropic.com/v1/messages") == "anthropic"
        assert _provider_of("https://api.openai.com/v1/chat/completions") == "openai"
        assert _provider_of("https://evil.com/anthropic") is None
        assert _provider_of("http://localhost:8080/openai") is None
        assert _provider_of("https://api.anthropic.com.attacker.net/v1") is None
        assert _provider_of("https://anthropic.attacker.com/x?u=api.openai.com") is None


class TestHumanAxisIsLoadBearing:
    """The human axis must genuinely change the count, not read like an absent line.

    A human line WINS a run outright (``_collapse_run_model``): a run that also
    authored a soft (UNVERIFIABLE) line counts because the human rescues it, where
    an absent line would leave the run soft and uncounted. This isolates the axis
    so the guarantee is not a tautology against the legacy absent semantics.
    """

    def test_human_line_rescues_an_otherwise_soft_run(self) -> None:
        from mareforma.trust._store import _count_run_distinct

        with_human = _count_run_distinct([
            ("a", "d1", ("model", "gpt-4o")),   # a distinct computed model: 1
            ("b", "d2", ("soft",)),             # run b also authored a soft line
            ("b", "d3", ("human",)),            # a human check wins run b: +1
        ])
        without_human = _count_run_distinct([
            ("a", "d1", ("model", "gpt-4o")),
            ("b", "d2", ("soft",)),
            ("b", "d3", ("absent",)),           # demoted: run b is soft -> 0
        ])
        assert with_human == 2
        assert without_human == 1
