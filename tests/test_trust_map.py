"""Tests for the per-finding trust map (read-side artifact) and its HTML render.

The map places every trust property at a tier with its residual, infers nothing
it cannot compute, and renders to a self-contained HTML file. These pin: every
property present, the pre-binding GROUNDED label, UNVERIFIABLE independence
under a single trust domain, "not present" for a pre-observer claim, and a
byte-stable, dependency-free HTML render.
"""
from __future__ import annotations

import json

from mareforma.trust_map import (
    NOT_PRESENT,
    PRE_BINDING_GROUNDED_LABEL,
    TRUST_MAP_VERSION,
    Tier,
    TrustMap,
    TrustProperty,
    _assemble,
)
from mareforma.trust_map_html import render_html

# The eleven properties the map must always place, in order.
_EXPECTED_PROPERTIES = (
    "attributability",
    "provenance",
    "grounding",
    "faithfulness",
    "methodological_validity",
    "leakage",
    "independence",
    "contestation",
    "standing",
    "trust_root",
    "witnessing",
)


def _claim(**overrides) -> dict:
    """A minimal claim row dict for _assemble, with sane defaults."""
    base = {
        "claim_id": "11111111-2222-3333-4444-555555555555",
        "text": "a finding",
        "classification": "ANALYTICAL",
        "support_level": "PRELIMINARY",
        "status": "open",
        "supports_json": "[]",
        "contradicts_json": "[]",
        "asserter_keyid": "abcdef0123456789",
        "signature_bundle": "{}",
        "transparency_logged": 1,
        "verified": True,
        "observed_grounding": None,
        "t_invalid": None,
    }
    base.update(overrides)
    return base


class TestEveryPropertyPresent:
    def test_all_properties_present_with_tier_and_residual(self) -> None:
        tmap = _assemble(_claim(), n_roots=1, has_inclusion=False)
        names = tuple(p.name for p in tmap.properties)
        assert names == _EXPECTED_PROPERTIES
        for p in tmap.properties:
            assert isinstance(p.tier, Tier)
            assert p.residual, f"{p.name} has an empty residual"

    def test_version_and_subject(self) -> None:
        tmap = _assemble(_claim(claim_id="cid-9"), n_roots=1,
                         has_inclusion=False)
        assert tmap.version == TRUST_MAP_VERSION
        assert tmap.subject_kind == "claim"
        assert tmap.subject_id == "cid-9"


class TestGroundingRow:
    def test_pre_observer_claim_renders_not_present(self) -> None:
        tmap = _assemble(_claim(observed_grounding=None),
                         n_roots=1, has_inclusion=False)
        g = tmap.get("grounding")
        assert g.value == NOT_PRESENT
        assert "predates" in g.residual

    def test_pre_binding_grounded_carries_ov6_label(self) -> None:
        record = {"version": "v0.3.8", "grounding": "GROUNDED",
                  "reason": "cited file opened and non-empty",
                  "receipt_digest": "sha256:deadbeef"}
        tmap = _assemble(_claim(observed_grounding=json.dumps(record)),
                         n_roots=1, has_inclusion=False)
        g = tmap.get("grounding")
        assert g.value == PRE_BINDING_GROUNDED_LABEL
        assert g.tier is Tier.PROXIED

    def test_grounding_carries_reason_and_cited_set(self) -> None:
        record = {"version": "v0.3.9", "grounding": "GROUNDED",
                  "reason": "matched read", "cited_sources": ["/data/a.csv"],
                  "receipt_digest": "sha256:beef"}
        tmap = _assemble(_claim(observed_grounding=json.dumps(record)),
                         n_roots=2, has_inclusion=False)
        g = tmap.get("grounding")
        assert g.value == "GROUNDED"  # post-binding, no pre-binding label
        assert "matched read" in g.residual
        assert "/data/a.csv" in g.residual

    def test_empty_grounded_set_is_named_not_implied_grounded(self) -> None:
        # A present-but-empty grounded set (no cited read was observed) must be
        # named as such, never rendered as if every cited source was grounded.
        record = {"version": "v0.3.9", "grounding": "GROUNDED",
                  "reason": "matched read", "cited_sources": ["/data/a.csv"],
                  "grounded_sources": [], "receipt_digest": "sha256:beef"}
        tmap = _assemble(_claim(observed_grounding=json.dumps(record)),
                         n_roots=1, has_inclusion=False)
        residual = tmap.get("grounding").residual
        assert "no cited read observed" in residual
        assert "not all read-verified" in residual  # the declared gap is shown

    def test_ungrounded_is_not_proxied(self) -> None:
        record = {"version": "v0.3.9", "grounding": "UNGROUNDED",
                  "reason": "no cited read", "receipt_digest": "sha256:0"}
        tmap = _assemble(_claim(observed_grounding=json.dumps(record)),
                         n_roots=1, has_inclusion=False)
        g = tmap.get("grounding")
        assert g.value == "UNGROUNDED"
        assert g.tier is Tier.COMPUTED


class TestFaithfulnessRow:
    def test_no_record_renders_not_present(self) -> None:
        # Faithfulness is not stored on the claim; with no re-execution supplied
        # the axis reads "not present", never inferred.
        tmap = _assemble(_claim(), n_roots=1, has_inclusion=False)
        f = tmap.get("faithfulness")
        assert f.value == NOT_PRESENT
        assert f.tier is Tier.COMPUTED
        assert "not checked" in f.residual

    def test_reexec_map_is_proxy_named(self) -> None:
        # A supplied REPRODUCED verdict places at the PROXY tier with the residual
        # naming what reproducibility does NOT cover: not correctness, not
        # independence. It must never read as truth or independence.
        record = {"version": "v0.3.10", "verdict": "REPRODUCED",
                  "residual": "same-arm re-run matched"}
        tmap = _assemble(_claim(), n_roots=1, has_inclusion=False,
                         reexec_record=record)
        f = tmap.get("faithfulness")
        assert f.value == "REPRODUCED"
        assert f.tier is Tier.PROXIED
        assert "not correct" in f.residual
        assert "not an independent" in f.residual

    def test_diverged_and_could_not_are_placed(self) -> None:
        for verdict in ("DIVERGED", "COULD_NOT_REEXECUTE"):
            tmap = _assemble(_claim(), n_roots=1, has_inclusion=False,
                             reexec_record={"verdict": verdict, "residual": "r"})
            f = tmap.get("faithfulness")
            assert f.value == verdict
            assert f.tier is Tier.PROXIED

    def test_unrecognised_verdict_is_not_present(self) -> None:
        # A hand-edited or future-shaped record must not overclaim a verdict the
        # map does not understand.
        tmap = _assemble(_claim(), n_roots=1, has_inclusion=False,
                         reexec_record={"verdict": "TOTALLY_FINE"})
        assert tmap.get("faithfulness").value == NOT_PRESENT

    def test_malformed_record_is_not_present(self) -> None:
        tmap = _assemble(_claim(), n_roots=1, has_inclusion=False,
                         reexec_record="not a record")
        assert tmap.get("faithfulness").value == NOT_PRESENT


class TestIndependenceAxis:
    def test_single_trust_domain_is_unverifiable(self) -> None:
        tmap = _assemble(_claim(), n_roots=1, has_inclusion=False)
        assert tmap.get("independence").value == "UNVERIFIABLE"

    def test_multi_root_is_not_called_independent(self) -> None:
        tmap = _assemble(_claim(), n_roots=2, has_inclusion=False)
        ind = tmap.get("independence")
        assert ind.value != "UNVERIFIABLE"
        # The map must never translate convergence into the word "independent".
        assert "independent" not in ind.value.lower()


class TestWitnessingHonesty:
    def test_flag_without_inclusion_is_not_witnessed(self) -> None:
        tmap = _assemble(_claim(transparency_logged=1),
                         n_roots=1, has_inclusion=False)
        assert tmap.get("witnessing").value == "not witnessed"

    def test_actual_inclusion_is_logged(self) -> None:
        tmap = _assemble(_claim(transparency_logged=1),
                         n_roots=1, has_inclusion=True)
        assert tmap.get("witnessing").value == "logged"

    def test_unsigned_claim_has_nothing_to_witness(self) -> None:
        tmap = _assemble(_claim(signature_bundle=None),
                         n_roots=1, has_inclusion=False)
        assert tmap.get("witnessing").value == NOT_PRESENT


class TestAttributability:
    def test_unsigned_claim(self) -> None:
        tmap = _assemble(_claim(asserter_keyid=None, signature_bundle=None),
                         n_roots=1, has_inclusion=False)
        assert tmap.get("attributability").value == "unsigned"

    def test_failed_reverify_is_surfaced_not_hidden(self) -> None:
        tmap = _assemble(_claim(verified=False), n_roots=1,
                         has_inclusion=False)
        assert "failed" in tmap.get("attributability").residual

    def test_non_enrolled_asserter_is_not_claimed_reverified(self) -> None:
        # verify_claim_signatures passes a non-enrolled asserter on the
        # claim-binding alone (no pubkey to check the signature against), so the
        # map must NOT render "re-verified" — that would overclaim a check that
        # never happened. asserter_enrolled=False is the honest signal.
        tmap = _assemble(_claim(), n_roots=1, has_inclusion=False,
                         sig_verified=True, asserter_enrolled=False)
        residual = tmap.get("attributability").residual
        assert "not an enrolled validator" in residual
        assert "re-verified on read" not in residual

    def test_enrolled_asserter_reads_reverified(self) -> None:
        tmap = _assemble(_claim(), n_roots=1, has_inclusion=False,
                         sig_verified=True, asserter_enrolled=True)
        assert "signature re-verified on read" == tmap.get("attributability").residual


class TestDeferredProperties:
    def test_leakage_is_deferred_with_named_residual(self) -> None:
        tmap = _assemble(_claim(), n_roots=1, has_inclusion=False)
        leak = tmap.get("leakage")
        assert leak.tier is Tier.DEFERRED
        assert "held out" in leak.residual or "held-out" in leak.residual

    def test_trust_root_is_deferred(self) -> None:
        tmap = _assemble(_claim(), n_roots=1, has_inclusion=False)
        assert tmap.get("trust_root").tier is Tier.DEFERRED


class TestCanonicalDigest:
    def test_digest_is_stable_across_equal_maps(self) -> None:
        a = _assemble(_claim(), n_roots=1, has_inclusion=False)
        b = _assemble(_claim(), n_roots=1, has_inclusion=False)
        assert a.canonical_digest() == b.canonical_digest()
        assert a.canonical_digest().startswith("sha256:")

    def test_digest_changes_when_a_property_changes(self) -> None:
        a = _assemble(_claim(), n_roots=1, has_inclusion=False)
        b = _assemble(_claim(), n_roots=2, has_inclusion=False)
        assert a.canonical_digest() != b.canonical_digest()


class TestBuildFromGraph:
    def test_build_trust_map_on_real_claim(self, graph) -> None:
        cid = graph.assert_claim("real", classification="ANALYTICAL",
                                 source_name="ds")
        tmap = graph.trust_map(cid)
        assert isinstance(tmap, TrustMap)
        assert tmap.subject_id == cid
        assert tmap.get("independence").value == "UNVERIFIABLE"

    def test_missing_claim_returns_none(self, graph) -> None:
        assert graph.trust_map("does-not-exist") is None

    def test_map_attributability_runs_a_real_signature_check(self, graph) -> None:
        # The map must reflect an ACTUAL signature re-verification, not the
        # promotion read-gate (which passes a signed PRELIMINARY row through
        # True). An honest signed PRELIMINARY claim reads "re-verified"; a
        # tampered signed field flips it to "failed", NEVER a false "re-verified".
        import base64

        cid = graph.assert_claim("prelim finding", classification="ANALYTICAL")
        honest = graph.trust_map(cid)
        assert honest.get("attributability").value != "unsigned"
        assert "re-verified on read" in honest.get("attributability").residual
        # Corrupt the asserter signature in the (editable) bundle without
        # re-signing. The promotion read-gate passes PRELIMINARY through True, so
        # only a real re-verification catches this.
        row = graph._conn.execute(
            "SELECT signature_bundle FROM claims WHERE claim_id = ?", (cid,)
        ).fetchone()
        env = json.loads(row["signature_bundle"])
        env["signatures"][0]["sig"] = base64.standard_b64encode(b"z" * 64).decode()
        graph._conn.execute(
            "UPDATE claims SET signature_bundle = ? WHERE claim_id = ?",
            (json.dumps(env), cid),
        )
        graph._conn.commit()
        tampered = graph.trust_map(cid)
        assert "failed re-verification" in tampered.get("attributability").residual
        assert "re-verified on read" not in tampered.get("attributability").residual


class TestHtmlRender:
    def _fixed_map(self) -> TrustMap:
        return TrustMap(
            version="v0.3.9",
            subject_kind="claim",
            subject_id="fixed-claim-id",
            properties=(
                TrustProperty("grounding", Tier.PROXIED, "GROUNDED", "a read"),
                TrustProperty("leakage", Tier.DEFERRED, None, "not evaluated"),
            ),
        )

    def test_render_is_deterministic(self) -> None:
        a = render_html(self._fixed_map())
        b = render_html(self._fixed_map())
        assert a == b

    def test_render_is_self_contained(self) -> None:
        html = render_html(self._fixed_map())
        # No external requests of any kind.
        assert "http://" not in html
        assert "https://" not in html
        assert "<script" not in html.lower()
        assert "src=" not in html.lower()
        assert "@import" not in html
        assert html.startswith("<!DOCTYPE html>")

    def test_render_golden(self) -> None:
        html = render_html(self._fixed_map())
        # Structural anchors an auditor / paper figure relies on.
        assert "<title>trust map — claim fixed-claim-id</title>" in html
        assert "map v0.3.9" in html
        assert ">grounding<" in html
        assert ">GROUNDED<" in html
        assert ">DEFERRED<" in html
        # A None value renders as an em-dash placeholder, never "None".
        assert ">None<" not in html
        assert "—" in html

    def test_html_escapes_dynamic_text(self) -> None:
        tmap = TrustMap("v1", "claim", "<script>x</script>",
                        (TrustProperty("p", Tier.COMPUTED, "<b>", "&amp"),))
        html = render_html(tmap)
        assert "<script>x</script>" not in html
        assert "&lt;script&gt;" in html


class TestZeroRootIndependence:
    def test_zero_roots_is_unverifiable_not_multi_root(self) -> None:
        # A validator-less graph (zero enrolled roots) renders the MOST
        # conservative independence, not the weak-prior multi-root branch.
        tmap = _assemble(_claim(), n_roots=0, has_inclusion=False)
        assert tmap.get("independence").value == "UNVERIFIABLE"
        assert tmap.get("trust_root").value == "no trust root enrolled"

    def test_single_root_is_unverifiable(self) -> None:
        tmap = _assemble(_claim(), n_roots=1, has_inclusion=False)
        assert tmap.get("independence").value == "UNVERIFIABLE"
        assert tmap.get("trust_root").value == "single trust domain"

    def test_two_roots_is_multi_root(self) -> None:
        tmap = _assemble(_claim(), n_roots=2, has_inclusion=False)
        assert tmap.get("independence").value == "MULTI_ROOT"
        assert tmap.get("trust_root").value == "multiple roots"


class TestPreBindingAllowlist:
    def test_grounded_without_version_is_pre_binding(self) -> None:
        # Allowlist polarity: a GROUNDED record with a missing/unknown version
        # must NOT render as a fully-bound GROUNDED.
        rec = {"grounding": "GROUNDED", "reason": "r"}  # no version key
        tmap = _assemble(_claim(observed_grounding=json.dumps(rec)),
                         n_roots=1, has_inclusion=False)
        assert tmap.get("grounding").value == PRE_BINDING_GROUNDED_LABEL

    def test_grounded_v039_is_bound(self) -> None:
        rec = {"version": "v0.3.9", "grounding": "GROUNDED", "reason": "r",
               "grounded_sources": ["/d.csv"]}
        tmap = _assemble(_claim(observed_grounding=json.dumps(rec)),
                         n_roots=1, has_inclusion=False)
        assert tmap.get("grounding").value == "GROUNDED"


class TestGroundingSurfacesGroundedSubset:
    def test_declared_cited_wider_than_grounded_is_flagged(self) -> None:
        # The map surfaces the grounded (read-verified) subset and names the
        # wider declared set, so B is not read as grounded when only A was.
        rec = {"version": "v0.3.9", "grounding": "GROUNDED", "reason": "r",
               "cited_sources": ["/A.csv", "/B.csv"],
               "grounded_sources": ["/A.csv"]}
        tmap = _assemble(_claim(observed_grounding=json.dumps(rec)),
                         n_roots=1, has_inclusion=False)
        resid = tmap.get("grounding").residual
        assert "grounded on: /A.csv" in resid
        assert "/B.csv" in resid and "declared cited" in resid


def test_grounding_render_survives_non_string_sources():
    # A DB-tampered non-string element in grounded/cited must degrade to an
    # honest rendered map, not crash build_trust_map (which would take down
    # `mareforma verify`/`map` for that claim).
    rec = {"version": "v0.3.9", "grounding": "GROUNDED", "reason": "r",
           "cited_sources": [1, "/A.csv"], "grounded_sources": [2]}
    tmap = _assemble(_claim(observed_grounding=json.dumps(rec)), n_roots=1,
                     has_inclusion=False)
    assert tmap.get("grounding").value  # rendered, did not raise
