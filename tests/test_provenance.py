"""Tests for the agent-readable provenance surface.

Coverage:
- ``EpistemicGraph.query_provenance`` lineage structure
- rebuildable ``claim_supports`` cache: build, maintain, rebuild,
  staleness detection, recursive walks
- multi-signature DSSE for the ``claim-with-roles:v1`` predicate variant
- self-validation gate walks every signature on the envelope
- self-verdict gate walks every signature on both referenced claims
- legacy single-signature envelopes still verify under the verifier
  (regression — multi-sig must not break single-sig)
- PROV-O JSON-LD exporter + four-invariant validator
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import mareforma
from mareforma import _supports
from mareforma import db as _db
from mareforma import signing as _signing


# ----------------------------------------------------------------------------
# claim_supports rebuildable cache
# ----------------------------------------------------------------------------


class TestSupportsCache:
    def test_cache_db_attached_on_open(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            graph.assert_claim("a")
        # The cache file lives outside graph.db.
        assert (tmp_path / ".mareforma" / "claim_supports_cache.db").exists()

    def test_edges_recorded_on_assert(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            b = graph.assert_claim("b", supports=[a])
            count = _supports.claim_supports_count(graph._conn)
            assert count == 1
            # Walks return the seeded edge.
            upstream = _supports.walk_upstream(graph._conn, b, depth=1)
            assert any(u["claim_id"] == a for u in upstream)

    def test_walk_downstream_uses_reverse_index(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            b = graph.assert_claim("b", supports=[a])
            c = graph.assert_claim("c", supports=[b])
            chain = _supports.walk_downstream(graph._conn, a, depth=4)
            ids = [r["claim_id"] for r in chain]
            assert b in ids
            assert c in ids

    def test_recursive_walk_respects_depth(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            b = graph.assert_claim("b", supports=[a])
            c = graph.assert_claim("c", supports=[b])
            d = graph.assert_claim("d", supports=[c])
            # depth=2 should reach b and c from d but not a.
            upstream = _supports.walk_upstream(graph._conn, d, depth=2)
            ids = {r["claim_id"] for r in upstream}
            assert c in ids
            assert b in ids
            assert a not in ids

    def test_doi_refs_not_in_cache(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            graph.assert_claim(
                "b", supports=[a, "10.1038/s41586-026-10652-y"],
            )
            # Only the UUID-shaped ref is in the cache.
            count = _supports.claim_supports_count(graph._conn)
            assert count == 1

    def test_cache_rebuild_after_external_delete(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            b = graph.assert_claim("b", supports=[a])
            assert _supports.claim_supports_count(graph._conn) == 1

        # Delete the cache file. Next open should detect & rebuild.
        cache = tmp_path / ".mareforma" / "claim_supports_cache.db"
        cache.unlink()

        with mareforma.open(tmp_path) as graph:
            count = _supports.claim_supports_count(graph._conn)
            assert count == 1
            upstream = _supports.walk_upstream(graph._conn, b, depth=1)
            assert any(u["claim_id"] == a for u in upstream)

    def test_restore_rebuilds_cache(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            graph.assert_claim("b", supports=[a])

        # Simulate catastrophic loss: drop graph.db + cache, restore from TOML.
        (tmp_path / ".mareforma" / "graph.db").unlink()
        (tmp_path / ".mareforma" / "claim_supports_cache.db").unlink()

        result = mareforma.restore(tmp_path)
        assert result["claims_restored"] == 2
        # Cache rebuilt with the restored chain.
        with mareforma.open(tmp_path) as graph:
            assert _supports.claim_supports_count(graph._conn) == 1


# ----------------------------------------------------------------------------
# query_provenance lineage shape
# ----------------------------------------------------------------------------


class TestQueryProvenance:
    def test_returns_focal_claim(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("alpha")
            lineage = graph.query_provenance(a)
        assert lineage["claim"]["claim_id"] == a
        assert lineage["claim"]["text"] == "alpha"
        assert lineage["depth"] == 4

    def test_upstream_chain_walked(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            b = graph.assert_claim("b", supports=[a])
            c = graph.assert_claim("c", supports=[b])
            lineage = graph.query_provenance(c)
        upstream_ids = {e["claim_id"] for e in lineage["upstream"]}
        assert a in upstream_ids
        assert b in upstream_ids
        # Each entry hydrated with the row.
        for entry in lineage["upstream"]:
            assert entry["row"] is not None
            assert entry["row"]["claim_id"] == entry["claim_id"]

    def test_downstream_chain_walked(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            b = graph.assert_claim("b", supports=[a])
            graph.assert_claim("c", supports=[b])
            lineage = graph.query_provenance(a)
        downstream_ids = {e["claim_id"] for e in lineage["downstream"]}
        # b and c are downstream of a.
        assert len(downstream_ids) == 2

    def test_role_attestations_in_lineage(self, tmp_path: Path) -> None:
        key_path = tmp_path / "asserter.key"
        _signing.save_private_key(_signing.generate_keypair(), key_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            a = graph.assert_claim("a")
            lineage = graph.query_provenance(a)
        attestations = lineage["claim"]["role_attestations"]
        # Single-sig envelope = one attestation, asserter's keyid present.
        assert len(attestations) == 1
        assert attestations[0]["keyid"]

    def test_unknown_claim_raises(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            with pytest.raises(_db.ClaimNotFoundError):
                graph.query_provenance("00000000-0000-4000-8000-000000000000")

    def test_depth_zero_returns_no_chain(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            b = graph.assert_claim("b", supports=[a])
            lineage = graph.query_provenance(b, depth=0)
        assert lineage["upstream"] == []
        assert lineage["downstream"] == []


# ----------------------------------------------------------------------------
# Multi-signature DSSE for claim-with-roles:v1
# ----------------------------------------------------------------------------


class TestMultiSigDSSE:
    def _statement_fields(self) -> dict:
        # The minimum claim_fields the canonicalizer accepts.
        return {
            "claim_id": "11111111-2222-4333-8444-555555555555",
            "text": "multi-sig claim text",
            "classification": "DERIVED",
            "support_level": "PRELIMINARY",
            "status": "open",
            "source_name": None,
            "generated_by": "compound-agent",
            "supports": [],
            "contradicts": [],
            "artifact_hash": None,
            "prev_hash": "0" * 64,
            "created_at": "2026-05-01T00:00:00+00:00",
        }

    def test_sign_with_two_roles_produces_two_signatures(self) -> None:
        planner = _signing.generate_keypair()
        executor = _signing.generate_keypair()
        envelope = _signing.sign_claim_with_roles(
            self._statement_fields(),
            [(planner, "planner"), (executor, "executor")],
        )
        assert len(envelope["signatures"]) == 2
        roles = {s["role"] for s in envelope["signatures"]}
        assert roles == {"planner", "executor"}

    def test_unknown_role_rejected(self) -> None:
        key = _signing.generate_keypair()
        with pytest.raises(ValueError, match="not one of"):
            _signing.sign_claim_with_roles(
                self._statement_fields(), [(key, "auditor")],
            )

    def test_duplicate_role_rejected(self) -> None:
        k1 = _signing.generate_keypair()
        k2 = _signing.generate_keypair()
        with pytest.raises(ValueError, match="duplicate role"):
            _signing.sign_claim_with_roles(
                self._statement_fields(),
                [(k1, "planner"), (k2, "planner")],
            )

    def test_empty_role_signers_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least one"):
            _signing.sign_claim_with_roles(self._statement_fields(), [])

    def test_verify_envelope_multi_happy(self) -> None:
        planner = _signing.generate_keypair()
        executor = _signing.generate_keypair()
        reviewer = _signing.generate_keypair()
        envelope = _signing.sign_claim_with_roles(
            self._statement_fields(),
            [(planner, "planner"), (executor, "executor"),
             (reviewer, "reviewer")],
        )
        assert _signing.verify_envelope_multi(envelope, {
            "planner": planner.public_key(),
            "executor": executor.public_key(),
            "reviewer": reviewer.public_key(),
        }) is True

    def test_verify_envelope_multi_wrong_key_fails(self) -> None:
        planner = _signing.generate_keypair()
        executor = _signing.generate_keypair()
        impostor = _signing.generate_keypair()
        envelope = _signing.sign_claim_with_roles(
            self._statement_fields(),
            [(planner, "planner"), (executor, "executor")],
        )
        # Wrong key for the executor role.
        assert _signing.verify_envelope_multi(envelope, {
            "planner": planner.public_key(),
            "executor": impostor.public_key(),
        }) is False

    def test_verify_envelope_multi_missing_role_key_fails(self) -> None:
        planner = _signing.generate_keypair()
        executor = _signing.generate_keypair()
        envelope = _signing.sign_claim_with_roles(
            self._statement_fields(),
            [(planner, "planner"), (executor, "executor")],
        )
        # Verifier omits the executor role's key.
        assert _signing.verify_envelope_multi(envelope, {
            "planner": planner.public_key(),
        }) is False

    def test_verify_envelope_multi_rejects_role_less_signature(self) -> None:
        # A legacy single-sig envelope (no role field) does NOT verify
        # under the multi-sig verifier — callers must use verify_envelope.
        key = _signing.generate_keypair()
        envelope = _signing.sign_claim(self._statement_fields(), key)
        # Forge a "claim-with-roles"-shaped call expecting role tags.
        assert _signing.verify_envelope_multi(envelope, {
            "planner": key.public_key(),
        }) is False


# ----------------------------------------------------------------------------
# REGRESSION: single-sig envelopes still verify under verify_envelope
# ----------------------------------------------------------------------------


class TestSingleSigBackwardsCompatible:
    """A legacy single-signature claim envelope must continue to verify
    under :func:`mareforma.signing.verify_envelope` after the multi-sig
    work landed. This is the cross-version compatibility guarantee.
    """

    def test_single_sig_claim_verifies(self, tmp_path: Path) -> None:
        key = _signing.generate_keypair()
        fields = {
            "claim_id": "22222222-3333-4444-8555-666666666666",
            "text": "legacy single-sig",
            "classification": "INFERRED",
            "support_level": "PRELIMINARY",
            "status": "open",
            "source_name": None,
            "generated_by": "agent",
            "supports": [],
            "contradicts": [],
            "artifact_hash": None,
            "prev_hash": "0" * 64,
            "created_at": "2026-05-01T00:00:00+00:00",
        }
        envelope = _signing.sign_claim(fields, key)
        assert _signing.verify_envelope(envelope, key.public_key()) is True

    def test_single_sig_envelope_still_loaded_and_round_tripped(
        self, tmp_path: Path,
    ) -> None:
        # End-to-end: assert a claim, restore the project from claims.toml,
        # confirm the round-tripped row still has a verifiable signature.
        key_path = tmp_path / "asserter.key"
        _signing.save_private_key(_signing.generate_keypair(), key_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            cid = graph.assert_claim("legacy claim")
            row = graph.get_claim(cid)
        # signature_bundle is a single-sig envelope.
        envelope = json.loads(row["signature_bundle"])
        assert len(envelope["signatures"]) == 1
        signer = _signing.load_private_key(key_path)
        assert _signing.verify_envelope(envelope, signer.public_key()) is True

        # Restore round-trip preserves the envelope, signature still valid.
        (tmp_path / ".mareforma" / "graph.db").unlink()
        (tmp_path / ".mareforma" / "claim_supports_cache.db").unlink()
        mareforma.restore(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            row = graph.get_claim(cid)
        envelope = json.loads(row["signature_bundle"])
        assert _signing.verify_envelope(envelope, signer.public_key()) is True


# ----------------------------------------------------------------------------
# Self-validation walks every signature on the envelope
# ----------------------------------------------------------------------------


class TestSelfValidationDefenseInDepth:
    def test_signer_keyids_lists_every_signature(self) -> None:
        k1 = _signing.generate_keypair()
        k2 = _signing.generate_keypair()
        fields = {
            "claim_id": "33333333-4444-4555-8666-777777777777",
            "text": "two-role claim",
            "classification": "DERIVED",
            "support_level": "PRELIMINARY",
            "status": "open",
            "source_name": None,
            "generated_by": "agent",
            "supports": [],
            "contradicts": [],
            "artifact_hash": None,
            "prev_hash": "0" * 64,
            "created_at": "2026-05-01T00:00:00+00:00",
        }
        envelope = _signing.sign_claim_with_roles(
            fields, [(k1, "planner"), (k2, "executor")],
        )
        bundle_json = json.dumps(envelope)
        keyids = _db._claim_signer_keyids(bundle_json)
        assert len(keyids) == 2

    def test_malformed_bundle_returns_empty(self) -> None:
        assert _db._claim_signer_keyids(None) == []
        assert _db._claim_signer_keyids("not-json") == []
        assert _db._claim_signer_keyids('{"signatures":"not-a-list"}') == []


# ----------------------------------------------------------------------------
# Self-verdict refusal walks every signature on both referenced claims
# ----------------------------------------------------------------------------


class TestSelfVerdictRefusal:
    def test_asserter_cannot_issue_verdict_on_own_claim(
        self, tmp_path: Path,
    ) -> None:
        key_path = tmp_path / "asserter.key"
        _signing.save_private_key(_signing.generate_keypair(), key_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            a = graph.assert_claim("a", classification="DERIVED")
            b = graph.assert_claim("b", classification="DERIVED")
            with pytest.raises(_db.VerdictIssuerError, match="self-verdicts"):
                graph.record_replication_verdict(
                    verdict_id="rv1",
                    cluster_id="cl1",
                    member_claim_id=a,
                    other_claim_id=b,
                    method="hash-match",
                    confidence={"x": 1},
                )

    def test_third_party_issuer_succeeds(self, tmp_path: Path) -> None:
        asserter = tmp_path / "asserter.key"
        issuer = tmp_path / "issuer.key"
        _signing.save_private_key(_signing.generate_keypair(), asserter)
        _signing.save_private_key(_signing.generate_keypair(), issuer)
        issuer_pem = _signing.public_key_to_pem(
            _signing.load_private_key(issuer).public_key(),
        )
        with mareforma.open(tmp_path, key_path=asserter) as graph:
            graph.enroll_validator(issuer_pem, identity="issuer")
            a = graph.assert_claim("a", classification="DERIVED")
            b = graph.assert_claim("b", classification="DERIVED")
        with mareforma.open(tmp_path, key_path=issuer) as graph:
            graph.record_replication_verdict(
                verdict_id="rv1",
                cluster_id="cl1",
                member_claim_id=a,
                other_claim_id=b,
                method="hash-match",
                confidence={"x": 1},
            )
        # No exception → success.


# ----------------------------------------------------------------------------
# PROV-O exporter
# ----------------------------------------------------------------------------


class TestProvOExport:
    def test_build_prov_o_minimal_graph(self, tmp_path: Path) -> None:
        from mareforma.exporters.prov_o import build_prov_o, validate_prov_o
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a", generated_by="agent-a")
            graph.assert_claim("b", supports=[a], generated_by="agent-b")
        doc = build_prov_o(tmp_path)
        validate_prov_o(doc)
        ids = {n["@id"] for n in doc["@graph"] if "@id" in n}
        # Entity, Activity, and Agent nodes present.
        assert any(":claim:" in i for i in ids)
        assert any(":activity:" in i for i in ids)
        assert any(":agent:" in i for i in ids)

    def test_wasderivedfrom_emitted_for_supports(self, tmp_path: Path) -> None:
        from mareforma.exporters.prov_o import build_prov_o
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a")
            b = graph.assert_claim("b", supports=[a])
        doc = build_prov_o(tmp_path)
        b_entity = next(
            n for n in doc["@graph"]
            if n.get("@id") == f"mareforma:claim:{b}"
        )
        derived = b_entity["prov:wasDerivedFrom"]
        if isinstance(derived, dict):
            derived = [derived]
        derived_ids = {d["@id"] for d in derived}
        assert f"mareforma:claim:{a}" in derived_ids

    def test_unsafe_agent_id_sanitised(self, tmp_path: Path) -> None:
        from mareforma.exporters.prov_o import build_prov_o
        with mareforma.open(tmp_path) as graph:
            graph.assert_claim("c", generated_by="evil#agent")
        doc = build_prov_o(tmp_path)
        ids = " ".join(n.get("@id", "") for n in doc["@graph"])
        assert "#" not in ids.replace("@id", "")

    def test_validate_catches_orphan_entity(self) -> None:
        from mareforma.exporters.prov_o import (
            validate_prov_o, ProvOValidationError,
        )
        doc = {
            "@context": {"prov": "http://www.w3.org/ns/prov#"},
            "@graph": [
                {"@id": "x", "@type": "prov:Entity"},  # no wasGeneratedBy
            ],
        }
        with pytest.raises(ProvOValidationError) as ei:
            validate_prov_o(doc)
        assert ei.value.invariant == "entity-needs-activity"

    def test_validate_catches_activity_missing_agent(self) -> None:
        from mareforma.exporters.prov_o import (
            validate_prov_o, ProvOValidationError,
        )
        doc = {
            "@context": {"prov": "http://www.w3.org/ns/prov#"},
            "@graph": [
                {"@id": "a", "@type": "prov:Activity"},
            ],
        }
        with pytest.raises(ProvOValidationError) as ei:
            validate_prov_o(doc)
        assert ei.value.invariant == "activity-needs-agent"

    def test_focal_claim_walks_ancestors(self, tmp_path: Path) -> None:
        from mareforma.exporters.prov_o import build_prov_o
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("root")
            b = graph.assert_claim("middle", supports=[a])
            c = graph.assert_claim("leaf", supports=[b])
            graph.assert_claim("unrelated")
        doc = build_prov_o(tmp_path, claim_id=c)
        claim_ids = {
            n["@id"] for n in doc["@graph"]
            if n.get("@type") == "prov:Entity"
        }
        assert f"mareforma:claim:{a}" in claim_ids
        assert f"mareforma:claim:{b}" in claim_ids
        assert f"mareforma:claim:{c}" in claim_ids

    def test_missing_graph_db_raises(self, tmp_path: Path) -> None:
        from mareforma.exporters.prov_o import build_prov_o
        with pytest.raises(FileNotFoundError):
            build_prov_o(tmp_path)

    def test_empty_graph_produces_valid_doc(self, tmp_path: Path) -> None:
        from mareforma.exporters.prov_o import build_prov_o, validate_prov_o
        with mareforma.open(tmp_path):
            pass
        doc = build_prov_o(tmp_path)
        # Empty graph → empty @graph list, still well-formed.
        validate_prov_o(doc)
        assert doc["@graph"] == []
