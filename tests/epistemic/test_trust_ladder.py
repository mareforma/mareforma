"""
tests/epistemic/test_trust_ladder.py — Honesty tests for trust ladder boundaries.

All tests here PASS by design. They document what the system permits — not
what it prevents. The same posture as test_spurious_replicated in
test_support_levels.py: make the known limitations explicit and visible.

Scenarios covered
-----------------
  validated_by is a cosmetic label
    - The validator's authenticated identity is the keyid embedded in the
      signed validation envelope; ``validated_by`` is a free-form display
      string and ``validate()`` stores whatever the caller passes.

  Local-key trust footprint
    - Anyone with read access to the project's signing key can act as the
      enrolled root validator. Mareforma is local-trust, not cross-org PKI.

  Self-supporting claim
    - Updating a claim to include its own claim_id in supports[] is
      rejected with CycleDetectedError.

  Cyclic supports
    - A supports B, B supports A — the second edge is rejected with
      CycleDetectedError on update_claim.

  Contradicting and supporting the same claim
    - A claim can list the same claim_id in both supports and contradicts.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import mareforma
from mareforma.db import open_db, add_claim, update_claim


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def open_graph(tmp_path: Path):
    """Open with a bootstrapped key so seed=True works."""
    from mareforma import signing as _signing
    key_path = tmp_path / "_test_key"
    if not key_path.exists():
        _signing.bootstrap_key(key_path)
    return mareforma.open(tmp_path, key_path=key_path)


def open_signed_graph(tmp_path: Path):
    """Open a graph with a bootstrapped signing key (auto-enrolled as root)."""
    from mareforma import signing as _signing
    key_path = tmp_path / "mareforma.key"
    _signing.bootstrap_key(key_path)
    return mareforma.open(tmp_path, key_path=key_path)


def _bootstrap_validator_key(tmp_path: Path) -> Path:
    """Bootstrap a second signing key — the substrate refuses self-validation,
    so promoting a REPLICATED claim needs a key distinct from the signer."""
    from mareforma import signing as _signing
    key_path = tmp_path / "validator.key"
    if not key_path.exists():
        _signing.bootstrap_key(key_path)
    return key_path


def _validator_pubkey_pem(key_path: Path) -> bytes:
    from mareforma import signing as _signing
    return _signing.public_key_to_pem(
        _signing.load_private_key(key_path).public_key(),
    )


# ---------------------------------------------------------------------------
# validated_by is a cosmetic label (the keyid in validation_signature is truth)
# ---------------------------------------------------------------------------

class TestTrustLaundering:
    def test_validated_by_is_a_cosmetic_label(self, tmp_path: Path) -> None:
        """validate() requires an enrolled validator (the loaded key auto-
        enrolls as root on first open), but the ``validated_by`` parameter
        is a free-form display string — the authenticated identity is the
        keyid embedded in the signed validation envelope.

        A caller can pass any label they like; downstream consumers MUST
        check ``validation_signature``'s keyid against the validators
        table if they want to know who actually validated.
        """
        validator_key_path = _bootstrap_validator_key(tmp_path)
        with open_signed_graph(tmp_path) as g:
            upstream = g.assert_claim("upstream reference", generated_by="seed", seed=True)
            id_a = g.assert_claim(
                "Drug X causes effect Y",
                supports=[upstream],
                generated_by="agent-A",
            )
            g.assert_claim(
                "Drug X causes effect Y",
                supports=[upstream],
                generated_by="agent-B",
            )
            claim = g.get_claim(id_a)
            assert claim["support_level"] == "REPLICATED"
            g.enroll_validator(
                _validator_pubkey_pem(validator_key_path), identity="v",
            )

        # The label "attacker@example.org" is stored verbatim in
        # validated_by, but the validator's real keyid lives in
        # validation_signature.
        with mareforma.open(tmp_path, key_path=validator_key_path) as g:
            g.validate(id_a, validated_by="attacker@example.org")

            claim = g.get_claim(id_a)
            assert claim["support_level"] == "ESTABLISHED"
            assert claim["validated_by"] == "attacker@example.org"
            assert claim["validation_signature"] is not None

    def test_validate_accepts_any_validated_by_string(self, tmp_path: Path) -> None:
        """validate() stores whatever display string is passed."""
        validator_key_path = _bootstrap_validator_key(tmp_path)
        with open_signed_graph(tmp_path) as g:
            upstream = g.assert_claim("prior", generated_by="seed", seed=True)
            rep_id = g.assert_claim("finding", supports=[upstream], generated_by="A")
            g.assert_claim("finding", supports=[upstream], generated_by="B")
            g.enroll_validator(
                _validator_pubkey_pem(validator_key_path), identity="v",
            )

        with mareforma.open(tmp_path, key_path=validator_key_path) as g:
            g.validate(rep_id, validated_by="NOT_A_REAL_PERSON_12345")

            claim = g.get_claim(rep_id)
            assert claim["validated_by"] == "NOT_A_REAL_PERSON_12345"
            assert claim["support_level"] == "ESTABLISHED"


# ---------------------------------------------------------------------------
# Self-supporting claim
# ---------------------------------------------------------------------------

class TestSelfSupport:
    def test_self_supporting_claim_via_update_rejected(self, tmp_path: Path) -> None:
        """A claim cannot be updated to include its own claim_id in
        ``supports[]``.

        Cycle detection on ``update_claim`` rejects the self-loop.
        Provenance chains are required to be acyclic.
        """
        from mareforma.db import CycleDetectedError
        conn = open_db(tmp_path)
        claim_id = add_claim(conn, tmp_path, "Self-referencing finding")
        with pytest.raises(CycleDetectedError, match="self-loop"):
            update_claim(conn, tmp_path, claim_id, supports=[claim_id])
        conn.close()

        with open_graph(tmp_path) as g:
            claim = g.get_claim(claim_id)
            supports = json.loads(claim["supports_json"])
        assert claim_id not in supports


# ---------------------------------------------------------------------------
# Cyclic supports
# ---------------------------------------------------------------------------

class TestCyclicSupports:
    def test_cyclic_supports_rejected(self, tmp_path: Path) -> None:
        """A supports B, B supports A — the second edge is rejected.

        Cycle detection on ``update_claim`` walks the supports[] graph
        forward from the proposed edge and rejects any closure back to
        the updated claim.

        Uses unsigned graph directly (not the keyed open_graph fixture)
        because the cycle test exercises the unsigned-edit window —
        signed claims refuse supports[] mutation upstream of cycle
        detection (SignedClaimImmutableError fires first).
        """
        from mareforma.db import CycleDetectedError
        conn = open_db(tmp_path)
        try:
            id_a = add_claim(conn, tmp_path, "Claim A", generated_by="agent-1")
            id_b = add_claim(conn, tmp_path, "Claim B", supports=[id_a], generated_by="agent-2")
            with pytest.raises(CycleDetectedError, match="cycle"):
                update_claim(conn, tmp_path, id_a, supports=[id_b])
        finally:
            conn.close()

        conn = open_db(tmp_path)
        try:
            a = dict(conn.execute(
                "SELECT * FROM claims WHERE claim_id = ?", (id_a,),
            ).fetchone())
            b = dict(conn.execute(
                "SELECT * FROM claims WHERE claim_id = ?", (id_b,),
            ).fetchone())
        finally:
            conn.close()

        # a.supports unchanged; b.supports still contains id_a.
        assert id_b not in json.loads(a["supports_json"])
        assert id_a in json.loads(b["supports_json"])


# ---------------------------------------------------------------------------
# Contradicting and supporting the same claim
# ---------------------------------------------------------------------------

class TestContradictAndSupport:
    def test_contradicting_and_supporting_same_claim_accepted(
        self, tmp_path: Path
    ) -> None:
        """A claim can list the same upstream in both supports and contradicts.

        No validation prevents this. The result is logically contradictory
        provenance, but the graph accepts it without error.
        """
        with open_graph(tmp_path) as g:
            upstream = g.assert_claim("upstream", generated_by="seed")
            claim_id = g.assert_claim(
                "Contradictory claim",
                supports=[upstream],
                contradicts=[upstream],
                generated_by="agent-X",
            )
            claim = g.get_claim(claim_id)

        assert upstream in json.loads(claim["supports_json"])
        assert upstream in json.loads(claim["contradicts_json"])


# ---------------------------------------------------------------------------
# Launch ship-gate: substrate end-to-end story
# ---------------------------------------------------------------------------
#
# These tests are the OSS substrate's ship gate. They exercise the
# complete v0.3.0 launch story end-to-end:
#
#   - in-toto Statement v1 + DSSE envelope on every signed claim
#   - GRADE EvidenceVector inside the signed predicate
#   - Verdict-issuer protocol: signed verdicts from enrolled validators
#     promote claims to REPLICATED and invalidate via t_invalid
#   - Restore round-trips claims + validators + verdicts
#
# The launch story DOES NOT include the inference layer (embedder, NLI,
# semantic-cluster predicate). Those live outside the OSS substrate. Any
# external verdict-issuer calls the verdict-issuer protocol below; the
# OSS substrate accepts the signed verdicts and gates the trust ladder
# accordingly.


class TestLaunchSubstrateShipGate:
    """The substrate's launch ship gate. If everything here passes,
    the substrate is launch-ready. Deeper coverage of each piece
    lives in test_signing*.py, test_evidence.py, test_canonical.py,
    test_statement.py, test_verdict_issuer.py, test_restore.py,
    test_reputation.py, test_validator_type.py, test_search_fts5.py.
    """

    def test_signed_claim_carries_in_toto_statement_v1_envelope(
        self, tmp_path: Path,
    ) -> None:
        """Every signed claim's envelope is a DSSE v1 wrapping an
        in-toto Statement v1 with predicateType
        https://mareforma.dev/claim/v1. Subject digest binds the
        text; predicate carries SIGNED_FIELDS + EvidenceVector."""
        from mareforma import signing as _signing
        with open_signed_graph(tmp_path) as g:
            cid = g.assert_claim("anchor finding")
            claim = g.get_claim(cid)
        envelope = json.loads(claim["signature_bundle"])
        assert envelope["payloadType"] == "application/vnd.in-toto+json"
        predicate = _signing.claim_predicate_from_envelope(envelope)
        assert predicate["claim_id"] == cid
        assert predicate["text"] == "anchor finding"
        assert "evidence" in predicate

    def test_grade_evidence_vector_on_every_claim(
        self, tmp_path: Path,
    ) -> None:
        """Every claim row carries an ev_* + evidence_json column set.
        The signed predicate binds them; restore catches tampering."""
        with open_signed_graph(tmp_path) as g:
            cid = g.assert_claim("evidence-bearing claim")
            row = g.get_claim(cid)
        for col in (
            "ev_risk_of_bias", "ev_inconsistency", "ev_indirectness",
            "ev_imprecision", "ev_pub_bias",
        ):
            assert col in row
            assert row[col] == 0  # default
        ev = json.loads(row["evidence_json"])
        assert "rationale" in ev
        assert "reporting_compliance" in ev

    def test_verdict_issuer_promotes_to_replicated_end_to_end(
        self, tmp_path: Path,
    ) -> None:
        """An enrolled validator's signed replication verdict promotes
        the referenced pair of claims from PRELIMINARY to REPLICATED.
        The OSS substrate accepts the verdict; the predicate that
        generates it (semantic-cluster, cross-method, hash-match,
        shared-resolved-upstream) lives outside the OSS."""
        from mareforma import signing as _signing
        root_key = tmp_path / "root.key"
        issuer_key = tmp_path / "issuer.key"
        _signing.bootstrap_key(root_key)
        _signing.bootstrap_key(issuer_key)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            pem = _signing.public_key_to_pem(
                _signing.load_private_key(issuer_key).public_key(),
            )
            g.enroll_validator(pem, identity="external-issuer")
            a = g.assert_claim("alpha finding", generated_by="lab-A")
            b = g.assert_claim("beta finding", generated_by="lab-B")
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_launch",
                cluster_id="cl_launch",
                member_claim_id=a, other_claim_id=b,
                method="semantic-cluster",
                confidence={"cosine": 0.94, "nli_forward": 0.87,
                            "nli_backward": 0.89},
            )
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"

    def test_contradiction_verdict_invalidates_and_query_default_excludes(
        self, tmp_path: Path,
    ) -> None:
        """A signed contradiction verdict from an enrolled validator
        invalidates the older referenced claim. Default query mode
        excludes invalidated claims; audit mode surfaces them."""
        from mareforma import signing as _signing
        root_key = tmp_path / "root.key"
        issuer_key = tmp_path / "issuer.key"
        _signing.bootstrap_key(root_key)
        _signing.bootstrap_key(issuer_key)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            pem = _signing.public_key_to_pem(
                _signing.load_private_key(issuer_key).public_key(),
            )
            g.enroll_validator(pem, identity="external-issuer")
            a = g.assert_claim("alpha finding", generated_by="lab-A")
            b = g.assert_claim("beta finding", generated_by="lab-B")
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_launch",
                member_claim_id=a, other_claim_id=b,
                confidence={"stance_forward": "refutes",
                            "stance_backward": "refutes"},
            )
        with mareforma.open(tmp_path, key_path=root_key) as g:
            visible_ids = {
                r["claim_id"]
                for r in g.query(include_unverified=True)
            }
            audit_ids = {
                r["claim_id"]
                for r in g.query(include_unverified=True,
                                 include_invalidated=True)
            }
        assert a not in visible_ids  # invalidated by signed verdict
        assert a in audit_ids
        assert b in visible_ids  # still valid

    def test_restore_round_trips_claims_validators_and_verdicts(
        self, tmp_path: Path,
    ) -> None:
        """Substrate restore reconstructs the full graph from claims.toml:
        validators, claims (with EvidenceVector + statement_cid),
        replication verdicts, contradiction verdicts. The trigger
        re-derives t_invalid from the replayed contradictions."""
        from mareforma import signing as _signing
        root_key = tmp_path / "root.key"
        issuer_key = tmp_path / "issuer.key"
        _signing.bootstrap_key(root_key)
        _signing.bootstrap_key(issuer_key)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            pem = _signing.public_key_to_pem(
                _signing.load_private_key(issuer_key).public_key(),
            )
            g.enroll_validator(pem, identity="external-issuer")
            a = g.assert_claim("alpha", generated_by="lab-A")
            b = g.assert_claim("beta", generated_by="lab-B")
            c = g.assert_claim("gamma", generated_by="lab-C")
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_e2e", cluster_id="cl",
                member_claim_id=a, other_claim_id=b,
                method="semantic-cluster", confidence={"cosine": 0.91},
            )
            g.record_contradiction_verdict(
                verdict_id="cv_e2e",
                member_claim_id=a, other_claim_id=c,
                confidence={"stance": "refutes"},
            )
        # Wipe graph.db; restore from claims.toml.
        for fname in ("graph.db", "graph.db-wal", "graph.db-shm"):
            p = tmp_path / ".mareforma" / fname
            if p.exists():
                p.unlink()
        result = mareforma.restore(tmp_path)
        assert result["claims_restored"] == 3
        assert result["validators_restored"] >= 2
        with mareforma.open(tmp_path, key_path=root_key) as g:
            # b promoted via the replication verdict.
            assert g.get_claim(b)["support_level"] == "REPLICATED"
            # a was the older of (a, c), so the trigger invalidated it.
            assert g.get_claim(a)["t_invalid"] is not None
            # The replication and contradiction verdicts both round-tripped.
            reps = g.replication_verdicts(include_invalidated=True)
            cons = g.contradiction_verdicts(include_invalidated=True)
            assert {v["verdict_id"] for v in reps} == {"rv_e2e"}
            assert {v["verdict_id"] for v in cons} == {"cv_e2e"}
