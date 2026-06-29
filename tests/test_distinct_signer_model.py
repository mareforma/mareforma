"""New-behaviour tests for the v0.3.7 distinct-signer trust model.

These cover the model change directly (not migrated from older expectations):

  * REPLICATED keys on two distinct, non-NULL ``asserter_keyid`` values sharing
    an ESTABLISHED+open anchor — not on distinct ``generated_by``.
  * artifact_hash is an EQUAL-data COLLAPSE, not a convergence reward.
  * data_id content-addressing collapses byte-identical reruns.
  * the ESTABLISHED boundary refuses a validator that asserted any claim in
    the converging set.
  * trust-layer counting agrees with promotion on the asserter_keyid axis,
    with the legacy NULL-keyid generated_by fallback preserved.
  * verify-on-read excludes forged high-trust rows from ``query`` and flags
    them ``verified=False`` in ``get_claim`` without raising.
  * single_trust_domain disclosure on a solo-operator ESTABLISHED row.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

import mareforma
from mareforma import signing as _signing
from mareforma.trust import _store
from tests._helpers import _bootstrap_key, _pem_of, _two_signers


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_path(tmp_path: Path) -> Path:
    return tmp_path / ".mareforma" / "graph.db"


def _open_root_graph(tmp_path: Path):
    """Open a graph whose loaded key auto-enrolls as the root validator."""
    key_path = _bootstrap_key(tmp_path, "root.key")
    return mareforma.open(tmp_path, key_path=key_path), key_path


# ===========================================================================
# REPLICATED promotion keys on distinct asserter_keyid
# ===========================================================================

class TestReplicatedKeysOnSigner:
    def test_distinct_signers_shared_anchor_promote(self, tmp_path: Path) -> None:
        sa, sb = _two_signers(tmp_path)
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            b = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"

    def test_same_signer_does_not_promote(self, tmp_path: Path) -> None:
        sa, _ = _two_signers(tmp_path)
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            # Distinct generated_by but the SAME signer -> same asserter_keyid.
            a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            b = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sa)
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"

    def test_unsigned_peer_does_not_promote(self, tmp_path: Path) -> None:
        """A signed claim converging with an unsigned (NULL keyid) peer does
        not promote: one of the two asserters is NULL."""
        sa, _ = _two_signers(tmp_path)
        # Open with NO loaded key so the second claim is unsigned (NULL keyid),
        # but seed needs a key — so build the seed in a signed handle first.
        key_path = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key_path) as g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
        # Reopen WITHOUT a key: the next claim is unsigned (NULL asserter_keyid).
        with mareforma.open(tmp_path) as g:
            b = g.assert_claim("B", supports=[up], generated_by="lab_b")
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"

    def test_two_null_peers_are_not_distinct_signers(self, tmp_path: Path) -> None:
        """Two unsigned (NULL keyid) peers are NOT two distinct signers — the
        legacy guard: NULL != NULL for convergence purposes."""
        # Seed must be ESTABLISHED, which needs a signed seed. Build it signed,
        # then write both converging peers unsigned.
        key_path = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key_path) as g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
        with mareforma.open(tmp_path) as g:
            a = g.assert_claim("A", supports=[up], generated_by="lab_a")
            b = g.assert_claim("B", supports=[up], generated_by="lab_b")
            assert g.get_claim(a)["asserter_keyid"] is None
            assert g.get_claim(b)["asserter_keyid"] is None
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"


# ===========================================================================
# artifact_hash: equal-data collapse (inverted from old convergence reward)
# ===========================================================================

class TestArtifactHashCollapse:
    def test_equal_hash_collapses_no_promote(self, tmp_path: Path) -> None:
        """Two distinct-signer peers that BOTH supply an EQUAL non-NULL
        artifact_hash are the same output and collapse — they do NOT promote."""
        sa, sb = _two_signers(tmp_path)
        h = hashlib.sha256(b"same-artifact").hexdigest()
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim(
                "A", supports=[up], generated_by="lab_a", signer=sa, artifact_hash=h,
            )
            b = g.assert_claim(
                "B", supports=[up], generated_by="lab_b", signer=sb, artifact_hash=h,
            )
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"

    def test_distinct_hash_does_not_block_promotion(self, tmp_path: Path) -> None:
        sa, sb = _two_signers(tmp_path)
        ha = hashlib.sha256(b"artifact-a").hexdigest()
        hb = hashlib.sha256(b"artifact-b").hexdigest()
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim(
                "A", supports=[up], generated_by="lab_a", signer=sa, artifact_hash=ha,
            )
            b = g.assert_claim(
                "B", supports=[up], generated_by="lab_b", signer=sb, artifact_hash=hb,
            )
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"

    def test_absent_hash_does_not_block_promotion(self, tmp_path: Path) -> None:
        sa, sb = _two_signers(tmp_path)
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            b = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            # No artifact_hash on either side: distinct signers still promote.
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"

    def test_double_null_hash_promotes_only_via_distinct_signer(
        self, tmp_path: Path,
    ) -> None:
        """Two absent (NULL) artifact hashes do not promote "on hash alone" —
        promotion only ever fires via two distinct signers. Same signer +
        absent hashes -> no promote."""
        sa, _ = _two_signers(tmp_path)
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            b = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sa)
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"


# ===========================================================================
# ESTABLISHED boundary: validator cannot equal any converging asserter
# ===========================================================================

class TestEstablishedBoundary:
    def test_validator_equal_to_asserter_refused(self, tmp_path: Path) -> None:
        from mareforma.db import SelfValidationError

        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        # Enroll sa and sb as validators so a self-validation attempt is gated
        # by the converging-set check (not merely the not-enrolled check).
        pem_a = _signing.public_key_to_pem(sa.public_key())
        pem_b = _signing.public_key_to_pem(sb.public_key())
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(pem_a, identity="a")
            g.enroll_validator(pem_b, identity="b")
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            assert g.get_claim(rep)["support_level"] == "REPLICATED"

        # sb asserted a peer in the converging set behind `rep` — it cannot
        # witness its own convergence into ESTABLISHED.
        sb_key = tmp_path / "_signer_b.key"
        with mareforma.open(tmp_path, key_path=sb_key) as g:
            with pytest.raises(SelfValidationError):
                g.validate(rep)

    def test_independent_validator_promotes(self, tmp_path: Path) -> None:
        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        val_key = _bootstrap_key(tmp_path, "val.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(_pem_of(val_key), identity="v")
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.validate(rep)
            assert g.get_claim(rep)["support_level"] == "ESTABLISHED"


# ===========================================================================
# Trust-layer counting agrees with promotion on the asserter_keyid axis
# ===========================================================================

def _prop():
    from mareforma.trust import Direction, Proposition
    return Proposition(
        subject="BRCA1", relation="affects", object="tumour growth",
        direction=Direction.DECREASES,
        scope={"population": "TNBC", "condition": "in vitro"},
    )


def _pred():
    from mareforma.trust import DirectionOfInterest, Prediction, TestType
    return Prediction(
        TestType.SUPERIORITY,
        direction_of_interest=DirectionOfInterest.DECREASE,
        alpha=0.05,
    )


def _est():
    from mareforma.trust import EffectEstimate, EffectType
    return EffectEstimate(-0.8, EffectType.SMD, p_value=0.001)


class TestTrustCounting:
    def test_same_signer_findings_count_as_one(self, tmp_path: Path) -> None:
        """Two findings written through ONE graph handle share one signer and
        count as a single independent support (not CORROBORATED)."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1")
            g.assert_finding(prop, pred, _est(), data_id="ds2", generated_by="run2")
            status = g.proposition_status(prop.content_id())
        # One distinct signer -> at most one independent support.
        assert status["independent_support"] == 1
        assert status["status_policy"] == "status_policy@v3"

    def test_distinct_signer_findings_corroborate(self, tmp_path: Path) -> None:
        """Each finding written through a graph handle opened with a DISTINCT
        key carries a distinct signer -> two independent supports."""
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

    def test_legacy_null_keyid_findings_count_under_generated_by(
        self, tmp_path: Path,
    ) -> None:
        """Unsigned findings (NULL asserter_keyid) fall back to the generated_by
        axis: two with distinct generated_by + distinct data_id still
        corroborate — no silent CORROBORATED downgrade."""
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path) as g:  # no key -> unsigned findings
            g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1")
            g.assert_finding(prop, pred, _est(), data_id="ds2", generated_by="run2")
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 2
        assert status["status"] == "CORROBORATED"


# ===========================================================================
# data_id content-addressing
# ===========================================================================

class TestContentAddressing:
    def test_equal_bytes_collapse(self, tmp_path: Path) -> None:
        """Equal dataset bytes content-address to the same data_id -> the
        independence guard collapses them to one unit."""
        prop, pred = _prop(), _pred()
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_bytes=b"identical", generated_by="run1",
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            # Same bytes -> same content-addressed data_id -> idempotent reuse,
            # collapses to ONE unit even across two distinct signers.
            g.assert_finding(
                prop, pred, _est(), data_bytes=b"identical", generated_by="run2",
            )
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 1

    def test_distinct_bytes_count_with_distinct_signers(self, tmp_path: Path) -> None:
        prop, pred = _prop(), _pred()
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_bytes=b"dataset-a", generated_by="run1",
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_bytes=b"dataset-b", generated_by="run2",
            )
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 2

    def test_data_id_and_data_bytes_mutually_exclusive(self, tmp_path: Path) -> None:
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=_bootstrap_key(tmp_path)) as g:
            with pytest.raises(ValueError, match="data_id|data_bytes|both"):
                g.assert_finding(
                    prop, pred, _est(), data_id="x", data_bytes=b"y",
                    generated_by="run1",
                )

    def test_content_address_helpers(self) -> None:
        did = _store.content_address_data_id(b"hello")
        assert did == "sha256:" + hashlib.sha256(b"hello").hexdigest()
        assert _store.is_content_addressed(did)
        assert not _store.is_content_addressed("plain-string")


# ===========================================================================
# verify-on-read
# ===========================================================================

def _build_established(tmp_path: Path):
    """Build an ESTABLISHED claim; return (root_key, val_key, rep_id, peer_id)."""
    sa, sb = _two_signers(tmp_path)
    root_key = _bootstrap_key(tmp_path, "root.key")
    val_key = _bootstrap_key(tmp_path, "val.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.enroll_validator(_pem_of(val_key), identity="v")
        up = g.assert_claim("anchor", generated_by="seed", seed=True)
        rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
        peer = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
    with mareforma.open(tmp_path, key_path=val_key) as g:
        g.validate(rep)
    return root_key, val_key, rep, peer


class TestVerifyOnRead:
    def test_tampered_established_excluded_and_flagged(self, tmp_path: Path) -> None:
        root_key, _, rep, _ = _build_established(tmp_path)
        # Forge: corrupt the validation_signature directly in sqlite.
        conn = sqlite3.connect(_db_path(tmp_path))
        try:
            conn.execute(
                "UPDATE claims SET validation_signature = ? WHERE claim_id = ?",
                ('{"payloadType":"forged","payload":"x","signatures":[]}', rep),
            )
            conn.commit()
        finally:
            conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            # get_claim never raises; flags verified=False.
            c = g.get_claim(rep)
            assert c["verified"] is False
            # query excludes the forged high-trust row.
            ids = {r["claim_id"] for r in g.query(min_support="ESTABLISHED", limit=99)}
            assert rep not in ids

    def test_legacy_unsigned_replicated_is_verify_exempt(self, tmp_path: Path) -> None:
        """A REPLICATED row whose asserter is not enrolled (no pubkey to check)
        is verify-exempt: returned as-is, never falsely excluded."""
        sa, sb = _two_signers(tmp_path)  # NOT enrolled as validators
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            c = g.get_claim(rep)
            assert c["support_level"] == "REPLICATED"
            assert c["verified"] is True
            ids = {r["claim_id"] for r in g.query(min_support="REPLICATED", limit=99)}
            assert rep in ids

    def test_tampered_enrolled_asserter_bundle_excluded(self, tmp_path: Path) -> None:
        """A tampered participant bundle on a REPLICATED row whose asserter IS an
        enrolled validator is excluded from query."""
        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        pem_a = _signing.public_key_to_pem(sa.public_key())
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(pem_a, identity="a")  # sa is now enrolled
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            assert g.get_claim(rep)["support_level"] == "REPLICATED"

        # Tamper the asserter bundle's signature bytes.
        conn = sqlite3.connect(_db_path(tmp_path))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT signature_bundle FROM claims WHERE claim_id = ?", (rep,),
            ).fetchone()
            bundle = json.loads(row["signature_bundle"])
            import base64
            sig = bytearray(base64.standard_b64decode(bundle["signatures"][0]["sig"]))
            sig[0] ^= 0xFF
            bundle["signatures"][0]["sig"] = base64.standard_b64encode(
                bytes(sig)
            ).decode("ascii")
            conn.execute(
                "UPDATE claims SET signature_bundle = ? WHERE claim_id = ?",
                (json.dumps(bundle, sort_keys=True, separators=(",", ":")), rep),
            )
            conn.commit()
        finally:
            conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            c = g.get_claim(rep)
            assert c["verified"] is False
            ids = {r["claim_id"] for r in g.query(min_support="REPLICATED", limit=99)}
            assert rep not in ids


# ===========================================================================
# single_trust_domain disclosure
# ===========================================================================

class TestSingleTrustDomain:
    def test_solo_operator_established_row_discloses_single_domain(
        self, tmp_path: Path,
    ) -> None:
        root_key, _, rep, _ = _build_established(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            c = g.get_claim(rep)
        assert c["support_level"] == "ESTABLISHED"
        assert c["single_trust_domain"] is True
        assert c["trust_domain_root"] is not None

    def test_export_bundle_predicate(self, tmp_path: Path) -> None:
        """The export bundle carries the mare:singleTrustDomain predicate on the
        ESTABLISHED row."""
        from mareforma import export_bundle as _eb

        _build_established(tmp_path)
        statement = _eb.build_statement(tmp_path)
        assert statement["predicate"]["mare:singleTrustDomain"] is True
        assert statement["predicate"]["mare:trustDomainRoot"] is not None

    def test_validators_module_predicates(self, tmp_path: Path) -> None:
        from mareforma import validators as _validators

        root_key, _, _, _ = _build_established(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert _validators.single_trust_domain(g._conn) is True
            assert _validators.trust_domain_root(g._conn) is not None
            assert len(_validators.enrollment_roots(g._conn)) == 1
