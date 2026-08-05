"""tests/test_established_upstream.py, ESTABLISHED-upstream gate + seed.

Covers:
  - REPLICATED requires an ESTABLISHED upstream (strict by default)
  - seed=True creates a directly-ESTABLISHED claim with a signed envelope
  - seed=True requires a loaded signer
  - seed=True refused for unenrolled keys
  - Seed envelope round-trip verify
  - Cross-type substitution refused (seed envelope ≠ validation envelope)
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

import mareforma
from mareforma import signing as _signing


def _key(tmp_path: Path) -> Path:
    key_path = tmp_path / "_root_key"
    if not key_path.exists():
        _signing.bootstrap_key(key_path)
    return key_path


def _validator_key(tmp_path: Path) -> Path:
    """A second key for validation, the graph refuses self-validation,
    so promotion tests need a key distinct from the one signing claims."""
    key_path = tmp_path / "_validator_key"
    if not key_path.exists():
        _signing.bootstrap_key(key_path)
    return key_path


def _validator_pem(tmp_path: Path) -> bytes:
    return _signing.public_key_to_pem(
        _signing.load_private_key(_validator_key(tmp_path)).public_key(),
    )


# ---------------------------------------------------------------------------
# Strict ESTABLISHED-upstream rule
# ---------------------------------------------------------------------------


class TestEstablishedUpstreamRule:
    def test_replicated_blocked_with_only_preliminary_upstream(
        self, tmp_path: Path,
    ) -> None:
        """Distinct signers, so the anchor's support_level is the only thing
        holding the pair at PRELIMINARY."""
        from tests._helpers import _two_signers
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            up = g.assert_claim("upstream", generated_by="seed")  # NO seed=True
            assert g.get_claim(up)["support_level"] == "PRELIMINARY"
            a = g.assert_claim("a", supports=[up], generated_by="A", signer=sa)
            b = g.assert_claim("b", supports=[up], generated_by="B", signer=sb)
            # Without seed=True the upstream is PRELIMINARY → REPLICATED gate
            # does not fire.
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"

    def test_replicated_fires_when_upstream_is_established(
        self, tmp_path: Path,
    ) -> None:
        from tests._helpers import _two_signers
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            up = g.assert_claim("seeded", generated_by="seed", seed=True)
            assert g.get_claim(up)["support_level"] == "ESTABLISHED"
            a = g.assert_claim("a", supports=[up], generated_by="A", signer=sa)
            b = g.assert_claim("b", supports=[up], generated_by="B", signer=sb)
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"

    def test_at_least_one_established_upstream_is_enough(
        self, tmp_path: Path,
    ) -> None:
        """Multiple upstreams; only ONE needs to be ESTABLISHED."""
        from tests._helpers import _two_signers
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            estd = g.assert_claim("seeded", generated_by="seed", seed=True)
            prelim = g.assert_claim("plain upstream", generated_by="seed")
            a = g.assert_claim(
                "a", supports=[estd, prelim], generated_by="A", signer=sa,
            )
            b = g.assert_claim(
                "b", supports=[estd, prelim], generated_by="B", signer=sb,
            )
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"


# ---------------------------------------------------------------------------
# Seed-claim bootstrap
# ---------------------------------------------------------------------------


class TestSeedClaimBootstrap:
    def test_seed_inserts_directly_as_established(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            cid = g.assert_claim("genesis", generated_by="seed", seed=True)
            row = g.get_claim(cid)
        assert row["support_level"] == "ESTABLISHED"
        assert row["validation_signature"] is not None
        assert row["validated_at"] is not None

    def test_seed_requires_loaded_signer(self, tmp_path: Path) -> None:
        # absent key path → no signer loaded → seed=True refused
        with mareforma.open(tmp_path, key_path=tmp_path / "absent") as g:
            with pytest.raises(ValueError, match="signing key"):
                g.assert_claim("would-be seed", seed=True)

    def test_seed_envelope_verifies(self, tmp_path: Path) -> None:
        key_path = _key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim("genesis", generated_by="seed", seed=True)
            row = g.get_claim(cid)
        envelope = json.loads(row["validation_signature"])
        assert envelope["payloadType"] == _signing.PAYLOAD_TYPE_SEED
        pub = _signing.load_private_key(key_path).public_key()
        assert _signing.verify_envelope(
            envelope, pub,
            expected_payload_type=_signing.PAYLOAD_TYPE_SEED,
        )

    def test_seed_envelope_distinct_from_validation_payload_type(
        self, tmp_path: Path,
    ) -> None:
        """A seed envelope must NOT verify as a validation envelope
        (cross-type substitution defense)."""
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            cid = g.assert_claim("genesis", generated_by="seed", seed=True)
            row = g.get_claim(cid)
        envelope = json.loads(row["validation_signature"])
        pub = _signing.load_private_key(_key(tmp_path)).public_key()
        # Expecting VALIDATION but got SEED → must refuse (raises
        # InvalidEnvelopeError, which is verify_envelope's signal for
        # cross-type substitution attempts).
        with pytest.raises(_signing.InvalidEnvelopeError, match="payloadType"):
            _signing.verify_envelope(
                envelope, pub,
                expected_payload_type=_signing.PAYLOAD_TYPE_VALIDATION,
            )


# ---------------------------------------------------------------------------
# Bootstrap-flow integration
# ---------------------------------------------------------------------------


class TestBootstrapIntegration:
    def test_full_chain_seed_then_replicate_then_validate(
        self, tmp_path: Path,
    ) -> None:
        from tests._helpers import _two_signers
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            root = g.assert_claim("root of trust", generated_by="seed", seed=True)
            a = g.assert_claim("finding", supports=[root], generated_by="A", signer=sa)
            g.assert_claim("finding", supports=[root], generated_by="B", signer=sb)
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            g.enroll_validator(_validator_pem(tmp_path), identity="v")

        # Promote A to ESTABLISHED via validate(), under the validator key
        # (the graph refuses self-validation).
        with mareforma.open(tmp_path, key_path=_validator_key(tmp_path)) as g:
            g.validate(a)
            assert g.get_claim(a)["support_level"] == "ESTABLISHED"

        # The validated claim is itself an ESTABLISHED upstream for
        # downstream peers, REPLICATED chain continues from it. New
        # claims are asserted by the root key again.
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            d = g.assert_claim("downstream", supports=[a], generated_by="D", signer=sa)
            e = g.assert_claim("downstream", supports=[a], generated_by="E", signer=sb)
            assert g.get_claim(d)["support_level"] == "REPLICATED"
            assert g.get_claim(e)["support_level"] == "REPLICATED"


class TestValidateConcurrentInvalidation:
    """validate_claim must not promote past a contradiction that lands in the
    check-to-write window: the early t_invalid gate runs in an autocommit
    SELECT, then a long stretch of crypto + evidence checks runs with no
    transaction open. A second writer setting t_invalid in that window must
    not be rideable into ESTABLISHED."""

    def test_contradiction_in_check_to_write_window_refused(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from tests._helpers import _two_signers
        import mareforma.db.core as _core

        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            root = g.assert_claim("root of trust", generated_by="seed", seed=True)
            a = g.assert_claim("finding", supports=[root], generated_by="A", signer=sa)
            g.assert_claim("finding", supports=[root], generated_by="B", signer=sb)
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            g.enroll_validator(_validator_pem(tmp_path), identity="v")

        db_path = tmp_path / ".mareforma" / "graph.db"
        original = _core._verify_evidence_seen

        def _interleave(conn, claim_id, evidence, now):
            # A signed contradiction verdict lands after validate_claim's early
            # gate passed: a second connection sets t_invalid before the
            # promotion UPDATE fires. Mirrors a concurrent contradiction worker.
            side = sqlite3.connect(db_path, timeout=30)
            try:
                side.execute("BEGIN IMMEDIATE")
                side.execute(
                    "UPDATE claims SET t_invalid = ? WHERE claim_id = ?",
                    (_core._now(), claim_id),
                )
                side.commit()
            finally:
                side.close()
            return original(conn, claim_id, evidence, now)

        monkeypatch.setattr(_core, "_verify_evidence_seen", _interleave)

        with mareforma.open(tmp_path, key_path=_validator_key(tmp_path)) as g:
            with pytest.raises(ValueError, match="invalidated"):
                g.validate(a)
            row = g._conn.execute(
                "SELECT support_level, t_invalid FROM claims WHERE claim_id = ?",
                (a,),
            ).fetchone()
        # The claim stayed at REPLICATED and kept its t_invalid marker, rather
        # than climbing to ESTABLISHED over an already-refuted verdict.
        assert row["support_level"] == "REPLICATED"
        assert row["t_invalid"] is not None
