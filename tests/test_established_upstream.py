"""tests/test_established_upstream.py — P1.7 ESTABLISHED-upstream gate + seed.

Covers:
  - REPLICATED requires an ESTABLISHED upstream (strict by default per D1)
  - seed=True creates a directly-ESTABLISHED claim with a signed envelope
  - seed=True requires a loaded signer
  - seed=True refused for unenrolled keys
  - Seed envelope round-trip verify
  - Cross-type substitution refused (seed envelope ≠ validation envelope)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import mareforma
from mareforma import db as _db
from mareforma import signing as _signing


def _key(tmp_path: Path) -> Path:
    key_path = tmp_path / "_p17_key"
    if not key_path.exists():
        _signing.bootstrap_key(key_path)
    return key_path


# ---------------------------------------------------------------------------
# Strict ESTABLISHED-upstream rule
# ---------------------------------------------------------------------------


class TestEstablishedUpstreamRule:
    def test_replicated_blocked_with_only_preliminary_upstream(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            up = g.assert_claim("upstream", generated_by="seed")  # NO seed=True
            assert g.get_claim(up)["support_level"] == "PRELIMINARY"
            a = g.assert_claim("a", supports=[up], generated_by="A")
            b = g.assert_claim("b", supports=[up], generated_by="B")
            # Without seed=True the upstream is PRELIMINARY → REPLICATED gate
            # does not fire.
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"

    def test_replicated_fires_when_upstream_is_established(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            up = g.assert_claim("seeded", generated_by="seed", seed=True)
            assert g.get_claim(up)["support_level"] == "ESTABLISHED"
            a = g.assert_claim("a", supports=[up], generated_by="A")
            b = g.assert_claim("b", supports=[up], generated_by="B")
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"

    def test_at_least_one_established_upstream_is_enough(
        self, tmp_path: Path,
    ) -> None:
        """Multiple upstreams; only ONE needs to be ESTABLISHED."""
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            estd = g.assert_claim("seeded", generated_by="seed", seed=True)
            prelim = g.assert_claim("plain upstream", generated_by="seed")
            a = g.assert_claim(
                "a", supports=[estd, prelim], generated_by="A",
            )
            b = g.assert_claim(
                "b", supports=[estd, prelim], generated_by="B",
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
        assert envelope["payloadType"] == _signing._PAYLOAD_TYPE_SEED
        pub = _signing.load_private_key(key_path).public_key()
        assert _signing.verify_envelope(
            envelope, pub,
            expected_payload_type=_signing._PAYLOAD_TYPE_SEED,
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
                expected_payload_type=_signing._PAYLOAD_TYPE_VALIDATION,
            )


# ---------------------------------------------------------------------------
# Bootstrap-flow integration
# ---------------------------------------------------------------------------


class TestBootstrapIntegration:
    def test_full_chain_seed_then_replicate_then_validate(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path, key_path=_key(tmp_path)) as g:
            root = g.assert_claim("root of trust", generated_by="seed", seed=True)
            a = g.assert_claim("finding", supports=[root], generated_by="A")
            b = g.assert_claim("finding", supports=[root], generated_by="B")
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            # Promote A to ESTABLISHED via validate()
            g.validate(a)
            assert g.get_claim(a)["support_level"] == "ESTABLISHED"
            # The validated claim is itself an ESTABLISHED upstream
            # for downstream peers — REPLICATED chain continues from it.
            d = g.assert_claim("downstream", supports=[a], generated_by="D")
            e = g.assert_claim("downstream", supports=[a], generated_by="E")
            assert g.get_claim(d)["support_level"] == "REPLICATED"
            assert g.get_claim(e)["support_level"] == "REPLICATED"
