"""Reputation-aware retrieval tests.

`graph.query()` gains an ``include_unverified`` kwarg and a per-row
``validator_reputation`` projection. ``graph.get_validator_reputation()``
returns the bulk map. Reputation is derived state — recomputed on every
call from the claims table, never cached.
"""

from __future__ import annotations

from pathlib import Path

import mareforma
from mareforma import signing as _signing
from tests._helpers import _bootstrap_key, _pem_of, _two_signers


# ---------------------------------------------------------------------------
# include_unverified filter
# ---------------------------------------------------------------------------

class TestValidatorReputationProjection:
    def _seed_and_promote(
        self,
        tmp_path: Path,
        n_promotions: int,
        root_key: Path,
        validator_key: Path,
    ) -> list[str]:
        """Build a graph with *n_promotions* claims promoted to
        ESTABLISHED under *validator_key*. Returns the promoted ids."""
        rep_ids: list[str] = []
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            seed = g.assert_claim("seed", generated_by="seed")
            for i in range(n_promotions):
                rep_id = g.assert_claim(
                    f"finding {i}", supports=[seed], generated_by=f"A{i}",
                    signer=sa,
                )
                g.assert_claim(
                    f"finding {i}", supports=[seed], generated_by=f"B{i}",
                    signer=sb,
                )
                rep_ids.append(rep_id)
            g.enroll_validator(_pem_of(validator_key), identity="v")
        with mareforma.open(tmp_path, key_path=validator_key) as g:
            for rep_id in rep_ids:
                g.validate(rep_id)
        return rep_ids

    def test_established_row_carries_validator_reputation(
        self, tmp_path: Path,
    ) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        validator_key = _bootstrap_key(tmp_path, "v.key")
        rep_ids = self._seed_and_promote(tmp_path, 3, root_key, validator_key)

        with mareforma.open(tmp_path, key_path=root_key) as g:
            results = g.query(limit=50)

        # Every claim comes back, so narrow to the ones promoted above.
        promoted = [r for r in results if r["claim_id"] in rep_ids]
        assert len(promoted) == 3
        for r in promoted:
            # Each promoted claim's reputation equals the validator's
            # total ESTABLISHED-validation count (3 promotions under
            # the same validator key).
            assert r["validator_reputation"] == 3

    def test_generator_enrolled_true_for_root_signed(
        self, tmp_path: Path,
    ) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.assert_claim("signed by root")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            results = g.query()
        assert len(results) == 1
        assert results[0]["generator_enrolled"] is True

    def test_generator_enrolled_false_for_unsigned(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path) as g:
            g.assert_claim("unsigned")
        with mareforma.open(tmp_path) as g:
            results = g.query()
        assert len(results) == 1
        assert results[0]["generator_enrolled"] is False


# ---------------------------------------------------------------------------
# get_validator_reputation bulk map
# ---------------------------------------------------------------------------

class TestGetValidatorReputation:
    def test_zero_validations_returns_zero(self, tmp_path: Path) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            reputation = g.get_validator_reputation()
        # Root is auto-enrolled but has zero ESTABLISHED claims yet.
        assert len(reputation) == 1
        assert list(reputation.values()) == [0]

    def test_unenrolled_keyids_absent_from_reputation(
        self, tmp_path: Path,
    ) -> None:
        """The reputation map only includes enrolled validator keyids."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            reputation = g.get_validator_reputation()
        # Only one entry, the root.
        assert len(reputation) == 1

    def test_reputation_recomputed_each_call(self, tmp_path: Path) -> None:
        """Reputation is derived state — never cached. A subsequent
        validation must be visible on the next call."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        v_key = _bootstrap_key(tmp_path, "v.key")
        v_keyid = _signing.public_key_id(
            _signing.load_private_key(v_key).public_key(),
        )
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            seed = g.assert_claim("seed", generated_by="seed")
            id1 = g.assert_claim(
                "f1", supports=[seed], generated_by="A1", signer=sa,
            )
            g.assert_claim("f1", supports=[seed], generated_by="B1", signer=sb)
            id2 = g.assert_claim(
                "f2", supports=[seed], generated_by="A2", signer=sa,
            )
            g.assert_claim("f2", supports=[seed], generated_by="B2", signer=sb)
            g.enroll_validator(_pem_of(v_key), identity="v")

        with mareforma.open(tmp_path, key_path=v_key) as g:
            g.validate(id1)
            before = g.get_validator_reputation()
            g.validate(id2)
            after = g.get_validator_reputation()

        assert before[v_keyid] == 1
        assert after[v_keyid] == 2


# ---------------------------------------------------------------------------
# validator_keyid denormalization
# ---------------------------------------------------------------------------

class TestValidatorKeyidColumn:
    def test_validate_populates_validator_keyid(self, tmp_path: Path) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        v_key = _bootstrap_key(tmp_path, "v.key")
        v_keyid = _signing.public_key_id(
            _signing.load_private_key(v_key).public_key(),
        )
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            seed = g.assert_claim("seed", generated_by="seed")
            rep_id = g.assert_claim(
                "f", supports=[seed], generated_by="A", signer=sa,
            )
            g.assert_claim("f", supports=[seed], generated_by="B", signer=sb)
            g.enroll_validator(_pem_of(v_key), identity="v")
        with mareforma.open(tmp_path, key_path=v_key) as g:
            g.validate(rep_id)
            claim = g.get_claim(rep_id)
        assert claim["validator_keyid"] == v_keyid

    def test_preliminary_claim_has_null_validator_keyid(
        self, tmp_path: Path,
    ) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            cid = g.assert_claim("preliminary")
            claim = g.get_claim(cid)
        assert claim["validator_keyid"] is None

