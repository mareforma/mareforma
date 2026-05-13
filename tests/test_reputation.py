"""Reputation-aware retrieval tests (spec.md #96 / MF-016).

`graph.query()` gains an ``include_unverified`` kwarg and a per-row
``validator_reputation`` projection. ``graph.get_validator_reputation()``
returns the bulk map. Reputation is derived state — recomputed on every
call from the claims table, never cached.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from mareforma import db as _db
from mareforma import signing as _signing


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _bootstrap_key(tmp_path: Path, name: str) -> Path:
    key_path = tmp_path / name
    _signing.bootstrap_key(key_path)
    return key_path


def _pem_of(key_path: Path) -> bytes:
    return _signing.public_key_to_pem(
        _signing.load_private_key(key_path).public_key(),
    )


# ---------------------------------------------------------------------------
# include_unverified filter
# ---------------------------------------------------------------------------

class TestIncludeUnverifiedFilter:
    def test_default_excludes_unsigned_preliminary(self, tmp_path: Path) -> None:
        """Unsigned PRELIMINARY claims are filtered by default — unsigned
        mode operates without a validators chain, so the generator is
        not an enrolled identity."""
        with mareforma.open(tmp_path) as g:
            g.assert_claim("alpha")
            g.assert_claim("beta")
        with mareforma.open(tmp_path) as g:
            results = g.query()
        assert results == []

    def test_include_unverified_true_surfaces_unsigned(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path) as g:
            g.assert_claim("alpha")
            g.assert_claim("beta")
        with mareforma.open(tmp_path) as g:
            results = g.query(include_unverified=True)
        assert len(results) == 2

    def test_default_includes_signed_preliminary_from_enrolled_keyid(
        self, tmp_path: Path,
    ) -> None:
        """A PRELIMINARY claim signed by an enrolled validator (the
        auto-enrolled root) is surfaced by the default filter."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.assert_claim("alpha")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            results = g.query()
        assert len(results) == 1

    def test_default_filters_preliminary_from_unenrolled_keyid(
        self, tmp_path: Path,
    ) -> None:
        """A claim signed by a key NOT in the validators table is
        unverified at the generator level — filtered by default."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        other_key = _bootstrap_key(tmp_path, "other.key")

        # Root opens first so it auto-enrolls. Then re-open with the
        # other key — `other` is NOT enrolled and signs a PRELIMINARY
        # claim, which the default filter must exclude.
        with mareforma.open(tmp_path, key_path=root_key):
            pass
        with mareforma.open(tmp_path, key_path=other_key) as g:
            g.assert_claim("from unenrolled key")

        with mareforma.open(tmp_path, key_path=root_key) as g:
            default_results = g.query()
            opt_in_results = g.query(include_unverified=True)

        assert default_results == []
        assert len(opt_in_results) == 1

    def test_filter_only_applies_to_preliminary(self, tmp_path: Path) -> None:
        """REPLICATED claims are not subject to the include_unverified
        filter — they already require the enrolled-chain check via
        REPLICATED's substrate gates."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            seed = g.assert_claim("seed", generated_by="seed", seed=True)
            g.assert_claim("rep", supports=[seed], generated_by="A")
            g.assert_claim("rep", supports=[seed], generated_by="B")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            default_results = g.query()
        # Three rows: seed (ESTABLISHED), rep-A (REPLICATED), rep-B (REPLICATED).
        # All three have signing keyid == root keyid which IS enrolled.
        assert len(default_results) == 3


# ---------------------------------------------------------------------------
# validator_reputation per-row projection
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
        with mareforma.open(tmp_path, key_path=root_key) as g:
            seed = g.assert_claim("seed", generated_by="seed", seed=True)
            for i in range(n_promotions):
                rep_id = g.assert_claim(
                    f"finding {i}", supports=[seed], generated_by=f"A{i}",
                )
                g.assert_claim(
                    f"finding {i}", supports=[seed], generated_by=f"B{i}",
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
            results = g.query(min_support="ESTABLISHED", limit=50)

        # The seed claim is also ESTABLISHED — filter to the promoted set.
        promoted = [r for r in results if r["claim_id"] in rep_ids]
        assert len(promoted) == 3
        for r in promoted:
            # Each promoted claim's reputation equals the validator's
            # total ESTABLISHED-validation count (3 promotions under
            # the same validator key).
            assert r["validator_reputation"] == 3

    def test_preliminary_row_reputation_is_zero(self, tmp_path: Path) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.assert_claim("preliminary")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            results = g.query(min_support="PRELIMINARY")
        prelim_rows = [r for r in results if r["support_level"] == "PRELIMINARY"]
        assert prelim_rows
        for r in prelim_rows:
            assert r["validator_reputation"] == 0

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
            results = g.query(include_unverified=True)
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

    def test_validator_count_matches_promotions(
        self, tmp_path: Path,
    ) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        v_key = _bootstrap_key(tmp_path, "v.key")
        v_keyid = _signing.public_key_id(
            _signing.load_private_key(v_key).public_key(),
        )
        # Promote 5 ESTABLISHED claims under v_key.
        rep_ids: list[str] = []
        with mareforma.open(tmp_path, key_path=root_key) as g:
            seed = g.assert_claim("seed", generated_by="seed", seed=True)
            for i in range(5):
                rep_id = g.assert_claim(
                    f"f{i}", supports=[seed], generated_by=f"A{i}",
                )
                g.assert_claim(
                    f"f{i}", supports=[seed], generated_by=f"B{i}",
                )
                rep_ids.append(rep_id)
            g.enroll_validator(_pem_of(v_key), identity="v")
        with mareforma.open(tmp_path, key_path=v_key) as g:
            for rep_id in rep_ids:
                g.validate(rep_id)

        with mareforma.open(tmp_path, key_path=root_key) as g:
            reputation = g.get_validator_reputation()

        assert reputation[v_keyid] == 5
        # The root signed the seed claim, which is ESTABLISHED — that
        # bootstrap event counts as one validation under the root keyid.
        root_keyid = _signing.public_key_id(
            _signing.load_private_key(root_key).public_key(),
        )
        assert reputation[root_keyid] == 1

    def test_unenrolled_keyids_absent_from_reputation(
        self, tmp_path: Path,
    ) -> None:
        """The reputation map only includes enrolled validator keyids."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            reputation = g.get_validator_reputation()
        # Only one entry — the root.
        assert len(reputation) == 1

    def test_reputation_recomputed_each_call(self, tmp_path: Path) -> None:
        """Reputation is derived state — never cached. A subsequent
        validation must be visible on the next call."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        v_key = _bootstrap_key(tmp_path, "v.key")
        v_keyid = _signing.public_key_id(
            _signing.load_private_key(v_key).public_key(),
        )
        with mareforma.open(tmp_path, key_path=root_key) as g:
            seed = g.assert_claim("seed", generated_by="seed", seed=True)
            id1 = g.assert_claim("f1", supports=[seed], generated_by="A1")
            g.assert_claim("f1", supports=[seed], generated_by="B1")
            id2 = g.assert_claim("f2", supports=[seed], generated_by="A2")
            g.assert_claim("f2", supports=[seed], generated_by="B2")
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
        with mareforma.open(tmp_path, key_path=root_key) as g:
            seed = g.assert_claim("seed", generated_by="seed", seed=True)
            rep_id = g.assert_claim("f", supports=[seed], generated_by="A")
            g.assert_claim("f", supports=[seed], generated_by="B")
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

