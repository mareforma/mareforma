"""One claim, one validation, and every surface says who vouched for the row.

The rule that made a validation terminal used to be a side effect of the
ladder: the row went to the top rung and the write was gated on the rung below
it, so a second call matched nothing and raised. Removing the ladder removed
the rule without anybody noticing, and the column holds one envelope, so one
validator's signed sign-off replaced another's with nothing recording that the
first had ever existed. The read path could not tell, because the envelope that
survived verified.

The disclosure half is the same shape. A claim whose signer nobody enrolled
used to be held back from the default read; it is served now, so a surface that
serves it has to say what it is rather than let a caller assume somebody
vouched for it.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from mareforma import signing as _sig
from mareforma.db import list_claims, open_db

from tests._helpers import _bootstrap_key


def _project_with_two_validators(root: Path) -> tuple[Path, Path, Path, str]:
    """A claim plus two enrolled validators, neither of which signed it."""
    root_key = _bootstrap_key(root, "root.key")
    alice, bob = root / "alice.key", root / "bob.key"
    _sig.bootstrap_key(alice)
    _sig.bootstrap_key(bob)
    with mareforma.open(root, key_path=root_key) as graph:
        claim_id = graph.assert_claim("a finding", generated_by="run")
        for key, who in ((alice, "alice"), (bob, "bob")):
            graph.enroll_validator(
                _sig.public_key_to_pem(_sig.load_private_key(key).public_key()),
                identity=who,
            )
    return root_key, alice, bob, claim_id


class TestAValidationIsTerminal:
    def test_a_second_validator_is_refused(self, tmp_path: Path) -> None:
        root_key, alice, bob, claim_id = _project_with_two_validators(tmp_path)
        with mareforma.open(tmp_path, key_path=alice) as graph:
            graph.validate(claim_id, validated_by="alice")

        with mareforma.open(tmp_path, key_path=bob) as graph:
            with pytest.raises(ValueError, match="already carries a validation"):
                graph.validate(claim_id, validated_by="bob")

    def test_the_first_validation_survives_the_attempt(
        self, tmp_path: Path,
    ) -> None:
        """The half that matters: the refusal is not a rollback of the record.

        A refusal that also cleared the first attestation would be worse than
        the overwrite it replaces.
        """
        root_key, alice, bob, claim_id = _project_with_two_validators(tmp_path)
        with mareforma.open(tmp_path, key_path=alice) as graph:
            graph.validate(claim_id, validated_by="alice")
            before = graph.get_claim(claim_id)

        with mareforma.open(tmp_path, key_path=bob) as graph:
            with pytest.raises(ValueError):
                graph.validate(claim_id, validated_by="bob")

        with mareforma.open(tmp_path, key_path=root_key) as graph:
            after = graph.get_claim(claim_id)
        assert after["validated_by"] == "alice"
        assert after["validation_signature"] == before["validation_signature"]
        assert after["validator_keyid"] == before["validator_keyid"], (
            "the reputation count moved to a validator who signed nothing"
        )

    def test_the_refusal_names_the_reason(self, tmp_path: Path) -> None:
        """Three things make the guarded write match nothing, and they call for
        different actions. A claim somebody already signed off on is not the
        same problem as one a verdict invalidated mid-call."""
        root_key, alice, bob, claim_id = _project_with_two_validators(tmp_path)
        with mareforma.open(tmp_path, key_path=alice) as graph:
            graph.validate(claim_id, validated_by="alice")
        with mareforma.open(tmp_path, key_path=bob) as graph:
            with pytest.raises(ValueError) as caught:
                graph.validate(claim_id, validated_by="bob")
        said = str(caught.value)
        assert "erase the first" in said
        assert "as its own claim" in said, (
            "the refusal does not say what to do instead"
        )


class TestEverySurfaceSaysWhoVouched:
    """``generator_enrolled`` rides on all three reads, not just the paged one.

    The enumerating reads carried it because they used to drop the row. Now
    nothing drops it, and the two surfaces an auditor reaches for, one claim at
    a time and the whole listing, carried nothing that said so.
    """

    def _unenrolled_claim(self, root: Path) -> tuple[Path, str]:
        root_key = _bootstrap_key(root, "root.key")
        stranger = root / "stranger.key"
        _sig.bootstrap_key(stranger)
        with mareforma.open(root, key_path=root_key) as graph:
            claim_id = graph.assert_claim(
                "a finding nobody vouched for", generated_by="run",
                signer=_sig.load_private_key(stranger),
            )
        return root_key, claim_id

    def test_get_claim_carries_it(self, tmp_path: Path) -> None:
        root_key, claim_id = self._unenrolled_claim(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as graph:
            row = graph.get_claim(claim_id)
        assert row["generator_enrolled"] is False

    def test_list_claims_carries_it(self, tmp_path: Path) -> None:
        root_key, claim_id = self._unenrolled_claim(tmp_path)
        conn = open_db(tmp_path)
        try:
            rows = {r["claim_id"]: r for r in list_claims(conn)}
        finally:
            conn.close()
        assert rows[claim_id]["generator_enrolled"] is False

    def test_it_is_true_when_the_signer_is_enrolled(
        self, tmp_path: Path,
    ) -> None:
        """The direction that makes the flag mean anything."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as graph:
            claim_id = graph.assert_claim("a finding", generated_by="run")
            assert graph.get_claim(claim_id)["generator_enrolled"] is True
