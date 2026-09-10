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


class TestTheRuleBindsMoreThanPython:
    """The Python gate binds callers who come through Python, and nobody else.

    ``validate_claim`` gates its UPDATE on ``validation_signature IS NULL``.
    That was the whole rule, so a second enrolled validator refused by
    ``validate()`` could sign its own envelope, write it with plain sqlite3, and
    the row would read verified under the new name with the first validator's
    envelope gone and nothing recording that it had ever been there. The read
    path could not notice, because the envelope that survived verified.
    """

    @staticmethod
    def _envelope_for(claim_id: str, key: Path) -> tuple[str, str, str]:
        """A genuine validation envelope for *claim_id*, signed by *key*."""
        import json
        from datetime import datetime, timezone

        priv = _sig.load_private_key(key)
        keyid = _sig.public_key_id(priv.public_key())
        when = datetime.now(timezone.utc).isoformat()
        envelope = _sig.sign_validation(
            {
                "claim_id": claim_id,
                "validator_keyid": keyid,
                "validated_at": when,
                "evidence_seen": [],
            },
            priv,
        )
        return json.dumps(envelope), keyid, when

    def test_a_direct_update_cannot_replace_a_validation(
        self, tmp_path: Path,
    ) -> None:
        import sqlite3

        root_key, alice, bob, claim_id = _project_with_two_validators(tmp_path)
        with mareforma.open(tmp_path, key_path=alice) as graph:
            graph.validate(claim_id, validated_by="alice")

        envelope, bob_keyid, when = self._envelope_for(claim_id, bob)
        conn = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        try:
            with pytest.raises(sqlite3.IntegrityError, match="validation_is_terminal"):
                conn.execute(
                    "UPDATE claims SET validated_by = ?, validated_at = ?, "
                    "validation_signature = ?, validator_keyid = ? "
                    "WHERE claim_id = ?",
                    ("bob", when, envelope, bob_keyid, claim_id),
                )
        finally:
            conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as graph:
            assert graph.get_claim(claim_id)["validated_by"] == "alice"

    def test_a_direct_update_cannot_move_the_attribution_alone(
        self, tmp_path: Path,
    ) -> None:
        """Leaving the envelope and moving the name is the quieter half.

        ``validated_by`` and ``validator_keyid`` are denormalised out of the
        signed payload, so moving them alone leaves the row naming one person
        and the envelope another.
        """
        import sqlite3

        root_key, alice, bob, claim_id = _project_with_two_validators(tmp_path)
        with mareforma.open(tmp_path, key_path=alice) as graph:
            graph.validate(claim_id, validated_by="alice")
        bob_keyid = _sig.public_key_id(
            _sig.load_private_key(bob).public_key()
        )

        conn = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        try:
            with pytest.raises(sqlite3.IntegrityError, match="validation_is_terminal"):
                conn.execute(
                    "UPDATE claims SET validated_by = ?, validator_keyid = ? "
                    "WHERE claim_id = ?",
                    ("bob", bob_keyid, claim_id),
                )
        finally:
            conn.close()

    def test_the_first_validation_is_still_written(self, tmp_path: Path) -> None:
        """The guard must not cost the honest write it sits in front of."""
        root_key, alice, bob, claim_id = _project_with_two_validators(tmp_path)
        with mareforma.open(tmp_path, key_path=alice) as graph:
            graph.validate(claim_id, validated_by="alice")
            assert graph.get_claim(claim_id)["validated_by"] == "alice"


class TestTheCheckToWriteWindow:
    """A contradiction landing mid-call loses the race, it does not ride in.

    ``validate_claim`` checks the claim is open and uninvalidated, then verifies
    the envelope and the cited evidence, then writes. A signed contradiction
    that lands between the check and the write would otherwise be overwritten by
    a validation that was decided before it existed, and the graph would carry a
    human's sign-off on a claim the record says is invalid.

    The guard is on the UPDATE itself. The class that used to exercise this
    window went out with the support ladder, and the branch has had nothing
    reaching it since.
    """

    def test_a_contradiction_in_the_window_refuses_the_validation(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from mareforma.db import core as _core

        root_key, alice, bob, claim_id = _project_with_two_validators(tmp_path)

        real = _core._verify_evidence_seen

        def land_a_contradiction(conn, promoted_claim_id, evidence_seen, validated_at):
            """Stand in for a concurrent writer inside the window.

            Called after the gate and before the guarded UPDATE, which is
            exactly where a racing verdict lands.
            """
            real(conn, promoted_claim_id, evidence_seen, validated_at)
            conn.execute(
                "UPDATE claims SET t_invalid = ? WHERE claim_id = ?",
                ("2026-01-01T00:00:00+00:00", promoted_claim_id),
            )

        monkeypatch.setattr(_core, "_verify_evidence_seen", land_a_contradiction)

        with mareforma.open(tmp_path, key_path=alice) as graph:
            with pytest.raises(ValueError, match="invalidated by a signed"):
                graph.validate(claim_id, validated_by="alice")

    def test_the_row_carries_no_validation_afterwards(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """A refusal that left the envelope behind would be the worse half."""
        from mareforma.db import core as _core

        root_key, alice, bob, claim_id = _project_with_two_validators(tmp_path)
        real = _core._verify_evidence_seen

        def land_a_contradiction(conn, promoted_claim_id, evidence_seen, validated_at):
            real(conn, promoted_claim_id, evidence_seen, validated_at)
            conn.execute(
                "UPDATE claims SET t_invalid = ? WHERE claim_id = ?",
                ("2026-01-01T00:00:00+00:00", promoted_claim_id),
            )

        monkeypatch.setattr(_core, "_verify_evidence_seen", land_a_contradiction)
        with mareforma.open(tmp_path, key_path=alice) as graph:
            with pytest.raises(ValueError):
                graph.validate(claim_id, validated_by="alice")

        with mareforma.open(tmp_path, key_path=root_key) as graph:
            row = graph.get_claim(claim_id)
            assert row["validation_signature"] is None
            assert row["validated_by"] is None

class TestReputationCountsTheSigner:


    """The count groups by the signed thing, not the column beside it.

    ``validator_keyid`` is unsigned. Grouping on it credited a validator for a
    row it never signed, while every read surface refused to serve that row: the
    count is a separate statement that never consulted the read path.
    """

    def test_a_stapled_envelope_credits_nobody_it_did_not_sign(
        self, tmp_path: Path,
    ) -> None:
        import sqlite3

        root_key, alice, bob, claim_id = _project_with_two_validators(tmp_path)
        with mareforma.open(tmp_path, key_path=alice) as graph:
            graph.validate(claim_id, validated_by="alice")
        bob_keyid = _sig.public_key_id(
            _sig.load_private_key(bob).public_key()
        )

        # A row carrying alice's envelope under bob's name. INSERT, because the
        # UPDATE route is closed above and the primary key forbids reusing the
        # claim_id.
        conn = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        conn.row_factory = sqlite3.Row
        row = dict(
            conn.execute(
                "SELECT * FROM claims WHERE claim_id = ?", (claim_id,)
            ).fetchone()
        )
        row.update(
            claim_id="11111111-2222-4333-8444-555555555555",
            validated_by="bob",
            validator_keyid=bob_keyid,
            prev_hash=None,
            idempotency_key=None,
        )
        conn.execute(
            f"INSERT INTO claims ({', '.join(row)}) "
            f"VALUES ({', '.join('?' * len(row))})",
            tuple(row.values()),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as graph:
            assert graph.get_validator_reputation()[bob_keyid] == 0, (
                "a validator was credited for a row it never signed"
            )
            served = graph.query("a finding", limit=99)
            assert all(c["validated_by"] == "alice" for c in served), (
                "the forged row reached a read surface"
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
