"""The observed axis stops arriving unattested through recovery.

``observed_grounding`` is the one signal on a claim meant not to be the
producer's own word. ``_attest_grounding`` is where the write path enforces
that: a verdict the process's observer minted is stored as the observer's
snapshot, and anything else is marked DECLARED with its GROUNDED claim
neutralised to OPAQUE.

Restore never passed through there. It writes the axis straight out of
claims.toml, so a producer could export a claim, edit GROUNDED into it,
re-sign with their own enrolled key, restore, and every read surface rendered
the result exactly like an execution mareforma watched.

Restore cannot re-run the check instead: the register it reads is in-process
and keyed on a receipt digest, so a fresh restore would strip the axis off
every honest claim too. So the observer's word travels in the file, and these
tests pin what that buys. It is parity, not prevention: the observer runs in
the producer's process and the producer holds the key, so a determined producer
can build an attestation as well. What it ends is the ordinary act of editing
the axis and nothing else.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
try:
    import tomllib          # 3.11+ stdlib
except ModuleNotFoundError:  # 3.10, where it is the tomli backport
    import tomli as tomllib  # type: ignore[no-redef]
from pathlib import Path

import pytest
import tomli_w

import mareforma
from mareforma import signing
from mareforma.db.core import grounding_attestation_state
from mareforma.db.restore import restore
from mareforma.db.errors import RestoreError
from mareforma.observe import observe
from tests._helpers import rewrite_backup, _bootstrap_key, _enroll_key, _load_signer


def _dataset(root: Path) -> Path:
    csv = root / "trial.csv"
    csv.write_text("arm,outcome\ntreat,1\ncontrol,0\n")
    return csv


def _observed_grounded(path: Path):
    """A GROUNDED verdict the observer earned by watching *path* be read."""
    with observe(cites=str(path)) as handle:
        path.read_text()
    assert handle.verdict.grounding.value == "GROUNDED", handle.verdict.reason
    return handle.verdict


def _hand_built(path: Path) -> dict:
    """The record a caller can type: a GROUNDED conclusion, no observation."""
    from mareforma.observe import GroundingVerdict, ObservedGrounding as OG

    return GroundingVerdict(
        OG.GROUNDED, "the caller says the data was read",
        cited_sources=(str(path),), grounded_sources=(str(path),),
    ).to_signed_dict()


def _observed_claim(root: Path) -> tuple[Path, str, Path]:
    """A graph with one claim whose GROUNDED verdict the observer computed."""
    key = _bootstrap_key(root, "root.key")
    data = _dataset(root)
    with mareforma.open(root, key_path=key) as g:
        claim_id = g.assert_claim(
            "an honestly observed finding", classification="ANALYTICAL",
            predicate_payload={
                "data_sources": [str(data.resolve())], "data_ids": [],
            },
            observed_grounding=_observed_grounded(data).to_signed_dict(),
        )
    return key, claim_id, data


def _state(root: Path, key: Path, claim_id: str) -> str:
    with mareforma.open(root, key_path=key) as g:
        return grounding_attestation_state(g._conn, claim_id)


def _axis(root: Path, key: Path, claim_id: str) -> str | None:
    with mareforma.open(root, key_path=key) as g:
        row = g._conn.execute(
            "SELECT observed_grounding FROM claims WHERE claim_id = ?",
            (claim_id,),
        ).fetchone()
    return json.loads(row[0] or "{}").get("grounding")


def _forge_axis_in_backup(root: Path, claim_id: str, key: Path) -> None:
    """The attack: edit the axis, re-sign the claim, rewrite the backup.

    Everything a producer holding their own key can do. Every signature the
    restore path checks still verifies afterwards, which is why the axis alone
    could never separate this from an honest claim.
    """
    toml_path = root / "claims.toml"
    doc = tomllib.loads(toml_path.read_text())
    claim = doc["claims"][claim_id]
    source = str((root / "trial.csv").resolve())
    forged = {
        "grounding": "GROUNDED",
        "reason": "the observer watched this read",
        "cited_sources": [source], "grounded_sources": [source],
        "provenance": "COMPUTED", "axis_version": "grounding@v1",
    }
    fields = {
        "claim_id": claim_id, "text": claim["text"],
        "classification": claim["classification"],
        "generated_by": claim["generated_by"],
        "supports": claim.get("supports", []),
        "contradicts": claim.get("contradicts", []),
        "source_name": claim.get("source_name"),
        "artifact_hash": claim.get("artifact_hash"),
        "created_at": claim["created_at"], "observed_grounding": forged,
    }
    evidence = json.loads(claim.get("evidence_json") or "{}")
    claim["signature_bundle"] = json.dumps(
        signing.sign_claim(fields, _load_signer(key), evidence=evidence)
    )
    claim["observed_grounding"] = json.dumps(forged)
    claim["statement_cid"] = hashlib.sha256(
        signing.canonical_statement(fields, evidence)
    ).hexdigest()
    rewrite_backup(toml_path, doc)


class TestWhoGetsOne:
    def test_an_observed_verdict_is_attested(self, tmp_path: Path) -> None:
        key, claim_id, _ = _observed_claim(tmp_path)
        assert _state(tmp_path, key, claim_id) == "attested"

    def test_a_declared_verdict_is_not(self, tmp_path: Path) -> None:
        """No row, and that absence is the signal rather than a gap.

        The write path already neutralises this to OPAQUE, so nothing is being
        rescued here. What matters is that it earns no attestation, because a
        restore is what would otherwise give the neutralised record a second
        chance at GROUNDED.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        data = _dataset(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            claim_id = g.assert_claim(
                "a declared finding", observed_grounding=_hand_built(data),
            )
        assert _state(tmp_path, key, claim_id) == "unattested"

    def test_a_declared_verdict_carrying_a_digest_is_still_not_attested(
        self, tmp_path: Path,
    ) -> None:
        """Both halves of the mint test carry weight, and only one was pinned.

        A record is the observer's when it has a receipt digest AND is not
        marked declared. The case above only omits the digest, so the digest
        half catches it alone and the declared half could be deleted with the
        whole suite green. A non-GROUNDED declaration keeps its digest, because
        the write path only strips it on the GROUNDED branch, so a hand-built
        record with a fabricated digest is the shape that reaches the other
        half. Without it, hand-typed becomes observer-signed.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            claim_id = g.assert_claim(
                "a declaration wearing a receipt",
                observed_grounding={
                    "grounding": "OPAQUE",
                    "receipt_digest": "f" * 64,
                    "provenance": "DECLARED",
                },
            )
        assert _state(tmp_path, key, claim_id) == "unattested"

    def test_a_claim_with_no_verdict_is_not_attested(
        self, tmp_path: Path,
    ) -> None:
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            claim_id = g.assert_claim("a claim with no grounding at all")
        assert _state(tmp_path, key, claim_id) == "unattested"


class TestItSurvivesRecovery:
    def test_an_honest_attestation_round_trips(self, tmp_path: Path) -> None:
        """The honest case has to keep its axis, or the fix costs more than it saves."""
        key, claim_id, _ = _observed_claim(tmp_path)
        shutil.rmtree(tmp_path / ".mareforma")
        restore(tmp_path)
        assert _axis(tmp_path, key, claim_id) == "GROUNDED"
        assert _state(tmp_path, key, claim_id) == "attested"

    def test_a_forged_axis_is_refused(self, tmp_path: Path) -> None:
        """The laundering path, closed.

        The claim is asserted with no grounding at all. Its axis is edited to
        GROUNDED in the backup and re-signed with the producer's own enrolled
        key, so every signature checks out, because the producer is signing
        their own claim. The release that wrote the attestations could only
        tell the two apart on read; this one stops the restore.

        What that buys is parity with the write path, which has always refused
        to take the axis on the producer's word. It does not beat the producer:
        the observer runs inside their process and they hold the key. The
        override is here because it is the same file to an operator who edited
        it on purpose.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        _dataset(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            claim_id = g.assert_claim(
                "the treatment lowers the outcome", classification="ANALYTICAL",
            )
        _forge_axis_in_backup(tmp_path, claim_id, key)
        shutil.rmtree(tmp_path / ".mareforma")

        with pytest.raises(RestoreError) as caught:
            restore(tmp_path)
        assert caught.value.kind == "grounding_unattested"
        assert not (tmp_path / ".mareforma").exists()

        # The same file, restored by somebody who says they meant it. The axis
        # still reads GROUNDED, and still has nothing attesting it, which is
        # the discriminator the attestation was written for.
        restore(tmp_path, trust_unaccounted_backup=True)
        assert _axis(tmp_path, key, claim_id) == "GROUNDED"
        assert _state(tmp_path, key, claim_id) == "unattested"

    def test_the_honest_and_forged_axes_are_distinguishable(
        self, tmp_path: Path,
    ) -> None:
        """Both say GROUNDED. Before the attestation, that was all there was."""
        honest_root = tmp_path / "honest"
        honest_root.mkdir()
        honest_key, honest_id, _ = _observed_claim(honest_root)
        shutil.rmtree(honest_root / ".mareforma")
        restore(honest_root)

        forged_root = tmp_path / "forged"
        forged_root.mkdir()
        forged_key = _bootstrap_key(forged_root, "root.key")
        _dataset(forged_root)
        with mareforma.open(forged_root, key_path=forged_key) as g:
            forged_id = g.assert_claim("a finding", classification="ANALYTICAL")
        _forge_axis_in_backup(forged_root, forged_id, forged_key)
        shutil.rmtree(forged_root / ".mareforma")
        restore(forged_root, trust_unaccounted_backup=True)

        assert _axis(honest_root, honest_key, honest_id) == "GROUNDED"
        assert _axis(forged_root, forged_key, forged_id) == "GROUNDED"
        assert _state(honest_root, honest_key, honest_id) == "attested"
        assert _state(forged_root, forged_key, forged_id) == "unattested"


class TestAnAttestationIsNotTransferable:
    def test_it_cannot_be_moved_onto_another_claim(
        self, tmp_path: Path,
    ) -> None:
        """It names a statement, so it does not travel to a second claim.

        Without the statement binding, one honestly observed claim would supply
        an attestation for every unobserved one beside it.
        """
        key, observed_id, data = _observed_claim(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            other_id = g.assert_claim("a second, unobserved finding")
            row = g._conn.execute(
                "SELECT * FROM grounding_attestations WHERE claim_id = ?",
                (observed_id,),
            ).fetchone()
            g._conn.execute(
                "INSERT INTO grounding_attestations(claim_id, statement_cid, "
                "receipt_digest, grounding, signer_keyid, signature, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (other_id, row["statement_cid"], row["receipt_digest"],
                 row["grounding"], row["signer_keyid"], row["signature"],
                 row["created_at"]),
            )
            g._conn.commit()
            assert grounding_attestation_state(g._conn, other_id) == "broken"

    def test_the_statement_binding_is_what_stops_the_move(
        self, tmp_path: Path,
    ) -> None:
        """The test above passes on the claim id alone, so this pins the rest.

        Moving an attestation to another claim changes both the claim id and the
        statement, and the id is checked first, so removing the statement
        binding entirely left that test green. Here the id matches and only the
        statement moves: nothing but the statement binding is left to catch it.
        """
        key, claim_id, _ = _observed_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering")
        raw.execute(
            "UPDATE claims SET statement_cid = ? WHERE claim_id = ?",
            ("f" * 64, claim_id),
        )
        raw.commit()
        raw.close()
        assert _state(tmp_path, key, claim_id) == "broken"

    def test_the_statement_is_signed_and_not_merely_compared(
        self, tmp_path: Path,
    ) -> None:
        """Moving both sides together is what the signature is for.

        The comparison alone is two unsigned columns agreeing with each other.
        An attacker who can write the attestation row edits the claim's
        statement and the attestation's copy of it to the same new value, and a
        check that only compares them sees a match. Only the signature notices,
        and only if the statement is inside it: with the field dropped from the
        signed payload, the previous test still passed.
        """
        key, claim_id, _ = _observed_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering")
        raw.execute("DROP TRIGGER IF EXISTS grounding_attestations_append_only")
        raw.execute(
            "UPDATE claims SET statement_cid = ? WHERE claim_id = ?",
            ("a" * 64, claim_id),
        )
        raw.execute(
            "UPDATE grounding_attestations SET statement_cid = ? "
            "WHERE claim_id = ?", ("a" * 64, claim_id),
        )
        raw.commit()
        raw.close()
        assert _state(tmp_path, key, claim_id) == "broken"

    @pytest.mark.parametrize("field, value", [
        ("grounding", "OPAQUE"),
        ("receipt_digest", "f" * 64),
    ])
    def test_editing_either_half_of_the_axis_strands_it(
        self, tmp_path: Path, field: str, value: str,
    ) -> None:
        """Move the axis and its attestation stops matching it.

        Broken, never quietly folded back into unattested: an attestation that
        disagrees with the claim it names is a stronger signal than none.

        One field at a time. Changing both at once left neither comparison
        load-bearing: either could be deleted on its own and the test stayed
        green. Flipping the axis from OPAQUE to GROUNDED while keeping the
        digest is the realistic tamper, and it was the uncovered one.
        """
        key, claim_id, _ = _observed_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering")
        stored = json.loads(raw.execute(
            "SELECT observed_grounding FROM claims WHERE claim_id = ?",
            (claim_id,),
        ).fetchone()[0])
        stored[field] = value
        raw.execute(
            "UPDATE claims SET observed_grounding = ? WHERE claim_id = ?",
            (json.dumps(stored), claim_id),
        )
        raw.commit()
        raw.close()
        assert _state(tmp_path, key, claim_id) == "broken"

    def test_an_attestation_re_signed_by_another_peer_reads_broken(
        self, tmp_path: Path,
    ) -> None:
        """It is the asserter's word, so no other key can give it.

        The check verified under whatever key the row named and never asked
        whether that was the claim's asserter, so an enrolled peer that asserted
        nothing could re-sign the attestation and the map would say "the
        observer that computed this verdict attested it under the asserting
        key". A peer cannot manufacture the axis, so this was a false
        attribution rather than a false axis, and a false attribution on the one
        surface that exists to say who vouched for what is still false.
        """
        from mareforma import signing as _sign
        from mareforma.db.core import _grounding_attestation_pae

        key, claim_id, _ = _observed_claim(tmp_path)
        peer_key = _bootstrap_key(tmp_path, "peer.key")
        _enroll_key(tmp_path, key, peer_key, identity="peer@example.org")
        peer = _load_signer(peer_key)
        peer_id = _sign.public_key_id(peer.public_key())

        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.row_factory = sqlite3.Row
        raw.execute("DROP TRIGGER IF EXISTS grounding_attestations_append_only")
        row = raw.execute(
            "SELECT * FROM grounding_attestations WHERE claim_id = ?",
            (claim_id,),
        ).fetchone()
        forged = peer.sign(_grounding_attestation_pae({
            "claim_id": claim_id,
            "statement_cid": row["statement_cid"],
            "receipt_digest": row["receipt_digest"],
            "grounding": row["grounding"],
        }))
        raw.execute(
            "UPDATE grounding_attestations SET signer_keyid = ?, signature = ? "
            "WHERE claim_id = ?", (peer_id, forged, claim_id),
        )
        raw.commit()
        raw.close()
        assert _state(tmp_path, key, claim_id) == "broken"

    def test_an_attestation_naming_an_unknown_signer_reads_broken(
        self, tmp_path: Path,
    ) -> None:
        """A signer nothing knows is not a signer, and had no test at all."""
        key, claim_id, _ = _observed_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("DROP TRIGGER IF EXISTS grounding_attestations_append_only")
        raw.execute(
            "UPDATE grounding_attestations SET signer_keyid = ? "
            "WHERE claim_id = ?", ("f" * 64, claim_id),
        )
        raw.commit()
        raw.close()
        assert _state(tmp_path, key, claim_id) == "broken"

    def test_a_signer_that_does_not_chain_to_the_root_reads_broken(
        self, tmp_path: Path,
    ) -> None:
        """Enrolment is a chain walk here too, not a row lookup.

        The unknown-signer case above plants a keyid that is in no validators
        row, which a bare lookup would also catch. This one leaves the row in
        the table and breaks its enrolment, so a lookup still finds the signer
        and only the walk refuses it. The signature over the attestation still
        verifies, which is the point: the key is real and it no longer belongs
        to this project.
        """
        key, claim_id, _ = _observed_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.row_factory = sqlite3.Row
        assert grounding_attestation_state(raw, claim_id) == "attested"

        raw.execute("DROP TRIGGER IF EXISTS validators_append_only")
        signer = raw.execute(
            "SELECT signer_keyid FROM grounding_attestations WHERE claim_id = ?",
            (claim_id,),
        ).fetchone()["signer_keyid"]
        raw.execute(
            "UPDATE validators SET enrolled_by_keyid = ? WHERE keyid = ?",
            ("f" * 64, signer),
        )
        raw.commit()
        present = raw.execute(
            "SELECT COUNT(*) FROM validators WHERE keyid = ?", (signer,),
        ).fetchone()[0]
        assert present, "the signer's row must stay for this to test the walk"

        try:
            assert grounding_attestation_state(raw, claim_id) == "broken"
        finally:
            raw.close()

    def test_a_forged_signature_reads_broken(self, tmp_path: Path) -> None:
        key, claim_id, _ = _observed_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("DROP TRIGGER IF EXISTS grounding_attestations_append_only")
        raw.execute(
            "UPDATE grounding_attestations SET signature = ? WHERE claim_id = ?",
            (b"\x00" * 64, claim_id),
        )
        raw.commit()
        raw.close()
        assert _state(tmp_path, key, claim_id) == "broken"


def _break_the_attestation(root: Path, claim_id: str) -> None:
    """Leave the attestation in place and make its signature fail."""
    raw = sqlite3.connect(root / ".mareforma" / "graph.db")
    raw.execute("DROP TRIGGER IF EXISTS grounding_attestations_append_only")
    raw.execute(
        "UPDATE grounding_attestations SET signature = ? WHERE claim_id = ?",
        (b"\x00" * 64, claim_id),
    )
    raw.commit()
    raw.close()


class TestABrokenAttestationIsTamper:
    """Broken is not absent, and no read surface may treat it as either absent
    or fine.

    The state is distinguished from ``unattested`` because absence is the
    ordinary condition of an older graph and of every declared verdict, while a
    stored attestation that does not check out is evidence somebody edited it.
    Carrying that only in the residual leaves the axis reading GROUNDED, the
    colour gold and the exit code 0, which is a report nobody acts on.
    """

    def test_the_axis_reads_tampered(self, tmp_path: Path) -> None:
        key, claim_id, _ = _observed_claim(tmp_path)
        _break_the_attestation(tmp_path, claim_id)
        from mareforma.trust_map import is_tamper_value

        with mareforma.open(tmp_path, key_path=key) as g:
            grounding = next(
                p for p in g.trust_map(claim_id).properties
                if p.name == "grounding"
            )
        assert is_tamper_value(grounding.value), (
            f"grounding rendered {grounding.value} over a broken attestation"
        )

    def test_the_verdict_says_tampered_not_unverifiable(
        self, tmp_path: Path,
    ) -> None:
        """The half a caller reads as an exit code, and it has to say which.

        A tampered axis alone already lifts the verdict off ``verified``, but it
        lands on ``unverifiable``, which means the check could not be made. This
        check was made and it failed, and the two are the distinction the
        attestation exists to draw. Asked with and without a map, because the
        verdict must not differ by whether the caller wanted one.
        """
        from mareforma._verify import classify_claim_verdict
        from mareforma.db.core import get_claim

        key, claim_id, _ = _observed_claim(tmp_path)
        _break_the_attestation(tmp_path, claim_id)
        with mareforma.open(tmp_path, key_path=key) as g:
            claim = dict(get_claim(g._conn, claim_id))
            with_map = classify_claim_verdict(g._conn, claim, claim_id)
            without = classify_claim_verdict(
                g._conn, claim, claim_id, with_trust_map=False,
            )
        assert with_map.verdict == "tampered", with_map.verdict
        assert without.verdict == "tampered", without.verdict

    def test_an_honest_attestation_is_left_alone(self, tmp_path: Path) -> None:
        """The premise. Without this the two above pass on a broken graph."""
        from mareforma.trust_map import is_tamper_value

        key, claim_id, _ = _observed_claim(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            grounding = next(
                p for p in g.trust_map(claim_id).properties
                if p.name == "grounding"
            )
        assert not is_tamper_value(grounding.value)


class TestTheGuards:
    @pytest.mark.parametrize("statement, marker", [
        ("DELETE FROM grounding_attestations", "no_delete"),
        ("UPDATE grounding_attestations SET grounding = 'x'", "append_only"),
    ])
    def test_the_table_refuses_edits(
        self, tmp_path: Path, statement: str, marker: str,
    ) -> None:
        _observed_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        with pytest.raises(sqlite3.IntegrityError, match=marker):
            raw.execute(statement)
        raw.close()


class TestItIsReported:
    def test_the_trust_map_names_the_state(self, tmp_path: Path) -> None:
        """A user reads the axis on the map, so the map has to carry this."""
        key, claim_id, _ = _observed_claim(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            prop = next(
                p for p in g.trust_map(claim_id).properties
                if p.name == "grounding"
            )
        assert prop.value.startswith("GROUNDED")
        assert "attested it under the asserting key" in prop.residual
        assert "not proof the read happened" in prop.residual

    def test_the_map_says_so_when_there_is_no_attestation(
        self, tmp_path: Path,
    ) -> None:
        key = _bootstrap_key(tmp_path, "root.key")
        _dataset(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            claim_id = g.assert_claim("a finding", classification="ANALYTICAL")
        _forge_axis_in_backup(tmp_path, claim_id, key)
        shutil.rmtree(tmp_path / ".mareforma")
        restore(tmp_path, trust_unaccounted_backup=True)

        with mareforma.open(tmp_path, key_path=key) as g:
            prop = next(
                p for p in g.trust_map(claim_id).properties
                if p.name == "grounding"
            )
        assert prop.value.startswith("GROUNDED")
        assert "no observer attestation" in prop.residual
        assert "edited into a backup and restored" in prop.residual

    def test_the_map_says_so_when_the_attestation_is_broken(
        self, tmp_path: Path,
    ) -> None:
        """The third state, and the only one the map never had a test for.

        Its note calls a broken attestation "a stronger signal than absence",
        which is the whole reason the state exists rather than folding into
        unattested. Blanking the note left the entire suite green.
        """
        key, claim_id, _ = _observed_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("DROP TRIGGER IF EXISTS grounding_attestations_append_only")
        raw.execute(
            "UPDATE grounding_attestations SET signature = ? WHERE claim_id = ?",
            (b"\x00" * 64, claim_id),
        )
        raw.commit()
        raw.close()

        with mareforma.open(tmp_path, key_path=key) as g:
            assert grounding_attestation_state(g._conn, claim_id) == "broken"
            prop = next(
                p for p in g.trust_map(claim_id).properties
                if p.name == "grounding"
            )
        assert "does not check out against this claim" in prop.residual


class TestItDoesNotBreakTheOrdinaryCase:
    def test_an_unsigned_claim_is_written_without_one(
        self, tmp_path: Path,
    ) -> None:
        """No signer, no attestation, and no failure on the way past.

        The premise is asserted rather than assumed. This graph is keyless only
        because the suite scopes XDG_CONFIG_HOME per test: run the same code
        against a machine that has a default key and ``open()`` finds it, the
        claim is signed, and the writer's signer-less branch is never reached.
        A test that stops testing what its name says, on a machine where the
        environment differs, is worse than one that fails.
        """
        with mareforma.open(tmp_path) as g:
            assert g._signer is None, (
                "this graph has a key, so the signer-less branch is not "
                "under test here"
            )
            claim_id = g.assert_claim("a claim on a graph with no key")
            assert grounding_attestation_state(g._conn, claim_id) == "unattested"

    def test_the_table_coming_back_empty_reads_unattested(
        self, tmp_path: Path,
    ) -> None:
        """Absent is the ordinary state, never broken and never attested.

        The reconciler rebuilds the table on the next open, so what this
        exercises is a present-but-empty table, not a missing one. The genuinely
        table-less read is the case below, and it answers differently.
        """
        key, claim_id, _ = _observed_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("DROP TRIGGER IF EXISTS grounding_attestations_no_delete")
        raw.execute("DROP TABLE grounding_attestations")
        raw.commit()
        raw.close()
        assert _state(tmp_path, key, claim_id) == "unattested"
        with mareforma.open(tmp_path, key_path=key) as g:
            present = g._conn.execute(
                "SELECT COUNT(*) FROM sqlite_master "
                "WHERE name = 'grounding_attestations'"
            ).fetchone()[0]
        assert present == 1, "the reconciler is what makes this the empty case"

    def test_a_read_with_the_table_genuinely_gone_reports_broken(
        self, tmp_path: Path,
    ) -> None:
        """Through a raw connection, nothing heals it first.

        Reported rather than treated as absent: a table somebody removed is not
        the same as a graph that never had one, and going quiet would turn the
        removal into the ordinary state.
        """
        key, claim_id, _ = _observed_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.row_factory = sqlite3.Row
        raw.execute("DROP TRIGGER IF EXISTS grounding_attestations_no_delete")
        raw.execute("DROP TABLE grounding_attestations")
        raw.commit()
        assert grounding_attestation_state(raw, claim_id) == "broken"
        raw.close()


class TestAProjectOlderThanTheAttestations:
    """The carve-out that keeps every pre-attestation project restorable.

    A GROUNDED axis arriving with nothing attesting it is how an axis edited
    after the fact looks, and this release refuses it. Backups written before
    the attestations existed carry none, so without the stamp check every
    GROUNDED claim in them would read as laundered and their operators would be
    refused their own history.

    The guard that draws that line had no test. Removing it turned nothing red
    across the whole suite while a real pre-attestation backup went from
    restoring to refused, which is the shape of a promise nothing holds.
    """

    def test_a_backup_older_than_the_attestations_still_restores(
        self, tmp_path: Path,
    ) -> None:
        import shutil as _shutil

        import tomli_w

        key = _bootstrap_key(tmp_path, "root.key")
        _dataset(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            claim_id = g.assert_claim(
                "the treatment lowers the outcome", classification="ANALYTICAL",
            )
        _forge_axis_in_backup(tmp_path, claim_id, key)

        # What a backup written before any of this looks like: the axis is
        # there, and the stamp, the attestations and the completeness table
        # are not, because the release that wrote it had none of them.
        toml_path = tmp_path / "claims.toml"
        doc = tomllib.loads(toml_path.read_text())
        doc.pop("backup_format", None)
        doc.pop("grounding_attestations", None)
        doc.pop("completeness", None)
        toml_path.write_text(tomli_w.dumps(doc))

        _shutil.rmtree(tmp_path / ".mareforma")
        restore(tmp_path)

        assert _axis(tmp_path, key, claim_id) == "GROUNDED"
        assert _state(tmp_path, key, claim_id) == "unattested"
