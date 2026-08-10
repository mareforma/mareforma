"""What the read path binds before it calls a promoted row verified.

Five guarantees, each one a way a served row used to be trusted on material
nobody checked.

* **Form, not bytes.** A signature covers canonical bytes, which NFC-normalize
  every string; the row keeps the bytes the caller passed. Text that arrives
  decomposed is therefore signed composed and stored decomposed, and a bare
  ``!=`` between the two reported an honest claim as tampered.
* **Enrolment, not membership.** ``validators`` takes a direct INSERT, so a real
  pubkey under a junk enrollment envelope reads as a validator to any check that
  asks only whether the row exists. The chain walk back to the self-signed root
  is what "registered" means everywhere else.
* **The seed exemption belongs to ESTABLISHED.** ``validation_signature`` is
  verified on read at that tier alone and is not on the laundering trigger's
  watch list, so below it the column is unauthenticated text that bought a lone
  claim the whole corroboration exemption.
* **The legacy grandfather is not a blank cheque.** A NULL ``asserter_keyid``
  exempts a promoted row from both signature checks. Every column that reading
  keys on is one the writer is already assigning, so the exemption also asks the
  project, through a different table, whether it signs at all.
* **The project policy is read through its signature.** The declaration is
  root-signed and one-way, but the flat columns beside it are a cache any writer
  can edit, and the table carried no write guard at all.
"""
from __future__ import annotations

import base64
import json
import sqlite3
import unicodedata
from pathlib import Path

import pytest

import mareforma
from mareforma import db as _db
from mareforma import signing as _signing
from mareforma import validators as _validators
from mareforma.db import (
    InvalidValidationEnvelopeError,
    get_claim,
    open_db,
    query_claims,
    strict_promotion_required,
    verify_claim_signatures,
)

from tests._helpers import _bootstrap_key, _pem_of, _two_signers

# "café" with a combining acute: the form a macOS filename, a PDF extract or
# most text typed in Vietnamese or Korean arrives in.
_DECOMPOSED = "café assay reaches 0.8 recovery"


def _adversary(root: Path) -> sqlite3.Connection:
    """A raw connection to graph.db, the way a co-resident process gets one."""
    conn = sqlite3.connect(str(root / ".mareforma" / "graph.db"))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")
    return conn


def _plant_validator(root: Path, key_path: Path, parent_keyid: str) -> str:
    """INSERT a validators row for *key_path*: real pubkey, junk envelope.

    The whole attack in one statement. The row exists, so every check that reads
    presence finds a validator; the enrollment envelope is junk, so the chain
    walk back to the self-signed root fails. Returns the planted keyid.
    """
    signer = _signing.load_private_key(key_path)
    keyid = _signing.public_key_id(signer.public_key())
    pem_b64 = base64.standard_b64encode(_pem_of(key_path)).decode("ascii")
    conn = _adversary(root)
    conn.execute(
        "INSERT INTO validators (keyid, pubkey_pem, identity, validator_type, "
        "enrolled_at, enrolled_by_keyid, enrollment_envelope) "
        "VALUES (?, ?, 'planted@lab.example', 'human', "
        "'2026-01-01T00:00:00+00:00', ?, '{}')",
        (keyid, pem_b64, parent_keyid),
    )
    conn.commit()
    conn.close()
    return keyid


def _replicated_pair(graph, *, artifact_hashes=(None, None)) -> tuple[str, str]:
    """A seed anchor plus two distinct-signer claims citing it."""
    sa, sb = _two_signers(graph._root)
    seed = graph.assert_claim("anchor", generated_by="seed", seed=True)
    a = graph.assert_claim(
        "child-a", generated_by="lab_a", supports=[seed], signer=sa,
        artifact_hash=artifact_hashes[0],
    )
    b = graph.assert_claim(
        "child-b", generated_by="lab_b", supports=[seed], signer=sb,
        artifact_hash=artifact_hashes[1],
    )
    return a, b


# ---------------------------------------------------------------------------
# (a) A signed value is compared up to NFC form
# ---------------------------------------------------------------------------


class TestSignedValuesCompareByForm:
    def test_decomposed_text_still_verifies(self, tmp_path: Path) -> None:
        """An honest claim whose text arrives decomposed must not read tampered.

        The signature covers the composed form (canonicalization normalizes) and
        the row keeps the decomposed bytes, so the audit path compared two
        spellings of the same string and reported the claim as rewritten.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim(_DECOMPOSED, classification="ANALYTICAL")
            claim = g.get_claim(cid)
        # The row really did keep the decomposed bytes; the defect is the
        # comparison, not the write, and this claim is already on disk.
        assert claim["text"] == _DECOMPOSED
        assert claim["text"] != unicodedata.normalize("NFC", claim["text"])

        conn = open_db(tmp_path)
        try:
            assert verify_claim_signatures(conn, claim) == (True, "")
        finally:
            conn.close()

    def test_a_real_text_edit_is_still_caught(self, tmp_path: Path) -> None:
        """Comparing by form narrows nothing: text that differs after
        normalization differs here too."""
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim(_DECOMPOSED, classification="ANALYTICAL")
        conn = _adversary(tmp_path)
        conn.execute("DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering")
        conn.execute(
            "UPDATE claims SET text = ? WHERE claim_id = ?",
            ("café assay reaches 0.9 recovery", cid),
        )
        conn.commit()
        conn.close()

        conn = open_db(tmp_path)
        try:
            ok, reason = verify_claim_signatures(conn, get_claim(conn, cid))
        finally:
            conn.close()
        assert not ok
        assert "'text'" in reason


# ---------------------------------------------------------------------------
# (b) A row in the validators table is not an enrolment
# ---------------------------------------------------------------------------


class TestPresenceIsNotEnrolment:
    def test_planted_validator_cannot_hold_an_established_row(
        self, tmp_path: Path,
    ) -> None:
        """A seed claim's validation envelope re-signed by a planted key must
        stop the row reading verified on the ESTABLISHED tier."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        plant_key = _bootstrap_key(tmp_path, "plant.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            seed = g.assert_claim("anchor", generated_by="seed", seed=True)
            assert g.get_claim(seed)["support_level"] == "ESTABLISHED"
            assert g.get_claim(seed)["verified"] is True
        root_keyid = _signing.public_key_id(
            _signing.load_private_key(root_key).public_key()
        )
        plant_keyid = _plant_validator(tmp_path, plant_key, root_keyid)

        conn = open_db(tmp_path)
        try:
            # Present, and not enrolled: the two answers the fix separates.
            assert _validators.get_validator(conn, plant_keyid) is not None
            assert _validators.is_enrolled(conn, plant_keyid) is False
        finally:
            conn.close()

        forged = _signing.sign_seed_claim(
            {
                "claim_id": seed,
                "validator_keyid": plant_keyid,
                "seeded_at": "2026-01-01T00:00:00+00:00",
            },
            _signing.load_private_key(plant_key),
        )
        conn = _adversary(tmp_path)
        conn.execute(
            "UPDATE claims SET validation_signature = ? WHERE claim_id = ?",
            (json.dumps(forged, sort_keys=True, separators=(",", ":")), seed),
        )
        conn.commit()
        conn.close()

        conn = open_db(tmp_path)
        try:
            assert get_claim(conn, seed)["verified"] is False
            assert _db.count_unverified_promoted(conn) == 1
        finally:
            conn.close()

    def test_planted_validator_cannot_attest_a_role(
        self, tmp_path: Path,
    ) -> None:
        """A role attestation signed by a planted key is not an attestation the
        project granted, and the audit path must say so."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        plant_key = _bootstrap_key(tmp_path, "plant.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            cid = g.assert_claim("has a role", classification="ANALYTICAL")
            claim = g.get_claim(cid)
        root_keyid = claim["asserter_keyid"]
        plant_keyid = _plant_validator(tmp_path, plant_key, root_keyid)

        env = json.loads(claim["signature_bundle"])
        pae = _signing.dsse_pae(
            _signing.PAYLOAD_TYPE_CLAIM,
            base64.standard_b64decode(env["payload"]),
        )
        env["signatures"].append({
            "keyid": plant_keyid,
            "sig": base64.standard_b64encode(
                _signing.load_private_key(plant_key).sign(pae)
            ).decode("ascii"),
            "role": "reviewer",
        })
        claim["signature_bundle"] = json.dumps(env)

        conn = open_db(tmp_path)
        try:
            ok, reason = verify_claim_signatures(conn, claim)
        finally:
            conn.close()
        assert not ok
        assert "role signature" in reason

    def test_planted_validator_cannot_promote_to_established(
        self, tmp_path: Path,
    ) -> None:
        """The write path refuses the same envelope the read path refuses: the
        planted key is not an enrolled validator, whatever the table says."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        plant_key = _bootstrap_key(tmp_path, "plant.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            _a, cid_b = _replicated_pair(g)
            assert g.get_claim(cid_b)["support_level"] == "REPLICATED"
        root_keyid = _signing.public_key_id(
            _signing.load_private_key(root_key).public_key()
        )
        plant_keyid = _plant_validator(tmp_path, plant_key, root_keyid)

        with mareforma.open(tmp_path, key_path=root_key) as g:
            now = _db._now()
            env = _signing.sign_validation(
                {
                    "claim_id": cid_b,
                    "validator_keyid": plant_keyid,
                    "validated_at": now,
                    "evidence_seen": [],
                },
                _signing.load_private_key(plant_key),
            )
            with pytest.raises(
                InvalidValidationEnvelopeError, match="not an enrolled",
            ):
                _db.validate_claim(
                    g._conn, g._root, cid_b,
                    validated_by="planted",
                    validation_signature=json.dumps(
                        env, sort_keys=True, separators=(",", ":"),
                    ),
                    validated_at=now,
                    evidence_seen=[],
                )
            assert g.get_claim(cid_b)["support_level"] == "REPLICATED"


# ---------------------------------------------------------------------------
# (c) The seed exemption applies at ESTABLISHED and nowhere else
# ---------------------------------------------------------------------------


class TestSeedExemptionIsTiered:
    def test_a_seed_envelope_on_a_replicated_row_exempts_nothing(
        self, tmp_path: Path,
    ) -> None:
        """Nothing verifies ``validation_signature`` below ESTABLISHED, so a
        lone claim flipped to REPLICATED with a seed-typed string in that column
        must not inherit the born-ESTABLISHED exemption."""
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            lone = g.assert_claim("a lone finding", generated_by="lab_a")
            assert g.get_claim(lone)["support_level"] == "PRELIMINARY"

        conn = _adversary(tmp_path)
        # The promotion marker is a speed bump by design; the guarantee is the
        # read-path re-derivation, so drop it and prove the row is still caught.
        conn.execute("DROP TRIGGER IF EXISTS claims_signed_promotion_backed")
        conn.execute(
            "UPDATE claims SET support_level = 'REPLICATED', "
            "validation_signature = ? WHERE claim_id = ?",
            (json.dumps({"payloadType": _signing.PAYLOAD_TYPE_SEED}), lone),
        )
        conn.commit()
        conn.close()

        conn = open_db(tmp_path)
        try:
            assert get_claim(conn, lone)["verified"] is False
            assert [c["claim_id"] for c in query_claims(conn)] == []
        finally:
            conn.close()

    def test_a_real_seed_claim_is_still_exempt(self, tmp_path: Path) -> None:
        """The exemption still does its job where it belongs: a born-ESTABLISHED
        seed never climbed the ladder and has no corroboration to show."""
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            seed = g.assert_claim("anchor", generated_by="seed", seed=True)
        conn = open_db(tmp_path)
        try:
            row = get_claim(conn, seed)
            assert row["support_level"] == "ESTABLISHED"
            assert row["verified"] is True
            assert _db.count_unverified_promoted(conn) == 0
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# (d) The NULL-keyid grandfather asks the project, not only the row
# ---------------------------------------------------------------------------


class TestLegacyGrandfatherNeedsAnUnsignedProject:
    def test_an_unsigned_promoted_row_is_not_served_by_a_signing_project(
        self, tmp_path: Path,
    ) -> None:
        """A project that enrols a validator does not serve a promoted claim
        carrying no signature at all. Otherwise an unsigned block appended to
        claims.toml (or one UPDATE here) mints a verified REPLICATED row."""
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a signed finding", generated_by="lab_a")
        with mareforma.open(tmp_path, key_path=key, load_key=False) as g:
            unsigned = g.assert_claim("no signature at all", generated_by="x")

        conn = _adversary(tmp_path)
        # No trigger to drop: the promotion guard fires on signed rows only,
        # which is the whole point, this row carries nothing to guard.
        conn.execute(
            "UPDATE claims SET support_level = 'REPLICATED' WHERE claim_id = ?",
            (unsigned,),
        )
        conn.commit()
        conn.close()

        conn = open_db(tmp_path)
        try:
            row = get_claim(conn, unsigned)
            assert row["signature_bundle"] is None
            assert row["asserter_keyid"] is None
            assert row["verified"] is False
            assert unsigned not in [c["claim_id"] for c in query_claims(conn)]
        finally:
            conn.close()

    def test_a_project_that_never_signs_keeps_its_grandfather(
        self, tmp_path: Path,
    ) -> None:
        """The legacy reading survives where it is honest: no validator, no
        statement_cid, nothing was ever signed here to be stripped."""
        with mareforma.open(tmp_path) as g:  # keyless project
            cid = g.assert_claim("keyless finding", generated_by="lab_a")

        conn = _adversary(tmp_path)
        conn.execute(
            "UPDATE claims SET support_level = 'REPLICATED' WHERE claim_id = ?",
            (cid,),
        )
        conn.commit()
        conn.close()

        conn = open_db(tmp_path)
        try:
            assert get_claim(conn, cid)["verified"] is True
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# (e) The project policy is read through its root signature, and guarded
# ---------------------------------------------------------------------------


def _strict_graph(tmp_path: Path) -> tuple[Path, str]:
    """A strict-promotion project holding one hand-promoted, dataless pair.

    Returns (root key path, the claim id to read). Both claims are created AFTER
    the declaration and carry no ``artifact_hash``, so the strict rule is the
    only thing standing between the row and a clean corroborated read: laundered
    away, the peer on the shared anchor backs the level.
    """
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key, strict_promotion=True) as g:
        a, b = _replicated_pair(g)
        assert g.get_claim(b)["support_level"] == "PRELIMINARY"
    conn = _adversary(tmp_path)
    conn.execute("DROP TRIGGER IF EXISTS claims_signed_promotion_backed")
    conn.execute(
        "UPDATE claims SET support_level = 'REPLICATED' "
        "WHERE claim_id IN (?, ?)",
        (a, b),
    )
    conn.commit()
    conn.close()
    return root_key, b


class TestProjectPolicyIsReadThroughItsSignature:
    def test_a_laundered_flag_does_not_retire_the_rule_on_read(
        self, tmp_path: Path,
    ) -> None:
        _root_key, cid = _strict_graph(tmp_path)
        conn = open_db(tmp_path)
        try:
            assert get_claim(conn, cid)["verified"] is False
        finally:
            conn.close()

        conn = _adversary(tmp_path)
        conn.execute("DROP TRIGGER IF EXISTS project_policy_append_only")
        conn.execute("UPDATE project_policy SET strict_promotion_required = 0")
        conn.commit()
        conn.close()

        conn = open_db(tmp_path)
        try:
            # The envelope still says the rule is declared, so the flat column
            # cannot retire it and the dataless row stays unverified.
            assert get_claim(conn, cid)["verified"] is False
        finally:
            conn.close()

    def test_a_laundered_flag_does_not_retire_the_rule_on_write(
        self, tmp_path: Path,
    ) -> None:
        _strict_graph(tmp_path)
        conn = _adversary(tmp_path)
        conn.execute("DROP TRIGGER IF EXISTS project_policy_append_only")
        conn.execute("UPDATE project_policy SET strict_promotion_required = 0")
        conn.commit()
        conn.close()

        conn = open_db(tmp_path)
        try:
            assert strict_promotion_required(conn) is True
        finally:
            conn.close()

    def test_a_policy_signed_by_a_stranger_is_not_the_projects(
        self, tmp_path: Path,
    ) -> None:
        """The envelope has to come from the project's own root, or a writer
        who holds any key at all declares the project's rules."""
        _root_key, cid = _strict_graph(tmp_path)
        stranger = _bootstrap_key(tmp_path, "stranger.key")
        stranger_priv = _signing.load_private_key(stranger)
        env = _signing.sign_project_policy(
            {
                "version": _signing._PROJECT_POLICY_VERSION,
                "rekor_required": False,
                "strict_promotion_required": False,
                "created_at": "2026-01-01T00:00:00+00:00",
                "rekor_declared_at": None,
                "strict_promotion_declared_at": None,
            },
            stranger_priv,
        )
        conn = _adversary(tmp_path)
        conn.execute("DROP TRIGGER IF EXISTS project_policy_append_only")
        conn.execute(
            "UPDATE project_policy SET strict_promotion_required = 0, "
            "signer_keyid = ?, envelope = ?, "
            "created_at = '2026-01-01T00:00:00+00:00', "
            "strict_promotion_declared_at = NULL",
            (
                _signing.public_key_id(stranger_priv.public_key()),
                json.dumps(env, sort_keys=True, separators=(",", ":")),
            ),
        )
        conn.commit()
        conn.close()

        conn = open_db(tmp_path)
        try:
            assert strict_promotion_required(conn) is True
            assert get_claim(conn, cid)["verified"] is False
        finally:
            conn.close()

    def test_the_policy_row_cannot_be_deleted_or_updated(
        self, tmp_path: Path,
    ) -> None:
        """The row records a one-way rule, so it cannot be dropped: without the
        guard, DELETE launders the whole declaration and the backup written
        afterwards carries no policy for restore to enforce."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key, strict_promotion=True):
            pass
        conn = _adversary(tmp_path)
        try:
            with pytest.raises(
                sqlite3.IntegrityError, match="project_policy_delete_blocked",
            ):
                conn.execute("DELETE FROM project_policy WHERE id = 1")
            with pytest.raises(
                sqlite3.IntegrityError, match="project_policy_locked",
            ):
                conn.execute(
                    "UPDATE project_policy SET strict_promotion_required = 0"
                )
        finally:
            conn.close()
        conn = open_db(tmp_path)
        try:
            assert strict_promotion_required(conn) is True
        finally:
            conn.close()

    def test_declaring_a_second_rule_still_works(self, tmp_path: Path) -> None:
        """The guards must not lock out the one writer that may replace the
        row: extending the policy signs the union and rewrites the singleton."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key, strict_promotion=True):
            pass
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.require_rekor_witnessing()
        conn = open_db(tmp_path)
        try:
            policy = _db.get_project_policy(conn)
            assert (policy["rekor_required"], policy["strict_promotion_required"]) \
                == (1, 1)
            # Still bound to its envelope after the rewrite, so the rules are
            # enforceable rather than merely stored.
            assert strict_promotion_required(conn) is True
        finally:
            conn.close()

    def test_the_guards_reconcile_onto_a_graph_that_already_exists(
        self, tmp_path: Path,
    ) -> None:
        """The triggers are managed, so a graph written before they existed
        gains them on the next open rather than staying unguarded."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key, strict_promotion=True):
            pass
        conn = _adversary(tmp_path)
        conn.execute("DROP TRIGGER IF EXISTS project_policy_append_only")
        conn.execute("DROP TRIGGER IF EXISTS project_policy_no_delete")
        conn.commit()
        conn.close()

        open_db(tmp_path).close()

        conn = _adversary(tmp_path)
        try:
            names = {
                r["name"] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                )
            }
            assert "project_policy_append_only" in names
            assert "project_policy_no_delete" in names
        finally:
            conn.close()
