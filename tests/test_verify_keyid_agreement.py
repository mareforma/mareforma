"""The verify key-id agreement guard.

``mareforma verify`` reads the signer out of the signature bundle, never off the
row's ``asserter_keyid`` column, which is an unsigned denormalisation. A row
whose column contradicts its own envelope was written outside the signing path,
so it is refused rather than trusted.

Two exploit shapes reach the same forged verdict, and both are covered here:

  (a) a bundle signed by an unenrolled key stapled under a row whose
      ``asserter_keyid`` names the enrolled root. Auditor mode has no pubkey for
      the unenrolled signer, so the bundle path used to fall through to a clean
      verdict while the CLI disclosed enrollment on the row column, which named
      the root. The result read ``verified`` at exit 0.
  (b) the same staple is reachable by a single UPDATE that sets
      ``signature_bundle`` and ``asserter_keyid`` together on a previously
      UNSIGNED row, because ``claims_signed_fields_no_laundering`` guards the
      keyid only ``WHEN OLD.signature_bundle IS NOT NULL``. The trigger is
      slipped entirely; verify is the backstop.

Two pinning tests fix the surrounding behaviour: the honest stranger (bundle
keyid == row keyid, signer not enrolled) stays unverifiable at exit 2, and a
legacy row that carries a bundle but never denormalised a keyid is not refused
by the guard.
"""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

import mareforma
from mareforma import signing
from mareforma.cli import (
    _VERIFY_FAIL,
    _VERIFY_OK,
    _VERIFY_UNVERIFIABLE,
    cli,
)
from mareforma.db import open_db
from tests._helpers import _bootstrap_key


def _forge_bundle_binding_row(conn, claim_id: str, signer) -> str:
    """Sign the exact signed fields of an existing row with *signer*.

    Returns a bundle JSON string whose predicate binds *claim_id* on every
    signed field, so it clears the audit path's claim-id and field checks and
    reaches the key-id guard.
    """
    row = conn.execute(
        "SELECT * FROM claims WHERE claim_id = ?", (claim_id,),
    ).fetchone()
    claim_fields = {
        "claim_id": row["claim_id"],
        "text": row["text"],
        "classification": row["classification"],
        "generated_by": row["generated_by"],
        "supports": json.loads(row["supports_json"] or "[]"),
        "contradicts": json.loads(row["contradicts_json"] or "[]"),
        "source_name": row["source_name"],
        "artifact_hash": row["artifact_hash"],
        "created_at": row["created_at"],
    }
    evidence = json.loads(row["evidence_json"]) if row["evidence_json"] else {}
    envelope = signing.sign_claim(claim_fields, signer, evidence=evidence)
    return json.dumps(envelope, sort_keys=True, separators=(",", ":"))


def _enroll_root_then_unsigned_claim(tmp_path: Path) -> tuple[str, str]:
    """Enroll a root validator, then assert one UNSIGNED claim under it.

    Returns ``(claim_id, root_keyid)``. Opening with the root key auto-enrolls
    it; reopening with ``load_key=False`` leaves the enrollment in place while
    the new claim carries no signature and no denormalised keyid.
    """
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key):
        pass
    root_keyid = signing.public_key_id(
        signing.load_private_key(root_key).public_key(),
    )
    with mareforma.open(tmp_path, load_key=False) as g:
        cid = g.assert_claim("a finding under review", classification="ANALYTICAL")
    return cid, root_keyid


def _db(tmp_path: Path):
    return open_db(tmp_path)


class TestKeyIdDisagreementRefused:
    def test_forged_bundle_keyid_disagreeing_with_row_is_refused(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """Shape (a): bundle signed by an unenrolled key, row column names the
        enrolled root. Before the guard this returned (True, "") and the CLI
        printed ``verified`` at exit 0; it must now refuse."""
        cid, root_keyid = _enroll_root_then_unsigned_claim(tmp_path)
        stranger = tmp_path / "stranger.key"
        signing.bootstrap_key(stranger)
        stranger_signer = signing.load_private_key(stranger)

        conn = _db(tmp_path)
        try:
            forged = _forge_bundle_binding_row(conn, cid, stranger_signer)
            # One UPDATE stapling a bundle signed by the stranger under the
            # enrolled root's keyid. OLD.signature_bundle is NULL, so the
            # laundering trigger's WHEN clause is false and this is permitted.
            conn.execute(
                "UPDATE claims SET signature_bundle = ?, asserter_keyid = ? "
                "WHERE claim_id = ?",
                (forged, root_keyid, cid),
            )
            conn.commit()
        finally:
            conn.close()

        monkeypatch.chdir(tmp_path)
        res = CliRunner().invoke(cli, ["verify", cid], catch_exceptions=False)
        assert res.exit_code == _VERIFY_FAIL, res.output

        res_json = CliRunner().invoke(
            cli, ["verify", cid, "--json"], catch_exceptions=False,
        )
        doc = json.loads(res_json.output)
        assert doc["verdict"] == "tampered"
        assert doc["exit_code"] == _VERIFY_FAIL
        assert "disagree" in doc["reason"]

    def test_unsigned_row_update_slips_the_laundering_trigger(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """Shape (b): setting bundle and keyid together on an unsigned row is
        NOT blocked by ``claims_signed_fields_no_laundering``. The schema alone
        does not stop the laundering; verify is what refuses the result."""
        cid, root_keyid = _enroll_root_then_unsigned_claim(tmp_path)
        stranger = tmp_path / "stranger.key"
        signing.bootstrap_key(stranger)
        stranger_signer = signing.load_private_key(stranger)

        conn = _db(tmp_path)
        try:
            forged = _forge_bundle_binding_row(conn, cid, stranger_signer)
            # The trigger is slipped: no IntegrityError is raised even though the
            # keyid moves, because the row was unsigned before this statement.
            conn.execute(
                "UPDATE claims SET signature_bundle = ?, asserter_keyid = ? "
                "WHERE claim_id = ?",
                (forged, root_keyid, cid),
            )
            conn.commit()
            stored = conn.execute(
                "SELECT asserter_keyid FROM claims WHERE claim_id = ?", (cid,),
            ).fetchone()["asserter_keyid"]
            assert stored == root_keyid
        finally:
            conn.close()

        monkeypatch.chdir(tmp_path)
        res = CliRunner().invoke(cli, ["verify", cid], catch_exceptions=False)
        assert res.exit_code == _VERIFY_FAIL, res.output


class TestPinnedBehaviour:
    def test_honest_stranger_is_unverifiable_not_tampered(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """Bundle keyid == row keyid, signer not enrolled. The guard passes (the
        row agrees with its envelope); the signer simply has no enrolled pubkey,
        so auditor mode reports exit 2, unverifiable, unchanged."""
        cid, _ = _enroll_root_then_unsigned_claim(tmp_path)
        stranger = tmp_path / "stranger.key"
        signing.bootstrap_key(stranger)
        stranger_signer = signing.load_private_key(stranger)
        stranger_keyid = signing.public_key_id(stranger_signer.public_key())

        conn = _db(tmp_path)
        try:
            forged = _forge_bundle_binding_row(conn, cid, stranger_signer)
            conn.execute(
                "UPDATE claims SET signature_bundle = ?, asserter_keyid = ? "
                "WHERE claim_id = ?",
                (forged, stranger_keyid, cid),
            )
            conn.commit()
        finally:
            conn.close()

        monkeypatch.chdir(tmp_path)
        res = CliRunner().invoke(cli, ["verify", cid], catch_exceptions=False)
        assert res.exit_code == _VERIFY_UNVERIFIABLE, res.output
        assert "not an enrolled validator" in res.output

    def test_legacy_bundle_without_denormalised_keyid_not_refused(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """A legacy row carries a bundle but never denormalised a keyid
        (``asserter_keyid`` NULL). The guard's ``ak is None`` arm lets it
        through; an enrolled signer still verifies at exit 0."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key):
            pass
        root_signer = signing.load_private_key(root_key)
        with mareforma.open(tmp_path, load_key=False) as g:
            cid = g.assert_claim("legacy finding", classification="ANALYTICAL")

        conn = _db(tmp_path)
        try:
            forged = _forge_bundle_binding_row(conn, cid, root_signer)
            # Bundle present, asserter_keyid left NULL: the legacy shape.
            conn.execute(
                "UPDATE claims SET signature_bundle = ? WHERE claim_id = ?",
                (forged, cid),
            )
            conn.commit()
        finally:
            conn.close()

        monkeypatch.chdir(tmp_path)
        res = CliRunner().invoke(cli, ["verify", cid], catch_exceptions=False)
        assert res.exit_code == _VERIFY_OK, res.output


class TestBundleWithNoSigner:
    """A bundle that names no signer is not the legacy row.

    ``_extract_signature_bundle_keyid`` answers None for two different things:
    a row that never denormalised its keyid (legacy, honest) and a bundle whose
    ``signatures`` array is empty or malformed (nothing to verify). The
    agreement guard's ``ak is None`` arm admitted both, so an empty array
    skipped the pubkey block entirely and fell through to verified. That is the
    same exit-0-over-nothing the guard was written to close, reached by dropping
    the array instead of swapping the key id.
    """

    def test_an_empty_signatures_array_is_refused(self, tmp_path: Path) -> None:
        from mareforma.db import get_claim, verify_claim_signatures

        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            cid = g.assert_claim("a finding", generated_by="agent/x")

        conn = _db(tmp_path)
        try:
            row = dict(get_claim(conn, cid))
            # The honest row verifies: this is the control, so a refusal below
            # cannot be blamed on the fixture.
            assert verify_claim_signatures(conn, row) == (True, "")

            # A REAL envelope: its payload still binds this row and every signed
            # field still matches, so every earlier check passes. Only the
            # signatures are gone, and the keyid column is cleared to reach the
            # legacy arm of the guard.
            env = json.loads(row["signature_bundle"])
            env["signatures"] = []
            row["signature_bundle"] = json.dumps(env)
            row["asserter_keyid"] = None

            ok, reason = verify_claim_signatures(conn, row)
            assert ok is False, "a bundle with no signature must not verify"
            assert "names no signer" in reason, reason
        finally:
            conn.close()

    def test_a_malformed_bundle_is_refused_the_same_way(
        self, tmp_path: Path,
    ) -> None:
        """The signatures key missing entirely takes the same path."""
        from mareforma.db import get_claim, verify_claim_signatures

        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            cid = g.assert_claim("a finding", generated_by="agent/x")

        conn = _db(tmp_path)
        try:
            row = dict(get_claim(conn, cid))
            env = json.loads(row["signature_bundle"])
            del env["signatures"]
            row["signature_bundle"] = json.dumps(env)
            row["asserter_keyid"] = None

            ok, _reason = verify_claim_signatures(conn, row)
            assert ok is False
        finally:
            conn.close()


class TestTrustMapDoesNotFailOpen:
    """The map must not vouch for a signature that is not there.

    ``build_trust_map`` gated its re-verification on ``asserter_keyid`` AND
    ``signature_bundle``, so a row carrying a stapled keyid and no bundle
    skipped the check and fell back to the stored ``verified`` gate, which
    get_claim passes through True for PRELIMINARY rows. The map then printed
    "signature re-verified on read" beside the root's keyid for a claim with no
    signature at all, while ``mareforma verify`` called the same claim tampered.
    The MCP server exposes this map standalone, with no verdict beside it.
    """

    def test_a_stapled_keyid_with_no_bundle_does_not_read_as_verified(
        self, tmp_path: Path,
    ) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            anchor = g.assert_claim("anchor", generated_by="agent/x")
            root_keyid = g.get_claim(anchor)["asserter_keyid"]
        with mareforma.open(tmp_path, load_key=False) as g:
            cid = g.assert_claim("unsigned finding", classification="ANALYTICAL")

        conn = _db(tmp_path)
        try:
            # Allowed: the laundering trigger guards the keyid only WHEN the row
            # already carried a bundle, so stapling onto an unsigned row slips it.
            conn.execute(
                "UPDATE claims SET asserter_keyid = ? WHERE claim_id = ?",
                (root_keyid, cid),
            )
            conn.commit()
        finally:
            conn.close()

        with mareforma.open(tmp_path, load_key=False) as g:
            props = g.trust_map(cid).to_dict()["properties"]
        att = next(p for p in props if p["name"] == "attributability")
        assert "failed re-verification" in att["residual"], att
        assert "re-verified on read" != att["residual"]

    def test_an_honest_signed_claim_still_reads_reverified(
        self, tmp_path: Path,
    ) -> None:
        """The control: the fix must not demote a genuinely signed claim."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            cid = g.assert_claim("honest finding", generated_by="agent/x")
            props = g.trust_map(cid).to_dict()["properties"]
        att = next(p for p in props if p["name"] == "attributability")
        assert att["residual"] == "signature re-verified on read", att
