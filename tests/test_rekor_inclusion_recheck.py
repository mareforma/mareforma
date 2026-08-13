"""The witnessing axis checks the inclusion proof, when it has a key to check it.

``rekor_inclusions`` refuses UPDATE and DELETE and permits INSERT, so a row
carrying a junk proof reaches a read looking exactly like one that witnesses
something. The axis read ``SELECT 1`` off that table and reported "an inclusion
record is stored". The proof was verified at restore and never again, which is
to say never, for anyone whose graph was not restored.

Verifying it needs the transparency log's own public key, and a read makes no
network call to get one: the key is pinned at
``<root>/.mareforma/rekor_log_pubkey.pem`` by an open that is handed one. So the
axis is key-conditional, and the three outcomes it can reach are genuinely
different. These tests hold them apart, because collapsing "nobody could check"
into "checked" is the failure the axis had.
"""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

import mareforma
from mareforma.db.core import get_claim, open_db
from tests._helpers import _bootstrap_key
from tests.test_rekor_verify import (
    _merkle_inclusion_path,
    _merkle_root,
    _pubkey_pem,
    _sign_checkpoint_ed25519,
)


def _leaf_for(envelope: dict) -> bytes:
    """The canonical hashedrekord record Rekor logs for *envelope*."""
    record = {
        "apiVersion": "0.0.1",
        "kind": "hashedrekord",
        "spec": {
            "data": {"hash": {"algorithm": "sha256", "value": hashlib.sha256(
                base64.standard_b64decode(envelope["payload"]),
            ).hexdigest()}},
            "signature": {
                "content": envelope["signatures"][0]["sig"],
                "publicKey": {"content": "<not-checked>"},
            },
        },
    }
    return json.dumps(record, separators=(",", ":")).encode("utf-8")


def _response_for(envelope: dict, log_key) -> dict:
    leaves = [f"filler{i}".encode() for i in range(7)]
    target = 3
    leaves[target] = _leaf_for(envelope)
    root = _merkle_root(leaves)
    checkpoint = _sign_checkpoint_ed25519(
        origin="rekor.test - 0001", tree_size=len(leaves), root_hash=root,
        signer_name="rekor.test", key=log_key,
    )
    return {
        "body": base64.standard_b64encode(leaves[target]).decode("ascii"),
        "integratedTime": 1700000000,
        "logIndex": target,
        "logID": "deadbeef",
        "verification": {"inclusionProof": {
            "checkpoint": checkpoint,
            "hashes": [h.hex() for h in _merkle_inclusion_path(leaves, target)],
            "logIndex": target,
            "rootHash": root.hex(),
            "treeSize": len(leaves),
        }},
    }


def _witnessed_claim(root: Path, *, pin_key: bool = True, corrupt: bool = False):
    """A signed claim with a real, verifying inclusion record beside it."""
    key = _bootstrap_key(root, "root.key")
    with mareforma.open(root, key_path=key) as g:
        cid = g.assert_claim("a witnessed claim")

    conn = open_db(root)
    try:
        envelope = json.loads(get_claim(conn, cid)["signature_bundle"])
        log_key = Ed25519PrivateKey.generate()
        response = _response_for(envelope, log_key)
        if corrupt:
            # One sibling hash changed. The Merkle walk no longer reaches the
            # root the signed checkpoint commits to.
            response["verification"]["inclusionProof"]["hashes"][0] = "00" * 32
        conn.execute(
            "INSERT INTO rekor_inclusions (claim_id, uuid, log_index, "
            "integrated_time, raw_response_b64, recorded_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (cid, "abc123", 3, 1700000000,
             base64.standard_b64encode(
                 json.dumps(response).encode()).decode("ascii"),
             "2026-05-27T00:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()

    if pin_key:
        (root / ".mareforma" / "rekor_log_pubkey.pem").write_bytes(
            _pubkey_pem(log_key))
    return key, cid


def _witnessing(root: Path, key, cid):
    with mareforma.open(root, key_path=key) as g:
        return g.trust_map(cid).get("witnessing")


class TestAProofThatChecksOut:
    def test_it_says_verified(self, tmp_path: Path) -> None:
        key, cid = _witnessed_claim(tmp_path)
        axis = _witnessing(tmp_path, key, cid)
        assert axis.value == "inclusion proof verified"
        assert "re-verified on this read" in axis.residual

    def test_the_old_wording_is_gone(self, tmp_path: Path) -> None:
        """It said the proof was not re-checked, which was true and is not now."""
        key, cid = _witnessed_claim(tmp_path)
        axis = _witnessing(tmp_path, key, cid)
        assert "not re-checked" not in axis.residual


class TestAProofThatDoesNot:
    def test_a_broken_merkle_path_reads_tampered(self, tmp_path: Path) -> None:
        key, cid = _witnessed_claim(tmp_path, corrupt=True)
        axis = _witnessing(tmp_path, key, cid)
        assert axis.value == "TAMPERED"
        assert "merkle_root_mismatch" in axis.residual

    def test_a_junk_record_reads_tampered(self, tmp_path: Path) -> None:
        """The row the table's own guards permit anyone to INSERT."""
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        (tmp_path / ".mareforma" / "rekor_log_pubkey.pem").write_bytes(
            _pubkey_pem(Ed25519PrivateKey.generate()))
        conn = open_db(tmp_path)
        try:
            conn.execute(
                "INSERT INTO rekor_inclusions (claim_id, uuid, log_index, "
                "integrated_time, raw_response_b64, recorded_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (cid, "planted", 1, 1, "eyJ0ZXN0IjogdHJ1ZX0=",
                 "2026-05-27T00:00:00Z"),
            )
            conn.commit()
        finally:
            conn.close()
        assert _witnessing(tmp_path, key, cid).value == "TAMPERED"

    def test_a_proof_signed_by_another_log_reads_tampered(
        self, tmp_path: Path,
    ) -> None:
        """A well-formed proof from a log this project never pinned.

        Checking the Merkle path alone would pass it: the arithmetic is sound
        and the tree is real. What it is missing is the signature of the log
        that would have to stand behind it.
        """
        key, cid = _witnessed_claim(tmp_path, pin_key=False)
        (tmp_path / ".mareforma" / "rekor_log_pubkey.pem").write_bytes(
            _pubkey_pem(Ed25519PrivateKey.generate()))
        axis = _witnessing(tmp_path, key, cid)
        assert axis.value == "TAMPERED"
        assert "checkpoint" in axis.residual


class TestNoKeyToCheckAgainst:
    def test_it_says_unchecked_not_verified(self, tmp_path: Path) -> None:
        """The state most projects are in, and it must not read as either
        outcome. Nothing was checked: that is not evidence for the entry and it
        is not evidence against it."""
        key, cid = _witnessed_claim(tmp_path, pin_key=False)
        axis = _witnessing(tmp_path, key, cid)
        assert axis.value == "inclusion record present, unchecked"
        assert "no transparency-log public key is pinned" in axis.residual

    def test_it_is_not_tampered(self, tmp_path: Path) -> None:
        key, cid = _witnessed_claim(tmp_path, pin_key=False)
        assert _witnessing(tmp_path, key, cid).value != "TAMPERED"

    def test_the_residual_says_how_to_get_one(self, tmp_path: Path) -> None:
        key, cid = _witnessed_claim(tmp_path, pin_key=False)
        assert "rekor_log_pubkey_pem" in _witnessing(tmp_path, key, cid).residual


class TestTheKeyComesFromTheProject:
    def test_it_is_read_off_the_pinned_file(self, tmp_path: Path) -> None:
        """Not fetched. A read makes no network call to decide what it trusts,
        and the pin is what an earlier open recorded."""
        from mareforma.trust_map import _pinned_log_pubkey

        key, cid = _witnessed_claim(tmp_path)
        conn = open_db(tmp_path)
        try:
            assert _pinned_log_pubkey(conn) == (
                tmp_path / ".mareforma" / "rekor_log_pubkey.pem").read_bytes()
        finally:
            conn.close()

    def test_no_pin_is_none_not_an_error(self, tmp_path: Path) -> None:
        from mareforma.trust_map import _pinned_log_pubkey

        conn = open_db(tmp_path)
        try:
            assert _pinned_log_pubkey(conn) is None
        finally:
            conn.close()


class TestAClaimWithNoRecord:
    def test_is_untouched(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        axis = _witnessing(tmp_path, key, cid)
        assert axis.value == "not witnessed"


class TestTheVerdictReadsIt:
    def test_a_failed_proof_fails_the_verdict(self, tmp_path: Path) -> None:
        """A tamper report the exit code ignores is one nobody's CI sees."""
        from mareforma._verify import TAMPERED, classify_claim_verdict

        key, cid = _witnessed_claim(tmp_path, corrupt=True)
        conn = open_db(tmp_path)
        try:
            verdict = classify_claim_verdict(conn, get_claim(conn, cid), cid)
        finally:
            conn.close()
        assert verdict.verdict == TAMPERED
        assert "transparency-log inclusion record does not verify" in verdict.reason

    def test_an_unchecked_record_does_not(self, tmp_path: Path) -> None:
        """No pinned key is the ordinary state, not a finding. Failing every
        claim in every project that never pinned one would say nothing about
        any of them."""
        from mareforma._verify import VERIFIED, classify_claim_verdict

        key, cid = _witnessed_claim(tmp_path, pin_key=False)
        conn = open_db(tmp_path)
        try:
            assert classify_claim_verdict(
                conn, get_claim(conn, cid), cid).verdict == VERIFIED
        finally:
            conn.close()

    def test_a_verifying_proof_does_not(self, tmp_path: Path) -> None:
        from mareforma._verify import VERIFIED, classify_claim_verdict

        key, cid = _witnessed_claim(tmp_path)
        conn = open_db(tmp_path)
        try:
            assert classify_claim_verdict(
                conn, get_claim(conn, cid), cid).verdict == VERIFIED
        finally:
            conn.close()
