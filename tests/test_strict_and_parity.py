"""strict_promotion (opt-in write gate) + multi-role read-path parity.

Multi-role parity: a forged role signature is caught on the live read path
(``mareforma verify`` / ``verify_claim_signatures``), not only at restore.

This file also held the strict-promotion tests. That flag made a project rule
out of a promotion requirement, and nothing is promoted any more.
"""
from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

import mareforma
from mareforma import signing
from mareforma.cli import cli
from mareforma.db import open_db, verify_claim_signatures




class TestVerifyClaimSignatures:
    def test_unsigned_claim_passes(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as g:  # no key → unsigned
            cid = g.assert_claim("unsigned", classification="ANALYTICAL")
            claim = g.get_claim(cid)
        conn = open_db(tmp_path)
        ok, reason = verify_claim_signatures(conn, claim)
        conn.close()
        assert ok and reason == ""

    def test_signed_claim_verifies(self, tmp_path: Path) -> None:
        kp = tmp_path / "k"
        signing.bootstrap_key(kp)
        with mareforma.open(tmp_path, key_path=kp) as g:
            cid = g.assert_claim("signed", classification="ANALYTICAL")
            claim = g.get_claim(cid)
        conn = open_db(tmp_path)
        ok, _ = verify_claim_signatures(conn, claim)
        conn.close()
        assert ok

    def test_forged_role_signature_caught_on_read(self, tmp_path: Path) -> None:
        # Multi-role parity: append a bogus reviewer role signature to a signed
        # claim's bundle. verify_claim_signatures (what `mareforma verify` runs
        # at any tier) must reject it, mirroring restore's rule.
        kp = tmp_path / "k"
        signing.bootstrap_key(kp)
        with mareforma.open(tmp_path, key_path=kp) as g:
            cid = g.assert_claim("has a forged role", classification="ANALYTICAL")
            claim = g.get_claim(cid)
        env = json.loads(claim["signature_bundle"])
        # The asserter keyid IS enrolled (root), so the forged sig is rejected
        # on the signature check, not merely on orphan-signer grounds.
        env["signatures"].append({
            "keyid": claim["asserter_keyid"],
            "sig": base64.standard_b64encode(b"x" * 64).decode("ascii"),
            "role": "reviewer",
        })
        claim["signature_bundle"] = json.dumps(env)
        conn = open_db(tmp_path)
        ok, reason = verify_claim_signatures(conn, claim)
        conn.close()
        assert not ok
        assert "role signature" in reason

    def test_forged_role_makes_verify_cli_fail(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            kp = signing.default_key_path()
            kp.parent.mkdir(parents=True, exist_ok=True)
            signing.bootstrap_key(kp)
            with mareforma.open(".") as g:
                cid = g.assert_claim("x", classification="ANALYTICAL")
                claim = g.get_claim(cid)
            env = json.loads(claim["signature_bundle"])
            env["signatures"].append({
                "keyid": claim["asserter_keyid"],
                "sig": base64.standard_b64encode(b"z" * 64).decode("ascii"),
                "role": "planner",
            })
            conn = open_db(Path("."))
            conn.execute("UPDATE claims SET signature_bundle=? WHERE claim_id=?",
                         (json.dumps(env), cid))
            conn.commit()
            conn.close()
            res = r.invoke(cli, ["verify", cid])
            assert res.exit_code == 1, res.output

    def test_unknown_role_rejected(self, tmp_path: Path) -> None:
        kp = tmp_path / "k"
        signing.bootstrap_key(kp)
        with mareforma.open(tmp_path, key_path=kp) as g:
            cid = g.assert_claim("y", classification="ANALYTICAL")
            claim = g.get_claim(cid)
        env = json.loads(claim["signature_bundle"])
        env["signatures"].append({
            "keyid": claim["asserter_keyid"],
            "sig": base64.standard_b64encode(b"x" * 64).decode("ascii"),
            "role": "superuser",  # not in VALID_CLAIM_ROLES
        })
        claim["signature_bundle"] = json.dumps(env)
        conn = open_db(tmp_path)
        ok, _ = verify_claim_signatures(conn, claim)
        conn.close()
        assert not ok
