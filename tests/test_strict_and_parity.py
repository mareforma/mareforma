"""strict_promotion (opt-in write gate) + multi-role read-path parity.

strict_promotion requires non-NULL data on both sides of a REPLICATED pair; off
by default the signer axis alone promotes. It is a project rule: asking for it
root-signs a one-way policy every later opener is held to. Multi-role parity: a
forged role signature is now caught on the live read path (``mareforma verify``
/ ``verify_claim_signatures``), not only at restore.
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
from mareforma.db import (
    ProjectPolicyError,
    get_project_policy,
    open_db,
    strict_promotion_required,
    verify_claim_signatures,
)


def _converge(tmp_path: Path, *, strict: bool, ah1, ah2) -> tuple[str, str]:
    """Two distinct-signer claims sharing an ESTABLISHED anchor. Return levels.

    Only k1 (the root) passes the flag: declaring it is a root-signed act, and
    the second signer is bound by the project policy it wrote.
    """
    k1 = tmp_path / "k1"
    k2 = tmp_path / "k2"
    signing.bootstrap_key(k1)
    signing.bootstrap_key(k2)
    with mareforma.open(tmp_path, key_path=k1, strict_promotion=strict) as g:
        anchor = g.assert_claim("anchor", classification="ANALYTICAL", seed=True)
    with mareforma.open(tmp_path, key_path=k1, strict_promotion=strict) as g:
        c1 = g.assert_claim("finding one", classification="ANALYTICAL",
                            supports=[anchor], artifact_hash=ah1)
    with mareforma.open(tmp_path, key_path=k2) as g:
        c2 = g.assert_claim("finding two", classification="ANALYTICAL",
                            supports=[anchor], artifact_hash=ah2)
        return g.get_claim(c1)["support_level"], g.get_claim(c2)["support_level"]


class TestStrictPromotion:
    def test_off_by_default_promotes_on_signer_axis_without_data(
        self, tmp_path: Path,
    ) -> None:
        l1, l2 = _converge(tmp_path, strict=False, ah1=None, ah2=None)
        assert l1 == "REPLICATED" and l2 == "REPLICATED"

    def test_strict_blocks_promotion_when_data_absent(self, tmp_path: Path) -> None:
        l1, l2 = _converge(tmp_path, strict=True, ah1=None, ah2=None)
        assert l1 == "PRELIMINARY" and l2 == "PRELIMINARY"

    def test_strict_blocks_when_only_one_side_has_data(
        self, tmp_path: Path,
    ) -> None:
        l1, l2 = _converge(tmp_path, strict=True, ah1="a" * 64, ah2=None)
        assert l1 == "PRELIMINARY" and l2 == "PRELIMINARY"

    def test_strict_promotes_with_distinct_data_both_sides(
        self, tmp_path: Path,
    ) -> None:
        l1, l2 = _converge(tmp_path, strict=True, ah1="a" * 64, ah2="b" * 64)
        assert l1 == "REPLICATED" and l2 == "REPLICATED"

    def test_default_unchanged_with_distinct_data(self, tmp_path: Path) -> None:
        l1, l2 = _converge(tmp_path, strict=False, ah1="a" * 64, ah2="b" * 64)
        assert l1 == "REPLICATED" and l2 == "REPLICATED"


    def test_the_gate_is_recorded_as_root_signed_project_state(
        self, tmp_path: Path,
    ) -> None:
        """Asking for the gate declares it: the rule lands in a root-signed
        policy row, so it outlives the handle that asked for it and no later
        opener can promote around it."""
        k1 = tmp_path / "k1"
        signing.bootstrap_key(k1)
        with mareforma.open(tmp_path, key_path=k1, strict_promotion=True):
            pass
        conn = open_db(tmp_path)
        try:
            policy = get_project_policy(conn)
            assert strict_promotion_required(conn) is True
        finally:
            conn.close()
        assert policy["strict_promotion_required"] == 1
        assert policy["signer_keyid"] == signing.public_key_id(
            signing.load_private_key(k1).public_key()
        )
        # One-way: reopening without the flag does not clear it.
        with mareforma.open(tmp_path, key_path=k1):
            pass
        conn = open_db(tmp_path)
        try:
            assert strict_promotion_required(conn) is True
        finally:
            conn.close()

    def test_a_non_root_key_cannot_declare_the_gate(self, tmp_path: Path) -> None:
        """The declaration is root-signed, so a second signer asking for it is
        refused rather than handed a gate that binds only its own writes."""
        k1 = tmp_path / "k1"
        k2 = tmp_path / "k2"
        signing.bootstrap_key(k1)
        signing.bootstrap_key(k2)
        with mareforma.open(tmp_path, key_path=k1):
            pass
        with pytest.raises(ProjectPolicyError):
            mareforma.open(tmp_path, key_path=k2, strict_promotion=True)

    def test_a_keyless_open_cannot_declare_the_gate(self, tmp_path: Path) -> None:
        """No key, no declaration: the caller is told, not quietly given a
        per-handle gate."""
        k1 = tmp_path / "k1"
        signing.bootstrap_key(k1)
        with mareforma.open(tmp_path, key_path=k1):
            pass
        with pytest.raises(ProjectPolicyError):
            mareforma.open(tmp_path, key_path=k1, load_key=False,
                           strict_promotion=True)

    def test_declaring_the_gate_keeps_the_witnessing_rule(
        self, tmp_path: Path,
    ) -> None:
        """Extending the policy signs the union: a rule declared earlier is
        never dropped by the declaration that adds the next one."""
        k1 = tmp_path / "k1"
        signing.bootstrap_key(k1)
        with mareforma.open(tmp_path, key_path=k1) as g:
            g.require_rekor_witnessing()
        with mareforma.open(tmp_path, key_path=k1, strict_promotion=True):
            pass
        conn = open_db(tmp_path)
        try:
            policy = get_project_policy(conn)
        finally:
            conn.close()
        assert policy["rekor_required"] == 1
        assert policy["strict_promotion_required"] == 1


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
