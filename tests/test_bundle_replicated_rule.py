"""The bundle's REPLICATED check is the graph's rule, not a paraphrase of it.

``_CorroborationIndex`` holds that a stored REPLICATED is legitimate on either
of two paths: an enrolled validator's signed replication verdict naming the
claim, or convergence, meaning a shared ESTABLISHED anchor carrying a peer under
a distinct asserter key whose artifact hash does not collapse into this one's.

The bundle verifier applied neither. It asked whether two distinct asserters
shared any upstream: weaker than convergence in three ways, and blind to the
verdict path in full. The blindness was the worse half, because it rejected
rather than admitted: mareforma exported bundles it then refused to verify.

One term of convergence cannot travel. No node carries observed_grounding, so
the bundle applies the graph's rule minus the grounding gate, which makes it a
weaker check than the read path rather than a different one.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import mareforma
from mareforma import signing as _signing
from mareforma.db.core import open_db
from mareforma.export_bundle import (
    BundleVerificationError,
    build_statement,
    sign_bundle,
    verify_bundle,
    write_bundle,
    _distinct_artifact,
)
from tests._helpers import _bootstrap_key, _enroll_key, _load_signer
from tests.test_export_bundle import _bootstrap


def _resign(root: Path, statement: dict, pk, name: str = "bundle.json") -> Path:
    bundle_path = root / name
    bundle_path.write_text(json.dumps(sign_bundle(statement, pk)),
                           encoding="utf-8")
    return bundle_path


class TestTheVerdictPath:
    """The half that made the exporter contradict its own verifier."""

    def _verdict_promoted(self, root: Path):
        key_path, pk = _bootstrap(root)
        member_key = _bootstrap_key(root, "member.key")
        _enroll_key(root, key_path, member_key, identity="member@example.org")
        with mareforma.open(root, key_path=key_path) as g:
            # Signed by a distinct key: a verdict issuer cannot verdict a claim
            # it authored.
            cid = g.assert_claim("promoted by a signed replication verdict",
                                 generated_by="x",
                                 signer=_load_signer(member_key))
            g.record_replication_verdict(
                verdict_id="v1", cluster_id="c1", member_claim_id=cid,
                other_claim_id=None, method="cross-method",
            )
        return key_path, pk, cid

    def test_the_graph_promotes_it(self, root_tmp: Path) -> None:
        """The premise. Without this the rest proves nothing."""
        _, _, cid = self._verdict_promoted(root_tmp)
        conn = open_db(root_tmp)
        try:
            level = conn.execute(
                "SELECT support_level FROM claims WHERE claim_id = ?", (cid,),
            ).fetchone()[0]
        finally:
            conn.close()
        assert level == "REPLICATED"

    def test_its_own_bundle_verifies(self, root_tmp: Path) -> None:
        """This raised. A level the graph granted, refused by the verifier the
        same package ships, over a graph nobody had touched."""
        _, pk, _ = self._verdict_promoted(root_tmp)
        bundle_path = root_tmp / "bundle.json"
        write_bundle(root_tmp, bundle_path, pk)
        verify_bundle(bundle_path, pk.public_key())

    def test_the_verdict_travels_in_the_bundle(self, root_tmp: Path) -> None:
        _, _, cid = self._verdict_promoted(root_tmp)
        carried = build_statement(root_tmp)["predicate"]["mare:replicationVerdicts"]
        assert [v["verdict_id"] for v in carried] == ["v1"]
        assert carried[0]["member_claim_id"] == cid

    def test_a_forged_verdict_does_not_back_the_level(
        self, root_tmp: Path,
    ) -> None:
        """Carrying the verdicts is only worth it if they are checked."""
        _, pk, _ = self._verdict_promoted(root_tmp)
        statement = build_statement(root_tmp)
        for v in statement["predicate"]["mare:replicationVerdicts"]:
            v["signature"] = "AAAA"
        with pytest.raises(BundleVerificationError, match="REPLICATED"):
            verify_bundle(_resign(root_tmp, statement, pk), pk.public_key())

    def test_a_verdict_from_a_stranger_does_not_back_the_level(
        self, root_tmp: Path,
    ) -> None:
        """A good signature is not enough. The key has to belong to the bundle.

        The issuer must be a validator whose enrollment chain verified inside
        this bundle, which is what makes the level checkable offline by
        somebody holding nothing else. Here the verdict is re-signed by a key
        that never enrolled, so the signature is perfectly good and the key is
        a stranger. It names nobody, and the level it would have backed has to
        stand on the convergence path or fail.
        """
        import base64

        from mareforma.db import _replication_verdict_pae

        _, pk, _ = self._verdict_promoted(root_tmp)
        stranger = _load_signer(_bootstrap_key(root_tmp, "stranger.key"))
        statement = build_statement(root_tmp)
        for v in statement["predicate"]["mare:replicationVerdicts"]:
            record = {
                "verdict_id": v["verdict_id"],
                "cluster_id": v["cluster_id"],
                "member_claim_id": v["member_claim_id"],
                "other_claim_id": v["other_claim_id"],
                "method": v["method"],
                "confidence": v.get("confidence") or {},
            }
            v["issuer_keyid"] = _signing.public_key_id(stranger.public_key())
            v["signature"] = base64.standard_b64encode(
                stranger.sign(_replication_verdict_pae(record))
            ).decode("ascii")
        with pytest.raises(BundleVerificationError, match="REPLICATED"):
            verify_bundle(_resign(root_tmp, statement, pk), pk.public_key())

    def test_a_verdict_naming_another_claim_does_not_back_it(
        self, root_tmp: Path,
    ) -> None:
        """A verdict is evidence about the claims it names and no others."""
        _, pk, _ = self._verdict_promoted(root_tmp)
        statement = build_statement(root_tmp)
        for v in statement["predicate"]["mare:replicationVerdicts"]:
            v["member_claim_id"] = "someone-else"
        with pytest.raises(BundleVerificationError, match="REPLICATED"):
            verify_bundle(_resign(root_tmp, statement, pk), pk.public_key())


class TestTheConvergencePath:
    def _converged(self, root: Path):
        root_key = root / "root.key"
        _signing.bootstrap_key(root_key)
        root_pk = _signing.load_private_key(root_key)
        val_key = root / "val.key"
        _signing.bootstrap_key(val_key)
        val_pem = _signing.public_key_to_pem(
            _signing.load_private_key(val_key).public_key())
        with mareforma.open(root, key_path=root_key) as g:
            seed = g.assert_claim("anchor", generated_by="seed", seed=True)
            g.enroll_validator(val_pem, identity="v")
            a = g.assert_claim("converged", supports=[seed], generated_by="A",
                               signer=root_pk)
            g.assert_claim("converged", supports=[seed], generated_by="B",
                           signer=_signing.load_private_key(val_key))
            assert g.get_claim(a)["support_level"] == "REPLICATED"
        return root_pk, seed, a

    def test_a_genuine_convergence_verifies(self, root_tmp: Path) -> None:
        root_pk, _, _ = self._converged(root_tmp)
        bundle_path = root_tmp / "bundle.json"
        write_bundle(root_tmp, bundle_path, root_pk)
        verify_bundle(bundle_path, root_pk.public_key())

    def test_the_anchor_has_to_be_established(self, root_tmp: Path) -> None:
        """The old check took any shared upstream.

        Convergence is corroboration on an anchor the graph already trusts. A
        pair agreeing on an unvetted upstream is two claims agreeing, which is
        what PRELIMINARY already means.
        """
        root_pk, seed, _ = self._converged(root_tmp)
        statement = build_statement(root_tmp)
        for node in statement["predicate"]["@graph"]:
            if node.get("@id") == f"mare:claim/{seed}":
                node["supportLevel"] = "PRELIMINARY"
        with pytest.raises(BundleVerificationError, match="REPLICATED"):
            verify_bundle(_resign(root_tmp, statement, root_pk),
                          root_pk.public_key())

    def test_the_same_artifact_under_two_keys_is_not_corroboration(
        self, root_tmp: Path,
    ) -> None:
        """One result signed twice. The old check counted it as two."""
        root_pk, _, _ = self._converged(root_tmp)
        statement = build_statement(root_tmp)
        for node in statement["predicate"]["@graph"]:
            if node.get("claimText") == "converged":
                node["artifactHash"] = "sha256:" + "ab" * 32
        with pytest.raises(BundleVerificationError, match="REPLICATED"):
            verify_bundle(_resign(root_tmp, statement, root_pk),
                          root_pk.public_key())

    def test_two_recorded_artifacts_that_differ_do_corroborate(
        self, root_tmp: Path,
    ) -> None:
        """Built rather than edited: artifactHash is inside the signed
        statement, so a bundle with it rewritten fails on the digest before
        the level is ever examined."""
        root_key = root_tmp / "root.key"
        _signing.bootstrap_key(root_key)
        root_pk = _signing.load_private_key(root_key)
        val_key = root_tmp / "val.key"
        _signing.bootstrap_key(val_key)
        val_pem = _signing.public_key_to_pem(
            _signing.load_private_key(val_key).public_key())
        with mareforma.open(root_tmp, key_path=root_key) as g:
            seed = g.assert_claim("anchor", generated_by="seed", seed=True)
            g.enroll_validator(val_pem, identity="v")
            a = g.assert_claim("converged", supports=[seed], generated_by="A",
                               signer=root_pk,
                               artifact_hash="11" * 32)
            g.assert_claim("converged", supports=[seed], generated_by="B",
                           signer=_signing.load_private_key(val_key),
                           artifact_hash="22" * 32)
            assert g.get_claim(a)["support_level"] == "REPLICATED"
        bundle_path = root_tmp / "bundle.json"
        write_bundle(root_tmp, bundle_path, root_pk)
        verify_bundle(bundle_path, root_pk.public_key())

    def test_both_terms_must_hold_on_one_peer(self, root_tmp: Path) -> None:
        """Some peer with a distinct key and some peer with a distinct artifact
        is not a corroboration by anybody. The two have to be the same claim."""
        root_pk, seed, a = self._converged(root_tmp)
        statement = build_statement(root_tmp)
        for node in statement["predicate"]["@graph"]:
            if node.get("claimText") != "converged":
                continue
            # The peer shares this claim's artifact, so it cannot corroborate.
            # A third node on the anchor carries a different one but no
            # signature, so it is not a peer at all.
            node["artifactHash"] = "sha256:" + "cd" * 32
        statement["predicate"]["@graph"].append({
            "@type": "mare:Claim", "@id": "mare:claim/unsigned-bystander",
            "supports": [seed], "artifactHash": "sha256:" + "ef" * 32,
            "supportLevel": "PRELIMINARY",
        })
        with pytest.raises(BundleVerificationError, match="REPLICATED"):
            verify_bundle(_resign(root_tmp, statement, root_pk),
                          root_pk.public_key())


class TestTheArtifactTermMatchesTheGraph:
    """``(? IS NULL OR c.artifact_hash IS NULL OR c.artifact_hash != ?)``.

    Read as "the hashes must differ" it rejects every honest convergence
    between two claims that recorded no artifact, which is the ordinary case
    for a text finding. A missing hash records that no artifact was named, not
    that the same one was.
    """

    @pytest.mark.parametrize("own,peer,expected", [
        (None, None, True),
        (None, "sha256:aa", True),
        ("sha256:aa", None, True),
        ("sha256:aa", "sha256:bb", True),
        ("sha256:aa", "sha256:aa", False),
    ])
    def test_it_agrees_with_the_sql(self, own, peer, expected) -> None:
        assert _distinct_artifact(own, peer) is expected


@pytest.fixture
def root_tmp(tmp_path: Path) -> Path:
    return tmp_path
