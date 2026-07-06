"""tests/test_export_bundle.py — SCITT-style signed export bundle.

Covers:
  - build_statement produces in-toto Statement v1 shape with the right
    _type, predicateType, and urn:mareforma:claim:<uuid> subject names
  - sign_bundle yields a DSSE envelope verifiable with the keypair
  - verify_bundle round-trips an untampered bundle and returns the
    Statement
  - tampered claim text breaks subject-digest verification
  - tampered bundle signature breaks DSSE verification
  - empty graph produces a valid bundle with zero subjects
  - cross-version skew (predicateType mismatch) is caught
  - CLI commands `mareforma export --bundle` and `mareforma verify`
    round-trip
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

import mareforma
from mareforma import signing as _signing
from mareforma.cli import cli
from mareforma.export_bundle import (
    BUNDLE_PAYLOAD_TYPE,
    BundleVerificationError,
    PREDICATE_TYPE,
    STATEMENT_TYPE,
    SUBJECT_PREFIX,
    build_statement,
    sign_bundle,
    verify_bundle,
    write_bundle,
)


def _bootstrap(tmp_path: Path):
    key_path = tmp_path / "k"
    _signing.bootstrap_key(key_path)
    return key_path, _signing.load_private_key(key_path)


# ---------------------------------------------------------------------------
# Statement shape
# ---------------------------------------------------------------------------


class TestStatementShape:
    def test_statement_has_intoto_type(self, tmp_path: Path) -> None:
        key_path, _ = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("c1", generated_by="seed", seed=True)
        statement = build_statement(tmp_path)
        assert statement["_type"] == STATEMENT_TYPE
        assert statement["_type"] == "https://in-toto.io/Statement/v1"

    def test_predicate_type_is_urn(self, tmp_path: Path) -> None:
        """URN namespace deliberately avoids a DNS perpetual-ownership
        commitment on mareforma.dev — schema dereferencing is via
        docs, not URL fetch."""
        key_path, _ = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("c1", generated_by="seed", seed=True)
        statement = build_statement(tmp_path)
        assert statement["predicateType"] == PREDICATE_TYPE
        assert statement["predicateType"] == "urn:mareforma:predicate:epistemic-graph:v1"
        assert statement["predicateType"].startswith("urn:")

    def test_subject_names_use_urn_prefix(self, tmp_path: Path) -> None:
        key_path, _ = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim("c1", generated_by="seed", seed=True)
        statement = build_statement(tmp_path)
        assert len(statement["subject"]) == 1
        assert statement["subject"][0]["name"] == f"{SUBJECT_PREFIX}{cid}"
        assert "sha256" in statement["subject"][0]["digest"]


# ---------------------------------------------------------------------------
# DSSE envelope
# ---------------------------------------------------------------------------


class TestDSSEEnvelope:
    def test_bundle_payload_type_intoto(self, tmp_path: Path) -> None:
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("c1", generated_by="seed", seed=True)
        bundle = sign_bundle(build_statement(tmp_path), pk)
        assert bundle["payloadType"] == BUNDLE_PAYLOAD_TYPE

    def test_bundle_keyid_matches(self, tmp_path: Path) -> None:
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("c1", generated_by="seed", seed=True)
        bundle = sign_bundle(build_statement(tmp_path), pk)
        expected_keyid = _signing.public_key_id(pk.public_key())
        assert bundle["signatures"][0]["keyid"] == expected_keyid


# ---------------------------------------------------------------------------
# Round-trip verification
# ---------------------------------------------------------------------------


class TestRoundTripVerification:
    def test_untampered_bundle_verifies(self, tmp_path: Path) -> None:
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            seed = g.assert_claim("genesis", generated_by="seed", seed=True)
            g.assert_claim("a", supports=[seed], generated_by="A")
            g.assert_claim("b", supports=[seed], generated_by="B")
        bundle_path = tmp_path / "bundle.json"
        write_bundle(tmp_path, bundle_path, pk)
        statement = verify_bundle(bundle_path, pk.public_key())
        assert statement["predicateType"] == PREDICATE_TYPE
        # 3 claims → 3 subjects
        assert len(statement["subject"]) == 3

    def test_empty_graph_bundle_verifies(self, tmp_path: Path) -> None:
        key_path, pk = _bootstrap(tmp_path)
        # No claims — fresh graph.
        with mareforma.open(tmp_path, key_path=key_path):
            pass
        bundle_path = tmp_path / "empty.json"
        write_bundle(tmp_path, bundle_path, pk)
        statement = verify_bundle(bundle_path, pk.public_key())
        assert statement["subject"] == []


# ---------------------------------------------------------------------------
# Tamper detection
# ---------------------------------------------------------------------------


class TestTamperDetection:
    def test_tampered_signature_fails(self, tmp_path: Path) -> None:
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("c1", generated_by="seed", seed=True)
        bundle_path = tmp_path / "bundle.json"
        write_bundle(tmp_path, bundle_path, pk)
        # Corrupt the signature.
        bundle = json.loads(bundle_path.read_text())
        bundle["signatures"][0]["sig"] = base64.standard_b64encode(b"x" * 64).decode("ascii")
        bundle_path.write_text(json.dumps(bundle))
        with pytest.raises(BundleVerificationError, match="signature"):
            verify_bundle(bundle_path, pk.public_key())

    def test_tampered_claim_text_in_predicate_fails(
        self, tmp_path: Path,
    ) -> None:
        """Mutate a claim's text inside the predicate, re-sign the
        bundle as if we own the key. The per-claim subject digest
        check catches the mismatch — bundle DSSE verifies, but the
        claim digest no longer matches the canonical_payload of the
        tampered text."""
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("genesis", generated_by="seed", seed=True)
        bundle_path = tmp_path / "bundle.json"
        write_bundle(tmp_path, bundle_path, pk)

        # Decode, mutate predicate, re-sign with the same key.
        bundle = json.loads(bundle_path.read_text())
        statement = json.loads(base64.standard_b64decode(bundle["payload"]))
        # Change the first claim's text.
        for node in statement["predicate"]["@graph"]:
            if node.get("@type") == "mare:Claim":
                node["claimText"] = "TAMPERED VALUE"
                break
        # Re-sign so the DSSE check passes but the subject digest doesn't.
        bundle = sign_bundle(statement, pk)
        bundle_path.write_text(json.dumps(bundle))

        with pytest.raises(BundleVerificationError, match="digest mismatch"):
            verify_bundle(bundle_path, pk.public_key())

    def test_wrong_predicate_type_fails(self, tmp_path: Path) -> None:
        """Future v2 predicate type → v1 verifier refuses."""
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("c1", generated_by="seed", seed=True)
        bundle_path = tmp_path / "bundle.json"
        write_bundle(tmp_path, bundle_path, pk)

        bundle = json.loads(bundle_path.read_text())
        statement = json.loads(base64.standard_b64decode(bundle["payload"]))
        statement["predicateType"] = "urn:mareforma:predicate:epistemic-graph:v2"
        bundle = sign_bundle(statement, pk)
        bundle_path.write_text(json.dumps(bundle))

        with pytest.raises(BundleVerificationError, match="predicateType"):
            verify_bundle(bundle_path, pk.public_key())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


class TestCLI:
    def _ensure_xdg(self, tmp_path: Path) -> None:
        xdg = _signing.default_key_path()
        if not xdg.exists():
            _signing.bootstrap_key(xdg)

    def test_export_bundle_writes_file(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            self._ensure_xdg(tmp_path)
            # First assert a claim so the bundle has subjects.
            import mareforma
            with mareforma.open() as g:
                g.assert_claim("seeded", generated_by="seed", seed=True)
            result = runner.invoke(cli, ["export", "--bundle"],
                                   catch_exceptions=False)
            assert result.exit_code == 0, result.output
            assert "signed bundle" in result.output
            assert Path("mareforma-bundle.json").exists()

    def test_verify_bundle_round_trip(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            self._ensure_xdg(tmp_path)
            import mareforma
            with mareforma.open() as g:
                g.assert_claim("seeded", generated_by="seed", seed=True)
            runner.invoke(cli, ["export", "--bundle"], catch_exceptions=False)
            result = runner.invoke(
                cli, ["verify", "mareforma-bundle.json"],
                catch_exceptions=False,
            )
            assert result.exit_code == 0, result.output
            assert "verified" in result.output

    def test_verify_tampered_bundle_exit_1(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            self._ensure_xdg(tmp_path)
            import mareforma
            with mareforma.open() as g:
                g.assert_claim("seeded", generated_by="seed", seed=True)
            runner.invoke(cli, ["export", "--bundle"], catch_exceptions=False)
            # Corrupt the signature.
            bundle = json.loads(Path("mareforma-bundle.json").read_text())
            bundle["signatures"][0]["sig"] = base64.standard_b64encode(
                b"x" * 64
            ).decode("ascii")
            Path("mareforma-bundle.json").write_text(json.dumps(bundle))
            result = runner.invoke(cli, ["verify", "mareforma-bundle.json"])
            assert result.exit_code == 1
            assert "verification failed" in result.output.lower()


# ---------------------------------------------------------------------------
# DSSE PAE + per-claim asserter signature verification
# ---------------------------------------------------------------------------


class TestBundleInterop:
    def test_bundle_envelope_verifies_with_the_standard_verifier(
        self, tmp_path: Path,
    ) -> None:
        """The bundle signs over the DSSE PAE encoding, so mareforma's own
        verify_envelope (and any standard DSSE verifier) accepts it."""
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("interop claim", generated_by="a")
        bundle = sign_bundle(build_statement(tmp_path), pk)
        assert _signing.verify_envelope(bundle, pk.public_key()) is True


class TestPerClaimSignature:
    def test_valid_bundle_carries_and_verifies_per_claim_signatures(
        self, tmp_path: Path,
    ) -> None:
        """A signed graph's bundle carries each claim's signature bundle and its
        asserter set, and verify_bundle checks every per-claim signature."""
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("first signed", generated_by="a")
            g.assert_claim("second signed", generated_by="b")
        bundle_path = tmp_path / "bundle.json"
        write_bundle(tmp_path, bundle_path, pk)
        statement = verify_bundle(bundle_path, pk.public_key())
        nodes = statement["predicate"]["@graph"]
        signed = [n for n in nodes if n.get("signatureBundle")]
        assert len(signed) == 2  # both claims carry asserter signatures

    def test_asserter_signature_must_cover_the_displayed_content(
        self, tmp_path: Path,
    ) -> None:
        """A malicious exporter cannot show content the asserter never signed:
        even with the subject digest rewritten to match the tampered node (so
        the self-referential digest check passes), the asserter signature over
        the original content must fail the bundle."""
        import hashlib
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("Compound X is SAFE at 10mg", generated_by="a")
        statement = build_statement(tmp_path)
        for node in statement["predicate"]["@graph"]:
            if node.get("signatureBundle"):
                cid = node["@id"][len("mare:claim/"):]
                node["claimText"] = "Compound X is LETHAL at 10mg"
                # Rewrite the subject digest to match the tampered node so the
                # node-vs-subject digest check still passes.
                new_digest = hashlib.sha256(
                    _signing.canonical_statement({
                        "claim_id": cid,
                        "text": node["claimText"],
                        "classification": node.get("classification", "INFERRED"),
                        "generated_by": node.get("generatedBy", "agent"),
                        "supports": node.get("supports", []),
                        "contradicts": node.get("contradicts", []),
                        "source_name": node.get("sourceName"),
                        "artifact_hash": node.get("artifactHash"),
                        "created_at": node.get("dateCreated", ""),
                    }, node.get("evidence") or {})
                ).hexdigest()
                for s in statement["subject"]:
                    if s["name"] == f"{SUBJECT_PREFIX}{cid}":
                        s["digest"]["sha256"] = new_digest
                break
        bundle = sign_bundle(statement, pk)  # valid exporter signature
        bundle_path = tmp_path / "bundle.json"
        bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
        with pytest.raises(
            BundleVerificationError, match="does not cover the presented content",
        ):
            verify_bundle(bundle_path, pk.public_key())

    def test_tampered_per_claim_signature_fails_verification(
        self, tmp_path: Path,
    ) -> None:
        """A bundle whose exporter signature is valid but that contains a claim
        with an invalid asserter signature must fail. The digest check alone
        (self-referential to the exporter key) would pass it."""
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("tamper target", generated_by="a")
        statement = build_statement(tmp_path)
        # Corrupt one claim's asserter signature, then sign the bundle validly
        # with the exporter key (the malicious-exporter scenario).
        tampered = False
        for node in statement["predicate"]["@graph"]:
            sb = node.get("signatureBundle")
            if sb:
                raw = bytearray(
                    base64.standard_b64decode(sb["signatures"][0]["sig"])
                )
                raw[0] ^= 0xFF
                sb["signatures"][0]["sig"] = base64.standard_b64encode(
                    bytes(raw)
                ).decode("ascii")
                tampered = True
                break
        assert tampered, "no per-claim signature was carried into the bundle"
        bundle = sign_bundle(statement, pk)
        bundle_path = tmp_path / "bundle.json"
        bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
        with pytest.raises(BundleVerificationError):
            verify_bundle(bundle_path, pk.public_key())
