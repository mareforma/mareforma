"""tests/test_export_bundle.py — SCITT-style signed export bundle.

Covers:
  - build_statement produces in-toto Statement v1 shape with the right
    _type, predicateType, and urn:mareforma:claim:<uuid> subject names
  - sign_bundle yields a DSSE envelope verifiable with the keypair
  - verify_bundle round-trips an untampered bundle and returns the
    Statement
  - tampered claim text breaks subject-digest verification
  - a claim dropped with its subject entry and re-signed still verifies:
    the bundle binds the claims it carries, not the claim set
  - tampered bundle signature breaks DSSE verification
  - empty graph produces a valid bundle with zero subjects
  - cross-version skew (predicateType mismatch) is caught
  - the exported validator set chains to one root of trust, and the
    bundle is signed by that root
  - CLI commands `mareforma export --bundle` and `mareforma verify`
    round-trip
"""

from __future__ import annotations

import base64
import contextlib
import errno
import io
import json
import os
import stat
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner

import mareforma
import mareforma.export_bundle
from mareforma._atomic import atomic_write_bytes, atomic_write_text
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
    def test_missing_graph_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="No epistemic graph found"):
            build_statement(tmp_path)
        assert (tmp_path / ".mareforma").exists() is False

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

    def test_dropped_claim_still_verifies(self, tmp_path: Path) -> None:
        """A bundle binds the claims it carries, not the claim set.

        Delete a claim's node and its subject entry, re-sign with the same
        key, and verification passes with one fewer subject: nothing counts
        claims or chains them, so omission is outside the bound. Pinned so
        the bound is read here rather than rediscovered against a bundle
        someone published.
        """
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            headline = g.assert_claim("headline", generated_by="seed", seed=True)
            awkward = g.assert_claim(
                "awkward result", generated_by="lab", contradicts=[headline],
            )
        bundle_path = tmp_path / "bundle.json"
        write_bundle(tmp_path, bundle_path, pk)
        statement = verify_bundle(bundle_path, pk.public_key())
        assert len(statement["subject"]) == 2

        statement["predicate"]["@graph"] = [
            node for node in statement["predicate"]["@graph"]
            if node.get("@id") != f"mare:claim/{awkward}"
        ]
        statement["subject"] = [
            s for s in statement["subject"]
            if s["name"] != f"{SUBJECT_PREFIX}{awkward}"
        ]
        bundle_path.write_text(json.dumps(sign_bundle(statement, pk)))

        reduced = verify_bundle(bundle_path, pk.public_key())
        assert len(reduced["subject"]) == 1

    def test_module_docstring_states_the_completeness_bound(self) -> None:
        doc = " ".join(mareforma.export_bundle.__doc__.split())
        assert "not that they are all the claims in the graph" in doc

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

    def test_export_bundle_refuses_json(self, tmp_path: Path) -> None:
        """--json asks for stdout, --bundle writes a signed file. Dropping the
        flag would hand a caller redirecting stdout a banner instead of an
        envelope, and leave a file it never asked for."""
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            self._ensure_xdg(tmp_path)
            import mareforma
            with mareforma.open() as g:
                g.assert_claim("seeded", generated_by="seed", seed=True)
            result = runner.invoke(cli, ["export", "--bundle", "--json"])
            assert result.exit_code == 1, result.output
            assert "mutually exclusive" in result.output
            assert not Path("mareforma-bundle.json").exists()

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

    def test_verify_bundle_with_public_pem_only(self, tmp_path: Path) -> None:
        # A bundle is what an outside party receives, so the exported public
        # PEM must be enough to reach a verdict: no private key, and no chmod
        # 600 on material that is not a secret.
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            self._ensure_xdg(tmp_path)
            with mareforma.open() as g:
                g.assert_claim("seeded", generated_by="seed", seed=True)
            runner.invoke(cli, ["export", "--bundle"], catch_exceptions=False)
            shown = runner.invoke(cli, ["key", "show", "--pem"])
            assert shown.exit_code == 0, shown.output
            pub = Path("signer_pub.pem")
            pub.write_text(shown.output)
            assert b"PRIVATE" not in pub.read_bytes()

            result = runner.invoke(
                cli, ["verify", "mareforma-bundle.json", "--key", str(pub)],
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

    def _project_rooted_at_own_key(self, tmp_path: Path) -> tuple[Path, str]:
        """Bootstrap a default key, then root the project at a different one."""
        self._ensure_xdg(tmp_path)
        root_key = Path("project.key").resolve()
        _signing.bootstrap_key(root_key)
        import mareforma
        with mareforma.open(".", key_path=root_key) as g:
            g.assert_claim("seeded", generated_by="seed", seed=True)
        keyid = _signing.public_key_id(
            _signing.load_private_key(root_key).public_key()
        )
        return root_key, keyid

    def test_export_bundle_refuses_when_the_key_is_not_the_root(
        self, tmp_path: Path,
    ) -> None:
        """Signing with the default key when the project roots elsewhere would
        write a bundle no key can verify. Refuse at export instead."""
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            _, keyid = self._project_rooted_at_own_key(tmp_path)
            result = runner.invoke(cli, ["export", "--bundle"])
            assert result.exit_code != 0, result.output
            assert keyid[:12] in result.output
            assert "--key" in result.output
            assert not Path("mareforma-bundle.json").exists()

    def test_export_bundle_key_option_signs_with_the_root(
        self, tmp_path: Path,
    ) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            root_key, _ = self._project_rooted_at_own_key(tmp_path)
            result = runner.invoke(
                cli, ["export", "--bundle", "--key", str(root_key)],
                catch_exceptions=False,
            )
            assert result.exit_code == 0, result.output
            statement = verify_bundle(
                Path("mareforma-bundle.json"),
                _signing.load_private_key(root_key).public_key(),
            )
            assert len(statement["subject"]) == 1

    def test_export_bundle_refuses_pre_bootstrap_unsigned_rows(
        self, tmp_path: Path,
    ) -> None:
        """A claim added before bootstrap stays unsigned, and verify reads an
        unsigned row in a signed graph as tampered. Refuse at export instead of
        writing an artifact that accuses an honest graph."""
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            added = runner.invoke(cli, ["claim", "add", "before bootstrap"],
                                  catch_exceptions=False)
            assert added.exit_code == 0, added.output
            unsigned_id = added.output.split()[-1].strip()
            self._ensure_xdg(tmp_path)
            runner.invoke(cli, ["claim", "add", "after bootstrap"],
                          catch_exceptions=False)

            result = runner.invoke(cli, ["export", "--bundle"])
            assert result.exit_code != 0, result.output
            assert unsigned_id in result.output
            assert not Path("mareforma-bundle.json").exists()

    def test_bundle_signed_by_another_key_reads_unverifiable(
        self, tmp_path: Path,
    ) -> None:
        """A bundle whose signer is not the local key is UNVERIFIABLE (exit 2),
        never tampered: a wrong verification key is not proof of tamper. Pinning
        the signer's public PEM reaches the definite verdict."""
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            root_key, _ = self._project_rooted_at_own_key(tmp_path)
            runner.invoke(cli, ["export", "--bundle", "--key", str(root_key)],
                          catch_exceptions=False)

            result = runner.invoke(cli, ["verify", "mareforma-bundle.json"])
            assert result.exit_code == 2, result.output
            assert "unverifiable" in result.output.lower()

            shown = runner.invoke(
                cli, ["key", "show", "--pem", "--key-path", str(root_key)])
            assert shown.exit_code == 0, shown.output
            pub = Path("signer_pub.pem")
            pub.write_text(shown.output)

            pinned = runner.invoke(
                cli, ["verify", "mareforma-bundle.json", "--key", str(pub)])
            assert pinned.exit_code == 0, pinned.output
            assert "verified" in pinned.output

    def test_bundle_pinned_to_the_wrong_key_reads_tampered(
        self, tmp_path: Path,
    ) -> None:
        """Pinning a key the bundle was not signed with is a definite negative
        (exit 1), not the advice to pin a key that the caller already followed.
        This is the cross-operator path: a received bundle checked against the
        producer's public PEM."""
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            root_key, _ = self._project_rooted_at_own_key(tmp_path)
            runner.invoke(cli, ["export", "--bundle", "--key", str(root_key)],
                          catch_exceptions=False)

            other_key = Path("other.key").resolve()
            _signing.bootstrap_key(other_key)
            shown = runner.invoke(
                cli, ["key", "show", "--pem", "--key-path", str(other_key)])
            assert shown.exit_code == 0, shown.output
            pub = Path("other_pub.pem")
            pub.write_text(shown.output)

            result = runner.invoke(
                cli, ["verify", "mareforma-bundle.json", "--key", str(pub),
                      "--json"])
            payload = json.loads(result.output)
            assert payload["verdict"] == "tampered", result.output
            assert payload["exit_code"] == 1, result.output
            assert result.exit_code == 1, result.output

    def test_verify_export_dir_honours_the_key_pin(
        self, tmp_path: Path,
    ) -> None:
        """Naming the directory must verify the same bundle the file target
        does. Dropping --key here reads an authentic bundle as tampered."""
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            root_key, _ = self._project_rooted_at_own_key(tmp_path)
            runner.invoke(cli, ["export", "--bundle", "--key", str(root_key)],
                          catch_exceptions=False)
            export_dir = Path("export-dir")
            export_dir.mkdir()
            Path("mareforma-bundle.json").rename(
                export_dir / "mareforma-bundle.json"
            )

            result = runner.invoke(
                cli, ["verify", str(export_dir), "--key", str(root_key)],
            )
            assert result.exit_code == 0, result.output
            assert "verified" in result.output


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


# ---------------------------------------------------------------------------
# Support-level attestation (no exporter-only inflation)
# ---------------------------------------------------------------------------


class TestSupportLevelAttestation:
    def test_established_without_a_validation_signature_fails(
        self, tmp_path: Path,
    ) -> None:
        """A PRELIMINARY claim relabelled ESTABLISHED by the exporter, with no
        validator-signed promotion, must fail — the level cannot be inflated."""
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("really preliminary", generated_by="a")
        statement = build_statement(tmp_path)
        for node in statement["predicate"]["@graph"]:
            if node.get("@type") == "mare:Claim":
                node["supportLevel"] = "ESTABLISHED"
                node.pop("validationSignature", None)
                break
        bundle = sign_bundle(statement, pk)
        bundle_path = tmp_path / "bundle.json"
        bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
        with pytest.raises(
            BundleVerificationError, match="carries no validation signature",
        ):
            verify_bundle(bundle_path, pk.public_key())

    def test_replicated_without_corroboration_fails(
        self, tmp_path: Path,
    ) -> None:
        """A lone claim relabelled REPLICATED, with no second distinct-signer
        claim on a shared upstream, must fail."""
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            seed = g.assert_claim("anchor", generated_by="seed", seed=True)
            g.assert_claim("lone claim", supports=[seed], generated_by="a")
        statement = build_statement(tmp_path)
        for node in statement["predicate"]["@graph"]:
            if node.get("claimText") == "lone claim":
                node["supportLevel"] = "REPLICATED"
                break
        bundle = sign_bundle(statement, pk)
        bundle_path = tmp_path / "bundle.json"
        bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
        with pytest.raises(
            BundleVerificationError, match="no shared upstream carries a second",
        ):
            verify_bundle(bundle_path, pk.public_key())

    def test_genuine_established_and_replicated_verify(
        self, tmp_path: Path,
    ) -> None:
        """A real REPLICATED pair (distinct signers) promoted to ESTABLISHED by
        a third validator round-trips: the validation envelope and distinct
        signers are present and check out.

        The promotion carries a display label, the documented happy path:
        ``validatedBy`` is cosmetic text, not the signer's keyid, so the
        verifier must not compare the two."""
        root_key = tmp_path / "root.key"
        _signing.bootstrap_key(root_key)
        root_pk = _signing.load_private_key(root_key)
        val_key = tmp_path / "val.key"
        _signing.bootstrap_key(val_key)
        val2_key = tmp_path / "val2.key"
        _signing.bootstrap_key(val2_key)
        val_pem = _signing.public_key_to_pem(
            _signing.load_private_key(val_key).public_key()
        )
        val2_pem = _signing.public_key_to_pem(
            _signing.load_private_key(val2_key).public_key()
        )
        with mareforma.open(tmp_path, key_path=root_key) as g:
            seed = g.assert_claim("anchor", generated_by="seed", seed=True)
            g.enroll_validator(val_pem, identity="v")
            g.enroll_validator(val2_pem, identity="v2")
            rep = g.assert_claim(
                "converged", supports=[seed], generated_by="A", signer=root_pk,
            )
            g.assert_claim(
                "converged", supports=[seed], generated_by="B",
                signer=_signing.load_private_key(val_key),
            )
            assert g.get_claim(rep)["support_level"] == "REPLICATED"
        with mareforma.open(tmp_path, key_path=val2_key) as g:
            g.validate(rep, validated_by="reviewer@example.org")
            assert g.get_claim(rep)["support_level"] == "ESTABLISHED"
        bundle_path = tmp_path / "bundle.json"
        write_bundle(tmp_path, bundle_path, root_pk)
        statement = verify_bundle(bundle_path, root_pk.public_key())
        labels = {
            n["claimText"]: n.get("validatedBy")
            for n in statement["predicate"]["@graph"]
            if n.get("@type") == "mare:Claim"
        }
        assert labels["converged"] == "reviewer@example.org"
        levels = {
            n["claimText"]: n["supportLevel"]
            for n in statement["predicate"]["@graph"]
            if n.get("@type") == "mare:Claim"
        }
        assert levels["converged"] in ("ESTABLISHED", "REPLICATED")

    def test_validation_envelope_declaring_another_validator_fails(
        self, tmp_path: Path,
    ) -> None:
        """A validation envelope whose declared validator_keyid is not the key
        that signed it must fail: the signature alone would let one validator
        present a promotion as another validator's work."""
        root_key = tmp_path / "root.key"
        _signing.bootstrap_key(root_key)
        root_pk = _signing.load_private_key(root_key)
        other_key = tmp_path / "other.key"
        _signing.bootstrap_key(other_key)
        other_pk = _signing.load_private_key(other_key)
        other_pem = _signing.public_key_to_pem(other_pk.public_key())

        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(other_pem, identity="other")
            cid = g.assert_claim("borrowed identity", generated_by="a")

        envelope = _signing.sign_validation(
            {
                "claim_id": cid,
                "validator_keyid": _signing.public_key_id(other_pk.public_key()),
                "validated_at": "2026-01-01T00:00:00Z",
                "evidence_seen": [],
            },
            root_pk,  # signed by the root, but declaring the other validator
        )
        statement = build_statement(tmp_path)
        for node in statement["predicate"]["@graph"]:
            if node.get("claimText") == "borrowed identity":
                node["supportLevel"] = "ESTABLISHED"
                node["validationSignature"] = envelope
                break
        bundle_path = tmp_path / "bundle.json"
        bundle_path.write_text(
            json.dumps(sign_bundle(statement, root_pk)), encoding="utf-8",
        )
        with pytest.raises(
            BundleVerificationError, match="declares validator",
        ):
            verify_bundle(bundle_path, root_pk.public_key())

    def test_llm_typed_validator_cannot_back_established(
        self, tmp_path: Path,
    ) -> None:
        """An ESTABLISHED display backed by an llm-typed validator must fail.

        The graph refuses this promotion in process (LLMValidatorPromotionError),
        and the bundle carries each validator's enrollment-bound validator_type,
        so the verifier has to enforce the same human-witnessed rule.
        """
        root_key = tmp_path / "root.key"
        _signing.bootstrap_key(root_key)
        root_pk = _signing.load_private_key(root_key)
        bot_key = tmp_path / "bot.key"
        _signing.bootstrap_key(bot_key)
        bot_pk = _signing.load_private_key(bot_key)
        bot_pem = _signing.public_key_to_pem(bot_pk.public_key())

        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(bot_pem, identity="bot", validator_type="llm")
            cid = g.assert_claim("bot promoted", generated_by="a")

        # Build the validation envelope the graph would refuse to accept.
        envelope = _signing.sign_validation(
            {
                "claim_id": cid,
                "validator_keyid": _signing.public_key_id(bot_pk.public_key()),
                "validated_at": "2026-01-01T00:00:00Z",
                "evidence_seen": [],
            },
            bot_pk,
        )
        statement = build_statement(tmp_path)
        for node in statement["predicate"]["@graph"]:
            if node.get("claimText") == "bot promoted":
                node["supportLevel"] = "ESTABLISHED"
                node["validationSignature"] = envelope
                node.pop("validatedBy", None)
                break
        bundle_path = tmp_path / "bundle.json"
        bundle_path.write_text(
            json.dumps(sign_bundle(statement, root_pk)), encoding="utf-8",
        )
        with pytest.raises(BundleVerificationError, match="validator_type='llm'"):
            verify_bundle(bundle_path, root_pk.public_key())


# ---------------------------------------------------------------------------
# Exported validator chain (the bundle's trust anchor)
# ---------------------------------------------------------------------------


class TestExportedValidatorChain:
    """Every asserter key the bundle presents is looked up in the chain-verified
    validator set, so a bundle whose validator set does not descend from one
    root, or that is signed by a key other than that root, must be refused."""

    def _root_and_validator(self, tmp_path: Path):
        """A graph with the root plus one enrolled validator, and one claim."""
        root_key = tmp_path / "root.key"
        _signing.bootstrap_key(root_key)
        root_pk = _signing.load_private_key(root_key)
        val_key = tmp_path / "val.key"
        _signing.bootstrap_key(val_key)
        val_pk = _signing.load_private_key(val_key)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(
                _signing.public_key_to_pem(val_pk.public_key()), identity="v",
            )
            g.assert_claim("anchored", generated_by="a")
        return root_pk, val_pk

    def _write(self, tmp_path: Path, statement: dict, signer) -> Path:
        bundle_path = tmp_path / "bundle.json"
        bundle_path.write_text(
            json.dumps(sign_bundle(statement, signer)), encoding="utf-8",
        )
        return bundle_path

    def test_bundle_without_validators_is_refused(self, tmp_path: Path) -> None:
        """Stripping the validator set would leave per-claim asserters unchecked,
        so a bundle with no root of trust cannot verify."""
        root_pk, _ = self._root_and_validator(tmp_path)
        statement = build_statement(tmp_path)
        statement["predicate"]["mare:validators"] = []
        bundle_path = self._write(tmp_path, statement, root_pk)
        with pytest.raises(
            BundleVerificationError, match="exactly one root of trust, found 0",
        ):
            verify_bundle(bundle_path, root_pk.public_key())

    def test_second_self_enrolled_root_is_refused(self, tmp_path: Path) -> None:
        """Two self-enrolled roots mean two trust domains, and the caller's
        pinned key can only anchor one of them."""
        root_pk, _ = self._root_and_validator(tmp_path)
        statement = build_statement(tmp_path)
        validators = statement["predicate"]["mare:validators"]
        root_entry = next(
            v for v in validators if v["keyid"] == v["enrolled_by_keyid"]
        )
        rogue = dict(root_entry, keyid="ca" * 32, enrolled_by_keyid="ca" * 32)
        validators.append(rogue)
        bundle_path = self._write(tmp_path, statement, root_pk)
        with pytest.raises(
            BundleVerificationError, match="exactly one root of trust, found 2",
        ):
            verify_bundle(bundle_path, root_pk.public_key())

    def test_validator_enrolled_by_an_absent_key_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """A validator whose parent is not in the bundle has no verifiable
        enrollment, so its key cannot back any claim."""
        root_pk, _ = self._root_and_validator(tmp_path)
        statement = build_statement(tmp_path)
        for v in statement["predicate"]["mare:validators"]:
            if v["keyid"] != v["enrolled_by_keyid"]:
                v["enrolled_by_keyid"] = "ca" * 32
                break
        bundle_path = self._write(tmp_path, statement, root_pk)
        with pytest.raises(
            BundleVerificationError, match="enrolled by a key absent",
        ):
            verify_bundle(bundle_path, root_pk.public_key())

    def test_tampered_enrollment_envelope_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """The parent's signature over the enrollment is what admits a validator;
        a flipped byte in it drops the validator out of the chain."""
        root_pk, _ = self._root_and_validator(tmp_path)
        statement = build_statement(tmp_path)
        for v in statement["predicate"]["mare:validators"]:
            if v["keyid"] != v["enrolled_by_keyid"]:
                envelope = json.loads(v["enrollment_envelope"])
                sig = base64.standard_b64decode(envelope["signatures"][0]["sig"])
                flipped = bytes([sig[0] ^ 0xFF]) + sig[1:]
                envelope["signatures"][0]["sig"] = (
                    base64.standard_b64encode(flipped).decode("ascii")
                )
                v["enrollment_envelope"] = json.dumps(envelope)
                break
        bundle_path = self._write(tmp_path, statement, root_pk)
        with pytest.raises(
            BundleVerificationError, match="enrollment failed verification",
        ):
            verify_bundle(bundle_path, root_pk.public_key())

    def test_mutually_enrolled_validators_never_chain_to_the_root(
        self, tmp_path: Path,
    ) -> None:
        """Two validators that enroll each other verify against one another but
        descend from no root, so the walk to the root has to refuse them."""
        root_pk, _ = self._root_and_validator(tmp_path)
        statement = build_statement(tmp_path)
        pair = []
        for name in ("a", "b"):
            path = tmp_path / f"{name}.key"
            _signing.bootstrap_key(path)
            pair.append(_signing.load_private_key(path))
        for signer, (holder, identity) in zip(
            pair[::-1], [(pair[0], "a"), (pair[1], "b")],
        ):
            row = {
                "keyid": _signing.public_key_id(holder.public_key()),
                "pubkey_pem": base64.standard_b64encode(
                    _signing.public_key_to_pem(holder.public_key())
                ).decode("ascii"),
                "identity": identity,
                "validator_type": "human",
                "enrolled_at": "2026-01-01T00:00:00Z",
                "enrolled_by_keyid": _signing.public_key_id(signer.public_key()),
            }
            statement["predicate"]["mare:validators"].append(
                dict(row, enrollment_envelope=json.dumps(
                    _signing.sign_validator_enrollment(row, signer)
                ))
            )
        bundle_path = self._write(tmp_path, statement, root_pk)
        with pytest.raises(
            BundleVerificationError, match="does not chain to the root",
        ):
            verify_bundle(bundle_path, root_pk.public_key())

    def test_bundle_signed_by_a_non_root_validator_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """The pinned key must be the root the validators chain to, otherwise the
        caller trusts one key and the claims chain to another."""
        _, val_pk = self._root_and_validator(tmp_path)
        bundle_path = self._write(tmp_path, build_statement(tmp_path), val_pk)
        with pytest.raises(
            BundleVerificationError, match="must be signed by its root",
        ):
            verify_bundle(bundle_path, val_pk.public_key())

    def test_validator_set_with_no_self_parented_root_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """Reparenting the root leaves a validator set with no anchor. Every
        per-claim check downstream reads the returned set as chain-verified, so
        a set that anchors nowhere has to be refused before any of them run."""
        root_pk, _ = self._root_and_validator(tmp_path)
        statement = build_statement(tmp_path)
        for v in statement["predicate"]["mare:validators"]:
            if v["keyid"] == v["enrolled_by_keyid"]:
                v["enrolled_by_keyid"] = "ca" * 32
                break
        bundle_path = self._write(tmp_path, statement, root_pk)
        with pytest.raises(
            BundleVerificationError, match="exactly one root of trust, found 0",
        ):
            verify_bundle(bundle_path, root_pk.public_key())

    def test_claim_stripped_of_its_signature_bundle_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """A verified bundle always carries a chain-verified validator set, so
        every claim in it must carry its asserter signature. Dropping one is how
        an exporter would smuggle a row nothing but itself vouches for."""
        root_pk, _ = self._root_and_validator(tmp_path)
        statement = build_statement(tmp_path)
        stripped = ""
        for node in statement["predicate"]["@graph"]:
            if node.get("signatureBundle"):
                del node["signatureBundle"]
                stripped = node["@id"].split("mare:claim/")[-1]
                break
        assert stripped, "no claim node carried a signature bundle"
        bundle_path = self._write(tmp_path, statement, root_pk)
        with pytest.raises(
            BundleVerificationError,
            match=f"claim:{stripped} carries no signature bundle",
        ):
            verify_bundle(bundle_path, root_pk.public_key())


# ---------------------------------------------------------------------------
# Durable writes (a failed export keeps the previous artifact)
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _disk_full():
    """Every file opened for writing inside the block fails with ENOSPC on its
    first write, the way a full disk fails an export that already opened its
    target."""
    real_open = io.open

    class _Full:
        def __init__(self, f):
            self._f = f

        def write(self, data):
            raise OSError(errno.ENOSPC, "No space left on device")

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self._f.close()
            return False

        def __getattr__(self, name):
            return getattr(self._f, name)

    def _open(file, mode="r", *args, **kwargs):
        f = real_open(file, mode, *args, **kwargs)
        return _Full(f) if ("w" in mode or "a" in mode) else f

    with mock.patch("io.open", _open):
        yield


class TestDurableExportWrite:
    """An export that fails partway must leave the artifact it was overwriting
    exactly as it was. A truncated bundle on a path a third party already pulls
    from reads as tampering, and the CLI message says the export did not
    happen."""

    def test_failed_bundle_rewrite_keeps_the_previous_bundle(
        self, tmp_path: Path,
    ) -> None:
        key_path, pk = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("first export claim", generated_by="a")
        bundle_path = tmp_path / "bundle.json"
        write_bundle(tmp_path, bundle_path, pk)
        original = bundle_path.read_bytes()

        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("second export claim", generated_by="a")
        with _disk_full(), pytest.raises(OSError) as exc:
            write_bundle(tmp_path, bundle_path, pk)
        assert exc.value.errno == errno.ENOSPC

        assert bundle_path.read_bytes() == original
        statement = verify_bundle(bundle_path, pk.public_key())
        assert len(statement["subject"]) == 1
        assert not list(tmp_path.glob(".bundle.json.*.tmp"))

    @pytest.mark.parametrize(
        "args, name",
        [
            ([], "ontology.jsonld"),
            (["--format=in-toto-v1"], "mareforma-statement.json"),
            (["--format=ro-crate-1.2"], "ro-crate-metadata.json"),
            (["--format=prov-o"], "mareforma-prov-o.jsonld"),
        ],
    )
    def test_failed_export_keeps_the_previous_artifact(
        self, tmp_path: Path, monkeypatch, args: list[str], name: str,
    ) -> None:
        key_path, _ = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("first export claim", generated_by="a")
        monkeypatch.chdir(tmp_path)
        out_path = tmp_path / name
        runner = CliRunner()
        result = runner.invoke(cli, ["export", *args])
        assert result.exit_code == 0, result.output
        original = out_path.read_bytes()

        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("second export claim", generated_by="a")
        with _disk_full():
            result = runner.invoke(cli, ["export", *args])
        assert result.exit_code == 1, result.output

        assert out_path.read_bytes() == original
        assert b"second export claim" not in original
        assert not list(tmp_path.glob(f".{name}.*.tmp"))


@pytest.mark.skipif(os.name != "posix", reason="POSIX file modes")
class TestExportedArtifactPermissions:
    """An export is what someone else reads, so it lands with the permissions
    an ordinary write would have given it: the umask for a new file, its own
    mode for one that already exists. The temp file the atomic replace goes
    through is created 0o600, and that mode must not ride across the rename."""

    @pytest.fixture
    def umask_022(self):
        previous = os.umask(0o022)
        try:
            yield
        finally:
            os.umask(previous)

    def _export(self, tmp_path: Path) -> Path:
        runner = CliRunner()
        result = runner.invoke(cli, ["export"])
        assert result.exit_code == 0, result.output
        return tmp_path / "ontology.jsonld"

    def test_new_export_follows_the_umask(
        self, tmp_path: Path, monkeypatch, umask_022,
    ) -> None:
        key_path, _ = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("first export claim", generated_by="a")
        monkeypatch.chdir(tmp_path)
        out_path = self._export(tmp_path)
        mode = stat.S_IMODE(out_path.stat().st_mode)
        assert mode == 0o644, f"expected 0o644, got {oct(mode)}"

    def test_re_export_keeps_the_mode_the_target_had(
        self, tmp_path: Path, monkeypatch, umask_022,
    ) -> None:
        key_path, _ = _bootstrap(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("first export claim", generated_by="a")
        monkeypatch.chdir(tmp_path)
        out_path = self._export(tmp_path)
        out_path.chmod(0o640)

        with mareforma.open(tmp_path, key_path=key_path) as g:
            g.assert_claim("second export claim", generated_by="a")
        self._export(tmp_path)

        mode = stat.S_IMODE(out_path.stat().st_mode)
        assert mode == 0o640, f"expected 0o640, got {oct(mode)}"

    def test_a_caller_asking_for_0o600_still_gets_it(
        self, tmp_path: Path, umask_022,
    ) -> None:
        """The private key writes through the same helper and must not pick up
        the umask."""
        path = tmp_path / "secret"
        atomic_write_text(path, "x", mode=0o600)
        assert stat.S_IMODE(path.stat().st_mode) == 0o600

    def test_a_write_never_touches_the_process_umask(
        self, tmp_path: Path, monkeypatch, umask_022,
    ) -> None:
        """The umask is process-wide and has no reader, so probing it clears it
        for every other thread in the host process. A host thread creating a
        file in that window gets none of the host's masking, on a file that is
        not ours and that we never report. No write path may call it."""
        calls: list[int] = []
        real_umask = os.umask

        def recording_umask(mask: int) -> int:
            calls.append(mask)
            return real_umask(mask)

        existing = tmp_path / "existing.json"
        existing.write_text("{}")
        monkeypatch.setattr(os, "umask", recording_umask)

        atomic_write_text(tmp_path / "new.json", "{}")
        atomic_write_text(existing, "{}")
        atomic_write_text(tmp_path / "keyed.json", "{}", mode=0o600)
        atomic_write_bytes(tmp_path / "bytes.toml", b"x")

        assert calls == [], f"os.umask called with {[oct(m) for m in calls]}"

    def test_a_new_file_follows_a_umask_set_after_import(
        self, tmp_path: Path,
    ) -> None:
        """Sampling the umask once at import would pass under the usual 0o022
        and be wrong for any process that sets its own, which daemons do after
        their imports. The mode has to come from the umask in force now."""
        previous = os.umask(0o077)
        try:
            path = tmp_path / "strict.json"
            atomic_write_text(path, "{}")
            mode = stat.S_IMODE(path.stat().st_mode)
        finally:
            os.umask(previous)
        assert mode == 0o600, f"expected 0o600, got {oct(mode)}"

    def test_bytes_writes_stay_owner_only_by_default(
        self, tmp_path: Path, umask_022,
    ) -> None:
        """claims.toml and the private key go through atomic_write_bytes and
        are not made readable by the umask default the exports use."""
        path = tmp_path / "claims.toml"
        atomic_write_bytes(path, b"x")
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
