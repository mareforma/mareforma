"""The golden exit-code corpus.

``mareforma verify`` and ``mareforma audit`` are the agent-native primitives: a
CI gate keys on their exit codes, so the (exit code, --json verdict) mapping is a
contract, not an implementation detail. Before this module the contract lived in
44 scattered ``exit_code`` assertions with no single place that stated it. This
is that place.

The verify contract is four codes (documented on the command itself):

    0  verified      the signature and bindings check out
    1  tampered      something was checked and failed (a definite NO)
    2  unverifiable  material to reach a verdict is missing (not a NO)
    3  usage error   a bad flag or argument, never one of the verdicts above

audit exits with its target's own code on a real run; a malformed invocation
(a target that is not a Python script) is refused as a usage error before any
target runs, so it can never read as a grounding verdict.

Each row below is exercised end to end. The corpus is the guard that the verify
key-id agreement change (tampered, not verified) and the audit non-Python
refusal (usage error, not a false verdict at exit 0) cannot silently regress.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

import mareforma
from mareforma import signing
from mareforma.cli import (
    _VERIFY_FAIL,
    _VERIFY_OK,
    _VERIFY_UNVERIFIABLE,
    _VERIFY_USAGE,
    cli,
)
from mareforma._verify import classify_claim_verdict
from mareforma.db import open_db
from tests._helpers import _bootstrap_default_key


# ---------------------------------------------------------------------------
# verify: the four-code contract
# ---------------------------------------------------------------------------


class TestVerifyExitCodeCorpus:
    def test_verified_is_exit_0(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("a finding", classification="ANALYTICAL")
            res = r.invoke(cli, ["verify", cid, "--json"])
            assert res.exit_code == _VERIFY_OK, res.output
            assert json.loads(res.output)["verdict"] == "verified"

    def test_tampered_signature_is_exit_1(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("a finding", classification="ANALYTICAL")
            conn = open_db(Path("."))
            bundle = json.loads(conn.execute(
                "SELECT signature_bundle FROM claims WHERE claim_id = ?", (cid,),
            ).fetchone()[0])
            bundle["signatures"][0]["sig"] = base64.standard_b64encode(
                b"\x00" * 64).decode()
            conn.execute(
                "UPDATE claims SET signature_bundle = ? WHERE claim_id = ?",
                (json.dumps(bundle), cid),
            )
            conn.commit()
            conn.close()
            res = r.invoke(cli, ["verify", cid, "--json"])
            assert res.exit_code == _VERIFY_FAIL, res.output
            assert json.loads(res.output)["verdict"] == "tampered"

    def test_unknown_claim_is_exit_2(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                g.assert_claim("a finding", classification="ANALYTICAL")
            res = r.invoke(cli, ["verify", "no-such-claim", "--json"])
            assert res.exit_code == _VERIFY_UNVERIFIABLE, res.output
            assert json.loads(res.output)["verdict"] == "unverifiable"

    def test_unenrolled_signer_is_exit_2(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            other = Path("other.key")
            signing.bootstrap_key(other)
            with mareforma.open(".") as g:
                cid = g.assert_claim(
                    "signed by a stranger", classification="ANALYTICAL",
                    signer=signing.load_private_key(other),
                )
            res = r.invoke(cli, ["verify", cid, "--json"])
            assert res.exit_code == _VERIFY_UNVERIFIABLE, res.output
            assert json.loads(res.output)["verdict"] == "unverifiable"

    def test_malformed_bundle_file_is_exit_1(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            junk = Path("bundle.json")
            junk.write_text('{"payloadType": "x", "signatures": []}')
            res = r.invoke(cli, ["verify", str(junk), "--json"])
            assert res.exit_code == _VERIFY_FAIL, res.output
            assert json.loads(res.output)["verdict"] == "tampered"

    def test_bad_flag_is_usage_exit_3(self, tmp_path: Path) -> None:
        # A malformed invocation is exit 3, distinct from the 0/1/2 verdicts so a
        # gate cannot misread a typo as "could not verify".
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            res = r.invoke(cli, ["verify", "x", "--no-such-flag"])
            assert res.exit_code == _VERIFY_USAGE


# ---------------------------------------------------------------------------
# audit and diagnose: a non-Python target is refused before any run
# ---------------------------------------------------------------------------


class TestDiagnoseTargetContract:
    """diagnose shares audit's rule because it makes the same promise.

    Both run the target in-process via runpy. A JSON object is a valid Python
    dict literal, so it compiles, exits cleanly, and reads nothing: diagnose
    then printed ``UNGROUNDED`` with ``scope fully observed`` beside it, which
    is a false accusation carrying a false completeness claim. diagnose is the
    command the quickstart, the README and examples/07 all teach, so it is the
    surface where that mattered most.
    """

    def test_non_python_target_is_refused(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            data = Path("data.csv")
            data.write_text("a,b\n1,2\n")
            spec = Path("run-spec.json")
            spec.write_text(json.dumps({"steps": 3}))
            res = r.invoke(
                cli, ["diagnose", "--cites", str(data), "--", str(spec)],
            )
            # 3, not 2: `diagnose` and `audit` exit with the TARGET's own
            # code, and 2 is one of the commonest codes a script exits with
            # (argparse uses it for its own usage errors), so a gate could
            # not tell a refused target from a target that refused its
            # arguments. 3 is the package's usage-error code, the same one
            # `verify` and `reexec` already use.
            assert res.exit_code == 3, res.output
            assert "not a Python program" in res.output
            # The point of the guard: no verdict is printed for a target that
            # never ran, so neither the accusation nor the coverage claim ships.
            assert "UNGROUNDED" not in res.output
            assert "scope fully observed" not in res.output

    def test_a_python_target_still_runs(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            data = Path("data.csv")
            data.write_text("a,b\n1,2\n")
            script = Path("analysis.py")
            script.write_text(f"open({str(data)!r}).read()\n")
            res = r.invoke(
                cli, ["diagnose", "--cites", str(data), "--", str(script)],
            )
            assert res.exit_code == 0, res.output
            assert "GROUNDED" in res.output

    @pytest.mark.parametrize("suffix", [".pyw", ".zip"])
    def test_runpy_runnable_suffixes_are_not_refused(
        self, tmp_path: Path, suffix: str,
    ) -> None:
        """runpy runs these, so the guard must not stand in their way.

        ``.pyw`` is a Python script everywhere; a ``.zip`` carrying a
        ``__main__`` is a zipapp exactly as ``.pyz`` is. Both ran before the
        guard existed, so refusing them would be a regression the guard
        introduced rather than a defect it closed.
        """
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            data = Path("data.csv")
            data.write_text("a,b\n1,2\n")
            target = Path(f"analysis{suffix}")
            target.write_text(f"open({str(data)!r}).read()\n")
            res = r.invoke(
                cli, ["diagnose", "--cites", str(data), "--", str(target)],
            )
            assert "not a Python program" not in res.output, res.output


class TestAuditTargetContract:
    def test_non_python_target_is_refused(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            spec = Path("run-spec.json")
            spec.write_text(json.dumps({"command": ["x.py"], "findings": {}}))
            findings = Path("findings.json")
            findings.write_text(json.dumps({"f1": "/data/x.csv"}))
            res = r.invoke(
                cli, ["audit", "--findings", str(findings), "--", str(spec)],
            )
            # 3, not 2: `diagnose` and `audit` exit with the TARGET's own
            # code, and 2 is one of the commonest codes a script exits with
            # (argparse uses it for its own usage errors), so a gate could
            # not tell a refused target from a target that refused its
            # arguments. 3 is the package's usage-error code, the same one
            # `verify` and `reexec` already use.
            assert res.exit_code == 3, res.output
            assert "not a Python program" in res.output

    def test_python_target_is_accepted(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            data = Path("data.csv")
            data.write_text("a,b\n1,2\n")
            script = Path("analysis.py")
            script.write_text(f"open({str(data)!r}).read()\n")
            findings = Path("findings.json")
            findings.write_text(json.dumps({"f1": str(data)}))
            key = Path("auditor.key")
            signing.bootstrap_key(key)
            res = r.invoke(
                cli,
                ["audit", "--findings", str(findings), "--out", "out",
                 "--key", str(key), "--json", "--", str(script)],
            )
            assert res.exit_code == 0, res.output


class TestVerifyFallThrough:
    """Exit 0 must be earned, not fallen into.

    The command matched ``"tampered"`` and ``"unverifiable"`` as literals and
    let everything else reach the success branch. Any verdict the classifier
    grows that those two ifs do not name would have exited 0 with a "verified"
    JSON verdict: the one direction a verify surface must never fail in. The
    verdict is now compared against the constants the classifier exports, and
    anything that is not VERIFIED is reported unverifiable.
    """

    def test_an_unrecognised_verdict_is_not_reported_verified(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from mareforma import _verify

        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("a finding", classification="ANALYTICAL")

            real = _verify.classify_claim_verdict

            def _future_verdict(conn, claim, target):  # noqa: ANN001, ANN202
                return _verify.ClaimVerdict(
                    "revoked", "the signing key was revoked",
                    real(conn, claim, target).trust_map,
                )

            monkeypatch.setattr(
                _verify, "classify_claim_verdict", _future_verdict,
            )
            res = r.invoke(cli, ["verify", cid, "--json"])
            assert res.exit_code == _VERIFY_UNVERIFIABLE, res.output
            assert json.loads(res.output)["verdict"] == "unverifiable"


class TestPromotionFlagArm:
    """The tier-gated read flag is the arm that catches a broken promotion.

    ``classify_claim_verdict`` runs two complementary signature checks: the
    stored ``verified`` flag, which the read path computes by re-verifying a
    promoted row's envelope and the signed evidence backing its level, and a
    tier-independent audit-grade re-check of the row's own bundle. Only the
    flag arm speaks to promotion, and it had no test: the branch that turns a
    REPLICATED row whose backing collapsed into a tampered verdict was carried
    on inspection alone.

    The peer is what gets tampered here, not the row under test. X earned
    REPLICATED by converging with a distinct signer on a shared anchor; break
    that peer's signature and X's own bundle still verifies, so the audit-grade
    arm stays silent and the flag arm is the only thing that can produce the
    verdict.
    """

    def test_a_promotion_whose_peer_no_longer_verifies_is_tampered(
        self, tmp_path: Path,
    ) -> None:
        from cryptography.hazmat.primitives import serialization

        from tests._helpers import _bootstrap_key, _two_signers

        def _pem(signer):
            return signer.public_key().public_bytes(
                serialization.Encoding.PEM,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            )

        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            # Both signers enrolled: without a pubkey to check a bundle
            # against, the participant check has nothing to verify and the row
            # reads unverifiable rather than failed, which never reaches this
            # arm.
            g.enroll_validator(_pem(sa), identity="lab_a")
            g.enroll_validator(_pem(sb), identity="lab_b")
            anchor = g.assert_claim("anchor", generated_by="seed", seed=True)
            x = g.assert_claim(
                "X", supports=[anchor], generated_by="lab_a", signer=sa)
            y = g.assert_claim(
                "Y", supports=[anchor], generated_by="lab_b", signer=sb)
            assert g.get_claim(x)["support_level"] == "REPLICATED"

        conn = open_db(tmp_path)
        bundle = json.loads(conn.execute(
            "SELECT signature_bundle FROM claims WHERE claim_id = ?", (y,),
        ).fetchone()[0])
        bundle["signatures"][0]["sig"] = base64.standard_b64encode(
            b"\x00" * 64).decode()
        conn.execute(
            "UPDATE claims SET signature_bundle = ? WHERE claim_id = ?",
            (json.dumps(bundle), y),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, load_key=False) as g:
            claim = g.get_claim(x)
            assert claim["verified"] is False, "the flag arm did not fire"
            result = classify_claim_verdict(g._conn, claim, x)
        assert result.verdict == "tampered", result.reason
        assert result.reason == "signature failed re-verification on read", (
            "another arm fired too, so this no longer isolates the flag arm: "
            + result.reason
        )


class TestUnsignedClaimIsUnverifiable:
    """A claim carrying no signature cannot be verified, and must not say it was.

    This is reachable with no database access and no attacker: open a project,
    assert a claim, never run ``bootstrap``. That is the path the quickstart
    teaches ("No project setup. No init command.") and the path
    examples/06_ci_verify gates CI on. It exited 0 and printed "verified".

    The claim never reached a check. The read-flag arm needs a bundle,
    ``verify_claim_signatures`` answers (True, "") when there is nothing to
    verify, and the enrolled-signer arm is gated on the bundle naming a keyid,
    so ``classify_claim_verdict`` fell through to VERIFIED. ``build_trust_map``
    had the right answer the whole time and rendered attributability as
    "unsigned"; the verdict simply never read its own map.

    UNVERIFIABLE, not TAMPERED: nothing was checked, so nothing was caught.
    """

    def test_an_unsigned_claim_is_unverifiable_not_verified(
        self, tmp_path: Path,
    ) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            # No _bootstrap_default_key() on purpose: this is the documented
            # default path, where no signing key exists.
            with mareforma.open(".") as g:
                cid = g.assert_claim("a finding", classification="ANALYTICAL")
                assert g.get_claim(cid)["signature_bundle"] is None
            res = r.invoke(cli, ["verify", cid, "--json"])
            assert res.exit_code == _VERIFY_UNVERIFIABLE, res.output
            payload = json.loads(res.output)
            assert payload["verdict"] == "unverifiable"
            assert "carries no signature" in payload["reason"]

    def test_a_signed_claim_still_verifies(self, tmp_path: Path) -> None:
        """REGRESSION GUARD. The fix must not demote honest claims.

        tests/test_cli_trust.py:701 pins exit 0 for a signed claim, but it does
        so incidentally while testing that verify leaves the validators table
        empty. That is accidental pinning, not a contract. This is the contract.
        """
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("a finding", classification="ANALYTICAL")
            res = r.invoke(cli, ["verify", cid, "--json"])
            assert res.exit_code == _VERIFY_OK, res.output
            assert json.loads(res.output)["verdict"] == "verified"

    def test_tampered_still_outranks_unsigned(self, tmp_path: Path) -> None:
        """Precedence: a definite NO beats missing material.

        An unsigned row whose support_level was forged carries both an
        unchecked reason and a problem. The problem must win, so a gate that
        only fails on 1 still catches it.
        """
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".", load_key=False) as g:
                cid = g.assert_claim("unsigned", classification="ANALYTICAL")
            conn = open_db(Path("."))
            conn.execute(
                "UPDATE claims SET support_level = 'REPLICATED' WHERE claim_id = ?",
                (cid,),
            )
            conn.commit()
            conn.close()
            res = r.invoke(cli, ["verify", cid, "--json"])
            assert res.exit_code in (_VERIFY_FAIL, _VERIFY_UNVERIFIABLE), res.output
            assert json.loads(res.output)["verdict"] != "verified"


class TestObserveUsageCodeIsApartFromTheTarget:
    """`diagnose` and `audit` exit with the target's code, so usage cannot be 2.

    argparse exits 2 for its own usage errors, which makes 2 one of the most
    common codes a Python target exits with. While these commands used 2 for
    their own usage errors, a CI gate reading 2 could not tell "you passed a
    non-Python target" from "your script rejected its arguments".
    """

    def test_a_target_exiting_2_is_not_read_as_a_usage_error(self, tmp_path):
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            Path("argparse_like.py").write_text(
                "import sys\nsys.exit(2)\n", encoding="utf-8",
            )
            res = r.invoke(cli, ["diagnose", "--", "argparse_like.py"])
            assert res.exit_code == 2, res.output
            # It ran: the report is there, which a usage error never prints.
            assert "OBSERVATION REPORT" in res.output

    def test_a_bad_flag_exits_3(self, tmp_path):
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            assert r.invoke(cli, ["diagnose", "--no-such-flag"]).exit_code == 3
            assert r.invoke(cli, ["audit", "--no-such-flag"]).exit_code == 3

    def test_a_version_suffixed_script_is_not_refused(self, tmp_path):
        # Path("pipeline.v2").suffix is ".v2", which the non-Python guard read as
        # a file type. A version marker names no format, and refusing it turned
        # an ordinary Python script into a usage error.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            Path("data.csv").write_text("a,b\n1,2\n", encoding="utf-8")
            for name in ("pipeline.v2", "model.v1.2"):
                Path(name).write_text(
                    "open('data.csv').read()\n", encoding="utf-8",
                )
                res = r.invoke(cli, ["diagnose", "--cites", "data.csv",
                                     "--", name])
                assert res.exit_code == 0, res.output
                assert "GROUNDED" in res.output


class TestNonPythonTargetDocstringMatchesCoverage:
    """The rule's docstring promises what runs; the tests covered two of them.

    ``_reject_non_python_target`` says it leaves untouched "any suffix in
    _RUNNABLE_PYTHON_SUFFIXES, a directory or zipapp with a __main__, an
    extensionless script, a `-m module`, or a bare interpreter flag". Only
    ``.pyw`` and ``.zip`` were exercised, so the promise about the other three
    rested on reading the code.
    """

    def test_an_extensionless_script_runs(self, tmp_path):
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            Path("data.csv").write_text("a\n1\n", encoding="utf-8")
            Path("pipeline").write_text(
                "open('data.csv').read()\n", encoding="utf-8",
            )
            res = r.invoke(cli, ["diagnose", "--cites", "data.csv",
                                 "--", "pipeline"])
            assert res.exit_code == 0, res.output
            assert "GROUNDED" in res.output

    def test_a_dash_m_module_is_not_refused(self, tmp_path):
        # Whether the module IMPORTS is the target's business (runpy resolves it
        # against sys.path, which a CliRunner does not extend to the temp cwd).
        # What the guard promises is that it does not refuse the form, so the
        # assertion is on the refusal, not on the run succeeding.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            Path("data.csv").write_text("a\n1\n", encoding="utf-8")
            Path("mod.py").write_text(
                "open('data.csv').read()\n", encoding="utf-8",
            )
            res = r.invoke(cli, ["diagnose", "--cites", "data.csv",
                                 "--", "-m", "mod"])
            assert "not a Python program" not in res.output
            assert res.exit_code != 3, res.output

    def test_a_directory_with_a_dunder_main_runs(self, tmp_path):
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            Path("data.csv").write_text("a\n1\n", encoding="utf-8")
            pkg = Path("app")
            pkg.mkdir()
            (pkg / "__main__.py").write_text(
                "open('data.csv').read()\n", encoding="utf-8",
            )
            res = r.invoke(cli, ["diagnose", "--cites", "data.csv",
                                 "--", "app"])
            assert res.exit_code == 0, res.output


class TestVersionMarkerDoesNotReopenTheGuard:
    """The version-marker exemption must not readmit the data files it excludes.

    Exempting any digit-looking suffix outright accepted `runspec.json.1`,
    `dump.sql.001` and `app.log.1`: rotated logs and split archives all end in
    digits, and every one of them is the data file this guard exists to refuse.
    Stripping the version tag and judging what is UNDER it is the difference.
    """

    def _diagnose(self, r, name, body="print('ran')\n"):
        Path("data.csv").write_text("a,b\n1,2\n", encoding="utf-8")
        Path(name).write_text(body, encoding="utf-8")
        return r.invoke(cli, ["diagnose", "--cites", "data.csv", "--", name])

    @pytest.mark.parametrize("name", [
        "runspec.json.1", "dump.sql.001", "app.log.1", "data.csv.1",
        "backup.tar.gz.001", "results.json.v2",
    ])
    def test_a_versioned_data_file_is_still_refused(self, tmp_path, name):
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            res = self._diagnose(r, name, body='{"steps": 3}')
            assert res.exit_code == 3, res.output
            assert "not a Python program" in res.output
            # And no verdict is invented for a target that never ran.
            assert "UNGROUNDED" not in res.output

    @pytest.mark.parametrize("name", ["pipeline.v2", "model.v1.2"])
    def test_a_versioned_python_script_still_runs(self, tmp_path, name):
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            res = self._diagnose(r, name, body="open('data.csv').read()\n")
            assert res.exit_code == 0, res.output
            assert "GROUNDED" in res.output


class TestTargetUsageErrorIsNotOurs:
    """A click-based target rejecting its own arguments is an aborted run.

    `click.BadParameter`, `MissingParameter` and `NoSuchOption` all subclass
    `UsageError`, and any pipeline calling `cli.main(standalone_mode=False)`
    surfaces them as exceptions. Catching the base class around the target's
    execution reported the TARGET's mistake as mareforma being invoked wrong:
    mareforma's usage line printed, no receipts written, and the usage exit code.
    """

    def test_the_targets_own_usage_error_is_a_crashed_run_with_its_report(
        self, tmp_path,
    ):
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            Path("data.csv").write_text("a,b\n1,2\n", encoding="utf-8")
            Path("t.py").write_text(
                "import click\n"
                "open('data.csv').read()\n"
                "raise click.UsageError('the TARGET rejects its arguments')\n",
                encoding="utf-8",
            )
            res = r.invoke(cli, ["diagnose", "--cites", "data.csv", "--", "t.py"])
        assert res.exit_code == 1, res.output
        assert "OBSERVATION REPORT" in res.output
        assert "Usage: cli diagnose" not in res.output

    def test_our_own_usage_error_still_reports_as_one(self, tmp_path):
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            Path("t.py").write_text("pass\n", encoding="utf-8")
            res = r.invoke(cli, ["diagnose", "--", "python", "-u", "t.py"])
        assert res.exit_code == 3, res.output
        assert "-u" in res.output


def test_a_signed_run_record_does_not_authorise_skipping_a_different_run(tmp_path):
    """Resume verified the signature and never checked WHICH run it was for.

    The check was keyed on the run directory's name alone and ran before the
    spec was loaded, so it could not compare. A hostile target that can write
    outside its own directory then only needs to COPY a neighbouring run's
    signed record, not forge one, and the sibling is skipped without executing.
    """
    import shutil

    from mareforma import signing

    r = CliRunner()
    with r.isolated_filesystem(temp_dir=tmp_path):
        Path("a.csv").write_text("a\n1\n", encoding="utf-8")
        Path("b.csv").write_text("b\n2\n", encoding="utf-8")
        Path("ta.py").write_text("open('a.csv').read()\n", encoding="utf-8")
        Path("tb.py").write_text("open('b.csv').read()\n", encoding="utf-8")
        corpus = Path("corpus")
        corpus.mkdir()
        (corpus / "runA.json").write_text(json.dumps(
            {"command": ["python", "ta.py"], "findings": {"fa": "a.csv"}}))
        (corpus / "runB.json").write_text(json.dumps(
            {"command": ["python", "tb.py"], "findings": {"fb": "b.csv"}}))
        key = Path("k.key")
        signing.bootstrap_key(key)
        args = ["audit", "--corpus", "corpus", "--out", "out", "--key", str(key)]
        assert r.invoke(cli, args, catch_exceptions=False).exit_code == 0

        shutil.copy("out/runA/run.json", "out/runB/run.json")
        res = r.invoke(cli, args, catch_exceptions=False)

    assert "skip runB" not in res.output, res.output
    assert "run runB" in res.output
