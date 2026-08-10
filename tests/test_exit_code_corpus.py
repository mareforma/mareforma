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
from mareforma.db import open_db


def _bootstrap_default_key() -> None:
    kp = signing.default_key_path()
    kp.parent.mkdir(parents=True, exist_ok=True)
    if not kp.exists():
        signing.bootstrap_key(kp)


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
            assert res.exit_code == 2, res.output
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
            # click usage error: exit 2, before any target runs, so no receipt
            # and no grounding verdict for a target that never executed.
            assert res.exit_code == 2, res.output
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
