"""CLI tests for the trust-experience layer: map, verify, diagnose.

Pins the verify exit-code contract (0/1/2/3), bundle-mode subsumption and
auditor mode, the pre-binding label, map's three output shapes, and diagnose's
runpy-in-process behaviour with its no-guessed-citation rule.
"""
from __future__ import annotations

import base64
import json
import os
import sqlite3
import subprocess
import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

import mareforma
from mareforma import signing
from mareforma.cli import cli, _claim_bound_sources, _trust_map_plaintext
from mareforma.db import open_db


def _bootstrap_default_key() -> None:
    """Create the XDG-default signing key (XDG is isolated per test)."""
    kp = signing.default_key_path()
    kp.parent.mkdir(parents=True, exist_ok=True)
    if not kp.exists():
        signing.bootstrap_key(kp)


def _count_validators() -> int:
    """Number of enrolled validators in the project at cwd."""
    from mareforma import validators as _validators

    conn = open_db(Path("."))
    try:
        return _validators.count_validators(conn)
    finally:
        conn.close()


def _v038_grounded_record() -> str:
    return json.dumps({
        "version": "v0.3.8", "grounding": "GROUNDED",
        "reason": "cited file opened and non-empty",
        "receipt_digest": "sha256:deadbeef",
    })


# ---------------------------------------------------------------------------
# map
# ---------------------------------------------------------------------------

class TestMapCommand:
    def test_map_text(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("m", classification="ANALYTICAL")
            res = r.invoke(cli, ["map", cid])
            assert res.exit_code == 0, res.output
            assert "TRUST MAP" in res.output
            assert "independence" in res.output
            assert "UNVERIFIABLE" in res.output

    def test_map_renders_an_absent_value_as_na(self, tmp_path: Path) -> None:
        """leakage is DEFERRED with no value on every map, so both text
        renderers show a placeholder. It must be the "n/a" the HTML render
        uses, not a bare comma that reads as truncated output."""
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("m", classification="ANALYTICAL")
                text = _trust_map_plaintext(g.trust_map(cid))
            res = r.invoke(cli, ["map", cid])
        leakage = next(
            ln for ln in text.splitlines() if ln.strip().startswith("leakage")
        )
        assert leakage.endswith("n/a")
        assert res.exit_code == 0, res.output
        assert "n/a" in res.output
        assert not any(ln.strip() == "," for ln in res.output.splitlines())

    def test_map_json(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("m", classification="ANALYTICAL")
            res = r.invoke(cli, ["map", cid, "--json"])
            assert res.exit_code == 0
            doc = json.loads(res.output)
            assert doc["subject_id"] == cid
            assert len(doc["properties"]) == 11

    def test_map_html_written_to_file(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("m", classification="ANALYTICAL")
            res = r.invoke(cli, ["map", cid, "--html", "--output", "tm.html"])
            assert res.exit_code == 0, res.output
            html = Path("tm.html").read_text()
            assert html.startswith("<!DOCTYPE html>")
            assert "https://" not in html and "<script" not in html.lower()

    def test_map_output_creates_the_parent_directory(
        self, tmp_path: Path,
    ) -> None:
        """A CI job writing the map into a build directory must not have to
        create it first, the way export already does not."""
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("m", classification="ANALYTICAL")
            for flag, target in (
                ([], "text/map.txt"),
                (["--json"], "json/map.json"),
                (["--html"], "html/map.html"),
            ):
                res = r.invoke(cli, ["map", cid, *flag, "--output", target])
                assert res.exit_code == 0, res.output
                assert res.exception is None
                assert Path(target).read_text()

    def test_map_output_reports_an_unwritable_path(self, tmp_path: Path) -> None:
        """An OSError on the write is a one-line error, never a traceback."""
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("m", classification="ANALYTICAL")
            Path("afile").write_text("not a directory")
            res = r.invoke(cli, ["map", cid, "--output", "afile/map.txt"])
            assert res.exit_code == 1
            assert res.exception is None or isinstance(res.exception, SystemExit)
            assert "Could not write afile/map.txt" in res.output

    def test_map_missing_claim_exits_1(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                g.assert_claim("m", classification="ANALYTICAL")
            res = r.invoke(cli, ["map", "nope"])
            assert res.exit_code == 1

    def test_map_html_and_json_mutually_exclusive(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("m", classification="ANALYTICAL")
            res = r.invoke(cli, ["map", cid, "--html", "--json"])
            assert res.exit_code == 1


# ---------------------------------------------------------------------------
# verify, exit-code contract
# ---------------------------------------------------------------------------

class TestVerifyExitCodes:
    def test_claim_verified_exit_0(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("v", classification="ANALYTICAL")
            res = r.invoke(cli, ["verify", cid])
            assert res.exit_code == 0, res.output
            assert "TRUST MAP" in res.output

    def test_unknown_claim_is_unverifiable_exit_2(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                g.assert_claim("v", classification="ANALYTICAL")
            res = r.invoke(cli, ["verify", "not-a-real-claim"])
            assert res.exit_code == 2

    def test_bad_flag_exit_code_distinct_from_unverifiable(
        self, tmp_path: Path,
    ) -> None:
        # A typo'd flag must NOT read as "unverifiable" (2) to a CI gate.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            res = r.invoke(cli, ["verify", "x", "--no-such-flag"])
            assert res.exit_code == 3
            assert res.exit_code != 2

    def test_claim_json_schema(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("v", classification="ANALYTICAL")
            res = r.invoke(cli, ["verify", cid, "--json"])
            assert res.exit_code == 0
            doc = json.loads(res.output)
            assert doc["verdict"] == "verified"
            assert doc["exit_code"] == 0
            assert doc["trust_map"]["subject_id"] == cid


_CI_README = (
    Path(__file__).resolve().parents[1] / "examples" / "06_ci_verify" / "README.md"
)


def _recipe_body(marker: str) -> str:
    """The shell body of the CI README step whose name contains *marker*."""
    step = _CI_README.read_text(encoding="utf-8")
    step = step[step.index(marker):]
    body = step[step.index("run: |") + len("run: |"):]
    return textwrap.dedent(body[: body.index("env:")])


def _run_recipe(body: str, tmp_path: Path, verify_exit: int, **env) -> int:
    """Run a recipe body under ``bash -e`` with ``mareforma verify`` stubbed."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    stub = bin_dir / "mareforma"
    stub.write_text(f"#!/bin/sh\nexit {verify_exit}\n")
    stub.chmod(0o755)
    script = tmp_path / "step.sh"
    script.write_text(body)
    return subprocess.run(
        ["bash", "-e", str(script)], cwd=tmp_path,
        env={"PATH": f"{bin_dir}:{os.environ['PATH']}", **env},
        capture_output=True, text=True,
    ).returncode


class TestCiRecipeHonorsTheExitCodeContract:
    """The shipped CI recipe must fail on every code that is not a verdict."""

    def test_usage_error_fails_the_gate(self, tmp_path: Path) -> None:
        # Exit 3 is a typo'd flag, not a verdict, so the gate cannot pass on it.
        body = _recipe_body("split tamper vs unverifiable")
        assert _run_recipe(body, tmp_path, 3, CLAIM_ID="claim-1") != 0
        assert _run_recipe(body, tmp_path, 0, CLAIM_ID="claim-1") == 0

    def test_unset_claim_id_fails_the_gate(self, tmp_path: Path) -> None:
        # An unconfigured repo variable is a misconfigured gate, not a pass.
        for marker in ("name: verify claim\n", "split tamper vs unverifiable"):
            body = _recipe_body(marker)
            assert _run_recipe(body, tmp_path, 0) != 0, marker

    def test_exit_code_tables_list_the_usage_code(self) -> None:
        assert "| `3` |" in _CI_README.read_text(encoding="utf-8")
        index = (_CI_README.parents[1] / "README.md").read_text(encoding="utf-8")
        assert "3 usage error" in index


class TestVerifyBundleMode:
    """The new command subsumes the old bundle-path invocation."""

    def _make_bundle(self) -> Path:
        with mareforma.open(".") as g:
            g.assert_claim("seeded", generated_by="seed", seed=True)
        from mareforma.export_bundle import write_bundle
        out = Path("mareforma-bundle.json")
        write_bundle(Path("."), out, signing.load_private_key(
            signing.default_key_path()))
        return out

    def test_existing_bundle_invocation_still_verifies(
        self, tmp_path: Path,
    ) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            self._make_bundle()
            res = r.invoke(cli, ["verify", "mareforma-bundle.json"])
            assert res.exit_code == 0
            assert "verified" in res.output.lower()

    def test_tampered_bundle_is_failure_exit_1(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            self._make_bundle()
            bundle = json.loads(Path("mareforma-bundle.json").read_text())
            bundle["signatures"][0]["sig"] = base64.standard_b64encode(
                b"x" * 64).decode("ascii")
            Path("mareforma-bundle.json").write_text(json.dumps(bundle))
            res = r.invoke(cli, ["verify", "mareforma-bundle.json"])
            assert res.exit_code == 1

    def test_missing_local_key_is_unverifiable_not_failure(
        self, tmp_path: Path,
    ) -> None:
        # A missing local key → exit 2 (unverifiable), never 1.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            self._make_bundle()
            signing.default_key_path().unlink()
            res = r.invoke(cli, ["verify", "mareforma-bundle.json", "--json"])
            assert res.exit_code == 2
            assert json.loads(res.output)["verdict"] == "unverifiable"

    def test_export_dir_detection(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            self._make_bundle()  # writes mareforma-bundle.json in cwd
            res = r.invoke(cli, ["verify", "."])
            assert res.exit_code == 0, res.output


class TestVerifyAuditorMode:
    def test_claim_verifies_with_only_public_material(
        self, tmp_path: Path,
    ) -> None:
        # Auditor mode: no local signing key, verification uses the graph's
        # enrolled validator pubkeys (public material).
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim("v", classification="ANALYTICAL")
            signing.default_key_path().unlink()  # drop private material
            res = r.invoke(cli, ["verify", cid])
            assert res.exit_code == 0, res.output


class TestVerifyPreBindingLabel:
    """An axis-v0.3.8 GROUNDED renders as pre-binding, not bound GROUNDED."""

    def test_pre_binding_label_in_verify_output(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim(
                    "v", classification="ANALYTICAL",
                    observed_grounding=json.loads(_v038_grounded_record()),
                )
            res = r.invoke(cli, ["verify", cid])
            assert res.exit_code == 0, res.output
            assert "pre-binding axis; citation binding not checkable" in res.output


# ---------------------------------------------------------------------------
# diagnose (E1)
# ---------------------------------------------------------------------------

class TestDiagnose:
    def _script(self, tmp_path: Path, body: str, name: str = "t.py") -> Path:
        p = tmp_path / name
        p.write_text(body)
        return p

    def test_no_cites_reports_observation_without_verdict(
        self, tmp_path: Path,
    ) -> None:
        data = tmp_path / "data.csv"
        data.write_text("a,b\n1,2\n")
        script = self._script(
            tmp_path, f"open({str(data)!r}).read()\n")
        r = CliRunner()
        res = r.invoke(cli, ["diagnose", "--json", "--", str(script)])
        assert res.exit_code == 0, res.output
        doc = json.loads(res.output)
        # Never guesses a citation: obvious data read, no --cites → NO verdict.
        assert doc["grounding"] is None
        assert any(str(data) in rr["identifier"] for rr in doc["reads"])

    def test_cites_produces_grounding_verdict(self, tmp_path: Path) -> None:
        data = tmp_path / "data.csv"
        data.write_text("a,b\n1,2\n")
        script = self._script(tmp_path, f"open({str(data)!r}).read()\n")
        r = CliRunner()
        res = r.invoke(
            cli, ["diagnose", "--json", "--cites", str(data), "--", str(script)])
        assert res.exit_code == 0, res.output
        doc = json.loads(res.output)
        assert doc["grounding"] is not None
        assert doc["grounding"]["grounding"] == "GROUNDED"

    def test_self_instrumented_target_still_reports_its_reads(
        self, tmp_path: Path,
    ) -> None:
        # The target opens its own observe() block. diagnose wraps it in an
        # outer scope, so its reads must still reach the report instead of
        # printing a confident UNGROUNDED with no reads at all.
        data = tmp_path / "data.csv"
        data.write_text("a,b\n1,2\n")
        script = self._script(
            tmp_path,
            "import mareforma.observe as obs\n"
            f"with obs.observe(cites={str(data)!r}):\n"
            f"    open({str(data)!r}).read()\n",
        )
        r = CliRunner()
        res = r.invoke(
            cli, ["diagnose", "--json", "--cites", str(data), "--", str(script)])
        assert res.exit_code == 0, res.output
        doc = json.loads(res.output)
        assert any(str(data) in rr["identifier"] for rr in doc["reads"])
        assert doc["grounding"]["grounding"] == "GROUNDED"

    def test_target_crash_partial_report_and_exit_code(
        self, tmp_path: Path,
    ) -> None:
        script = self._script(tmp_path, "raise SystemExit(5)\n")
        r = CliRunner()
        res = r.invoke(cli, ["diagnose", "--json", "--", str(script)])
        assert res.exit_code == 5
        # A non-zero SystemExit is an aborted run, exactly what `partial`
        # renders as "target exited with error".
        assert json.loads(res.output)["partial"] is True

    def test_clean_systemexit_zero_is_not_partial(self, tmp_path: Path) -> None:
        script = self._script(tmp_path, "raise SystemExit(0)\n")
        r = CliRunner()
        res = r.invoke(cli, ["diagnose", "--json", "--", str(script)])
        assert res.exit_code == 0
        assert json.loads(res.output)["partial"] is False

    def test_aborted_run_never_reports_ungrounded(self, tmp_path: Path) -> None:
        # The target exits on an error path before touching its cited file.
        # UNGROUNDED means the scope was fully observed and the data did not
        # arrive; a truncated run is not that, so the verdict floors to OPAQUE.
        data = tmp_path / "data.csv"
        data.write_text("a,b\n1,2\n")
        script = self._script(tmp_path, "raise SystemExit(1)\n")
        r = CliRunner()
        res = r.invoke(
            cli, ["diagnose", "--json", "--cites", str(data), "--", str(script)])
        assert res.exit_code == 1
        doc = json.loads(res.output)
        assert doc["partial"] is True
        assert doc["grounding"]["grounding"] == "OPAQUE"

    def test_non_int_systemexit_code_is_echoed(self, tmp_path: Path) -> None:
        # CPython prints a non-int code and exits 1. The message is the
        # target's own, so it goes to stderr and never into a verdict.
        script = self._script(
            tmp_path, "raise SystemExit('fatal: upstream data missing')\n")
        r = CliRunner()
        res = r.invoke(cli, ["diagnose", "--json", "--", str(script)])
        assert res.exit_code == 1
        assert "fatal: upstream data missing" in res.output

    def test_interpreter_flag_is_a_usage_error(self, tmp_path: Path) -> None:
        # `python -u script.py` is a shape users copy out of their shell. The
        # target runs in-process, so the flag cannot be honoured; reporting it
        # as an invocation error beats running nothing and blaming the target.
        script = self._script(tmp_path, "pass\n")
        r = CliRunner()
        res = r.invoke(cli, ["diagnose", "--", "python", "-u", str(script)])
        assert res.exit_code == 2, res.output
        assert "-u" in res.output
        assert "OBSERVATION REPORT" not in res.output

    def test_uncaught_exception_exits_1_and_marks_partial(
        self, tmp_path: Path,
    ) -> None:
        script = self._script(tmp_path, "raise ValueError('boom')\n")
        r = CliRunner()
        res = r.invoke(cli, ["diagnose", "--json", "--", str(script)])
        assert res.exit_code == 1
        doc = json.loads(res.output)
        assert doc["partial"] is True


# ---------------------------------------------------------------------------
# verify, grounding→citation binding re-check (the read-side gate)
# ---------------------------------------------------------------------------

_REAL = "/data/real.csv"
_DECOY = "/data/decoy.csv"


def _grounded_record(grounded: list[str]) -> dict:
    return {
        "version": "v0.3.9", "grounding": "GROUNDED",
        "reason": "cited read observed", "cited_sources": [_REAL],
        "grounded_sources": grounded,
    }


class TestVerifyGroundingBinding:
    def test_claim_bound_sources_reads_predicate_payload(self) -> None:
        # The finding citation lives in the predicate_payload column, not a
        # (nonexistent) data_source column. Reading the wrong place silently
        # no-ops the whole binding re-check.
        ca = "sha256:" + "a" * 64
        claim = {"predicate_payload": json.dumps(
            {"data_sources": [_REAL], "data_ids": [ca, "string-token"]})}
        assert _claim_bound_sources(claim) == [_REAL, ca]
        assert _claim_bound_sources({"data_source": "/x"}) == []  # dead field ignored
        assert _claim_bound_sources({}) == []
        assert _claim_bound_sources({"predicate_payload": "not json"}) == []

    def test_matched_binding_verifies_exit_0(self, tmp_path: Path) -> None:
        # A GROUNDED verdict whose grounded set matches the finding's citation
        # verifies clean, proves the binding check is WIRED, not skipped.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim(
                    "grounded finding", classification="ANALYTICAL",
                    predicate_payload={"data_sources": [_REAL], "data_ids": []},
                    observed_grounding=_grounded_record([_REAL]),
                )
            res = r.invoke(cli, ["verify", cid])
            assert res.exit_code == 0, res.output

    def test_disjoint_binding_is_tamper_exit_1(self, tmp_path: Path) -> None:
        # A GROUNDED verdict whose grounded set is disjoint from the finding's
        # signed data_sources. The producer signed both, so the signature
        # verifies; the binding re-check is the only thing that can catch it,
        # and it must.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim(
                    "grounded finding", classification="ANALYTICAL",
                    predicate_payload={"data_sources": [_REAL], "data_ids": []},
                    observed_grounding=_grounded_record([_DECOY]),
                )
            res = r.invoke(cli, ["verify", cid, "--json"])
            assert res.exit_code == 1, res.output
            assert "grounding binding violation" in res.output

    def test_clearing_the_citation_on_a_signed_row_is_refused(
        self, tmp_path: Path,
    ) -> None:
        # predicate_payload is unsigned, but the binding re-check reads it, so
        # one UPDATE clearing it would turn the tampered verdict above into a
        # clean one. The append-only trigger refuses that write, the guard
        # asserter_keyid already earns as an unsigned column a read path trusts.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            with mareforma.open(".") as g:
                cid = g.assert_claim(
                    "grounded finding", classification="ANALYTICAL",
                    predicate_payload={"data_sources": [_REAL], "data_ids": []},
                    observed_grounding=_grounded_record([_DECOY]),
                )
                with pytest.raises(
                    sqlite3.IntegrityError, match="signed_field_locked",
                ):
                    g._conn.execute(
                        "UPDATE claims SET predicate_payload = '' "
                        "WHERE claim_id = ?", (cid,),
                    )
            res = r.invoke(cli, ["verify", cid, "--json"])
            assert res.exit_code == 1, res.output
            assert "grounding binding violation" in res.output


# ---------------------------------------------------------------------------
# read-only commands must not enroll the caller's key
# ---------------------------------------------------------------------------

class TestReadCommandsDoNotEnroll:
    """`verify`, `map` and `validator list` are auditor-side reads.

    Opening the graph with a signer auto-enrolls that key as the project's
    self-signed root when the validators table is empty, which is a write no
    read command may perform: root enrollment is immutable and would lock the
    producer out of their own graph.
    """

    def _unsigned_project(self, tmp_path: Path) -> str:
        """Write a claim with no key present, then bootstrap one."""
        with mareforma.open(".") as g:
            cid = g.assert_claim("unsigned", classification="ANALYTICAL")
        assert _count_validators() == 0
        _bootstrap_default_key()
        return cid

    def test_verify_leaves_validators_empty(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            cid = self._unsigned_project(tmp_path)
            res = r.invoke(cli, ["verify", cid])
            assert res.exit_code == 0, res.output
            assert _count_validators() == 0

    def test_map_leaves_validators_empty(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            cid = self._unsigned_project(tmp_path)
            res = r.invoke(cli, ["map", cid])
            assert res.exit_code == 0, res.output
            assert _count_validators() == 0

    def test_validator_list_leaves_validators_empty(self, tmp_path: Path) -> None:
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            self._unsigned_project(tmp_path)
            res = r.invoke(cli, ["validator", "list"])
            assert res.exit_code == 0, res.output
            assert "No validators enrolled" in res.output
            assert _count_validators() == 0
