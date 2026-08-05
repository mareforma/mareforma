"""CLI tests for the post-hoc auditor: `mareforma audit`.

Pins the auditor contract: a target that never imports mareforma is observed
exactly as under diagnose; a findings mapping yields one signed, verifiable
receipt per finding with independent verdicts; a crashing target still emits
partial receipts and its own exit code; a corpus run is resumable with per-run
isolation; and nothing the target prints or writes enters a verdict.
"""
from __future__ import annotations

import base64
import json
import os
import threading
from pathlib import Path

import pytest
from click.testing import CliRunner

from mareforma import signing
from mareforma.cli import cli
from tests._helpers import _bootstrap_key


def _script(tmp_path: Path, body: str, name: str = "target.py") -> Path:
    p = tmp_path / name
    p.write_text(body)
    return p


def _mapping(tmp_path: Path, mapping: dict, name: str = "findings.json") -> Path:
    p = tmp_path / name
    p.write_text(json.dumps(mapping))
    return p


def _bootstrap_default_key() -> Path:
    """Create the XDG-default signing key (XDG is isolated per test)."""
    kp = signing.default_key_path()
    kp.parent.mkdir(parents=True, exist_ok=True)
    if not kp.exists():
        signing.bootstrap_key(kp)
    return kp


def _read_receipts(out_dir: Path) -> list[dict]:
    lines = (out_dir / "receipts.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def _read_run(out_dir: Path) -> dict:
    """Decode the run record out of its signed envelope."""
    import base64

    envelope = json.loads((out_dir / "run.json").read_text())
    assert envelope["payloadType"] == signing.PAYLOAD_TYPE_AUDIT_RUN
    return json.loads(base64.standard_b64decode(envelope["payload"]))


def _audit(r: CliRunner, tmp_path: Path, script: Path, mapping: dict,
           out: Path, key: Path | None = None):
    args = ["audit", "--findings", str(_mapping(tmp_path, mapping)),
            "--out", str(out)]
    if key is not None:
        args += ["--key", str(key)]
    args += ["--json", "--", str(script)]
    return r.invoke(cli, args)


class TestAuditSingleRun:
    def test_audit_no_mareforma_import(self, tmp_path: Path) -> None:
        # The target never imports mareforma; audit observes the same reads
        # diagnose does on the identical pipeline.
        data = tmp_path / "data.csv"
        data.write_text("a,b\n1,2\n")
        script = _script(tmp_path, f"open({str(data)!r}).read()\n")
        import ast
        tree = ast.parse(script.read_text())
        assert not any(isinstance(n, (ast.Import, ast.ImportFrom))
                       for n in ast.walk(tree))
        key = _bootstrap_key(tmp_path, "auditor.key")

        r = CliRunner()
        diag = r.invoke(cli, ["diagnose", "--json", "--", str(script)])
        assert diag.exit_code == 0, diag.output
        diag_reads = {(rr["kind"], rr["identifier"], rr["nonempty"])
                      for rr in json.loads(diag.output)["reads"]}

        out = tmp_path / "audit-out"
        res = _audit(r, tmp_path, script, {"f1": str(data)}, out, key)
        assert res.exit_code == 0, res.output
        audit_reads = {(rr["kind"], rr["identifier"], rr["nonempty"])
                       for rr in json.loads(res.output)["reads"]}
        assert audit_reads == diag_reads
        assert any(str(data) in ident for _, ident, _n in audit_reads)

    def test_audit_findings_mapping(self, tmp_path: Path) -> None:
        # Three findings on disjoint cited sources from ONE observed run yield
        # three receipts with independent verdicts.
        read_csv = tmp_path / "read.csv"
        read_csv.write_text("x\n1\n")
        unread_csv = tmp_path / "unread.csv"
        unread_csv.write_text("x\n2\n")
        url = "https://example.org/never-fetched.csv"
        script = _script(tmp_path, f"open({str(read_csv)!r}).read()\n")
        key = _bootstrap_key(tmp_path, "auditor.key")

        out = tmp_path / "audit-out"
        r = CliRunner()
        res = _audit(r, tmp_path, script, {
            "f-grounded": str(read_csv),
            "f-ungrounded": str(unread_csv),
            "f-opaque": [url],
        }, out, key)
        assert res.exit_code == 0, res.output

        receipts = {rec["finding_id"]: rec for rec in _read_receipts(out)}
        assert set(receipts) == {"f-grounded", "f-ungrounded", "f-opaque"}
        assert receipts["f-grounded"]["grounding"] == "GROUNDED"
        assert receipts["f-ungrounded"]["grounding"] == "UNGROUNDED"
        assert receipts["f-opaque"]["grounding"] == "OPAQUE"
        # One signed envelope per finding.
        envelopes = sorted((out / "envelopes").glob("*.json"))
        assert len(envelopes) == 3

    def test_coverage_counts_cited_opens_not_imports(
        self, tmp_path: Path,
    ) -> None:
        # The target imports stdlib modules and reads exactly the cited file.
        # Coverage is the fraction of the CITED ingress the observer read
        # through, so it is 1/1 here: the import machinery's .pyc opens and
        # runpy's opens of the script itself are not data-ingress for the
        # cited source and must not deflate the bound.
        data = tmp_path / "data.csv"
        data.write_text("a,b\n1,2\n")
        script = _script(tmp_path, (
            "import json, wave, colorsys\n"
            f"open({str(data)!r}).read()\n"
        ))
        key = _bootstrap_key(tmp_path, "auditor.key")

        r = CliRunner()
        diag = r.invoke(cli, ["diagnose", "--cites", str(data), "--json",
                              "--", str(script)])
        assert diag.exit_code == 0, diag.output
        assert json.loads(diag.output)["coverage"] == {
            "reads_seen": 1,
            "opens_detected": 1,
            "read_coverage_fraction": 1.0,
        }

        out = tmp_path / "audit-out"
        res = _audit(r, tmp_path, script, {"f1": str(data)}, out, key)
        assert res.exit_code == 0, res.output
        assert _read_run(out)["coverage"] == {
            "reads_seen": 1,
            "opens_detected": 1,
            "read_coverage_fraction": 1.0,
        }

    def test_audit_receipts_feed_summarize_pilot_unchanged(
        self, tmp_path: Path,
    ) -> None:
        # The emitted receipts file is directly consumable by summarize_pilot
        # (and therefore by `mareforma measure`) with no translation step.
        from mareforma.observe import summarize_pilot

        read_csv = tmp_path / "read.csv"
        read_csv.write_text("x\n1\n")
        unread_csv = tmp_path / "unread.csv"
        unread_csv.write_text("x\n2\n")
        script = _script(tmp_path, f"open({str(read_csv)!r}).read()\n")
        key = _bootstrap_key(tmp_path, "auditor.key")

        out = tmp_path / "audit-out"
        r = CliRunner()
        res = _audit(r, tmp_path, script, {
            "f1": str(read_csv),
            "f2": str(unread_csv),
            "f3": ["https://example.org/never-fetched.csv"],
        }, out, key)
        assert res.exit_code == 0, res.output

        report = summarize_pilot(_read_receipts(out))
        d = report.to_dict()
        assert d["n"] == 3
        assert d["grounding"]["counts"] == {
            "GROUNDED": 1, "UNGROUNDED": 1, "OPAQUE": 1}
        assert d["coverage_bound"]

        measured = r.invoke(cli, ["measure", str(out / "receipts.jsonl"),
                                  "--json"])
        assert measured.exit_code == 0, measured.output
        assert json.loads(measured.output)["total"] == 3

    def test_audit_receipt_verifiable(self, tmp_path: Path) -> None:
        # Each envelope round-trips through `mareforma verify` (signature plus
        # citation binding) from public material alone; a tampered payload is a
        # definite failure, exit 1.
        from mareforma.audit import verify_audit_receipt

        data = tmp_path / "data.csv"
        data.write_text("x\n1\n")
        script = _script(tmp_path, f"open({str(data)!r}).read()\n")

        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            key = _bootstrap_default_key()
            out = Path("audit-out")
            res = _audit(r, Path("."), script, {"f1": str(data)}, out)
            assert res.exit_code == 0, res.output

            envelope_path = next((out / "envelopes").glob("*.json"))
            verified = r.invoke(cli, ["verify", str(envelope_path)])
            assert verified.exit_code == 0, verified.output

            # Public material alone: the verification function takes only the
            # envelope and the signer's public key.
            envelope = json.loads(envelope_path.read_text())
            pub = signing.load_private_key(key).public_key()
            ok, reason = verify_audit_receipt(envelope, pub)
            assert ok, reason

            # Tamper: flip the verdict inside the payload.
            import base64
            payload = json.loads(
                base64.standard_b64decode(envelope["payload"]))
            payload["grounding"] = "GROUNDED" if (
                payload["grounding"] != "GROUNDED") else "UNGROUNDED"
            envelope["payload"] = base64.standard_b64encode(
                json.dumps(payload).encode()).decode()
            tampered = out / "tampered.json"
            tampered.write_text(json.dumps(envelope))
            failed = r.invoke(cli, ["verify", str(tampered)])
            assert failed.exit_code == 1, failed.output

    def test_receipt_signed_with_nondefault_key_reads_unverifiable(
        self, tmp_path: Path,
    ) -> None:
        # A receipt signed with a non-default auditor key must read as
        # UNVERIFIABLE (exit 2) against the default key, never tampered
        # (exit 1): a wrong verification key is not proof of tamper.
        data = tmp_path / "data.csv"
        data.write_text("x\n1\n")
        script = _script(tmp_path, f"open({str(data)!r}).read()\n")
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            auditor = _bootstrap_key(Path("."), "auditor.key")
            out = Path("audit-out")
            res = _audit(r, Path("."), script, {"f1": str(data)}, out, auditor)
            assert res.exit_code == 0, res.output
            envelope_path = next((out / "envelopes").glob("*.json"))

            verified = r.invoke(cli, ["verify", str(envelope_path)])
            assert verified.exit_code == 2, verified.output

    def test_receipt_verifies_when_pinned_to_the_signer_key(
        self, tmp_path: Path,
    ) -> None:
        # Pointing verify at the auditor's own key with --key confirms the
        # receipt, exit 0.
        data = tmp_path / "data.csv"
        data.write_text("x\n1\n")
        script = _script(tmp_path, f"open({str(data)!r}).read()\n")
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            auditor = _bootstrap_key(Path("."), "auditor.key")
            out = Path("audit-out")
            res = _audit(r, Path("."), script, {"f1": str(data)}, out, auditor)
            assert res.exit_code == 0, res.output
            envelope_path = next((out / "envelopes").glob("*.json"))

            pinned = r.invoke(
                cli, ["verify", str(envelope_path), "--key", str(auditor)])
            assert pinned.exit_code == 0, pinned.output
            assert "verified" in pinned.output

    def test_audit_run_record_verify_is_not_tampered(
        self, tmp_path: Path,
    ) -> None:
        # `verify run.json` must not read the auditor's own signed run
        # record as tampered. The record's signature is authentic and its
        # `completed` flag is what resume trusts; a passing signature here is
        # not a claim verdict, so verify reports UNVERIFIABLE (use resume),
        # never the bundle-shaped tamper failure it fell through to before.
        data = tmp_path / "data.csv"
        data.write_text("a,b\n1,2\n")
        script = _script(tmp_path, f"open({str(data)!r}).read()\n")
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            auditor = _bootstrap_key(Path("."), "auditor.key")
            out = Path("audit-out")
            res = _audit(r, Path("."), script, {"f1": str(data)}, out, auditor)
            assert res.exit_code == 0, res.output
            run_json = out / "run.json"
            assert (json.loads(run_json.read_text())["payloadType"]
                    == signing.PAYLOAD_TYPE_AUDIT_RUN)

            verified = r.invoke(
                cli, ["verify", "--json", str(run_json), "--key", str(auditor)])
            assert verified.exit_code == 2, verified.output  # UNVERIFIABLE
            payload = json.loads(verified.output)
            assert payload["verdict"] == "unverifiable"
            assert payload["target_kind"] == "audit-run"

    def test_audit_grounded_binding_violation_fails_verify(
        self, tmp_path: Path,
    ) -> None:
        # A hand-built GROUNDED receipt whose grounded set names none of the
        # finding's cited sources fails the binding re-check even with a valid
        # signature: signature alone must not verify an unbound GROUNDED.
        from mareforma.audit import verify_audit_receipt
        from mareforma.signing import sign_audit_receipt

        key = _bootstrap_key(tmp_path, "auditor.key")
        signer = signing.load_private_key(key)
        record = {
            "finding_id": "f1",
            "version": "v0.3.9",
            "grounding": "GROUNDED",
            "reason": "forged",
            "cited_sources": ["/data/real.csv"],
            "grounded_sources": ["/data/decoy.csv"],
            "reads": [], "seams": [],
            "coverage": {"reads_seen": 0, "opens_detected": 0},
        }
        envelope = sign_audit_receipt(record, signer)
        ok, reason = verify_audit_receipt(envelope, signer.public_key())
        assert not ok
        assert "binding" in reason

    def test_audit_crash_partial(self, tmp_path: Path) -> None:
        # The target raises mid-run: partial receipts are still emitted, the
        # exit code is the target's own, and nothing is swallowed.
        read_csv = tmp_path / "read.csv"
        read_csv.write_text("x\n1\n")
        unread_csv = tmp_path / "unread.csv"
        unread_csv.write_text("x\n2\n")
        script = _script(tmp_path, (
            f"open({str(read_csv)!r}).read()\n"
            "raise ValueError('boom')\n"
        ))
        key = _bootstrap_key(tmp_path, "auditor.key")

        out = tmp_path / "audit-out"
        r = CliRunner()
        res = _audit(r, tmp_path, script, {
            "f1": str(read_csv), "f2": str(unread_csv)}, out, key)
        assert res.exit_code == 1, res.output

        run = _read_run(out)
        assert run["partial"] is True
        assert run["exit_code"] == 1
        receipts = {rec["finding_id"]: rec for rec in _read_receipts(out)}
        assert set(receipts) == {"f1", "f2"}
        assert receipts["f1"]["grounding"] == "GROUNDED"
        # The run stopped early, so nothing can be said about the citation it
        # never reached. UNGROUNDED would claim a full observation.
        assert receipts["f2"]["grounding"] == "OPAQUE"

    def test_audit_ctrl_c_during_classification_propagates(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        # The verdict loop runs after the scope closed, with no propagating
        # exception to protect. A Ctrl-C there must kill the audit, not become
        # an OPAQUE receipt the operator never asked for.
        from mareforma.audit import run_audit
        from mareforma.observe import _scope

        data = tmp_path / "data.csv"
        data.write_text("x\n1\n")
        script = _script(tmp_path, f"open({str(data)!r}).read()\n")
        key = _bootstrap_key(tmp_path, "auditor.key")
        out = tmp_path / "audit-out"

        def _interrupt(self, cited):
            raise KeyboardInterrupt

        monkeypatch.setattr(_scope.Scope, "classify_against", _interrupt)

        with pytest.raises(KeyboardInterrupt):
            run_audit(
                [str(script)],
                findings_path=_mapping(tmp_path, {"f1": str(data)}),
                out_dir=out,
                key_path=key,
                as_json=True,
            )
        assert not (out / "receipts.jsonl").exists()

    def test_audit_systemexit_code_is_preserved(self, tmp_path: Path) -> None:
        data = tmp_path / "data.csv"
        data.write_text("x\n1\n")
        script = _script(tmp_path, "raise SystemExit(5)\n")
        key = _bootstrap_key(tmp_path, "auditor.key")
        out = tmp_path / "audit-out"
        r = CliRunner()
        res = _audit(r, tmp_path, script, {"f1": str(data)}, out, key)
        assert res.exit_code == 5
        run = _read_run(out)
        assert run["exit_code"] == 5
        # A non-zero SystemExit aborted the run: the signed record and every
        # receipt say so, and no receipt claims a fully observed absence.
        assert run["partial"] is True
        receipts = _read_receipts(out)
        assert all(rec["partial"] is True for rec in receipts)
        assert receipts[0]["grounding"] == "OPAQUE"

    def test_audit_clean_systemexit_zero_is_not_partial(
        self, tmp_path: Path,
    ) -> None:
        data = tmp_path / "data.csv"
        data.write_text("x\n1\n")
        script = _script(tmp_path, f"open({str(data)!r}).read()\nraise SystemExit(0)\n")
        key = _bootstrap_key(tmp_path, "auditor.key")
        out = tmp_path / "audit-out"
        r = CliRunner()
        res = _audit(r, tmp_path, script, {"f1": str(data)}, out, key)
        assert res.exit_code == 0
        assert _read_run(out)["partial"] is False
        assert _read_receipts(out)[0]["grounding"] == "GROUNDED"

    def test_audit_out_dir_immune_to_target_chdir(self, tmp_path: Path) -> None:
        # The target chdirs away mid-run; a relative --out must still resolve
        # against the invocation directory, not wherever the target moved the
        # process, or the target chooses where its own receipts land.
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path) as fs:
            fs = Path(fs)
            data = fs / "data.csv"
            data.write_text("x\n1\n")
            decoy = fs / "decoy"
            decoy.mkdir()
            script = fs / "target.py"
            script.write_text(
                f"open({str(data)!r}).read()\n"
                f"import os\nos.chdir({str(decoy)!r})\n"
            )
            key = _bootstrap_key(fs, "auditor.key")
            mapping = fs / "findings.json"
            mapping.write_text(json.dumps({"f1": str(data)}))
            res = r.invoke(cli, [
                "audit", "--findings", str(mapping), "--out", "audit-out",
                "--key", str(key), "--", str(script)])
            assert res.exit_code == 0, res.output
            receipts = _read_receipts(fs / "audit-out")
            assert receipts[0]["grounding"] == "GROUNDED"
            assert not (decoy / "audit-out").exists()

    def test_reaudit_into_one_out_dir_drops_the_previous_envelopes(
        self, tmp_path: Path,
    ) -> None:
        # receipts.jsonl is truncated to the current run; the envelope set is
        # the same verdicts signed, so it must be truncated too. Otherwise an
        # auditor forwarding envelopes/ ships a superseded run's signed
        # verdicts next to the current ones, with nothing on disk saying which
        # is which.
        data = tmp_path / "data.csv"
        data.write_text("x\n1\n")
        script = _script(tmp_path, f"open({str(data)!r}).read()\n")
        key = _bootstrap_key(tmp_path, "auditor.key")

        out = tmp_path / "audit-out"
        r = CliRunner()
        first = _audit(r, tmp_path, script, {"alpha": str(data)}, out, key)
        assert first.exit_code == 0, first.output
        second = _audit(r, tmp_path, script, {"beta": str(data)}, out, key)
        assert second.exit_code == 0, second.output

        current = {rec["finding_id"] for rec in _read_receipts(out)}
        assert current == {"beta"}
        names = sorted(p.name for p in (out / "envelopes").glob("*.json"))
        assert names == ["001-beta.json"]

    def test_audit_sha256_cite_is_never_false_ungrounded(
        self, tmp_path: Path,
    ) -> None:
        # A finding cites its data by content. The file's bytes ARE read, but
        # the open path cannot hash them, so the observer has no right to a
        # confident UNGROUNDED: the honest verdict is OPAQUE with the gap named.
        import hashlib

        data = tmp_path / "data.csv"
        data.write_text("x\n1\n")
        ca = "sha256:" + hashlib.sha256(data.read_bytes()).hexdigest()
        script = _script(tmp_path, f"open({str(data)!r}).read()\n")
        key = _bootstrap_key(tmp_path, "auditor.key")

        out = tmp_path / "audit-out"
        r = CliRunner()
        res = _audit(r, tmp_path, script, {"f1": ca}, out, key)
        assert res.exit_code == 0, res.output
        receipts = _read_receipts(out)
        assert receipts[0]["grounding"] == "OPAQUE"
        assert any(s["kind"] == "coverage-gap" for s in receipts[0]["seams"])

    def test_audit_no_self_report(self, tmp_path: Path) -> None:
        # A target that prints and writes fake GROUNDED markers gains nothing:
        # the verdict derives only from observer records, and the cited source
        # was never read.
        data = tmp_path / "data.csv"
        data.write_text("x\n1\n")
        out = tmp_path / "audit-out"
        fake = json.dumps({"finding_id": "f1", "grounding": "GROUNDED"})
        script = _script(tmp_path, (
            "import os, json\n"
            f"print({fake!r})\n"
            f"os.makedirs({str(out)!r}, exist_ok=True)\n"
            f"open(os.path.join({str(out)!r}, 'receipts.jsonl'), 'w')"
            f".write({fake!r} + '\\n')\n"
        ))
        key = _bootstrap_key(tmp_path, "auditor.key")

        r = CliRunner()
        res = _audit(r, tmp_path, script, {"f1": str(data)}, out, key)
        assert res.exit_code == 0, res.output
        receipts = _read_receipts(out)
        assert len(receipts) == 1
        assert receipts[0]["grounding"] == "UNGROUNDED"
        assert receipts[0]["reason"] != "GROUNDED"


class TestAuditCorpus:
    def _spec(self, corpus: Path, tmp_path: Path, run_id: str) -> Path:
        data = tmp_path / f"{run_id}.csv"
        data.write_text("x\n1\n")
        script = _script(tmp_path, f"open({str(data)!r}).read()\n",
                         name=f"{run_id}.py")
        spec = corpus / f"{run_id}.json"
        spec.write_text(json.dumps({
            "command": [str(script)],
            "findings": {f"{run_id}-f1": str(data)},
        }))
        return spec

    def test_audit_corpus_resume(self, tmp_path: Path, monkeypatch) -> None:
        # Kill the corpus after the first run; re-invocation skips the
        # completed run and completes the rest with real per-run isolation.
        import mareforma.audit as audit_mod

        corpus = tmp_path / "corpus"
        corpus.mkdir()
        for run_id in ("run-a", "run-b", "run-c"):
            self._spec(corpus, tmp_path, run_id)
        key = _bootstrap_key(tmp_path, "auditor.key")
        out = tmp_path / "corpus-out"

        # Killed once the first run's record is signed, which is the point at
        # which it counts as complete.
        real_sign = audit_mod._sign_run_outputs
        executed: list[str] = []

        def killed_after_first(run_dir, handoff, signer):
            real_sign(run_dir, handoff, signer)
            executed.append(run_dir.name)
            raise RuntimeError("simulated kill")

        monkeypatch.setattr(audit_mod, "_sign_run_outputs", killed_after_first)
        r = CliRunner()
        first = r.invoke(cli, ["audit", "--corpus", str(corpus),
                               "--out", str(out), "--key", str(key)])
        assert isinstance(first.exception, RuntimeError)
        assert executed == ["run-a"]
        first_run_json = (out / "run-a" / "run.json").read_text()

        monkeypatch.setattr(audit_mod, "_sign_run_outputs", real_sign)
        second = r.invoke(cli, ["audit", "--corpus", str(corpus),
                                "--out", str(out), "--key", str(key)])
        assert second.exit_code == 0, second.output
        # The completed run was skipped, not re-run: its record is untouched.
        assert (out / "run-a" / "run.json").read_text() == first_run_json
        for run_id in ("run-a", "run-b", "run-c"):
            run = _read_run(out / run_id)
            assert run["completed"] is True
            receipts = _read_receipts(out / run_id)
            assert receipts[0]["finding_id"] == f"{run_id}-f1"
            assert receipts[0]["grounding"] == "GROUNDED"

    def test_audit_corpus_ignores_planted_unsigned_run_record(
        self, tmp_path: Path,
    ) -> None:
        # A hostile target in run A could pre-plant run B's directory with a
        # plain {"completed": true} record and forged receipts. Resume honors
        # only a run record signed by the auditor's key, so the planted run
        # re-executes and the forged receipts are replaced.
        corpus = tmp_path / "corpus"
        corpus.mkdir()
        self._spec(corpus, tmp_path, "run-a")
        key = _bootstrap_key(tmp_path, "auditor.key")
        out = tmp_path / "corpus-out"
        run_dir = out / "run-a"
        run_dir.mkdir(parents=True)
        (run_dir / "run.json").write_text(json.dumps(
            {"completed": True, "exit_code": 0, "partial": False}))
        (run_dir / "receipts.jsonl").write_text(json.dumps(
            {"finding_id": "run-a-f1", "grounding": "GROUNDED",
             "reason": "forged"}) + "\n")

        r = CliRunner()
        res = r.invoke(cli, ["audit", "--corpus", str(corpus),
                             "--out", str(out), "--key", str(key)])
        assert res.exit_code == 0, res.output
        assert "skip" not in res.output
        receipts = _read_receipts(run_dir)
        assert receipts[0]["reason"] != "forged"
        assert receipts[0]["grounding"] == "GROUNDED"
        assert list((run_dir / "envelopes").glob("*.json"))

    def test_audit_corpus_target_cannot_sign_a_sibling_run(
        self, tmp_path: Path,
    ) -> None:
        # A hostile target walks the frame stack for the auditor's signing key
        # and plants a correctly signed run record and receipts in a sibling
        # run's directory. The signer must not share an interpreter with an
        # audited target, so the sibling still executes and keeps only the
        # verdicts the observer produced.
        corpus = tmp_path / "corpus"
        corpus.mkdir()
        data = tmp_path / "shared.csv"
        data.write_text("x\n1\n")
        sentinel = tmp_path / "victim-ran.txt"
        hostile = _script(tmp_path, (
            "import json, sys\n"
            "from pathlib import Path\n"
            "signer = out = None\n"
            "frame = sys._getframe()\n"
            "while frame is not None:\n"
            "    signer = signer or frame.f_locals.get('signer')\n"
            "    if out is None and isinstance(frame.f_locals.get('out'), Path):\n"
            "        out = frame.f_locals['out']\n"
            "    frame = frame.f_back\n"
            "if signer is None:\n"
            "    raise SystemExit(0)\n"
            "from mareforma import signing\n"
            "victim = out.parent / 'zz-victim'\n"
            "(victim / 'envelopes').mkdir(parents=True, exist_ok=True)\n"
            "receipt = {'finding_id': 'zz-victim-f1', 'grounding': 'GROUNDED',\n"
            "           'reason': 'FORGED BY RUN A', 'exit_code': 0,\n"
            "           'partial': False}\n"
            "(victim / 'receipts.jsonl').write_text(json.dumps(receipt) + '\\n')\n"
            "(victim / 'envelopes' / '001-forged.json').write_text(\n"
            "    json.dumps(signing.sign_audit_receipt(receipt, signer)))\n"
            "(victim / 'run.json').write_text(json.dumps(\n"
            "    signing.sign_audit_run({'completed': True, 'exit_code': 0,\n"
            "                            'partial': False}, signer)))\n"
        ), name="aa_hostile.py")
        victim = _script(tmp_path, (
            f"open({str(data)!r}).read()\n"
            f"open({str(sentinel)!r}, 'w').write('ran')\n"
        ), name="zz_victim.py")
        (corpus / "aa-hostile.json").write_text(json.dumps({
            "command": [str(hostile)],
            "findings": {"aa-hostile-f1": str(data)},
        }))
        (corpus / "zz-victim.json").write_text(json.dumps({
            "command": [str(victim)],
            "findings": {"zz-victim-f1": str(data)},
        }))
        key = _bootstrap_key(tmp_path, "auditor.key")
        out = tmp_path / "corpus-out"

        r = CliRunner()
        res = r.invoke(cli, ["audit", "--corpus", str(corpus),
                             "--out", str(out), "--key", str(key)])
        assert res.exit_code == 0, res.output
        assert "skip zz-victim" not in res.output
        assert sentinel.exists(), "the victim run never executed"
        receipts = _read_receipts(out / "zz-victim")
        assert [rec["reason"] for rec in receipts] != ["FORGED BY RUN A"]
        assert not (out / "zz-victim" / "envelopes" / "001-forged.json").exists()

    def test_audit_corpus_signs_the_child_observation_not_the_directory(
        self, tmp_path: Path,
    ) -> None:
        # The target reads nothing, so the honest verdict is UNGROUNDED, and
        # rewrites receipts.jsonl to GROUNDED from an atexit hook that runs
        # after the observer wrote its records and before the child exits.
        # The parent signs what the child observed, so the envelope keeps the
        # honest verdict whatever the run directory holds.
        corpus = tmp_path / "corpus"
        corpus.mkdir()
        data = tmp_path / "real.csv"
        data.write_text("x\n1\n")
        launder = _script(tmp_path, (
            "import atexit, json, sys\n"
            "def rewrite():\n"
            "    out = sys.argv[sys.argv.index('--out') + 1]\n"
            "    path = out + '/receipts.jsonl'\n"
            "    rec = json.loads(open(path).read())\n"
            "    rec['grounding'] = 'GROUNDED'\n"
            f"    rec['grounded_sources'] = [{str(data)!r}]\n"
            "    open(path, 'w').write(json.dumps(rec) + '\\n')\n"
            "atexit.register(rewrite)\n"
        ), name="launder.py")
        (corpus / "run1.json").write_text(json.dumps({
            "command": [str(launder)],
            "findings": {"F1": str(data)},
        }))
        key = _bootstrap_key(tmp_path, "auditor.key")
        out = tmp_path / "corpus-out"

        r = CliRunner()
        res = r.invoke(cli, ["audit", "--corpus", str(corpus),
                             "--out", str(out), "--key", str(key)])
        assert res.exit_code == 0, res.output
        envelope = json.loads(
            (out / "run1" / "envelopes" / "001-F1.json").read_text())
        record = json.loads(base64.standard_b64decode(envelope["payload"]))
        assert record["grounding"] == "UNGROUNDED", record
        assert not record["grounded_sources"]

    @pytest.mark.skipif(
        not Path("/proc/self/fd").exists(), reason="needs /proc to find fds")
    def test_audit_corpus_ignores_what_the_target_wrote_on_the_channel(
        self, tmp_path: Path,
    ) -> None:
        # The target reads nothing, so the honest verdict is UNGROUNDED. It
        # hunts every channel out of its process, the descriptors it inherited
        # and any path they name, and writes a forged record on each one while
        # it runs and again from an atexit hook that fires after the observer
        # has written and closed. What the parent signs must stay the child's
        # observation, and no key the target chose may ride into it.
        corpus = tmp_path / "corpus"
        corpus.mkdir()
        data = tmp_path / "real.csv"
        data.write_text("x\n1\n")
        forge = _script(tmp_path, (
            "import atexit, json, os\n"
            "FORGED = json.dumps({\n"
            "    'run_record': {'target': ['forge.py'], 'exit_code': 0,\n"
            "                   'partial': False, 'findings': ['F1'],\n"
            "                   'reads': [], 'seams': [],\n"
            "                   'coverage': {'reads_seen': 0,\n"
            "                                'opens_detected': 0,\n"
            "                                'read_coverage_fraction': None},\n"
            "                   'completed': True, 'forged': True},\n"
            "    'receipts': [{'finding_id': 'F1', 'grounding': 'GROUNDED',\n"
            "                  'reason': 'forged', 'exit_code': 0,\n"
            f"                  'grounded_sources': [{str(data)!r}],\n"
            f"                  'cited_sources': [{str(data)!r}],\n"
            "                  'reads': [], 'seams': [], 'partial': False}],\n"
            "})\n"
            "channels = []\n"
            "for name in os.listdir('/proc/self/fd'):\n"
            "    fd = int(name)\n"
            "    if fd < 3:\n"
            "        continue\n"
            "    try:\n"
            "        channels.append(\n"
            "            (os.dup(fd), os.readlink('/proc/self/fd/' + name)))\n"
            "    except OSError:\n"
            "        pass\n"
            "def forge():\n"
            "    for fd, link in channels:\n"
            "        try:\n"
            "            os.write(fd, FORGED.encode() + b'\\n')\n"
            "        except OSError:\n"
            "            pass\n"
            "        try:\n"
            "            if os.path.exists(link):\n"
            "                open(link, 'w').write(FORGED)\n"
            "        except OSError:\n"
            "            pass\n"
            "forge()\n"
            "atexit.register(forge)\n"
        ), name="forge.py")
        (corpus / "run1.json").write_text(json.dumps({
            "command": [str(forge)],
            "findings": {"F1": str(data)},
        }))
        key = _bootstrap_key(tmp_path, "auditor.key")
        out = tmp_path / "corpus-out"

        r = CliRunner()
        res = r.invoke(cli, ["audit", "--corpus", str(corpus),
                             "--out", str(out), "--key", str(key)])
        assert res.exit_code == 0, res.output
        envelope = json.loads(
            (out / "run1" / "envelopes" / "001-F1.json").read_text())
        record = json.loads(base64.standard_b64decode(envelope["payload"]))
        assert record["grounding"] == "UNGROUNDED", record
        assert not record["grounded_sources"]
        assert record["reason"] != "forged"
        run = _read_run(out / "run1")
        assert "forged" not in run, run
        assert run["coverage"]["reads_seen"] == 0

    @pytest.mark.skipif(
        not Path("/proc/self/fd").exists(), reason="needs /proc to find fds")
    def test_audit_corpus_completes_when_the_target_leaves_no_terminator(
        self, tmp_path: Path,
    ) -> None:
        # An honest run: the target reads its cited source, so the verdict is
        # GROUNDED. It also writes one byte with no terminator to every
        # descriptor it inherited, the handoff channel among them, which puts
        # those bytes in front of the observer's frame. The run must still
        # complete, or a target could deny its own audit indefinitely.
        corpus = tmp_path / "corpus"
        corpus.mkdir()
        data = tmp_path / "real.csv"
        data.write_text("x\n1\n")
        noisy = _script(tmp_path, (
            f"open({str(data)!r}).read()\n"
            "import os, stat\n"
            "for name in os.listdir('/proc/self/fd'):\n"
            "    try:\n"
            "        fd = int(name)\n"
            "        if fd > 2 and stat.S_ISFIFO(os.fstat(fd).st_mode):\n"
            "            os.write(fd, b'x')\n"
            "    except OSError:\n"
            "        pass\n"
        ), name="noisy.py")
        (corpus / "run1.json").write_text(json.dumps({
            "command": [str(noisy)],
            "findings": {"F1": str(data)},
        }))
        key = _bootstrap_key(tmp_path, "auditor.key")
        out = tmp_path / "corpus-out"

        r = CliRunner()
        res = r.invoke(cli, ["audit", "--corpus", str(corpus),
                             "--out", str(out), "--key", str(key)])
        assert res.exit_code == 0, res.output
        run = _read_run(out / "run1")
        assert run["completed"] is True
        assert run["coverage"]["reads_seen"] >= 1, run
        receipts = _read_receipts(out / "run1")
        assert receipts[0]["grounding"] == "GROUNDED", receipts
        # Resume must see it as done rather than re-run it forever.
        again = r.invoke(cli, ["audit", "--corpus", str(corpus),
                               "--out", str(out), "--key", str(key)])
        assert again.exit_code == 0, again.output
        assert "skip run1" in again.output, again.output

    def test_audit_corpus_records_crashing_target_exit_code(
        self, tmp_path: Path,
    ) -> None:
        # A crashing target inside a corpus run still emits partial receipts
        # and records the target's own exit code; the corpus completes.
        corpus = tmp_path / "corpus"
        corpus.mkdir()
        data = tmp_path / "crash.csv"
        data.write_text("x\n1\n")
        crash = _script(tmp_path, (
            f"open({str(data)!r}).read()\n"
            "raise ValueError('boom')\n"
        ), name="crash.py")
        exits = _script(tmp_path, "raise SystemExit(7)\n", name="exits.py")
        (corpus / "crash.json").write_text(json.dumps({
            "command": [str(crash)],
            "findings": {"crash-f1": str(data)},
        }))
        (corpus / "exits.json").write_text(json.dumps({
            "command": [str(exits)],
            "findings": {"exits-f1": str(data)},
        }))
        key = _bootstrap_key(tmp_path, "auditor.key")
        out = tmp_path / "corpus-out"

        r = CliRunner()
        res = r.invoke(cli, ["audit", "--corpus", str(corpus),
                             "--out", str(out), "--key", str(key)])
        assert res.exit_code == 0, res.output
        run = _read_run(out / "crash")
        assert run["exit_code"] == 1
        assert run["partial"] is True
        receipts = _read_receipts(out / "crash")
        assert receipts[0]["grounding"] == "GROUNDED"
        run = _read_run(out / "exits")
        assert run["exit_code"] == 7


class TestAuditHandoffChannel:
    """What a corpus parent accepts on the handoff pipe: the child's nonce
    line, then the frame carrying it, wherever on the stream it lands. The
    audited target shares that descriptor, so everything else on it is the
    target's, not the observer's.
    """

    NONCE = "0123456789abcdef"

    def _read(self, stream: str):
        from mareforma.audit import _read_handoff

        read_fd, write_fd = os.pipe()
        # Filled from a second thread: a stream longer than the pipe buffer
        # blocks its writer until the reader drains it, the way the real
        # channel does.
        writer = threading.Thread(
            target=self._fill, args=(write_fd, stream.encode("utf-8")),
            daemon=True,
        )
        writer.start()
        try:
            return _read_handoff(read_fd)
        finally:
            writer.join(timeout=60)

    @staticmethod
    def _fill(write_fd: int, payload: bytes) -> None:
        with os.fdopen(write_fd, "wb") as fh:
            fh.write(payload)

    def _frame(self, run_record: dict, prefix: str | None = None) -> str:
        head = self.NONCE + " " if prefix is None else prefix
        return head + json.dumps(
            {"run_record": run_record, "receipts": []}) + "\n"

    def test_only_the_frame_carrying_the_childs_nonce_is_read(self) -> None:
        # The target writes a well-formed frame of its own before the observer
        # emits and another after it. Neither carries the nonce the child sent
        # before the target could run, so neither is the observer's record.
        handoff = self._read(
            self.NONCE + "\n"
            + self._frame({"completed": True, "exit_code": 1}, prefix="")
            + self._frame({"completed": True, "exit_code": 0})
            + self._frame({"completed": True, "exit_code": 2}, prefix="")
        )
        assert handoff == {"receipts": [],
                           "run_record": {"completed": True, "exit_code": 0}}

    def test_a_frame_the_child_never_finished_hands_over_nothing(self) -> None:
        # A child killed mid-write leaves a line without its terminator, and a
        # child killed before it emitted leaves the nonce alone.
        assert self._read(self.NONCE + "\n"
                          + self._frame({"completed": True})[:-5]) is None
        assert self._read(self.NONCE + "\n") is None
        assert self._read("") is None

    def test_target_bytes_in_front_of_the_frame_do_not_hide_it(self) -> None:
        # The target owes the shared descriptor no terminator, so what it
        # leaves there runs into the front of the observer's frame. The frame
        # is still the observer's record: a target able to suppress the record
        # could veto its own audit, and every re-invocation would repeat it.
        record = {"completed": True, "exit_code": 0}
        expected = {"receipts": [], "run_record": record}
        assert self._read(self.NONCE + "\n" + self._frame(record)) == expected
        assert self._read(
            self.NONCE + "\n" + "x" + self._frame(record)) == expected
        assert self._read(
            self.NONCE + "\n" + '{"a":' + self._frame(record)) == expected

    def test_a_long_unterminated_prefix_does_not_hide_the_frame(self) -> None:
        # Nor does a large one, and reading it costs the parent a fixed
        # buffer: what it discards it discards as it goes.
        record = {"completed": True, "exit_code": 0}
        assert self._read(
            self.NONCE + "\n" + "n" * (4 << 20) + self._frame(record)
        ) == {"receipts": [], "run_record": record}

    def test_the_last_frame_carrying_the_nonce_is_the_record(self) -> None:
        # The observer emits once, after the target has finished running, so
        # the marked frame that comes last is the one it wrote.
        assert self._read(
            self.NONCE + "\n"
            + self._frame({"completed": True, "exit_code": 9})
            + self._frame({"completed": True, "exit_code": 0})
        ) == {"receipts": [],
              "run_record": {"completed": True, "exit_code": 0}}

    def test_a_key_the_observer_did_not_write_is_refused(self) -> None:
        # The parent signs the run record whole, so a key it does not know,
        # inside the record or beside it, is a refusal rather than something
        # passed through to the signer.
        assert self._read(
            self.NONCE + "\n"
            + self._frame({"completed": True, "forged": True})) is None
        assert self._read(self.NONCE + "\n" + self.NONCE + " " + json.dumps(
            {"run_record": {"completed": True}, "receipts": [],
             "forged": True}) + "\n") is None


class TestAuditReceiptPublicVerify:
    """A signed audit receipt is publicly verifiable: a third party who holds
    only the auditor's exported public key can confirm it, without ever seeing
    the private key."""

    def test_receipt_verifies_with_public_pem_only(self, tmp_path: Path) -> None:
        data = tmp_path / "data.csv"
        data.write_text("x\n1\n")
        script = _script(tmp_path, f"open({str(data)!r}).read()\n")
        r = CliRunner()
        with r.isolated_filesystem(temp_dir=tmp_path):
            _bootstrap_default_key()
            auditor = _bootstrap_key(Path("."), "auditor.key")
            out = Path("audit-out")
            res = _audit(r, Path("."), script, {"f1": str(data)}, out, auditor)
            assert res.exit_code == 0, res.output
            envelope_path = next((out / "envelopes").glob("*.json"))

            # Export the public half only, the material a third party gets.
            shown = r.invoke(
                cli, ["key", "show", "--pem", "--key-path", str(auditor)])
            assert shown.exit_code == 0, shown.output
            pub = Path("auditor_pub.pem")
            pub.write_text(shown.output)
            assert b"PRIVATE" not in pub.read_bytes()

            verified = r.invoke(
                cli, ["verify", str(envelope_path), "--key", str(pub)])
            assert verified.exit_code == 0, verified.output
            assert "verified" in verified.output
