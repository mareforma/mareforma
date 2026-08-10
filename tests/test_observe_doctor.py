"""Observer coverage self-report (the doctor) and the measure CLI.

The doctor tells a stranger what the observer can and cannot see in THIS
environment before they trust a verdict; the measure command aggregates a run's
receipts into the reported split. These pin their contracts.
"""
from __future__ import annotations

import json

from click.testing import CliRunner

import mareforma.observe as obs
from mareforma.cli import cli
from mareforma.observe import _doctor, _scope


def test_coverage_report_lists_stdlib_and_seams():
    report = obs.coverage_report()
    stdlib = {row["loader"] for row in report["stdlib_wrapped"]}
    assert any("builtins.open" in s for s in stdlib)
    assert any("sqlite3" in s for s in stdlib)
    # builtins.open is always wrapped once the observer installs.
    assert all(
        row["wrapped"] for row in report["stdlib_wrapped"]
        if "builtins.open" in row["loader"]
    )
    kinds = {row["kind"] for row in report["seam_kinds"]}
    assert {"socket", "subprocess", "thread", "coverage-gap"} <= kinds
    assert report["known_bounds"]


def test_coverage_report_names_every_seam_kind_the_classifier_records():
    # The table is the operator's list of what can force OPAQUE, so a kind the
    # classifier records and the table omits reads as a seam that cannot happen.
    # Equality, not containment: the next kind added to _scope fails here.
    report = obs.coverage_report()
    kinds = {row["kind"] for row in report["seam_kinds"]}
    assert kinds == set(_scope.SEAM_KINDS)
    assert _scope.ABORT_SEAM in kinds
    # Every kind carries a written effect, not the fail-closed placeholder.
    assert set(_doctor._SEAM_EFFECTS) == set(_scope.SEAM_KINDS)
    assert all(row["effect"] for row in report["seam_kinds"])


def test_third_party_report_marks_httpx_wrapped():
    # httpx is a core dependency, so it is importable; once the observer is
    # installed it is wrapped.
    report = obs.coverage_report()
    httpx_rows = [r for r in report["third_party"] if "httpx" in r["loader"]]
    assert httpx_rows
    assert any(r["wrapped"] for r in httpx_rows)


def test_observe_doctor_cli_json():
    runner = CliRunner()
    result = runner.invoke(cli, ["observe", "--doctor", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "stdlib_wrapped" in payload
    assert "seam_kinds" in payload


def test_observe_doctor_cli_human():
    runner = CliRunner()
    result = runner.invoke(cli, ["observe", "--doctor"])
    assert result.exit_code == 0
    assert "Observer coverage" in result.output


def test_observe_without_doctor_exits_2():
    runner = CliRunner()
    result = runner.invoke(cli, ["observe"])
    assert result.exit_code == 2


def _write_receipts(path, verdicts):
    path.write_text("\n".join(json.dumps(v.receipt()) for v in verdicts))


def test_measure_cli_reports_split(tmp_path):
    from mareforma.observe import GroundingVerdict, ReadRecord, SeamEvent
    from mareforma.observe import ObservedGrounding as OG

    verdicts = [
        GroundingVerdict(OG.GROUNDED, "g", cited_sources=("/x",),
                         reads=(ReadRecord("file", "/x", True),)),
        GroundingVerdict(OG.OPAQUE, "o", seams=(SeamEvent("subprocess", "s"),)),
    ]
    recs = tmp_path / "r.jsonl"
    _write_receipts(recs, verdicts)
    runner = CliRunner()
    result = runner.invoke(cli, ["measure", str(recs), "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["total"] == 2
    assert payload["opaque_by_seam"] == {"subprocess": 1}
    assert "summary" in payload


def test_measure_cli_accepts_redact_home_flag(tmp_path):
    # Smoke: --redact-home is accepted and the command still succeeds. The measure
    # report itself carries no paths (counts / fractions / seam-kinds), so redaction
    # is a no-op on this surface; the redaction LOGIC is covered by the unit test
    # below. Asserting "path not in output" here is vacuously true and proves
    # nothing, so this only checks the flag path does not crash.
    from mareforma.observe import GroundingVerdict, ReadRecord
    from mareforma.observe import ObservedGrounding as OG

    home_path = str(tmp_path / "secret" / "data.csv")
    verdicts = [
        GroundingVerdict(OG.GROUNDED, "g", cited_sources=(home_path,),
                         grounded_sources=(home_path,),
                         reads=(ReadRecord("file", home_path, True),)),
    ]
    recs = tmp_path / "r.jsonl"
    _write_receipts(recs, verdicts)
    result = CliRunner().invoke(cli, ["measure", str(recs), "--json", "--redact-home"])
    assert result.exit_code == 0


def test_redact_home_rewrites_home_paths(tmp_path, monkeypatch):
    # The actual redaction contract: every absolute home path in an emitted
    # artifact is rewritten to ~, recursing through dicts and lists, leaving
    # non-home strings and non-strings untouched.
    from mareforma.cli import _redact_home

    monkeypatch.setenv("HOME", str(tmp_path))
    home_path = str(tmp_path / "secret" / "data.csv")
    obj = {"a": home_path, "b": ["keep", {"c": home_path}], "n": 3}
    red = _redact_home(obj)
    assert str(tmp_path) not in json.dumps(red)
    assert red["a"] == "~/secret/data.csv"
    assert red["b"][1]["c"] == "~/secret/data.csv"
    assert red["b"][0] == "keep"
    assert red["n"] == 3


def test_redact_home_root_container_is_noop(monkeypatch):
    # HOME=/ (a root container) must NOT turn redaction into a global slash->tilde
    # corruptor: a one-character home is skipped, every path survives intact.
    from mareforma.cli import _redact_home

    monkeypatch.setenv("HOME", "/")
    obj = {"a": "/data/trial.csv", "b": ["/etc/x", {"c": "/home/u/y"}]}
    red = _redact_home(obj)
    assert red["a"] == "/data/trial.csv"
    assert red["b"][0] == "/etc/x"
    assert red["b"][1]["c"] == "/home/u/y"
    assert "~" not in json.dumps(red)


def test_measure_cli_bad_path_exits_1(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["measure", str(tmp_path / "nope.jsonl")])
    assert result.exit_code == 1


def test_coverage_report_lists_every_wrapped_loader():
    # The report is the observer's self-report. A loader that is wrapped but has
    # no row under-reports coverage: a polars or duckdb operator reads "not
    # covered" and instruments a seam the observer already sees.
    report = obs.coverage_report()
    loaders = {row["loader"] for row in report["stdlib_wrapped"]}
    loaders |= {row["loader"] for row in report["third_party"]}
    for name in ("io.open", "polars", "duckdb"):
        assert any(name in loader for loader in loaders), name


def test_every_installed_wrapper_is_declared():
    # Drift guard: every key the loaders put in ``_reals`` belongs to a reported
    # loader group or to the seam plumbing. A loader added without a doctor row
    # fails here instead of silently shrinking the report.
    from mareforma.observe import _loaders

    obs.coverage_report()  # installs, so _reals reflects this environment
    declared = set(_loaders.SEAM_WRAPS)
    for keys in (*_loaders.STDLIB_WRAPS.values(),
                 *_loaders.THIRD_PARTY_WRAPS.values()):
        declared.update(keys)
    assert set(_loaders._reals) <= declared
