"""``mareforma diagnose`` — run a Python target under the observer and report.

The zero-config wedge: point it at a script and it answers the two questions a
transcript cannot: did real data flow into this run, and did a step silently
fail behind a seam the observer could not cross. It runs the target IN-PROCESS
via :mod:`runpy` (the coverage.py pattern) — launching a subprocess would put
the target behind the observer's own subprocess seam and defeat the point.

Honesty rules, inherited from the observer:

- **No guessed citations.** Without ``--cites`` the report is observation-only:
  the reads it saw, the seams it hit, the coverage fraction — but NO grounding
  verdict. A grounding verdict requires a stated citation; inventing one would
  fabricate a grounding claim.
- **A crash is still a report.** If the target raises, its traceback prints as
  Python would, the partial observation is still emitted (marked as partial),
  and diagnose exits with the target's own exit code.
"""
from __future__ import annotations

import json
import os
import re
import runpy
import sys
import traceback
from typing import Callable

import click

_INTERP = re.compile(r"python[0-9.]*\Z")


def _looks_like_interpreter(token: str) -> bool:
    """True if *token* is a leading ``python`` / ``python3.x`` interpreter word."""
    return bool(_INTERP.fullmatch(os.path.basename(token)))


def _exit_code_of(exc: SystemExit) -> int:
    """Normalize a SystemExit's code to an int, Python's own convention."""
    code = exc.code
    if code is None:
        return 0
    if isinstance(code, int):
        return code
    return 1


def _run_target(command: list[str]) -> None:
    """Execute the target in-process. Raises SystemExit / the target's exception.

    Strips a leading interpreter token (``python``/``python3``) so both
    ``diagnose -- python x.py`` and ``diagnose -- x.py`` work. ``-m mod`` runs a
    module; anything else runs a script path.
    """
    argv = list(command)
    if argv and _looks_like_interpreter(argv[0]):
        argv = argv[1:]
    if not argv:
        raise click.UsageError(
            "diagnose needs a target after `--`, e.g. `-- python analysis.py`"
        )

    old_argv = sys.argv
    # Snapshot the whole path list: run_path/run_module and the target itself
    # may insert entries (the common `sys.path.insert(0, here)` idiom), so
    # restoring only index 0 would leak — or duplicate — entries into a
    # long-lived host process (tests). Restore the list wholesale.
    old_path = list(sys.path)
    try:
        if argv[0] == "-m":
            if len(argv) < 2:
                raise click.UsageError("`-m` needs a module name")
            module = argv[1]
            sys.argv = [module, *argv[2:]]
            runpy.run_module(module, run_name="__main__", alter_sys=True)
        else:
            script = argv[0]
            sys.argv = [script, *argv[1:]]
            runpy.run_path(script, run_name="__main__")
    finally:
        sys.argv = old_argv
        sys.path[:] = old_path


def _build_report(command, cites, verdict, exit_code, crashed, tb_text) -> dict:
    """Assemble the observation report from the computed verdict."""
    reads = [
        {"kind": r.kind, "identifier": r.identifier, "nonempty": r.nonempty}
        for r in verdict.reads
    ]
    seams = [{"kind": s.kind, "detail": s.detail} for s in verdict.seams]
    coverage = {
        "reads_seen": verdict.reads_seen,
        "opens_detected": verdict.opens_detected,
        "read_coverage_fraction": verdict.read_coverage_fraction(),
    }
    report = {
        "target": list(command),
        "exit_code": exit_code,
        "partial": crashed,
        "cites": list(cites),
        "reads": reads,
        "seams": seams,
        "coverage": coverage,
        # A grounding verdict is emitted ONLY when a citation was stated. Without
        # cites, "UNGROUNDED" would be meaningless (there is no source to ground
        # on), so the verdict is withheld rather than misreported.
        "grounding": (
            {"grounding": verdict.grounding.value, "reason": verdict.reason}
            if cites else None
        ),
    }
    if tb_text:
        report["traceback"] = tb_text
    return report


def _echo_report(report: dict, redact: "Callable[[str], str] | None") -> None:
    """Print the observation report in human-readable form."""
    def out(line: str) -> None:
        click.echo(redact(line) if redact else line)

    click.echo(click.style("OBSERVATION REPORT", bold=True, fg="cyan"))
    out("  target: " + " ".join(report["target"]))
    if report["partial"]:
        click.echo(click.style(
            "  target exited with error, partial observation", fg="yellow"))
    cov = report["coverage"]
    frac = cov["read_coverage_fraction"]
    frac_s = "n/a" if frac is None else f"{frac:.2f}"
    click.echo(
        f"  reads: {len(report['reads'])}  "
        f"seams: {len(report['seams'])}  "
        f"coverage: {cov['reads_seen']}/{cov['opens_detected']} ({frac_s})"
    )
    click.echo("")
    if report["reads"]:
        click.echo("  Reads:")
        for r in report["reads"]:
            tag = "" if r["nonempty"] else "  (empty)"
            out(f"    [{r['kind']}] {r['identifier']}{tag}")
    else:
        click.echo("  Reads: none observed")
    if report["seams"]:
        click.echo("  Seams (observer could not see across):")
        for s in report["seams"]:
            out(f"    [{s['kind']}] {s['detail']}")
    else:
        click.echo("  Seams: none")
    click.echo("")
    if report["grounding"] is not None:
        g = report["grounding"]
        color = {"GROUNDED": "green", "UNGROUNDED": "red", "OPAQUE": "yellow"}.get(
            g["grounding"], "white")
        click.echo(
            "  Grounding: " + click.style(g["grounding"], fg=color, bold=True))
        out(f"    {g['reason']}")
    else:
        click.echo(
            "  Grounding: not computed (no --cites; diagnose never guesses a "
            "citation)")


def run_diagnose(
    command: list[str],
    *,
    cites: list[str],
    as_json: bool,
    redact_home: "Callable[[str], str] | None",
) -> int:
    """Run COMMAND under the observer and print the report. Returns an exit code."""
    from mareforma.observe import observe

    exit_code = 0
    crashed = False
    tb_text = None

    with observe(cites=tuple(cites) or None) as obs:
        try:
            _run_target(command)
        except click.UsageError:
            # Bad invocation of diagnose itself — re-raise so click reports it.
            raise
        except SystemExit as exc:
            exit_code = _exit_code_of(exc)
        except BaseException:  # noqa: BLE001 — a target crash is expected input
            crashed = True
            exit_code = 1
            tb_text = traceback.format_exc()

    verdict = obs.verdict
    report = _build_report(command, cites, verdict, exit_code, crashed, tb_text)

    if as_json:
        # The traceback rides in the report's "traceback" field — the
        # machine-readable equivalent of Python's own stderr dump.
        text = json.dumps(report, indent=2)
        click.echo(redact_home(text) if redact_home else text)
    else:
        if crashed and tb_text:
            # Print the traceback the way Python would, before the report.
            click.echo(tb_text, err=True, nl=False)
        _echo_report(report, redact_home)

    return exit_code
