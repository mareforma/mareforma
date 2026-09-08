"""The shipped examples are executed, not just read.

Two releases in a row removed something the examples used and left them
crashing on the first call. Nothing caught it: the other example tests parse
the files or ``exec`` one extracted function, and the CI job that runs them end
to end is a separate leg nobody waits for. So an example could name a removed
parameter and the suite stayed green over it.

These run each one as a subprocess, in a temporary directory, the way a reader
would. A non-zero exit is the failure; the output is only read to say what
broke. Slow by the standards of a unit test and worth it: an example that does
not run is a page of documentation that lies, and it is the first thing anybody
new to the package touches.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = ROOT / "examples"

# The ones that run offline against a temporary graph. Others need an API key
# or a network service and exit early without one, so running them here would
# assert nothing.
_SELF_CONTAINED = (
    "01_api_walkthrough",
    "02_compounding_agents",
    "03_documented_contestation",
    "04_private_data_public_findings",
)


def _script(name: str) -> Path:
    return EXAMPLES / name / f"{name}.py"


@pytest.mark.parametrize("name", _SELF_CONTAINED)
def test_the_example_runs_end_to_end(name: str, tmp_path: Path) -> None:
    script = _script(name)
    if not script.is_file():
        pytest.skip(f"{name} is not in this checkout")
    try:
        import langchain_core  # noqa: F401
    except ImportError:
        if name in ("03_documented_contestation", "04_private_data_public_findings"):
            pytest.skip("this example wraps its tools with langchain_core")

    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    # Its own HOME and config, so a run cannot read or write the developer's
    # real key, and cannot pick up a graph left by another test.
    env["HOME"] = str(tmp_path)
    env["XDG_CONFIG_HOME"] = str(tmp_path)
    run = subprocess.run(
        [sys.executable, str(script)],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=300,
    )
    assert run.returncode == 0, (
        f"{name} exited {run.returncode}\n"
        f"--- stdout tail ---\n{run.stdout[-1500:]}\n"
        f"--- stderr tail ---\n{run.stderr[-2000:]}"
    )


# Examples this file does not run, each with the reason and where it is held
# instead. Every entry has to be one or the other: an example nothing accounts
# for is a page that rots quietly.
_ACCOUNTED_ELSEWHERE = {
    "05_drug_target_provenance": "needs an API key; exits early without one",
    "06_ci_verify": "a CI recipe, no script; the exit codes it relies on are "
                    "pinned by test_exit_code_corpus and test_cli_trust",
    "07_silent_failure_catch": "the demo gate; run under the observer by "
                               "test_examples_silent_failure, which needs pandas",
}


def test_every_example_is_accounted_for() -> None:
    """A new example is covered the day it lands, not the day someone recalls.

    The lists above are the thing that rots. An earlier version of this guard
    only looked for a script named after its directory, so the two examples
    that are not named that way, including the demo gate the README leads with,
    were skipped in silence and the guard still passed. It reads the directory
    now and every entry has to be either run here or named above.

    Reading the directory is also what makes it the one test here that cannot
    run from the sdist. The archive ships the suite and not the examples, so
    there is no tree to read and nothing to account for; the siblings skip per
    example on the same absence. Without this the guard raises
    ``FileNotFoundError`` in the sdist job, which is the only leg that runs the
    suite from the archive.
    """
    if not EXAMPLES.is_dir():
        pytest.skip("the examples tree is not in this checkout")
    present = {d.name for d in EXAMPLES.iterdir() if d.is_dir()}
    unaccounted = sorted(
        present - set(_SELF_CONTAINED) - set(_ACCOUNTED_ELSEWHERE)
    )
    assert not unaccounted, (
        f"examples nothing accounts for: {unaccounted}. Add each to the list "
        "this file runs, or to _ACCOUNTED_ELSEWHERE with where it is held."
    )


def test_the_examples_named_as_held_elsewhere_really_are() -> None:
    """A reason written in a comment is not a test.

    Each entry names where its example is covered. If that file goes, the
    example is unaccounted for again and this says so rather than the entry
    quietly becoming a promise nobody keeps.
    """
    holders = {
        "06_ci_verify": ("test_exit_code_corpus.py", "test_cli_trust.py"),
        "07_silent_failure_catch": ("test_examples_silent_failure.py",),
    }
    here = Path(__file__).resolve().parent
    for example, files in holders.items():
        for name in files:
            assert (here / name).is_file(), (
                f"{example} is recorded as held by {name}, which is gone"
            )
