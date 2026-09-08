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


def test_every_self_contained_example_is_listed() -> None:
    """A new example is covered the day it lands, not the day someone recalls.

    The list above is the thing that rots: an example added later runs nowhere
    until a person remembers this file. So the directory is the authority and
    the list has to account for every entry in it, either by running it or by
    naming it as needing something a test cannot supply.
    """
    needs_more_than_a_temp_dir = {"05_drug_target_provenance"}
    present = {
        d.name for d in EXAMPLES.iterdir()
        if d.is_dir() and _script(d.name).is_file()
    }
    unaccounted = sorted(present - set(_SELF_CONTAINED) - needs_more_than_a_temp_dir)
    assert not unaccounted, (
        f"examples nothing runs: {unaccounted}. Add each to the list above, or "
        "to the set of ones needing more than a temporary directory."
    )
