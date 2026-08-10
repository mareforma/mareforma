"""Example 07's grounding catch, held as a test.

Two pipelines print the same ratio; only one reads the cited data. The example
exists to show that ``mareforma diagnose`` tells them apart, so the guard runs
both under the observer and pins the verdicts. Needs pandas (the ``test-heavy``
extra), so it skips on the dev-only legs the way the other grounding tests do.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from mareforma.cli import cli
from tests._helpers import _requires_repo_checkout

pytest.importorskip("pandas")
pytestmark = _requires_repo_checkout

_EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "07_silent_failure_catch"


def _diagnose(script: str, monkeypatch) -> str:
    monkeypatch.chdir(_EXAMPLE)
    res = CliRunner().invoke(
        cli, ["diagnose", "--cites", "data.csv", "--", script],
        catch_exceptions=False,
    )
    assert res.exit_code == 0, res.output
    return res.output


def test_honest_run_is_grounded(monkeypatch) -> None:
    assert "Grounding: GROUNDED" in _diagnose("analysis.py", monkeypatch)


def test_silent_fallback_is_ungrounded(monkeypatch) -> None:
    assert "Grounding: UNGROUNDED" in _diagnose("analysis_fallback.py", monkeypatch)
