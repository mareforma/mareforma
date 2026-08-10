"""Disclosure of the unverified-policy stall.

A project's trust rules live in a root-signed ``project_policy`` envelope. When
the flat row is edited out from under that envelope (a co-resident process with
plain SQLite, which the append-only trigger cannot reach), every enforcement
falls back to the fail-closed strictest reading. That is correct as a defence
but silent as a signal: promotions and restores refuse with no stated cause.

``health()`` and ``mareforma status`` now name the state so an operator meets
the tampered policy directly rather than inferring it from a refusal.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from click.testing import CliRunner

import mareforma
from mareforma.cli import cli
from mareforma.db import open_db
from mareforma.health import HealthReport, _compute_traffic_light, compute_health
from tests._helpers import _bootstrap_key


def _set_signed_policy_with_a_claim(tmp_path: Path) -> Path:
    """Declare a root-signed policy and record one claim; return the db path."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.require_rekor_witnessing()
        g.assert_claim("a finding under a signed policy", classification="ANALYTICAL")
    return tmp_path / ".mareforma" / "graph.db"


def _tamper_policy_flat_column(db_file: Path) -> None:
    """Flip a policy rule in the flat row so the signed envelope no longer binds.

    The append-only trigger is dropped first, standing in for a co-resident
    process editing the row with plain SQLite (the trigger is per-connection,
    keyed on a temp marker only ``set_project_policy`` creates).
    """
    conn = sqlite3.connect(str(db_file))
    try:
        conn.executescript("DROP TRIGGER IF EXISTS project_policy_append_only;")
        conn.execute(
            "UPDATE project_policy "
            "SET strict_promotion_required = 1 - strict_promotion_required "
            "WHERE id = 1"
        )
        conn.commit()
    finally:
        conn.close()


def test_health_reports_the_unverified_policy_stall(tmp_path: Path) -> None:
    db_file = _set_signed_policy_with_a_claim(tmp_path)

    conn = open_db(tmp_path)
    try:
        assert compute_health(conn).policy_unverified is False
    finally:
        conn.close()

    _tamper_policy_flat_column(db_file)

    conn = open_db(tmp_path)
    try:
        report = compute_health(conn)
    finally:
        conn.close()
    assert report.policy_unverified is True
    assert report.traffic_light == "yellow"
    assert "policy" in report.rationale.lower()


def test_status_prints_the_unverified_policy_stall(
    tmp_path: Path, monkeypatch,
) -> None:
    db_file = _set_signed_policy_with_a_claim(tmp_path)
    _tamper_policy_flat_column(db_file)

    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(cli, ["status"], catch_exceptions=False)
    assert res.exit_code == 0, res.output
    assert "policy unverified" in res.output.lower()

    res_json = CliRunner().invoke(
        cli, ["status", "--json"], catch_exceptions=False,
    )
    doc = json.loads(res_json.output)
    assert doc["policy_unverified"] is True


def test_untampered_policy_is_not_flagged(tmp_path: Path) -> None:
    """A project whose policy verifies is not reported as stalled."""
    _set_signed_policy_with_a_claim(tmp_path)
    conn = open_db(tmp_path)
    try:
        report = compute_health(conn)
    finally:
        conn.close()
    assert report.policy_unverified is False
    assert report.traffic_light != "yellow" or "policy" not in report.rationale.lower()


def test_policy_stall_does_not_mask_a_forged_promotion() -> None:
    """The policy overlay is added to the claim-census reason, not substituted.

    A project with both a tampered policy and a promoted claim that no longer
    re-verifies must surface both on `status`; the forged-promotion warning was
    the pre-existing signal and cannot be hidden by the new one.
    """
    report = HealthReport(
        claims_open=1,
        support_level_breakdown={"REPLICATED": 1},
        standing_promoted=1,
        failed_verification=1,
        policy_unverified=True,
    )
    light, rationale = _compute_traffic_light(report)
    assert light == "yellow"
    assert "do not re-verify" in rationale
    assert "policy" in rationale.lower()
