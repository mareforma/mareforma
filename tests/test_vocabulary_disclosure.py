"""Disclosure of the retired support ladder at the read surfaces.

The support ladder (PRELIMINARY / REPLICATED / ESTABLISHED) is retired and
removed in v0.4.0. Because the column is NOT NULL DEFAULT 'PRELIMINARY', a
project that never named a level still stores one, so no call site warns on the
commonest path. ``health()`` and ``mareforma status`` disclose the retirement
where the value is served, and the ``min_support`` rejection names it too.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

import mareforma
from mareforma.cli import cli
from mareforma.db import open_db, query_claims, search_claims
from mareforma.health import compute_health


def test_health_flags_the_retired_ladder_when_claims_exist(tmp_path: Path) -> None:
    with mareforma.open(tmp_path) as g:
        g.assert_claim("a finding that never named a support level")

    conn = open_db(tmp_path)
    try:
        report = compute_health(conn)
    finally:
        conn.close()
    # The project stores 'PRELIMINARY' by default, so the disclosure applies
    # even though no level was ever typed.
    assert report.support_level_breakdown.get("PRELIMINARY", 0) == 1
    assert report.support_level_retired is True


def test_health_does_not_flag_an_empty_project(tmp_path: Path) -> None:
    with mareforma.open(tmp_path):
        pass
    conn = open_db(tmp_path)
    try:
        report = compute_health(conn)
    finally:
        conn.close()
    assert report.support_level_breakdown == {}
    assert report.support_level_retired is False


def test_status_discloses_the_retired_ladder(tmp_path: Path, monkeypatch) -> None:
    with mareforma.open(tmp_path) as g:
        g.assert_claim("a finding that never named a support level")

    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(cli, ["status"], catch_exceptions=False)
    assert res.exit_code == 0, res.output
    assert "retired axis" in res.output
    assert "v0.4.0" in res.output

    res_json = CliRunner().invoke(cli, ["status", "--json"], catch_exceptions=False)
    doc = json.loads(res_json.output)
    assert doc["support_level_retired"] is True


def test_unknown_min_support_error_names_the_retirement(tmp_path: Path) -> None:
    with mareforma.open(tmp_path):
        pass
    conn = open_db(tmp_path)
    try:
        for call in (query_claims, search_claims):
            with pytest.raises(ValueError) as exc:
                if call is search_claims:
                    call(conn, "finding", min_support="LEGENDARY")
                else:
                    call(conn, min_support="LEGENDARY")
            message = str(exc.value)
            # Still lists the accepted values (they filter this release) ...
            assert "PRELIMINARY" in message and "ESTABLISHED" in message
            # ... and names the retirement and the axis to read instead.
            assert "deprecated" in message and "v0.4.0" in message
            assert "status" in message
    finally:
        conn.close()
