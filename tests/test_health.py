"""tests/test_health.py — unit tests for mareforma/health.py."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from mareforma.db import add_claim, open_db
from mareforma.health import HealthReport, compute_health


def _open(tmp_path: Path) -> sqlite3.Connection:
    (tmp_path / ".mareforma").mkdir(parents=True, exist_ok=True)
    return open_db(tmp_path)


# ---------------------------------------------------------------------------
# Traffic light state derivation
# ---------------------------------------------------------------------------


class TestTrafficLight:
    def test_red_when_no_claims(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.traffic_light == "red"
        assert "claim" in report.rationale.lower()

    def test_yellow_when_all_preliminary(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Single agent finding")
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.traffic_light == "yellow"
        assert "PRELIMINARY" in report.rationale

    def test_green_when_replicated_claim_exists(self, tmp_path: Path) -> None:
        # Seed the upstream via the graph API (REPLICATED requires an
        # ESTABLISHED upstream), then drop down to the db API for the
        # rest of the test.
        from mareforma import signing as _sig
        import mareforma
        key = tmp_path / "k"
        _sig.bootstrap_key(key)
        with mareforma.open(tmp_path, key_path=key) as g:
            prior = g.assert_claim("prior", generated_by="seed", seed=True)

        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "finding A", supports=[prior], generated_by="agent_A")
            add_claim(conn, tmp_path, "finding B", supports=[prior], generated_by="agent_B")
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.traffic_light == "green"


# ---------------------------------------------------------------------------
# Per-status + per-support-level counters
# ---------------------------------------------------------------------------


class TestCounts:
    def test_claims_open_vs_resolved(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Open claim", status="open")
            add_claim(conn, tmp_path, "Resolved claim", status="contested")
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.claims_open == 1
        assert report.claims_resolved == 1

    def test_claims_contradicted_count(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Contested finding", contradicts=["10.1038/some"])
            add_claim(conn, tmp_path, "Normal finding")
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.claims_contradicted == 1

    def test_support_level_breakdown(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Claim 1")
            add_claim(conn, tmp_path, "Claim 2")
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.support_level_breakdown.get("PRELIMINARY", 0) == 2


# ---------------------------------------------------------------------------
# Never-raises contract
# ---------------------------------------------------------------------------


class TestNeverRaises:
    def test_empty_project_no_error(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            report = compute_health(tmp_path, conn)
            assert isinstance(report, HealthReport)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Corruption-vs-empty differentiation
# ---------------------------------------------------------------------------


class TestCorruptionVsEmpty:
    """A corrupted graph.db that cannot be read must surface as
    traffic_light='error' so an operator running ``mareforma health``
    sees a different signal than for a fresh, empty project. Before
    this distinction existed, a SELECT failure was silently swallowed
    and the resulting empty counters were folded into the standard
    ``red`` empty-graph branch — operationally indistinguishable.
    """

    def test_closed_connection_surfaces_as_error(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        conn.close()
        # SELECT against a closed connection raises ProgrammingError;
        # compute_health must catch and surface ``error``.
        report = compute_health(tmp_path, conn)
        assert report.traffic_light == "error"
        assert "Could not read" in report.rationale
        assert "not the same as an empty graph" in report.rationale

    def test_missing_claims_table_surfaces_as_error(self, tmp_path: Path) -> None:
        # Open a fresh DB, drop the claims table, then check that
        # compute_health surfaces the read failure as ``error``.
        conn = _open(tmp_path)
        try:
            conn.execute("DROP TABLE claims_fts")
            conn.execute("DROP TABLE claims")
            conn.commit()
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.traffic_light == "error"

    def test_empty_graph_still_red_not_error(self, tmp_path: Path) -> None:
        """Sanity: a legitimately-empty graph stays at ``red``, NOT
        ``error``. The distinction only fires for actual read failures.
        """
        conn = _open(tmp_path)
        try:
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.traffic_light == "red"
        assert "No claims recorded" in report.rationale
