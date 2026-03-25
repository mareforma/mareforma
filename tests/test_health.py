"""
tests/test_health.py — unit tests for mareforma/health.py.

Covers:
  - compute_health: red (no claims), yellow (unclaimed/no-lit/unsupported-source), green
  - compute_health: confidence_breakdown and contradicted counts
  - compute_health: never raises on empty project
  - compute_health: epistemic fields (classes, distances, support) populated
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from mareforma.db import (
    add_claim,
    begin_run,
    end_run,
    open_db,
    record_deps,
    write_transform_class,
)
from mareforma.health import HealthReport, compute_health
from mareforma.initializer import initialize
from mareforma.registry import add_source


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def project(tmp_path: Path) -> Path:
    initialize(tmp_path)
    return tmp_path


@pytest.fixture()
def conn(project: Path) -> sqlite3.Connection:
    c = open_db(project)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Traffic light: red
# ---------------------------------------------------------------------------

class TestRed:
    def test_no_claims_is_red(self, project: Path, conn: sqlite3.Connection) -> None:
        report = compute_health(project, conn)
        assert report.traffic_light == "red"
        assert "claim" in report.rationale.lower()
        assert report.claims_open == 0
        assert report.claims_resolved == 0


# ---------------------------------------------------------------------------
# Traffic light: yellow
# ---------------------------------------------------------------------------

class TestYellow:
    def test_yellow_when_unclaimed_transforms(
        self, project: Path, conn: sqlite3.Connection
    ) -> None:
        # Record a transform run but add NO claims
        import uuid
        run_id = str(uuid.uuid4())
        begin_run(conn, run_id, "mydata.load", "h1", "h2")
        end_run(conn, run_id, status="success", duration_ms=100)
        # Add a claim linked to a different transform to avoid red
        add_claim(conn, project, "Some finding", source_name=None)

        report = compute_health(project, conn)
        assert report.traffic_light == "yellow"
        assert "mydata.load" in report.unclaimed_transforms

    def test_yellow_when_source_has_no_claims(
        self, project: Path, conn: sqlite3.Connection
    ) -> None:
        raw = project / "data" / "morphology" / "raw"
        raw.mkdir(parents=True)
        add_source(project, "morphology", str(raw), "Test")

        # Add a claim not tied to any source
        add_claim(conn, project, "Unrelated finding", source_name=None)

        report = compute_health(project, conn)
        assert report.traffic_light == "yellow"
        assert "morphology" in report.unsupported_sources

    def test_yellow_when_source_has_no_claims(
        self, project: Path, conn: sqlite3.Connection
    ) -> None:
        # Register a source but don't claim it → yellow (unsupported_sources non-empty)
        raw = project / "data" / "morphology" / "raw"
        raw.mkdir(parents=True)
        add_source(project, "morphology", str(raw), "Test")
        add_claim(conn, project, "A finding", source_name=None)
        report = compute_health(project, conn)
        assert report.traffic_light == "yellow"
        assert "morphology" in report.unsupported_sources


# ---------------------------------------------------------------------------
# Traffic light: green
# ---------------------------------------------------------------------------

class TestGreen:
    def test_green_all_covered(self, project: Path, conn: sqlite3.Connection) -> None:
        # Register source and add a claim linked to it
        raw = project / "data" / "morphology" / "raw"
        raw.mkdir(parents=True)
        add_source(project, "morphology", str(raw), "Test")
        add_claim(conn, project, "A finding", source_name="morphology")

        report = compute_health(project, conn)
        assert report.traffic_light == "green"


# ---------------------------------------------------------------------------
# Counts and breakdowns
# ---------------------------------------------------------------------------

class TestCounts:
    def test_confidence_breakdown(self, project: Path, conn: sqlite3.Connection) -> None:
        add_claim(conn, project, "Claim 1", confidence="exploratory")
        add_claim(conn, project, "Claim 2", confidence="exploratory")
        add_claim(conn, project, "Claim 3", confidence="preliminary")

        report = compute_health(project, conn)
        assert report.confidence_breakdown["exploratory"] == 2
        assert report.confidence_breakdown["preliminary"] == 1

    def test_claims_open_vs_resolved(self, project: Path, conn: sqlite3.Connection) -> None:
        add_claim(conn, project, "Open claim", status="open")
        add_claim(conn, project, "Resolved claim", status="supported")

        report = compute_health(project, conn)
        assert report.claims_open == 1
        assert report.claims_resolved == 1

    def test_claims_contradicted_count(self, project: Path, conn: sqlite3.Connection) -> None:
        add_claim(conn, project, "Contradicted claim", contradicts=["10.1038/some"])
        add_claim(conn, project, "Normal claim")

        report = compute_health(project, conn)
        assert report.claims_contradicted == 1


# ---------------------------------------------------------------------------
# Never raises
# ---------------------------------------------------------------------------

class TestNeverRaises:
    def test_empty_project_no_error(self, tmp_path: Path) -> None:
        """compute_health must not raise even on a minimal/empty project."""
        initialize(tmp_path)
        conn = open_db(tmp_path)
        try:
            report = compute_health(tmp_path, conn)
            assert isinstance(report, HealthReport)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Epistemic distance layer fields (v0.3)
# ---------------------------------------------------------------------------

class TestEpistemicDistanceFields:
    def _make_run(
        self,
        conn: sqlite3.Connection,
        name: str,
        deps: list[str] | None = None,
        cls: str | None = None,
        output_hash: str | None = None,
    ) -> str:
        import uuid
        run_id = str(uuid.uuid4())
        begin_run(conn, run_id, name, "ih", "sh")
        record_deps(conn, name, deps or [])
        end_run(conn, run_id, status="success", output_hash=output_hash or f"h_{name}")
        if cls:
            write_transform_class(
                conn, run_id,
                transform_class=cls,
                class_confidence=0.9,
                class_method="heuristic",
                class_reason="test",
            )
        return run_id

    def test_epistemic_fields_populated(self, project: Path, conn: sqlite3.Connection) -> None:
        """All three dicts populated from a single-step linear pipeline."""
        self._make_run(conn, "src.load", cls="raw")
        self._make_run(conn, "src.filter", deps=["src.load"], cls="processed")

        report = compute_health(project, conn)
        # transform_classes
        assert report.transform_classes["src.load"] == "raw"
        assert report.transform_classes["src.filter"] == "processed"
        # transform_distances
        assert report.transform_distances["src.load"] == pytest.approx(0.0)
        assert report.transform_distances["src.filter"] == pytest.approx(0.1)
        # transform_support (single runs → SINGLE)
        assert report.transform_support["src.load"] == "SINGLE"
        assert report.transform_support["src.filter"] == "SINGLE"

    def test_epistemic_fields_empty_on_empty_db(self, project: Path, conn: sqlite3.Connection) -> None:
        """All three dicts are empty when no transforms have been run."""
        report = compute_health(project, conn)
        assert report.transform_classes == {}
        assert report.transform_distances == {}
        assert report.transform_support == {}
