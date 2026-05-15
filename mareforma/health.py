"""health.py — Epistemic health report for mareforma status.

Traffic light (claim-based)
---------------------------
  green  : at least one REPLICATED or ESTABLISHED claim
  yellow : claims exist but all are PRELIMINARY
  red    : no claims at all
  error  : graph.db could not be read (corruption, missing table, locked)

The ``error`` state is distinct from ``red``: a fresh project legitimately
has no claims yet (``red``), but a corrupted graph.db that cannot be
read at all is a different operational signal and gets its own traffic
light so operators looking at ``mareforma health`` can tell them apart.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class HealthReport:
    claims_open: int = 0
    claims_resolved: int = 0
    claims_contradicted: int = 0
    support_level_breakdown: dict[str, int] = field(default_factory=dict)
    traffic_light: str = "green"
    rationale: str = ""


def compute_health(root: Path, conn: sqlite3.Connection) -> HealthReport:
    """Build a HealthReport from graph.db.

    Never raises. On a SQLite read failure the report's traffic light
    is set to ``"error"`` so an operator looking at ``mareforma health``
    can distinguish a corrupted / unreadable graph from an empty one
    (which legitimately returns ``"red"`` for ``no claims recorded``).
    """
    report = HealthReport()

    try:
        from mareforma.db import list_claims, DatabaseError

        claims = list_claims(conn)
    except (sqlite3.OperationalError, sqlite3.DatabaseError, DatabaseError) as exc:
        # Read failure: surface as ``error`` rather than folding into
        # the empty-graph ``red`` state. Counters stay at zero so the
        # caller can tell the report is non-substantive.
        report.traffic_light = "error"
        report.rationale = (
            "Could not read claims table from graph.db "
            f"({type(exc).__name__}: {exc}). Run `graph.restore()` or "
            "investigate the .mareforma/ directory; this is not the "
            "same as an empty graph."
        )
        return report

    for c in claims:
        if c["status"] == "open":
            report.claims_open += 1
        else:
            report.claims_resolved += 1

        try:
            contradicts = json.loads(c.get("contradicts_json", "[]") or "[]")
        except (TypeError, ValueError):
            # Malformed JSON in a single row is per-row corruption,
            # not whole-DB corruption. Skip the contradicts accounting
            # for this row and continue producing a report for the rest.
            contradicts = []
        if contradicts:
            report.claims_contradicted += 1

        level = c.get("support_level", "PRELIMINARY")
        report.support_level_breakdown[level] = (
            report.support_level_breakdown.get(level, 0) + 1
        )

    report.traffic_light, report.rationale = _compute_traffic_light(report)
    return report


def _compute_traffic_light(report: HealthReport) -> tuple[str, str]:
    total = report.claims_open + report.claims_resolved
    if total == 0:
        return "red", "No claims recorded. Call graph.assert_claim() to start."

    established = report.support_level_breakdown.get("ESTABLISHED", 0)
    replicated = report.support_level_breakdown.get("REPLICATED", 0)
    if established + replicated == 0:
        return "yellow", "All claims are PRELIMINARY — no independent replication yet."

    return "green", "At least one independently replicated or validated claim."
