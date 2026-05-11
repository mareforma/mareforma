"""health.py — Epistemic health report for mareforma status.

Traffic light (claim-based)
---------------------------
  green  : at least one REPLICATED or ESTABLISHED claim
  yellow : claims exist but all are PRELIMINARY
  red    : no claims at all
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

    Never raises — returns a partial report on any component failure.
    """
    report = HealthReport()

    try:
        from mareforma.db import list_claims

        claims = list_claims(conn)
        for c in claims:
            if c["status"] == "open":
                report.claims_open += 1
            else:
                report.claims_resolved += 1

            contradicts = json.loads(c.get("contradicts_json", "[]") or "[]")
            if contradicts:
                report.claims_contradicted += 1

            level = c.get("support_level", "PRELIMINARY")
            report.support_level_breakdown[level] = (
                report.support_level_breakdown.get(level, 0) + 1
            )
    except Exception:  # noqa: BLE001
        pass

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
