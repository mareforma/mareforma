"""
health.py — Epistemic health report for mareforma status.

Traffic light (claim-based layer)
----------------------------------
  green  : claims exist, all transforms have claims, all sources have claims
  yellow : any unclaimed transforms OR sources with no claims
  red    : no claims at all

Support levels (from support.py)
----------------------------------
  SINGLE      → REPLICATED → CONVERGED → CONSISTENT → ESTABLISHED
  CONSISTENT requires at least one DOI in supports_json (via claim --supports).
  No metadata fetch needed — the DOI string alone suffices.

Epistemic distance layer (v0.3+)
---------------------------------
  transform_classes  : dict[name → class string] from transform_runs
  transform_distances: dict[name → float] from distance.compute_all()
  transform_support  : dict[name → level string] from support.compute_all()

The report contains factual counts and dicts. Never raises: compute_health()
catches all partial failures and fills what it can.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class HealthReport:
    # Claim-based layer (v0.2.x)
    claims_open: int = 0
    claims_resolved: int = 0          # supported + contested + retracted
    claims_contradicted: int = 0      # claims with non-empty contradicts_json
    unclaimed_transforms: list[str] = field(default_factory=list)
    unsupported_sources: list[str] = field(default_factory=list)
    confidence_breakdown: dict[str, int] = field(default_factory=dict)
    traffic_light: str = "green"
    rationale: str = ""
    # Epistemic distance layer (v0.3)
    transform_classes: dict[str, str] = field(default_factory=dict)
    transform_distances: dict[str, float] = field(default_factory=dict)
    transform_support: dict[str, str] = field(default_factory=dict)


def compute_health(root: Path, conn: sqlite3.Connection) -> HealthReport:
    """Build a HealthReport from graph.db + mareforma.project.toml.

    Never raises — returns a partial report on any component failure.
    """
    report = HealthReport()

    # --- Claims ---
    try:
        from mareforma.db import list_claims, get_unclaimed_transforms
        import json

        claims = list_claims(conn)
        for c in claims:
            if c["status"] == "open":
                report.claims_open += 1
            else:
                report.claims_resolved += 1

            contradicts = json.loads(c.get("contradicts_json", "[]") or "[]")
            if contradicts:
                report.claims_contradicted += 1

            conf = c.get("confidence", "exploratory")
            report.confidence_breakdown[conf] = (
                report.confidence_breakdown.get(conf, 0) + 1
            )

        report.unclaimed_transforms = get_unclaimed_transforms(conn)
    except Exception:  # noqa: BLE001
        pass

    # --- Sources with no claims ---
    try:
        from mareforma.registry import load as load_toml

        toml_data = load_toml(root)
        sources = list(toml_data.get("sources", {}).keys())

        claimed_sources: set[str] = set()
        try:
            from mareforma.db import list_claims as _lc
            for c in _lc(conn):
                if c.get("source_name"):
                    claimed_sources.add(c["source_name"])
        except Exception:  # noqa: BLE001
            pass

        report.unsupported_sources = [
            s for s in sources if s not in claimed_sources
        ]
    except Exception:  # noqa: BLE001
        pass

    # --- Traffic light ---
    report.traffic_light, report.rationale = _compute_traffic_light(report)

    # --- Epistemic distance layer (v0.3) ---
    try:
        from mareforma.distance import compute_all as dist_all
        report.transform_distances = dist_all(conn)
    except Exception:  # noqa: BLE001
        pass

    try:
        from mareforma.support import compute_all as sup_all
        report.transform_support = sup_all(conn, root)
    except Exception:  # noqa: BLE001
        pass

    try:
        rows = conn.execute(
            """
            SELECT transform_name,
                   (SELECT transform_class FROM transform_runs tr2
                    WHERE tr2.transform_name = tr.transform_name
                      AND tr2.status = 'success'
                      AND tr2.transform_class IS NOT NULL
                    ORDER BY tr2.timestamp DESC LIMIT 1) AS cls
            FROM (SELECT DISTINCT transform_name FROM transform_runs WHERE status='success') tr
            """
        ).fetchall()
        report.transform_classes = {
            r["transform_name"]: (r["cls"] or "unknown") for r in rows
        }
    except Exception:  # noqa: BLE001
        pass

    return report


def _compute_traffic_light(report: HealthReport) -> tuple[str, str]:
    """Derive traffic light color and rationale from report counts."""
    total_claims = report.claims_open + report.claims_resolved

    if total_claims == 0:
        return "red", "No claims recorded. Run ctx.claim() or mareforma claim add."

    yellow_reasons: list[str] = []
    if report.unclaimed_transforms:
        n = len(report.unclaimed_transforms)
        yellow_reasons.append(
            f"{n} transform{'s' if n != 1 else ''} with no claims"
        )
    if report.unsupported_sources:
        n = len(report.unsupported_sources)
        yellow_reasons.append(
            f"{n} source{'s' if n != 1 else ''} with no claims"
        )

    if yellow_reasons:
        return "yellow", "; ".join(yellow_reasons).capitalize() + "."

    return "green", "All transforms claimed. Keep going."
