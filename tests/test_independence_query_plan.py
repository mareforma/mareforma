"""independence_counts must not full-scan effect_estimates.

The read joins evidence_lines → contrasts (on line_id) → effect_estimates
(on contrast_id). Without an index on contrasts(line_id) the planner had no
keyed path into contrasts and fell back to scanning effect_estimates for
the whole join. idx_contrast_line closes that; this pins the plan.
"""

from __future__ import annotations

from pathlib import Path

from mareforma.db import open_db
from mareforma.trust._store import INDEPENDENCE_COUNTS_SQL


def _plan(conn) -> list[str]:
    rows = conn.execute(
        "EXPLAIN QUERY PLAN " + INDEPENDENCE_COUNTS_SQL, ("dummy-content-id",)
    ).fetchall()
    return [r["detail"] for r in rows]


def test_no_full_scan_of_effect_estimates(tmp_path: Path) -> None:
    conn = open_db(tmp_path)
    try:
        details = _plan(conn)
        # A full-table scan renders as "SCAN effect_estimates"; a keyed
        # lookup renders as "SEARCH effect_estimates USING INDEX ...".
        scans = [d for d in details if "SCAN" in d and "effect_estimates" in d]
        assert not scans, f"effect_estimates is full-scanned: {details}"
    finally:
        conn.close()


def test_contrasts_reached_by_line_id_index(tmp_path: Path) -> None:
    conn = open_db(tmp_path)
    try:
        details = " | ".join(_plan(conn))
        assert "idx_contrast_line" in details, (
            f"contrasts should be reached via idx_contrast_line: {details}"
        )
    finally:
        conn.close()


def test_count_anchors_on_findings_via_idx_find_content(tmp_path: Path) -> None:
    """The count enumerates from the signed findings on idx_find_content.

    Anchoring on ``findings`` and LEFT JOINing downward is what keeps a finding
    whose evidence rows were deleted visible to the per-finding digest check.
    Losing ``idx_find_content`` here was measured at up to 2858x on a large
    graph, so the plan is pinned: ``f`` must be searched through the content_id
    index, not scanned.
    """
    conn = open_db(tmp_path)
    try:
        details = _plan(conn)
        assert any(
            "SEARCH f USING INDEX idx_find_content" in d for d in details
        ), f"findings must be reached via idx_find_content: {details}"
        assert not any(
            "SCAN f" in d for d in details
        ), f"findings must not be full-scanned: {details}"
    finally:
        conn.close()
