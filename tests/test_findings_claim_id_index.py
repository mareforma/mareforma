"""findings must carry a claim_id index for the write and read paths.

Three shipped paths resolve ``findings.claim_id``: the trust-map effective-
independence lookup, the ``_claim_model_lineage`` join run per convergence peer,
and the ``run_first_execution`` pre-registration guard on every finding submit.
Without an index on ``findings(claim_id)`` each falls back to a full table scan
that grows with the graph. ``idx_find_claim`` closes that; this pins the plans.
"""

from __future__ import annotations

from pathlib import Path

from mareforma.db import open_db


def _plan(conn, sql: str) -> str:
    rows = conn.execute("EXPLAIN QUERY PLAN " + sql, ("dummy",)).fetchall()
    return " | ".join(r["detail"] for r in rows)


_TRUST_MAP_SQL = "SELECT content_id FROM findings WHERE claim_id = ? LIMIT 1"
_LINEAGE_SQL = (
    "SELECT el.model_lineage FROM findings f "
    "JOIN evidence_lines el ON el.finding_id = f.finding_id "
    "WHERE f.claim_id = ? AND el.model_lineage IS NOT NULL LIMIT 1"
)
_FIRST_EXEC_SQL = (
    "SELECT MIN(f.created_at) AS first_at FROM findings f "
    "JOIN claims c ON c.claim_id = f.claim_id "
    "WHERE c.generated_by = ?"
)


def test_trust_map_lookup_uses_claim_index(tmp_path: Path) -> None:
    conn = open_db(tmp_path)
    try:
        plan = _plan(conn, _TRUST_MAP_SQL)
        assert "idx_find_claim" in plan, plan
        assert "SCAN findings" not in plan, plan
    finally:
        conn.close()


def test_model_lineage_join_uses_claim_index(tmp_path: Path) -> None:
    conn = open_db(tmp_path)
    try:
        plan = _plan(conn, _LINEAGE_SQL)
        assert "idx_find_claim" in plan, plan
        # The findings side of the join must not be full-scanned.
        assert "SCAN f " not in plan and not plan.endswith("SCAN f"), plan
    finally:
        conn.close()


def test_first_execution_guard_does_not_scan_findings(tmp_path: Path) -> None:
    conn = open_db(tmp_path)
    try:
        plan = _plan(conn, _FIRST_EXEC_SQL)
        assert "idx_find_claim" in plan, plan
        assert "SCAN f " not in plan and not plan.endswith("SCAN f"), plan
    finally:
        conn.close()
