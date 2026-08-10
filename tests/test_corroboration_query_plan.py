"""The corroboration peer probe must reach its anchor by primary key.

The probe answers a single-row question: does this row's ESTABLISHED anchor
carry a qualifying peer. It runs once per row served, so its plan decides
whether a bulk read is linear or quadratic in the graph.

Reaching the anchor through ``j.value`` rather than as a parameter hides from
SQLite that ``a`` is one known row. The planner then drives the query off
``idx_claims_support_level`` and walks every ESTABLISHED claim in the graph,
once per row served. That shipped once: a 200-claim project went from 26 ms to
221 ms on ``query()``, and an 800-claim one from 8.5 ms to 35 seconds on
``claim list``, while the signature-count guard stayed green because the number
of verifications was never what changed.

Timing cannot catch this on a small fixture and a signature counter cannot
catch it at all, so the plan is pinned directly.
"""
from __future__ import annotations

from pathlib import Path

from mareforma.db import open_db
from mareforma.db.core import (
    _QUALIFYING_PEER_SQL,
    _QUALIFYING_PEER_STRICT_SUFFIX,
)


def _plan(conn, sql: str) -> list[str]:
    """Plan *sql*, binding one placeholder per ``?`` the query actually holds.

    The count is read off the query rather than hard-coded, so a change to the
    filters fails on the plan assertion below, which is the thing under test,
    instead of on a binding-count error that says nothing about the plan.
    """
    rows = conn.execute(
        "EXPLAIN QUERY PLAN " + sql, ["anchor-id"] * sql.count("?")
    ).fetchall()
    return [r["detail"] for r in rows]


def _anchor_step(details: list[str]) -> str:
    """The plan line for the anchor join, which the query aliases ``a``."""
    for detail in details:
        if " a " in f" {detail} " and ("SEARCH" in detail or "SCAN" in detail):
            return detail
    raise AssertionError(f"no plan step for the anchor alias: {details}")


def test_anchor_is_reached_by_primary_key(tmp_path: Path) -> None:
    """A primary-key seek, not a support-level sweep."""
    conn = open_db(tmp_path)
    try:
        step = _anchor_step(_plan(conn, _QUALIFYING_PEER_SQL))
        assert "claim_id=?" in step, (
            f"the anchor is not pinned by claim_id, so the planner is free to "
            f"drive off another index and walk every candidate row: {step}"
        )
    finally:
        conn.close()


def test_anchor_is_not_reached_through_the_support_level_index(
    tmp_path: Path,
) -> None:
    """Naming ``idx_claims_support_level`` for the anchor is the regression.

    That index selects every ESTABLISHED claim, which is the set this probe
    must not walk.
    """
    conn = open_db(tmp_path)
    try:
        for sql in (
            _QUALIFYING_PEER_SQL,
            _QUALIFYING_PEER_SQL + _QUALIFYING_PEER_STRICT_SUFFIX,
        ):
            step = _anchor_step(_plan(conn, sql))
            assert "idx_claims_support_level" not in step, (
                f"the anchor is reached through the support-level index, so "
                f"every ESTABLISHED claim is walked once per row served: {step}"
            )
    finally:
        conn.close()


def test_strict_mode_keeps_the_same_anchor_plan(tmp_path: Path) -> None:
    """The strict-promotion suffix must not cost the seek."""
    conn = open_db(tmp_path)
    try:
        plain = _anchor_step(_plan(conn, _QUALIFYING_PEER_SQL))
        strict = _anchor_step(
            _plan(conn, _QUALIFYING_PEER_SQL + _QUALIFYING_PEER_STRICT_SUFFIX)
        )
        assert plain == strict, (
            f"strict mode changes how the anchor is reached: {plain!r} vs {strict!r}"
        )
    finally:
        conn.close()
