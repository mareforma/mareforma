"""#33: the cycle check is one recursive CTE, not a query per node.

``_check_no_cycle`` walked ``supports[]`` with a Python DFS that ran one
``SELECT`` per visited claim, so its cost scaled with the depth of the
ancestral chain. It now issues a single recursive-CTE reachability query.
This pins both the correctness and the query-count collapse.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

import mareforma
from mareforma.db import CycleDetectedError, _check_no_cycle


def _chain(graph, n: int) -> list[str]:
    """Assert a→b→c… chain where each claim supports the previous one."""
    ids = [graph.assert_claim("root claim 0")]
    for i in range(1, n):
        ids.append(graph.assert_claim(f"claim {i}", supports=[ids[-1]]))
    return ids


def test_cycle_check_is_single_query_regardless_of_depth(tmp_path: Path) -> None:
    with mareforma.open(tmp_path) as graph:
        ids = _chain(graph, 12)
        conn = graph._conn
        # Count SQL statements _check_no_cycle issues for a deep walk.
        statements: list[str] = []
        conn.set_trace_callback(statements.append)
        try:
            # A brand-new claim supporting the chain tip walks the whole
            # chain but must not be a cycle.
            _check_no_cycle(conn, str(uuid.uuid4()), [ids[-1]])
        finally:
            conn.set_trace_callback(None)
    # One recursive CTE — not one SELECT per visited claim.
    selects = [s for s in statements if "claims" in s.lower() or "recursive" in s.lower()]
    assert len(selects) == 1, f"expected a single walk query, ran: {statements}"


def test_cycle_detected_through_the_chain(tmp_path: Path) -> None:
    with mareforma.open(tmp_path) as graph:
        ids = _chain(graph, 4)
        conn = graph._conn
        # The chain root already reaches nothing; the tip reaches the root.
        # Adding root→tip would close root→…→tip→…→root.
        with pytest.raises(CycleDetectedError):
            _check_no_cycle(conn, ids[0], [ids[-1]])


def test_self_loop_still_rejected(tmp_path: Path) -> None:
    with mareforma.open(tmp_path) as graph:
        [a] = _chain(graph, 1)
        with pytest.raises(CycleDetectedError, match="itself"):
            _check_no_cycle(graph._conn, a, [a])


def test_non_cycle_supports_allowed(tmp_path: Path) -> None:
    with mareforma.open(tmp_path) as graph:
        ids = _chain(graph, 4)
        # A fresh claim id not reachable from the chain tip is fine.
        _check_no_cycle(graph._conn, str(uuid.uuid4()), [ids[-1]])
