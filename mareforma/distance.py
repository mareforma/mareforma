"""
distance.py — Epistemic distance computation for mareforma.

Epistemic distance measures how far a transform's output is from the raw data
it was derived from. It accumulates CLASS_WEIGHTS along the depends_on DAG
(stored in transform_deps) from each root to the named transform.

Class weights
-------------
  raw       : 0.0  — direct measurement
  processed : 0.1  — faithful transformation
  analysed  : 0.5  — interpretation begins
  inferred  : 1.0  — model output / prediction
  unknown   : 0.5  — conservative estimate

A linear chain raw → processed → analysed → inferred has total distance 1.6.

Public API
----------
  compute(name, conn)     → float   (single transform — used by trace)
  compute_all(conn)       → dict    (all transforms — used by health)

Implementation: single BFS pass (_bfs_all) loads all edges once, seeds roots
with their own weight, and propagates pessimistically (max distance) through
the DAG. Cycles raise DatabaseError.

ASCII diagram:

  morphology.raw        (class=raw,       weight=0.0)  distance=0.0
       │
  morphology.filter     (class=processed, weight=0.1)  distance=0.1
       │
  morphology.features   (class=analysed,  weight=0.5)  distance=0.6
       │
  morphology.classify   (class=inferred,  weight=1.0)  distance=1.6
"""

from __future__ import annotations

import sqlite3
from collections import deque

from mareforma.db import DatabaseError


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CLASS_WEIGHTS: dict[str, float] = {
    "raw":       0.0,
    "processed": 0.1,
    "analysed":  0.5,
    "inferred":  1.0,
    "unknown":   0.5,  # conservative — same as analysed
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute(transform_name: str, conn: sqlite3.Connection) -> float:
    """Compute epistemic distance for a single transform.

    Walks the depends_on DAG via BFS and sums CLASS_WEIGHTS for each ancestor.
    The named transform's own class is included in the sum.

    Returns 0.0 if the transform has never run or has no class recorded.

    Raises
    ------
    DatabaseError
        If a cycle is detected in transform_deps.
    """
    all_distances = _bfs_all(conn)
    return all_distances.get(transform_name, 0.0)


def compute_all(conn: sqlite3.Connection) -> dict[str, float]:
    """Compute epistemic distance for all transforms in one BFS pass.

    Returns a dict mapping transform_name → distance.
    Used by compute_health() to avoid O(N) separate BFS calls.

    Raises
    ------
    DatabaseError
        If a cycle is detected in transform_deps.
    """
    return _bfs_all(conn)


# ---------------------------------------------------------------------------
# BFS implementation
# ---------------------------------------------------------------------------

def _get_latest_class(conn: sqlite3.Connection, transform_name: str) -> str:
    """Return the most recent non-unknown class for *transform_name*, or 'unknown'."""
    try:
        row = conn.execute(
            """
            SELECT transform_class FROM transform_runs
            WHERE transform_name = ?
              AND status = 'success'
              AND transform_class IS NOT NULL
              AND transform_class != 'unknown'
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (transform_name,),
        ).fetchone()
        return row["transform_class"] if row else "unknown"
    except sqlite3.OperationalError:
        return "unknown"


def _get_parents(conn: sqlite3.Connection, transform_name: str) -> list[str]:
    """Return direct parents of *transform_name* from transform_deps."""
    try:
        rows = conn.execute(
            "SELECT depends_on_name FROM transform_deps WHERE transform_name = ?",
            (transform_name,),
        ).fetchall()
        return [row["depends_on_name"] for row in rows]
    except sqlite3.OperationalError:
        return []


def _bfs_all(conn: sqlite3.Connection) -> dict[str, float]:
    """Single BFS pass computing distances for all transforms.

    Algorithm:
      1. Find all root transforms (appear as children in transform_deps
         but never as parents, OR have no rows as children at all).
      2. BFS from roots, accumulating distance as parent_distance + CLASS_WEIGHTS[class].
      3. If a transform is visited twice via different paths, take the max distance
         (pessimistic — trust the longer chain).
      4. Cycle detection: if BFS queue grows beyond expected size, raise DatabaseError.

    Returns dict[transform_name → distance].
    """
    # Load all edges once
    try:
        all_edges = conn.execute(
            "SELECT transform_name, depends_on_name FROM transform_deps"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}

    if not all_edges:
        # No deps recorded yet — all transforms are roots with distance 0
        try:
            rows = conn.execute(
                "SELECT DISTINCT transform_name FROM transform_runs WHERE status='success'"
            ).fetchall()
            names = [r["transform_name"] for r in rows]
        except sqlite3.OperationalError:
            return {}
        return {
            name: CLASS_WEIGHTS.get(_get_latest_class(conn, name), 0.5)
            for name in names
        }

    # Build adjacency: parent → list of children
    parents_of: dict[str, list[str]] = {}   # child → list of parents
    all_nodes: set[str] = set()
    for row in all_edges:
        child, parent = row["transform_name"], row["depends_on_name"]
        parents_of.setdefault(child, []).append(parent)
        all_nodes.add(child)
        all_nodes.add(parent)

    # Root nodes: appear in all_nodes but have no parents in this DAG
    roots = [n for n in all_nodes if n not in parents_of]

    distances: dict[str, float] = {}
    queue: deque[str] = deque(roots)
    visited_count: dict[str, int] = {}
    max_visits = len(all_nodes) + 1  # cycle detection threshold

    # Seed roots with their own class weight
    for root in roots:
        cls = _get_latest_class(conn, root)
        distances[root] = CLASS_WEIGHTS.get(cls, 0.5)

    while queue:
        node = queue.popleft()
        visited_count[node] = visited_count.get(node, 0) + 1
        if visited_count[node] > max_visits:
            raise DatabaseError(
                f"Cycle detected in transform DAG at: '{node}'. "
                "Check depends_on declarations."
            )

        # Find children of this node
        children = [
            child for child, parents in parents_of.items()
            if node in parents
        ]

        parent_dist = distances.get(node, 0.0)
        for child in children:
            cls = _get_latest_class(conn, child)
            child_dist = parent_dist + CLASS_WEIGHTS.get(cls, 0.5)
            # Pessimistic: take max if child reachable via multiple paths
            if child not in distances or child_dist > distances[child]:
                distances[child] = child_dist
                queue.append(child)

    # Include any transforms with no deps at all (not in transform_deps)
    try:
        all_run_names = {
            r["transform_name"]
            for r in conn.execute(
                "SELECT DISTINCT transform_name FROM transform_runs WHERE status='success'"
            ).fetchall()
        }
    except sqlite3.OperationalError:
        all_run_names = set()

    for name in all_run_names:
        if name not in distances:
            cls = _get_latest_class(conn, name)
            distances[name] = CLASS_WEIGHTS.get(cls, 0.5)

    return distances
