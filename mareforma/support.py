"""
support.py — Epistemic support level computation for mareforma.

Support level measures how much independent evidence agrees with a transform's
output. Levels accumulate as the project grows — adding sources and literature
automatically upgrades the support level without any annotation.

Support levels (precedence: ESTABLISHED > CONVERGED/CONSISTENT > REPLICATED > SINGLE)
---------------------------------------------------------------------------
  SINGLE      : one registered source feeds all ancestors
  REPLICATED  : same transform + same source, ≥2 successful runs, stable output_hash
  CONVERGED   : same transform function name applied to ≥2 distinct source prefixes
                e.g. morphology.features + patchseq.features → CONVERGED
  CONSISTENT  : any claim on this transform has non-empty supports_json (literature linked)
  ESTABLISHED : CONVERGED + CONSISTENT

Convention-based convergence detection
---------------------------------------
CONVERGED is detected when the same "step name" (the part after the first dot in
the transform name) appears with ≥2 different source prefixes in transform_runs.
This requires no content comparison — researchers control convergence by naming
their transforms consistently (morphology.features, patchseq.features).

Literature-backed detection
----------------------------
CONSISTENT is detected when any claim linked to this transform (via the evidence
table) has a non-empty supports_json column — i.e. the researcher passed at least
one DOI via ctx.claim(..., supports=["10.1038/..."]) or claim add --supports.
No metadata fetch is required; the DOI string alone is sufficient.

Public API
----------
  compute(transform_name, conn, root)    → str   (single — used by trace)
  compute_all(conn, root)                → dict  (all — used by health)
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPPORT_LEVELS = (
    "SINGLE",
    "REPLICATED",
    "CONVERGED",
    "CONSISTENT",
    "ESTABLISHED",
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute(
    transform_name: str,
    conn: sqlite3.Connection,
    root: Path,
) -> str:
    """Compute support level for *transform_name*.

    Returns one of: SINGLE, REPLICATED, CONVERGED, CONSISTENT, ESTABLISHED.
    Falls back to SINGLE on any error.
    """
    try:
        return _compute_one(transform_name, conn, root)
    except Exception:  # noqa: BLE001
        return "SINGLE"


def compute_all(
    conn: sqlite3.Connection,
    root: Path,
) -> dict[str, str]:
    """Compute support levels for all transforms with successful runs.

    Returns dict mapping transform_name → support level.
    Used by compute_health() for a single-pass batch computation.
    """
    try:
        return _compute_all(conn, root)
    except Exception:  # noqa: BLE001
        return {}


# ---------------------------------------------------------------------------
# Implementation
# ---------------------------------------------------------------------------

def _get_step_name(transform_name: str) -> str:
    """Extract the step suffix: 'morphology.features' → 'features'."""
    parts = transform_name.split(".", 1)
    return parts[1] if len(parts) == 2 else transform_name


def _get_source_prefix(transform_name: str) -> str:
    """Extract the source prefix: 'morphology.features' → 'morphology'."""
    return transform_name.split(".", 1)[0]


def _all_ancestor_names(transform_name: str, conn: sqlite3.Connection) -> set[str]:
    """BFS over transform_deps to collect all ancestors (including self)."""
    visited: set[str] = {transform_name}
    queue = [transform_name]
    try:
        while queue:
            current = queue.pop()
            rows = conn.execute(
                "SELECT depends_on_name FROM transform_deps WHERE transform_name = ?",
                (current,),
            ).fetchall()
            for row in rows:
                parent = row["depends_on_name"]
                if parent not in visited:
                    visited.add(parent)
                    queue.append(parent)
    except sqlite3.OperationalError:
        pass
    return visited


def _compute_one(
    transform_name: str,
    conn: sqlite3.Connection,
    root: Path,
) -> str:
    # Gather all successful runs for this transform
    try:
        runs = conn.execute(
            """
            SELECT run_id, input_hash, output_hash
            FROM transform_runs
            WHERE transform_name = ? AND status = 'success'
            ORDER BY timestamp DESC
            """,
            (transform_name,),
        ).fetchall()
    except sqlite3.OperationalError:
        return "SINGLE"

    if not runs:
        return "SINGLE"

    step_name = _get_step_name(transform_name)
    source_prefix = _get_source_prefix(transform_name)

    consistent = _is_consistent(transform_name, conn)
    converged = _is_converged(step_name, source_prefix, conn)

    if converged and consistent:
        return "ESTABLISHED"
    if converged:
        return "CONVERGED"
    if consistent:
        return "CONSISTENT"

    # REPLICATED: ≥2 successful runs with stable output_hash
    if len(runs) >= 2:
        hashes = [r["output_hash"] for r in runs if r["output_hash"]]
        if hashes and len(set(hashes)) == 1:
            return "REPLICATED"

    return "SINGLE"


def _is_consistent(transform_name: str, conn: sqlite3.Connection) -> bool:
    """True if any claim linked to this transform has non-empty supports_json.

    Checks claims linked via the evidence table to any run of this transform,
    plus claims whose source_name matches the transform's source prefix.
    No paper metadata fetch required — DOI strings in supports_json suffice.
    """
    try:
        rows = conn.execute(
            """
            SELECT c.supports_json
            FROM claims c
            JOIN evidence e ON e.claim_id = c.claim_id
            JOIN transform_runs tr ON tr.run_id = e.run_id
            WHERE tr.transform_name = ?
            """,
            (transform_name,),
        ).fetchall()
        for row in rows:
            supports = row["supports_json"] or "[]"
            try:
                if json.loads(supports):
                    return True
            except (json.JSONDecodeError, TypeError):
                pass
        # Also check claims by source_name (for claims made outside transforms)
        source_prefix = _get_source_prefix(transform_name)
        rows2 = conn.execute(
            "SELECT supports_json FROM claims WHERE source_name = ?",
            (source_prefix,),
        ).fetchall()
        for row in rows2:
            supports = row["supports_json"] or "[]"
            try:
                if json.loads(supports):
                    return True
            except (json.JSONDecodeError, TypeError):
                pass
        return False
    except sqlite3.OperationalError:
        return False


def _is_converged(step_name: str, own_prefix: str, conn: sqlite3.Connection) -> bool:
    """True if step_name appears with ≥2 distinct source prefixes in transform_runs."""
    if not step_name or "." not in step_name and step_name == own_prefix:
        # No step suffix (transform_name has no dot) — can't detect convergence
        return False
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT transform_name FROM transform_runs
            WHERE status = 'success'
              AND transform_name LIKE ?
            """,
            (f"%.{step_name}",),
        ).fetchall()
        all_names = {r["transform_name"] for r in rows}
        prefixes = {_get_source_prefix(n) for n in all_names}
        return len(prefixes) >= 2  # noqa: PLR2004
    except sqlite3.OperationalError:
        return False


def _compute_all(
    conn: sqlite3.Connection,
    root: Path,
) -> dict[str, str]:
    """Batch support computation for all transforms with successful runs."""
    try:
        all_names = [
            row["transform_name"]
            for row in conn.execute(
                "SELECT DISTINCT transform_name FROM transform_runs WHERE status='success'"
            ).fetchall()
        ]
    except sqlite3.OperationalError:
        return {}

    # Pre-compute convergence map: step_name → set of source prefixes
    convergence_map: dict[str, set[str]] = {}
    for name in all_names:
        step = _get_step_name(name)
        prefix = _get_source_prefix(name)
        if step != name:  # has a dot
            convergence_map.setdefault(step, set()).add(prefix)

    converged_steps = {s for s, prefixes in convergence_map.items() if len(prefixes) >= 2}  # noqa: PLR2004

    result: dict[str, str] = {}
    for name in all_names:
        try:
            step = _get_step_name(name)
            converged = step in converged_steps and step != name
            consistent = _is_consistent(name, conn)

            if converged and consistent:
                result[name] = "ESTABLISHED"
            elif converged:
                result[name] = "CONVERGED"
            elif consistent:
                result[name] = "CONSISTENT"
            else:
                runs = conn.execute(
                    """
                    SELECT output_hash FROM transform_runs
                    WHERE transform_name = ? AND status = 'success'
                    ORDER BY timestamp DESC
                    """,
                    (name,),
                ).fetchall()
                hashes = [r["output_hash"] for r in runs if r["output_hash"]]
                if len(hashes) >= 2 and len(set(hashes)) == 1:  # noqa: PLR2004
                    result[name] = "REPLICATED"
                else:
                    result[name] = "SINGLE"
        except Exception:  # noqa: BLE001
            result[name] = "SINGLE"

    return result
