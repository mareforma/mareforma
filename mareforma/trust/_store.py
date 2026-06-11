"""_store.py: persistence for the trust layer over a sqlite3 connection.

Pure SQL helpers that the EpistemicGraph methods call. They never sign or
commit (the caller owns the transaction and the signed attestation); they only
read and write the structured proposition/evidence tables. Keeping the SQL here
keeps the graph object thin and keeps every trust query in one place.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from typing import Optional

from mareforma._canonical import canonicalize

from .bearing import Bearing
from .estimate import EvidenceLine
from .prediction import Prediction
from .proposition import Direction, Proposition
from .status import STATUS_POLICY, FrameStatus, Status, compute_status

# Per-status rank on the support ladder, for the min_status retrieval filter.
# REFUTED/CONTESTED are off the ladder (rank -1), so they are excluded by any
# floor, including UNTESTED.
_SUPPORT_RANK = {
    Status.UNTESTED.value: 0,
    Status.PRELIMINARY.value: 1,
    Status.CORROBORATED.value: 2,
    Status.REFUTED.value: -1,
    Status.CONTESTED.value: -1,
}

# The only valid min_status floors are the three support-ladder statuses.
_VALID_FLOORS = frozenset(
    {Status.UNTESTED.value, Status.PRELIMINARY.value, Status.CORROBORATED.value}
)


def _uuid() -> str:
    return str(uuid.uuid4())


# -- writes ------------------------------------------------------------------

def register_proposition(conn: sqlite3.Connection, prop: Proposition, now: str) -> str:
    """Insert the proposition row if absent; return its content_id.

    Idempotent and concurrency-safe via ON CONFLICT DO NOTHING on the
    content_id primary key.
    """
    cid = prop.content_id()
    conn.execute(
        "INSERT INTO propositions "
        "(content_id, frame_id, subject, relation, object, direction, "
        " scope_json, magnitude, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(content_id) DO NOTHING",
        (
            cid,
            prop.frame_id(),
            prop.subject,
            prop.relation,
            prop.object,
            prop.direction.value,
            json.dumps(dict(prop.scope), sort_keys=True, ensure_ascii=False),
            prop.magnitude,
            now,
        ),
    )
    return cid


def register_plan(
    conn: sqlite3.Connection, content_id: str, prediction: Prediction, now: str
) -> str:
    """Register a pre-registered plan bound to content_id; return its plan_id.

    The plan_id is content-addressed over (content_id + the prediction fields),
    so registering the same plan twice is a no-op (ON CONFLICT DO NOTHING). A
    retry after a partially-written finding cannot create a duplicate,
    un-deletable plan row.
    """
    p = prediction
    plan_id = hashlib.sha256(
        canonicalize({"content_id": content_id, **p.to_dict()})
    ).hexdigest()
    conn.execute(
        "INSERT INTO predictions "
        "(plan_id, content_id, inference_regime, test_type, direction_of_interest, "
        " equivalence_lower, equivalence_upper, alpha, preregistered, registered_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(plan_id) DO NOTHING",
        (
            plan_id,
            content_id,
            p.inference_regime.value,
            p.test_type.value,
            p.direction_of_interest.value if p.direction_of_interest else None,
            p.equivalence_lower,
            p.equivalence_upper,
            p.alpha,
            1 if p.preregistered else 0,
            now,
        ),
    )
    return plan_id


def insert_finding(
    conn: sqlite3.Connection,
    content_id: str,
    plan_id: str,
    claim_id: str,
    bearing: Bearing,
    line: EvidenceLine,
    now: str,
) -> str:
    """Write the finding plus its single-line evidence tree; return finding_id."""
    finding_id = _uuid()
    conn.execute(
        "INSERT INTO findings "
        "(finding_id, content_id, plan_id, claim_id, bearing_direction, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (finding_id, content_id, plan_id, claim_id, bearing.direction.value, now),
    )
    line_id = _uuid()
    conn.execute(
        "INSERT INTO evidence_lines "
        "(line_id, finding_id, modality, provenance_id, design_type, data_id, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            line_id,
            finding_id,
            line.modality,
            line.provenance_id,
            line.design_type,
            line.data_id,
            now,
        ),
    )
    contrast_id = _uuid()
    conn.execute(
        "INSERT INTO contrasts (contrast_id, line_id, control_type) VALUES (?, ?, ?)",
        (contrast_id, line_id, line.contrast.control_type.value),
    )
    est = line.estimate
    conn.execute(
        "INSERT INTO effect_estimates "
        "(estimate_id, contrast_id, estimate_value, effect_type, scale, p_value, "
        " ci_lower, ci_upper, ci_level, n_total) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            _uuid(),
            contrast_id,
            est.estimate_value,
            est.effect_type.value,
            est.scale.value,
            est.p_value,
            est.ci_lower,
            est.ci_upper,
            est.ci_level,
            est.n_total,
        ),
    )
    return finding_id


# -- reads -------------------------------------------------------------------

def find_existing_finding(
    conn: sqlite3.Connection, content_id: str, data_id: str
) -> Optional[sqlite3.Row]:
    """Return the existing finding row for (content_id, data_id), else None.

    The idempotency anchor: re-asserting the same finding on the same dataset
    returns the prior finding rather than double-counting it. The full row is
    returned so the caller can report the same shape (plan_id, claim_id) as a
    fresh finding.
    """
    return conn.execute(
        "SELECT f.* FROM findings f "
        "JOIN evidence_lines el ON el.finding_id = f.finding_id "
        "WHERE f.content_id = ? AND el.data_id = ? LIMIT 1",
        (content_id, data_id),
    ).fetchone()


def independence_counts(conn: sqlite3.Connection, content_id: str) -> tuple[int, int]:
    """(independent_support, independent_refute) over distinct data_ids.

    Count distinct data_ids among supporting lines and among refuting lines on
    this content_id (the distinct-artifact independence heuristic).
    """
    row = conn.execute(
        "SELECT "
        " COUNT(DISTINCT CASE WHEN f.bearing_direction = 'supports' "
        "                     THEN el.data_id END) AS support, "
        " COUNT(DISTINCT CASE WHEN f.bearing_direction = 'refutes' "
        "                     THEN el.data_id END) AS refute "
        "FROM findings f JOIN evidence_lines el ON el.finding_id = f.finding_id "
        "WHERE f.content_id = ?",
        (content_id,),
    ).fetchone()
    return int(row["support"] or 0), int(row["refute"] or 0)


def get_proposition_row(
    conn: sqlite3.Connection, content_id: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM propositions WHERE content_id = ?", (content_id,)
    ).fetchone()


def _frame_status(
    conn: sqlite3.Connection, frame_id: str, direction: Direction
) -> FrameStatus:
    """CONTESTED iff some contrary proposition in the same frame has >=1
    independent supporting line; CONSISTENT otherwise. Stops at the first such
    contrary.
    """
    contraries = [d.value for d in direction.contrary_set if d != direction]
    if not contraries:
        return FrameStatus.CONSISTENT
    placeholders = ",".join("?" for _ in contraries)
    rows = conn.execute(
        f"SELECT content_id FROM propositions "
        f"WHERE frame_id = ? AND direction IN ({placeholders})",
        (frame_id, *contraries),
    ).fetchall()
    for r in rows:
        support, _ = independence_counts(conn, r["content_id"])
        if support >= 1:
            return FrameStatus.CONTESTED
    return FrameStatus.CONSISTENT


def proposition_status(conn: sqlite3.Connection, content_id: str) -> Optional[dict]:
    """The retrieval view: derived Status + counts + frame contest, or None.

    ``status`` is the same-proposition state (support vs refute lines on this
    content_id). ``frame_status`` is the separate frame-level contest (a contrary
    proposition in the same frame has independent support). They are two
    different signals and never the same number.
    """
    row = get_proposition_row(conn, content_id)
    if row is None:
        return None
    support, refute = independence_counts(conn, content_id)
    status = compute_status(support, refute)
    frame_status = _frame_status(conn, row["frame_id"], Direction(row["direction"]))
    return {
        "content_id": content_id,
        "frame_id": row["frame_id"],
        "direction": row["direction"],
        "status": status.value,
        "independent_support": support,
        "independent_refute": refute,
        "frame_status": frame_status.value,
        "status_policy": STATUS_POLICY,
    }


def query_frame(
    conn: sqlite3.Connection, frame_id: str, min_status: Optional[str] = None
) -> list[dict]:
    """Everything known about a question (frame_id), each with its derived view.

    ``min_status`` filters to propositions at or above a floor on the
    UNTESTED < PRELIMINARY < CORROBORATED support ladder. Only those three are
    valid floors; REFUTED and CONTESTED are off the ladder and are excluded by
    any floor.
    """
    floor = None
    if min_status is not None:
        if min_status not in _VALID_FLOORS:
            raise ValueError(
                f"min_status must be one of {sorted(_VALID_FLOORS)}; got {min_status!r}"
            )
        floor = _SUPPORT_RANK[min_status]

    rows = conn.execute(
        "SELECT content_id FROM propositions WHERE frame_id = ?", (frame_id,)
    ).fetchall()
    out: list[dict] = []
    for r in rows:
        view = proposition_status(conn, r["content_id"])
        if view is None:
            continue
        if floor is not None and _SUPPORT_RANK[view["status"]] < floor:
            continue
        out.append(view)
    return out
