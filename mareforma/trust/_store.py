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

from .bearing import Bearing, BearingDirection, compute_bearing
from .estimate import EffectEstimate, EvidenceLine
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


# data_id content-addressing -------------------------------------------------
#
# A finding's independence guard counts distinct datasets by data_id. When the
# agent supplies the dataset bytes, mareforma hashes them itself so two
# findings over byte-identical data collapse to one line (a re-run is not a
# second dataset) and an agent cannot fabricate distinctness with a made-up
# string. The ``sha256:`` prefix makes the content-addressed value
# self-describing: a data_id without it is an agent-attested string fallback,
# which a consumer can discount.

_CONTENT_ADDRESS_PREFIX = "sha256:"


def content_address_data_id(data_bytes: bytes) -> str:
    """Return the content-addressed data_id for *data_bytes* (``sha256:<hex>``)."""
    if not isinstance(data_bytes, (bytes, bytearray)):
        raise TypeError(
            f"data_bytes must be bytes, got {type(data_bytes).__name__}"
        )
    return _CONTENT_ADDRESS_PREFIX + hashlib.sha256(bytes(data_bytes)).hexdigest()


def is_content_addressed(data_id: str) -> bool:
    """True iff *data_id* was content-addressed from dataset bytes."""
    return isinstance(data_id, str) and data_id.startswith(_CONTENT_ADDRESS_PREFIX)


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


def compute_plan_id(content_id: str, prediction: Prediction) -> str:
    """Content-addressed plan_id over (content_id + the prediction's identity).

    The plan_id is the identity of a decision *rule*: it hashes the gate-bearing
    fields (test_type, direction_of_interest, the equivalence margins, alpha,
    inference_regime) bound to a proposition. ``preregistered`` is deliberately
    EXCLUDED: it is provenance metadata about how the row was created (a real
    pre-registration vs a one-shot synthesised by ``assert_finding``), not part
    of the rule's identity. Two callers asserting the same rule must land on the
    same plan_id whether or not either flagged it pre-registered, so a finding
    can bind to a pre-registered plan regardless of the flag. Pure function: no
    DB read, deterministic across hosts (RFC 8785 bytes).
    """
    ident = {k: v for k, v in prediction.to_dict().items() if k != "preregistered"}
    return hashlib.sha256(
        canonicalize({"content_id": content_id, **ident})
    ).hexdigest()


def plan_exists(conn: sqlite3.Connection, plan_id: str) -> bool:
    """True iff a registered plan (predictions row) with this plan_id exists."""
    row = conn.execute(
        "SELECT 1 FROM predictions WHERE plan_id = ? LIMIT 1", (plan_id,)
    ).fetchone()
    return row is not None


def get_plan_claim_id(conn: sqlite3.Connection, plan_id: str) -> Optional[str]:
    """The claim_id of the plan attestation written by ``register_plan``.

    The plan claim is written via ``assert_claim`` under the idempotency key
    ``plan:{plan_id}``; this looks it up so a finding can cite it in its signed
    ``supports[]``. Returns None when no such claim exists (e.g. a predictions
    row planted directly by SQL without going through ``register_plan``).
    """
    row = conn.execute(
        "SELECT claim_id FROM claims WHERE idempotency_key = ? LIMIT 1",
        (f"plan:{plan_id}",),
    ).fetchone()
    return row["claim_id"] if row is not None else None


def register_plan(
    conn: sqlite3.Connection,
    content_id: str,
    prediction: Prediction,
    now: str,
    *,
    preregistered: bool,
) -> str:
    """Register a plan bound to content_id; return its plan_id.

    The plan_id is content-addressed (see :func:`compute_plan_id`), so
    registering the same plan twice is a no-op (ON CONFLICT DO NOTHING). A retry
    after a partially-written finding cannot create a duplicate, un-deletable
    plan row.

    ``preregistered`` is set explicitly by the caller (1 for an up-front
    ``register_plan`` call, 0 for the plan ``assert_finding`` synthesises in its
    one-shot path) and is NOT part of the plan_id, so the row's flag and the
    rule's identity are decoupled. The flag is append-only: the first writer
    wins it; a later registration of the same plan_id leaves it unchanged.
    """
    p = prediction
    plan_id = compute_plan_id(content_id, prediction)
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
            1 if preregistered else 0,
            now,
        ),
    )
    return plan_id


def insert_finding(
    conn: sqlite3.Connection,
    content_id: str,
    plan_id: str,
    claim_id: str,
    bearings: list[Bearing],
    lines: list[EvidenceLine],
    now: str,
) -> str:
    """Write the finding plus its N-line evidence tree; return finding_id.

    ``lines`` and ``bearings`` are parallel: ``bearings[i]`` is the gate output
    for ``lines[i].estimate`` under the finding's one prediction. The single-line
    case is ``len(lines) == 1``. ``findings.bearing_direction`` is a denormalised
    per-finding cache of the FIRST line's bearing (the column is NOT NULL). It is
    correct for single-line findings, where it equals the one line's bearing. The
    authoritative per-line bearings are the gate output over each stored estimate;
    :func:`independence_counts` recomputes them on read so that a multi-line
    finding whose lines disagree is counted per line, not off this cache.
    """
    if not lines:
        raise ValueError("a finding must carry at least one evidence line")
    if len(bearings) != len(lines):
        raise ValueError("bearings and lines must be parallel (same length)")
    finding_id = _uuid()
    conn.execute(
        "INSERT INTO findings "
        "(finding_id, content_id, plan_id, claim_id, bearing_direction, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (finding_id, content_id, plan_id, claim_id, bearings[0].direction.value, now),
    )
    for line in lines:
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


def finding_data_ids(conn: sqlite3.Connection, finding_id: str) -> set[str]:
    """The set of distinct ``data_id`` values on a finding's evidence lines.

    The multi-line idempotency anchor: a finding's identity within a
    ``content_id`` is its full data_id set, so a re-submission is idempotent only
    when it carries the same set under the same plan (see ``submit_finding``).
    """
    rows = conn.execute(
        "SELECT DISTINCT data_id FROM evidence_lines WHERE finding_id = ?",
        (finding_id,),
    ).fetchall()
    return {r["data_id"] for r in rows}


def _count_run_distinct(pairs: list[tuple[str, str]]) -> int:
    """Independent count over (run, dataset) pairs, run-distinct policy.

    A unit of independent evidence requires BOTH a fresh run (``generated_by``)
    AND a fresh dataset (``data_id``): one run contributes at most one unit (so a
    single run cannot self-certify), and the same dataset counts once even if it
    re-appears (the ``data_id`` guard).

    The count is order-free. Each dataset is attributed to exactly one run (the
    smallest token, a deterministic tie-break), then the answer is the number of
    distinct runs that own at least one dataset. Under the write-time invariant
    (``submit_finding``'s fork-guard makes every ``data_id`` belong to exactly one
    finding, hence one run) this is exactly "distinct runs among the lines"; the
    per-dataset attribution only matters as defence-in-depth if a future path
    (e.g. federation import) ever lets one dataset appear under two runs, in which
    case it stays deterministic and conservative rather than order-dependent.
    """
    run_of_dataset: dict[str, str] = {}
    for run, data_id in pairs:
        prior = run_of_dataset.get(data_id)
        if prior is None or run < prior:
            run_of_dataset[data_id] = run
    return len(set(run_of_dataset.values()))


def independence_counts(conn: sqlite3.Connection, content_id: str) -> tuple[int, int]:
    """(independent_support, independent_refute) by distinct signer, data_id guard.

    Per-line bearing is recomputed on read: each evidence line's stored estimate
    is gated against its finding's stored prediction (the gate inputs are
    persisted precisely so a reader can recompute and catch drift), so a
    multi-line finding whose lines disagree is counted line by line, never off
    the finding's denormalised ``bearing_direction`` cache.

    Independence is then counted by distinct **signer** (the claim's
    ``asserter_keyid``) with a ``data_id`` guard (see
    :func:`_count_run_distinct`): one signer yields at most one independent
    support and one independent refute. This is the same WHO axis the
    REPLICATED promotion query keys on, read from the same denormalised claim
    column, so promotion and trust counting can never disagree. Legacy
    evidence lines whose claim predates the keyid column (NULL
    ``asserter_keyid``) fall back to the retired ``generated_by`` run axis so
    they keep their count instead of silently collapsing two NULL signers to
    one (status_policy@v3). The two axes are namespaced (``k:`` vs ``g:``) so a
    keyid can never alias a run label.
    """
    rows = conn.execute(
        "SELECT el.data_id AS data_id, cl.generated_by AS generated_by, "
        " cl.asserter_keyid AS asserter_keyid, "
        " est.estimate_value, est.effect_type, est.scale, est.p_value, "
        " est.ci_lower, est.ci_upper, est.ci_level, est.n_total, "
        " pr.test_type, pr.direction_of_interest, pr.equivalence_lower, "
        " pr.equivalence_upper, pr.alpha, pr.inference_regime "
        "FROM findings f "
        "JOIN evidence_lines el ON el.finding_id = f.finding_id "
        "JOIN contrasts c ON c.line_id = el.line_id "
        "JOIN effect_estimates est ON est.contrast_id = c.contrast_id "
        "JOIN predictions pr ON pr.plan_id = f.plan_id "
        "JOIN claims cl ON cl.claim_id = f.claim_id "
        "WHERE f.content_id = ?",
        (content_id,),
    ).fetchall()

    supports: list[tuple[str, str]] = []
    refutes: list[tuple[str, str]] = []
    for r in rows:
        # Recompute the per-line bearing from stored inputs. Every row written by
        # submit_finding was gated at write, so this is total for normal data. A
        # row that no longer reconstructs into a gateable bearing (drift,
        # corruption, or a direct/foreign writer landing a non-numeric column) is
        # skipped rather than allowed to raise: one un-gateable line must not deny
        # reads for the whole proposition (and its frame's contraries). The catch
        # is broad on purpose: the failure can surface as ValueError (enum / range),
        # TypeError (non-numeric column reaching math.isfinite), or
        # InconsistentEstimateError (the gate). Writes are gated by EffectEstimate /
        # compute_bearing before persistence, so a broad skip here cannot mask a
        # write bug.
        try:
            estimate = EffectEstimate(
                estimate_value=r["estimate_value"],
                effect_type=r["effect_type"],
                scale=r["scale"],
                p_value=r["p_value"],
                ci_lower=r["ci_lower"],
                ci_upper=r["ci_upper"],
                ci_level=r["ci_level"],
                n_total=r["n_total"],
            )
            prediction = Prediction(
                test_type=r["test_type"],
                alpha=r["alpha"],
                direction_of_interest=r["direction_of_interest"],
                equivalence_lower=r["equivalence_lower"],
                equivalence_upper=r["equivalence_upper"],
                inference_regime=r["inference_regime"],
            )
            direction = compute_bearing(estimate, prediction).direction
        except Exception:
            continue
        # Independence axis = distinct signer (asserter_keyid), the same WHO
        # the REPLICATED promotion keys on. Legacy lines whose claim has no
        # keyid fall back to the retired generated_by run axis so they keep
        # their count; the k:/g: namespace stops a keyid aliasing a run label.
        keyid = r["asserter_keyid"]
        run_token = f"k:{keyid}" if keyid is not None else f"g:{r['generated_by']}"
        pair = (run_token, r["data_id"])
        if direction is BearingDirection.SUPPORTS:
            supports.append(pair)
        elif direction is BearingDirection.REFUTES:
            refutes.append(pair)
    return _count_run_distinct(supports), _count_run_distinct(refutes)


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
