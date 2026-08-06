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
from pathlib import Path
from typing import Iterable, Optional

from mareforma._canonical import canonicalize

from .bearing import Bearing, BearingDirection
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
    Status.CONVERGENT.value: 2,
    Status.REFUTED.value: -1,
    Status.CONTESTED.value: -1,
}

# The only valid min_status floors are the three support-ladder statuses.
_VALID_FLOORS = frozenset(
    {Status.UNTESTED.value, Status.PRELIMINARY.value, Status.CONVERGENT.value}
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
    inference_regime) bound to a proposition. ``preregistered`` is not among
    them: it is provenance the store owns (a real pre-registration vs a one-shot
    synthesised by ``assert_finding``), not part of the rule's identity, so a
    finding binds to a pre-registered plan regardless of how the row was made.
    Pure function: no DB read, deterministic across hosts (RFC 8785 bytes).
    """
    return hashlib.sha256(
        canonicalize({"content_id": content_id, **prediction.to_dict()})
    ).hexdigest()


def plan_exists(conn: sqlite3.Connection, plan_id: str) -> bool:
    """True iff a registered plan (predictions row) with this plan_id exists."""
    row = conn.execute(
        "SELECT 1 FROM predictions WHERE plan_id = ? LIMIT 1", (plan_id,)
    ).fetchone()
    return row is not None


def plan_registration(
    conn: sqlite3.Connection, plan_id: str
) -> Optional[sqlite3.Row]:
    """The ``registered_at`` + ``preregistered`` of a plan, or None if absent.

    Read by the pre-registration guard: only a plan that claims pre-registration
    (``preregistered = 1``) is gated on its registration time, so both fields are
    fetched together.
    """
    return conn.execute(
        "SELECT registered_at, preregistered FROM predictions "
        "WHERE plan_id = ? LIMIT 1",
        (plan_id,),
    ).fetchone()


def run_first_execution(
    conn: sqlite3.Connection, run_token: str
) -> Optional[str]:
    """The earliest finding-execution timestamp attributed to *run_token*, or None.

    A finding is a run's observed execution: it records an outcome the run
    computed. The run's first execution is the earliest ``created_at`` over the
    findings whose attestation claim carries this ``generated_by`` token. Returns
    None when the run has authored no finding yet, it has not begun executing,
    so no later plan can post-date it. ISO-8601 UTC timestamps compare
    lexicographically, so ``MIN`` is the chronological earliest.
    """
    row = conn.execute(
        "SELECT MIN(f.created_at) AS first_at FROM findings f "
        "JOIN claims c ON c.claim_id = f.claim_id "
        "WHERE c.generated_by = ?",
        (run_token,),
    ).fetchone()
    return row["first_at"] if row is not None else None


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


def prediction_from_row(row) -> Prediction:
    """Rebuild the decision rule a stored ``predictions`` row (or join) carries.

    The one place the persisted columns become a :class:`Prediction` again:
    the read path gates every line through it, and the retirement path asks it
    whether a plan's rule can still be run at all. It applies the live write-time
    invariants, so a row a release with a wider bound wrote (an alpha at or above
    0.5, which no gate can discriminate at) raises here rather than gating at a
    rule that decides nothing. :func:`superseding_prediction` is the supported
    way out of that state.
    """
    return Prediction(
        test_type=row["test_type"],
        alpha=row["alpha"],
        direction_of_interest=row["direction_of_interest"],
        equivalence_lower=row["equivalence_lower"],
        equivalence_upper=row["equivalence_upper"],
        inference_regime=row["inference_regime"],
    )


def replacement_prediction(row, alpha: float) -> Prediction:
    """The retired row's rule, repeated at *alpha*.

    Only the alpha moves. Everything the gate reads to decide direction
    (``test_type``, ``direction_of_interest``, the equivalence margins, the
    regime) is carried over from the registration, so a repair cannot re-choose
    the rule once the numbers are known: an operator who could also flip
    ``direction_of_interest`` would be picking the side of the null that the
    results already landed on.
    """
    return Prediction(
        test_type=row["test_type"],
        alpha=alpha,
        direction_of_interest=row["direction_of_interest"],
        equivalence_lower=row["equivalence_lower"],
        equivalence_upper=row["equivalence_upper"],
        inference_regime=row["inference_regime"],
    )


def get_plan_row(conn: sqlite3.Connection, plan_id: str) -> Optional[sqlite3.Row]:
    """The full ``predictions`` row for *plan_id*, or None."""
    return conn.execute(
        "SELECT * FROM predictions WHERE plan_id = ?", (plan_id,)
    ).fetchone()


def plan_retirement(
    conn: sqlite3.Connection, plan_id: str
) -> Optional[sqlite3.Row]:
    """The retirement record for *plan_id*, or None if the plan is live."""
    return conn.execute(
        "SELECT * FROM plan_retirements WHERE plan_id = ?", (plan_id,)
    ).fetchone()


def superseding_prediction(
    conn: sqlite3.Connection, plan_id: str
) -> Optional[Prediction]:
    """The rule a retired plan's evidence now stands under, or None.

    Read only for a plan whose own stored rule cannot be run (see
    :func:`prediction_from_row`), so resolution can never move a line that
    counts today: the lines it reaches are the ones counting zero. Raises if the
    superseding row is itself un-gateable, which the caller treats like any other
    un-gateable line and skips.
    """
    row = conn.execute(
        "SELECT p.test_type, p.alpha, p.direction_of_interest, "
        " p.equivalence_lower, p.equivalence_upper, p.inference_regime "
        "FROM plan_retirements r JOIN predictions p ON p.plan_id = r.superseded_by "
        "WHERE r.plan_id = ?",
        (plan_id,),
    ).fetchone()
    return None if row is None else prediction_from_row(row)


def plan_estimates(conn: sqlite3.Connection, plan_id: str) -> list[sqlite3.Row]:
    """Every stored effect estimate on an evidence line under *plan_id*."""
    return conn.execute(
        "SELECT est.estimate_value, est.effect_type, est.scale, est.p_value, "
        " est.ci_lower, est.ci_upper, est.ci_level, est.n_total "
        "FROM findings f "
        "JOIN evidence_lines el ON el.finding_id = f.finding_id "
        "JOIN contrasts c ON c.line_id = el.line_id "
        "JOIN effect_estimates est ON est.contrast_id = c.contrast_id "
        "WHERE f.plan_id = ?",
        (plan_id,),
    ).fetchall()


def estimate_from_row(row) -> EffectEstimate:
    """Rebuild the stored estimate a gate reads from a persisted row."""
    return EffectEstimate(
        estimate_value=row["estimate_value"],
        effect_type=row["effect_type"],
        scale=row["scale"],
        p_value=row["p_value"],
        ci_lower=row["ci_lower"],
        ci_upper=row["ci_upper"],
        ci_level=row["ci_level"],
        n_total=row["n_total"],
    )


def estimates_digest(triples: "Iterable[tuple[str, str, dict]]") -> str:
    """Digest over a finding's ``(data_id, control_type, estimate)`` content.

    A finding binds this into its claim's signed record so a later read can
    recompute it from the live rows and detect an altered or deleted estimate,
    contrast, or evidence line: the digest commits to what the line set *should*
    contain, so a removed row is caught rather than invisible to an inner join.

    It commits to CONTENT, never to row ids. ``finding_id``, ``line_id``,
    ``contrast_id``, and ``estimate_id`` are all minted inside
    :func:`insert_finding` AFTER the claim is signed, so a digest over them could
    never be reproduced on read.

    A canonically SORTED multiset with explicit counts, not an ordered list.
    ``evidence_lines`` carries no ordinal column, ``created_at`` is identical
    across a finding's lines, and ``line_id`` is a random uuid, so write order is
    not recoverable on read (it survives only as an accident of the query plan,
    which ``ANALYZE`` changes). Duplicate lines are reachable through the public
    API, so the per-tuple count is load-bearing: a set loses it and would permit
    one silent deletion. Each tuple is keyed by its canonical bytes, the keys are
    sorted, and the digest covers ``[[tuple, count], ...]``.
    """
    counts: dict[bytes, int] = {}
    reps: dict[bytes, list] = {}
    for data_id, control_type, estimate in triples:
        rep = [data_id, control_type, estimate]
        key = canonicalize(rep)
        counts[key] = counts.get(key, 0) + 1
        reps.setdefault(key, rep)
    entries = [[reps[key], counts[key]] for key in sorted(counts)]
    return hashlib.sha256(canonicalize(entries)).hexdigest()


def estimates_digest_from_lines(lines: "Iterable[EvidenceLine]") -> str:
    """The finding digest computed from its :class:`EvidenceLine` objects.

    The write-time path. Serialises each line's estimate through
    :meth:`EffectEstimate.to_dict`, the exact serialisation
    :func:`estimates_digest_from_rows` rebuilds on read, so write and read agree
    byte-for-byte.
    """
    return estimates_digest(
        (line.data_id, line.contrast.control_type.value, line.estimate.to_dict())
        for line in lines
    )


def estimates_digest_from_rows(rows: "Iterable[sqlite3.Row]") -> str:
    """The finding digest recomputed from the live joined rows.

    The read-time path. Rebuilds each estimate through
    :func:`estimate_from_row` and serialises it with the same
    :meth:`EffectEstimate.to_dict` the write path uses, so a digest recomputed
    here equals the one signed at write time iff no estimate, contrast, or line
    was altered or removed.

    A row whose estimate is absent, an ``effect_estimates``/``contrasts``/
    ``evidence_lines`` row deleted so the count query's LEFT JOINs yield NULLs
    downward, contributes a ``None`` estimate in place of a rebuilt one. The
    write path never signs a ``None`` estimate (a live finding always has its
    full tree), so the recomputed digest necessarily differs from the signed
    one and the deletion is caught as a mismatch over the whole finding rather
    than slipping through: recomputing ``estimate_from_row`` on a NULL row would
    raise, which would abandon the digest check and let the finding's surviving
    lines count. ``estimate_value`` is ``NOT NULL`` in the schema, so a NULL
    there is the reliable signal that the downward join found no estimate.
    """
    return estimates_digest(
        (
            r["data_id"],
            r["control_type"],
            estimate_from_row(r).to_dict()
            if r["estimate_value"] is not None
            else None,
        )
        for r in rows
    )


def retirement_claim_text(plan_id: str, superseded_by: str, reason: str) -> str:
    """The text the retirement attestation signs.

    The row's three fields are unsigned columns, and a retirement decides which
    rule a proposition's evidence is gated under, so the triple is rendered into
    the claim text, which is signed and chained. Restore rebuilds this string
    from the replayed row and compares it against the verified claim, so an
    edited backup cannot re-point a retirement or rewrite its reason.
    """
    return (
        f"Retired plan {plan_id}, superseded by plan {superseded_by}. "
        f"Reason: {reason}"
    )


def retire_plan(
    conn: sqlite3.Connection,
    plan_id: str,
    superseded_by: str,
    reason: str,
    claim_id: str,
    now: str,
) -> None:
    """Record the retirement of *plan_id* in favour of *superseded_by*."""
    conn.execute(
        "INSERT INTO plan_retirements "
        "(plan_id, superseded_by, reason, claim_id, retired_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (plan_id, superseded_by, reason, claim_id, now),
    )


def insert_finding(
    conn: sqlite3.Connection,
    content_id: str,
    plan_id: str,
    claim_id: str,
    bearings: list[Bearing],
    lines: list[EvidenceLine],
    now: str,
    *,
    model_lineage: "str | None" = None,
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

    ``model_lineage`` is the JSON model/method lineage the observer captured for
    the authoring scope (COMPUTED / PROXY / UNVERIFIABLE), or None when no model
    call was observed. It is written on every line of the finding: the scope
    authors the finding as a whole, so its lineage attributes each line, and the
    per-line column lets a reader key independence on distinct model as well as
    signer and data.
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
            "(line_id, finding_id, modality, provenance_id, design_type, data_id, "
            " model_lineage, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                line_id,
                finding_id,
                line.modality,
                line.provenance_id,
                line.design_type,
                line.data_id,
                model_lineage,
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


def finding_model_lineage(
    conn: sqlite3.Connection, finding_id: str
) -> Optional[dict]:
    """The model/method lineage recorded on a finding's evidence lines, or None.

    The lineage is written identically on every line of a finding, so the first
    non-NULL value represents the finding. Returns the parsed dict, or None when
    the finding was authored without an observed model call.
    """
    row = conn.execute(
        "SELECT model_lineage FROM evidence_lines "
        "WHERE finding_id = ? AND model_lineage IS NOT NULL LIMIT 1",
        (finding_id,),
    ).fetchone()
    if row is None or row["model_lineage"] is None:
        return None
    try:
        return json.loads(row["model_lineage"])
    except (ValueError, TypeError):
        return None


def _collapse_run_model(keys: list[tuple]) -> tuple:
    """The single model state a run authored across its surviving datasets.

    A human line wins outright: a check a human authored is the strongest
    independent axis and is never demoted by a model-lineage sibling under the
    same signer (a run contributes at most one unit, so the highest-value key
    stands). Otherwise: any soft line makes the whole run soft (its independence
    cannot be certified); two distinct COMPUTED roots under one run also collapse
    to soft (a run that authored under more than one model cannot be pinned to a
    single distinct one); a single COMPUTED root is that ``("model", root)``;
    otherwise the run made no model claim (``("absent",)``).
    """
    if any(k[0] == "human" for k in keys):
        return ("human",)
    if any(k[0] == "soft" for k in keys):
        return ("soft",)
    roots = {k[1] for k in keys if k[0] == "model"}
    if len(roots) > 1:
        return ("soft",)
    if roots:
        return ("model", next(iter(roots)))
    return ("absent",)


def _count_run_distinct(units: list[tuple[str, str, tuple]]) -> int:
    """Independent count over (run, dataset, model) units, model-distinct policy.

    A unit of independent evidence requires a fresh run (``generated_by`` /
    signer), a fresh dataset (``data_id``), AND a distinct model/method. Two
    same-model checks, distinct signer and distinct dataset but the same
    COMPUTED model, are one line of evidence, not two, so they no longer promote
    on the signer + data axes alone.

    A human check is the exception and the highest-value source: it needs no
    distinct model (a human is not a model), so a human run counts per signer and
    is never folded into a model root, a human check plus a model check reads as
    two, where two same-model checks read as one.

    The count is order-free. Each dataset is first attributed to exactly one run
    (the smallest token, a deterministic tie-break), carrying that line's model
    key, so a re-appearing dataset counts once. The surviving datasets are then
    grouped by run, this preserves the "one signer contributes at most one
    unit" cap, and each run's model state is resolved
    (see :func:`_collapse_run_model`). The answer folds the two axes:

    - COMPUTED runs collapse by family root, so two distinct signers on the same
      model count once;
    - absent runs (no observed model call) keep the legacy signer axis, so every
      pre-observer finding is unchanged;
    - soft runs (PROXY / UNVERIFIABLE) are UNVERIFIABLE for independence and add
      no unit, so a soft pair cannot silently corroborate, never a silent pass.

    A body of only-soft or single lines still stands at one supporting line
    (PRELIMINARY), never zero: soft lineage weakens independence, it does not
    erase the evidence.
    """
    best: dict[str, tuple[str, tuple]] = {}
    for run, data_id, model_key in units:
        cur = best.get(data_id)
        if cur is None or run < cur[0]:
            best[data_id] = (run, model_key)

    run_models: dict[str, list[tuple]] = {}
    for run, model_key in best.values():
        run_models.setdefault(run, []).append(model_key)

    human_runs: set[str] = set()
    computed_roots: set[str] = set()
    absent_runs: set[str] = set()
    for run, keys in run_models.items():
        state = _collapse_run_model(keys)
        if state[0] == "human":
            human_runs.add(run)
        elif state[0] == "model":
            computed_roots.add(state[1])
        elif state[0] == "absent":
            absent_runs.add(run)
        # soft: contributes no independent unit
    hard = len(human_runs) + len(computed_roots) + len(absent_runs)
    if hard:
        return hard
    return 1 if run_models else 0


# The read query behind independence_counts. Kept as a module constant so a
# regression test can pin its EXPLAIN QUERY PLAN: the count anchors on findings
# through idx_find_content, and the join stays keyed through idx_contrast_line
# and idx_estimate_contrast (no full scan of effect_estimates).
#
# The row is anchored on ``findings`` and joins DOWNWARD with LEFT JOINs to
# evidence_lines, contrasts, and effect_estimates. The anchor matters: a finding
# is the unit the signed estimates digest commits to, so enumerating from the
# signed findings (rather than inner-joining up from the estimates) keeps a
# finding visible even when its evidence rows were deleted. An inner join drops
# the whole finding when any downward row is missing, and for a single-line
# finding, the modal shape, that erases the finding before its digest can be
# checked, so a deleted estimate reads as if the line never existed. The LEFT
# JOINs keep one row per finding with NULLs downward instead, which the
# per-finding digest check in :func:`mareforma.trust._gate._derive_units` sees
# and refuses (:func:`estimates_digest_from_rows` handles the NULLs). On an
# untampered graph every finding has its full line/contrast/estimate tree, so
# the LEFT JOINs yield exactly the rows the inner joins did and nothing changes.
# The ``predictions`` and ``claims`` joins stay INNER: they key off the
# finding's own ``plan_id`` / ``claim_id`` columns, not the deletable downward
# chain.
#
# The withdrawal columns are SELECTed, not filtered on: only a live claim
# contributes a line, but the exclusion is disclosed rather than applied in SQL
# (see :func:`_independence_units`).
INDEPENDENCE_COUNTS_SQL = (
    "SELECT el.line_id AS line_id, el.data_id AS data_id, "
    " f.finding_id AS finding_id, c.control_type AS control_type, "
    " el.model_lineage AS model_lineage, f.plan_id AS plan_id, "
    " cl.generated_by AS generated_by, "
    " cl.asserter_keyid AS asserter_keyid, cl.claim_id AS claim_id, "
    " cl.signature_bundle AS signature_bundle, "
    " cl.predicate_payload AS predicate_payload, "
    " cl.status AS claim_status, cl.t_invalid AS t_invalid, "
    " cl.text AS claim_text, "
    " est.estimate_value, est.effect_type, est.scale, est.p_value, "
    " est.ci_lower, est.ci_upper, est.ci_level, est.n_total, "
    " pr.test_type, pr.direction_of_interest, pr.equivalence_lower, "
    " pr.equivalence_upper, pr.alpha, pr.inference_regime, "
    " pr.preregistered AS preregistered "
    "FROM findings f "
    "LEFT JOIN evidence_lines el ON el.finding_id = f.finding_id "
    "LEFT JOIN contrasts c ON c.line_id = el.line_id "
    "LEFT JOIN effect_estimates est ON est.contrast_id = c.contrast_id "
    "JOIN predictions pr ON pr.plan_id = f.plan_id "
    "JOIN claims cl ON cl.claim_id = f.claim_id "
    "WHERE f.content_id = ?"
)


class _UngateablePlan(Exception):
    """A line's plan carries a rule no gate can run, and nothing supersedes it.

    Internal to the read path: it separates the one drop an operator can repair
    (``retire_plan``) from drift and corruption, which no API fixes.
    """


def _gateable_prediction(conn: sqlite3.Connection, row) -> tuple[Prediction, bool]:
    """The rule a stored line is gated under, and whether it was superseded.

    Normally the line's own plan (``superseded`` False). When that plan's stored
    rule cannot be run, the line is gated under the plan that supersedes it,
    which an operator registered explicitly through
    :meth:`EpistemicGraph.retire_plan` (``superseded`` True). Resolution is
    reached only from that failure, so a retirement record can only ever reach
    lines that count zero as they stand: it can recover a dropped line, never
    drop a counted one, whatever a direct writer plants in the table.

    The ``superseded`` flag rides back so the caller can mark the count as
    resting on a replacement (post-hoc) plan: a replacement is registered after
    the estimates are visible, so a reader must be able to tell it from a
    pre-registered gate.
    """
    try:
        return prediction_from_row(row), False
    except ValueError as exc:
        try:
            superseding = superseding_prediction(conn, row["plan_id"])
        except ValueError:
            # The superseding row cannot be run either (only reachable by a
            # direct/foreign write): the line stays dropped, fail closed.
            superseding = None
        if superseding is None:
            raise _UngateablePlan(str(exc)) from exc
        # A retirement carries the retired rule over unchanged except its alpha:
        # it recovers evidence stranded under an un-gateable alpha, it does not
        # re-choose the side of the null once the numbers are known. That
        # identity is a property of the read, not only the write (finding 4): a
        # direct writer who re-points ``plan_retirements.superseded_by`` at
        # another registered plan whose rule differs would otherwise reflip the
        # line's bearing under a rule the retirement never sanctioned. Rebuild
        # the retired rule at the replacement's alpha and refuse a replacement
        # that does not reproduce it.
        if superseding != replacement_prediction(row, superseding.alpha):
            raise _UngateablePlan(
                "the replacement rule differs from the retired rule beyond its "
                "alpha; a retirement may only re-register the same rule at a "
                "gateable alpha"
            ) from exc
        return superseding, True


class SkipDisclosure:
    """The health-channel half of the skipped-line disclosure, deduplicated.

    A dropped line is a state, not an event: the claim stays withdrawn and an
    un-gateable row stays un-gateable, so every later read notices the same drop
    again. The per-read signal a caller acts on is ``lines_skipped`` in the
    returned view, recomputed every call; this channel records a drop once per
    ``(op, content_id, line_id)`` so an agent polling
    :func:`proposition_status` cannot grow ``.mareforma/health.jsonl`` in
    proportion to reads.

    The drop is disclosed on read rather than at the write that caused it
    because the writes that cause it need not pass through mareforma:
    ``status`` is an unsigned column any handle holding the graph may rewrite
    directly, and that keyless flip is exactly the erasure the disclosure
    exists to catch.

    The root and the dedupe set live in one object so a reader that can emit is
    a reader that deduplicates; there is no way to arm the emitter without it.
    """

    __slots__ = ("_root", "_seen")

    def __init__(self, root: "Path | str") -> None:
        self._root = root
        self._seen: set[tuple[str, str, str]] = set()

    def record(self, op: str, content_id: str, line_id: str, **detail) -> None:
        key = (op, content_id, line_id)
        if key in self._seen:
            return
        self._seen.add(key)
        from mareforma.health import append_health_event
        append_health_event(
            self._root, op, outcome="degraded",
            content_id=content_id, line_id=line_id, **detail,
        )


def _independence_units(
    conn: sqlite3.Connection, content_id: str, *, memo: "dict | None" = None,
    disclose: "SkipDisclosure | None" = None,
):
    """Yield ``(direction, run_token, data_id, model_key)`` per gateable line.

    The shared read path behind :func:`independence_counts` and
    :func:`effective_independence`. Per-line bearing is recomputed on read: each
    evidence line's stored estimate is gated against its finding's stored
    prediction (the gate inputs are persisted precisely so a reader can recompute
    and catch drift), so a multi-line finding whose lines disagree is counted
    line by line, never off the finding's denormalised ``bearing_direction``
    cache.

    The run token is the distinct **signer** (the claim's ``asserter_keyid``),
    the same WHO the REPLICATED promotion keys on. The denormalized column is not
    itself signed, so it is trusted only when the claim's bundle authenticates it
    (embeds the same keyid, binds to this claim, and verifies when the signer is
    enrolled); a forged or unbacked keyid falls back to the retired
    ``generated_by`` run axis so it cannot inflate the count beyond the soft
    string axis. The ``k:`` / ``g:`` namespace stops a keyid aliasing a run
    label. The model key carries the distinct-model/method axis, read from the
    SIGNED lineage rather than the unsigned column so a forged column cannot
    inflate the count (see :func:`mareforma.trust._gate._authentic_model_key`); a
    line with no observed model call whose signer
    is an enrolled human validator is re-keyed ``("human",)``, the human axis
    the status ladder counts (see
    :func:`mareforma.trust._gate._is_human_signer`). The per-finding map
    disclosure narrows that key back to soft, see :func:`_supporting_units`.

    The verification itself, live claim, gateable bearing, authenticated signer
    and model, lives behind one boundary in :mod:`mareforma.trust._gate`, which
    both this read path and restore reach so the rule cannot drift into two
    copies. This function applies the read-path disposition to what that boundary
    returns: it counts the verified lines and discloses the skipped ones.

    Two kinds of line are dropped from the count: one that no longer
    reconstructs into a gateable bearing, and one whose claim is no longer live
    (editorially withdrawn, or invalidated by a signed contradiction verdict).
    Dropping is never conservative, a dropped REFUTING line manufactures
    consensus, so no drop is silent. All are tallied under ``skipped`` in
    the ``memo`` (read back as ``lines_skipped``, see
    :func:`proposition_status`) and, when ``disclose`` is given, recorded once
    per line on the ``.mareforma/health.jsonl`` channel as
    ``bearing_recompute_skipped``, ``plan_rebind_skipped`` (the finding's
    ``plan_id`` column no longer matches the plan its claim records),
    ``plan_rule_rebind_skipped`` (a ``predictions`` rule column no longer hashes
    to the plan_id keying it), ``ungateable_plan_skipped`` (the repairable case,
    whose event names the plan :meth:`EpistemicGraph.retire_plan` takes) and
    ``withdrawn_line_skipped`` (see :class:`SkipDisclosure`).

    ``memo`` is an optional per-read-call cache. It carries the shared
    :class:`mareforma.trust._gate.GateCache` so a claim's signature verifies once
    across a frame read (the same claim is walked for its own proposition and
    again as a contrary), and the per-proposition ``skipped`` tally read back as
    ``lines_skipped``. Absent ``memo``, a fresh cache still dedups within the
    single call.
    """
    from ._gate import GateCache, verified_gate_inputs

    skipped = memo.setdefault("skipped", {}) if memo is not None else {}
    if memo is not None:
        cache = memo.get("gate_cache")
        if cache is None:
            cache = memo["gate_cache"] = GateCache()
    else:
        cache = GateCache()

    gate_inputs = verified_gate_inputs(conn, content_id, cache=cache)
    # A proposition depends on a replacement (post-hoc) plan when any counted
    # line is gated under a plan that was not pre-registered: either the finding's
    # own one-shot plan or the replacement a retirement resolved it to. Recorded
    # here, next to the skip tally, so proposition_status can surface it (see
    # its ``post_hoc`` field).
    if memo is not None:
        memo.setdefault("post_hoc", {})[content_id] = any(
            line.post_hoc for line in gate_inputs.units
        )
    for line in gate_inputs.skipped:
        skipped[content_id] = skipped.get(content_id, 0) + 1
        if disclose is not None:
            disclose.record(line.op, content_id, line.line_id, **line.detail)
    for line in gate_inputs.units:
        yield line.direction, line.run_token, line.data_id, line.model_key


def independence_counts(
    conn: sqlite3.Connection, content_id: str, *, memo: "dict | None" = None,
    disclose: "SkipDisclosure | None" = None,
) -> tuple[int, int]:
    """(independent_support, independent_refute) by distinct signer + model, data guard.

    Counted by distinct **signer** (the claim's ``asserter_keyid``) AND distinct
    **model/method**, with a ``data_id`` guard (see :func:`_count_run_distinct`):
    one signer yields at most one independent support and one independent refute,
    and two same-model checks no longer read as two independent lines. A keyid
    counts only when the claim's signature bundle authenticates it
    (:func:`mareforma.trust._gate._signer_axis`), so this axis is
    not the unsigned column
    the REPLICATED promotion query reads; that query is a separate check under
    its own editorial filters, and the two answer different questions and can
    differ. So can this count and the trust map's number:
    :func:`effective_independence` re-keys a line with no observed model call to
    soft, so it sits at or below the count here. Legacy evidence lines whose
    claim predates the keyid column (NULL ``asserter_keyid``) fall back to the
    retired ``generated_by`` run axis, and a finding with no observed model call
    carries no model constraint, so both keep their count instead of collapsing
    (status_policy@v4). A no-model-call line
    signed by an enrolled human validator counts on the human axis, never folded
    into a model root. That axis rests on a self-declared ``validator_type``, so
    the trust map's per-finding disclosure does not certify it; only this legacy
    ladder counts it. The two run
    axes are namespaced (``k:`` vs ``g:``) so a keyid can never alias a run label.

    Only a live claim contributes: a retracted or contested claim, or one a
    signed contradiction verdict invalidated, is excluded on both sides, so a
    withdrawn support can drop a proposition off CONVERGENT and a withdrawn
    refutation can drop it off REFUTED (see
    :func:`mareforma.trust.status.compute_status`). Every such exclusion is
    disclosed as a skipped line, so a reader can tell a proposition nobody
    contested from one whose refutations were withdrawn.

    ``memo`` is an optional per-read-call cache. A proposition's counts are
    constant within one read call, so a ``query_frame`` pass that counts a
    proposition once for itself and again for every sibling walking it as a
    contrary computes it once and reuses the result, sharing the same cache with
    :func:`_independence_units` so a claim's signature verifies once per call.
    """
    if memo is not None:
        counts_cache = memo.setdefault("counts", {})
        if content_id in counts_cache:
            return counts_cache[content_id]
    supports: list[tuple[str, str, tuple]] = []
    refutes: list[tuple[str, str, tuple]] = []
    for direction, run_token, data_id, model_key in _independence_units(
        conn, content_id, memo=memo, disclose=disclose
    ):
        unit = (run_token, data_id, model_key)
        if direction is BearingDirection.SUPPORTS:
            supports.append(unit)
        elif direction is BearingDirection.REFUTES:
            refutes.append(unit)
    result = (_count_run_distinct(supports), _count_run_distinct(refutes))
    if memo is not None:
        memo["counts"][content_id] = result
    return result


def effective_independence(
    conn: sqlite3.Connection, content_id: str,
    *, memo: "dict | None" = None, disclose: "SkipDisclosure | None" = None,
) -> dict:
    """The effective-independence number for a proposition, with a soft flag.

    ``number`` is the count of pairwise-distinct (model, data, signer) SUPPORTING
    checks over observed model calls. ``soft`` is True when a supporting line
    carries PROXY / UNVERIFIABLE model lineage or no observed model call at all,
    including one signed by a self-declared human validator: the count then rests
    on lineage that cannot certify a distinct model, which the trust map surfaces
    as UNVERIFIABLE rather than a confident number. The legacy status ladder
    (:func:`independence_counts`) is unchanged and still counts a human check on
    its own axis; this narrows only the per-finding disclosure.

    ``lines_skipped`` is how many of the proposition's evidence lines the shared
    verifier dropped (an unauthenticated signer, a withdrawn claim, an un-gateable
    or repointed line): the same tally :func:`proposition_status` reports, surfaced
    here so the independence axis does not read a confident number off a line set
    that silently lost lines. ``memo`` / ``disclose`` thread the per-call cache and
    the health channel through, so a dropped line is disclosed on this surface too,
    not only on ``proposition_status``.

    Coarse by design: distinct-model is binary this release. The graded
    cross-model residual (how *far apart* two distinct models are) is DEFERRED ,
    named here, not computed.
    """
    if memo is None:
        memo = {}
    supports, soft = _supporting_units(
        conn, content_id, memo=memo, disclose=disclose,
    )
    return {
        "number": _count_run_distinct(supports),
        "soft": soft,
        "lines_skipped": memo.get("skipped", {}).get(content_id, 0),
    }


def _supporting_units(
    conn: sqlite3.Connection, content_id: str,
    *, memo: "dict | None" = None, disclose: "SkipDisclosure | None" = None,
) -> tuple[list[tuple[str, str, tuple]], bool]:
    """The SUPPORTING ``(run, data, model)`` units for a proposition, plus soft.

    The shared collection behind :func:`effective_independence` and
    :func:`effective_independence_receipt`. ``soft`` is True when any supporting
    line carried PROXY / UNVERIFIABLE model lineage, or no observed model call at
    all (absent / human): the per-finding disclosure certifies a distinct model
    only for observed calls, so an unobserved model cannot be told apart and is
    soft here. Both are re-keyed to soft so they add no confident hard unit,
    dropping the count to the single-line floor rather than reverting to the
    pre-v0.3.10 signer axis. This narrows only the map's per-finding
    certification; the legacy status ladder (:func:`independence_counts`) still
    counts distinct signers and still reads the human axis.

    The human key is soft here because it rests on ``validator_type``, which is
    self-declared, defaults to ``'human'``, and cannot be chosen at all for the
    root a fresh graph auto-enrolls. Nothing was observed: the finding carries no
    model call and no person attested to it. A key the operator happens to hold
    must not turn an unobserved line into a certified independent one.

    ``memo`` / ``disclose`` are threaded to :func:`_independence_units` so a line
    the verifier drops is tallied in ``lines_skipped`` and disclosed on the health
    channel here too, not silently lost the way this surface lost them before.
    """
    supports: list[tuple[str, str, tuple]] = []
    soft = False
    for direction, run_token, data_id, model_key in _independence_units(
        conn, content_id, memo=memo, disclose=disclose,
    ):
        if direction is BearingDirection.SUPPORTS:
            if model_key[0] in ("absent", "human"):
                model_key = ("soft",)
            supports.append((run_token, data_id, model_key))
            if model_key[0] == "soft":
                soft = True
    return supports, soft


def effective_independence_receipt(
    conn: sqlite3.Connection, content_id: str,
    *, memo: "dict | None" = None, disclose: "SkipDisclosure | None" = None,
) -> dict:
    """The per-finding independence record a measurement receipt carries.

    Extends :func:`effective_independence` with ``naive``: the supporting count a
    signer-axis counter would report BEFORE the model-distinct collapse, taken
    over HARD (non-soft) lineage only. ``naive - number`` isolates the same-model
    (COMPUTED) collapse, two distinct signers on one model that a naive counter
    would read as two independent lines, while excluding soft-lineage weakening,
    which ``soft`` reports separately. A measurement aggregates these records into
    the independence arm of the report (see
    :func:`mareforma.observe.measure.summarize_independence`).

    ``naive`` counts distinct-signer supporting lines by re-keying every hard unit
    to the model-absent axis (so no two lines collapse on their model root), then
    reusing :func:`_count_run_distinct`. So a same-model pair reads ``naive=2,
    number=1`` (collapse of 1); a distinct-model pair reads ``naive=2, number=2``
    (no collapse); a soft-only body reads ``naive=0, number=1, soft=True``, not a
    collapse, an unverifiable count.

    ``memo`` / ``disclose`` thread the per-call cache and the health channel
    through :func:`_supporting_units`, so a line the verifier drops is tallied in
    ``lines_skipped`` and disclosed on this surface too.
    """
    if memo is None:
        memo = {}
    supports, soft = _supporting_units(
        conn, content_id, memo=memo, disclose=disclose,
    )
    number = _count_run_distinct(supports)
    hard = [(run, data_id, mk) for run, data_id, mk in supports if mk[0] != "soft"]
    naive = _count_run_distinct(
        [(run, data_id, ("absent",)) for run, data_id, _mk in hard]
    )
    # The receipt keeps its shape (number / naive / soft): it is aggregated into a
    # measurement's independence arm and its schema is stable. The ``memo`` /
    # ``disclose`` threaded above still route a dropped line to the health channel;
    # the drop is surfaced as ``lines_skipped`` on proposition_status and
    # effective_independence, the read views, not on this record.
    return {"number": number, "naive": naive, "soft": soft}


def get_proposition_row(
    conn: sqlite3.Connection, content_id: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM propositions WHERE content_id = ?", (content_id,)
    ).fetchone()


def _frame_status(
    conn: sqlite3.Connection, frame_id: str, direction: Direction,
    *, memo: "dict | None" = None, disclose: "SkipDisclosure | None" = None,
) -> FrameStatus:
    """CONTESTED iff some contrary proposition in the same frame has >=1
    independent supporting line; CONSISTENT otherwise. Stops at the first such
    contrary. ``memo`` shares the per-call independence cache (see
    :func:`independence_counts`).
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
        support, _ = independence_counts(
            conn, r["content_id"], memo=memo, disclose=disclose
        )
        if support >= 1:
            return FrameStatus.CONTESTED
    return FrameStatus.CONSISTENT


def proposition_status(
    conn: sqlite3.Connection, content_id: str, *, memo: "dict | None" = None,
    disclose: "SkipDisclosure | None" = None,
) -> Optional[dict]:
    """The retrieval view: derived Status + counts + frame contest, or None.

    ``status`` is the same-proposition state (support vs refute lines on this
    content_id). ``frame_status`` is the separate frame-level contest (a contrary
    proposition in the same frame has independent support). They are two
    different signals and never the same number.

    ``lines_skipped`` is how many of this proposition's evidence lines were left
    out of the counts, either because they no longer reconstruct into a gateable
    bearing or because their claim is no longer live (see
    :func:`_independence_units`). It tells a one-line proposition apart from a
    two-line proposition with a dropped line, which the counts alone cannot: a
    dropped refutation reads as consensus. ``disclose`` is the health channel
    the drop is also recorded on, once per line (see :class:`SkipDisclosure`).

    ``post_hoc`` is True when the count rests on a replacement (post-hoc) plan:
    any counted line gated under a plan that was not pre-registered, either a
    one-shot finding's own plan or the replacement a retirement resolved a
    stranded line to. The alpha of such a plan was chosen with the estimates in
    view, so the view is not shape-identical to a pre-registered result; this
    flag is what lets a reader tell the two apart.

    ``memo`` is an optional per-read-call cache shared between the own-status
    count and the frame-contest count so each proposition's counts (and each
    claim's signature verify) are computed once per call. Defaults to a fresh
    cache, so a standalone call still shares work between its two passes.
    """
    if memo is None:
        memo = {}
    row = get_proposition_row(conn, content_id)
    if row is None:
        return None
    support, refute = independence_counts(
        conn, content_id, memo=memo, disclose=disclose
    )
    status = compute_status(support, refute)
    frame_status = _frame_status(
        conn, row["frame_id"], Direction(row["direction"]), memo=memo,
        disclose=disclose,
    )
    return {
        "content_id": content_id,
        "frame_id": row["frame_id"],
        "direction": row["direction"],
        "status": status.value,
        "independent_support": support,
        "independent_refute": refute,
        "lines_skipped": memo.get("skipped", {}).get(content_id, 0),
        "post_hoc": memo.get("post_hoc", {}).get(content_id, False),
        "frame_status": frame_status.value,
        "status_policy": STATUS_POLICY,
    }


def query_frame(
    conn: sqlite3.Connection, frame_id: str, min_status: Optional[str] = None,
    *, disclose: "SkipDisclosure | None" = None,
) -> list[dict]:
    """Everything known about a question (frame_id), each with its derived view.

    ``min_status`` filters to propositions at or above a floor on the
    UNTESTED < PRELIMINARY < CONVERGENT support ladder. Only those three are
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
    # One memo for the whole frame read: proposition_status counts each
    # proposition for itself and again as a contrary of its siblings, so sharing
    # the cache across the frame collapses those repeats to one compute (and one
    # signature verify) per proposition.
    memo: dict = {}
    out: list[dict] = []
    for r in rows:
        view = proposition_status(conn, r["content_id"], memo=memo, disclose=disclose)
        if view is None:
            continue
        if floor is not None and _SUPPORT_RANK[view["status"]] < floor:
            continue
        out.append(view)
    return out
