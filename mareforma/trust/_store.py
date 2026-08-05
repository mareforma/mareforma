"""_store.py: persistence for the trust layer over a sqlite3 connection.

Pure SQL helpers that the EpistemicGraph methods call. They never sign or
commit (the caller owns the transaction and the signed attestation); they only
read and write the structured proposition/evidence tables. Keeping the SQL here
keeps the graph object thin and keeps every trust query in one place.
"""
from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import uuid
from pathlib import Path
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


def _authentic_signer_keyid(
    conn: sqlite3.Connection,
    claim_id: str,
    asserter_keyid: "str | None",
    bundle_json: "str | None",
) -> "str | None":
    """Return ``asserter_keyid`` only when the signed bundle authenticates it.

    The denormalized ``asserter_keyid`` column is not itself part of the signed
    envelope, so a direct/foreign writer can set it to any string to inflate the
    independence count. We trust it as the WHO axis only when the claim's
    ``signature_bundle`` (a) embeds that same keyid, (b) binds to this
    ``claim_id``, and (c) verifies against the pubkey when the signer is an
    enrolled validator. This mirrors the read-path verify gate. When it does not
    authenticate, the caller falls back to the soft ``generated_by`` axis, which
    is no weaker than the pre-keyid behaviour. Any structural failure returns
    None (fall back), never raises: one un-authenticatable line must not deny the
    whole proposition's count.
    """
    if asserter_keyid is None or not bundle_json:
        return None
    try:
        from .. import signing as _signing
        from .. import validators as _validators

        env = json.loads(bundle_json)
        if env["signatures"][0]["keyid"] != asserter_keyid:
            return None
        pred = _signing.claim_predicate_from_envelope(env)
        if pred.get("claim_id") != claim_id:
            return None
        signer_row = _validators.get_validator(conn, asserter_keyid)
        if signer_row is not None:
            pem = base64.standard_b64decode(signer_row["pubkey_pem"])
            pub = _signing.public_key_from_pem(pem)
            if not _signing.verify_envelope(env, pub):
                return None
        return asserter_keyid
    except Exception:
        return None


def _is_human_signer(conn: sqlite3.Connection, keyid: str) -> bool:
    """True iff *keyid* is an enrolled validator whose type is ``human``.

    The human-independence signal keys off the validator schema rather than a
    new column: a check whose signer is an enrolled human validator is a human
    check. ``is_enrolled`` walks the chain back to a self-signed root and
    verifies each enrollment envelope, and the envelope binds ``validator_type``
    (see :func:`mareforma.validators.verify_enrollment`), so a row whose type was
    flipped by a direct SQL UPDATE fails the chain and is not treated as human.
    Only an authenticated signer keyid (see :func:`_authentic_signer_keyid`) ever
    reaches here, so an unbacked keyid cannot claim the human axis. Any structural
    failure returns False: one un-resolvable signer must not deny the whole
    proposition's count.
    """
    try:
        from .. import validators as _validators

        if not _validators.is_enrolled(conn, keyid):
            return False
        row = _validators.get_validator(conn, keyid)
        return bool(row and row.get("validator_type") == "human")
    except Exception:
        return False


# The read query behind independence_counts. Kept as a module constant so a
# regression test can pin its EXPLAIN QUERY PLAN (no full scan of
# effect_estimates, the join stays keyed through idx_contrast_line and
# idx_estimate_contrast).
#
# The withdrawal columns are SELECTed, not filtered on: only a live claim
# contributes a line, but the exclusion is disclosed rather than applied in SQL
# (see :func:`_independence_units`).
INDEPENDENCE_COUNTS_SQL = (
    "SELECT el.line_id AS line_id, el.data_id AS data_id, "
    " el.model_lineage AS model_lineage, f.plan_id AS plan_id, "
    " cl.generated_by AS generated_by, "
    " cl.asserter_keyid AS asserter_keyid, cl.claim_id AS claim_id, "
    " cl.signature_bundle AS signature_bundle, "
    " cl.status AS claim_status, cl.t_invalid AS t_invalid, "
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
    "WHERE f.content_id = ?"
)


def _signed_model_lineage(
    conn: sqlite3.Connection,
    claim_id: str,
    bundle_json: "str | None",
) -> "dict | None":
    """The model lineage the claim's SIGNED envelope binds, or None.

    A finding binds its model lineage into the signed observed record
    (``finding/v2``), so the read path can re-authenticate the denormalized
    ``evidence_lines.model_lineage`` column against material the signer covered.
    Returns the signed lineage dict only when the bundle (a) is a claim envelope
    that (b) binds to this ``claim_id``, (c) carries a model lineage on its
    observed record, and (d) is signed by an enrolled validator whose signature
    verifies. A non-enrolled signer (whose bundle cannot be verified), a bad
    signature, a v1 finding (no signed lineage), an unsigned claim, or any
    structural failure returns None, the caller then treats the line as soft,
    never a fabricated distinct model. Never raises: one un-authenticatable line
    must not deny the whole proposition's count.
    """
    if not bundle_json:
        return None
    try:
        from .. import signing as _signing
        from .. import validators as _validators

        env = json.loads(bundle_json)
        pred = _signing.claim_predicate_from_envelope(env)
        if pred.get("claim_id") != claim_id:
            return None
        grounding = pred.get("observed_grounding")
        if not isinstance(grounding, dict):
            return None
        lineage = grounding.get("model_lineage")
        if not isinstance(lineage, dict):
            return None
        keyid = env["signatures"][0]["keyid"]
        signer_row = _validators.get_validator(conn, keyid)
        if signer_row is None:
            # Fail closed: a non-enrolled signer's bundle cannot be verified, so
            # its lineage is unauthenticated. Trusting it lets a producer sign a
            # fabricated distinct model with a throwaway key and inflate the
            # count (the read-side parallel of the forged-column defence). An
            # unauthenticatable line reads soft, never a counted model.
            return None
        pem = base64.standard_b64decode(signer_row["pubkey_pem"])
        pub = _signing.public_key_from_pem(pem)
        if not _signing.verify_envelope(env, pub):
            return None
        return lineage
    except Exception:
        return None


def _authentic_model_key(
    conn: sqlite3.Connection,
    claim_id: str,
    raw_column: "str | None",
    bundle_json: "str | None",
) -> tuple:
    """The independence model key for a line, read from SIGNED material.

    The ``evidence_lines.model_lineage`` column is denormalized and unsigned, so
    a direct/foreign writer can rewrite it to a fabricated distinct COMPUTED root,
    or erase it, to inflate independence. We therefore key on the SIGNED lineage
    the claim's envelope binds, never the raw column:

    - a claim carrying an authenticated signed lineage keys on that signed copy
      whatever the column says, so neither a forged column nor a stripped one can
      move the count (erasing it would otherwise read as "no model call", which
      the signer axis counts per signer);
    - a NULL column with no signed lineage made no model claim → ``("absent",)``
      (the legacy signer axis still applies; a human line is re-keyed upstream);
    - a present column with no authenticatable signed lineage (a v1 finding, an
      unsigned claim, or a bundle that does not verify) reads ``("soft",)``, a
      distinct model that cannot be certified, never counted.

    This is the model-axis parallel of :func:`_authentic_signer_keyid`.
    """
    # Lazy import: ``mareforma.observe`` imports ``trust._store`` (for
    # ``is_content_addressed``), so importing the lineage helper at module top
    # would close a cycle. By call time both modules are fully loaded.
    from mareforma.observe._lineage import independence_model_key

    signed = _signed_model_lineage(conn, claim_id, bundle_json)
    if signed is None:
        return ("absent",) if raw_column is None else ("soft",)
    return independence_model_key(signed)


class _UngateablePlan(Exception):
    """A line's plan carries a rule no gate can run, and nothing supersedes it.

    Internal to the read path: it separates the one drop an operator can repair
    (``retire_plan``) from drift and corruption, which no API fixes.
    """


def _gateable_prediction(conn: sqlite3.Connection, row) -> Prediction:
    """The rule a stored line is gated under, resolving a retired plan.

    Normally the line's own plan. When that plan's stored rule cannot be run,
    the line is gated under the plan that supersedes it, which an operator
    registered explicitly through :meth:`EpistemicGraph.retire_plan`. Resolution
    is reached only from that failure, so a retirement record can only ever
    reach lines that count zero as they stand: it can recover a dropped line,
    never drop a counted one, whatever a direct writer plants in the table.
    """
    try:
        return prediction_from_row(row)
    except ValueError as exc:
        try:
            superseding = superseding_prediction(conn, row["plan_id"])
        except ValueError:
            # The superseding row cannot be run either (only reachable by a
            # direct/foreign write): the line stays dropped, fail closed.
            superseding = None
        if superseding is None:
            raise _UngateablePlan(str(exc)) from exc
        return superseding


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
    inflate the count (see :func:`_authentic_model_key`); a line with no observed
    model call whose signer
    is an enrolled human validator is re-keyed ``("human",)``, the human axis
    the status ladder counts (see :func:`_is_human_signer`). The per-finding map
    disclosure narrows that key back to soft, see :func:`_supporting_units`.

    Two kinds of line are dropped from the count: one that no longer
    reconstructs into a gateable bearing, and one whose claim is no longer live
    (editorially withdrawn, or invalidated by a signed contradiction verdict).
    Dropping is never conservative, a dropped REFUTING line manufactures
    consensus, so no drop is silent. All are tallied under ``skipped`` in
    the ``memo`` (read back as ``lines_skipped``, see
    :func:`proposition_status`) and, when ``disclose`` is given, recorded once
    per line on the ``.mareforma/health.jsonl`` channel as
    ``bearing_recompute_skipped``, ``ungateable_plan_skipped`` (the repairable
    case, whose event names the plan :meth:`EpistemicGraph.retire_plan` takes)
    and ``withdrawn_line_skipped`` (see :class:`SkipDisclosure`).

    ``memo`` is an optional per-read-call cache. The signer/model axis of a line
    (``run_token`` and ``model_key``) is constant per claim, the authenticating
    columns are written identically on every line of a finding, so it is
    computed once per ``claim_id`` and reused, sparing the repeated Ed25519
    verify a multi-line finding, and a frame read that walks the same claim as a
    contrary, would otherwise pay. Only ``direction`` and ``data_id`` vary per
    line. A claim belongs to exactly one proposition, so the cache never
    collides across content_ids sharing a memo. Absent ``memo``, a fresh local
    cache still dedups within the single call.
    """
    per_claim = memo.setdefault("signer", {}) if memo is not None else {}
    skipped = memo.setdefault("skipped", {}) if memo is not None else {}

    def skip(line_id: str, op: str, **detail) -> None:
        skipped[content_id] = skipped.get(content_id, 0) + 1
        if disclose is not None:
            disclose.record(op, content_id, line_id, **detail)

    rows = conn.execute(INDEPENDENCE_COUNTS_SQL, (content_id,)).fetchall()
    for r in rows:
        # A claim the graph no longer treats as live contributes no line, in
        # BOTH directions: promotion applies the same predicate, so promotion and
        # trust counting agree, and query_claims excludes the same rows by
        # default. The exclusion is disclosed rather than applied in the WHERE
        # clause. ``t_invalid`` moves only behind a signed contradiction verdict,
        # but ``status`` is a plain column outside the signed payload that any
        # handle holding the graph may rewrite, so a keyless writer flipping a
        # refutation to contested would otherwise erase it from the count with
        # nothing on the read saying so.
        if r["claim_status"] != "open" or r["t_invalid"] is not None:
            skip(
                r["line_id"], "withdrawn_line_skipped",
                claim_id=r["claim_id"], claim_status=r["claim_status"],
                invalidated=r["t_invalid"] is not None,
            )
            continue
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
        # write bug. The skip is counted and logged: a dropped REFUTING line reads
        # as consensus, so absence of evidence must never look like absence of a
        # problem.
        #
        # A plan whose own rule cannot be run is the one case an operator can
        # repair, so it is resolved through its retirement (the plan that
        # supersedes it) before the line is dropped, and its skip names the plan
        # rather than the generic recompute failure: the plan_id is what
        # retire_plan takes. Resolution is bounded to exactly these rows, which
        # count zero as they stand, so a retirement can never drop a line that
        # counts, only recover one that does not.
        try:
            estimate = estimate_from_row(r)
            prediction = _gateable_prediction(conn, r)
            direction = compute_bearing(estimate, prediction).direction
        except _UngateablePlan as exc:
            skip(
                r["line_id"], "ungateable_plan_skipped",
                plan_id=r["plan_id"], error=type(exc.__cause__).__name__,
            )
            continue
        except Exception as exc:
            skip(
                r["line_id"], "bearing_recompute_skipped",
                error=type(exc).__name__,
            )
            continue
        claim_id = r["claim_id"]
        cached = per_claim.get(claim_id)
        if cached is None:
            keyid = _authentic_signer_keyid(
                conn, claim_id, r["asserter_keyid"], r["signature_bundle"],
            )
            run_token = (
                f"k:{keyid}" if keyid is not None else f"g:{r['generated_by']}"
            )
            model_key = _authentic_model_key(
                conn, claim_id, r["model_lineage"], r["signature_bundle"],
            )
            # A line with no observed model call, signed by an enrolled human
            # validator, keys to the human axis for the legacy status ladder,
            # which needs no distinct model. A line that DID observe a model call
            # keeps its model key even under a human signer, the check was the
            # model's, and the human only signed it, so the model-distinct axis
            # still governs. The per-finding map disclosure narrows this key back
            # to soft (see :func:`_supporting_units`): validator_type is
            # self-declared and defaulted, so it certifies nothing.
            if model_key[0] == "absent" and keyid is not None and _is_human_signer(
                conn, keyid
            ):
                model_key = ("human",)
            cached = (run_token, model_key)
            per_claim[claim_id] = cached
        run_token, model_key = cached
        yield direction, run_token, r["data_id"], model_key


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
    (:func:`_authentic_signer_keyid`), so this axis is not the unsigned column
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


def effective_independence(conn: sqlite3.Connection, content_id: str) -> dict:
    """The effective-independence number for a proposition, with a soft flag.

    ``number`` is the count of pairwise-distinct (model, data, signer) SUPPORTING
    checks over observed model calls. ``soft`` is True when a supporting line
    carries PROXY / UNVERIFIABLE model lineage or no observed model call at all,
    including one signed by a self-declared human validator: the count then rests
    on lineage that cannot certify a distinct model, which the trust map surfaces
    as UNVERIFIABLE rather than a confident number. The legacy status ladder
    (:func:`independence_counts`) is unchanged and still counts a human check on
    its own axis; this narrows only the per-finding disclosure.

    Coarse by design: distinct-model is binary this release. The graded
    cross-model residual (how *far apart* two distinct models are) is DEFERRED , 
    named here, not computed.
    """
    supports, soft = _supporting_units(conn, content_id)
    return {"number": _count_run_distinct(supports), "soft": soft}


def _supporting_units(
    conn: sqlite3.Connection, content_id: str
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
    """
    supports: list[tuple[str, str, tuple]] = []
    soft = False
    for direction, run_token, data_id, model_key in _independence_units(
        conn, content_id
    ):
        if direction is BearingDirection.SUPPORTS:
            if model_key[0] in ("absent", "human"):
                model_key = ("soft",)
            supports.append((run_token, data_id, model_key))
            if model_key[0] == "soft":
                soft = True
    return supports, soft


def effective_independence_receipt(
    conn: sqlite3.Connection, content_id: str
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
    """
    supports, soft = _supporting_units(conn, content_id)
    number = _count_run_distinct(supports)
    hard = [(run, data_id, mk) for run, data_id, mk in supports if mk[0] != "soft"]
    naive = _count_run_distinct(
        [(run, data_id, ("absent",)) for run, data_id, _mk in hard]
    )
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
