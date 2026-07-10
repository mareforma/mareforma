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
    same-model checks — distinct signer and distinct dataset but the same
    COMPUTED model — are one line of evidence, not two, so they no longer promote
    on the signer + data axes alone.

    A human check is the exception and the highest-value source: it needs no
    distinct model (a human is not a model), so a human run counts per signer and
    is never folded into a model root — a human check plus a model check reads as
    two, where two same-model checks read as one.

    The count is order-free. Each dataset is first attributed to exactly one run
    (the smallest token, a deterministic tie-break), carrying that line's model
    key, so a re-appearing dataset counts once. The surviving datasets are then
    grouped by run — this preserves the "one signer contributes at most one
    unit" cap — and each run's model state is resolved
    (see :func:`_collapse_run_model`). The answer folds the two axes:

    - COMPUTED runs collapse by family root, so two distinct signers on the same
      model count once;
    - absent runs (no observed model call) keep the legacy signer axis, so every
      pre-observer finding is unchanged;
    - soft runs (PROXY / UNVERIFIABLE) are UNVERIFIABLE for independence and add
      no unit, so a soft pair cannot silently corroborate — never a silent pass.

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
    verifies each enrollment envelope — and the envelope binds ``validator_type``
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
# effect_estimates — the join stays keyed through idx_contrast_line and
# idx_estimate_contrast).
INDEPENDENCE_COUNTS_SQL = (
    "SELECT el.data_id AS data_id, el.model_lineage AS model_lineage, "
    " cl.generated_by AS generated_by, "
    " cl.asserter_keyid AS asserter_keyid, cl.claim_id AS claim_id, "
    " cl.signature_bundle AS signature_bundle, "
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


def _line_model_key(raw: "str | None") -> tuple:
    """The independence model key for a stored ``model_lineage`` column value.

    ``raw`` is the JSON string persisted on the evidence line, or ``None`` when
    the finding was authored without an observed model call. A column that no
    longer parses is treated as soft (never a fabricated distinct model).
    """
    # Lazy import: ``mareforma.observe`` imports ``trust._store`` (for
    # ``is_content_addressed``), so importing the lineage helper at module top
    # would close a cycle. By call time both modules are fully loaded.
    from mareforma.observe._lineage import independence_model_key

    if raw is None:
        return ("absent",)
    try:
        lineage = json.loads(raw)
    except (ValueError, TypeError):
        lineage = None
    if not isinstance(lineage, dict):
        return ("soft",)
    return independence_model_key(lineage)


def _independence_units(conn: sqlite3.Connection, content_id: str):
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
    label. The model key carries the distinct-model/method axis
    (see :func:`_line_model_key`); a line with no observed model call whose signer
    is an enrolled human validator is re-keyed ``("human",)`` — the human axis,
    the highest-value independent source (see :func:`_is_human_signer`).
    """
    rows = conn.execute(INDEPENDENCE_COUNTS_SQL, (content_id,)).fetchall()
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
        keyid = _authentic_signer_keyid(
            conn, r["claim_id"], r["asserter_keyid"], r["signature_bundle"],
        )
        run_token = f"k:{keyid}" if keyid is not None else f"g:{r['generated_by']}"
        model_key = _line_model_key(r["model_lineage"])
        # A line with no observed model call, signed by an enrolled human
        # validator, is a human check: the highest-value independent axis, which
        # needs no distinct model. A line that DID observe a model call keeps its
        # model key even under a human signer — the check was the model's, and
        # the human only signed it — so the model-distinct axis still governs.
        if model_key[0] == "absent" and keyid is not None and _is_human_signer(
            conn, keyid
        ):
            model_key = ("human",)
        yield direction, run_token, r["data_id"], model_key


def independence_counts(conn: sqlite3.Connection, content_id: str) -> tuple[int, int]:
    """(independent_support, independent_refute) by distinct signer + model, data guard.

    Counted by distinct **signer** (the claim's ``asserter_keyid``) AND distinct
    **model/method**, with a ``data_id`` guard (see :func:`_count_run_distinct`):
    one signer yields at most one independent support and one independent refute,
    and two same-model checks no longer read as two independent lines. This is
    the same WHO axis the REPLICATED promotion query keys on, read from the same
    denormalised claim column, and the same model axis the promotion gate now
    reads off the evidence line, so promotion and trust counting can never
    disagree. Legacy evidence lines whose claim predates the keyid column (NULL
    ``asserter_keyid``) fall back to the retired ``generated_by`` run axis, and a
    finding with no observed model call carries no model constraint, so both keep
    their count instead of collapsing (status_policy@v3). A no-model-call line
    signed by an enrolled human validator counts on the human axis — the
    highest-value independent source, never folded into a model root. The two run
    axes are namespaced (``k:`` vs ``g:``) so a keyid can never alias a run label.
    """
    supports: list[tuple[str, str, tuple]] = []
    refutes: list[tuple[str, str, tuple]] = []
    for direction, run_token, data_id, model_key in _independence_units(
        conn, content_id
    ):
        unit = (run_token, data_id, model_key)
        if direction is BearingDirection.SUPPORTS:
            supports.append(unit)
        elif direction is BearingDirection.REFUTES:
            refutes.append(unit)
    return _count_run_distinct(supports), _count_run_distinct(refutes)


def effective_independence(conn: sqlite3.Connection, content_id: str) -> dict:
    """The effective-independence number for a proposition, with a soft flag.

    ``number`` is the count of pairwise-distinct (model, data, signer) SUPPORTING
    checks — the same model-aware axis :func:`independence_counts` promotes on, so
    the two never disagree. A supporting check a human authored (no observed model
    call, signed by an enrolled human validator) counts as the highest-value
    independent source: it needs no distinct model, so a human check plus a model
    check reads as two where two same-model checks read as one. ``soft`` is True
    when a supporting line carries PROXY / UNVERIFIABLE model lineage: the count
    then rests on lineage that cannot certify a distinct model, which the trust
    map surfaces as UNVERIFIABLE rather than a confident number.

    Coarse by design: distinct-model is binary this release. The graded
    cross-model residual (how *far apart* two distinct models are) is DEFERRED —
    named here, not computed.
    """
    supports: list[tuple[str, str, tuple]] = []
    soft = False
    for direction, run_token, data_id, model_key in _independence_units(
        conn, content_id
    ):
        if direction is BearingDirection.SUPPORTS:
            supports.append((run_token, data_id, model_key))
            if model_key[0] == "soft":
                soft = True
    return {"number": _count_run_distinct(supports), "soft": soft}


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
