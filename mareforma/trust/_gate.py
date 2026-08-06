"""_gate.py: the one boundary that owns verified trust-gate inputs.

A trust gate decides a proposition's status from database rows, and most of
those rows carry unsigned columns a process with SQL access can rewrite. The
verification that turns a raw row into a countable line, its claim is live, its
bearing still reconstructs, its signer and model are read from signed material,
lived inlined on the read path and, separately, on restore. Two copies of one
rule drift; that drift is what four rounds of review kept finding.

This module is the single place that rule lives. A caller obtains gate inputs
only through :func:`verified_gate_inputs` (the live read path) or
:func:`verify_gate_inputs_or_refuse` (restore). Both run the one verifier,
:func:`_derive_units`; they differ only in what they do with a line that does
not verify. The read path drops it and discloses the drop; restore refuses the
whole recovery. That is the "two entry points into one verifier" the design
requires: the disposition is chosen by which entry point was called, never by a
boolean flag threaded through a shared function (a boolean is how the two copies
drifted apart in the first place).

The verified rows are handed back inside a :class:`GateInputs`, a frozen record
with no public constructor that accepts a raw ``sqlite3.Row``. The only way to
build one is the two functions above, so a future edit cannot reintroduce a
skipped check by passing unverified data straight into the counter.
"""
from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from typing import Callable

from .bearing import BearingDirection, compute_bearing
from ._store import (
    INDEPENDENCE_COUNTS_SQL,
    _UngateablePlan,
    _gateable_prediction,
    compute_plan_id,
    estimate_from_row,
    plan_retirement,
)

# Skip reasons. A line the verifier cannot count is tagged with one of these.
# ``BEARING_RECOMPUTE``, ``PLAN_REBIND`` and ``PLAN_RULE_REBIND`` are the
# corruption cases: a row whose stored estimate and prediction no longer
# reconstruct into a gateable bearing, a finding whose ``plan_id`` column no
# longer matches the plan its own claim records, and a ``predictions`` row whose
# rule columns no longer hash to the ``plan_id`` keying them. The other two are
# legitimate lifecycle states, a claim editorially withdrawn or invalidated, and
# a plan whose rule no gate can run and nothing supersedes. The restore entry
# point refuses only on corruption: a valid backup can legitimately carry a
# retracted claim or a stranded plan, and refusing those would cost the operator
# a recovery over honest state.
_WITHDRAWN = "withdrawn_line_skipped"
_UNGATEABLE = "ungateable_plan_skipped"
_BEARING_RECOMPUTE = "bearing_recompute_skipped"
_PLAN_REBIND = "plan_rebind_skipped"
_PLAN_RULE_REBIND = "plan_rule_rebind_skipped"
_CORRUPTION_REASONS = frozenset(
    {_BEARING_RECOMPUTE, _PLAN_REBIND, _PLAN_RULE_REBIND}
)


class GateInputRefused(Exception):
    """A gate-input line did not verify and the caller refuses to proceed.

    Raised only by :func:`verify_gate_inputs_or_refuse` (the restore entry
    point). The read path never raises: it drops the line and discloses it.
    """


class GateCache:
    """The per-derivation verify cache (design section 7).

    Keyed on ``(table, row_id, digest)`` so an entry is bound to the exact row
    bytes it was computed from: a later edit to the row changes the digest and
    misses the entry rather than being served a stale answer. It carries
    positive results only. A verification that fails is not recorded, so an
    enrolment or a signature that lands after a miss is re-derived on the next
    call rather than pinned to an "absent" that outlives the state describing it
    (the caching defect where a stored "not enrolled" served a later forgery).

    Scoped to one derivation call. A read shares a single instance across a
    frame so each claim's signature verifies once; it is never persisted past
    the rows it was built for. Callers hold a ``GateCache``, not a bare dict, so
    the keying and the no-negative rule cannot be bypassed by reaching for
    ``{}``.
    """

    __slots__ = ("_entries",)

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str, str], object] = {}

    def resolve(
        self, table: str, row_id: str, digest: str, compute: Callable[[], object],
    ) -> object:
        """Return the cached positive value for this row, else compute + store it.

        A ``None`` result is a negative verification and is deliberately not
        cached (see the class docstring). Any other value is a positive
        resolution and is memoized for the rest of the derivation call.
        """
        key = (table, row_id, digest)
        cached = self._entries.get(key)
        if cached is not None:
            return cached
        value = compute()
        if value is not None:
            self._entries[key] = value
        return value


@dataclass(frozen=True)
class _GateLine:
    """One verified evidence line, ready for the independence count.

    ``direction`` is the recomputed per-line bearing, ``run_token`` the distinct
    signer axis (``k:``/``g:`` namespaced), ``data_id`` the distinct-dataset key
    and ``model_key`` the distinct-model/method axis, all read from verified or
    signed material by :func:`_derive_units`. ``post_hoc`` is True when the plan
    this line is gated under was not pre-registered (a one-shot plan or the
    replacement a retirement resolved it to), so a count that rests on it can be
    disclosed as post-hoc rather than passing for a pre-registered gate.
    """

    direction: BearingDirection
    run_token: str
    data_id: str
    model_key: tuple
    post_hoc: bool


@dataclass(frozen=True)
class _SkippedLine:
    """A line the verifier could not count, with the health-channel detail.

    ``op`` is the skip reason (one of the module constants) and doubles as the
    health-event name the read path discloses under. ``detail`` carries the
    per-event fields (claim id, plan id, error type) the disclosure records.
    """

    line_id: str
    op: str
    detail: dict = field(default_factory=dict)


# The token that authorises a GateInputs. It is module-private, so no code
# outside this module can pass it, and the dataclass refuses construction
# without it. That is what makes "you cannot build gate inputs from a raw row"
# a property of the type rather than a convention.
_ISSUED = object()


@dataclass(frozen=True)
class GateInputs:
    """The verified gate inputs for one proposition.

    Carries already-verified lines (:class:`_GateLine`) and the lines that could
    not be verified (:class:`_SkippedLine`), plus the :class:`GateCache` the
    derivation used. It is not publicly constructible: the only ways to obtain
    one are :func:`verified_gate_inputs` and :func:`verify_gate_inputs_or_refuse`,
    both of which run the verifier first. A caller therefore cannot hand a raw
    ``sqlite3.Row`` to the counter by building a ``GateInputs`` around it.
    """

    content_id: str
    units: tuple[_GateLine, ...]
    skipped: tuple[_SkippedLine, ...]
    cache: GateCache
    _token: object

    def __post_init__(self) -> None:
        if self._token is not _ISSUED:
            raise TypeError(
                "GateInputs is not publicly constructible; obtain it from "
                "verified_gate_inputs() or verify_gate_inputs_or_refuse()."
            )


def _bundle_digest(bundle_json: "str | None") -> str:
    """A stable cache key fragment for a claim's signature bundle."""
    if not bundle_json:
        return "0"
    return hashlib.sha256(bundle_json.encode("utf-8")).hexdigest()


def _committed_plan_id(
    payload_json: "str | None", claim_id: str, cache: GateCache,
) -> "str | None":
    """The plan_id the finding's own claim records, or None.

    A finding binds the plan it was filed under into its claim's ``finding/v2``
    record (``predicate_payload``, written since v0.3.10). The read query gates
    on the ``findings.plan_id`` column, which nothing in that record covers, so a
    direct writer can re-point it at another rule to flip a REFUTED line to
    SUPPORTS. Reading the plan_id back from the claim's own record lets the
    caller catch that: the column must match what the finding recorded.

    Returns the recorded plan_id, or None when the claim carries no record or no
    ``plan_id`` in it (a pre-v0.3.10 finding, or an unparseable payload). The
    resolution is a property of the claim, identical across a multi-line
    finding's lines, so it is memoized once per claim in the shared cache.
    """
    digest = _bundle_digest(payload_json)

    def compute() -> "str | None":
        if not payload_json:
            return None
        try:
            payload = json.loads(payload_json)
        except (ValueError, TypeError):
            return None
        pid = payload.get("plan_id") if isinstance(payload, dict) else None
        return pid if isinstance(pid, str) else None

    return cache.resolve("finding_plan", claim_id, digest, compute)  # type: ignore[return-value]


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


def _resolve_signer_and_model(
    conn: sqlite3.Connection, row, cache: GateCache,
) -> tuple[str, tuple]:
    """The (run_token, model_key) for a line's claim, resolved once per claim.

    The signer and model axes are a property of the claim, not the line: the
    authenticating columns are written identically on every line of a finding,
    so the resolution is memoized in the shared cache keyed on the claim and its
    bundle bytes. A frame read that walks the same claim for a proposition and
    again as a contrary therefore verifies the signature once.
    """
    claim_id = row["claim_id"]
    digest = _bundle_digest(row["signature_bundle"])

    def compute() -> tuple[str, tuple]:
        keyid = _authentic_signer_keyid(
            conn, claim_id, row["asserter_keyid"], row["signature_bundle"],
        )
        run_token = (
            f"k:{keyid}" if keyid is not None else f"g:{row['generated_by']}"
        )
        model_key = _authentic_model_key(
            conn, claim_id, row["model_lineage"], row["signature_bundle"],
        )
        # A line with no observed model call, signed by an enrolled human
        # validator, keys to the human axis for the legacy status ladder, which
        # needs no distinct model. A line that DID observe a model call keeps its
        # model key even under a human signer, the check was the model's and the
        # human only signed it, so the model-distinct axis still governs.
        if model_key[0] == "absent" and keyid is not None and _is_human_signer(
            conn, keyid,
        ):
            model_key = ("human",)
        return (run_token, model_key)

    return cache.resolve("claims", claim_id, digest, compute)  # type: ignore[return-value]


def _derive_units(
    conn: sqlite3.Connection, content_id: str, cache: GateCache,
) -> tuple[list[_GateLine], list[_SkippedLine]]:
    """The one verifier: verified lines and skipped lines for a proposition.

    Both entry points call this. Per gated line the bearing is recomputed from
    the stored estimate against the finding's stored prediction (the gate inputs
    are persisted precisely so a reader can recompute and catch drift), and the
    signer and model axes are read from signed material. Two kinds of line are
    dropped: one whose claim is no longer live (editorially withdrawn, or
    invalidated by a signed contradiction verdict), and one that no longer
    reconstructs into a gateable bearing. A plan whose own rule cannot be run is
    resolved through its retirement before the line is dropped, and its skip
    names the plan the retirement takes. No drop is silent: a dropped REFUTING
    line would manufacture consensus, so every skip is returned for the caller's
    disposition.
    """
    units: list[_GateLine] = []
    skipped: list[_SkippedLine] = []
    rows = conn.execute(INDEPENDENCE_COUNTS_SQL, (content_id,)).fetchall()
    for r in rows:
        # A claim the graph no longer treats as live contributes no line, in
        # BOTH directions. ``t_invalid`` moves only behind a signed contradiction
        # verdict, but ``status`` is a plain column outside the signed payload
        # that any handle holding the graph may rewrite, so a keyless writer
        # flipping a refutation to contested would otherwise erase it from the
        # count with nothing on the read saying so.
        if r["claim_status"] != "open" or r["t_invalid"] is not None:
            skipped.append(
                _SkippedLine(
                    r["line_id"], _WITHDRAWN,
                    {
                        "claim_id": r["claim_id"],
                        "claim_status": r["claim_status"],
                        "invalidated": r["t_invalid"] is not None,
                    },
                )
            )
            continue
        # Re-derive the plan the line is gated under. The read query gates on the
        # unsigned ``findings.plan_id`` column, but the finding recorded the plan
        # it was filed under in its own claim, so the column must match that
        # record. A mismatch (a direct writer re-pointed the line at another rule
        # to flip its bearing) or a claim that records no plan is dropped rather
        # than gated on the column: a repointed REFUTING line would otherwise
        # read as consensus.
        committed = _committed_plan_id(r["predicate_payload"], r["claim_id"], cache)
        if committed != r["plan_id"]:
            skipped.append(
                _SkippedLine(
                    r["line_id"], _PLAN_REBIND,
                    {
                        "claim_id": r["claim_id"],
                        "plan_id": r["plan_id"],
                        "committed_plan_id": committed,
                    },
                )
            )
            continue
        # Recompute the per-line bearing from stored inputs. A row that no longer
        # reconstructs into a gateable bearing (drift, corruption, or a
        # direct/foreign writer landing a non-numeric column) is skipped rather
        # than allowed to raise: one un-gateable line must not deny reads for the
        # whole proposition. The catch is broad on purpose: the failure can
        # surface as ValueError (enum / range), TypeError (non-numeric column
        # reaching math.isfinite), or InconsistentEstimateError (the gate).
        try:
            estimate = estimate_from_row(r)
            prediction, superseded = _gateable_prediction(conn, r)
            direction = compute_bearing(estimate, prediction).direction
        except _UngateablePlan as exc:
            skipped.append(
                _SkippedLine(
                    r["line_id"], _UNGATEABLE,
                    {"plan_id": r["plan_id"], "error": type(exc.__cause__).__name__},
                )
            )
            continue
        except Exception as exc:
            skipped.append(
                _SkippedLine(
                    r["line_id"], _BEARING_RECOMPUTE, {"error": type(exc).__name__},
                )
            )
            continue
        # Re-derive the plan_id the gated rule hashes to and confirm it matches
        # the plan_id keying it. A plan_id is content-addressed over its rule
        # (test_type, direction_of_interest, the equivalence margins, alpha,
        # inference_regime; see :func:`compute_plan_id`), so a rewrite of any
        # rule-bearing ``predictions`` column re-points the row at a rule whose
        # hash is a different plan_id. The read query gates on the row's columns,
        # not that binding, so a direct writer could otherwise flip a line's
        # bearing (a SUPPORTS to a REFUTES) by editing the rule in place. This is
        # the predictions-table parallel of the ``findings.plan_id`` re-derivation
        # above: the row the gate runs must be the rule its plan_id names. For a
        # line gated under a retirement's replacement, the binding checked is the
        # replacement's own plan_id; the retirement record (restore-verified
        # against its signed attestation) names it.
        gated_plan_id = r["plan_id"]
        if superseded:
            retirement = plan_retirement(conn, r["plan_id"])
            gated_plan_id = retirement["superseded_by"] if retirement else None
        if gated_plan_id is None or (
            compute_plan_id(content_id, prediction) != gated_plan_id
        ):
            skipped.append(
                _SkippedLine(
                    r["line_id"], _PLAN_RULE_REBIND,
                    {"plan_id": r["plan_id"], "gated_plan_id": gated_plan_id},
                )
            )
            continue
        run_token, model_key = _resolve_signer_and_model(conn, r, cache)
        # The count rests on a post-hoc plan when the line's own plan was not
        # pre-registered (a one-shot plan) or a retirement resolved it to a
        # replacement, whose alpha was chosen with the estimates in view.
        post_hoc = superseded or not r["preregistered"]
        units.append(
            _GateLine(direction, run_token, r["data_id"], model_key, post_hoc)
        )
    return units, skipped


def verified_gate_inputs(
    conn: sqlite3.Connection, content_id: str, *, cache: GateCache,
) -> GateInputs:
    """Verified gate inputs for a proposition, the live read-path entry point.

    Runs the one verifier and hands back every line that verified plus the ones
    that did not. The read path counts the verified lines and discloses the
    skipped ones; a line that cannot be verified is dropped, never counted, and
    never raises. This is the drop-and-disclose disposition; the strict
    disposition is :func:`verify_gate_inputs_or_refuse`.
    """
    units, skipped = _derive_units(conn, content_id, cache)
    return GateInputs(content_id, tuple(units), tuple(skipped), cache, _ISSUED)


def verify_gate_inputs_or_refuse(
    conn: sqlite3.Connection, content_id: str, *, cache: GateCache,
) -> GateInputs:
    """Verified gate inputs for a proposition, the restore entry point.

    Runs the same verifier as :func:`verified_gate_inputs`, then holds restore's
    stricter posture: a line that fails to reconstruct into a gateable bearing is
    corruption, not a lifecycle state, and restore refuses the whole recovery
    rather than committing a graph a later read would silently drop lines from.
    A legitimately withdrawn claim or a stranded (un-runnable, unsuperseded) plan
    is not corruption and is accepted, so an honest backup carrying either still
    restores. Raises :class:`GateInputRefused` on the first corrupt line.
    """
    units, skipped = _derive_units(conn, content_id, cache)
    for s in skipped:
        if s.op not in _CORRUPTION_REASONS:
            continue
        if s.op == _PLAN_REBIND:
            raise GateInputRefused(
                f"proposition {content_id} carries an evidence line whose "
                f"plan_id column ({s.detail.get('plan_id')}) no longer matches "
                f"the plan its own claim records "
                f"({s.detail.get('committed_plan_id')}); the recovered graph "
                "would silently drop it."
            )
        if s.op == _PLAN_RULE_REBIND:
            raise GateInputRefused(
                f"proposition {content_id} carries an evidence line whose "
                f"prediction rule no longer hashes to the plan_id keying it "
                f"(plan {s.detail.get('plan_id')}); a rule-bearing predictions "
                "column was rewritten, and the recovered graph would silently "
                "drop the line."
            )
        raise GateInputRefused(
            f"proposition {content_id} carries an evidence line whose stored "
            f"estimate no longer reconstructs into a gateable bearing "
            f"({s.detail.get('error', 'unknown')}); the recovered graph "
            "would silently drop it."
        )
    return GateInputs(content_id, tuple(units), tuple(skipped), cache, _ISSUED)
