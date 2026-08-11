"""
_graph.py: EpistemicGraph, agent-native interface to the mareforma epistemic graph.

Usage
-----
  graph = mareforma.open()                         # current directory
  graph = mareforma.open("/path/to/project")       # explicit path
  graph = mareforma.open(Path("my_project"))

  with mareforma.open() as graph:                  # context manager
      claim_id = graph.assert_claim("...", classification="ANALYTICAL")
      results  = graph.query("topic X")
      status   = graph.proposition_status(prop)    # the derived answer/question axes
      graph.validate(claim_id, validated_by="reviewer@example.org")

Trust vocabulary
----------------
  Read trust off the two derived axes: ``Status`` per content_id is the state
  of the answer, ``FrameStatus`` / ``question_status`` per frame_id is the
  state of the question, both computed on every read from the graph. The
  stored ``support_level`` column (PRELIMINARY -> REPLICATED -> ESTABLISHED)
  is the legacy promotion ladder; its public labels are deprecated for v0.4.0,
  though ``query(min_support=...)`` still filters on them for this release.

Flow
----
  assert_claim()
    ├─ idempotency check (if key provided)
    ├─ validate classification
    ├─ INSERT via db.add_claim()
    └─ convergence check fires inside add_claim() (writes support_level)

  query()
    └─ SELECT via db.query_claims() with text/support/classification filters

  validate()
    └─ UPDATE via db.validate_claim(): the human-witness promotion gate
"""

from __future__ import annotations

import base64
import functools
import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from mareforma import db as _db
# NOTE: mareforma.signing is imported lazily inside the methods that need
# it (refresh_unsigned, validate, and friends) as a plain local import.
# This does NOT preserve any unsigned-only open path: `import mareforma`
# re-exports from mareforma.signing (see __init__), which imports
# cryptography eagerly, so a broken cryptography extension fails at
# `import mareforma` before any graph can be opened. The lazy import is
# a local-import convenience, not a degraded-mode guarantee.

if TYPE_CHECKING:
    import sqlite3


# Fields that get sanitize-and-wrap for LLM consumption. Free-form text
# the LLM is likely to splice into a reasoning step.
_LLM_WRAP_FIELDS = ("text", "comparison_summary")

# Fields that get sanitize-only, short labels we don't wrap because
# delimiters would add noise without containing anything an attacker
# could realistically use as a multi-line injection payload.
_LLM_SANITIZE_FIELDS = ("source_name", "generated_by", "validated_by")

# The two sets above are the only fields whose treatment is named. Every
# other string in the row is cleaned by the closing pass in
# _format_row_for_llm, so adding a column cannot quietly add an unsanitized
# field to an LLM-bound result.
_LLM_NAMED_FIELDS = frozenset(_LLM_WRAP_FIELDS + _LLM_SANITIZE_FIELDS)

# The run token a claim is attributed to when the caller names none. Every
# path that resolves an absent generated_by uses this one name so a write and
# the checks that read it back cannot drift apart.
DEFAULT_RUN_TOKEN = "agent"

_MIN_SUPPORT_DEPRECATION = (
    "query(min_support=...) is deprecated: the support ladder is retired and "
    "the whole support_level column goes in v0.4.0, filter and all. Read the "
    "trust map's independence axis, or proposition_status(), for how much "
    "distinct backing a finding actually has."
)


def _caller_stacklevel() -> int:
    """The stacklevel that attributes a warning to the first frame outside us.

    A fixed number cannot be right here. ``query`` reaches its caller in four
    frames, but ``query_for_llm`` delegates to ``query``, so the same warning
    needs five to get past the library, and any future public read that
    delegates would need its own count. A wrong count is not cosmetic: Python's
    default filter ignores a DeprecationWarning unless it comes from
    ``__main__``, so an attribution inside mareforma silences the notice for
    every real caller, and it collapses every call site onto one dedup key so
    only the first ever reports. Walking out of the package answers it for
    every path at once. Falls back to 2 if the whole stack is ours, which only
    happens when mareforma calls itself.
    """
    import sys
    from pathlib import Path

    package_dir = str(Path(__file__).resolve().parent)
    frame = sys._getframe(1)
    level = 1
    while frame is not None:
        if not str(Path(frame.f_code.co_filename).resolve()).startswith(package_dir):
            return level
        frame = frame.f_back
        level += 1
    return 2


def _warn_min_support(value) -> None:
    """Warn once per call when a read still filters on the retired ladder.

    The retirement warned only on ``mareforma.REPLICATED``, the module
    attribute, which is not how anyone uses the ladder: callers pass the level
    as a plain string to ``min_support``. So the announcement reached the one
    path nobody takes and stayed silent on the path everybody does, which would
    have made the v0.4.0 removal arrive unannounced for every real caller.
    """
    if value is None:
        return
    from mareforma._deprecation import _emit

    # +1 for _emit's own frame; see its docstring.
    _emit(_MIN_SUPPORT_DEPRECATION, _caller_stacklevel() + 1)


def _model_lineage_of(grounding):
    """The model/method lineage a grounding verdict carries, or None.

    Tolerant of a verdict without the field (a hand-built or pre-observer
    verdict) and of a plain None, so the lineage thread never raises on an
    absent or legacy verdict.
    """
    if grounding is None:
        return None
    return getattr(grounding, "model_lineage", None)


def _format_row_for_llm(row: dict, prompt_safety) -> dict:
    """Apply prompt-safety sanitization to a claim row. Pure function;
    the ``prompt_safety`` module is passed in to keep the import lazy
    on the hot path of plain ``query``."""
    out = dict(row)
    for field in _LLM_WRAP_FIELDS:
        if field in out and out[field] is not None:
            sanitized = prompt_safety.sanitize_for_llm(out[field])
            out[field] = prompt_safety.wrap_untrusted(sanitized)
    for field in _LLM_SANITIZE_FIELDS:
        if field in out:
            # Strip forged delimiters here too, not just hostile codepoints.
            # These fields skip the *wrapper* because a short label gains
            # nothing from delimiters, which is a different question from
            # whether the label may carry one. A run token reading
            # ``</untrusted_data>`` is serialised into the same object as the
            # wrapped text and closes a delimiter it was never given, which is
            # the whole breakout this layer exists to stop. Every other string
            # in the row is already stripped by the closing pass below; these
            # three were the only ones exempt.
            out[field] = prompt_safety.strip_forged_tags(
                prompt_safety.sanitize_for_llm(out[field])
            )
    # Close the set: the remaining columns carry caller-supplied prose too
    # (evidence rationales, adapter predicate payloads, grounding reasons),
    # so strip hostile codepoints and forged delimiters from every string
    # left. No wrapper on these: several are JSON the caller parses.
    for field, value in out.items():
        if field not in _LLM_NAMED_FIELDS and isinstance(value, str):
            out[field] = prompt_safety.strip_forged_tags(
                prompt_safety.sanitize_for_llm(value)
            )
    return out


def _synchronized(method):
    """Serialize a graph call under the graph's re-entrant lock.

    The connection is opened with ``check_same_thread=False``, so one graph may
    be driven from several threads. Transaction ownership in the db layer is
    decided by ``not conn.in_transaction``, a connection-wide property that
    cannot tell "this thread is nested in its own BEGIN" from "another thread
    holds a transaction on the shared connection." A second thread would read a
    first thread's open ``BEGIN IMMEDIATE`` as its own, skip its own
    transaction, and silently join (and, on the first thread's rollback, lose)
    its write.

    Wrapping every mutating method with this decorator makes at most one thread
    a writer at a time, so ``not conn.in_transaction`` becomes a thread-correct
    ownership test. The lock is an ``RLock`` so the existing nested-call pattern
    (``submit_finding`` calling ``assert_claim`` inside one transaction, on the
    same thread) re-enters instead of deadlocking.

    Reads take the same lock. sqlite3 isolation is per CONNECTION, not per
    thread, so a read issued while another thread holds an open
    ``BEGIN IMMEDIATE`` on the shared connection runs inside that transaction
    and returns rows the writer's rollback then erases. The cost is that a
    reader waits for the writer ahead of it, including the Rekor round trip
    ``submit_finding`` holds inside its transaction. Waiting is the lesser
    harm: handing a caller a claim_id, a support level, or a trust map for
    state that never lands is the failure this project exists to catch.
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return wrapper


class EpistemicGraph:
    """Agent-native interface to a local mareforma epistemic graph.

    Do not instantiate directly: use mareforma.open().
    """

    def __init__(
        self,
        conn: "sqlite3.Connection",
        root: Path,
        *,
        signer: object | None = None,
        rekor_url: str | None = None,
        require_rekor: bool = False,
        trust_insecure_rekor: bool = False,
        rekor_log_pubkey_pem: bytes | None = None,
        strict_promotion: bool = False,
        validator_type: str = "human",
    ) -> None:
        self._conn = conn
        self._root = root
        # The health-channel side of the read-path skipped-line disclosure.
        # One per handle: a dropped line is a state, so it is recorded once,
        # not once per read (see trust._store.SkipDisclosure).
        from mareforma.trust import _store as _trust_store
        self._skips = _trust_store.SkipDisclosure(root)
        # Re-entrant lock serializing graph mutations across threads. See
        # _synchronized: the connection is shareable across threads, so writers
        # must not race on transaction ownership.
        self._lock = threading.RLock()
        self._signer = signer
        self._rekor_url = rekor_url
        self._require_rekor = require_rekor
        # The session opt-in for a private Rekor on a non-public address.
        # mareforma.open() validates the URL once with this flag; every
        # submit and fetch re-validates, so the flag has to travel with
        # the URL or those re-validations reject what open() accepted.
        self._trust_insecure_rekor = trust_insecure_rekor
        # Opt-in gate: require data on both sides of a REPLICATED pair. Off by
        # default; threaded into every write path that can trigger promotion.
        # Asking for it also declares it on the project (see the end of
        # __init__), so this handle's copy of the flag only ever agrees with
        # the stored policy the write paths read.
        self._strict_promotion = strict_promotion
        # Rekor log operator's public key, used to verify the signed
        # checkpoint that anchors each inclusion proof. When None,
        # mareforma trusts only the submit-time response binding (OUR
        # hash + OUR signature inside the returned entry); the residual
        # gap is the "trust the log operator's submit-time response"
        # posture documented in README "Limits of the Rekor integration".
        # When supplied, every signed-claim submit and every restore
        # cross-verifies the log's signed Merkle root.
        self._rekor_log_pubkey_pem = rekor_log_pubkey_pem
        self._closed = False
        # Convergence detection swallows SQLite errors so a misconfigured
        # trigger or contention pattern cannot crash a write. A WARNING is
        # logged each time, but operators not watching logs would never know
        # promotions stopped firing. Track the count here so it can be
        # asserted in tests and surfaced in dashboards.
        self._convergence_errors = 0
        # Rows a read dropped because their signature did not re-verify. The
        # enumerating surfaces cannot return them, so without this counter a
        # tampered graph reads as a graph with fewer claims.
        self._read_verify_exclusions = 0
        self._read_unverified_exclusions = 0
        # Whether any disclosure count stopped at its scan ceiling, so a reader
        # knows the total is a floor rather than an exact number.
        self._read_unverified_saturated = False
        # Per-kind occurrence counts behind the health-log rate limit. Not the
        # row totals: those are the numbers a reader wants, these only decide
        # when a line is worth writing.
        self._health_append_counts: dict[str, int] = {}
        # The running total at the last line written for each kind, so a spike
        # between two ordinary reads is not lost between powers of two.
        self._health_append_totals: dict[str, int] = {}
        # The grounding record the finding path has already attested, held for
        # the one nested assert_claim call it makes. See _attest_grounding.
        self._attested_grounding: "dict | None" = None

        # Bootstrap-of-trust: the first key opened against a fresh project's
        # graph.db auto-enrolls as the root validator. This is silent and
        # idempotent, subsequent opens with the same key are no-ops. New
        # validators (beyond the root) are added explicitly via the
        # `mareforma validator add` CLI or validators.enroll_validator().
        #
        # If a different key has already enrolled as root (the user
        # opened the project with the wrong key, or two simultaneous
        # bootstraps and this one lost the race), auto_enroll_root
        # silently returns None and the loaded signer is NOT enrolled.
        # Surface that immediately so the operator notices before any
        # validate() call fails with a less obvious error.
        if signer is not None:
            from mareforma import signing as _signing
            from mareforma import validators as _validators
            _validators.auto_enroll_root(
                self._conn,
                signer,
                identity="root",
                validator_type=validator_type,
                root=self._root,
            )
            keyid = _signing.public_key_id(signer.public_key())
            if not _validators.is_enrolled(self._conn, keyid):
                import warnings as _warnings
                prefix = (
                    f"Opened project with key {keyid[:12]}… but this key is "
                    "not an enrolled validator"
                )
                roots = _validators.enrollment_roots(self._conn)
                if len(roots) == 1:
                    msg = (
                        f"{prefix} (a different key holds the root). "
                        "graph.validate() will refuse until this key is "
                        "enrolled by an existing validator via "
                        "`mareforma validator add`."
                    )
                else:
                    # Without a single root no chain verifies, for any key.
                    msg = (
                        f"{prefix}: the validators table carries {len(roots)} "
                        "self-signed roots, so no enrollment chain verifies. "
                        "graph.validate() will refuse for every key until the "
                        "table is repaired; run `mareforma validator list`."
                    )
                _warnings.warn(msg, stacklevel=2)

        # strict_promotion governs a state transition applied to rows other
        # sessions write, so it is a project rule and is recorded as one: the
        # root signs a one-way policy every later opener reads. A caller who
        # cannot make that declaration is refused here rather than handed a
        # gate that only holds while their own handle is doing the writing.
        if strict_promotion:
            self._declare_project_policy(
                "the strict-promotion policy", strict_promotion_required=True,
            )

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    @_synchronized
    def assert_claim(
        self,
        text: str,
        *,
        classification: str = "INFERRED",
        supports: list[str] | None = None,
        contradicts: list[str] | None = None,
        idempotency_key: str | None = None,
        generated_by: str | None = None,
        source_name: str | None = None,
        status: str = "open",
        artifact_hash: str | None = None,
        evidence: "dict | None" = None,
        seed: bool = False,
        signer: "object | None" = None,
        predicate_payload: dict | None = None,
        original_signature_bundle: str | None = None,
        grounding_sensor: "object | None" = None,
        observed_grounding: dict | None = None,
        finding_record: dict | None = None,
    ) -> str:
        # signer:
        #     Per-call override for the graph's loaded signer. When
        #     ``None`` (default), the call inherits the signer passed
        #     to ``mareforma.open(key_path=...)``. When supplied, the
        #     claim is signed with this key instead. Note: this does
        #     NOT check that the signer's keyid is enrolled in the
        #     validators table, same trust model as
        #     ``mareforma.open(key_path=...)`` (anyone can sign, but
        #     only enrolled keys can ``validate()`` claims to
        #     ESTABLISHED). Use for multi-signer hosts that have
        #     multiple keys loaded (e.g. one per role-actor in the
        #     ``claim-with-roles:v1`` predicate variant).
        # predicate_payload:
        #     Optional structured predicate body for adapters that
        #     ship a typed predicateType (tool-call/v1,
        #     ingested-trace/v1, wet-lab-assay/<class>/v1, etc.).
        #     Stored in the ``predicate_payload`` column for
        #     queryable filters. NOTE: this column is NOT bound into
        #     the signed envelope or chain hash, it is a query-side
        #     denormalisation only. Adapters that depend on
        #     cryptographic integrity of the predicate body should
        #     encode it inside the claim text JSON; this column is
        #     the queryable index, not the source of truth.
        # original_signature_bundle:
        #     Optional source-side DSSE envelope, preserved by
        #     federation-import flows. The active ``signature_bundle``
        #     carries the receiver's re-signed envelope; this column
        #     holds the original for downstream verifiers that want
        #     to reconstruct the source-side proof. NOTE: mareforma
        #     does NOT validate this string at write time (only that
        #     it parses as JSON for normalisation). Pass a structurally
        #     valid DSSE envelope JSON or leave None.
        """Assert a claim into the epistemic graph. Returns claim_id.

        Parameters
        ----------
        text:
            The claim text. Cannot be empty.
        classification:
            'INFERRED' (default) | 'ANALYTICAL' | 'DERIVED'
        supports:
            List of claim_ids or DOIs this claim is grounded in.
        contradicts:
            List of claim_ids or DOIs this claim contests.
        idempotency_key:
            Stable key for retry-safe writes. Same key returns the same
            ``claim_id`` only when EVERY semantic field also matches
            (text, classification, generated_by, supports, contradicts,
            source_name, artifact_hash, evidence, observed_grounding).
            Any mismatch raises
            :class:`mareforma.db.IdempotencyConflictError`. Silently
            merging two different claims would discard the second
            author's content and break REPLICATED detection. For
            cross-lab convergence, assert two separate claims that
            share an ``ESTABLISHED`` entry in ``supports[]`` and are
            signed by two distinct keys (distinct ``asserter_keyid``):
            that's the path that fires REPLICATED honestly. Pass a
            per-call ``signer`` for each distinct asserter.
        generated_by:
            Agent identifier. Use ``"model/version/context"`` format.
            Defaults to ``'agent'``. A display label only: it does not
            decide REPLICATED convergence (the ``asserter_keyid`` from
            the signature does).
        source_name:
            Data source this claim derives from. Required for ANALYTICAL
            classification to be meaningful.
        status:
            Editorial status at insert time: 'open' (default) | 'contested'
            | 'retracted'. Use 'contested' to flag a dispute at assertion
            time instead of asserting 'open' then updating. Status is not
            part of the signed payload and remains mutable via
            ``update_claim`` even on signed rows.
        artifact_hash:
            SHA256 hex digest of the output artifact (figure, CSV, model)
            backing this claim. When supplied it is bound into the signed
            payload and used as a secondary collapse on REPLICATED: two
            peers citing the same upstream that BOTH supply an EQUAL hash
            are the same output, so they collapse to one line and do not
            converge on their own. Distinct hashes, or an absent hash on
            either side, do not block the distinct-signer convergence.
            Compute with ``hashlib.sha256(bytes).hexdigest()``.
        evidence:
            Optional evidence-vector dict declaring the asserter's
            confidence in the evidence backing this claim. Five downgrade
            domains in ``[-2, 0]`` (``risk_of_bias``, ``inconsistency``,
            ``indirectness``, ``imprecision``, ``publication_bias``),
            three upgrade flags (``large_effect``, ``dose_response``,
            ``opposing_confounding``), a ``rationale`` dict, and a
            ``reporting_compliance`` list. Bound into the signed
            predicate and denormalized into the ``ev_*`` columns for
            queryable filters. Defaults to all-zeros (the asserter
            flagged no quality concerns).
        grounding_sensor:
            Optional sensor scoring the claim text against its cited
            supports. Its result is a declaration: ``grounding_score``
            and ``grounding_rationale`` are folded into the signed
            evidence vector, the same posture as the rest of it. The
            sensor never writes the observed axis, so a self-declared
            score can never be read as a computed verdict.
        observed_grounding:
            Observed-grounding record computed by ``observe()`` or by
            ``submit_finding``, as ``obs.verdict.to_signed_dict()``. Bound
            into the signed statement and the chain hash and stored in the
            queryable ``observed_grounding`` column. ``UNGROUNDED`` or
            ``OPAQUE`` blocks promotion; absent is read as no verdict
            recorded and blocks nothing.
            The axis is written from what the observer computed, not from
            this argument: the record is looked up by its receipt digest and
            the OBSERVER'S copy is what gets signed, so an edited state on a
            real digest is discarded. A record the observer did not produce
            in this process is stored and reported as ``DECLARED`` and can
            never occupy ``GROUNDED``, so a hand-built verdict cannot read as
            a computed one on any surface.
        finding_record:
            The signed record of a finding's verdict inputs (content_id,
            frame_id, plan_id, data_ids, bearing, and the estimates
            digest), passed by ``submit_finding``. Bound into the signed
            statement and the chain hash only when present, so a plain
            claim signs to byte-identical bytes. A verdict re-derives
            against this copy on read; absent means the claim is not a
            finding, or predates the record.

        Returns
        -------
        str
            The UUID claim_id.

        Raises
        ------
        ValueError
            If ``classification`` is not a valid value, ``text`` is empty,
            or ``artifact_hash`` is not a 64-character lowercase hex SHA256.
        mareforma.db.IdempotencyConflictError
            If ``idempotency_key`` is set and any semantic field differs
            from the existing row.
        """
        self._check_open()
        # Sign-after-author invariant: a claim must be authored inside a
        # grounding scope and signed AFTER it closes. Asserting while the scope
        # that grounds it is still open would sign a verdict computed from an
        # incomplete observation, so it is refused rather than silently signed.
        from mareforma import observe as _observe
        if _observe.scope_is_open():
            raise RuntimeError(
                "cannot assert a claim while a grounding observe() scope is "
                "open: close the scope first, then assert with the computed "
                "grounding verdict. Signing inside an open scope would bind a "
                "verdict from a partial observation."
            )
        # DOIs in supports/contradicts are accepted as reference identifiers;
        # they are no longer network-resolved, so a fresh claim is never
        # quarantined as unresolved at assertion time.
        unresolved = False

        # Evidence is an optional plain dict bound into the signed predicate.
        # ``None`` defers to the default all-zeros vector applied in add_claim.
        if evidence is not None and not isinstance(evidence, dict):
            raise TypeError(
                f"evidence must be a dict or None; got {type(evidence).__name__}"
            )
        ev = dict(evidence) if evidence else None

        # Snapshot the grounding sensor's verdict into the evidence vector
        # so the score is signed alongside the rest of the claim. A
        # broken sensor (any Exception subclass: bad shape, model
        # failure, OSError, KeyError, IndexError, network error, etc.)
        # does NOT block assertion, we log a warning and drop the
        # score. BaseException-only failures (KeyboardInterrupt /
        # SystemExit / MemoryError) propagate so signal-driven
        # shutdown still works. Asserter philosophy: mareforma
        # signs what the asserter claims; verifier wiring is a
        # quality hint, not a gate.
        #
        # SECURITY: the verifier sees the full claim text and the
        # supports list. A verifier backed by a remote API (LLM
        # provider, HuggingFace Inference, etc.) will transmit
        # claim content to that endpoint. Callers handling
        # privacy-sensitive content should wire local verifiers
        # only.
        #
        # The supports list is passed as an immutable tuple so a
        # hostile or buggy verifier cannot mutate the asserter's
        # citation list before the predicate is signed.
        if grounding_sensor is not None:
            import warnings as _warnings
            try:
                score, rationale = grounding_sensor.grounding_score(
                    text, tuple(supports or ()),
                )
                if not isinstance(rationale, str):
                    raise TypeError(
                        "grounding_sensor rationale must be a str; got "
                        f"{type(rationale).__name__}"
                    )
                # Validate the score before it is signed: a bad score
                # (bool, non-numeric, NaN, or out of [0, 1]) raises here and
                # falls through to the warning path, so a broken sensor never
                # binds a nonsense grounding verdict into the predicate.
                if isinstance(score, bool) or not isinstance(score, (int, float)):
                    raise TypeError(
                        "grounding_sensor score must be a float; got "
                        f"{type(score).__name__}"
                    )
                score_f = float(score)
                if score_f != score_f:  # NaN
                    raise ValueError("grounding_sensor score must not be NaN")
                if score_f < 0.0 or score_f > 1.0:
                    raise ValueError(
                        f"grounding_sensor score {score_f} out of [0.0, 1.0]"
                    )
                ev = {
                    **(ev or {}),
                    "grounding_score": score_f,
                    "grounding_rationale": rationale,
                }
                from mareforma import health as _health
                _health.append_health_event(
                    self._root, "grounding_verdict",
                    score=score_f,
                )
            except Exception as exc:
                _warnings.warn(
                    f"grounding_sensor raised {type(exc).__name__}: "
                    f"{exc}; asserting without grounding_score.",
                    RuntimeWarning,
                    stacklevel=2,
                )
                # Emit a failure event so rolling stats can compute
                # availability = ok / (ok + fail) alongside pass_rate;
                # otherwise a flaky sensor with 100% pass-when-running
                # but 50% success reports as 100% pass_rate and the
                # operator never sees the unreliability.
                from mareforma import health as _health
                _health.append_health_event(
                    self._root, "grounding_verdict",
                    outcome="fail",
                    error=type(exc).__name__,
                )

        def _bump_convergence_errors(_exc: Exception) -> None:
            self._convergence_errors += 1

        from mareforma.observe._binding import predicate_citation_sources

        # A claim asserted directly carries nothing to bind a verdict against:
        # source_name is free text and no data_id or data_source lands in a
        # bindable field, so the verdict-to-citation check the finding path runs
        # is inapplicable here. Annotate the record with the same not-applicable
        # marker so an unbound verdict is distinguishable on read from one that
        # passed the check. assert_finding binds first and hands down a predicate
        # carrying the citation, so a bound verdict is left alone and the marker
        # is never appended twice. A non-dict verdict is left untouched so
        # add_claim raises its own TypeError on it.
        #
        # Settle provenance before that: the observed axis is written from what
        # the observer computed, never from the caller's dict. The finding path
        # attested its verdict before it bound it, and binding can strip the
        # receipt digest this keys on, so the record it hands down is passed
        # through by identity rather than attested a second time (which would
        # read the observer's own verdict back as a declaration).
        if (
            isinstance(observed_grounding, dict)
            and observed_grounding is not self._attested_grounding
        ):
            observed_grounding = self._attest_grounding(
                observed_grounding, observed_grounding,
            )
        if isinstance(observed_grounding, dict) and not (
            predicate_citation_sources(predicate_payload)
        ):
            observed_grounding = self._annotate_unbound(observed_grounding)

        return _db.add_claim(
            self._conn,
            self._root,
            text,
            classification=classification,
            supports=supports,
            contradicts=contradicts,
            idempotency_key=idempotency_key,
            generated_by=generated_by or DEFAULT_RUN_TOKEN,
            source_name=source_name,
            status=status,
            unresolved=unresolved,
            artifact_hash=artifact_hash,
            evidence=ev,
            seed=seed,
            signer=signer if signer is not None else self._signer,
            rekor_url=self._rekor_url,
            require_rekor=self._require_rekor,
            trust_insecure_rekor=self._trust_insecure_rekor,
            on_convergence_error=_bump_convergence_errors,
            rekor_log_pubkey_pem=self._rekor_log_pubkey_pem,
            predicate_payload=predicate_payload,
            original_signature_bundle=original_signature_bundle,
            observed_grounding=observed_grounding,
            finding_record=finding_record,
            strict_promotion=self._strict_promotion,
        )

    @_synchronized
    def query(
        self,
        text: str | None = None,
        *,
        min_support: str | None = None,
        classification: str | None = None,
        limit: int = 20,
        include_unverified: bool = False,
        include_invalidated: bool = False,
        refutation_filter: str | None = None,
    ) -> list[dict]:
        """Query claims from the epistemic graph.

        Returns claim dicts with the raw ``text`` field. **If the
        caller plans to splice these into an LLM prompt context,
        use** :meth:`query_for_llm` **instead**: it wraps the text in
        ``<untrusted_data>...</untrusted_data>`` markers so the LLM
        treats retrieved content as data, not instructions
        (Greshake et al., AISec '23, arXiv:2302.12173). This method
        returns bytes verbatim; the burden of escape is on the
        caller.

        Parameters
        ----------
        text:
            Optional substring filter on claim text (case-insensitive).
        min_support:
            Minimum support level: 'PRELIMINARY' | 'REPLICATED' | 'ESTABLISHED'.
        classification:
            Filter by classification: 'INFERRED' | 'ANALYTICAL' | 'DERIVED'.
        limit:
            Maximum number of results. Default 20.
        include_unverified:
            When ``False`` (default), PRELIMINARY claims whose signing key
            is not enrolled in the project's ``validators`` table are
            excluded. Pass ``True`` to surface unverified preliminary
            claims (e.g. inspection of pending work). REPLICATED and
            ESTABLISHED rows already require an enrolled chain and are
            never filtered by this flag.
        include_invalidated:
            When ``False`` (default), claims marked invalid by a signed
            contradiction verdict (``t_invalid IS NOT NULL``) are
            excluded. Pass ``True`` for audit / history queries.
        refutation_filter:
            Optional refutation-state filter, one of ``"clean"`` /
            ``"contradicted"`` / ``"contested"`` / ``"retracted"`` /
            ``"any"``. Composes with the other filters via AND:

            * ``"clean"``: restrict to ``t_invalid IS NULL`` AND
              ``status = 'open'`` (the strictest "nothing wrong"
              cohort).
            * ``"contradicted"``: restrict to ``t_invalid IS NOT
              NULL``; overrides the default ``include_invalidated``
              gate so contradicted rows surface even when the flag
              wasn't flipped.
            * ``"contested"``: restrict to ``status = 'contested'``.
            * ``"retracted"``: restrict to ``status = 'retracted'``.
            * ``"any"``: surface every refutation state; implies
              ``include_invalidated=True``.

            Composition examples::

                # high-confidence ESTABLISHED claims with no refutation
                graph.query(
                    min_support="ESTABLISHED",
                    refutation_filter="clean",
                )

                # every claim with a signed contradiction, including
                # the contradicting + contradicted pairs
                graph.query(
                    refutation_filter="contradicted",
                    include_invalidated=True,
                )

                # clean claims mentioning "gene therapy" within
                # unverified preliminary work. refutation_filter is a
                # query-only feature; the search method does not accept it.
                graph.query(
                    "gene therapy",
                    refutation_filter="clean",
                    include_unverified=True,
                )

        Returns
        -------
        list[dict]
            Claim dicts ordered by support_level (desc) then created_at (desc).
            Each dict contains the standard claim columns plus two
            reputation projections computed at query time:

              - ``validator_reputation`` (int): for ESTABLISHED rows, the
                number of ESTABLISHED claims signed by the same
                validator. ``0`` for non-ESTABLISHED rows.
              - ``generator_enrolled`` (bool): True iff the claim's
                signing keyid is in the validators table.

        Raises
        ------
        ValueError
            If ``min_support`` or ``classification`` is not a valid value.
        ScanCeilingReached
            If the read exhausted its scan ceiling (``max(limit * 50, 5000)``
            ordered rows) before collecting ``limit`` survivors. Rows dropped
            by verify-on-read do not count as survivors, so a graph carrying
            many unverifiable rows can bury a real match behind the ceiling.
            The read refuses rather than return a short list that reads as an
            empty graph; narrow the query or lower ``limit``.
        """
        self._check_open()
        _warn_min_support(min_support)
        return _db.query_claims(
            self._conn,
            text=text,
            min_support=min_support,
            classification=classification,
            limit=limit,
            include_unverified=include_unverified,
            include_invalidated=include_invalidated,
            refutation_filter=refutation_filter,
            on_verify_excluded=self._record_verify_exclusions,
            on_unverified_excluded=self._record_unverified_exclusions,
        )

    @_synchronized
    def update_claim(
        self,
        claim_id: str,
        *,
        status: str | None = None,
        text: str | None = None,
        supports: list[str] | None = None,
        contradicts: list[str] | None = None,
        comparison_summary: str | None = None,
    ) -> None:
        """Update mutable fields on an existing claim.

        ``status`` and ``comparison_summary`` are always editable.
        ``text`` / ``supports`` / ``contradicts`` are part of the signed
        payload and refuse to mutate when the claim carries a signature
        bundle: use a retraction-plus-new-assertion flow on those
        cases.

        Trust model on ``status`` mutations
        -----------------------------------
        A status change (open / contested / retracted) is an EDITORIAL
        action: it produces no signed envelope, requires no validator
        keyid, and is not round-tripped through the signature-verify
        layer. An ESTABLISHED claim can be flipped to ``retracted`` by
        any process with DB write access; nothing in mareforma
        cryptographically records who pulled the lever. Compare with
        signed contradiction verdicts, which DO require an enrolled
        validator's signature and DO survive restore intact.

        For a cryptographically-traceable retraction story, prefer the
        retract-then-supersede pattern: assert a new claim with
        ``contradicts=[<old_claim_id>]`` signed by a validator key.
        That produces a signed envelope plus a contradiction verdict
        that restore can re-verify.

        Concurrency
        -----------
        Two processes calling ``update_claim`` on the same claim are
        serialised by SQLite at the row level; semantics are
        last-writer-wins with no conflict detection. Callers that need
        compare-and-set semantics on ``status`` should add their own
        out-of-band lock or assert a new claim instead of mutating an
        existing one.

        Raises :class:`ClaimNotFoundError`,
        :class:`SignedClaimImmutableError`,
        :class:`IllegalStateTransitionError`, or :class:`ValueError`
        per the underlying :func:`mareforma.db.update_claim` contract.
        """
        self._check_open()
        _db.update_claim(
            self._conn,
            self._root,
            claim_id,
            status=status,
            text=text,
            supports=supports,
            contradicts=contradicts,
            comparison_summary=comparison_summary,
            strict_promotion=self._strict_promotion,
        )

    @_synchronized
    def refutation_status(self, claim_id: str) -> dict:
        """Return the refutation classification for *claim_id*.

        Result shape: ``{"state", "reason", "signal"}`` where
        ``state`` is one of :data:`mareforma.db.REFUTATION_STATES`
        (``"clean"`` | ``"contradicted"`` | ``"contested"`` |
        ``"retracted"``), ``reason`` is a short human-readable
        explanation, and ``signal`` is ``"signed-verdict"`` /
        ``"editorial"`` / ``"none"`` indicating the strength of the
        underlying evidence.

        Raises :class:`ClaimNotFoundError` if no such claim exists.
        """
        self._check_open()
        row = _db.get_claim(self._conn, claim_id)
        if row is None:
            raise _db.ClaimNotFoundError(
                f"Claim '{claim_id}' not found."
            )
        return _db.refutation_status(row)

    @_synchronized
    def search(
        self,
        query: str,
        *,
        min_support: str | None = None,
        classification: str | None = None,
        limit: int = 20,
        include_unverified: bool = False,
        include_invalidated: bool = False,
    ) -> list[dict]:
        """FTS5 full-text search over claim text.

        Returns claim dicts ordered by FTS5 rank (best match first).
        Parameters mirror :meth:`query`: same filters, same per-row
        projection (``validator_reputation``, ``generator_enrolled``),
        same ``include_unverified`` semantics. The difference is the
        underlying engine: :meth:`query` uses LIKE substring matching;
        :meth:`search` uses FTS5 with the unicode61 tokenizer (diacritics
        folded) and supports the FTS5 query grammar.

        Parameters
        ----------
        query:
            FTS5 MATCH expression. Examples:

            - ``"gene"``: single token
            - ``"\\"epistemic graph\\""``: phrase (note: escape quotes
              in Python source)
            - ``"gene*"``: prefix
            - ``"gene OR pathway"``: boolean
            - ``"gene NEAR pathway"``: proximity

            Pure-wildcard queries (``"*"``) are refused: they would
            scan the entire table.
        min_support, classification, limit, include_unverified:
            See :meth:`query`.

        Raises
        ------
        ValueError
            If ``query`` is empty or pure wildcards, or fails FTS5
            parsing. Also for invalid ``min_support`` / ``classification``.
        ScanCeilingReached
            Same scan ceiling as :meth:`query`, on the ranked fetch.
        """
        self._check_open()
        _warn_min_support(min_support)
        return _db.search_claims(
            self._conn,
            query,
            min_support=min_support,
            classification=classification,
            limit=limit,
            include_unverified=include_unverified,
            include_invalidated=include_invalidated,
            on_verify_excluded=self._record_verify_exclusions,
            on_unverified_excluded=self._record_unverified_exclusions,
        )

    def _record_verify_exclusions(self, n: int) -> None:
        """Record that a read dropped *n* rows failing signature re-verification.

        Counted on the graph and appended to the health log, because no read
        surface can show the rows themselves: without this a tampered graph
        answers a query with a shorter list and nothing else.

        The counter is exact; the health-log append is rate-limited by
        :meth:`_health_append_due`. A dropped row is a STATE, not an event: the
        signature stays broken, so every later read finds it again, and a
        long-lived reader (``mareforma mcp serve`` holds one graph for the
        process lifetime) would otherwise write one line per poll forever.
        """
        self._read_verify_exclusions += n
        if not self._health_append_due(
                "read_verify_excluded", self._read_verify_exclusions):
            return
        from mareforma import health as _health
        _health.append_health_event(
            self._root, "read_verify_excluded", outcome="fail",
            n=n, total=self._read_verify_exclusions,
        )

    def _record_unverified_exclusions(self, n: int, saturated: bool = False) -> None:
        """Record that a read held back *n* rows behind the unverified filter.

        A PRELIMINARY claim whose generator key is not enrolled is dropped from
        an enumerating read unless the caller passes ``include_unverified=True``.
        Held back silently, that turns a record written under an unenrolled key
        into an empty answer, and a caller reads the empty list as "there is
        nothing here" rather than "there is something here you did not ask to
        see". Counted so a surface can say how many, and rate-limited in the
        health log for the same reason the verify exclusions are: it is a state
        every read re-encounters, not a new event each time.
        """
        self._read_unverified_exclusions += n
        if saturated:
            self._read_unverified_saturated = True
        if not self._health_append_due(
                "read_unverified_excluded", self._read_unverified_exclusions):
            return
        from mareforma import health as _health
        _health.append_health_event(
            self._root, "read_unverified_excluded", outcome="degraded",
            n=n, total=self._read_unverified_exclusions,
        )

    def _health_append_due(self, kind: str, total: int) -> bool:
        """Whether this occurrence of *kind* earns a health-log line.

        Two triggers, because the two things a reader looks for are different.

        Occurrence count, on the 1st, 2nd, 4th, 8th and so on, so a persistent
        condition costs a logarithmic number of lines instead of one per read and
        the first occurrence is always recorded. Counting occurrences rather than
        rows because a read that drops three rows every time steps the row total
        3, 6, 9, which never lands on a power of two, so gating on the total
        alone would write nothing at all.

        Magnitude, whenever the running total has at least doubled since the last
        line written for this kind. Without it a sudden spike is silent: a read
        that dropped 500 rows between two ordinary reads is the one a reader most
        wants to see, and it lands on no power of two.
        """
        seen = self._health_append_counts.get(kind, 0) + 1
        self._health_append_counts[kind] = seen
        last = self._health_append_totals.get(kind, 0)
        due = seen & (seen - 1) == 0 or total >= max(1, last * 2)
        if due:
            self._health_append_totals[kind] = total
        return due

    # ------------------------------------------------------------------
    # Verdict-issuer protocol
    # ------------------------------------------------------------------

    @_synchronized
    def record_replication_verdict(
        self,
        *,
        verdict_id: str,
        cluster_id: str,
        member_claim_id: str,
        other_claim_id: str | None = None,
        method: str,
        confidence: dict | None = None,
    ) -> None:
        """Insert a signed replication verdict.

        The signing key is the graph's own loaded key (the same one
        used by :meth:`assert_claim`); its keyid must be enrolled in
        the project's ``validators`` table.

        The OSS core accepts verdicts from any enrolled identity.
        The predicates that GENERATE verdicts (semantic-cluster,
        cross-method, contradiction-detection) live outside the OSS
        core and call this method to write their output.

        Parameters
        ----------
        verdict_id
            Caller-supplied unique id for the verdict row.
        cluster_id
            Caller-supplied cluster identifier shared across all
            verdicts in one replication cluster.
        member_claim_id
            The claim being asserted as replicated.
        other_claim_id
            Optional second member of the replication pair (None for
            single-row cross-method verdicts).
        method
            One of ``hash-match``, ``semantic-cluster``,
            ``shared-resolved-upstream``, ``cross-method``.
        confidence
            Optional dict of confidence values (e.g.
            ``{"cosine": 0.92, "nli_forward": 0.88}``), never fused
            into a single score per the report.

        Raises
        ------
        VerdictIssuerError
            If the graph has no signer (unsigned mode), the signer's
            keyid is not enrolled, the method is invalid, or any
            referenced claim_id is missing.
        """
        self._check_open()
        if self._signer is None:
            from mareforma.db import VerdictIssuerError
            raise VerdictIssuerError(
                "Cannot record a verdict without a signer. Open the "
                "graph with key_path= or run `mareforma bootstrap`."
            )
        _db.record_replication_verdict(
            self._conn, self._root,
            verdict_id=verdict_id,
            cluster_id=cluster_id,
            member_claim_id=member_claim_id,
            other_claim_id=other_claim_id,
            method=method,
            confidence=confidence,
            signer=self._signer,
        )

    @_synchronized
    def record_contradiction_verdict(
        self,
        *,
        verdict_id: str,
        member_claim_id: str,
        other_claim_id: str,
        confidence: dict | None = None,
    ) -> None:
        """Insert a signed contradiction verdict.

        The trigger ``contradiction_invalidates_older`` sets
        ``t_invalid`` on the older of the two referenced claims.
        Default queries (``include_invalidated=False``) will then
        exclude the invalidated claim.

        Same enrollment + claim-existence + signature-binding contract
        as :meth:`record_replication_verdict`.
        """
        self._check_open()
        if self._signer is None:
            from mareforma.db import VerdictIssuerError
            raise VerdictIssuerError(
                "Cannot record a verdict without a signer. Open the "
                "graph with key_path= or run `mareforma bootstrap`."
            )
        _db.record_contradiction_verdict(
            self._conn, self._root,
            verdict_id=verdict_id,
            member_claim_id=member_claim_id,
            other_claim_id=other_claim_id,
            confidence=confidence,
            signer=self._signer,
        )

    @_synchronized
    def replication_verdicts(
        self,
        *,
        member_claim_id: str | None = None,
        cluster_id: str | None = None,
        include_invalidated: bool = False,
    ) -> list[dict]:
        """List signed replication verdicts, optionally filtered.

        By default, verdicts whose member or other claim has been
        invalidated by a signed contradiction verdict are excluded,
        same surface as :meth:`query`. Pass ``include_invalidated=True``
        for audit / history queries.
        """
        self._check_open()
        return _db.list_replication_verdicts(
            self._conn,
            member_claim_id=member_claim_id,
            cluster_id=cluster_id,
            include_invalidated=include_invalidated,
        )

    @_synchronized
    def contradiction_verdicts(
        self, *, claim_id: str | None = None,
        include_invalidated: bool = False,
    ) -> list[dict]:
        """List signed contradiction verdicts, optionally filtered.

        By default, verdicts on invalidated claims are excluded; pass
        ``include_invalidated=True`` for audit-mode listings, the
        typical use, since a contradiction verdict IS the evidence
        for invalidation.
        """
        self._check_open()
        return _db.list_contradiction_verdicts(
            self._conn, claim_id=claim_id,
            include_invalidated=include_invalidated,
        )

    @_synchronized
    def get_validator_reputation(self) -> dict[str, int]:
        """Return ``{validator_keyid: count}`` for every enrolled validator.

        Count is the number of ESTABLISHED claims whose validation
        envelope was signed by that keyid. Validators with zero
        ESTABLISHED validations appear with ``count=0``. Derived state,
        recomputed on every call from the claims table; never cached.
        """
        self._check_open()
        return _db.get_validator_reputation(self._conn)

    @_synchronized
    def get_claim(self, claim_id: str) -> dict | None:
        """Return a single claim dict by ID, or None if not found."""
        self._check_open()
        return _db.get_claim(self._conn, claim_id)

    @_synchronized
    def trust_map(self, claim_id: str, *, reexec_record: "dict | None" = None):
        """Return the per-finding :class:`mareforma.trust_map.TrustMap` for a claim.

        A read-side artifact that places every trust property, attributability,
        provenance, grounding, faithfulness, methodological validity, leakage,
        independence, contestation, standing, trust-root, witnessing, at its
        tier with the residual named. Adds no signed field; derived from what is
        already stored. ``reexec_record`` optionally supplies a re-execution
        faithfulness verdict to place on the PROXY-tier faithfulness axis; when
        omitted that axis reads ``not present``. Returns ``None`` if the claim
        does not exist.
        """
        self._check_open()
        from mareforma.trust_map import build_trust_map

        return build_trust_map(
            self._conn, claim_id, reexec_record=reexec_record,
            disclose=self._skips,
        )

    # ------------------------------------------------------------------
    # Trust layer: propositions, findings, derived Status
    # ------------------------------------------------------------------

    @_synchronized
    def register_proposition(self, proposition: "Proposition") -> str:
        """Register a falsifiable :class:`mareforma.trust.Proposition`.

        Returns the ``content_id`` and is idempotent on it (re-registering the
        same proposition returns the existing node). A non-falsifiable
        proposition (no direction or empty scope) is refused, because it forbids
        no observation and cannot anchor evidence.
        """
        self._check_open()
        from mareforma.db.core import _now
        from mareforma.trust import NonFalsifiablePropositionError, _store

        if not proposition.is_falsifiable():
            raise NonFalsifiablePropositionError(
                "proposition must commit to a direction and a non-empty scope; "
                f"got direction={proposition.direction.value}, "
                f"scope={dict(proposition.scope)!r}"
            )
        now = _now()
        with self._conn:
            return _store.register_proposition(self._conn, proposition, now)

    @_synchronized
    def register_plan(
        self,
        proposition: "Proposition",
        prediction: "Prediction",
        *,
        generated_by: str | None = None,
    ) -> str:
        """Pre-register a :class:`mareforma.trust.Prediction` against a proposition.

        Binds the decision rule to the proposition *before the numbers are seen*:
        the load-bearing move of the hypothetico-deductive method. Three
        effects, idempotent together:

        1. Registers the proposition (idempotent on ``content_id``).
        2. Writes the append-only ``predictions`` row with ``preregistered=1``.
        3. Writes its own signed claim, the **plan attestation**, via the
           normal :meth:`assert_claim` path under idempotency key
           ``plan:{plan_id}``, carrying a ``plan/v1`` predicate payload. This
           claim is an ordinary signed claim, so it is Rekor-anchorable like any
           other (no special-casing).

        Returns the content-addressed ``plan_id`` (see
        :func:`mareforma.trust._store.compute_plan_id`). Re-registering the same
        prediction is a no-op: the claim's idempotency key returns the existing
        attestation and both the proposition and prediction rows hit
        ``ON CONFLICT DO NOTHING``, so no duplicate claim or row is written.

        Raises :class:`NonFalsifiablePropositionError` for a proposition that
        commits to no direction or has an empty scope.

        The plan claim is committed before the structured rows (same ordering as
        :meth:`assert_finding`); a retry reuses the claim idempotently rather
        than orphaning it.
        """
        self._check_open()
        from mareforma.db.core import _now
        from mareforma.trust import NonFalsifiablePropositionError, _store
        from mareforma.trust.prediction import validate_alpha

        if not proposition.is_falsifiable():
            raise NonFalsifiablePropositionError(
                "proposition must commit to a direction and a non-empty scope; "
                f"got direction={proposition.direction.value}, "
                f"scope={dict(proposition.scope)!r}"
            )

        # Re-validate the alpha at the write boundary, not only at Prediction
        # construction: a Prediction built off the normal route (e.g. a frozen
        # field set past __post_init__) must not be able to persist an
        # un-gateable plan through here. This is what keeps the stranded state
        # provably legacy-only, which retire_plan's docstring assumes.
        validate_alpha(prediction.alpha)

        cid = proposition.content_id()
        plan_id = _store.compute_plan_id(cid, prediction)

        claim_id = self.assert_claim(
            proposition.text(),
            generated_by=generated_by,
            idempotency_key=f"plan:{plan_id}",
            predicate_payload={
                "trust": "plan/v1",
                "content_id": cid,
                "frame_id": proposition.frame_id(),
                "plan_id": plan_id,
                **prediction.to_dict(),
                # The store's word, written last so the attestation can only
                # restate the predictions row this call is about to write.
                "preregistered": True,
            },
        )

        now = _now()
        with self._conn:
            _store.register_proposition(self._conn, proposition, now)
            _store.register_plan(
                self._conn, cid, prediction, now, preregistered=True
            )

        from mareforma import health as _health
        _health.append_health_event(
            self._root, "register_plan", plan_claim=claim_id,
        )
        return plan_id

    @_synchronized
    def retire_plan(self, plan_id: str, *, alpha: float, reason: str) -> dict:
        """Retire a plan the gates cannot run and re-register its evidence.

        A plan written by a release with a wider alpha bound can carry a rule no
        gate can discriminate at (alpha at or above 0.5 marks every p-value
        significant and asks for a confidence level of zero or less). The graph
        still restores, but every evidence line under that plan drops out of the
        counts and the proposition reads UNTESTED. The ``predictions`` row is
        append-only and cannot be deleted, so there is nothing to correct in
        place. This is the way out, and it is the operator's call to make: a
        plan never retires itself.

        Three effects, in one transaction:

        1. Registers the **replacement**: the retired plan's own rule repeated at
           *alpha*. Only the alpha moves, so a repair cannot re-choose the side
           of the null once the numbers are known. The row carries
           ``preregistered=0`` and its claim states what it supersedes: it was
           registered after the evidence, and the record says so rather than
           reading as an original pre-registration.
        2. Records the retirement (``plan_retirements``) with *reason*, and
           writes its own signed **retirement attestation** whose text renders
           the plan, the replacement and the reason, so restore re-derives the
           record from signed material.
        3. Leaves the retired row exactly as registered. Nothing is rewritten and
           nothing is deleted; the read path gates the evidence that stood under
           the retired plan against the replacement from here on.

        Returns a receipt: ``plan_id``, ``superseded_by``, ``reason``,
        ``retired_at``, ``claim_id`` (the retirement attestation),
        ``plan_claim_id`` (the replacement's attestation) and ``lines_recovered``
        (evidence lines that gate again under the replacement). Idempotent:
        retiring the same plan at the same alpha returns the recorded receipt.

        Raises :class:`NoRegisteredPlanError` when no such plan is registered,
        and :class:`PlanNotRetirableError` when the plan's rule still runs (a
        retirement is not a way to withdraw evidence a reader dislikes), when
        the plan is already retired (a second retirement would let the operator
        shop for the alpha that reads best), or when no line under the plan
        would gate under the replacement, which would spend the one retirement
        for nothing. An *alpha* outside ``(0, 0.5)`` raises ``ValueError``, from
        the same bound every registration is held to.
        """
        self._check_open()
        from mareforma.db.core import _now, _validate_claim_text
        from mareforma.trust import (
            NoRegisteredPlanError,
            PlanNotRetirableError,
            Proposition,
            _store,
        )

        if not isinstance(reason, str) or not reason.strip():
            raise ValueError(
                "reason must be a non-empty string: the retirement record is "
                "what tells a later reader why the plan was retired"
            )
        # Clean the reason through the claim-text rule before it is stored, so
        # the column holds the same string the signed attestation carries and
        # restore's re-derivation compares like with like.
        reason = _validate_claim_text(reason)

        row = _store.get_plan_row(self._conn, plan_id)
        if row is None:
            raise NoRegisteredPlanError(
                f"no registered plan with plan_id={plan_id[:12]}…; there is "
                "nothing to retire"
            )
        try:
            _store.prediction_from_row(row)
        except ValueError:
            pass
        else:
            raise PlanNotRetirableError(
                f"plan {plan_id[:12]}… states a rule the gates can still run, "
                "so it is not retirable. Retirement recovers evidence stranded "
                "under an un-runnable rule; it is not a way to withdraw a "
                "finding, which is what graph.update_claim(claim_id, "
                "status='retracted') is for."
            )

        # The replacement repeats the retired rule at a gateable alpha. Building
        # it applies the (0, 0.5) bound, so an alpha that would strand the
        # evidence again is refused here, before anything is written.
        replacement = _store.replacement_prediction(row, alpha)
        superseded_by = _store.compute_plan_id(row["content_id"], replacement)

        recorded = _store.plan_retirement(self._conn, plan_id)
        if recorded is not None:
            if recorded["superseded_by"] == superseded_by:
                return self._retirement_receipt(recorded)
            raise PlanNotRetirableError(
                f"plan {plan_id[:12]}… is already retired and superseded by "
                f"plan {recorded['superseded_by'][:12]}…. Re-pointing it at a "
                "second alpha would be choosing the rule that reads best once "
                "the numbers are known."
            )

        # The replacement must be a fresh registration. If a plan with this
        # exact rule + alpha already exists (someone pre-registered it earlier,
        # or it stands as another retirement's replacement), the write below
        # would reuse that plan's attestation via the idempotency key and leave
        # its predictions row as it stands. A pre-registered row (preregistered=1)
        # would then record this post-hoc repair as a pre-registration and drop
        # the supersedes disclosure silently. This retirement is not on record
        # (recorded is None above), so an existing replacement here is never this
        # retirement's own: refuse, naming it, rather than laundering the choice.
        if _store.plan_exists(self._conn, superseded_by):
            raise PlanNotRetirableError(
                f"retiring plan {plan_id[:12]}… at alpha={alpha} would reuse the "
                f"already-registered plan {superseded_by[:12]}… as its "
                "replacement. That plan may be a pre-registration; adopting it "
                "here would record a post-hoc repair as one and drop the "
                "supersedes disclosure. Retire at an alpha whose replacement "
                "plan does not already exist."
            )

        # How much evidence the replacement actually recovers. A line whose
        # stored estimate is unreadable stays dropped either way, but a
        # retirement that recovers nothing spends the one retirement this plan
        # gets, so it is refused with the gate's own reason. The guard fires on
        # recovered==0 alone: a plan with no evidence at all (stranded==0) must
        # not spend its one retirement either.
        recovered, stranded, refusal = self._lines_recovered(
            plan_id, replacement,
        )
        if not recovered:
            detail = f": {refusal}" if refusal is not None else ""
            raise PlanNotRetirableError(
                f"retiring plan {plan_id[:12]}… at alpha={alpha} would recover "
                f"none of its {stranded} evidence line(s){detail}. "
                "A plan is retired once, so a retirement that recovers nothing "
                "is refused rather than spent."
            )

        prop_row = _store.get_proposition_row(self._conn, row["content_id"])
        proposition = Proposition.from_dict({
            "subject": prop_row["subject"],
            "relation": prop_row["relation"],
            "object": prop_row["object"],
            "direction": prop_row["direction"],
            "scope": json.loads(prop_row["scope_json"] or "{}"),
            "magnitude": prop_row["magnitude"],
        })
        retirement_text = _store.retirement_claim_text(
            plan_id, superseded_by, reason,
        )

        # Both claims and both rows land together: a committed replacement with
        # no retirement record would leave the evidence stranded under a plan
        # the graph now reads as live.
        now = _now()
        conn = self._conn
        _own_txn = not conn.in_transaction
        if _own_txn:
            conn.execute("BEGIN IMMEDIATE")
        try:
            plan_claim_id = self.assert_claim(
                proposition.text(),
                idempotency_key=f"plan:{superseded_by}",
                predicate_payload={
                    "trust": "plan/v1",
                    "content_id": row["content_id"],
                    "frame_id": proposition.frame_id(),
                    "plan_id": superseded_by,
                    **replacement.to_dict(),
                    # The store's word: this plan was registered to take over
                    # evidence already in the graph, so it claims no
                    # pre-registration and names what it replaces.
                    "preregistered": False,
                    "supersedes": plan_id,
                    "supersedes_reason": reason,
                },
            )
            _store.register_plan(
                conn, row["content_id"], replacement, now, preregistered=False,
            )
            claim_id = self.assert_claim(
                retirement_text,
                supports=[plan_claim_id],
                idempotency_key=f"plan-retirement:{plan_id}",
                predicate_payload={
                    "trust": "plan-retirement/v1",
                    "content_id": row["content_id"],
                    "plan_id": plan_id,
                    "superseded_by": superseded_by,
                    "reason": reason,
                },
            )
            _store.retire_plan(
                conn, plan_id, superseded_by, reason, claim_id, now,
            )
            if _own_txn:
                conn.commit()
                from mareforma.db.core import _backup_claims_toml
                _backup_claims_toml(conn, self._root)
        except BaseException:
            if _own_txn:
                conn.rollback()
            raise

        from mareforma import health as _health
        _health.append_health_event(
            self._root, "retire_plan", plan_id=plan_id,
            superseded_by=superseded_by, lines_recovered=recovered,
        )
        return {
            "plan_id": plan_id,
            "superseded_by": superseded_by,
            "reason": reason,
            "retired_at": now,
            "claim_id": claim_id,
            "plan_claim_id": plan_claim_id,
            "lines_recovered": recovered,
        }

    def _lines_recovered(self, plan_id: str, replacement) -> tuple:
        """``(recovered, stranded, first refusal)`` for *plan_id* under *replacement*.

        A stranded line is recovered when the replacement can gate its stored
        estimate. One that still cannot be gated (an unreadable estimate, a CI
        at a level this alpha does not read) stays dropped, and its refusal is
        carried back so the caller can name why.
        """
        from mareforma.trust import _store, compute_bearing

        recovered = 0
        refusal = None
        estimates = _store.plan_estimates(self._conn, plan_id)
        for est_row in estimates:
            try:
                compute_bearing(_store.estimate_from_row(est_row), replacement)
            except Exception as exc:  # noqa: BLE001 - reported, not swallowed
                refusal = refusal or exc
                continue
            recovered += 1
        return recovered, len(estimates), refusal

    def _retirement_receipt(self, row) -> dict:
        """The receipt for a retirement already on record (idempotent replay).

        ``lines_recovered`` is recomputed rather than stored, so a replay
        reports the graph as it stands and the key means the same thing on both
        paths: the retired plan's lines that gate under the replacement.
        """
        from mareforma.trust import _store

        replacement = _store.prediction_from_row(
            _store.get_plan_row(self._conn, row["superseded_by"])
        )
        recovered, _stranded, _refusal = self._lines_recovered(
            row["plan_id"], replacement,
        )
        return {
            "plan_id": row["plan_id"],
            "superseded_by": row["superseded_by"],
            "reason": row["reason"],
            "retired_at": row["retired_at"],
            "claim_id": row["claim_id"],
            "plan_claim_id": _store.get_plan_claim_id(
                self._conn, row["superseded_by"],
            ),
            "lines_recovered": recovered,
        }

    @_synchronized
    def assert_finding(
        self,
        proposition: "Proposition",
        prediction: "Prediction",
        estimate: "EffectEstimate | None" = None,
        *,
        data_id: str | None = None,
        data_bytes: bytes | None = None,
        data_source: str | None = None,
        lines: "Sequence[EvidenceLine] | None" = None,
        generated_by: str | None = None,
        control_type: "ControlType | str | None" = None,
        modality: str | None = None,
        provenance_id: str | None = None,
        design_type: str | None = None,
        code_ref: str | None = None,
        idempotency_key: str | None = None,
        grounding: "GroundingVerdict | None" = None,
        grounding_strict: bool = False,
    ) -> dict:
        """Record a finding: a computed bearing of an outcome on a proposition.

        The minimal write: a structured Proposition, a pre-registered Prediction,
        and the evidence. Pass either ``estimate`` + ``data_id`` (one line) or
        ``lines`` (the multi-line evidence tree), never both. mareforma computes
        a Bearing per line (never declared), persists the evidence tree, writes a
        signed claim as the attestation, and derives the proposition's count-based
        Status from the independent lines.

        Idempotent on the finding's ``data_id`` set: re-asserting the same
        finding on the same dataset(s) returns the prior finding rather than
        double-counting it.

        All input validation (falsifiability, estimate consistency, each line's
        gate) runs before the signed claim is written, so a rejected finding
        never leaves an orphan claim. The structured rows are then written in one
        transaction after the claim; their CHECK constraints mirror the already
        validated Python values, so a failure there is not expected. If one did
        occur the claim would remain as an attestation with no finding, and a
        retry would reuse that claim idempotently rather than duplicate it.

        One-shot convenience. This composes the two earned steps: it registers
        the proposition and a synthesised plan (``preregistered=0``, so a real
        :meth:`register_plan` pre-registration stays distinguishable), then
        delegates to :meth:`submit_finding`. The return shape, idempotency,
        atomicity, and derived Status are all preserved. The synthesised plan is
        a no-op when a plan for the same proposition and prediction is already on
        record, so a one-shot landing on a pre-registered plan submits under that
        plan and can raise :class:`~mareforma.trust.PostHocPlanError` like any
        other submission. A one-shot finding does
        not separately attest its plan, so its signed ``supports[]`` carries no
        plan edge; use the explicit :meth:`register_plan` / :meth:`submit_finding`
        split when you want the signed plan -> finding edge.
        """
        self._check_open()
        from mareforma.db.core import _now
        from mareforma.trust import (
            Contrast,
            ControlType,
            EvidenceLine,
            NonFalsifiablePropositionError,
            _store,
            compute_bearing,
        )
        from mareforma.trust.prediction import validate_alpha

        if not proposition.is_falsifiable():
            raise NonFalsifiablePropositionError(
                "proposition must commit to a direction and a non-empty scope; "
                f"got direction={proposition.direction.value}, "
                f"scope={dict(proposition.scope)!r}"
            )

        # When the caller supplies dataset bytes, content-address them into the
        # data_id so the independence guard collapses byte-identical reruns and
        # cannot be fooled by a fabricated string. Bytes and an explicit
        # string data_id are mutually exclusive.
        if data_bytes is not None:
            if data_id is not None:
                raise ValueError(
                    "pass either data_id (a string) or data_bytes (hashed), "
                    "not both"
                )
            data_id = _store.content_address_data_id(data_bytes)

        # Validate the gate inputs (estimate/data_id consistency, then the gate)
        # for EVERY line BEFORE writing anything, so a rejected one-shot finding
        # leaves no dangling proposition/plan behind, preserving the
        # all-or-nothing behaviour. submit_finding re-runs these cheaply; the
        # duplication buys atomicity at the convenience layer.
        if lines is not None:
            if estimate is not None or data_id is not None:
                raise ValueError(
                    "pass either (estimate + data_id) or lines, not both"
                )
            pre_lines = list(lines)
            if not pre_lines:
                raise ValueError("a finding must carry at least one evidence line")
            for ln in pre_lines:
                if not isinstance(ln, EvidenceLine):
                    raise TypeError("every item in lines must be an EvidenceLine")
                compute_bearing(ln.estimate, prediction)
        else:
            if estimate is None or data_id is None:
                raise ValueError(
                    "single-line mode requires both estimate and data_id"
                )
            ct = control_type if control_type is not None else ControlType.NEGATIVE
            EvidenceLine(
                estimate=estimate,
                data_id=data_id,
                contrast=Contrast(ct),
                modality=modality,
                provenance_id=provenance_id,
                design_type=design_type,
            )
            compute_bearing(estimate, prediction)

        cid = proposition.content_id()
        # Synthesise the proposition + a non-pre-registered plan, then submit
        # against them. preregistered=0 marks this as a one-shot rather than a
        # genuine up-front pre-registration. ON CONFLICT DO NOTHING keeps it
        # idempotent and never upgrades an existing pre-registered plan's flag.
        #
        # Re-validate the alpha at this write boundary, the same bound
        # register_plan holds. Prediction.__post_init__ already enforces it, but
        # a rule reaching persistence off the normal route (a frozen-instance
        # bypass, a future caller) must not mint a plan no gate can run: that is
        # the stranded state retire_plan exists to repair, and it should only
        # ever arise from a release that predates this bound.
        validate_alpha(prediction.alpha)
        now = _now()
        with self._conn:
            _store.register_proposition(self._conn, proposition, now)
            _store.register_plan(
                self._conn, cid, prediction, now, preregistered=False
            )

        return self.submit_finding(
            proposition,
            prediction,
            estimate,
            data_id=data_id,
            data_source=data_source,
            lines=lines,
            generated_by=generated_by,
            control_type=control_type,
            modality=modality,
            provenance_id=provenance_id,
            design_type=design_type,
            code_ref=code_ref,
            idempotency_key=idempotency_key,
            grounding=grounding,
            grounding_strict=grounding_strict,
        )

    @_synchronized
    def submit_finding(
        self,
        proposition: "Proposition",
        prediction: "Prediction",
        estimate: "EffectEstimate | None" = None,
        *,
        data_id: str | None = None,
        data_bytes: bytes | None = None,
        data_source: str | None = None,
        lines: "Sequence[EvidenceLine] | None" = None,
        generated_by: str | None = None,
        control_type: "ControlType | str | None" = None,
        modality: str | None = None,
        provenance_id: str | None = None,
        design_type: str | None = None,
        code_ref: str | None = None,
        idempotency_key: str | None = None,
        grounding: "GroundingVerdict | None" = None,
        grounding_strict: bool = False,
    ) -> dict:
        """Submit a finding against a plan that was already pre-registered.

        The second half of the register-plan-then-submit split. Computes the
        ``plan_id`` from the proposition + prediction and REQUIRES that plan to
        already exist (via :meth:`register_plan`), else raises
        :class:`NoRegisteredPlanError`. Then it computes a Bearing per evidence
        line, writes the finding's signed claim whose ``supports[]`` cites the
        plan attestation's claim_id (so the plan -> finding edge is *signed*, not
        merely denormalised), persists the evidence tree, and derives the
        proposition's Status.

        Single- vs multi-line input. Pass either ``estimate`` + ``data_id`` (one
        line) OR ``lines`` (a sequence of pre-built :class:`EvidenceLine`, the
        multi-line evidence tree), never both. In multi-line mode the per-line
        attributes (``control_type``, ``modality``, ``provenance_id``,
        ``design_type``) live on each line and must not be passed as scalars.

        Idempotency anchor. A finding's identity within a ``content_id`` is its
        full ``data_id`` set: re-submitting the same set under the same plan
        returns the prior finding. **Fork-guard:** if the submitted datasets
        already belong to a finding under a *different* plan, span more than one
        existing finding, or differ from an existing finding's set, this raises
        :class:`FindingPlanForkError` rather than silently returning a prior
        bearing.

        Independence note. Status counts independent support/refute by distinct
        *run* (``generated_by``) with a ``data_id`` guard (see
        :func:`mareforma.trust._store.independence_counts`), so the run token
        must be per-run-unique; a default/None token is flagged as a health event
        because it collapses independence.

        Grounding. ``grounding`` takes the verdict an ``observe()`` scope
        computed, and only such a verdict writes the observed axis. A
        :class:`~mareforma.observe.GroundingVerdict` a caller constructed is a
        declaration, whatever its type says: it is stored and reported as
        ``DECLARED`` and neutralised out of ``GROUNDED``, so it cannot promote
        and cannot read as an execution mareforma watched. The verdict is
        attested before it is bound to the finding's citation, so a declared one
        cannot borrow a real citation either.

        All input validation (falsifiability, estimate consistency, each line's
        gate) runs before the signed claim is written, so a rejected finding
        never leaves an orphan claim. The authoritative existence check and the
        structured-row writes run inside one transaction (no TOCTOU); a retry
        reuses the finding claim idempotently rather than duplicating it.
        """
        self._check_open()
        from mareforma.db.core import _now
        from mareforma.trust import (
            Contrast,
            ControlType,
            EvidenceLine,
            FindingPlanForkError,
            NoRegisteredPlanError,
            NonFalsifiablePropositionError,
            PostHocPlanError,
            _store,
            compute_bearing,
        )

        if not proposition.is_falsifiable():
            raise NonFalsifiablePropositionError(
                "proposition must commit to a direction and a non-empty scope; "
                f"got direction={proposition.direction.value}, "
                f"scope={dict(proposition.scope)!r}"
            )

        # When the caller supplies dataset bytes, content-address them into the
        # data_id so the independence guard collapses byte-identical reruns and
        # cannot be fooled by a fabricated string. Bytes and an explicit
        # string data_id are mutually exclusive.
        if data_bytes is not None:
            if data_id is not None:
                raise ValueError(
                    "pass either data_id (a string) or data_bytes (hashed), "
                    "not both"
                )
            data_id = _store.content_address_data_id(data_bytes)

        # Resolve single-line vs multi-line input into a list of EvidenceLine.
        # Building each line validates its estimate/data_id before any write.
        if lines is not None:
            if estimate is not None or data_id is not None:
                raise ValueError(
                    "pass either (estimate + data_id) or lines, not both"
                )
            if any(
                v is not None
                for v in (control_type, modality, provenance_id, design_type,
                          data_source)
            ):
                raise ValueError(
                    "in multi-line mode the per-line attributes (control_type, "
                    "modality, provenance_id, design_type, data_source) belong on "
                    "each EvidenceLine, not as scalar arguments"
                )
            evidence_lines = list(lines)
            if not evidence_lines:
                raise ValueError("a finding must carry at least one evidence line")
            if any(not isinstance(ln, EvidenceLine) for ln in evidence_lines):
                raise TypeError("every item in lines must be an EvidenceLine")
        else:
            if estimate is None or data_id is None:
                raise ValueError(
                    "single-line mode requires both estimate and data_id"
                )
            ct = control_type if control_type is not None else ControlType.NEGATIVE
            evidence_lines = [
                EvidenceLine(
                    estimate=estimate,
                    data_id=data_id,
                    contrast=Contrast(ct),
                    modality=modality,
                    provenance_id=provenance_id,
                    design_type=design_type,
                    data_source=data_source,
                )
            ]

        # One Bearing per line (gate validation runs here, before any write).
        bearings = [compute_bearing(ln.estimate, prediction) for ln in evidence_lines]
        primary_bearing = bearings[0]
        # A run token that is present but blank is a caller error: independence
        # is counted by distinct run, so an empty/whitespace token would silently
        # collapse it. None is allowed (it defaults downstream and is warned
        # below); an explicit blank is rejected.
        if generated_by is not None and not generated_by.strip():
            raise ValueError(
                "generated_by must be a non-empty run token (or None to default); "
                "a blank token collapses distinct-run independence"
            )

        # Flag string-fallback data_ids: a line whose data_id was NOT
        # content-addressed from bytes is agent-attested, so its distinctness
        # is soft. Surface it as a durable health event rather than silently
        # treating it as content-addressed.
        if any(not _store.is_content_addressed(ln.data_id) for ln in evidence_lines):
            from mareforma import health as _health
            _health.append_health_event(
                self._root, "data_id_string_fallback",
                content_id=proposition.content_id(),
            )

        cid = proposition.content_id()
        plan_id = _store.compute_plan_id(cid, prediction)
        data_id_set = {ln.data_id for ln in evidence_lines}
        # Single-line identity tracks the DATASET set, not the raw line count:
        # a finding with two lines over one dataset (e.g. two contrasts) is still
        # one independent dataset, so it keeps the single-line idempotency key
        # and back-compat scalar data_id.
        single_line = len(data_id_set) == 1

        def _fork_error(reason: str) -> FindingPlanForkError:
            return FindingPlanForkError(
                f"a finding for content_id={cid[:12]}… cannot be written: {reason}. "
                "Within a proposition a dataset set stands under exactly one plan; "
                "re-submitting under a changed rule or a different set is refused, "
                "not silently ignored."
            )

        def _resolve(conn) -> tuple[str, object]:
            """('idempotent', row) | ('new', None); raises FindingPlanForkError."""
            touched: dict[str, object] = {}
            for d in data_id_set:
                row = _store.find_existing_finding(conn, cid, d)
                if row is not None:
                    touched[row["finding_id"]] = row
            if not touched:
                return ("new", None)
            if len(touched) > 1:
                raise _fork_error(
                    "the submitted datasets already span more than one finding"
                )
            (row,) = touched.values()
            # A retired plan's datasets stand under the plan that superseded it,
            # which is what the read path counts them under, so a re-submission
            # under the replacement is the same finding, not a fork.
            retirement = _store.plan_retirement(conn, row["plan_id"])
            superseded_by = retirement["superseded_by"] if retirement else None
            if row["plan_id"] != plan_id and superseded_by != plan_id:
                raise _fork_error(
                    f"its datasets already stand under plan {row['plan_id'][:12]}…, "
                    f"but the prediction now passed resolves to plan {plan_id[:12]}…"
                )
            if _store.finding_data_ids(conn, row["finding_id"]) != data_id_set:
                raise _fork_error(
                    "the submitted dataset set differs from the existing finding's"
                )
            return ("idempotent", row)

        # Pre-flight (fast path, clean errors). The authoritative checks repeat
        # in-transaction below to close the TOCTOU window.
        kind, existing = _resolve(self._conn)
        if kind == "idempotent":
            from mareforma import health as _health
            _health.append_health_event(
                self._root, "submit_finding",
                bearing=primary_bearing.direction.value, idempotent=True,
            )
            view = _store.proposition_status(self._conn, cid, disclose=self._skips)
            return {
                "finding_id": existing["finding_id"],
                "content_id": cid,
                "plan_id": existing["plan_id"],
                "claim_id": existing["claim_id"],
                "bearing": primary_bearing.to_dict(),
                "bearings": [b.to_dict() for b in bearings],
                "status": view["status"] if view else None,
                "idempotent": True,
                "proposition_status": view,
                # Report the verdict actually stored on the existing claim, not a
                # freshly-passed one: an idempotent replay reuses the first
                # write's signed claim and does not re-record grounding.
                "grounding": self._stored_grounding(existing["claim_id"]),
                # Likewise report the lineage stored on the existing finding's
                # lines, not a freshly-captured one: the replay does not rewrite
                # the evidence tree.
                "model_lineage": _store.finding_model_lineage(
                    self._conn, existing["finding_id"]
                ),
            }
        if not _store.plan_exists(self._conn, plan_id):
            raise NoRegisteredPlanError(
                f"no registered plan for (content_id={cid[:12]}…, "
                f"plan_id={plan_id[:12]}…). Call register_plan(proposition, "
                "prediction) before submit_finding, or use assert_finding for "
                "the one-shot path that registers the plan for you."
            )

        # Pre-registration guard: a plan that CLAIMS
        # pre-registration (preregistered=1) must have been registered BEFORE
        # this run first executed. A run's first observed execution is its
        # earliest prior finding under the same generated_by token; a plan whose
        # registered_at post-dates it was written after the run was already
        # producing outcomes, so honoring it would launder a post-hoc rule as a
        # pre-registration. Refuse it here, before any write, exactly like the
        # NoRegisteredPlanError above. A one-shot synthesised plan
        # (preregistered=0) makes no pre-registration claim and is exempt, and a
        # run with no prior finding has not begun, so nothing can post-date it.
        # No in-transaction re-check is needed: a concurrent finding by this run
        # can only land at a timestamp at or after now (>= registered_at on an
        # honored path), so it cannot retroactively move the run's first
        # execution before the plan's registration.
        #
        # Omitting generated_by is not a third exemption. The claim write
        # resolves it to DEFAULT_RUN_TOKEN, so the work IS attributed to a run;
        # the guard resolves it the same way and asks about the same token. The
        # consequence is intended: a project that never sets a run token puts
        # every finding under one identity, so once any finding exists no later
        # preregistered=1 plan can be submitted under the default. That is the
        # collapsed run identity the health event below already reports.
        run_token = generated_by or DEFAULT_RUN_TOKEN
        reg = _store.plan_registration(self._conn, plan_id)
        if reg is not None and reg["preregistered"] == 1:
            first_exec = _store.run_first_execution(self._conn, run_token)
            if first_exec is not None and reg["registered_at"] > first_exec:
                raise PostHocPlanError(
                    f"plan {plan_id[:12]}… was registered at "
                    f"{reg['registered_at']}, after run {run_token!r} first "
                    f"executed at {first_exec}. A plan registered once the run "
                    "was already producing findings is not a pre-registration; "
                    "it is refused, not honored. Pre-register the plan before "
                    "the run executes, or submit under a fresh run token."
                )

        # Bind the grounding verdict to the finding's citation, AFTER the
        # idempotency check, so an idempotent replay (which reuses the first
        # write's stored verdict and discards this one) never fires a spurious
        # downgrade health event or raises in strict mode for a result that is
        # thrown away. The finding's own citation identifiers are the
        # content-addressed data_id set plus any data_source location(s),
        # normalized ONCE here at write time; a GROUNDED whose cited set is
        # disjoint downgrades to OPAQUE (or raises in strict mode). Normalized
        # strings are persisted (data_sources below, cited_sources in the signed
        # record) so the read side re-checks by pure string comparison, never by
        # touching a verifier's filesystem. The assert path enforces that no
        # observe() scope is still open when this signs.
        finding_sources = self._finding_citation_sources(evidence_lines)
        grounding_signed = self._bind_grounding(
            grounding, finding_sources, strict=grounding_strict,
            content_id=cid,
        )
        data_sources = self._normalized_data_sources(evidence_lines)

        # The model/method lineage the observer captured for the authoring scope
        # (COMPUTED / PROXY / UNVERIFIABLE), or None when no model call was
        # observed. It rides the grounding verdict from the observe() scope. The
        # denormalised evidence-line column keys the independence read, but that
        # column is unsigned, so it also rides the SIGNED observed record here:
        # the read side re-authenticates the column against this signed copy (the
        # same defence the signer column already earns), and a v1 finding that
        # carries no signed lineage reads soft rather than a fabricated distinct
        # model. A finding without an observed model call adds no key, so it stays
        # byte-identical to a pre-observer finding.
        model_lineage = _model_lineage_of(grounding)
        model_lineage_json = (
            json.dumps(model_lineage.to_dict(), sort_keys=True, ensure_ascii=False)
            if model_lineage is not None
            else None
        )
        if model_lineage is not None and grounding_signed is not None:
            grounding_signed = {
                **grounding_signed,
                "model_lineage": model_lineage.to_dict(),
            }

        # The claim idempotency_key: keep the exact single-line form for parity;
        # multi-line keys on a stable hash of the sorted dataset set.
        if idempotency_key is not None:
            finding_key = idempotency_key
        elif single_line:
            finding_key = f"finding:{cid}:{next(iter(data_id_set))}"
        else:
            import hashlib
            digest = hashlib.sha256(
                "\x00".join(sorted(data_id_set)).encode()
            ).hexdigest()
            finding_key = f"finding:{cid}:set:{digest}"

        # Authoritative existence + fork + plan checks AND all writes run in one
        # transaction (BEGIN IMMEDIATE). The finding claim is written INSIDE this
        # transaction via assert_claim, which joins an open transaction
        # (conn.in_transaction) rather than committing its own, so a fork or
        # existence race that takes a non-insert branch rolls the claim back
        # instead of stranding a committed, signed claim on the chain.
        now = _now()
        conn = self._conn
        _own_txn = not conn.in_transaction
        if _own_txn:
            conn.execute("BEGIN IMMEDIATE")
        try:
            kind, existing = _resolve(conn)
            if kind == "idempotent":
                finding_id = existing["finding_id"]
                result_claim_id = existing["claim_id"]
                idempotent = True
            else:
                if not _store.plan_exists(conn, plan_id):
                    # The plan is append-only, so this is unreachable in practice;
                    # the re-check keeps the FK insert from ever failing opaquely.
                    raise NoRegisteredPlanError(
                        f"plan {plan_id[:12]}… disappeared between check and write"
                    )
                # Cite the plan attestation in the finding's SIGNED supports[] so
                # the plan -> finding edge is cryptographic, not just denormalised
                # metadata. supports=None is correct for the one-shot assert_finding
                # path, whose synthesised plan (preregistered=0) has no attestation
                # claim; the signed edge exists only when register_plan wrote one.
                # Run-token precondition: distinct-run independence is
                # meaningless when the run token is the default/None. Flag it
                # loudly (non-blocking) only here, on an actual new write, not
                # on idempotent re-submits or calls about to fork/raise.
                if not generated_by or generated_by == DEFAULT_RUN_TOKEN:
                    from mareforma import health as _health
                    _health.append_health_event(
                        self._root, "submit_finding",
                        generated_by_default=True, content_id=cid[:12],
                    )
                plan_claim_id = _store.get_plan_claim_id(conn, plan_id)
                supports = [plan_claim_id] if plan_claim_id else None
                # The finding's verdict inputs, bound into the SIGNED statement
                # (see _statement.build_statement). The unsigned predicate_payload
                # below stays for readers; this is the copy a verdict re-derives
                # against. estimates_digest commits to the line set's CONTENT so
                # an altered or deleted estimate is caught on read; it is computed
                # here, at signing time, over the same lines insert_finding writes.
                finding_record = {
                    "content_id": cid,
                    "frame_id": proposition.frame_id(),
                    "plan_id": plan_id,
                    "data_ids": sorted(data_id_set),
                    "bearing": primary_bearing.direction.value,
                    "estimates_digest": _store.estimates_digest_from_lines(
                        evidence_lines
                    ),
                }
                predicate_payload = {
                    # v2 binds the model lineage into the signed observed
                    # record (see grounding_signed above); a v1 finding has
                    # lineage on the evidence tree only, which the read side
                    # treats as unverifiable rather than a distinct model.
                    "trust": "finding/v2",
                    "content_id": cid,
                    "frame_id": proposition.frame_id(),
                    "plan_id": plan_id,
                    # Back-compat scalar for single-line readers; the full set
                    # is always in data_ids.
                    "data_id": next(iter(data_id_set)) if single_line else None,
                    "data_ids": sorted(data_id_set),
                    # Normalized read location(s) the finding declares, next
                    # to data_ids so the read side re-checks the grounding
                    # binding against the persisted citation set (this
                    # column is unsigned; the append-only trigger locks it
                    # on a signed row). Omitted (not an empty
                    # list) when no line names a data_source, so a finding
                    # without one is byte-identical to a pre-v0.3.9 finding.
                    **({"data_sources": data_sources} if data_sources else {}),
                    "code_ref": code_ref,
                    "bearing": primary_bearing.direction.value,
                    "bearings": [b.direction.value for b in bearings],
                }
                # This verdict was attested at bind time, on the caller's own
                # object, before the binding rewrote it. Hand the exact record
                # down so the claim write recognises it and does not attest it a
                # second time: the bind step may have stripped the receipt digest
                # the second pass would key on, which would read the observer's
                # own verdict back as a caller declaration.
                self._attested_grounding = grounding_signed
                try:
                    claim_id = self.assert_claim(
                        proposition.text(),
                        generated_by=generated_by,
                        supports=supports,
                        idempotency_key=finding_key,
                        observed_grounding=grounding_signed,
                        finding_record=finding_record,
                        predicate_payload=predicate_payload,
                    )
                finally:
                    self._attested_grounding = None
                finding_id = _store.insert_finding(
                    conn, cid, plan_id, claim_id, bearings, evidence_lines, now,
                    model_lineage=model_lineage_json,
                )
                result_claim_id = claim_id
                idempotent = False
            # Read the derived status inside the transaction so the returned dict
            # is an isolated snapshot of the graph immediately after this write,
            # not a post-commit read that a concurrent finding could have moved.
            view = _store.proposition_status(conn, cid, disclose=self._skips)
            if _own_txn:
                conn.commit()
                # add_claim ran inside this transaction (own_transaction=False),
                # so it deferred the claims.toml backup to the transaction owner.
                # Snapshot the now-committed state here, never the uncommitted
                # rows a rollback would have erased.
                from mareforma.db.core import _backup_claims_toml
                _backup_claims_toml(conn, self._root)
        except BaseException:
            if _own_txn:
                conn.rollback()
            raise

        from mareforma import health as _health
        _health.append_health_event(
            self._root, "submit_finding",
            bearing=primary_bearing.direction.value,
            idempotent=idempotent,
        )

        return {
            "finding_id": finding_id,
            "content_id": cid,
            "plan_id": plan_id,
            "claim_id": result_claim_id,
            "bearing": primary_bearing.to_dict(),
            "bearings": [b.to_dict() for b in bearings],
            "status": view["status"] if view else None,
            "idempotent": idempotent,
            "proposition_status": view,
            # The observed grounding record bound into the signed envelope, or
            # None when no verdict was supplied. Additive: it never changes the
            # bearing or the derived status. On an idempotent reuse, report the
            # verdict actually stored on the reused claim, not the passed one.
            "grounding": (
                self._stored_grounding(result_claim_id)
                if idempotent
                else grounding_signed
            ),
            # The model/method lineage recorded on the finding's evidence lines,
            # or None when no model call was observed. On an idempotent reuse,
            # report the lineage stored on the reused finding, not the passed one.
            "model_lineage": (
                _store.finding_model_lineage(self._conn, finding_id)
                if idempotent
                else (model_lineage.to_dict() if model_lineage is not None else None)
            ),
        }

    def _stored_grounding(self, claim_id: str) -> dict | None:
        """The observed-grounding record stored on a claim, or None.

        Reads the queryable column so an idempotent replay reports what was
        actually persisted and signed, rather than a verdict a later call passed
        but never recorded.
        """
        row = self._conn.execute(
            "SELECT observed_grounding FROM claims WHERE claim_id = ?", (claim_id,)
        ).fetchone()
        if row is None or row["observed_grounding"] is None:
            return None
        import json as _json

        try:
            return _json.loads(row["observed_grounding"])
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _normalize_grounding(grounding) -> dict | None:
        """Coerce a grounding argument into the signed observed-grounding record.

        Accepts a :class:`mareforma.observe.GroundingVerdict` (the normal path),
        a pre-built signed-record dict, or None. Returns the compact record
        (version, grounding, reason, cited_sources, grounded_sources,
        receipt_digest) bound into the signed envelope, or None when no verdict
        was supplied. This does NOT
        cross-check the finding's citation, that is :meth:`_bind_grounding`,
        which the write path calls instead.
        """
        if grounding is None:
            return None
        to_signed = getattr(grounding, "to_signed_dict", None)
        if callable(to_signed):
            return to_signed()
        if isinstance(grounding, dict):
            return grounding
        raise TypeError(
            "grounding must be a GroundingVerdict (from mareforma.observe) or "
            f"None; got {type(grounding).__name__}"
        )

    @staticmethod
    def _finding_citation_sources(evidence_lines) -> tuple[str, ...]:
        """The finding's matchable citation identifiers, for the binding check.

        The union of every line's content-addressed ``data_id`` (a ``sha256:``,
        already canonical) and its normalized ``data_source`` (a read location).
        An agent-attested STRING data_id is excluded: it is an opaque
        token, not a matchable path or content address, and normalizing it would
        require a realpath the read side cannot reproduce (verify-on-read is
        filesystem-free), so binding against it would let an honest claim
        false-flag on a different host. A finding whose only citation is a string
        data_id is the already-flagged soft regime (data_id_string_fallback); its
        verdict is kept as not-applicable rather than bound. Both the persisted
        identifiers (data_sources, and content-addressed data_ids) are exactly
        what the read side re-checks against, so write and read agree by
        construction.
        """
        from mareforma.observe._citation import normalize_identifier
        from mareforma.trust import _store

        out: list[str] = []
        for ln in evidence_lines:
            if _store.is_content_addressed(ln.data_id):
                out.append(ln.data_id)
            if ln.data_source:
                norm_src = normalize_identifier(ln.data_source)
                if norm_src:
                    out.append(norm_src)
        return tuple(dict.fromkeys(out))

    @staticmethod
    def _normalized_data_sources(evidence_lines) -> list[str]:
        """The normalized, deduped, sorted ``data_source`` values for the payload."""
        from mareforma.observe._citation import normalize_identifier

        return sorted(
            {
                norm
                for ln in evidence_lines
                if ln.data_source
                for norm in (normalize_identifier(ln.data_source),)
                if norm
            }
        )

    @staticmethod
    def _attest_grounding(supplied, record: dict) -> dict:
        """The observed-grounding record to STORE, taken from the observer.

        ``supplied`` is what the caller handed in (a verdict object on the
        finding path, the record itself on the claim path) and ``record`` is that
        value in signed-record shape.

        The observed axis is the one signal on a claim that is not the producer's
        own word, so this is where the write path stops taking it on the
        producer's word. Three inputs, one rule:

        - a :class:`~mareforma.observe.GroundingVerdict` the observer computed:
          the SNAPSHOT taken when the observer minted it is stored. Not
          ``record``, which is a re-serialization of the caller's live object:
          the object is frozen, but ``object.__setattr__`` reaches through a
          frozen dataclass, and a re-serialization would carry whatever it was
          made to say. The snapshot cannot;
        - a dict whose ``receipt_digest`` the observer emitted (the documented
          ``obs.verdict.to_signed_dict()`` call): the OBSERVER'S record for that
          digest is stored, not the caller's copy, so a hand-edited state on a
          real digest is discarded rather than signed;
        - anything else: a declaration. It is marked and its GROUNDED claim is
          neutralised (see :func:`~mareforma.observe._verdict.declared_record`).

        The boundary is this process, and it is drawn by the digest, not by the
        process id: a record carried in from another process reads DECLARED
        UNLESS its receipt digest matches a verdict THIS process minted, which
        happens whenever both observed the same reads. Measured: a child process
        observing a file the parent never touched reads DECLARED in the parent,
        and the same record reads GROUNDED once the parent has observed that file
        itself. So an observe-here / assert-there pipeline degrades to DECLARED,
        and the digest is a bearer token within a process rather than a proof
        that THIS claim is the one the observation was about.

        This covers the write path only. :func:`mareforma.restore` writes
        ``observed_grounding`` straight from ``claims.toml`` and does not pass
        through here, so a record that was neutralised can be exported, edited,
        re-signed by the producer's own key and restored as GROUNDED.
        """
        from mareforma.observe._verdict import (
            declared_record,
            minted_record,
            minted_snapshot,
        )

        snapshot = minted_snapshot(supplied)
        if snapshot is not None:
            return snapshot
        observed = minted_record(record)
        return observed if observed is not None else declared_record(record)

    @staticmethod
    def _annotate_unbound(record: dict) -> dict:
        """Mark a verdict that had no citation to bind against, at most once.

        The finding path binds before it calls :meth:`assert_claim` and the claim
        path checks again, so the marker must be idempotent: a reader tells an
        unexercised binding from a passed one by its presence, not its count.
        """
        from mareforma.observe._binding import UNBOUND_ANNOTATION

        reason = record.get("reason", "")
        if UNBOUND_ANNOTATION in reason:
            return record
        return {**record, "reason": f"{reason} {UNBOUND_ANNOTATION}"}

    def _bind_grounding(
        self, grounding, finding_sources, *, strict, content_id
    ) -> dict | None:
        """Cross-check a verdict's cited set against the finding's citation.

        Returns the signed observed-grounding record to persist. A GROUNDED
        verdict whose cited set is disjoint from ``finding_sources`` is not stored
        as GROUNDED: in strict mode this raises
        :class:`GroundingCitationMismatchError`; by default it is downgraded to
        OPAQUE with the disjoint reason and a ``grounding_citation_mismatch``
        health event, so a misconfigured producer is visible when drift starts,
        not at audit. A verdict that already matches, or a finding with no
        citation to bind, is returned unchanged (the latter annotated). The check
        is pure string comparison over the normalized identifiers.

        Provenance is settled first, before any binding: what gets bound is the
        record the OBSERVER wrote (see :meth:`_attest_grounding`), so a caller
        cannot bind a verdict it authored itself onto a real citation.
        """
        record = self._normalize_grounding(grounding)
        if record is None:
            return None
        record = self._attest_grounding(grounding, record)

        from mareforma.observe._binding import (
            DISJOINT_REASON,
            BindingState,
            GroundingCitationMismatchError,
            check_grounding_binding,
        )
        from mareforma.observe._verdict import ObservedGrounding

        # Bind against the sources a read was ACTUALLY observed for, not the
        # declared cite set: a producer who lists the dataset in observe(cites=)
        # but reads only an incidental decoy grounds on the decoy, so the declared
        # set would MATCH a finding whose own data never flowed. ``grounded_sources``
        # is that read-observed subset (empty for a hand-built verdict that names
        # none, which correctly cannot demonstrate binding).
        verdict_grounded = tuple(record.get("grounded_sources") or ())
        result = check_grounding_binding(verdict_grounded, finding_sources)

        if result.state is BindingState.MATCHED:
            return record
        if result.state is BindingState.NOT_APPLICABLE:
            # Nothing to bind against; keep the verdict and annotate so a reader
            # sees the binding was not exercised rather than silently assumed.
            return self._annotate_unbound(record)

        # DISJOINT. Only a GROUNDED verdict is unsafe to store as-is, an OPAQUE
        # or UNGROUNDED verdict does not promote and does not claim the data
        # arrived, so a mismatched cited set on it is not a false trust signal.
        if record.get("grounding") != ObservedGrounding.GROUNDED.value:
            return record

        if strict:
            raise GroundingCitationMismatchError(
                DISJOINT_REASON + f"; content_id={content_id[:12]}…"
            )

        from mareforma import health as _health
        _health.append_health_event(
            self._root, "grounding_citation_mismatch",
            content_id=content_id[:12],
        )
        # Downgrade to OPAQUE and strip the GROUNDED-specific evidence so the
        # stored record is self-consistent: leaving the original cited/grounded
        # sets and receipt digest would have the OPAQUE record still committing to
        # a GROUNDED receipt an auditor could flag as mutated.
        record = dict(record)
        record["grounding"] = ObservedGrounding.OPAQUE.value
        record["reason"] = DISJOINT_REASON
        record["grounded_sources"] = []
        record["cited_sources"] = []
        record.pop("receipt_digest", None)
        return record

    @_synchronized
    def proposition_status(self, proposition_or_content_id) -> dict | None:
        """The retrieval view for one proposition: derived Status, independence
        counts, and the frame-level contest. Accepts a content_id or a
        :class:`Proposition`. Returns None if the proposition is not registered.
        """
        self._check_open()
        from mareforma.trust import _store

        cid = (
            proposition_or_content_id
            if isinstance(proposition_or_content_id, str)
            else proposition_or_content_id.content_id()
        )
        return _store.proposition_status(self._conn, cid, disclose=self._skips)

    @_synchronized
    def get_proposition(self, content_id: str) -> dict | None:
        """Return the stored proposition row as a dict, or None."""
        self._check_open()
        from mareforma.trust import _store

        row = _store.get_proposition_row(self._conn, content_id)
        return dict(row) if row is not None else None

    @_synchronized
    def query_frame(
        self, frame_id_or_proposition, *, min_status: str | None = None
    ) -> list[dict]:
        """Everything known about a question (frame_id), each with its derived
        view. Accepts a frame_id or a :class:`Proposition`. ``min_status``
        filters to propositions meeting a support floor on the
        UNTESTED < PRELIMINARY < CONVERGENT ladder.
        """
        self._check_open()
        from mareforma.trust import _store

        fid = (
            frame_id_or_proposition
            if isinstance(frame_id_or_proposition, str)
            else frame_id_or_proposition.frame_id()
        )
        return _store.query_frame(
            self._conn, fid, min_status=min_status, disclose=self._skips
        )

    def query_for_llm(
        self,
        text: str | None = None,
        *,
        min_support: str | None = None,
        classification: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        """Same as :meth:`query` but the result is safe to splice into an
        LLM prompt as untrusted data.

        Each result dict's ``text`` and ``comparison_summary`` fields are
        sanitized (zero-width / bidi / control characters stripped, length
        capped at 100k chars) AND wrapped in
        ``<untrusted_data>...</untrusted_data>`` delimiters. The short
        metadata fields ``source_name``, ``generated_by``, ``validated_by``
        are sanitized but not wrapped: they are short labels, not
        free-form text. Every other string value is sanitized and has
        forged ``<untrusted_data>`` delimiters replaced by ``[stripped]``,
        without a wrapper, since several of them (``evidence_json``,
        ``predicate_payload``, ``observed_grounding``, the ``*_json``
        columns) are JSON the caller parses. Non-string values pass
        through unchanged.

        One consequence: the signature fields in this view are cleaned
        text, not the signed bytes. Read them from :meth:`query` when the
        signature has to verify.

        The caller must still tell the LLM in the system prompt that
        everything inside ``<untrusted_data>`` is data, not instructions.
        This method provides the safe content; the prompt contract is
        the caller's responsibility.

        See :mod:`mareforma.prompt_safety` for the underlying primitives.
        """
        from mareforma import prompt_safety as _ps

        rows = self.query(
            text=text,
            min_support=min_support,
            classification=classification,
            limit=limit,
        )
        return [_format_row_for_llm(row, _ps) for row in rows]

    @_synchronized
    def validate(
        self,
        claim_id: str,
        *,
        validated_by: str | None = None,
        evidence_seen: list[str] | None = None,
    ) -> None:
        """Promote a REPLICATED claim to ESTABLISHED (human validation).

        Identity check
        --------------
        The graph must have a loaded signer (open with ``key_path=...`` or
        run ``mareforma bootstrap`` once) AND that key must be enrolled in
        the project's ``validators`` table. The first key opened on a
        fresh graph auto-enrolls as the root; additional validators are
        added via ``mareforma validator add`` (CLI) or
        :func:`mareforma.validators.enroll_validator`.

        The validation event is itself signed (binding claim_id +
        validator_keyid + validated_at + evidence_seen). The signed
        envelope is stored on the row's ``validation_signature`` column
        so the promotion is independently verifiable.

        Parameters
        ----------
        claim_id:
            UUID of the claim to promote.
        validated_by:
            Optional human-readable label stored alongside the keyid.
            The validator's keyid is the real identity; this string is
            for display only.
        evidence_seen:
            Optional list of claim_ids the validator declares to have
            reviewed before signing. ``None`` is normalized to ``[]``
            (the explicit "I reviewed nothing" admission) and bound
            into the signed envelope. Every non-empty entry must be a
            strict-v4 UUID matching an existing claim with
            ``created_at <= validated_at``; otherwise
            :class:`mareforma.db.EvidenceCitationError` is raised before
            any state change.

            The validator's enumeration is self-declared. mareforma
            cannot prove the validator actually opened the cited claims,
            but the field shifts "a human pressed a button" to "a human
            pressed a button AND named the evidence they consulted." A
            validator who consistently signs ``evidence_seen=[]`` leaves
            an audit-visible trail of unreviewed promotions.

        Raises
        ------
        ClaimNotFoundError
            If claim_id does not exist.
        ValueError
            If support_level is not 'REPLICATED', or the graph has no
            loaded signer, or the loaded signer is not enrolled as a
            validator on this project.
        EvidenceCitationError
            If any ``evidence_seen`` entry is malformed, points to a
            non-existent claim, or post-dates the validation timestamp.
        InvalidValidationEnvelopeError
            If the signed envelope produced by the loaded signer fails
            any mareforma-level structural or cryptographic gate
            (malformed payload, non-enrolled signer, wrong payloadType,
            signature verification failure, or payload-field mismatch
            against the row being promoted). Should not fire on the
            standard wrapper path (the wrapper builds the envelope
            from the same kwargs it threads through), but is listed
            for completeness because the underlying
            :func:`mareforma.db.validate_claim` defends against
            a bypass at this layer too.
        LLMValidatorPromotionError
            If the loaded signer is enrolled with ``validator_type='llm'``.
            LLM-typed validators can sign validation envelopes but
            cannot promote past REPLICATED. Have a human-typed
            validator call :meth:`validate` instead.
        SelfValidationError
            If the loaded signer's keyid equals the claim's
            ``signature_bundle`` signing keyid. Promotion requires an
            external witnessing validator; self-validation is the
            trivial-loop attack.
        """
        self._check_open()
        from mareforma import signing as _signing
        from mareforma import validators as _validators

        if self._signer is None:
            raise ValueError(
                "graph.validate() requires a loaded signing key. Run "
                "`mareforma bootstrap` once, then open the graph with "
                "the default XDG key path (or pass key_path=... explicitly)."
            )
        keyid = _signing.public_key_id(self._signer.public_key())
        if not _validators.is_enrolled(self._conn, keyid):
            raise ValueError(
                f"Key {keyid[:12]}… is not an enrolled validator on this "
                "project. The first key opened against a fresh graph auto-"
                "enrolls as the root; additional validators must be enrolled "
                "by an already-enrolled key via `mareforma validator add`."
            )

        # CRITICAL: the timestamp signed into the envelope MUST equal the
        # timestamp written to the row. Computing _now() twice (once here
        # and again inside db.validate_claim) would diverge by microseconds
        # and silently defeat the tamper-evidence claim.
        now = _db._now()
        # Normalize evidence_seen, None → []. Always present in the
        # signed envelope so an empty list is an *explicit* statement
        # (the validator reviewed nothing) rather than an absent field.
        evidence_seen_normalized = list(evidence_seen) if evidence_seen else []
        envelope = _signing.sign_validation(
            {
                "claim_id": claim_id,
                "validator_keyid": keyid,
                "validated_at": now,
                "evidence_seen": evidence_seen_normalized,
            },
            self._signer,
        )
        bundle_json = json.dumps(envelope, sort_keys=True, separators=(",", ":"))

        _db.validate_claim(
            self._conn, self._root, claim_id,
            validated_by=validated_by,
            validation_signature=bundle_json,
            validated_at=now,
            evidence_seen=evidence_seen_normalized,
        )

    @_synchronized
    def enroll_validator(
        self,
        pubkey_pem: bytes,
        *,
        identity: str,
        validator_type: str = "human",
    ) -> dict:
        """Enroll a new validator on this project, signed by the loaded key.

        The graph's current signer (which must already be an enrolled
        validator on this project) signs the new validator's enrollment
        envelope and inserts a row. The new validator can then call
        :meth:`validate` on this project's claims.

        The new row is committed before this method returns. There is no
        rollback path: append-only validator history mirrors the
        append-only claim history.

        Parameters
        ----------
        pubkey_pem:
            PEM-encoded SubjectPublicKeyInfo bytes of the new validator's
            Ed25519 public key.
        identity:
            Display label (email, lab name). Bound into the signed
            enrollment envelope. Capped at 256 printable characters;
            control characters are rejected.
        validator_type:
            ``'human'`` (default) or ``'llm'``. Self-declared honesty
            signal bound into the signed enrollment envelope. LLM-typed
            validators may sign validation envelopes but cannot promote
            a claim past REPLICATED: :meth:`validate` refuses them in
            mareforma.

        Raises
        ------
        ValueError
            If no signer is loaded.
        ValidatorNotEnrolledError
            If the current signer is not yet enrolled on this project.
        ValidatorAlreadyEnrolledError
            If the new public key is already in the validators table.
        InvalidIdentityError
            If ``identity`` is empty, too long, or contains control
            characters.
        InvalidValidatorTypeError
            If ``validator_type`` is not ``'human'`` or ``'llm'``.
        """
        self._check_open()
        from mareforma import validators as _validators
        if self._signer is None:
            raise ValueError(
                "graph.enroll_validator requires a loaded signing key. "
                "Run `mareforma bootstrap` once and reopen the graph."
            )
        return _validators.enroll_validator(
            self._conn, self._signer, pubkey_pem,
            identity=identity, validator_type=validator_type,
        )

    @_synchronized
    def require_rekor_witnessing(self) -> dict:
        """Declare that this project's findings must be witnessed by the
        transparency log before they can converge.

        The root validator signs a project-policy envelope; the declaration is
        persisted and emitted to ``claims.toml``. On recovery,
        ``restore(..., enforce_rekor_policy=True)`` verifies this envelope
        against the enrolled root and refuses to mark any signed claim
        convergence-eligible without a verified, claim-bound inclusion proof.

        One-way, and root-only: a project cannot un-require witnessing (mirrors
        validator enrollment), and only the single root of trust may set the
        policy. Idempotent once set. Returns the effective policy dict.

        Raises
        ------
        ProjectPolicyError
            If no signer is loaded, or the loaded signer is not the project's
            single root validator. Subclasses ``ValueError``.
        """
        self._check_open()
        return self._declare_project_policy("the Rekor witnessing policy",
                                            rekor_required=True)

    def _declare_project_policy(
        self,
        what: str,
        *,
        rekor_required: bool = False,
        strict_promotion_required: bool = False,
    ) -> dict:
        """Root-sign an extension of the project's trust policy.

        The declaration is project-wide and one-way, so the flags asked for are
        unioned with the stored ones and signed together: extending a policy
        never drops a rule an earlier declaration recorded. Each flag keeps the
        time it was first declared, so extending the policy cannot restate an
        older rule as newer than it is. Idempotent, a declaration that adds
        nothing returns the stored policy untouched. ``what`` names the rule in
        the refusal messages.
        """
        if self._signer is None:
            raise _db.ProjectPolicyError(
                f"Declaring {what} needs the project's root signing key. "
                "Reopen the graph with key_path pointing at it."
            )
        from mareforma import signing as _signing
        from mareforma import validators as _validators
        from mareforma.db.core import _now
        signer_keyid = _signing.public_key_id(self._signer.public_key())
        root_keyid = _validators.trust_domain_root(self._conn)
        if root_keyid is None or signer_keyid != root_keyid:
            raise _db.ProjectPolicyError(
                f"Only the project's root validator may declare {what}. "
                "Reopen with the root key."
            )
        existing = _db.get_project_policy(self._conn)
        had_rekor, had_strict = _db.project_policy_flags(existing)
        rekor_required = rekor_required or had_rekor
        strict_promotion_required = strict_promotion_required or had_strict
        if existing is not None and (had_rekor, had_strict) == (
            rekor_required, strict_promotion_required
        ):
            return existing
        created_at = _now()
        # A flag already declared keeps its own start time; one this call adds
        # starts now. Without that, extending the policy would restamp the
        # older rule and any check grandfathering on it would skip everything
        # written between the two declarations.
        was_rekor_at, was_strict_at = _db.project_policy_declared_at(existing)
        rekor_declared_at = was_rekor_at or (
            created_at if rekor_required else None
        )
        strict_declared_at = was_strict_at or (
            created_at if strict_promotion_required else None
        )
        envelope = _signing.sign_project_policy(
            {
                "version": _signing._PROJECT_POLICY_VERSION,
                "rekor_required": rekor_required,
                "strict_promotion_required": strict_promotion_required,
                "created_at": created_at,
                "rekor_declared_at": rekor_declared_at,
                "strict_promotion_declared_at": strict_declared_at,
            },
            self._signer,
        )
        return _db.set_project_policy(
            self._conn, self._root,
            envelope=json.dumps(envelope, sort_keys=True, separators=(",", ":")),
            signer_keyid=signer_keyid,
            rekor_required=rekor_required,
            strict_promotion_required=strict_promotion_required,
            created_at=created_at,
            rekor_declared_at=rekor_declared_at,
            strict_promotion_declared_at=strict_declared_at,
        )

    @_synchronized
    def list_validators(self) -> list[dict]:
        """Return the validator rows, ordered by enrollment time.

        Each row carries ``verified``: False when its enrollment chain does not
        walk back to the project's single self-signed root, so a planted row is
        never reported as an enrollment.
        """
        self._check_open()
        from mareforma import validators as _validators
        return _validators.list_validators_verified(self._conn)

    @_synchronized
    def refresh_convergence(self) -> dict[str, int]:
        """Retry convergence detection for every flagged claim.

        Convergence detection (PRELIMINARY → REPLICATED promotion) runs
        after a successful claim INSERT. When a SQLite trigger or
        contention pattern causes that detection to raise, mareforma
        swallows the error so the write never crashes, logs a WARNING,
        increments :attr:`convergence_errors`, and sets
        ``convergence_retry_needed = 1`` on the affected claim.

        This method walks every flagged row, re-runs detection, and
        clears the flag on success. Failed retries stay flagged and are
        eligible for the next call. A single error on retry increments
        :attr:`convergence_errors` again, mirroring the original
        swallowed-error semantics.

        Returns
        -------
        dict
            ``{"checked", "retried_ok", "promoted", "still_pending"}``:
            int counts. ``checked`` is the total rows examined;
            ``retried_ok`` is the number that ran detection cleanly this
            pass (the flag was cleared); ``promoted`` is the subset of
            those whose support level actually moved, so a claim with no
            converging peer recovers cleanly and counts zero promotions;
            ``still_pending`` is the number that errored again and remain
            flagged.

        Side effects: only the per-claim flag column and (transitively)
        the convergence-detection promotions themselves are mutated.
        Signed predicate fields are unchanged.
        """
        self._check_open()

        flagged = _db.list_convergence_retry_claims(self._conn)

        checked = len(flagged)
        retried_ok = 0
        promoted = 0
        still_pending = 0

        with self.defer_backup():
            for row in flagged:
                try:
                    supports = json.loads(row.get("supports_json") or "[]")
                except (json.JSONDecodeError, TypeError):
                    supports = []
                generated_by = row.get("generated_by") or DEFAULT_RUN_TOKEN
                artifact_hash = row.get("artifact_hash")
                claim_id = row["claim_id"]

                def _bump(_exc: Exception) -> None:
                    self._convergence_errors += 1

                ok = _db._maybe_update_replicated(
                    self._conn,
                    claim_id,
                    supports,
                    generated_by,
                    artifact_hash,
                    on_error=_bump,
                    strict_promotion=self._strict_promotion,
                )
                if ok:
                    _db.clear_convergence_retry_flag(
                        self._conn, self._root, claim_id,
                    )
                    retried_ok += 1
                    # The helper returns clean-or-swallowed-error, never
                    # whether a row was promoted, so read the support level
                    # back. Detection that ran and moved nothing (the common
                    # case: no converging peer) must not report a promotion.
                    if self._support_level(claim_id) != row["support_level"]:
                        promoted += 1
                else:
                    still_pending += 1

        return {
            "checked": checked,
            "retried_ok": retried_ok,
            "promoted": promoted,
            "still_pending": still_pending,
        }

    def _support_level(self, claim_id: str) -> str | None:
        """The claim's current support level, or None when it is gone.

        Reads the column directly so a retry pass can tell a promotion from a
        clean run that moved nothing.
        """
        row = self._conn.execute(
            "SELECT support_level FROM claims WHERE claim_id = ?", (claim_id,)
        ).fetchone()
        return row["support_level"] if row is not None else None

    def classify_supports(
        self, values: list[str],
    ) -> list[dict[str, str]]:
        """Classify each entry as ``claim`` | ``doi`` | ``external``.

        Thin wrapper over :func:`mareforma.db.classify_supports`. Returns
        ``[{"value": ..., "type": ...}, ...]`` in input order.
        Mareforma uses this same classification for cycle detection,
        REPLICATED anchoring, dangling-reference audit, and JSON-LD
        export. Exposed publicly so callers can introspect what
        mareforma sees for any candidate list before insertion.

        Pure-function: no network, no database read. Same input always
        yields the same output.
        """
        return _db.classify_supports(values)

    @_synchronized
    def query_provenance(
        self,
        claim_id: str,
        *,
        depth: int = 4,
    ) -> dict:
        """Return a structured provenance lineage for *claim_id*.

        The returned object is the agent-readable interface to
        mareforma. It snapshots, in one deterministic shape:

        * the focal claim's identity, classification, support_level,
          status, GRADE evidence vector, asserter, and role
          attestations (the signatures in the DSSE envelope)
        * a recursive upstream chain (``supports[]`` walked to *depth*
          hops via the rebuildable :mod:`mareforma._supports` cache)
        * inbound contradictions (claims this one contradicts and
          claims that contradict it, including signed
          ``contradiction_verdicts`` rows)
        * the replication signal (which clusters this claim sits in,
          via ``replication_verdicts``)
        * a transparency-log slice (Rekor inclusion proofs for the
          focal claim and its ancestors)

        The shape is intentionally JSON-serialisable end-to-end so the
        caller can feed it directly to a downstream agent prompt,
        attach it to a PROV-O export, or persist it as audit evidence.
        No fields are post-processed beyond denormalisation; every
        signed envelope is returned verbatim from the row so consumers
        can independently re-verify against the enrolled validators.

        Parameters
        ----------
        claim_id
            UUIDv4 claim identifier to anchor the walk on.
        depth
            Maximum recursive hops to follow into the upstream chain.
            Bounded at the cache walker level; ``depth=0`` returns the
            focal claim and metadata only (no upstream chain).

        Returns
        -------
        dict
            ``{"claim", "upstream", "downstream", "contradictions",
            "replication", "transparency"}``. ``claim`` carries the
            focal row + role attestations; ``upstream`` /
            ``downstream`` are lists of ``{"claim_id", "depth",
            "position", "row"}`` entries.

        Raises
        ------
        ClaimNotFoundError
            If *claim_id* does not exist in the graph.
        """
        self._check_open()
        from mareforma import _supports

        # claim_id is interpolated into a LIKE pattern below; an
        # attacker-controlled claim_id containing % or _ wildcards
        # would force a full-table scan. Validate UUID shape up front
        # so the LIKE pattern is constrained to a hex-only payload.
        if not _db._is_claim_id(claim_id):
            raise _db.ClaimNotFoundError(
                f"Claim '{claim_id}' is not a valid claim_id; cannot "
                "build lineage."
            )

        # query_provenance is an audit surface, so it FLAGS each high-trust
        # row's verify-on-read result rather than excluding a tampered row:
        # an auditor must be able to see a forged ESTABLISHED/REPLICATED row
        # and know it failed verification. One cache for the whole walk, the
        # focal row included, so a signature is checked once per call.
        prov_verify_cache: dict = {}

        focal = _db.get_claim(
            self._conn, claim_id, verify_cache=prov_verify_cache,
        )
        if focal is None:
            raise _db.ClaimNotFoundError(
                f"Claim '{claim_id}' not found; cannot build lineage."
            )

        # Signers on the DSSE envelope. claim:v1 has one (the
        # asserter); claim-with-roles:v1 has N (planner / executor /
        # reviewer / validator). The keyid IS cryptographically bound
        # (each signature is verified over the PAE on disk during
        # restore). The ``role`` string sits on the signature entry
        # and is NOT covered by the signed payload bytes, see
        # :func:`mareforma.signing.sign_claim_with_roles` for the
        # trust boundary. The field is exposed here as
        # ``role_attestations_unverified`` so callers can't mistake
        # the role tag for a mareforma guarantee.
        role_attestations_unverified: list[dict] = []
        if focal.get("signature_bundle"):
            try:
                bundle = json.loads(focal["signature_bundle"])
                for sig in bundle.get("signatures", []) or []:
                    if isinstance(sig, dict):
                        role_attestations_unverified.append({
                            "keyid": sig.get("keyid"),
                            "role_unverified": sig.get("role"),
                        })
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass

        # Upstream / downstream walks via the rebuildable cache.
        upstream_edges = (
            _supports.walk_upstream(self._conn, claim_id, depth=depth)
            if depth >= 1 else []
        )
        downstream_edges = (
            _supports.walk_downstream(self._conn, claim_id, depth=depth)
            if depth >= 1 else []
        )

        def _hydrate(edges: list[dict]) -> list[dict]:
            if not edges:
                return []
            # Batched fetch: one query per ~999 edges instead of one
            # query per edge. SQLite's variable-count cap is 999 in
            # most builds; chunk the IN-list to stay under it.
            ids = list({e["claim_id"] for e in edges})
            rows_by_id: dict[str, dict] = {}
            chunk_size = 900
            for i in range(0, len(ids), chunk_size):
                chunk = ids[i:i + chunk_size]
                placeholders = ",".join("?" * len(chunk))
                cursor = self._conn.execute(
                    f"SELECT {_db._CLAIM_SELECT} FROM claims "
                    f"WHERE claim_id IN ({placeholders})",
                    chunk,
                )
                for row in cursor.fetchall():
                    rd = dict(row)
                    rd["verified"] = _db._row_verified_on_read(
                        self._conn, rd, prov_verify_cache,
                    )
                    rows_by_id[row["claim_id"]] = rd
            return [
                {
                    "claim_id": e["claim_id"],
                    "depth": e["depth"],
                    "position": e["position"],
                    "row": rows_by_id.get(e["claim_id"]),
                }
                for e in edges
            ]

        # Inbound contradictions: claims that list this one in their
        # contradicts[] array. Uses json_each so SQLite can scan the
        # JSON values directly instead of falling back to LIKE-based
        # substring match on every row. Still O(N) in the absence of a
        # reverse-cache table (deferred future work), but the json_each
        # form is friendlier to future expression-index work.
        contradicts_back: list[str] = []
        try:
            inbound = self._conn.execute(
                "SELECT DISTINCT c.claim_id FROM claims c, "
                "json_each(c.contradicts_json) je "
                "WHERE je.value = ?",
                (claim_id,),
            ).fetchall()
            contradicts_back = [r["claim_id"] for r in inbound]
        except sqlite3.OperationalError:
            # Fallback for SQLite builds without json1 (vanishingly rare
            # on the documented ≥3.30 floor, but cheap insurance).
            for r in self._conn.execute(
                "SELECT claim_id, contradicts_json FROM claims "
                "WHERE contradicts_json LIKE ?",
                (f'%"{claim_id}"%',),
            ).fetchall():
                try:
                    if claim_id in json.loads(r["contradicts_json"] or "[]"):
                        contradicts_back.append(r["claim_id"])
                except (json.JSONDecodeError, TypeError):
                    continue

        # query_provenance is an AUDIT surface; show the verdicts that
        # invalidated the focal claim. Without include_invalidated=True
        # a signed contradiction verdict against this claim would be
        # filtered out, exactly the verdict the operator needs to see
        # when investigating provenance of an invalidated claim.
        verdicts_for = _db.list_contradiction_verdicts(
            self._conn, claim_id=claim_id, include_invalidated=True,
        )
        repl_verdicts = _db.list_replication_verdicts(
            self._conn, member_claim_id=claim_id,
            include_invalidated=True,
        )

        # Operational log: this is a queryable signal, emit one event.
        from mareforma import health as _health
        _health.append_health_event(
            self._root, "provenance_query", depth=depth,
        )

        return {
            "claim": {
                **focal,
                "role_attestations_unverified": (
                    role_attestations_unverified
                ),
            },
            "upstream": _hydrate(upstream_edges),
            "downstream": _hydrate(downstream_edges),
            "contradictions": {
                "this_contradicts": json.loads(
                    focal.get("contradicts_json") or "[]"
                ),
                "contradicted_by": contradicts_back,
                "signed_verdicts": verdicts_for,
            },
            "replication": repl_verdicts,
            "depth": depth,
        }

    @_synchronized
    def find_dangling_supports(self) -> list[dict]:
        """Return UUID-shaped ``supports[]`` entries that point nowhere.

        A "dangling" reference is a UUID-shaped entry in some claim's
        ``supports[]`` whose claim_id does not exist in this graph. DOIs
        and other free-form strings are external references and are NOT
        flagged: only UUID-shaped strings that look like local claim_ids
        but resolve to no row.

        Returns ``[{"claim_id", "dangling_ref"}, ...]`` sorted
        deterministically. Empty list when the graph is clean.

        Mareforma accepts dangling references at assertion time by
        design: a ``supports`` entry could legitimately reference a
        claim from another project or a not-yet-asserted upstream. This
        helper is for auditing integrity, not for blocking writes.
        REPLICATED detection already refuses to promote on a dangling
        reference, so a hanging arrow cannot trigger spurious promotion.

        Raises
        ------
        mareforma.DatabaseError
            If the audit query fails (an unparseable ``supports_json`` planted
            by a hand-edit is the reachable case). The raw sqlite3 error is not
            part of the public surface, so it is translated rather than leaked.
        """
        self._check_open()
        try:
            return _db.find_dangling_supports(self._conn)
        except sqlite3.Error as exc:
            raise _db.DatabaseError(
                f"Failed to audit dangling supports: {exc}",
            ) from exc

    @_synchronized
    def refresh_unsigned(self) -> dict[str, int]:
        """Retry Rekor submission for every signed-but-not-logged claim.

        For each claim whose
        ``signature_bundle`` is non-NULL and whose ``transparency_logged``
        is 0, the original envelope is re-submitted to the Rekor URL the
        graph was opened with. Success updates the bundle (attaches the
        log entry coordinates) and flips ``transparency_logged`` to 1; the
        REPLICATED check fires inside the same transaction.

        No-op modes
        -----------
        - If the graph was opened without ``rekor_url``, returns immediately:
          there is no log to submit to. The result reports zero checked.
        - If a row has a malformed ``signature_bundle`` (manual edit,
          partial restore from claims.toml), it is quarantined as still
          unlogged with a warning.

        Returns
        -------
        dict
            ``{"checked": N, "logged": M, "still_unlogged": K}``.
        """
        self._check_open()
        if self._rekor_url is None:
            return {"checked": 0, "logged": 0, "still_unlogged": 0}

        import warnings
        from mareforma import signing as _signing

        unlogged = _db.list_unlogged_claims(self._conn)
        logged_count = 0
        still_unlogged = 0

        # If the user lacks a signer, we cannot rebuild the public key from
        # the bundle alone for Rekor's hashedrekord schema (it needs the
        # PEM). Return early with a warning.
        if self._signer is None:
            if unlogged:
                warnings.warn(
                    f"refresh_unsigned() found {len(unlogged)} unlogged claims "
                    "but the graph was opened without a key. Open with "
                    "key_path=... to retry the Rekor submission.",
                    stacklevel=2,
                )
            return {
                "checked": len(unlogged),
                "logged": 0,
                "still_unlogged": len(unlogged),
            }

        public_key = self._signer.public_key()
        current_keyid = _signing.public_key_id(public_key)

        with self.defer_backup():
            for claim in unlogged:
                cid = claim["claim_id"]
                try:
                    envelope = json.loads(claim["signature_bundle"])
                except (json.JSONDecodeError, TypeError):
                    warnings.warn(
                        f"Claim {cid} has a malformed signature_bundle; "
                        "skipping during refresh_unsigned.",
                        stacklevel=2,
                    )
                    still_unlogged += 1
                    continue

                # Key-rotation guard. If the user ran `mareforma bootstrap
                # --overwrite` since the claim was signed, this graph's signer
                # cannot re-submit on the old key's behalf. Rekor would reject
                # the public-key vs signature mismatch every time; warn and
                # skip so the operator notices instead of retrying forever.
                try:
                    bundle_keyid = envelope["signatures"][0]["keyid"]
                except (KeyError, IndexError, TypeError):
                    warnings.warn(
                        f"Claim {cid} signature_bundle has no keyid; skipping.",
                        stacklevel=2,
                    )
                    still_unlogged += 1
                    continue
                if bundle_keyid != current_keyid:
                    warnings.warn(
                        f"Claim {cid} was signed by keyid {bundle_keyid[:12]}… "
                        f"but the current signer is {current_keyid[:12]}…. The "
                        "old key must be restored to re-log this claim. Skipping.",
                        stacklevel=2,
                    )
                    still_unlogged += 1
                    continue

                # Drift guard. If the row was tampered after assert_claim, the
                # envelope's signed payload no longer matches the live row.
                # Submitting it to Rekor would create a permanent public record
                # of a claim text that no longer exists locally. Compare the
                # canonical re-derivation of the live row against the envelope
                # payload bytes.
                try:
                    payload_bytes = base64.standard_b64decode(envelope["payload"])
                except (KeyError, TypeError, ValueError):
                    warnings.warn(
                        f"Claim {cid} signature_bundle payload could not be "
                        "decoded; skipping during refresh_unsigned.",
                        stacklevel=2,
                    )
                    still_unlogged += 1
                    continue
                # The signed payload is a canonical in-toto Statement v1
                # whose predicate carries the evidence vector. Re-derive
                # with the row's stored evidence_json so a row+envelope
                # drift detector compares like-with-like.
                try:
                    evidence_dict = json.loads(claim.get("evidence_json") or "{}")
                except (ValueError, TypeError):
                    evidence_dict = {}
                live_fields = {
                    "claim_id": cid,
                    "text": claim["text"],
                    "classification": claim["classification"],
                    "generated_by": claim["generated_by"],
                    "supports": json.loads(claim.get("supports_json") or "[]"),
                    "contradicts": json.loads(claim.get("contradicts_json") or "[]"),
                    "source_name": claim.get("source_name"),
                    "artifact_hash": claim.get("artifact_hash"),
                    "created_at": claim["created_at"],
                }
                # A grounded claim binds its observed_grounding verdict into the
                # signed payload, so the re-derivation must include it or the drift
                # guard fires on every untampered grounded row. Add the key only when
                # present, mirroring the restore path, so pre-observer claims stay
                # byte-identical.
                from mareforma.db.restore import _parse_observed_grounding

                observed_grounding = _parse_observed_grounding(
                    claim.get("observed_grounding")
                )
                if observed_grounding is not None:
                    live_fields["observed_grounding"] = observed_grounding
                # A finding binds its verdict inputs into the signed payload too;
                # the record has no row column, so re-derive it from the envelope
                # itself. Absent for non-finding and pre-record claims, so their
                # re-derivation stays byte-identical.
                try:
                    _pred = _signing.claim_predicate_from_envelope(envelope)
                    finding_record = (
                        _pred.get("finding_record") if isinstance(_pred, dict)
                        else None
                    )
                except Exception:
                    finding_record = None
                if isinstance(finding_record, dict):
                    live_fields["finding_record"] = finding_record
                live_payload = _signing.canonical_statement(live_fields, evidence_dict)
                if live_payload != payload_bytes:
                    warnings.warn(
                        f"Claim {cid} row drifted from its signed payload; "
                        "refusing to log a stale signature to Rekor. "
                        "Investigate the row vs signature_bundle mismatch.",
                        stacklevel=2,
                    )
                    still_unlogged += 1
                    continue

                # Step-4-replay path. If the Rekor saga's sidecar INSERT
                # succeeded but the claims-row UPDATE failed (213 design),
                # rekor_inclusions has the entry for this claim. Replay the
                # UPDATE from stored coords instead of submitting again to
                # avoid creating a duplicate Rekor entry.
                #
                # Placed AFTER the drift guard so a tampered row cannot ride
                # the sidecar replay to re-attach valid Rekor coords to
                # invalid payload bytes. The drift guard refusal is uniform
                # across both the replay and re-submit paths, there is no
                # way to launder a stale signature through this method.
                saved_entry = _db.get_rekor_inclusion(self._conn, cid)
                if saved_entry is not None:
                    augmented = _signing.attach_rekor_entry(envelope, saved_entry)
                    new_bundle = json.dumps(
                        augmented, sort_keys=True, separators=(",", ":"),
                    )
                    _db.mark_claim_logged(
                        self._conn, self._root, cid, new_bundle,
                        strict_promotion=self._strict_promotion,
                    )
                    logged_count += 1
                    continue

                logged, entry = _signing.submit_to_rekor(
                    envelope, public_key, rekor_url=self._rekor_url,
                    allow_insecure=self._trust_insecure_rekor,
                )
                if logged and entry is not None:
                    # Merkle inclusion-proof verification (opt-in). Mirrors
                    # the submit-time path in db._attempt_rekor_saga: when
                    # the graph was opened with a log pubkey, re-fetch the
                    # entry and cryptographically verify before persisting.
                    # On verification failure, the entry stays unlogged
                    # (the operator can retry once they investigate).
                    proof_entry = None
                    if self._rekor_log_pubkey_pem is not None:
                        entry_uuid = entry.get("uuid")
                        if not isinstance(entry_uuid, str) or not entry_uuid:
                            warnings.warn(
                                f"Claim {cid} submitted to Rekor but the "
                                "response had no uuid; cannot verify "
                                "inclusion proof. Leaving unlogged.",
                                stacklevel=2,
                            )
                            still_unlogged += 1
                            continue
                        try:
                            full_body = _signing.fetch_inclusion_proof(
                                entry_uuid, self._rekor_url,
                                allow_insecure=self._trust_insecure_rekor,
                            )
                            _signing.verify_rekor_inclusion(
                                full_body, self._rekor_log_pubkey_pem, envelope,
                            )
                            proof_entry = full_body
                        except _signing.RekorInclusionError as exc:
                            warnings.warn(
                                f"Claim {cid} inclusion-proof verification "
                                f"failed (uuid {entry_uuid}, reason="
                                f"{exc.reason}). Leaving unlogged; refresh "
                                "again after investigating.",
                                stacklevel=2,
                            )
                            still_unlogged += 1
                            continue
                    # Saga step 3 (sidecar write) BEFORE step 4 (row UPDATE),
                    # mirroring _attempt_rekor_saga in db.py. Without this,
                    # a mark_claim_logged failure (drift refusal, transient
                    # IntegrityError, contention) would leave the entry in
                    # Rekor with no local sidecar record; the next
                    # refresh_unsigned would re-submit and create a duplicate
                    # Rekor entry. Writing the sidecar first lets the next
                    # refresh route through the saved_entry replay path
                    # above instead.
                    if not _db._record_rekor_inclusion(
                        self._conn, cid, entry, proof_entry=proof_entry,
                    ):
                        # Sidecar write itself failed (rare; emits its own
                        # warning). Leave the row unlogged, refresh_unsigned
                        # will retry, accepting the duplicate-Rekor-entry
                        # risk documented in _record_rekor_inclusion.
                        still_unlogged += 1
                        continue
                    augmented = _signing.attach_rekor_entry(envelope, entry)
                    new_bundle = json.dumps(
                        augmented, sort_keys=True, separators=(",", ":"),
                    )
                    _db.mark_claim_logged(self._conn, self._root, cid, new_bundle,
                                          strict_promotion=self._strict_promotion)
                    logged_count += 1
                else:
                    still_unlogged += 1

        from mareforma import health as _health
        _health.append_health_event(
            self._root, "refresh_unsigned",
            succeeded=logged_count,
            checked=len(unlogged),
        )
        return {
            "checked": len(unlogged),
            "logged": logged_count,
            "still_unlogged": still_unlogged,
        }

    def get_tools(
        self, *, generated_by: str = DEFAULT_RUN_TOKEN,
        include_deprecated_aliases: bool = False,
    ) -> list:
        """Return agent tool callables pre-bound to this graph.

        Returns two plain Python functions that any agent framework can wrap.
        ``generated_by`` is baked into the closure as a display and provenance
        label on each claim. REPLICATED independence keys on the signing key
        (``asserter_keyid``), not on that label, and every tool from one
        binding signs with the key the graph was opened with: all claims
        recorded through it share one asserter keyid. Independent lines need a
        graph handle per agent, each opened with its own key, or
        ``graph.assert_claim(..., signer=...)`` with distinct keys.

        Parameters
        ----------
        generated_by:
            Agent identifier, e.g. ``"agent/model-a/lab_a"``, carried as a
            display label. Defaults to ``'agent'``. It plays no part in the
            trust axes.
        include_deprecated_aliases:
            When True, appends a deprecated ``assert_finding`` tool that
            forwards to ``record_claim`` and warns on use. The LLM-facing
            claim-recording tool was renamed from ``assert_finding`` to
            ``record_claim`` because the old name collided with
            :meth:`EpistemicGraph.assert_finding` (a different, one-shot
            finding path). The alias is kept for one release; leave this
            False for the clean two-tool surface.

        Returns
        -------
        list
            ``[query_graph, record_claim]`` (plus a deprecated
            ``assert_finding`` alias when ``include_deprecated_aliases``).

        Note
        ----
        The returned callables are bound to this graph instance. Using
        them after ``graph.close()`` raises ``RuntimeError`` with a
        message pointing back at ``mareforma.open(...)``.

        Example
        -------
        >>> tools = graph.get_tools(generated_by="agent/claude-sonnet-4-6/lab_a")
        >>> # LangChain
        >>> lc_tools = [tool(fn) for fn in tools]
        >>> # Anthropic SDK: pass to tools= in client.messages.create()
        """
        self._check_open()

        def query_graph(topic: str, min_support: str | None = None) -> str:
            """Query the epistemic graph for what is already established about a topic.

            Call this BEFORE asserting any new finding. If REPLICATED or ESTABLISHED
            findings exist, build on them using DERIVED classification with their
            claim_ids in supports=[]. Returns a JSON list of matching claims.

            Parameters
            ----------
            topic:
                Substring to search for in claim text (case-insensitive).
            min_support:
                Minimum trust level: PRELIMINARY, REPLICATED, or ESTABLISHED.
                Defaults to no filter. It used to default to ``PRELIMINARY``,
                which is the floor and so filtered nothing, but still counted
                as the caller asking for the retired support ladder: every call
                warned about a deprecation the caller had not opted into, and
                the warning named a library default the agent author could not
                change. Passing nothing now means asking for nothing.

            Returns
            -------
            str
                JSON array of claim dicts with keys: text, support_level,
                classification, status, claim_id. The ``text`` field is
                sanitized and wrapped in
                ``<untrusted_data>...</untrusted_data>``: this tool is
                consumed by an LLM, so it routes through the same
                prompt-safety layer as :meth:`query_for_llm`. ``status``
                is surfaced so the LLM can spot editorial taint
                (``contested`` / ``retracted``) even on REPLICATED rows.
            """
            results = self.query_for_llm(topic, min_support=min_support)
            return json.dumps([
                {
                    "text": r["text"],
                    "support_level": r["support_level"],
                    "classification": r["classification"],
                    "status": r["status"],
                    "claim_id": r["claim_id"],
                }
                for r in results
            ])

        def record_claim(
            text: str,
            classification: str = "INFERRED",
            supports: list[str] | None = None,
            contradicts: list[str] | None = None,
            source: str = "",
        ) -> str:
            """Record a new finding in the epistemic graph.

            Use ANALYTICAL only if a real data pipeline ran and returned output.
            Asserting ANALYTICAL on null data is permanently recorded as such.
            Use DERIVED when building explicitly on existing graph claims: cite
            their claim_ids in supports=[]. Use INFERRED for all LLM reasoning.
            Use contradicts= to document explicit tension with existing claims.

            Parameters
            ----------
            text:
                The falsifiable assertion. Cannot be empty.
            classification:
                Epistemic origin: INFERRED (default), ANALYTICAL, or DERIVED.
            supports:
                List of upstream claim_ids this finding rests on.
            contradicts:
                List of claim_ids this finding is in explicit tension with.
            source:
                Data source name. Required for ANALYTICAL to be meaningful.

            Returns
            -------
            str
                The claim_id UUID of the recorded finding.
            """
            return self.assert_claim(
                text,
                classification=classification,
                generated_by=generated_by,
                supports=supports,
                contradicts=contradicts,
                source_name=source or None,
            )

        tools = [query_graph, record_claim]

        if include_deprecated_aliases:
            def assert_finding(
                text: str,
                classification: str = "INFERRED",
                supports: list[str] | None = None,
                contradicts: list[str] | None = None,
                source: str = "",
            ) -> str:
                """Deprecated alias for ``record_claim``.

                The claim-recording agent tool was renamed to
                ``record_claim``; ``assert_finding`` collided with
                :meth:`EpistemicGraph.assert_finding`. This alias forwards
                to ``record_claim`` and is kept for one release.
                """
                from mareforma._deprecation import _emit

                _emit(
                    "The 'assert_finding' agent tool was renamed to "
                    "'record_claim' (the old name shadowed "
                    "EpistemicGraph.assert_finding). Update your tool "
                    "wiring; this deprecated alias will be removed in a "
                    "future release.",
                    3,  # +1 for _emit's own frame
                )
                return record_claim(
                    text,
                    classification=classification,
                    supports=supports,
                    contradicts=contradicts,
                    source=source,
                )

            tools.append(assert_finding)

        return tools

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    @property
    def convergence_errors(self) -> int:
        """Number of swallowed SQLite errors during convergence detection.

        Convergence detection (PRELIMINARY → REPLICATED promotion) runs
        after a successful claim INSERT and swallows SQLite errors so a
        misconfigured trigger or contention pattern can never crash a
        write. A WARNING is logged each time; this counter mirrors that
        log so the failure is observable without log parsing.

        Resets to zero each time the graph is re-opened. A non-zero value
        means at least one assertion since open completed but its
        promotion check did not run cleanly; inspect the warnings in the
        ``mareforma`` logger for details.
        """
        return self._convergence_errors

    @property
    def read_verify_exclusions(self) -> int:
        """Rows :meth:`query` and :meth:`search` dropped as unverifiable.

        A REPLICATED or ESTABLISHED row whose signature does not re-verify
        is excluded from every enumerating read, and no flag brings it back.
        The result is a shorter list that reads exactly like a graph missing
        those claims, so the exclusion is counted here (and appended to
        ``.mareforma/health.jsonl`` as ``read_verify_excluded``).

        Resets to zero each time the graph is re-opened, and counts only the
        reads this session made: zero means nothing was excluded from what was
        read, not that the graph is untampered. A non-zero value means a claim
        on disk failed re-verification; name it with :meth:`get_claim` or
        ``mareforma verify`` to see which.

        The health-log line is rate-limited (written at 1, 2, 4, 8, ...
        occurrences) because the exclusion is a state every later read finds
        again; this counter is exact and is the one to read.
        """
        return self._read_verify_exclusions

    @property
    def read_unverified_exclusions(self) -> int:
        """Rows :meth:`query` and :meth:`search` held back behind the filter.

        A PRELIMINARY claim whose generator key is not in the validators table
        is dropped from an enumerating read unless ``include_unverified=True``.
        Unlike the verify exclusions above, a flag DOES bring these back: they
        are not tampered rows, they are rows the default read does not vouch
        for. Counted so an empty answer can be told from an empty record, which
        is the difference between "there is nothing here" and "there is
        something here you did not ask to see".

        Resets to zero each time the graph is re-opened, and counts only the
        reads this session made.
        """
        return self._read_unverified_exclusions

    @_synchronized
    def health(self) -> dict[str, int]:
        """Single-call audit summary of mareforma state.

        Aggregates the counters operators inspect when they want a
        snapshot of "what's the graph telling me right now?" without
        having to write multiple queries. Pure observability over
        existing surfaces, no side effects.

        Returns
        -------
        dict[str, int]
            ``claim_count``: total claims in the graph (signed and
            unsigned, all support levels, all statuses).
            ``validator_count``: total rows in the validators table
            (every enrolled identity, including LLM-typed).
            ``unresolved_claims``: claims flagged ``unresolved=1``
            (a legacy quarantine flag; blocks REPLICATED promotion).
            ``unsigned_claims``: claims with ``signature_bundle IS
            NULL`` (no Ed25519 envelope; blocks REPLICATED promotion
            and any cross-restore verification).
            ``dangling_supports``: count of UUID-shaped ``supports[]``
            entries pointing to claims that do not exist in the graph
            (returned in detail by :meth:`find_dangling_supports`).
            ``convergence_errors``: current value of the swallowed-
            error counter (see :attr:`convergence_errors`).
            ``convergence_retry_pending``: claims with
            ``convergence_retry_needed=1`` waiting for
            :meth:`refresh_convergence` to re-run detection.

        A "healthy" graph has zeros across ``unresolved_claims``,
        ``unsigned_claims``, ``dangling_supports``,
        ``convergence_errors``, and ``convergence_retry_pending``.
        Non-zero values do not by themselves indicate a defect: they
        indicate something the operator should look at.
        """
        self._check_open()

        def _count(sql: str) -> int:
            row = self._conn.execute(sql).fetchone()
            return int(row[0]) if row is not None else 0

        claim_count = _count("SELECT COUNT(*) FROM claims")
        validator_count = _count("SELECT COUNT(*) FROM validators")
        unresolved_claims = _count(
            "SELECT COUNT(*) FROM claims WHERE unresolved = 1"
        )
        unsigned_claims = _count(
            "SELECT COUNT(*) FROM claims WHERE signature_bundle IS NULL"
        )

        # The column is part of the current schema and ``open_db``
        # column-presence-checks every open, so any reachable conn
        # here has the column. No defensive try/except needed, a
        # missing column would mean a corrupt graph.db, which is the
        # operator-level concern open_db already raises for.
        convergence_retry_pending = _count(
            "SELECT COUNT(*) FROM claims "
            "WHERE convergence_retry_needed = 1"
        )

        dangling_supports = len(_db.find_dangling_supports(self._conn))

        return {
            "claim_count": claim_count,
            "validator_count": validator_count,
            "unresolved_claims": unresolved_claims,
            "unsigned_claims": unsigned_claims,
            "dangling_supports": dangling_supports,
            "convergence_errors": self._convergence_errors,
            "convergence_retry_pending": convergence_retry_pending,
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @_synchronized
    def backup(self) -> None:
        """Write ``claims.toml`` now, the recovery source for ``restore``.

        Each mutation refreshes the backup on its own; call this to force a
        write, for example at the end of a :meth:`defer_backup` batch.
        """
        self._check_open()
        _db._backup_claims_toml(self._conn, self._root)

    @contextmanager
    def defer_backup(self):
        """Group many mutations under a single ``claims.toml`` rewrite.

        Inside the block a mutation marks the backup due rather than rewriting
        the whole file; the write happens once when the block exits, and
        ``claims.toml`` reflects the committed state again after it. Use it for a
        bulk import or a loop of writes. Outside the block each mutation refreshes
        the backup as usual.
        """
        self._check_open()
        _db.suspend_backup(self._conn)
        try:
            yield
        finally:
            _db.resume_backup(self._conn, self._root)

    @_synchronized
    def close(self) -> None:
        """Close the underlying database connection.

        Takes the graph lock like every other mutator: the connection is
        shared across threads, and closing it under another thread's live
        statements crashes the process instead of raising. Waiting here lets
        an in-flight write commit first.

        Subsequent calls on this graph raise ``RuntimeError`` with an
        actionable message instead of leaking a raw
        ``sqlite3.ProgrammingError``.
        """
        if not self._closed:
            # Flush any backup a still-open deferral window left pending, so a
            # graph closed mid-batch still leaves claims.toml current. Drain
            # every nesting level at once, before the connection closes.
            _db._drain_backup_window(self._conn, self._root)
            self._conn.close()
            self._closed = True

    def _check_open(self) -> None:
        """Guard against use after close. Public methods call this first."""
        if self._closed:
            raise RuntimeError(
                "EpistemicGraph is closed. The context manager exited "
                "or .close() was called explicitly. Re-open the graph "
                "with mareforma.open(...) before calling this method."
            )

    def __enter__(self) -> "EpistemicGraph":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"EpistemicGraph(root={self._root})"
