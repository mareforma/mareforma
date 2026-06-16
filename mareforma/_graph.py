"""
_graph.py — EpistemicGraph: agent-native interface to the mareforma epistemic graph.

Usage
-----
  graph = mareforma.open()                         # current directory
  graph = mareforma.open("/path/to/project")       # explicit path
  graph = mareforma.open(Path("my_project"))

  with mareforma.open() as graph:                  # context manager
      claim_id = graph.assert_claim("...", classification="ANALYTICAL")
      results  = graph.query("topic X", min_support="REPLICATED")
      graph.validate(claim_id, validated_by="reviewer@example.org")

Flow
----
  assert_claim()
    ├─ idempotency check (if key provided)
    ├─ validate classification
    ├─ INSERT via db.add_claim()
    └─ REPLICATED check fires inside add_claim()

  query()
    └─ SELECT via db.query_claims() with text/support/classification filters

  validate()
    └─ UPDATE via db.validate_claim() — requires REPLICATED, sets ESTABLISHED
"""

from __future__ import annotations

import base64
import json
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

from mareforma import db as _db
from mareforma import doi_resolver as _doi
# NOTE: mareforma.signing is imported lazily inside refresh_unsigned so that
# unsigned-only users can still open the graph even if the cryptography
# extension fails at import time. Don't promote this to a module-level
# import without weighing that failure-mode tradeoff.

if TYPE_CHECKING:
    import sqlite3
    from mareforma._evidence import EvidenceVector


# Fields that get sanitize-and-wrap for LLM consumption. Free-form text
# the LLM is likely to splice into a reasoning step.
_LLM_WRAP_FIELDS = ("text", "comparison_summary")

# Fields that get sanitize-only — short labels we don't wrap because
# delimiters would add noise without containing anything an attacker
# could realistically use as a multi-line injection payload.
_LLM_SANITIZE_FIELDS = ("source_name", "generated_by", "validated_by")


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
            out[field] = prompt_safety.sanitize_for_llm(out[field])
    return out


class EpistemicGraph:
    """Agent-native interface to a local mareforma epistemic graph.

    Do not instantiate directly — use mareforma.open().
    """

    def __init__(
        self,
        conn: "sqlite3.Connection",
        root: Path,
        *,
        signer: object | None = None,
        signer_identity: str | None = None,
        rekor_url: str | None = None,
        require_rekor: bool = False,
        rekor_log_pubkey_pem: bytes | None = None,
    ) -> None:
        self._conn = conn
        self._root = root
        self._signer = signer
        self._rekor_url = rekor_url
        self._require_rekor = require_rekor
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

        # Bootstrap-of-trust: the first key opened against a fresh project's
        # graph.db auto-enrolls as the root validator. This is silent and
        # idempotent — subsequent opens with the same key are no-ops. New
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
                identity=signer_identity or "root",
            )
            keyid = _signing.public_key_id(signer.public_key())
            if not _validators.is_enrolled(self._conn, keyid):
                import warnings as _warnings
                _warnings.warn(
                    f"Opened project with key {keyid[:12]}… but this key "
                    "is not an enrolled validator (a different key holds "
                    "the root). graph.validate() will refuse until this "
                    "key is enrolled by an existing validator via "
                    "`mareforma validator add`.",
                    stacklevel=2,
                )

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

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
        evidence: "EvidenceVector | dict | None" = None,
        seed: bool = False,
        signer: "object | None" = None,
        predicate_payload: dict | None = None,
        original_signature_bundle: str | None = None,
        grounding_sensor: "object | None" = None,
    ) -> str:
        # signer:
        #     Per-call override for the graph's loaded signer. When
        #     ``None`` (default), the call inherits the signer passed
        #     to ``mareforma.open(key_path=...)``. When supplied, the
        #     claim is signed with this key instead. Note: this does
        #     NOT check that the signer's keyid is enrolled in the
        #     validators table — same trust model as
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
        #     the signed envelope or chain hash — it is a query-side
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
            source_name, artifact_hash). Any mismatch raises
            :class:`mareforma.db.IdempotencyConflictError` — silent
            merging two different claims would discard the second
            author's content and break REPLICATED detection. For
            cross-lab convergence, assert two separate claims that
            share an entry in ``supports[]`` with different
            ``generated_by`` values — that's the path that fires
            REPLICATED honestly.
        generated_by:
            Agent identifier. Use ``"model/version/context"`` format.
            Defaults to ``'agent'``.
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
            payload and used as a parallel REPLICATED signal: two peers
            citing the same upstream that BOTH supply a hash must agree
            on the hash before they converge. Compute with
            ``hashlib.sha256(bytes).hexdigest()``.
        evidence:
            Optional GRADE 5-domain ``EvidenceVector`` declaring the
            asserter's confidence in the evidence backing this claim.
            Accepts either a populated
            :class:`mareforma.EvidenceVector` instance or a dict in the
            same shape as :meth:`EvidenceVector.to_dict`. Five downgrade
            domains in ``[-2, 0]`` (``risk_of_bias``, ``inconsistency``,
            ``indirectness``, ``imprecision``, ``publication_bias``),
            three upgrade flags (``large_effect``, ``dose_response``,
            ``opposing_confounding``), a ``rationale`` dict (required for
            any nonzero domain — the GRADE anti-handwaving rule), and a
            ``reporting_compliance`` list. Bound into the signed
            predicate and denormalized into the ``ev_*`` columns for
            queryable filters. Defaults to all-zeros (the asserter
            flagged no quality concerns).

        Returns
        -------
        str
            The UUID claim_id.

        Raises
        ------
        ValueError
            If ``classification`` is not a valid value, ``text`` is empty,
            or ``artifact_hash`` is not a 64-character lowercase hex SHA256.
        mareforma._evidence.EvidenceVectorError
            If ``evidence`` violates a GRADE invariant (out-of-range domain,
            nonzero domain without a rationale, malformed structure).
        mareforma.db.IdempotencyConflictError
            If ``idempotency_key`` is set and any semantic field differs
            from the existing row.

        Notes
        -----
        Any DOI in ``supports[]`` or ``contradicts[]`` is HEAD-checked against
        Crossref and DataCite at assertion time. If any DOI fails to resolve,
        the claim is stored with ``unresolved=True`` and is ineligible for
        REPLICATED promotion. Call :meth:`refresh_unresolved` later to retry.
        """
        self._check_open()
        # Resolve any DOIs in supports/contradicts. Strings that don't match
        # DOI format are treated as claim_id references and pass through.
        dois = _doi.extract_dois((supports or []) + (contradicts or []))
        unresolved = False
        if dois:
            results = _doi.resolve_dois_with_cache(self._conn, dois)
            unresolved = any(not r for r in results.values())

        # Normalize evidence into an EvidenceVector instance. None →
        # default all-zeros. dict → validated reconstruction. Existing
        # EvidenceVector → pass through. Anything else raises.
        from mareforma._evidence import EvidenceVector
        if evidence is None:
            ev = EvidenceVector()
        elif isinstance(evidence, EvidenceVector):
            ev = evidence
        elif isinstance(evidence, dict):
            ev = EvidenceVector.from_dict(evidence)
        else:
            raise TypeError(
                f"evidence must be EvidenceVector | dict | None; "
                f"got {type(evidence).__name__}"
            )

        # Snapshot the grounding sensor's verdict into the EvidenceVector
        # so the score is signed alongside the rest of the claim. A
        # broken sensor (any Exception subclass: bad shape, model
        # failure, OSError, KeyError, IndexError, network error, etc.)
        # does NOT block assertion — we log a warning and drop the
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
                ev = EvidenceVector.from_dict({
                    **ev.to_dict(),
                    "grounding_score": float(score),
                    "grounding_rationale": rationale,
                })
                from mareforma import health as _health
                _health.append_health_event(
                    self._root, "grounding_verdict",
                    score=float(score),
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

        return _db.add_claim(
            self._conn,
            self._root,
            text,
            classification=classification,
            supports=supports,
            contradicts=contradicts,
            idempotency_key=idempotency_key,
            generated_by=generated_by or "agent",
            source_name=source_name,
            status=status,
            unresolved=unresolved,
            artifact_hash=artifact_hash,
            evidence=ev,
            seed=seed,
            signer=signer if signer is not None else self._signer,
            rekor_url=self._rekor_url,
            require_rekor=self._require_rekor,
            on_convergence_error=_bump_convergence_errors,
            rekor_log_pubkey_pem=self._rekor_log_pubkey_pem,
            predicate_payload=predicate_payload,
            original_signature_bundle=original_signature_bundle,
        )

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
        use** :meth:`query_for_llm` **instead** — it wraps the text in
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

            * ``"clean"`` — restrict to ``t_invalid IS NULL`` AND
              ``status = 'open'`` (the strictest "nothing wrong"
              cohort).
            * ``"contradicted"`` — restrict to ``t_invalid IS NOT
              NULL``; overrides the default ``include_invalidated``
              gate so contradicted rows surface even when the flag
              wasn't flipped.
            * ``"contested"`` — restrict to ``status = 'contested'``.
            * ``"retracted"`` — restrict to ``status = 'retracted'``.
            * ``"any"`` — surface every refutation state; implies
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

                # full-text search within unverified preliminary work
                graph.search(
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
        """
        self._check_open()
        return _db.query_claims(
            self._conn,
            text=text,
            min_support=min_support,
            classification=classification,
            limit=limit,
            include_unverified=include_unverified,
            include_invalidated=include_invalidated,
            refutation_filter=refutation_filter,
        )

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
        bundle — use a retraction-plus-new-assertion flow on those
        cases.

        Trust model on ``status`` mutations
        -----------------------------------
        A status change (open / contested / retracted) is an EDITORIAL
        action — it produces no signed envelope, requires no validator
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
        )

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
        Parameters mirror :meth:`query` — same filters, same per-row
        projection (``validator_reputation``, ``generator_enrolled``),
        same ``include_unverified`` semantics. The difference is the
        underlying engine: :meth:`query` uses LIKE substring matching;
        :meth:`search` uses FTS5 with the unicode61 tokenizer (diacritics
        folded) and supports the FTS5 query grammar.

        Parameters
        ----------
        query:
            FTS5 MATCH expression. Examples:

            - ``"gene"`` — single token
            - ``"\\"epistemic graph\\""`` — phrase (note: escape quotes
              in Python source)
            - ``"gene*"`` — prefix
            - ``"gene OR pathway"`` — boolean
            - ``"gene NEAR pathway"`` — proximity

            Pure-wildcard queries (``"*"``) are refused — they would
            scan the entire table.
        min_support, classification, limit, include_unverified:
            See :meth:`query`.

        Raises
        ------
        ValueError
            If ``query`` is empty or pure wildcards, or fails FTS5
            parsing. Also for invalid ``min_support`` / ``classification``.
        """
        self._check_open()
        return _db.search_claims(
            self._conn,
            query,
            min_support=min_support,
            classification=classification,
            limit=limit,
            include_unverified=include_unverified,
            include_invalidated=include_invalidated,
        )

    # ------------------------------------------------------------------
    # Verdict-issuer protocol
    # ------------------------------------------------------------------

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
            ``{"cosine": 0.92, "nli_forward": 0.88}``) — never fused
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

    def replication_verdicts(
        self,
        *,
        member_claim_id: str | None = None,
        cluster_id: str | None = None,
        include_invalidated: bool = False,
    ) -> list[dict]:
        """List signed replication verdicts, optionally filtered.

        By default, verdicts whose member or other claim has been
        invalidated by a signed contradiction verdict are excluded —
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

    def contradiction_verdicts(
        self, *, claim_id: str | None = None,
        include_invalidated: bool = False,
    ) -> list[dict]:
        """List signed contradiction verdicts, optionally filtered.

        By default, verdicts on invalidated claims are excluded; pass
        ``include_invalidated=True`` for audit-mode listings — the
        typical use, since a contradiction verdict IS the evidence
        for invalidation.
        """
        self._check_open()
        return _db.list_contradiction_verdicts(
            self._conn, claim_id=claim_id,
            include_invalidated=include_invalidated,
        )

    def get_validator_reputation(self) -> dict[str, int]:
        """Return ``{validator_keyid: count}`` for every enrolled validator.

        Count is the number of ESTABLISHED claims whose validation
        envelope was signed by that keyid. Validators with zero
        ESTABLISHED validations appear with ``count=0``. Derived state
        — recomputed on every call from the claims table; never cached.
        """
        self._check_open()
        return _db.get_validator_reputation(self._conn)

    def get_claim(self, claim_id: str) -> dict | None:
        """Return a single claim dict by ID, or None if not found."""
        self._check_open()
        return _db.get_claim(self._conn, claim_id)

    # ------------------------------------------------------------------
    # Trust layer: propositions, findings, derived Status
    # ------------------------------------------------------------------

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

    def register_plan(
        self,
        proposition: "Proposition",
        prediction: "Prediction",
        *,
        generated_by: str | None = None,
    ) -> str:
        """Pre-register a :class:`mareforma.trust.Prediction` against a proposition.

        Binds the decision rule to the proposition *before the numbers are seen*
        — the load-bearing move of the hypothetico-deductive method. Three
        effects, idempotent together:

        1. Registers the proposition (idempotent on ``content_id``).
        2. Writes the append-only ``predictions`` row with ``preregistered=1``.
        3. Writes its own signed claim — the **plan attestation** — via the
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

        if not proposition.is_falsifiable():
            raise NonFalsifiablePropositionError(
                "proposition must commit to a direction and a non-empty scope; "
                f"got direction={proposition.direction.value}, "
                f"scope={dict(proposition.scope)!r}"
            )

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

    def assert_finding(
        self,
        proposition: "Proposition",
        prediction: "Prediction",
        estimate: "EffectEstimate",
        *,
        data_id: str,
        generated_by: str | None = None,
        control_type: "ControlType | str | None" = None,
        modality: str | None = None,
        provenance_id: str | None = None,
        design_type: str | None = None,
        code_ref: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        """Record a finding: a computed bearing of an outcome on a proposition.

        The minimal write: a structured Proposition, a pre-registered Prediction,
        the result numbers (an :class:`EffectEstimate`), and a content-addressed
        ``data_id`` for the dataset. mareforma computes the Bearing (never
        declared), persists the single-line evidence tree, writes a signed claim
        as the attestation, and derives the proposition's count-based Status from
        the independent lines.

        Idempotent on (``content_id``, ``data_id``): re-asserting the same
        finding on the same dataset returns the prior finding rather than
        double-counting it.

        All input validation (falsifiability, estimate consistency, the gate)
        runs before the signed claim is written, so a rejected finding never
        leaves an orphan claim. The structured rows are then written in one
        transaction after the claim; their CHECK constraints mirror the already
        validated Python values, so a failure there is not expected. If one did
        occur the claim would remain as an attestation with no finding, and a
        retry would reuse that claim idempotently rather than duplicate it.

        One-shot convenience. Since v0.3.5 this composes the two earned steps
        — it registers the proposition and a synthesised plan (``preregistered=0``,
        so a real :meth:`register_plan` pre-registration stays distinguishable),
        then delegates to :meth:`submit_finding`. The return shape, idempotency
        on (``content_id``, ``data_id``), atomicity, and derived Status are all
        preserved unchanged. A one-shot finding does not separately attest its
        plan, so (matching the v0.3.4 one-shot) its signed ``supports[]`` carries
        no plan edge; use the explicit :meth:`register_plan` /
        :meth:`submit_finding` split when you want the signed plan -> finding
        edge.
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

        if not proposition.is_falsifiable():
            raise NonFalsifiablePropositionError(
                "proposition must commit to a direction and a non-empty scope; "
                f"got direction={proposition.direction.value}, "
                f"scope={dict(proposition.scope)!r}"
            )

        # Validate the gate inputs (estimate/data_id consistency, then the gate)
        # BEFORE writing anything, so a rejected one-shot finding leaves no
        # dangling proposition/plan behind — preserving v0.3.4's all-or-nothing
        # behaviour. submit_finding re-runs these cheaply; the duplication buys
        # atomicity at the convenience layer.
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
            generated_by=generated_by,
            control_type=control_type,
            modality=modality,
            provenance_id=provenance_id,
            design_type=design_type,
            code_ref=code_ref,
            idempotency_key=idempotency_key,
        )

    def submit_finding(
        self,
        proposition: "Proposition",
        prediction: "Prediction",
        estimate: "EffectEstimate",
        *,
        data_id: str,
        generated_by: str | None = None,
        control_type: "ControlType | str | None" = None,
        modality: str | None = None,
        provenance_id: str | None = None,
        design_type: str | None = None,
        code_ref: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        """Submit a finding against a plan that was already pre-registered.

        The second half of the register-plan-then-submit split. Computes the
        ``plan_id`` from the proposition + prediction and REQUIRES that plan to
        already exist (via :meth:`register_plan`), else raises
        :class:`NoRegisteredPlanError`. Then it computes the Bearing, writes the
        finding's signed claim whose ``supports[]`` cites the plan attestation's
        claim_id (so the plan -> finding edge is *signed*, not merely
        denormalised), persists the single-line evidence tree, and derives the
        proposition's Status.

        Idempotent on (``content_id``, ``data_id``): re-submitting the same
        dataset returns the prior finding. **Fork-guard:** if a finding already
        exists for (``content_id``, ``data_id``) but under a *different* plan_id
        than the prediction now passed, this raises
        :class:`FindingPlanForkError` rather than silently returning the prior
        bearing — a changed decision rule must not be swallowed by the
        (``content_id``, ``data_id``) idempotency anchor.

        All input validation (falsifiability, estimate consistency, the gate)
        runs before the signed claim is written, so a rejected finding never
        leaves an orphan claim. The authoritative existence check and the
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
            _store,
            compute_bearing,
        )

        if not proposition.is_falsifiable():
            raise NonFalsifiablePropositionError(
                "proposition must commit to a direction and a non-empty scope; "
                f"got direction={proposition.direction.value}, "
                f"scope={dict(proposition.scope)!r}"
            )

        ct = control_type if control_type is not None else ControlType.NEGATIVE
        # Building the line validates the estimate/data_id; computing the bearing
        # validates the gate (e.g. a mismatched CI level raises here). Both run
        # before the signed claim is written.
        line = EvidenceLine(
            estimate=estimate,
            data_id=data_id,
            contrast=Contrast(ct),
            modality=modality,
            provenance_id=provenance_id,
            design_type=design_type,
        )
        bearing = compute_bearing(estimate, prediction)
        cid = proposition.content_id()
        plan_id = _store.compute_plan_id(cid, prediction)

        def _fork_error(existing_plan_id: str) -> FindingPlanForkError:
            return FindingPlanForkError(
                f"a finding for (content_id={cid[:12]}…, data_id={data_id!r}) "
                f"already exists under plan {existing_plan_id[:12]}…, but the "
                f"prediction now passed resolves to plan {plan_id[:12]}…. The "
                "same dataset stands under exactly one plan for a proposition; "
                "re-submitting under a changed rule is refused, not silently "
                "ignored."
            )

        # Pre-flight (fast path, clean errors). The authoritative checks repeat
        # in-transaction below to close the TOCTOU window.
        existing = _store.find_existing_finding(self._conn, cid, data_id)
        if existing is not None:
            if existing["plan_id"] != plan_id:
                raise _fork_error(existing["plan_id"])
            # Emit here too, so an idempotent re-submit is logged whether it is
            # detected on this fast path or in the in-transaction re-check.
            from mareforma import health as _health
            _health.append_health_event(
                self._root, "submit_finding",
                bearing=bearing.direction.value, idempotent=True,
            )
            view = _store.proposition_status(self._conn, cid)
            return {
                "finding_id": existing["finding_id"],
                "content_id": cid,
                "plan_id": existing["plan_id"],
                "claim_id": existing["claim_id"],
                "bearing": bearing.to_dict(),
                "status": view["status"] if view else None,
                "idempotent": True,
                "proposition_status": view,
            }
        if not _store.plan_exists(self._conn, plan_id):
            raise NoRegisteredPlanError(
                f"no registered plan for (content_id={cid[:12]}…, "
                f"plan_id={plan_id[:12]}…). Call register_plan(proposition, "
                "prediction) before submit_finding, or use assert_finding for "
                "the one-shot path that registers the plan for you."
            )

        # Authoritative existence + fork + plan checks AND all writes run in one
        # transaction (BEGIN IMMEDIATE). The finding claim is written INSIDE this
        # transaction via assert_claim, which joins an open transaction
        # (conn.in_transaction) rather than committing its own — so a fork or
        # existence race that takes a non-insert branch rolls the claim back
        # instead of stranding a committed, signed claim on the chain.
        now = _now()
        conn = self._conn
        _own_txn = not conn.in_transaction
        if _own_txn:
            conn.execute("BEGIN IMMEDIATE")
        try:
            existing = _store.find_existing_finding(conn, cid, data_id)
            if existing is not None:
                if existing["plan_id"] != plan_id:
                    raise _fork_error(existing["plan_id"])
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
                plan_claim_id = _store.get_plan_claim_id(conn, plan_id)
                supports = [plan_claim_id] if plan_claim_id else None
                claim_id = self.assert_claim(
                    proposition.text(),
                    generated_by=generated_by,
                    supports=supports,
                    idempotency_key=idempotency_key or f"finding:{cid}:{data_id}",
                    predicate_payload={
                        "trust": "finding/v1",
                        "content_id": cid,
                        "frame_id": proposition.frame_id(),
                        "plan_id": plan_id,
                        "data_id": data_id,
                        "code_ref": code_ref,
                        "bearing": bearing.direction.value,
                    },
                )
                finding_id = _store.insert_finding(
                    conn, cid, plan_id, claim_id, bearing, line, now
                )
                result_claim_id = claim_id
                idempotent = False
            # Read the derived status inside the transaction so the returned dict
            # is an isolated snapshot of the graph immediately after this write,
            # not a post-commit read that a concurrent finding could have moved.
            view = _store.proposition_status(conn, cid)
            if _own_txn:
                conn.commit()
        except BaseException:
            if _own_txn:
                conn.rollback()
            raise

        from mareforma import health as _health
        _health.append_health_event(
            self._root, "submit_finding",
            bearing=bearing.direction.value,
            idempotent=idempotent,
        )

        return {
            "finding_id": finding_id,
            "content_id": cid,
            "plan_id": plan_id,
            "claim_id": result_claim_id,
            "bearing": bearing.to_dict(),
            "status": view["status"] if view else None,
            "idempotent": idempotent,
            "proposition_status": view,
        }

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
        return _store.proposition_status(self._conn, cid)

    def get_proposition(self, content_id: str) -> dict | None:
        """Return the stored proposition row as a dict, or None."""
        self._check_open()
        from mareforma.trust import _store

        row = _store.get_proposition_row(self._conn, content_id)
        return dict(row) if row is not None else None

    def query_frame(
        self, frame_id_or_proposition, *, min_status: str | None = None
    ) -> list[dict]:
        """Everything known about a question (frame_id), each with its derived
        view. Accepts a frame_id or a :class:`Proposition`. ``min_status``
        filters to propositions meeting a support floor on the
        UNTESTED < PRELIMINARY < CORROBORATED ladder.
        """
        self._check_open()
        from mareforma.trust import _store

        fid = (
            frame_id_or_proposition
            if isinstance(frame_id_or_proposition, str)
            else frame_id_or_proposition.frame_id()
        )
        return _store.query_frame(self._conn, fid, min_status=min_status)

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
        are sanitized but not wrapped — they are short labels, not
        free-form text. Other fields (``claim_id``, ``support_level``,
        timestamps) pass through unchanged.

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

            The validator's enumeration is self-declared — mareforma
            cannot prove the validator actually opened the cited claims —
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
            standard wrapper path — the wrapper builds the envelope
            from the same kwargs it threads through — but is listed
            for completeness because the underlying
            :func:`mareforma.db.validate_claim` defends against
            a bypass at this layer too.
        LLMValidatorPromotionError
            If the loaded signer is enrolled with ``validator_type='llm'``.
            LLM-typed validators can sign validation envelopes but
            cannot promote past REPLICATED — have a human-typed
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
        # Normalize evidence_seen — None → []. Always present in the
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
        rollback path — append-only validator history mirrors the
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
            a claim past REPLICATED — :meth:`validate` refuses them in
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

    def list_validators(self) -> list[dict]:
        """Return all enrolled validators ordered by enrollment time."""
        self._check_open()
        from mareforma import validators as _validators
        return _validators.list_validators(self._conn)

    def refresh_unresolved(self) -> dict[str, int]:
        """Retry DOI resolution for all claims currently marked unresolved.

        For each unresolved claim, re-checks every DOI in its ``supports[]``
        and ``contradicts[]``. If every DOI now resolves, the claim's
        unresolved flag is cleared and REPLICATED eligibility is re-evaluated.

        Network behavior
        ----------------
        DOIs are deduped across all unresolved claims and resolved exactly
        once per call, bypassing the cache (``force=True``). The cache is
        then overwritten with the fresh result. Shared DOIs across many
        claims therefore generate one HTTP request, not N — and the negative
        cache is never wiped wholesale.

        No-DOI claims
        -------------
        A claim flagged unresolved with no DOIs in supports/contradicts is
        a stale-flag artefact. The flag is cleared and a warning is emitted
        so the operator notices the data shape was unexpected.

        Returns
        -------
        dict
            ``{"checked": N, "resolved": M, "still_unresolved": K}`` — counts
            of claims processed and outcomes.
        """
        self._check_open()
        import warnings

        unresolved_claims = _db.list_unresolved_claims(self._conn)

        # Step 1: dedupe DOIs across all unresolved claims and resolve once.
        # A single corrupt JSON row (manual edit, partial restore from
        # claims.toml) must not abort the entire refresh — quarantine it
        # and let the rest of the claims through.
        claim_dois: dict[str, list[str]] = {}
        all_dois: set[str] = set()
        quarantined: list[str] = []
        for claim in unresolved_claims:
            try:
                supports = json.loads(claim.get("supports_json") or "[]")
                contradicts = json.loads(claim.get("contradicts_json") or "[]")
            except json.JSONDecodeError:
                warnings.warn(
                    f"Claim {claim['claim_id']} has corrupt supports_json or "
                    "contradicts_json; skipping during refresh.",
                    stacklevel=2,
                )
                quarantined.append(claim["claim_id"])
                continue
            dois = _doi.extract_dois(supports + contradicts)
            claim_dois[claim["claim_id"]] = dois
            all_dois.update(dois)

        results = (
            _doi.resolve_dois_with_cache(self._conn, list(all_dois), force=True)
            if all_dois
            else {}
        )

        # Step 2: decide per-claim using the shared results.
        resolved_count = 0
        still_unresolved = len(quarantined)
        for claim in unresolved_claims:
            cid = claim["claim_id"]
            if cid in quarantined:
                continue
            dois = claim_dois[cid]
            if not dois:
                warnings.warn(
                    f"Claim {cid} was flagged unresolved but contains no DOIs "
                    "in supports/contradicts. Clearing flag.",
                    stacklevel=2,
                )
                _db.mark_claim_resolved(self._conn, self._root, cid)
                resolved_count += 1
                continue

            if all(results.get(d, False) for d in dois):
                _db.mark_claim_resolved(self._conn, self._root, cid)
                resolved_count += 1
            else:
                still_unresolved += 1

        return {
            "checked": len(unresolved_claims),
            "resolved": resolved_count,
            "still_unresolved": still_unresolved,
        }

    def refresh_all_dois(self) -> dict[str, int]:
        """Force-re-resolve every DOI in the graph, bypassing the positive cache.

        Walks every claim's ``supports[]`` and ``contradicts[]``, dedupes the
        DOIs, and re-runs the HEAD check against Crossref + DataCite,
        bypassing the 30-day positive cache. The ``doi_cache`` table is
        overwritten with fresh results, so subsequent ``assert_claim``
        calls see the new state.

        Use when you suspect a referenced DOI has been retracted or its
        registry state has changed. ``refresh_unresolved`` only retries
        claims that were flagged at insert time; this method covers the
        case where a previously-resolved DOI has since failed.

        This method does **not** mutate ``support_level`` or the per-claim
        ``unresolved`` flag — re-running a HEAD check is not strong enough
        evidence to demote across the trust ladder, and the no-back-
        transitions invariant is intentional. To find claims affected by
        a newly-failing DOI, run::

            failed = [r["doi"] for r in conn.execute(
                "SELECT doi FROM doi_cache WHERE resolved = 0"
            )]

        and search ``supports_json``/``contradicts_json`` for those values.

        Returns
        -------
        dict
            ``{"checked", "still_resolved", "now_unresolved",
            "newly_failed"}`` — int counts. ``newly_failed`` is the number
            of DOIs whose cache state flipped from resolved to unresolved
            (the drift signal the operator usually wants).
        """
        self._check_open()

        all_dois: set[str] = set()
        for row in self._conn.execute(
            "SELECT supports_json, contradicts_json FROM claims"
        ).fetchall():
            try:
                supports = json.loads(row["supports_json"] or "[]")
                contradicts = json.loads(row["contradicts_json"] or "[]")
            except (json.JSONDecodeError, TypeError):
                continue
            all_dois.update(_doi.extract_dois(supports + contradicts))

        if not all_dois:
            return {
                "checked": 0,
                "still_resolved": 0,
                "now_unresolved": 0,
                "newly_failed": 0,
            }

        # Snapshot the prior cache state for every DOI we're about to refresh,
        # so we can report which entries flipped from resolved → unresolved.
        placeholders = ",".join("?" * len(all_dois))
        prior = {
            r["doi"]: bool(r["resolved"])
            for r in self._conn.execute(
                f"SELECT doi, resolved FROM doi_cache "
                f"WHERE doi IN ({placeholders})",
                list(all_dois),
            ).fetchall()
        }

        results = _doi.resolve_dois_with_cache(
            self._conn, list(all_dois), force=True,
        )

        still_resolved = sum(1 for ok in results.values() if ok)
        now_unresolved = sum(1 for ok in results.values() if not ok)
        newly_failed = sum(
            1
            for d, ok in results.items()
            if (not ok) and prior.get(d, False) is True
        )

        from mareforma import health as _health
        _health.append_health_event(
            self._root, "refresh_unresolved",
            succeeded=still_resolved,
            checked=len(results),
        )
        return {
            "checked": len(results),
            "still_resolved": still_resolved,
            "now_unresolved": now_unresolved,
            "newly_failed": newly_failed,
        }

    def find_drifted_dois(self, *, limit: int | None = None) -> list[dict]:
        """Walk the doi_cache and report DOIs whose metadata has drifted.

        Fetches Crossref / DataCite metadata for every cached resolved
        DOI, recomputes a stable content digest (title + year +
        container + author family names), and returns the DOIs whose
        digest differs from the one stored at last resolution.

        First-seen rows (no stored digest) are seeded with the current
        digest and excluded from the result — they're a baseline, not
        drift. Returns ``[]`` when httpx is unavailable or no drift is
        detected.

        Use as a periodic health-check: a drifted DOI may indicate a
        retraction, correction, or indexing-host swap on a referenced
        paper. Whether to refresh the cache or flag affected claims is
        a policy decision left to the caller.

        Parameters
        ----------
        limit
            Optional cap on how many DOIs to inspect per call. ``None``
            walks every resolved row.

        Returns
        -------
        list[dict]
            ``[{"doi", "stored_digest", "current_digest",
            "last_checked_at"}, ...]`` — one entry per drifted DOI.
        """
        self._check_open()
        from mareforma import health as _health
        drifted, walked, aborted = _doi.find_drifted_dois(
            self._conn, limit=limit,
        )
        # Emit a coherent (drifted, total_inspected) pair plus an
        # outcome that distinguishes "clean full scan" from "walk
        # aborted on 429 after K rows" — otherwise the rolling
        # rate-limit-recovery signal in stats CLI is invisible.
        _health.append_health_event(
            self._root, "doi_drift_scan",
            outcome="partial" if aborted else "ok",
            drifted=len(drifted),
            total_inspected=walked,
        )
        return drifted

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
            ``{"checked", "promoted", "still_pending"}`` — int counts.
            ``checked`` is the total rows examined; ``promoted`` is the
            number that ran detection cleanly this pass (the flag was
            cleared); ``still_pending`` is the number that errored
            again and remain flagged.

        Side effects: only the per-claim flag column and (transitively)
        the convergence-detection promotions themselves are mutated.
        Signed predicate fields are unchanged.
        """
        self._check_open()

        flagged = _db.list_convergence_retry_claims(self._conn)

        checked = len(flagged)
        promoted = 0
        still_pending = 0

        for row in flagged:
            try:
                supports = json.loads(row.get("supports_json") or "[]")
            except (json.JSONDecodeError, TypeError):
                supports = []
            generated_by = row.get("generated_by") or "agent"
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
            )
            if ok:
                _db.clear_convergence_retry_flag(
                    self._conn, self._root, claim_id,
                )
                promoted += 1
            else:
                still_pending += 1

        return {
            "checked": checked,
            "promoted": promoted,
            "still_pending": still_pending,
        }

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

        focal = _db.get_claim(self._conn, claim_id)
        if focal is None:
            raise _db.ClaimNotFoundError(
                f"Claim '{claim_id}' not found; cannot build lineage."
            )

        # Signers on the DSSE envelope. claim:v1 has one (the
        # asserter); claim-with-roles:v1 has N (planner / executor /
        # reviewer / validator). The keyid IS cryptographically bound
        # (each signature is verified over the PAE on disk during
        # restore). The ``role`` string sits on the signature entry
        # and is NOT covered by the signed payload bytes — see
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
                    rows_by_id[row["claim_id"]] = dict(row)
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
        # filtered out — exactly the verdict the operator needs to see
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

    def find_dangling_supports(self) -> list[dict]:
        """Return UUID-shaped ``supports[]`` entries that point nowhere.

        A "dangling" reference is a UUID-shaped entry in some claim's
        ``supports[]`` whose claim_id does not exist in this graph. DOIs
        and other free-form strings are external references and are NOT
        flagged — only UUID-shaped strings that look like local claim_ids
        but resolve to no row.

        Returns ``[{"claim_id", "dangling_ref"}, ...]`` sorted
        deterministically. Empty list when the graph is clean.

        Mareforma accepts dangling references at assertion time by
        design — a ``supports`` entry could legitimately reference a
        claim from another project or a not-yet-asserted upstream. This
        helper is for auditing integrity, not for blocking writes.
        REPLICATED detection already refuses to promote on a dangling
        reference, so a hanging arrow cannot trigger spurious promotion.
        """
        self._check_open()
        return _db.find_dangling_supports(self._conn)

    def refresh_unsigned(self) -> dict[str, int]:
        """Retry Rekor submission for every signed-but-not-logged claim.

        Mirrors :meth:`refresh_unresolved`. For each claim whose
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
            # whose predicate carries the EvidenceVector. Re-derive
            # with the row's stored evidence_json so a row+envelope
            # drift detector compares like-with-like.
            try:
                evidence_dict = json.loads(claim.get("evidence_json") or "{}")
            except (ValueError, TypeError):
                evidence_dict = {}
            live_payload = _signing.canonical_statement({
                "claim_id": cid,
                "text": claim["text"],
                "classification": claim["classification"],
                "generated_by": claim["generated_by"],
                "supports": json.loads(claim.get("supports_json") or "[]"),
                "contradicts": json.loads(claim.get("contradicts_json") or "[]"),
                "source_name": claim.get("source_name"),
                "artifact_hash": claim.get("artifact_hash"),
                "created_at": claim["created_at"],
            }, evidence_dict)
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
            # across both the replay and re-submit paths — there is no
            # way to launder a stale signature through this method.
            saved_entry = _db.get_rekor_inclusion(self._conn, cid)
            if saved_entry is not None:
                augmented = _signing.attach_rekor_entry(envelope, saved_entry)
                new_bundle = json.dumps(
                    augmented, sort_keys=True, separators=(",", ":"),
                )
                _db.mark_claim_logged(
                    self._conn, self._root, cid, new_bundle,
                )
                logged_count += 1
                continue

            logged, entry = _signing.submit_to_rekor(
                envelope, public_key, rekor_url=self._rekor_url,
            )
            if logged and entry is not None:
                # Merkle inclusion-proof verification (opt-in). Mirrors
                # the submit-time path in db._attempt_rekor_saga: when
                # the graph was opened with a log pubkey, re-fetch the
                # entry and cryptographically verify before persisting.
                # On verification failure, the entry stays unlogged
                # (the operator can retry once they investigate).
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
                        )
                        _signing.verify_rekor_inclusion(
                            full_body, self._rekor_log_pubkey_pem,
                        )
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
                if not _db._record_rekor_inclusion(self._conn, cid, entry):
                    # Sidecar write itself failed (rare; emits its own
                    # warning). Leave the row unlogged — refresh_unsigned
                    # will retry, accepting the duplicate-Rekor-entry
                    # risk documented in _record_rekor_inclusion.
                    still_unlogged += 1
                    continue
                augmented = _signing.attach_rekor_entry(envelope, entry)
                new_bundle = json.dumps(
                    augmented, sort_keys=True, separators=(",", ":"),
                )
                _db.mark_claim_logged(self._conn, self._root, cid, new_bundle)
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

    def get_tools(self, *, generated_by: str = "agent") -> list:
        """Return agent tool callables pre-bound to this graph.

        Returns two plain Python functions that any agent framework can wrap.
        ``generated_by`` is baked into the closure — set it to the calling
        agent's identifier so REPLICATED detection works across independent runs.

        Parameters
        ----------
        generated_by:
            Agent identifier, e.g. ``"agent/model-a/lab_a"``.
            Defaults to ``'agent'``.

        Returns
        -------
        list
            ``[query_graph, assert_finding]``

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
        >>> # Anthropic SDK — pass to tools= in client.messages.create()
        """
        self._check_open()

        def query_graph(topic: str, min_support: str = "PRELIMINARY") -> str:
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

            Returns
            -------
            str
                JSON array of claim dicts with keys: text, support_level,
                classification, status, claim_id. The ``text`` field is
                sanitized and wrapped in
                ``<untrusted_data>...</untrusted_data>`` — this tool is
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

        def assert_finding(
            text: str,
            classification: str = "INFERRED",
            supports: list[str] | None = None,
            contradicts: list[str] | None = None,
            source: str = "",
        ) -> str:
            """Record a new finding in the epistemic graph.

            Use ANALYTICAL only if a real data pipeline ran and returned output.
            Asserting ANALYTICAL on null data is permanently recorded as such.
            Use DERIVED when building explicitly on existing graph claims — cite
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

        return [query_graph, assert_finding]

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

    def health(self) -> dict[str, int]:
        """Single-call audit summary of mareforma state.

        Aggregates the counters operators inspect when they want a
        snapshot of "what's the graph telling me right now?" without
        having to write multiple queries. Pure observability over
        existing surfaces — no side effects.

        Returns
        -------
        dict[str, int]
            ``claim_count`` — total claims in the graph (signed and
            unsigned, all support levels, all statuses).
            ``validator_count`` — total rows in the validators table
            (every enrolled identity, including LLM-typed).
            ``unresolved_claims`` — claims flagged ``unresolved=1``
            (DOI HEAD-check failed at some point; blocks REPLICATED
            promotion until ``refresh_unresolved()`` clears them).
            ``unsigned_claims`` — claims with ``signature_bundle IS
            NULL`` (no Ed25519 envelope; blocks REPLICATED promotion
            and any cross-restore verification).
            ``dangling_supports`` — count of UUID-shaped ``supports[]``
            entries pointing to claims that do not exist in the graph
            (returned in detail by :meth:`find_dangling_supports`).
            ``convergence_errors`` — current value of the swallowed-
            error counter (see :attr:`convergence_errors`).
            ``convergence_retry_pending`` — claims with
            ``convergence_retry_needed=1`` waiting for
            :meth:`refresh_convergence` to re-run detection.

        A "healthy" graph has zeros across ``unresolved_claims``,
        ``unsigned_claims``, ``dangling_supports``,
        ``convergence_errors``, and ``convergence_retry_pending``.
        Non-zero values do not by themselves indicate a defect — they
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

        # The column is part of the the current schema and ``open_db``
        # column-presence-checks every open, so any reachable conn
        # here has the column. No defensive try/except needed — a
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

    def close(self) -> None:
        """Close the underlying database connection.

        Subsequent calls on this graph raise ``RuntimeError`` with an
        actionable message instead of leaking a raw
        ``sqlite3.ProgrammingError``.
        """
        if not self._closed:
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
