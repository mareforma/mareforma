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
    ) -> None:
        self._conn = conn
        self._root = root
        self._signer = signer
        self._rekor_url = rekor_url
        self._require_rekor = require_rekor

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
        artifact_hash: str | None = None,
    ) -> str:
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
            Stable key for retry-safe writes. Same key → same claim_id returned.
        generated_by:
            Agent identifier. Use ``"model/version/context"`` format.
            Defaults to ``'agent'``.
        source_name:
            Data source this claim derives from. Required for ANALYTICAL
            classification to be meaningful.
        artifact_hash:
            SHA256 hex digest of the output artifact (figure, CSV, model)
            backing this claim. When supplied it is bound into the signed
            payload and used as a parallel REPLICATED signal: two peers
            citing the same upstream that BOTH supply a hash must agree
            on the hash before they converge. Compute with
            ``hashlib.sha256(bytes).hexdigest()``.

        Returns
        -------
        str
            The UUID claim_id.

        Raises
        ------
        ValueError
            If ``classification`` is not a valid value, ``text`` is empty,
            or ``artifact_hash`` is not a 64-character lowercase hex SHA256.

        Notes
        -----
        Any DOI in ``supports[]`` or ``contradicts[]`` is HEAD-checked against
        Crossref and DataCite at assertion time. If any DOI fails to resolve,
        the claim is stored with ``unresolved=True`` and is ineligible for
        REPLICATED promotion. Call :meth:`refresh_unresolved` later to retry.
        """
        # Resolve any DOIs in supports/contradicts. Strings that don't match
        # DOI format are treated as claim_id references and pass through.
        dois = _doi.extract_dois((supports or []) + (contradicts or []))
        unresolved = False
        if dois:
            results = _doi.resolve_dois_with_cache(self._conn, dois)
            unresolved = any(not r for r in results.values())

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
            unresolved=unresolved,
            artifact_hash=artifact_hash,
            signer=self._signer,
            rekor_url=self._rekor_url,
            require_rekor=self._require_rekor,
        )

    def query(
        self,
        text: str | None = None,
        *,
        min_support: str | None = None,
        classification: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        """Query claims from the epistemic graph.

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

        Returns
        -------
        list[dict]
            Claim dicts ordered by support_level (desc) then created_at (desc).
            Each dict contains: ``claim_id``, ``text``, ``classification``,
            ``support_level``, ``idempotency_key``, ``validated_by``,
            ``validated_at``, ``status``, ``source_name``, ``generated_by``,
            ``supports_json``, ``contradicts_json``, ``comparison_summary``,
            ``created_at``, ``updated_at``.

        Raises
        ------
        ValueError
            If ``min_support`` or ``classification`` is not a valid value.
        """
        return _db.query_claims(
            self._conn,
            text=text,
            min_support=min_support,
            classification=classification,
            limit=limit,
        )

    def get_claim(self, claim_id: str) -> dict | None:
        """Return a single claim dict by ID, or None if not found."""
        return _db.get_claim(self._conn, claim_id)

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

    def validate(self, claim_id: str, *, validated_by: str | None = None) -> None:
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
        validator_keyid + validated_at). The signed envelope is stored
        on the row's ``validation_signature`` column so the promotion is
        independently verifiable.

        Parameters
        ----------
        claim_id:
            UUID of the claim to promote.
        validated_by:
            Optional human-readable label stored alongside the keyid.
            The validator's keyid is the real identity; this string is
            for display only.

        Raises
        ------
        ClaimNotFoundError
            If claim_id does not exist.
        ValueError
            If support_level is not 'REPLICATED', or the graph has no
            loaded signer, or the loaded signer is not enrolled as a
            validator on this project.
        """
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
        envelope = _signing.sign_validation(
            {
                "claim_id": claim_id,
                "validator_keyid": keyid,
                "validated_at": now,
            },
            self._signer,
        )
        bundle_json = json.dumps(envelope, sort_keys=True, separators=(",", ":"))

        _db.validate_claim(
            self._conn, self._root, claim_id,
            validated_by=validated_by,
            validation_signature=bundle_json,
            validated_at=now,
        )

    def enroll_validator(
        self,
        pubkey_pem: bytes,
        *,
        identity: str,
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
        """
        from mareforma import validators as _validators
        if self._signer is None:
            raise ValueError(
                "graph.enroll_validator requires a loaded signing key. "
                "Run `mareforma bootstrap` once and reopen the graph."
            )
        return _validators.enroll_validator(
            self._conn, self._signer, pubkey_pem, identity=identity,
        )

    def list_validators(self) -> list[dict]:
        """Return all enrolled validators ordered by enrollment time."""
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
            live_payload = _signing.canonical_payload({
                "claim_id": cid,
                "text": claim["text"],
                "classification": claim["classification"],
                "generated_by": claim["generated_by"],
                "supports": json.loads(claim.get("supports_json") or "[]"),
                "contradicts": json.loads(claim.get("contradicts_json") or "[]"),
                "source_name": claim.get("source_name"),
                "artifact_hash": claim.get("artifact_hash"),
                "created_at": claim["created_at"],
            })
            if live_payload != payload_bytes:
                warnings.warn(
                    f"Claim {cid} row drifted from its signed payload; "
                    "refusing to log a stale signature to Rekor. "
                    "Investigate the row vs signature_bundle mismatch.",
                    stacklevel=2,
                )
                still_unlogged += 1
                continue

            logged, entry = _signing.submit_to_rekor(
                envelope, public_key, rekor_url=self._rekor_url,
            )
            if logged and entry is not None:
                augmented = _signing.attach_rekor_entry(envelope, entry)
                new_bundle = json.dumps(
                    augmented, sort_keys=True, separators=(",", ":"),
                )
                _db.mark_claim_logged(self._conn, self._root, cid, new_bundle)
                logged_count += 1
            else:
                still_unlogged += 1

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
        The returned callables are bound to this graph instance.
        Using them after ``graph.close()`` raises a SQLite error.

        Example
        -------
        >>> tools = graph.get_tools(generated_by="agent/claude-sonnet-4-6/lab_a")
        >>> # LangChain
        >>> lc_tools = [tool(fn) for fn in tools]
        >>> # Anthropic SDK — pass to tools= in client.messages.create()
        """

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
                classification, claim_id. The ``text`` field is sanitized
                and wrapped in ``<untrusted_data>...</untrusted_data>`` —
                this tool is consumed by an LLM, so it routes through the
                same prompt-safety layer as :meth:`query_for_llm`.
            """
            results = self.query_for_llm(topic, min_support=min_support)
            return json.dumps([
                {
                    "text": r["text"],
                    "support_level": r["support_level"],
                    "classification": r["classification"],
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
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying database connection."""
        self._conn.close()

    def __enter__(self) -> "EpistemicGraph":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"EpistemicGraph(root={self._root})"
