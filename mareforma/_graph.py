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
        rekor_url: str | None = None,
        require_rekor: bool = False,
    ) -> None:
        self._conn = conn
        self._root = root
        self._signer = signer
        self._rekor_url = rekor_url
        self._require_rekor = require_rekor

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

        Returns
        -------
        str
            The UUID claim_id.

        Raises
        ------
        ValueError
            If ``classification`` is not a valid value or ``text`` is empty.

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

    def validate(self, claim_id: str, *, validated_by: str | None = None) -> None:
        """Promote a REPLICATED claim to ESTABLISHED (human validation).

        Parameters
        ----------
        claim_id:
            UUID of the claim to promote.
        validated_by:
            Identifier of the human reviewer (e.g. email). Stored on the claim.

        Raises
        ------
        ClaimNotFoundError
            If claim_id does not exist.
        ValueError
            If support_level is not 'REPLICATED'.
        """
        _db.validate_claim(self._conn, self._root, claim_id, validated_by=validated_by)

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
                classification, claim_id.
            """
            results = self.query(topic, min_support=min_support)
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
