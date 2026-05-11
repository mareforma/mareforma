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

import json
from pathlib import Path
from typing import TYPE_CHECKING

from mareforma import db as _db

if TYPE_CHECKING:
    import sqlite3


class EpistemicGraph:
    """Agent-native interface to a local mareforma epistemic graph.

    Do not instantiate directly — use mareforma.open().
    """

    def __init__(self, conn: "sqlite3.Connection", root: Path) -> None:
        self._conn = conn
        self._root = root

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
        """
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
        _db.validate_claim(self._conn, claim_id, validated_by=validated_by)

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
