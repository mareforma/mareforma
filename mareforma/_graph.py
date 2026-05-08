"""
_graph.py — EpistemicGraph: agent-native interface to the mareforma epistemic graph.

Usage
-----
  graph = mareforma.open()                         # current directory
  graph = mareforma.open("/path/to/project")       # explicit path
  graph = mareforma.open(Path("my_project"))

  with mareforma.open() as graph:                  # context manager
      claim_id = graph.assert_claim("...", stated_confidence=0.8)
      results  = graph.query("inhibitory input", min_support="REPLICATED")
      graph.validate(claim_id, validated_by="jane@lab.org")

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
        stated_confidence: float | None = None,
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
        stated_confidence:
            Float 0.0–1.0. Defaults to 0.4 if not provided.
        supports:
            List of claim_ids or DOIs this claim is grounded in.
        contradicts:
            List of claim_ids or DOIs this claim contests.
        idempotency_key:
            Stable key for retry-safe writes. Same key → same claim_id returned.
        generated_by:
            Agent identifier. Defaults to 'agent'.
        source_name:
            Data source this claim derives from.
        """
        return _db.add_claim(
            self._conn,
            self._root,
            text,
            classification=classification,
            stated_confidence=stated_confidence,
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
        List of claim dicts ordered by stated_confidence desc, recency desc.
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

        Raises
        ------
        ClaimNotFoundError
            If claim_id does not exist.
        ValueError
            If support_level is not 'REPLICATED'.
        """
        _db.validate_claim(self._conn, claim_id, validated_by=validated_by)

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
