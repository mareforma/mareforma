"""
health_extension.py: Contradiction detection extension for health.py (Step 3).

Adds detect_contradictions() to the end of the health report.
Two modes:
  1. Structural: explicit contradicts[] JSON links between claims.
  2. Heuristic:  polarity mismatch on shared key terms across documents.

Zero extra dependencies. Integration: import and call at the end of
the existing health report in health.py.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Polarity patterns
# ---------------------------------------------------------------------------

_NEGATION = re.compile(
    r"\b(does not|did not|no significant|failed to|no effect|"
    r"not significant|no reduction|not reduced|unchanged|no change)\b",
    re.IGNORECASE,
)

_POSITIVE = re.compile(
    r"\b(reduces|reduced|decrease|decreased|lower|lowered|"
    r"improves|improved|significant reduction|significantly reduced)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Contradiction dataclass
# ---------------------------------------------------------------------------


@dataclass
class Contradiction:
    claim_a_id: str
    claim_a_text: str
    claim_a_confidence: float
    claim_a_doi: str
    claim_b_id: str
    claim_b_text: str
    claim_b_confidence: float
    claim_b_doi: str
    kind: str  # "structural" | "heuristic"
    shared_terms: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Key-term extraction
# ---------------------------------------------------------------------------


# Common sentence-initial / discourse words that the capitalised-token
# regex would otherwise pick up. Without filtering these, two abstracts
# that both contain 'Patients' or 'Results' generate a spurious shared
# term and a false contradiction.
_STOPWORD_TERMS: frozenset[str] = frozenset({
    "A", "An", "And", "As", "At", "Be", "But", "By", "For", "From",
    "Here", "However", "If", "In", "Is", "It", "Its", "Methods", "Or",
    "Of", "On", "Patients", "Results", "Subjects", "That", "The", "This",
    "These", "Those", "To", "We", "Were", "What", "When", "Where",
    "Which", "While", "With", "Conclusion", "Conclusions", "Background",
    "Discussion", "Introduction", "Objective", "Objectives", "Outcome",
    "Outcomes", "Findings",
})


def _extract_key_terms(text: str) -> set[str]:
    """Extract capitalised multi-word tokens and biomarker patterns.

    Filters out a curated stopword list of sentence-initial / discourse
    words so common openings ('Patients', 'Results') don't get treated
    as shared topic terms across unrelated abstracts.
    """
    tokens = re.findall(r"\b([A-Z][a-zA-Z\-]*(?:\s+[A-Z][a-zA-Z\-]*)*)\b", text)
    tokens += re.findall(r"\b([A-Z]{2,}(?:-\d+)?)\b", text)
    return {
        t.strip() for t in tokens
        if len(t) >= 2 and t.strip() not in _STOPWORD_TERMS
    }


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


def detect_contradictions(conn: sqlite3.Connection) -> list[Contradiction]:
    """
    Run structural + heuristic contradiction detection over literature_claims.
    Returns a list of Contradiction objects.
    """
    rows = conn.execute(
        "SELECT claim_id, source_doc_id, doi, claim_text, confidence, contradicts "
        "FROM literature_claims"
    ).fetchall()

    contradictions: list[Contradiction] = []
    by_id = {r["claim_id"]: r for r in rows}

    # --- Structural ---
    for row in rows:
        if row["contradicts"]:
            try:
                linked_ids = json.loads(row["contradicts"])
            except (json.JSONDecodeError, TypeError):
                continue
            for other_id in linked_ids:
                if other_id in by_id:
                    other = by_id[other_id]
                    contradictions.append(Contradiction(
                        claim_a_id=row["claim_id"],
                        claim_a_text=row["claim_text"],
                        claim_a_confidence=row["confidence"],
                        claim_a_doi=row["doi"] or "",
                        claim_b_id=other["claim_id"],
                        claim_b_text=other["claim_text"],
                        claim_b_confidence=other["confidence"],
                        claim_b_doi=other["doi"] or "",
                        kind="structural",
                    ))

    # --- Heuristic ---
    # A claim with BOTH a positive and a negation phrase ("reduced X
    # but did not improve Y") is ambiguous, not a polarity signal.
    # Excluding these from both sides prevents a row from contradicting
    # itself across the cross-product and prevents inflated contradiction
    # counts on common scientific-abstract phrasing.
    pure_positive = [
        r for r in rows
        if _POSITIVE.search(r["claim_text"])
        and not _NEGATION.search(r["claim_text"])
    ]
    pure_negative = [
        r for r in rows
        if _NEGATION.search(r["claim_text"])
        and not _POSITIVE.search(r["claim_text"])
    ]

    # Hoist negative-side term extraction out of the inner loop —
    # avoids O(P×N) regex sweeps on the same N rows.
    neg_with_terms = [
        (neg, _extract_key_terms(neg["claim_text"])) for neg in pure_negative
    ]

    for pos in pure_positive:
        pos_terms = _extract_key_terms(pos["claim_text"])
        for neg, neg_terms in neg_with_terms:
            if pos["source_doc_id"] == neg["source_doc_id"]:
                continue  # same document — not a contradiction
            shared = pos_terms & neg_terms
            if shared:
                contradictions.append(Contradiction(
                    claim_a_id=pos["claim_id"],
                    claim_a_text=pos["claim_text"],
                    claim_a_confidence=pos["confidence"],
                    claim_a_doi=pos["doi"] or "",
                    claim_b_id=neg["claim_id"],
                    claim_b_text=neg["claim_text"],
                    claim_b_confidence=neg["confidence"],
                    claim_b_doi=neg["doi"] or "",
                    kind="heuristic",
                    shared_terms=sorted(shared),
                ))

    return contradictions


def report(contradictions: list[Contradiction]) -> str:
    """Format contradiction list as plain text for the health report."""
    if not contradictions:
        return "No contradictions detected."

    lines = [f"Contradictions found: {len(contradictions)}", ""]
    for i, c in enumerate(contradictions, 1):
        lines.append(f"[{i}] {c.kind.upper()}")
        lines.append(
            f"  A ({c.claim_a_doi}): {c.claim_a_text}  [conf={c.claim_a_confidence}]"
        )
        lines.append(
            f"  B ({c.claim_b_doi}): {c.claim_b_text}  [conf={c.claim_b_confidence}]"
        )
        if c.shared_terms:
            lines.append(f"  Shared terms: {', '.join(c.shared_terms)}")
        lines.append("")
    return "\n".join(lines)
