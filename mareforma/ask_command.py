"""``mareforma ask``: FTS5 BM25 search over ingested literature claims.

Sanitises input (quote each token) so hyphens and special characters
are treated as literals, not FTS5 operators. Returns hits ranked by
BM25 score, highest first.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

console = Console()


@dataclass
class QueryResult:
    claim_id: str
    claim_text: str
    confidence: float
    doi: str
    title: str
    score: float  # BM25, higher = better (sign flipped from FTS5 native)


def ask(
    question: str,
    db: sqlite3.Connection,
    limit: int = 5,
) -> list[QueryResult]:
    """
    Search literature claims via FTS5 BM25.
    Returns QueryResult list, highest score first.
    """
    # FTS5 token sanitisation. Each token is double-quoted so hyphens
    # and special characters are treated as literals, not operators.
    # Embedded double quotes must be escaped by doubling per the FTS5
    # spec; without this, input like 'mutations of "BRCA1"' produces
    # 'mutations" "of" ""BRCA1""' which raises OperationalError.
    tokens = [
        '"' + w.replace('"', '""') + '"'
        for w in question.split()
        if w.strip()
    ]
    if not tokens:
        return []
    safe_question = " ".join(tokens)

    rows = db.execute(
        """
        SELECT
            lc.claim_id,
            lc.claim_text,
            lc.confidence,
            lc.doi,
            lc.title,
            -bm25(literature_claims_fts) AS score
        FROM literature_claims_fts
        JOIN literature_claims lc ON lc.rowid = literature_claims_fts.rowid
        WHERE literature_claims_fts MATCH ?
        ORDER BY score DESC
        LIMIT ?
        """,
        (safe_question, limit),
    ).fetchall()

    return [
        QueryResult(
            claim_id=r["claim_id"],
            claim_text=r["claim_text"],
            confidence=r["confidence"],
            doi=r["doi"] or "",
            title=r["title"] or "",
            score=r["score"],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Click command
# ---------------------------------------------------------------------------


@click.command("ask")
@click.argument("question")
@click.option(
    "--db",
    "db_path",
    default=".mareforma/graph.db",
    show_default=True,
    help="Path to graph.db",
)
@click.option(
    "--limit",
    default=5,
    show_default=True,
    help="Maximum results to return",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Output raw JSON",
)
def ask_cli(question, db_path, limit, as_json):
    """Search literature claims using full-text search."""
    from mareforma.db import open_db_from_db_path

    resolved_db = Path(db_path).resolve()
    if not resolved_db.exists():
        console.print(f"[red]Error:[/red] DB not found: {resolved_db}")
        sys.exit(1)

    conn = open_db_from_db_path(resolved_db)
    results = ask(question, conn, limit=limit)
    conn.close()

    if as_json:
        click.echo(json.dumps([
            {
                "claim_id": r.claim_id,
                "claim_text": r.claim_text,
                "confidence": r.confidence,
                "doi": r.doi,
                "score": r.score,
            }
            for r in results
        ], indent=2))
        return

    if not results:
        console.print("[dim]No matching claims found.[/dim]")
        return

    table = Table(title=f'Results for "{question}"')
    table.add_column("Score", justify="right", style="dim")
    table.add_column("Claim")
    table.add_column("Conf", justify="right")
    table.add_column("DOI", style="dim")

    for r in results:
        table.add_row(
            f"{r.score:.4f}",
            r.claim_text[:80] + ("…" if len(r.claim_text) > 80 else ""),
            f"{r.confidence:.0%}",
            r.doi,
        )

    console.print(table)
