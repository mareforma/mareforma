"""Narrative Markdown exporter for ingested literature claims.

Groups claims by source document. Flags contradictions inline via the
literature-contradiction detector (structural + polarity heuristic).
Pairs with ``mareforma ingest`` — ingest extracts; this exporter
renders.
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import click
from rich.console import Console

console = Console()


def export_narrative(conn: sqlite3.Connection) -> str:
    """Generate Markdown narrative from all ingested literature claims."""
    from mareforma._literature_health import detect_contradictions

    rows = conn.execute(
        """
        SELECT claim_id, source_doc_id, doi, title, claim_text,
               confidence, ingested_at
        FROM literature_claims
        ORDER BY source_doc_id, confidence DESC
        """
    ).fetchall()

    if not rows:
        return "# Literature Summary\n\nNo claims ingested yet.\n"

    # Group by source document
    docs: dict[str, list] = {}
    for row in rows:
        docs.setdefault(row["source_doc_id"], []).append(row)

    contradictions = detect_contradictions(conn)
    flagged_ids: set[str] = set()
    for c in contradictions:
        flagged_ids.add(c.claim_a_id)
        flagged_ids.add(c.claim_b_id)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = ["# Literature Summary", ""]
    lines.append(
        f"*Generated at {now} UTC. "
        f"{len(rows)} claim(s) across {len(docs)} source(s).*"
    )
    lines.append("")

    if contradictions:
        lines.append(f"> **Contradictions detected:** {len(contradictions)}")
        for c in contradictions:
            terms = ", ".join(c.shared_terms) if c.shared_terms else "—"
            lines.append(
                f"> - [{c.claim_a_doi}] vs [{c.claim_b_doi}]: "
                f"shared terms `{terms}`"
            )
        lines.append("")

    for doc_id, claims in docs.items():
        first = claims[0]
        lines.append(f"## {first['title'] or doc_id}")
        if first["doi"]:
            lines.append(f"**DOI:** {first['doi']}  ")
        lines.append(f"**Source ID:** `{doc_id}`  ")
        lines.append(f"**Ingested:** {first['ingested_at']}")
        lines.append("")
        lines.append("### Claims")
        lines.append("")

        for claim in claims:
            bar_filled = round(claim["confidence"] * 10)
            conf_bar = "█" * bar_filled + "░" * (10 - bar_filled)
            flag = " ⚠ *contradicted*" if claim["claim_id"] in flagged_ids else ""
            lines.append(
                f"- **{claim['claim_text']}**{flag}  \n"
                f"  Confidence: {claim['confidence']:.0%} `{conf_bar}`  \n"
                f"  ID: `{claim['claim_id']}`"
            )
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Click command registration (shown as-is for integration into cli.py)
# ---------------------------------------------------------------------------


@click.command("narrative")
@click.option(
    "--db",
    "db_path",
    default=".mareforma/graph.db",
    show_default=True,
    help="Path to graph.db",
)
@click.option(
    "--output",
    "-o",
    default=None,
    help="Write to file instead of stdout",
)
def narrative_cmd(db_path, output):
    """Export literature claims as a Markdown narrative."""
    from mareforma.db import open_db

    resolved_db = Path(db_path).resolve()
    if not resolved_db.exists():
        console.print(f"[red]Error:[/red] DB not found: {resolved_db}")
        raise SystemExit(1)

    if resolved_db.parent.name == ".mareforma":
        project_root = resolved_db.parent.parent
    else:
        project_root = resolved_db.parent
    conn = open_db(project_root)
    md = export_narrative(conn)
    conn.close()

    if output:
        Path(output).write_text(md, encoding="utf-8")
        console.print(f"[green]✓[/green] Written to {output}")
    else:
        click.echo(md)
