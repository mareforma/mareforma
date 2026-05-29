"""``mareforma ingest`` — read a literature file, extract claim drafts.

Two modes:

- Default (structured): parses files with explicit ``TITLE:`` / ``DOI:``
  / ``CLAIMS:`` sections. Pure stdlib + ``click`` + ``rich`` — no
  external service.
- ``--llm``: gated on a local ``anthropic`` install. Extracts claims
  from arbitrary text via the Claude API. Exits with a clear message
  if ``anthropic`` is not installed.

Extracted claims write to the ``literature_claims`` table (separate
from the signed ``claims`` table; ingest produces drafts pending
review).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import click
import tomli_w
from rich.console import Console
from rich.table import Table

console = Console()

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_structured(text: str) -> dict:
    """
    Parse a structured abstract (TITLE: / DOI: / CLAIMS: format).
    Returns meta dict with keys: title, doi, claims_raw.
    """
    lines = text.strip().splitlines()
    meta: dict = {"title": "", "doi": "", "claims_raw": []}
    in_claims = False

    for line in lines:
        line = line.strip()
        if line.startswith("TITLE:"):
            meta["title"] = line[len("TITLE:"):].strip()
        elif line.startswith("DOI:"):
            meta["doi"] = line[len("DOI:"):].strip()
        elif line.startswith("CLAIMS:"):
            in_claims = True
        elif in_claims and line.startswith("-"):
            meta["claims_raw"].append(line[1:].strip())

    return meta


def _parse_claim_line(raw: str) -> tuple[str, float]:
    """Parse 'Some claim text (confidence: 0.85)' → (text, confidence)."""
    if "(confidence:" in raw:
        text_part, conf_part = raw.rsplit("(confidence:", 1)
        text = text_part.strip()
        try:
            confidence = float(conf_part.rstrip(")").strip())
        except ValueError:
            confidence = 0.5
    else:
        text = raw.strip()
        confidence = 0.5
    return text, confidence


def _doc_id(doi: str, title: str) -> str:
    """Stable 16-char hex ID for a source document."""
    return hashlib.sha256(f"{doi}|{title}".encode()).hexdigest()[:16]


def _claim_id(doc_id: str, index: int) -> str:
    return f"{doc_id}:{index:03d}"


# ---------------------------------------------------------------------------
# LLM extraction (gated)
# ---------------------------------------------------------------------------

_LLM_SYSTEM = (
    "You extract scientific claims from abstracts. "
    "Return a JSON array of objects with keys: "
    "claim_text (string), confidence (float 0–1). "
    "Return only the JSON array — no prose."
)


def _extract_llm(text: str, model: str, doi: str, title: str) -> list[dict]:
    """
    Use Claude to extract claims from arbitrary text.
    Raises ImportError if anthropic is not installed (caller handles this).
    """
    import anthropic  # noqa: PLC0415 — intentionally gated

    client = anthropic.Anthropic()
    message = client.messages.create(
        model=model,
        max_tokens=1024,
        system=_LLM_SYSTEM,
        messages=[{"role": "user", "content": text}],
    )
    raw_json = message.content[0].text.strip()
    parsed = json.loads(raw_json)

    doc_id = _doc_id(doi, title)
    now = datetime.now(timezone.utc).isoformat()
    results = []
    for i, item in enumerate(parsed):
        results.append({
            "claim_id": _claim_id(doc_id, i),
            "source_doc_id": doc_id,
            "doi": doi,
            "title": title,
            "claim_text": item["claim_text"],
            "confidence": float(item.get("confidence", 0.5)),
            "extracted_by": f"ingest:llm:{model}",
            "ingested_at": now,
            "contradicts": None,
        })
    return results


# ---------------------------------------------------------------------------
# Core ingest function (used by CLI and tests)
# ---------------------------------------------------------------------------


def ingest_file(
    file_path: Path,
    db: sqlite3.Connection,
    *,
    extracted_by: str = "ingest:mock",
    use_llm: bool = False,
    model: str = "claude-opus-4-6",
) -> list[dict]:
    """
    Parse file_path, extract claims, write to DB.
    Returns list of claim dicts.
    """
    text = file_path.read_text(encoding="utf-8", errors="replace")
    meta = _parse_structured(text)
    doi = meta["doi"]
    title = meta["title"]

    if use_llm:
        claims = _extract_llm(text, model, doi, title)
    else:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = _doc_id(doi, title)
        claims = []
        for i, raw in enumerate(meta["claims_raw"]):
            claim_text, confidence = _parse_claim_line(raw)
            claims.append({
                "claim_id": _claim_id(doc_id, i),
                "source_doc_id": doc_id,
                "doi": doi,
                "title": title,
                "claim_text": claim_text,
                "confidence": confidence,
                "extracted_by": extracted_by,
                "ingested_at": now,
                "contradicts": None,
            })

    for row in claims:
        db.execute(
            """
            INSERT OR REPLACE INTO literature_claims
            (claim_id, source_doc_id, doi, title, claim_text, confidence,
             extracted_by, ingested_at, contradicts)
            VALUES (:claim_id, :source_doc_id, :doi, :title, :claim_text,
                    :confidence, :extracted_by, :ingested_at, :contradicts)
            """,
            row,
        )
    db.commit()
    return claims


def claims_to_toml(claims: list[dict]) -> str:
    """Serialize claim list to TOML string (mirrors claims.toml schema)."""
    doc: dict = {}
    for c in claims:
        doc[c["claim_id"]] = {
            "text": c["claim_text"],
            "confidence": c["confidence"],
            "source_doc_id": c["source_doc_id"],
            "doi": c["doi"] or "",
            "extracted_by": c["extracted_by"],
            "ingested_at": c["ingested_at"],
        }
    return tomli_w.dumps({"claim": doc})


# ---------------------------------------------------------------------------
# Click command
# ---------------------------------------------------------------------------


@click.command("ingest")
@click.argument("file", type=click.Path(exists=False))
@click.option(
    "--db",
    "db_path",
    default=".mareforma/graph.db",
    show_default=True,
    help="Path to graph.db",
)
@click.option("--llm", "use_llm", is_flag=True, default=False, help="Use LLM extraction")
@click.option(
    "--model",
    default="claude-opus-4-6",
    show_default=True,
    help="Claude model (only used with --llm)",
)
def ingest_cli(file, db_path, use_llm, model):
    """Ingest a literature file into the provenance graph."""
    from mareforma.db import open_db_from_db_path

    file_path = Path(file)
    if not file_path.exists():
        console.print(f"[red]Error:[/red] File not found: {file_path}")
        sys.exit(1)

    if use_llm:
        try:
            import anthropic  # noqa: F401
        except (ImportError, TypeError):
            console.print(
                "[red]Error:[/red] --llm requires the `anthropic` package. "
                "Install it with: pip install anthropic"
            )
            sys.exit(1)

    db_file = Path(db_path).resolve()
    conn = open_db_from_db_path(db_file)

    try:
        claims = ingest_file(file_path, conn, use_llm=use_llm, model=model)
    except Exception as exc:
        console.print(f"[red]Error during ingest:[/red] {exc}")
        conn.close()
        sys.exit(2)

    conn.close()

    if not claims:
        console.print("[yellow]Warning:[/yellow] No claims found in file.")
        return

    table = Table(title=f"Ingested {len(claims)} claim(s)")
    table.add_column("ID", style="dim")
    table.add_column("Claim")
    table.add_column("Conf", justify="right")
    table.add_column("Extracted by", style="dim")

    for c in claims:
        table.add_row(
            c["claim_id"],
            c["claim_text"][:80] + ("…" if len(c["claim_text"]) > 80 else ""),
            f"{c['confidence']:.0%}",
            c["extracted_by"],
        )

    console.print(table)
    console.print(f"\n[green]✓[/green] Written to [bold]{db_file}[/bold]")
