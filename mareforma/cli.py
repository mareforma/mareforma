"""
cli.py — Mareforma command-line interface.

Commands
--------
    mareforma claim add TEXT [options]         assert a scientific claim
    mareforma claim list [--status] [--source] list claims
    mareforma claim show ID                    show claim details
    mareforma claim update ID [options]        update a claim
    mareforma status                           epistemic health dashboard
    mareforma export [--output path]           write ontology.jsonld
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

if sys.platform == "win32":
    for _stream in (sys.stdout, sys.stderr):
        if hasattr(_stream, "reconfigure"):
            _stream.reconfigure(encoding="utf-8")

from mareforma import __version__, __description__


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _root() -> Path:
    return Path.cwd()


def _err(msg: str) -> None:
    click.echo(click.style("Error: ", fg="red", bold=True) + msg, err=True)


def _ok(msg: str) -> None:
    click.echo(click.style("✓ ", fg="green") + msg)


def _info(msg: str) -> None:
    click.echo(click.style("  ", fg="cyan") + msg)


# ---------------------------------------------------------------------------
# Root group
# ---------------------------------------------------------------------------

@click.group(help=f"{__description__}\n\nRun 'mareforma <command> --help' for details.")
@click.version_option(__version__, prog_name="mareforma")
def cli() -> None:
    pass


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@cli.command("status")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit JSON to stdout.")
def status_cmd(as_json: bool) -> None:
    """Show epistemic health dashboard.

    Traffic light: GREEN (≥1 replicated/established), YELLOW (all preliminary),
    RED (no claims).

    Examples:

        mareforma status

        mareforma status --json
    """
    import dataclasses
    from mareforma.db import open_db, DatabaseError
    from mareforma.health import compute_health

    root = _root()

    try:
        conn = open_db(root)
        try:
            report = compute_health(root, conn)
        finally:
            conn.close()
    except DatabaseError as exc:
        _err(f"Could not read graph.db: {exc}")
        sys.exit(1)

    if as_json:
        click.echo(json.dumps(dataclasses.asdict(report), indent=2))
        return

    click.echo("  " + "-" * 50)
    click.echo(
        f"  Claims:  {report.claims_open} open  /  "
        f"{report.claims_resolved} resolved  /  "
        f"{report.claims_contradicted} contradicted"
    )

    if report.support_level_breakdown:
        click.echo("  Support level breakdown:")
        for level in ("ESTABLISHED", "REPLICATED", "PRELIMINARY"):
            count = report.support_level_breakdown.get(level, 0)
            if count:
                bar = "█" * min(count, 20)
                click.echo(f"    {level:14} {bar}  {count}")

    click.echo("  " + "-" * 50)
    light_colors = {"green": "green", "yellow": "yellow", "red": "red"}
    color = light_colors.get(report.traffic_light, "white")
    click.echo(
        "  Status:  " +
        click.style(report.traffic_light.upper(), fg=color, bold=True)
    )
    click.echo(f"  Reason:  {report.rationale}")
    click.echo("")


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--output", default=None,
              help="Output path. Default: <cwd>/ontology.jsonld.")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Print JSON-LD to stdout instead of writing a file.")
def export(output: str | None, as_json: bool) -> None:
    """Export all claims as a JSON-LD document (ontology.jsonld).

    Examples:

        mareforma export

        cat ontology.jsonld | jq '.["@graph"][]'
    """
    from mareforma.exporters.jsonld import JSONLDExporter

    root = _root()

    try:
        exporter = JSONLDExporter(root)
        if as_json:
            doc = exporter.export()
            click.echo(json.dumps(doc, indent=2, ensure_ascii=False))
            return
        out_path = Path(output) if output else None
        written = exporter.write(out_path)
        _ok(f"Exported claims → {written.relative_to(root)}")
    except Exception as exc:
        _err(f"Export failed: {exc}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# claim
# ---------------------------------------------------------------------------

@cli.group()
def claim() -> None:
    """Manage scientific claims.

    Claims are falsifiable assertions with a classification (INFERRED |
    ANALYTICAL | DERIVED) and a graph-derived support level (PRELIMINARY →
    REPLICATED → ESTABLISHED).

    Examples:

        mareforma claim add "Target T is elevated in condition C"
            --classification ANALYTICAL --source dataset_alpha

        mareforma claim list --status open

        mareforma claim update <ID> --status contested
    """


@claim.command("add")
@click.argument("text")
@click.option("--classification", default="INFERRED", show_default=True,
              help="INFERRED, ANALYTICAL, or DERIVED.")
@click.option("--status", default="open", show_default=True,
              help="open, contested, or retracted.")
@click.option("--source", "source_name", default=None,
              help="Data source this claim derives from.")
@click.option("--supports", "supports", multiple=True, metavar="ID_OR_DOI",
              help="Upstream claim_id or DOI (repeatable).")
@click.option("--contradicts", "contradicts", multiple=True, metavar="ID_OR_DOI",
              help="Claim_id or DOI this claim contests (repeatable).")
@click.option("--generated-by", "generated_by", default="human", show_default=True,
              help="Agent identifier or 'human'.")
def claim_add(text, classification, status, source_name, supports, contradicts, generated_by):
    """Add a new scientific claim TEXT."""
    from mareforma.db import open_db, add_claim, DatabaseError

    root = _root()
    try:
        conn = open_db(root)
        try:
            claim_id = add_claim(
                conn, root, text,
                classification=classification,
                status=status,
                source_name=source_name,
                generated_by=generated_by,
                supports=list(supports) or None,
                contradicts=list(contradicts) or None,
            )
        finally:
            conn.close()
    except ValueError as exc:
        _err(str(exc))
        sys.exit(1)
    except DatabaseError as exc:
        _err(str(exc))
        sys.exit(1)

    _ok(f"Claim added [{classification}]: {text[:60]}{'...' if len(text) > 60 else ''}")
    _info(f"ID: {claim_id}")


@claim.command("list")
@click.option("--status", default=None, help="Filter: open, contested, retracted.")
@click.option("--source", "source_name", default=None, help="Filter by source name.")
@click.option("--json", "as_json", is_flag=True, default=False)
def claim_list(status, source_name, as_json):
    """List scientific claims, optionally filtered."""
    from mareforma.db import open_db, list_claims, DatabaseError

    root = _root()
    try:
        conn = open_db(root)
        try:
            claims = list_claims(conn, status=status, source_name=source_name)
        finally:
            conn.close()
    except DatabaseError as exc:
        _err(f"Failed to list claims: {exc}")
        sys.exit(1)

    if as_json:
        click.echo(json.dumps(claims, indent=2))
        return

    if not claims:
        _info("No claims found.")
        return

    click.echo(click.style(f"CLAIMS  ({len(claims)} total)", bold=True, fg="cyan"))
    click.echo("")
    for c in claims:
        click.echo(
            f"  [{c['status']:10}] [{c.get('support_level', 'PRELIMINARY'):12}] "
            f"[{c.get('classification', 'INFERRED'):10}] {c['text'][:60]}"
        )
        click.echo(f"             id: {c['claim_id']}")
        if c.get("source_name"):
            click.echo(f"         source: {c['source_name']}")
        click.echo("")


@claim.command("show")
@click.argument("claim_id")
@click.option("--json", "as_json", is_flag=True, default=False)
def claim_show(claim_id, as_json):
    """Show full details for a claim by ID."""
    from mareforma.db import open_db, get_claim, DatabaseError

    root = _root()
    try:
        conn = open_db(root)
        try:
            c = get_claim(conn, claim_id)
        finally:
            conn.close()
    except DatabaseError as exc:
        _err(f"Failed to fetch claim: {exc}")
        sys.exit(1)

    if c is None:
        _err(f"Claim '{claim_id}' not found.")
        sys.exit(1)

    if as_json:
        click.echo(json.dumps(c, indent=2))
        return

    click.echo(click.style("CLAIM", bold=True, fg="cyan"))
    click.echo(f"  id             : {c['claim_id']}")
    click.echo(f"  text           : {c['text']}")
    click.echo(f"  classification : {c.get('classification', 'INFERRED')}")
    click.echo(f"  support_level  : {c.get('support_level', 'PRELIMINARY')}")
    click.echo(f"  generated_by   : {c.get('generated_by', 'human')}")
    click.echo(f"  status         : {c['status']}")
    if c.get("source_name"):
        click.echo(f"  source         : {c['source_name']}")
    supports = json.loads(c.get("supports_json", "[]") or "[]")
    contradicts = json.loads(c.get("contradicts_json", "[]") or "[]")
    if supports:
        click.echo(f"  supports       : {', '.join(supports)}")
    if contradicts:
        click.echo(f"  contradicts    : {', '.join(contradicts)}")
    if c.get("comparison_summary"):
        click.echo(f"  summary        : {c['comparison_summary']}")
    click.echo(f"  created_at     : {c['created_at']}")
    click.echo(f"  updated_at     : {c['updated_at']}")


@claim.command("update")
@click.argument("claim_id")
@click.option("--status", default=None, help="New status: open, contested, retracted.")
@click.option("--text", default=None, help="New claim text.")
@click.option("--supports", "supports", multiple=True, metavar="ID_OR_DOI")
@click.option("--contradicts", "contradicts", multiple=True, metavar="ID_OR_DOI")
def claim_update(claim_id, status, text, supports, contradicts):
    """Update fields on an existing claim by ID."""
    from mareforma.db import open_db, update_claim, DatabaseError, ClaimNotFoundError

    root = _root()
    try:
        conn = open_db(root)
        try:
            update_claim(
                conn, root, claim_id,
                status=status,
                text=text,
                supports=list(supports) if supports else None,
                contradicts=list(contradicts) if contradicts else None,
            )
        finally:
            conn.close()
    except ClaimNotFoundError as exc:
        _err(str(exc))
        sys.exit(1)
    except ValueError as exc:
        _err(str(exc))
        sys.exit(1)
    except DatabaseError as exc:
        _err(f"Failed to update claim: {exc}")
        sys.exit(1)

    _ok(f"Claim '{claim_id}' updated.")
