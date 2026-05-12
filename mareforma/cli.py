"""
cli.py — Mareforma command-line interface.

Commands
--------
    mareforma bootstrap                        generate Ed25519 signing key
    mareforma validator add --pubkey ...       enroll a new validator
    mareforma validator list                   list enrolled validators
    mareforma claim add TEXT [options]         assert a scientific claim
    mareforma claim list [--status] [--source] list claims
    mareforma claim show ID                    show claim details
    mareforma claim update ID [options]        update a claim
    mareforma claim validate ID [options]      promote REPLICATED → ESTABLISHED
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
# bootstrap — one-time identity setup
# ---------------------------------------------------------------------------

@cli.command("bootstrap")
@click.option(
    "--key-path", default=None,
    help="Override the default key path (~/.config/mareforma/key).",
)
@click.option(
    "--overwrite", is_flag=True, default=False,
    help="Replace an existing key. DESTRUCTIVE: every claim signed by the "
         "prior key becomes unverifiable AND any claim not yet submitted to "
         "Rekor (transparency_logged=0) becomes permanently un-loggable. "
         "Back up the old key and drain the unlogged queue first.",
)
def bootstrap_cmd(key_path: str | None, overwrite: bool) -> None:
    """Generate an Ed25519 signing key for this user.

    Run once after installing mareforma. The key is written to
    ``~/.config/mareforma/key`` (XDG-compliant) with mode 0600. Every claim
    written via ``mareforma.open()`` is then signed with this key.

    To verify a claim, share the public key (printed below) with whoever
    needs to validate your output.

    ``--overwrite`` is destructive: it strands every claim signed by the
    prior key — both for verification and for any pending Rekor submission.
    See ``mareforma.signing.bootstrap_key`` for the safe rotation path.
    """
    from mareforma import signing as _signing

    target = Path(key_path) if key_path else _signing.default_key_path()
    try:
        path, keyid = _signing.bootstrap_key(target, overwrite=overwrite)
    except _signing.SigningError as exc:
        _err(str(exc))
        sys.exit(1)

    _ok(f"Generated signing key at {path}")
    _info(f"Public key id: {keyid}")
    _info("Share the keyid with collaborators so they can verify your claims.")


# ---------------------------------------------------------------------------
# validator — manage the per-project validators table
# ---------------------------------------------------------------------------

@cli.group()
def validator() -> None:
    """Manage the per-project validators table (who may promote ESTABLISHED)."""


@validator.command("add")
@click.option(
    "--pubkey", "pubkey_arg", required=True,
    help="PEM-encoded public key. Pass a file path or paste the PEM text.",
)
@click.option(
    "--identity", required=True,
    help="Display label for the validator (email, lab name, etc.).",
)
def validator_add(pubkey_arg: str, identity: str) -> None:
    """Enroll a new validator on the current project.

    The currently loaded signing key (from ``~/.config/mareforma/key`` or
    the path passed to ``mareforma.open(key_path=...)``) signs the
    enrollment and becomes the parent of the new validator. The signer
    must already be enrolled — typically because they were the first key
    opened against this project's graph.db and auto-enrolled as the root.
    """
    import mareforma
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    # 64 KB is generous — Ed25519 PEM public keys are well under 1 KB.
    # The cap prevents `--pubkey /var/log/syslog` (or any oversized
    # readable file) from loading megabytes into RAM before PEM parsing
    # rejects them.
    _MAX_PEM_SIZE = 64 * 1024

    pem_bytes: bytes
    pubkey_path = Path(pubkey_arg)
    if pubkey_path.exists():
        try:
            with pubkey_path.open("rb") as fh:
                pem_bytes = fh.read(_MAX_PEM_SIZE + 1)
        except OSError as exc:
            _err(f"Could not read {pubkey_path}: {exc}")
            sys.exit(1)
        if len(pem_bytes) > _MAX_PEM_SIZE:
            _err(
                f"--pubkey file {pubkey_path} exceeds the "
                f"{_MAX_PEM_SIZE}-byte limit; an Ed25519 PEM should be "
                "well under 1 KB. Pass the actual public-key file."
            )
            sys.exit(1)
    else:
        pem_bytes = pubkey_arg.encode("utf-8")

    try:
        _signing.public_key_from_pem(pem_bytes)
    except _signing.SigningError as exc:
        _err(f"Invalid public key: {exc}")
        sys.exit(1)

    try:
        with mareforma.open(_root()) as graph:
            if graph._signer is None:
                _err(
                    "No signing key loaded. Run `mareforma bootstrap` first, "
                    "or pass key_path explicitly via the library API."
                )
                sys.exit(1)
            try:
                row = _validators.enroll_validator(
                    graph._conn, graph._signer, pem_bytes, identity=identity,
                )
            except _validators.ValidatorNotEnrolledError as exc:
                _err(str(exc))
                sys.exit(1)
            except _validators.ValidatorAlreadyEnrolledError as exc:
                _err(str(exc))
                sys.exit(1)
    except _signing.SigningError as exc:
        _err(str(exc))
        sys.exit(1)

    _ok(f"Enrolled validator {row['identity']}")
    _info(f"keyid:            {row['keyid']}")
    _info(f"enrolled_by:      {row['enrolled_by_keyid']}")
    _info(f"enrolled_at:      {row['enrolled_at']}")


@validator.command("list")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit JSON to stdout.")
def validator_list(as_json: bool) -> None:
    """List enrolled validators for the current project."""
    import mareforma
    from mareforma import validators as _validators

    with mareforma.open(_root()) as graph:
        rows = _validators.list_validators(graph._conn)

    if as_json:
        click.echo(json.dumps(rows, indent=2))
        return

    if not rows:
        _info("No validators enrolled. Run `mareforma bootstrap` and open "
              "the project once with that key to enroll the root validator.")
        return

    for row in rows:
        is_root = row["enrolled_by_keyid"] == row["keyid"]
        marker = " (root)" if is_root else ""
        click.echo(click.style(f"  {row['identity']}{marker}", bold=True))
        click.echo(f"    keyid:       {row['keyid']}")
        click.echo(f"    enrolled_by: {row['enrolled_by_keyid']}")
        click.echo(f"    enrolled_at: {row['enrolled_at']}")


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
@click.option("--generated-by", "generated_by", default="agent", show_default=True,
              help="Agent identifier.")
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
    click.echo(f"  generated_by   : {c.get('generated_by', 'agent')}")
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


@claim.command("validate")
@click.argument("claim_id")
@click.option("--validated-by", "validated_by", default=None,
              help="Identifier of the human reviewer (e.g. email).")
def claim_validate(claim_id, validated_by):
    """Promote a REPLICATED claim to ESTABLISHED (human validation).

    The currently loaded signing key (from ``~/.config/mareforma/key``)
    must be enrolled as a validator on this project. The validation
    event is signed and the signed envelope is persisted to the row.

    Examples:

        mareforma claim validate <ID>

        mareforma claim validate <ID> --validated-by reviewer@example.org
    """
    import mareforma
    from mareforma.db import DatabaseError, ClaimNotFoundError

    try:
        with mareforma.open(_root()) as graph:
            graph.validate(claim_id, validated_by=validated_by)
    except ClaimNotFoundError as exc:
        _err(str(exc))
        sys.exit(1)
    except ValueError as exc:
        _err(str(exc))
        sys.exit(1)
    except DatabaseError as exc:
        _err(f"Failed to validate claim: {exc}")
        sys.exit(1)

    _ok(f"Claim '{claim_id}' promoted to ESTABLISHED.")
    if validated_by:
        _info(f"validated_by: {validated_by}")
