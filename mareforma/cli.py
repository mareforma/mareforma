"""
cli.py: Mareforma command-line interface.

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
    mareforma restore [path]                   rebuild graph.db from claims.toml
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


def _discover_root(start: "Path | None" = None) -> "Path | None":
    """Nearest ancestor of *start* (cwd) that holds a mareforma project.

    Returns the directory containing ``.mareforma/graph.db``, or None when no
    project exists at or above cwd. Read-only commands use this so they report
    on an existing project (from a subdirectory too) rather than silently
    creating an empty ``graph.db`` in the current directory.
    """
    cur = (start or Path.cwd()).resolve()
    for d in (cur, *cur.parents):
        if (d / ".mareforma" / "graph.db").exists():
            return d
    return None


def _read_only_root() -> Path:
    """Resolve the project root for a read-only command, or exit 1 if none.

    Never creates a project, that is a write-path side effect.
    """
    root = _discover_root()
    if root is None:
        _err(
            "No mareforma project here or in any parent directory. Write a "
            "claim first (e.g. `mareforma claim add ...`) to create one."
        )
        sys.exit(1)
    return root


def _err(msg: str) -> None:
    click.echo(click.style("Error: ", fg="red", bold=True) + msg, err=True)


def _ok(msg: str) -> None:
    click.echo(click.style("✓ ", fg="green") + msg)


def _info(msg: str) -> None:
    click.echo(click.style("  ", fg="cyan") + msg)


_TIER_FG = {"COMPUTED": "green", "PROXIED": "yellow", "DEFERRED": "white"}


def _trust_map_plaintext(tmap) -> str:
    """Unstyled text rendering of a TrustMap, for writing to a file (no ANSI)."""
    lines = [
        "TRUST MAP",
        f"  {tmap.subject_kind} {tmap.subject_id}",
        f"  map version: {tmap.version}",
        "",
    ]
    for p in tmap.properties:
        val = ", " if p.value is None else str(p.value)
        lines.append(f"  {p.name:24} [{p.tier.value:8}] {val}")
        lines.append(f"      {p.residual}")
        lines.append("")
    return "\n".join(lines)


def _echo_trust_map(tmap, *, redact_home: bool = False) -> None:
    """Print a TrustMap as a human-readable ledger."""
    def out(line: str) -> None:
        click.echo(_redact_home(line) if redact_home else line)

    click.echo(click.style("TRUST MAP", bold=True, fg="cyan"))
    out(f"  {tmap.subject_kind} {tmap.subject_id}")
    click.echo(f"  map version: {tmap.version}")
    click.echo("")
    for p in tmap.properties:
        color = _TIER_FG.get(p.tier.value, "white")
        val = ", " if p.value is None else str(p.value)
        click.echo(
            "  " + click.style(f"{p.name:24}", bold=True) + " "
            + click.style(f"[{p.tier.value:8}]", fg=color) + " "
        )
        out(f"      {val}")
        out(f"      {p.residual}")
        click.echo("")


# ---------------------------------------------------------------------------
# Root group
# ---------------------------------------------------------------------------

@click.group(help=f"{__description__}\n\nRun 'mareforma <command> --help' for details.")
@click.version_option(__version__, prog_name="mareforma")
def cli() -> None:
    pass


# ---------------------------------------------------------------------------
# bootstrap, one-time identity setup
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
    prior key, both for verification and for any pending Rekor submission.
    See ``mareforma.signing.bootstrap_key`` for the safe rotation path.
    """
    from mareforma import signing as _signing

    target = Path(key_path) if key_path else _signing.default_key_path()
    try:
        path, keyid = _signing.bootstrap_key(target, overwrite=overwrite)
    except _signing.SigningError as exc:
        hint = " Pass --overwrite to replace it." if not overwrite else ""
        _err(f"{exc}{hint}")
        sys.exit(1)

    _ok(f"Generated signing key at {path}")
    _info(f"Public key id: {keyid}")
    _info("Share the keyid with collaborators so they can verify your claims.")
    _info("")
    _info("Next steps:")
    _info("  • The first key opened against a project's graph auto-enrolls")
    _info("    as the root validator on that project.")
    _info("  • To promote a claim to ESTABLISHED you need a SECOND enrolled")
    _info("    key (mareforma refuses self-validation). Have a")
    _info("    collaborator run `mareforma bootstrap`, then run")
    _info("    `mareforma key show --pem > pubkey.pem` and send it to you;")
    _info("    enroll them with `mareforma validator add --pubkey pubkey.pem")
    _info("    --identity <label>`.")


# ---------------------------------------------------------------------------
# key, inspect the locally-configured signing key
# ---------------------------------------------------------------------------

@cli.group()
def key() -> None:
    """Inspect the locally-configured signing key."""


@key.command("show")
@click.option(
    "--key-path", default=None,
    help="Override the default key path (~/.config/mareforma/key).",
)
@click.option(
    "--pem", "as_pem", is_flag=True, default=False,
    help="Emit ONLY the PEM-encoded public key to stdout (no other output). "
         "Pipe to a file when sending to a project admin who will enroll you "
         "as a validator: `mareforma key show --pem > pubkey.pem`.",
)
@click.option(
    "--keyid", "as_keyid", is_flag=True, default=False,
    help="Emit ONLY the keyid (SHA-256 hex of the raw pubkey bytes) to stdout. "
         "Useful for scripting and for confirming which key is loaded.",
)
def key_show(key_path: str | None, as_pem: bool, as_keyid: bool) -> None:
    """Print the locally-configured public key.

    The private key never leaves the file at ``--key-path`` (or
    ``~/.config/mareforma/key``). What this command emits is the
    PUBLIC half: safe to email, paste, or pipe.

    \b
    Examples:
        mareforma key show                    # human-readable identity card
        mareforma key show --pem > pub.pem    # for `validator add --pubkey`
        mareforma key show --keyid            # short hash for scripts
    """
    from mareforma import signing as _signing

    if as_pem and as_keyid:
        _err("--pem and --keyid are mutually exclusive.")
        sys.exit(1)

    target = Path(key_path) if key_path else _signing.default_key_path()
    if not target.exists():
        _err(
            f"No signing key at {target}. Run `mareforma bootstrap` to "
            "create one."
        )
        sys.exit(1)

    try:
        private = _signing.load_private_key(target)
    except _signing.SigningError as exc:
        _err(f"Could not load key at {target}: {exc}")
        sys.exit(1)

    public = private.public_key()
    keyid = _signing.public_key_id(public)
    pem_bytes = _signing.public_key_to_pem(public)

    if as_pem:
        # Raw PEM to stdout, no styling, no trailing newline added beyond
        # the PEM's own. Designed for `> pub.pem` redirection.
        click.echo(pem_bytes.decode("ascii"), nl=False)
        return

    if as_keyid:
        click.echo(keyid)
        return

    _ok(f"Signing key at {target}")
    _info(f"keyid: {keyid}")
    _info("")
    _info("Public PEM (safe to share):")
    click.echo(pem_bytes.decode("ascii"), nl=False)


# ---------------------------------------------------------------------------
# validator, manage the per-project validators table
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
@click.option(
    "--type", "validator_type",
    type=click.Choice(["human", "llm"]), default="human", show_default=True,
    help=(
        "Self-declared validator type. 'human' may promote claims to "
        "ESTABLISHED; 'llm' may sign validations but cannot promote "
        "past REPLICATED."
    ),
)
def validator_add(pubkey_arg: str, identity: str, validator_type: str) -> None:
    """Enroll a new validator on the current project.

    The currently loaded signing key (from ``~/.config/mareforma/key`` or
    the path passed to ``mareforma.open(key_path=...)``) signs the
    enrollment and becomes the parent of the new validator. The signer
    must already be enrolled, typically because they were the first key
    opened against this project's graph.db and auto-enrolled as the root.

    \b
    Examples:
        mareforma validator add --pubkey alice.pem --identity alice@lab.org
        mareforma validator add --pubkey bot.pem --identity reviewer-bot --type llm
    """
    import mareforma
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    # 64 KB is generous, Ed25519 PEM public keys are well under 1 KB.
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
                    graph._conn, graph._signer, pem_bytes,
                    identity=identity, validator_type=validator_type,
                )
            except _validators.ValidatorNotEnrolledError as exc:
                _err(str(exc))
                sys.exit(1)
            except _validators.ValidatorAlreadyEnrolledError as exc:
                _err(str(exc))
                sys.exit(1)
            except _validators.InvalidValidatorTypeError as exc:
                _err(str(exc))
                sys.exit(1)
    except _signing.SigningError as exc:
        _err(str(exc))
        sys.exit(1)

    _ok(f"Enrolled validator {row['identity']} ({row['validator_type']})")
    _info(f"keyid:            {row['keyid']}")
    _info(f"validator_type:   {row['validator_type']}")
    _info(f"enrolled_by:      {row['enrolled_by_keyid']}")
    _info(f"enrolled_at:      {row['enrolled_at']}")


@validator.command("list")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit JSON to stdout.")
def validator_list(as_json: bool) -> None:
    """List enrolled validators for the current project."""
    import mareforma
    from mareforma import validators as _validators

    with mareforma.open(_read_only_root()) as graph:
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
        type_tag = f" [{row['validator_type']}]"
        click.echo(click.style(
            f"  {row['identity']}{type_tag}{marker}", bold=True,
        ))
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

    \b
    Examples:
        mareforma status
        mareforma status --json
    """
    import dataclasses
    import sqlite3
    from mareforma.db import open_db, DatabaseError
    from mareforma.health import compute_health

    root = _read_only_root()

    try:
        conn = open_db(root)
        try:
            report = compute_health(root, conn)
        finally:
            conn.close()
    except (DatabaseError, sqlite3.DatabaseError) as exc:
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


@cli.command("activity")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit JSON to stdout instead of a formatted table.")
@click.option("--last", "last_n", type=int, default=None,
              help="Aggregate only the last N events. Default: all events.")
def activity_cmd(as_json: bool, last_n: int | None) -> None:
    """Show rolling operational rates from the activity log.

    The activity log captures one JSONL line per mareforma operation
    that produces operational signal: provenance queries, grounding
    sensor verdicts, refresh_unsigned retries. ``mareforma activity``
    aggregates the log and prints rolling rates (grounding-pass-rate,
    Rekor-log-recovery-rate) so an operator can see how mareforma
    is behaving over time without re-querying graph.db.

    Distinct from ``mareforma status``: ``status`` is a snapshot of
    graph state right now; ``activity`` is a rolling view of what
    mareforma has been doing.

    \b
    Examples:
        mareforma activity
        mareforma activity --last=100
        mareforma activity --json
    """
    from mareforma.health import compute_rolling_stats
    root = _read_only_root()
    stats = compute_rolling_stats(root, last_n=last_n)
    if as_json:
        click.echo(json.dumps(stats, indent=2, sort_keys=True))
        if stats.get("read_error"):
            sys.exit(1)
        return
    if stats.get("read_error"):
        _err("Activity log unreadable. Check filesystem permissions "
             "on .mareforma/health.jsonl.")
        sys.exit(1)
    click.echo("  " + "-" * 50)
    click.echo(f"  Events scanned: {stats['events_total']}")
    malformed = stats.get("malformed_lines", 0)
    if malformed:
        click.echo(
            f"  Malformed lines: {malformed}  "
            "(check .mareforma/health.jsonl for corruption)"
        )
    if not stats["ops"]:
        click.echo("  No operational events recorded yet.")
        click.echo("  " + "-" * 50)
        return
    for op in sorted(stats["ops"]):
        bucket = stats["ops"][op]
        click.echo(f"  {op}:  {bucket['count']} events  "
                   f"({bucket['ok']} ok / {bucket['fail']} fail / "
                   f"{bucket['partial']} partial)")
        extras = {
            k: v for k, v in bucket.items()
            if k not in ("count", "ok", "fail", "partial")
        }
        for k, v in sorted(extras.items()):
            click.echo(f"      {k}: {v}")
    click.echo("  " + "-" * 50)


@cli.command("stats", hidden=True, deprecated=True)
@click.option("--json", "as_json", is_flag=True, default=False)
@click.option("--last", "last_n", type=int, default=None)
@click.pass_context
def stats_cmd(ctx: click.Context, as_json: bool, last_n: int | None) -> None:
    """Deprecated alias of ``mareforma activity``.

    ``mareforma stats`` and ``mareforma status`` are one letter apart
    and semantically different (rolling rates vs. snapshot), so v0.3.1
    renames the rolling-rates command to ``mareforma activity``. The
    old name is kept as an alias for one release; v0.4 removes it.
    """
    import warnings as _warnings
    _warnings.warn(
        "`mareforma stats` has been renamed to `mareforma activity` "
        "to break the stats/status homonym; the alias will be removed "
        "in v0.4. Switch your scripts to `mareforma activity`.",
        DeprecationWarning,
        stacklevel=2,
    )
    ctx.invoke(activity_cmd, as_json=as_json, last_n=last_n)


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--output", default=None,
              help="Output path. Default depends on --format / --bundle.")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Print JSON to stdout instead of writing a file.")
@click.option("--bundle", is_flag=True, default=False,
              help="Produce a SCITT-style signed bundle (in-toto Statement "
                   "v1 + DSSE envelope). Requires a loaded signing key.")
@click.option(
    "--format", "fmt",
    type=click.Choice(["jsonld", "in-toto-v1", "ro-crate-1.2", "prov-o"]),
    default="jsonld",
    help=(
        "Export format. 'jsonld' (default) = mareforma-native JSON-LD; "
        "'in-toto-v1' = unsigned in-toto Statement v1 (sigstore / SLSA / "
        "GUAC ecosystem); 'ro-crate-1.2' = RO-Crate 1.2 Process Run Crate "
        "metadata (Galaxy / EuroScienceGateway / FAIR-EASE ecosystem); "
        "'prov-o' = W3C PROV-O JSON-LD for provenance-aware tooling. "
        "Use --bundle for a signed in-toto Statement v1 (different from "
        "--format=in-toto-v1 which is unsigned)."
    ),
)
def export(
    output: str | None, as_json: bool, bundle: bool, fmt: str,
) -> None:
    """Export all claims, optionally as a signed bundle or interop format.

    \b
    Examples:
        mareforma export
        mareforma export --bundle
        mareforma export --format=in-toto-v1
        mareforma export --format=ro-crate-1.2 --output crate-metadata.json
        cat ontology.jsonld | jq '.["@graph"][]'
    """
    root = _read_only_root()

    if bundle and fmt != "jsonld":
        _err(
            "--bundle and --format are mutually exclusive. --bundle produces "
            "a signed in-toto v1 envelope; --format selects an unsigned "
            "export shape. Choose one."
        )
        sys.exit(1)

    def _display_path(p: Path) -> str:
        """Show paths inside root as relative; absolute outside the tree."""
        try:
            return str(p.relative_to(root))
        except ValueError:
            return str(p)

    if fmt == "in-toto-v1":
        from mareforma.exporters.in_toto import build_statement
        try:
            statement = build_statement(root)
            if as_json:
                click.echo(json.dumps(statement, indent=2, ensure_ascii=False))
                return
            out_path = (
                Path(output) if output else root / "mareforma-statement.json"
            )
            out_path.write_text(
                json.dumps(statement, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            _ok(f"Exported in-toto Statement v1 → {_display_path(out_path)}")
        except Exception as exc:
            _err(f"in-toto export failed: {exc}")
            sys.exit(1)
        return

    if fmt == "ro-crate-1.2":
        from mareforma.exporters.ro_crate import build_crate
        try:
            crate = build_crate(root)
            if as_json:
                click.echo(json.dumps(crate, indent=2, ensure_ascii=False))
                return
            out_path = (
                Path(output) if output else root / "ro-crate-metadata.json"
            )
            out_path.write_text(
                json.dumps(crate, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            _ok(f"Exported RO-Crate 1.2 → {_display_path(out_path)}")
        except Exception as exc:
            _err(f"RO-Crate export failed: {exc}")
            sys.exit(1)
        return

    if fmt == "prov-o":
        from mareforma.exporters.prov_o import build_prov_o
        try:
            doc = build_prov_o(root)
            if as_json:
                click.echo(json.dumps(doc, indent=2, ensure_ascii=False))
                return
            out_path = (
                Path(output) if output else root / "mareforma-prov-o.jsonld"
            )
            out_path.write_text(
                json.dumps(doc, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            _ok(f"Exported PROV-O JSON-LD → {_display_path(out_path)}")
        except Exception as exc:
            _err(f"PROV-O export failed: {exc}")
            sys.exit(1)
        return

    if bundle:
        # Signed bundle path, needs a key.
        from mareforma import signing as _signing
        from mareforma.export_bundle import write_bundle
        try:
            key_path = _signing.default_key_path()
            if not key_path.exists():
                _err(
                    "mareforma export --bundle requires a signing key. "
                    "Run `mareforma bootstrap` first."
                )
                sys.exit(1)
            private_key = _signing.load_private_key(key_path)
            out_path = (
                Path(output)
                if output
                else root / "mareforma-bundle.json"
            )
            written = write_bundle(root, out_path, private_key)
            _ok(f"Exported signed bundle → {_display_path(written)}")
        except Exception as exc:
            _err(f"Bundle export failed: {exc}")
            sys.exit(1)
        return

    from mareforma.exporters.jsonld import JSONLDExporter

    try:
        exporter = JSONLDExporter(root)
        if as_json:
            doc = exporter.export()
            click.echo(json.dumps(doc, indent=2, ensure_ascii=False))
            return
        out_path = Path(output) if output else None
        written = exporter.write(out_path)
        _ok(f"Exported claims → {_display_path(written)}")
    except Exception as exc:
        _err(f"Export failed: {exc}")
        sys.exit(1)


# verify exit-code contract (E3, stable across releases, keyed by CI gates):
#   0  verified
#   1  tamper or binding violation (a definite NO)
#   2  unverifiable (missing material to reach a verdict, not a NO)
#   3  usage error (a bad flag / missing argument, NOT one of the above)
# The split between 1 and 2 is the whole point: a CI gate must be able to tell
# "this claim is tampered" from "I could not check this claim." Click defaults a
# usage error to exit 2, which would collide with "unverifiable"; the command
# class below remaps it to 3 so a typo'd flag can never read as a verdict.
_VERIFY_OK = 0
_VERIFY_FAIL = 1
_VERIFY_UNVERIFIABLE = 2
_VERIFY_USAGE = 3


class _VerifyCommand(click.Command):
    """A command whose click usage errors exit 3, distinct from the 0/1/2 verdicts.

    A bad flag or missing argument is neither "verified", "tampered", nor
    "unverifiable", surfacing it as exit 2 would let a CI gate misread a typo as
    "could not verify." Bumping the usage-error exit code keeps the verdict codes
    unambiguous.
    """

    def parse_args(self, ctx, args):
        try:
            return super().parse_args(ctx, args)
        except click.UsageError as exc:
            exc.exit_code = _VERIFY_USAGE
            raise


def _verify_signed_file(
    path: Path, as_json: bool, key_path: str | None = None,
) -> int:
    """Route a signed file to its verifier by payload type.

    A per-finding audit receipt and a bundle are both DSSE envelopes; the
    ``payloadType`` names which one this is, so the router never guesses from
    the filename. A file that is not JSON at all falls through to the bundle
    verifier, which reports the precise failure. ``key_path`` (from ``--key``)
    pins the signer's key when the default local key is not the signer.
    """
    from mareforma import signing as _signing

    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        doc = None
    if (
        isinstance(doc, dict)
        and doc.get("payloadType") == _signing.PAYLOAD_TYPE_AUDIT_RECEIPT
    ):
        return _verify_audit_receipt_file(path, doc, as_json, key_path)
    if (
        isinstance(doc, dict)
        and doc.get("payloadType") == _signing.PAYLOAD_TYPE_AUDIT_RUN
    ):
        return _verify_audit_run_file(path, doc, as_json, key_path)
    return _verify_bundle_file(path, as_json, key_path)


def _load_verify_public_key(path: Path):
    """Load the public half a verifier needs, from a private key or a public PEM.

    An audit receipt is verified with the auditor's public key, so a third party
    who holds only the exported public PEM (``mareforma key show --pem``) can
    check it. A private key file yields its public half; a public PEM is used as
    is. Anything else raises ``SigningError`` so the caller reports it as
    unverifiable, never a false tamper.
    """
    from mareforma import signing as _signing

    try:
        return _signing.load_private_key(path).public_key()
    except _signing.SigningError:
        pass
    try:
        return _signing.public_key_from_pem(path.read_bytes())
    except Exception as exc:  # noqa: BLE001
        raise _signing.SigningError(
            f"{path} is neither a signing key nor a public key PEM"
        ) from exc


def _verify_audit_run_file(
    path: Path, envelope: dict, as_json: bool, key_path: str | None = None,
) -> int:
    """Report on a signed audit ``run.json`` without ever calling it tamper.

    The run record is the auditor's own DSSE envelope; its ``completed`` flag
    is the corpus resume key read by ``_run_completed``, not a claim verdict.
    So a valid signature here is UNVERIFIABLE (use resume, not verify), never
    ``_VERIFY_OK``; an absent or wrong key is UNVERIFIABLE too. This type never
    reaches the bundle verifier, so an authentic record can never read as the
    ``_VERIFY_FAIL`` tamper class the way a bundle payload-type mismatch does.
    """
    from mareforma import signing as _signing
    from mareforma.signing import verify_envelope

    def emit(reason: str) -> int:
        if as_json:
            click.echo(json.dumps(
                {"target": str(path), "target_kind": "audit-run",
                 "verdict": "unverifiable", "exit_code": _VERIFY_UNVERIFIABLE,
                 "reason": reason},
                indent=2))
        else:
            _err(reason)
        return _VERIFY_UNVERIFIABLE

    pinned = key_path is not None
    verify_key_path = Path(key_path) if pinned else _signing.default_key_path()
    if not verify_key_path.exists():
        return emit(
            "no signer key available to check this audit run record. This is "
            "unverifiable, not a failure; audit run records are trusted via "
            "resume, not verify.")
    try:
        public_key = _load_verify_public_key(verify_key_path)
    except _signing.SigningError as exc:
        return emit(f"could not load the key to check this run record: {exc}")
    ok = verify_envelope(
        envelope, public_key,
        expected_payload_type=_signing.PAYLOAD_TYPE_AUDIT_RUN)
    if ok:
        return emit(
            "audit run record: signature checks out. This is not a claim "
            "verdict; a run record is trusted through resume, not verify.")
    return emit(
        "audit run record: signature did not check out with this key. Pin the "
        "auditor's key with --key. Run records are trusted via resume.")


def _verify_audit_receipt_file(
    path: Path, envelope: dict, as_json: bool, key_path: str | None = None,
) -> int:
    """Verify a signed per-finding audit receipt with public material only.

    Same auditor posture as the bundle path: the key's public half is the
    material a verifier has; an absent key is UNVERIFIABLE (exit 2), a
    grounding-binding violation or a tampered payload is a definite failure
    (exit 1). A receipt signed with a different key than the one supplied is
    UNVERIFIABLE (wrong key, not tamper) unless the caller pinned that key
    with ``--key``, in which case the mismatch is a definite failure.
    """
    from mareforma import signing as _signing
    from mareforma.audit import RECEIPT_KEY_MISMATCH_REASON, verify_audit_receipt

    def emit(verdict: str, code: int, reason: str) -> int:
        if as_json:
            click.echo(json.dumps(
                {"target": str(path), "target_kind": "audit-receipt",
                 "verdict": verdict, "exit_code": code, "reason": reason},
                indent=2))
        elif code == _VERIFY_OK:
            _ok(reason)
        else:
            _err(reason)
        return code

    pinned = key_path is not None
    verify_key_path = Path(key_path) if pinned else _signing.default_key_path()
    if not verify_key_path.exists():
        if pinned:
            return emit(
                "unverifiable", _VERIFY_UNVERIFIABLE,
                f"no signer key at {verify_key_path}. This is unverifiable, "
                "not a failure; supply an existing key with --key.")
        return emit(
            "unverifiable", _VERIFY_UNVERIFIABLE,
            "no public key available to verify this receipt (no local key "
            "found). This is unverifiable, not a failure; supply the "
            "signer's key with --key.")
    try:
        public_key = _load_verify_public_key(verify_key_path)
        ok, reason = verify_audit_receipt(envelope, public_key)
    except _signing.InvalidEnvelopeError as exc:
        return emit("tampered", _VERIFY_FAIL, f"malformed audit receipt: {exc}")
    except _signing.SigningError as exc:
        return emit("unverifiable", _VERIFY_UNVERIFIABLE,
                    f"could not load the key to verify: {exc}")
    if not ok:
        if reason == RECEIPT_KEY_MISMATCH_REASON and not pinned:
            return emit(
                "unverifiable", _VERIFY_UNVERIFIABLE,
                "this receipt was signed with a different key than the local "
                "one. This is unverifiable, not a failure; pin the signer's "
                "key with --key.")
        return emit("tampered", _VERIFY_FAIL, reason)
    return emit("verified", _VERIFY_OK, reason)


def _verify_bundle_file(
    path: Path, as_json: bool, key_path: str | None = None,
) -> int:
    """Verify a signed bundle with public material only. Returns an exit code.

    Auditor posture: verification needs only the public key. The local signing
    key (its public half) is the material a solo operator has; when it is absent
    the bundle is UNVERIFIABLE (exit 2), never a failure (exit 1), the auditor
    lacks the key, the bundle is not proven tampered. ``key_path`` (from
    ``--key``) pins the signer's key when the default local key is not it.
    """
    from mareforma import signing as _signing
    from mareforma.export_bundle import BundleVerificationError, verify_bundle

    verify_key_path = (
        Path(key_path) if key_path is not None else _signing.default_key_path()
    )
    if not verify_key_path.exists():
        reason = (
            "no public key available to verify this bundle (no local key found). "
            "This is unverifiable, not a failure; supply the signer's key."
        )
        if as_json:
            click.echo(json.dumps(
                {"target": str(path), "target_kind": "bundle",
                 "verdict": "unverifiable", "exit_code": _VERIFY_UNVERIFIABLE,
                 "reason": reason}, indent=2))
        else:
            _err(reason)
        return _VERIFY_UNVERIFIABLE
    try:
        private_key = _signing.load_private_key(verify_key_path)
        statement = verify_bundle(path, private_key.public_key())
    except BundleVerificationError as exc:
        reason = f"bundle verification failed: {exc}"
        if as_json:
            click.echo(json.dumps(
                {"target": str(path), "target_kind": "bundle",
                 "verdict": "tampered", "exit_code": _VERIFY_FAIL,
                 "reason": reason}, indent=2))
        else:
            _err(reason)
        return _VERIFY_FAIL
    except _signing.SigningError as exc:
        reason = f"could not load the local key to verify: {exc}"
        if as_json:
            click.echo(json.dumps(
                {"target": str(path), "target_kind": "bundle",
                 "verdict": "unverifiable", "exit_code": _VERIFY_UNVERIFIABLE,
                 "reason": reason}, indent=2))
        else:
            _err(reason)
        return _VERIFY_UNVERIFIABLE
    n_subjects = len(statement.get("subject") or [])
    if as_json:
        click.echo(json.dumps(
            {"target": str(path), "target_kind": "bundle",
             "verdict": "verified", "exit_code": _VERIFY_OK,
             "subjects": n_subjects}, indent=2))
    else:
        _ok(f"Bundle verified: {n_subjects} claim subject(s) match.")
    return _VERIFY_OK


def _verify_claim(target: str, as_json: bool, redact_home: bool) -> int:
    """Verify a stored claim end to end and print its trust map.

    Auditor mode: uses only public material (the graph's enrolled validator
    pubkeys). Re-verifies signatures on read, re-checks the grounding→citation
    binding against the FROZEN routine, and prints the trust map. A claim that
    cannot be located is UNVERIFIABLE (2); a signature or binding that fails is a
    definite tamper/violation (1).
    """
    import mareforma
    from mareforma.db import DatabaseError, verify_claim_signatures
    from mareforma.observe._binding import check_grounding_binding
    from mareforma.trust_map import build_trust_map, parse_grounding_record

    def emit_json(payload: dict) -> None:
        # Every JSON path, success AND failure, honors --redact-home. The
        # tampered/unverifiable payloads embed the trust map whose residuals
        # carry cited-source paths, so a dropped redaction here would leak
        # $HOME on exactly the receipt an auditor forwards.
        text = json.dumps(payload, indent=2)
        click.echo(_redact_home(text) if redact_home else text)

    def unverifiable(reason: str) -> int:
        if as_json:
            emit_json({"target": target, "target_kind": "claim",
                       "verdict": "unverifiable",
                       "exit_code": _VERIFY_UNVERIFIABLE, "reason": reason})
        else:
            _err(reason)
        return _VERIFY_UNVERIFIABLE

    root = _discover_root()
    if root is None:
        return unverifiable(
            f"no mareforma project here or above to resolve claim {target!r}; "
            "cannot verify"
        )

    try:
        with mareforma.open(root) as graph:
            claim = graph.get_claim(target)
            if claim is None:
                return unverifiable(
                    f"claim {target!r} not found in this project; cannot verify"
                )

            problems: list[str] = []
            # Signature re-verification. Two complementary checks:
            #  (a) the tier-gated read flag (ESTABLISHED validation envelope /
            #      REPLICATED participant bundle), and
            #  (b) an audit-grade, tier-INDEPENDENT re-check (signed-field
            #      binding + asserter + role signatures) that catches a tampered
            #      PRELIMINARY signed claim the flag would pass through.
            if claim.get("signature_bundle") and not claim.get("verified"):
                problems.append("signature failed re-verification on read")
            sig_ok, sig_reason = verify_claim_signatures(graph._conn, claim)
            if not sig_ok:
                problems.append(sig_reason)

            # Grounding→citation binding re-check. Bind on ``grounded_sources``
            # (the cited sources a read was actually observed for), not the
            # declared ``cited_sources``, matching the write side and the
            # verify-on-read path (mareforma.db.restore). A producer who declares
            # a dataset in cites but reads only a decoy grounds on the decoy, so
            # the declared set would falsely MATCH. The check runs even when the
            # set is EMPTY, because "finding cites data + verdict grounded on none
            # of it" is itself a binding violation. A pre-binding verdict has no
            # such field and is annotated by the trust map, not failed here.
            grounding = parse_grounding_record(claim.get("observed_grounding"))
            if (
                isinstance(grounding, dict)
                and grounding.get("grounding") == "GROUNDED"
                and grounding.get("grounded_sources") is not None
            ):
                verdict_grounded = tuple(grounding.get("grounded_sources") or ())
                finding_sources = tuple(_claim_bound_sources(claim))
                result = check_grounding_binding(verdict_grounded, finding_sources)
                if result.disjoint:
                    problems.append(f"grounding binding violation: {result.reason}")

            # build_trust_map re-fetches the row and runs its own audit-grade
            # signature re-verification, so the standalone map is honest.
            tmap = build_trust_map(graph._conn, target)
            tmap_dict = tmap.to_dict() if tmap else None

            if problems:
                if as_json:
                    emit_json({"target": target, "target_kind": "claim",
                               "verdict": "tampered", "exit_code": _VERIFY_FAIL,
                               "reason": "; ".join(problems),
                               "trust_map": tmap_dict})
                else:
                    _err("; ".join(problems))
                    _info("")
                    _echo_trust_map(tmap, redact_home=redact_home)
                return _VERIFY_FAIL

            if as_json:
                emit_json({"target": target, "target_kind": "claim",
                           "verdict": "verified", "exit_code": _VERIFY_OK,
                           "trust_map": tmap_dict})
            else:
                _ok(f"Claim {target} verified.")
                _info("")
                _echo_trust_map(tmap, redact_home=redact_home)
            return _VERIFY_OK
    except DatabaseError as exc:
        return unverifiable(f"could not read the project graph: {exc}")


def _claim_bound_sources(claim: dict) -> list[str]:
    """The finding's bound data-source identifiers for the binding re-check.

    Read from the SIGNED predicate payload, the normalized ``data_sources`` the
    finding declares its grounding is over, plus any content-addressed
    ``data_ids``, exactly the set the write side bound against and the
    verify-on-read path re-checks (see
    :func:`mareforma.db.restore._verify_grounding_binding_on_read`). NOT the
    claim's ``supports`` (claim-id / DOI upstreams that would never intersect a
    data-path set), and NOT ``source_name`` (a free-text label that never binds).
    A string-only ``data_id`` with no ``data_source`` yields an empty set, so the
    binding reads as ``not_applicable``.

    ``data_source`` is not a claim column, so the finding citation lives only in
    ``predicate_payload``; reading it from anywhere else silently no-ops the
    binding re-check.
    """
    raw = claim.get("predicate_payload")
    if not isinstance(raw, str):
        return []
    try:
        predicate = json.loads(raw)
    except (ValueError, TypeError):
        return []
    if not isinstance(predicate, dict):
        return []
    from mareforma.trust._store import is_content_addressed

    data_sources = predicate.get("data_sources") or []
    data_ids = predicate.get("data_ids") or []
    out = [s for s in data_sources if isinstance(s, str)]
    out += [d for d in data_ids if isinstance(d, str) and is_content_addressed(d)]
    return out


def _verify_export_dir(path: Path, as_json: bool) -> int:
    """Verify an export directory by finding and verifying its signed bundle."""
    bundle = path / "mareforma-bundle.json"
    if bundle.is_file():
        return _verify_bundle_file(bundle, as_json)
    reason = (
        f"no signed bundle (mareforma-bundle.json) found in {path}; nothing to "
        "verify with public material"
    )
    if as_json:
        click.echo(json.dumps(
            {"target": str(path), "target_kind": "export-dir",
             "verdict": "unverifiable", "exit_code": _VERIFY_UNVERIFIABLE,
             "reason": reason}, indent=2))
    else:
        _err(reason)
    return _VERIFY_UNVERIFIABLE


@cli.command(cls=_VerifyCommand)
@click.argument("target")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit a machine-readable verdict + trust map to stdout.")
@click.option("--redact-home", "redact_home", is_flag=True, default=False,
              help="Rewrite $HOME to ~ in the printed trust map (never applied "
                   "to signed receipts).")
@click.option("--key", "key_path", default=None, metavar="FILE",
              help="Signer key to verify a bundle or audit receipt against "
                   "(defaults to the local bootstrap key). Pin this when the "
                   "receipt was signed with a non-default auditor key.")
def verify(target: str, as_json: bool, redact_home: bool,
           key_path: str | None) -> None:
    """Verify a claim, a signed bundle, an audit receipt, or an export directory.

    TARGET is detected by shape: an existing file is verified as a signed
    bundle or, by its payload type, as a per-finding audit receipt; an
    existing directory as an export dir; anything else as a claim id resolved
    against the local project. Verifying a claim uses only public material
    (auditor mode) and prints its trust map.

    A receipt or bundle signed with a non-default auditor key reads as
    unverifiable against the local key; pin the signer with ``--key`` to reach
    a definite verdict.

    \b
    Exit codes (stable, for CI gates):
        0  verified
        1  tamper or binding violation
        2  unverifiable (missing material to reach a verdict)
        3  usage error (bad flag / argument)

    \b
    Examples:
        mareforma verify <claim-id>
        mareforma verify <claim-id> --json
        mareforma verify mareforma-bundle.json
        mareforma verify audit/envelopes/001-finding.json --key auditor.key
        mareforma verify ./export-dir
    """
    p = Path(target)
    try:
        if p.exists():
            if p.is_dir():
                code = _verify_export_dir(p, as_json)
            else:
                code = _verify_signed_file(p, as_json, key_path)
        else:
            code = _verify_claim(target, as_json, redact_home)
    except Exception as exc:  # noqa: BLE001
        # An unexpected failure means we could not reach a verdict, which is
        # UNVERIFIABLE (exit 2), NOT a tamper (exit 1). Letting it escape would
        # surface as Python's exit 1, the exact 1-vs-2 confusion the stable
        # exit-code contract exists to prevent for CI gates.
        reason = f"verification could not complete: {exc or type(exc).__name__}"
        if as_json:
            click.echo(json.dumps(
                {"target": target, "verdict": "unverifiable",
                 "exit_code": _VERIFY_UNVERIFIABLE, "reason": reason},
                indent=2))
        else:
            _err(reason)
        code = _VERIFY_UNVERIFIABLE
    sys.exit(code)


@cli.command("map")
@click.argument("claim_id")
@click.option("--html", "as_html", is_flag=True, default=False,
              help="Render the trust map as one self-contained HTML file.")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit the trust map as JSON to stdout.")
@click.option("--output", default=None,
              help="Write the artifact to this path instead of stdout "
                   "(HTML defaults to stdout when omitted).")
@click.option("--redact-home", "redact_home", is_flag=True, default=False,
              help="Rewrite $HOME to ~ in the emitted artifact.")
def map_cmd(claim_id: str, as_html: bool, as_json: bool,
            output: str | None, redact_home: bool) -> None:
    """Show the per-finding trust map for a claim.

    Places every trust property (attributability, provenance, grounding,
    faithfulness, methodological validity, leakage, independence, contestation,
    standing, trust-root, witnessing) at its tier with the residual named. A
    read-side artifact: it adds no signed field and infers nothing it cannot
    compute.

    \b
    Examples:
        mareforma map <claim-id>
        mareforma map <claim-id> --json
        mareforma map <claim-id> --html --output trust-map.html
    """
    if as_html and as_json:
        _err("--html and --json are mutually exclusive.")
        sys.exit(1)

    import mareforma
    import sqlite3
    from mareforma.db import DatabaseError

    root = _read_only_root()
    try:
        with mareforma.open(root) as graph:
            tmap = graph.trust_map(claim_id)
    except (DatabaseError, sqlite3.DatabaseError) as exc:
        _err(f"Could not read graph.db: {exc}")
        sys.exit(1)

    if tmap is None:
        _err(f"Claim '{claim_id}' not found.")
        sys.exit(1)

    if as_html:
        from mareforma.trust_map_html import render_html

        html = render_html(tmap)
        if redact_home:
            html = _redact_home(html)
        if output:
            Path(output).write_text(html, encoding="utf-8")
            _ok(f"Wrote trust map → {output}")
        else:
            click.echo(html, nl=False)
        return

    if as_json:
        text = json.dumps(tmap.to_dict(), indent=2)
        if redact_home:
            text = _redact_home(text)
        if output:
            Path(output).write_text(text + "\n", encoding="utf-8")
            _ok(f"Wrote trust map → {output}")
        else:
            click.echo(text)
        return

    if output:
        text = _trust_map_plaintext(tmap)
        if redact_home:
            text = _redact_home(text)
        Path(output).write_text(text, encoding="utf-8")
        _ok(f"Wrote trust map → {output}")
        return

    _echo_trust_map(tmap, redact_home=redact_home)


@cli.command("diagnose", context_settings={"ignore_unknown_options": True})
@click.option("--cites", "cites", multiple=True, metavar="SRC",
              help="A source the run should ground on (path/URL/sha256:). "
                   "Repeatable. Without it, diagnose reports observation only "
                   "and computes no grounding verdict; it never guesses a "
                   "citation.")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit the observation report as JSON.")
@click.option("--redact-home", "redact_home", is_flag=True, default=False,
              help="Rewrite $HOME to ~ in the emitted report.")
@click.argument("command", nargs=-1, type=click.UNPROCESSED, required=True)
def diagnose_cmd(cites: tuple[str, ...], as_json: bool, redact_home: bool,
                 command: tuple[str, ...]) -> None:
    """Run a Python target under the observer and report what data flowed.

    Runs COMMAND in-process (via runpy, the coverage.py pattern; a subprocess
    would hide the target behind the observer's own seam) with the grounding
    observer active, then prints the observed reads, seams, and coverage. With
    ``--cites`` it also computes and prints the grounding verdict for those
    sources; without it, the report is observation-only (no verdict is invented
    for a citation you did not state).

    A target that crashes still prints its partial observation and exits with
    the target's own exit code.

    \b
    Examples:
        mareforma diagnose -- python analysis.py
        mareforma diagnose --cites /data/trial.csv -- analysis.py
        mareforma diagnose -- -m mypkg.pipeline
    """
    from mareforma.diagnose import run_diagnose

    sys.exit(run_diagnose(
        list(command), cites=list(cites), as_json=as_json,
        redact_home=(_redact_home if redact_home else None),
    ))


@cli.command("audit", context_settings={"ignore_unknown_options": True})
@click.option("--findings", "findings_path", default=None, metavar="FILE",
              help="JSON object mapping finding_id to its cited source(s). "
                   "Required unless --corpus is given.")
@click.option("--corpus", "corpus_dir", default=None, metavar="DIR",
              help="Directory of run specs (*.json, each carrying 'command' "
                   "and 'findings'). Resumable; one fresh interpreter per run.")
@click.option("--out", "out_dir", default="mareforma-audit", show_default=True,
              metavar="DIR",
              help="Output directory for the run record, receipts, and signed "
                   "envelopes.")
@click.option("--key", "key_path", default=None, metavar="FILE",
              help="Auditor signing key (defaults to the bootstrap key).")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit the run record as JSON.")
@click.option("--redact-home", "redact_home", is_flag=True, default=False,
              help="Rewrite $HOME to ~ in the printed report (never applied "
                   "to signed receipts).")
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
def audit_cmd(findings_path: str | None, corpus_dir: str | None, out_dir: str,
              key_path: str | None, as_json: bool, redact_home: bool,
              command: tuple[str, ...]) -> None:
    """Audit a third-party pipeline: one signed grounding receipt per finding.

    Runs COMMAND in-process under the grounding observer, exactly like
    diagnose; the target never imports mareforma. The findings mapping names
    what each finding claims to cite, the observer alone supplies what
    happened, and nothing the target prints or writes enters a verdict. Each
    finding gets a verdict receipt (``receipts.jsonl`` feeds ``mareforma
    measure``) plus a signed envelope ``mareforma verify`` checks from public
    material alone. Exits with the target's own exit code; a crashing target
    still emits its partial receipts.

    The target shares the auditor's interpreter (in-process observation is
    what makes its reads visible), so the receipts grade a pipeline that does
    not attack its auditor: a target written to defeat the audit could
    fabricate what the observer records. The signature attests the auditor's
    observation, not the target's honesty.

    With --corpus, iterates run specs instead: one fresh interpreter per run,
    resumable (a run whose signed record verifies as complete is skipped on
    re-invocation).

    \b
    Examples:
        mareforma audit --findings findings.json -- python analysis.py
        mareforma audit --findings findings.json --out audit/ -- -m mypkg.run
        mareforma audit --corpus runs/ --out audit/
    """
    from mareforma.audit import run_audit, run_corpus

    if corpus_dir and (findings_path or command):
        raise click.UsageError(
            "--corpus takes run specs; drop --findings and the command")
    if corpus_dir and (as_json or redact_home):
        raise click.UsageError(
            "--json and --redact-home apply to a single run, not --corpus")
    if corpus_dir:
        sys.exit(run_corpus(corpus_dir, out_dir=out_dir, key_path=key_path))
    if not findings_path:
        raise click.UsageError("audit needs --findings FILE (or --corpus DIR)")
    if not command:
        raise click.UsageError(
            "audit needs a target after `--`, e.g. `-- python analysis.py`")
    sys.exit(run_audit(
        list(command), findings_path=findings_path, out_dir=out_dir,
        key_path=key_path, as_json=as_json,
        redact_home=(_redact_home if redact_home else None),
    ))


# ---------------------------------------------------------------------------
# observe, coverage self-report (the doctor)
# ---------------------------------------------------------------------------

@cli.command("observe")
@click.option("--doctor", is_flag=True, help="Report observer coverage for this environment.")
@click.option("--json", "as_json", is_flag=True, help="Emit the report as JSON.")
def observe_cmd(doctor: bool, as_json: bool) -> None:
    """Inspect the execution-observed grounding machinery.

    \b
    Examples:
        mareforma observe --doctor
        mareforma observe --doctor --json
    """
    if not doctor:
        _err("mareforma observe currently supports --doctor. Run with --doctor.")
        sys.exit(2)
    from mareforma.observe import coverage_report

    report = coverage_report()
    if as_json:
        click.echo(json.dumps(report, indent=2, sort_keys=True))
        return
    _ok("Observer coverage for this environment")
    click.echo(click.style("  stdlib loaders:", bold=True))
    for row in report["stdlib_wrapped"]:
        mark = "✓" if row["wrapped"] else "·"
        _info(f"{mark} {row['loader']}")
    click.echo(click.style("  third-party loaders (wrapped if you use them):", bold=True))
    for row in report["third_party"]:
        if row["wrapped"]:
            mark, note = "✓", "wrapped"
        elif row["importable"]:
            mark, note = "·", "importable, not yet active"
        else:
            mark, note = " ", "not installed"
        _info(f"{mark} {row['loader']}, {note}")
    click.echo(click.style("  seams that force OPAQUE:", bold=True))
    for row in report["seam_kinds"]:
        _info(f"{row['kind']}: {row['effect']}")
    click.echo(click.style("  known bounds:", bold=True))
    for bound in report["known_bounds"]:
        _info(f"- {bound}")


# ---------------------------------------------------------------------------
# measure, aggregate grounding verdicts into the paper's number
# ---------------------------------------------------------------------------

@cli.command("measure")
@click.argument("receipts_path")
@click.option("--json", "as_json", is_flag=True, help="Emit the report as JSON.")
@click.option(
    "--redact-home", is_flag=True,
    help="Rewrite $HOME to ~ in emitted paths (never applied to signed receipts).",
)
def measure_cmd(receipts_path: str, as_json: bool, redact_home: bool) -> None:
    """Aggregate a pipeline's grounding verdicts into the reported split.

    RECEIPTS_PATH is a JSON array of verdict receipts, or a JSONL file with one
    receipt per line (as written by a measurement run). The report gives the
    GROUNDED / UNGROUNDED / OPAQUE split, the incidental-read rate, mean read
    coverage, and OPAQUE bucketed by seam kind. When a receipt carries an
    ``independence`` record, the independence arm is reported alongside it: the
    effective-independence distribution, the UNVERIFIABLE fraction, and the
    same-model-collapse rate.

    \b
    Examples:
        mareforma measure run-receipts.jsonl
        mareforma measure run-receipts.json --json --redact-home
    """
    from mareforma.observe import (
        independence_records,
        summarize_independence,
        summarize_receipts,
    )
    from mareforma.observe.measure import PilotReport

    try:
        receipts = _load_receipts(Path(receipts_path))
    except (OSError, ValueError) as exc:
        _err(f"Could not read receipts: {exc}")
        sys.exit(1)

    grounding = summarize_receipts(receipts)
    report = grounding.to_dict()
    closing = grounding.closing_sentence()
    indep = summarize_independence(independence_records(receipts))
    indep_report = indep.to_dict() if indep.total else None
    indep_closing = indep.closing_sentence() if indep.total else None
    # The always-on honesty bound: grounded prevalence is a lower bound to within
    # the OPAQUE coverage gap, whatever the OPAQUE fraction, never printed only
    # when OPAQUE dominates.
    coverage_bound = PilotReport(grounding=grounding, independence=indep).coverage_bound()
    if redact_home:
        report = _redact_home(report)
        closing = _redact_home(closing)
    if as_json:
        payload = {**report, "summary": closing, "coverage_bound": coverage_bound}
        if indep_report is not None:
            payload["independence"] = {**indep_report, "summary": indep_closing}
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    _ok(f"Grounding report over {report['total']} findings")
    counts = report["counts"]
    for state in ("GROUNDED", "UNGROUNDED", "OPAQUE"):
        _info(f"{state}: {counts[state]} ({report['fractions'][state]:.0%})")
    if report["opaque_by_seam"]:
        _info("OPAQUE by seam: " + ", ".join(
            f"{k}={v}" for k, v in sorted(report["opaque_by_seam"].items())
        ))
    _info(f"incidental-read rate: {report['incidental_read_rate']:.0%}")
    if report["mean_read_coverage"] is not None:
        _info(f"mean read coverage: {report['mean_read_coverage']:.0%}")
    click.echo(closing)
    click.echo(coverage_bound)
    if indep_report is not None:
        _ok(f"Independence report over {indep_report['total']} findings")
        dist = indep_report["distribution_counts"]
        _info(
            f"effective independence: {dist['1']} at 1, {dist['>=2']} at >=2"
            + (f", {dist['0']} at 0" if dist["0"] else "")
        )
        _info(
            f"UNVERIFIABLE (soft lineage): {indep_report['unverifiable']} "
            f"({indep_report['unverifiable_fraction']:.0%})"
        )
        _info(
            f"same-model-collapse rate: {indep_report['same_model_collapse_rate']:.0%}"
        )
        click.echo(indep_closing)


def _load_receipts(path: Path) -> list:
    """Load verdict receipts from a JSON array file or a JSONL file."""
    text = path.read_text(encoding="utf-8")
    stripped = text.lstrip()
    if stripped.startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError("expected a JSON array of receipts")
    else:
        data = []
        for line in text.splitlines():
            line = line.strip()
            if line:
                data.append(json.loads(line))
    for i, receipt in enumerate(data):
        if not isinstance(receipt, dict):
            raise ValueError(
                f"receipt {i} is a {type(receipt).__name__}, expected a JSON object"
            )
    return data


def _redact_home(obj):
    """Rewrite the absolute home directory to ~ in every string in *obj*.

    Applied to EMITTED artifacts only (never to a signed receipt, whose digest
    must match the bytes that were signed). Recurses through dicts and lists.
    """
    home = str(Path.home())
    # A one-character home (``/``, a root container with HOME=/) would turn this
    # into a global slash→tilde corruptor, mangling every path in the artifact.
    # Only redact a real, multi-character home prefix.
    redact = len(home) > 1 and home != "~"

    def walk(x):
        if isinstance(x, str):
            return x.replace(home, "~") if redact else x
        if isinstance(x, dict):
            return {k: walk(v) for k, v in x.items()}
        if isinstance(x, list):
            return [walk(v) for v in x]
        return x

    return walk(obj)


# ---------------------------------------------------------------------------
# reexec, re-run a recorded pipeline and check the number reproduces
# ---------------------------------------------------------------------------

@cli.command("reexec")
@click.argument("run_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--json", "as_json", is_flag=True, help="Emit the verdict as JSON.")
@click.option("--map", "map_claim", default=None, metavar="CLAIM_ID",
              help="Also render CLAIM_ID's trust map with the faithfulness "
                   "verdict placed on its PROXY-tier axis.")
def reexec_cmd(run_path: Path, as_json: bool, map_claim: str | None) -> None:
    """Re-run a recorded pipeline and check the reported number reproduces.

    RUN_PATH is a JSON run record: the recorded reported_value, a pipeline
    naming a 'module:attr' entry point (plus optional args), a declared
    tolerance and rel_tolerance, and, when the run cannot be re-run faithfully,
    "reexecutable": false with a not_reexecutable_reason (world_contact,
    private_data, or expensive_compute).

    The verdict is three-valued and honest: REPRODUCED (re-ran and matched),
    DIVERGED (re-ran and differed), or COULD_NOT_REEXECUTE (could not run, so
    faithfulness is unknown, never a false REPRODUCED). It attests
    reproducibility, not correctness, and a same-arm re-run is not independence.

    With --map CLAIM_ID it re-renders that claim's trust map with the verdict
    placed on the faithfulness axis, so the proxy is read next to every other
    trust property. The verdict is not stored; it is a read-side overlay.

    Exit code carries the verdict: 0 REPRODUCED, 1 DIVERGED,
    2 COULD_NOT_REEXECUTE, 3 usage error (a malformed run record, or a --map
    claim id that does not exist, both distinct from an honest inconclusive
    re-run).

    \b
    Examples:
        mareforma reexec run.json
        mareforma reexec run.json --json
        mareforma reexec run.json --map <claim-id>
    """
    from mareforma.reexec import FaithfulnessVerdict, MalformedRunError, reexec

    try:
        result = reexec(run_path)
    except MalformedRunError as exc:
        _err(f"Malformed run record: {exc}")
        sys.exit(3)

    if as_json:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        verdict = result.verdict.value
        if result.verdict is FaithfulnessVerdict.REPRODUCED:
            _ok(f"{verdict}: re-execution matched the recorded number")
        elif result.verdict is FaithfulnessVerdict.DIVERGED:
            _err(f"{verdict}: re-execution produced a different number")
        else:
            _err(f"{verdict}: the pipeline could not be re-executed")
        if result.reproduced_value is not None:
            _info(f"recorded: {result.recorded_value}  reproduced: {result.reproduced_value}")
        else:
            _info(f"recorded: {result.recorded_value}  reproduced: , ")
        _info(f"tolerance: abs={result.tolerance}  rel={result.rel_tolerance}")
        _info(f"residual: {result.residual}")

    if map_claim is not None:
        import mareforma

        root = _read_only_root()
        with mareforma.open(root) as graph:
            tmap = graph.trust_map(map_claim, reexec_record=result.to_map_record())
        if tmap is None:
            _err(f"Claim '{map_claim}' not found; cannot render its trust map.")
            sys.exit(3)
        if as_json:
            click.echo(json.dumps(tmap.to_dict(), indent=2))
        else:
            _echo_trust_map(tmap, redact_home=False)

    _exit = {
        FaithfulnessVerdict.REPRODUCED: 0,
        FaithfulnessVerdict.DIVERGED: 1,
        FaithfulnessVerdict.COULD_NOT_REEXECUTE: 2,
    }[result.verdict]
    sys.exit(_exit)


# ---------------------------------------------------------------------------
# claim
# ---------------------------------------------------------------------------

@cli.group()
def claim() -> None:
    """Manage scientific claims.

    Claims are falsifiable assertions with a classification (INFERRED |
    ANALYTICAL | DERIVED) and a graph-derived support level (PRELIMINARY →
    REPLICATED → ESTABLISHED).

    \b
    Examples:
        mareforma claim add "Target T is elevated in condition C" \\
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
@click.option("--artifact-hash", "artifact_hash", default=None,
              help="SHA256 hex digest of the artifact backing this claim.")
def claim_add(text, classification, status, source_name, supports, contradicts,
              generated_by, artifact_hash):
    """Add a new scientific claim TEXT.

    Routes through ``mareforma.open()`` so the XDG-default signing key
    is auto-loaded. A bootstrapped key produces a signed claim; an
    unsigned graph (no key) produces an unsigned claim. Mareforma
    decides; the CLI does not bypass signing.
    """
    import mareforma
    from mareforma.db import DatabaseError, MareformaError

    root = _root()
    try:
        with mareforma.open(root) as graph:
            claim_id = graph.assert_claim(
                text,
                classification=classification,
                status=status,
                source_name=source_name,
                generated_by=generated_by,
                supports=list(supports) or None,
                contradicts=list(contradicts) or None,
                artifact_hash=artifact_hash,
            )
    except ValueError as exc:
        _err(str(exc))
        sys.exit(1)
    except DatabaseError as exc:
        _err(str(exc))
        sys.exit(1)
    except MareformaError as exc:
        # Belt-and-suspenders for any future MareformaError subclass we
        # haven't enumerated here, better a generic message than a
        # traceback.
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

    root = _read_only_root()
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

    root = _read_only_root()
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
    """Update fields on an existing claim by ID.

    Routes through ``mareforma.open()`` so the loaded-graph context
    (XDG key, signer enrollment) is consistent with the Python API.
    Mareforma's append-only triggers (claims_signed_fields_no_laundering)
    block any update that would mutate signed predicate fields on a
    signed row: status-only updates remain allowed.
    """
    import mareforma
    from mareforma.db import (
        DatabaseError, ClaimNotFoundError, MareformaError,
        update_claim as _update,
    )

    root = _root()
    try:
        with mareforma.open(root) as graph:
            _update(
                graph._conn, root, claim_id,
                status=status,
                text=text,
                supports=list(supports) if supports else None,
                contradicts=list(contradicts) if contradicts else None,
            )
    except ClaimNotFoundError as exc:
        _err(str(exc))
        sys.exit(1)
    except ValueError as exc:
        _err(str(exc))
        sys.exit(1)
    except DatabaseError as exc:
        _err(f"Failed to update claim: {exc}")
        sys.exit(1)
    except MareformaError as exc:
        _err(str(exc))
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

    \b
    Examples:
        mareforma claim validate <ID>
        mareforma claim validate <ID> --validated-by reviewer@example.org
    """
    import mareforma
    from mareforma.db import (
        DatabaseError, ClaimNotFoundError, SelfValidationError,
        LLMValidatorPromotionError, MareformaError,
    )

    try:
        with mareforma.open(_root()) as graph:
            graph.validate(claim_id, validated_by=validated_by)
    except ClaimNotFoundError as exc:
        _err(str(exc))
        sys.exit(1)
    except SelfValidationError as exc:
        # Common first-run trip-up, the user opened the graph with the
        # same key that signed the claim. Surface mareforma's
        # explanation and the exact remediation command.
        _err(str(exc))
        _info("")
        _info("Resolution: enroll a second validator (a different key) and")
        _info("run `mareforma claim validate` while that key is loaded.")
        _info("See `mareforma validator add --help` and `mareforma key show --help`.")
        sys.exit(1)
    except LLMValidatorPromotionError as exc:
        _err(str(exc))
        sys.exit(1)
    except ValueError as exc:
        # Mareforma ValueErrors carry actionable text (wrong support_level,
        # signer not enrolled, no signer loaded). Pass through verbatim.
        _err(str(exc))
        sys.exit(1)
    except DatabaseError as exc:
        _err(f"Failed to validate claim: {exc}")
        sys.exit(1)
    except MareformaError as exc:
        # Belt-and-suspenders for any future MareformaError subclass we
        # forget to enumerate here. Better a generic message than a
        # traceback.
        _err(str(exc))
        sys.exit(1)

    _ok(f"Claim '{claim_id}' promoted to ESTABLISHED.")
    if validated_by:
        _info(f"validated_by: {validated_by}")


# ---------------------------------------------------------------------------
# restore
# ---------------------------------------------------------------------------

@cli.command("restore")
@click.argument(
    "claims_toml_path",
    type=click.Path(exists=False, dir_okay=False, path_type=Path),
    required=False,
)
def restore_cmd(claims_toml_path: Path | None) -> None:
    """Rebuild graph.db from claims.toml (catastrophic-loss recovery).

    Reads the TOML state file written by every claim/validator mutation
    and rebuilds the project's graph.db from scratch. The command
    refuses to run if graph.db already contains claims: restore is
    fresh-only, not merge.

    Every signature is verified before any row is inserted: enrollment
    envelopes against parent keys, claim bundles against enrolled
    signers, validation envelopes against validator keys. The first
    failure rolls back the entire transaction.

    \b
    Examples:
        mareforma restore                    # uses ./claims.toml
        mareforma restore backups/state.toml # explicit source
    """
    import mareforma
    from mareforma.db import RestoreError

    try:
        result = mareforma.restore(_root(), claims_toml=claims_toml_path)
    except RestoreError as exc:
        _err(str(exc))
        sys.exit(1)

    _ok(f"Restored graph.db from claims.toml ({_root()}/.mareforma/graph.db).")
    _info(f"validators_restored: {result['validators_restored']}")
    _info(f"claims_restored:     {result['claims_restored']}")


