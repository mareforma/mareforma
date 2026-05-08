"""
cli.py — Mareforma command-line interface.

Commands
--------
    mareforma init                             initialise project in cwd
    mareforma add-source <n>                   register a data source
    mareforma explain [source] [--json]        dump ontology context
    mareforma check                            validate project health
    mareforma status                           epistemic health dashboard
    mareforma build [source] [--dry-run]       run the pipeline DAG
    mareforma log                              show build history
    mareforma diff <transform>                 compare last two runs
    mareforma export [--output path]           write ontology.jsonld
    mareforma claim add TEXT [options]         assert a scientific claim
    mareforma claim list [--status] [--source] list claims
    mareforma claim show ID                    show claim details
    mareforma claim update ID [options]        update a claim
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

# Force UTF-8 output on Windows so Unicode symbols (✓, →, •) render correctly.
if sys.platform == "win32":
    for _stream in (sys.stdout, sys.stderr):
        if hasattr(_stream, "reconfigure"):
            _stream.reconfigure(encoding="utf-8")

from mareforma import __version__, __description__
from mareforma.initializer import initialize
from mareforma.registry import (
    MareformaError,
    ProjectNotFoundError,
    SourceAlreadyExistsError,
    SourceNotFoundError,
    TOMLParseError,
    add_source as registry_add_source,
    get_project,
    get_source,
    list_sources,
    load as load_toml,
    validate,
)
from mareforma.scaffold import scaffold_source


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _root() -> Path:
    """Return the current working directory as the project root."""
    return Path.cwd()


def _err(msg: str) -> None:
    click.echo(click.style("Error: ", fg="red", bold=True) + msg, err=True)


def _warn(msg: str) -> None:
    click.echo(click.style("Warning: ", fg="yellow", bold=True) + msg, err=True)


def _ok(msg: str) -> None:
    click.echo(click.style("✓ ", fg="green") + msg)


def _attach_claims(conn, runs: list[dict]) -> list[dict]:
    """Attach claim_ids from the evidence table to each run dict.

    Used by ``diff`` and ``cross-diff``.
    """
    result = []
    for run in runs:
        try:
            ev_rows = conn.execute(
                "SELECT claim_id FROM evidence WHERE run_id = ?",
                (run["run_id"],),
            ).fetchall()
            claim_ids = [r["claim_id"] for r in ev_rows]
        except Exception:  # noqa: BLE001
            claim_ids = []
        result.append({**run, "claim_ids": claim_ids})
    return result


def _info(msg: str) -> None:
    click.echo(msg)


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------

@click.group(help=f"{__description__}\n\nRun 'mareforma <command> --help' for details on each command.")
@click.version_option(__version__, prog_name="mareforma")
def cli() -> None:
    pass


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------

@cli.command()
@click.option(
    "--path",
    default=".",
    show_default=True,
    help="Directory to initialise. Defaults to current directory.",
)
def init(path: str) -> None:
    """Initialise a mareforma project.

    Safe to run on an existing project: adds any missing pieces without
    overwriting files you have already edited.
    """
    root = Path(path).resolve()
    try:
        messages = initialize(root)
    except MareformaError as exc:
        _err(str(exc))
        sys.exit(1)

    for msg in messages:
        _info(msg)


# ---------------------------------------------------------------------------
# add-source
# ---------------------------------------------------------------------------

@cli.command("add-source")
@click.argument("name")
@click.option("--path", default=None, help="Path to raw data. Defaults to data/<n>/raw/.")
@click.option("--description", default="", help="One-line description of this source.")
@click.option("--force", is_flag=True, default=False, help="Overwrite existing source entry.")
def add_source(name: str, path: str | None, description: str, force: bool) -> None:
    """Register a new data source called NAME.

    Creates the source directory scaffold under data/NAME/ and adds an entry
    to mareforma.project.toml. Fill in the generated entry with format and
    acquisition protocol path.

    Examples:

        mareforma add-source morphology_data

        mareforma add-source ephys --path /mnt/nas/ephys/raw --description "Patch-clamp recordings"
    """
    root = _root()
    source_path = path or f"data/{name}/raw/"

    # Check whether path exists — warn but don't block.
    resolved = Path(source_path)
    if not resolved.is_absolute():
        resolved = root / resolved
    if not resolved.exists():
        _warn(
            f"Path '{source_path}' does not exist on disk. "
            "Source registered anyway — create the path before building."
        )

    try:
        registry_add_source(root, name, source_path, description, force=force)
    except ProjectNotFoundError as exc:
        _err(str(exc))
        sys.exit(1)
    except SourceAlreadyExistsError as exc:
        _err(str(exc))
        sys.exit(1)
    except TOMLParseError as exc:
        _err(str(exc))
        sys.exit(1)

    # Scaffold the source directories.
    scaffold_msgs = scaffold_source(root, name)
    for msg in scaffold_msgs:
        _info(msg)

    _ok(f"Source '{name}' registered in mareforma.project.toml")
    _info(
        f"\nNext: edit mareforma.project.toml → [sources.{name}]\n"
        "  Fill in: description, format"
    )


# ---------------------------------------------------------------------------
# explain
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("source", required=False, default=None)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON to stdout.")
def explain(source: str | None, as_json: bool) -> None:
    """Dump ontology context for SOURCE (or the whole project).

    With no arguments, prints the project summary and lists all sources.

    With a source name, prints full metadata for that source — useful for
    piping to an agent:

        mareforma explain morphology_data --json | claude ...
    """
    root = _root()

    try:
        if source is None:
            _explain_project(root, as_json)
        else:
            _explain_source(root, source, as_json)
    except (ProjectNotFoundError, SourceNotFoundError, TOMLParseError) as exc:
        _err(str(exc))
        sys.exit(1)


def _explain_project(root: Path, as_json: bool) -> None:
    project = get_project(root)
    sources = list_sources(root)

    if as_json:
        click.echo(json.dumps({"project": project, "sources": sources}, indent=2))
        return

    click.echo(click.style("PROJECT", bold=True, fg="cyan"))
    click.echo(f"  name        : {project.get('name', '—')}")
    click.echo(f"  description : {project.get('description', '—') or '(empty)'}")
    click.echo(f"  created     : {project.get('created', '—')}")
    click.echo(f"  version     : {project.get('mareforma_version', '—')}")
    click.echo("")
    click.echo(click.style("SOURCES", bold=True, fg="cyan") + f"  ({len(sources)} registered)")
    if sources:
        for s in sources:
            click.echo(f"  • {s}")
        click.echo("\nRun 'mareforma explain <source>' for full details.")
    else:
        click.echo("  None. Add one with: mareforma add-source <n>")


def _explain_source(root: Path, name: str, as_json: bool) -> None:
    src = get_source(root, name)

    if as_json:
        click.echo(json.dumps({name: src}, indent=2))
        return

    click.echo(click.style(f"SOURCE: {name}", bold=True, fg="cyan"))
    click.echo(f"  path              : {src.get('path', '—')}")
    click.echo(f"  description       : {src.get('description', '—') or '(empty)'}")
    click.echo(f"  format            : {src.get('format', '—') or '(empty)'}")
    click.echo(f"  version           : {src.get('version', '—') or '(empty)'}")
    click.echo(f"  status            : {src.get('status', '—')}")
    click.echo(f"  added             : {src.get('added', '—')}")
    click.echo(f"  added_by          : {src.get('added_by', '—') or '(empty)'}")
    acq = src.get("acquisition", {})
    if acq:
        click.echo("")
        click.echo(click.style("  ACQUISITION", bold=True))
        click.echo(f"    protocol_file : {acq.get('protocol_file', '—')}")


# ---------------------------------------------------------------------------
# check
# ---------------------------------------------------------------------------

@cli.command()
def check() -> None:
    """Validate the project ontology and data source paths.

    Exits 0 if clean, 1 if any warnings are found. Safe to use in CI.

    Checks:
      - TOML file parses without errors
      - All registered source paths exist on disk
      - Required fields (description, format) are not empty
    """
    root = _root()

    try:
        issues = validate(root)
    except ProjectNotFoundError as exc:
        _err(str(exc))
        sys.exit(1)
    except TOMLParseError as exc:
        _err(str(exc))
        sys.exit(1)

    if not issues:
        _ok("Project ontology is valid. No issues found.")
        sys.exit(0)

    warnings = [i for i in issues if i["level"] == "warning"]
    errors = [i for i in issues if i["level"] == "error"]

    for issue in errors:
        _err(f"[{issue['source']}] {issue['message']}")
    for issue in warnings:
        _warn(f"[{issue['source']}] {issue['message']}")

    click.echo("")
    summary = []
    if errors:
        summary.append(click.style(f"{len(errors)} error(s)", fg="red", bold=True))
    if warnings:
        summary.append(click.style(f"{len(warnings)} warning(s)", fg="yellow", bold=True))
    click.echo("check: " + ", ".join(summary))

    sys.exit(1)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@cli.command("status")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit JSON to stdout.")
def status_cmd(as_json: bool) -> None:
    """Show epistemic health dashboard.

    Reports on claim coverage, unclaimed transforms, source linkage,
    and literature registration. Displays a traffic light indicator:
    GREEN (all covered), YELLOW (gaps found), RED (no claims at all).

    Examples:

        mareforma status

        mareforma status --json
    """
    from mareforma.db import open_db, DatabaseError
    from mareforma.health import compute_health

    root = _root()

    try:
        load_toml(root)
    except (ProjectNotFoundError, TOMLParseError) as exc:
        _err(str(exc))
        sys.exit(1)

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
        import dataclasses
        click.echo(json.dumps(dataclasses.asdict(report), indent=2))
        return

    project = get_project(root)
    click.echo(click.style(f"Project: {project.get('name', '?')}", bold=True))
    click.echo("  " + "-" * 50)

    click.echo(
        f"  Claims:           {report.claims_open} open  /  "
        f"{report.claims_resolved} resolved  /  "
        f"{report.claims_contradicted} contradicted"
    )

    if report.unclaimed_transforms:
        click.echo(
            click.style("  Unclaimed transforms:", fg="yellow") +
            "  " + "  ".join(report.unclaimed_transforms)
        )
    else:
        click.echo("  Unclaimed transforms: none")

    if report.unsupported_sources:
        click.echo(
            click.style("  Sources with no claims:", fg="yellow") +
            "  " + "  ".join(report.unsupported_sources)
        )
    else:
        click.echo("  Sources with no claims: none")

    if report.support_level_breakdown:
        click.echo("  Support level breakdown:")
        for level in ("ESTABLISHED", "REPLICATED", "PRELIMINARY"):
            count = report.support_level_breakdown.get(level, 0)
            if count:
                bar = "\u2588" * min(count, 20)
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
# build
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("source", required=False, default=None)
@click.option("--dry-run", is_flag=True, default=False,
              help="Show what would run without executing transforms.")
@click.option("--force", is_flag=True, default=False,
              help="Run all nodes even if inputs are unchanged.")
@click.option("--no-git", is_flag=True, default=False,
              help="Skip git tagging after a successful build.")
def build(source, dry_run, force, no_git):
    """Run the pipeline DAG.

    Discovers transforms from data/*/preprocessing/build_transform.py,
    resolves dependency order, and runs only stale nodes.

    Optionally limit to a single SOURCE (e.g. 'morphology').

    Examples:

        mareforma build

        mareforma build morphology

        mareforma build --dry-run

        mareforma build --force
    """
    from mareforma.pipeline.discovery import discover, DiscoveryError
    from mareforma.pipeline.dag import resolve, CyclicDependencyError, MissingDependencyError
    from mareforma.pipeline.runner import TransformRunner
    from mareforma.transforms import registry as transform_registry
    from mareforma import git as git_mod
    from mareforma.db import open_db, set_build_meta, DatabaseError
    from datetime import datetime, timezone

    root = _root()

    try:
        registry_data = load_toml(root)
    except (ProjectNotFoundError, TOMLParseError) as exc:
        _err(str(exc))
        sys.exit(1)

    transform_registry.clear()

    try:
        records = discover(root, registry_data, source_filter=source)
    except DiscoveryError as exc:
        _err(str(exc))
        sys.exit(1)

    if not records:
        if source:
            _warn(f"No transforms found for source '{source}'.")
            _info(f"Check that data/{source}/preprocessing/build_transform.py "
                  "exists and contains @transform-decorated functions.")
        else:
            _warn("No transforms found in any build_transform.py file.")
            _info("Add @transform-decorated functions to "
                  "data/<source>/preprocessing/build_transform.py")
        sys.exit(0)

    try:
        ordered = resolve(records)
    except MissingDependencyError as exc:
        _err(str(exc))
        sys.exit(1)
    except CyclicDependencyError as exc:
        _err(str(exc))
        sys.exit(1)

    if dry_run:
        _info("Dry run — showing execution plan:\n")

    runner = TransformRunner(
        root=root,
        registry_data=registry_data,
        force=force,
        dry_run=dry_run,
    )
    result = runner.run(ordered)

    if dry_run:
        sys.exit(0)

    # Write build-level metadata (git_sha, timestamp) to graph.db.
    git_sha = git_mod.get_current_sha(root) if not no_git else None
    try:
        conn = open_db(root)
        try:
            set_build_meta(
                conn,
                timestamp=datetime.now(timezone.utc).isoformat(),
                git_sha=git_sha,
            )
        finally:
            conn.close()
    except DatabaseError as exc:
        _warn(f"Could not write build metadata to graph.db: {exc}")

    if result.success:
        try:
            from mareforma.exporters.jsonld import JSONLDExporter
            out = JSONLDExporter(root).write()
            _info(f"  updated  {out.relative_to(root)}")
        except Exception as exc:
            _warn(f"ontology.jsonld export failed: {exc}")

        if not no_git and git_mod.is_git_repo(root):
            tag = git_mod.tag_build(root)
            if tag:
                _info(f"  tagged   {tag}")
            git_mod.snapshot_lock(root)

    sys.exit(0 if result.success else 1)


# ---------------------------------------------------------------------------
# log
# ---------------------------------------------------------------------------

@cli.command("log")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit JSON to stdout.")
def build_log(as_json):
    """Show pipeline build history from graph.db."""
    from mareforma.db import open_db, all_transform_runs, get_build_meta, DatabaseError

    root = _root()

    try:
        load_toml(root)
    except (ProjectNotFoundError, TOMLParseError) as exc:
        _err(str(exc))
        sys.exit(1)

    try:
        conn = open_db(root)
        try:
            nodes = all_transform_runs(conn)
            meta = get_build_meta(conn)
        finally:
            conn.close()
    except DatabaseError as exc:
        _err(f"Could not read build history: {exc}")
        sys.exit(1)

    build_timestamp = meta.get("last_build_timestamp")
    git_sha = meta.get("last_git_sha")

    if as_json:
        click.echo(json.dumps({
            "build_timestamp": build_timestamp,
            "git_sha": git_sha,
            "nodes": nodes,
        }, indent=2))
        return

    if not nodes:
        _info("No builds recorded yet. Run 'mareforma build' first.")
        return

    click.echo(click.style("LAST BUILD", bold=True, fg="cyan"))
    click.echo(f"  timestamp : {build_timestamp or '—'}")
    click.echo(f"  git sha   : {git_sha or '—'}")
    click.echo("")
    click.echo(click.style("TRANSFORMS", bold=True, fg="cyan"))

    for name, node in sorted(nodes.items()):
        status = node.get("status", "?")
        duration = node.get("duration_ms", 0)
        ts = (node.get("timestamp") or "")[:19].replace("T", " ")
        icon = "✓" if status == "success" else "✗" if status == "failed" else "○"
        click.echo(f"  {icon} {name}  {status}  {duration}ms  {ts}")

    click.echo("")


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------

@cli.command("diff")
@click.argument("transform_name")
@click.option("--run1", default=None, help="First run ID (partial prefix accepted).")
@click.option("--run2", default=None, help="Second run ID (partial prefix accepted).")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit JSON to stdout.")
def diff(transform_name: str, run1: str | None, run2: str | None, as_json: bool) -> None:
    """Compare the two most recent runs of TRANSFORM_NAME.

    Shows run IDs, timestamps, status, duration, output hash delta,
    and claims attached to each run.

    Examples:

        mareforma diff morphology.register

        mareforma diff morphology.register --json
    """
    from mareforma.db import open_db, get_runs_for_transform, DatabaseError

    root = _root()

    try:
        load_toml(root)
    except (ProjectNotFoundError, TOMLParseError) as exc:
        _err(str(exc))
        sys.exit(1)

    try:
        conn = open_db(root)
        try:
            if run1 or run2:
                # Fetch all runs and filter by partial prefix
                all_runs = get_runs_for_transform(conn, transform_name)
                selected = []
                for run in all_runs:
                    rid = run["run_id"]
                    if run1 and rid.startswith(run1):
                        selected.insert(0, run)
                    elif run2 and rid.startswith(run2):
                        selected.append(run)
                runs = selected[:2]
            else:
                runs = get_runs_for_transform(conn, transform_name, limit=2)

            # Attach claim IDs to each run via the evidence table
            runs_with_claims = _attach_claims(conn, runs)
        finally:
            conn.close()
    except DatabaseError as exc:
        _err(f"Could not read graph.db: {exc}")
        sys.exit(1)

    if not runs_with_claims:
        # Check if the transform exists at all
        _err(f"No runs recorded for '{transform_name}'.")
        try:
            from mareforma.db import open_db as _odb, all_transform_runs as _atr
            conn2 = _odb(root)
            try:
                available = list(_atr(conn2).keys())
            finally:
                conn2.close()
            if available:
                _info(f"Available transforms: {', '.join(sorted(available))}")
        except Exception:
            pass
        sys.exit(1)

    if len(runs_with_claims) == 1:
        _info(f"Only one run recorded for '{transform_name}' — nothing to diff.")
        if as_json:
            click.echo(json.dumps({"runs": runs_with_claims}, indent=2))
        sys.exit(0)

    if as_json:
        click.echo(json.dumps({"runs": runs_with_claims}, indent=2))
        return

    newer, older = runs_with_claims[0], runs_with_claims[1]

    click.echo(click.style(f"DIFF: {transform_name}", bold=True, fg="cyan"))
    click.echo("")

    for label, run in [("NEWER", newer), ("OLDER", older)]:
        ts = (run.get("timestamp") or "")[:19].replace("T", " ")
        status = run.get("status", "?")
        duration = run.get("duration_ms") or 0
        run_short = (run.get("run_id") or "")[:8]
        click.echo(click.style(f"  {label}  {run_short}...", bold=True))
        click.echo(f"    timestamp : {ts}")
        click.echo(f"    status    : {status}")
        click.echo(f"    duration  : {duration}ms")
        click.echo(f"    out_hash  : {(run.get('output_hash') or '—')[:16]}...")
        if run["claim_ids"]:
            click.echo(f"    claims    : {', '.join(c[:8] + '...' for c in run['claim_ids'])}")
        else:
            click.echo("    claims    : none")
        click.echo("")

    # Hash delta
    h1 = newer.get("output_hash") or ""
    h2 = older.get("output_hash") or ""
    if h1 and h2 and h1 == h2:
        click.echo("  (no change in outputs)")
    elif h1 != h2:
        click.echo(
            click.style("  Output hash changed", fg="yellow") +
            f" — artifacts differ between these runs."
        )


# ---------------------------------------------------------------------------
# cross-diff
# ---------------------------------------------------------------------------

@cli.command("cross-diff")
@click.argument("transform_a")
@click.argument("transform_b")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit JSON to stdout.")
def cross_diff(transform_a: str, transform_b: str, as_json: bool) -> None:
    """Compare the latest run of TRANSFORM_A against TRANSFORM_B.

    Compares output hashes, artifacts (matched by suffix after stripping
    the transform name prefix), and attached claims. Use this to stress-test
    a finding across forks — different diseases, datasets, or model configs —
    and see exactly where results converge or diverge.

    Examples:

        mareforma cross-diff ra_cd4.analysis sle_cd4.analysis

        mareforma cross-diff ra_cd4.literature_reasoning sle_cd4.literature_reasoning --json
    """
    from mareforma.db import (
        open_db, get_runs_for_transform, get_artifacts_for_run, DatabaseError,
    )

    root = _root()

    try:
        load_toml(root)
    except (ProjectNotFoundError, TOMLParseError) as exc:
        _err(str(exc))
        sys.exit(1)

    try:
        conn = open_db(root)
        try:
            runs_a = get_runs_for_transform(conn, transform_a, limit=1)
            runs_b = get_runs_for_transform(conn, transform_b, limit=1)

            if not runs_a:
                _err(f"No runs recorded for '{transform_a}'.")
                sys.exit(1)
            if not runs_b:
                _err(f"No runs recorded for '{transform_b}'.")
                sys.exit(1)

            run_a = _attach_claims(conn, runs_a)[0]
            run_b = _attach_claims(conn, runs_b)[0]

            artifacts_a = get_artifacts_for_run(conn, run_a["run_id"])
            artifacts_b = get_artifacts_for_run(conn, run_b["run_id"])
        finally:
            conn.close()
    except DatabaseError as exc:
        _err(f"Could not read graph.db: {exc}")
        sys.exit(1)

    # Strip transform name prefix → compare artifacts by suffix only.
    # ctx.save("foo") inside transform "a.b" stores artifact as "a.b.foo".
    # Stripping "a.b." gives suffix "foo", enabling cross-transform comparison.
    def _suffix(artifact_name: str, transform_name: str) -> str:
        prefix = f"{transform_name}."
        return artifact_name[len(prefix):] if artifact_name.startswith(prefix) else artifact_name

    by_suffix_a = {_suffix(a["artifact_name"], transform_a): a for a in artifacts_a}
    by_suffix_b = {_suffix(a["artifact_name"], transform_b): a for a in artifacts_b}
    all_suffixes = sorted(set(by_suffix_a) | set(by_suffix_b))

    artifact_delta = []
    for suffix in all_suffixes:
        in_a = suffix in by_suffix_a
        in_b = suffix in by_suffix_b
        if in_a and in_b:
            sha_a = by_suffix_a[suffix].get("sha256") or ""
            sha_b = by_suffix_b[suffix].get("sha256") or ""
            status = "same" if (sha_a and sha_b and sha_a == sha_b) else "changed"
        elif in_a:
            status = "only_in_a"
        else:
            status = "only_in_b"
        artifact_delta.append({
            "suffix": suffix,
            "status": status,
            "a": by_suffix_a.get(suffix),
            "b": by_suffix_b.get(suffix),
        })

    if as_json:
        click.echo(json.dumps({
            "transform_a": transform_a,
            "transform_b": transform_b,
            "run_a": run_a,
            "run_b": run_b,
            "artifact_delta": artifact_delta,
        }, indent=2, default=str))
        return

    # Human-readable output
    click.echo(
        click.style("CROSS-DIFF", bold=True, fg="cyan")
        + f"  {transform_a}  ↔  {transform_b}"
    )
    click.echo("")

    for label, name, run in [("A", transform_a, run_a), ("B", transform_b, run_b)]:
        ts = (run.get("timestamp") or "")[:19].replace("T", " ")
        run_short = (run.get("run_id") or "")[:8]
        click.echo(click.style(f"  {label}  {name}  {run_short}...", bold=True))
        click.echo(f"    timestamp : {ts}")
        click.echo(f"    status    : {run.get('status', '?')}")
        click.echo(f"    duration  : {run.get('duration_ms') or 0}ms")
        click.echo(f"    out_hash  : {(run.get('output_hash') or '—')[:16]}...")
        if run["claim_ids"]:
            click.echo(f"    claims    : {', '.join(c[:8] + '...' for c in run['claim_ids'])}")
        else:
            click.echo("    claims    : none")
        click.echo("")

    # Output hash delta
    h_a = run_a.get("output_hash") or ""
    h_b = run_b.get("output_hash") or ""
    if h_a and h_b and h_a == h_b:
        click.echo(click.style("  Output hash: identical", fg="green"))
    else:
        click.echo(click.style("  Output hash: differs", fg="yellow"))
    click.echo("")

    # Artifact delta
    click.echo(click.style("  ARTIFACTS", bold=True))
    if not artifact_delta:
        click.echo("    (none recorded)")
    for entry in artifact_delta:
        suffix = entry["suffix"]
        status = entry["status"]
        if status == "same":
            size = (entry["a"] or {}).get("size_bytes") or 0
            click.echo(click.style("    =  ", fg="green") + f"{suffix}  ({size} bytes)")
        elif status == "changed":
            size_a = (entry["a"] or {}).get("size_bytes") or 0
            size_b = (entry["b"] or {}).get("size_bytes") or 0
            click.echo(click.style("    ≠  ", fg="yellow") + f"{suffix}  ({size_a}B → {size_b}B)")
        elif status == "only_in_a":
            click.echo(click.style("    A  ", fg="cyan") + f"{suffix}  (only in {transform_a})")
        else:
            click.echo(click.style("    B  ", fg="cyan") + f"{suffix}  (only in {transform_b})")


# ---------------------------------------------------------------------------
# trace
# ---------------------------------------------------------------------------

@cli.command("trace")
@click.argument("transform_name")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit JSON to stdout.")
def trace(transform_name: str, as_json: bool) -> None:
    """Show epistemic distance and support level for TRANSFORM_NAME and its ancestors.

    Displays the full chain from raw data to the named result, with each
    transform's class (RAW/PROCESSED/ANALYSED/INFERRED/unknown) and support
    level (SINGLE/REPLICATED/CONVERGED/CONSISTENT/ESTABLISHED).

    Example output:

    \b
        morphology_data
        →
        ├── morphology.load     RAW        SINGLE
        ├── morphology.filter   PROCESSED  SINGLE
        └── morphology.depth    INFERRED   CONVERGED ●

    Transforms with unknown class show ⚠ unknown — inspect the build log
    for why classification was skipped.
    """
    from mareforma.db import open_db
    from mareforma.distance import CLASS_WEIGHTS, _get_latest_class, _get_parents
    from mareforma.support import compute as compute_support

    root = _root()

    conn = open_db(root)
    try:
        # Collect ancestors via BFS
        visited: list[str] = []
        seen: set[str] = set()
        queue = [transform_name]
        while queue:
            current = queue.pop(0)
            if current in seen:
                continue
            seen.add(current)
            visited.append(current)
            parents = _get_parents(conn, current)
            queue.extend(p for p in parents if p not in seen)

        if len(visited) == 1:
            # Check if the transform has any runs at all
            row = conn.execute(
                "SELECT run_id FROM transform_runs WHERE transform_name = ? LIMIT 1",
                (transform_name,),
            ).fetchone()
            if not row:
                _err(
                    f"No runs recorded for '{transform_name}'. "
                    "Run 'mareforma build' first."
                )
                # Show available transforms
                try:
                    available = [
                        r["transform_name"]
                        for r in conn.execute(
                            "SELECT DISTINCT transform_name FROM transform_runs "
                            "WHERE status='success'"
                        ).fetchall()
                    ]
                    if available:
                        _info(f"Available transforms: {', '.join(sorted(available))}")
                except Exception:  # noqa: BLE001
                    pass
                import sys
                sys.exit(1)

        # Build trace data
        trace_rows = []
        for name in reversed(visited):  # roots first
            cls = _get_latest_class(conn, name)
            support = compute_support(name, conn, root)
            distance = CLASS_WEIGHTS.get(cls, 0.5)
            trace_rows.append({
                "transform_name": name,
                "class": cls,
                "support": support,
                "distance_weight": distance,
            })

        if as_json:
            click.echo(json.dumps({"transform": transform_name, "chain": trace_rows}, indent=2))
            return

        # Render ASCII tree
        click.echo("")
        click.echo(click.style(f"mareforma trace  {transform_name}", bold=True))
        click.echo("")

        _CLASS_COLOR = {
            "raw":       "blue",
            "processed": "cyan",
            "analysed":  "yellow",
            "inferred":  "red",
            "unknown":   "white",
        }
        _SUPPORT_MARKER = {
            "ESTABLISHED": " ●●",
            "CONVERGED":   " ●",
            "CONSISTENT":  " ◆",
            "REPLICATED":  " ◇",
            "SINGLE":      "",
        }

        for i, row in enumerate(trace_rows):
            is_last = (i == len(trace_rows) - 1)
            prefix = "└──" if is_last else "├──"
            name = row["transform_name"]
            cls = row["class"]
            support = row["support"]
            marker = _SUPPORT_MARKER.get(support, "")

            cls_label = ("⚠ unknown" if cls == "unknown" else cls.upper()).ljust(10)
            sup_label = support.ljust(12)
            color = _CLASS_COLOR.get(cls, "white")

            line = (
                f"  {prefix} {name.ljust(40)} "
                f"{click.style(cls_label, fg=color)}  "
                f"{click.style(sup_label + marker, bold=(marker != ''))}"
            )
            click.echo(line)

        click.echo("")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--output", default=None,
              help="Output path for ontology.jsonld. Default: project root.")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Print JSON-LD to stdout instead of writing a file.")
def export(output, as_json):
    """Export the project ontology as a JSON-LD document (ontology.jsonld).

    Encodes all data sources, transforms, claims, and provenance relationships
    in machine-readable linked-data format. Attach to a preprint or
    feed to an agent:

        mareforma export

        cat ontology.jsonld | jq '.["@graph"][]'
    """
    from mareforma.exporters.jsonld import JSONLDExporter

    root = _root()

    try:
        load_toml(root)
    except (ProjectNotFoundError, TOMLParseError) as exc:
        _err(str(exc))
        sys.exit(1)

    try:
        exporter = JSONLDExporter(root)

        if as_json:
            doc = exporter.export()
            click.echo(json.dumps(doc, indent=2, ensure_ascii=False))
            return

        out_path = Path(output) if output else None
        written = exporter.write(out_path)
        _ok(f"Exported ontology -> {written.relative_to(root)}")

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

        mareforma claim add "L2/3 neurons have a mean axon extent of 0.7 mm (n=312)"
            --classification ANALYTICAL --source morphology

        mareforma claim list --status open

        mareforma claim update <ID> --status contested
    """


@claim.command("add")
@click.argument("text")
@click.option("--classification", default="INFERRED", show_default=True,
              help="Claim classification: INFERRED, ANALYTICAL, DERIVED.")
@click.option("--status", default="open", show_default=True,
              help="Editorial status: open, contested, retracted.")
@click.option("--source", "source_name", default=None,
              help="Registered source this claim is about.")
@click.option("--supports", "supports", multiple=True, metavar="DOI_OR_ID",
              help="DOI or claim_id this claim rests on (repeatable).")
@click.option("--contradicts", "contradicts", multiple=True, metavar="DOI_OR_ID",
              help="DOI or claim_id this claim contests (repeatable).")
@click.option("--generated-by", "generated_by", default="human", show_default=True,
              help="'human' or a model identifier string.")
def claim_add(text, classification, status, source_name, supports, contradicts, generated_by):
    """Add a new scientific claim TEXT.

    Examples:

        mareforma claim add "L2/3 neurons have a mean axon extent of 0.7 mm (n=312)"
            --classification ANALYTICAL --source morphology

        mareforma claim add "Spiking frequency increases with cortical depth"
            --supports 10.64898/2026.03.05.709819
    """
    from mareforma.db import open_db, add_claim, DatabaseError

    root = _root()

    try:
        load_toml(root)
    except (ProjectNotFoundError, TOMLParseError) as exc:
        _err(str(exc))
        sys.exit(1)

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
    _info(f"  ID: {claim_id}")


@claim.command("list")
@click.option("--status", default=None,
              help="Filter by status: open, contested, retracted.")
@click.option("--source", "source_name", default=None,
              help="Filter by registered source name.")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit JSON to stdout.")
def claim_list(status, source_name, as_json):
    """List scientific claims, optionally filtered."""
    from mareforma.db import open_db, list_claims, DatabaseError

    root = _root()

    try:
        load_toml(root)
    except (ProjectNotFoundError, TOMLParseError) as exc:
        _err(str(exc))
        sys.exit(1)

    try:
        conn = open_db(root)
        try:
            claims = list_claims(conn, status=status, source_name=source_name)
        finally:
            conn.close()
    except Exception as exc:
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
        click.echo(f"  [{c['status']:10}] [{c.get('support_level', 'PRELIMINARY'):12}] [{c.get('classification', 'INFERRED'):10}] {c['text'][:60]}")
        click.echo(f"             id: {c['claim_id']}")
        if c.get("source_name"):
            click.echo(f"         source: {c['source_name']}")
        click.echo("")


@claim.command("show")
@click.argument("claim_id")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit JSON to stdout.")
def claim_show(claim_id, as_json):
    """Show full details for a claim by ID."""
    from mareforma.db import open_db, get_claim, list_claims_with_evidence, DatabaseError, ClaimNotFoundError

    root = _root()

    try:
        load_toml(root)
    except (ProjectNotFoundError, TOMLParseError) as exc:
        _err(str(exc))
        sys.exit(1)

    try:
        conn = open_db(root)
        try:
            c = get_claim(conn, claim_id)
            if c is None:
                _err(f"Claim '{claim_id}' not found.")
                sys.exit(1)
            evidence = list_claims_with_evidence(conn, claim_id)
        finally:
            conn.close()
    except Exception as exc:
        _err(f"Failed to fetch claim: {exc}")
        sys.exit(1)

    if as_json:
        click.echo(json.dumps({"claim": c, "evidence": evidence}, indent=2))
        return

    click.echo(click.style("CLAIM", bold=True, fg="cyan"))
    click.echo(f"  id             : {c['claim_id']}")
    click.echo(f"  text           : {c['text']}")
    click.echo(f"  classification : {c.get('classification', 'INFERRED')}")
    click.echo(f"  support_level  : {c.get('support_level', 'PRELIMINARY')}")
    click.echo(f"  generated_by   : {c.get('generated_by', 'human')}")
    click.echo(f"  status         : {c['status']}")
    if c.get("source_name"):
        click.echo(f"  source             : {c['source_name']}")

    supports = json.loads(c.get("supports_json", "[]") or "[]")
    contradicts = json.loads(c.get("contradicts_json", "[]") or "[]")
    if supports:
        click.echo(f"  supports           : {', '.join(supports)}")
    if contradicts:
        click.echo(f"  contradicts        : {', '.join(contradicts)}")
    if c.get("comparison_summary"):
        click.echo(f"  comparison_summary : {c['comparison_summary']}")

    click.echo(f"  created_at         : {c['created_at']}")
    click.echo(f"  updated_at         : {c['updated_at']}")

    if evidence:
        click.echo("")
        click.echo(click.style("  EVIDENCE", bold=True))
        for ev in evidence:
            run_ref = ev.get("run_id") or "—"
            art_ref = ev.get("artifact_name") or "—"
            click.echo(f"    run_id        : {run_ref}")
            click.echo(f"    artifact_name : {art_ref}")


@claim.command("update")
@click.argument("claim_id")
@click.option("--status", default=None,
              help="New editorial status: open, contested, retracted.")
@click.option("--text", default=None,
              help="New claim text.")
@click.option("--supports", "supports", multiple=True, metavar="DOI_OR_ID",
              help="Replace supports list (repeatable).")
@click.option("--contradicts", "contradicts", multiple=True, metavar="DOI_OR_ID",
              help="Replace contradicts list (repeatable).")
def claim_update(claim_id, status, text, supports, contradicts):
    """Update fields on an existing claim by ID."""
    from mareforma.db import open_db, update_claim, DatabaseError, ClaimNotFoundError

    root = _root()

    try:
        load_toml(root)
    except (ProjectNotFoundError, TOMLParseError) as exc:
        _err(str(exc))
        sys.exit(1)

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


# ---------------------------------------------------------------------------
# agent-log
# ---------------------------------------------------------------------------

@cli.command("agent-log")
@click.argument("run_id", required=False, default=None)
@click.option("--limit", default=50, show_default=True,
              help="Maximum number of events to show.")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit JSON to stdout.")
def agent_log(run_id: str | None, limit: int, as_json: bool) -> None:
    """Show agent provenance events recorded by MareformaObserver.

    Lists events from the agent_events table. If RUN_ID is provided, shows
    events for that transform run only. RUN_ID may be a partial prefix.

    Examples:

        mareforma agent-log

        mareforma agent-log abc123

        mareforma agent-log --limit 20 --json
    """
    from mareforma.db import open_db, DatabaseError

    root = _root()

    try:
        conn = open_db(root)
    except DatabaseError as exc:
        _err(f"Could not open graph.db: {exc}")
        sys.exit(1)

    try:
        # Ensure agent_events table exists (created lazily — may not be present)
        from mareforma.agent._schema import AGENT_EVENTS_DDL
        conn.executescript(AGENT_EVENTS_DDL)

        if run_id is not None:
            # Support partial prefix match
            rows = conn.execute(
                """
                SELECT event_id, run_id, event_type, name, timestamp,
                       status, duration_ms, input_hash, output_hash
                FROM agent_events
                WHERE run_id LIKE ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (f"{run_id}%", limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT event_id, run_id, event_type, name, timestamp,
                       status, duration_ms, input_hash, output_hash
                FROM agent_events
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    except Exception as exc:
        _err(f"Failed to read agent events: {exc}")
        sys.exit(1)
    finally:
        conn.close()

    events = [dict(r) for r in rows]

    if as_json:
        click.echo(json.dumps(events, indent=2))
        return

    if not events:
        _info("No agent events recorded.")
        if run_id:
            _info(f"No events found for run_id prefix '{run_id}'.")
        return

    click.echo(click.style(f"AGENT LOG  ({len(events)} events)", bold=True, fg="cyan"))
    click.echo("")

    for ev in events:
        status = ev["status"]
        icon = (
            click.style("✓", fg="green") if status == "success"
            else click.style("✗", fg="red") if status == "failed"
            else click.style("○", fg="yellow")
        )
        ts = (ev["timestamp"] or "")[:19].replace("T", " ")
        duration = f"  {ev['duration_ms']}ms" if ev["duration_ms"] is not None else ""
        run_short = (ev["run_id"] or "")[:8]
        event_short = (ev["event_id"] or "")[:8]

        click.echo(
            f"  {icon} [{ev['event_type']:12}] {ev['name']:30} "
            f"{status:12}{duration}"
        )
        click.echo(f"       run:{run_short}  event:{event_short}  {ts}")
        if ev.get("input_hash"):
            click.echo(f"       in :{ev['input_hash'][:16]}...")
        if ev.get("output_hash"):
            click.echo(f"       out:{ev['output_hash'][:16]}...")
        click.echo("")
