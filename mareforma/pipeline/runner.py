"""
pipeline/runner.py — TransformRunner: orchestrate build execution.

Responsibilities
----------------
1. Accept a topologically sorted list of TransformRecords
2. For each node, decide: skip (cached) or run
3. Generate a run_id, open a BuildContext, call the transform function
4. Write provenance to graph.db (transform_runs + artifacts)
5. Render rich terminal output (tree + live status)
6. Collect and surface errors without hiding upstream context

Build output (rich)
-------------------
    ┌─ mareforma build ──────────────────────────────┐
    │                                                │
    │  morphology                                    │
    │  ├── morphology.load          ✓ cached         │
    │  ├── morphology.register   ⠋  running...       │
    │  └── morphology.features      ○ pending        │
    │                                                │
    │  ephys                                         │
    │  └── ephys.load               ✓ cached         │
    │                                                │
    │  ─────────────────────────────────────────     │
    │  3 cached  1 ran  0 failed  (2.3s)             │
    └────────────────────────────────────────────────┘

Execution flow (with graph.db provenance)
-----------------------------------------
    runner.run(ordered):
      conn = open_db(root)            ← one connection for the whole build
      migrate_from_lock_json(conn)    ← no-op on fresh installs
      try:
        for record in ordered:
          if db.is_stale(...):
            run_id = uuid4()
            db.begin_run(conn, run_id, ...)       ← status='running'
            ctx = BuildContext(..., run_id, conn)
            ┌─ transform boundary ──────────────────────────────────────┐
            │ try:                                                       │
            │   record.fn(ctx)    ← ctx.claim() and ctx.save() fire here │
            │ except Exception:                                          │
            │   status = "failed"                                        │
            └────────────────────────────────────────────────────────────┘
            db.end_run(conn, run_id, status=..., ...)  ← outside try block
      finally:
        conn.close()                  ← SIGINT / success / exception all close

Error handling
--------------
- Transform raises → status=failed, error recorded in graph.db, build continues
  for independent branches, then exits with code 1 after all possible nodes run.
- Missing dependency artifact → ArtifactNotFoundError → treated as failure.
- Downstream nodes of a failed node are skipped automatically.
- DatabaseError from db writes propagates up (not swallowed as transform failure).
"""

from __future__ import annotations

import hashlib
import time
import uuid
from pathlib import Path
from typing import Any, TYPE_CHECKING

from mareforma.pipeline.context import BuildContext, hash_directory
from mareforma.db import (
    begin_run,
    end_run,
    hash_string,
    is_stale,
    migrate_from_lock_json,
    open_db,
    record_deps,
)
from mareforma.inspector import classify_run

if TYPE_CHECKING:
    from mareforma.transforms import TransformRecord


class BuildResult:
    """Summary of a completed build."""

    def __init__(self) -> None:
        self.ran: list[str] = []
        self.cached: list[str] = []
        self.failed: list[str] = []
        self.skipped: list[str] = []
        self.errors: dict[str, str] = {}  # name → error message
        self.duration_ms: int = 0

    @property
    def success(self) -> bool:
        return len(self.failed) == 0

    def summary(self) -> str:
        parts = []
        if self.ran:
            parts.append(f"{len(self.ran)} ran")
        if self.cached:
            parts.append(f"{len(self.cached)} cached")
        if self.skipped:
            parts.append(f"{len(self.skipped)} skipped")
        if self.failed:
            parts.append(f"{len(self.failed)} failed")
        elapsed = self.duration_ms / 1000
        return "  ".join(parts) + f"  ({elapsed:.1f}s)"


class TransformRunner:
    """Executes a topologically sorted list of TransformRecords.

    Parameters
    ----------
    root:
        Project root (mareforma.project.toml location).
    registry_data:
        Parsed project TOML (for BuildContext.source_path).
    force:
        If True, run all nodes regardless of cache.
    dry_run:
        If True, show what would run without executing anything.
    """

    def __init__(
        self,
        root: Path,
        registry_data: dict[str, Any],
        *,
        force: bool = False,
        dry_run: bool = False,
    ) -> None:
        self._root = root
        self._registry_data = registry_data
        self._force = force
        self._dry_run = dry_run

    def run(self, ordered: list["TransformRecord"]) -> BuildResult:
        """Execute *ordered* (topologically sorted) transforms.

        Returns a BuildResult summary.
        """
        try:
            from rich.console import Console
            console = Console()
            _rich = True
        except ImportError:
            console = None
            _rich = False

        result = BuildResult()
        build_start = time.monotonic()

        # Track which nodes were re-run this session (for downstream staleness)
        rerun_set: set[str] = set()
        # Track failed nodes so dependents can be skipped
        failed_set: set[str] = set()

        if _rich and console:
            console.print()
            console.print("[bold cyan]mareforma build[/bold cyan]")
            console.print()

        # Open one db connection for the whole build.
        conn = open_db(self._root)
        try:
            # One-time migration from pipeline.lock.json if present.
            migrate_from_lock_json(conn, self._root)

            for record in ordered:
                # Skip if any dependency failed
                blocked_by = [d for d in record.depends_on if d in failed_set]
                if blocked_by:
                    result.skipped.append(record.name)
                    failed_set.add(record.name)  # propagate skip
                    _print_node(
                        console, record.name, "skipped",
                        note=f"blocked by: {', '.join(blocked_by)}",
                    )
                    continue

                # Compute hashes for staleness check
                input_hash = self._input_hash(record)
                source_hash = hash_string(record.source_code)

                stale = is_stale(
                    conn,
                    record.name,
                    input_hash=input_hash,
                    source_hash=source_hash,
                    force=self._force,
                )
                # Also stale if any upstream was re-run this session
                if not stale and any(d in rerun_set for d in record.depends_on):
                    stale = True

                if not stale:
                    result.cached.append(record.name)
                    _print_node(console, record.name, "cached")
                    continue

                if self._dry_run:
                    result.ran.append(record.name)
                    _print_node(console, record.name, "would_run")
                    rerun_set.add(record.name)
                    continue

                # --- Execute transform ---
                run_id = str(uuid.uuid4())
                _print_node(console, record.name, "running")

                # Write run START to db (outside transform try block so a DB error
                # is not misreported as a transform failure).
                begin_run(conn, run_id, record.name, input_hash, source_hash)
                record_deps(conn, record.name, record.depends_on)

                ctx = BuildContext(
                    root=self._root,
                    transform_name=record.name,
                    registry_data=self._registry_data,
                    run_id=run_id,
                    db=conn,
                    console=console,
                )

                node_start = time.monotonic()
                status = "success"
                error_msg: str | None = None
                output_hash = ""

                # Transform boundary: only user transform exceptions are caught here.
                try:
                    record.fn(ctx)
                    output_hash = self._output_hash(record.name, ctx)
                except Exception as exc:
                    status = "failed"
                    error_msg = f"{type(exc).__name__}: {exc}"
                    result.failed.append(record.name)
                    result.errors[record.name] = error_msg
                    failed_set.add(record.name)
                    _print_node(console, record.name, "failed", note=error_msg)

                duration_ms = round((time.monotonic() - node_start) * 1000)

                # Write run END to db (outside transform try block).
                end_run(
                    conn,
                    run_id,
                    status=status,
                    output_hash=output_hash,
                    duration_ms=duration_ms,
                    error_message=error_msg,
                )

                if status == "success":
                    result.ran.append(record.name)
                    rerun_set.add(record.name)
                    classify_run(conn, run_id, record.name, self._root)
                    _print_node(console, record.name, "done")

        finally:
            conn.close()

        result.duration_ms = round((time.monotonic() - build_start) * 1000)

        # Print summary
        if _rich and console:
            console.print()
            color = "green" if result.success else "red"
            console.print(f"[{color}]{result.summary()}[/{color}]")
            console.print()

            if result.failed and result.errors:
                console.print("[bold red]Errors:[/bold red]")
                for name, err in result.errors.items():
                    console.print(f"  [red]✗[/red] [bold]{name}[/bold]: {err}")
                console.print()
        else:
            print(f"\n{result.summary()}")
            if result.failed:
                for name, err in result.errors.items():
                    print(f"  FAILED {name}: {err}")

        return result

    def _input_hash(self, record: "TransformRecord") -> str:
        """Hash the raw/ dir for the source owning this transform."""
        source_name = record.name.split(".")[0]
        sources = self._registry_data.get("sources", {})
        if source_name in sources:
            raw_path = Path(sources[source_name]["path"])
            if not raw_path.is_absolute():
                raw_path = self._root / raw_path
            return hash_directory(raw_path)
        return hash_string(record.name)

    def _output_hash(self, name: str, ctx: BuildContext) -> str:
        """Hash all artifacts saved by a transform during this run."""
        from mareforma.pipeline.context import hash_artifact
        h = hashlib.sha256()
        for artifact_name, path in ctx.saved_artifacts.items():
            if artifact_name.startswith(name):
                try:
                    h.update(hash_artifact(path).encode())
                except OSError:
                    pass
        return h.hexdigest()


# ---------------------------------------------------------------------------
# Rich output helpers
# ---------------------------------------------------------------------------

_STATUS_STYLE = {
    "cached":    ("[dim]✓[/dim]", "[dim]cached[/dim]"),
    "running":   ("[yellow]⠋[/yellow]", "[yellow]running...[/yellow]"),
    "done":      ("[green]✓[/green]", "[green]done[/green]"),
    "failed":    ("[red]✗[/red]", "[red]failed[/red]"),
    "skipped":   ("[dim]○[/dim]", "[dim]skipped[/dim]"),
    "would_run": ("[cyan]→[/cyan]", "[cyan]would run[/cyan]"),
}


def _print_node(
    console: Any,
    name: str,
    status: str,
    note: str | None = None,
) -> None:
    icon, label = _STATUS_STYLE.get(status, ("?", status))
    suffix = f"  [dim italic]{note}[/dim italic]" if note else ""
    if console is not None:
        console.print(f"  {icon} [bold]{name}[/bold]  {label}{suffix}")
    else:
        print(f"  [{status}] {name}{(' — ' + note) if note else ''}")
