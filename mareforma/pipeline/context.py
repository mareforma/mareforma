"""
pipeline/context.py — BuildContext injected into every transform by the runner.

The BuildContext is the transform's window into the pipeline. It provides:
  - ctx.load(name)          read the output of a named upstream transform
  - ctx.save(name, data)    write output and record schema + artifact in graph.db
  - ctx.claim(text, ...)    assert an explicit scientific claim, linked to this run
  - ctx.source_path(source) resolve the raw/ path for a registered source
  - ctx.log(msg)            write a message to the rich console + graph.db

Serialisation
-------------
ctx.save / ctx.load use pickle by default, which handles any Python object.
For DataFrames, pass fmt="parquet" or fmt="csv" to get readable outputs.

Schema recording
----------------
When ctx.save() receives a pandas DataFrame, it automatically records
column names, dtypes, and shape in graph.db's artifacts table.

Provenance
----------
Every ctx.save() call records the artifact in graph.db linked to the current
run_id. Every ctx.claim() call creates a Claim linked to this run_id.

Confidence scale (for ctx.claim)
---------------------------------
  anecdotal   : single observation, no systematic analysis
  exploratory : systematic, single dataset, not replicated  (default)
  preliminary : internally replicated or consistent across subsets
  supported   : externally replicated or large N
  established : multiple independent replications

ctx.claim() flow
-----------------------
  @transform fn calls ctx.claim("L2/3 neurons have X", supports=["10.1038/x"])
       │
       ├─▶ validate confidence/status/replication_status
       │
       ├─▶ Emit warnings (non-fatal):
       │     1. source_name is None → provenance incomplete warning
       │     2. confidence in (supported, established) and not supports → no literature warning
       │
       └─▶ db.add_claim(conn, root, text, ..., supports=supports, run_id=self._run_id)
               │
               ├─▶ INSERT INTO claims (...)
               ├─▶ INSERT INTO evidence (claim_id, run_id, ...)
               └─▶ _backup_claims_toml(conn, root)  → claims.toml
"""

from __future__ import annotations

import hashlib
import json
import os
import pickle
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3
    from rich.console import Console


_SUPPORTED_FORMATS = ("pickle", "parquet", "csv", "json")


class BuildContext:
    """Passed to every transform function during ``mareforma build``.

    Parameters
    ----------
    root:
        Project root directory (where mareforma.project.toml lives).
    transform_name:
        The name of the currently-running transform.
    registry_data:
        Parsed mareforma.project.toml dict (sources block).
    run_id:
        UUID string for this transform run. Used to link provenance records.
    db:
        Open sqlite3.Connection to graph.db. Used by ctx.save() and ctx.claim().
    console:
        Rich Console for log output. If None, falls back to print.
    artifacts_dir:
        Directory where ctx.save outputs are written.
        Default: .mareforma/artifacts/
    """

    def __init__(
        self,
        root: Path,
        transform_name: str,
        registry_data: dict[str, Any],
        run_id: str,
        db: "sqlite3.Connection",
        console: "Console | None" = None,
        artifacts_dir: Path | None = None,
    ) -> None:
        self._root = root
        self._transform_name = transform_name
        self._registry_data = registry_data
        self._run_id = run_id
        self._db = db
        self._console = console
        self._artifacts_dir = artifacts_dir or (root / ".mareforma" / "artifacts")
        self._artifacts_dir.mkdir(parents=True, exist_ok=True)
        self._saved: dict[str, Path] = {}  # name → output path
        self._schemas: dict[str, dict] = {}  # name → schema metadata

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, name: str) -> Any:
        """Load the output of the named upstream transform.

        The artifact must have been saved by a previous transform in this
        build or a prior build (found in .mareforma/artifacts/).

        Raises
        ------
        ArtifactNotFoundError
            If the named artifact does not exist.
        """
        # Check in-memory saves first (same build, earlier node)
        if name in self._saved:
            return self._read(self._saved[name])

        # Fall back to persisted artifact from a prior build
        candidates = list(self._artifacts_dir.glob(f"{_safe_name(name)}.*"))
        if not candidates:
            raise ArtifactNotFoundError(
                f"No artifact found for '{name}'.\n"
                f"Ensure '{name}' runs before this transform (check depends_on)."
            )
        # Pick the most recently modified
        path = max(candidates, key=lambda p: p.stat().st_mtime)
        return self._read(path)

    def save(self, name: str, data: Any, fmt: str = "pickle") -> Path:
        """Persist *data* as an artifact named *name*.

        Parameters
        ----------
        name:
            Artifact identifier, e.g. ``"registered_skeletons"``.
            Will be namespaced under the current transform automatically.
        data:
            Any Python object. DataFrames get special schema recording.
        fmt:
            Serialisation format: ``"pickle"`` (default), ``"parquet"``,
            ``"csv"``, or ``"json"``.

        Returns
        -------
        Path
            The path where the artifact was written.

        Notes
        -----
        If called twice with the same *name* in one transform, the second
        call silently overwrites the first (last write wins).
        """
        if fmt not in _SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format '{fmt}'. Choose from: {_SUPPORTED_FORMATS}"
            )

        full_name = f"{self._transform_name}.{name}"

        if full_name in self._saved:
            self.log(f"  [debug] overwriting artifact '{full_name}' (last write wins)")

        try:
            path = self._write(full_name, data, fmt)
        except OSError as exc:
            from mareforma.db import ContextError  # avoid circular at module level
            raise ArtifactSaveError(
                f"Failed to save artifact '{full_name}': {exc}"
            ) from exc

        self._saved[full_name] = path

        # Record schema metadata if this is a DataFrame
        schema = _extract_schema(data, fmt, path)
        if schema:
            self._schemas[full_name] = schema
            self.log(
                f"  schema recorded: {schema.get('shape', '')} "
                f"{schema.get('columns', '')}"
            )

        # Record artifact in graph.db
        from mareforma.db import record_artifact
        try:
            size = path.stat().st_size
            sha = hash_artifact(path)
            record_artifact(
                self._db, self._run_id, full_name, path, fmt,
                sha256=sha, size_bytes=size, schema=schema,
            )
        except Exception:  # noqa: BLE001 — db write failure must not crash the transform
            self.log(f"  [warning] could not record artifact '{full_name}' in graph.db")

        return path

    def claim(
        self,
        text: str,
        *,
        confidence: str = "exploratory",
        status: str = "open",
        replication_status: str = "unknown",
        source_name: str | None = None,
        artifact_name: str | None = None,
        supports: list[str] | None = None,
        contradicts: list[str] | None = None,
        generated_by: str = "human",
        generation_method: str = "explicit",
    ) -> str:
        """Assert an explicit scientific claim, linked to this transform run.

        Parameters
        ----------
        text:
            The claim as a plain-English, falsifiable assertion.
            E.g. ``"L2/3 neurons have a mean axon extent of 0.7 mm (n=312)"``.
        confidence:
            Confidence category. One of: anecdotal, exploratory (default),
            preliminary, supported, established.
        status:
            Epistemic status. One of: open (default), supported, contested,
            retracted.
        replication_status:
            Replication evidence. One of: unknown (default), single_study,
            independently_replicated, failed_replication, meta_analyzed.
        source_name:
            Optional: the registered source this claim is about.
        artifact_name:
            Optional: the specific artifact (within this run) that supports
            the claim. E.g. ``"morphology.features.features"``.
        supports:
            Optional list of DOI strings or claim_ids this claim rests on.
            PaperConnector is called for each DOI; failures are non-fatal.
        contradicts:
            Optional list of DOI strings or claim_ids this claim contests.
        generated_by:
            'human' (default) or a model identifier string.
        generation_method:
            'explicit' (default) | 'agent-wrapped' | 'inferred'

        Returns
        -------
        str
            The UUID claim_id for future reference.

        Raises
        ------
        ValueError
            If confidence, status, or replication_status are invalid.
        """
        from mareforma.db import add_claim, validate_confidence, validate_status, validate_replication_status

        # Validate first so we fail before any literature writes
        validate_confidence(confidence)
        validate_status(status)
        validate_replication_status(replication_status)

        # Warnings — non-fatal, yellow-styled
        if source_name is None:
            self._warn(
                f"Claim '{text[:40]}{'...' if len(text) > 40 else ''}' "
                "has no source_name — provenance will be incomplete."
            )
        if confidence in ("supported", "established") and not supports:
            self._warn(
                f"Claim has '{confidence}' confidence but no supporting "
                "literature (supports=[])."
            )

        resolved_supports = list(supports) if supports else []

        claim_id = add_claim(
            self._db,
            self._root,
            text,
            confidence=confidence,
            status=status,
            replication_status=replication_status,
            source_name=source_name,
            run_id=self._run_id,
            artifact_name=artifact_name,
            generated_by=generated_by,
            generation_method=generation_method,
            supports=resolved_supports or None,
            contradicts=contradicts,
        )
        self.log(
            f"  claim recorded [{confidence}]: "
            f"{text[:60]}{'...' if len(text) > 60 else ''}"
        )
        return claim_id

    def _warn(self, msg: str) -> None:
        """Emit a non-fatal warning to the console, yellow-styled."""
        if self._console is not None:
            self._console.print(f"    [yellow]Warning:[/yellow] {msg}")
        else:
            import sys
            print(f"    Warning: {msg}", file=sys.stderr)

    def source_path(self, source_name: str) -> Path:
        """Return the raw/ path for a registered source.

        Raises
        ------
        KeyError
            If *source_name* is not registered in the project ontology.
        ValueError
            If the resolved path escapes the project root (path traversal guard).
        """
        sources = self._registry_data.get("sources", {})
        if source_name not in sources:
            registered = list(sources.keys())
            raise KeyError(
                f"Source '{source_name}' not in ontology. "
                f"Registered: {registered}"
            )
        raw_path = Path(sources[source_name]["path"])
        if not raw_path.is_absolute():
            raw_path = (self._root / raw_path).resolve()
        else:
            raw_path = raw_path.resolve()
        return raw_path

    def log(self, msg: str) -> None:
        """Write *msg* to the rich console (or stdout if unavailable)."""
        if self._console is not None:
            self._console.print(f"    [dim]{msg}[/dim]")
        else:
            print(f"    {msg}")

    # ------------------------------------------------------------------
    # Public read-only views (used by runner)
    # ------------------------------------------------------------------

    @property
    def root(self) -> Path:
        """Project root directory (where mareforma.project.toml lives)."""
        return self._root

    @property
    def run_id(self) -> str:
        """UUID string for the current transform run."""
        return self._run_id

    @property
    def saved_artifacts(self) -> dict[str, Path]:
        """Mapping of full artifact name → path for artifacts saved this run."""
        return dict(self._saved)

    @property
    def schemas(self) -> dict[str, dict]:
        """Schemas recorded during this build. Read by the runner."""
        return self._schemas

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _artifact_path(self, name: str, fmt: str) -> Path:
        return self._artifacts_dir / f"{_safe_name(name)}.{fmt}"

    def _write(self, name: str, data: Any, fmt: str) -> Path:
        path = self._artifact_path(name, fmt)
        if fmt == "pickle":
            path.write_bytes(pickle.dumps(data))
        elif fmt == "json":
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        elif fmt == "parquet":
            data.to_parquet(path, index=False)
        elif fmt == "csv":
            data.to_csv(path, index=False)
        return path

    def _read(self, path: Path) -> Any:
        fmt = path.suffix.lstrip(".")
        if fmt == "pickle":
            return pickle.loads(path.read_bytes())
        elif fmt == "json":
            return json.loads(path.read_text(encoding="utf-8"))
        elif fmt == "parquet":
            import pandas as pd
            return pd.read_parquet(path)
        elif fmt == "csv":
            import pandas as pd
            return pd.read_csv(path)
        else:
            raise ValueError(f"Unknown artifact format: {fmt}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_name(name: str) -> str:
    """Convert a dotted transform name to a safe filename."""
    return name.replace(".", "__")


def _extract_schema(data: Any, fmt: str, path: Path) -> dict[str, Any] | None:
    """Return schema metadata for *data* if it's a pandas DataFrame."""
    try:
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            return {
                "shape": list(data.shape),
                "columns": list(data.columns),
                "dtypes": {col: str(dtype) for col, dtype in data.dtypes.items()},
                "format": fmt,
                "path": str(path),
            }
    except ImportError:
        pass
    return None


def hash_artifact(path: Path) -> str:
    """Return a stable SHA-256 hex digest of a file's contents."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def hash_directory(path: Path) -> str:
    """Return a stable hash of all files in *path* (sorted, recursive)."""
    h = hashlib.sha256()
    if not path.exists():
        return h.hexdigest()
    for file in sorted(path.rglob("*")):
        if file.is_file():
            h.update(str(file.relative_to(path)).encode())
            h.update(file.read_bytes())
    return h.hexdigest()


class ArtifactNotFoundError(Exception):
    """Raised when ctx.load() cannot find the named artifact."""


class ArtifactSaveError(Exception):
    """Raised when ctx.save() fails to write an artifact to disk."""
