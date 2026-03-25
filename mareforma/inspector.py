"""
inspector.py — Transform content inspection for epistemic classification.

Classifies each completed transform run as one of:
  RAW        : root node (no depends_on parents) — no file reading
  PROCESSED  : output values ⊆ input values, row count ≤ input count
  ANALYSED   : new values introduced, within input value range
  INFERRED   : output values outside all input value ranges
  unknown    : any load failure, file too large, unknown format, or numpy/pandas unavailable

Classification flow
-------------------
  classify_run(conn, run_id, root)
        │
        ├─[no parents in transform_deps]──► RAW (heuristic, confidence=1.0)
        │
        ├─[cache hit: same output_hash already classified]──► return cached
        │
        ├─[get artifact paths for this run + parent runs]
        │
        ├─[load output + input files via _load_as_frame()]
        │      ├─[file not found]────────────────────────► unknown
        │      ├─[file > MAX_INSPECT_BYTES]───────────────► unknown
        │      ├─[unknown extension]──────────────────────► unknown
        │      ├─[pandas/numpy not installed]─────────────► unknown
        │      └─[load OK]──► _compare_frames(out_df, inp_df)
        │                           ├─ PROCESSED
        │                           ├─ ANALYSED
        │                           └─ INFERRED
        │
        └─[write result to transform_runs via db.write_transform_class()]

Never raises: all exceptions → unknown class, confidence=0.0.
"""

from __future__ import annotations

import sqlite3
import warnings
from pathlib import Path

from mareforma.db import (
    DatabaseError,
    get_artifact_paths,
    get_parent_artifact_paths,
    lookup_cached_class,
    write_transform_class,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_INSPECT_BYTES = 100 * 1024 * 1024  # 100 MB

_SUPPORTED_EXTENSIONS = frozenset({
    ".csv", ".tsv",
    ".parquet",
    ".npy", ".npz",
})

_CLASS_REASON_MAX = 500  # characters


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_run(
    conn: sqlite3.Connection,
    run_id: str,
    transform_name: str,
    root: Path,
) -> None:
    """Classify *run_id* and write the result to transform_runs.

    Always returns None — classification failure writes 'unknown' rather
    than raising.

    Parameters
    ----------
    conn:
        Open graph.db connection.
    run_id:
        The run_id of the completed (status='success') transform run.
    transform_name:
        Name of the transform, used to look up parents in transform_deps.
    root:
        Project root (used to validate artifact paths are within the project).
    """
    try:
        _classify(conn, run_id, transform_name, root)
    except Exception as exc:  # noqa: BLE001
        warnings.warn(
            f"[inspector] classify_run failed for run {run_id}: {exc}. "
            "Class recorded as 'unknown'.",
            stacklevel=2,
        )
        try:
            write_transform_class(
                conn, run_id,
                transform_class="unknown",
                class_confidence=0.0,
                class_method="content_inspection",
                class_reason=f"inspection error: {str(exc)[:200]}",
            )
        except DatabaseError:
            pass  # db write also failed — nothing to do


def _classify(
    conn: sqlite3.Connection,
    run_id: str,
    transform_name: str,
    root: Path,
) -> None:
    """Inner classification logic. May raise — caller handles."""
    # Root node detection: no parents in transform_deps → RAW
    parents = conn.execute(
        "SELECT depends_on_name FROM transform_deps WHERE transform_name = ? LIMIT 1",
        (transform_name,),
    ).fetchall()

    if not parents:
        write_transform_class(
            conn, run_id,
            transform_class="raw",
            class_confidence=1.0,
            class_method="heuristic",
            class_reason="root node: no depends_on parents",
        )
        return

    # Classification cache: same output_hash already classified?
    run_row = conn.execute(
        "SELECT output_hash FROM transform_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    output_hash = run_row["output_hash"] if run_row else ""

    cached = lookup_cached_class(conn, output_hash)
    if cached:
        cls, conf, method, reason = cached
        write_transform_class(
            conn, run_id,
            transform_class=cls,
            class_confidence=conf,
            class_method=method,
            class_reason=f"[cached] {reason}",
        )
        return

    # Get artifact paths for this run (outputs) and parents (inputs)
    output_paths = [
        Path(p) for p in get_artifact_paths(conn, run_id)
    ]
    input_paths = [
        Path(p) for p in get_parent_artifact_paths(conn, transform_name)
    ]

    if not output_paths:
        write_transform_class(
            conn, run_id,
            transform_class="unknown",
            class_confidence=0.0,
            class_method="heuristic",
            class_reason="no output artifacts recorded for this run",
        )
        return

    # Load output and input frames
    out_frames = _load_paths(output_paths, root)
    inp_frames = _load_paths(input_paths, root) if input_paths else []

    if out_frames is None:
        write_transform_class(
            conn, run_id,
            transform_class="unknown",
            class_confidence=0.0,
            class_method="content_inspection",
            class_reason="output files could not be loaded (see warnings)",
        )
        return

    # If no inputs could be loaded, we can only say 'unknown'
    if not inp_frames:
        write_transform_class(
            conn, run_id,
            transform_class="unknown",
            class_confidence=0.0,
            class_method="content_inspection",
            class_reason="no input artifacts available for comparison",
        )
        return

    # Combine multiple frames into one for comparison
    try:
        import pandas as pd  # noqa: PLC0415
        out_df = pd.concat(out_frames, ignore_index=True)
        inp_df = pd.concat(inp_frames, ignore_index=True)
    except Exception as exc:
        write_transform_class(
            conn, run_id,
            transform_class="unknown",
            class_confidence=0.0,
            class_method="content_inspection",
            class_reason=f"concat failed: {str(exc)[:200]}",
        )
        return

    cls, conf, reason = _compare_frames(out_df, inp_df)
    write_transform_class(
        conn, run_id,
        transform_class=cls,
        class_confidence=conf,
        class_method="content_inspection",
        class_reason=reason[:_CLASS_REASON_MAX],
    )


# ---------------------------------------------------------------------------
# Frame loading
# ---------------------------------------------------------------------------

def _load_paths(paths: list[Path], root: Path) -> list | None:
    """Load all paths as DataFrames. Returns None if any critical load fails."""
    frames = []
    for path in paths:
        frame = _load_as_frame(path, root)
        if frame is not None:
            frames.append(frame)
    # If we loaded at least some frames, return them (partial is OK for inputs)
    return frames if frames else None


def _load_as_frame(path: Path, root: Path):
    """Load a single file as a pandas DataFrame, or return None.

    Supports: .csv / .tsv, .parquet, .npy / .npz.
    Returns None on any error (file too large, unknown format,
    missing dependency, load failure, path outside project root).
    """
    try:
        import pandas as pd  # noqa: PLC0415
    except ImportError:
        warnings.warn(
            "[inspector] pandas is not installed — transform classification unavailable. "
            "Install with: pip install pandas",
            stacklevel=3,
        )
        return None

    # Security: path must be inside the project root
    try:
        resolved = path.resolve()
        root_resolved = root.resolve()
        if not str(resolved).startswith(str(root_resolved)):
            warnings.warn(
                f"[inspector] artifact path {path} is outside project root — skipping",
                stacklevel=3,
            )
            return None
    except OSError:
        return None

    if not path.exists():
        return None

    # Size guard
    try:
        size = path.stat().st_size
    except OSError:
        return None

    if size > MAX_INSPECT_BYTES:
        warnings.warn(
            f"[inspector] {path.name} is {size // (1024*1024)} MB "
            f"(> {MAX_INSPECT_BYTES // (1024*1024)} MB limit) — skipping",
            stacklevel=3,
        )
        return None

    ext = path.suffix.lower()
    if ext not in _SUPPORTED_EXTENSIONS:
        return None

    try:
        if ext in (".csv", ".tsv"):
            sep = "\t" if ext == ".tsv" else ","
            return pd.read_csv(path, sep=sep)
        elif ext == ".parquet":
            return pd.read_parquet(path)
        elif ext in (".npy", ".npz"):
            try:
                import numpy as np  # noqa: PLC0415
            except ImportError:
                warnings.warn(
                    "[inspector] numpy is not installed — .npy/.npz files will be skipped",
                    stacklevel=3,
                )
                return None
            arr = np.load(path, allow_pickle=False)
            if ext == ".npz":
                # npz: combine all arrays into one column
                arrays = [arr[k].flatten() for k in arr.files]
                import numpy as np2  # noqa: F811, PLC0415
                combined = np2.concatenate(arrays) if arrays else np2.array([])
                return pd.DataFrame({"value": combined})
            else:
                return pd.DataFrame({"value": arr.flatten()})
    except Exception:  # noqa: BLE001
        return None

    return None


# ---------------------------------------------------------------------------
# Frame comparison
# ---------------------------------------------------------------------------

def _compare_frames(
    out_df,
    inp_df,
) -> tuple[str, float, str]:
    """Compare output DataFrame against input DataFrame.

    Returns
    -------
    (class, confidence, reason)
        class      : 'processed' | 'analysed' | 'inferred' | 'unknown'
        confidence : 0.0–1.0
        reason     : human-readable explanation
    """
    try:
        import pandas as pd  # noqa: PLC0415
        import numpy as np  # noqa: PLC0415
    except ImportError:
        return "unknown", 0.0, "pandas/numpy unavailable"

    if out_df.empty:
        return "processed", 0.9, "output is empty (0 rows) — trivially PROCESSED"

    # Collect all numeric values from input and output
    def _numeric_values(df) -> np.ndarray:
        nums = []
        for col in df.columns:
            try:
                s = pd.to_numeric(df[col], errors="coerce").dropna()
                if len(s):
                    nums.append(s.values)
            except Exception:  # noqa: BLE001
                pass
        return np.concatenate(nums) if nums else np.array([])

    out_vals = _numeric_values(out_df)
    inp_vals = _numeric_values(inp_df)

    # No numeric data to compare — fall back to row count heuristic
    if len(out_vals) == 0 or len(inp_vals) == 0:
        if len(out_df) <= len(inp_df):
            return (
                "processed", 0.5,
                "no numeric columns for comparison; row count ≤ input → PROCESSED (low confidence)",
            )
        return (
            "analysed", 0.4,
            "no numeric columns for comparison; row count > input → ANALYSED (low confidence)",
        )

    inp_min, inp_max = float(inp_vals.min()), float(inp_vals.max())
    out_min, out_max = float(out_vals.min()), float(out_vals.max())

    # INFERRED: output values outside input range
    if out_min < inp_min - 1e-9 or out_max > inp_max + 1e-9:
        reason = (
            f"output range [{out_min:.4g}, {out_max:.4g}] extends beyond "
            f"input range [{inp_min:.4g}, {inp_max:.4g}]"
        )
        return "inferred", 0.85, reason

    # Check if output values are a strict subset of input values (PROCESSED)
    # Use a set comparison on rounded values to handle float precision
    try:
        inp_set = set(np.round(inp_vals, 8))
        out_set = set(np.round(out_vals, 8))
        new_values = out_set - inp_set

        if not new_values and len(out_df) <= len(inp_df):
            return (
                "processed", 0.85,
                f"all {len(out_set)} output values present in input; "
                f"row count {len(out_df)} ≤ {len(inp_df)}",
            )

        if new_values:
            n = len(new_values)
            reason = (
                f"{n} new value{'s' if n != 1 else ''} in output not present in input "
                f"(within input range [{inp_min:.4g}, {inp_max:.4g}])"
            )
            return "analysed", 0.80, reason

    except Exception:  # noqa: BLE001
        pass

    # Default: if row count grows, call it ANALYSED
    if len(out_df) > len(inp_df):
        return (
            "analysed", 0.6,
            f"output has more rows ({len(out_df)}) than input ({len(inp_df)})",
        )

    return "processed", 0.6, "output values within input range; row count stable"
