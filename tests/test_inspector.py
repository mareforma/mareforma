"""
tests/test_inspector.py — unit tests for mareforma/inspector.py.

Covers:
  - classify_run: root node (no parents) → raw
  - classify_run: PROCESSED (output values ⊆ input, fewer rows)
  - classify_run: ANALYSED (new values, within input range)
  - classify_run: INFERRED (values outside input range)
  - classify_run: file > MAX_INSPECT_BYTES → unknown
  - classify_run: unknown file extension → unknown
  - classify_run: file not found → unknown (never raises)
  - classify_run: 0-row output → processed
  - _load_as_frame: path outside project root → None (security boundary)
  - classification cache: same output_hash reuses prior result
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from mareforma.db import (
    open_db,
    begin_run,
    end_run,
    record_deps,
    record_artifact,
    write_transform_class,
    lookup_cached_class,
)
from mareforma.inspector import (
    MAX_INSPECT_BYTES,
    _load_as_frame,
    classify_run,
)

pd = pytest.importorskip("pandas")
np = pytest.importorskip("numpy")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _open(tmp_path: Path) -> sqlite3.Connection:
    return open_db(tmp_path)


def _make_run(
    conn: sqlite3.Connection,
    name: str,
    root: Path,
    depends_on: list[str] | None = None,
    artifact_path: Path | None = None,
    output_hash: str = "abc123",
) -> str:
    """Create a successful transform run with optional artifact."""
    import uuid
    run_id = str(uuid.uuid4())
    begin_run(conn, run_id, name, "ih", "sh")
    record_deps(conn, name, depends_on or [])
    if artifact_path:
        record_artifact(conn, run_id, f"{name}.out", artifact_path, "csv")
    end_run(conn, run_id, status="success", output_hash=output_hash)
    return run_id


# ---------------------------------------------------------------------------
# Root node → RAW
# ---------------------------------------------------------------------------

def test_root_node_classified_as_raw(tmp_path):
    conn = _open(tmp_path)
    run_id = _make_run(conn, "morphology.load", tmp_path)
    classify_run(conn, run_id, "morphology.load", tmp_path)
    row = conn.execute(
        "SELECT transform_class, class_method FROM transform_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    assert row["transform_class"] == "raw"
    assert row["class_method"] == "heuristic"


# ---------------------------------------------------------------------------
# Content inspection: PROCESSED
# ---------------------------------------------------------------------------

def test_processed_csv(tmp_path):
    conn = _open(tmp_path)

    parent_csv = tmp_path / "parent.csv"
    inp_df = pd.DataFrame({"x": range(1, 101), "y": range(100, 0, -1)})
    inp_df.to_csv(parent_csv, index=False)
    _make_run(conn, "src.load", tmp_path, artifact_path=parent_csv)

    child_csv = tmp_path / "child.csv"
    inp_df.head(50).to_csv(child_csv, index=False)
    child_id = _make_run(conn, "src.filter", tmp_path, depends_on=["src.load"], artifact_path=child_csv)
    classify_run(conn, child_id, "src.filter", tmp_path)

    row = conn.execute(
        "SELECT transform_class FROM transform_runs WHERE run_id = ?", (child_id,)
    ).fetchone()
    assert row["transform_class"] == "processed"


# ---------------------------------------------------------------------------
# Content inspection: ANALYSED
# ---------------------------------------------------------------------------

def test_analysed_new_values_within_range(tmp_path):
    conn = _open(tmp_path)

    parent_csv = tmp_path / "parent.csv"
    pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0, 5.0]}).to_csv(parent_csv, index=False)
    _make_run(conn, "src.load", tmp_path, artifact_path=parent_csv)

    child_csv = tmp_path / "child.csv"
    pd.DataFrame({"x_mean": [1.5, 2.5, 3.5, 4.5]}).to_csv(child_csv, index=False)
    child_id = _make_run(conn, "src.analyse", tmp_path, depends_on=["src.load"], artifact_path=child_csv)
    classify_run(conn, child_id, "src.analyse", tmp_path)

    row = conn.execute(
        "SELECT transform_class FROM transform_runs WHERE run_id = ?", (child_id,)
    ).fetchone()
    assert row["transform_class"] == "analysed"


# ---------------------------------------------------------------------------
# Content inspection: INFERRED
# ---------------------------------------------------------------------------

def test_inferred_values_outside_input_range(tmp_path):
    conn = _open(tmp_path)

    parent_csv = tmp_path / "parent.csv"
    pd.DataFrame({"x": [1.0, 2.0, 3.0]}).to_csv(parent_csv, index=False)
    _make_run(conn, "src.load", tmp_path, artifact_path=parent_csv)

    child_csv = tmp_path / "child.csv"
    pd.DataFrame({"pred": [100.0, 200.0, 300.0]}).to_csv(child_csv, index=False)
    child_id = _make_run(conn, "src.predict", tmp_path, depends_on=["src.load"], artifact_path=child_csv)
    classify_run(conn, child_id, "src.predict", tmp_path)

    row = conn.execute(
        "SELECT transform_class FROM transform_runs WHERE run_id = ?", (child_id,)
    ).fetchone()
    assert row["transform_class"] == "inferred"


# ---------------------------------------------------------------------------
# File too large → unknown
# ---------------------------------------------------------------------------

def test_large_file_yields_unknown(tmp_path, monkeypatch):
    conn = _open(tmp_path)

    parent_csv = tmp_path / "parent.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(parent_csv, index=False)
    _make_run(conn, "src.load", tmp_path, artifact_path=parent_csv)

    child_csv = tmp_path / "child.csv"
    pd.DataFrame({"x": [1, 2]}).to_csv(child_csv, index=False)
    child_id = _make_run(conn, "src.filter", tmp_path, depends_on=["src.load"], artifact_path=child_csv)

    real_stat = Path.stat

    def fake_stat(self, **kwargs):
        result = real_stat(self, **kwargs)
        if self == child_csv:
            class FakeStat:
                st_size = MAX_INSPECT_BYTES + 1
                def __getattr__(self, name):
                    return getattr(result, name)
            return FakeStat()
        return result

    monkeypatch.setattr(Path, "stat", fake_stat)
    classify_run(conn, child_id, "src.filter", tmp_path)

    row = conn.execute(
        "SELECT transform_class FROM transform_runs WHERE run_id = ?", (child_id,)
    ).fetchone()
    assert row["transform_class"] == "unknown"


# ---------------------------------------------------------------------------
# Unknown extension → unknown
# ---------------------------------------------------------------------------

def test_unknown_extension_yields_unknown(tmp_path):
    conn = _open(tmp_path)

    parent_csv = tmp_path / "parent.csv"
    pd.DataFrame({"x": [1, 2]}).to_csv(parent_csv, index=False)
    _make_run(conn, "src.load", tmp_path, artifact_path=parent_csv)

    child_pkl = tmp_path / "child.pkl"
    child_pkl.write_bytes(b"fake pickle")
    child_id = _make_run(conn, "src.pickle", tmp_path, depends_on=["src.load"], artifact_path=child_pkl)

    classify_run(conn, child_id, "src.pickle", tmp_path)
    row = conn.execute(
        "SELECT transform_class FROM transform_runs WHERE run_id = ?", (child_id,)
    ).fetchone()
    assert row["transform_class"] == "unknown"


# ---------------------------------------------------------------------------
# File not found → unknown (never raises)
# ---------------------------------------------------------------------------

def test_file_not_found_yields_unknown_no_raise(tmp_path):
    conn = _open(tmp_path)
    parent_csv = tmp_path / "parent.csv"
    pd.DataFrame({"x": [1, 2]}).to_csv(parent_csv, index=False)
    _make_run(conn, "src.load", tmp_path, artifact_path=parent_csv)

    ghost = tmp_path / "ghost.csv"
    child_id = _make_run(conn, "src.ghost", tmp_path, depends_on=["src.load"], artifact_path=ghost)

    classify_run(conn, child_id, "src.ghost", tmp_path)  # must not raise
    row = conn.execute(
        "SELECT transform_class FROM transform_runs WHERE run_id = ?", (child_id,)
    ).fetchone()
    assert row["transform_class"] == "unknown"


# ---------------------------------------------------------------------------
# 0-row output → processed
# ---------------------------------------------------------------------------

def test_empty_output_classified_as_processed(tmp_path):
    conn = _open(tmp_path)
    parent_csv = tmp_path / "parent.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(parent_csv, index=False)
    _make_run(conn, "src.load", tmp_path, artifact_path=parent_csv)

    child_csv = tmp_path / "child.csv"
    pd.DataFrame({"x": []}).to_csv(child_csv, index=False)
    child_id = _make_run(conn, "src.filter", tmp_path, depends_on=["src.load"], artifact_path=child_csv)

    classify_run(conn, child_id, "src.filter", tmp_path)
    row = conn.execute(
        "SELECT transform_class FROM transform_runs WHERE run_id = ?", (child_id,)
    ).fetchone()
    assert row["transform_class"] == "processed"


# ---------------------------------------------------------------------------
# _load_as_frame: path outside project root → None (security boundary)
# ---------------------------------------------------------------------------

def test_load_path_outside_root_returns_none(tmp_path):
    other = tmp_path.parent / "outside.csv"
    try:
        pd.DataFrame({"x": [1]}).to_csv(other, index=False)
        result = _load_as_frame(other, tmp_path)
        assert result is None
    finally:
        other.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Classification cache
# ---------------------------------------------------------------------------

def test_classification_cache_reuses_prior_result(tmp_path):
    conn = _open(tmp_path)

    parent_csv = tmp_path / "parent.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(parent_csv, index=False)
    _make_run(conn, "src.load", tmp_path, artifact_path=parent_csv)

    child_csv = tmp_path / "child.csv"
    pd.DataFrame({"x": [1, 2]}).to_csv(child_csv, index=False)

    shared_hash = "same_output_hash_abc"

    child_id_1 = _make_run(
        conn, "src.filter", tmp_path,
        depends_on=["src.load"], artifact_path=child_csv, output_hash=shared_hash,
    )
    classify_run(conn, child_id_1, "src.filter", tmp_path)

    row1 = conn.execute(
        "SELECT transform_class FROM transform_runs WHERE run_id = ?", (child_id_1,)
    ).fetchone()
    assert row1["transform_class"] is not None

    # Second run with same hash: should use cache
    child_id_2 = _make_run(
        conn, "src.filter", tmp_path,
        depends_on=["src.load"], artifact_path=child_csv, output_hash=shared_hash,
    )
    classify_run(conn, child_id_2, "src.filter", tmp_path)

    row2 = conn.execute(
        "SELECT transform_class, class_reason FROM transform_runs WHERE run_id = ?",
        (child_id_2,),
    ).fetchone()
    assert row2["transform_class"] == row1["transform_class"]
    assert "[cached]" in (row2["class_reason"] or "")
