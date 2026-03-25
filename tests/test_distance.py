"""
tests/test_distance.py — unit tests for mareforma/distance.py.

Covers:
  - compute_all: empty db → empty dict
  - compute_all: single root (no deps) → distance = weight of its class
  - compute_all: linear chain RAW → PROCESSED → ANALYSED → distance accumulates
  - compute_all: no class recorded → unknown weight used
  - compute: single transform lookup and unknown transform → 0.0
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from mareforma.db import open_db, begin_run, end_run, record_deps, write_transform_class
from mareforma.distance import CLASS_WEIGHTS, compute, compute_all


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _open(tmp_path: Path) -> sqlite3.Connection:
    return open_db(tmp_path)


def _run(conn, name, deps=None, cls=None):
    """Create a successful run for *name* with optional class."""
    run_id = str(uuid.uuid4())
    begin_run(conn, run_id, name, "ih", "sh")
    record_deps(conn, name, deps or [])
    end_run(conn, run_id, status="success", output_hash=f"h_{name}")
    if cls:
        write_transform_class(
            conn, run_id,
            transform_class=cls, class_confidence=0.9,
            class_method="heuristic", class_reason="test",
        )
    return run_id


# ---------------------------------------------------------------------------
# Empty db
# ---------------------------------------------------------------------------

def test_compute_all_empty_db(tmp_path):
    conn = _open(tmp_path)
    result = compute_all(conn)
    assert result == {}


# ---------------------------------------------------------------------------
# Single root node
# ---------------------------------------------------------------------------

def test_single_root(tmp_path):
    """Root node with a non-trivial class accumulates its own weight."""
    conn = _open(tmp_path)
    _run(conn, "morph.load", cls="processed")
    result = compute_all(conn)
    assert result["morph.load"] == pytest.approx(CLASS_WEIGHTS["processed"])


# ---------------------------------------------------------------------------
# Linear chain
# ---------------------------------------------------------------------------

def test_linear_chain_accumulates(tmp_path):
    conn = _open(tmp_path)
    _run(conn, "src.load",     deps=[],             cls="raw")
    _run(conn, "src.filter",   deps=["src.load"],    cls="processed")
    _run(conn, "src.features", deps=["src.filter"],  cls="analysed")
    _run(conn, "src.classify", deps=["src.features"], cls="inferred")

    d = compute_all(conn)
    assert d["src.load"]     == pytest.approx(0.0)
    assert d["src.filter"]   == pytest.approx(0.1)
    assert d["src.features"] == pytest.approx(0.6)   # 0.1 + 0.5
    assert d["src.classify"] == pytest.approx(1.6)   # 0.1 + 0.5 + 1.0


# ---------------------------------------------------------------------------
# No class recorded → unknown weight used
# ---------------------------------------------------------------------------

def test_no_class_uses_unknown_weight(tmp_path):
    conn = _open(tmp_path)
    _run(conn, "src.load",   deps=[],           cls=None)  # no class
    _run(conn, "src.filter", deps=["src.load"], cls=None)

    d = compute_all(conn)
    assert d["src.load"]   == pytest.approx(0.5)   # CLASS_WEIGHTS["unknown"]
    assert d["src.filter"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# compute() — single transform and unknown name
# ---------------------------------------------------------------------------

def test_compute_single(tmp_path):
    conn = _open(tmp_path)
    _run(conn, "src.load",   cls="raw")
    _run(conn, "src.filter", deps=["src.load"], cls="processed")

    assert compute("src.load",   conn) == pytest.approx(0.0)
    assert compute("src.filter", conn) == pytest.approx(0.1)
    assert compute("nonexistent.transform", conn) == pytest.approx(0.0)
