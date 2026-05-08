"""
tests/test_support.py — unit tests for mareforma/support.py.

Covers:
  - SINGLE: one run, one source; unknown transform returns SINGLE
  - REPLICATED: ≥2 runs with identical output_hash
  - REPLICATED → SINGLE if output_hash varies
  - CONVERGED: same step name across ≥2 distinct source prefixes
  - CONSISTENT: any claim linked to this transform has non-empty supports_json
  - ESTABLISHED: CONVERGED + CONSISTENT
  - compute_all: empty db → empty dict; matches compute() per transform
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from mareforma.db import (
    open_db,
    begin_run,
    end_run,
    record_deps,
)
from mareforma.support import (
    compute,
    compute_all,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _open(tmp_path: Path):
    return open_db(tmp_path)


def _run(
    conn,
    name: str,
    deps: list[str] | None = None,
    output_hash: str | None = None,
) -> str:
    run_id = str(uuid.uuid4())
    begin_run(conn, run_id, name, "ih", "sh")
    record_deps(conn, name, deps or [])
    end_run(conn, run_id, status="success", output_hash=output_hash or f"h_{run_id}")
    return run_id


def _add_claim_with_support(conn, transform_name: str) -> None:
    """Insert a claim with non-empty supports_json, linked to a run of transform_name."""
    run_id = _run(conn, transform_name)
    claim_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO claims
            (claim_id, source_name, text, classification, supports_json, contradicts_json,
             status, generated_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            claim_id, transform_name.split(".")[0], "test claim",
            "ANALYTICAL", json.dumps(["10.1038/test"]), "[]", "open",
            "human", "2026-01-01", "2026-01-01",
        ),
    )
    conn.execute(
        "INSERT INTO evidence (claim_id, run_id, created_at) VALUES (?, ?, ?)",
        (claim_id, run_id, "2026-01-01"),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Empty db
# ---------------------------------------------------------------------------

def test_compute_all_empty_db(tmp_path):
    conn = _open(tmp_path)
    assert compute_all(conn, tmp_path) == {}


# ---------------------------------------------------------------------------
# SINGLE
# ---------------------------------------------------------------------------

def test_single_one_run(tmp_path):
    conn = _open(tmp_path)
    _run(conn, "morphology.load")
    assert compute("morphology.load", conn, tmp_path) == "SINGLE"


def test_single_different_hashes(tmp_path):
    """Two runs with different output_hashes → not REPLICATED → SINGLE."""
    conn = _open(tmp_path)
    _run(conn, "morphology.load", output_hash="hash_a")
    _run(conn, "morphology.load", output_hash="hash_b")
    assert compute("morphology.load", conn, tmp_path) == "SINGLE"


def test_compute_unknown_transform_returns_single(tmp_path):
    """Transform never run → SINGLE (never raises)."""
    conn = _open(tmp_path)
    assert compute("nonexistent.transform", conn, tmp_path) == "SINGLE"


# ---------------------------------------------------------------------------
# REPLICATED
# ---------------------------------------------------------------------------

def test_replicated_two_stable_runs(tmp_path):
    conn = _open(tmp_path)
    _run(conn, "morphology.load", output_hash="stable_hash")
    _run(conn, "morphology.load", output_hash="stable_hash")
    assert compute("morphology.load", conn, tmp_path) == "REPLICATED"


# ---------------------------------------------------------------------------
# CONVERGED
# ---------------------------------------------------------------------------

def test_converged_two_sources_same_step(tmp_path):
    conn = _open(tmp_path)
    _run(conn, "morphology.features")
    _run(conn, "patchseq.features")
    assert compute("morphology.features", conn, tmp_path) == "CONVERGED"
    assert compute("patchseq.features", conn, tmp_path) == "CONVERGED"


def test_not_converged_single_source_two_runs(tmp_path):
    """Same source prefix twice is not CONVERGED — with stable hash it's REPLICATED."""
    conn = _open(tmp_path)
    _run(conn, "morphology.features", output_hash="stable_hash")
    _run(conn, "morphology.features", output_hash="stable_hash")
    assert compute("morphology.features", conn, tmp_path) == "REPLICATED"


def test_not_converged_no_dot_name(tmp_path):
    """Transform with no dot cannot be CONVERGED."""
    conn = _open(tmp_path)
    _run(conn, "rootonly")
    assert compute("rootonly", conn, tmp_path) in {"SINGLE", "REPLICATED"}


# ---------------------------------------------------------------------------
# CONSISTENT
# ---------------------------------------------------------------------------

def test_consistent_claim_with_supports(tmp_path):
    """Claim with non-empty supports_json → CONSISTENT."""
    conn = _open(tmp_path)
    _run(conn, "morphology.load")
    _add_claim_with_support(conn, "morphology.load")
    assert compute("morphology.load", conn, tmp_path) == "CONSISTENT"


def test_not_consistent_claim_empty_supports(tmp_path):
    """Claim exists but supports_json = [] → not CONSISTENT."""
    conn = _open(tmp_path)
    run_id = _run(conn, "morphology.load")
    claim_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO claims
            (claim_id, source_name, text, classification, supports_json, contradicts_json,
             status, generated_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (claim_id, "morphology", "test", "INFERRED", "[]", "[]", "open",
         "human", "2026-01-01", "2026-01-01"),
    )
    conn.execute(
        "INSERT INTO evidence (claim_id, run_id, created_at) VALUES (?, ?, ?)",
        (claim_id, run_id, "2026-01-01"),
    )
    conn.commit()
    assert compute("morphology.load", conn, tmp_path) == "SINGLE"


# ---------------------------------------------------------------------------
# ESTABLISHED
# ---------------------------------------------------------------------------

def test_established_converged_and_consistent(tmp_path):
    """CONVERGED + CONSISTENT → ESTABLISHED."""
    conn = _open(tmp_path)
    _run(conn, "morphology.features")
    _run(conn, "patchseq.features")
    _add_claim_with_support(conn, "morphology.features")
    assert compute("morphology.features", conn, tmp_path) == "ESTABLISHED"


# ---------------------------------------------------------------------------
# compute_all: batch vs single agreement
# ---------------------------------------------------------------------------

def test_compute_all_matches_compute_single(tmp_path):
    conn = _open(tmp_path)
    _run(conn, "morphology.load", output_hash="h1")
    _run(conn, "morphology.load", output_hash="h1")
    _run(conn, "morphology.features")
    _run(conn, "patchseq.features")

    all_results = compute_all(conn, tmp_path)
    for name in ["morphology.load", "morphology.features", "patchseq.features"]:
        assert all_results[name] == compute(name, conn, tmp_path), f"Mismatch for {name}"
