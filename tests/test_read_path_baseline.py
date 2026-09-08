"""Read-path wall-clock baseline for ``refutation_status``, ``get_claim`` and ``query``.

These three shapes read a claim (or a page of claims) and classify or return
it directly off stored columns. A later change that adds a per-row signature
and enrolment replay to the read path pays its cost here, on exactly these
calls, so this module records a repeatable before number the change can be
measured against.

It is a RECORDER, not a gate. There is no wall-clock assertion: absolute
timings are machine- and load-dependent and would flake in CI. The only
assertions are that each call runs and produces a finite, positive median.
The committed snapshot in ``tests/data/read_path_baseline.json`` is the
reference the next change diffs against; re-run this module with
``MAREFORMA_PERF_OUT=<path>`` to regenerate it on the same machine.

Two costs must be measured SEPARATELY when the read-path replay lands: the
replay itself, and any ``is_enrolled`` caching that offsets it. Bundling them
makes a replay regression look free because the cache win hides it. Time the
replay against this baseline first, with caching held constant, then add the
cache and re-measure.
"""
from __future__ import annotations

import json
import os
import statistics
import time
from pathlib import Path

import pytest

import mareforma
from tests._helpers import _bootstrap_key, _enroll_key

# A few hundred claims: large enough that a per-row read cost shows against the
# fixed call overhead, small enough to build and time inside the default suite.
_GRAPH_SIZE = 300
# Iterations per shape. A short warmup discards first-call import/JIT effects,
# then the median over the sample is the reported number.
_WARMUP = 5
_ITERS = 60
_BASELINE_PATH = Path(__file__).parent / "data" / "read_path_baseline.json"


def _build_signed_graph(tmp_path: Path) -> tuple[list[str], list[str]]:
    """Build a ~300-claim graph signed by the enrolled root.

    Returns ``(claim_ids, contradicted_ids)``. The root key opened here is the
    project's enrolled generator, so every claim is signed and verifies on
    read; that is what makes the read-path filter and the refutation classifier
    do real work rather than short-circuiting on unsigned rows. A second
    enrolled key issues a handful of contradiction verdicts over the
    root-signed claims (a verdict issuer must be an external witness whose keyid
    is not on the claim envelope), so some rows classify as "contradicted" off a
    signed verdict rather than the clean fast path.
    """
    root_key = _bootstrap_key(tmp_path, "root.key")
    ids: list[str] = []
    with mareforma.open(tmp_path, key_path=root_key) as g:
        # One backup rewrite for the whole build; a per-claim re-serialisation
        # is quadratic and would swamp the graph the benchmark reads from.
        with g.defer_backup():
            anchor = g.assert_claim("baseline anchor", generated_by="bench")
            ids.append(anchor)
            for i in range(1, _GRAPH_SIZE):
                supports = [anchor] if i % 3 == 0 else []
                ids.append(
                    g.assert_claim(
                        f"baseline claim number {i}",
                        generated_by="bench",
                        supports=supports,
                    )
                )

    # Enrol a second witness and issue the verdicts from it: the older claim of
    # each disjoint pair is the one the trigger marks invalid.
    witness_key = _bootstrap_key(tmp_path, "witness.key")
    _enroll_key(tmp_path, root_key, witness_key, identity="witness@bench.example")
    contradicted: list[str] = []
    with mareforma.open(tmp_path, key_path=witness_key) as g:
        for j in range(0, 20, 2):
            older, newer = ids[j + 1], ids[j + 1 + _GRAPH_SIZE // 2]
            g.record_contradiction_verdict(
                verdict_id=f"bench-verdict-{j}",
                member_claim_id=newer,
                other_claim_id=older,
            )
            contradicted.append(older)
    return ids, contradicted


def _median_ms(fn, *, iters: int = _ITERS, warmup: int = _WARMUP) -> float:
    """Median wall-clock of *fn* in milliseconds over *iters* calls."""
    for _ in range(warmup):
        fn()
    samples: list[float] = []
    for _ in range(iters):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


def _measure(tmp_path: Path) -> dict:
    """Build the graph and time the three read shapes; return the record."""
    ids, contradicted = _build_signed_graph(tmp_path)
    root_key = tmp_path / "root.key"
    # Pick probe targets that exercise the classifier's signed-verdict path and
    # the plain path, and a mid-graph claim for the single-row read.
    contradicted_id = contradicted[0]
    plain_id = ids[len(ids) // 2]
    with mareforma.open(tmp_path, key_path=root_key) as g:
        results = {
            "refutation_status_contradicted_ms": _median_ms(
                lambda: g.refutation_status(contradicted_id)
            ),
            "refutation_status_clean_ms": _median_ms(
                lambda: g.refutation_status(plain_id)
            ),
            "get_claim_ms": _median_ms(lambda: g.get_claim(plain_id)),
            "query_limit_20_ms": _median_ms(
                lambda: g.query(limit=20), iters=_ITERS // 2
            ),
            "query_limit_200_ms": _median_ms(
                lambda: g.query(limit=200), iters=_ITERS // 2
            ),
        }
    return {
        "graph_size": _GRAPH_SIZE,
        "iterations": _ITERS,
        "warmup": _WARMUP,
        "mareforma_version": mareforma.__version__,
        "shapes_ms": results,
    }


def test_read_path_baseline_records(tmp_path: Path) -> None:
    """Time the three read shapes and record them; no wall-clock gate.

    Prints a table (run with ``-s`` to see it) and, when ``MAREFORMA_PERF_OUT``
    is set, writes the JSON record there so the committed snapshot can be
    regenerated on the measuring machine. Against the committed baseline it
    prints the per-shape delta for a quick eyeball, but never fails on it: the
    diff is for the human and the change that adds the replay, not a CI gate.
    """
    record = _measure(tmp_path)
    shapes = record["shapes_ms"]

    print("\nread-path baseline (median ms, graph_size="
          f"{record['graph_size']}, mareforma {record['mareforma_version']}):")
    for name, value in shapes.items():
        print(f"  {name:38s} {value:8.4f} ms")

    baseline = None
    if _BASELINE_PATH.exists():
        baseline = json.loads(_BASELINE_PATH.read_text())
        print("delta vs committed baseline "
              f"(mareforma {baseline.get('mareforma_version', '?')}):")
        for name, value in shapes.items():
            prior = baseline.get("shapes_ms", {}).get(name)
            if prior:
                pct = (value - prior) / prior * 100.0
                print(f"  {name:38s} {value - prior:+8.4f} ms ({pct:+6.1f}%)")

    out = os.environ.get("MAREFORMA_PERF_OUT")
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        Path(out).write_text(json.dumps(record, indent=2) + "\n")

    # The only gate: every shape ran and produced a finite, positive median.
    for name, value in shapes.items():
        assert value > 0.0 and value == value, f"{name} produced {value}"
