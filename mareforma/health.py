"""health.py — Epistemic health report for mareforma status.

Traffic light (claim-based)
---------------------------
  green  : at least one REPLICATED or ESTABLISHED claim
  yellow : claims exist but all are PRELIMINARY
  red    : no claims at all
  error  : graph.db could not be read (corruption, missing table, locked)

The ``error`` state is distinct from ``red``: a fresh project legitimately
has no claims yet (``red``), but a corrupted graph.db that cannot be
read at all is a different operational signal and gets its own traffic
light so operators looking at ``mareforma health`` can tell them apart.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class HealthReport:
    claims_open: int = 0
    claims_resolved: int = 0
    claims_contradicted: int = 0
    support_level_breakdown: dict[str, int] = field(default_factory=dict)
    traffic_light: str = "green"
    rationale: str = ""


def compute_health(root: Path, conn: sqlite3.Connection) -> HealthReport:
    """Build a HealthReport from graph.db.

    Never raises. On a SQLite read failure the report's traffic light
    is set to ``"error"`` so an operator looking at ``mareforma health``
    can distinguish a corrupted / unreadable graph from an empty one
    (which legitimately returns ``"red"`` for ``no claims recorded``).
    """
    report = HealthReport()

    try:
        from mareforma.db import list_claims, DatabaseError

        claims = list_claims(conn)
    except (sqlite3.OperationalError, sqlite3.DatabaseError, DatabaseError) as exc:
        # Read failure: surface as ``error`` rather than folding into
        # the empty-graph ``red`` state. Counters stay at zero so the
        # caller can tell the report is non-substantive.
        report.traffic_light = "error"
        report.rationale = (
            "Could not read claims table from graph.db "
            f"({type(exc).__name__}: {exc}). Run `graph.restore()` or "
            "investigate the .mareforma/ directory; this is not the "
            "same as an empty graph."
        )
        return report

    for c in claims:
        if c["status"] == "open":
            report.claims_open += 1
        else:
            report.claims_resolved += 1

        try:
            contradicts = json.loads(c.get("contradicts_json", "[]") or "[]")
        except (TypeError, ValueError):
            # Malformed JSON in a single row is per-row corruption,
            # not whole-DB corruption. Skip the contradicts accounting
            # for this row and continue producing a report for the rest.
            contradicts = []
        if contradicts:
            report.claims_contradicted += 1

        level = c.get("support_level", "PRELIMINARY")
        report.support_level_breakdown[level] = (
            report.support_level_breakdown.get(level, 0) + 1
        )

    report.traffic_light, report.rationale = _compute_traffic_light(report)
    return report


def _compute_traffic_light(report: HealthReport) -> tuple[str, str]:
    total = report.claims_open + report.claims_resolved
    if total == 0:
        return "red", "No claims recorded. Call graph.assert_claim() to start."

    established = report.support_level_breakdown.get("ESTABLISHED", 0)
    replicated = report.support_level_breakdown.get("REPLICATED", 0)
    if established + replicated == 0:
        return "yellow", "All claims are PRELIMINARY — no independent replication yet."

    return "green", "At least one independently replicated or validated claim."


# ---------------------------------------------------------------------------
# Operational event log (.mareforma/health.jsonl) + rolling-stats reader
# ---------------------------------------------------------------------------
#
# Distinct from the HealthReport snapshot above. The event log is an
# append-only JSONL trail of operational signals (provenance queries,
# grounding verdicts, DOI drift scans, refresh retries). Operators
# read rolling rates off the trail via :func:`compute_rolling_stats`
# and the ``mareforma stats`` CLI. Best-effort write — a failure to
# append is logged via a RuntimeWarning and the underlying operation
# still completes.

import datetime as _dt
import warnings as _warnings


HEALTH_LOG_FILENAME = "health.jsonl"


def _health_log_path(root: Path) -> Path:
    return Path(root) / ".mareforma" / HEALTH_LOG_FILENAME


def append_health_event(
    root: Path | str,
    op: str,
    *,
    outcome: str = "ok",
    **counters,
) -> None:
    """Append one operational event to ``.mareforma/health.jsonl``.

    Schema per line::

        {"ts": <iso8601 UTC>, "op": <str>, "outcome": <str>, ...counters}

    ``op`` is a short identifier of the operation
    (``"provenance_query"``, ``"grounding_verdict"``,
    ``"doi_drift_scan"``, ``"refresh_unresolved"``,
    ``"refresh_unsigned"``). ``outcome`` is ``"ok"`` / ``"fail"`` /
    ``"partial"``. Extra ``counters`` are merged into the JSON line
    verbatim — keep them small and JSON-encodable.

    Writes are best-effort: any permission / disk / encoding failure
    is swallowed with a RuntimeWarning so the upstream substrate
    operation always completes. The substrate's signed graph never
    depends on this log being writable.

    Encoding: ``json.dumps(allow_nan=False)`` so NaN / Infinity
    counters do not produce non-portable JSONL that breaks ``jq`` and
    browser ``JSON.parse``. Non-JSON-encodable counters
    (``datetime`` / ``set`` / ``bytes``) raise ``TypeError`` from
    ``json.dumps``; that is caught here and surfaced as a warning so
    a caller wiring a buggy emitter does not silently lose the
    upstream operation result.

    Concurrency: on POSIX (Linux + macOS), ``open(path, "a")`` uses
    ``O_APPEND`` which guarantees atomic line-sized writes up to
    ``PIPE_BUF`` (4 KB). Event lines are well under that. On
    Windows, append atomicity across processes is not guaranteed —
    operators running mareforma cross-process on Windows must
    serialise health-log writes externally.
    """
    path = _health_log_path(root)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        event = {
            "ts": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "op": op,
            "outcome": outcome,
        }
        for k, v in counters.items():
            event[k] = v
        line = json.dumps(event, sort_keys=True, allow_nan=False)
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except (OSError, TypeError, ValueError) as exc:
        _warnings.warn(
            f"health log append failed ({type(exc).__name__}: {exc}); "
            "the underlying operation still completed.",
            RuntimeWarning,
            stacklevel=2,
        )


def compute_rolling_stats(
    root: Path | str,
    *,
    last_n: int | None = None,
) -> dict:
    """Aggregate ``.mareforma/health.jsonl`` into rolling rates.

    Reads the JSONL trail (the last ``last_n`` events when given,
    otherwise the whole file) and returns a dict of per-operation
    summaries. Each summary carries the event count, ok-rate, and a
    handful of op-specific aggregates:

    * ``provenance_query`` → ``avg_depth``
    * ``grounding_verdict`` → ``avg_score`` + ``pass_rate`` (score > 0.5)
    * ``doi_drift_scan`` → ``avg_drifted`` + ``total_inspected``
    * ``refresh_unresolved`` / ``refresh_unsigned`` → ``avg_succeeded``

    Missing or malformed lines are skipped without raising; the log
    is operator-visible diagnostics, not a substrate-trust surface.
    """
    path = _health_log_path(root)
    if not path.exists():
        return {"events_total": 0, "ops": {}}
    # Bounded reads use deque so a 10 GB log + last_n=100 stays at
    # O(100) memory instead of buffering the full file before slicing.
    from collections import deque
    buffer: deque | list
    if last_n is not None and last_n > 0:
        buffer = deque(maxlen=int(last_n))
    else:
        buffer = []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    buffer.append(json.loads(line))
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
    except OSError:
        return {"events_total": 0, "ops": {}, "read_error": True}
    events = list(buffer)
    ops: dict[str, dict] = {}
    for ev in events:
        op = ev.get("op")
        if not isinstance(op, str):
            continue
        bucket = ops.setdefault(op, {
            "count": 0, "ok": 0, "fail": 0, "partial": 0,
            "_aggregates": {},
        })
        bucket["count"] += 1
        outcome = ev.get("outcome", "ok")
        if outcome in ("ok", "fail", "partial"):
            bucket[outcome] += 1
        # Op-specific aggregates.
        agg = bucket["_aggregates"]
        if op == "provenance_query":
            depth = ev.get("depth")
            if isinstance(depth, (int, float)):
                agg.setdefault("depth_sum", 0)
                agg.setdefault("depth_n", 0)
                agg["depth_sum"] += depth
                agg["depth_n"] += 1
        elif op == "grounding_verdict":
            score = ev.get("score")
            if isinstance(score, (int, float)) and score == score:
                agg.setdefault("score_sum", 0.0)
                agg.setdefault("score_n", 0)
                agg.setdefault("pass_n", 0)
                agg["score_sum"] += float(score)
                agg["score_n"] += 1
                if score > 0.5:
                    agg["pass_n"] += 1
        elif op == "doi_drift_scan":
            drifted = ev.get("drifted")
            total = ev.get("total_inspected")
            if isinstance(drifted, int):
                agg.setdefault("drifted_sum", 0)
                agg.setdefault("drifted_n", 0)
                agg["drifted_sum"] += drifted
                agg["drifted_n"] += 1
            if isinstance(total, int):
                agg.setdefault("inspected_sum", 0)
                agg["inspected_sum"] += total
        elif op in ("refresh_unresolved", "refresh_unsigned"):
            succeeded = ev.get("succeeded")
            if isinstance(succeeded, int):
                agg.setdefault("succeeded_sum", 0)
                agg.setdefault("succeeded_n", 0)
                agg["succeeded_sum"] += succeeded
                agg["succeeded_n"] += 1
    # Promote aggregates to rates.
    for op, bucket in ops.items():
        agg = bucket.pop("_aggregates")
        if op == "provenance_query" and agg.get("depth_n"):
            bucket["avg_depth"] = round(
                agg["depth_sum"] / agg["depth_n"], 3,
            )
        if op == "grounding_verdict" and agg.get("score_n"):
            bucket["avg_score"] = round(
                agg["score_sum"] / agg["score_n"], 3,
            )
            bucket["pass_rate"] = round(
                agg["pass_n"] / agg["score_n"], 3,
            )
        if op == "doi_drift_scan":
            if agg.get("drifted_n"):
                bucket["avg_drifted"] = round(
                    agg["drifted_sum"] / agg["drifted_n"], 3,
                )
            if "inspected_sum" in agg:
                bucket["total_inspected"] = agg["inspected_sum"]
        if op in ("refresh_unresolved", "refresh_unsigned"):
            if agg.get("succeeded_n"):
                bucket["avg_succeeded"] = round(
                    agg["succeeded_sum"] / agg["succeeded_n"], 3,
                )
    return {"events_total": len(events), "ops": ops}
