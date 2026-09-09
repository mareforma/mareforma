"""health.py: Epistemic health report for mareforma status.

Traffic light (claim-based)
---------------------------
  green  : at least one standing claim carries a signed validation
  yellow : claims exist but none standing carries one, either because
           nobody has signed off on anything or because every claim that
           was signed off on has since been retracted or invalidated
  red    : no claims at all
  error  : graph.db could not be read (corruption, missing table, locked)

The ``error`` state is distinct from ``red``: a fresh project legitimately
has no claims yet (``red``), but a corrupted graph.db that cannot be
read at all is a different operational signal and gets its own traffic
light so operators looking at ``mareforma status`` can tell them apart.
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
    # Claims a signed contradiction verdict marked invalid, the same
    # reading as refutation_status(row)["state"] == "contradicted". It
    # is not a count of claims that assert a contradiction, and it does
    # not partition with open / resolved.
    claims_contradicted: int = 0
    # Rows carrying signed material that does not re-verify on read: an
    # envelope stapled onto another row, a signed field rewritten underneath,
    # a swapped signature. This counted promoted rows, because a stored level
    # was the unsigned word a direct writer could raise; the level is gone and
    # the tampering it stood for is not. The traffic light cannot read green
    # while it is non-zero.
    # Standing claims carrying a validation envelope: open, not invalidated
    # by a signed contradiction verdict. This counted promoted claims; the
    # signed attestation is what outlived the level they were promoted to.
    standing_validated: int = 0
    # Rows carrying signed material that does not re-verify on read: an
    # envelope stapled onto another row, a signed field rewritten underneath,
    # a swapped signature. The traffic light cannot read green while it is
    # non-zero.
    failed_verification: int = 0
    # A stored project-policy row whose root signature no longer backs it. Every
    # enforcement then reads the fail-closed strictest policy (witnessing and
    # strict promotion both required, dated before every claim), so promotions
    # and restores refuse with no visible cause. Surfaced here so an operator
    # meets the tampered policy on `mareforma status` rather than inferring it
    # from a refusal. A project that never declared a policy reports False.
    policy_unverified: bool = False
    traffic_light: str = "green"
    rationale: str = ""


def compute_health(conn: sqlite3.Connection) -> HealthReport:
    """Build a HealthReport from graph.db.

    Never raises. On a SQLite read failure the report's traffic light
    is set to ``"error"`` so an operator looking at ``mareforma status``
    can distinguish a corrupted / unreadable graph from an empty one
    (which legitimately returns ``"red"`` for ``no claims recorded``).
    """
    report = HealthReport()

    try:
        from mareforma.db import DatabaseError

        # One grouped pass, not a materialised table. The census is five
        # integers, and reading every row's text, signature bundle and
        # payloads into Python to add them up costs memory proportional to
        # the stored findings. ``t_invalid IS NOT NULL`` is the same test
        # refutation_status applies for its ``contradicted`` state, so the
        # word keeps one meaning across both surfaces.
        rows = conn.execute(
            "SELECT COUNT(*) AS n, "
            "SUM(status = 'open') AS n_open, "
            "SUM(t_invalid IS NOT NULL) AS n_contradicted, "
            "SUM(validation_signature IS NOT NULL "
            "    AND status = 'open' AND t_invalid IS NULL) AS n_standing "
            "FROM claims"
        ).fetchall()
    except (sqlite3.OperationalError, sqlite3.DatabaseError, DatabaseError) as exc:
        # Read failure: surface as ``error`` rather than folding into
        # the empty-graph ``red`` state. Counters stay at zero so the
        # caller can tell the report is non-substantive.
        report.traffic_light = "error"
        report.rationale = (
            "Could not read claims table from graph.db "
            f"({type(exc).__name__}: {exc}). Run `mareforma restore` "
            "(or `mareforma.restore(project_root)`) or "
            "investigate the .mareforma/ directory; this is not the "
            "same as an empty graph."
        )
        return report

    for r in rows:
        report.claims_open += r["n_open"] or 0
        report.claims_resolved += (r["n"] or 0) - (r["n_open"] or 0)
        report.claims_contradicted += r["n_contradicted"] or 0
        # A retracted or verdict-invalidated claim is no longer evidence of
        # anything, so it does not count as standing.
        report.standing_validated += r["n_standing"] or 0

    # Re-verify the rows carrying signed material, so a graph whose
    # signatures no longer check out cannot read green. Kept apart from the
    # grouped census: that counts rows, this counts the ones whose signed
    # material does not back what the row says.
    try:
        from mareforma.db import DatabaseError, count_unverified_rows

        report.failed_verification = count_unverified_rows(conn)
    except (sqlite3.OperationalError, sqlite3.DatabaseError, DatabaseError) as exc:
        report.traffic_light = "error"
        report.rationale = (
            "Could not re-verify the signed claims in graph.db "
            f"({type(exc).__name__}: {exc}). Run `mareforma restore` "
            "(or `mareforma.restore(project_root)`) or "
            "investigate the .mareforma/ directory; this is not the "
            "same as an empty graph."
        )
        return report

    # A stored policy whose envelope no longer binds reads maximally strict on
    # every enforcement; name it here so the stall is visible on status rather
    # than met only as a refused promotion or restore. A graph without the
    # project_policy table (an older schema) simply has no policy to stall, so a
    # read failure here is not substantive and leaves the flag False.
    try:
        from mareforma.db import project_policy_unverified

        report.policy_unverified = project_policy_unverified(conn)
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc):
            # OperationalError also covers a locked database, a disk I/O error
            # and a missing column, none of which mean "there is no policy".
            # Narrowing on the class alone left those reporting no stall, which
            # is the direction this whole branch exists to stop.
            report.policy_unverified = False
            report.traffic_light = "error"
            report.rationale = (
                f"the project policy could not be read ({type(exc).__name__}: "
                f"{exc}), so whether a policy is stalled is unknown, not answered"
            )
            return report
        # The narrow case this tolerance was written for: an older schema with
        # no project_policy table, where there is no policy to stall.
        report.policy_unverified = False
    except (sqlite3.DatabaseError, DatabaseError) as exc:
        # Everything else. DatabaseError is the base class of nearly every
        # sqlite failure including corruption, so swallowing it here reported
        # "no policy stall" for a graph that could not be read at all, while the
        # three sibling reads above set traffic_light = "error" on the same
        # exception. A status command that cannot read the policy must say so.
        report.policy_unverified = False
        report.traffic_light = "error"
        report.rationale = (
            f"the project policy could not be read ({type(exc).__name__}: "
            f"{exc}), so whether a policy is stalled is unknown, not answered"
        )
        return report

    report.traffic_light, report.rationale = _compute_traffic_light(report)
    return report


def _compute_traffic_light(report: HealthReport) -> tuple[str, str]:
    light, rationale = _claim_census_light(report)

    # A stored policy whose signature no longer backs it forces every rule to the
    # strictest reading, so promotions and restores refuse without a visible
    # cause. That bars green. It is orthogonal to the claim census, so it is
    # layered on top rather than folded into the cascade: a green project turns
    # yellow, and a project already flagged for a different reason (a forged
    # promotion, say) keeps that reason and gains this one, so neither is masked.
    if report.policy_unverified:
        policy = (
            "The project policy is present but its root signature does not "
            "verify: enforcement has fallen back to the strictest reading "
            "(witnessing and strict promotion both required, dated before every "
            "claim). Re-sign the policy with the project root or restore from a "
            "clean backup."
        )
        if light == "green":
            return "yellow", policy
        return light, f"{rationale} {policy}"

    return light, rationale


def _claim_census_light(report: HealthReport) -> tuple[str, str]:
    """The traffic light from the claim census alone (no policy overlay)."""
    total = report.claims_open + report.claims_resolved
    if total == 0:
        return "red", "No claims recorded. Call graph.assert_claim() to start."

    if report.standing_validated == 0:
        return "yellow", (
            "No claim carries a validation a human signed, or every claim that "
            "did has been retracted or invalidated by a signed contradiction "
            "verdict. Trust is read off the derived axes; this light only says "
            "whether anyone has signed off on anything."
        )

    # A row whose signed material does not check out bars green: the count says
    # the project has standing evidence, and at least one piece of it does not
    # re-verify, so it is not evidence of anything.
    if report.failed_verification > 0:
        return "yellow", (
            f"{report.failed_verification} claim(s) do not re-verify on read: "
            "an envelope stapled onto another row, a signed field rewritten "
            "underneath, or a swapped signature. Run "
            "`mareforma verify <claim_id>` to see which, then retract or repair."
        )

    return "green", "At least one standing claim carries a signed validation."


# ---------------------------------------------------------------------------
# Operational event log (.mareforma/health.jsonl) + rolling-stats reader
# ---------------------------------------------------------------------------
#
# Distinct from the HealthReport snapshot above. The event log is an
# append-only JSONL trail of operational signals (provenance queries,
# grounding verdicts, DOI drift scans, refresh retries). Operators
# read rolling rates off the trail via :func:`compute_rolling_stats`
# and the ``mareforma activity`` CLI. Best-effort write, a failure to
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
    ``"refresh_unsigned"``). ``outcome`` is ``"ok"`` / ``"fail"`` /
    ``"partial"``. Extra ``counters`` are merged into the JSON line
    verbatim: keep them small and JSON-encodable.

    Writes are best-effort: any permission / disk / encoding failure
    is swallowed with a RuntimeWarning so the upstream mareforma
    operation always completes. Mareforma's signed graph never
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
    Windows, append atomicity across processes is not guaranteed;
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
    * ``refresh_unsigned`` → ``avg_succeeded``

    Missing or malformed lines are skipped without raising; the log
    is operator-visible diagnostics, not a mareforma-trust surface.
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
    malformed_lines = 0
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except (json.JSONDecodeError, TypeError, ValueError):
                    malformed_lines += 1
                    continue
                # Valid JSON that is not an object (a scalar or list) is
                # malformed for aggregation: the loop below reads ``ev.get``.
                if not isinstance(obj, dict):
                    malformed_lines += 1
                    continue
                buffer.append(obj)
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
        elif op == "refresh_unsigned":
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
        if op == "refresh_unsigned":
            if agg.get("succeeded_n"):
                bucket["avg_succeeded"] = round(
                    agg["succeeded_sum"] / agg["succeeded_n"], 3,
                )
    return {
        "events_total": len(events),
        "malformed_lines": malformed_lines,
        "ops": ops,
    }
