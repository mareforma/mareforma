"""Kill-switch A: six API-only fixtures a correct instrument MUST catch.

Before spending on a natural-corpus run, the instrument has to prove it catches
the failures it claims to. Each fixture below builds ONE such failure with the
real observer / measurement / trust machinery — no mocks of the thing under test
— and reports whether the instrument caught it. If any one is missed, the
measurement is not yet trustworthy and the run stops before any spend. These are
seeded dissociation fixtures with known ground truth, not a prevalence estimate.

The six failures:

1. a silent zero-row fallback  — a cited read that returned nothing must not be GROUNDED;
2. an excluded partition       — a cited source that was never read is named, not hidden;
3. a same-model corroboration  — two checks on one model stay effective-independence 1;
4. a number with no execution  — a finding with no observed cited read is UNGROUNDED (empty provenance);
5. a decoy incidental read     — a non-cited read is refused as grounding;
6. an unrecognized-host model  — a model call to an arbitrary host is UNVERIFIABLE, not a distinct model.

``run_all(tmp_path)`` runs the six and returns their outcomes; the test asserts
every one was caught.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import mareforma
from mareforma.observe import ObservedGrounding, observe
from mareforma.observe._lineage import ModelLineageTier, resolve_lineage
from mareforma.observe.measure import summarize
from mareforma.trust._store import effective_independence_receipt
from tests._helpers import _bootstrap_key, _est, _pred, _prop

_CLAUDE = "claude-3-5-sonnet-20241022"  # a recognized-family COMPUTED root


@dataclass(frozen=True)
class KillSwitchOutcome:
    """One kill-switch fixture's result: what a correct instrument must show."""

    name: str
    expectation: str
    observed: str
    caught: bool


def _computed_verdict(model_id: str):
    """An OPAQUE grounding verdict carrying a COMPUTED model lineage.

    The finding path reads only the lineage off the verdict, so the grounding
    state is irrelevant here; the recognized provider host is what earns COMPUTED.
    """
    from mareforma.observe import GroundingVerdict

    lineage = resolve_lineage(
        model_id, source="socket", method="m",
        decoding={"temperature": None, "top_p": None, "seed": None},
        provider="anthropic",
    )
    return GroundingVerdict(
        grounding=ObservedGrounding.OPAQUE, reason="lineage carrier",
        model_lineage=lineage,
    )


def silent_zero_row_fallback(tmp_path: Path) -> KillSwitchOutcome:
    data = tmp_path / "ks1_empty.csv"
    data.write_text("")  # zero rows: the query "returned" nothing
    with observe(cites=str(data)) as h:
        open(str(data)).read()  # the read happens but carries nothing
    v = h.verdict
    caught = v.grounding is not ObservedGrounding.GROUNDED
    return KillSwitchOutcome(
        "silent_zero_row_fallback",
        "a zero-row cited read is never GROUNDED",
        v.grounding.value, caught,
    )


def excluded_partition(tmp_path: Path) -> KillSwitchOutcome:
    part_a = tmp_path / "ks2_part_a.csv"
    part_a.write_text("x\n1\n")
    part_b = tmp_path / "ks2_part_b.csv"
    part_b.write_text("x\n2\n")
    with observe(cites=[str(part_a), str(part_b)]) as h:
        open(str(part_a)).read()  # partition B is silently excluded
    v = h.verdict
    excluded = set(v.cited_sources) - set(v.grounded_sources)
    # The excluded partition is named even though A grounded the finding: the
    # cited-but-ungrounded set is non-empty, so the absence is legible.
    caught = bool(excluded)
    return KillSwitchOutcome(
        "excluded_partition",
        "a cited partition that was never read is named (cited minus grounded)",
        f"excluded={sorted(excluded)}", caught,
    )


def same_model_corroboration(tmp_path: Path) -> KillSwitchOutcome:
    ka = _bootstrap_key(tmp_path, "ks3_a.key")
    kb = _bootstrap_key(tmp_path, "ks3_b.key")
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=ka) as g:
        g.assert_finding(prop, pred, _est(), data_id="ks3_ds1", generated_by="ks3_r1",
                         grounding=_computed_verdict(_CLAUDE))
    with mareforma.open(tmp_path, key_path=kb) as g:
        g.assert_finding(prop, pred, _est(), data_id="ks3_ds2", generated_by="ks3_r2",
                         grounding=_computed_verdict(_CLAUDE))
        rec = effective_independence_receipt(g._conn, prop.content_id())
    # A naive signer counter sees two lines; the model-aware number stays 1.
    caught = rec["number"] == 1 and rec["naive"] == 2
    return KillSwitchOutcome(
        "same_model_corroboration",
        "two same-model checks stay effective-independence 1 (naive 2)",
        f"number={rec['number']}, naive={rec['naive']}", caught,
    )


def number_with_no_execution(tmp_path: Path) -> KillSwitchOutcome:
    data = tmp_path / "ks4.csv"
    data.write_text("x\n1\n")
    with observe(cites=str(data)) as h:
        _ = 2 + 2  # a number produced with no observed cited read: empty provenance
    v = h.verdict
    caught = v.grounding is ObservedGrounding.UNGROUNDED and len(v.reads) == 0
    return KillSwitchOutcome(
        "number_with_no_execution",
        "a number with no observed cited read is UNGROUNDED (empty provenance)",
        f"{v.grounding.value}, reads={len(v.reads)}", caught,
    )


def decoy_incidental_read(tmp_path: Path) -> KillSwitchOutcome:
    data = tmp_path / "ks5_data.csv"
    data.write_text("x\n1\n")
    decoy = tmp_path / "ks5_config.yaml"
    decoy.write_text("k: v\n")
    with observe(cites=str(data)) as h:
        open(str(decoy)).read()  # a non-cited decoy read
    v = h.verdict
    report = summarize([v])
    caught = (
        v.grounding is not ObservedGrounding.GROUNDED
        and report.incidental_reads == 1
    )
    return KillSwitchOutcome(
        "decoy_incidental_read",
        "a non-cited read is refused as grounding and flagged incidental",
        f"{v.grounding.value}, incidental={report.incidental_reads}", caught,
    )


def unrecognized_host_model(tmp_path: Path) -> KillSwitchOutcome:
    # A body-parse to an UNRECOGNIZED host (no provider): the producer chose the
    # endpoint, so the "model" field is producer-controlled and cannot mint a
    # distinct model. Even a recognized-family string stays UNVERIFIABLE.
    lineage = resolve_lineage(
        _CLAUDE, source="socket", method="m",
        decoding={"temperature": None, "top_p": None, "seed": None},
        provider=None,
    )
    caught = lineage.tier is ModelLineageTier.UNVERIFIABLE
    return KillSwitchOutcome(
        "unrecognized_host_model",
        "a model call to an arbitrary host is UNVERIFIABLE, not a distinct model",
        lineage.tier.value, caught,
    )


# The six kill-switches, in the order listed above.
KILL_SWITCHES = (
    silent_zero_row_fallback,
    excluded_partition,
    same_model_corroboration,
    number_with_no_execution,
    decoy_incidental_read,
    unrecognized_host_model,
)


def run_all(tmp_path: Path) -> list[KillSwitchOutcome]:
    """Run the six kill-switch fixtures and return their outcomes."""
    return [case(tmp_path) for case in KILL_SWITCHES]
