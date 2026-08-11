"""The kill-switch fixtures: six seeded failures a correct instrument MUST catch.

Four of the six need only the observer and the filesystem, so they ship in the
wheel under :mod:`mareforma.selfcheck` and are re-exported here. The other two are
model-axis fixtures that stay in the test suite: they drive a real ``httpx.Client``
call through the observer's socket seam, and only ``pytest_httpx`` leaves a genuine
network transport in place (an offline ``MockTransport`` tiers PROXY and a
non-provider host tiers UNVERIFIABLE), so no substitution preserves their outcome.

``run_all(tmp_path, httpx_mock)`` runs all six and returns their outcomes; the test
asserts every one was caught.

The six failures:

1. a silent zero-row fallback , a cited read that returned nothing is seen, never GROUNDED;
2. an excluded partition      , a cited source that was never read is named, not hidden;
3. a same-model corroboration , two checks on one model stay effective-independence 1;
4. a number with no execution , a finding with no observed cited read is UNGROUNDED (empty provenance);
5. a decoy incidental read    , a non-cited read is refused as grounding;
6. an unrecognized-host model , a model call to an arbitrary host is UNVERIFIABLE, not a distinct model.
"""
from __future__ import annotations

from pathlib import Path

import httpx

import mareforma
from mareforma.observe._lineage import ModelLineageTier
from mareforma.selfcheck import (
    KillSwitchOutcome,
    decoy_incidental_read,
    excluded_partition,
    number_with_no_execution,
    silent_zero_row_fallback,
)
from mareforma.trust._store import effective_independence_receipt
from tests._helpers import _bootstrap_key, _enroll_key, _est, _pred, _prop

_CLAUDE = "claude-3-5-sonnet-20241022"  # a recognized-family COMPUTED root
_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"  # a recognized provider host
_ARBITRARY_URL = "https://producer-chosen.example/v1/messages"  # an unrecognized host


def register_model_responses(httpx_mock) -> None:
    """Register canned 2xx bodies for the two model-axis host URLs.

    ``pytest_httpx`` patches ``HTTPTransport.handle_request`` but leaves a real
    ``httpx.HTTPTransport`` on the client, so a plain ``httpx.Client()`` reads as a
    genuine network transport at the seam (unlike an offline ``MockTransport``,
    which the observer classifies as a producer-controlled declaration and tiers
    PROXY). That is what lets the same-model call earn COMPUTED and the arbitrary
    host earn UNVERIFIABLE, both through the observer's own host recognition.
    """
    httpx_mock.add_response(url=_ANTHROPIC_URL, json={}, is_reusable=True)
    httpx_mock.add_response(url=_ARBITRARY_URL, json={}, is_reusable=True)


def _observed_grounding(client: httpx.Client, url: str, csv_path: Path):
    """Drive one ``observe()`` scope: a real socket model call plus a cited read.

    The observer derives the provider from the request host and parses the model
    off the POST body itself, so the model lineage on the verdict is COMPUTED (or
    UNVERIFIABLE for an unrecognized host) because the seam earned it, not because
    a fixture handed the provider in.
    """
    from mareforma.observe import observe

    with observe(cites=str(csv_path)) as h:
        client.post(
            url,
            json={
                "model": _CLAUDE,
                "temperature": 0.0,
                "messages": [{"role": "user", "content": "analyze"}],
            },
        )
        open(str(csv_path)).read()
    return h.verdict


def same_model_corroboration(tmp_path: Path) -> KillSwitchOutcome:
    ka = _bootstrap_key(tmp_path, "ks3_a.key")
    kb = _bootstrap_key(tmp_path, "ks3_b.key")
    # kb is enrolled under the root so its observed lineage authenticates on
    # read; only a verified line joins the naive count that the collapse acts on.
    _enroll_key(tmp_path, ka, kb)
    data_a = tmp_path / "ks3_a.csv"
    data_a.write_text("x\n1\n")
    data_b = tmp_path / "ks3_b.csv"
    data_b.write_text("x\n2\n")
    prop, pred = _prop(), _pred()
    client = httpx.Client()
    try:
        # Both checks run the SAME model through the real socket seam, so the
        # observer mints each COMPUTED lineage from the recognized host itself.
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(prop, pred, _est(), data_id="ks3_ds1", generated_by="ks3_r1",
                             grounding=_observed_grounding(client, _ANTHROPIC_URL, data_a))
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(prop, pred, _est(), data_id="ks3_ds2", generated_by="ks3_r2",
                             grounding=_observed_grounding(client, _ANTHROPIC_URL, data_b))
            rec = effective_independence_receipt(g._conn, prop.content_id())
    finally:
        client.close()
    # A naive signer counter sees two lines; the model-aware number stays 1.
    caught = rec["number"] == 1 and rec["naive"] == 2
    return KillSwitchOutcome(
        "same_model_corroboration",
        "two same-model checks stay effective-independence 1 (naive 2)",
        f"number={rec['number']}, naive={rec['naive']}", caught,
    )


def unrecognized_host_model(tmp_path: Path) -> KillSwitchOutcome:
    # A body-parse to an UNRECOGNIZED host: the producer chose the endpoint, so the
    # "model" field is producer-controlled and cannot mint a distinct model. The
    # call goes through the real socket seam, so the observer derives no provider
    # from the arbitrary host itself and tiers the lineage UNVERIFIABLE, even for
    # a recognized-family string.
    data = tmp_path / "ks6.csv"
    data.write_text("x\n1\n")
    client = httpx.Client()
    try:
        verdict = _observed_grounding(client, _ARBITRARY_URL, data)
    finally:
        client.close()
    lineage = verdict.model_lineage
    caught = lineage is not None and lineage.tier is ModelLineageTier.UNVERIFIABLE
    observed = lineage.tier.value if lineage is not None else "no-lineage"
    return KillSwitchOutcome(
        "unrecognized_host_model",
        "a model call to an arbitrary host is UNVERIFIABLE, not a distinct model",
        observed, caught,
    )


# The six kill-switches, in the order listed above: four shipped self-checks
# plus the two model-axis fixtures that stay in the suite.
KILL_SWITCHES = (
    silent_zero_row_fallback,
    excluded_partition,
    same_model_corroboration,
    number_with_no_execution,
    decoy_incidental_read,
    unrecognized_host_model,
)


def run_all(tmp_path: Path, httpx_mock) -> "list[KillSwitchOutcome]":
    """Run the six kill-switch fixtures and return their outcomes.

    The two model-axis fixtures drive real ``httpx.Client()`` calls through the
    observer's socket seam, so ``httpx_mock`` supplies their canned provider
    responses (see :func:`register_model_responses`).
    """
    register_model_responses(httpx_mock)
    return [case(tmp_path) for case in KILL_SWITCHES]
