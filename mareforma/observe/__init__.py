"""Execution-observed grounding: compute whether real data flowed into a finding.

The public surface of the observer. Wrap the span that authors a finding in
``observe(...)``; inside it, wrapped loaders and a PEP-578 audit hook record
what data actually flowed. On exit the observer computes a
:class:`GroundingVerdict`, ``GROUNDED``, ``UNGROUNDED``, or ``OPAQUE``, from
what it saw, never from what the producer declared. Pass the verdict to
``assert_finding(..., grounding=verdict)`` to bind it into the signed envelope.

    from mareforma.observe import observe

    with observe(cites="/data/trial.csv") as obs:
        frame = pandas.read_csv("/data/trial.csv")
        estimate = analyze(frame)
    # author outside the scope, then sign:
    graph.assert_finding(prop, pred, estimate, data_id=..., grounding=obs.verdict)

Only a verdict this observer computed can write the observed axis. A record a
caller builds by hand, whether a dict or an instance of
:class:`GroundingVerdict`, is stored and reported as DECLARED and can never
occupy GROUNDED on the ASSERT path (see :func:`._verdict.declared_record`).

Two bounds on that sentence, both measured, neither closed:

* it is the assert path only. :func:`mareforma.restore` writes the record
  straight from ``claims.toml``, so a producer can export a neutralised record,
  edit it, re-sign it with its own key and restore it as GROUNDED, and
  ``mareforma verify`` then exits 0 on it;
* "computed" means the observer's classifier ran, not that the data moved. The
  scope's recorder is reachable through this module's own exports, so in-process
  code can have the observer mint a GROUNDED for a read that never happened. The
  boundary here is the process, not the caller's honesty inside it.

The verdict is computed from execution of a COOPERATING producer: the binding is
tamper-evidence over what a cooperating run did, not a proof against an
adversarial operator. A finding must be AUTHORED inside the scope and SIGNED
after it closes, asserting a claim while the grounding scope is still open is a
sign-after-author violation and raises.
"""
from __future__ import annotations

from contextlib import contextmanager

from . import _audit, _doctor, _loaders, _scope, measure, oracle, scrambles
from ._binding import (
    BindingResult,
    BindingState,
    GroundingCitationMismatchError,
    check_grounding_binding,
)
from ._citation import cited_set
from ._doctor import coverage_report
from ._lineage import ModelLineage, ModelLineageTier
from ._scope import current_scope, scope_is_open
from ._verdict import (
    GROUNDING_AXIS_VERSION,
    GroundingVerdict,
    ObservedGrounding,
    ReadRecord,
    SeamEvent,
)
from ._verdict import _mint as _verdict_mint
from .measure import (
    GroundingAxisMismatchError,
    GroundingReport,
    IndependenceReport,
    InfluenceReport,
    PilotReport,
    independence_records,
    influence_records,
    summarize,
    summarize_independence,
    summarize_independence_receipts,
    summarize_influence,
    summarize_influence_receipts,
    summarize_pilot,
    summarize_receipts,
)
from .oracle import (
    MetricReducer,
    NotTestedReason,
    OracleInfluence,
    OracleResult,
    ReconcileResult,
    Reconciliation,
    declared_reducer,
    numeric_extraction_reducer,
    perturbation_oracle,
    reconcile,
    scalar_reducer,
)
from .scrambles import Scramble, scramble_family


def declare_model(
    model,
    *,
    method=None,
    temperature=None,
    top_p=None,
    seed=None,
    provider=None,
):
    """Declare the model behind the current scope (the PROXY lineage tier).

    A cooperating producer whose model call does not route through a wrapped
    ``httpx`` POST (a custom SDK, a batching layer) declares it here. A
    declaration is always agent-attested, so it is PROXY, never COMPUTED, which
    only a body-parse at the socket seam earns. A declared model whose base is
    not declarable (a hosted fine-tune, a moving alias) is UNVERIFIABLE. A no-op
    outside a scope, like every other recording chokepoint.
    """
    scope = current_scope()
    if scope is None:
        return
    from ._lineage import resolve_lineage

    scope.record_model(
        resolve_lineage(
            model,
            source="declared",
            method=method,
            decoding={"temperature": temperature, "top_p": top_p, "seed": seed},
            provider=provider,
        )
    )


class ScopeNotClosedError(RuntimeError):
    """Raised when a verdict is read before its ``observe()`` block has closed."""


class ObserveHandle:
    """The object yielded by ``observe()``. Carries the verdict after exit."""

    def __init__(self, scope: "_scope.Scope"):
        self._scope = scope
        self._verdict: "GroundingVerdict | None" = None

    @property
    def verdict(self) -> "GroundingVerdict":
        """The computed verdict. Available only after the ``with`` block closes.

        The verdict is a function of the whole observed span, so it cannot be
        read while the span is still open, reading it early would classify an
        incomplete observation. Access it after the block.
        """
        if self._verdict is None:
            raise ScopeNotClosedError(
                "the grounding verdict is only available after the observe() "
                "block closes; read `handle.verdict` after the `with` statement"
            )
        return self._verdict


@contextmanager
def observe(cites=None, *, content_address: bool = False):
    """Open an observation scope over the code that authors a finding.

    Parameters
    ----------
    cites:
        The source(s) the finding cites, a path, a URL, a ``sha256:`` data_id,
        or an iterable of these. A read GROUNDS the finding only when it matches
        one of these; an incidental read (config, tokenizer, cache) does not.
    content_address:
        Opt in to content-address matching: a read matches a cited ``sha256:``
        data_id when the hash of its returned bytes equals it. Off by default,
        because identifier matching avoids hashing large reads on the common
        path. With hashing off no read can ever match a ``sha256:`` citation,
        so such a finding floors to OPAQUE (a named coverage gap), never a
        confident UNGROUNDED.

    Yields
    ------
    ObserveHandle
        Whose ``.verdict`` holds the :class:`GroundingVerdict` after the block.
    """
    _audit.ensure_installed()
    _loaders.ensure_installed()
    _loaders.refresh_third_party()
    scope = _scope.enter(cited_set(cites), content_address=content_address)
    handle = ObserveHandle(scope)
    try:
        yield handle
    finally:
        _scope.exit(scope)
        try:
            handle._verdict = scope.classify()
        except BaseException as exc:  # noqa: BLE001
            # Verdict computation is pure and should not raise, but if it ever
            # does it must not supplant an exception propagating from the
            # with-body. Degrade to an honest OPAQUE rather than mask the error.
            # Minted like any other verdict the observer produced: it promotes
            # nothing, and leaving it unminted would report the observer's own
            # failure as a caller's declaration.
            handle._verdict = GroundingVerdict(
                grounding=ObservedGrounding.OPAQUE,
                reason=(
                    "verdict computation failed during scope teardown: "
                    f"{type(exc).__name__}"
                ),
            )
            _verdict_mint(handle._verdict)


__all__ = [
    "observe",
    "ObserveHandle",
    "ScopeNotClosedError",
    "GroundingVerdict",
    "ObservedGrounding",
    "ReadRecord",
    "SeamEvent",
    "GROUNDING_AXIS_VERSION",
    "current_scope",
    "scope_is_open",
    # Model/method lineage captured at the call boundary.
    "declare_model",
    "ModelLineage",
    "ModelLineageTier",
    # Coverage self-report (the doctor).
    "coverage_report",
    # Verdict↔citation binding.
    "check_grounding_binding",
    "BindingResult",
    "BindingState",
    "GroundingCitationMismatchError",
    # Causal oracle (independent influence ground truth).
    "perturbation_oracle",
    "OracleInfluence",
    "OracleResult",
    "NotTestedReason",
    "scramble_family",
    "Scramble",
    "reconcile",
    "ReconcileResult",
    "Reconciliation",
    # Declared metric reducer (prose findings need a stated reduction).
    "MetricReducer",
    "scalar_reducer",
    "declared_reducer",
    "numeric_extraction_reducer",
    # Aggregate measurement over many verdicts.
    "summarize",
    "GroundingReport",
    "summarize_receipts",
    "GroundingAxisMismatchError",
    # The independence arm of the measurement.
    "IndependenceReport",
    "summarize_independence",
    "summarize_independence_receipts",
    "independence_records",
    # The influence arm of the measurement (does the finding depend on its data).
    "InfluenceReport",
    "summarize_influence",
    "summarize_influence_receipts",
    "influence_records",
    # The slim natural-prevalence pilot (three arms + the OPAQUE-coverage bound).
    "PilotReport",
    "summarize_pilot",
]
