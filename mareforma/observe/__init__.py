"""Execution-observed grounding: compute whether real data flowed into a finding.

The public surface of the observer. Wrap the span that authors a finding in
``observe(...)``; inside it, wrapped loaders and a PEP-578 audit hook record
what data actually flowed. On exit the observer computes a
:class:`GroundingVerdict` — ``GROUNDED``, ``UNGROUNDED``, or ``OPAQUE`` — from
what it saw, never from what the producer declared. Pass the verdict to
``assert_finding(..., grounding=verdict)`` to bind it into the signed envelope.

    with mareforma.observe(cites="/data/trial.csv") as obs:
        frame = pandas.read_csv("/data/trial.csv")
        estimate = analyze(frame)
    # author outside the scope, then sign:
    graph.assert_finding(prop, pred, estimate, data_id=..., grounding=obs.verdict)

The verdict is computed from execution of a COOPERATING producer: the binding is
tamper-evidence over what a cooperating run did, not a proof against an
adversarial operator. A finding must be AUTHORED inside the scope and SIGNED
after it closes — asserting a claim while the grounding scope is still open is a
sign-after-author violation and raises.
"""
from __future__ import annotations

from contextlib import contextmanager

from . import _audit, _loaders, _scope, measure, oracle
from ._citation import cited_set
from ._scope import current_scope, scope_is_open
from ._verdict import (
    GROUNDING_AXIS_VERSION,
    GroundingVerdict,
    ObservedGrounding,
    ReadRecord,
    SeamEvent,
)
from .measure import GroundingReport, summarize
from .oracle import (
    OracleInfluence,
    OracleResult,
    Reconciliation,
    perturbation_oracle,
    reconcile,
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
        read while the span is still open — reading it early would classify an
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
        The source(s) the finding cites — a path, a URL, a ``sha256:`` data_id,
        or an iterable of these. A read GROUNDS the finding only when it matches
        one of these; an incidental read (config, tokenizer, cache) does not.
    content_address:
        Opt in to content-address matching: a read matches a cited ``sha256:``
        data_id when the hash of its returned bytes equals it. Off by default,
        because identifier matching avoids hashing large reads on the common
        path.

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
            handle._verdict = GroundingVerdict(
                grounding=ObservedGrounding.OPAQUE,
                reason=(
                    "verdict computation failed during scope teardown: "
                    f"{type(exc).__name__}"
                ),
            )


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
    # Causal oracle (independent influence ground truth).
    "perturbation_oracle",
    "OracleInfluence",
    "OracleResult",
    "reconcile",
    "Reconciliation",
    # Aggregate measurement over many verdicts.
    "summarize",
    "GroundingReport",
]
