"""Typed errors for the trust layer.

These subclass :class:`mareforma.db.errors.MareformaError` so callers can
catch trust-layer failures with the same root as the rest of the package,
and catch each failure mode precisely rather than guessing at a bare
``ValueError``. Mirrors the existing ``IllegalStateTransitionError`` pattern.
"""
from __future__ import annotations

from mareforma.db.errors import MareformaError


class TrustError(MareformaError):
    """Base class for every trust-layer error."""


class NonFalsifiablePropositionError(TrustError):
    """A Proposition that forbids no observation was submitted.

    Raised at registration when ``direction == UNSPECIFIED`` or ``scope`` is
    empty: such an assertion is not an empirical proposition (Popper's
    demarcation, made operational), so registration refuses it and it never
    anchors a finding. (The ``Proposition`` value object can still compute a
    ``content_id``; the gate is enforced by the registration call, not the
    constructor.)
    """


class NoRegisteredPlanError(TrustError):
    """A finding was submitted against a plan that was never registered.

    Raised by :meth:`EpistemicGraph.submit_finding` when the plan_id computed
    from the supplied proposition + prediction has no ``predictions`` row.
    ``submit_finding`` requires the plan to be pre-registered first (via
    :meth:`EpistemicGraph.register_plan`) so the prediction is bound before the
    numbers are seen; the one-shot :meth:`EpistemicGraph.assert_finding` path
    registers the plan for you and never raises this.
    """


class FindingPlanForkError(TrustError):
    """A finding already exists for (content_id, data_id) under a different plan.

    The finding idempotency anchor is (content_id, data_id), which is orthogonal
    to plan_id. Re-submitting on the same dataset under a *changed* prediction
    would silently return the prior bearing and quietly drop the new decision
    rule. Rather than launder a fork, :meth:`EpistemicGraph.submit_finding`
    raises: the same dataset can stand under exactly one plan for a proposition.
    """


class InconsistentEstimateError(TrustError):
    """An EffectEstimate failed an input-consistency check.

    Examples: the confidence interval does not bracket the point estimate; a
    partial CI triple (some but not all of lower/upper/level); a p-value outside
    [0, 1]; neither a p-value nor a confidence interval was supplied; a
    superiority or equivalence gate given a CI at the wrong level. The core
    rejects inconsistent input rather than storing it.
    """
