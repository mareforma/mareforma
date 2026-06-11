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


class InconsistentEstimateError(TrustError):
    """An EffectEstimate failed an input-consistency check.

    Examples: the confidence interval does not bracket the point estimate; a
    partial CI triple (some but not all of lower/upper/level); a p-value outside
    [0, 1]; neither a p-value nor a confidence interval was supplied; a
    superiority or equivalence gate given a CI at the wrong level. The core
    rejects inconsistent input rather than storing it.
    """
