"""status.py: derived Status (replaces the count and the dead trust ladder).

Status is a function of independent-line counts on a single ``content_id``, not
an assertion of truth and not a human gate. It is a versioned *policy* over
durable stored inputs (stamped :data:`STATUS_POLICY`), never baked into the
schema. Improving the rule later is a new policy over the same data, not a
migration.

Independence is a distinct-signer heuristic, not proof: two supporting lines
count as independent support iff they come from different signers (the claim's
``asserter_keyid``) AND different datasets (``data_id``). This is the same WHO
axis the REPLICATED promotion query keys on, so promotion and trust counting
agree by construction. One signer contributes at most one independent support
(so a single signer cannot self-certify) and at most one independent refute;
re-running the identical dataset under a new signer adds nothing. Legacy lines
whose claim predates the keyid column fall back to the retired ``generated_by``
run axis, so their counts are preserved rather than collapsed. A **refute line**
is an evidence line whose recomputed ``Bearing.direction == refutes``. The counts
the state machine reads are ``independent_support`` and ``independent_refute``,
both computed signer-distinct (see
:func:`mareforma.trust._store.independence_counts`).

CONVERGENT is a convergence marker, not a corroboration or independence verdict:
two or more independent-lineage supporting lines converge. It states the
structural fact and no more. Cross-model error correlation is unmodeled and is
the named residual: distinct-model is necessary, not sufficient, for
independence (a kill-switch measured distinct-provider model pairs as
error-correlated as any pair), so the map does not translate a convergence
marker into the word "independent".

REFUTED / CONTESTED are derived labels, not auto-refutation: a REFUTED status
means "no surviving independent support," not "this proposition is false."
"""
from __future__ import annotations

from enum import Enum, EnumMeta

# The status-policy version, independent of the package version. It bumps only
# when the status computation itself changes, not on every release. A finding's
# Status carries the policy that computed it, so a later policy change stays
# identifiable on old rows. Status is recomputed on read and never stored in the
# schema, so a vocabulary change is a new policy over the same counts, not a
# migration. v4 renamed the top label CORROBORATED to the convergence marker
# CONVERGENT; the counting rule is unchanged.
STATUS_POLICY = "status_policy@v4"

# Retired status label -> its live replacement. CORROBORATED named a
# corroboration/independence verdict, but distinct-model is necessary, not
# sufficient, for independence, so the word over-claimed. The old name keeps
# resolving for one release (by value and by attribute) and warns; a future
# release removes it.
_RETIRED_STATUS_ALIASES = {"CORROBORATED": "CONVERGENT"}


def _warn_retired_status(old: str, new: str) -> None:
    import warnings as _warnings

    _warnings.warn(
        f"Status.{old} is retired: it named a corroboration/independence "
        f"verdict, but distinct-model is necessary, not sufficient, for "
        f"independence. Use Status.{new}, a convergence marker for two or more "
        f"lineage-distinct supporting lines converging. This alias resolves "
        f"this release and is removed in a future release.",
        DeprecationWarning,
        stacklevel=3,
    )


class _StatusMeta(EnumMeta):
    """Resolve a retired status member name (attribute access) with a warning.

    ``Status.CORROBORATED`` returns :attr:`Status.CONVERGENT` and warns; every
    other missing attribute stays an ``AttributeError`` so a typo is not
    swallowed. Value lookup (``Status("CORROBORATED")``) is handled by
    :meth:`Status._missing_`.
    """

    def __getattr__(cls, name: str):
        new = _RETIRED_STATUS_ALIASES.get(name)
        if new is not None:
            _warn_retired_status(name, new)
            return cls[new]
        return super().__getattr__(name)


class Status(str, Enum, metaclass=_StatusMeta):
    UNTESTED = "UNTESTED"
    PRELIMINARY = "PRELIMINARY"
    CONVERGENT = "CONVERGENT"
    REFUTED = "REFUTED"
    CONTESTED = "CONTESTED"

    @classmethod
    def _missing_(cls, value: object) -> "Status | None":
        """Resolve a retired status value string with a deprecation warning.

        Keeps ``Status("CORROBORATED")`` working for one release, mapping it to
        :attr:`Status.CONVERGENT`; anything else stays an unresolved value
        (``ValueError``).
        """
        if isinstance(value, str):
            new = _RETIRED_STATUS_ALIASES.get(value)
            if new is not None:
                _warn_retired_status(value, new)
                return cls[new]
        return None


class FrameStatus(str, Enum):
    CONSISTENT = "consistent"
    CONTESTED = "contested"


def compute_status(independent_support: int, independent_refute: int) -> Status:
    """The deterministic state machine.

    - UNTESTED:     no supporting or refuting lines.
    - CONTESTED:    independent support AND independent refute on the same
                    proposition.
    - REFUTED:      >= 1 independent refute, 0 independent support.
    - CONVERGENT:   >= 2 independent-lineage supporting lines converge, 0
                    independent refute. A convergence marker, not a
                    corroboration or independence verdict: cross-model error
                    correlation is unmodeled and is the named residual.
    - PRELIMINARY:  exactly 1 independent support, 0 independent refute.

    The counts are over LIVE claims only: a claim that was retracted or
    contested, or that a signed contradiction verdict invalidated, contributes no
    line in either direction (see
    :func:`mareforma.trust._store.independence_counts`). Withdrawal is therefore
    symmetric: retracting a supporting finding can move a proposition off
    CONVERGENT, and retracting a refuting one can move it off REFUTED, back to
    the state the surviving evidence supports.
    """
    if independent_support < 0 or independent_refute < 0:
        raise ValueError("independence counts must be non-negative")

    if independent_support == 0 and independent_refute == 0:
        return Status.UNTESTED
    if independent_support >= 1 and independent_refute >= 1:
        return Status.CONTESTED
    if independent_refute >= 1:  # and independent_support == 0
        return Status.REFUTED
    if independent_support >= 2:  # and independent_refute == 0
        return Status.CONVERGENT
    return Status.PRELIMINARY  # exactly 1 support, 0 refute


def compute_frame_status(contrary_independent_support: int) -> FrameStatus:
    """A proposition's frame is contested when a contrary proposition in the
    same frame has at least one independent supporting line.

    Count-only for now (no weight comparison). The frame status is computed
    at retrieval; it does not mutate either proposition's own Status and does
    not silently corroborate either side, it only surfaces the contest.
    """
    if contrary_independent_support < 0:
        raise ValueError("contrary support count must be non-negative")
    return (
        FrameStatus.CONTESTED
        if contrary_independent_support >= 1
        else FrameStatus.CONSISTENT
    )
