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

REFUTED / CONTESTED are derived labels, not auto-refutation: a REFUTED status
means "no surviving independent support," not "this proposition is false."
"""
from __future__ import annotations

from enum import Enum

# The status-policy version, independent of the package version. It bumps only
# when the status computation itself changes, not on every release. A finding's
# Status carries the policy that computed it, so a later policy change stays
# identifiable on old rows.
STATUS_POLICY = "status_policy@v3"


class Status(str, Enum):
    UNTESTED = "UNTESTED"
    PRELIMINARY = "PRELIMINARY"
    CORROBORATED = "CORROBORATED"
    REFUTED = "REFUTED"
    CONTESTED = "CONTESTED"


class FrameStatus(str, Enum):
    CONSISTENT = "consistent"
    CONTESTED = "contested"


def compute_status(independent_support: int, independent_refute: int) -> Status:
    """The deterministic state machine.

    - UNTESTED:     no supporting or refuting lines.
    - CONTESTED:    independent support AND independent refute on the same
                    proposition.
    - REFUTED:      >= 1 independent refute, 0 independent support.
    - CORROBORATED: >= 2 independent support, 0 independent refute.
    - PRELIMINARY:  exactly 1 independent support, 0 independent refute.
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
        return Status.CORROBORATED
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
