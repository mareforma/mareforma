"""Verdict-to-citation binding: does the verdict attest the finding's own data.

The observer computes a GROUNDED verdict against the source(s) the producer named
in ``observe(cites=...)``. Nothing in that computation checks that those cited
sources are the same data the FINDING claims to rest on. Without this gate a
producer can read ``/etc/hostname`` inside the scope, earn a signed GROUNDED, and
bind it onto a finding citing a trial dataset it never touched — the verdict is
honest about the read it saw, dishonest about the claim it is attached to.

This module is that gate. It compares the sources the verdict actually observed a
matching read for (its ``grounded_sources``, not the full declared cite set)
against the finding's own citation identifiers and reports one of three states:

- ``MATCHED``        — the verdict cites at least one source the finding cites.
                       The GROUNDED attestation is about the finding's own data.
- ``DISJOINT``       — the finding cites data, and the verdict names none of it
                       (or names nothing at all). A GROUNDED here is unbound; it
                       is downgraded to OPAQUE at bind time, or raised in strict
                       mode.
- ``NOT_APPLICABLE`` — the finding carries no citation to bind against, so there
                       is nothing to demonstrate. The verdict is kept as-is.

The comparison is PURE STRING equality over already-normalized identifiers. Both
sides are normalized exactly once, at write time (the verdict's cited set when the
scope was entered; the finding's sources when the claim is signed), and the
normalized strings are persisted inside the signed record. Read-side re-checks
(verify-on-read, ``restore``, the audit CLI) compare the STORED strings with no
filesystem access — realpath on a verifier's host would false-flag an honest
cross-host claim whose paths do not exist there.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class BindingState(str, Enum):
    """The three outcomes of a verdict↔citation binding check."""

    MATCHED = "MATCHED"
    DISJOINT = "DISJOINT"
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True)
class BindingResult:
    """The binding outcome plus the human-readable reason behind it."""

    state: BindingState
    reason: str

    @property
    def matched(self) -> bool:
        return self.state is BindingState.MATCHED

    @property
    def disjoint(self) -> bool:
        return self.state is BindingState.DISJOINT

    @property
    def not_applicable(self) -> bool:
        return self.state is BindingState.NOT_APPLICABLE


class GroundingCitationMismatchError(Exception):
    """Raised in strict mode when a verdict's cited set is disjoint from the
    finding's citation. The default (non-strict) path downgrades to OPAQUE
    instead of raising, so a cooperating-but-misconfigured producer is not
    broken mid-run — the health event names the fix.
    """


# The signed reason a disjoint downgrade carries. Frozen wording: the read side
# and the write side both emit it, and tests pin it, so it must not drift.
DISJOINT_REASON = "verdict cited-set disjoint from finding citation"

# The marker appended to a verdict that had no citation to bind against, so a
# reader tells an unexercised check from a passed one. Both write paths (the
# finding bind and the plain claim) emit it; frozen wording, same as above.
UNBOUND_ANNOTATION = "[no finding citation to bind]"


def predicate_citation_sources(predicate) -> tuple[str, ...]:
    """The bindable citation identifiers a claim's predicate declares.

    The normalized ``data_sources`` plus any content-addressed ``data_ids``.
    One rule, read by the write gate (is a GROUNDED verdict storable) and by
    both read-side re-checks (is a stored one still honest); they have to agree
    by construction or a row restore accepts is one the audit surface rejects.
    Empty for a plain claim: ``source_name`` is free text and never binds, and a
    string-fallback ``data_id`` is an opaque token the read side cannot
    reproduce. A non-string element is dropped rather than compared as itself,
    which is how a hand-edited or foreign-written row arrives.
    """
    from mareforma.trust._store import is_content_addressed

    if not isinstance(predicate, dict):
        return ()
    sources = predicate.get("data_sources") or []
    data_ids = predicate.get("data_ids") or []
    out = [s for s in sources if isinstance(s, str)]
    out += [d for d in data_ids if isinstance(d, str) and is_content_addressed(d)]
    return tuple(out)


def check_grounding_binding(
    verdict_cited: tuple[str, ...],
    finding_sources: tuple[str, ...],
) -> BindingResult:
    """Compare a verdict's cited set against a finding's citation identifiers.

    Both arguments are tuples of ALREADY-NORMALIZED identifiers (absolute paths,
    ``scheme://host/path`` URLs, or ``sha256:`` content addresses). This routine
    does pure string comparison — it never normalizes, hashes, or touches the
    filesystem — so it is safe to run on any host, including one where the cited
    paths do not exist.

    - ``finding_sources`` empty → ``NOT_APPLICABLE``: the finding cites no data,
      so a verdict cannot be bound to it. The caller keeps the verdict as-is.
    - ``finding_sources`` non-empty, ``verdict_cited`` empty → ``DISJOINT``: the
      finding rests on data but the verdict demonstrates no cited read. Binding
      is not shown; a GROUNDED here is unearned.
    - any shared identifier → ``MATCHED``.
    - otherwise → ``DISJOINT``.

    A GROUNDED verdict always carries a non-empty cited set by construction, so
    the empty-``verdict_cited`` arm mainly guards hand-built records.
    """
    finding_set = set(finding_sources)
    if not finding_set:
        return BindingResult(
            BindingState.NOT_APPLICABLE,
            "no finding citation to bind",
        )
    if not verdict_cited:
        return BindingResult(
            BindingState.DISJOINT,
            "the finding cites data but the verdict names no cited source; "
            "binding not demonstrated (pass data_source= naming the read source)",
        )
    for c in verdict_cited:
        if c in finding_set:
            return BindingResult(
                BindingState.MATCHED,
                "the verdict cites a source the finding cites",
            )
    return BindingResult(
        BindingState.DISJOINT,
        DISJOINT_REASON + " (pass data_source= naming the read source)",
    )
