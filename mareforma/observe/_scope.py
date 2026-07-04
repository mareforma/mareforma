"""The observed scope: the contextvar-held window in which reads are attributed.

A scope is the span between ``with observe(...) as obs:`` and its exit. Inside
it, wrapped loaders record the reads they see and the audit hook records seams
and opens. On exit the scope is frozen and :meth:`Scope.classify` computes the
verdict from what was captured. Nothing about the verdict is declared; it is a
function of the recorded evidence.

Scope is held in a :class:`contextvars.ContextVar`, not thread-local, so it
propagates correctly into ``asyncio`` tasks (the event loop copies the context
into each task). It deliberately does NOT reach library-spawned threads or a
pipeline that owns its own loop. Work handed off inside the scope — a thread
started (``Thread.start``) or a thread-pool submit/map — is caught at the
hand-off and recorded as a seam, so an unseeable read there becomes ``OPAQUE``
rather than a confident false ``UNGROUNDED``. A resource opened BEFORE the scope
and reused inside it (a module-level handle or pooled connection) stays the
documented coverage bound described in ``_loaders``: its reads are invisible.
"""
from __future__ import annotations

from contextvars import ContextVar

from ._citation import normalize_identifier, read_matches_citation
from ._verdict import GroundingVerdict, ObservedGrounding, ReadRecord, SeamEvent

# The active scope for the current context. ``None`` means nothing is observing;
# every wrapper and the audit hook short-circuit on that in a single read.
_active: "ContextVar[Scope | None]" = ContextVar(
    "mareforma_observe_scope", default=None
)


def current_scope() -> "Scope | None":
    """The scope active in the current context, or None. The hot-path predicate."""
    return _active.get()


def scope_is_open() -> bool:
    """True iff a scope is currently open in this context.

    Used by the assert path to enforce the sign-after-author invariant: a claim
    must be authored inside a scope and signed AFTER it closes, never while the
    scope that grounds it is still open.
    """
    return _active.get() is not None


class Scope:
    """A single observation window. Mutable while open, frozen after exit."""

    def __init__(self, cited: tuple[str, ...], *, content_address: bool = False):
        self.cited = cited
        self.content_address = content_address
        self.reads: list[ReadRecord] = []
        self.seams: list[SeamEvent] = []
        self.opens: list[str] = []
        self._error: str | None = None
        self._token = None

    # -- recording (called by loaders + audit hook; must never raise upward) --

    def record_read(
        self, kind: str, identifier: str, nonempty: bool, content_address=None
    ) -> None:
        self.reads.append(ReadRecord(kind, identifier, nonempty, content_address))

    def record_seam(self, kind: str, detail: str) -> None:
        self.seams.append(SeamEvent(kind, detail))

    def record_open(self, path) -> None:
        if isinstance(path, str) and path:
            self.opens.append(path)

    def mark_error(self, reason: str) -> None:
        """Latch the first observer-internal error. Forces OPAQUE at classify.

        Fail-safe: an error in our own frames means we can no
        longer trust our observation, so the honest verdict is OPAQUE. First
        reason wins so the receipt names the root cause.
        """
        if self._error is None:
            self._error = reason

    # -- classification ------------------------------------------------------

    def classify(self) -> GroundingVerdict:
        """Compute the verdict from captured reads, seams, and opens.

        Order of decision:
          1. Any observer-internal error → OPAQUE (we cannot trust ourselves).
          2. A cited read returned non-empty data → GROUNDED.
          3. Otherwise, if anything could have hidden a read — a spawn seam, or
             a cited path opened through an uninstrumented reader — → OPAQUE.
          4. Only when the scope was fully seen and no cited data arrived →
             UNGROUNDED. This is the sole path to UNGROUNDED, which is what
             makes UNGROUNDED trustworthy.
        """
        cited = self.cited
        reads = tuple(self.reads)
        seams = list(self.seams)
        # Read coverage is a property of the FILE surface only: the audit hook's
        # open events are file opens, and the coverage gap it measures is the
        # builtins.open wrapper missing an os.open / C-extension read of a file.
        # sqlite and http reads never emit an open event, so counting them here
        # would divide across disjoint universes and push the fraction above 1.
        reads_seen = sum(1 for r in reads if r.kind == "file")
        opens_detected = len(self.opens)

        base = dict(
            cited_sources=cited,
            reads=reads,
            reads_seen=reads_seen,
            opens_detected=opens_detected,
        )

        if self._error is not None:
            return GroundingVerdict(
                grounding=ObservedGrounding.OPAQUE,
                reason=f"observer-internal error, verdict withheld: {self._error}",
                seams=tuple(seams),
                **base,
            )

        for r in reads:
            if r.nonempty and read_matches_citation(
                r.identifier, r.content_address, cited
            ):
                # Be honest about what "non-empty" means per loader: sqlite and
                # http wrappers see the actual returned rows/bytes, but the
                # builtins.open path proxies data flow by file size at open time
                # and does not observe the bytes consumed. Do not let the signed
                # reason claim byte-level flow the observer did not see.
                if r.kind == "file":
                    reason = (
                        "the cited source was opened for reading and is "
                        "non-empty (file; the open path proxies data flow by "
                        "file size, it does not observe the bytes read)"
                    )
                else:
                    reason = (
                        "a read matching the cited source returned non-empty "
                        f"data ({r.kind})"
                    )
                return GroundingVerdict(
                    grounding=ObservedGrounding.GROUNDED,
                    reason=reason,
                    matched_identifier=normalize_identifier(r.identifier),
                    seams=tuple(seams),
                    **base,
                )

        # No qualifying cited read. Did anything hide one?
        read_idents = {normalize_identifier(r.identifier) for r in reads}
        for op in self.opens:
            n = normalize_identifier(op)
            if n in cited and n not in read_idents:
                seams.append(
                    SeamEvent(
                        "coverage-gap",
                        "cited source opened through an uninstrumented reader",
                    )
                )
                break

        if seams:
            kinds = ", ".join(sorted({s.kind for s in seams}))
            return GroundingVerdict(
                grounding=ObservedGrounding.OPAQUE,
                reason=(
                    "no cited read observed, but a seam could have hidden one "
                    f"({kinds}); absence cannot be trusted"
                ),
                seams=tuple(seams),
                **base,
            )

        return GroundingVerdict(
            grounding=ObservedGrounding.UNGROUNDED,
            reason=(
                "scope fully observed; no read matching the cited source "
                "returned data"
            ),
            seams=tuple(seams),
            **base,
        )


def enter(cited: tuple[str, ...], *, content_address: bool = False) -> Scope:
    """Push a new scope onto the current context and return it."""
    scope = Scope(cited, content_address=content_address)
    scope._token = _active.set(scope)
    return scope


def exit(scope: Scope) -> None:
    """Pop *scope*, restoring the prior context (supports nesting)."""
    if scope._token is not None:
        _active.reset(scope._token)
        scope._token = None


# -- the single ingress-recording chokepoint --------------------------------

def record_read(kind: str, identifier: str, nonempty: bool, content_address=None) -> None:
    """Record one read against the active scope, if any. The one place every
    wrapped loader routes ingress through — so the recording rule lives once.
    """
    scope = _active.get()
    if scope is not None:
        scope.record_read(kind, identifier, nonempty, content_address)
