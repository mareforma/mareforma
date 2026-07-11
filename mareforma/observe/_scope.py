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

from ._citation import citation_kind, normalize_identifier, read_matches_citation
from ._verdict import GroundingVerdict, ObservedGrounding, ReadRecord, SeamEvent

# Which citation kinds a socket seam can hide a read of. A socket delivers bytes
# over the network, so it is relevant to a URL or a content-address (whose bytes
# can arrive from anywhere) but NOT to a local file (an in-process file read hits
# the open audit event; a C-extension file is floored to OPAQUE by its own
# coverage-gap seam). Every other seam kind hides anything.
_SOCKET_BLOCKS: frozenset[str] = frozenset({"url", "content-address", "unknown"})
_BLOCKS_EVERYTHING: frozenset[str] = frozenset(
    {"subprocess", "thread", "coverage-gap"}
)


def _seam_blocks_ungrounded(seam_kind: str, cited_kinds: set[str]) -> bool:
    """Whether a seam of this kind blocks an UNGROUNDED verdict for this cited set.

    Conservative-ANY: the seam blocks if it could have hidden a read of ANY
    citation kind present. Fail-closed on both axes — an unknown seam kind blocks
    everything, and an unknown citation kind is blocked by every seam — so a gap
    the matrix does not model lands OPAQUE, never a confident UNGROUNDED. An empty
    citation set has no tell to recover, so any seam blocks.
    """
    if not cited_kinds:
        return True
    for k in cited_kinds:
        if k == "unknown":
            return True
        if seam_kind == "socket":
            if k in _SOCKET_BLOCKS:
                return True
        elif seam_kind in _BLOCKS_EVERYTHING:
            return True
        else:
            # Unknown seam kind: fail-closed, blocks any citation.
            return True
    return False

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
        # Model/method lineage records captured in this span (a wrapped httpx
        # POST body-parse, or a producer declaration). Empty when no model call
        # authored the finding — the lineage is then absent, never fabricated.
        self.models: list = []
        self._error: str | None = None
        self._token = None

    # -- recording (called by loaders + audit hook; must never raise upward) --

    def record_read(
        self, kind: str, identifier: str, nonempty: bool, content_address=None
    ) -> None:
        self.reads.append(ReadRecord(kind, identifier, nonempty, content_address))

    def record_seam(self, kind: str, detail: str) -> None:
        self.seams.append(SeamEvent(kind, detail))

    def record_model(self, lineage) -> None:
        self.models.append(lineage)

    def model_lineage(self):
        """The single finding-level model/method lineage, or None if no model
        call was observed in this span."""
        from ._lineage import collapse_lineage

        return collapse_lineage(self.models)

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
            model_lineage=self.model_lineage(),
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
                # builtins.open path AND the C-extension readers (h5py / pyarrow /
                # netCDF4) proxy data flow by file size at open time and do not
                # observe the bytes consumed. Do not let the signed reason claim
                # byte-level flow the observer did not see.
                if r.kind in ("file", "c-extension"):
                    reader = "file" if r.kind == "file" else "C-extension reader"
                    reason = (
                        "the cited source was opened for reading and is "
                        f"non-empty ({reader}; the open path proxies data flow by "
                        "file size, it does not observe the bytes read)"
                    )
                else:
                    reason = (
                        "a read matching the cited source returned non-empty "
                        f"data ({r.kind})"
                    )
                # The cited sources an actual non-empty read was observed for —
                # every cited entry that a read bound to, not just the first. The
                # binding gate checks THESE against the finding's citation, never
                # the declared `cited`: a decoy read of one cited source cannot
                # ground a finding whose own cited data was never read.
                grounded = tuple(
                    c
                    for c in cited
                    if any(
                        rr.nonempty
                        and read_matches_citation(
                            rr.identifier, rr.content_address, (c,)
                        )
                        for rr in reads
                    )
                )
                return GroundingVerdict(
                    grounding=ObservedGrounding.GROUNDED,
                    reason=reason,
                    grounded_sources=grounded,
                    matched_identifier=normalize_identifier(r.identifier),
                    seams=tuple(seams),
                    **base,
                )

        # No qualifying cited read. Did anything hide one? Fold in the coverage
        # gaps that a fully-observed scope cannot rule out: a cited source opened
        # through an uninstrumented reader, a cited C-extension file whose bytes
        # never emit a PEP-578 event, a cited URL with no observed HTTP read, and
        # a content-address citation whose match the observer never had the hash
        # to attempt. Each becomes a coverage-gap seam, which the relevance
        # matrix below treats as blocking every citation kind (fail-closed).
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
        for c in cited:
            if c in read_idents:
                continue
            kind = citation_kind(c)
            if kind == "c-extension-file":
                seams.append(
                    SeamEvent(
                        "coverage-gap",
                        "C-extension reader, bytes not observable via PEP-578",
                    )
                )
            elif kind == "url":
                seams.append(
                    SeamEvent(
                        "coverage-gap",
                        "cited URL with no observed HTTP read; coverage unknown",
                    )
                )
            elif kind == "content-address":
                # A sha256: citation matches by hash of the read's bytes. With
                # hashing off no read carries one, so a non-match is guaranteed
                # regardless of what was read; with hashing on, an unhashed
                # non-empty read (the open path never hashes) could still have
                # carried the cited bytes. Either way the absence of a match is
                # not evidence of absence.
                if not self.content_address:
                    seams.append(
                        SeamEvent(
                            "coverage-gap",
                            "content-address citation without content "
                            "addressing enabled; reads were not hashed",
                        )
                    )
                elif any(
                    r.nonempty and r.content_address is None for r in reads
                ):
                    seams.append(
                        SeamEvent(
                            "coverage-gap",
                            "content-address citation alongside unhashed "
                            "non-empty reads; the cited bytes could have "
                            "arrived through one",
                        )
                    )

        # Seam-relevance matrix. A seam blocks UNGROUNDED only if it could have
        # hidden a read of a citation kind actually in the set (conservative-ANY:
        # one relevant citation is enough). A socket seam cannot deliver a local
        # file read, so it does NOT block a file-cited finding — that is the tell
        # a silent fallback on an LLM-shaped pipeline leaves behind. It DOES block
        # URL- and content-address-cited findings (bytes can arrive over the
        # network). Subprocess / thread / coverage-gap seams, and any unknown
        # seam or citation kind, block everything (fail-closed).
        cited_kinds = {citation_kind(c) for c in cited}
        relevant = [s for s in seams if _seam_blocks_ungrounded(s.kind, cited_kinds)]
        if relevant:
            kinds = ", ".join(sorted({s.kind for s in relevant}))
            return GroundingVerdict(
                grounding=ObservedGrounding.OPAQUE,
                reason=(
                    "no cited read observed, but a seam relevant to the cited "
                    f"source(s) could have hidden one ({kinds}); absence cannot "
                    "be trusted"
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

    def classify_against(self, cited: tuple[str, ...]) -> GroundingVerdict:
        """Classify this scope's captured evidence against a different cited set.

        The post-hoc auditor computes one verdict per finding from a single
        observed run: the evidence (reads, seams, opens, lineage) is shared and
        only the cited set differs per finding. The records are copied onto a
        fresh scope so :meth:`classify` stays the one classification routine
        and no call mutates the observed scope.
        """
        other = Scope(cited, content_address=self.content_address)
        other.reads = list(self.reads)
        other.seams = list(self.seams)
        other.opens = list(self.opens)
        other.models = list(self.models)
        other._error = self._error
        return other.classify()


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
