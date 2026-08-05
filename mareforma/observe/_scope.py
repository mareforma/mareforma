"""The observed scope: the contextvar-held window in which reads are attributed.

A scope is the span between ``with observe(...) as obs:`` and its exit. Inside
it, wrapped loaders record the reads they see and the audit hook records seams
and opens. On exit the scope is frozen and :meth:`Scope.classify` computes the
verdict from what was captured. Nothing about the verdict is declared; it is a
function of the recorded evidence.

Scope is held in a :class:`contextvars.ContextVar`, not thread-local, so it
propagates into ``asyncio`` tasks CREATED INSIDE it (the event loop copies the
context into each task). A task that predates the scope carries a context copy
without it and is seamed at scope entry instead. It deliberately does NOT reach
library-spawned threads or a pipeline that owns its own loop. Work handed off
inside the scope, a thread started (``Thread.start``) or a thread-pool
submit/map, is caught at the hand-off and recorded as a seam, so an unseeable
read there becomes ``OPAQUE`` rather than a confident false ``UNGROUNDED``. A
resource opened BEFORE the scope and reused inside it (a module-level handle or
pooled connection) stays the documented coverage bound described in
``_loaders``: its reads are invisible.
"""
from __future__ import annotations

from contextvars import ContextVar

from ._citation import (
    citation_kind,
    normalize_identifier,
    read_norm_matches,
)
from ._verdict import GroundingVerdict, ObservedGrounding, ReadRecord, SeamEvent

# Which citation kinds a socket seam can hide a read of. A socket delivers bytes
# over the network, so it is relevant to a URL or a content-address (whose bytes
# can arrive from anywhere) but NOT to a local file (an in-process file read hits
# the open audit event; a C-extension file is floored to OPAQUE by its own
# coverage-gap seam). Every other seam kind hides anything.
_SOCKET_BLOCKS: frozenset[str] = frozenset({"url", "content-address", "unknown"})
# The target aborted before the scope closed. Not a boundary in space but one in
# time: the run stopped part way, so what it did not read says nothing about what
# the pipeline reads. Recorded as a seam so the one classification routine
# handles it, and blocking like every other non-socket seam.
ABORT_SEAM = "abort"
_BLOCKS_EVERYTHING: frozenset[str] = frozenset(
    {"subprocess", "thread", "coverage-gap", ABORT_SEAM}
)
# A failed open hides nothing: it is raised only when the observed failures
# account for EVERY open of the cited path, so no reader is left unexplained and
# the open provably returned no file object. That is evidence of absence, and a
# scope that never attempted the read lands UNGROUNDED already, so blocking here
# would make more observation buy a weaker verdict.
_NEVER_BLOCKS: frozenset[str] = frozenset({"failed-open"})
# Every seam kind the classifier can record, for the reports that enumerate them
# (the doctor). Derived from the matrix above, so a kind cannot enter the
# classifier without entering the report.
SEAM_KINDS: frozenset[str] = _BLOCKS_EVERYTHING | _NEVER_BLOCKS | {"socket"}


def _seam_blocks_ungrounded(seam_kind: str, cited_kinds: set[str]) -> bool:
    """Whether a seam of this kind blocks an UNGROUNDED verdict for this cited set.

    Conservative-ANY: the seam blocks if it could have hidden a read of ANY
    citation kind present. Fail-closed on both axes, an unknown seam kind blocks
    everything, and an unknown citation kind is blocked by every seam, so a gap
    the matrix does not model lands OPAQUE, never a confident UNGROUNDED. An empty
    citation set has no tell to recover, so any seam blocks. The one exception is
    a seam that records a failure rather than a blind spot.
    """
    if seam_kind in _NEVER_BLOCKS:
        return False
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
        # (identifier, exception type name) for each wrapped read-mode open
        # that raised. A failed open delivered no data; classify() uses these
        # to name the failure when they account for every unexplained open of
        # a cited path.
        self.failed_opens: list[tuple[str, str]] = []
        # Model/method lineage records captured in this span (a wrapped httpx
        # POST body-parse, or a producer declaration). Empty when no model call
        # authored the finding, the lineage is then absent, never fabricated.
        self.models: list = []
        self._error: str | None = None
        self._token = None
        # The scope this one was opened inside, if any. A nested scope holds the
        # contextvar for its whole span, so every read the parent would have seen
        # lands here instead; exit() replays them into the parent.
        self._parent: "Scope | None" = None
        # (normalized identifier, ReadRecord) for each read, paired once and
        # reused across every classify pass. The post-hoc auditor shares this so
        # a corpus audit pairs the shared reads once, not once per finding.
        self._norm_reads: "list[tuple[str, ReadRecord]] | None" = None
        # Normalized form per raw read identifier, so a read loop over one path
        # resolves it once.
        self._norm_cache: dict[str, str] = {}
        # Open counts keyed by normalized identifier, computed once and shared
        # the same way: the opens are identical across findings too.
        self._norm_opens: "dict[str, int] | None" = None

    # -- recording (called by loaders + audit hook; must never raise upward) --

    def record_read(
        self, kind: str, identifier: str, nonempty: bool, content_address=None
    ) -> None:
        self.reads.append(
            ReadRecord(kind, self._normalize(identifier), nonempty, content_address)
        )

    def _normalize(self, identifier: str) -> str:
        """Normalize a read identifier once, memoized per raw string.

        Normalization happens HERE, on the producing host, where the filesystem
        that resolves a relative path and the process that set the working
        directory both live. A receipt then carries identifiers any reader can
        compare by plain string equality, from any directory and on any host,
        which is the same rule the citation binding follows. A read loop hands
        the same path in thousands of times, so the memo keeps ``realpath`` to
        one call per distinct identifier.
        """
        norm = self._norm_cache.get(identifier)
        if norm is None:
            norm = normalize_identifier(identifier)
            self._norm_cache[identifier] = norm
        return norm

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

    def record_failed_open(self, path, exc_type: str) -> None:
        if isinstance(path, str) and path:
            self.failed_opens.append((path, exc_type))

    def mark_error(self, reason: str) -> None:
        """Latch the first observer-internal error. Forces OPAQUE at classify.

        Fail-safe: an error in our own frames means we can no
        longer trust our observation, so the honest verdict is OPAQUE. First
        reason wins so the receipt names the root cause.
        """
        if self._error is None:
            self._error = reason

    # -- classification ------------------------------------------------------

    def _normalized_reads(self) -> "list[tuple[str, ReadRecord]]":
        """Each read paired with its normalized identifier, paired once.

        The identifier is normalized at record time, so this is the pairing the
        classifier and the post-hoc auditor both want, built once and shared
        across every finding rather than rebuilt per classify pass.
        """
        if self._norm_reads is None:
            self._norm_reads = [(r.identifier, r) for r in self.reads]
        return self._norm_reads

    def _normalized_opens(self) -> "dict[str, int]":
        """Open counts by normalized identifier, computed once.

        Same memoization as the reads: ``normalize_identifier`` reaches
        ``os.path.realpath``, and a run opens the same paths repeatedly.
        """
        if self._norm_opens is None:
            counts: dict[str, int] = {}
            cache: dict[str, str] = {}
            for op in self.opens:
                norm = cache.get(op)
                if norm is None:
                    norm = normalize_identifier(op)
                    cache[op] = norm
                counts[norm] = counts.get(norm, 0) + 1
            self._norm_opens = counts
        return self._norm_opens

    def coverage_counts(self) -> "tuple[int, int]":
        """``(reads_seen, opens_detected)``, the honest coverage bound.

        Both halves range over the CITED paths the audit hook saw opened, so
        the fraction answers one question: of the ingress detected for the
        cited sources, how much did the observer read through. It is the
        complement of the coverage-gap seam ``classify`` raises, so a cited
        file read through ``builtins.open`` gives 1/1 and one read through
        ``os.open`` gives 0/1 alongside its seam.

        Counting every open in the process instead would fold in the import
        machinery's ``.pyc`` reads and the target script itself, deflating the
        bound by how much the target imports rather than by what the observer
        missed.
        """
        read_idents = {norm for norm, _ in self._normalized_reads()}
        opened = {n for n in self._normalized_opens() if n in self.cited}
        return len(opened & read_idents), len(opened)

    def classify(self) -> GroundingVerdict:
        """Compute the verdict from captured reads, seams, and opens.

        Order of decision:
          1. Any observer-internal error → OPAQUE (we cannot trust ourselves).
          2. A cited read returned non-empty data → GROUNDED.
          3. Otherwise, if anything could have hidden a read, a spawn seam, or
             a cited path opened through an uninstrumented reader, → OPAQUE.
          4. Only when the scope was fully seen and no cited data arrived →
             UNGROUNDED. This is the sole path to UNGROUNDED, which is what
             makes UNGROUNDED trustworthy.
        """
        cited = self.cited
        reads = tuple(self.reads)
        norm_reads = self._normalized_reads()
        seams = list(self.seams)
        reads_seen, opens_detected = self.coverage_counts()

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

        for norm, r in norm_reads:
            if r.nonempty and read_norm_matches(
                norm, r.content_address, cited
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
                # The cited sources an actual non-empty read was observed for , 
                # every cited entry that a read bound to, not just the first. The
                # binding gate checks THESE against the finding's citation, never
                # the declared `cited`: a decoy read of one cited source cannot
                # ground a finding whose own cited data was never read.
                grounded = tuple(
                    c
                    for c in cited
                    if any(
                        rr.nonempty
                        and read_norm_matches(rn, rr.content_address, (c,))
                        for rn, rr in norm_reads
                    )
                )
                return GroundingVerdict(
                    grounding=ObservedGrounding.GROUNDED,
                    reason=reason,
                    grounded_sources=grounded,
                    matched_identifier=norm,
                    seams=tuple(seams),
                    **base,
                )

        # No qualifying cited read. Did anything hide one? Fold in the coverage
        # gaps that a fully-observed scope cannot rule out: a cited source opened
        # through an uninstrumented reader, a cited C-extension file whose bytes
        # never emit a PEP-578 event, a cited URL with no observed HTTP read, and
        # a content-address citation whose match the observer never had the hash
        # to attempt. Each becomes a coverage-gap seam, which the relevance
        # matrix below treats as blocking every citation kind (fail-closed). An
        # open the observer watched FAIL is not one of them: it is recorded as a
        # failed-open seam, which names the failure without blocking.
        read_idents = {norm for norm, _ in norm_reads}
        failed_open_types = ""
        unexplained = {
            n: count
            for n, count in self._normalized_opens().items()
            if n in cited and n not in read_idents
        }
        if unexplained:
            # A wrapped open that raised delivered no data. When such failures
            # account for EVERY unexplained open of the cited paths (count-
            # aware, per identifier), nothing is left unexplained and the
            # honest narrative is that the open failed, which does not block
            # UNGROUNDED. One open more than the observed failures means a
            # reader the wrapper never saw, so the hidden-reader coverage gap
            # stays: a failed open must never lend its story to an open that
            # could have read the data.
            failed: dict[str, int] = {}
            failed_types: set[str] = set()
            for path, exc_type in self.failed_opens:
                fn = normalize_identifier(path)
                if fn in unexplained:
                    failed[fn] = failed.get(fn, 0) + 1
                    failed_types.add(exc_type)
            if all(failed.get(n, 0) >= count for n, count in unexplained.items()):
                failed_open_types = ", ".join(sorted(failed_types))
                seams.append(
                    SeamEvent(
                        "failed-open",
                        f"the observed open of the cited source failed "
                        f"({failed_open_types}); no data flowed through the "
                        "failed open",
                    )
                )
            else:
                seams.append(
                    SeamEvent(
                        "coverage-gap",
                        "cited source opened through an uninstrumented reader",
                    )
                )
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
        # file read, so it does NOT block a file-cited finding, that is the tell
        # a silent fallback on an LLM-shaped pipeline leaves behind. It DOES block
        # URL- and content-address-cited findings (bytes can arrive over the
        # network). Subprocess / thread / coverage-gap seams, and any unknown
        # seam or citation kind, block everything (fail-closed). A failed-open
        # seam blocks nothing: it records a read that provably did not happen.
        cited_kinds = {citation_kind(c) for c in cited}
        relevant = [s for s in seams if _seam_blocks_ungrounded(s.kind, cited_kinds)]
        if relevant:
            kinds = ", ".join(sorted({s.kind for s in relevant}))
            if any(s.kind == ABORT_SEAM for s in relevant):
                # Say what actually happened. The run was cut short, so no read
                # was hidden; the observation simply never covered the whole
                # pipeline, which is a different fact and the honest one.
                reason = (
                    "no cited read observed, but the target aborted before the "
                    f"scope closed ({kinds}); the observation is truncated, so "
                    "absence cannot be trusted"
                )
            else:
                reason = (
                    "no cited read observed, but a seam relevant to the cited "
                    f"source(s) could have hidden one ({kinds}); absence cannot "
                    "be trusted"
                )
            return GroundingVerdict(
                grounding=ObservedGrounding.OPAQUE,
                reason=reason,
                seams=tuple(seams),
                **base,
            )

        reason = (
            "scope fully observed; no read matching the cited source "
            "returned data"
        )
        if failed_open_types:
            reason = (
                "scope fully observed; the open of the cited source failed "
                f"({failed_open_types}) and no read matching it returned data"
            )
        return GroundingVerdict(
            grounding=ObservedGrounding.UNGROUNDED,
            reason=reason,
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
        other.failed_opens = list(self.failed_opens)
        other.models = list(self.models)
        other._error = self._error
        # The reads are identical across findings, so the normalized form is too:
        # share it so a corpus audit normalizes the shared reads once, not once
        # per finding. Only the cited set differs per call.
        other._norm_reads = self._normalized_reads()
        other._norm_opens = self._normalized_opens()
        return other.classify()


def enter(cited: tuple[str, ...], *, content_address: bool = False) -> Scope:
    """Push a new scope onto the current context and return it."""
    scope = Scope(cited, content_address=content_address)
    scope._parent = _active.get()
    scope._token = _active.set(scope)
    _seam_pre_scope_tasks(scope)
    return scope


def _seam_pre_scope_tasks(scope: Scope) -> None:
    """Seam the asyncio tasks that already existed when *scope* opened.

    A task created BEFORE the scope runs with a context copy that has no scope,
    so a read it performs inside the scope's window is neither recorded nor seen:
    the same hand-off blind spot the thread-pool wrapper covers, and the same
    fail-closed answer. The ``thread`` kind blocks every citation kind, so the
    span lands OPAQUE rather than a confident false UNGROUNDED. It
    over-approximates (an unrelated heartbeat task seams too), which is the
    conservative direction.
    """
    try:
        import asyncio

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # no running loop: no task can be pending
        if asyncio.all_tasks(loop) - {asyncio.current_task()}:
            scope.record_seam(
                "thread", "asyncio task pending outside the scope's context"
            )
    except BaseException as exc:  # noqa: BLE001
        scope.mark_error(f"asyncio task snapshot failed: {type(exc).__name__}")


def exit(scope: Scope) -> None:
    """Pop *scope*, replaying its evidence into the parent (supports nesting).

    Everything the inner scope captured happened inside the outer span too, so
    the outer scope inherits it. Without the replay the outer scope would
    classify against an empty evidence set and return a confident false
    UNGROUNDED for reads it never had a chance to see.
    """
    if scope._token is None:
        return
    _active.reset(scope._token)
    scope._token = None
    parent = scope._parent
    scope._parent = None
    if parent is None:
        return
    parent.reads.extend(scope.reads)
    parent.seams.extend(scope.seams)
    parent.opens.extend(scope.opens)
    parent.failed_opens.extend(scope.failed_opens)
    parent.models.extend(scope.models)
    if scope._error is not None:
        parent.mark_error(scope._error)
    # The parent's read set grew, so any memoized normalization is stale.
    parent._norm_reads = None


# -- the single ingress-recording chokepoint --------------------------------

def record_read(kind: str, identifier: str, nonempty: bool, content_address=None) -> None:
    """Record one read against the active scope, if any. The one place every
    wrapped loader routes ingress through, so the recording rule lives once.
    """
    scope = _active.get()
    if scope is not None:
        scope.record_read(kind, identifier, nonempty, content_address)


def record_abort(exit_code: int) -> None:
    """Record that the observed target aborted, against the active scope if any.

    Call it from inside the scope, before it closes: what the target had not
    read yet was never going to be observed, so the verdict must stop short of
    a confident UNGROUNDED.
    """
    scope = _active.get()
    if scope is not None:
        scope.record_seam(
            ABORT_SEAM,
            f"target aborted before the scope closed (exit code {exit_code})",
        )
