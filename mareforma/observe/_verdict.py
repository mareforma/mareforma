"""Observed-grounding verdict: the computed axis, its receipt, and its digest.

The observed grounding axis is SEPARATE from the declared ``classification``
enum. Declared classification (``INFERRED`` / ``ANALYTICAL`` / ``DERIVED``) is
what the producing agent asserts. Observed grounding is what execution shows:
did cited data actually flow into the scope that authored the finding. The two
never share a value space, so a reader can never confuse a self-declaration
with a computed result.

Three states, and only three:

- ``GROUNDED``  , a read that matches the finding's cited source returned
                   non-empty data inside the observed scope. For the
                   ``builtins.open`` path this is a stat-based proxy (the cited
                   file was opened for reading and is non-empty); the sqlite and
                   http wrappers observe the actual returned rows/bytes. So for a
                   plain file GROUNDED means "the cited data was accessed and has
                   content," not that the bytes were provably consumed.
- ``UNGROUNDED``, no qualifying cited read, and nothing hid one: the scope was
                   fully observed and the cited data genuinely did not arrive.
                   This is the silent-fallback tell. One residual bound: a read
                   through a resource opened BEFORE the scope (a module-level or
                   pooled connection reused inside it) is neither wrapped nor
                   seamed, so it can read as UNGROUNDED, open the cited source
                   inside the scope for the tell to hold (see the coverage-bound
                   note in :mod:`mareforma.observe._loaders`).
- ``OPAQUE``    , the observer could not see. A spawn seam (thread, subprocess,
                   uninstrumented socket) or an uninstrumented read of the cited
                   source occurred, so absence cannot be trusted. Never a
                   confident verdict across a boundary the observer cannot cross.

``OPAQUE`` is first-class on purpose: a binary GROUNDED/UNGROUNDED across a seam
the observer cannot see is confidently wrong, which is worse than admitting the
blind spot.
"""
from __future__ import annotations

import hashlib
import threading
import weakref
from dataclasses import dataclass
from enum import Enum

from .._canonical import canonicalize

# Version of the observed-grounding axis. Bound into the signed field so a
# verifier reading an envelope knows which axis semantics produced the verdict,
# and so a future axis revision (new states, new match rules) is distinguishable
# from this one rather than silently reinterpreted.
#
# v0.3.9 bumps the shape: the cited set the verdict was computed against now
# rides INSIDE the signed record (``cited_sources``), so verify-on-read can
# re-check the verdict against the finding's citation instead of trusting the
# write-time result. A v0.3.8 envelope omits ``cited_sources``; its absence is
# "the citation binding was not checkable," never tampering, the pre-binding
# label the auditor surface renders.
#
# v0.3.11 bumps the MATCH RULE: a read identifier is normalized when it is
# RECORDED, on the host whose filesystem can resolve it, and the summarizer then
# compares normalized strings directly. A v0.3.9 receipt carries the loader's raw
# identifier, so the same run's reads match its cited set under one rule and not
# the other, and one report cannot mix the two. ``summarize_receipts`` refuses
# the mismatch rather than reinterpreting an older receipt under the newer rule.
# The digest is over the receipt, not a claim about it: an older receipt stays
# verifiable against its own bytes.
GROUNDING_AXIS_VERSION = "v0.3.11"


# The provenance of a stored verdict that the observer did NOT compute. Absent
# from a record the observer wrote, so an observed record signs to the same bytes
# it always did; present means a caller handed the record in and mareforma
# watched no execution behind it.
DECLARED_PROVENANCE = "DECLARED"

# Appended to a declared record's reason, at most once, so the surfaces that
# render the reason (the trust map's residual, the CLI) say where the verdict
# came from without needing to know about a new field. Same idiom, and the same
# frozen-wording rule, as UNBOUND_ANNOTATION in :mod:`._binding`.
#
# It says what is known, for the same reason DECLARED_GROUNDED_REASON does: that
# nothing this process's observer computed matches this record. Whether the
# caller wrote it by hand is a stronger claim than the register can support.
DECLARED_ANNOTATION = (
    "[no verdict mareforma observed in this process matches this record]"
)

# The reason a declared GROUNDED carries once it is neutralised. It replaces the
# caller's reason rather than annotating it: the caller's sentence describes a
# read that mareforma never saw, so keeping it would leave an OPAQUE record still
# narrating a grounded one.
#
# It says what is KNOWN, which is that no observer record in this process matches
# this verdict. It does not say the caller declared it, because that is not
# knowable here: a record whose verdict has been collected and whose digest entry
# was evicted looks identical to one nobody ever computed, and asserting the
# stronger sentence made a false statement about a good-faith run.
DECLARED_GROUNDED_REASON = (
    "GROUNDED is not stored: no verdict this process's observer computed matches "
    "this record, so mareforma cannot say it watched any execution behind it"
)


class ObservedGrounding(str, Enum):
    """The computed grounding axis. Distinct value space from ``classification``."""

    GROUNDED = "GROUNDED"
    UNGROUNDED = "UNGROUNDED"
    OPAQUE = "OPAQUE"

    def promotes(self) -> bool:
        """Only GROUNDED may ever count toward support-level promotion.

        UNGROUNDED and OPAQUE are both non-promoting: a finding whose data did
        not observably flow, or whose flow could not be observed, must not lift
        a proposition up the support ladder. Grounding is a necessary floor,
        never sufficient, promotion still needs the independent-signer counts.
        """
        return self is ObservedGrounding.GROUNDED


def as_int(value: object) -> int:
    """Coerce a persisted record field to a non-negative int, defaulting to 0.

    A hand-authored, older, or truncated record may carry a missing, null, or
    non-numeric field; it degrades to 0 rather than raising, so one bad record
    never denies the whole report.
    """
    try:
        return max(0, int(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _read_grounding(value: object, reason: str) -> tuple["ObservedGrounding", str]:
    """Coerce a receipt's grounding field, degrading an unknown state to OPAQUE.

    A future axis state, a typo, or an explicit null reads as OPAQUE (the
    conservative, non-promoting bucket) with the unreadable value named in the
    reason, so the blind spot is stated rather than either abandoned or promoted.
    """
    try:
        return ObservedGrounding(value), reason
    except ValueError:
        note = f"unrecognized grounding {value!r}, read as OPAQUE"
        return ObservedGrounding.OPAQUE, f"{note}: {reason}" if reason else note


@dataclass(frozen=True)
class ReadRecord:
    """One data-ingress event captured inside an observed scope.

    ``identifier`` is the normalized handle the read touched: an absolute,
    symlink-resolved file path, a database connection target, or a
    ``scheme://host/path`` for a URL. Normalization happens when the read is
    recorded, on the host that resolved it, so no credential (the userinfo and
    query a presigned URL carries) is ever stored and a reader can compare the
    identifier without touching a filesystem.
    ``nonempty`` is whether the read returned any bytes or rows, an empty read
    of a cited source is the silent-fallback signature, so it is recorded as a
    read that happened but carried nothing. ``content_address`` is the
    ``sha256:`` digest of the returned bytes, filled in only on the opt-in
    content-address path (see :mod:`mareforma.observe._citation`).
    """

    kind: str
    identifier: str
    nonempty: bool
    content_address: str | None = None


@dataclass(frozen=True)
class SeamEvent:
    """A boundary the observer cannot see across, captured inside a scope.

    ``kind`` is ``thread`` / ``subprocess`` / ``socket`` / ``coverage-gap`` /
    ``abort`` (the target stopped before the scope closed) / ``failed-open``.
    ``detail`` is a short, non-sensitive descriptor (the audit event name, the
    connection host, or the cited path opened via an uninstrumented reader). A
    seam inside a scope with no qualifying cited read forces ``OPAQUE``, the
    read could have happened on the far side of the seam. ``failed-open`` is
    the exception: it records an open the observer watched fail, which hides
    nothing and leaves the verdict ``UNGROUNDED``.
    """

    kind: str
    detail: str


@dataclass(frozen=True)
class GroundingVerdict:
    """The computed grounding verdict plus its inspectable receipt.

    Frozen. The observed axis is the one field on a claim that is not the
    producer's own word, and a mutable verdict hands it straight back: the
    measured attack was to run a real ``observe()`` scope, assign
    ``verdict.grounding = GROUNDED`` on the object it returned, and pass it in.
    Freezing turns that assignment into an error at the point it is written.
    Freezing alone is not the guarantee, it is the cheap half: the write path
    stores the SNAPSHOT taken when the observer minted the verdict, so even an
    edit made through ``object.__setattr__`` is discarded rather than signed.

    The verdict is what a human stakes trust on: one of three states, a plain
    reason, and the cited sources it was computed against. The receipt is the
    full evidence, every read and seam the observer captured, so the verdict
    can be audited rather than taken on faith. Only the receipt DIGEST is bound
    into the signed envelope (:meth:`to_signed_dict`), to keep envelopes small.
    mareforma does NOT persist the full receipt itself; the digest commits to
    it, so a caller who retains the receipt out of band can detect any mutation,
    but there is no stored receipt for mareforma to re-check on read.
    """

    grounding: ObservedGrounding
    reason: str
    cited_sources: tuple[str, ...] = ()
    # The cited sources a matching non-empty read was actually observed for, the
    # subset of ``cited_sources`` that GROUNDED the verdict. The binding gate
    # checks THIS against the finding's citation, never the full declared
    # ``cited_sources``: a producer who lists a dataset in ``cites`` but reads
    # only an incidental decoy would otherwise earn a MATCHED binding on a source
    # it never read. Empty for UNGROUNDED / OPAQUE (nothing grounded the verdict).
    grounded_sources: tuple[str, ...] = ()
    reads: tuple[ReadRecord, ...] = ()
    seams: tuple[SeamEvent, ...] = ()
    matched_identifier: str | None = None
    version: str = GROUNDING_AXIS_VERSION
    # Reads the observer saw versus opens it detected but could not read
    # (uninstrumented reader). Powers the read-coverage-fraction measurement.
    reads_seen: int = 0
    opens_detected: int = 0
    # The model/method lineage captured in the authoring scope, tiered like
    # data_id (COMPUTED / PROXY / UNVERIFIABLE), or None when no model call was
    # observed. Carried on the verdict so the observe -> assert_finding thread
    # can persist it on the evidence line. It is NOT part of the signed receipt
    # or its digest: the identity rides the evidence line, not the
    # grounding envelope, so a pre-observer receipt digest is unchanged.
    model_lineage: "ModelLineage | None" = None

    def receipt(self) -> dict:
        """The full, canonicalizable receipt of what the observer captured."""
        return {
            "version": self.version,
            "grounding": self.grounding.value,
            "reason": self.reason,
            "cited_sources": list(self.cited_sources),
            "grounded_sources": list(self.grounded_sources),
            "matched_identifier": self.matched_identifier,
            "reads": [
                {
                    "kind": r.kind,
                    "identifier": r.identifier,
                    "nonempty": r.nonempty,
                    "content_address": r.content_address,
                }
                for r in self.reads
            ],
            "seams": [{"kind": s.kind, "detail": s.detail} for s in self.seams],
            "coverage": {
                "reads_seen": self.reads_seen,
                "opens_detected": self.opens_detected,
            },
        }

    def receipt_digest(self) -> str:
        """``sha256:<hex>`` over the canonical receipt bytes.

        Deterministic across hosts and Python versions (RFC 8785 canonical
        JSON), so a caller who retains the receipt out of band can re-derive
        this digest exactly and detect any mutation of that receipt.
        """
        return "sha256:" + hashlib.sha256(canonicalize(self.receipt())).hexdigest()

    def to_signed_dict(self) -> dict:
        """The compact record bound INTO the signed in-toto statement.

        Carries the verdict, the reason, the cited set it was computed against,
        the receipt digest, and the axis version, enough to re-check the verdict
        AND its binding to the finding's citation on read, and to confirm an
        out-of-band receipt is unmutated for a caller that keeps one, without
        inflating the envelope with the full read list. mareforma binds the
        digest, not the receipt itself. This is an OPTIONAL, versioned field:
        pre-v0.3.8 envelopes omit it entirely, and its absence is read as "not
        present," never as tampering. The ``cited_sources`` member arrived in
        v0.3.9; a v0.3.8 record omits it, and the auditor surface renders that as
        "pre-binding axis; citation binding not checkable." ``grounded_sources``
        rides alongside it: the binding is re-checked on read against the sources
        actually read, not the declared cite set.

        Pure: it reads this object and returns a dict. What the write path
        actually stores for an observed verdict is the snapshot taken at mint
        time, not this call's output (see :func:`_mint`), so serializing has no
        say in provenance and cannot be made to have one.
        """
        return {
            "version": self.version,
            "grounding": self.grounding.value,
            "reason": self.reason,
            "cited_sources": list(self.cited_sources),
            "grounded_sources": list(self.grounded_sources),
            "receipt_digest": self.receipt_digest(),
        }

    @classmethod
    def from_receipt(cls, receipt: dict) -> "GroundingVerdict":
        """Reconstruct a verdict from a full receipt dict (the inverse of
        :meth:`receipt`).

        A measurement run persists receipts (which carry the reads and seams a
        signed envelope does not) so the aggregate report can bucket OPAQUE by
        seam kind. This rebuilds a verdict from one so the same summarize path
        serves both live verdicts and persisted receipts. Unknown / missing
        fields degrade to their defaults rather than raise, so a hand-authored or
        older receipt still summarizes: an unreadable grounding state reads as
        OPAQUE (named in the reason) and unreadable coverage counts read as 0,
        never GROUNDED, so a malformed record cannot promote itself.
        """
        cov = receipt.get("coverage") or {}
        grounding, reason = _read_grounding(
            receipt.get("grounding", "OPAQUE"), receipt.get("reason", "")
        )
        return cls(
            grounding=grounding,
            reason=reason,
            cited_sources=tuple(receipt.get("cited_sources") or ()),
            grounded_sources=tuple(receipt.get("grounded_sources") or ()),
            reads=tuple(
                ReadRecord(
                    kind=r.get("kind", ""),
                    identifier=r.get("identifier", ""),
                    nonempty=bool(r.get("nonempty")),
                    content_address=r.get("content_address"),
                )
                for r in receipt.get("reads") or ()
            ),
            seams=tuple(
                SeamEvent(kind=s.get("kind", ""), detail=s.get("detail", ""))
                for s in receipt.get("seams") or ()
            ),
            matched_identifier=receipt.get("matched_identifier"),
            version=receipt.get("version", GROUNDING_AXIS_VERSION),
            reads_seen=as_int(cov.get("reads_seen")),
            opens_detected=as_int(cov.get("opens_detected")),
        )

    def read_coverage_fraction(self) -> float | None:
        """Fraction of the detected cited opens the observer read through.

        ``reads_seen / opens_detected``, both counted over the cited paths seen
        opened. ``None`` when no cited path was opened (the fraction is
        undefined, not zero). A value below 1.0 means the observer detected
        data-ingress it could not see the bytes of, the honest coverage bound
        the measurement reports.
        """
        if self.opens_detected <= 0:
            return None
        return self.reads_seen / self.opens_detected


# Freezing a dataclass with ``eq=True`` also makes it hashable, and this one must
# not be. ``model_lineage.decoding`` is a dict, so the generated hash succeeds on
# a verdict with no observed model call and raises ``unhashable type: 'dict'`` the
# moment there is one: ``set(verdicts)`` would work in a test and fail in
# production, on a difference that has nothing to do with hashing. Before the
# freeze the type was uniformly unhashable, and that is the contract kept here.
# Assigned after the class because the decorator overwrites an in-body
# ``__hash__``.
GroundingVerdict.__hash__ = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Provenance: which verdicts this process's observer actually computed
# ---------------------------------------------------------------------------
#
# The observed axis is the one signal on a claim that is not the producer's own
# word, so the write path must not take a verdict on the producer's word. A
# verdict is COMPUTED when it came out of :meth:`Scope.classify` in this process
# and DECLARED otherwise, and the write path stores the observer's own record for
# the first and neutralises the second (see ``EpistemicGraph._attest_grounding``).
#
# The boundary this draws is the PROCESS, and it is worth stating at full
# strength rather than the flattering version. Any in-process code can mint, and
# it does not need to import this module to do it: the scope's own recorder is
# exported, so a caller can hand the observer a read that never happened and the
# verdict it computes from that is genuinely minted. Nor is the register a proof
# that a mint belongs to a particular claim: it is keyed on the receipt digest
# and scoped to nothing, so any mint in the process authenticates any record
# carrying that digest.
#
# What it does stop is the ordinary, undetectable path, a caller writing the
# conclusion by hand and having every read surface render it as an execution
# mareforma watched. That is worth having; it is not the same as the observed
# axis being unforgeable by the producer, and nothing here should be read as
# claiming it is.
#
# The register is keyed on the verdict OBJECT's identity, not on a flag the
# object carries. A flag is settable, and every way of setting it was reachable:
# per instance, through a subclass that defaults it True, and through a duck type
# that is not a GroundingVerdict at all. Membership of a module-private table
# cannot be claimed from outside this module.
#
# Each entry holds the SNAPSHOT of what the observer computed, taken at mint
# time. The write path stores that snapshot, never a re-serialization of the
# caller's live object, so an edit landed on the object between mint and write
# (through ``object.__setattr__``, which the frozen dataclass does not stop) is
# discarded rather than signed.
#
# There are two registers because there are two call shapes, and they have
# different lifetimes.
#
# ``_MINTED`` is keyed on object identity, for the caller who hands the VERDICT
# to ``assert_finding``. Its entries are dropped by a finalizer when the verdict
# is collected, so it holds one entry per LIVE verdict and a long-lived process
# that mints per request does not grow it.
#
# ``_BY_DIGEST`` is keyed on the receipt digest, for the documented
# ``assert_claim(observed_grounding=obs.verdict.to_signed_dict())`` call, where
# what arrives is a plain dict with no object behind it. Its entries CANNOT hang
# off the verdict's lifetime: the verdict in that expression is very often a
# temporary that is already collected by the time the record is written, and
# tying the two dropped a genuinely observed GROUNDED on the release's own
# documented call. So it is an independent table with a cap, and a miss falls
# back to scanning the live verdicts, which is what keeps an honest caller who
# still holds the object from ever losing to eviction.
#
# Eviction therefore reaches only a record whose verdict is already dead, and it
# costs COMPUTED standing, never the reverse. The reason it leaves behind says
# what is actually known, that no observer record in this process matches, and
# not that the caller declared it, which is what the earlier wording asserted and
# could not know.
#
# The boundary this draws is the PROCESS, and it is worth stating at full
# strength rather than the flattering version. Any in-process code can mint, and
# it does not need to import this module to do it: the scope's own recorder is
# exported, so a caller can hand the observer a read that never happened and the
# verdict it computes from that is genuinely minted. Nor is the register a proof
# that a mint belongs to a particular claim: it is keyed on the receipt digest
# and scoped to nothing, so any mint in the process authenticates any record
# carrying that digest.
#
# What it does stop is the ordinary, undetectable path, a caller writing the
# conclusion by hand and having every read surface render it as an execution
# mareforma watched. That is worth having; it is not the same as the observed
# axis being unforgeable by the producer, and nothing here should be read as
# claiming it is.
_BY_DIGEST_LIMIT = 8192
_MINTED: "dict[int, tuple]" = {}
_BY_DIGEST: "dict[str, dict]" = {}
_MINTED_LOCK = threading.Lock()


def _forget(key: int) -> None:
    """Drop a dead verdict's identity entry. Its finalizer, registered at mint."""
    with _MINTED_LOCK:
        _MINTED.pop(key, None)


def _mint(verdict: "GroundingVerdict") -> "GroundingVerdict":
    """Register a verdict as the observer's own. The single mint point.

    Called by :meth:`Scope.classify` on the verdict it just computed from the
    recorded reads and seams, and by the observer's own teardown fallback. It is
    never called on anything a caller supplied, and it is module-private so a
    caller cannot call it on one: minting a hand-built verdict is the attack, not
    the migration path for a test that wants a GROUNDED record. A test that needs
    one runs a scope that reads a real file.

    Returns the verdict so a caller can mint and return in one expression.
    """
    if type(verdict) is not GroundingVerdict:
        return verdict
    snapshot = verdict.to_signed_dict()
    key = id(verdict)
    digest = snapshot.get("receipt_digest")
    digest = digest if isinstance(digest, str) else None
    with _MINTED_LOCK:
        _MINTED[key] = (weakref.ref(verdict), snapshot)
        if digest is not None:
            _BY_DIGEST[digest] = snapshot
            while len(_BY_DIGEST) > _BY_DIGEST_LIMIT:
                _BY_DIGEST.pop(next(iter(_BY_DIGEST)))
    weakref.finalize(verdict, _forget, key)
    return verdict


def minted_snapshot(verdict: object) -> "dict | None":
    """What the observer computed for THIS object, or None if it did not.

    The weakref check is not decoration: ``id()`` is reused once an object dies,
    and while the finalizer clears the entry at that moment, comparing the stored
    reference against the object asked about makes a reused address unable to
    inherit another verdict's standing under any interleaving.
    """
    if type(verdict) is not GroundingVerdict:
        return None
    with _MINTED_LOCK:
        entry = _MINTED.get(id(verdict))
    if entry is None:
        return None
    ref, snapshot = entry
    return dict(snapshot) if ref() is verdict else None


def minted_record(record: object) -> "dict | None":
    """The observer's own record for a caller-supplied one, or None.

    Keyed on ``receipt_digest``, and it returns THE OBSERVER'S copy, not the
    caller's. That is the whole point: a caller who takes a real verdict's record
    and flips ``grounding`` to GROUNDED keeps a digest that resolves to the
    UNGROUNDED record the observer actually wrote, and that is what gets stored.
    A digest the observer never emitted resolves to nothing.

    On a miss the live verdicts are scanned before giving up, so a caller still
    holding the object never loses to the digest table's cap.
    """
    if not isinstance(record, dict):
        return None
    digest = record.get("receipt_digest")
    if not isinstance(digest, str):
        return None
    with _MINTED_LOCK:
        stored = _BY_DIGEST.get(digest)
        if stored is None:
            for ref, snapshot in _MINTED.values():
                if ref() is not None and snapshot.get("receipt_digest") == digest:
                    stored = snapshot
                    break
    return dict(stored) if stored is not None else None


def declared_record(record: dict) -> dict:
    """Mark a caller-supplied verdict as declared, and strip its GROUNDED claim.

    Two things happen, and they are separate on purpose. ``provenance`` records
    the fact for any reader that wants it. Neutralising GROUNDED to OPAQUE is
    what makes the fact unmissable: every read surface, the promotion gate, the
    trust map, the CLI, the restore path, keys on the grounding STATE, so a
    marker alone would still leave a hand-built verdict rendering as an execution
    mareforma watched. OPAQUE is the honest state for it, the observer could not
    see, and the reason says why. UNGROUNDED and OPAQUE are left standing: they
    promote nothing, so a declaration cannot buy anything with them.

    The GROUNDED-specific evidence goes with the state, for the same reason the
    disjoint downgrade drops it: an OPAQUE record still committing to a GROUNDED
    receipt reads as a mutated record to an auditor.
    """
    out = dict(record)
    out["provenance"] = DECLARED_PROVENANCE
    if out.get("grounding") == ObservedGrounding.GROUNDED.value:
        # The neutralised reason already says declared, so it stands alone.
        out["grounding"] = ObservedGrounding.OPAQUE.value
        out["reason"] = DECLARED_GROUNDED_REASON
        out["grounded_sources"] = []
        out.pop("receipt_digest", None)
        return out
    reason = out.get("reason")
    reason = reason if isinstance(reason, str) else ""
    if DECLARED_ANNOTATION not in reason:
        out["reason"] = f"{reason} {DECLARED_ANNOTATION}".strip()
    return out
