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


@dataclass
class GroundingVerdict:
    """The computed grounding verdict plus its inspectable receipt.

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
