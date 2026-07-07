"""Observed-grounding verdict: the computed axis, its receipt, and its digest.

The observed grounding axis is SEPARATE from the declared ``classification``
enum. Declared classification (``INFERRED`` / ``ANALYTICAL`` / ``DERIVED``) is
what the producing agent asserts. Observed grounding is what execution shows:
did cited data actually flow into the scope that authored the finding. The two
never share a value space, so a reader can never confuse a self-declaration
with a computed result.

Three states, and only three:

- ``GROUNDED``   — a read that matches the finding's cited source returned
                   non-empty data inside the observed scope. For the
                   ``builtins.open`` path this is a stat-based proxy (the cited
                   file was opened for reading and is non-empty); the sqlite and
                   http wrappers observe the actual returned rows/bytes. So for a
                   plain file GROUNDED means "the cited data was accessed and has
                   content," not that the bytes were provably consumed.
- ``UNGROUNDED`` — no qualifying cited read, and nothing hid one: the scope was
                   fully observed and the cited data genuinely did not arrive.
                   This is the silent-fallback tell. One residual bound: a read
                   through a resource opened BEFORE the scope (a module-level or
                   pooled connection reused inside it) is neither wrapped nor
                   seamed, so it can read as UNGROUNDED — open the cited source
                   inside the scope for the tell to hold (see the coverage-bound
                   note in :mod:`mareforma.observe._loaders`).
- ``OPAQUE``     — the observer could not see. A spawn seam (thread, subprocess,
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
GROUNDING_AXIS_VERSION = "v0.3.8"


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
        never sufficient — promotion still needs the independent-signer counts.
        """
        return self is ObservedGrounding.GROUNDED


@dataclass(frozen=True)
class ReadRecord:
    """One data-ingress event captured inside an observed scope.

    ``identifier`` is the normalized handle the read touched: an absolute file
    path, a database connection target, or a ``scheme://host/path`` for a URL.
    ``nonempty`` is whether the read returned any bytes or rows — an empty read
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

    ``kind`` is ``thread`` / ``subprocess`` / ``socket`` / ``coverage-gap``.
    ``detail`` is a short, non-sensitive descriptor (the audit event name, the
    connection host, or the cited path opened via an uninstrumented reader). A
    seam inside a scope with no qualifying cited read forces ``OPAQUE`` — the
    read could have happened on the far side of the seam.
    """

    kind: str
    detail: str


@dataclass
class GroundingVerdict:
    """The computed grounding verdict plus its inspectable receipt.

    The verdict is what a human stakes trust on: one of three states, a plain
    reason, and the cited sources it was computed against. The receipt is the
    full evidence — every read and seam the observer captured — so the verdict
    can be audited rather than taken on faith. Only the receipt DIGEST is bound
    into the signed envelope (:meth:`to_signed_dict`), to keep envelopes small.
    mareforma does NOT persist the full receipt itself; the digest commits to
    it, so a caller who retains the receipt out of band can detect any mutation,
    but there is no stored receipt for mareforma to re-check on read.
    """

    grounding: ObservedGrounding
    reason: str
    cited_sources: tuple[str, ...] = ()
    reads: tuple[ReadRecord, ...] = ()
    seams: tuple[SeamEvent, ...] = ()
    matched_identifier: str | None = None
    version: str = GROUNDING_AXIS_VERSION
    # Reads the observer saw versus opens it detected but could not read
    # (uninstrumented reader). Powers the read-coverage-fraction measurement.
    reads_seen: int = 0
    opens_detected: int = 0

    def receipt(self) -> dict:
        """The full, canonicalizable receipt of what the observer captured."""
        return {
            "version": self.version,
            "grounding": self.grounding.value,
            "reason": self.reason,
            "cited_sources": list(self.cited_sources),
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

        Carries the verdict, the reason, the receipt digest, and the axis
        version — enough to re-check the verdict, and to confirm an
        out-of-band receipt is unmutated for a caller that keeps one, without
        inflating the envelope with the full read list. mareforma binds the
        digest, not the receipt itself. This is an OPTIONAL, versioned field:
        pre-v0.3.8 envelopes omit it entirely, and its absence is read as "not
        present," never as tampering.
        """
        return {
            "version": self.version,
            "grounding": self.grounding.value,
            "reason": self.reason,
            "receipt_digest": self.receipt_digest(),
        }

    def read_coverage_fraction(self) -> float | None:
        """Fraction of detected opens the observer actually read through.

        ``reads_seen / opens_detected``. ``None`` when nothing was opened (the
        fraction is undefined, not zero). A value below 1.0 means the observer
        detected data-ingress it could not see the bytes of — the honest
        coverage bound the measurement reports.
        """
        if self.opens_detected <= 0:
            return None
        return self.reads_seen / self.opens_detected
