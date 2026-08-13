"""The per-finding trust map: place every trust property honestly.

A claim carries many trust properties, and they are not equal. Some mareforma
computes from evidence (who signed it, whether cited data was observed to flow,
whether a signed contradiction stands). Some it computes through a proxy whose
bound it names (a file read is stat-based, not byte-proven). Some it does not
evaluate at all this release and says so rather than inferring (leakage across a
held-out partition; a private trust root). The trust map is the one artifact that
places EACH property at its tier with the residual named, so an auditor reads a
claim's trust as a structured, honest ledger instead of a single word.

Design invariants:

- **Read-side only.** The map is derived from what is already stored and signed.
  It adds no new signed field; nothing here changes a verdict or a support level.
- **Honest, never inferred.** An unobservable property is stated as such
  (``DEFERRED`` / ``not present`` / ``UNVERIFIABLE``), never guessed. A property
  the observer could not see is not a confident answer.
- **Versioned and canonicalizable.** :meth:`TrustMap.to_dict` is stable and
  :meth:`TrustMap.canonical_digest` commits to it, so two hosts render the same
  map for the same stored claim.

The map is the read model behind ``mareforma map`` and the trust section of
``mareforma verify``.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum

from ._canonical import canonicalize

# Version of the trust-map shape. Bound into the rendered record so a consumer
# knows which property set + tier semantics produced it, and a future revision
# is distinguishable rather than silently reinterpreted.
TRUST_MAP_VERSION = "v0.3.13"

# Observed-grounding axis versions KNOWN to carry the verdict↔citation binding.
# An ALLOWLIST, not a denylist: only a GROUNDED verdict stamped with one of these
# is presented as a bound GROUNDED. Anything else, a missing/absent version, a
# hand-edited record, an older axis, or a future axis that drops binding, reads
# as pre-binding ("citation binding not checkable"), which is the honest, fail-
# safe default. A denylist would let an unknown/absent version overclaim as bound.
_BINDING_AXIS_VERSIONS = frozenset({"v0.3.9", "v0.3.11"})

# The rendered string for a GROUNDED verdict computed on a pre-binding axis. A
# golden-file test pins this exact text; do not reword without updating it.
PRE_BINDING_GROUNDED_LABEL = "GROUNDED (pre-binding axis; citation binding not checkable)"

# The value an axis carries when what it read is not a weaker answer but a
# broken one: a planted trust root, a write guard that was missing on open, a
# contradiction record the signed verdicts do not support, a transparency-log
# entry that does not verify. Spelled once so the axes and every renderer agree
# on it, because a renderer that does not recognise the word paints a tamper
# report in the colour it uses for a healthy computed axis, which is what both
# of them did.
#
# Distinct from ``mareforma._verify.TAMPERED`` and deliberately not merged with
# it. That one is a claim VERDICT ("tampered", lowercase) and this is the state
# of one axis of a map; a claim can carry a tampered axis and still be the wrong
# thing to hand a caller as a verdict, which is the whole reason the substrate
# axis reaches the verdict as UNVERIFIABLE rather than as this.
TAMPERED_VALUE = "TAMPERED"


def is_tamper_value(value) -> bool:
    """True when an axis value reports a broken substrate rather than a weak one.

    The one predicate every renderer asks, so "does this need to look alarming"
    cannot be answered differently in the terminal and in the HTML view.
    """
    return value == TAMPERED_VALUE


# Rendered when a claim carries no stored value for a property that would
# otherwise be computed (a pre-observer claim has no grounding verdict). Never
# inferred to a confident answer.
NOT_PRESENT = "not present"

# The placeholder every renderer substitutes for an absent value, so a blank
# cell always means a rendering failure and never an absent value. One spelling
# for every renderer, so the text and HTML views of one map agree. A golden-file
# test pins this exact text; do not reword without updating it.
ABSENT_VALUE = "n/a"

# The faithfulness verdicts the map will place. An ALLOWLIST: only these three
# render as a faithfulness signal; any other value in a supplied record reads as
# "not present", the honest fail-safe (a hand-edited or future-shaped record does
# not overclaim a verdict the map does not understand).
_FAITHFULNESS_VERDICTS = frozenset({"REPRODUCED", "DIVERGED", "COULD_NOT_REEXECUTE"})

# Prepended to a faithfulness residual so the PROXY signal can never be read as
# truth or as independence, whatever the verdict.
_FAITHFULNESS_PROXY_NOTE = (
    "re-execution proxy: reproducible is not correct, and a same-arm re-run is "
    "not an independent line of evidence"
)

# What the schema census does not reach, on the axis that reports what it does.
#
# The census records write guards, which are reconciled on every open, so the
# census is what remembers a guard that came back. The full-text index is
# neither. It is built once, by a script that does not run again on an
# initialised graph, and nothing reconciles it, so there is no repair for the
# census to be the memory of.
#
# Measured, both states. Dropped outright, the next write and the next search
# both fail with SQLite's own message, and no open puts it back. Emptied in
# place, writes keep working and later claims are indexed as usual, so search
# answers and its answer is short by the rows that were removed, while ``query``
# returns all of them. The second is the dangerous one: it looks like a result.
#
# Named here rather than repaired. Rebuilding an index from the claims table is
# a write on the read path, and this axis reports rather than repairs.
CENSUS_REACH_RESIDUAL = (
    "the census covers write guards and not the search index, which is built "
    "once and never reconciled: rows can be taken out of it with nothing "
    "recorded here, and search then answers short rather than failing, so use "
    "query rather than search where completeness matters"
)


class TrustMapVersionError(RuntimeError):
    """The trust-map code version disagrees with the package it ships inside.

    ``TRUST_MAP_VERSION`` witnesses this module's property set and tier
    semantics; ``mareforma.__version__`` names the package the module is packaged
    within. A build that ships a stale ``trust_map`` beside a differently
    versioned package renders a map whose logic does not match the version it
    reports, so a residual can be under-named while the map still reads as
    authoritative. The map builder fails closed on that state instead of
    presenting a map whose honesty it cannot vouch for.
    """


def _require_consistent_version() -> None:
    """Fail closed unless the trust-map code version matches the package version.

    Refusing here keeps a drifted build from silently emitting a trust map: an
    inconsistent build cannot promise that its independence residual (or any
    axis) matches the version it stamps, so it must not present one.
    """
    from mareforma import __version__ as package_version

    stamped = TRUST_MAP_VERSION.removeprefix("v")
    if stamped != package_version:
        raise TrustMapVersionError(
            f"trust-map code version {TRUST_MAP_VERSION!r} does not match package "
            f"version {package_version!r}: this build is inconsistent, so its "
            "trust map cannot be trusted to match the shipped logic"
        )


class Tier(str, Enum):
    """Where a property's answer comes from, the honesty of the signal.

    - ``COMPUTED`` , derived directly from stored evidence this release.
    - ``PROXIED``  , computed through a proxy signal whose bound is named
                      (e.g. a file read observed by stat, not by byte).
    - ``DEFERRED`` , not evaluated this release; the residual is named so the
                      gap is explicit rather than silent.
    """

    COMPUTED = "COMPUTED"
    PROXIED = "PROXIED"
    DEFERRED = "DEFERRED"


@dataclass(frozen=True)
class TrustProperty:
    """One property of a claim's trust, placed at its tier with the residual.

    ``name`` is the property (``grounding``, ``independence``, …). ``tier`` is
    where the answer comes from. ``value`` is the property's state, a verdict, a
    count, a level, or ``None`` / :data:`NOT_PRESENT` when there is nothing to
    show. ``residual`` names what the answer does NOT cover: the honest bound on
    a computed value, or the reason a deferred property is deferred.
    """

    name: str
    tier: Tier
    value: str | None
    residual: str

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "tier": self.tier.value,
            "value": self.value,
            "residual": self.residual,
        }


@dataclass(frozen=True)
class TrustMap:
    """A claim's trust as a placed, honest ledger of properties."""

    version: str
    subject_kind: str
    subject_id: str
    properties: tuple[TrustProperty, ...]

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "subject_kind": self.subject_kind,
            "subject_id": self.subject_id,
            "properties": [p.to_dict() for p in self.properties],
        }

    def canonical_digest(self) -> str:
        """``sha256:<hex>`` over the canonical map bytes (RFC 8785)."""
        return "sha256:" + hashlib.sha256(canonicalize(self.to_dict())).hexdigest()

    def get(self, name: str) -> "TrustProperty | None":
        for p in self.properties:
            if p.name == name:
                return p
        return None


def parse_grounding_record(value) -> "dict | None":
    """Coerce a stored ``observed_grounding`` value into a record dict, or None.

    ``get_claim`` returns the raw column (a JSON string), while some callers
    already hold a decoded dict. One parser keeps the map, ``verify``, and the
    graph read path from drifting on how a malformed record is treated (→ None,
    never a partial dict).
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        import json as _json

        try:
            parsed = _json.loads(value)
        except (ValueError, TypeError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def display_value(value) -> str:
    """Render a property value for a reader, an absent one as ``n/a``.

    One placeholder for every renderer of a map, text and HTML alike, so two
    views of the same property cannot disagree on how absence reads.
    """
    return ABSENT_VALUE if value is None else str(value)


def _short(keyid: str | None) -> str:
    """First 12 hex chars of a keyid for display, or a placeholder."""
    if not keyid:
        return ABSENT_VALUE
    return f"{keyid[:12]}…"


def _source_strings(value: object) -> list[str]:
    """A stored source list as display strings, tolerating a tampered shape.

    A hand-built or tampered record can hold an unhashable element or a value
    that is not a list at all where a list of paths belongs. The map has to
    render what it reads, so anything else degrades to its string form instead
    of raising out of ``build_trust_map`` and taking down verify/map for the
    claim.
    """
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    return [str(value)]


def _grounding_property(claim: dict) -> TrustProperty:
    """Place the observed-grounding axis, carrying the verdict reason + cited set.

    A pre-observer claim (no stored verdict) renders ``not present``, never
    inferred. A GROUNDED verdict on a pre-binding axis renders with the
    pre-binding label so an auditor sees the citation binding was not checkable
    when it was computed.
    """
    record = parse_grounding_record(claim.get("observed_grounding"))
    if not isinstance(record, dict) or not record.get("grounding"):
        return TrustProperty(
            name="grounding",
            tier=Tier.COMPUTED,
            value=NOT_PRESENT,
            residual=(
                "no observed-grounding verdict on this claim; it predates the "
                "execution-observed axis (grounding was never computed for it)"
            ),
        )
    state = record.get("grounding")
    reason = record.get("reason") or ""
    cited = _source_strings(record.get("cited_sources"))
    grounded_raw = record.get("grounded_sources")
    grounded = None if grounded_raw is None else _source_strings(grounded_raw)
    version = record.get("version")
    pre_binding = version not in _BINDING_AXIS_VERSIONS
    # A plain file read is a stat-based proxy (opened + non-empty), not a
    # byte-level proof, so a GROUNDED file verdict is PROXIED; the observer's
    # own reason carries the specifics.
    tier = Tier.COMPUTED
    if state == "GROUNDED":
        value = PRE_BINDING_GROUNDED_LABEL if pre_binding else "GROUNDED"
        tier = Tier.PROXIED
    else:
        value = state
    # Surface the GROUNDED set (the sources a read was actually observed for and
    # the binding is checked against), not just the declared cite set: showing
    # only the declared set would imply every cited source was grounded when only
    # the read-observed subset was. Name the declared set separately when it is
    # wider, so the gap is visible, not hidden.
    if grounded is not None:
        note = (
            f"; grounded on: {', '.join(grounded)}" if grounded
            else "; grounded on: (no cited read observed)"
        )
        if cited and set(cited) - set(grounded):
            note += f"; declared cited (not all read-verified): {', '.join(cited)}"
    else:
        note = (
            f"; declared cited set, binding not checkable: {', '.join(cited)}"
            if cited else "; cited set: (none recorded)"
        )
    residual = f"{reason}{note}" if reason else f"observed axis{note}"
    return TrustProperty(name="grounding", tier=tier, value=value, residual=residual)


def _faithfulness_property(reexec_record: "dict | None") -> TrustProperty:
    """Place the re-execution faithfulness axis (a PROXY-tier signal).

    Faithfulness is not stored on the claim; it is supplied by a re-execution
    run (see :mod:`mareforma.reexec`). When no run is supplied the axis renders
    ``not present`` (never inferred): faithfulness was not checked. When a run is
    supplied the verdict, REPRODUCED / DIVERGED / COULD_NOT_REEXECUTE, is placed
    at the PROXY tier with the residual naming what reproducibility does NOT
    cover, so it cannot be read as truth or as independence. A malformed or
    unrecognised record reads as ``not present``, the fail-safe default.
    """
    record = parse_grounding_record(reexec_record)
    verdict = record.get("verdict") if isinstance(record, dict) else None
    if verdict not in _FAITHFULNESS_VERDICTS:
        return TrustProperty(
            name="faithfulness",
            tier=Tier.COMPUTED,
            value=NOT_PRESENT,
            residual=(
                "no re-execution recorded for this claim; whether the recorded "
                "pipeline reproduces its number was not checked (a reproducibility "
                "proxy, not correctness or independence)"
            ),
        )
    reason = record.get("residual") or ""
    residual = f"{_FAITHFULNESS_PROXY_NOTE}; {reason}" if reason else _FAITHFULNESS_PROXY_NOTE
    return TrustProperty(
        name="faithfulness",
        tier=Tier.PROXIED,
        value=verdict,
        residual=residual,
    )


def _independence_property(
    claim: dict, n_roots: int, effective: "dict | None" = None,
) -> TrustProperty:
    """Place the INDEPENDENCE axis, distinct from the support ladder.

    For a finding (``effective`` supplied, the effective-independence record
    from :func:`mareforma.trust._store.effective_independence`), the axis reports
    the per-finding effective number of pairwise-distinct (model, data, signer)
    supporting checks. Where a supporting line's model lineage is soft (PROXY /
    UNVERIFIABLE) and no clean pair corroborates, the axis reads UNVERIFIABLE
    rather than a confident number, a distinct model cannot be certified.
    Coarse by design: distinct-model is binary this release; the graded
    cross-model residual is DEFERRED, not computed. When the number stands but
    every signer traces to a single trust root (``n_roots < 2``), the residual
    names that operator-Sybil topology: the operator owns every enrolled key, so
    every axis is operator-assertable (the signer keys are mintable and the model
    lineage is signed by the operator's own key, so a distinct model is not
    cross-checked by an independent party). The number is producer-assertable
    within one trust domain, not certified independence across operators.

    For a non-finding claim (``effective`` is ``None``), the axis falls back to
    the graph-level validator-root topology disclosure. Distinctness that rests
    on operator-mintable keys alone is UNVERIFIABLE: one operator can mint any
    number of keys, so "two distinct signers" proves nothing about independent
    lines of evidence when they all trace to one trust root. Fewer than two
    enrolled roots (zero or one) is that unverifiable case; only two or more is
    the weak-convergence-prior case, and even then the map does not translate a
    convergence marker into the word "independent". The residual says it is a
    topology disclosure, not a per-claim measurement.
    """
    if effective is not None:
        number = int(effective.get("number", 0))
        # A line the shared verifier dropped (an unauthenticated signer, a
        # withdrawn claim, an un-gateable or repointed line) is not in the number.
        # Surface the count so the independence axis is not read as confident off a
        # line set that silently lost lines.
        skipped = int(effective.get("lines_skipped", 0))
        skip_note = (
            f"; {skipped} evidence line(s) were dropped from the count and "
            "disclosed (unauthenticated signer, withdrawn claim, or un-gateable "
            "line)" if skipped else ""
        )
        if effective.get("soft") and number < 2:
            return TrustProperty(
                name="independence",
                tier=Tier.COMPUTED,
                value="UNVERIFIABLE",
                residual=(
                    "a supporting line's model lineage is PROXY/UNVERIFIABLE, or "
                    "the line observed no model call at all, so a distinct model "
                    "cannot be certified; a human signer does not lift it, "
                    "validator_type is self-declared and no person attested to "
                    "the finding; independent corroboration is unverifiable "
                    "(per-finding model/data/signer axis)" + skip_note
                ),
            )
        residual = (
            f"{number} pairwise-distinct (model, data, signer) supporting "
            "check(s); coarse by design: distinct-model is binary this "
            "release, the graded cross-model residual is DEFERRED, not "
            "computed" + skip_note
        )
        # Operator-Sybil disclosure: under a single trust root the operator owns
        # every enrolled key, so every axis of distinctness is operator-assertable,
        # not just the signer. The signer keys are operator-mintable, and the
        # model lineage each finding binds is signed by the operator's own
        # enrolled key, so a distinct model is not cross-checked by an independent
        # party: the operator can re-sign a fabricated lineage under a key it
        # controls. The number is producer-assertable within one trust domain,
        # not certified cross-model independence, so the residual names it; a
        # certified number needs distinct trust roots.
        if n_roots >= 2:
            # A number computed on a broken substrate is worse than no number.
            # Every count on this axis rests on is_enrolled, and is_enrolled
            # walks the chain through _count_self_signed_rows, which refuses the
            # whole table the moment a second self-signed row exists. So with
            # two roots the count was assembled from checks that all answered
            # False, and printing it invites the reader to trust the one figure
            # the tamper guarantees is meaningless. The number is kept in the
            # residual for forensics, not offered as the value.
            return _multi_root_is_tamper(f"; the discarded count was {number}")
        # Only zero or one root reaches here, since two or more returned above.
        # The condition this used to test is now the shape of the function.
        detail = (
            "no trust root is enrolled" if n_roots == 0
            else "all validators trace to a single trust root"
        )
        residual += (
            f"; {detail}, so every axis of distinctness is "
            "operator-assertable: the signer keys are operator-mintable and "
            "the model lineage is signed by the operator's own key, so a "
            "distinct model is not cross-checked by an independent party. "
            "The count is producer-assertable within one trust domain, not "
            "certified independence across operators"
        )
        return TrustProperty(
            name="independence",
            tier=Tier.COMPUTED,
            value=str(number),
            residual=residual,
        )
    if n_roots < 2:
        detail = (
            "no trust root is enrolled" if n_roots == 0
            else "all validators trace to a single trust root"
        )
        return TrustProperty(
            name="independence",
            tier=Tier.COMPUTED,
            value="UNVERIFIABLE",
            residual=(
                f"{detail}; distinctness rests on operator-mintable keys alone, so "
                "independent lines of evidence cannot be verified (graph-level "
                "validator topology, not a per-claim measure)"
            ),
        )
    return _MULTI_ROOT_IS_TAMPER


def _multi_root_is_tamper(extra: str = "") -> TrustProperty:
    """The independence axis when a second self-signed root exists.

    There is no legitimate multi-root state in this product, which is why this
    reads as tamper rather than as the weak convergence prior it used to claim.
    Three places say so independently: ``validators._verify_chain`` refuses
    every keyid in the table when more than one self-signed row is present,
    ``validators.trust_domain_root`` answers None for the same condition, and
    ``db.restore`` refuses a backup that carries a second root outright. No
    code path enrols one.

    A writer with SQL access, on the other hand, can INSERT one: the validators
    table blocks UPDATE and DELETE and permits INSERT. That single statement
    turns every enrolment check in the graph False while the old value of this
    axis moved UP, from the single-domain disclosure to a convergence prior. An
    axis that improves when the substrate breaks is worse than no axis.
    """
    return TrustProperty(
        name="independence",
        tier=Tier.COMPUTED,
        value=TAMPERED_VALUE,
        residual=(
            "more than one self-signed root is enrolled, which no code path "
            "creates; the chain walk therefore refuses every keyid in the "
            "table, so no enrolment-dependent count on this axis means "
            "anything" + extra
        ),
    )


_MULTI_ROOT_IS_TAMPER = _multi_root_is_tamper()


def _standing_property(claim: dict) -> TrustProperty:
    """Place standing / ratification: the computed gate, human-in-the-loop by design."""
    level = claim.get("support_level") or "PRELIMINARY"
    verified = claim.get("verified")
    if level == "ESTABLISHED":
        detail = (
            "ratified to ESTABLISHED by a signed human-validator envelope"
            if verified
            else "marked ESTABLISHED but the validation envelope did not verify on read"
        )
    elif level == "REPLICATED":
        detail = "REPLICATED by distinct-signer convergence; ratification to ESTABLISHED is human-in-the-loop by design"
    else:
        detail = "PRELIMINARY; no ratification gate cleared"
    return TrustProperty(
        name="standing",
        tier=Tier.COMPUTED,
        value=level,
        residual=detail,
    )


def _witnessing_property(
    claim: dict, has_inclusion: bool,
    inclusion: "tuple[str, str] | None" = None,
) -> TrustProperty:
    """Place witnessing honestly against the actual transparency-log inclusion.

    ``transparency_logged`` defaults to 1 even when no transparency log is in
    use (a signed claim REPLICATES on the local signature alone), so the flag
    alone cannot be read as "witnessed." The map keys off whether an actual
    inclusion record exists: present → witnessed; a set flag with no inclusion →
    not gated on witnessing (the log was disabled); a cleared flag → inclusion
    pending.
    """
    logged = claim.get("transparency_logged")
    signed = claim.get("signature_bundle")
    if not signed:
        return TrustProperty(
            name="witnessing",
            tier=Tier.COMPUTED,
            value=NOT_PRESENT,
            residual="unsigned claim; nothing to witness in a transparency log",
        )
    if has_inclusion:
        # The record used to be reported as though its existence were the
        # evidence. rekor_inclusions refuses UPDATE and DELETE and permits
        # INSERT, so a row carrying a junk proof reaches this branch exactly as
        # a real one does, and the proof was checked at restore and never again.
        #
        # It is checked here now, against the log key pinned for this project,
        # and the three outcomes are kept apart. Verified is the only one that
        # earns the word. A proof that fails is a definite finding. And with no
        # pinned key nothing was checked at all, which is neither evidence for
        # the entry nor against it, and is the state most projects are in.
        state, detail = inclusion or (_INCLUSION_NO_KEY, "not re-checked")
        if state == _INCLUSION_VERIFIED:
            return TrustProperty(
                name="witnessing",
                tier=Tier.COMPUTED,
                value="inclusion proof verified",
                residual=(
                    "signed, and the stored transparency-log entry was "
                    "re-verified on this read: " + detail
                ),
            )
        if state == _INCLUSION_FAILED:
            return TrustProperty(
                name="witnessing",
                tier=Tier.COMPUTED,
                value=TAMPERED_VALUE,
                residual=(
                    "a transparency-log inclusion record is stored and it does "
                    "not verify against the pinned log key (" + detail
                    + "); the table permits INSERT, so a row witnessing nothing "
                    "reaches a read looking exactly like one that does"
                ),
            )
        return TrustProperty(
            name="witnessing",
            tier=Tier.COMPUTED,
            value="inclusion record present, unchecked",
            residual=(
                "signed, and a transparency-log inclusion record is stored, but "
                "it was not re-checked on this read: " + detail
            ),
        )
    if logged == 1:
        return TrustProperty(
            name="witnessing",
            tier=Tier.COMPUTED,
            value="not witnessed",
            residual=(
                "signed but no transparency-log inclusion; the log was not enabled, "
                "so the top of the support ladder is unreachable (it requires witnessing)"
            ),
        )
    return TrustProperty(
        name="witnessing",
        tier=Tier.COMPUTED,
        value="pending",
        residual="signed; transparency-log inclusion is pending retry",
    )


def build_trust_map(
    conn,
    claim_id: str,
    *,
    reexec_record: "dict | None" = None,
    disclose=None,
) -> "TrustMap | None":
    """Build the trust map for a stored claim, or ``None`` if it does not exist.

    ``conn`` is an open graph connection. ``reexec_record`` optionally carries
    a re-execution faithfulness verdict (from :meth:`mareforma.reexec.ReexecResult.to_map_record`)
    to place on the map's PROXY-tier faithfulness axis; when omitted the axis
    reads ``not present``. ``disclose`` optionally carries the graph's
    :class:`mareforma.trust._store.SkipDisclosure` so a line the independence axis
    drops is recorded on the health channel, the same disclosure the read path
    threads; when omitted the axis still counts and reports ``lines_skipped`` but
    emits no health event.
    """
    from mareforma.db import get_claim

    claim = get_claim(conn, claim_id)
    if claim is None:
        return None
    from mareforma import validators as _validators

    n_roots = len(_validators.enrollment_roots(conn))
    has_inclusion = _has_rekor_inclusion(conn, claim_id)
    # Attributability must reflect an ACTUAL signature check, not the promotion
    # gate: get_claim's ``verified`` passes PRELIMINARY rows through True without
    # re-verifying, so trusting it would make the map assert "signature
    # re-verified on read" for a signed PRELIMINARY claim it never checked (and
    # miss a tamper). Run the audit-grade, tier-independent re-verification here,
    # the same one ``mareforma verify`` uses, so the standalone map is honest.
    sig_verified = None
    asserter_enrolled = None
    # EITHER column, not both. Gating on both let a row carrying a stapled
    # ``asserter_keyid`` and no bundle skip the check entirely, so
    # ``att_verified`` fell back to the stored ``verified`` gate below, which
    # get_claim passes through True for PRELIMINARY rows. The map then read
    # "signature re-verified on read" beside a keyid, for a claim with no
    # signature at all, while ``mareforma verify`` called the same claim
    # tampered. The MCP server now exposes this map standalone, with no verdict
    # beside it, so the disagreement had nothing to correct it.
    if claim.get("asserter_keyid") or claim.get("signature_bundle"):
        from mareforma.db import (
            _extract_signature_bundle_keyid,
            verify_claim_signatures,
        )
        from mareforma.validators import is_enrolled

        sig_verified, _ = verify_claim_signatures(conn, claim)
        # verify_claim_signatures returns (True, "") for a non-enrolled asserter:
        # it can only check the claim-binding, never the signature against a
        # pubkey (the lean model has no key to check it against). Tell the two
        # apart so the map does not claim "re-verified" for a binding-only pass.
        #
        # Read enrolment on the signer the BUNDLE names, the same keyid
        # verify_claim_signatures checks, not the row's unsigned column: a row
        # whose column disagrees with its envelope is refused above, and a row
        # with no bundle has no signer to look up.
        bundle_keyid = _extract_signature_bundle_keyid(
            claim.get("signature_bundle")
        )
        if bundle_keyid is not None:
            asserter_enrolled = is_enrolled(conn, bundle_keyid)
    effective = _effective_independence(conn, claim_id, disclose=disclose)
    from mareforma.db.core import refutation_status, schema_census_missing

    return _assemble(
        claim, n_roots, has_inclusion,
        sig_verified=sig_verified, asserter_enrolled=asserter_enrolled,
        reexec_record=reexec_record,
        effective_independence=effective,
        census_missing=schema_census_missing(conn),
        refutation_contestation=refutation_status(claim, conn),
        inclusion=(_recheck_inclusion(conn, claim_id, claim)
                   if has_inclusion else None),
    )


def _effective_independence(conn, claim_id: str, *, disclose=None) -> "dict | None":
    """The effective-independence record for a finding claim, or None.

    A claim is a finding when a ``findings`` row binds it to a proposition; the
    independence axis then reports the per-finding effective number over that
    proposition's evidence lines. A plain claim (no finding row, or a graph whose
    schema predates the evidence tree) has no such number, so the axis falls back
    to the validator-topology disclosure. ``disclose`` threads the health channel
    through so a line the independence count drops is recorded there.
    """
    import sqlite3

    try:
        row = conn.execute(
            "SELECT content_id FROM findings WHERE claim_id = ? LIMIT 1",
            (claim_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    from mareforma.trust._store import effective_independence

    return effective_independence(conn, row["content_id"], disclose=disclose)


# What a stored inclusion record is worth on this read, and the three answers
# are genuinely different. The proof is only checkable against the transparency
# log's own public key, and mareforma never fetches that during a read: the key
# is pinned at <root>/.mareforma/rekor_log_pubkey.pem on the first open that is
# handed one, and a read with no pinned key has nothing to check against. So the
# axis is key-conditional, and it says which of the three it is rather than
# collapsing "nobody could check" into "checked".
_INCLUSION_VERIFIED = "verified"
_INCLUSION_NO_KEY = "no-log-key"
_INCLUSION_FAILED = "failed"


def _pinned_log_pubkey(conn) -> "bytes | None":
    """The transparency log's pinned public key for this project, or None.

    Read off the connection's own file rather than taken as an argument,
    because every caller of the map builder would otherwise have to know about
    Rekor to ask a question about witnessing. ``PRAGMA database_list`` names the
    main database file, and the project root is its grandparent.
    """
    from pathlib import Path

    try:
        for _, name, path in conn.execute("PRAGMA database_list"):
            if name == "main" and path:
                pem = Path(path).parent / "rekor_log_pubkey.pem"
                return pem.read_bytes() if pem.is_file() else None
    except Exception:
        return None
    return None


def _recheck_inclusion(conn, claim_id: str, claim: dict) -> "tuple[str, str]":
    """Re-verify the stored inclusion proof; return ``(state, detail)``.

    ``rekor_inclusions`` refuses UPDATE and DELETE and permits INSERT, so a row
    carrying a junk proof reaches a read exactly as a real one does, and the
    axis used to report both as "an inclusion record is stored". The proof was
    checked at restore and never again, which is to say never, for anyone whose
    graph was not restored.

    Three answers, and the middle one is the point. With a pinned log key the
    proof either verifies end to end (Merkle path, the log's signed checkpoint,
    and the binding to this claim's own envelope) or it does not, and a failure
    is a definite finding rather than a weaker record. With no pinned key
    nothing was checked, and saying so is the honest report: it is not evidence
    against the entry, and it is not evidence for it either.
    """
    import base64
    import json as _json
    import sqlite3

    try:
        row = conn.execute(
            "SELECT raw_response_b64 FROM rekor_inclusions WHERE claim_id = ?",
            (claim_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return _INCLUSION_NO_KEY, "no inclusions table on this schema"
    if row is None:
        return _INCLUSION_NO_KEY, "no inclusion record"

    pem = _pinned_log_pubkey(conn)
    if pem is None:
        return _INCLUSION_NO_KEY, (
            "no transparency-log public key is pinned for this project, so the "
            "proof could not be checked against the log that would have to "
            "stand behind it; pin one with mareforma.open(rekor_log_pubkey_pem=)"
        )

    from mareforma.signing.rekor import RekorInclusionError, verify_rekor_inclusion

    try:
        body = _json.loads(base64.standard_b64decode(row[0]))
        envelope = _json.loads(claim.get("signature_bundle") or "null")
    except Exception:
        return _INCLUSION_FAILED, (
            "the stored Rekor response is not readable as an entry, so the "
            "record witnesses nothing"
        )
    if not isinstance(envelope, dict):
        return _INCLUSION_NO_KEY, (
            "the claim carries no signature envelope for the entry to bind to"
        )
    try:
        verify_rekor_inclusion(body, pem, envelope)
    except RekorInclusionError as exc:
        return _INCLUSION_FAILED, f"{exc.reason}: {exc}"
    except Exception as exc:                                # pragma: no cover
        return _INCLUSION_FAILED, f"the proof could not be checked: {exc}"
    return _INCLUSION_VERIFIED, (
        "the Merkle inclusion path, the log's signed checkpoint and the "
        "binding to this claim's envelope all check out against the pinned "
        "log key"
    )


def _has_rekor_inclusion(conn, claim_id: str) -> bool:
    """True iff a transparency-log inclusion record exists for this claim."""
    import sqlite3

    try:
        row = conn.execute(
            "SELECT 1 FROM rekor_inclusions WHERE claim_id = ? LIMIT 1",
            (claim_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        # No inclusions table (older schema): treat as no inclusion.
        return False
    return row is not None


def _assemble(
    claim: dict, n_roots: int, has_inclusion: bool, *, sig_verified: "bool | None" = None,
    asserter_enrolled: "bool | None" = None, reexec_record: "dict | None" = None,
    effective_independence: "dict | None" = None,
    census_missing: "tuple[str, ...]" = (),
    refutation_contestation: "dict | None" = None,
    inclusion: "tuple[str, str] | None" = None,
) -> TrustMap:
    """Assemble a TrustMap from an already-fetched claim dict (pure).

    ``n_roots`` is the number of self-signed trust roots. Zero or one means
    independence cannot be verified; two or more is not a stronger case but a
    tamper report, because no code path enrols a second root and the chain walk
    refuses the whole table once one exists. ``census_missing`` names write
    guards a previous open found absent, which the read path cannot re-derive
    because they heal silently on the way in. ``sig_verified`` is the result of an ACTUAL
    audit-grade signature re-verification (``verify_claim_signatures``); when
    ``None`` (a direct caller that did not run one) it falls back to the stored
    ``verified`` column, which is the support-level read gate, NOT a signature
    check on PRELIMINARY rows. ``asserter_enrolled`` is ``False`` when the signed
    asserter is not an enrolled validator: ``verify_claim_signatures`` passes
    (binding only, no pubkey to check against), so the map must not claim the
    signature was cryptographically re-verified.
    """
    # Refuse before stamping: a build whose trust-map code drifted from the
    # package version cannot vouch that this map's residuals match the shipped
    # logic, so it fails closed rather than present a possibly under-named axis.
    _require_consistent_version()
    from mareforma.db import REPLAY_TAMPER_SIGNALS, refutation_from_column

    supports = claim.get("supports_json")
    contradicts = claim.get("contradicts_json")
    try:
        import json as _json

        n_supports = len(_json.loads(supports or "[]"))
        n_contradicts = len(_json.loads(contradicts or "[]"))
    except (ValueError, TypeError):
        n_supports = n_contradicts = 0

    asserter = claim.get("asserter_keyid")
    # An actual signature re-verification if one was run, else the stored gate.
    att_verified = sig_verified if sig_verified is not None else claim.get("verified")
    attributability = TrustProperty(
        name="attributability",
        tier=Tier.COMPUTED,
        value=(_short(asserter) if asserter else "unsigned"),
        residual=(
            "no signature; the asserter is not cryptographically bound" if not asserter
            else "asserter signature present, but the asserter is not an enrolled "
                 "validator, so the signature was not cryptographically re-verified"
                 if asserter_enrolled is False
            else "signature re-verified on read" if att_verified
            else "signature failed re-verification on read"
        ),
    )

    provenance = TrustProperty(
        name="provenance",
        tier=Tier.COMPUTED,
        value=f"{n_supports} supports / {n_contradicts} contradicts",
        residual=(
            "the declared provenance graph the asserter recorded; a declaration, "
            "not proof that the cited upstreams were used"
        ),
    )

    grounding = _grounding_property(claim)

    faithfulness = _faithfulness_property(reexec_record)

    methodological = TrustProperty(
        name="methodological_validity",
        tier=Tier.COMPUTED,
        value=claim.get("classification") or "INFERRED",
        residual=(
            "declared classification; bearing (supports/refutes/neutral vs a "
            "registered prediction) is computed for findings that carry an "
            "effect estimate and prediction"
        ),
    )

    leakage = TrustProperty(
        name="leakage",
        tier=Tier.DEFERRED,
        value=None,
        residual=(
            "partition/held-out independence is not evaluated; a finding may "
            "reuse data it should have held out and this map would not show it"
        ),
    )

    independence = _independence_property(claim, n_roots, effective_independence)

    # The contestation axis reads the replay when the builder was handed a
    # graph, and the column when it was not. A disagreement between the two is
    # a tamper report rather than a weaker contradiction: t_invalid carries no
    # trigger, so one UPDATE fabricates a contradiction with no verdict behind
    # it or erases a real one from every read surface, and either way the axis
    # used to render the edit as though it were the finding.
    ref = refutation_contestation
    if ref is None:
        ref = refutation_from_column(claim)
    tampered = ref["signal"] in REPLAY_TAMPER_SIGNALS
    contestation = TrustProperty(
        name="contestation",
        tier=Tier.COMPUTED,
        value=TAMPERED_VALUE if tampered else ref["state"],
        residual=f"{ref['reason']} (signal: {ref['signal']})",
    )

    standing = _standing_property(claim)

    # The substrate axis. Two conditions make it a tamper report rather than a
    # disclosure, and both are things a SQL writer can do that no code path
    # does: plant a second self-signed root, or drop a write guard. Neither is
    # visible on any other axis, and the second is invisible by the time a read
    # happens, because two repairs run silently on the way in. That is what the
    # census exists to have written down beforehand.
    if n_roots >= 2 or census_missing:
        reasons = []
        if n_roots >= 2:
            reasons.append(
                f"{n_roots} self-signed roots are enrolled and no code path "
                "creates a second one, so every enrolment check in this graph "
                "now fails"
            )
        if census_missing:
            reasons.append(
                "a write guard was found missing on open: "
                + ", ".join(census_missing)
                + ". Whatever it permitted while it was down is not "
                "recoverable, and a guard that came back is not a guard that "
                "was never gone"
            )
        reasons.append(CENSUS_REACH_RESIDUAL)
        trust_root = TrustProperty(
            name="trust_root",
            tier=Tier.COMPUTED,
            value=TAMPERED_VALUE,
            residual="; ".join(reasons),
        )
    else:
        trust_root = TrustProperty(
            name="trust_root",
            tier=Tier.DEFERRED,
            value=(
                "no trust root enrolled" if n_roots == 0
                else "single trust domain"
            ),
            residual=(
                "trust-root concentration is disclosed, not established: a private or "
                "externally-anchored root of trust is not evaluated this release; "
                + CENSUS_REACH_RESIDUAL
            ),
        )

    witnessing = _witnessing_property(claim, has_inclusion, inclusion)

    properties = (
        attributability,
        provenance,
        grounding,
        faithfulness,
        methodological,
        leakage,
        independence,
        contestation,
        standing,
        trust_root,
        witnessing,
    )
    return TrustMap(
        version=TRUST_MAP_VERSION,
        subject_kind="claim",
        subject_id=claim.get("claim_id") or "",
        properties=properties,
    )
