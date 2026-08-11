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
TRUST_MAP_VERSION = "v0.3.11"

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
        if n_roots < 2:
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
    return TrustProperty(
        name="independence",
        tier=Tier.COMPUTED,
        value="MULTI_ROOT",
        residual=(
            "more than one root of trust is enrolled; distinct signers under "
            "distinct roots is a weak convergence prior, not proof of independence "
            "(graph-level validator topology, not a per-claim measure)"
        ),
    )


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


def _witnessing_property(claim: dict, has_inclusion: bool) -> TrustProperty:
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
        # States what was observed, not what was proved. `has_inclusion` comes
        # from `SELECT 1 FROM rekor_inclusions`, so it answers "a record exists",
        # and the stored `raw_response_b64` is never opened here. The Merkle
        # proof is checked at restore, not on read, and the `rekor_inclusions`
        # triggers block UPDATE and DELETE but permit INSERT, so a row with a
        # junk proof reaches this branch. The old sentence asserted an inclusion
        # proof had been checked, which is a claim this function cannot make.
        return TrustProperty(
            name="witnessing",
            tier=Tier.COMPUTED,
            value="inclusion record present",
            residual=(
                "signed, and a transparency-log inclusion record is stored; "
                "the inclusion proof itself is not re-checked on read"
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
    return _assemble(
        claim, n_roots, has_inclusion,
        sig_verified=sig_verified, asserter_enrolled=asserter_enrolled,
        reexec_record=reexec_record,
        effective_independence=effective,
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
) -> TrustMap:
    """Assemble a TrustMap from an already-fetched claim dict (pure).

    ``n_roots`` is the number of enrolled trust roots. Fewer than two (zero or
    one) means independence cannot be verified; only two or more is the
    weak-convergence-prior case. ``sig_verified`` is the result of an ACTUAL
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
    from mareforma.db import refutation_status

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

    ref = refutation_status(claim)
    contestation = TrustProperty(
        name="contestation",
        tier=Tier.COMPUTED,
        value=ref["state"],
        residual=f"{ref['reason']} (signal: {ref['signal']})",
    )

    standing = _standing_property(claim)

    trust_root = TrustProperty(
        name="trust_root",
        tier=Tier.DEFERRED,
        value=(
            "no trust root enrolled" if n_roots == 0
            else "single trust domain" if n_roots == 1
            else "multiple roots"
        ),
        residual=(
            "trust-root concentration is disclosed, not established: a private or "
            "externally-anchored root of trust is not evaluated this release"
        ),
    )

    witnessing = _witnessing_property(claim, has_inclusion)

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
