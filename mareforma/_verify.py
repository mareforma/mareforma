"""Shared claim-verdict classification for the verify surfaces.

``mareforma verify`` (the CLI) and the read-and-verify server both turn a
stored claim into one of three verdicts: VERIFIED, TAMPERED, or UNVERIFIABLE.
The rule is identical across surfaces and must stay identical, so it lives here
once instead of being copied into each caller. The CLI wraps the verdict in an
exit code and a printed trust map; the server wraps it in a tool result. Both
run the same signature re-verification, the same enrolled-signer check, and the
same grounding-to-citation binding re-check against the frozen routine.

The split between the two failure verdicts is the contract a CI gate keys on:
TAMPERED (a definite NO) means something was checked and failed; UNVERIFIABLE
means material was missing and nothing could be checked. A claim that is both
tampered and signed by an unenrolled key is TAMPERED, because a definite NO
outranks missing material: a gate that only warns on the softer verdict must
not wave through a failure the check actually caught.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    import sqlite3

    from mareforma.trust_map import TrustMap

VERIFIED = "verified"
TAMPERED = "tampered"
UNVERIFIABLE = "unverifiable"


@dataclass(frozen=True)
class ClaimVerdict:
    """The verdict on one stored claim, verify-surface independent.

    ``reason`` is empty on a VERIFIED verdict and carries the joined problem or
    missing-material description otherwise. ``trust_map`` is the audit-grade map
    when the claim was located (every path but the earliest exits carries one),
    so the caller can render "who signed this and what backs it" alongside the
    verdict.
    """

    verdict: str
    reason: str
    trust_map: "TrustMap | None"

    @property
    def ok(self) -> bool:
        return self.verdict == VERIFIED


def classify_claim_verdict(
    conn: "sqlite3.Connection", claim: dict, target: str,
) -> ClaimVerdict:
    """Auditor-mode verdict on a located *claim*, from public material only.

    Re-verifies signatures on read, re-checks the grounding-to-citation binding
    against the frozen routine, and builds the trust map. Assumes the claim was
    already located; a missing claim or an unopenable project is the caller's to
    handle, because those are UNVERIFIABLE for reasons that never reach a map.
    """
    from mareforma.db import (
        _extract_signature_bundle_keyid,
        verify_claim_signatures,
    )
    from mareforma.observe._binding import check_grounding_binding
    from mareforma.trust_map import build_trust_map, parse_grounding_record

    # Two lists, because the exit-code contract splits on exactly this:
    # *problems* is a definite NO (TAMPERED), something was checked and failed;
    # *unchecked* is missing material (UNVERIFIABLE), something could not be
    # checked at all. A CI gate keys on the difference.
    problems: list[str] = []
    unchecked: list[str] = []
    # A claim carrying no signature at all reaches none of the checks below:
    # the read-flag arm needs a bundle, verify_claim_signatures answers (True,
    # "") when there is nothing to verify, and the enrolled-signer arm is gated
    # on the bundle naming a keyid. So the function fell through to VERIFIED and
    # `mareforma verify` exited 0 over a claim nothing had authenticated.
    #
    # Reachable with no database access and no attacker: mareforma.open(root)
    # then assert_claim() on a project where bootstrap was never run produces an
    # unsigned claim, and that is the path the quickstart teaches and the path
    # examples/06_ci_verify gates CI on. build_trust_map already got this right
    # and renders attributability as "unsigned"; the verdict simply never read
    # its own map.
    #
    # UNVERIFIABLE rather than TAMPERED: nothing was checked, so nothing was
    # caught. That is the same reading the unenrolled-signer arm below takes,
    # and it keeps the 1-vs-2 split meaning what the module docstring says it
    # means.
    if not claim.get("signature_bundle"):
        unchecked.append(
            "claim carries no signature, so nothing about it could be "
            "authenticated. Sign the claim (see `mareforma bootstrap`) to "
            "reach a real verdict."
        )
    # Signature re-verification. Two complementary checks:
    #  (a) the tier-gated read flag (ESTABLISHED validation envelope /
    #      REPLICATED participant bundle), and
    #  (b) an audit-grade, tier-INDEPENDENT re-check (signed-field binding +
    #      asserter + role signatures) that catches a tampered PRELIMINARY
    #      signed claim the flag would pass through.
    if claim.get("signature_bundle") and not claim.get("verified"):
        problems.append("signature failed re-verification on read")
    sig_ok, sig_reason = verify_claim_signatures(conn, claim)
    if not sig_ok:
        problems.append(sig_reason)
    # Auditor mode authenticates against enrolled validator pubkeys only. A
    # signed claim whose named signer is not enrolled cannot be authenticated
    # from public material, so reporting it VERIFIED would let a gate pass a
    # forged signature under a keyid the project never enrolled. It is not a
    # tamper verdict either: nothing was checked, so nothing was caught, and the
    # honest reading is missing material (UNVERIFIABLE). Read enrollment on the
    # signer the BUNDLE names, the same keyid verify_claim_signatures checks, not
    # the row's asserter_keyid column, so the two surfaces can never disagree.
    bundle_keyid = _extract_signature_bundle_keyid(claim.get("signature_bundle"))
    if bundle_keyid is not None:
        from mareforma.validators import is_enrolled

        if not is_enrolled(conn, bundle_keyid):
            unchecked.append(
                "signer keyid is not an enrolled validator, so the signature "
                "cannot be authenticated from public material. This is "
                "unverifiable, not a failure; enroll the signer's key to reach "
                "a verdict."
            )

    # Grounding-to-citation binding re-check. Bind on ``grounded_sources`` (the
    # cited sources a read was actually observed for), not the declared
    # ``cited_sources``, matching the write side and the verify-on-read path. A
    # producer who declares a dataset but reads only a decoy grounds on the
    # decoy, so the declared set would falsely MATCH. The check runs even when
    # the set is EMPTY, because "finding cites data + verdict grounded on none of
    # it" is itself a binding violation. A pre-binding verdict has no such field
    # and is annotated by the trust map, not failed here.
    grounding = parse_grounding_record(claim.get("observed_grounding"))
    if (
        isinstance(grounding, dict)
        and grounding.get("grounding") == "GROUNDED"
        and grounding.get("grounded_sources") is not None
    ):
        verdict_grounded = tuple(grounding.get("grounded_sources") or ())
        finding_sources = claim_bound_sources(claim)
        result = check_grounding_binding(verdict_grounded, finding_sources)
        if result.disjoint:
            problems.append(f"grounding binding violation: {result.reason}")

    # build_trust_map re-fetches the row and runs its own audit-grade signature
    # re-verification, so the standalone map is honest.
    tmap = build_trust_map(conn, target)

    # A definite NO outranks missing material: a claim that is both tampered and
    # signed by an unenrolled key is tampered.
    if problems:
        return ClaimVerdict(TAMPERED, "; ".join(problems), tmap)
    if unchecked:
        return ClaimVerdict(UNVERIFIABLE, "; ".join(unchecked), tmap)
    return ClaimVerdict(VERIFIED, "", tmap)


def claim_bound_sources(claim: dict) -> tuple[str, ...]:
    """The finding's bound data-source identifiers for the binding re-check.

    Read from the ``predicate_payload`` column and passed through
    :func:`mareforma.observe._binding.predicate_citation_sources`, the one rule
    the write side bound against and the verify-on-read path re-checks (see
    :func:`mareforma.db.restore._verify_grounding_binding_on_read`). NOT the
    claim's ``supports`` (claim-id / DOI upstreams that would never intersect a
    data-path set), and NOT ``source_name`` (a free-text label that never binds).
    A string-only ``data_id`` with no ``data_source`` yields an empty set, so the
    binding reads as ``not_applicable``.

    ``data_source`` is not a claim column, so the finding citation lives only in
    ``predicate_payload``; reading it from anywhere else silently no-ops the
    binding re-check. That column is a denormalisation the signed envelope does
    not cover, so the append-only trigger locks it on a signed row: without that
    lock, clearing it would empty this set and pass a violation as clean.
    """
    from mareforma.observe._binding import predicate_citation_sources

    raw = claim.get("predicate_payload")
    if not isinstance(raw, str):
        return ()
    try:
        predicate = json.loads(raw)
    except (ValueError, TypeError):
        return ()
    return predicate_citation_sources(predicate)
