"""
export_bundle.py: SCITT-style signed export bundle.

Wraps the JSON-LD graph export in an in-toto Statement v1 envelope:

    {
      "_type": "https://in-toto.io/Statement/v1",
      "subject": [
        {
          "name":   "urn:mareforma:claim:<uuid>",
          "digest": {"sha256": "<canonical_payload_hash>"}
        },
        ...
      ],
      "predicateType": "urn:mareforma:predicate:epistemic-graph:v1",
      "predicate": <JSON-LD export>
    }

The bundle is then signed by the local Ed25519 key using a DSSE-style
envelope. Verification checks the bundle signature AND every per-claim
signature inside ``predicate``.

Design choices (one-way doors, locked currently):

- **Subject naming**: ``urn:mareforma:claim:<uuid>``. The URN namespace
  is forever once published; consumers will key off this prefix.
- **predicateType**: ``urn:mareforma:predicate:epistemic-graph:v1``.
  URN (not DNS) defers a perpetual-ownership commitment on
  ``mareforma.dev`` for schema dereferencing. Evolution to v2 carries
  a new predicateType, leaving v1 verifiers intact.
- **Predicate body**: the existing JSON-LD export. No additional
  PROV-O modelling: the JSON-LD scoping rationale already covers
  why (see ``mareforma/exporters/jsonld.py`` module docstring).

The schema lives in ``docs/reference/scitt-bundle.md``.
"""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Any, Optional

from mareforma import __version__


STATEMENT_TYPE = "https://in-toto.io/Statement/v1"
PREDICATE_TYPE = "urn:mareforma:predicate:epistemic-graph:v1"
SUBJECT_PREFIX = "urn:mareforma:claim:"
BUNDLE_PAYLOAD_TYPE = "application/vnd.in-toto+json"


def _subject_for_claim(claim: dict) -> dict[str, Any]:
    """Build one in-toto subject entry from a claim row.

    Hash material: the canonical Statement v1 bytes of the claim (the
    same bytes the per-claim signature is computed over via DSSE PAE).
    Reusing ``signing.canonical_statement`` keeps bundle digests
    aligned with per-claim signatures: a downstream tool that
    re-derives the digest from the row's fields + evidence_json must
    agree with the bundle.
    """
    from mareforma import signing as _signing
    try:
        evidence_dict = json.loads(claim.get("evidence_json") or "{}")
    except (ValueError, TypeError):
        evidence_dict = {}
    chain_input = _signing.canonical_statement({
        "claim_id": claim["claim_id"],
        "text": claim["text"],
        "classification": claim["classification"],
        "generated_by": claim["generated_by"],
        "supports": json.loads(claim.get("supports_json") or "[]"),
        "contradicts": json.loads(claim.get("contradicts_json") or "[]"),
        "source_name": claim.get("source_name"),
        "artifact_hash": claim.get("artifact_hash"),
        "created_at": claim["created_at"],
    }, evidence_dict)
    digest = hashlib.sha256(chain_input).hexdigest()
    return {
        "name": f"{SUBJECT_PREFIX}{claim['claim_id']}",
        "digest": {"sha256": digest},
    }


def build_statement(root: Path) -> dict[str, Any]:
    """Build the in-toto Statement v1 for the graph at *root*.

    The Statement is unsigned: call :func:`sign_bundle` to produce
    the DSSE envelope.
    """
    from mareforma.db import open_db, list_claims
    from mareforma.exporters.jsonld import JSONLDExporter
    from mareforma import validators as _validators

    conn = open_db(root)
    try:
        claims = list_claims(conn)
        single_trust_domain = _validators.single_trust_domain(conn)
        trust_domain_root = _validators.trust_domain_root(conn)
        validator_rows = _validators.list_validators(conn)
    finally:
        conn.close()
    subjects = [_subject_for_claim(c) for c in claims]
    predicate = JSONLDExporter(root).export()
    # Carry each claim's own asserter signature into its node so verify_bundle
    # can check it offline, and the enrolled validator set (with enrollment
    # envelopes) so those signatures verify against a chain-checked key rather
    # than collapsing to the exporter's. The digest is derived from named
    # fields, so this added field does not perturb it.
    sig_by_claim = {c["claim_id"]: c.get("signature_bundle") for c in claims}
    for node in predicate.get("@graph", []):
        node_id = node.get("@id", "")
        if node_id.startswith("mare:claim/"):
            bundle_json = sig_by_claim.get(node_id[len("mare:claim/"):])
            if bundle_json:
                node["signatureBundle"] = json.loads(bundle_json)
    # Disclose the validator topology of the exporting graph: singleTrustDomain
    # is true when every validator traces to one root of trust. It labels
    # trust-domain concentration over the ESTABLISHED rows in this bundle; it is
    # a disclosure, not a Sybil guard over the participant topology.
    predicate = {
        **predicate,
        "mare:singleTrustDomain": single_trust_domain,
        "mare:trustDomainRoot": trust_domain_root,
        "mare:validators": [
            {
                "keyid": v["keyid"],
                "pubkey_pem": v["pubkey_pem"],
                "identity": v["identity"],
                "validator_type": v["validator_type"],
                "enrolled_at": v["enrolled_at"],
                "enrolled_by_keyid": v["enrolled_by_keyid"],
                "enrollment_envelope": v["enrollment_envelope"],
            }
            for v in validator_rows
        ],
    }
    return {
        "_type": STATEMENT_TYPE,
        "subject": subjects,
        "predicateType": PREDICATE_TYPE,
        "predicate": predicate,
    }


def sign_bundle(
    statement: dict[str, Any],
    private_key,  # Ed25519PrivateKey
) -> dict[str, Any]:
    """Wrap *statement* in a DSSE envelope signed by *private_key*.

    The envelope shape mirrors ``signing.sign_claim`` so consumers
    that already know how to verify mareforma claim envelopes can
    verify the bundle with the same primitives.
    """
    from mareforma import signing as _signing
    payload_bytes = json.dumps(
        statement, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    # Sign over the DSSE PAE encoding, not the raw payload, so the envelope
    # verifies with mareforma's own verify_envelope and any standard DSSE
    # verifier — the type is bound into the signature, not just carried beside
    # it.
    sig = private_key.sign(_signing.dsse_pae(BUNDLE_PAYLOAD_TYPE, payload_bytes))
    keyid = _signing.public_key_id(private_key.public_key())
    return {
        "payloadType": BUNDLE_PAYLOAD_TYPE,
        "payload": base64.standard_b64encode(payload_bytes).decode("ascii"),
        "signatures": [
            {
                "keyid": keyid,
                "sig": base64.standard_b64encode(sig).decode("ascii"),
            }
        ],
        "mare:bundleVersion": __version__,
    }


def write_bundle(root: Path, output_path: Path, private_key) -> Path:
    """Build, sign, and write a bundle. Returns the path written."""
    statement = build_statement(root)
    bundle = sign_bundle(statement, private_key)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(bundle, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return output_path


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


class BundleVerificationError(Exception):
    """Raised when a bundle fails verification.

    The exception message names the first failing check so the
    caller can route between "this is corrupt" and "this is a
    cross-version skew" without parsing English.
    """


def _verify_exported_validators(
    validators: list[dict],
) -> tuple[dict[str, Any], str]:
    """Verify the exported validator set; return (keyid -> public-key, root).

    Each enrollment envelope is verified against its parent's pubkey (the root
    is self-verified), mirroring the restore path, and every validator must
    chain to a single root of trust with no islands or cycles. Per-claim
    asserter signatures are then checked against these chain-verified keys, so a
    malicious exporter cannot substitute fabricated asserters. Any failure
    raises :class:`BundleVerificationError`.
    """
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    by_keyid = {v.get("keyid"): v for v in validators}
    roots = [
        k for k, v in by_keyid.items() if v.get("enrolled_by_keyid") == k
    ]
    if len(roots) != 1:
        raise BundleVerificationError(
            f"validators:expected exactly one root of trust, found {len(roots)}"
        )
    root = roots[0]

    verified: dict[str, Any] = {}
    for keyid, v in by_keyid.items():
        parent = by_keyid.get(v.get("enrolled_by_keyid"))
        if parent is None:
            raise BundleVerificationError(
                f"validators:{str(keyid)[:12]}… enrolled by a key absent "
                "from the bundle"
            )
        try:
            parent_pem = base64.standard_b64decode(parent["pubkey_pem"])
        except (ValueError, TypeError, KeyError) as exc:
            raise BundleVerificationError(
                f"validators:{str(keyid)[:12]}… parent pubkey not base64"
            ) from exc
        if not _validators.verify_enrollment(v, parent_pem):
            raise BundleVerificationError(
                f"validators:{str(keyid)[:12]}… enrollment failed verification"
            )
        try:
            verified[keyid] = _signing.public_key_from_pem(
                base64.standard_b64decode(v["pubkey_pem"])
            )
        except (ValueError, TypeError, KeyError, _signing.SigningError) as exc:
            raise BundleVerificationError(
                f"validators:{str(keyid)[:12]}… pubkey unparseable"
            ) from exc

    # Every validator must reach the single root by walking parents — no
    # island component and no cycle can smuggle in an off-root asserter key.
    for keyid in by_keyid:
        seen: set = set()
        cur = keyid
        while cur != root:
            if cur in seen or cur not in by_keyid:
                raise BundleVerificationError(
                    f"validators:{str(keyid)[:12]}… does not chain to the root"
                )
            seen.add(cur)
            cur = by_keyid[cur].get("enrolled_by_keyid")
    return verified, root


def verify_bundle(
    bundle_path: Path,
    public_key,  # Ed25519PublicKey
) -> dict[str, Any]:
    """Verify a bundle's DSSE envelope AND each per-claim signature.

    Returns the parsed Statement on success. Raises
    :class:`BundleVerificationError` on any check failure (envelope
    type wrong, signature mismatch, predicateType skew, per-claim
    digest mismatch, per-claim signature failure).

    A per-claim signature failure raises immediately: partial
    verification doesn't surface "you have an authenticated bundle
    that contains some invalid claims" as success.
    """
    from mareforma import signing as _signing

    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    if bundle.get("payloadType") != BUNDLE_PAYLOAD_TYPE:
        raise BundleVerificationError(
            f"bundle:payloadType mismatch: got "
            f"{bundle.get('payloadType')!r}, expected {BUNDLE_PAYLOAD_TYPE!r}"
        )

    # Verify bundle-level DSSE.
    try:
        payload_bytes = base64.standard_b64decode(bundle["payload"])
    except (KeyError, ValueError) as exc:
        raise BundleVerificationError(f"bundle:payload decode failed: {exc}") from exc

    sigs = bundle.get("signatures") or []
    if not sigs:
        raise BundleVerificationError("bundle:signatures missing or empty")

    keyid = _signing.public_key_id(public_key)
    matching = [s for s in sigs if s.get("keyid") == keyid]
    if not matching:
        raise BundleVerificationError(
            f"bundle:no signature matches the given public key (keyid {keyid[:12]}…)"
        )
    try:
        sig_bytes = base64.standard_b64decode(matching[0]["sig"])
    except (KeyError, ValueError) as exc:
        raise BundleVerificationError(f"bundle:signature decode failed: {exc}") from exc

    from cryptography.exceptions import InvalidSignature
    try:
        public_key.verify(
            sig_bytes, _signing.dsse_pae(BUNDLE_PAYLOAD_TYPE, payload_bytes),
        )
    except InvalidSignature as exc:
        raise BundleVerificationError(
            "bundle:signature verification failed — bundle has been tampered"
        ) from exc

    # Parse the verified Statement.
    try:
        statement = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BundleVerificationError(f"bundle:payload not JSON: {exc}") from exc

    if statement.get("_type") != STATEMENT_TYPE:
        raise BundleVerificationError(
            f"statement:_type mismatch: got {statement.get('_type')!r}, "
            f"expected {STATEMENT_TYPE!r}"
        )
    if statement.get("predicateType") != PREDICATE_TYPE:
        raise BundleVerificationError(
            f"statement:predicateType mismatch: got "
            f"{statement.get('predicateType')!r}, expected {PREDICATE_TYPE!r}"
        )

    # Verify each subject digest against the corresponding claim's
    # canonical_payload in the predicate.
    subjects = {s["name"]: s["digest"]["sha256"] for s in statement.get("subject", [])}
    predicate = statement.get("predicate") or {}
    nodes = predicate.get("@graph") or []
    # Chain-verify the exported validator set: every validator descends from a
    # single root of trust. The bundle must then be signed BY that root, so the
    # public key the caller pins is the same key every per-claim asserter chains
    # to — the whole bundle anchors to one key the caller chose to trust, not to
    # the exporter as a separate party. A validator-less bundle is refused
    # (_verify_exported_validators requires one root), so a signed graph cannot
    # be stripped to a digest-only bundle that skips per-claim verification.
    verified_validators, trust_root = _verify_exported_validators(
        predicate.get("mare:validators") or []
    )
    if keyid != trust_root:
        raise BundleVerificationError(
            f"bundle:signed by {keyid[:12]}… but the validators chain to root "
            f"{str(trust_root)[:12]}…; a bundle must be signed by its root"
        )
    for node in nodes:
        node_id = node.get("@id", "")
        if not node_id.startswith("mare:claim/"):
            continue
        claim_id = node_id[len("mare:claim/"):]
        subject_name = f"{SUBJECT_PREFIX}{claim_id}"
        if subject_name not in subjects:
            raise BundleVerificationError(
                f"statement:subject missing for claim {claim_id!r}"
            )
        # Per-claim asserter signature. The digest check above is
        # self-referential to the exporter key; this proves each claim was
        # signed by its stated asserter. In a signed graph every claim must
        # carry its bundle, so a stripped signature cannot hide an unsigned row.
        sig_bundle = node.get("signatureBundle")
        if sig_bundle is None:
            if verified_validators:
                raise BundleVerificationError(
                    f"claim:{claim_id} carries no signature bundle but the "
                    "graph is signed"
                )
        else:
            try:
                asserter_keyid = sig_bundle["signatures"][0]["keyid"]
            except (KeyError, IndexError, TypeError) as exc:
                raise BundleVerificationError(
                    f"claim:{claim_id} signature bundle is malformed"
                ) from exc
            asserter_pub = verified_validators.get(asserter_keyid)
            if asserter_pub is None:
                raise BundleVerificationError(
                    f"claim:{claim_id} asserter {str(asserter_keyid)[:12]}… is "
                    "not a chain-verified validator in the bundle"
                )
            try:
                sig_ok = _signing.verify_envelope(sig_bundle, asserter_pub)
            except _signing.InvalidEnvelopeError as exc:
                raise BundleVerificationError(
                    f"claim:{claim_id} signature bundle is structurally "
                    f"invalid: {exc}"
                ) from exc
            if not sig_ok:
                raise BundleVerificationError(
                    f"claim:{claim_id} asserter signature failed verification"
                )
            try:
                bound = _signing.claim_predicate_from_envelope(sig_bundle)
            except _signing.InvalidEnvelopeError as exc:
                raise BundleVerificationError(
                    f"claim:{claim_id} signature payload is unparseable: {exc}"
                ) from exc
            if bound.get("claim_id") != claim_id:
                raise BundleVerificationError(
                    f"claim:{claim_id} signature binds a different claim_id "
                    f"({bound.get('claim_id')!r})"
                )
            # Bind the asserter signature to the CONTENT, not just the claim_id:
            # re-derive the subject digest from the validator-signed fields and
            # require it equals the bundle's subject digest. Without this the
            # asserter signs only an id while the exporter alone vouches for the
            # displayed text/evidence — the signature would be decorative.
            asserter_digest = hashlib.sha256(
                _signing.canonical_statement({
                    "claim_id": claim_id,
                    "text": bound.get("text"),
                    "classification": bound.get("classification"),
                    "generated_by": bound.get("generated_by"),
                    "supports": bound.get("supports") or [],
                    "contradicts": bound.get("contradicts") or [],
                    "source_name": bound.get("source_name"),
                    "artifact_hash": bound.get("artifact_hash"),
                    "created_at": bound.get("created_at"),
                }, bound.get("evidence") or {})
            ).hexdigest()
            if asserter_digest != subjects.get(subject_name):
                raise BundleVerificationError(
                    f"claim:{claim_id} asserter signature does not cover the "
                    "presented content — text or evidence differs from what "
                    "was signed"
                )
        # Re-derive the canonical Statement v1 hash from the @graph
        # node. evidence is part of the signed predicate, so the
        # JSON-LD node carries it and verify uses the same shape that
        # the build path used.
        chain_input = _signing.canonical_statement({
            "claim_id": claim_id,
            "text": node.get("claimText", ""),
            "classification": node.get("classification", "INFERRED"),
            "generated_by": node.get("generatedBy", "agent"),
            "supports": node.get("supports", []),
            "contradicts": node.get("contradicts", []),
            "source_name": node.get("sourceName"),
            "artifact_hash": node.get("artifactHash"),
            "created_at": node.get("dateCreated", ""),
        }, node.get("evidence") or {})
        expected = hashlib.sha256(chain_input).hexdigest()
        if subjects[subject_name] != expected:
            raise BundleVerificationError(
                f"statement:subject digest mismatch for {claim_id!r} — "
                "bundle contents have been tampered"
            )
    return statement
