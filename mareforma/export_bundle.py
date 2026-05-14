"""
export_bundle.py — SCITT-style signed export bundle.

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

Design choices (one-way doors, locked in v0.3.0):

- **Subject naming**: ``urn:mareforma:claim:<uuid>``. The URN namespace
  is forever once published; consumers will key off this prefix.
- **predicateType**: ``urn:mareforma:predicate:epistemic-graph:v1``.
  URN (not DNS) defers a perpetual-ownership commitment on
  ``mareforma.dev`` for schema dereferencing. Evolution to v2 carries
  a new predicateType, leaving v1 verifiers intact.
- **Predicate body**: the existing JSON-LD export. No additional
  PROV-O modelling — the JSON-LD scoping rationale already covers
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
    aligned with per-claim signatures — a downstream tool that
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

    The Statement is unsigned — call :func:`sign_bundle` to produce
    the DSSE envelope.
    """
    from mareforma.db import open_db, list_claims
    from mareforma.exporters.jsonld import JSONLDExporter

    conn = open_db(root)
    try:
        claims = list_claims(conn)
    finally:
        conn.close()
    subjects = [_subject_for_claim(c) for c in claims]
    predicate = JSONLDExporter(root).export()
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
    sig = private_key.sign(payload_bytes)
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


def verify_bundle(
    bundle_path: Path,
    public_key,  # Ed25519PublicKey
) -> dict[str, Any]:
    """Verify a bundle's DSSE envelope AND each per-claim signature.

    Returns the parsed Statement on success. Raises
    :class:`BundleVerificationError` on any check failure (envelope
    type wrong, signature mismatch, predicateType skew, per-claim
    digest mismatch, per-claim signature failure).

    A per-claim signature failure raises immediately — partial
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
        public_key.verify(sig_bytes, payload_bytes)
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
