"""
_statement.py — in-toto Statement v1 envelope for mareforma claims.

Every signed claim is wrapped in an in-toto Statement v1 (the de-facto
standard for signed software-supply-chain attestations, also used by
Sigstore and SLSA). The envelope shape is::

    {
      "_type":         "https://in-toto.io/Statement/v1",
      "subject":       [{"name": "mareforma:claim:<id>",
                         "digest": {"sha256": "<text_sha256>"}}],
      "predicateType": "urn:mareforma:predicate:claim:v1",
      "predicate":     { <claim fields + EvidenceVector> }
    }

The Statement dict is canonicalized (sorted keys, NFC text, no whitespace)
and signed via DSSE v1. ``statement_cid`` = sha256(canonical bytes); used
by ``restore`` as a cross-check anchor.

Why in-toto Statement v1
------------------------
SLSA, Sigstore, GUAC, in-toto itself, and a growing chunk of the
provenance-tooling ecosystem speak this shape. By emitting it
verbatim, mareforma signatures are inspectable with off-the-shelf
tools (``in-toto-attestation`` Python lib, ``cosign verify-blob``,
etc.) instead of a mareforma-only verifier. Hostile reviewers who
recognize the shape on inspection don't have to take "we rolled our
own" at face value.

predicateType
-------------
``urn:mareforma:predicate:claim:v1`` — versioned. A future v2 predicate
schema (e.g. adding new evidence dimensions) gets a new predicateType;
v1 stays valid forever for already-signed claims. URN (not DNS) — the
identifier is a stable name, not a fetched document, and avoids a
perpetual-ownership commitment on any DNS name.
"""

from __future__ import annotations

import hashlib
import unicodedata
from typing import Any, Optional, Sequence

from ._canonical import canonicalize


STATEMENT_TYPE = "https://in-toto.io/Statement/v1"
PREDICATE_TYPE = "urn:mareforma:predicate:claim:v1"
SUBJECT_NAME_PREFIX = "mareforma:claim:"


def text_sha256(text: str) -> str:
    """SHA-256 hex of the NFC-normalized UTF-8 bytes of *text*.

    Used as the subject digest in the in-toto Statement. NFC matters:
    visually-identical text with different code-point decomposition
    must produce the same digest so the envelope's subject reference
    is stable under benign normalization.
    """
    norm = unicodedata.normalize("NFC", text)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


def build_statement(
    *,
    claim_id: str,
    text: str,
    classification: str,
    generated_by: str,
    supports: Sequence[str],
    contradicts: Sequence[str],
    source_name: Optional[str],
    artifact_hash: Optional[str],
    created_at: str,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    """Assemble an unsigned in-toto Statement v1 for a claim.

    The returned dict is ready to be canonicalized + signed. Field
    selection mirrors :data:`mareforma.signing.SIGNED_FIELDS` plus the
    ``evidence`` block, so the signature binds the canonical claim
    fields plus the GRADE vector in one byte sequence.

    Parameters
    ----------
    claim_id
        The graph-assigned claim id; also embedded in subject.name so
        the envelope is unforgeably bound to this id.
    text
        The claim text. Hashed (NFC-normalized) into subject.digest.
    classification
        ``INFERRED``, ``ANALYTICAL``, ``DERIVED``, or ``SEED``.
    generated_by
        Producing-agent identifier.
    supports / contradicts
        Upstream / refuted claim ids or DOIs.
    source_name
        Optional source label (e.g. notebook path, paper).
    artifact_hash
        Optional sha256 of an attached artifact (notebook output, image).
    created_at
        ISO 8601 UTC, microsecond precision (existing mareforma convention).
    evidence
        GRADE EvidenceVector serialized via :meth:`EvidenceVector.to_dict`.
    """
    predicate = {
        "claim_id": claim_id,
        "text": text,
        "classification": classification,
        "generated_by": generated_by,
        "supports": list(supports or []),
        "contradicts": list(contradicts or []),
        "source_name": source_name,
        "artifact_hash": artifact_hash,
        "created_at": created_at,
        "evidence": evidence,
    }
    return {
        "_type": STATEMENT_TYPE,
        "subject": [
            {
                "name": f"{SUBJECT_NAME_PREFIX}{claim_id}",
                "digest": {"sha256": text_sha256(text)},
            },
        ],
        "predicateType": PREDICATE_TYPE,
        "predicate": predicate,
    }


def statement_cid(statement: dict[str, Any]) -> str:
    """Content identifier for a Statement: sha256 of canonical bytes.

    Stable across runs / Python versions / dict-insertion orders.
    """
    return hashlib.sha256(canonicalize(statement)).hexdigest()
