"""Predicate construction + decode + verification for `tool-call/v1`.

Mareforma's outer envelope uses ``urn:mareforma:predicate:claim:v1``,
that's mareforma's signed predicate, binding `claim_id`, `text`,
`classification`, `generated_by`, `supports`, `contradicts`,
`source_name`, `artifact_hash`, `created_at`, and the GRADE evidence.

The `tool-call/v1` predicate lives *inside* the claim's text as a
tagged JSON block (same pattern the dependency maqueta's
container-exec/v1 predicate uses). Decode is via a deterministic
substring scan; encode refuses ambiguous boundary characters
(``</predicate>`` smuggled into a tool name, etc.).

Public:

- :data:`PREDICATE_TYPE_V1`: the URI reserved by this maqueta.
- :data:`PREDICATE_TAG_OPEN` / :data:`PREDICATE_TAG_CLOSE`: boundary tags.
- :func:`build_tool_call_predicate`: assembles the predicate dict.
- :func:`encode_predicate_into_text`: writes the tagged block.
- :func:`decode_predicate_from_text`: strict inverse of the encode.
- :func:`verify_tool_call_envelope`: verify a claim's DSSE envelope
  AND assert its embedded `tool-call/v1` predicate is well-formed.
"""

from __future__ import annotations

import json
from typing import Any

from mareforma import signing

from mareforma.tools import PredicateBoundaryError


__all__ = [
    "PREDICATE_TYPE_V1",
    "PREDICATE_TAG_OPEN",
    "PREDICATE_TAG_CLOSE",
    "build_tool_call_predicate",
    "encode_predicate_into_text",
    "decode_predicate_from_text",
    "verify_tool_call_envelope",
]


from mareforma.predicate_types import TOOL_CALL_V1 as PREDICATE_TYPE_V1

PREDICATE_TAG_OPEN = "<predicate tool-call v1>"
PREDICATE_TAG_CLOSE = "</predicate>"


_REQUIRED_FIELDS = (
    "predicate_type",
    "tool_namespace",
    "tool_name",
    "tool_version",
    "tool_config_fingerprint",
    "arguments_canonical",
    "arguments_digest",
    "result_canonical_form",
    "result_digest",
    "result_bytes_size",
    "started_at",
    "completed_at",
    "cache_hit",
    "tool_call_id",
    # ``cache_origin`` is required as of Phase 2 — null when cache_hit
    # is false, "local-graph" or "external-cache" when true. Honest
    # null is the truth-telling signal.
    "cache_origin",
)


def build_tool_call_predicate(
    *,
    tool_namespace: str,
    tool_name: str,
    tool_version: str,
    tool_config_fingerprint: str,
    arguments_canonical: dict[str, Any],
    arguments_digest: str,
    result_canonical_form: str,
    result_digest: str,
    result_bytes_size: int,
    started_at: str,
    completed_at: str,
    cache_hit: bool,
    tool_call_id: str,
    data_source_version: str | None = None,
    parent_claim_id: str | None = None,
    cache_origin: str | None = None,
) -> dict[str, Any]:
    """Assemble the `tool-call/v1` predicate dict.

    Required fields are positional-only via keyword arguments; optional
    fields default to honest null/false values rather than being
    omitted. A predicate with ``data_source_version: null`` is the
    truth-telling signal that the source didn't surface a version, not
    a missing-field bug.
    """

    predicate: dict[str, Any] = {
        "predicate_type": PREDICATE_TYPE_V1,
        "tool_namespace": tool_namespace,
        "tool_name": tool_name,
        "tool_version": tool_version,
        "tool_config_fingerprint": tool_config_fingerprint,
        "arguments_canonical": arguments_canonical,
        "arguments_digest": arguments_digest,
        "result_canonical_form": result_canonical_form,
        "result_digest": result_digest,
        "result_bytes_size": result_bytes_size,
        "started_at": started_at,
        "completed_at": completed_at,
        "cache_hit": cache_hit,
        "tool_call_id": tool_call_id,
        "data_source_version": data_source_version,
        "parent_claim_id": parent_claim_id,
        "cache_origin": cache_origin,
    }
    return predicate


def encode_predicate_into_text(predicate: dict[str, Any], summary: str) -> str:
    """Write the predicate as a tagged JSON block, then the summary line.

    Matches the dependency maqueta's pattern. Refuses to encode a
    predicate whose JSON form contains the boundary marker: that's a
    tamper signal, raise rather than silently writing ambiguous text.
    """

    payload = json.dumps(predicate, sort_keys=True, separators=(",", ":"))
    if PREDICATE_TAG_OPEN in payload or PREDICATE_TAG_CLOSE in payload:
        raise PredicateBoundaryError(
            "predicate JSON contains a tag boundary marker — refusing to "
            "encode an ambiguous claim text"
        )
    return f"{PREDICATE_TAG_OPEN}{payload}{PREDICATE_TAG_CLOSE}\n{summary}"


def decode_predicate_from_text(text: str) -> dict[str, Any]:
    """Inverse of :func:`encode_predicate_into_text`.

    Strict: missing open tag, missing close tag, non-object JSON, or
    a missing required field each raise ``ValueError``.
    """

    if not text.startswith(PREDICATE_TAG_OPEN):
        raise ValueError("claim text is missing the predicate tag header")
    close = text.find(PREDICATE_TAG_CLOSE, len(PREDICATE_TAG_OPEN))
    if close < 0:
        raise ValueError("claim text is missing the predicate close tag")
    payload = text[len(PREDICATE_TAG_OPEN):close]
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise ValueError("predicate must decode to a JSON object")
    missing = [f for f in _REQUIRED_FIELDS if f not in parsed]
    if missing:
        raise ValueError(
            "predicate missing required fields: %s" % ", ".join(missing)
        )
    if parsed["predicate_type"] != PREDICATE_TYPE_V1:
        raise ValueError(
            "predicate_type %r is not %r" % (
                parsed["predicate_type"], PREDICATE_TYPE_V1,
            )
        )
    return parsed


def verify_tool_call_envelope(
    envelope: dict[str, Any], public_key: Any
) -> dict[str, Any]:
    """Verify the outer DSSE envelope AND extract the tool-call predicate.

    Returns the decoded `tool-call/v1` predicate dict on success.
    Raises if either the outer signature fails OR the embedded
    predicate is malformed.

    Two layers of verification:

    1. ``mareforma.signing.verify_envelope``: confirms the DSSE
       signature over the outer in-toto Statement v1.
    2. :func:`decode_predicate_from_text`: confirms the embedded
       ``tool-call/v1`` predicate is well-formed.

    Both must pass; mareforma guarantees layer 1, this module
    adds layer 2.
    """

    if not signing.verify_envelope(envelope, public_key):
        raise ValueError("outer DSSE signature failed verification")
    inner = signing.claim_predicate_from_envelope(envelope)
    text = inner.get("text", "")
    return decode_predicate_from_text(text)
