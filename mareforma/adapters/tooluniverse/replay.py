"""Replay-from-claim — verify a tool call byte-for-byte from its signed claim alone.

Phase 1 ships the single-claim case: given a claim_id and a runtime
tool registry, look up the tool, re-execute the call with the
predicate's canonical arguments, canonicalise the new result, and
assert it matches the predicate's pinned `result_digest`.

The full chain replay across `supports[]` for a multi-tool reasoning
trajectory is Phase 6's federation capstone.
"""

from __future__ import annotations

from typing import Any, Mapping

from mareforma.tools import ReplayResult, Tool, ToolCallError
from mareforma.canonicalize import canonicalize, digest_bytes
from .predicate import decode_predicate_from_text


__all__ = [
    "replay_from_claim",
    "MissingToolError",
    "MalformedClaimError",
]


class MissingToolError(LookupError):
    """Raised when the tool the predicate names is not in the runtime registry.

    Replay can't proceed without an executable tool conforming to the
    Tool protocol. The error message carries the (namespace, name,
    version) the predicate pinned so callers know what to install /
    register.
    """


class MalformedClaimError(ValueError):
    """Raised when the claim's text doesn't decode to a tool-call/v1 predicate."""


def replay_from_claim(
    graph: Any,
    claim_id: str,
    tool_registry: Mapping[str, Tool],
    *,
    expected_tool_config_fingerprint: str | None = None,
) -> ReplayResult:
    """Re-execute the tool call and compare result digests.

    ``tool_registry`` maps ``"{namespace}/{name}"`` to a Tool conforming
    instance. The version is checked against the predicate's pinned
    ``tool_version`` and surfaces as ``diff_fields`` on mismatch.

    ``expected_tool_config_fingerprint`` (Phase 4) — when supplied,
    the replayer compares it against the predicate's pinned
    ``tool_config_fingerprint``. Mismatch adds
    ``"tool_config_fingerprint"`` to ``diff_fields``. This is the
    audit hook for stale-cache and config-drift detection (Phase 2
    SEC-T201, Phase 3 SEC-T305 follow-up). Backward-compatible:
    omitting the kwarg skips the check.

    Returns :class:`ReplayResult`. ``ok=True`` requires:

    1. Tool found in the registry.
    2. ``tool.version == predicate.tool_version``.
    3. The tool's ``.call(**canonical_args)`` returns a ToolResult
       whose canonical bytes hash to the pinned ``result_digest``.

    Raises only on missing-tool (LookupError) or malformed-claim
    (ValueError). Digest mismatches are *signal* (returned in the
    ReplayResult) not failure modes; callers decide what to do with
    them.
    """

    claim = graph.get_claim(claim_id)
    if claim is None:
        raise MalformedClaimError(f"claim {claim_id!r} not found in graph")

    text = claim.get("text") or ""
    try:
        predicate = decode_predicate_from_text(text)
    except ValueError as exc:
        raise MalformedClaimError(
            f"claim {claim_id!r} does not carry a tool-call/v1 predicate: {exc}"
        ) from exc

    expected_result_digest = predicate["result_digest"]
    expected_args_digest = predicate["arguments_digest"]
    canonical_form = predicate["result_canonical_form"]

    key = f"{predicate['tool_namespace']}/{predicate['tool_name']}"
    tool = tool_registry.get(key)
    if tool is None:
        raise MissingToolError(
            "tool %r (version %s) not found in runtime registry "
            "(registered: %s)" % (
                key, predicate["tool_version"],
                ", ".join(sorted(tool_registry.keys())) or "<empty>",
            )
        )

    diff_fields: list[str] = []
    if tool.version != predicate["tool_version"]:
        diff_fields.append("tool_version")

    if (
        expected_tool_config_fingerprint is not None
        and expected_tool_config_fingerprint
        != predicate["tool_config_fingerprint"]
    ):
        diff_fields.append("tool_config_fingerprint")

    canonical_args = predicate["arguments_canonical"]
    # Reverify arguments_digest before executing — protects against
    # the bizarre case where the claim's stored canonical args were
    # tampered (and the substrate trigger missed it).
    args_bytes = canonicalize(canonical_args, form=canonical_form)
    args_digest_observed = "sha256:" + digest_bytes(args_bytes)
    if args_digest_observed != expected_args_digest:
        diff_fields.append("arguments_digest")

    try:
        result = tool.call(**canonical_args)
    except Exception as exc:
        raise ToolCallError(
            f"replay: underlying tool {tool.name!r} raised: {exc}"
        ) from exc

    data = result.get("data") if isinstance(result, dict) else None
    if data is None:
        raise ToolCallError(
            f"replay: tool {tool.name!r} returned no 'data' field"
        )

    result_bytes = canonicalize(data, form=canonical_form)
    observed_result_digest = "sha256:" + digest_bytes(result_bytes)
    if observed_result_digest != expected_result_digest:
        diff_fields.append("result_digest")

    return ReplayResult(
        ok=not diff_fields,
        observed_result_digest=observed_result_digest,
        expected_result_digest=expected_result_digest,
        diff_fields=tuple(diff_fields),
    )
