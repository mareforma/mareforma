"""Exec-class routing — exec tools delegate to the container-exec predicate.

Exec-class ToolUniverse tools (Python execution, code execution) need
hardened sandboxing AND a different attestation shape than retrieval
tools. The dep maqueta `sandboxed-provenance-aware-execution` shipped
`ContainerExecutorTool` for this — a Tool-protocol-conforming class
that runs user code in a hardened container and asserts a
``container-exec/v1`` claim.

This module:

- Provides ``is_exec_class(tool)`` — detection via `tool.category`.
- Provides ``build_container_exec_predicate(...)`` — assembles the
  ``container-exec/v1`` predicate dict from the metadata an
  exec-class tool reports.
- Provides ``encode_container_exec_predicate_into_text(...)`` and
  ``decode_container_exec_predicate(...)`` — same byte-stuffing-safe
  envelope shape as the tool-call/v1 path.

The full container-exec executor (real Docker, real isolation) lives
in the dep maqueta. This module's job is the *routing decision* and
the *attestation shape* — when the routing target is the dep
maqueta's ContainerExecutorTool, the import + delegate happens; when
it's a mock (Phase 3 tests; production callers that don't need real
isolation for some reason), the same predicate shape lands in the
graph regardless.
"""

from __future__ import annotations

import json
from typing import Any

from mareforma.tools import PredicateBoundaryError


__all__ = [
    "CONTAINER_EXEC_PREDICATE_TYPE",
    "EXEC_CLASS_CATEGORIES",
    "CE_TAG_OPEN",
    "CE_TAG_CLOSE",
    "is_exec_class",
    "build_container_exec_predicate",
    "encode_container_exec_predicate_into_text",
    "decode_container_exec_predicate",
]


from mareforma.predicate_types import CONTAINER_EXEC_V1 as CONTAINER_EXEC_PREDICATE_TYPE
EXEC_CLASS_CATEGORIES: frozenset[str] = frozenset({
    "python_exec",
    "code_execution",
    "exec",
    "execute",
})


CE_TAG_OPEN = "<predicate container-exec v1>"
CE_TAG_CLOSE = "</predicate>"


_CE_REQUIRED_FIELDS = (
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
    "tool_call_id",
    "image_digest",
    "source_digest",
    "runtime",
    "variance_mode",
)


def is_exec_class(tool: Any) -> bool:
    """Return True iff ``tool.category`` is in EXEC_CLASS_CATEGORIES."""

    category = getattr(tool, "category", None)
    return category in EXEC_CLASS_CATEGORIES


def build_container_exec_predicate(
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
    tool_call_id: str,
    image_digest: str,
    source_digest: str,
    runtime: str,
    variance_mode: str = "deterministic",
    runtime_version: str | None = None,
    executor_version: str | None = None,
    parent_claim_id: str | None = None,
    input_artifacts: list[dict[str, Any]] | None = None,
    resource_limits: dict[str, Any] | None = None,
    entropy_seed: str | None = None,
) -> dict[str, Any]:
    """Assemble a container-exec/v1 predicate dict.

    Field set mirrors the dep maqueta's predicate (sandboxed-provenance-
    aware-execution) so envelopes federate across both maqueta dirs
    without translation. The dep maqueta's exporter already understands
    every field here.
    """

    predicate: dict[str, Any] = {
        "predicate_type": CONTAINER_EXEC_PREDICATE_TYPE,
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
        "tool_call_id": tool_call_id,
        "image_digest": image_digest,
        "source_digest": source_digest,
        "runtime": runtime,
        "variance_mode": variance_mode,
        "runtime_version": runtime_version,
        "executor_version": executor_version,
        "parent_claim_id": parent_claim_id,
        "input_artifacts": input_artifacts or [],
        "resource_limits": resource_limits or {},
        "entropy_seed": entropy_seed,
    }
    return predicate


def encode_container_exec_predicate_into_text(
    predicate: dict[str, Any], summary: str
) -> str:
    """Encode predicate as tagged JSON + summary. Refuses boundary smuggling."""

    payload = json.dumps(predicate, sort_keys=True, separators=(",", ":"))
    if CE_TAG_OPEN in payload or CE_TAG_CLOSE in payload:
        raise PredicateBoundaryError(
            "container-exec predicate JSON contains a tag boundary marker"
        )
    return f"{CE_TAG_OPEN}{payload}{CE_TAG_CLOSE}\n{summary}"


def decode_container_exec_predicate(text: str) -> dict[str, Any]:
    """Strict inverse of encode_container_exec_predicate_into_text."""

    if not text.startswith(CE_TAG_OPEN):
        raise ValueError("claim text is missing the container-exec predicate header")
    close = text.find(CE_TAG_CLOSE, len(CE_TAG_OPEN))
    if close < 0:
        raise ValueError("claim text is missing the container-exec predicate close tag")
    payload = text[len(CE_TAG_OPEN):close]
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise ValueError("container-exec predicate must decode to a JSON object")
    missing = [f for f in _CE_REQUIRED_FIELDS if f not in parsed]
    if missing:
        raise ValueError(
            "container-exec predicate missing required fields: %s"
            % ", ".join(missing)
        )
    if parsed["predicate_type"] != CONTAINER_EXEC_PREDICATE_TYPE:
        raise ValueError(
            "predicate_type %r is not %r" % (
                parsed["predicate_type"], CONTAINER_EXEC_PREDICATE_TYPE,
            )
        )
    return parsed
