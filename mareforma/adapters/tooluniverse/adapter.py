"""ProvenanceToolAdapter — the load-bearing wedge primitive.

Wraps any object satisfying the :class:`Tool` protocol so each
``.call(**kwargs)`` produces a signed mareforma claim with a
``tool-call/v1`` predicate.

Phase 1 ships the sync path against in-process Python tools.
Phase 2 layers on cache-hit-as-fresh-claim semantics and the
``mareforma-tu`` CLI. Phase 3 adds ``.call_async`` for
TaskManager-shaped async tools. Phase 4 hardens against adversarial
inputs and formalises selective wrapping.
"""

from __future__ import annotations

import uuid
import warnings
from datetime import datetime, timezone
from typing import Any

from mareforma import sanitize_for_llm

from mareforma.tools import Tool, ToolCallError, ToolResult
from mareforma.canonicalize import (
    DEFAULT_CANONICALIZER,
    canonicalize,
    digest_bytes,
    fingerprint_tool_config,
)
from .exec_routing import (
    build_container_exec_predicate,
    encode_container_exec_predicate_into_text,
    is_exec_class,
)
from .predicate import (
    build_tool_call_predicate,
    encode_predicate_into_text,
)


__all__ = [
    "ProvenanceToolAdapter",
    "ResultTooLargeError",
    "MAX_RESULT_BYTES_DEFAULT",
    "MissingToolVersionWarning",
]


_TOOL_NAMESPACE_TOOLUNIVERSE = "tooluniverse"

# 10 MB default cap — matches typical LLM-context payload limits with
# headroom. Phase 4 introduces; user-overridable per adapter.
MAX_RESULT_BYTES_DEFAULT = 10 * 1024 * 1024


class ResultTooLargeError(ValueError):
    """Raised when a tool result exceeds `max_result_bytes` and
    `truncate_oversized` is False (the default)."""


class MissingToolVersionWarning(UserWarning):
    """Emitted when a wrapped tool has no `version` attribute.

    The predicate records `"unknown"`; the warning surfaces the gap
    so operators can either patch the tool or accept the unknown
    signal explicitly.
    """


class ProvenanceToolAdapter:
    """Wrap a tool so every call signs a `tool-call/v1` claim.

    Construction is cheap; runtime cost is one canonicalisation +
    digest of arguments + one canonicalisation + digest of the result +
    one mareforma `assert_claim`.

    Parameters
    ----------
    tool : Tool
        Anything conforming to the :class:`Tool` protocol. ToolUniverse
        callables wrapped via :class:`ToolUniverseToolWrapper` (Phase 2)
        or local mocks conforming structurally both work.
    graph : EpistemicGraph
        An open mareforma graph with a loaded signer. Unsigned graphs
        raise at first call.
    parent_claim_id : str | None
        The upstream claim this tool call serves (typically the
        ``assert_claim`` for the reasoning step that decided to call
        this tool). Recorded in the predicate AND in the substrate's
        ``supports[]`` chain so lineage walks find both.
    role : str
        Logical role of this call (Phase 3 adds role attestations as
        a separate signed structure). Phase 1 records the role as a
        free-form string in the predicate; the signed attestation
        round is Phase 3.
    tool_namespace : str
        Default ``"tooluniverse"``. Other ateliers passing custom tool
        registries override this.
    canonicalizer : str
        The canonicalizer registered with
        :func:`canonicalize.register_canonicalizer` to use for both
        arguments and result. Defaults to ``json-c14n-v1``. The
        predicate records the form chosen so replay uses the same.
    tool_config : dict | None
        The tool's config dict, if available; used to compute the
        ``tool_config_fingerprint``. ToolUniverse's `ToolCallable`
        instances expose this via their underlying tool's ``tool_config``
        attribute. Passing ``None`` produces a fingerprint over an
        empty dict (records ``sha256:`` of an empty JCS-c14n empty
        object) — honest signal that the config was unavailable.
    """

    def __init__(
        self,
        tool: Tool,
        graph: Any,
        *,
        parent_claim_id: str | None = None,
        role: str = "executor",
        tool_namespace: str = _TOOL_NAMESPACE_TOOLUNIVERSE,
        canonicalizer: str = DEFAULT_CANONICALIZER,
        tool_config: dict[str, Any] | None = None,
        max_result_bytes: int = MAX_RESULT_BYTES_DEFAULT,
        truncate_oversized: bool = False,
    ) -> None:
        self.tool = tool
        self.graph = graph
        self.parent_claim_id = parent_claim_id
        self.role = role
        self.tool_namespace = tool_namespace
        self.canonicalizer = canonicalizer
        self._tool_config = tool_config or {}
        self.max_result_bytes = max_result_bytes
        self.truncate_oversized = truncate_oversized
        # Pre-compute the tool config fingerprint at construction so a
        # mid-life config mutation by the caller is detectable via a
        # repeat call to ``fingerprint_tool_config``.
        self._tool_config_fingerprint = fingerprint_tool_config(
            self._tool_config
        )
        # Phase 3: detect exec-class at wrap time so the call paths can
        # branch on the predicate URI without re-inspecting category.
        self._is_exec_class = is_exec_class(tool)
        # Phase 4: sanitise + record tool-identity strings ONCE so the
        # predicate doesn't pay sanitisation cost on every call.
        self._sanitized_tool_name = _sanitize_identity(
            getattr(tool, "name", "<unnamed>"), field="tool.name",
        )
        self._sanitized_tool_version = _resolve_tool_version(tool)

    def call(self, **kwargs: Any) -> ToolResult:
        """Synchronously invoke the wrapped tool and sign a claim.

        Returns the underlying tool's :class:`ToolResult`. The signed
        claim's id is recorded on the returned dict under
        ``metadata["mareforma_claim_id"]`` so callers can reference
        it directly (rather than re-querying the graph).

        Raises :class:`ToolCallError` if the underlying tool's
        ``.call(**kwargs)`` raises. The signed claim is NOT written
        in that case — failed calls produce no provenance row by
        default; Phase 4 may introduce a failure-class predicate
        if a use case emerges.
        """

        started_at = _utc_now()
        canonical_args = _canonical_args(kwargs)
        arguments_bytes = canonicalize(canonical_args, form=self.canonicalizer)
        arguments_digest = "sha256:" + digest_bytes(arguments_bytes)
        tool_call_id = str(uuid.uuid4())

        try:
            result = self.tool.call(**kwargs)
        except Exception as exc:
            raise ToolCallError(
                f"underlying tool {self.tool.name!r} raised: {exc}"
            ) from exc

        return self._assert_claim_from_result(
            result=result,
            canonical_args=canonical_args,
            arguments_digest=arguments_digest,
            tool_call_id=tool_call_id,
            started_at=started_at,
        )

    async def call_async(self, **kwargs: Any) -> ToolResult:
        """Asynchronously invoke a TaskManager-shape tool and sign on completion.

        The tool must expose ``start_call(**kwargs) -> (task_id,
        awaitable)``. The adapter awaits the awaitable, signs the
        claim *only* on completion, and records the original
        ``task_id`` in the predicate's ``tool_call_id`` field so
        forensic correlation with ToolUniverse's TaskManager log
        works.

        Failed awaits surface as :class:`ToolCallError` with no
        claim written — same posture as sync ``.call``.
        """

        started_at = _utc_now()
        canonical_args = _canonical_args(kwargs)
        arguments_bytes = canonicalize(canonical_args, form=self.canonicalizer)
        arguments_digest = "sha256:" + digest_bytes(arguments_bytes)

        try:
            task_id, fut = await self.tool.start_call(**kwargs)
        except Exception as exc:
            raise ToolCallError(
                f"underlying tool {self.tool.name!r} start_call raised: {exc}"
            ) from exc

        try:
            result = await fut
        except Exception as exc:
            raise ToolCallError(
                f"underlying tool {self.tool.name!r} await raised: {exc}"
            ) from exc

        return self._assert_claim_from_result(
            result=result,
            canonical_args=canonical_args,
            arguments_digest=arguments_digest,
            tool_call_id=task_id,
            started_at=started_at,
        )

    # ------------------------------------------------------------------
    # Internal helpers — shared between sync and async paths.
    # ------------------------------------------------------------------

    def _assert_claim_from_result(
        self,
        *,
        result: dict[str, Any],
        canonical_args: dict[str, Any],
        arguments_digest: str,
        tool_call_id: str,
        started_at: str,
    ) -> ToolResult:
        """Common path: validate, canonicalise, sign, return.

        Both ``.call`` and ``.call_async`` funnel here once they have
        a ``result`` dict from the underlying tool. This avoids drift
        between the two paths.
        """

        data = result.get("data") if isinstance(result, dict) else None
        if data is None:
            raise ToolCallError(
                f"tool {self.tool.name!r} returned no 'data' field; "
                "ToolResult.data is required"
            )

        result_bytes = canonicalize(data, form=self.canonicalizer)

        # Phase 4 size cap. Refuse (or truncate-with-flag) before we
        # spend cycles signing a payload that will never fit downstream.
        result_truncated = False
        if len(result_bytes) > self.max_result_bytes:
            if not self.truncate_oversized:
                raise ResultTooLargeError(
                    f"tool {self._sanitized_tool_name!r} returned "
                    f"{len(result_bytes)} canonical bytes; cap is "
                    f"{self.max_result_bytes}. Either raise "
                    "max_result_bytes or pass truncate_oversized=True."
                )
            # Truncation: keep a structurally honest marker AND the
            # bytes that fit. The signed result is the truncated form;
            # the predicate flags it so replay knows.
            result_bytes = result_bytes[: self.max_result_bytes]
            result_truncated = True

        result_digest_hex = digest_bytes(result_bytes)
        result_digest = "sha256:" + result_digest_hex
        result_bytes_size = len(result_bytes)
        completed_at = _utc_now()

        metadata = result.get("metadata") or {}
        # Stash for downstream builders that need to set the predicate flag.
        self._last_result_truncated = result_truncated

        if self._is_exec_class:
            claim_text, supports, _ = self._build_container_exec_claim(
                metadata=metadata,
                canonical_args=canonical_args,
                arguments_digest=arguments_digest,
                result_digest=result_digest,
                result_bytes_size=result_bytes_size,
                started_at=started_at,
                completed_at=completed_at,
                tool_call_id=tool_call_id,
            )
        else:
            claim_text, supports = self._build_tool_call_claim(
                metadata=metadata,
                canonical_args=canonical_args,
                arguments_digest=arguments_digest,
                result_digest=result_digest,
                result_bytes_size=result_bytes_size,
                started_at=started_at,
                completed_at=completed_at,
                tool_call_id=tool_call_id,
                result=result,
            )

        claim_id = self.graph.assert_claim(
            claim_text,
            classification="ANALYTICAL",
            generated_by=f"adapter/{self.role}/{self.tool.name}",
            supports=supports if supports else None,
            source_name=f"{self.tool_namespace}/{self.tool.name}",
            artifact_hash=result_digest_hex,
        )

        return {
            "data": data,
            "metadata": {
                **metadata,
                "mareforma_claim_id": claim_id,
                "tool_call_id": tool_call_id,
            },
            "source_version": (
                result.get("source_version") if isinstance(result, dict)
                else None
            ),
        }

    def _build_tool_call_claim(
        self,
        *,
        metadata: dict[str, Any],
        canonical_args: dict[str, Any],
        arguments_digest: str,
        result_digest: str,
        result_bytes_size: int,
        started_at: str,
        completed_at: str,
        tool_call_id: str,
        result: dict[str, Any],
    ) -> tuple[str, list[str]]:
        cache_hit = bool(metadata.get("cache_hit", False))
        cache_origin = metadata.get("cache_origin") if cache_hit else None
        cache_original_claim_id = (
            metadata.get("cache_original_claim_id") if cache_hit else None
        )

        predicate = build_tool_call_predicate(
            tool_namespace=self.tool_namespace,
            tool_name=self._sanitized_tool_name,
            tool_version=self._sanitized_tool_version,
            tool_config_fingerprint=self._tool_config_fingerprint,
            arguments_canonical=canonical_args,
            arguments_digest=arguments_digest,
            result_canonical_form=self.canonicalizer,
            result_digest=result_digest,
            result_bytes_size=result_bytes_size,
            started_at=started_at,
            completed_at=completed_at,
            cache_hit=cache_hit,
            cache_origin=cache_origin,
            tool_call_id=tool_call_id,
            data_source_version=(
                result.get("source_version") if isinstance(result, dict)
                else None
            ),
            parent_claim_id=self.parent_claim_id,
            result_truncated=getattr(self, "_last_result_truncated", False),
        )

        summary = (
            f"tool_call {self.tool_namespace}/{self._sanitized_tool_name} "
            f"args={arguments_digest[:19]} result={result_digest[:19]}"
        )
        claim_text = encode_predicate_into_text(predicate, summary)

        supports: list[str] = []
        if self.parent_claim_id:
            supports.append(self.parent_claim_id)
        if cache_original_claim_id and cache_original_claim_id not in supports:
            supports.append(cache_original_claim_id)
        return claim_text, supports

    def _build_container_exec_claim(
        self,
        *,
        metadata: dict[str, Any],
        canonical_args: dict[str, Any],
        arguments_digest: str,
        result_digest: str,
        result_bytes_size: int,
        started_at: str,
        completed_at: str,
        tool_call_id: str,
    ) -> tuple[str, list[str], dict[str, Any]]:
        predicate = build_container_exec_predicate(
            tool_namespace=self.tool_namespace,
            tool_name=self._sanitized_tool_name,
            tool_version=self._sanitized_tool_version,
            tool_config_fingerprint=self._tool_config_fingerprint,
            arguments_canonical=canonical_args,
            arguments_digest=arguments_digest,
            result_canonical_form=self.canonicalizer,
            result_digest=result_digest,
            result_bytes_size=result_bytes_size,
            started_at=started_at,
            completed_at=completed_at,
            tool_call_id=tool_call_id,
            image_digest=metadata.get("image_digest", "unknown"),
            source_digest=metadata.get("source_digest", "sha256:" + "0" * 64),
            runtime=metadata.get("runtime", "runc"),
            variance_mode=metadata.get("variance_mode", "deterministic"),
            runtime_version=metadata.get("runtime_version"),
            executor_version=metadata.get("executor_version"),
            parent_claim_id=self.parent_claim_id,
            input_artifacts=metadata.get("input_artifacts"),
            resource_limits=metadata.get("resource_limits"),
            entropy_seed=metadata.get("entropy_seed"),
        )

        summary = (
            f"container_exec {self.tool_namespace}/{self._sanitized_tool_name} "
            f"args={arguments_digest[:19]} result={result_digest[:19]}"
        )
        claim_text = encode_container_exec_predicate_into_text(
            predicate, summary,
        )

        supports: list[str] = []
        if self.parent_claim_id:
            supports.append(self.parent_claim_id)
        return claim_text, supports, predicate


def _utc_now() -> str:
    """Return UTC ISO 8601 with `+00:00` suffix.

    Mareforma's substrate convention (see /tmp/primario/context/architecture.md).
    """

    return datetime.now(timezone.utc).isoformat()


def _canonical_args(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Normalise argument dict ordering before canonicalisation.

    JCS does the byte-level canonicalisation (sorted keys); the
    dict-level canonical view we keep around for the predicate is
    sorted via ``dict(sorted(...))`` so the recorded `arguments_canonical`
    field reads in canonical order too. (rfc8785 doesn't mutate the
    dict it dumps.)
    """

    return dict(sorted(kwargs.items()))


def _sanitize_identity(value: str, *, field: str) -> str:
    """Strip control / NUL / bidi characters from a tool-identity string.

    Uses mareforma's `sanitize_for_llm` for the substantive cleanup
    (zero-width, bidi overrides, control chars), then additionally
    refuses NUL bytes that `sanitize_for_llm` leaves in place. The
    identity strings (`tool_name`, `tool_version`) appear in:

    - the signed predicate's text
    - the substrate-level `source_name` and `generated_by` fields
    - log lines and CLI output

    so a downstream operator must see a clean string regardless of
    what the tool reported.
    """

    if not isinstance(value, str):
        value = str(value)
    cleaned = sanitize_for_llm(value)
    # NUL bytes survive sanitize_for_llm; strip them explicitly.
    cleaned = cleaned.replace("\x00", "")
    if not cleaned:
        raise ValueError(f"{field} sanitised to empty string from {value!r}")
    return cleaned


def _resolve_tool_version(tool: Any) -> str:
    """Return tool.version as a sanitised string, or "unknown" with warning.

    Mirrors the substrate's posture for missing optional fields: honest
    `unknown` signal with an audit-able warning. Replay's `tool_version`
    check then naturally surfaces the gap as a `diff_fields` entry.
    """

    version = getattr(tool, "version", None)
    if version is None:
        warnings.warn(
            f"tool {getattr(tool, 'name', '<unnamed>')!r} has no `version` "
            "attribute; recording as 'unknown'. Replay will surface this "
            "as a tool_version diff against any pinned version.",
            MissingToolVersionWarning,
            stacklevel=3,
        )
        return "unknown"
    return _sanitize_identity(version, field="tool.version")
