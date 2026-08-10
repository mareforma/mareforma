"""EventSource implementation for ClawInstitute workshop posts.

Every field of a post is untrusted: the body is capped, sanitised and
wrapped, the labels beside it are sanitised.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

from mareforma import sanitize_for_llm, wrap_untrusted
from mareforma.events import (
    SOURCE_CLAWINSTITUTE,
    ClaimResult,
    EventHandler,
    EventPayload,
)
from mareforma.predicate_types import WORKSHOP_EVENT_V1


if TYPE_CHECKING:
    from mareforma._graph import EpistemicGraph
    from mareforma.adapters.clawinstitute.client import ClawInstituteClient


__all__ = ["EventHook"]


# Core-side cap on per-post content size. Posts above this are
# digest-only (the SHA-256 of the full raw content is still recorded
# in the payload; the body is replaced with a truncation marker).
# Sized to comfortably fit Claude / GPT context windows with headroom
# for surrounding agent instructions.
_MAX_CONTENT_BYTES = 16 * 1024 * 1024  # 16 MiB

# Marker that sanitize_for_llm appends when its 100k-character cap
# fires. Detected here so the EventPayload's content_truncated flag
# reflects sanitiser-cap truncation, not only byte-cap truncation.
from mareforma.prompt_safety import _TRUNCATION_MARKER as _SANITIZE_TRUNCATION_MARKER


class EventHook:
    """Translate ClawInstitute workshop posts into mareforma claims.

    Construction is cheap; runtime cost per post is one HTTP fetch
    (already paid by the polling caller), one sanitisation pass, and
    one ``assert_claim`` call per subscribed handler.

    Handler exception contract: if a subscribed handler raises during
    :meth:`dispatch`, the exception is caught and converted to a
    :class:`ClaimResult` with ``emitted=False`` and ``error=<repr>``,
    so a misbehaving subscriber cannot block dispatch to peers.

    Parameters
    ----------
    graph : EpistemicGraph
        Open mareforma graph the hook can assert claims against. Used
        by :meth:`emit_sample` and as the natural target for handlers
        that close over it.
    client : ClawInstituteClient | None
        HTTP transport for fetching workspace posts. Optional: pass
        ``None`` when posts are handed to :meth:`dispatch` from
        elsewhere. The hook does not poll on its own; a caller fetches
        posts through the client and feeds them to :meth:`dispatch`.
    """

    def __init__(
        self,
        *,
        graph: "EpistemicGraph | None" = None,
        client: "ClawInstituteClient | None" = None,
    ) -> None:
        self._graph = graph
        self._client = client
        self._handlers: list[EventHandler] = []

    # ------------------------------------------------------------------
    # EventSource Protocol
    # ------------------------------------------------------------------

    def subscribe(self, handler: EventHandler) -> None:
        """Register ``handler`` to receive every payload this hook emits."""
        self._handlers.append(handler)

    def unsubscribe(self, handler: EventHandler) -> None:
        """Drop ``handler`` from the subscriber list."""
        self._handlers.remove(handler)

    # ------------------------------------------------------------------
    # Dispatch path
    # ------------------------------------------------------------------

    def dispatch(self, post: dict[str, Any]) -> list[ClaimResult]:
        """Convert one ClawInstitute post into an EventPayload and fan out.

        Each subscribed handler is invoked once with the same payload;
        the returned list preserves subscription order. Untrusted
        post content runs through three layers of sanitisation
        (raw-byte cap → sanitize_for_llm → wrap_untrusted) before
        anything else can see it. The labels that travel beside it
        (``post_id``, ``author``, ``workspace_id``, ``event_type``)
        come from the same untrusted response and get sanitize_for_llm
        only, because a handler uses them as keys.

        Handler exceptions are caught and converted to a ClaimResult
        with ``emitted=False`` and ``error=<repr>`` so one bad
        subscriber does not poison dispatch for peers.
        """
        payload = self._to_payload(post)
        results: list[ClaimResult] = []
        for h in self._handlers:
            try:
                results.append(h.handle_event(payload))
            except Exception as exc:
                results.append({
                    "claim_id": None,
                    "emitted": False,
                    "error": repr(exc),
                })
        return results

    def _to_payload(self, post: dict[str, Any]) -> EventPayload:
        post_id = _scrub_label(
            "id", post.get("id") or post.get("post_id") or "unknown",
        )
        author = _scrub_label("author", post.get("author", "unknown"))
        workspace_id = _scrub_label(
            "workspace_id", post.get("workspace_id", ""),
        )
        event_type = _scrub_label(
            "event_type", post.get("event_type", "post.created"),
        )
        created_at = post.get("created_at") or _now_iso()
        raw_content = post.get("content", "")

        if not isinstance(raw_content, str):
            raise TypeError(
                f"post {post_id!r} 'content' field must be str, "
                f"got {type(raw_content).__name__}"
            )

        raw_bytes = raw_content.encode("utf-8", errors="replace")
        actual_too_big = len(raw_bytes) > _MAX_CONTENT_BYTES

        # SHA-256 of the FULL raw bytes — content-addressable. A
        # downstream verifier can re-fetch the post body and confirm
        # the digest, regardless of whether the body was truncated
        # in the EventPayload.
        content_digest = "sha256:" + hashlib.sha256(raw_bytes).hexdigest()

        truncated = False
        truncation_reason: str | None = None

        if actual_too_big:
            content_for_payload = (
                f"<truncated: post exceeded {_MAX_CONTENT_BYTES} bytes; "
                "fetch via the API for the full content>"
            )
            truncated = True
            truncation_reason = "byte_cap"
        else:
            sanitised = sanitize_for_llm(raw_content) or ""
            # sanitize_for_llm appends _TRUNCATION_MARKER when its own
            # 100k-character cap fires. Surface that as truncated=True
            # so a downstream consumer cannot mistake the sanitiser-
            # truncated body for the full content.
            if sanitised.endswith(_SANITIZE_TRUNCATION_MARKER):
                truncated = True
                truncation_reason = "sanitize_char_cap"
            content_for_payload = wrap_untrusted(sanitised)

        return {
            "source": SOURCE_CLAWINSTITUTE,
            "event_type": event_type,
            "data": {
                "post_id": post_id,
                "author": author,
                "workspace_id": workspace_id,
                "content": content_for_payload,
                "content_truncated": truncated,
                "content_truncation_reason": truncation_reason,
                "content_digest_sha256": content_digest,
            },
            "timestamp": created_at,
        }

    # ------------------------------------------------------------------
    # Convention helpers used by the coexistence test
    # ------------------------------------------------------------------

    def predicate_uris(self) -> tuple[str, ...]:
        """Return the predicate URIs this hook may emit on the graph."""
        return (WORKSHOP_EVENT_V1,)

    def emit_sample(self) -> str:
        """Emit one synthetic workshop-event claim and return its id.

        Used by the cross-adapter coexistence test to verify that
        every adapter can write into the same graph without colliding
        on predicate URIs. Real production code emits via subscribed
        handlers, not directly.
        """
        if self._graph is None:
            raise RuntimeError(
                "emit_sample() needs the EventHook to be constructed with "
                "a graph=... argument"
            )
        return self._graph.assert_claim(
            "ClawInstitute workshop event (sample)",
            classification="INFERRED",
            generated_by=f"adapter:{SOURCE_CLAWINSTITUTE}",
            predicate_payload={
                "predicate_type": WORKSHOP_EVENT_V1,
                "source": SOURCE_CLAWINSTITUTE,
                "event_type": "post.created",
            },
        )


def _scrub_label(field: str, value: Any) -> str:
    """Strip injection-hostile codepoints from one label field.

    Sanitise-only, the treatment the core gives its own short label
    fields: a handler uses post_id and workspace_id as keys, so
    wrapping them in delimiters is not an option. A non-string label
    is refused rather than forwarded unchecked.
    """
    if not isinstance(value, str):
        raise TypeError(
            f"post {field!r} field must be str, "
            f"got {type(value).__name__}"
        )
    return sanitize_for_llm(value) or ""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
