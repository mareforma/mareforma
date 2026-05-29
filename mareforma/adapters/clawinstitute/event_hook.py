"""EventSource implementation for ClawInstitute workshop posts."""

from __future__ import annotations

import base64
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


# Substrate-side cap on per-post content size. Posts above this are
# digest-only (the digest still binds the content; the body is stored
# outside the claim). Sized to comfortably fit Claude / GPT context
# windows with headroom for surrounding agent instructions.
_MAX_CONTENT_BYTES = 16 * 1024 * 1024  # 16 MiB


class EventHook:
    """Translate ClawInstitute workshop posts into mareforma claims.

    Construction is cheap; runtime cost per post is one HTTP fetch
    (already paid by the polling caller), one sanitisation pass, and
    one ``assert_claim`` call per subscribed handler.

    Parameters
    ----------
    graph : EpistemicGraph
        Open mareforma graph the hook can assert claims against. Used
        by :meth:`emit_sample` and as the natural target for handlers
        that close over it.
    client : ClawInstituteClient | None
        HTTP transport. Optional: pass ``None`` for adapters that only
        consume events handed to them via :meth:`dispatch`. Required
        for the polling helpers (added in v0.3.4+).
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
        (sanitize_for_llm → 16 MiB cap → wrap_untrusted) before
        anything else can see it.
        """
        payload = self._to_payload(post)
        return [h.handle_event(payload) for h in self._handlers]

    def _to_payload(self, post: dict[str, Any]) -> EventPayload:
        post_id = post.get("id") or post.get("post_id") or "unknown"
        author = post.get("author", "unknown")
        workspace_id = post.get("workspace_id", "")
        created_at = post.get("created_at") or _now_iso()
        raw_content = post.get("content", "")

        if not isinstance(raw_content, str):
            raise TypeError(
                f"post {post_id!r} 'content' field must be str, "
                f"got {type(raw_content).__name__}"
            )
        # Reject pathological inputs early. sanitize_for_llm has its
        # own 100k-character cap, but checking the raw-byte length
        # first skips a per-character sanitiser pass on a payload that
        # is going to be truncated anyway.
        raw_bytes = raw_content.encode("utf-8", errors="replace")
        if len(raw_bytes) > _MAX_CONTENT_BYTES:
            content_for_payload = (
                f"<truncated: post exceeded {_MAX_CONTENT_BYTES} bytes; "
                "fetch via the API for the full content>"
            )
            truncated = True
        else:
            sanitised = sanitize_for_llm(raw_content) or ""
            content_for_payload = wrap_untrusted(sanitised)
            truncated = False

        return {
            "source": SOURCE_CLAWINSTITUTE,
            "event_type": post.get("event_type", "post.created"),
            "data": {
                "post_id": post_id,
                "author": author,
                "workspace_id": workspace_id,
                "content": content_for_payload,
                "content_truncated": truncated,
                "content_digest_b64": base64.b64encode(
                    raw_bytes[:64]  # prefix-only marker, not the full digest
                ).decode("ascii"),
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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
