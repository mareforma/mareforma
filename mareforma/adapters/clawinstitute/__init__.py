"""ClawInstitute adapter: workshop events → signed mareforma claims.

The adapter is generic: ClawInstitute hosts many applications, and
any of them produces workshop posts. ``EventHook`` polls a workspace
for new posts, emits an :class:`~mareforma.events.EventPayload` per
post, and each subscribed handler can turn that into a signed
``urn:mareforma:predicate:workshop-event:v1`` claim.

The HTTP transport is a :class:`~typing.Protocol`
(:class:`ClawInstituteClient`) so tests and downstream code can plug
in a mock or a custom client without subclassing. The default
implementation (:class:`HttpxClient`) uses httpx and reads
``CLAWINSTITUTE_TOKEN`` / ``CLAWINSTITUTE_BASE_URL`` from the
environment when not given explicitly.

Untrusted workspace content (post bodies, attachments) flows through
three layers of sanitisation before any handler sees it:

1. :func:`mareforma.sanitize_for_llm` strips NULs and mareforma
   boundary tokens.
2. A 16 MiB cap rejects payloads that would otherwise blow agent
   context windows.
3. :func:`mareforma.wrap_untrusted` wraps the content in
   ``<untrusted_data>`` / ``</untrusted_data>`` tags so a downstream
   LLM cannot mistake it for a directive.

Install: ``pip install mareforma[clawinstitute]``.
"""

from __future__ import annotations

from mareforma.adapters.clawinstitute.client import (
    ApiVersionError,
    AuthError,
    ClawInstituteApiError,
    ClawInstituteClient,
    ConnectionError as ClawConnectionError,
    HttpxClient,
    JsonDecodeError,
    NotFoundError,
    ServerError,
    TimeoutError as ClawTimeoutError,
    UnexpectedShapeError,
)
from mareforma.adapters.clawinstitute.event_hook import EventHook


__all__ = [
    "ApiVersionError",
    "AuthError",
    "ClawConnectionError",
    "ClawInstituteApiError",
    "ClawInstituteClient",
    "ClawTimeoutError",
    "EventHook",
    "HttpxClient",
    "JsonDecodeError",
    "NotFoundError",
    "ServerError",
    "UnexpectedShapeError",
]
