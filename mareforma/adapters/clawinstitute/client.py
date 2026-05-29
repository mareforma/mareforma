"""HTTP transport contract for ClawInstitute REST.

The Protocol pattern lets tests inject a deterministic stub without
mocking ``httpx``, and lets downstream users plug in a custom client
(e.g. retry wrapper, auth via a different mechanism) without
subclassing. :class:`HttpxClient` is the default implementation.
"""

from __future__ import annotations

import json
import os
from typing import Any, Protocol, runtime_checkable


__all__ = [
    "ApiVersionError",
    "AuthError",
    "ClawInstituteApiError",
    "ClawInstituteClient",
    "ConnectionError",
    "HttpxClient",
    "JsonDecodeError",
    "NotFoundError",
    "ServerError",
    "TimeoutError",
    "UnexpectedShapeError",
]


# API version this client expects from the ClawInstitute REST surface.
# The version is checked at runtime against the API's reported version
# (if any) and raises ApiVersionError on incompatibility — pinning the
# contract so a server-side API upgrade surfaces as a typed error here,
# not as a silent shape mismatch downstream.
SUPPORTED_API_VERSION: str = "v1"


class ClawInstituteApiError(Exception):
    """Parent class for every error this client raises.

    Lets a caller catch all client-side failures with a single ``except``
    while still pattern-matching on the typed subclasses below.
    """


class ConnectionError(ClawInstituteApiError):  # noqa: A001 — typed parent
    """Network reachability failure (DNS, TCP, TLS)."""


class TimeoutError(ClawInstituteApiError):  # noqa: A001 — typed parent
    """Request exceeded the configured per-call timeout."""


class AuthError(ClawInstituteApiError):
    """401/403 from the API — missing or invalid token."""


class NotFoundError(ClawInstituteApiError):
    """404 from the API — workspace / post / file does not exist."""


class ServerError(ClawInstituteApiError):
    """5xx from the API — upstream is unhealthy."""


class JsonDecodeError(ClawInstituteApiError):
    """Response body was not valid JSON."""


class UnexpectedShapeError(ClawInstituteApiError):
    """Response decoded but lacked the documented fields."""


class ApiVersionError(ClawInstituteApiError):
    """API version reported by the server is not supported."""


@runtime_checkable
class ClawInstituteClient(Protocol):
    """HTTP transport contract for the ClawInstitute REST API.

    Implementations MUST raise the typed exceptions above (or a
    subclass) — opaque exceptions defeat the adapter's error-handling
    contract.
    """

    def list_workspace_posts(
        self, workspace_id: str, *, since: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return posts in ``workspace_id`` newer than ``since`` (ISO 8601)."""
        ...

    def get_post(self, post_id: str) -> dict[str, Any]:
        """Return full content for a single post."""
        ...

    def api_version(self) -> str:
        """Return the server-reported API version string."""
        ...


class HttpxClient:
    """Default ``ClawInstituteClient`` backed by ``httpx``.

    Reads token + base URL from constructor args, falling back to
    ``CLAWINSTITUTE_TOKEN`` and ``CLAWINSTITUTE_BASE_URL`` environment
    variables. Per-call timeout defaults to 30 s; override with
    ``timeout=`` at construction time.
    """

    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        resolved_url = base_url or os.environ.get("CLAWINSTITUTE_BASE_URL")
        resolved_token = token or os.environ.get("CLAWINSTITUTE_TOKEN")
        if not resolved_url:
            raise AuthError(
                "ClawInstitute base URL not supplied and "
                "CLAWINSTITUTE_BASE_URL is not set in the environment"
            )
        if not resolved_token:
            raise AuthError(
                "ClawInstitute API token not supplied and "
                "CLAWINSTITUTE_TOKEN is not set in the environment"
            )
        self._base_url = resolved_url.rstrip("/")
        self._token = resolved_token
        self._timeout = float(timeout)

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        """Execute one HTTP call and convert library errors into typed ones."""
        import httpx

        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }
        url = f"{self._base_url}{path}"
        try:
            response = httpx.request(
                method, url, headers=headers, timeout=self._timeout, **kwargs,
            )
        except httpx.TimeoutException as exc:
            raise TimeoutError(f"timeout calling {method} {path}: {exc}") from exc
        except httpx.ConnectError as exc:
            raise ConnectionError(
                f"connection error calling {method} {path}: {exc}"
            ) from exc
        except httpx.RequestError as exc:
            raise ConnectionError(
                f"transport error calling {method} {path}: {exc}"
            ) from exc

        status = response.status_code
        if status in (401, 403):
            raise AuthError(f"{status} from {method} {path}")
        if status == 404:
            raise NotFoundError(f"404 from {method} {path}")
        if 500 <= status < 600:
            raise ServerError(f"{status} from {method} {path}")
        if not 200 <= status < 300:
            raise ClawInstituteApiError(
                f"unexpected status {status} from {method} {path}"
            )

        try:
            payload = response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            raise JsonDecodeError(
                f"non-JSON response body from {method} {path}: {exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise UnexpectedShapeError(
                f"expected JSON object from {method} {path}, "
                f"got {type(payload).__name__}"
            )
        return payload

    def list_workspace_posts(
        self, workspace_id: str, *, since: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, str] = {}
        if since is not None:
            params["since"] = since
        body = self._request(
            "GET", f"/api/{SUPPORTED_API_VERSION}/workspaces/{workspace_id}/posts",
            params=params or None,
        )
        posts = body.get("posts")
        if not isinstance(posts, list):
            raise UnexpectedShapeError(
                f"'posts' field missing or not a list in workspaces response"
            )
        return posts

    def get_post(self, post_id: str) -> dict[str, Any]:
        body = self._request("GET", f"/api/{SUPPORTED_API_VERSION}/posts/{post_id}")
        return body

    def api_version(self) -> str:
        body = self._request("GET", "/api/version")
        version = body.get("version")
        if not isinstance(version, str):
            raise UnexpectedShapeError(
                "'version' field missing or not a string in /api/version"
            )
        if not version.startswith(SUPPORTED_API_VERSION):
            raise ApiVersionError(
                f"server reports API version {version!r}; this client "
                f"requires {SUPPORTED_API_VERSION!r}. Upgrade the "
                "mareforma[clawinstitute] extra to a matching release."
            )
        return version
