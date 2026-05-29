"""HTTP transport contract for ClawInstitute REST.

The Protocol pattern lets tests inject a deterministic stub without
mocking ``httpx``, and lets downstream users plug in a custom client
(e.g. retry wrapper, auth via a different mechanism) without
subclassing. :class:`HttpxClient` is the default implementation,
backed by a pooled :class:`httpx.Client` so polling workloads do not
pay per-request TCP/TLS setup.
"""

from __future__ import annotations

import json
import os
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote


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
    "SUPPORTED_API_VERSION",
    "TimeoutError",
    "UnexpectedShapeError",
]


# API version this client expects from the ClawInstitute REST surface.
# Baked into every request URL — the path pin IS the version contract,
# and a 404 / 5xx surfaces a real mismatch from the server. The
# api_version() probe below is an OPTIONAL caller-invoked check, not a
# hot-path gate; callers who want fail-fast version-mismatch behaviour
# should call it once at startup.
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
        """Return the server-reported API version string.

        OPTIONAL probe — not auto-invoked. Callers that want fail-fast
        version-mismatch behaviour should call this once at startup.
        Raises :class:`ApiVersionError` if the server's version does
        not match :data:`SUPPORTED_API_VERSION`.
        """
        ...


class HttpxClient:
    """Default ``ClawInstituteClient`` backed by a pooled ``httpx.Client``.

    Reads token + base URL from constructor args, falling back to
    ``CLAWINSTITUTE_TOKEN`` and ``CLAWINSTITUTE_BASE_URL`` environment
    variables. Per-call timeout defaults to 30 s; override with
    ``timeout=`` at construction time. Use as a context manager to
    release the connection pool at scope exit:

        with HttpxClient() as c:
            posts = c.list_workspace_posts("w1")
    """

    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        # Import httpx at construction time, not per-request — the
        # extras-install hint surfaces here instead of inside every
        # _request call.
        try:
            import httpx
        except ImportError as exc:
            raise ImportError(
                "mareforma.adapters.clawinstitute.HttpxClient requires "
                "httpx. Install via: pip install mareforma[clawinstitute]"
            ) from exc

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
        self._timeout = float(timeout)

        # Pooled client: shared TCP + TLS across calls, ~3-5× faster
        # for polling workloads vs httpx.request() per call. Headers
        # are set once and reused. follow_redirects=False prevents an
        # open-redirect on the API host from leaking the Bearer token.
        self._http = httpx.Client(
            base_url=self._base_url,
            headers={
                "Authorization": f"Bearer {resolved_token}",
                "Accept": "application/json",
            },
            timeout=self._timeout,
            follow_redirects=False,
        )

    def __enter__(self) -> "HttpxClient":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def close(self) -> None:
        """Release the pooled connection. Safe to call multiple times."""
        if getattr(self, "_http", None) is not None:
            self._http.close()
            self._http = None  # type: ignore[assignment]

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute one HTTP call and convert library errors into typed ones.

        Only ``params`` and ``json_body`` are forwarded to httpx. Other
        kwargs (verify=, follow_redirects=, auth=, cert=) are NOT
        accepted — those would let a caller bypass TLS validation or
        the Bearer-token redirect protection.
        """
        import httpx

        if self._http is None:
            raise ClawInstituteApiError(
                "HttpxClient is closed; construct a new one"
            )

        try:
            response = self._http.request(
                method, path, params=params, json=json_body,
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
        # URL-quote workspace_id with safe='' so '/' and '..' cannot
        # traverse out of /workspaces/<id>/posts into another route.
        safe_id = quote(workspace_id, safe="")
        params: dict[str, str] = {}
        if since is not None:
            params["since"] = since
        body = self._request(
            "GET", f"/api/{SUPPORTED_API_VERSION}/workspaces/{safe_id}/posts",
            params=params or None,
        )
        posts = body.get("posts")
        if not isinstance(posts, list):
            raise UnexpectedShapeError(
                f"'posts' field missing or not a list in workspaces response"
            )
        return posts

    def get_post(self, post_id: str) -> dict[str, Any]:
        safe_id = quote(post_id, safe="")
        body = self._request(
            "GET", f"/api/{SUPPORTED_API_VERSION}/posts/{safe_id}",
        )
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
