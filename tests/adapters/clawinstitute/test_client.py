"""Tests for the ClawInstituteClient Protocol + HttpxClient default."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import httpx
import pytest

from mareforma.adapters.clawinstitute.client import (
    SUPPORTED_API_VERSION,
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


def test_client_protocol_runtime_checkable():
    """A stub satisfying the three methods isinstance-passes."""

    class Stub:
        def list_workspace_posts(self, workspace_id, *, since=None):
            return []

        def get_post(self, post_id):
            return {}

        def api_version(self):
            return SUPPORTED_API_VERSION

    assert isinstance(Stub(), ClawInstituteClient)


def test_client_protocol_negative():
    class HalfStub:
        def list_workspace_posts(self, workspace_id, *, since=None):
            return []

    assert not isinstance(HalfStub(), ClawInstituteClient)


def test_httpx_client_requires_base_url(monkeypatch):
    monkeypatch.delenv("CLAWINSTITUTE_BASE_URL", raising=False)
    monkeypatch.setenv("CLAWINSTITUTE_TOKEN", "t")
    with pytest.raises(AuthError, match="base URL"):
        HttpxClient()


def test_httpx_client_requires_token(monkeypatch):
    monkeypatch.setenv("CLAWINSTITUTE_BASE_URL", "https://example.invalid")
    monkeypatch.delenv("CLAWINSTITUTE_TOKEN", raising=False)
    with pytest.raises(AuthError, match="token"):
        HttpxClient()


def test_httpx_client_reads_env_when_no_args(monkeypatch):
    monkeypatch.setenv("CLAWINSTITUTE_BASE_URL", "https://api.example.invalid")
    monkeypatch.setenv("CLAWINSTITUTE_TOKEN", "tok123")
    with HttpxClient() as c:
        assert c._base_url == "https://api.example.invalid"


def test_httpx_client_close_is_idempotent():
    c = HttpxClient(base_url="https://x", token="t")
    c.close()
    c.close()  # second call must not raise


def test_httpx_client_uses_pooled_client():
    """HttpxClient holds an httpx.Client, not module-level httpx.request."""
    c = HttpxClient(base_url="https://x", token="t")
    try:
        assert isinstance(c._http, httpx.Client)
        # Pool config: bearer header, follow_redirects off.
        assert c._http.headers["Authorization"] == "Bearer t"
        assert c._http.follow_redirects is False
    finally:
        c.close()


def _json_response(status: int, body: Any) -> httpx.Response:
    import json
    return httpx.Response(
        status_code=status,
        content=json.dumps(body).encode("utf-8"),
        request=httpx.Request("GET", "https://example.invalid/x"),
        headers={"Content-Type": "application/json"},
    )


def test_request_maps_401_to_autherror(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")
    try:
        monkeypatch.setattr(c._http, "request", lambda *a, **k: _json_response(401, {"err": "no"}))
        with pytest.raises(AuthError):
            c.list_workspace_posts("w")
    finally:
        c.close()


def test_request_maps_403_to_autherror(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")
    try:
        monkeypatch.setattr(c._http, "request", lambda *a, **k: _json_response(403, {}))
        with pytest.raises(AuthError):
            c.list_workspace_posts("w")
    finally:
        c.close()


def test_request_maps_404_to_notfounderror(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")
    try:
        monkeypatch.setattr(c._http, "request", lambda *a, **k: _json_response(404, {}))
        with pytest.raises(NotFoundError):
            c.get_post("missing")
    finally:
        c.close()


def test_request_maps_5xx_to_servererror(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")
    try:
        monkeypatch.setattr(c._http, "request", lambda *a, **k: _json_response(502, {}))
        with pytest.raises(ServerError):
            c.list_workspace_posts("w")
    finally:
        c.close()


def test_request_maps_timeout(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")

    def boom(*a, **k):
        raise httpx.ReadTimeout("timed out", request=httpx.Request("GET", "https://x"))

    try:
        monkeypatch.setattr(c._http, "request", boom)
        with pytest.raises(ClawTimeoutError):
            c.list_workspace_posts("w")
    finally:
        c.close()


def test_request_maps_connecterror(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")

    def boom(*a, **k):
        raise httpx.ConnectError("nope", request=httpx.Request("GET", "https://x"))

    try:
        monkeypatch.setattr(c._http, "request", boom)
        with pytest.raises(ClawConnectionError):
            c.list_workspace_posts("w")
    finally:
        c.close()


def test_non_json_body_raises_jsondecodeerror(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")
    bad = httpx.Response(
        status_code=200,
        content=b"<html>not json</html>",
        request=httpx.Request("GET", "https://x/y"),
        headers={"Content-Type": "text/html"},
    )
    try:
        monkeypatch.setattr(c._http, "request", lambda *a, **k: bad)
        with pytest.raises(JsonDecodeError):
            c.list_workspace_posts("w")
    finally:
        c.close()


def test_non_object_json_raises_unexpectedshape(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")
    try:
        monkeypatch.setattr(c._http, "request", lambda *a, **k: _json_response(200, [1, 2, 3]))
        with pytest.raises(UnexpectedShapeError, match="expected JSON object"):
            c.list_workspace_posts("w")
    finally:
        c.close()


def test_list_workspace_posts_missing_posts_field(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")
    try:
        monkeypatch.setattr(
            c._http, "request", lambda *a, **k: _json_response(200, {"foo": "bar"}),
        )
        with pytest.raises(UnexpectedShapeError, match="'posts'"):
            c.list_workspace_posts("w")
    finally:
        c.close()


def test_list_workspace_posts_returns_list(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")
    posts = [{"id": "p1"}, {"id": "p2"}]
    try:
        monkeypatch.setattr(
            c._http, "request",
            lambda *a, **k: _json_response(200, {"posts": posts}),
        )
        assert c.list_workspace_posts("w") == posts
    finally:
        c.close()


def test_workspace_id_url_quoted(monkeypatch):
    """workspace_id is URL-quoted so '..' and '/' cannot traverse the path."""
    c = HttpxClient(base_url="https://x", token="t")
    captured: dict[str, Any] = {}

    def capture(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        return _json_response(200, {"posts": []})

    try:
        monkeypatch.setattr(c._http, "request", capture)
        c.list_workspace_posts("../admin")
        # The '/' and '.' MUST have been quoted.
        assert "/api/v1/workspaces/" + quote("../admin", safe="") + "/posts" == captured["path"]
        assert "../admin" not in captured["path"]
    finally:
        c.close()


def test_post_id_url_quoted(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")
    captured: dict[str, Any] = {}

    def capture(method, path, **kwargs):
        captured["path"] = path
        return _json_response(200, {"id": "x"})

    try:
        monkeypatch.setattr(c._http, "request", capture)
        c.get_post("p/with/slashes")
        assert captured["path"] == "/api/v1/posts/" + quote("p/with/slashes", safe="")
    finally:
        c.close()


def test_request_rejects_arbitrary_kwargs():
    """_request only accepts params + json_body keyword args — caller cannot
    pass verify=False or follow_redirects=True to bypass TLS / token safety."""
    c = HttpxClient(base_url="https://x", token="t")
    try:
        with pytest.raises(TypeError):
            c._request("GET", "/x", verify=False)  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            c._request("GET", "/x", follow_redirects=True)  # type: ignore[call-arg]
    finally:
        c.close()


def test_request_after_close_raises():
    c = HttpxClient(base_url="https://x", token="t")
    c.close()
    with pytest.raises(ClawInstituteApiError, match="closed"):
        c.list_workspace_posts("w")


def test_api_version_mismatch_raises(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")
    try:
        monkeypatch.setattr(
            c._http, "request",
            lambda *a, **k: _json_response(200, {"version": "v99"}),
        )
        with pytest.raises(ApiVersionError, match="v99"):
            c.api_version()
    finally:
        c.close()


def test_api_version_match(monkeypatch):
    c = HttpxClient(base_url="https://x", token="t")
    try:
        monkeypatch.setattr(
            c._http, "request",
            lambda *a, **k: _json_response(200, {"version": f"{SUPPORTED_API_VERSION}.3"}),
        )
        assert c.api_version().startswith(SUPPORTED_API_VERSION)
    finally:
        c.close()


def test_typed_exceptions_share_parent():
    """Callers can catch every client failure with one except clause."""
    for exc in (
        AuthError, NotFoundError, ServerError, JsonDecodeError,
        UnexpectedShapeError, ApiVersionError, ClawTimeoutError,
        ClawConnectionError,
    ):
        assert issubclass(exc, ClawInstituteApiError)
