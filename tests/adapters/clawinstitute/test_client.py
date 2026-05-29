"""Tests for the ClawInstituteClient Protocol + HttpxClient default."""

from __future__ import annotations

from typing import Any

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
    c = HttpxClient()
    assert c._base_url == "https://api.example.invalid"
    assert c._token == "tok123"


def _stub_response(status: int, body: Any) -> httpx.Response:
    import json
    return httpx.Response(
        status_code=status,
        content=json.dumps(body).encode("utf-8") if not isinstance(body, bytes) else body,
        request=httpx.Request("GET", "https://example.invalid/x"),
        headers={"Content-Type": "application/json"},
    )


def test_request_maps_401_to_autherror(monkeypatch):
    client = HttpxClient(base_url="https://x", token="t")
    monkeypatch.setattr(httpx, "request", lambda *a, **k: _stub_response(401, {"err": "no"}))
    with pytest.raises(AuthError):
        client.list_workspace_posts("w")


def test_request_maps_403_to_autherror(monkeypatch):
    client = HttpxClient(base_url="https://x", token="t")
    monkeypatch.setattr(httpx, "request", lambda *a, **k: _stub_response(403, {}))
    with pytest.raises(AuthError):
        client.list_workspace_posts("w")


def test_request_maps_404_to_notfounderror(monkeypatch):
    client = HttpxClient(base_url="https://x", token="t")
    monkeypatch.setattr(httpx, "request", lambda *a, **k: _stub_response(404, {}))
    with pytest.raises(NotFoundError):
        client.get_post("missing")


def test_request_maps_5xx_to_servererror(monkeypatch):
    client = HttpxClient(base_url="https://x", token="t")
    monkeypatch.setattr(httpx, "request", lambda *a, **k: _stub_response(502, {}))
    with pytest.raises(ServerError):
        client.list_workspace_posts("w")


def test_request_maps_timeout(monkeypatch):
    client = HttpxClient(base_url="https://x", token="t")

    def boom(*a, **k):
        raise httpx.ReadTimeout("timed out", request=httpx.Request("GET", "https://x"))

    monkeypatch.setattr(httpx, "request", boom)
    with pytest.raises(ClawTimeoutError):
        client.list_workspace_posts("w")


def test_request_maps_connecterror(monkeypatch):
    client = HttpxClient(base_url="https://x", token="t")

    def boom(*a, **k):
        raise httpx.ConnectError("nope", request=httpx.Request("GET", "https://x"))

    monkeypatch.setattr(httpx, "request", boom)
    with pytest.raises(ClawConnectionError):
        client.list_workspace_posts("w")


def test_non_json_body_raises_jsondecodeerror(monkeypatch):
    client = HttpxClient(base_url="https://x", token="t")
    bad = httpx.Response(
        status_code=200,
        content=b"<html>not json</html>",
        request=httpx.Request("GET", "https://x/y"),
        headers={"Content-Type": "text/html"},
    )
    monkeypatch.setattr(httpx, "request", lambda *a, **k: bad)
    with pytest.raises(JsonDecodeError):
        client.list_workspace_posts("w")


def test_non_object_json_raises_unexpectedshape(monkeypatch):
    client = HttpxClient(base_url="https://x", token="t")
    monkeypatch.setattr(httpx, "request", lambda *a, **k: _stub_response(200, [1, 2, 3]))
    with pytest.raises(UnexpectedShapeError, match="expected JSON object"):
        client.list_workspace_posts("w")


def test_list_workspace_posts_missing_posts_field(monkeypatch):
    client = HttpxClient(base_url="https://x", token="t")
    monkeypatch.setattr(
        httpx, "request", lambda *a, **k: _stub_response(200, {"foo": "bar"}),
    )
    with pytest.raises(UnexpectedShapeError, match="'posts'"):
        client.list_workspace_posts("w")


def test_list_workspace_posts_returns_list(monkeypatch):
    client = HttpxClient(base_url="https://x", token="t")
    posts = [{"id": "p1"}, {"id": "p2"}]
    monkeypatch.setattr(
        httpx, "request",
        lambda *a, **k: _stub_response(200, {"posts": posts}),
    )
    assert client.list_workspace_posts("w") == posts


def test_api_version_mismatch_raises(monkeypatch):
    client = HttpxClient(base_url="https://x", token="t")
    monkeypatch.setattr(
        httpx, "request",
        lambda *a, **k: _stub_response(200, {"version": "v99"}),
    )
    with pytest.raises(ApiVersionError, match="v99"):
        client.api_version()


def test_api_version_match(monkeypatch):
    client = HttpxClient(base_url="https://x", token="t")
    monkeypatch.setattr(
        httpx, "request",
        lambda *a, **k: _stub_response(200, {"version": f"{SUPPORTED_API_VERSION}.3"}),
    )
    assert client.api_version().startswith(SUPPORTED_API_VERSION)


def test_typed_exceptions_share_parent():
    """Callers can catch every client failure with one except clause."""
    for exc in (
        AuthError, NotFoundError, ServerError, JsonDecodeError,
        UnexpectedShapeError, ApiVersionError, ClawTimeoutError,
        ClawConnectionError,
    ):
        assert issubclass(exc, ClawInstituteApiError)
