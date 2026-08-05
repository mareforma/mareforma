"""A producer-controlled transport cannot mint COMPUTED lineage.

COMPUTED is the trustworthy tier: a body-parse at the socket seam to a
recognized provider host. The provider host is genuine, but the 2xx response is
produced by the producer's own HTTP stack. A local ``httpx.MockTransport`` (or
any non-network transport) answers ``200`` offline, so a producer could POST to
``api.anthropic.com`` and ``api.openai.com`` through it, never contact a model,
and forge two distinct COMPUTED models. The aiohttp seam is the same story: the
observer wraps whatever ``ClientSession._request`` is current when the scope
opens, so a mock patched in first would be attested as a real call. Only a real
network stack earns COMPUTED; anything else is a declaration (PROXY).
"""
from __future__ import annotations

import asyncio

import httpx
import pytest

import mareforma.observe as obs
from mareforma.observe import ModelLineageTier, _loaders

_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_OPENAI_URL = "https://api.openai.com/v1/chat/completions"


def _offline_client() -> httpx.Client:
    return httpx.Client(
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json={}))
    )


def test_mock_transport_post_is_not_computed() -> None:
    client = _offline_client()
    with obs.observe() as h:
        client.post(
            _ANTHROPIC_URL,
            json={
                "model": "claude-3-5-sonnet-20241022",
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
    client.close()
    lineage = h.verdict.model_lineage
    assert lineage is not None
    # A producer-controlled transport is a declaration, never execution-attested.
    assert lineage.tier is not ModelLineageTier.COMPUTED
    assert lineage.tier is ModelLineageTier.PROXY


def test_mock_transport_send_is_not_computed() -> None:
    client = _offline_client()
    with obs.observe() as h:
        req = client.build_request(
            "POST", _OPENAI_URL,
            json={"model": "gpt-4o-2024-08-06",
                  "messages": [{"role": "user", "content": "hi"}]},
        )
        client.send(req)
    client.close()
    lineage = h.verdict.model_lineage
    assert lineage is not None
    assert lineage.tier is ModelLineageTier.PROXY


def test_two_offline_models_are_not_a_distinct_pair() -> None:
    # The whole exploit: two distinct model strings sent through a local
    # transport must not read as verified cross-model corroboration.
    from mareforma.observe._lineage import model_distinct_pair

    client = _offline_client()
    with obs.observe() as ha:
        client.post(_ANTHROPIC_URL,
                    json={"model": "claude-3-5-sonnet-20241022",
                          "messages": [{"role": "user", "content": "x"}]})
    with obs.observe() as hb:
        client.post(_OPENAI_URL,
                    json={"model": "gpt-4o-2024-08-06",
                          "messages": [{"role": "user", "content": "x"}]})
    client.close()
    a = ha.verdict.model_lineage.to_dict()
    b = hb.verdict.model_lineage.to_dict()
    assert model_distinct_pair(a, b) is False


def _aiohttp_lineage(session, url: str, model: str):
    """Lineage from one offline aiohttp request through the observer's wrapper."""

    class _Resp:
        status = 200

    async def fake_request(self, *a, **k):
        return _Resp()

    wrapper = _loaders._make_aiohttp_request_wrapper(fake_request)

    async def go():
        with obs.observe() as h:
            await wrapper(
                session, "POST", url,
                json={"model": model,
                      "messages": [{"role": "user", "content": "x"}]},
            )
        return h.verdict.model_lineage

    return asyncio.run(go())


def test_offline_aiohttp_request_is_not_computed() -> None:
    lineage = _aiohttp_lineage(object(), _ANTHROPIC_URL,
                               "claude-3-5-sonnet-20241022")
    assert lineage is not None
    assert lineage.model_id == "claude-3-5-sonnet-20241022"
    assert lineage.tier is ModelLineageTier.PROXY


def test_two_offline_aiohttp_models_are_not_a_distinct_pair() -> None:
    # The httpx exploit over litellm's default transport: two model strings
    # through an offline _request must not read as cross-model corroboration.
    from mareforma.observe._lineage import model_distinct_pair

    a = _aiohttp_lineage(object(), _ANTHROPIC_URL, "claude-3-5-sonnet-20241022")
    b = _aiohttp_lineage(object(), _OPENAI_URL, "gpt-4o-2024-08-06")
    assert model_distinct_pair(a.to_dict(), b.to_dict()) is False


def test_pre_patched_aiohttp_session_is_not_computed() -> None:
    # The realistic delivery: the producer patches ClientSession._request before
    # the scope opens, so the observer wraps the mock over a genuine session with
    # a real TCP connector. The connector is not what earns COMPUTED.
    aiohttp = pytest.importorskip("aiohttp")

    async def build():
        return aiohttp.ClientSession()

    session = asyncio.run(build())
    try:
        lineage = _aiohttp_lineage(session, _OPENAI_URL, "gpt-4o-2024-08-06")
    finally:
        asyncio.run(session.close())
    assert lineage.tier is ModelLineageTier.PROXY


def test_aiohttp_gate_shapes_still_exist() -> None:
    # The gate is an allowlist over aiohttp's own shapes. If aiohttp moves them
    # the gate must be updated, not left quietly answering PROXY for every real
    # model call, so pin what it matches against the installed library.
    aiohttp = pytest.importorskip("aiohttp")

    real = _loaders._reals.get(
        "aiohttp.ClientSession._request",
        aiohttp.ClientSession.__dict__["_request"],
    )
    assert real.__module__ == "aiohttp.client"

    async def build():
        return aiohttp.ClientSession()

    session = asyncio.run(build())
    try:
        assert isinstance(session._connector, aiohttp.TCPConnector)
    finally:
        asyncio.run(session.close())


def test_real_network_transport_still_computed(httpx_mock) -> None:
    # pytest-httpx patches HTTPTransport.handle_request but leaves the client's
    # real network transport in place, standing in for a genuine provider call:
    # that path still earns COMPUTED.
    httpx_mock.add_response(url=_ANTHROPIC_URL, json={"content": []})
    client = httpx.Client()
    with obs.observe() as h:
        client.post(_ANTHROPIC_URL,
                    json={"model": "claude-3-5-sonnet-20241022",
                          "messages": [{"role": "user", "content": "hi"}]})
    client.close()
    assert h.verdict.model_lineage.tier is ModelLineageTier.COMPUTED
