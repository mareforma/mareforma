"""A producer-controlled httpx transport cannot mint COMPUTED lineage.

COMPUTED is the trustworthy tier: a body-parse at the socket seam to a
recognized provider host. The provider host is genuine, but the 2xx response is
produced by the producer's own HTTP stack. A local ``httpx.MockTransport`` (or
any non-network transport) answers ``200`` offline, so a producer could POST to
``api.anthropic.com`` and ``api.openai.com`` through it, never contact a model,
and forge two distinct COMPUTED models. Only a real network transport earns
COMPUTED; a producer-controlled transport is a declaration (PROXY).
"""
from __future__ import annotations

import httpx

import mareforma.observe as obs
from mareforma.observe import ModelLineageTier

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
