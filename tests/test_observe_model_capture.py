"""Model/method lineage captured at the call boundary, tiered like data_id.

The observer wraps ``httpx`` POST and parses the request JSON body for the model
field (Anthropic and OpenAI shapes). A body-parse at the socket seam is COMPUTED
(the producer does not control that path); a producer-declared model is PROXY;
a hosted fine-tune, alias, or wrapper whose base is not declarable is
UNVERIFIABLE. No model call in scope leaves the lineage absent, never fabricated.
"""
from __future__ import annotations

import asyncio

import httpx
import pytest

import mareforma.observe as obs
from mareforma.observe import ModelLineageTier, declare_model

_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_OPENAI_URL = "https://api.openai.com/v1/chat/completions"


# -- COMPUTED: body-parse at the socket seam ---------------------------------

def test_httpx_post_model_captured_anthropic(httpx_mock):
    httpx_mock.add_response(url=_ANTHROPIC_URL, json={"content": []})
    client = httpx.Client()
    # A streaming POST (body carries stream=true) must still capture the model:
    # the request body is available at the socket seam even when the response is
    # never materialized.
    with obs.observe() as h:
        client.post(
            _ANTHROPIC_URL,
            json={
                "model": "claude-3-5-sonnet-20241022",
                "temperature": 0.0,
                "top_p": 0.9,
                "stream": True,
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
    client.close()

    lineage = h.verdict.model_lineage
    assert lineage is not None
    assert lineage.tier is ModelLineageTier.COMPUTED
    assert lineage.model_id == "claude-3-5-sonnet-20241022"
    assert lineage.family_root == "claude-3-5-sonnet"
    assert lineage.version == "20241022"
    assert lineage.provider == "anthropic"
    assert lineage.decoding["temperature"] == 0.0
    assert lineage.decoding["top_p"] == 0.9
    assert lineage.method  # a method tag identifying the tool/pipeline call
    # A streaming POST records a socket seam for the response (byte flow unseen)
    # without forcing the body into memory.
    assert any(s.kind == "socket" for s in h.verdict.seams)


def test_httpx_post_model_captured_openai(httpx_mock):
    httpx_mock.add_response(url=_OPENAI_URL, json={"choices": []})

    async def go():
        client = httpx.AsyncClient()
        with obs.observe() as h:
            await client.post(
                _OPENAI_URL,
                json={
                    "model": "gpt-4o-2024-08-06",
                    "temperature": 0.7,
                    "top_p": 1.0,
                    "seed": 42,
                    "stream": True,
                    "messages": [{"role": "user", "content": "hi"}],
                },
            )
        await client.aclose()
        return h.verdict

    verdict = asyncio.run(go())
    lineage = verdict.model_lineage
    assert lineage is not None
    assert lineage.tier is ModelLineageTier.COMPUTED
    assert lineage.model_id == "gpt-4o-2024-08-06"
    assert lineage.family_root == "gpt-4o"
    assert lineage.version == "2024-08-06"
    assert lineage.provider == "openai"
    assert lineage.decoding["temperature"] == 0.7
    assert lineage.decoding["seed"] == 42
    assert any(s.kind == "socket" for s in verdict.seams)


# -- PROXY: producer-declared ------------------------------------------------

def test_producer_wrapper_is_proxy():
    # A cooperating producer that does not route through a wrapped httpx POST
    # declares the model. A declaration is agent-attested, so it is PROXY even
    # for a base that would family-root cleanly — never COMPUTED.
    with obs.observe() as h:
        declare_model(
            "claude-3-5-sonnet-20241022",
            method="agent-sdk",
            temperature=0.0,
        )
    lineage = h.verdict.model_lineage
    assert lineage is not None
    assert lineage.tier is ModelLineageTier.PROXY
    assert lineage.family_root == "claude-3-5-sonnet"
    assert lineage.method == "agent-sdk"


# -- absent: no model call ---------------------------------------------------

def test_no_model_call_leaves_lineage_absent():
    with obs.observe() as h:
        _ = 2 + 2  # no model call in scope
    assert h.verdict.model_lineage is None


# -- UNVERIFIABLE: soft lineage ----------------------------------------------

def test_family_rooted_finetune_is_unverifiable(httpx_mock):
    # A hosted fine-tune string carries a base the observer cannot declare: it
    # must read UNVERIFIABLE, not as a counted distinct model.
    httpx_mock.add_response(url=_OPENAI_URL, json={"choices": []})
    client = httpx.Client()
    with obs.observe() as h:
        client.post(
            _OPENAI_URL,
            json={
                "model": "ft:gpt-4o-2024-08-06:acme::rExAbC12",
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
    client.close()
    lineage = h.verdict.model_lineage
    assert lineage is not None
    assert lineage.tier is ModelLineageTier.UNVERIFIABLE
    # A distinct model STRING with an undeclarable base is not a distinct model:
    # the family root does not resolve.
    assert lineage.family_root is None


def test_soft_lineage_is_unverifiable():
    # A moving alias (the base weights are not declarable) is UNVERIFIABLE even
    # when declared: soft lineage dominates the PROXY tier a declaration carries.
    with obs.observe() as h:
        declare_model("claude-3-5-sonnet-latest", method="agent-sdk")
    lineage = h.verdict.model_lineage
    assert lineage is not None
    assert lineage.tier is ModelLineageTier.UNVERIFIABLE
    assert lineage.family_root is None
