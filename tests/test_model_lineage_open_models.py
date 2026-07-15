"""Open-weight model families root to a family release, never a vendor string.

The same open weights are served under provider-specific names
(``meta-llama/Llama-3.1-70B-Instruct``, ``llama-3.1-70b-versatile``,
``llama-v3p1-70b-instruct``), so the root is the family plus its release
number: coarse enough that one release served by two hosts collapses to ONE
model on the independence axis, never two. The rounding direction is safety:
a root that cannot be parsed to a family release is UNVERIFIABLE, and two
variants of one family count as the same model, so naming variance can only
under-claim distinctness, never mint a fake independent line.
"""
from __future__ import annotations

import pytest

from mareforma.observe._lineage import (
    ModelLineageTier,
    model_distinct_pair,
    resolve_lineage,
)
from mareforma.observe._loaders import _provider_of


def _declared(model_id):
    return resolve_lineage(
        model_id, source="declared", method="agent-sdk",
        decoding={"temperature": None, "top_p": None, "seed": None},
    )


def _hosted(model_id, provider):
    return resolve_lineage(
        model_id, source="socket", method="/v1/chat/completions",
        decoding={"temperature": None, "top_p": None, "seed": None},
        provider=provider,
    )


# -- family roots -------------------------------------------------------------

@pytest.mark.parametrize(
    ("model_id", "root"),
    [
        ("llama-3.1-70b-versatile", "llama-3.1"),
        ("meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo", "llama-3.1"),
        ("accounts/fireworks/models/llama-v3p1-70b-instruct", "llama-3.1"),
        ("llama3-8b-8192", "llama-3"),
        ("Qwen/Qwen2.5-72B-Instruct", "qwen-2.5"),
        ("qwen3-235b-a22b", "qwen-3"),
        ("mistralai/Mistral-7B-Instruct-v0.3", "mistral"),
        ("mistral-large-2411", "mistral"),
        ("mixtral-8x7b-32768", "mixtral-8x7b"),
        ("google/gemma-3-27b-it", "gemma-3"),
        ("deepseek-v3", "deepseek-v3"),
        ("deepseek-r1", "deepseek-r1"),
        ("deepseek-chat", "deepseek"),
        ("microsoft/phi-4", "phi-4"),
        ("gemini-2.5-pro", "gemini-2.5"),
        ("grok-4", "grok-4"),
        ("zai-org/GLM-4.5", "glm-4.5"),
        ("moonshotai/Kimi-K2-Instruct", "kimi-k2"),
    ],
)
def test_open_family_roots(model_id, root):
    lin = _declared(model_id)
    assert lin.tier is ModelLineageTier.PROXY
    assert lin.family_root == root


def test_unrooted_open_names_stay_unverifiable():
    # A name that parses to no family release cannot certify a distinct model.
    for model_id in ("my-lab-model", "llama-guard-1b", "qwq-32b", "gemini-pro"):
        assert _declared(model_id).tier is ModelLineageTier.UNVERIFIABLE


def test_alias_and_finetune_guards_dominate_open_families():
    # Soft lineage rules are unchanged: a moving alias or a hosted fine-tune of
    # an open family never roots, exactly as for the closed families.
    assert _declared("llama-3.1-latest").tier is ModelLineageTier.UNVERIFIABLE
    assert _declared("ft:llama-3.1-8b:acme::x1").tier is ModelLineageTier.UNVERIFIABLE


# -- recognized provider hosts ------------------------------------------------

@pytest.mark.parametrize(
    ("url", "provider"),
    [
        ("https://api.groq.com/openai/v1/chat/completions", "groq"),
        ("https://api.together.xyz/v1/chat/completions", "together"),
        ("https://api.together.ai/v1/chat/completions", "together"),
        ("https://api.fireworks.ai/inference/v1/chat/completions", "fireworks"),
        ("https://api.mistral.ai/v1/chat/completions", "mistral"),
        ("https://api.deepseek.com/chat/completions", "deepseek"),
        ("https://api.anthropic.com/v1/messages", "anthropic"),
        ("https://api.openai.com/v1/chat/completions", "openai"),
    ],
)
def test_recognized_provider_hosts(url, provider):
    assert _provider_of(url) == provider


def test_router_and_lookalike_hosts_are_not_providers():
    # A router's host does not pin which upstream served the weights, and a
    # lookalike host is an attacker naming a provider inside a URL they control.
    for url in (
        "https://openrouter.ai/api/v1/chat/completions",
        "https://evil.com/groq",
        "https://api.groq.com.attacker.net/v1",
        "http://localhost:8000/v1/chat/completions",
    ):
        assert _provider_of(url) is None


# -- independence semantics across hosts --------------------------------------

def test_same_release_on_two_hosts_collapses_to_one_model():
    # The whole point of family rooting: one open release served by two
    # recognized hosts is ONE model, never a forged pair of independent lines.
    a = _hosted("llama-3.1-70b-versatile", "groq").to_dict()
    b = _hosted("meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo", "together").to_dict()
    assert a["tier"] == "COMPUTED" and b["tier"] == "COMPUTED"
    assert model_distinct_pair(a, b) is False


def test_distinct_families_on_recognized_hosts_are_distinct():
    a = _hosted("llama-3.1-70b-versatile", "groq").to_dict()
    b = _hosted("deepseek-v3", "deepseek").to_dict()
    assert model_distinct_pair(a, b) is True


def test_two_releases_of_one_family_are_distinct():
    a = _hosted("llama-3.1-70b-versatile", "groq").to_dict()
    b = _hosted("meta-llama/Llama-4-Scout-17B-16E-Instruct", "together").to_dict()
    assert model_distinct_pair(a, b) is True


def test_open_model_at_unrecognized_host_stays_unverifiable():
    # The host gate is unchanged: family rooting never upgrades a socket
    # capture at an endpoint the producer controls.
    lin = resolve_lineage(
        "llama-3.1-70b-instruct", source="socket", method="/v1/chat/completions",
        decoding={"temperature": None, "top_p": None, "seed": None},
        provider=None,
    )
    assert lin.tier is ModelLineageTier.UNVERIFIABLE
    assert lin.family_root is None
