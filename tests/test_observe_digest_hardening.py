"""The local weights probe accepts only content-addressed digests.

Other local servers ship Ollama-compatible surfaces whose ``digest`` field is
not a hash of the served weights: SGLang answers ``/api/tags`` with a hardcoded
non-hex sentinel, and LocalAI answers with the sha256 of the producer-chosen
model NAME. Both satisfy the probe's loopback ``/api/`` trigger, so an
unvalidated probe would mint a COMPUTED weights-digest lineage off a fabricated
identity and let one producer forge cross-model independence with off-the-shelf
software. The probe therefore rejects any digest that is not a well-formed
sha256 payload, and any digest equal to the sha256 of the model's own name,
failing closed to no digest (the call stays UNVERIFIABLE).
"""
from __future__ import annotations

import hashlib
import json
import urllib.request

import pytest

from mareforma.observe import _loaders

# A realistic Ollama digest: the full 64-hex sha256 of the model manifest.
_REAL_DIGEST = hashlib.sha256(b"weights bytes, not a name").hexdigest()

# SGLang's Ollama-compat surface returns this constant for every model.
_SGLANG_SENTINEL = "sha256:sglang0000000000000000000000000000000000000000000000000000000000"


class _FakeResp:
    def __init__(self, payload: bytes):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self, n=-1):
        return self._payload


@pytest.fixture(autouse=True)
def _clear_probe_cache():
    _loaders._ollama_digest_cache.clear()
    yield
    _loaders._ollama_digest_cache.clear()


def _serve(monkeypatch, models: list[dict]) -> None:
    payload = json.dumps({"models": models}).encode()
    monkeypatch.setattr(
        urllib.request.OpenerDirector,
        "open",
        lambda self, req, timeout=None: _FakeResp(payload),
    )


def test_real_content_digest_is_accepted(monkeypatch):
    _serve(monkeypatch, [{"name": "qwen3:0.6b", "digest": _REAL_DIGEST}])
    dig = _loaders._probe_ollama_digest("http://localhost:11434/api/chat", "qwen3:0.6b")
    assert dig == "sha256:" + _REAL_DIGEST


def test_sglang_sentinel_digest_is_rejected(monkeypatch):
    # SGLang serves /api/tags unconditionally with a constant non-hex sentinel.
    # A probe that accepted it would mint COMPUTED for a model SGLang never
    # content-addressed, and the constant would score as a distinct model.
    _serve(monkeypatch, [{"name": "llama-3.1-70b", "digest": _SGLANG_SENTINEL}])
    assert (
        _loaders._probe_ollama_digest("http://localhost:30000/api/chat", "llama-3.1-70b")
        is None
    )


def test_name_hash_digest_is_rejected(monkeypatch):
    # LocalAI's Ollama-compat surface returns sha256(model-name): well-formed
    # hex, but a hash of a producer-chosen string, not of the served weights.
    name = "my-renamed-model"
    _serve(monkeypatch, [{"name": name, "digest": hashlib.sha256(name.encode()).hexdigest()}])
    assert (
        _loaders._probe_ollama_digest("http://localhost:8080/api/chat", name)
        is None
    )


def test_prefixed_name_hash_digest_is_rejected(monkeypatch):
    # The same name-hash with a sha256: prefix must not slip past normalization.
    name = "my-renamed-model"
    dig = "sha256:" + hashlib.sha256(name.encode()).hexdigest()
    _serve(monkeypatch, [{"name": name, "digest": dig}])
    assert (
        _loaders._probe_ollama_digest("http://localhost:8080/api/chat", name)
        is None
    )


def test_short_or_malformed_digest_is_rejected(monkeypatch):
    # A truncated or non-hex digest is not a sha256 payload and cannot be a
    # content address; fail closed rather than normalize it into one.
    for bad in ("abc", "sha256:abc", "sha256:" + "g" * 64, ""):
        _loaders._ollama_digest_cache.clear()
        _serve(monkeypatch, [{"name": "m", "digest": bad}])
        assert (
            _loaders._probe_ollama_digest("http://localhost:11434/api/chat", "m")
            is None
        )


def test_rejection_is_cached_like_any_negative(monkeypatch):
    opens: list[str] = []
    payload = json.dumps(
        {"models": [{"name": "m", "digest": _SGLANG_SENTINEL}]}
    ).encode()

    def fake_open(self, req, timeout=None):
        opens.append(req.full_url)
        return _FakeResp(payload)

    monkeypatch.setattr(urllib.request.OpenerDirector, "open", fake_open)
    for _ in range(3):
        assert _loaders._probe_ollama_digest("http://localhost:11434/api/chat", "m") is None
    # /api/ps yields the sentinel (rejected), then /api/tags the same; the None
    # is cached, so later calls never re-probe.
    assert len(opens) == 2
