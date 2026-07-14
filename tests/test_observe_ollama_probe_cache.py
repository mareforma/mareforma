"""The local Ollama digest probe is memoized per (server, model).

Without a cache the probe fires one to two blocking HTTP GETs against the running
inference server on every observed model call, injecting round-trips into the
host pipeline's own path and doubling request load onto a server that may already
be busy generating. The lineage answer is identical every call, so the first
resolution is cached (including a negative result) and reused within a short TTL.
"""
from __future__ import annotations

import json
import urllib.request

import pytest

from mareforma.observe import _loaders


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


def test_repeated_calls_probe_the_server_once(monkeypatch):
    payload = json.dumps(
        {"models": [{"name": "cachedmodel", "digest": "abc"}]}
    ).encode()
    opens: list[str] = []

    def fake_open(self, req, timeout=None):
        opens.append(req.full_url)
        return _FakeResp(payload)

    monkeypatch.setattr(urllib.request.OpenerDirector, "open", fake_open)
    for _ in range(5):
        dig = _loaders._probe_ollama_digest(
            "http://localhost:11434/api/chat", "cachedmodel"
        )
        assert dig == "sha256:abc"
    assert len(opens) == 1  # one probe, then the cache serves the rest


def test_negative_result_is_cached_not_reprobed(monkeypatch):
    opens: list[str] = []

    def fake_open(self, req, timeout=None):
        opens.append(req.full_url)
        raise TimeoutError("slow server")

    monkeypatch.setattr(urllib.request.OpenerDirector, "open", fake_open)
    for _ in range(4):
        assert (
            _loaders._probe_ollama_digest("http://localhost:11434/api/chat", "gone")
            is None
        )
    # First probe tries /api/ps and /api/tags (two opens, both fail); the None is
    # cached, so no later call re-probes.
    assert len(opens) == 2
