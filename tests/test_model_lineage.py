"""Model/method lineage recorded on the evidence line, tiered like data_id.

The lineage captured in an ``observe()`` scope rides the grounding verdict into
``assert_finding`` / ``submit_finding``, is written to the additive
``evidence_lines.model_lineage`` column, and is COMPUTED / PROXY / UNVERIFIABLE
exactly as the observer tiered it. A finding authored without a model call leaves
the column NULL, byte-identical to a pre-observer finding.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import mareforma
import mareforma.observe as mobs
from mareforma.observe import declare_model
from tests._helpers import _bootstrap_key, _est, _pred, _prop


def _lineage_rows(tmp_path: Path) -> list[str | None]:
    conn = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
    try:
        return [
            r[0]
            for r in conn.execute(
                "SELECT model_lineage FROM evidence_lines"
            ).fetchall()
        ]
    finally:
        conn.close()


def test_model_lineage_persisted_on_evidence_line(tmp_path: Path) -> None:
    key = _bootstrap_key(tmp_path, "root.key")
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=key) as g:
        with mobs.observe() as h:
            declare_model("gpt-4o-2024-08-06", method="agent-sdk", temperature=0.2)
        result = g.assert_finding(
            prop, pred, _est(), data_id="ds1", generated_by="run1",
            grounding=h.verdict,
        )
    assert result["model_lineage"]["tier"] == "PROXY"
    assert result["model_lineage"]["family_root"] == "gpt-4o"

    (stored,) = _lineage_rows(tmp_path)
    assert stored is not None
    parsed = json.loads(stored)
    assert parsed["tier"] == "PROXY"
    assert parsed["model_id"] == "gpt-4o-2024-08-06"
    assert parsed["family_root"] == "gpt-4o"


def test_finding_without_model_leaves_lineage_null(tmp_path: Path) -> None:
    key = _bootstrap_key(tmp_path, "root.key")
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=key) as g:
        with mobs.observe() as h:
            _ = 2 + 2  # no model call authored the finding
        result = g.assert_finding(
            prop, pred, _est(), data_id="ds1", generated_by="run1",
            grounding=h.verdict,
        )
    assert result["model_lineage"] is None
    (stored,) = _lineage_rows(tmp_path)
    assert stored is None


def test_finetune_lineage_persisted_unverifiable(tmp_path: Path) -> None:
    key = _bootstrap_key(tmp_path, "root.key")
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=key) as g:
        with mobs.observe() as h:
            declare_model("ft:gpt-4o-2024-08-06:acme::rExAbC12")
        result = g.assert_finding(
            prop, pred, _est(), data_id="ds1", generated_by="run1",
            grounding=h.verdict,
        )
    assert result["model_lineage"]["tier"] == "UNVERIFIABLE"
    assert result["model_lineage"]["family_root"] is None
    (stored,) = _lineage_rows(tmp_path)
    assert json.loads(stored)["tier"] == "UNVERIFIABLE"


# -- local content-addressed lineage (weights digest) ------------------------

def test_local_digest_is_computed_by_weights_attestor():
    # A local model call (no recognized remote host) whose weights digest the
    # observer resolved is COMPUTED via the weights-digest attestor, even though
    # the model name (qwen3:0.6b) roots to no known remote family.
    from mareforma.observe._lineage import ModelLineageTier, resolve_lineage

    lin = resolve_lineage(
        "qwen3:0.6b", source="socket", method="/api/chat",
        decoding={"temperature": None, "top_p": None, "seed": None},
        provider=None, digest="sha256:7df6b6e09427",
    )
    assert lin.tier is ModelLineageTier.COMPUTED
    assert lin.attestor == "weights-digest"
    assert lin.digest == "sha256:7df6b6e09427"


def test_local_socket_without_digest_stays_unverifiable():
    # The same local call WITHOUT a resolved digest is unchanged: UNVERIFIABLE,
    # never COMPUTED off a producer-controlled endpoint alone.
    from mareforma.observe._lineage import ModelLineageTier, resolve_lineage

    lin = resolve_lineage(
        "qwen3:0.6b", source="socket", method="/api/chat",
        decoding={"temperature": None, "top_p": None, "seed": None},
        provider=None, digest=None,
    )
    assert lin.tier is ModelLineageTier.UNVERIFIABLE
    assert lin.attestor is None


def test_recognized_host_attestor_is_provider_host():
    from mareforma.observe._lineage import ModelLineageTier, resolve_lineage

    lin = resolve_lineage(
        "claude-3-5-sonnet-20241022", source="socket", method="/v1/messages",
        decoding={"temperature": None, "top_p": None, "seed": None},
        provider="anthropic",
    )
    assert lin.tier is ModelLineageTier.COMPUTED
    assert lin.attestor == "provider-host"


def test_digest_drives_distinctness_and_collapse():
    # Two distinct weights digests are distinct models; the same digest collapses.
    from mareforma.observe._lineage import model_distinct_pair, resolve_lineage

    def lin(dig):
        return resolve_lineage(
            "qwen3:0.6b", source="socket", method="/api/chat",
            decoding={"temperature": None, "top_p": None, "seed": None},
            provider=None, digest=dig,
        ).to_dict()

    assert model_distinct_pair(lin("sha256:aaa"), lin("sha256:bbb")) is True
    assert model_distinct_pair(lin("sha256:aaa"), lin("sha256:aaa")) is False


# -- collapse_lineage over a multi-model span --------------------------------

def _socket(model_id, provider=None, digest=None):
    from mareforma.observe._lineage import resolve_lineage
    return resolve_lineage(
        model_id, source="socket", method="/api/chat",
        decoding={"temperature": None, "top_p": None, "seed": None},
        provider=provider, digest=digest,
    )


def test_collapse_single_record_passthrough():
    from mareforma.observe._lineage import collapse_lineage, ModelLineageTier
    lin = _socket("qwen3:0.6b", digest="sha256:aaa")
    out = collapse_lineage([lin])
    assert out is lin
    assert out.tier is ModelLineageTier.COMPUTED


def test_collapse_same_local_model_stays_computed_digest():
    # The send + aiohttp double-record of ONE local call: same digest twice
    # collapses to the single COMPUTED weights-digest model, not a mixed span.
    from mareforma.observe._lineage import collapse_lineage, ModelLineageTier
    a = _socket("qwen3:0.6b", digest="sha256:aaa")
    b = _socket("qwen3:0.6b", digest="sha256:aaa")
    out = collapse_lineage([a, b])
    assert out.tier is ModelLineageTier.COMPUTED
    assert out.attestor == "weights-digest"
    assert out.digest == "sha256:aaa"


def test_collapse_two_distinct_local_digests_is_unverifiable():
    # Two distinct LOCAL models (both family_root None) must NOT collapse to one
    # COMPUTED model. Distinctness keys on the digest, so a mixed local span is
    # UNVERIFIABLE with no stale identity.
    from mareforma.observe._lineage import (
        collapse_lineage, independence_model_key, ModelLineageTier,
    )
    a = _socket("qwen3:0.6b", digest="sha256:aaa")
    b = _socket("llama3:8b", digest="sha256:bbb")
    out = collapse_lineage([a, b])
    assert out.tier is ModelLineageTier.UNVERIFIABLE
    assert out.digest is None and out.attestor is None
    assert independence_model_key(out.to_dict()) == ("soft",)


def test_collapse_local_plus_remote_drops_stale_digest():
    # A local + remote span must not carry the local digest onto a remote root:
    # it is a mixed span -> UNVERIFIABLE, and independence never keys on the
    # stale digest.
    from mareforma.observe._lineage import (
        collapse_lineage, independence_model_key, ModelLineageTier,
    )
    local = _socket("qwen3:0.6b", digest="sha256:aaa")
    remote = _socket("claude-3-5-sonnet-20241022", provider="anthropic")
    out = collapse_lineage([local, remote])
    assert out.tier is ModelLineageTier.UNVERIFIABLE
    assert out.digest is None
    assert independence_model_key(out.to_dict()) == ("soft",)


def test_collapse_two_distinct_remote_roots_is_unverifiable():
    from mareforma.observe._lineage import collapse_lineage, ModelLineageTier
    a = _socket("claude-3-5-sonnet-20241022", provider="anthropic")
    b = _socket("gpt-4o-2024-08-06", provider="openai")
    assert collapse_lineage([a, b]).tier is ModelLineageTier.UNVERIFIABLE


# -- the local Ollama digest probe (no live server) --------------------------

class _FakeResp:
    def __init__(self, payload: bytes):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self, n=-1):
        return self._payload


def test_probe_extracts_digest_from_api_ps(monkeypatch):
    import json
    import urllib.request
    from mareforma.observe import _loaders
    payload = json.dumps(
        {"models": [{"name": "qwen3:0.6b", "digest": "7df6b6e0"}]}
    ).encode()
    monkeypatch.setattr(
        urllib.request.OpenerDirector, "open",
        lambda self, req, timeout=None: _FakeResp(payload),
    )
    dig = _loaders._probe_ollama_digest("http://localhost:11434/api/chat", "qwen3:0.6b")
    assert dig == "sha256:7df6b6e0"  # normalized with the sha256: prefix


def test_probe_returns_none_on_failure(monkeypatch):
    import urllib.request
    from mareforma.observe import _loaders

    def boom(self, req, timeout=None):
        raise TimeoutError("slow server")

    monkeypatch.setattr(urllib.request.OpenerDirector, "open", boom)
    assert _loaders._probe_ollama_digest("http://localhost:11434/api/chat", "m") is None


def test_probe_never_contacts_a_remote_host(monkeypatch):
    # The security guarantee: a non-loopback host is never probed at all.
    import urllib.request
    from mareforma.observe import _loaders
    calls = []
    monkeypatch.setattr(
        urllib.request.OpenerDirector, "open",
        lambda self, req, timeout=None: calls.append(req) or _FakeResp(b"{}"),
    )
    assert _loaders._probe_ollama_digest("https://api.openai.com/v1/chat", "m") is None
    assert calls == []  # never opened a connection for a remote host


def test_probe_does_not_follow_redirects_off_host():
    # Security regression guard for the REAL opener: it has no redirect handler,
    # so a loopback server that 302s must NOT be followed to the redirect target.
    # Run a live local server that redirects /api/ps -> /followed and assert the
    # target path is never requested and no digest leaks back.
    import http.server
    import json
    import threading
    from mareforma.observe import _loaders

    hits = []

    class _H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            hits.append(self.path)
            if self.path == "/api/ps":
                self.send_response(302)
                self.send_header("Location", "/followed")
                self.end_headers()
            elif self.path == "/followed":
                body = json.dumps(
                    {"models": [{"name": "m", "digest": "deadbeef"}]}
                ).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *a):  # silence the test server
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 0), _H)
    port = srv.server_address[1]
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        dig = _loaders._probe_ollama_digest(f"http://127.0.0.1:{port}/api/chat", "m")
    finally:
        srv.shutdown()
        t.join(timeout=2)
    assert dig is None            # the 302 was not followed, so no digest
    assert "/followed" not in hits  # the redirect target was never contacted
