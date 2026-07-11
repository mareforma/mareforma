"""False-UNGROUNDED holes closed: keep-alive HTTP, C-extension I/O, seam relevance.

UNGROUNDED must mean "the scope was fully observed and the cited data did not
arrive," never "the observer could not see the read." These tests pin the holes
that violated it — pooled HTTP sessions, C-runtime file readers — as OPAQUE, and
pin the seam-relevance matrix that keeps a socket seam from hiding the UNGROUNDED
tell on a local-file citation. Each is a known-bound regression guard: a change
that reopens the hole (flips one of these back to a confident UNGROUNDED, or a
header-based GROUNDED) fails here.
"""
from __future__ import annotations

import pytest

import mareforma.observe as obs
from mareforma.observe import ObservedGrounding as OG


@pytest.fixture
def cited_file(tmp_path):
    p = tmp_path / "trial.csv"
    p.write_text("arm,value\nA,1\nB,2\n")
    return str(p)


# -- seam-relevance matrix ---------------------------------------------------

def test_file_cited_socket_seam_stays_ungrounded():
    # The recovered tell: an in-scope LLM call (a socket seam) must NOT hide the
    # silent fallback on a file-cited finding. A socket cannot deliver a local
    # file read, so absence is still trustworthy.
    import socket

    with obs.observe(cites="/no/such/file.csv") as h:
        try:
            socket.create_connection(("127.0.0.1", 1), timeout=0.05)
        except OSError:
            pass
    assert h.verdict.grounding is OG.UNGROUNDED
    # The seam is still recorded — it is just not relevant to a file citation.
    assert any(s.kind == "socket" for s in h.verdict.seams)


def test_url_cited_socket_seam_is_opaque():
    import socket

    with obs.observe(cites="https://example.org/data.csv") as h:
        try:
            socket.create_connection(("127.0.0.1", 1), timeout=0.05)
        except OSError:
            pass
    assert h.verdict.grounding is OG.OPAQUE


def test_content_address_cited_socket_seam_is_opaque():
    # A sha256: citation's bytes can arrive over the network, so a socket seam
    # blocks its UNGROUNDED too.
    import socket

    with obs.observe(cites="sha256:" + "a" * 64) as h:
        try:
            socket.create_connection(("127.0.0.1", 1), timeout=0.05)
        except OSError:
            pass
    assert h.verdict.grounding is OG.OPAQUE


def test_content_address_cited_without_hashing_is_opaque(tmp_path):
    # Content addressing off: no read carries a hash, so a sha256: citation can
    # never match — even when the cited bytes WERE read. Absence of a match is
    # not evidence of absence; a confident UNGROUNDED here would be false.
    import hashlib

    data = tmp_path / "d.bin"
    data.write_bytes(b"payload-bytes")
    ca = "sha256:" + hashlib.sha256(b"payload-bytes").hexdigest()
    with obs.observe(cites=ca) as h:
        open(data, "rb").read()
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "coverage-gap" for s in h.verdict.seams)


def test_content_address_cited_hashing_on_unhashed_read_is_opaque(tmp_path):
    # Hashing on, but the bytes arrived through the open path, which cannot
    # hash them: the unhashed non-empty read could have carried the cited
    # bytes, so absence of a hash match still cannot be trusted.
    import hashlib

    data = tmp_path / "d.bin"
    data.write_bytes(b"payload-bytes")
    ca = "sha256:" + hashlib.sha256(b"payload-bytes").hexdigest()
    with obs.observe(cites=ca, content_address=True) as h:
        open(data, "rb").read()
    assert h.verdict.grounding is OG.OPAQUE


def test_content_address_cited_hashing_on_no_reads_is_ungrounded():
    # The floor is conditional, not blanket: hashing on and nothing read in a
    # fully observed scope means no channel could have carried the cited
    # bytes, and UNGROUNDED keeps its teeth.
    with obs.observe(cites="sha256:" + "a" * 64, content_address=True) as h:
        pass
    assert h.verdict.grounding is OG.UNGROUNDED


def test_mixed_file_url_socket_seam_is_opaque():
    # Conservative-ANY: a set with one URL citation is blocked by a socket seam
    # even though the file member alone would not be.
    import socket

    with obs.observe(cites=["/no/such/file.csv", "https://example.org/x"]) as h:
        try:
            socket.create_connection(("127.0.0.1", 1), timeout=0.05)
        except OSError:
            pass
    assert h.verdict.grounding is OG.OPAQUE


def test_thread_seam_blocks_file_citation():
    # A thread can hide any read, including a local file, so it blocks UNGROUNDED
    # for a file citation where a socket would not.
    import threading

    with obs.observe(cites="/no/such/file.csv") as h:
        t = threading.Thread(target=lambda: None)
        t.start()
        t.join()
    assert h.verdict.grounding is OG.OPAQUE


def test_url_cited_no_http_call_is_opaque_not_ungrounded():
    # A cited URL with zero observed HTTP coverage is unknown coverage, not
    # genuine absence: OPAQUE, never a confident UNGROUNDED.
    with obs.observe(cites="https://example.org/data.csv") as h:
        _ = 2 + 2
    assert h.verdict.grounding is OG.OPAQUE


# -- late-import hook --------------------------------------------------------

def test_late_import_requests_is_wrapped(cited_file):
    # refresh_third_party runs at scope entry; a module imported INSIDE the scope
    # must still be wrapped by the late-import hook, or its reads go unseen.
    responses = pytest.importorskip("responses")

    url = "https://example.org/data.csv"
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, url, body="col\n1\n", status=200)
        with obs.observe(cites=url) as h:
            import requests  # imported inside the open scope

            requests.get(url)
    assert h.verdict.grounding is OG.GROUNDED


def test_late_import_pandas_is_wrapped(cited_file):
    pytest.importorskip("pandas")
    with obs.observe(cites=cited_file) as h:
        import pandas  # imported inside the open scope

        pandas.read_csv(cited_file)
    assert h.verdict.grounding is OG.GROUNDED


def test_late_import_h5py_is_wrapped(tmp_path):
    h5py = pytest.importorskip("h5py")
    path = str(tmp_path / "d.h5")
    with h5py.File(path, "w") as f:
        f["x"] = [1, 2, 3]
    with obs.observe(cites=path) as h:
        import h5py as _h5  # re-import inside the scope

        with _h5.File(path, "r") as f:
            _ = f["x"][:]
    assert h.verdict.grounding is OG.GROUNDED


# -- keep-alive HTTP wrappers ------------------------------------------------

def test_requests_session_get_is_recorded():
    responses = pytest.importorskip("responses")
    requests = pytest.importorskip("requests")

    url = "https://example.org/data.csv"
    session = requests.Session()  # opened before the scope (pooled)
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, url, body="col\n1\n", status=200)
        with obs.observe(cites=url) as h:
            session.get(url)
    assert h.verdict.grounding is OG.GROUNDED


def test_requests_session_empty_body_is_ungrounded():
    responses = pytest.importorskip("responses")
    requests = pytest.importorskip("requests")

    url = "https://example.org/empty.csv"
    session = requests.Session()
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, url, body="", status=200)
        with obs.observe(cites=url) as h:
            session.get(url)
    # The HTTP read WAS observed and returned empty — genuine absence, the silent
    # fallback tell. UNGROUNDED here is honest (contrast the no-call case, which
    # is OPAQUE because coverage is unknown).
    assert h.verdict.grounding is OG.UNGROUNDED


def test_requests_module_level_get_is_recorded():
    # Closes the zero-coverage note: even the module-level requests.get had no
    # test.
    responses = pytest.importorskip("responses")
    requests = pytest.importorskip("requests")

    url = "https://example.org/data.csv"
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, url, body="col\n1\n", status=200)
        with obs.observe(cites=url) as h:
            requests.get(url)
    assert h.verdict.grounding is OG.GROUNDED


def test_requests_session_streaming_is_opaque_not_grounded():
    responses = pytest.importorskip("responses")
    requests = pytest.importorskip("requests")

    url = "https://example.org/stream.csv"
    session = requests.Session()
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, url, body="col\n1\n", status=200)
        with obs.observe(cites=url) as h:
            r = session.get(url, stream=True)
            r.close()
    # Streaming: the body is not available at return, so no header-based GROUNDED.
    assert h.verdict.grounding is OG.OPAQUE


def test_requests_session_post_is_recorded_by_url_not_method():
    # Regression: every verb routes through Session.request(method, url), so the
    # observer records the URL, never the HTTP method string. Before the fix a
    # wrapped `request` read args[0] ("POST") as the source — a cited POST went
    # unrecorded (false OPAQUE) and every GET logged a spurious "GET" read.
    responses = pytest.importorskip("responses")
    requests = pytest.importorskip("requests")

    url = "https://example.org/data.csv"
    session = requests.Session()
    with responses.RequestsMock() as rsps:
        rsps.add(responses.POST, url, body="col\n1\n", status=200)
        with obs.observe(cites=url) as h:
            session.post(url)
    # The cited URL was read (GROUNDED), and no read is named after the verb.
    assert h.verdict.grounding is OG.GROUNDED
    read_idents = {r.identifier for r in h.verdict.reads}
    assert "POST" not in read_idents
    assert "GET" not in read_idents


def test_requests_session_subclass_overriding_request_is_observed():
    # A Session subclass that OVERRIDES request (auth/retry SDK pattern) and does
    # not delegate to super().request would bypass a request-only wrapper entirely.
    # Wrapping the inherited verb methods keeps it observed, so a pooled URL-cited
    # read still grounds instead of landing a confident false UNGROUNDED.
    requests = pytest.importorskip("requests")

    class DirectSession(requests.Session):
        def request(self, method, url, *args, **kwargs):
            # Custom transport, no super().request() call — only the inherited
            # verb wrapper can observe this response.
            resp = requests.Response()
            resp.status_code = 200
            resp._content = b"col\n1\n"
            resp.url = url
            return resp

    url = "https://example.org/data.csv"
    session = DirectSession()
    with obs.observe(cites=url) as h:
        session.get(url)
    assert h.verdict.grounding is OG.GROUNDED


def test_httpx_client_get_is_recorded(httpx_mock):
    import httpx

    url = "https://example.org/data.csv"
    httpx_mock.add_response(url=url, text="col\n1\n")
    client = httpx.Client()
    with obs.observe(cites=url) as h:
        client.get(url)
    client.close()
    assert h.verdict.grounding is OG.GROUNDED


def test_httpx_async_client_get_is_recorded(httpx_mock):
    import asyncio

    import httpx

    url = "https://example.org/data.csv"
    httpx_mock.add_response(url=url, text="col\n1\n")

    async def go():
        client = httpx.AsyncClient()
        with obs.observe(cites=url) as h:
            await client.get(url)
        await client.aclose()
        return h.verdict.grounding

    assert asyncio.run(go()) is OG.GROUNDED


def test_aiohttp_request_records_a_socket_seam():
    # aiohttp streams the body (read in host code after our wrapper returns), so
    # the wrapper records a socket seam rather than a header-based read — a pooled
    # aiohttp GET of a cited URL therefore lands OPAQUE, never false UNGROUNDED.
    # Driven through the wrapper directly with a stub coroutine so the test needs
    # no live network (aioresponses does not track current aiohttp).
    pytest.importorskip("aiohttp")
    import asyncio

    from mareforma.observe import _loaders

    async def fake_request(self, *a, **k):
        return "response"

    wrapper = _loaders._make_async_http_method_wrapper(fake_request, streaming=True)

    async def go():
        with obs.observe(cites="https://example.org/data.csv") as h:
            await wrapper(object(), "GET", "https://example.org/data.csv")
        return h.verdict

    verdict = asyncio.run(go())
    assert verdict.grounding is OG.OPAQUE
    assert any(s.kind == "socket" for s in verdict.seams)


# -- C-extension readers -----------------------------------------------------

def test_cited_h5_with_no_wrapper_is_opaque_floor(tmp_path):
    # A cited .h5 path with no PEP-578 open event and no wrapped h5py read is a
    # coverage gap, not genuine absence: OPAQUE, never UNGROUNDED. Simulate the
    # "reader we could not see" by never opening it through an instrumented path.
    path = str(tmp_path / "data.h5")
    open(path, "wb").write(b"\x89HDF\r\n\x1a\n" + b"0" * 64)
    with obs.observe(cites=path) as h:
        _ = 2 + 2  # nothing reads the cited h5 through an instrumented path
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "coverage-gap" for s in h.verdict.seams)


def test_wrapped_h5py_read_is_grounded(tmp_path):
    h5py = pytest.importorskip("h5py")
    path = str(tmp_path / "d.h5")
    with h5py.File(path, "w") as f:
        f["x"] = [1, 2, 3]
    with obs.observe(cites=path) as h:
        with h5py.File(path, "r") as f:
            _ = f["x"][:]
    assert h.verdict.grounding is OG.GROUNDED


def test_wrapped_h5py_grounded_reason_does_not_overclaim_byte_flow(tmp_path):
    # The C-extension readers use a stat-based non-emptiness proxy exactly like
    # builtins.open — they never see the bytes. The signed reason must say so, not
    # claim the read "returned non-empty data" as the sqlite/http wrappers (which
    # do observe returns) may.
    h5py = pytest.importorskip("h5py")
    path = str(tmp_path / "d.h5")
    with h5py.File(path, "w") as f:
        f["x"] = [1, 2, 3]
    with obs.observe(cites=path) as h:
        with h5py.File(path, "r") as f:
            _ = f["x"][:]
    assert h.verdict.grounding is OG.GROUNDED
    assert "does not observe the bytes read" in h.verdict.reason
    assert "returned non-empty data" not in h.verdict.reason


def test_wrapped_pyarrow_read_is_grounded(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = str(tmp_path / "d.parquet")
    pq.write_table(pa.table({"x": [1, 2, 3]}), path)
    with obs.observe(cites=path) as h:
        pq.read_table(path)
    assert h.verdict.grounding is OG.GROUNDED


def test_cited_parquet_with_no_read_is_opaque(tmp_path):
    pytest.importorskip("pyarrow")
    path = str(tmp_path / "d.parquet")
    open(path, "wb").write(b"PAR1" + b"0" * 32 + b"PAR1")
    with obs.observe(cites=path) as h:
        _ = 2 + 2
    assert h.verdict.grounding is OG.OPAQUE


# -- fail-safe ---------------------------------------------------------------

def test_http_wrapper_error_is_opaque_host_unaffected(monkeypatch):
    # A failure inside the observer's own HTTP recording must degrade to OPAQUE
    # and never re-raise into the host: the host call still returns its body.
    responses = pytest.importorskip("responses")
    requests = pytest.importorskip("requests")
    from mareforma.observe import _loaders

    url = "https://example.org/data.csv"

    def boom(*a, **k):
        raise RuntimeError("observer bug")

    monkeypatch.setattr(_loaders, "_record_http_response", boom)
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, url, body="col\n1\n", status=200)
        with obs.observe(cites=url) as h:
            r = requests.get(url)
    assert r.text == "col\n1\n"  # host result intact
    assert h.verdict.grounding is OG.OPAQUE
