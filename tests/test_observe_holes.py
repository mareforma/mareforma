"""False-UNGROUNDED holes closed: keep-alive HTTP, C-extension I/O, seam relevance.

UNGROUNDED must mean "the scope was fully observed and the cited data did not
arrive," never "the observer could not see the read." These tests pin the holes
that violated it, pooled HTTP sessions, C-runtime file readers, as OPAQUE, and
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
    # The seam is still recorded, it is just not relevant to a file citation.
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
    # never match, even when the cited bytes WERE read. Absence of a match is
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
    # The HTTP read WAS observed and returned empty, genuine absence, the silent
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
    # wrapped `request` read args[0] ("POST") as the source, a cited POST went
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
            # Custom transport, no super().request() call, only the inherited
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


def test_httpx_base_url_relative_get_grounds_the_absolute_url(httpx_mock):
    # The modal httpx idiom: a client with base_url and a relative path. The
    # read must be recorded under the absolute URL that was fetched, so a
    # finding citing that URL is GROUNDED instead of a false OPAQUE.
    import httpx

    httpx_mock.add_response(url="https://example.org/data.csv", text="col\n1\n")
    client = httpx.Client(base_url="https://example.org")
    with obs.observe(cites="https://example.org/data.csv") as h:
        client.get("/data.csv")
    client.close()
    assert h.verdict.grounding is OG.GROUNDED
    assert [r.identifier for r in h.verdict.reads] == [
        "https://example.org/data.csv"
    ]


def test_httpx_async_base_url_relative_get_grounds_the_absolute_url(httpx_mock):
    import asyncio

    import httpx

    httpx_mock.add_response(url="https://example.org/data.csv", text="col\n1\n")

    async def go():
        client = httpx.AsyncClient(base_url="https://example.org")
        with obs.observe(cites="https://example.org/data.csv") as h:
            await client.get("/data.csv")
        await client.aclose()
        return h.verdict

    verdict = asyncio.run(go())
    assert verdict.grounding is OG.GROUNDED
    assert [r.identifier for r in verdict.reads] == [
        "https://example.org/data.csv"
    ]


def test_http_read_never_binds_a_file_citation(tmp_path):
    # A network read must not live in the local-file namespace: fetching a path
    # that looks like the cited file is not a read of that file, so it cannot
    # ground the finding.
    import httpx

    p = str(tmp_path / "trial.csv")
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="col\n1\n")
    )
    client = httpx.Client(transport=transport, base_url="https://example.org")
    with obs.observe(cites=p) as h:
        client.get(p)
    client.close()
    assert h.verdict.grounding is not OG.GROUNDED
    assert all(r.identifier.startswith("https://") for r in h.verdict.reads)


def test_mock_transport_does_not_ground_the_cited_url():
    # The gate the lineage axis applies, on the data axis: an in-process
    # transport answers 200 offline, so the cited URL was never fetched. The
    # citation must floor to OPAQUE with the gap named, not read as GROUNDED.
    import httpx

    url = "https://example.org/trial-data.csv"
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text="arm,outcome\nA,1\n")
        )
    )
    with obs.observe(cites=url) as h:
        client.get(url)
    client.close()
    assert h.verdict.grounding is OG.OPAQUE
    assert h.verdict.reads == ()
    assert any(
        s.kind == "coverage-gap" and "transport" in s.detail
        for s in h.verdict.seams
    )


def test_mock_transport_post_does_not_ground_the_cited_url():
    # The same response the lineage axis reads as PROXY. The two axes must not
    # disagree about one offline response: a model-provider URL the producer
    # answered itself grounds nothing.
    import httpx

    url = "https://api.openai.com/v1/chat/completions"
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"ok": True})
        )
    )
    with obs.observe(cites=url) as h:
        client.post(url, json={"model": "gpt-4o-2024-08-06", "messages": []})
    client.close()
    assert h.verdict.grounding is OG.OPAQUE
    assert h.verdict.reads == ()


def test_unresolvable_http_read_is_a_coverage_gap_not_a_file_read():
    # No response URL and no client base_url: the observer cannot say what was
    # fetched, so it records nothing and names the gap. The cited file then
    # floors to OPAQUE rather than being bound by a network read.
    from mareforma.observe import _loaders

    class Response:
        status_code = 200
        content = b"col\n1\n"

    def fake_get(self, *a, **k):
        return Response()

    wrapper = _loaders._make_http_method_wrapper(fake_get, streaming_kw=None)
    with obs.observe(cites="/no/such/file.csv") as h:
        wrapper(object(), "/no/such/file.csv")
    assert h.verdict.grounding is OG.OPAQUE
    assert h.verdict.reads == ()
    assert any(s.kind == "coverage-gap" for s in h.verdict.seams)


def test_aiohttp_request_records_a_socket_seam():
    # aiohttp streams the body (read in host code after our wrapper returns), so
    # the wrapper records a socket seam rather than a header-based read, a pooled
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


# -- read reach: io.open / pathlib -------------------------------------------

def test_pathlib_read_is_grounded(tmp_path):
    # pathlib.Path.open / read_text / read_bytes call io.open, a separate
    # reference from builtins.open. Wrapping io.open reaches them: a cited
    # pathlib read is GROUNDED, not OPAQUE-coverage-gap.
    from pathlib import Path

    p = Path(tmp_path / "d.csv")
    p.write_text("col\n1\n2\n")
    with obs.observe(cites=str(p)) as h:
        p.read_text()
    assert h.verdict.grounding is OG.GROUNDED

    with obs.observe(cites=str(p)) as h:
        with p.open() as f:
            f.read()
    assert h.verdict.grounding is OG.GROUNDED


def test_builtins_open_recorded_once_not_doubled(tmp_path):
    # Wrapping both builtins.open and io.open must not double-record: each name
    # calls its own captured original, so one open is one read.
    p = str(tmp_path / "d.csv")
    open(p, "w").write("col\n1\n")
    with obs.observe(cites=p) as h:
        open(p).read()
    assert h.verdict.grounding is OG.GROUNDED
    assert h.verdict.reads_seen == 1


def test_post_open_stat_race_is_opaque_not_ungrounded(tmp_path, monkeypatch):
    # The open SUCCEEDS, then stat fails (a delete race). The bytes flowed
    # through the fd, so this must floor to OPAQUE (coverage-gap), never a
    # confident false UNGROUNDED off a stat that could not be read.
    from mareforma.observe import _loaders
    p = str(tmp_path / "d.csv")
    open(p, "w").write("x\n1\n2\n")
    real_stat = _loaders.os.stat

    def racing_stat(path, *a, **k):
        if str(path) == p:
            raise FileNotFoundError(p)  # unlinked between open and stat
        return real_stat(path, *a, **k)

    monkeypatch.setattr(_loaders.os, "stat", racing_stat)
    with obs.observe(cites=p) as h:
        open(p).read()
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "coverage-gap" for s in h.verdict.seams)


# -- status-gating: an error body is not a read ------------------------------

def test_http_error_body_is_not_grounded(httpx_mock):
    # A 404 that still returns a non-empty body must NOT ground a cited URL: the
    # bytes are an error page, not the cited data. Status-blind grounding would
    # read the error body as a real read; the cited URL floors to OPAQUE instead.
    import httpx

    url = "https://example.org/data.csv"
    httpx_mock.add_response(url=url, status_code=404, text="not found\n")
    client = httpx.Client()
    with obs.observe(cites=url) as h:
        client.get(url)
    client.close()
    assert h.verdict.grounding is not OG.GROUNDED


def test_observer_error_does_not_default_to_grounded(httpx_mock):
    # A non-success response the observer records must fail closed, not assume a
    # read happened off the error body.
    import httpx

    url = "https://example.org/data.csv"
    httpx_mock.add_response(url=url, status_code=500, text="err\n")
    client = httpx.Client()
    with obs.observe(cites=url) as h:
        client.get(url)
    client.close()
    assert h.verdict.grounding is not OG.GROUNDED


# -- false-UNGROUNDED containment: engines that own their I/O -----------------

def test_polars_read_csv_is_grounded(tmp_path):
    # polars reads through its Rust core, no Python open, so a cited read was a
    # false UNGROUNDED. Wrapping the eager readers records it: GROUNDED.
    pl = pytest.importorskip("polars")
    p = str(tmp_path / "d.csv")
    open(p, "w").write("x\n1\n2\n3\n")
    with obs.observe(cites=p) as h:
        pl.read_csv(p)
    assert h.verdict.grounding is OG.GROUNDED


def test_polars_lazy_scan_collect_is_opaque_not_ungrounded(tmp_path):
    # A lazy scan defers the read to collect, which runs through the Rust core
    # with no observable path, so a cited lazy read must floor to OPAQUE (a
    # coverage-gap seam), never a confident false UNGROUNDED that brands a
    # genuinely data-grounded finding as a silent failure.
    pl = pytest.importorskip("polars")
    p = str(tmp_path / "d.csv")
    open(p, "w").write("x\n1\n2\n3\n")
    with obs.observe(cites=p) as h:
        pl.scan_csv(p).collect()
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "coverage-gap" for s in h.verdict.seams)


def test_duckdb_path_read_is_opaque_not_ungrounded(tmp_path):
    # duckdb reads a path named INSIDE the SQL string through its C++ core: the
    # observer cannot extract the path or see the read, so a cited duckdb read
    # floors to OPAQUE (a per-invocation coverage-gap seam), never a confident
    # false UNGROUNDED.
    duckdb = pytest.importorskip("duckdb")
    p = str(tmp_path / "d.csv")
    open(p, "w").write("x\n1\n2\n3\n")
    with obs.observe(cites=p) as h:
        duckdb.sql(f"SELECT * FROM '{p}'").fetchall()
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "coverage-gap" for s in h.verdict.seams)


def test_duckdb_connection_read_is_opaque_not_ungrounded(tmp_path):
    # The connection idiom, duckdb.connect().execute/.sql, is the canonical usage
    # and the only path to a persistent .duckdb file. Its reads route through the
    # same uninstrumented core as the module-level functions, so a cited read
    # through a connection must floor to OPAQUE (a coverage-gap seam), never a
    # confident false UNGROUNDED that brands a genuinely data-grounded finding as
    # a silent failure.
    duckdb = pytest.importorskip("duckdb")
    p = str(tmp_path / "d.csv")
    open(p, "w").write("x\n1\n2\n3\n")
    with obs.observe(cites=p) as h:
        duckdb.connect().execute(f"SELECT * FROM read_csv_auto('{p}')").fetchall()
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "coverage-gap" for s in h.verdict.seams)


def test_duckdb_persistent_db_connection_read_is_opaque(tmp_path):
    # A persistent .duckdb file is reachable only through a connection, never the
    # module-level functions. A cited read through it floors to OPAQUE, not a
    # false UNGROUNDED.
    duckdb = pytest.importorskip("duckdb")
    p = str(tmp_path / "d.csv")
    open(p, "w").write("x\n1\n2\n3\n")
    with obs.observe(cites=p) as h:
        conn = duckdb.connect(str(tmp_path / "store.duckdb"))
        conn.sql(f"SELECT * FROM '{p}'").fetchall()
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "coverage-gap" for s in h.verdict.seams)


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
    # builtins.open, they never see the bytes. The signed reason must say so, not
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


# -- failed opens of a cited source are named, not blamed on a reader --------

def test_failed_open_names_the_failure_not_a_reader(tmp_path):
    # A read-mode open of the cited source that raises leaves an audit `open`
    # event behind but no read. The seam must say the open failed and name the
    # exception type; blaming an uninstrumented reader misdescribes a failure
    # the observer saw in full. Verdict stays OPAQUE (fail-closed): a failed
    # wrapped open does not rule out a hidden successful one elsewhere.
    missing = str(tmp_path / "missing.csv")
    with obs.observe(cites=missing) as h:
        try:
            open(missing)
        except FileNotFoundError:
            pass
    assert h.verdict.grounding is OG.OPAQUE
    gaps = [s for s in h.verdict.seams if s.kind == "coverage-gap"]
    assert gaps, "a failed cited open must still record a coverage-gap seam"
    assert any("failed (FileNotFoundError)" in s.detail for s in gaps)
    assert all("uninstrumented" not in s.detail for s in gaps)
    # Exception TYPE only: the detail is signed and publishable, so it must
    # never carry the path the exception message would leak.
    assert all(missing not in s.detail for s in gaps)


def test_pandas_failed_read_names_the_failure(tmp_path):
    # The try/except-fallback shape real pipelines have: the data load fails,
    # the code falls back silently, the finding still prints. The narrative
    # must name the failed open, not an uninstrumented reader.
    pd = pytest.importorskip("pandas")
    missing = str(tmp_path / "gone.csv")
    with obs.observe(cites=missing) as h:
        try:
            pd.read_csv(missing)
        except (FileNotFoundError, OSError):
            _ = 0.83  # cached fallback
    assert h.verdict.grounding is OG.OPAQUE
    assert any(
        s.kind == "coverage-gap" and "failed (FileNotFoundError)" in s.detail
        for s in h.verdict.seams
    )


def test_os_open_keeps_the_uninstrumented_reader_message(tmp_path):
    # A successful os.open bypasses the wrapper: only the audit event fires.
    # That is exactly the hidden-reader case, so the generic message must stay.
    import os

    p = tmp_path / "trial.csv"
    p.write_text("arm,value\nA,1\n")
    with obs.observe(cites=str(p)) as h:
        fd = os.open(str(p), os.O_RDONLY)
        os.close(fd)
    assert h.verdict.grounding is OG.OPAQUE
    assert any(
        s.kind == "coverage-gap" and "uninstrumented reader" in s.detail
        for s in h.verdict.seams
    )


def test_failed_open_beside_hidden_open_stays_generic(tmp_path):
    # One wrapped open of the cited path fails, but a second open of the SAME
    # path happens through os.open (hidden from the wrapper). The failure
    # explains only one of the two opens, so the seam must NOT claim the open
    # failed: the hidden one could have read the data.
    import os

    p = tmp_path / "trial.csv"
    p.write_text("arm,value\nA,1\n")
    with obs.observe(cites=str(p)) as h:
        fd = os.open(str(p), os.O_RDONLY)  # hidden successful open
        os.close(fd)
        try:
            open(str(p), encoding="no-such-codec")  # wrapped open, fails
        except LookupError:
            pass
    assert h.verdict.grounding is OG.OPAQUE
    assert any(
        s.kind == "coverage-gap" and "uninstrumented reader" in s.detail
        for s in h.verdict.seams
    )
    assert all("failed (" not in s.detail for s in h.verdict.seams)
