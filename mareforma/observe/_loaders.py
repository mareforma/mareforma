"""Loader wrapping: proxy data-ingress calls to record what flowed.

The rule: wrap stdlib loaders (``builtins.open``,
``sqlite3.connect``) unconditionally, and wrap third-party loaders ONLY if the
host already imported them. mareforma never imports a third-party loader to wrap
it — no new core deps, coverage documented as "wraps X if you use X." The wrapped
third-party loaders are ``pandas``; the keep-alive HTTP clients ``requests``
(module ``get`` + ``Session.get``/``request``), ``httpx`` (module ``get`` +
``Client``/``AsyncClient`` ``get``), and ``aiohttp`` (``ClientSession``, recorded
as a network seam because its body streams); and the C-runtime scientific readers
``h5py.File``, ``pyarrow.parquet``/``feather.read_table``, and ``netCDF4.Dataset``.
A loader imported INSIDE an open scope is caught too, by the late-import hook.

What the wrappers do not cover (``io.open``, ``os.open``,
``pathlib.Path.open``, ``mmap``, an unwrapped C-extension reader) is covered
honestly by the audit hook's open/seam detection and the classifier's
coverage-gap floors, not silently missed: a cited C-runtime file with no observed
read floors to ``OPAQUE`` ("bytes not observable via PEP-578"), never a false
``UNGROUNDED``.

Every wrapper obeys two invariants:

- Transparent outside a scope. With no active scope the wrapper delegates
  straight to the real callable — no observation cost, no behavior change.
- Fail-safe. The real call runs OUTSIDE the observation try-block,
  so a host-side failure (a missing file, a bad query) propagates to the host
  exactly as it would unwrapped. Only the observer's OWN logic is wrapped in
  the try, and any failure there marks the scope opaque and is swallowed —
  nothing the observer does re-raises into the host.

All ingress is recorded through the single :func:`mareforma.observe._scope.record_read`
chokepoint, so the recording rule lives in exactly one place.

Coverage bound: a loader must be OPENED inside the scope to be observed. A file
handle or database connection created before the scope (a module-level or pooled
connection) and reused inside it is not swapped for an observing wrapper, so its
reads are invisible and the finding reads as UNGROUNDED. This is the same class
of documented limit as a value loaded once and reused: flow observation sees the
reads that happen through loaders it wrapped at open time, not resources opened
earlier. Open the cited source inside the scope for it to count.
"""
from __future__ import annotations

import builtins
import os
import sqlite3
import sys
import threading
import _thread
from typing import Any

from . import _scope
from ._verdict import GroundingVerdict  # noqa: F401  (re-exported convenience)

# Read modes for open(): a mode without 'w'/'x'/'a' and without '+' truncation
# intent is an ingress. We record any mode that can read ('r', 'rb', 'r+', ...).
_READ_MODE_HINT = ("r", "+")

_installed = False
_reals: dict[str, Any] = {}
_install_lock = threading.Lock()


# Third-party top-level modules the observer can wrap once the host imports them.
# The late-import hook re-runs the third-party wrap when one of these is imported
# while a scope is open, so a loader imported INSIDE the observed span (the
# flagship diagnose command imports its loaders inside the scope) is still caught.
_WRAPPABLE_TOP_LEVEL: frozenset[str] = frozenset(
    {"pandas", "httpx", "requests", "aiohttp", "h5py", "pyarrow", "netCDF4"}
)


def ensure_installed() -> None:
    """Install every applicable wrapper once, idempotently.

    Called on first ``observe()``. stdlib wrappers always install; third-party
    wrappers install only for modules already present in ``sys.modules``. The
    late-import hook makes a loader imported after this point still get wrapped.
    """
    global _installed
    if _installed:
        return
    # Lock + double-check: two threads racing the first observe() could each read
    # the real builtins.open before the other published its wrapper, permanently
    # double-wrapping open and corrupting _reals. Serialize the one-time install.
    with _install_lock:
        if _installed:
            return
        _wrap_open()
        _wrap_sqlite()
        _wrap_thread_seams()
        _wrap_executor_seams()
        _wrap_import_hook()
        _wrap_third_party_if_present()
        _installed = True


def refresh_third_party() -> None:
    """Re-scan for third-party loaders imported since install.

    A pipeline may import pandas/httpx after the first ``observe()``. Called on
    each scope entry so a loader imported late still gets wrapped, without ever
    forcing an import.
    """
    if not _installed:
        return
    # Same lock as the one-time install: two threads entering observe()
    # concurrently after a late import could otherwise both pass the
    # ``key in _reals`` guard and one capture the other's wrapper as the real,
    # double-wrapping the loader permanently. Serialize the re-scan too.
    with _install_lock:
        _wrap_third_party_if_present()


# -- stdlib: always ----------------------------------------------------------

def _wrap_open() -> None:
    if "open" in _reals:
        return
    real_open = builtins.open
    _reals["open"] = real_open

    def observed_open(file, mode="r", *args, **kwargs):
        result = real_open(file, mode, *args, **kwargs)  # host errors propagate
        scope = _scope.current_scope()
        if scope is None:
            return result
        try:
            if _is_read_mode(mode):
                identifier = _path_str(file)
                if identifier:
                    regular, nonempty = _file_read_signal(identifier)
                    if not regular and _reads_cited(scope, identifier):
                        # A fifo / device / stream / procfs cited source: its
                        # bytes are not observable by stat, so we cannot honestly
                        # call it empty. Record a coverage gap so absence becomes
                        # OPAQUE, never a false UNGROUNDED.
                        scope.record_seam(
                            "coverage-gap",
                            "cited source is a non-regular file (fifo/device/"
                            "stream); its bytes are not observable by stat",
                        )
                    _scope.record_read("file", identifier, nonempty)
        except BaseException as exc:  # noqa: BLE001
            # Only the exception TYPE, never its message: the message can carry
            # an absolute filesystem path (FileNotFoundError, PermissionError),
            # and this reason is signed into the claim and can be published.
            scope.mark_error(f"open wrapper failed: {type(exc).__name__}")
        return result

    builtins.open = observed_open


def _wrap_sqlite() -> None:
    if "sqlite3.connect" in _reals:
        return
    real_connect = sqlite3.connect
    _reals["sqlite3.connect"] = real_connect

    def observed_connect(database, *args, **kwargs):
        # Transparent when nothing is observing: no factory swap, no behavior
        # change (wrappers no-op outside an active scope).
        if _scope.current_scope() is None:
            return real_connect(database, *args, **kwargs)
        # ``factory`` is the 6th positional parameter of sqlite3.connect
        # (database, timeout, detect_types, isolation_level, check_same_thread,
        # factory, ...). If the caller pinned their own factory either
        # positionally or by keyword, delegate transparently rather than inject
        # ours (injecting a kwarg atop a positional factory would raise a
        # TypeError into host code). Only swap when the slot is genuinely free.
        _factory_passed = "factory" in kwargs or len(args) >= 5
        if not _factory_passed:
            kwargs["factory"] = _ObservedConnection
        conn = real_connect(database, *args, **kwargs)  # host errors propagate
        if _factory_passed:
            # The caller pinned their own connection factory, so our observing
            # cursor is not installed and any read through this connection is
            # invisible. Record a coverage gap so absence of a cited read
            # degrades to OPAQUE rather than a confident UNGROUNDED — the read
            # may have happened where we could not see it.
            scope = _scope.current_scope()
            if scope is not None:
                scope.record_seam(
                    "coverage-gap",
                    "sqlite opened with a caller-pinned factory; "
                    "its cursors are not observed",
                )
            return conn
        try:
            target = _path_str(database) or str(database)
            if isinstance(conn, _ObservedConnection):
                conn._mf_target = target
        except BaseException:  # noqa: BLE001
            scope = _scope.current_scope()
            if scope is not None:
                scope.mark_error("sqlite connect wrapper failed")
        return conn

    sqlite3.connect = observed_connect


class _ObservedCursor(sqlite3.Cursor):
    """A Cursor subclass that records whether a fetch returned rows.

    Subclassing (not proxying) keeps ``isinstance(cur, sqlite3.Cursor)`` true,
    so no duck-typing downstream breaks. Each fetch records against
    the connection's target after the real fetch runs.
    """

    def _mf_record(self, nonempty: bool) -> None:
        scope = _scope.current_scope()
        if scope is None:
            return
        try:
            target = getattr(self.connection, "_mf_target", None) or "sqlite:memory"
            _scope.record_read("sqlite", target, nonempty)
        except BaseException as exc:  # noqa: BLE001
            scope.mark_error(f"sqlite cursor wrapper failed: {type(exc).__name__}")

    def fetchone(self):
        row = super().fetchone()
        self._mf_record(row is not None)
        return row

    def fetchall(self):
        rows = super().fetchall()
        self._mf_record(bool(rows))
        return rows

    def fetchmany(self, *args, **kwargs):
        rows = super().fetchmany(*args, **kwargs)
        self._mf_record(bool(rows))
        return rows

    # `for row in cur` (and `for row in conn.execute(...)`) is the idiomatic
    # sqlite read. It drives the C-level iterator, which does NOT dispatch to the
    # fetch* overrides above, so without these a cited sqlite read consumed by
    # iteration would go unrecorded and the finding would read as a CONFIDENT
    # FALSE UNGROUNDED. Record once, on the first row the iteration yields.
    def execute(self, *args, **kwargs):
        self._mf_iter_recorded = False
        return super().execute(*args, **kwargs)

    def __iter__(self):
        return self

    def __next__(self):
        row = super().__next__()  # raises StopIteration at exhaustion
        if not getattr(self, "_mf_iter_recorded", False):
            self._mf_iter_recorded = True
            self._mf_record(True)
        return row


class _ObservedConnection(sqlite3.Connection):
    """Connection whose cursors observe fetches. ``_mf_target`` is the db path."""

    _mf_target = "sqlite:memory"

    def cursor(self, factory=_ObservedCursor):
        return super().cursor(factory)

    def execute(self, *args, **kwargs):
        cur = self.cursor()
        return cur.execute(*args, **kwargs)


# -- thread seams: wrap the start entry points (cross-version) ----------------
#
# The PEP-578 audit hook cannot carry thread-seam detection on its own: CPython
# emits NO thread-start audit event before 3.13 (only 3.13+ raises
# _thread.start_joinable_thread). A thread-hidden read would then give a
# CONFIDENT FALSE UNGROUNDED — the exact failure OPAQUE exists to prevent. So
# the robust mechanism wraps the thread entry points directly, which works on
# every supported version; the audit-hook thread events stay as extra 3.13+
# coverage for threads spawned via C paths.

def _wrap_thread_seams() -> None:
    if "threading.Thread.start" in _reals:
        return
    real_start = threading.Thread.start
    _reals["threading.Thread.start"] = real_start

    def observed_start(self, *args, **kwargs):
        _mark_thread_seam("threading.Thread.start")
        return real_start(self, *args, **kwargs)  # host behavior unchanged

    threading.Thread.start = observed_start

    # threading captured its own reference to _thread.start_new_thread at import
    # time, so patching _thread here does NOT double-count threading.Thread — it
    # only catches callers using the raw low-level API.
    real_snt = _thread.start_new_thread
    _reals["_thread.start_new_thread"] = real_snt

    def observed_snt(function, args, kwargs=None):
        _mark_thread_seam("_thread.start_new_thread")
        if kwargs is None:
            return real_snt(function, args)
        return real_snt(function, args, kwargs)

    _thread.start_new_thread = observed_snt


def _wrap_executor_seams() -> None:
    """Seam a thread-pool submit/map issued inside a scope.

    ``_wrap_thread_seams`` only catches a thread *started* inside the scope. A
    reused ``ThreadPoolExecutor`` whose worker threads were spawned before the
    scope opened would run a read on a pre-existing thread the scope's contextvar
    never reaches — neither wrapped nor seamed, so an unseen read read as a
    confident UNGROUNDED. Wrapping ``submit``/``map`` records the seam at the
    hand-off point instead, turning that blind spot into OPAQUE. Class-level and
    a no-op outside a scope, exactly like the Thread.start wrapper.
    """
    if "ThreadPoolExecutor.submit" in _reals:
        return
    from concurrent.futures import ThreadPoolExecutor

    real_submit = ThreadPoolExecutor.submit
    _reals["ThreadPoolExecutor.submit"] = real_submit

    def observed_submit(self, *args, **kwargs):
        _mark_thread_seam("concurrent.futures.ThreadPoolExecutor.submit")
        return real_submit(self, *args, **kwargs)  # host behavior unchanged

    ThreadPoolExecutor.submit = observed_submit

    real_map = ThreadPoolExecutor.map
    _reals["ThreadPoolExecutor.map"] = real_map

    def observed_map(self, *args, **kwargs):
        _mark_thread_seam("concurrent.futures.ThreadPoolExecutor.map")
        return real_map(self, *args, **kwargs)

    ThreadPoolExecutor.map = observed_map


def _mark_thread_seam(detail: str) -> None:
    scope = _scope.current_scope()
    if scope is None:
        return
    try:
        scope.record_seam("thread", detail)
    except BaseException as exc:  # noqa: BLE001
        scope.mark_error(f"thread seam wrapper failed: {type(exc).__name__}")


# -- third-party: only if already imported -----------------------------------

def _wrap_third_party_if_present() -> None:
    _wrap_pandas_if_present()
    _wrap_httpx_if_present()
    _wrap_requests_if_present()
    _wrap_requests_session_if_present()
    _wrap_httpx_clients_if_present()
    _wrap_aiohttp_if_present()
    _wrap_h5py_if_present()
    _wrap_pyarrow_if_present()
    _wrap_netcdf4_if_present()


# -- late-import hook: wrap a loader imported while a scope is open -----------

def _wrap_import_hook() -> None:
    """Re-run the third-party wrap when a wrappable module is imported in-scope.

    ``refresh_third_party()`` runs only at scope ENTRY, so a loader imported
    INSIDE an open scope (as diagnose and any observe() user does) would never
    be wrapped — its reads would go unseen and the finding read as a confident
    false UNGROUNDED. Wrapping ``builtins.__import__`` closes that: when a scope
    is open and one of the wrappable modules finishes importing, the third-party
    wrap re-runs. Fail-safe (any error is swallowed) and cheap when idle (a
    single contextvar read gates the whole body), so a process that never
    observes pays nothing.
    """
    if "builtins.__import__" in _reals:
        return
    real_import = builtins.__import__
    _reals["builtins.__import__"] = real_import

    def observed_import(name, *args, **kwargs):
        module = real_import(name, *args, **kwargs)  # host import unchanged
        if _scope.current_scope() is not None and name:
            top = name.split(".", 1)[0]
            if top in _WRAPPABLE_TOP_LEVEL:
                try:
                    with _install_lock:
                        _wrap_third_party_if_present()
                except BaseException:  # noqa: BLE001
                    scope = _scope.current_scope()
                    if scope is not None:
                        scope.mark_error("late-import wrap failed")
        return module

    builtins.__import__ = observed_import


def _wrap_pandas_if_present() -> None:
    pd = sys.modules.get("pandas")
    if pd is None:
        return
    for name in ("read_csv", "read_parquet", "read_table", "read_json", "read_excel"):
        key = f"pandas.{name}"
        if key in _reals:
            continue
        real = getattr(pd, name, None)
        if real is None:
            continue
        _reals[key] = real
        setattr(pd, name, _make_return_value_wrapper(real, "pandas", _df_source, _df_nonempty))


def _wrap_httpx_if_present() -> None:
    httpx = sys.modules.get("httpx")
    if httpx is None:
        return
    real_get = getattr(httpx, "get", None)
    if real_get is not None and "httpx.get" not in _reals:
        _reals["httpx.get"] = real_get
        httpx.get = _make_return_value_wrapper(
            real_get, "http", _resp_source, _resp_nonempty
        )


def _wrap_requests_if_present() -> None:
    requests = sys.modules.get("requests")
    if requests is None:
        return
    real_get = getattr(requests, "get", None)
    if real_get is not None and "requests.get" not in _reals:
        _reals["requests.get"] = real_get
        # Streaming-aware: requests.get(url, stream=True) does not materialize the
        # body at return, so the return-value wrapper's _resp_nonempty would force
        # the whole download into memory (defeating the caller's streaming intent).
        # Record a socket seam instead, matching the Session path.
        requests.get = _make_http_func_wrapper(real_get, streaming_kw="stream")


# -- keep-alive HTTP: sessions and async clients -----------------------------
#
# The module-level httpx.get / requests.get are wrapped above. But real agents
# reuse a pooled Session / Client opened BEFORE the scope, so no new socket
# connects inside it (keep-alive) and no socket seam fires — an unwrapped pooled
# retrieval reads as a confident false UNGROUNDED. Wrapping the session methods
# closes that. Streaming responses (stream=True, aiohttp, httpx.stream) do not
# have their body available at wrapper return, so they record a SOCKET seam
# (network delivery the observer did not see) rather than a header-based
# GROUNDED or a false UNGROUNDED — socket seams block URL/content-address
# citations but leave a file-cited finding's tell intact.

def _wrap_requests_session_if_present() -> None:
    requests = sys.modules.get("requests")
    if requests is None:
        return
    Session = getattr(requests, "Session", None)
    if Session is None:
        return
    # Wrap the shared request(method, url, ...) AND the verb methods. Wrapping
    # request (method-aware, so the URL is recorded, not the HTTP verb) captures
    # direct request() calls and every verb of a plain Session. The verbs are ALSO
    # wrapped so a Session SUBCLASS that overrides request (a common auth/retry SDK
    # pattern) is still observed through its inherited verbs — otherwise its pooled
    # reads would land a confident false UNGROUNDED. A base-Session verb call
    # double-records the same URL (verb wrapper + inner request wrapper); that is
    # benign — matching is existential, the identifier is identical, and http reads
    # are not counted in the coverage fraction.
    if "requests.Session.request" not in _reals:
        real = getattr(Session, "request", None)
        if real is not None:
            _reals["requests.Session.request"] = real
            Session.request = _make_http_method_wrapper(
                real, streaming_kw="stream", method_arg=True
            )
    for name in ("get", "post", "put", "patch", "delete"):
        key = f"requests.Session.{name}"
        if key in _reals:
            continue
        real = getattr(Session, name, None)
        if real is None:
            continue
        _reals[key] = real
        setattr(
            Session, name, _make_http_method_wrapper(real, streaming_kw="stream")
        )


def _wrap_httpx_clients_if_present() -> None:
    httpx = sys.modules.get("httpx")
    if httpx is None:
        return
    Client = getattr(httpx, "Client", None)
    if Client is not None and "httpx.Client.get" not in _reals:
        real = getattr(Client, "get", None)
        if real is not None:
            _reals["httpx.Client.get"] = real
            Client.get = _make_http_method_wrapper(real, streaming_kw=None)
    AsyncClient = getattr(httpx, "AsyncClient", None)
    if AsyncClient is not None and "httpx.AsyncClient.get" not in _reals:
        real = getattr(AsyncClient, "get", None)
        if real is not None:
            _reals["httpx.AsyncClient.get"] = real
            AsyncClient.get = _make_async_http_method_wrapper(real, streaming=False)


def _wrap_aiohttp_if_present() -> None:
    aiohttp = sys.modules.get("aiohttp")
    if aiohttp is None:
        return
    Session = getattr(aiohttp, "ClientSession", None)
    if Session is None or "aiohttp.ClientSession._request" in _reals:
        return
    real = getattr(Session, "_request", None)
    if real is None:
        return
    # aiohttp streams the body (await resp.read() happens in host code, after our
    # wrapper returns), so we never see the bytes: record a socket seam. All
    # ClientSession.get/post/... route through _request, so one wrap covers them.
    _reals["aiohttp.ClientSession._request"] = real
    Session._request = _make_async_http_method_wrapper(real, streaming=True)


def _make_http_method_wrapper(real, *, streaming_kw, method_arg=False):
    """Wrap a bound HTTP method that returns a response.

    ``streaming_kw`` names the keyword whose truthy value means the body is not
    materialized at return (``"stream"`` for requests); a streaming call records
    a socket seam instead of a read. ``method_arg`` is True for a shared
    ``request(self, method, url, ...)`` method (requests ``Session.request``):
    the leading HTTP verb is dropped before recording so the URL — not the string
    ``"GET"`` — is read as the source.
    """

    def wrapper(self, *args, **kwargs):
        result = real(self, *args, **kwargs)  # host errors propagate untouched
        scope = _scope.current_scope()
        if scope is None:
            return result
        try:
            if streaming_kw and kwargs.get(streaming_kw):
                scope.record_seam(
                    "socket", "streaming HTTP response; byte flow not observed"
                )
            else:
                rec_args = args[1:] if method_arg else args
                _record_http_response(scope, rec_args, kwargs, result)
        except BaseException as exc:  # noqa: BLE001
            scope.mark_error(f"http loader wrapper failed: {type(exc).__name__}")
        return result

    return wrapper


def _make_http_func_wrapper(real, *, streaming_kw):
    """Wrap a module-level HTTP function (``requests.get(url, ...)``) that returns
    a response. Unlike the bound-method wrapper there is no ``self``. A streaming
    call (``stream=True``) records a socket seam instead of materializing the
    body, so the observer stays behavior-neutral and does not force a large
    download into memory.
    """

    def wrapper(*args, **kwargs):
        result = real(*args, **kwargs)  # host errors propagate untouched
        scope = _scope.current_scope()
        if scope is None:
            return result
        try:
            if streaming_kw and kwargs.get(streaming_kw):
                scope.record_seam(
                    "socket", "streaming HTTP response; byte flow not observed"
                )
            else:
                _record_http_response(scope, args, kwargs, result)
        except BaseException as exc:  # noqa: BLE001
            scope.mark_error(f"http loader wrapper failed: {type(exc).__name__}")
        return result

    return wrapper


def _make_async_http_method_wrapper(real, *, streaming):
    """Wrap an async HTTP method. ``streaming`` True records a socket seam (the
    body is never materialized in the observer frame); False records the read.
    """

    async def wrapper(self, *args, **kwargs):
        result = await real(self, *args, **kwargs)  # host errors propagate
        scope = _scope.current_scope()
        if scope is None:
            return result
        try:
            if streaming:
                scope.record_seam(
                    "socket", "streaming HTTP response; byte flow not observed"
                )
            else:
                _record_http_response(scope, args, kwargs, result)
        except BaseException as exc:  # noqa: BLE001
            scope.mark_error(f"http loader wrapper failed: {type(exc).__name__}")
        return result

    return wrapper


def _record_http_response(scope, args, kwargs, result) -> None:
    url = _resp_source(args, kwargs, result)
    if not url:
        return
    content_address = None
    if scope.content_address:
        content_address = _maybe_content_address(result)
    _scope.record_read("http", url, _resp_nonempty(result), content_address)


# -- C-extension readers: h5py / pyarrow / netCDF4 ---------------------------
#
# HDF5 / netCDF / Arrow open through the C runtime's own open(2)/fopen, emitting
# NO Python PEP-578 event, so a cited read of one is invisible to the open hook
# and the classifier floors it to OPAQUE. Wrapping the reader records the read
# directly so a cited scientific-data read reaches GROUNDED. Only-if-imported,
# like the other third-party wrappers — mareforma never imports these itself.

def _wrap_h5py_if_present() -> None:
    h5py = sys.modules.get("h5py")
    if h5py is None or "h5py.File" in _reals:
        return
    real = getattr(h5py, "File", None)
    if real is None:
        return
    _reals["h5py.File"] = real
    h5py.File = _make_c_ext_class_wrapper(real)


def _wrap_pyarrow_if_present() -> None:
    pq = sys.modules.get("pyarrow.parquet")
    if pq is not None and "pyarrow.parquet.read_table" not in _reals:
        real = getattr(pq, "read_table", None)
        if real is not None:
            _reals["pyarrow.parquet.read_table"] = real
            pq.read_table = _make_c_ext_wrapper(real)
    feather = sys.modules.get("pyarrow.feather")
    if feather is not None and "pyarrow.feather.read_table" not in _reals:
        real = getattr(feather, "read_table", None)
        if real is not None:
            _reals["pyarrow.feather.read_table"] = real
            feather.read_table = _make_c_ext_wrapper(real)


def _wrap_netcdf4_if_present() -> None:
    netCDF4 = sys.modules.get("netCDF4")
    if netCDF4 is None or "netCDF4.Dataset" in _reals:
        return
    real = getattr(netCDF4, "Dataset", None)
    if real is None:
        return
    _reals["netCDF4.Dataset"] = real
    netCDF4.Dataset = _make_c_ext_class_wrapper(real)


def _record_c_ext_read(args, kwargs) -> None:
    """Record a C-extension read of the path (stat-based non-emptiness proxy).

    The same honest proxy the builtins.open path uses — the C reader's byte flow
    is not observable either. No-op outside a scope; never raises into host code.
    """
    scope = _scope.current_scope()
    if scope is None:
        return
    try:
        path = _c_ext_source(args, kwargs)
        if path:
            _, nonempty = _file_read_signal(path)
            _scope.record_read("c-extension", path, nonempty)
    except BaseException as exc:  # noqa: BLE001
        scope.mark_error(f"c-extension loader wrapper failed: {type(exc).__name__}")


def _make_c_ext_wrapper(real):
    """Wrap a C-extension reader FUNCTION (pyarrow read_table / feather) that takes
    a file path first and returns the data. Kind ``c-extension`` so the receipt
    names the reader. For a class reader (h5py.File / netCDF4.Dataset) use
    :func:`_make_c_ext_class_wrapper`, which preserves ``isinstance``.
    """

    def wrapper(*args, **kwargs):
        result = real(*args, **kwargs)  # host errors propagate untouched
        _record_c_ext_read(args, kwargs)
        return result

    return wrapper


def _make_c_ext_class_wrapper(real_cls):
    """Wrap a C-extension reader CLASS (h5py.File / netCDF4.Dataset) as a subclass,
    so ``isinstance(f, h5py.File)`` and ``class X(h5py.File)`` keep working after
    instrumentation — replacing the class with a plain function makes those raise
    ``TypeError``. Mirrors the sqlite path, which subclasses Cursor/Connection for
    the same reason. If the type is not subclassable, fall back to the function
    wrapper (isinstance is then not preserved, but the read is still recorded).

    NOTE: this path only runs when h5py / netCDF4 are installed (the test-heavy
    extra), so it is not exercised by the base CI leg.
    """
    try:

        class _WrappedCExtReader(real_cls):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)  # host errors propagate untouched
                _record_c_ext_read(args, kwargs)

        _WrappedCExtReader.__name__ = getattr(real_cls, "__name__", "CExtReader")
        _WrappedCExtReader.__qualname__ = getattr(
            real_cls, "__qualname__", _WrappedCExtReader.__name__
        )
        return _WrappedCExtReader
    except TypeError:
        # Not subclassable (some C-extension types): degrade to the function
        # wrapper — the read is still recorded, only isinstance is not preserved.
        return _make_c_ext_wrapper(real_cls)


def _c_ext_source(args, kwargs) -> str:
    src = None
    for key in ("name", "filename", "source", "path", "filepath"):
        if key in kwargs:
            src = kwargs[key]
            break
    if src is None and args:
        src = args[0]
    return _path_str(src) if src is not None else ""


def _make_return_value_wrapper(real, kind, source_of, nonempty_of):
    """Wrap a loader that RETURNS the data object (DataFrame, Response).

    The real call runs first (host errors propagate). Then, inside a scope, the
    return value is inspected for its source identifier and non-emptiness and
    recorded. content_address is computed only when the scope opted in and the
    return exposes bytes cheaply.
    """

    def wrapper(*args, **kwargs):
        result = real(*args, **kwargs)  # host errors propagate untouched
        scope = _scope.current_scope()
        if scope is None:
            return result
        try:
            identifier = source_of(args, kwargs, result)
            nonempty = nonempty_of(result)
            content_address = None
            if scope.content_address:
                content_address = _maybe_content_address(result)
            if identifier:
                _scope.record_read(kind, identifier, nonempty, content_address)
        except BaseException as exc:  # noqa: BLE001
            scope.mark_error(f"{kind} loader wrapper failed: {type(exc).__name__}")
        return result

    return wrapper


# -- helpers -----------------------------------------------------------------

def mode_reads_existing(mode) -> bool:
    """Whether an open mode reads an EXISTING file (an ingress of cited data).

    ``'r'`` in the mode is exactly the read set: ``r``, ``rb``, ``r+``, ``rb+``.
    ``w`` / ``x`` / ``a`` modes create, truncate, or append and never carry an
    existing external file's bytes, even with ``+`` (``w+`` / ``a+`` read back
    self-written data, not the cited source), so they are not an ingress. Shared
    with the audit hook so the loader and the coverage hook classify modes the
    same way.
    """
    return isinstance(mode, str) and "r" in mode


def _is_read_mode(mode) -> bool:
    return mode_reads_existing(mode)


def _path_str(file) -> str:
    """Best-effort string identifier for an open()/connect() target."""
    if isinstance(file, (str, bytes)):
        return file.decode("utf-8", "replace") if isinstance(file, bytes) else file
    if isinstance(file, os.PathLike):
        return os.fspath(file)
    return ""


def _file_read_signal(identifier: str) -> tuple[bool, bool]:
    """``(is_regular_file, nonempty)`` for an open target.

    ``nonempty`` is a cheap, non-consuming proxy for "the read returned data,"
    trusted ONLY for regular files, whose stat size is meaningful. A non-regular
    target (fifo, character/block device, socket file, procfs entry) reports a
    stat size of 0 even when the read delivers bytes, so it must never be called
    ``nonempty=False`` and allowed to drive a false ``UNGROUNDED`` — the caller
    records a coverage-gap seam for a cited non-regular source instead, forcing
    ``OPAQUE``. A missing / unstattable target is treated as a regular empty
    read (opening a truly absent file would already have raised before here).
    """
    try:
        import stat as _stat

        st = os.stat(identifier)
        if _stat.S_ISREG(st.st_mode):
            return True, st.st_size > 0
        return False, False
    except OSError:
        return True, False


def _reads_cited(scope, identifier: str) -> bool:
    """Whether an opened path matches one of the scope's cited sources."""
    try:
        from ._citation import read_matches_citation

        return read_matches_citation(identifier, None, scope.cited)
    except BaseException:  # noqa: BLE001
        return False


def _df_source(args, kwargs, result) -> str:
    src = kwargs.get("filepath_or_buffer")
    if src is None and args:
        src = args[0]
    return _path_str(src) if src is not None else ""


def _df_nonempty(result) -> bool:
    try:
        return len(result) > 0
    except (TypeError, ValueError):
        return result is not None


def _resp_source(args, kwargs, result) -> str:
    url = kwargs.get("url")
    if url is None and args:
        url = args[0]
    if url is not None:
        return _path_str(url) or str(url)
    return str(getattr(result, "url", "")) or ""


def _resp_nonempty(result) -> bool:
    try:
        content = getattr(result, "content", None)
        if content is not None:
            return len(content) > 0
        text = getattr(result, "text", None)
        return bool(text)
    except BaseException:  # noqa: BLE001
        return True


def _maybe_content_address(result):
    try:
        content = getattr(result, "content", None)
        if isinstance(content, (bytes, bytearray)):
            from ..trust._store import content_address_data_id

            return content_address_data_id(bytes(content))
    except BaseException:  # noqa: BLE001
        return None
    return None
