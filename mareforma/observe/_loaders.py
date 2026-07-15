"""Loader wrapping: proxy data-ingress calls to record what flowed.

The rule: wrap stdlib loaders (``builtins.open``, ``io.open``,
``sqlite3.connect``) unconditionally, and wrap third-party loaders ONLY if the
host already imported them. mareforma never imports a third-party loader to wrap
it, so there are no new core deps and coverage is documented as "wraps X if you
use X."

Reads. ``builtins.open`` and ``io.open`` cover the stdlib read paths, so
``pathlib.Path.open``/``read_text``/``read_bytes`` and ``zipfile``, which route
through ``io.open``, are seen. ``sqlite3`` observes the rows it returns. The
wrapped third-party readers are ``pandas`` and the eager ``polars`` readers (a
returned frame's row count is the non-empty signal), and the C-runtime scientific
readers ``h5py.File``, ``pyarrow.parquet``/``feather.read_table``, and
``netCDF4.Dataset`` (a stat-size proxy). A ``duckdb`` query reads its path from
inside the SQL, beyond the observer's view, so it records a per-invocation
coverage-gap seam rather than a denied read.

HTTP and model lineage. ``requests`` (module ``get`` + ``Session`` verbs) and
``httpx`` (module ``get`` + ``Client``/``AsyncClient`` ``get``) record a read;
``aiohttp`` records a network seam because its body streams. A model call is an
HTTP POST whose body names the model: the ``httpx`` ``Client``/``AsyncClient``
``post`` and ``send`` wrappers and the ``aiohttp`` request wrapper parse that body
for the model lineage, the paths the provider SDKs and litellm take. A grounded
read and a computed lineage are gated on a 2xx response, so an error body never
grounds a cited URL and a failed call never mints a model. A call to a local
inference server is content-addressed by the served weights' digest, resolved from
the running server through a scope-detached probe.

A loader imported INSIDE an open scope is caught too, by the late-import hook.

What the wrappers do not cover (``os.open``, ``mmap``, an unwrapped C-extension
reader) is covered honestly by the audit hook's open/seam detection and the
classifier's coverage-gap floors, not silently missed: a cited file with no
observed read floors to ``OPAQUE`` ("bytes not observable via PEP-578"), never a
false ``UNGROUNDED``.

Every wrapper obeys two invariants:

- Transparent outside a scope. With no active scope the wrapper delegates straight
  to the real callable, at no observation cost and with no behavior change.
- Fail-safe. The real call runs OUTSIDE the observation try-block, so a host-side
  failure (a missing file, a bad query) propagates to the host exactly as it would
  unwrapped. Only the observer's OWN logic is in the try, and any failure there
  marks the scope opaque and is swallowed; nothing the observer does re-raises into
  the host.

All ingress is recorded through the single :func:`mareforma.observe._scope.record_read`
chokepoint, so the recording rule lives in exactly one place.

Coverage bound: a loader must be OPENED inside the scope to be observed. A file
handle or database connection created before the scope (a module-level or pooled
connection) and reused inside it is not swapped for an observing wrapper, so its
reads are invisible and the finding reads as UNGROUNDED. Open the cited source
inside the scope for it to count.
"""
from __future__ import annotations

import builtins
import os
import sqlite3
import sys
import threading
import time
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
    {"pandas", "httpx", "requests", "aiohttp", "h5py", "pyarrow", "netCDF4",
     "polars", "duckdb"}
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
        _wrap_io_open()
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

def _make_observed_open(real_open):
    """An open() wrapper that records a cited read, bound to ``real_open``.

    Shared by the ``builtins.open`` and ``io.open`` wrappers. Each wrapper calls
    its OWN captured original, so a single open is recorded exactly once, the
    two names are distinct references (wrapping ``builtins.open`` never rebinds
    ``io.open``), and neither delegates to the other by name.
    """

    def observed_open(file, mode="r", *args, **kwargs):
        try:
            result = real_open(file, mode, *args, **kwargs)
        except BaseException as exc:
            # The host error propagates untouched, but the failure itself is
            # evidence: a read-mode open of a cited path that raised cannot
            # have delivered data, and classify() uses that to name the
            # failure instead of blaming an uninstrumented reader. Only the
            # exception TYPE is recorded (the message can carry a path).
            scope = _scope.current_scope()
            if scope is not None:
                try:
                    if _is_read_mode(mode):
                        identifier = _path_str(file)
                        if identifier:
                            scope.record_failed_open(
                                identifier, type(exc).__name__
                            )
                except BaseException as inner:  # noqa: BLE001
                    scope.mark_error(
                        f"open wrapper failed: {type(inner).__name__}"
                    )
            raise
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

    return observed_open


def _wrap_open() -> None:
    if "open" in _reals:
        return
    _reals["open"] = builtins.open
    builtins.open = _make_observed_open(builtins.open)


def _wrap_io_open() -> None:
    # pathlib.Path.open / read_text / read_bytes, zipfile, and a direct io.open
    # all call io.open, a SEPARATE reference from builtins.open. Wrapping
    # builtins.open alone misses the dominant modern read idiom (pathlib), which
    # then floors to OPAQUE-coverage-gap through the audit hook. Wrapping io.open
    # closes that reach gap at the chokepoint, on the same stat-proxy read tier.
    import io as _io

    if "io.open" in _reals:
        return
    real = getattr(_io, "open", None)
    if real is None:
        return
    _reals["io.open"] = real
    wrapped = _make_observed_open(real)
    _io.open = wrapped
    # On Python 3.10 and earlier, pathlib routes Path.open through a
    # _NormalAccessor whose ``open`` captured io.open by reference at import,
    # before this wrap runs, so pathlib reads would bypass the rebinding above
    # and floor to OPAQUE. Rebind that captured reference too, as a staticmethod
    # so the accessor instance is not bound in as the file argument. Python 3.11+
    # removed the accessor and calls io.open at the module attribute, so the
    # lookup is None there and this is a no-op.
    import pathlib as _pathlib

    accessor = getattr(_pathlib, "_NormalAccessor", None)
    if accessor is not None and getattr(accessor, "open", None) is real:
        _reals["pathlib._NormalAccessor.open"] = accessor.open
        accessor.open = staticmethod(wrapped)


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
            # degrades to OPAQUE rather than a confident UNGROUNDED, the read
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
# emits NO thread-start audit event before 3.12 (3.12 raises
# _thread.start_new_thread and 3.13+ _thread.start_joinable_thread). A
# thread-hidden read would then give a
# CONFIDENT FALSE UNGROUNDED, the exact failure OPAQUE exists to prevent. So
# the robust mechanism wraps the thread entry points directly, which works on
# every supported version; the audit-hook thread events stay as extra 3.12+
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
    # time, so patching _thread here does NOT double-count threading.Thread, it
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
    never reaches, neither wrapped nor seamed, so an unseen read read as a
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
    _wrap_polars_if_present()
    _wrap_duckdb_if_present()


# -- late-import hook: wrap a loader imported while a scope is open -----------

def _wrap_import_hook() -> None:
    """Re-run the third-party wrap when a wrappable module is imported in-scope.

    ``refresh_third_party()`` runs only at scope ENTRY, so a loader imported
    INSIDE an open scope (as diagnose and any observe() user does) would never
    be wrapped, its reads would go unseen and the finding read as a confident
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


def _pl_source(args, kwargs, result) -> str:
    # polars eager readers take the path as ``source`` (kwarg) or the first
    # positional argument.
    src = kwargs.get("source")
    if src is None and args:
        src = args[0]
    return _path_str(src) if src is not None else ""


def _wrap_polars_if_present() -> None:
    # polars reads through its Rust core with no Python open, so a cited read is
    # invisible to the open hook and would floor to a false UNGROUNDED. Wrapping
    # the EAGER readers records the read at return with result-nonemptiness. The
    # lazy scanners (``scan_*``) read nothing at call time; their read happens in
    # ``LazyFrame.collect``, wrapped below with a coverage-gap seam so a cited
    # lazy read floors to OPAQUE rather than a confident false UNGROUNDED.
    pl = sys.modules.get("polars")
    if pl is None:
        return
    for name in ("read_csv", "read_parquet", "read_ipc", "read_json",
                 "read_ndjson", "read_avro", "read_excel"):
        key = f"polars.{name}"
        if key in _reals:
            continue
        real = getattr(pl, name, None)
        if real is None:
            continue
        _reals[key] = real
        setattr(pl, name, _make_return_value_wrapper(real, "polars", _pl_source, _df_nonempty))
    # A lazy scan reads nothing at call time; the read happens inside
    # ``LazyFrame.collect`` through the Rust core, invisible to the open hook,
    # so a cited lazy read would floor to a confident false UNGROUNDED. Record a
    # coverage-gap seam at collect so a cited read through lazy polars floors to
    # OPAQUE instead. This never grounds a lazy read; a source genuinely read
    # through an instrumented eager path still wins GROUNDED (reads beat seams).
    lf_cls = getattr(pl, "LazyFrame", None)
    if lf_cls is None:
        return
    key = "polars.LazyFrame.collect"
    real_collect = getattr(lf_cls, "collect", None)
    if real_collect is not None and key not in _reals:
        _reals[key] = real_collect
        setattr(lf_cls, "collect", _make_lazy_polars_seam_wrapper(real_collect))


def _make_lazy_polars_seam_wrapper(real):
    """Wrap ``LazyFrame.collect`` to record a per-invocation coverage-gap seam.

    A lazy scan defers the read to collect, which runs through the polars Rust
    core with no Python open: the observer can neither see the read nor recover
    the scanned path, so a cited lazy read must not be a confident UNGROUNDED.
    The seam floors a cited source the collect might have read to OPAQUE, while a
    source genuinely read through an instrumented eager path still wins GROUNDED
    (reads beat seams). This never grounds a lazy read; it only refuses to deny
    one the observer could not see.
    """

    def wrapper(*args, **kwargs):
        result = real(*args, **kwargs)  # host errors propagate untouched
        scope = _scope.current_scope()
        if scope is None:
            return result
        try:
            scope.record_seam(
                "coverage-gap",
                "lazy polars collect read through an uninstrumented engine; the "
                "scanned path is not observable, so a cited read cannot be denied",
            )
        except BaseException as exc:  # noqa: BLE001
            scope.mark_error(f"polars lazy wrapper failed: {type(exc).__name__}")
        return result

    return wrapper


def _make_duckdb_seam_wrapper(real):
    """Wrap a duckdb entry point to record a per-invocation coverage-gap seam.

    duckdb reads a path named INSIDE the SQL string through its C++ core: the
    observer can neither see the read nor extract the path from the query, so a
    cited duckdb read must not be a confident UNGROUNDED. Recording a coverage-gap
    seam per query floors a cited source the query might have read to OPAQUE,
    while a source genuinely read through an instrumented path still wins GROUNDED
    (reads beat seams). This never grounds a duckdb read; it only refuses to deny
    one the observer could not see.
    """

    def wrapper(*args, **kwargs):
        result = real(*args, **kwargs)  # host errors propagate untouched
        scope = _scope.current_scope()
        if scope is None:
            return result
        try:
            scope.record_seam(
                "coverage-gap",
                "duckdb query read through an uninstrumented engine; the path in "
                "the SQL is not observable, so a cited read cannot be denied",
            )
        except BaseException as exc:  # noqa: BLE001
            scope.mark_error(f"duckdb wrapper failed: {type(exc).__name__}")
        return result

    return wrapper


_DUCKDB_QUERY_ENTRIES = ("sql", "execute", "query", "read_csv", "read_parquet", "read_json")


def _wrap_duckdb_if_present() -> None:
    duckdb = sys.modules.get("duckdb")
    if duckdb is None:
        return
    for name in _DUCKDB_QUERY_ENTRIES:
        key = f"duckdb.{name}"
        if key in _reals:
            continue
        real = getattr(duckdb, name, None)
        if real is None:
            continue
        _reals[key] = real
        setattr(duckdb, name, _make_duckdb_seam_wrapper(real))
    # The module-level functions run only on the default in-memory connection.
    # The canonical idiom, and the only one that reaches a persistent .duckdb
    # file, is duckdb.connect().execute/.sql/..., whose reads route through the
    # same uninstrumented core. Wrap the connection class's query methods so a
    # cited read through a connection floors to OPAQUE, never a false UNGROUNDED.
    # Instance attributes are read-only, so the wrap is on the class.
    conn_cls = getattr(duckdb, "DuckDBPyConnection", None)
    if conn_cls is None:
        return
    for name in _DUCKDB_QUERY_ENTRIES:
        key = f"duckdb.DuckDBPyConnection.{name}"
        if key in _reals:
            continue
        real = getattr(conn_cls, name, None)
        if real is None:
            continue
        _reals[key] = real
        setattr(conn_cls, name, _make_duckdb_seam_wrapper(real))


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
# connects inside it (keep-alive) and no socket seam fires, an unwrapped pooled
# retrieval reads as a confident false UNGROUNDED. Wrapping the session methods
# closes that. Streaming responses (stream=True, aiohttp, httpx.stream) do not
# have their body available at wrapper return, so they record a SOCKET seam
# (network delivery the observer did not see) rather than a header-based
# GROUNDED or a false UNGROUNDED, socket seams block URL/content-address
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
    # pattern) is still observed through its inherited verbs, otherwise its pooled
    # reads would land a confident false UNGROUNDED. A base-Session verb call
    # double-records the same URL (verb wrapper + inner request wrapper); that is
    # benign, matching is existential, the identifier is identical, and http reads
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
    # POST carries the model call: parse the request body for the model/method
    # lineage at the socket seam. A streaming POST (body ``stream=true``) does
    # not materialize the response, so its response is recorded as a socket seam
    # rather than forced into memory, the request body is available either way,
    # so the model is still captured.
    if Client is not None and "httpx.Client.post" not in _reals:
        real = getattr(Client, "post", None)
        if real is not None:
            _reals["httpx.Client.post"] = real
            Client.post = _make_http_post_method_wrapper(real)
    # send() is the SDK/litellm chokepoint: the openai/anthropic SDKs build a
    # Request and call send() directly, bypassing the json= .post wrapper. Wrap it
    # for lineage so the independence axis fires on real pipelines, not only on
    # hand-rolled .post callers.
    if Client is not None and "httpx.Client.send" not in _reals:
        real = getattr(Client, "send", None)
        if real is not None:
            _reals["httpx.Client.send"] = real
            Client.send = _make_http_send_wrapper(real)
    AsyncClient = getattr(httpx, "AsyncClient", None)
    if AsyncClient is not None and "httpx.AsyncClient.get" not in _reals:
        real = getattr(AsyncClient, "get", None)
        if real is not None:
            _reals["httpx.AsyncClient.get"] = real
            AsyncClient.get = _make_async_http_method_wrapper(real, streaming=False)
    if AsyncClient is not None and "httpx.AsyncClient.post" not in _reals:
        real = getattr(AsyncClient, "post", None)
        if real is not None:
            _reals["httpx.AsyncClient.post"] = real
            AsyncClient.post = _make_async_http_post_method_wrapper(real)
    if AsyncClient is not None and "httpx.AsyncClient.send" not in _reals:
        real = getattr(AsyncClient, "send", None)
        if real is not None:
            _reals["httpx.AsyncClient.send"] = real
            AsyncClient.send = _make_async_http_send_wrapper(real)


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
    # The wrapper also parses the request JSON body for model lineage (litellm's
    # default transport is aiohttp) without consuming the response stream.
    _reals["aiohttp.ClientSession._request"] = real
    Session._request = _make_aiohttp_request_wrapper(real)


def _make_http_method_wrapper(real, *, streaming_kw, method_arg=False):
    """Wrap a bound HTTP method that returns a response.

    ``streaming_kw`` names the keyword whose truthy value means the body is not
    materialized at return (``"stream"`` for requests); a streaming call records
    a socket seam instead of a read. ``method_arg`` is True for a shared
    ``request(self, method, url, ...)`` method (requests ``Session.request``):
    the leading HTTP verb is dropped before recording so the URL, not the string
    ``"GET"``, is read as the source.
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


# -- model/method lineage at the POST seam -----------------------------------
#
# A model call is an HTTP POST whose JSON body names the model. Wrapping
# Client.post / AsyncClient.post lets the observer parse that body at the socket
# seam, the COMPUTED lineage tier, which the producer does not control. The
# response side follows the existing streaming rule: a streaming POST (body
# ``stream=true``) records a socket seam instead of materializing the body.

def _make_http_post_method_wrapper(real):
    """Wrap a bound ``Client.post`` that returns a response."""

    def wrapper(self, *args, **kwargs):
        result = real(self, *args, **kwargs)  # host errors propagate untouched
        scope = _scope.current_scope()
        if scope is None:
            return result
        try:
            _record_http_post(scope, self, args, kwargs, result)
        except BaseException as exc:  # noqa: BLE001
            scope.mark_error(f"http loader wrapper failed: {type(exc).__name__}")
        return result

    return wrapper


def _make_async_http_post_method_wrapper(real):
    """Wrap a bound ``AsyncClient.post`` that returns a response."""

    async def wrapper(self, *args, **kwargs):
        result = await real(self, *args, **kwargs)  # host errors propagate
        scope = _scope.current_scope()
        if scope is None:
            return result
        try:
            _record_http_post(scope, self, args, kwargs, result)
        except BaseException as exc:  # noqa: BLE001
            scope.mark_error(f"http loader wrapper failed: {type(exc).__name__}")
        return result

    return wrapper


def _make_http_send_wrapper(real):
    """Wrap ``httpx.Client.send(request)``, the SDK/litellm model-call path.

    Captures model lineage only; grounding reads stay with the verb wrappers, so
    ``send`` never records an http read (which would risk grounding a finding off
    an API error body). A ``.post(json=)`` call routes through ``send`` too, so
    lineage may be recorded twice, benign: equal lineages collapse to one.
    """

    def wrapper(self, *args, **kwargs):
        result = real(self, *args, **kwargs)  # host errors propagate untouched
        scope = _scope.current_scope()
        if scope is None:
            return result
        try:
            request = kwargs.get("request") or (args[0] if args else None)
            if request is not None:
                _record_model_from_httpx_request(scope, self, request, result)
        except BaseException as exc:  # noqa: BLE001
            scope.mark_error(f"http loader wrapper failed: {type(exc).__name__}")
        return result

    return wrapper


def _make_async_http_send_wrapper(real):
    """Wrap ``httpx.AsyncClient.send(request)`` (async SDK model-call path)."""

    async def wrapper(self, *args, **kwargs):
        result = await real(self, *args, **kwargs)  # host errors propagate
        scope = _scope.current_scope()
        if scope is None:
            return result
        try:
            request = kwargs.get("request") or (args[0] if args else None)
            if request is not None:
                _record_model_from_httpx_request(scope, self, request, result)
        except BaseException as exc:  # noqa: BLE001
            scope.mark_error(f"http loader wrapper failed: {type(exc).__name__}")
        return result

    return wrapper


def _make_aiohttp_request_wrapper(real):
    """Wrap ``aiohttp.ClientSession._request``, litellm's default transport.

    aiohttp streams the response body (read in host code after we return), so the
    response is recorded as a socket seam, never a grounding read. But the request
    JSON body is available in kwargs without touching the stream, so a model call
    over aiohttp still yields COMPUTED lineage (2xx-gated like every seam).
    """

    async def wrapper(self, *args, **kwargs):
        result = await real(self, *args, **kwargs)  # host errors propagate
        scope = _scope.current_scope()
        if scope is None:
            return result
        try:
            scope.record_seam(
                "socket", "streaming HTTP response; byte flow not observed"
            )
            body = _request_json_body(kwargs)
            if isinstance(body, dict):
                _record_model_lineage(scope, body, _aiohttp_url(args, kwargs), result)
        except BaseException as exc:  # noqa: BLE001
            scope.mark_error(f"http loader wrapper failed: {type(exc).__name__}")
        return result

    return wrapper


def _aiohttp_url(args, kwargs) -> str:
    # aiohttp _request(self, method, url, ...): the URL is the 2nd positional arg
    # (args[0] is the HTTP verb), or the ``url`` kwarg.
    url = kwargs.get("url")
    if url is None and len(args) >= 2:
        url = args[1]
    return str(url or "")


def _record_http_post(scope, client, args, kwargs, result) -> None:
    # Parse the request body once and thread it to both consumers.
    body = _request_json_body(kwargs)
    _record_model_from_request(scope, client, args, kwargs, body, result)
    if isinstance(body, dict) and bool(body.get("stream")):
        # Streaming POST: the response body is not materialized at return.
        # Record a socket seam rather than forcing the download into memory,
        # matching the requests/aiohttp streaming seams.
        scope.record_seam(
            "socket", "streaming HTTP response; byte flow not observed"
        )
    else:
        _record_http_response(scope, args, kwargs, result)


def _record_model_from_request(scope, client, args, kwargs, body, result) -> None:
    if not isinstance(body, dict):
        return
    url = _resp_source(args, kwargs, None)
    _record_model_lineage(scope, body, url, result, client=client)


def _transport_is_networked(client, url) -> bool:
    """Whether an httpx client answers *url* through a real network transport.

    COMPUTED is gated on this. The provider HOST is genuine, but the 2xx
    response is produced by the client's transport, so a producer-supplied
    in-process transport (``httpx.MockTransport``, WSGI/ASGI, or a custom class)
    answers ``200`` offline and certifies no model call. Only httpx's own network
    transports earn COMPUTED, an ALLOWLIST, so an unknown transport fails safe to
    producer-controlled (PROXY) rather than being trusted. ``client is None`` (a
    seam with no httpx client, e.g. aiohttp) reads as networked: this gate governs
    the httpx transport path only.
    """
    if client is None:
        return True
    try:
        import httpx

        networked = (httpx.HTTPTransport, httpx.AsyncHTTPTransport)
        try:
            transport = client._transport_for_url(httpx.URL(url))
        except BaseException:  # noqa: BLE001 - URL/mount resolution quirk
            transport = getattr(client, "_transport", None)
        return isinstance(transport, networked)
    except BaseException:  # noqa: BLE001 - httpx absent or shape changed
        return True


def _record_model_lineage(scope, body, url, result, *, client=None) -> None:
    """Resolve and record lineage from a parsed model-call body + request URL.

    Shared tail for every model-call seam (``.post``, ``.send``, aiohttp). Gated
    on a 2xx response: a call that failed (or a response the observer cannot
    read) never executed, so minting COMPUTED off the request body would
    attribute a run that did not happen. Records no lineage on failure rather
    than a downgraded record, which would collapse a later successful retry of
    the same model to UNVERIFIABLE.

    ``client`` is the httpx client behind the seam, when there is one. A
    producer-controlled transport (a local ``MockTransport``, WSGI/ASGI, or a
    custom class) authors its own 2xx, so it certifies no real model call: the
    lineage is recorded as a producer DECLARATION (PROXY), never COMPUTED, so an
    offline transport cannot forge verified cross-model independence.
    """
    if not isinstance(body, dict) or _response_ok(result) is not True:
        return
    model_id = body.get("model")
    if not isinstance(model_id, str) or not model_id:
        return
    networked = _transport_is_networked(client, url)
    provider = _provider_of(url)
    # A local inference server (no recognized remote host) can be content-
    # addressed by the served weights' digest, a verifiable distinct identity a
    # local model name lacks. Probed only for a real local server, never a remote
    # host and never a producer-controlled transport.
    digest = (
        _probe_ollama_digest(url, model_id)
        if provider is None and networked
        else None
    )
    from ._lineage import resolve_lineage

    lineage = resolve_lineage(
        model_id,
        # A producer-controlled transport is a declaration, not a socket capture.
        source="socket" if networked else "declared",
        method=_method_of(url),
        decoding={
            "temperature": body.get("temperature"),
            "top_p": body.get("top_p"),
            "seed": body.get("seed"),
        },
        provider=provider,
        digest=digest,
    )
    scope.record_model(lineage)


def _record_model_from_httpx_request(scope, client, request, result) -> None:
    """Capture lineage from a pre-built ``httpx.Request`` (the SDK ``send`` path).

    The openai/anthropic SDKs and litellm build a ``Request`` and dispatch it via
    ``client.send(request)``, the model never passes through a wrapped ``.post``
    ``json=`` kwarg. The request body has already been serialized to bytes on the
    request object, so it is read WITHOUT calling ``request.read()``/``aread()``
    (which would consume a streaming upload and change host behavior). A
    non-buffered/streaming body yields no lineage rather than a guess.
    """
    try:
        raw = request.content  # already-built bytes; never triggers a stream read
    except BaseException:  # noqa: BLE001 - streaming/unread body: no lineage
        return
    if not isinstance(raw, (bytes, bytearray)) or not raw:
        return
    try:
        import json as _json

        body = _json.loads(raw)
    except (ValueError, TypeError):
        return
    url = str(getattr(request, "url", "") or "")
    _record_model_lineage(scope, body, url, result, client=client)


def _request_json_body(kwargs):
    """The request JSON body of an httpx POST, or None.

    ``json=`` is the common shape (a dict). ``content=`` / ``data=`` carry raw
    bytes/str a caller may have serialized itself; parse those best-effort. A
    non-JSON body yields None (no model to capture), never an error.
    """
    body = kwargs.get("json")
    if isinstance(body, dict):
        return body
    for key in ("content", "data"):
        raw = kwargs.get(key)
        if isinstance(raw, (bytes, bytearray, str)):
            try:
                import json as _json

                return _json.loads(raw)
            except (ValueError, TypeError):
                return None
    return None


# The recognized model providers, keyed by their registered domains. Genuine
# inference providers only: a router/aggregator (openrouter.ai) is deliberately
# absent, because its host does not pin which upstream actually served the
# weights, so a router call cannot certify a model identity.
_PROVIDER_DOMAINS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("anthropic", ("anthropic.com",)),
    ("openai", ("openai.com",)),
    ("groq", ("groq.com",)),
    ("together", ("together.xyz", "together.ai")),
    ("fireworks", ("fireworks.ai",)),
    ("mistral", ("mistral.ai",)),
    ("deepseek", ("deepseek.com",)),
)


def _provider_of(url) -> "str | None":
    """The recognized model provider for a request URL, matched on the HOST.

    COMPUTED lineage is gated on this, so the match must be on the parsed host,
    never a substring of the whole (producer-controlled) URL: a POST to
    ``https://evil.com/anthropic`` or ``api.anthropic.com.attacker.net`` must NOT
    read as a provider, or a producer could mint a COMPUTED distinct model by
    naming a provider anywhere in a URL they control. The suffix match accepts
    only the provider's own registered domain and genuine sub-domains of it,
    which an attacker cannot forge.
    """
    from urllib.parse import urlsplit

    try:
        host = (urlsplit(url or "").hostname or "").lower()
    except (ValueError, TypeError):
        return None
    for provider, domains in _PROVIDER_DOMAINS:
        for domain in domains:
            if host == domain or host.endswith("." + domain):
                return provider
    return None


_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0", ""})


def _ollama_server_base(url) -> "str | None":
    """The base URL of a LOCAL Ollama server for a request URL, or None.

    Recognized by a loopback host on Ollama's port (11434) or its ``/api/`` path
   , the fingerprint from the local-inference-server survey. Only a local server
    is ever probed; a remote unrecognized host is never contacted.
    """
    from urllib.parse import urlsplit

    try:
        parts = urlsplit(url or "")
    except (ValueError, TypeError):
        return None
    host = (parts.hostname or "").lower()
    if host not in _LOCAL_HOSTS:
        return None
    if parts.port == 11434 or (parts.path or "").startswith("/api/"):
        port = parts.port or 11434
        return f"{parts.scheme or 'http'}://{parts.hostname}:{port}"
    return None


# A model call loop hits the same (server, model) thousands of times and the
# digest never changes within a run, so the probe result is memoized behind a
# short TTL. The TTL (not an unbounded cache) keeps the concurrent-tag-remap
# residual named in the probe docstring bounded rather than pinned for the whole
# run. Negative results are cached too, so a miss is not re-probed on every call.
_OLLAMA_DIGEST_TTL = 60.0  # seconds
_ollama_digest_cache: "dict[tuple[str, str], tuple[float, str | None]]" = {}


def _probe_ollama_digest(url, model_id) -> "str | None":
    """Best-effort weights digest for a local Ollama model, scope-detached.

    Memoized per ``(server base, model)`` behind ``_OLLAMA_DIGEST_TTL`` so a long
    agent loop resolves the digest once and reuses it instead of firing a probe
    per inference call. The first miss runs the network body below; later calls
    within the TTL, hit or miss, reuse the stored answer.
    """
    base = _ollama_server_base(url)
    if not base:
        return None
    key = (base, model_id)
    now = time.monotonic()
    hit = _ollama_digest_cache.get(key)
    if hit is not None and now - hit[0] < _OLLAMA_DIGEST_TTL:
        return hit[1]
    digest = _query_ollama_digest(base, model_id)
    _ollama_digest_cache[key] = (now, digest)
    return digest


def _content_addressed_digest(dig, names) -> "str | None":
    """The normalized ``sha256:<64hex>`` digest iff it can be a content address.

    The probe's trigger (a loopback ``/api/`` server) is satisfied by more than
    Ollama: other local servers ship Ollama-compatible surfaces whose ``digest``
    is not a hash of the served weights. SGLang answers with a hardcoded non-hex
    sentinel, and LocalAI answers with the sha256 of the producer-chosen model
    NAME. Accepting either would mint a COMPUTED weights-digest lineage off a
    fabricated identity, and the fake digest would score as a DISTINCT model,
    forging cross-model independence. So the digest must be a well-formed sha256
    payload (kills the sentinel) and must not equal the sha256 of any of the
    model's own name strings (kills the name-hash). Anything else fails closed
    to None, the call stays UNVERIFIABLE. Residual: a server that fabricates a
    plausible random digest is the operator-Sybil boundary already named on the
    ``weights-digest`` attestor, not a new one.
    """
    import hashlib
    import re as _re

    if not isinstance(dig, str) or not dig:
        return None
    mo = _re.match(r"^(?:sha256:)?([0-9a-f]{64})$", dig)
    if mo is None:
        return None
    payload = mo.group(1)
    for name in names:
        if isinstance(name, str) and name:
            if payload == hashlib.sha256(name.encode("utf-8")).hexdigest():
                return None
    return "sha256:" + payload


def _query_ollama_digest(base, model_id) -> "str | None":
    """Query a local Ollama server for a model's weights digest, scope-detached.

    Queries the running server (``/api/ps`` loaded runner, then ``/api/tags``
    installed) for the served model's manifest sha256, the digest of the weights
    that served the call. DETACHES the observer scope so this probe records
    nothing into the scope under measurement (its own socket/read events no-op),
    uses a hard short timeout, and returns None on any failure, never raises,
    never blocks the inference path. A concurrent tag remap between the call and
    this probe is a named residual; the digest is self-attested regardless.

    The answer is gated through :func:`_content_addressed_digest`: a digest an
    Ollama-compatible surface fabricated (a constant sentinel, a name hash) is
    rejected rather than minted into a COMPUTED identity.

    The probe REFUSES to follow redirects and caps the response body: a server at
    the loopback base could otherwise 302 the probe to a non-local host (the
    ``_LOCAL_HOSTS`` allowlist gates only the initial target) or slow-drip an
    unbounded body under the per-socket timeout. Both stay on the loopback host
    with a bounded read, honouring "a remote host is never contacted."
    """
    token = _scope._active.set(None)  # detach: loader wrappers + audit hook no-op
    try:
        import json as _json
        import urllib.error
        import urllib.request

        # An opener with NO redirect handler: a 3xx is returned as-is, never
        # followed off the loopback host, so the allowlisted base is the only
        # host contacted. HTTP(S) handlers only.
        opener = urllib.request.OpenerDirector()
        opener.add_handler(urllib.request.HTTPHandler())
        opener.add_handler(urllib.request.HTTPSHandler())
        _MAX = 2 * 1024 * 1024  # cap the body so a hostile server cannot exhaust memory
        for path in ("/api/ps", "/api/tags"):
            try:
                req = urllib.request.Request(base + path, method="GET")
                with opener.open(req, timeout=1.5) as resp:
                    data = _json.loads(resp.read(_MAX))
            except BaseException:  # noqa: BLE001 - best effort; any failure -> None
                continue
            for m in data.get("models", []) or []:
                if model_id in (m.get("name"), m.get("model")):
                    dig = _content_addressed_digest(
                        m.get("digest"), (model_id, m.get("name"), m.get("model"))
                    )
                    if dig is not None:
                        return dig
        return None
    finally:
        _scope._active.reset(token)


def _method_of(url) -> "str | None":
    """The request path, a stable tool/pipeline identity tag for the call."""
    from urllib.parse import urlsplit

    try:
        path = urlsplit(url or "").path
    except (ValueError, TypeError):
        return None
    return path or None


# -- C-extension readers: h5py / pyarrow / netCDF4 ---------------------------
#
# HDF5 / netCDF / Arrow open through the C runtime's own open(2)/fopen, emitting
# NO Python PEP-578 event, so a cited read of one is invisible to the open hook
# and the classifier floors it to OPAQUE. Wrapping the reader records the read
# directly so a cited scientific-data read reaches GROUNDED. Only-if-imported,
# like the other third-party wrappers, mareforma never imports these itself.

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

    The same honest proxy the builtins.open path uses, the C reader's byte flow
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
    instrumentation, replacing the class with a plain function makes those raise
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
        # wrapper, the read is still recorded, only isinstance is not preserved.
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
    ``nonempty=False`` and allowed to drive a false ``UNGROUNDED``, the caller
    records a coverage-gap seam for a cited non-regular source instead, forcing
    ``OPAQUE``. An unstattable target (the open succeeded but stat then raised, a
    delete race) is reported as NON-regular for the same reason: its bytes are
    unobservable, so a cited read floors to ``OPAQUE``, never a false
    ``UNGROUNDED``.
    """
    try:
        import stat as _stat

        st = os.stat(identifier)
        if _stat.S_ISREG(st.st_mode):
            return True, st.st_size > 0
        return False, False
    except OSError:
        # The open already returned a handle; stat failing now (a delete race, a
        # permission change) means we cannot observe the bytes, so report it as
        # non-regular. The caller records a coverage-gap seam for a cited source
        # and floors to OPAQUE, never a confident false UNGROUNDED empty read.
        return False, False


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


def _response_ok(result):
    """Whether an HTTP response is a success, library-agnostic.

    ``httpx`` and ``requests`` expose ``status_code``; ``aiohttp`` exposes
    ``status``. A shape the observer cannot read returns ``None``, callers
    treat that as fail-closed (neither ground the read nor mint lineage) rather
    than assume the call succeeded. Shared by the read and lineage paths so a
    single rule governs both.

    Only 2xx counts as success. httpx does not follow redirects by default, so a
    3xx is a short "moved" stub the observer received in place of the cited bytes
    or the model answer: grounding a read or minting lineage off it would attest
    delivery that never happened.
    """
    code = getattr(result, "status_code", None)
    if code is None:
        code = getattr(result, "status", None)
    if isinstance(code, int):
        return 200 <= code < 300
    return None


def _resp_nonempty(result) -> bool:
    # A non-success response carries an error body, not the cited data; and a
    # response the observer cannot introspect is not evidence of a read. Both
    # fail closed to "not a read" so a cited URL floors to OPAQUE, never GROUNDED
    # off an error page.
    try:
        if _response_ok(result) is not True:
            return False
        content = getattr(result, "content", None)
        if content is not None:
            return len(content) > 0
        text = getattr(result, "text", None)
        return bool(text)
    except BaseException:  # noqa: BLE001
        return False


def _maybe_content_address(result):
    try:
        content = getattr(result, "content", None)
        if isinstance(content, (bytes, bytearray)):
            from ..trust._store import content_address_data_id

            return content_address_data_id(bytes(content))
    except BaseException:  # noqa: BLE001
        return None
    return None
