"""Loader wrapping: proxy data-ingress calls to record what flowed.

The rule: wrap stdlib loaders (``builtins.open``,
``sqlite3.connect``) unconditionally, and wrap third-party loaders
(``pandas``, ``httpx``, ``requests``) ONLY if the host already imported them.
mareforma never imports a third-party loader to wrap it — no new core deps,
coverage documented as "wraps X if you use X." What the wrappers do not cover
(``io.open``, ``os.open``, ``pathlib.Path.open``, ``mmap``, C-extension I/O) is
covered honestly by the audit hook's open/seam detection, not silently missed.

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


def ensure_installed() -> None:
    """Install every applicable wrapper once, idempotently.

    Called on first ``observe()``. stdlib wrappers always install; third-party
    wrappers install only for modules already present in ``sys.modules``.
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
        requests.get = _make_return_value_wrapper(
            real_get, "http", _resp_source, _resp_nonempty
        )


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
