"""PEP-578 audit hook: seam detection and open-coverage capture.

The wrapped loaders (:mod:`mareforma.observe._loaders`) see the reads they can
proxy and whether they returned data. This audit hook covers what the loaders
cannot: the spawn seams that hide reads from the observer, and the opens that
happen through uninstrumented readers (``os.open``, C-extension I/O). It is the
honesty gate — it is what lets ``UNGROUNDED`` mean genuine absence instead of
"absence I could not see."

The hook is a permanent, process-global cost once installed: ``sys.addaudithook``
cannot be undone (PEP 578). It is installed lazily on the first ``observe()`` so
a process that never observes never pays it, and it no-ops as cheaply as
possible when no scope is active. This global surface is a documented opt-in.

An audit hook that raises propagates the exception into the audited call site —
that would break the host pipeline. So this hook NEVER raises: any internal
failure is swallowed and marks the active scope opaque (fail-safe), and nothing
crosses back into host code.
"""
from __future__ import annotations

import sys
import threading

# Thread-start audit events, version-dependent. CPython emits no thread-start
# audit event before 3.12; 3.12 emits `_thread.start_new_thread` and 3.13+
# `_thread.start_joinable_thread` (measured across 3.10-3.14). So thread-seam
# detection cannot rest on the audit hook alone — the wrapper in _loaders.py
# (`_wrap_thread_seams`) is the primary, cross-version mechanism, and these
# event names are extra coverage from 3.12 on for threads started via C paths
# that bypass the Python-level wrapper. A cross-version test pins this set.
# Missing a thread seam is a CONFIDENT FALSE UNGROUNDED — a read hidden in a
# library thread would read as genuine absence — which is why the robust
# wrapper carries the guarantee.
THREAD_SEAM_EVENTS: frozenset[str] = frozenset(
    {
        "_thread.start_joinable_thread",  # CPython 3.13+
        "_thread.start_new_thread",       # CPython 3.12
    }
)

# Subprocess / new-process seams: the child runs in a separate interpreter the
# observer cannot instrument, so a read on the far side is invisible.
SUBPROCESS_SEAM_EVENTS: frozenset[str] = frozenset(
    {
        "subprocess.Popen",
        "os.exec",
        "os.posix_spawn",
        "os.spawn",
        "os.fork",
        "os.forkpty",
    }
)

# Uninstrumented network ingress. A raw socket connection could carry the cited
# data on a path no wrapped loader sees; when no cited read matched, that is a
# blind spot, not an absence.
SOCKET_SEAM_EVENTS: frozenset[str] = frozenset({"socket.connect"})

# The generic open event. Fires for builtins.open, io.open, and os.open, so it
# catches opens the builtins.open wrapper misses (the coverage caveat). It does
# NOT reveal whether the read returned data — only that the path was opened — so
# it drives COVERAGE, not the GROUNDED verdict.
_OPEN_EVENT = "open"

_installed = False
_install_lock = threading.Lock()


def ensure_installed() -> None:
    """Install the audit hook once, idempotently. Called on first ``observe()``."""
    global _installed
    if _installed:
        return
    # Lock + double-check: two threads racing the first observe() must not both
    # add the hook (sys.addaudithook cannot be undone, so a double-add is a
    # permanent duplicate cost).
    with _install_lock:
        if _installed:
            return
        sys.addaudithook(_audit_hook)
        _installed = True


def _audit_hook(event: str, args: tuple) -> None:
    # Cheapest possible no-op when nothing is observing: one contextvar read.
    # Imported lazily inside the hook so importing this module does not pull the
    # whole scope machinery, and to keep the hot path a single attribute lookup.
    from ._scope import current_scope

    scope = current_scope()
    if scope is None:
        return
    try:
        if event in THREAD_SEAM_EVENTS:
            scope.record_seam("thread", event)
        elif event in SUBPROCESS_SEAM_EVENTS:
            scope.record_seam("subprocess", event)
        elif event in SOCKET_SEAM_EVENTS:
            scope.record_seam("socket", _socket_detail(args))
        elif event == _OPEN_EVENT:
            scope.record_open(_open_read_target(args))
    except BaseException as exc:  # noqa: BLE001
        # A hook must never propagate: that would raise inside the host's own
        # call. Degrade the scope to opaque (we can no longer trust our own
        # observation) and swallow. Only within our own frame — the host's
        # audited call continues untouched.
        try:
            scope.mark_error(f"audit hook failed on {event!r}: {type(exc).__name__}")
        except BaseException:
            pass


def _open_read_target(args: tuple):
    """Path of an ``open`` audit event WHEN it opens for reading, else None.

    The open event fires for reads and writes alike (path, mode, flags). Only a
    read-open of the cited path is a coverage signal: a write-open is egress and
    must not be mistaken for an uninstrumented read. builtins.open passes a
    string mode; os.open passes mode=None and an int flags. Never raises.
    """
    try:
        raw = args[0] if args else None
        path = _coerce_path(raw)
        if path is None:
            return None
        mode = args[1] if len(args) > 1 else None
        flags = args[2] if len(args) > 2 else None
        if isinstance(mode, str):
            # Classify the mode the same way the loader does, so a write/create
            # open ('w'/'x'/'a', including 'w+'/'a+') is never counted as an
            # uninstrumented read of the cited source.
            from ._loaders import mode_reads_existing

            return path if mode_reads_existing(mode) else None
        if isinstance(flags, int):
            import os as _os

            if (flags & _os.O_ACCMODE) == _os.O_WRONLY:
                return None
            return path
        # Unknown shape: count it, so a coverage gap is never silently dropped.
        return path
    except BaseException:  # noqa: BLE001
        return None


def _coerce_path(raw):
    if isinstance(raw, str):
        return raw
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    try:
        import os as _os

        if isinstance(raw, _os.PathLike):
            return _os.fspath(raw)
    except BaseException:  # noqa: BLE001
        return None
    return None


def _socket_detail(args: tuple) -> str:
    """A short, non-sensitive descriptor of a socket.connect target."""
    try:
        addr = args[1] if len(args) > 1 else None
        if isinstance(addr, tuple) and addr:
            return f"connect:{addr[0]}"
        return "connect"
    except BaseException:  # noqa: BLE001
        return "connect"
