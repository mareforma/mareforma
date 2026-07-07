"""Seam detection: threads, subprocesses, sockets, and coverage gaps → OPAQUE.

These pin the honesty guarantee: when a read could have happened somewhere the
observer cannot see, the verdict is OPAQUE, never a confident UNGROUNDED. The
cross-version thread-seam test is the load-bearing one — the audit hook alone
does not emit a thread event before CPython 3.12, so the direct wrapper has to
carry it.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import _thread
import time

import pytest

import mareforma.observe as obs
from mareforma.observe import ObservedGrounding as OG
from mareforma.observe import _audit


@pytest.fixture
def cited(tmp_path):
    p = tmp_path / "data.csv"
    p.write_text("x\n1\n")
    return str(p)


def test_thread_hidden_read_is_opaque(cited):
    # The read happens in a library-spawned thread the scope's contextvar does
    # not reach. Detection must catch the thread START and mark OPAQUE.
    with obs.observe(cites=cited) as h:
        t = threading.Thread(target=lambda: open(cited).read())
        t.start()
        t.join()
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "thread" for s in h.verdict.seams)


def test_raw_thread_start_is_a_seam(cited):
    done = threading.Event()
    with obs.observe(cites=cited) as h:
        _thread.start_new_thread(
            lambda: (open(cited).read(), done.set()), ()
        )
        # Bounded wait: a thread body that raised before signaling fails the
        # test here instead of hanging the suite on an unbounded busy-wait.
        assert done.wait(timeout=2), "spawned thread did not finish in time"
    assert h.verdict.grounding is OG.OPAQUE


def test_thread_seam_detection_does_not_depend_on_a_thread_audit_event():
    # The guarantee under test: thread-seam detection is carried by the direct
    # wrapper of threading.Thread.start (_loaders._wrap_thread_seams), NOT by a
    # thread audit event. CPython emits no thread-start audit event before 3.12;
    # 3.12 raises _thread.start_new_thread and 3.13+ _thread.start_joinable_thread.
    # The wrapper is the cross-version carrier, so detection must hold regardless
    # of the audit event.
    from mareforma.observe import _loaders

    with obs.observe(cites="/no/such/cited/source") as h:
        threading.Thread(target=lambda: None).start()
    # The wrapper is installed and is what recorded the seam.
    assert "threading.Thread.start" in _loaders._reals
    assert any(s.kind == "thread" for s in h.verdict.seams)
    assert h.verdict.grounding is OG.OPAQUE

    # Version-fact guard: no thread-start audit event fires before 3.12, so on
    # those versions the wrapper is the sole carrier. 3.12 emits
    # _thread.start_new_thread and 3.13+ _thread.start_joinable_thread as extra
    # coverage on top of the wrapper, so this only pins the pre-3.12 reality.
    if sys.version_info < (3, 12):
        seen = []
        sys.addaudithook(lambda event, args: seen.append(event))
        threading.Thread(target=lambda: None).start()
        time.sleep(0.02)
        assert [e for e in seen if e in _audit.THREAD_SEAM_EVENTS] == []


def test_reused_thread_pool_submit_is_a_seam(cited):
    # A ThreadPoolExecutor whose worker thread was spawned BEFORE the scope runs
    # the read on a pre-existing thread the scope contextvar never reaches. The
    # thread was not started inside the scope, so Thread.start seam detection
    # misses it; wrapping submit records the seam at hand-off, so an unseen read
    # is OPAQUE, not a confident UNGROUNDED.
    from concurrent.futures import ThreadPoolExecutor

    ex = ThreadPoolExecutor(max_workers=1)
    ex.submit(lambda: None).result()  # spawn the worker before the scope opens
    try:
        with obs.observe(cites=cited) as h:
            ex.submit(lambda: open(cited).read()).result()
    finally:
        ex.shutdown()
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "thread" for s in h.verdict.seams)


def test_sqlite_pinned_factory_is_a_coverage_gap(tmp_path):
    # A sqlite connection opened INSIDE the scope with a caller-pinned factory is
    # not swapped for the observing connection, so its cursor reads are invisible.
    # Absence of a cited read must degrade to OPAQUE, never a confident absence.
    import sqlite3

    db = tmp_path / "cited.db"
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE t(x)")
    c.execute("INSERT INTO t VALUES (1)")
    c.commit()
    c.close()

    class MyConn(sqlite3.Connection):
        pass

    with obs.observe(cites=str(db)) as h:
        conn = sqlite3.connect(db, factory=MyConn)
        rows = conn.execute("SELECT * FROM t").fetchall()
        conn.close()
    assert rows == [(1,)]
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "coverage-gap" for s in h.verdict.seams)


def test_subprocess_is_a_seam(cited):
    with obs.observe(cites=cited) as h:
        subprocess.run(["true"], capture_output=True)
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "subprocess" for s in h.verdict.seams)


def test_socket_connect_is_a_seam():
    # A socket seam blocks UNGROUNDED for a URL citation: the bytes could have
    # arrived over that connection. (For a local-FILE citation the same seam is
    # irrelevant and the verdict stays UNGROUNDED — see the seam-relevance tests
    # in test_observe_scope.py.)
    with obs.observe(cites="https://example.org/data.csv") as h:
        try:
            socket.create_connection(("127.0.0.1", 1), timeout=0.05)
        except OSError:
            pass
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "socket" for s in h.verdict.seams)


def test_cited_read_wins_over_a_seam(cited):
    # A seam only forces OPAQUE when no cited read was observed. A confirmed
    # cited non-empty read grounds the finding regardless of an unrelated seam.
    with obs.observe(cites=cited) as h:
        open(cited).read()
        try:
            socket.create_connection(("127.0.0.1", 1), timeout=0.05)
        except OSError:
            pass
    assert h.verdict.grounding is OG.GROUNDED


def test_uninstrumented_open_of_cited_path_is_a_coverage_gap(cited):
    # os.open bypasses the builtins.open wrapper; the audit hook still detects
    # the open of the cited path, so absence of a wrapped read becomes OPAQUE
    # (a coverage gap), not a false UNGROUNDED.
    with obs.observe(cites=cited) as h:
        fd = os.open(cited, os.O_RDONLY)
        os.read(fd, 16)
        os.close(fd)
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "coverage-gap" for s in h.verdict.seams)
    assert h.verdict.read_coverage_fraction() == 0.0


def test_genuine_absence_is_ungrounded_not_opaque(cited):
    # No cited read, no seam, nothing opened: the scope was fully observed and
    # the data genuinely did not arrive. This is the only path to UNGROUNDED.
    with obs.observe(cites=cited) as h:
        _ = 2 + 2
    assert h.verdict.grounding is OG.UNGROUNDED
    assert h.verdict.seams == ()
