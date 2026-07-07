"""Observer scope, wrapped loaders, read-to-citation binding, and fail-safe.

Covers the observe() context lifecycle, the stdlib and conditional third-party
loader wrapping, the citation match that separates cited reads from incidental
ones, and the fail-safe that degrades to OPAQUE without ever touching the host.
"""
from __future__ import annotations

import os
import sqlite3

import pytest

import mareforma.observe as obs
from mareforma.observe import ObservedGrounding as OG
from mareforma.observe import _scope


@pytest.fixture
def data_files(tmp_path):
    data = tmp_path / "trial.csv"
    data.write_text("arm,value\nA,1\nB,2\n")
    cfg = tmp_path / "config.yaml"
    cfg.write_text("threshold: 0.5\n")
    empty = tmp_path / "empty.csv"
    empty.write_text("")
    return {"data": str(data), "cfg": str(cfg), "empty": str(empty)}


# -- context lifecycle -------------------------------------------------------

def test_enter_exit_leaves_no_active_scope(data_files):
    assert _scope.current_scope() is None
    with obs.observe(cites=data_files["data"]) as h:
        assert _scope.current_scope() is not None
    assert _scope.current_scope() is None
    assert h.verdict is not None


def test_nested_scopes_restore_the_outer(data_files):
    with obs.observe(cites="/a") as outer:  # noqa: F841
        assert _scope.current_scope() is not None
        with obs.observe(cites="/b"):
            pass
        # The outer scope must be active again after the inner one closes.
        assert _scope.current_scope() is not None
    assert _scope.current_scope() is None


def test_exception_inside_scope_does_not_leak_the_scope(data_files):
    with pytest.raises(ValueError):
        with obs.observe(cites=data_files["data"]) as h:
            raise ValueError("boom")
    assert _scope.current_scope() is None
    # The verdict is still computed from what was seen before the exception.
    assert h.verdict.grounding is OG.UNGROUNDED


def test_verdict_unavailable_until_scope_closes(data_files):
    with obs.observe(cites=data_files["data"]) as h:
        with pytest.raises(obs.ScopeNotClosedError):
            _ = h.verdict


# -- wrapped loaders: files --------------------------------------------------

def test_file_read_nonempty_cited_is_grounded(data_files):
    with obs.observe(cites=data_files["data"]) as h:
        open(data_files["data"]).read()
    assert h.verdict.grounding is OG.GROUNDED


def test_file_read_empty_cited_is_ungrounded(data_files):
    # The cited file exists but is empty: the read carried nothing, which is the
    # silent-fallback signature, so the verdict is UNGROUNDED, not GROUNDED.
    with obs.observe(cites=data_files["empty"]) as h:
        open(data_files["empty"]).read()
    assert h.verdict.grounding is OG.UNGROUNDED


def test_file_grounded_reason_does_not_overclaim_byte_flow(data_files):
    # The builtins.open path proxies flow by file size and does not observe the
    # bytes read, so the signed reason must NOT claim it saw returned data. The
    # published verdict has to be honest about what it actually observed.
    with obs.observe(cites=data_files["data"]) as h:
        open(data_files["data"]).read()
    assert h.verdict.grounding is OG.GROUNDED
    assert "does not observe the bytes read" in h.verdict.reason
    assert "returned non-empty data" not in h.verdict.reason


def test_sqlite_grounded_reason_keeps_observed_return_wording(tmp_path):
    # sqlite reads observe the actual returned rows, so the stronger wording is
    # accurate and stays — only the file proxy is softened.
    import sqlite3

    db = str(tmp_path / "r.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE t(x)")
    con.execute("INSERT INTO t VALUES (1)")
    con.commit()
    con.close()
    with obs.observe(cites=db) as h:
        c = sqlite3.connect(db)
        c.execute("SELECT x FROM t").fetchall()
        c.close()
    assert h.verdict.grounding is OG.GROUNDED
    assert "returned non-empty data (sqlite)" in h.verdict.reason


def test_write_mode_open_is_not_an_ingress(tmp_path):
    out = tmp_path / "out.csv"
    with obs.observe(cites=str(out)) as h:
        with open(out, "w") as f:
            f.write("x\n")
    # Opening for writing is egress, not a read: it must not ground a finding.
    assert h.verdict.grounding is OG.UNGROUNDED


def test_write_plus_mode_of_cited_path_is_ungrounded_not_opaque(tmp_path):
    # A 'w+' open reads back self-written bytes, not the cited external source.
    # The audit hook and the loader must classify it the same way, so it stays
    # UNGROUNDED and is never mistaken for an uninstrumented-read coverage gap.
    out = tmp_path / "out.csv"
    with obs.observe(cites=str(out)) as h:
        f = open(out, "w+")
        f.write("self\n")
        f.seek(0)
        f.read()
        f.close()
    assert h.verdict.grounding is OG.UNGROUNDED
    assert all(s.kind != "coverage-gap" for s in h.verdict.seams)


def test_connection_opened_before_scope_is_a_documented_bound(tmp_path):
    # DOCUMENTED BOUND: a connection opened BEFORE the scope is not swapped for
    # an observing wrapper, so its reads inside the scope are invisible and the
    # finding reads as UNGROUNDED (same class as a value loaded once and reused).
    # Pinned as intended behavior; open the cited source inside the scope to
    # have it counted.
    db = str(tmp_path / "pre.db")
    pre = sqlite3.connect(db)  # opened OUTSIDE any scope
    pre.execute("CREATE TABLE t(x)")
    pre.execute("INSERT INTO t VALUES (1)")
    pre.commit()
    with obs.observe(cites=db) as h:
        pre.execute("SELECT * FROM t").fetchall()  # read via the pre-scope conn
    pre.close()
    assert h.verdict.grounding is OG.UNGROUNDED


def test_sqlite_iteration_of_a_cited_read_is_grounded(tmp_path):
    # `for row in conn.execute(...)` is the idiomatic sqlite read and drives the
    # C-level iterator, not fetch*. A cited source consumed this way must ground
    # the finding, not read as a false UNGROUNDED.
    db = str(tmp_path / "iter.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE t(x)")
    con.execute("INSERT INTO t VALUES (1)")
    con.commit()
    con.close()
    with obs.observe(cites=db) as h:
        c = sqlite3.connect(db)  # opened INSIDE the scope: wrapped
        rows = [r for r in c.execute("SELECT x FROM t")]
        c.close()
    assert rows == [(1,)]
    assert h.verdict.grounding is OG.GROUNDED


def test_iteration_of_an_empty_cited_read_is_not_grounded(tmp_path):
    # Iteration that yields no rows is an empty cited read: the data did not
    # arrive, so the finding is not GROUNDED (no seam either -> UNGROUNDED).
    db = str(tmp_path / "iter_empty.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE t(x)")
    con.commit()
    con.close()
    with obs.observe(cites=db) as h:
        c = sqlite3.connect(db)
        rows = [r for r in c.execute("SELECT x FROM t")]
        c.close()
    assert rows == []
    assert h.verdict.grounding is not OG.GROUNDED


@pytest.mark.skipif(
    not os.path.exists("/dev/zero"), reason="needs a character device"
)
def test_non_regular_cited_read_is_opaque_not_ungrounded():
    # A cited fifo / device / stream reports stat size 0 even when the read
    # delivers bytes, so it must never read as a confident UNGROUNDED: the honest
    # verdict is OPAQUE via a coverage-gap seam.
    dev = "/dev/zero"
    with obs.observe(cites=dev) as h:
        f = open(dev)  # non-regular, opens without blocking
        f.close()
    assert h.verdict.grounding is OG.OPAQUE
    assert any(s.kind == "coverage-gap" for s in h.verdict.seams)


def test_read_coverage_fraction_never_exceeds_one(tmp_path):
    # Coverage is a property of the file surface: mixing a file read with a
    # sqlite read must not push the fraction above 1.0 (they count different
    # universes; only file reads and file opens are compared).
    data = tmp_path / "d.csv"
    data.write_text("x\n1\n")
    db = str(tmp_path / "x.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE t(x)")
    con.execute("INSERT INTO t VALUES (1)")
    con.commit()
    con.close()
    with obs.observe(cites=str(data)) as h:
        open(str(data)).read()
        c = sqlite3.connect(db)
        c.execute("SELECT * FROM t").fetchall()
        c.close()
    cov = h.verdict.read_coverage_fraction()
    # A cited file was opened and read here, so coverage is a real fraction, not
    # None: assert both, or a regression that stops computing it slips through.
    assert cov is not None and cov <= 1.0


def test_positional_factory_does_not_break_connect(tmp_path):
    # A caller pinning their own Connection factory positionally (the 6th arg)
    # inside a scope must not hit a "multiple values for factory" TypeError from
    # the wrapper: the wrapper delegates transparently instead of injecting.
    db = str(tmp_path / "c.db")

    class MyConn(sqlite3.Connection):
        pass

    with obs.observe(cites=db):
        conn = sqlite3.connect(db, 5.0, 0, "", True, MyConn)
        assert isinstance(conn, MyConn)
        conn.close()


# -- wrapped loaders: sqlite -------------------------------------------------

def test_sqlite_nonempty_fetch_cited_is_grounded(tmp_path):
    db = str(tmp_path / "study.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE t(x)")
    con.execute("INSERT INTO t VALUES (1)")
    con.commit()
    con.close()
    with obs.observe(cites=db) as h:
        c = sqlite3.connect(db)
        rows = c.execute("SELECT * FROM t").fetchall()
        c.close()
    assert rows
    assert h.verdict.grounding is OG.GROUNDED


def test_sqlite_empty_fetch_cited_is_ungrounded(tmp_path):
    db = str(tmp_path / "study.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE t(x)")
    con.commit()
    con.close()
    with obs.observe(cites=db) as h:
        c = sqlite3.connect(db)
        c.execute("SELECT * FROM t").fetchall()  # empty
        c.close()
    assert h.verdict.grounding is OG.UNGROUNDED


# -- conditional third-party wrapping ---------------------------------------

def test_third_party_wrap_absent_when_module_not_imported():
    # A loader mareforma never imports itself stays unwrapped: coverage is
    # "wraps X if you use X," and nothing forces an import. Only checkable when
    # pandas has not been imported yet; skip rather than pass vacuously once it
    # has (a sibling test imports it), so the invariant is really asserted.
    import sys

    import mareforma.observe._loaders as L

    if "pandas" in sys.modules:
        pytest.skip("pandas already imported; the unwrapped-until-used "
                    "invariant cannot be checked in this process")
    assert "pandas.read_csv" not in L._reals


def test_third_party_wrap_present_when_module_imported(tmp_path):
    pd = pytest.importorskip("pandas")
    data = tmp_path / "d.csv"
    data.write_text("a,b\n1,2\n")
    with obs.observe(cites=str(data)) as h:
        pd.read_csv(str(data))
    assert h.verdict.grounding is OG.GROUNDED


# -- read-to-citation binding ------------------------------------------------

def test_incidental_read_does_not_ground(data_files):
    # Reading a config file (not the cited source) must not ground the finding,
    # even though it is a real non-empty read through the wrapped open().
    with obs.observe(cites=data_files["data"]) as h:
        open(data_files["cfg"]).read()
    assert h.verdict.grounding is OG.UNGROUNDED


def test_incidental_and_cited_read_is_grounded(data_files):
    with obs.observe(cites=data_files["data"]) as h:
        open(data_files["cfg"]).read()   # incidental
        open(data_files["data"]).read()  # cited
    assert h.verdict.grounding is OG.GROUNDED


def test_content_address_opt_in_matches_by_hash():
    # Exercised via the citation layer directly to avoid a live network call.
    from mareforma.observe._citation import read_matches_citation
    from mareforma.trust._store import content_address_data_id

    ca = content_address_data_id(b"payload-bytes")
    assert read_matches_citation("https://api/x", ca, (ca,)) is True
    assert read_matches_citation("https://api/x", None, (ca,)) is False


# -- fail-safe (chaos) -------------------------------------------------------

def test_observer_internal_error_degrades_to_opaque_host_unaffected(
    data_files, monkeypatch
):
    # Inject a fault into the observer's own recording path. The host still
    # gets its data, and the verdict is OPAQUE (we can no longer trust what we
    # saw), never a re-raise into host code.
    def boom(*a, **k):
        raise RuntimeError("injected fault")

    monkeypatch.setattr(_scope, "record_read", boom)
    with obs.observe(cites=data_files["data"]) as h:
        content = open(data_files["data"]).read()
    assert content.startswith("arm")  # host read succeeded
    assert h.verdict.grounding is OG.OPAQUE


def test_host_exception_propagates_untouched(data_files, tmp_path):
    missing = str(tmp_path / "nope.csv")
    with pytest.raises(FileNotFoundError):
        with obs.observe(cites=data_files["data"]) as h:
            open(missing).read()
    # A host-side failure is the host's; the observer records no cited read.
    assert h.verdict.grounding is OG.UNGROUNDED
