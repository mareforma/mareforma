"""Adoption bugs: the documented form must run, and the common mistakes must be
named rather than silently mishandled.

Covers three fixes:

- ``mareforma.observe``'s own docstring showed a call that raised ``'module'
  object is not callable`` when copied; the documented import form now runs.
- a read on a ``forkserver`` child is invisible to the observer, so it must land
  OPAQUE, not a confident false UNGROUNDED.
- ``mareforma.open("graph.db")`` is the sqlite-style mistake; it now refuses with
  guidance instead of creating a nested project inside a directory named
  ``graph.db``.
"""
from __future__ import annotations

import multiprocessing as mp
from pathlib import Path

import pytest

import mareforma


# ---------------------------------------------------------------------------
# The observe docstring is runnable
# ---------------------------------------------------------------------------

class TestObserveDocstringRunnable:
    def test_module_attribute_is_the_package_not_a_callable(self):
        # The reason the old example raised: mareforma.observe is the SUBMODULE,
        # so calling it is a TypeError. The function is reached by import.
        import types

        assert isinstance(mareforma.observe, types.ModuleType)

    def test_documented_import_form_runs(self, tmp_path):
        from mareforma.observe import observe

        target = tmp_path / "trial.csv"
        target.write_text("a,b\n1,2\n")
        with observe(cites=str(target)) as obs:
            target.read_text()
        assert obs.verdict is not None

    def test_docstring_shows_the_runnable_form(self):
        import mareforma.observe as observe_pkg

        doc = observe_pkg.__doc__ or ""
        # The corrected form imports the function; it never calls the module.
        assert "from mareforma.observe import observe" in doc
        assert "mareforma.observe(cites=" not in doc


# ---------------------------------------------------------------------------
# A forkserver child read is OPAQUE, not UNGROUNDED
# ---------------------------------------------------------------------------

def _read_in_child(path: str, done: str) -> None:
    """Read the cited file in a child process, then signal completion by file.

    A file signal avoids a Queue's semaphores, whose teardown noise would
    otherwise clutter the test output.
    """
    Path(path).read_text()
    Path(done).write_text("ok")


_HAS_FORKSERVER = "forkserver" in mp.get_all_start_methods()


class TestForkserverIsOpaque:
    @pytest.mark.skipif(not _HAS_FORKSERVER, reason="forkserver unavailable")
    def test_forkserver_child_read_lands_opaque(self, tmp_path):
        from mareforma.observe import observe
        from mareforma.observe._verdict import ObservedGrounding

        target = tmp_path / "trial.csv"
        target.write_text("a,b\n1,2\n")
        ctx = mp.get_context("forkserver")

        # Warm the fork server OUTSIDE any scope, the realistic app-init case: a
        # later child then reaches the parent only as a socket connect.
        warm_done = tmp_path / "warm.done"
        warm = ctx.Process(target=_read_in_child, args=(str(target), str(warm_done)))
        warm.start()
        warm.join()

        done = tmp_path / "child.done"
        with observe(cites=str(target)) as obs:
            p = ctx.Process(target=_read_in_child, args=(str(target), str(done)))
            p.start()
            p.join()

        assert obs.verdict.grounding is ObservedGrounding.OPAQUE

    def test_process_start_records_a_subprocess_seam(self, tmp_path):
        """Independent of start method, a Process.start inside a scope is seamed.

        This is the primary mechanism the audit hook cannot guarantee for a
        forkserver child; it is exercised here on the default start method too.
        """
        from mareforma.observe import _loaders, _scope, observe

        _loaders.ensure_installed()
        target = tmp_path / "trial.csv"
        target.write_text("x\n")
        done = tmp_path / "seam.done"
        with observe(cites=str(target)):
            scope = _scope.current_scope()
            p = mp.get_context("spawn").Process(
                target=_read_in_child, args=(str(target), str(done)),
            )
            p.start()
            p.join()
            seam_kinds = {s.kind for s in scope.seams}
        assert "subprocess" in seam_kinds


# ---------------------------------------------------------------------------
# open() refuses the sqlite-style mistake
# ---------------------------------------------------------------------------

class TestOpenGuardsAgainstNestedProject:
    def test_open_on_a_dir_named_graph_db_refuses(self, tmp_path):
        mistake = tmp_path / "graph.db"
        with pytest.raises(ValueError, match="project root directory"):
            mareforma.open(mistake)
        # No project was created inside the refused path.
        assert not (mistake / ".mareforma").exists()

    def test_open_on_an_existing_file_refuses(self, tmp_path):
        f = tmp_path / "graph.db"
        f.write_text("not a project")
        with pytest.raises(ValueError, match="is a file"):
            mareforma.open(f)

    def test_open_on_a_dot_mareforma_graph_db_path_refuses(self, tmp_path):
        mistake = tmp_path / ".mareforma" / "graph.db"
        with pytest.raises(ValueError, match="not the graph.db file"):
            mareforma.open(mistake)

    def test_open_on_a_real_directory_still_works(self, tmp_path):
        with mareforma.open(tmp_path) as graph:
            assert (tmp_path / ".mareforma" / "graph.db").exists()
            graph.assert_claim("a claim", classification="ANALYTICAL")

    def test_existing_project_named_graph_db_still_opens(self, tmp_path):
        # The name heuristic must not lock out an already-initialised project
        # that happens to be named graph.db: it carries its own .mareforma/. Build
        # a real project, then give its directory the db file's name.
        normal = tmp_path / "proj"
        with mareforma.open(normal) as graph:
            graph.assert_claim("a claim", classification="ANALYTICAL")
        oddly_named = tmp_path / "graph.db"
        normal.rename(oddly_named)
        with mareforma.open(oddly_named) as graph:
            assert graph.query() is not None
