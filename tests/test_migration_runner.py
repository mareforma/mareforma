"""The migration is one transaction, and a crash at any step changes nothing.

The failure this guards against is the worst one the project has: the message an
older build printed on a version it did not recognise told the operator to
delete graph.db and warned that the chain could not be rebuilt from claims.toml.
On a file that failed to migrate that advice destroys exactly what the product
exists to protect, and the migration is a path this code creates.

So there is no half-migrated state to advise about. The rebuild and the version
bump commit together or roll back together, and these tests injure the rebuild
at every one of its seven steps to show it.

Three mechanisms, because they fail differently and a matrix built on one of
them proves less than it looks. The authorizer denies at prepare time, so it
shows a step never ran. The progress handler interrupts inside a running
statement, which is the partial case. SIGKILL in a subprocess takes the process
away without unwinding anything, which is the only one that tests durability.
"""

from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import warnings
from pathlib import Path

import pytest

import mareforma
from mareforma.db import core as _core
from mareforma.db._schema_sql import _ALL_EXPECTED_TRIGGERS, _CLAIM_COLUMNS
from mareforma.db.core import MigrationError
from tests._helpers import (
    _bootstrap_key, _enroll_key, _requires_drop_column,
)


# The seven steps, each named by the authorizer event that begins it. Observed
# by logging a real rebuild rather than derived from the source, so a step that
# stops firing shows up as a test that can no longer injure it.
_STEPS = (
    ("create the new table", sqlite3.SQLITE_CREATE_TABLE, "claims_new"),
    ("copy the rows", sqlite3.SQLITE_INSERT, "claims_new"),
    ("drop the old table", sqlite3.SQLITE_DROP_TABLE, "claims"),
    ("rename", sqlite3.SQLITE_ALTER_TABLE, "main"),
    ("recreate the triggers", sqlite3.SQLITE_CREATE_TRIGGER,
     "claims_signed_fields_no_laundering"),
    ("recreate the indexes", sqlite3.SQLITE_CREATE_INDEX,
     "idx_claims_artifact_hash"),
    ("bump the version", sqlite3.SQLITE_PRAGMA, "user_version"),
)


def _db(root: Path) -> Path:
    return root / ".mareforma" / "graph.db"


def _populated(root: Path, claims: int = 6) -> Path:
    """A graph with claims, a signed verdict and an observed axis on it."""
    from mareforma.observe import observe

    key = _bootstrap_key(root, "root.key")
    data = root / "trial.csv"
    data.write_text("arm,outcome\ntreat,1\n")
    ids = []
    with mareforma.open(root, key_path=key) as g:
        with observe(cites=str(data.resolve())) as handle:
            data.read_text()
        ids.append(g.assert_claim(
            "an observed finding", classification="ANALYTICAL",
            predicate_payload={
                "data_sources": [str(data.resolve())], "data_ids": [],
            },
            observed_grounding=handle.verdict.to_signed_dict(),
        ))
        for i in range(claims - 1):
            ids.append(g.assert_claim(f"claim {i}", generated_by=f"run{i}"))
    witness = _bootstrap_key(root, "witness.key")
    _enroll_key(root, key, witness, identity="witness@example.org")
    with mareforma.open(root, key_path=witness) as g:
        g.record_contradiction_verdict(
            verdict_id="v1", member_claim_id=ids[2], other_claim_id=ids[1],
        )
    return key


def _fingerprint(root: Path) -> dict:
    """Everything a crash must leave exactly as it was."""
    conn = sqlite3.connect(_db(root))
    try:
        return {
            "user_version": conn.execute("PRAGMA user_version").fetchone()[0],
            "rows": conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0],
            "cids": [
                r[0] for r in conn.execute(
                    "SELECT statement_cid FROM claims ORDER BY claim_id"
                )
            ],
            "triggers": {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                )
            },
            "indexes": {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index' "
                    "AND tbl_name = 'claims'"
                )
            },
            "stray": [
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE sql LIKE '%claims_new%'"
                )
            ],
        }
    finally:
        conn.close()


def _assert_untouched(root: Path, before: dict) -> None:
    after = _fingerprint(root)
    assert after["user_version"] == before["user_version"]
    assert after["rows"] == before["rows"]
    assert after["cids"] == before["cids"]
    assert after["triggers"] == before["triggers"]
    assert after["indexes"] == before["indexes"]
    assert after["stray"] == [], f"the rebuild left {after['stray']} behind"
    assert set(_ALL_EXPECTED_TRIGGERS) <= after["triggers"]



def _rebuild_claims_unchanged(conn) -> None:
    """A step that rebuilds ``claims`` under the definition this release has.

    The runner's mechanics are what these tests are about: the copy, the drop,
    the rename, the guards, the version bump, and what a failure part-way
    leaves behind. A step that changes the table would make every one of them
    depend on which columns this release happens to have.

    It reads the live column list rather than assuming one, so it runs on a
    graph a released version wrote as readily as on a fresh one: what the
    current definition can hold is carried, and whatever is left over is
    declared as dropped, which is what the runner requires of any step.

    It lives here rather than in the package because the package has no such
    step any more. The one it shipped was there to prove the machinery on real
    graphs before a narrowing step relied on it, and that release never
    published, so it was removed rather than frozen in place.
    """
    live = {r[1] for r in conn.execute("PRAGMA table_info(claims)")}
    keep = tuple(c for c in _core._CLAIM_COLUMNS if c in live)
    # Whatever this release cannot carry, the objects that read it cannot be
    # carried either: the rebuild replays an index or an unmanaged trigger
    # verbatim, and one naming a column that is going fails at the CREATE. A
    # narrowing step retires them by name, and this stands in for one.
    going = set(live) - set(keep)
    if going:
        for kind, name, sql in conn.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE tbl_name = 'claims' AND type IN ('index', 'trigger')"
        ).fetchall():
            if sql and any(col in sql for col in going):
                conn.execute(f"DROP {kind.upper()} IF EXISTS {name}")
    _core._rebuild_table(
        conn, table="claims",
        create_sql=_core.claims_rebuild_sql("claims_new"),
        columns=keep,
        drops=tuple(sorted(live - set(keep))),
    )


class TestTheHappyPath:
    def test_a_rebuild_leaves_the_graph_it_started_with(
        self, tmp_path: Path,
    ) -> None:
        """A same-schema rebuild is a no-op that touches every object."""
        _populated(tmp_path)
        before = _fingerprint(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with _core._upgrade_window(conn):
            _core._run_migration(
                conn, to_version=before["user_version"],
                steps=_rebuild_claims_unchanged,
            )
        conn.close()
        _assert_untouched(tmp_path, before)

    def test_every_table_that_references_claims_still_takes_a_row(
        self, tmp_path: Path,
    ) -> None:
        """The check that catches a rebuild both pragmas would pass.

        A rename in the wrong order rewrites the REFERENCES clause of every
        table pointing at claims, and both ``foreign_key_check`` and
        ``integrity_check`` come back clean on the result. What does not come
        back clean is writing to it.
        """
        key = _populated(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with _core._upgrade_window(conn):
            _core._run_migration(
                conn, to_version=1, steps=_rebuild_claims_unchanged,
            )
        # Match either spelling. The reparse check renames the table aside and
        # back, and SQLite requotes the clause it rewrites, so the stored text
        # becomes REFERENCES "claims". The reference is the same; only the
        # quoting moved, and nothing in the tree compares table DDL text.
        referencing = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND (sql LIKE '%REFERENCES claims%' "
                "     OR sql LIKE '%REFERENCES \"claims\"%')"
            )
        }
        conn.close()
        assert referencing, "no table references claims, the check is vacuous"

        # Recording a verdict walks the foreign keys the rename would have
        # broken, and it is the operation the whole graph exists for.
        witness = tmp_path / "witness.key"
        with mareforma.open(tmp_path, key_path=witness) as g:
            ids = [c["claim_id"] for c in g.query(include_invalidated=True)]
            g.record_contradiction_verdict(
                verdict_id="v2", member_claim_id=ids[4], other_claim_id=ids[3],
            )

    def test_the_full_text_index_is_not_double_populated(
        self, tmp_path: Path,
    ) -> None:
        """Recreate the sync triggers before the copy and every row indexes twice.

        ``integrity_check`` is clean either way, so the count is the only thing
        that shows it.
        """
        _populated(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        before = conn.execute("SELECT COUNT(*) FROM claims_fts").fetchone()[0]
        conn.row_factory = sqlite3.Row
        with _core._upgrade_window(conn):
            _core._run_migration(
                conn, to_version=1, steps=_rebuild_claims_unchanged,
            )
        after = conn.execute("SELECT COUNT(*) FROM claims_fts").fetchone()[0]
        claims = conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
        conn.close()
        assert after == before == claims

    def test_both_pragmas_are_put_back(self, tmp_path: Path) -> None:
        """Both are connection-scoped, and leaving either is a live hazard.

        Foreign keys off is an unenforced schema for every later write.
        ``legacy_alter_table`` on means every later rename silently stops
        rewriting references, which is the laundering primitive this path is
        gated to prevent.
        """
        _populated(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        with _core._upgrade_window(conn):
            _core._run_migration(
                conn, to_version=1, steps=_rebuild_claims_unchanged,
            )
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute("PRAGMA legacy_alter_table").fetchone()[0] == 0
        conn.close()


class TestTheCrashMatrix:
    """Deny each step at prepare time. The step never runs, nothing changes."""

    @pytest.mark.parametrize(
        "label, action, target", _STEPS, ids=[s[0] for s in _STEPS],
    )
    def test_a_denied_step_changes_nothing(
        self, tmp_path: Path, label: str, action: int, target: str,
    ) -> None:
        _populated(tmp_path)
        before = _fingerprint(tmp_path)

        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row

        def deny_one(act, arg1, arg2, dbname, trigger):
            if act == action and arg1 == target:
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        conn.set_authorizer(deny_one)
        with pytest.raises(MigrationError, match="rolled back"):
            with _core._upgrade_window(conn):
                _core._run_migration(
                    conn, to_version=2, steps=_rebuild_claims_unchanged,
                )
        conn.set_authorizer(None)
        conn.close()

        _assert_untouched(tmp_path, before)

    def test_the_graph_still_opens_after_every_denied_step(
        self, tmp_path: Path,
    ) -> None:
        """Injuring one step must not leave a file the library cannot open."""
        key = _populated(tmp_path)
        for _, action, target in _STEPS:
            conn = sqlite3.connect(_db(tmp_path))
            conn.row_factory = sqlite3.Row
            conn.set_authorizer(
                lambda a, a1, a2, d, t, _a=action, _t=target:
                sqlite3.SQLITE_DENY if (a == _a and a1 == _t)
                else sqlite3.SQLITE_OK
            )
            with pytest.raises(MigrationError):
                with _core._upgrade_window(conn):
                    _core._run_migration(
                        conn, to_version=2,
                        steps=_rebuild_claims_unchanged,
                    )
            conn.set_authorizer(None)
            conn.close()
            with mareforma.open(tmp_path, key_path=key) as g:
                assert g.query(include_invalidated=True)

    def test_the_failure_never_tells_anyone_to_delete_the_database(
        self, tmp_path: Path,
    ) -> None:
        """The message is the point of the whole phase.

        A crashed migration used to reach the remedy that says delete graph.db
        and warns the chain cannot be rebuilt from claims.toml. On a graph this
        code failed to migrate, that destroys an intact chain over a fault that
        changed nothing.
        """
        _populated(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        conn.set_authorizer(
            lambda a, a1, a2, d, t:
            sqlite3.SQLITE_DENY
            if (a == sqlite3.SQLITE_DROP_TABLE and a1 == "claims")
            else sqlite3.SQLITE_OK
        )
        with pytest.raises(MigrationError) as caught:
            with _core._upgrade_window(conn):
                _core._run_migration(
                    conn, to_version=2, steps=_rebuild_claims_unchanged,
                )
        conn.set_authorizer(None)
        conn.close()

        message = str(caught.value).lower()
        assert "do not delete" in message
        assert "changed nothing" in message
        assert "delete .mareforma/graph.db" not in message
        assert "start fresh" not in message


class TestInterruptedMidStatement:
    def test_an_interrupted_copy_changes_nothing(self, tmp_path: Path) -> None:
        """The partial case the authorizer cannot reach.

        A denied step never starts. A progress handler that returns non-zero
        stops a statement that is already running, part-way through writing
        rows, which is the state a real crash leaves.
        """
        _populated(tmp_path, claims=60)
        before = _fingerprint(tmp_path)

        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row

        # Armed by the authorizer at the moment the copy is prepared, so the
        # interrupt lands inside that statement rather than on whichever
        # statement happens to run first. Counting alone tripped on the temp
        # table the upgrade window opens, and never reached the copy at all.
        armed = {"yes": False}

        def arm(action, arg1, arg2, dbname, trigger):
            if action == sqlite3.SQLITE_INSERT and arg1 == "claims_new":
                armed["yes"] = True
            return sqlite3.SQLITE_OK

        conn.set_authorizer(arm)
        conn.set_progress_handler(lambda: 1 if armed["yes"] else 0, 8)
        with pytest.raises(MigrationError):
            with _core._upgrade_window(conn):
                _core._run_migration(
                    conn, to_version=2, steps=_rebuild_claims_unchanged,
                )
        assert armed["yes"], "the copy never started, nothing was interrupted"
        conn.set_progress_handler(None, 0)
        conn.set_authorizer(None)
        # Closing is what a crashed process does, and it rolls back an open
        # transaction the interrupted ROLLBACK could not.
        conn.close()

        _assert_untouched(tmp_path, before)


class TestKilledOutright:
    def test_a_killed_migration_leaves_the_graph_intact(
        self, tmp_path: Path,
    ) -> None:
        """Durability, which neither of the other two mechanisms tests.

        A denied step and an interrupt both unwind through Python. SIGKILL does
        not: the process stops between one write and the next, and what stands
        afterwards is whatever SQLite's journal guarantees on its own.
        """
        _populated(tmp_path, claims=40)
        before = _fingerprint(tmp_path)

        script = textwrap.dedent(
            """
            import os, signal, sqlite3, sys, warnings
            warnings.filterwarnings("ignore")
            sys.path.insert(0, sys.argv[1])
            from mareforma.db import core as _core

            conn = sqlite3.connect(sys.argv[2])
            conn.row_factory = sqlite3.Row
            calls = {"n": 0}

            def die():
                calls["n"] += 1
                if calls["n"] > 4:
                    os.kill(os.getpid(), signal.SIGKILL)
                return 0

            conn.set_progress_handler(die, 8)
            with _core._upgrade_window(conn):
                _core._run_migration(
                    conn, to_version=2, steps=_rebuild_claims_unchanged,
                )
            """
        )
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        proc = subprocess.run(
            [sys.executable, "-c", script,
             str(Path(__file__).resolve().parent.parent), str(_db(tmp_path))],
            capture_output=True, text=True, env=env,
        )
        assert proc.returncode == -signal.SIGKILL, (
            f"the subprocess was not killed mid-migration: {proc.returncode}\n"
            f"{proc.stdout}\n{proc.stderr}"
        )
        _assert_untouched(tmp_path, before)


_REPO_ROOT = Path(__file__).resolve().parent.parent
# The two releases a user upgrading from is actually holding.
_RELEASES = (("0.3.11", "66b73d4"), ("0.3.12", "94e0fc7"))


def _release_available(commit: str) -> bool:
    try:
        subprocess.run(
            ["git", "-C", str(_REPO_ROOT), "rev-parse", "--verify",
             f"{commit}^{{commit}}"],
            capture_output=True, check=True,
        )
        return True
    except (OSError, subprocess.CalledProcessError):
        return False


def _graph_built_by(version: str, commit: str, work: Path, root: Path) -> None:
    """Lay down a graph using the released package, in a subprocess.

    A fixture this tree creates carries this tree's triggers, indexes and FTS
    state. A graph a released version wrote carries its own, plus whatever the
    columns migration adds on the first open here, which is the drift a rebuild
    has to survive and the reason these run against the real thing.
    """
    pkg_dir = work / version
    pkg_dir.mkdir(parents=True, exist_ok=True)
    archive = subprocess.run(
        ["git", "-C", str(_REPO_ROOT), "archive", commit, "mareforma"],
        capture_output=True, check=True,
    ).stdout
    subprocess.run(["tar", "-x", "-C", str(pkg_dir)], input=archive, check=True)

    script = textwrap.dedent(
        f"""
        import sys, warnings
        warnings.filterwarnings("ignore")
        sys.path.insert(0, sys.argv[1])
        from pathlib import Path
        import mareforma
        assert mareforma.__version__.startswith("{version}"), mareforma.__version__
        from mareforma import signing
        root = Path(sys.argv[2])
        key = root / "root.key"
        signing.bootstrap_key(key)
        with mareforma.open(root, key_path=key) as g:
            for i in range(6):
                g.assert_claim("claim %d" % i, generated_by="run%d" % i)
        """
    )
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    proc = subprocess.run(
        [sys.executable, "-c", script, str(pkg_dir), str(root)],
        capture_output=True, text=True, env=env,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"the {version} subprocess failed to build a graph:\n{proc.stderr}"
        )


@pytest.mark.parametrize(
    "version, commit", _RELEASES, ids=[r[0] for r in _RELEASES],
)
class TestAgainstGraphsRealUsersHold:
    """The matrix, re-run on graphs the released packages wrote.

    Everything above runs on a graph this tree made. That graph has this tree's
    schema by construction, so it cannot show whether the rebuild survives the
    drift an upgraded file carries: tables the additive script added on the way
    in, columns the upgrade helper added, indexes that were never in the
    original.
    """

    def test_a_rebuild_survives_the_drift(
        self, tmp_path: Path, version: str, commit: str,
    ) -> None:
        if not _release_available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_built_by(version, commit, tmp_path / "work", project)

        # First open under this tree applies the additive script and the column
        # migration. That upgraded-but-drifted file is what the rebuild meets.
        with mareforma.open(project, key_path=project / "root.key") as g:
            assert len(g.query(include_invalidated=True)) == 6
        before = _fingerprint(project)

        conn = sqlite3.connect(_db(project))
        conn.row_factory = sqlite3.Row
        with _core._upgrade_window(conn):
            _core._run_migration(
                conn, to_version=before["user_version"],
                steps=_rebuild_claims_unchanged,
            )
        conn.close()

        _assert_untouched(project, before)
        with mareforma.open(project, key_path=project / "root.key") as g:
            assert len(g.query(include_invalidated=True)) == 6

    def test_a_migration_runs_through_the_real_open_path(
        self, tmp_path: Path, version: str, commit: str, monkeypatch,
    ) -> None:
        """The whole sequence on a released graph, in the order an open uses it.

        The other tests here reach the rebuild on a raw connection, which skips
        the census, the column ALTERs and the exact-set check, and so cannot
        show that they run in an order that works. This one registers a route
        and opens the file the way a user would.
        """
        if not _release_available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_built_by(version, commit, tmp_path / "work", project)

        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 2)
        monkeypatch.setattr(
            _core, "_MIGRATIONS", {1: (2, _rebuild_claims_unchanged)},
        )
        with mareforma.open(project, key_path=project / "root.key") as g:
            assert len(g.query(include_invalidated=True)) == 6
            assert g._conn.execute(
                "PRAGMA user_version"
            ).fetchone()[0] == 2
            live = {
                r[0] for r in g._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                )
            }
        assert set(_ALL_EXPECTED_TRIGGERS) <= live

    @pytest.mark.parametrize(
        "label, action, target", _STEPS, ids=[s[0] for s in _STEPS],
    )
    def test_a_denied_step_changes_nothing(
        self, tmp_path: Path, version: str, commit: str,
        label: str, action: int, target: str,
    ) -> None:
        if not _release_available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_built_by(version, commit, tmp_path / "work", project)
        with mareforma.open(project, key_path=project / "root.key"):
            pass
        before = _fingerprint(project)

        conn = sqlite3.connect(_db(project))
        conn.row_factory = sqlite3.Row
        conn.set_authorizer(
            lambda a, a1, a2, d, t:
            sqlite3.SQLITE_DENY if (a == action and a1 == target)
            else sqlite3.SQLITE_OK
        )
        with pytest.raises(MigrationError):
            with _core._upgrade_window(conn):
                _core._run_migration(
                    conn, to_version=2, steps=_rebuild_claims_unchanged,
                )
        conn.set_authorizer(None)
        conn.close()

        _assert_untouched(project, before)
        with mareforma.open(project, key_path=project / "root.key") as g:
            assert len(g.query(include_invalidated=True)) == 6


class TestWhatTheMigrationMustNotQuietlyRepair:
    """A migration is a repair, and repairs are what the census runs ahead of."""

    def test_a_migration_does_not_heal_a_dropped_guard_before_the_census(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """The reconciler restores the whole managed set, not the rebuild's own.

        So a migration in the open path used to put back a guard somebody had
        dropped, on a table the rebuild never touched, before the census could
        write down that it was gone. The rows deleted while it was down stay
        deleted, and no later open can see that it ever happened.
        """
        key = _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("DROP TRIGGER rekor_inclusions_append_only")
        raw.execute("DROP TRIGGER replication_verdicts_append_only")
        raw.execute("PRAGMA user_version = 1")
        raw.commit()
        raw.close()

        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 2)
        monkeypatch.setattr(
            _core, "_MIGRATIONS", {1: (2, _rebuild_claims_unchanged)},
        )
        conn = _core.open_db(tmp_path)
        missing = _core.schema_census_missing(conn)
        conn.close()

        assert "rekor_inclusions_append_only" in missing
        assert "replication_verdicts_append_only" in missing

    def test_the_columns_are_added_before_the_migration_runs(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """A step copies this release's column list, so the columns must exist.

        An older file is missing them until the upgrade ALTERs have run. Migrate
        first and the step fails on exactly the graphs migrations exist for, and
        the file is stuck: too old for this release, too new for its own.
        """
        _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering")
        # Put the file back in the shape a release before observed_grounding
        # left it, which is what the column upgrade exists to repair.
        keep = [c for c in _CLAIM_COLUMNS if c != "observed_grounding"]
        raw.execute(
            f"CREATE TABLE claims_old AS SELECT {', '.join(keep)} FROM claims"
        )
        raw.execute("DROP TABLE claims")
        # The same pragma the rebuild needs, for the same reason: a trigger on
        # another table names claims in its body and a modern rename reparses
        # the whole schema while it is missing.
        raw.execute("PRAGMA legacy_alter_table = ON")
        raw.execute("ALTER TABLE claims_old RENAME TO claims")
        raw.execute("PRAGMA legacy_alter_table = OFF")
        raw.execute("PRAGMA user_version = 1")
        raw.commit()
        raw.close()

        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 2)
        monkeypatch.setattr(
            _core, "_MIGRATIONS", {1: (2, _rebuild_claims_unchanged)},
        )
        conn = _core.open_db(tmp_path)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(claims)")}
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 2
        conn.close()
        assert "observed_grounding" in cols


class TestConcurrentUpgrades:
    def test_a_held_write_lock_does_not_produce_the_delete_advice(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """Two people upgrading at once is the first contact with this code.

        The lock is taken inside the guarded block, so contention reads as a
        migration that changed nothing. Outside it, the raw sqlite error reached
        the generic open handler and the loser was told to delete graph.db.
        """
        _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("PRAGMA user_version = 1")
        raw.commit()
        raw.close()

        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 2)
        monkeypatch.setattr(
            _core, "_MIGRATIONS", {1: (2, _rebuild_claims_unchanged)},
        )
        holder = sqlite3.connect(_db(tmp_path), timeout=0.1)
        holder.execute("BEGIN IMMEDIATE")
        holder.execute("PRAGMA user_version = 1")
        try:
            with pytest.raises(Exception) as caught:
                conn = sqlite3.connect(_db(tmp_path), timeout=0.1)
                conn.row_factory = sqlite3.Row
                try:
                    _core._migrate_to_current(conn, 1)
                finally:
                    conn.close()
        finally:
            holder.rollback()
            holder.close()

        message = str(caught.value).lower()
        assert "delete .mareforma/graph.db" not in message
        assert "start fresh" not in message
        assert "do not delete" in message

    def test_a_step_does_not_apply_twice(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """Both openers read the version before either took the lock.

        The rebuild that ships is idempotent, which hides this. A step that
        inserts a row is not, and would run twice with no error and the right
        version afterwards.
        """
        _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("CREATE TABLE applied (n INTEGER)")
        raw.execute("PRAGMA user_version = 1")
        raw.commit()
        raw.close()

        def step(c):
            c.execute("INSERT INTO applied (n) VALUES (1)")

        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 2)
        monkeypatch.setattr(_core, "_MIGRATIONS", {1: (2, step)})

        first = sqlite3.connect(_db(tmp_path))
        first.row_factory = sqlite3.Row
        second = sqlite3.connect(_db(tmp_path))
        second.row_factory = sqlite3.Row
        # Both saw version 1 before either ran.
        _core._migrate_to_current(first, 1)
        _core._migrate_to_current(second, 1)
        applied = first.execute("SELECT COUNT(*) FROM applied").fetchone()[0]
        version = first.execute("PRAGMA user_version").fetchone()[0]
        first.close()
        second.close()

        assert applied == 1, "the step ran twice"
        assert version == 2


class TestAPartialChain:
    def test_the_message_does_not_claim_the_graph_is_unchanged(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """Step one commits, step two fails. Both halves of the old sentence
        were false, and the remedy it gave was unreachable: the release that
        wrote the file refuses the version the chain has already reached.
        """
        _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("PRAGMA user_version = 1")
        raw.commit()
        raw.close()

        def boom(c):
            raise RuntimeError("step two could not run")

        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 3)
        monkeypatch.setattr(
            _core, "_MIGRATIONS",
            {1: (2, _rebuild_claims_unchanged), 2: (3, boom)},
        )
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with pytest.raises(MigrationError) as caught:
            _core._migrate_to_current(conn, 1)
        landed = conn.execute("PRAGMA user_version").fetchone()[0]
        conn.close()

        message = str(caught.value).lower()
        assert landed == 2, "the committed step did not stand"
        assert "reached user_version=2" in message
        assert "opens at 2 with this release" in message
        assert "the graph is at the version it was at" not in message
        assert "do not delete" in message


class TestABrokenRegistry:
    @pytest.mark.parametrize("route, why", [
        ((1, "does not advance"), "a self-route loops forever"),
        ((9, "overshoots"), "a route past the current version bricks the file"),
    ], ids=["self-route", "overshoot"])
    def test_a_route_that_does_not_move_forward_is_refused(
        self, tmp_path: Path, monkeypatch, route, why: str,
    ) -> None:
        """One typo in a registry entry, caught before anything runs.

        A self-route commits a full table rebuild per pass on a function the
        open path calls, so the open hangs while rewriting claims forever. An
        overshoot commits a version this release then refuses as newer than
        itself, leaving a graph its own writer cannot open.
        """
        _populated(tmp_path)
        before = _fingerprint(tmp_path)
        to_version = route[0]
        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 2)
        monkeypatch.setattr(
            _core, "_MIGRATIONS",
            {1: (to_version, _rebuild_claims_unchanged)},
        )
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with pytest.raises(MigrationError, match="does not move forward"):
            _core._migrate_to_current(conn, 1)
        conn.close()
        _assert_untouched(tmp_path, before)


class TestTheRebuildLeavesTheTableAsItFoundIt:
    def test_a_guard_this_release_does_not_name_survives(
        self, tmp_path: Path,
    ) -> None:
        """The reconciler puts back the set this release names, and no more.

        A graph carrying a write guard from an earlier release loses it to the
        drop, with the reconciler not missing it and the census not reporting
        it. On an append-only store that is a guard gone with no record.
        """
        _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute(
            "CREATE TRIGGER legacy_claims_guard BEFORE DELETE ON claims "
            "BEGIN SELECT RAISE(ABORT, 'mareforma:legacy'); END"
        )
        raw.commit()
        raw.close()

        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with _core._upgrade_window(conn):
            _core._run_migration(
                conn, to_version=1, steps=_rebuild_claims_unchanged,
            )
        live = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'"
            )
        }
        conn.close()
        assert "legacy_claims_guard" in live

    def test_the_foreign_key_pragma_failing_on_success_is_not_swallowed(
        self, tmp_path: Path,
    ) -> None:
        """A connection handed back with foreign keys off is an unenforced schema.

        The swallow exists so an interrupted migration reports its own error
        rather than the noise after it. That reasoning covers the failure path
        only; on success the connection goes back to a caller.
        """
        _populated(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")

        def deny_restore(action, arg1, arg2, dbname, trigger):
            if action == sqlite3.SQLITE_PRAGMA and arg1 == "foreign_keys" \
                    and arg2 == "ON":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        conn.set_authorizer(deny_restore)
        # A MigrationError, not the sqlite error it came from: open_db wraps any
        # sqlite3.Error in the generic open failure, whose remedy is to delete
        # graph.db, and the migration has already committed by this point.
        with pytest.raises(MigrationError, match="do not delete"):
            with _core._upgrade_window(conn):
                _core._run_migration(
                    conn, to_version=1,
                    steps=_rebuild_claims_unchanged,
                )
        conn.set_authorizer(None)
        conn.close()


class TestNothingIsWrittenIntoAFileThisReleaseWillRefuse:
    """Refusing a graph must not change it on the way to refusing it."""

    def test_a_future_graph_gets_no_census_row(self, tmp_path: Path) -> None:
        """The census store forgets nothing, so writing into a newer graph is
        permanent. A guard this build expects and a later one retired would be
        recorded as missing forever, by the build least able to judge it.
        """
        key = _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("DROP TRIGGER rekor_inclusions_append_only")
        raw.execute("DELETE FROM schema_census")
        raw.execute("PRAGMA user_version = 99")
        raw.commit()
        raw.close()

        with pytest.raises(Exception, match="ahead of"):
            with mareforma.open(tmp_path, key_path=key):
                pass

        raw = sqlite3.connect(_db(tmp_path))
        rows = raw.execute("SELECT COUNT(*) FROM schema_census").fetchone()[0]
        raw.close()
        assert rows == 0, "the refusal wrote a tamper record into a newer graph"

    def test_a_file_that_is_not_a_graph_gets_no_tables(
        self, tmp_path: Path,
    ) -> None:
        """Pointing this at an unrelated database should refuse it, not build in it."""
        stray = tmp_path / "not-a-graph.db"
        conn = sqlite3.connect(stray)
        conn.execute("CREATE TABLE unrelated (x INTEGER)")
        conn.execute("PRAGMA user_version = 1")
        conn.commit()
        conn.close()

        with pytest.raises(Exception):
            _core.open_db_from_db_path(stray)

        conn = sqlite3.connect(stray)
        names = {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master")
        }
        conn.close()
        assert "schema_census" not in names
        assert "schema_guards_seen" not in names

    def test_a_version_with_no_route_alters_nothing_first(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """The column ALTERs commit one at a time, so refusing after them leaves
        a file this release will not open and the release that wrote it now
        rejects for carrying columns it does not know.
        """
        _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("PRAGMA user_version = 1")
        raw.commit()
        raw.close()
        before = _fingerprint(tmp_path)

        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 3)
        monkeypatch.setattr(_core, "_MIGRATIONS", {})
        with pytest.raises(MigrationError, match="no migration"):
            _core.open_db(tmp_path)
        _assert_untouched(tmp_path, before)


class TestAConcurrentUpgradeCannotSlipPastTheGate:
    def test_a_version_moved_past_this_release_is_refused(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """The future gate runs on the version read before the lock was taken.

        Another opener can move the file past this release while this one waits,
        and the early return then adopts whatever is on disk.
        """
        _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("PRAGMA user_version = 1")
        raw.commit()
        raw.close()

        def jump(c):
            raise AssertionError("the step should never run")

        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 2)
        monkeypatch.setattr(_core, "_MIGRATIONS", {1: (2, jump)})
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        # Somebody else took it to 7 in the window before the lock.
        other = sqlite3.connect(_db(tmp_path))
        other.execute("PRAGMA user_version = 7")
        other.commit()
        other.close()

        with pytest.raises(MigrationError, match="past the 2"):
            _core._migrate_to_current(conn, 1)
        conn.close()

    def test_a_version_that_does_not_advance_stops(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """A step that commits without moving the version loops forever here,
        rebuilding the table each pass, inside an open().
        """
        _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("PRAGMA user_version = 1")
        raw.commit()
        raw.close()

        def noop(conn_, **kwargs):
            """A step whose effect a concurrent writer undid after it committed."""

        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 2)
        monkeypatch.setattr(
            _core, "_MIGRATIONS", {1: (2, _rebuild_claims_unchanged)},
        )
        monkeypatch.setattr(_core, "_run_migration", noop)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with pytest.raises(MigrationError, match="did not advance"):
            _core._migrate_to_current(conn, 1)
        conn.close()

    def test_a_step_another_opener_already_ran_leaves_keys_enforced(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """The early return hands the connection back like any success does."""
        _populated(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        with _core._upgrade_window(conn):
            _core._run_migration(
                conn, to_version=2, from_version=99,
                steps=_rebuild_claims_unchanged,
            )
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        conn.close()


class TestANarrowingRebuildRefusesWhatItCannotCarry:
    def test_an_undeclared_column_is_not_silently_erased(
        self, tmp_path: Path,
    ) -> None:
        """The exact column-set check runs after the rebuild, so a rebuild that
        drops an unknown column leaves that check comparing against the
        laundered result and finding nothing to report.
        """
        from mareforma.db._schema_sql import claims_rebuild_sql

        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("ALTER TABLE claims ADD COLUMN retraction_reason TEXT")
        raw.execute("UPDATE claims SET retraction_reason = 'kept'")
        raw.commit()
        raw.close()

        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with pytest.raises(MigrationError, match="retraction_reason"):
            with _core._upgrade_window(conn):
                _core._rebuild_table(
                    conn, table="claims",
                    create_sql=claims_rebuild_sql("claims_new"),
                    columns=_CLAIM_COLUMNS,
                )
        kept = conn.execute(
            "SELECT COUNT(*) FROM claims WHERE retraction_reason = 'kept'"
        ).fetchone()[0]
        conn.close()
        assert kept == 3

    def test_a_trigger_naming_a_dropped_column_stops_the_migration(
        self, tmp_path: Path,
    ) -> None:
        """SQLite resolves a trigger body when it fires, not when it is created.

        So replaying one that names a dropped column succeeds, the migration
        commits, and the next write to the table fails on a graph the migration
        reported as upgraded. Atomicity does not help: nothing failed inside the
        transaction.
        """
        from mareforma.db._schema_sql import claims_rebuild_sql

        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute(
            "CREATE TRIGGER legacy_summary_guard BEFORE DELETE ON claims "
            "BEGIN SELECT RAISE(ABORT, 'gone') "
            "WHERE OLD.comparison_summary IS NOT NULL; END"
        )
        raw.commit()
        raw.close()

        keep = tuple(c for c in _CLAIM_COLUMNS if c != "comparison_summary")
        narrower = claims_rebuild_sql("claims_new").replace(
            "    comparison_summary TEXT,\n", "", 1,
        )
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with pytest.raises(MigrationError, match="legacy_summary_guard"):
            with _core._upgrade_window(conn):
                _core._rebuild_table(
                    conn, table="claims", create_sql=narrower, columns=keep,
                    drops=("comparison_summary",),
                )
        conn.close()


class TestAnUnroutableGraphIsNotWrittenInto:
    def test_a_version_with_no_route_gets_no_census_row(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """The census store forgets nothing, so writing into a file this build
        has just said it cannot interpret is permanent.

        A guard this release expects that an old unroutable file never had was
        recorded as missing, and the release that wrote the file then read that
        record and branded every claim in it.
        """
        key = _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("DELETE FROM schema_census")
        raw.execute("DROP TRIGGER rekor_inclusions_append_only")
        raw.execute("PRAGMA user_version = 1")
        raw.commit()
        raw.close()

        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 3)
        monkeypatch.setattr(_core, "_MIGRATIONS", {2: (3, lambda c: None)})
        with pytest.raises(MigrationError, match="no migration"):
            _core.open_db(tmp_path)

        raw = sqlite3.connect(_db(tmp_path))
        rows = raw.execute("SELECT COUNT(*) FROM schema_census").fetchone()[0]
        raw.close()
        assert rows == 0, "the refusal wrote a tamper record into the graph"


class TestTheWholeRouteIsCheckedBeforeAnythingRuns:
    def test_a_gap_two_links_along_is_refused_before_the_first_step(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """Checking only the first link asks "is there a step from here".

        A chain with a gap further along passes that, commits the steps before
        the gap, and then refuses saying nothing was changed. The file is left
        at a version this release will not open and the release that wrote it
        rejects as too new: bricked forward, one link later.
        """
        _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("PRAGMA user_version = 1")
        raw.commit()
        raw.close()
        before = _fingerprint(tmp_path)

        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 3)
        monkeypatch.setattr(
            _core, "_MIGRATIONS", {1: (2, _rebuild_claims_unchanged)},
        )
        with pytest.raises(MigrationError, match="no migration from 2"):
            _core.open_db(tmp_path)
        _assert_untouched(tmp_path, before)


class TestTheSchemaHasToStillResolve:
    def _narrow(self):
        from mareforma.db._schema_sql import claims_rebuild_sql

        return (
            tuple(c for c in _CLAIM_COLUMNS if c != "comparison_summary"),
            claims_rebuild_sql("claims_new").replace(
                "    comparison_summary TEXT,\n", "", 1,
            ),
        )

    def _rebuild(self, tmp_path: Path):
        keep, narrower = self._narrow()
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        try:
            with _core._upgrade_window(conn):
                _core._run_migration(
                    conn, to_version=2,
                    steps=lambda c: _core._rebuild_table(
                        c, table="claims", create_sql=narrower, columns=keep,
                        drops=("comparison_summary",),
                    ),
                )
        finally:
            conn.close()

    @pytest.mark.parametrize("ddl, why", [
        ("CREATE TRIGGER legacy_upper BEFORE DELETE ON claims BEGIN "
         "SELECT RAISE(ABORT, 'gone') "
         "WHERE OLD.COMPARISON_SUMMARY IS NOT NULL; END",
         "SQLite identifiers are case-insensitive"),
        ("CREATE TRIGGER legacy_elsewhere AFTER INSERT ON validators BEGIN "
         "SELECT RAISE(ABORT,'x') WHERE (SELECT comparison_summary FROM claims) "
         "IS NULL; END",
         "the object hangs off another table and is never dropped"),
        ("CREATE VIEW legacy_view AS SELECT claim_id, comparison_summary "
         "FROM claims",
         "a view is not a trigger and was never scanned"),
    ], ids=["case-different", "on-another-table", "a-view"])
    def test_an_object_left_naming_a_dropped_column_stops_it(
        self, tmp_path: Path, ddl: str, why: str,
    ) -> None:
        """A body SQLite resolves only when it runs, checked while it can roll back.

        Pattern-matching the column name against trigger text missed all three
        of these. Renaming the table aside and back with the legacy pragma off
        reparses the whole schema and asks SQLite the question instead.
        """
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute(ddl)
        raw.commit()
        raw.close()
        before = _fingerprint(tmp_path)

        with pytest.raises(MigrationError, match="no longer resolves"):
            self._rebuild(tmp_path)
        _assert_untouched(tmp_path, before)

    @pytest.mark.parametrize("ddl, why", [
        ("CREATE TRIGGER legacy_set AFTER INSERT ON validators BEGIN "
         "UPDATE claims SET comparison_summary = 'x'; END",
         "an UPDATE SET target is not resolved by the reparse"),
        ("CREATE TRIGGER legacy_cols AFTER INSERT ON validators BEGIN "
         "INSERT INTO claims(claim_id, comparison_summary) VALUES('a','b'); END",
         "an INSERT column list is not resolved by the reparse"),
    ], ids=["update-set-target", "insert-column-list"])
    def test_a_position_the_reparse_cannot_resolve_still_stops_it(
        self, tmp_path: Path, ddl: str, why: str,
    ) -> None:
        """The reparse is the primary check and it is not complete.

        Measured: it resolves view bodies, WHEN clauses, trigger SELECTs and
        NEW.column, and it does not resolve an UPDATE SET target or an INSERT
        column list. Either one committed, reported success, and killed the next
        write. The shipped schema already has this shape:
        contradiction_invalidates_older is UPDATE claims SET t_invalid on
        another table.
        """
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute(ddl)
        raw.commit()
        raw.close()
        before = _fingerprint(tmp_path)

        with pytest.raises(MigrationError, match="comparison_summary"):
            self._rebuild(tmp_path)
        _assert_untouched(tmp_path, before)

    def test_a_reference_this_migration_breaks_stops_it(
        self, tmp_path: Path,
    ) -> None:
        """A REFERENCES clause that resolved before the rebuild and not after.

        Reaching this needs the dropped column to be a usable parent key
        beforehand, and there is exactly one shape where that survives the
        rebuild: a table-level UNIQUE clause. Its index is implicit and carries
        no SQL, so the rebuild does not replay it, and a definition that drops
        the column and the clause together rebuilds cleanly and leaves the child
        dangling. A separate CREATE UNIQUE INDEX fails at the replay step
        instead, which is loud and needs no check of its own.

        Measured both ways before this test was written. The reparse does not
        see either, and neither does the write probe: a foreign key is not
        compiled into a statement's trigger program.
        """
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.executescript("""
            CREATE TABLE legacy_parent (
                id INTEGER PRIMARY KEY, key TEXT UNIQUE, keep TEXT
            );
            CREATE TABLE legacy_child (k TEXT REFERENCES legacy_parent(key));
        """)
        raw.commit()
        raw.close()
        before = _fingerprint(tmp_path)

        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        try:
            with pytest.raises(MigrationError, match="no longer resolves"):
                with _core._upgrade_window(conn):
                    _core._run_migration(
                        conn, to_version=2,
                        steps=lambda c: _core._rebuild_table(
                            c, table="legacy_parent",
                            create_sql=(
                                "CREATE TABLE legacy_parent_new ("
                                "id INTEGER PRIMARY KEY, keep TEXT)"
                            ),
                            columns=("id", "keep"), drops=("key",),
                        ),
                    )
        finally:
            conn.close()
        _assert_untouched(tmp_path, before)

    def test_a_reference_that_arrived_broken_is_not_blamed_on_the_migration(
        self, tmp_path: Path,
    ) -> None:
        """The fixture this replaces never tested what its name said.

        ``REFERENCES claims(comparison_summary)`` names a column that is neither
        a primary key nor unique, so ``PRAGMA foreign_key_check`` calls it a
        mismatch before any migration runs. The test passed because a different
        check, a name scan over the stored SQL, happened to catch the column
        name. With the scan gone the case is what it always was: a graph that
        arrived with a dangling reference, which the rebuild neither causes nor
        worsens, and which the child's writes were already failing on.
        """
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute(
            "CREATE TABLE legacy_ref (k TEXT REFERENCES claims(comparison_summary))"
        )
        raw.commit()
        raw.close()

        conn = sqlite3.connect(_db(tmp_path))
        try:
            with pytest.raises(sqlite3.Error, match="foreign key mismatch"):
                conn.execute("PRAGMA foreign_key_check(legacy_ref)").fetchall()
        finally:
            conn.close()

        self._rebuild(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        cols = {r[1] for r in conn.execute("PRAGMA table_info(claims)")}
        conn.close()
        assert "comparison_summary" not in cols, "the migration should have run"

    @_requires_drop_column
    def test_the_reference_check_asks_the_child_not_the_parent(
        self, tmp_path: Path,
    ) -> None:
        """Scoped to the parent it saw nothing, and its comment said otherwise.

        Pinned as its own measurement rather than left implicit in the test
        above, because that one passes as long as anything refuses, and for a
        while the thing refusing was a different check entirely.
        """
        keep, narrower = self._narrow()
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute(
            "CREATE TABLE legacy_ref (k TEXT REFERENCES claims(comparison_summary))"
        )
        raw.commit()
        raw.close()

        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute("BEGIN")
            conn.execute("ALTER TABLE claims DROP COLUMN comparison_summary")
            # The parent says nothing.
            assert conn.execute(
                "PRAGMA foreign_key_check(claims)"
            ).fetchall() == []
            # The child raises, which is why the scope is the children.
            with pytest.raises(sqlite3.Error, match="foreign key mismatch"):
                conn.execute("PRAGMA foreign_key_check(legacy_ref)").fetchall()
            with pytest.raises(MigrationError, match="legacy_ref"):
                _core._require_references_resolve(conn, "claims", {})
            conn.execute("ROLLBACK")
        finally:
            conn.close()

    def test_the_probe_refuses_a_name_collision_clearly(
        self, tmp_path: Path,
    ) -> None:
        """The old message blamed a trigger or view and offered no way forward."""
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("CREATE TABLE _mareforma_reparse_probe (x TEXT)")
        raw.commit()
        raw.close()

        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with pytest.raises(MigrationError, match="already in this graph"):
            with _core._upgrade_window(conn):
                _core._run_migration(
                    conn, to_version=2, steps=_rebuild_claims_unchanged,
                )
        conn.close()

    def test_the_check_does_not_depend_on_an_inherited_pragma(
        self, tmp_path: Path,
    ) -> None:
        """The check IS the reference rewriting, so it sets its own pragma.

        Inheriting it meant a connection that already had the legacy behaviour
        on turned the whole check into a no-op that passed everything.
        """
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("CREATE VIEW legacy_view AS SELECT comparison_summary FROM claims")
        raw.commit()
        raw.close()

        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA legacy_alter_table = ON")
        keep, narrower = self._narrow()
        with pytest.raises(MigrationError):
            with _core._upgrade_window(conn):
                _core._run_migration(
                    conn, to_version=2,
                    steps=lambda c: _core._rebuild_table(
                        c, table="claims", create_sql=narrower, columns=keep,
                        drops=("comparison_summary",),
                    ),
                )
        conn.close()

    def test_a_body_that_never_spells_the_column_still_stops_it(
        self, tmp_path: Path,
    ) -> None:
        """The shape no check that matches names can see.

        A trigger doing ``INSERT INTO archive SELECT * FROM claims`` reaches the
        dropped column through a star, so nothing in its text to match on. Every
        name in it still resolves, so the reparse passes. Only the arity moved.

        Measured before the write probe existed: the migration committed,
        reported success, and the next write to claims died with "table
        claims_archive has 36 columns but 35 values were supplied", on a graph
        the operator had just been told was migrated.
        """
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        cols = ", ".join(f'"{c}" TEXT' for c in _CLAIM_COLUMNS)
        raw.execute(f"CREATE TABLE claims_archive ({cols})")
        raw.execute(
            "CREATE TRIGGER claims_archive_tr AFTER INSERT ON claims BEGIN "
            "INSERT INTO claims_archive SELECT * FROM claims; END"
        )
        raw.commit()
        raw.close()
        before = _fingerprint(tmp_path)

        with pytest.raises(MigrationError, match="no longer compiles"):
            self._rebuild(tmp_path)
        _assert_untouched(tmp_path, before)

    def test_a_trigger_scoped_to_one_column_is_still_reached(
        self, tmp_path: Path,
    ) -> None:
        """``AFTER UPDATE OF`` is compiled only by a write to that column.

        Measured: an update naming one column reaches one trigger. So the probe
        names every column, or a guard declared on a column it did not happen to
        pick goes unchecked. The body here is an UPDATE SET target, which the
        reparse does not resolve, so this fails unless the probe reaches it.
        """
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute(
            "CREATE TRIGGER legacy_scoped AFTER UPDATE OF branch_id ON claims "
            "BEGIN UPDATE claims SET comparison_summary = 'x'; END"
        )
        raw.commit()
        raw.close()
        before = _fingerprint(tmp_path)

        with pytest.raises(MigrationError, match="comparison_summary"):
            self._rebuild(tmp_path)
        _assert_untouched(tmp_path, before)

    def test_a_trigger_only_another_trigger_fires_is_reached(
        self, tmp_path: Path,
    ) -> None:
        """Nothing writes to this table directly, so only the first trigger
        reaches it. Compiling a statement compiles its whole trigger program,
        including the programs of the triggers it fires."""
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("CREATE TABLE legacy_relay (x TEXT)")
        raw.execute(
            "CREATE TRIGGER legacy_first AFTER INSERT ON claims BEGIN "
            "INSERT INTO legacy_relay (x) VALUES ('a'); END"
        )
        raw.execute(
            "CREATE TRIGGER legacy_second AFTER INSERT ON legacy_relay BEGIN "
            "UPDATE claims SET comparison_summary = 'x'; END"
        )
        raw.commit()
        raw.close()
        before = _fingerprint(tmp_path)

        with pytest.raises(MigrationError, match="comparison_summary"):
            self._rebuild(tmp_path)
        _assert_untouched(tmp_path, before)

    def test_a_graph_that_arrived_broken_is_not_blamed_on_the_migration(
        self, tmp_path: Path,
    ) -> None:
        """A migration answers for what it changed.

        A hand-edited graph can carry a trigger that already names a column
        nothing has, on a table this rebuild does not touch. Refusing there
        would tell an operator the migration broke a graph it found broken,
        which is the class of false accusation this whole release exists to
        avoid making.
        """
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute(
            "CREATE TRIGGER legacy_already_broken AFTER INSERT ON validators "
            "BEGIN UPDATE validators SET no_such_column = 'x'; END"
        )
        raw.commit()
        raw.close()

        self._rebuild(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        cols = {r[1] for r in conn.execute("PRAGMA table_info(claims)")}
        conn.close()
        assert "comparison_summary" not in cols, "the migration should have run"

    def test_a_second_break_on_an_already_broken_table_is_still_reported(
        self, tmp_path: Path,
    ) -> None:
        """The masking case in the check that keeps the migration honest.

        The probe reports one entry per table, so if it stopped at the first
        statement that would not compile, a table that arrived broken on its
        insert path and that the rebuild then broke on its delete path would
        report the same first error both times. The comparison would read the
        two as equal and let the new break through, on a table already excused
        from the report.
        """
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute(
            "CREATE TRIGGER legacy_broken_insert AFTER INSERT ON validators "
            "BEGIN UPDATE validators SET no_such_column = 'x'; END"
        )
        raw.execute(
            "CREATE TRIGGER legacy_breaks_later BEFORE DELETE ON validators "
            "BEGIN UPDATE claims SET comparison_summary = 'x'; END"
        )
        raw.commit()
        raw.close()
        before = _fingerprint(tmp_path)

        with pytest.raises(MigrationError, match="comparison_summary"):
            self._rebuild(tmp_path)
        _assert_untouched(tmp_path, before)

    def test_a_write_guard_is_compiled_and_never_fired(
        self, tmp_path: Path,
    ) -> None:
        """The probe prepares statements and runs none of them.

        Every append-only table here refuses deletes with RAISE(ABORT). A probe
        that actually wrote would trip its own guards and refuse every rebuild,
        and one that wrote to claims would have to satisfy thirty-six columns of
        CHECK constraints to get there.
        """
        _populated(tmp_path, claims=3)
        before = _fingerprint(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        try:
            assert _core._uncompilable_writes(conn, "claims") == {}
            assert "claims" in _core._canary_targets(conn, "claims")
        finally:
            conn.close()
        # The probe wrote nothing: same rows, same chain, same guards.
        _assert_untouched(tmp_path, before)

    def test_a_column_named_only_inside_a_message_does_not_block(
        self, tmp_path: Path,
    ) -> None:
        """The false positive the regex had, and the advice it gave.

        The word appearing in an error string is not a reference. Blocking on it
        offered one remedy, removing an append-only write guard, which is the
        class of advice this whole path exists to stop giving.
        """
        _populated(tmp_path, claims=3)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute(
            "CREATE TRIGGER legacy_message_guard BEFORE DELETE ON claims BEGIN "
            "SELECT RAISE(ABORT, 'mareforma: comparison_summary is locked'); END"
        )
        raw.commit()
        raw.close()

        self._rebuild(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        live = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'"
            )
        }
        cols = {r[1] for r in conn.execute("PRAGMA table_info(claims)")}
        conn.close()
        assert "legacy_message_guard" in live
        assert "comparison_summary" not in cols


class TestTheStepAndTheDefinitionHaveToAgree:
    def test_a_declared_drop_the_definition_still_carries_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """The dangerous shape: the column survives and the copy skips it.

        Every row silently loses its value, or takes a DEFAULT so nothing even
        looks empty, and the exact column-set check on the open path compares
        set against set, sees no difference and reports nothing.
        """
        from mareforma.db._schema_sql import claims_rebuild_sql

        _populated(tmp_path, claims=3)
        keep = tuple(c for c in _CLAIM_COLUMNS if c != "comparison_summary")
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with pytest.raises(MigrationError, match="still has those columns"):
            with _core._upgrade_window(conn):
                _core._rebuild_table(
                    conn, table="claims",
                    create_sql=claims_rebuild_sql("claims_new"),
                    columns=keep, drops=("comparison_summary",),
                )
        conn.close()

    def test_a_drop_for_a_column_that_was_never_there_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """The step and the table disagree, so the rest of the step is suspect."""
        from mareforma.db._schema_sql import claims_rebuild_sql

        _populated(tmp_path, claims=3)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with pytest.raises(MigrationError, match="does not have"):
            with _core._upgrade_window(conn):
                _core._rebuild_table(
                    conn, table="claims",
                    create_sql=claims_rebuild_sql("claims_new"),
                    columns=_CLAIM_COLUMNS, drops=("no_such_column",),
                )
        conn.close()

    def test_the_copy_carries_rowid_so_the_chain_tip_does_not_move(
        self, tmp_path: Path,
    ) -> None:
        """The claim chain's tip is read in rowid order.

        A copy that let SQLite choose its own order could move the tip with
        nothing raising, so the copy names rowid.
        """
        _populated(tmp_path, claims=6)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        before = [
            (r["rowid"], r["claim_id"]) for r in conn.execute(
                "SELECT rowid, claim_id FROM claims ORDER BY rowid"
            )
        ]
        tip_before = conn.execute(
            "SELECT prev_hash FROM claims ORDER BY rowid DESC LIMIT 1"
        ).fetchone()[0]
        with _core._upgrade_window(conn):
            _core._run_migration(
                conn, to_version=1, steps=_rebuild_claims_unchanged,
            )
        after = [
            (r["rowid"], r["claim_id"]) for r in conn.execute(
                "SELECT rowid, claim_id FROM claims ORDER BY rowid"
            )
        ]
        tip_after = conn.execute(
            "SELECT prev_hash FROM claims ORDER BY rowid DESC LIMIT 1"
        ).fetchone()[0]
        conn.close()
        assert after == before
        assert tip_after == tip_before


class TestTheGate:
    def test_the_rebuild_refuses_outside_the_upgrade_path(
        self, tmp_path: Path,
    ) -> None:
        """It drops every guard on claims and runs with foreign keys off.

        Reachable from a versioned migration and from nowhere else, or it is a
        laundering primitive with a docstring.
        """
        _populated(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with pytest.raises(MigrationError, match="outside the upgrade path"):
            _rebuild_claims_unchanged(conn)
        conn.close()

    def test_the_window_closes_even_when_the_migration_fails(
        self, tmp_path: Path,
    ) -> None:
        _populated(tmp_path)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        conn.set_authorizer(
            lambda a, a1, a2, d, t:
            sqlite3.SQLITE_DENY
            if (a == sqlite3.SQLITE_DROP_TABLE and a1 == "claims")
            else sqlite3.SQLITE_OK
        )
        with pytest.raises(MigrationError):
            with _core._upgrade_window(conn):
                _core._run_migration(
                    conn, to_version=2, steps=_rebuild_claims_unchanged,
                )
        # An authorizer that allows everything, rather than None: removing one
        # by passing None only works from 3.11, and this package supports 3.10,
        # where the read below comes back "not authorized" instead.
        conn.set_authorizer(lambda *_: sqlite3.SQLITE_OK)
        assert not _core._upgrade_window_open(conn)
        conn.close()


class TestTheColumnList:
    def test_a_narrower_target_copies_correctly(self, tmp_path: Path) -> None:
        """The shape a column-dropping migration needs, proven on this one.

        ``INSERT INTO new SELECT * FROM old`` works while the schemas match and
        stops the moment they do not. If the rebuild shipped that form, the
        release that drops a column would have to rewrite the copy step, and
        every crash test run against it would have proved the wrong thing.

        So drop a column here and copy into the narrower table. A positional
        copy cannot do this, and the values have to land under the names they
        started with rather than one column across.
        """
        from mareforma.db._schema_sql import claims_rebuild_sql

        _populated(tmp_path, claims=4)
        keep = tuple(c for c in _CLAIM_COLUMNS if c != "comparison_summary")
        narrower = claims_rebuild_sql("claims_new").replace(
            "    comparison_summary TEXT,\n", "", 1,
        )
        assert "comparison_summary" not in narrower

        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        before = {
            r["claim_id"]: (r["text"], r["statement_cid"], r["created_at"])
            for r in conn.execute(
                "SELECT claim_id, text, statement_cid, created_at FROM claims"
            )
        }
        with _core._upgrade_window(conn):
            _core._run_migration(
                conn, to_version=2,
                steps=lambda c: _core._rebuild_table(
                    c, table="claims", create_sql=narrower, columns=keep,
                    drops=("comparison_summary",),
                ),
            )
        built = tuple(r[1] for r in conn.execute("PRAGMA table_info(claims)"))
        after = {
            r["claim_id"]: (r["text"], r["statement_cid"], r["created_at"])
            for r in conn.execute(
                "SELECT claim_id, text, statement_cid, created_at FROM claims"
            )
        }
        conn.close()

        assert "comparison_summary" not in built
        assert len(built) == len(_CLAIM_COLUMNS) - 1
        assert after == before, "values did not land under their own names"

    def test_the_rebuilt_definition_matches_the_column_list(self) -> None:
        """A definition and a list that disagree would copy into the wrong table."""
        from mareforma.db._schema_sql import claims_rebuild_sql

        conn = sqlite3.connect(":memory:")
        conn.execute(claims_rebuild_sql("claims_new"))
        built = tuple(
            r[1] for r in conn.execute("PRAGMA table_info(claims_new)")
        )
        conn.close()
        assert set(built) == set(_CLAIM_COLUMNS)
        assert len(built) == len(_CLAIM_COLUMNS)


class TestTheVersionGate:
    def test_a_newer_graph_is_refused_without_the_delete_advice(
        self, tmp_path: Path,
    ) -> None:
        """A file from a later release is not a broken file.

        The old message told the operator to delete it and warned the chain
        could not be reconstructed, which is advice to destroy an intact graph
        because the reader is behind.
        """
        key = _populated(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("PRAGMA user_version = 99")
        raw.commit()
        raw.close()

        with pytest.raises(Exception) as caught:
            with mareforma.open(tmp_path, key_path=key):
                pass
        message = str(caught.value).lower()
        assert "99" in message
        assert "do not delete" in message
        assert "upgrade mareforma" in message
        assert "start fresh" not in message

    def test_a_version_with_no_route_is_refused_and_changes_nothing(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        _populated(tmp_path)
        before = _fingerprint(tmp_path)
        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 5)
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        with pytest.raises(MigrationError, match="no migration"):
            _core._migrate_to_current(conn, 1)
        conn.close()
        _assert_untouched(tmp_path, before)

    def test_a_registered_route_runs_and_moves_the_version(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """The registry is empty in this release, so wire one to prove it walks.

        What a later release adds is an entry. If this stops working, that
        release finds out here rather than on a user's graph.
        """
        _populated(tmp_path)
        monkeypatch.setattr(_core, "_SCHEMA_VERSION", 2)
        monkeypatch.setattr(
            _core, "_MIGRATIONS", {1: (2, _rebuild_claims_unchanged)},
        )
        conn = sqlite3.connect(_db(tmp_path))
        conn.row_factory = sqlite3.Row
        assert _core._migrate_to_current(conn, 1) == 2
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 2
        conn.close()

    def test_every_registered_route_ends_at_the_current_version(self) -> None:
        """The registry is walked, not trusted.

        The previous release shipped the machinery with an empty registry so
        the rebuild was proven before anything irreversible used it. What that
        release bought is that adding a route is an entry rather than a
        rewrite, so what is checked here is the entry: every version below the
        current one has a step, and the chain arrives exactly at the current
        one rather than short of it or past it.
        """
        assert _core._MIGRATIONS, "no route out of any earlier version"
        for start in range(1, _core._SCHEMA_VERSION):
            assert _core._plan_migration(start)[-1] == _core._SCHEMA_VERSION
