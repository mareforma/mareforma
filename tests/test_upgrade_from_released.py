"""Upgrading a graph a released mareforma wrote, through the ordinary open.

The migration runner's own suite reaches the rebuild on a raw connection, or
through an open with the registry replaced. Both prove the machinery and
neither proves the registry, so what is checked here is the whole thing as a
user meets it: install the new version, open the project, and the file that
was on disk is the file that comes back.

The source versions are named rather than inferred. A graph carries one number
and every release that shares it wrote a different file: the earlier two
predate the tables the additive script now creates, and the later one has them
with rows in them. A migration that only ever met the last of those would be
untested against the graphs most people are holding.
"""

from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path
from unittest import mock

import pytest

import mareforma
from mareforma.db import core as _core
from mareforma.db._schema_sql import _ALL_EXPECTED_TRIGGERS
from mareforma.db.core import (
    _MIGRATIONS,
    _SCHEMA_VERSION,
    MigrationError,
    verify_claim_signatures,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent

# Every version that wrote a graph this release must carry forward, with the
# commit that is that version. Pinned to commits rather than tags so the
# fixture is the code that shipped, not whatever a moving ref points at now.
# Every release the migration claims to carry, by the commit its tag names.
# 0.3.13 was missing and 0.3.14 pointed at the last commit of the release branch
# rather than the merge the tag is on. The package trees happen to match there,
# which is exactly why it went unnoticed: a source that is right by coincidence
# is a source nobody can check at a glance.
_SOURCES = (
    ("0.3.11", "66b73d4"),
    ("0.3.12", "94e0fc7"),
    ("0.3.13", "8cfe69f"),
    ("0.3.14", "1bcff54"),
)


def _available(commit: str) -> bool:
    try:
        subprocess.run(
            ["git", "-C", str(_REPO_ROOT), "rev-parse", "--verify",
             f"{commit}^{{commit}}"],
            capture_output=True, check=True,
        )
        return True
    except (OSError, subprocess.CalledProcessError):
        return False


def _db(root: Path) -> Path:
    return root / ".mareforma" / "graph.db"


_BUILD = textwrap.dedent(
    """
    import sys, warnings
    warnings.filterwarnings("ignore")
    sys.path.insert(0, sys.argv[1])
    from pathlib import Path
    import mareforma
    assert mareforma.__version__.startswith(sys.argv[3]), mareforma.__version__
    from mareforma import signing
    root = Path(sys.argv[2])
    signing.bootstrap_key(root / "root.key")
    with mareforma.open(root, key_path=root / "root.key") as g:
        for i in range(6):
            g.assert_claim("claim %d" % i, generated_by="run%d" % i)
    """
)


def _graph_written_by(version: str, commit: str, work: Path, root: Path) -> None:
    """Lay a graph down with the released package, in its own interpreter."""
    pkg = work / version
    pkg.mkdir(parents=True, exist_ok=True)
    archive = subprocess.run(
        ["git", "-C", str(_REPO_ROOT), "archive", commit, "mareforma"],
        capture_output=True, check=True,
    ).stdout
    subprocess.run(["tar", "-x", "-C", str(pkg)], input=archive, check=True)

    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    proc = subprocess.run(
        [sys.executable, "-c", _BUILD, str(pkg), str(root), version],
        capture_output=True, text=True, env=env,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"the {version} subprocess failed to write a graph:\n{proc.stderr}"
        )


# Every table the rebuild could orphan. It runs with foreign keys off, so a
# step that deleted or stranded a child row breaks nothing at the time and
# nothing later counts them.
_CHILD_TABLES = (
    "validators", "verdict_chain", "grounding_attestations",
    "contradiction_verdicts", "replication_verdicts", "rekor_inclusions",
    "propositions", "predictions", "findings", "evidence_lines",
    "plan_retirements", "schema_census", "schema_guards_seen",
)


def _state(root: Path) -> dict:
    """What has to be the same on both sides of the upgrade.

    Every column, not a chosen few. Comparing four of the thirty-six let a copy
    that shifted, blanked or defaulted any of the other thirty-two pass every
    assertion in this file, and a positional copy across a narrower target is
    the one way the rebuild can corrupt a graph without raising.

    The constraints and the indexes are compared for the same reason. A
    replacement definition that dropped a CHECK leaves the same columns, the
    same triggers and the same rows, and both integrity pragmas call it clean;
    a rebuild that failed to replay `idx_claims_prev_hash`, which is UNIQUE,
    leaves a graph where two claims can share a chain link.

    Trigger bodies rather than trigger names, because a guard recreated with a
    body that does nothing has the right name, and the product's own census
    compares text for exactly that reason.
    """
    conn = sqlite3.connect(_db(root))
    conn.row_factory = sqlite3.Row
    try:
        return {
            "user_version": conn.execute("PRAGMA user_version").fetchone()[0],
            "claims": conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0],
            # The columns actually present, so this still answers on a table a
            # narrowing test has just taken one out of.
            # Keyed by column name, not positional. A narrowing upgrade
            # removes a column, so two positional tuples differ by construction
            # and say nothing about whether the columns that survived were
            # carried faithfully, which is the only question worth asking.
            "rows": [
                dict(r) for r in conn.execute(
                    "SELECT {} FROM claims ORDER BY rowid".format(
                        ", ".join(
                            c[1] for c in conn.execute(
                                "PRAGMA table_info(claims)"
                            )
                        )
                    )
                )
            ],
            "definition": conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' "
                "AND name = 'claims'"
            ).fetchone()[0],
            "indexes": {
                (r["name"], r["sql"]) for r in conn.execute(
                    "SELECT name, sql FROM sqlite_master WHERE type = 'index' "
                    "AND tbl_name = 'claims'"
                )
            },
            "triggers": {
                (r["name"], r["sql"]) for r in conn.execute(
                    "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
                )
            },
            "children": {
                table: conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                for table in _CHILD_TABLES
                if conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                    "AND name = ?", (table,)
                ).fetchone()
            },
            "stale": [
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE sql LIKE '%claims_old%' OR sql LIKE '%claims_new%'"
                )
            ],
        }
    finally:
        conn.close()



def _rows_agree_on_surviving_columns(before: list, after: list) -> None:
    """Every column present on both sides holds the value it held.

    The columns a migration drops are not compared, because they are gone on
    purpose. Everything else has to come through untouched, which is the whole
    claim a rebuild makes.
    """
    assert len(before) == len(after), "the upgrade changed the row count"
    for old_row, new_row in zip(before, after):
        shared = set(old_row) & set(new_row)
        assert shared, "the two sides share no columns at all"
        for column in sorted(shared):
            assert old_row[column] == new_row[column], column


def _definition_without(column: str) -> str:
    """The claims definition with *column* and the constraints naming it gone.

    A migration that narrows the table authors its replacement, for the reason
    the rebuild's docstring gives: filtering a column out of authored DDL means
    parsing it, and a parser that mishandles a CHECK writes a table accepting
    what the old one refused. This is a test editing known lines to measure the
    copy step, not a pattern for a migration to copy.
    """
    lines = _core.claims_rebuild_sql("claims_new").splitlines()
    # Whole word, because _CLAIM_COLUMNS holds both signature_bundle and
    # original_signature_bundle, and a substring match would silently take
    # the second when asked for the first.
    names = re.compile(rf"\b{re.escape(column)}\b")
    keep, dropped_any = [], False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("--"):
            keep.append(line)
            continue
        if names.search(stripped):
            dropped_any = True
            continue
        keep.append(line)
    if dropped_any:
        # A removal at the end leaves the constraint above it ending in a comma
        # with nothing following, which SQLite refuses.
        for i in range(len(keep) - 1, -1, -1):
            body = keep[i].strip()
            if body and not body.startswith("--") and body != ")":
                keep[i] = keep[i].rstrip().rstrip(",")
                break
    return "\n".join(keep)



def _rebuild_claims_to_the_current_shape(conn) -> None:
    """Rebuild ``claims`` under the definition this release has.

    Stands in for the package's no-op rebuild, which is gone: it existed to
    prove the machinery on real graphs before a narrowing step relied on it,
    and that release never published. Reads the live column list, carries what
    the current definition can hold, declares the rest as dropped, and retires
    the objects that read them, which is what any narrowing step must do.
    """
    live = {r[1] for r in conn.execute("PRAGMA table_info(claims)")}
    keep = tuple(c for c in _core._CLAIM_COLUMNS if c in live)
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
        drops=tuple(sorted(going)),
    )


class TestNarrowingTheTable:
    """The shape a later release uses, run against a graph a release wrote.

    The rebuild ships taking its column list as a parameter so that a narrowing
    reuses it rather than rewriting the copy step. That is only worth anything
    if a narrowing has actually been carried, and until now none had.
    """

    # The last named source, rather than a third copy of the pin: the two
    # drifted apart the moment one was updated and the other was not.
    _SOURCE = _SOURCES[-1]

    def _upgraded_project(self, tmp_path: Path) -> Path:
        version, commit = self._SOURCE
        if not _available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_written_by(version, commit, tmp_path / "work", project)
        with mareforma.open(project, key_path=project / "root.key"):
            pass
        return project

    def _narrow(self, project: Path, column: str) -> None:
        conn = sqlite3.connect(_db(project))
        try:
            keep = tuple(c for c in _core._CLAIM_COLUMNS if c != column)
            assert len(keep) == len(_core._CLAIM_COLUMNS) - 1
            with _core._upgrade_window(conn):
                _core._run_migration(
                    conn, to_version=_core._SCHEMA_VERSION + 1,
                    steps=lambda c: _core._rebuild_table(
                        c, table="claims",
                        create_sql=_definition_without(column),
                        columns=keep, drops=(column,),
                    ),
                )
        finally:
            conn.close()

    def test_a_column_nothing_else_names_is_carried_away(
        self, tmp_path: Path,
    ) -> None:
        """Every row keeps the identity it was signed under.

        The copy names its columns on both sides, so this is the check that the
        naming holds: a positional copy across a narrower target would shift
        every value one place and leave a table that still looks well formed.
        """
        project = self._upgraded_project(tmp_path)
        before = _state(project)

        self._narrow(project, "comparison_summary")

        conn = sqlite3.connect(_db(project))
        conn.row_factory = sqlite3.Row
        try:
            columns = {r[1] for r in conn.execute("PRAGMA table_info(claims)")}
            kept = ", ".join(
                c[1] for c in conn.execute("PRAGMA table_info(claims)")
            )
            conn.row_factory = sqlite3.Row
            after = [
                dict(r) for r in conn.execute(
                    f"SELECT {kept} FROM claims ORDER BY rowid"
                )
            ]
            stale = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE sql LIKE '%claims_old%' "
                "OR sql LIKE '%claims_new%'"
            )]
        finally:
            conn.close()

        assert "comparison_summary" not in columns
        assert columns == set(_core._CLAIM_COLUMNS) - {"comparison_summary"}
        # Compared on the columns that had to survive, not on the one that was
        # meant to go.
        _rows_agree_on_surviving_columns(before["rows"], after)
        assert stale == []

    def test_a_column_the_schema_still_depends_on_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """The trust ladder's column, dropped without removing what reads it.

        Three triggers, an index and a table-level constraint name that column.
        A rebuild that carried this would produce a graph whose every write
        fails, and both integrity pragmas would call it healthy.

        **What refuses it is the index, not the verification.** Traced rather
        than assumed: the step dies putting `idx_claims_support_level` back,
        before the three checks that run after. Deleting all three leaves this
        green, which is why the case below exists as well. Recorded here so
        nobody reads this test as evidence for a layer it never reaches.

        The half that matters as much: the refusal costs nothing. The step is
        one transaction, so the table, its rows and its version are what they
        were, which is why nothing on this path tells anyone to delete a file.
        """
        project = self._upgraded_project(tmp_path)
        before = _state(project)

        with pytest.raises(_core.MigrationError) as caught:
            self._narrow(project, "generated_by")
        assert "generated_by" in str(caught.value)

        assert _state(project) == before
        with mareforma.open(project, key_path=project / "root.key") as graph:
            assert len(graph.query(include_invalidated=True)) == 6
            graph.assert_claim("written after the refusal", generated_by="after")

    def test_a_column_only_a_trigger_names_is_refused_by_the_verification(
        self, tmp_path: Path,
    ) -> None:
        """The layer built for this, reached at last.

        A column with no index behind it gets past the step that happens to
        catch the ladder's column, and lands on the check that asks SQLite
        whether the schema it just built still resolves. A search trigger reads
        the dropped column through ``NEW``, so it does not, and the rebuild
        refuses rather than leaving a table nothing can write to.

        Held to the mechanism, not to the column name appearing somewhere in
        the message, so this cannot go green on a step that failed earlier for
        an unrelated reason.
        """
        project = self._upgraded_project(tmp_path)
        before = _state(project)

        with pytest.raises(_core.MigrationError) as caught:
            self._narrow(project, "text")
        assert "the schema no longer resolves" in str(caught.value)

        assert _state(project) == before
        with mareforma.open(project, key_path=project / "root.key") as graph:
            assert len(graph.query(include_invalidated=True)) == 6
            graph.assert_claim("written after the refusal", generated_by="after")


@pytest.mark.parametrize(
    "version, commit", _SOURCES, ids=[s[0] for s in _SOURCES],
)
class TestTheUpgradeAUserPerforms:

    def test_the_graph_opens_and_arrives_at_the_current_version(
        self, tmp_path: Path, version: str, commit: str,
    ) -> None:
        """No monkeypatching. The registry that ships is the one that runs."""
        if not _available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_written_by(version, commit, tmp_path / "work", project)

        before = _state(project)
        assert before["user_version"] < _core._SCHEMA_VERSION, (
            f"{version} wrote user_version={before['user_version']}, so this "
            "does not exercise an upgrade at all"
        )

        with mareforma.open(project, key_path=project / "root.key") as graph:
            assert len(graph.query(include_invalidated=True)) == 6

        after = _state(project)
        assert after["user_version"] == _core._SCHEMA_VERSION

    def test_every_signature_and_every_chain_link_is_the_one_it_was(
        self, tmp_path: Path, version: str, commit: str,
    ) -> None:
        """The thing a bad migration destroys, and the reason the refusal it
        replaces told people to delete the file.

        Row counts surviving is not the property. The chain hashes, the content
        identifiers and the signature envelopes have to be byte for byte what
        the older release wrote, because nothing can put them back.
        """
        if not _available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_written_by(version, commit, tmp_path / "work", project)

        before = _state(project)
        with mareforma.open(project, key_path=project / "root.key"):
            pass
        after = _state(project)

        assert after["claims"] == before["claims"] == 6
        _rows_agree_on_surviving_columns(before["rows"], after["rows"])
        # A table the additive script creates on the way in is expected to
        # appear. A row disappearing from one that was already there is not,
        # and the rebuild runs with foreign keys off, so nothing else counts.
        #
        # Two of them are the schema's record of its own guards rather than
        # evidence, and they are supposed to move: a release that adds a guard
        # to a table every existing graph already has is recorded as missing on
        # the first open, and the graph then carries it in its seen set. Held to
        # "never loses a row" instead, which is the property that matters for a
        # record whose whole job is that nothing forgets.
        _BOOKKEEPING = ("schema_census", "schema_guards_seen")
        for table, count in before["children"].items():
            if table in _BOOKKEEPING:
                assert after["children"][table] >= count, (
                    f"{table} lost rows across the upgrade, {count} to "
                    f"{after['children'][table]}"
                )
                continue
            assert after["children"][table] == count, (
                f"{table} went from {count} to {after['children'][table]} "
                "across the upgrade"
            )

        # Bytes matching is not the property. The claim has to still be
        # provable against the row it now sits in.
        conn = sqlite3.connect(_db(project))
        conn.row_factory = sqlite3.Row
        try:
            for row in conn.execute("SELECT * FROM claims"):
                ok, why = verify_claim_signatures(conn, dict(row))
                assert ok, f"{row['claim_id']} stopped verifying: {why}"
        finally:
            conn.close()

    def test_the_upgraded_graph_carries_every_guard(
        self, tmp_path: Path, version: str, commit: str,
    ) -> None:
        """A rebuild drops the table and takes its triggers with it.

        Checked after a successful migration rather than only after a failed
        one: the recreate is a step that can be skipped without anything else
        going wrong until somebody tries to write.
        """
        if not _available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_written_by(version, commit, tmp_path / "work", project)

        with mareforma.open(project, key_path=project / "root.key"):
            pass

        after = _state(project)
        assert set(_ALL_EXPECTED_TRIGGERS) <= {
            name for name, _ in after["triggers"]
        }
        assert after["stale"] == [], (
            "the schema still names the rebuild's temporary table, so a "
            "reference was rewritten to point at a table that is gone"
        )

    def test_the_search_index_is_neither_doubled_nor_emptied(
        self, tmp_path: Path, version: str, commit: str,
    ) -> None:
        """The failure the integrity pragmas call healthy.

        The rebuild drops the table and puts its triggers back. Put the search
        triggers back before the copy instead of after and every row is indexed
        twice, and both pragmas still say the file is fine; leave them off and
        the index is empty while every claim is still there. Neither shows up
        as a row count on ``claims``, so both are counted here directly, and a
        search is run because a matching row count with a broken index is a
        third way to be wrong.
        """
        if not _available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_written_by(version, commit, tmp_path / "work", project)

        with mareforma.open(project, key_path=project / "root.key") as graph:
            assert len(graph.search("claim")) == 6

        conn = sqlite3.connect(_db(project))
        try:
            claims = conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
            indexed = conn.execute(
                "SELECT COUNT(*) FROM claims_fts"
            ).fetchone()[0]
            sync = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger' "
                    "AND name LIKE '%fts%'"
                )
            }
        finally:
            conn.close()

        assert indexed == claims == 6, (
            f"{claims} claims and {indexed} indexed rows after the upgrade"
        )
        expected = {n for n in _ALL_EXPECTED_TRIGGERS if "fts" in n}
        assert sync == expected, (
            f"search sync triggers after the upgrade: {sync}, wanted {expected}"
        )

    def test_the_upgraded_graph_still_takes_writes(
        self, tmp_path: Path, version: str, commit: str,
    ) -> None:
        """The check the two integrity pragmas cannot make.

        A rename can rewrite a foreign key to name the table that was dropped,
        and both ``foreign_key_check`` and ``integrity_check`` come back clean
        on a graph that can no longer record anything. Writing is what tells
        them apart.
        """
        if not _available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_written_by(version, commit, tmp_path / "work", project)

        with mareforma.open(project, key_path=project / "root.key") as graph:
            new_id = graph.assert_claim("written after the upgrade",
                                        generated_by="after")
            assert graph.get_claim(new_id) is not None
            assert len(graph.query(include_invalidated=True)) == 7

        conn = sqlite3.connect(_db(project))
        try:
            for pragma in ("foreign_key_check", "integrity_check"):
                rows = conn.execute(f"PRAGMA {pragma}").fetchall()
                assert rows in ([], [("ok",)]), f"{pragma}: {rows}"
        finally:
            conn.close()

    def test_opening_twice_migrates_once_and_then_holds_still(
        self, tmp_path: Path, version: str, commit: str,
    ) -> None:
        """A migration that runs on every open is a rebuild under a write lock
        every time somebody reads, and the version would not show it."""
        if not _available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_written_by(version, commit, tmp_path / "work", project)

        with mareforma.open(project, key_path=project / "root.key"):
            pass
        first = _state(project)
        conn = sqlite3.connect(_db(project))
        cookie = conn.execute("PRAGMA schema_version").fetchone()[0]
        conn.close()

        with mareforma.open(project, key_path=project / "root.key"):
            pass
        conn = sqlite3.connect(_db(project))
        cookie_again = conn.execute("PRAGMA schema_version").fetchone()[0]
        conn.close()

        assert _state(project) == first
        assert cookie_again == cookie, (
            "the second open changed the schema, so something is rebuilding "
            "or reconciling on every open"
        )


class TestWhatAFailedUpgradeTellsTheOperator:
    """Every message on this path is read by someone whose graph will not open.

    The failure mode this machinery replaced told them to delete the file that
    holds the chain and every signature. So the sentences are held to the same
    standard as the code: each one has to name a cure that works, and none may
    suggest deleting anything.
    """

    def _project(self, tmp_path: Path) -> Path:
        version, commit = _SOURCES[-1]
        if not _available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_written_by(version, commit, tmp_path / "work", project)
        with mareforma.open(project, key_path=project / "root.key"):
            pass
        return project

    def test_a_taken_scratch_name_names_the_cure(self, tmp_path: Path) -> None:
        """A graph holding a table called `claims_new` opened on every release
        before this one, so meeting it for the first time here must not leave
        the operator with a file nothing will open and no idea why."""
        project = self._project(tmp_path)
        conn = sqlite3.connect(_db(project))
        conn.execute("CREATE TABLE claims_new (x INTEGER)")
        conn.commit()
        try:
            with pytest.raises(_core.MigrationError) as caught:
                with _core._upgrade_window(conn):
                    _core._run_migration(
                        conn, to_version=_core._SCHEMA_VERSION + 1,
                        steps=_rebuild_claims_to_the_current_shape,
                    )
        finally:
            conn.close()

        said = str(caught.value)
        assert "claims_new" in said
        assert "Rename or drop" in said
        # The only mention of deleting on this path is the instruction not to.
        assert "Do not delete it" in said
        assert "delete .mareforma" not in said
        assert "start fresh" not in said

    def test_a_column_this_release_does_not_know_says_upgrade(
        self, tmp_path: Path,
    ) -> None:
        """The remedy the column-set check used to give. That check now runs
        after the migration, so the migration has to give it instead."""
        project = self._project(tmp_path)
        conn = sqlite3.connect(_db(project))
        conn.execute("ALTER TABLE claims ADD COLUMN vendor_note TEXT")
        conn.commit()

        def _declares_nothing(c) -> None:
            """A step that names its columns and no drops, which is the shape
            every step has until one deliberately narrows. The adaptive helper
            above declares whatever it finds, so it would swallow the column
            this test plants and prove nothing."""
            _core._rebuild_table(
                c, table="claims",
                create_sql=_core.claims_rebuild_sql("claims_new"),
                columns=_core._CLAIM_COLUMNS,
            )

        try:
            with pytest.raises(_core.MigrationError) as caught:
                with _core._upgrade_window(conn):
                    _core._run_migration(
                        conn, to_version=_core._SCHEMA_VERSION + 1,
                        steps=_declares_nothing,
                    )
        finally:
            conn.close()

        said = str(caught.value)
        assert "vendor_note" in said
        assert "upgrade the package" in said

    def test_losing_a_race_says_to_try_again(self, tmp_path: Path) -> None:
        """Contention is not a fault in the file.

        Without its own sentence the loser waits out the busy timeout and is
        handed a failure that reads like a defect and never mentions retrying,
        which is the one thing that would help.
        """
        project = self._project(tmp_path)
        holder = sqlite3.connect(_db(project))
        holder.execute("BEGIN IMMEDIATE")
        loser = sqlite3.connect(_db(project), timeout=0.2)
        try:
            with pytest.raises(_core.MigrationError) as caught:
                with _core._upgrade_window(loser):
                    _core._run_migration(
                        loser, to_version=_core._SCHEMA_VERSION + 1,
                        steps=_rebuild_claims_to_the_current_shape,
                    )
        finally:
            loser.close()
            holder.rollback()
            holder.close()

        said = str(caught.value)
        assert "another process is upgrading this graph" in said
        assert "Open it again in a moment" in said
        assert "Do not delete graph.db" in said
        assert "start fresh" not in said


class TestTheVersionStampIsEarned:

    def test_a_zeroed_version_migrates_rather_than_being_stamped(
        self, tmp_path: Path,
    ) -> None:
        """One PRAGMA must not buy the current version for free.

        The fresh-database branch ends by stamping the current version, so a
        populated graph whose version was zeroed came out marked as having had
        every migration with none of them run. That was harmless only while the
        one registered step changed nothing, and the stamp is now the only
        evidence a step ever ran.

        Checked by the rebuild's own fingerprint rather than by the number it
        writes, because the number is exactly what a stamp would have produced.
        """
        version, commit = _SOURCES[0]
        if not _available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_written_by(version, commit, tmp_path / "work", project)

        conn = sqlite3.connect(_db(project))
        conn.execute("PRAGMA user_version = 0")
        conn.commit()
        conn.close()

        with mareforma.open(project, key_path=project / "root.key") as graph:
            assert len(graph.query(include_invalidated=True)) == 6

        after = _state(project)
        assert after["user_version"] == _core._SCHEMA_VERSION
        # SQLite quotes the table name when it rewrites references during the
        # rename, so this is present only on a table the rebuild actually built.
        assert after["definition"].startswith('CREATE TABLE "claims"'), (
            "the version moved without the step running"
        )


class TestAChainOfSteps:
    """A route longer than one step, which no release has ever shipped.

    The runner was written for a chain and refuses a route with a gap before it
    commits anything, but every real route so far has been one hop. The
    interesting failure lives in the chain: the steps are separate
    transactions, so a stop partway leaves a graph at neither end of the route,
    and the release that wrote the file refuses the version it has reached.

    The second step is synthetic, because this release has only one. That is
    the point of testing it here rather than waiting: the day a real second
    step is registered is a bad day to discover the runner cannot resume.
    """

    @pytest.mark.skipif(
        not _available(_SOURCES[0][1]), reason="pinned commit not in this clone",
    )
    def test_the_oldest_graph_reaches_the_current_version(
        self, tmp_path: Path,
    ) -> None:
        """One open, the whole route, and the claims come through unchanged."""
        version, commit = _SOURCES[0]
        root = tmp_path / "project"
        _graph_written_by(version, commit, tmp_path / "work", root)

        with sqlite3.connect(_db(root)) as conn:
            assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
        before = _state(root)

        with mareforma.open(root, key_path=root / "root.key"):
            pass

        after = _state(root)
        assert after["user_version"] == _SCHEMA_VERSION
        assert after["claims"] == before["claims"]
        # The signed child tables, not every table: the guard census records
        # what it saw on the way through, so it is supposed to differ across an
        # open, and comparing it would only assert that the open ran.
        signed = ("verdict_chain", "grounding_attestations", "validators",
                  "contradiction_verdicts", "replication_verdicts")
        for table in signed:
            if table in before["children"]:
                assert after["children"].get(table) == before["children"][table], table
        _rows_agree_on_surviving_columns(before["rows"], after["rows"])

    @pytest.mark.skipif(
        not _available(_SOURCES[0][1]), reason="pinned commit not in this clone",
    )
    def test_a_stop_partway_says_where_it_stopped_and_resumes(
        self, tmp_path: Path,
    ) -> None:
        """The failure a chain has that a single step does not.

        Each hop commits on its own, so a second hop that fails leaves the file
        at the first hop's version: neither where it started nor where it was
        going. Telling the operator nothing changed would point them at a
        remedy that cannot work, because the release that wrote the file
        refuses the version the chain has reached. It has to name that version
        and say the open can simply be run again.
        """
        version, commit = _SOURCES[0]
        root = tmp_path / "project"
        _graph_written_by(version, commit, tmp_path / "work", root)

        reached = _SCHEMA_VERSION
        route = dict(_MIGRATIONS)
        route[reached] = (reached + 1, _blow_up_in_the_second_hop)

        with mock.patch.dict(_MIGRATIONS, route, clear=True), \
                mock.patch.object(_core, "_SCHEMA_VERSION", reached + 1):
            with pytest.raises(MigrationError) as caught:
                with mareforma.open(root, key_path=root / "root.key"):
                    pass
        message = str(caught.value)
        assert f"reached user_version={reached}" in message
        assert "committed" in message and "Do not delete" in message
        assert "unchanged" not in message, (
            "the graph is not unchanged: the first hop committed"
        )

        with sqlite3.connect(_db(root)) as conn:
            assert conn.execute("PRAGMA user_version").fetchone()[0] == reached

        # Re-running the open is the documented remedy, so it has to work.
        with mareforma.open(root, key_path=root / "root.key"):
            pass
        with sqlite3.connect(_db(root)) as conn:
            assert conn.execute(
                "PRAGMA user_version"
            ).fetchone()[0] == _SCHEMA_VERSION


def _blow_up_in_the_second_hop(conn: sqlite3.Connection) -> None:
    """A second hop that fails after the first one has committed."""
    raise sqlite3.OperationalError("the second hop refused on purpose")


class TestContentionIsNotCalledCorruption:

    def test_a_locked_graph_is_told_to_wait_not_to_delete(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """A write on the way in can meet another opener's lock.

        Several happen before any migration guard, the census and the column
        additions among them, and a lock there surfaced as "delete graph.db and
        start fresh" on a file with every byte intact. This release makes it
        reachable in ordinary use, because the migration holds the lock for
        seconds on a large graph's first open.
        """
        version, commit = _SOURCES[0]
        if not _available(commit):
            pytest.skip(f"the {version} commit is not reachable")
        project = tmp_path / "project"
        project.mkdir()
        _graph_written_by(version, commit, tmp_path / "work", project)

        holder = sqlite3.connect(_db(project), timeout=30)
        holder.execute("BEGIN IMMEDIATE")

        # The open path takes SQLite's default five second wait. Shortened here
        # so the suite does not spend five seconds proving a message.
        real = sqlite3.connect

        def impatient(*args, **kwargs):
            kwargs.setdefault("timeout", 0.2)
            return real(*args, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", impatient)
        try:
            with pytest.raises(Exception) as caught:
                _core.open_db(project)
        finally:
            monkeypatch.undo()
            holder.rollback()
            holder.close()

        said = str(caught.value)
        assert "Do not delete graph.db" in said
        assert "open it again in a moment" in said
        assert "start fresh" not in said
