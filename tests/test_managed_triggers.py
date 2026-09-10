"""Every trigger in the schema is reconciled on every open.

The set used to be split, and the split was not a decision about which guards
matter. ``_SCHEMA_SQL`` runs once, on a fresh database, and never again, so a
trigger created only there could be dropped and would simply stay dropped:
``contradiction_verdicts_no_delete``, the append-only guards on
``rekor_inclusions`` and ``replication_verdicts``, the claims state-machine
checks. Seventeen guards whose removal was permanent, sitting next to seventeen
whose removal lasted until the next open, and nothing about the tables said
which was which.

Two properties are pinned here. Every guard comes back, and the reconciler is
still a pure read when there is nothing to do, because it now runs over twice
as many triggers on the hot path of every open and a reconciler that churns
takes a write lock each time it is asked a question.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import mareforma
from mareforma.db._schema_sql import (
    _ALL_EXPECTED_TRIGGERS,
    _AUTHORED_TRIGGERS,
    _EXPECTED_TRIGGER_TABLES,
    _MANAGED_TRIGGERS,
)
from mareforma.db.core import _SCHEMA_VERSION, open_db, schema_census_missing
from tests._helpers import _bootstrap_key

# The guards that were reconciled before this, so the ones that were not are
# everything else. Named by derivation rather than by a copied list: a copied
# list would go stale the moment a trigger is added, and staleness is the
# failure this whole change is about.
_ONCE_UNMANAGED = tuple(sorted(
    set(_ALL_EXPECTED_TRIGGERS) - {name for name, _ in _AUTHORED_TRIGGERS}
))


def _db(root: Path) -> Path:
    return root / ".mareforma" / "graph.db"


def _schema_version(root: Path) -> int:
    """SQLite's own DDL counter, read outside any mareforma open.

    It ticks on every CREATE or DROP. Reading it through ``open_db`` would
    include the ticks of the open doing the reading.
    """
    raw = sqlite3.connect(_db(root))
    try:
        return raw.execute("PRAGMA schema_version").fetchone()[0]
    finally:
        raw.close()


def _raw(root: Path, *statements: str) -> None:
    raw = sqlite3.connect(_db(root))
    for stmt in statements:
        raw.execute(stmt)
    raw.commit()
    raw.close()


def _triggers(root: Path) -> set[str]:
    raw = sqlite3.connect(_db(root))
    try:
        return {r[0] for r in raw.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'")}
    finally:
        raw.close()


class TestTheReconciledSetIsTheExpectedSet:
    def test_they_are_the_same_thing(self) -> None:
        """Derived from one another, so a new trigger cannot miss the reconciler.

        The old list was maintained by hand beside a growing schema, which is
        the other half of why guards went unreconciled: nobody decided to leave
        contradiction_verdicts_no_delete out, it was just never added.
        """
        assert {name for name, _ in _MANAGED_TRIGGERS} == set(_ALL_EXPECTED_TRIGGERS)
        assert len(_MANAGED_TRIGGERS) == len(_ALL_EXPECTED_TRIGGERS)

    def test_the_authored_ones_are_a_subset_not_the_whole(self) -> None:
        """Authored text is about where a definition is written, nothing else."""
        authored = {name for name, _ in _AUTHORED_TRIGGERS}
        assert authored < set(_ALL_EXPECTED_TRIGGERS)
        assert _ONCE_UNMANAGED, "nothing left to promote, this suite is vacuous"

    def test_a_fresh_graph_carries_every_one(self, tmp_path: Path) -> None:
        open_db(tmp_path).close()
        assert _triggers(tmp_path) == set(_ALL_EXPECTED_TRIGGERS)


class TestEveryGuardComesBack:
    @pytest.mark.parametrize("guard", _ONCE_UNMANAGED)
    def test_a_dropped_guard_is_restored_on_the_next_open(
        self, tmp_path: Path, guard: str,
    ) -> None:
        open_db(tmp_path).close()
        _raw(tmp_path, f"DROP TRIGGER {guard}")
        assert guard not in _triggers(tmp_path)

        open_db(tmp_path).close()
        assert guard in _triggers(tmp_path)

    @pytest.mark.parametrize("guard", _ONCE_UNMANAGED)
    def test_the_restored_text_is_the_wanted_text(
        self, tmp_path: Path, guard: str,
    ) -> None:
        """Restoring the wrong text would be worse than not restoring it.

        A guard whose body no longer matches is a guard the reconciler will
        rewrite on every single open, forever, taking a write lock each time.
        """
        open_db(tmp_path).close()
        _raw(tmp_path, f"DROP TRIGGER {guard}")
        open_db(tmp_path).close()

        raw = sqlite3.connect(_db(tmp_path))
        try:
            stored = raw.execute(
                "SELECT sql FROM sqlite_master WHERE name = ?", (guard,),
            ).fetchone()[0]
        finally:
            raw.close()
        assert stored == _ALL_EXPECTED_TRIGGERS[guard]

    def test_healing_does_not_hide_the_drop(self, tmp_path: Path) -> None:
        """Reconciling more guards makes the census matter more, not less.

        Before this, a guard from _SCHEMA_SQL stayed visibly gone and a reader
        could see it in sqlite_master. Now nothing stays gone, so the census is
        the only witness there is.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        _raw(tmp_path, "DROP TRIGGER contradiction_verdicts_no_delete")

        with mareforma.open(tmp_path, key_path=key) as g:
            root = g.trust_map(cid).get("trust_root")
        assert root.value == "TAMPERED"
        assert "contradiction_verdicts_no_delete" in root.residual
        assert "contradiction_verdicts_no_delete" in _triggers(tmp_path)


class TestTheSteadyStateOpenChangesNothing:
    def test_schema_version_holds_across_repeated_opens(
        self, tmp_path: Path,
    ) -> None:
        """SQLite's DDL counter, which ticks on every CREATE and DROP.

        The direct assertion would be "the guards are present", and it passes
        just as well in a world where the reconciler drops and recreates all
        thirty-eight on every open. This one does not: churn moves the counter.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a claim")

        readings = [_schema_version(tmp_path)]
        for _ in range(5):
            open_db(tmp_path).close()
            readings.append(_schema_version(tmp_path))
        assert len(set(readings)) == 1, readings

    def test_it_moves_exactly_once_for_a_real_repair(
        self, tmp_path: Path,
    ) -> None:
        """The counter is only useful if it would have caught the churn."""
        open_db(tmp_path).close()
        before = _schema_version(tmp_path)
        _raw(tmp_path, "DROP TRIGGER rekor_inclusions_no_delete")
        open_db(tmp_path).close()
        after = _schema_version(tmp_path)
        assert after > before

        open_db(tmp_path).close()
        assert _schema_version(tmp_path) == after     # and then it settles


class TestTheCensusStoreCannotBeEmptied:
    """The store is the only record once every guard heals on every open.

    Which makes it the obvious thing to attack. It is the last table in the
    schema that carried evidence and no guard, and the same SQL access that
    drops a trigger emptied it in one statement.
    """

    def _tampered_graph(self, tmp_path: Path) -> None:
        _bootstrap_key(tmp_path, "root.key")
        open_db(tmp_path).close()
        _raw(tmp_path, "DROP TRIGGER findings_no_delete")
        open_db(tmp_path).close()                     # records it, heals it

    @pytest.mark.parametrize("statement", [
        "DELETE FROM schema_census",
        "UPDATE schema_census SET missing = '[]'",
        "DELETE FROM schema_guards_seen",
        "UPDATE schema_guards_seen SET name = 'x'",
    ])
    def test_the_store_refuses(self, tmp_path: Path, statement: str) -> None:
        self._tampered_graph(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        try:
            with pytest.raises(sqlite3.IntegrityError):
                raw.execute(statement)
        finally:
            raw.close()

    def test_the_report_survives_the_attempt(self, tmp_path: Path) -> None:
        self._tampered_graph(tmp_path)
        raw = sqlite3.connect(_db(tmp_path))
        try:
            raw.execute("DELETE FROM schema_census")
        except sqlite3.IntegrityError:
            pass
        raw.close()

        conn = open_db(tmp_path)
        try:
            assert "findings_no_delete" in schema_census_missing(conn)
        finally:
            conn.close()

    def test_the_report_survives_a_backup_and_restore(
        self, tmp_path: Path,
    ) -> None:
        """A round trip must not be the thing that forgets.

        The census is the one record a later open cannot rebuild: by then the
        guards have healed, so a live re-derivation answers "nothing is missing"
        on exactly the graph that was tampered with. Leaving it out of the
        backup made tamper, back up, restore into a laundry: every surface that
        had just called the graph tampered went quiet.
        """
        from mareforma.db.restore import restore

        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a claim to recover", generated_by="run1")
        _raw(tmp_path, "DROP TRIGGER findings_no_delete")
        # Reopening records the drop and heals it. The write is what makes the
        # backup rewrite, which is the file that now has to carry the record;
        # a session that changes nothing leaves the older backup in place, and
        # that one predates the tamper anyway.
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("written after the guard went", generated_by="run2")

        conn = open_db(tmp_path)
        try:
            assert "findings_no_delete" in schema_census_missing(conn)
        finally:
            conn.close()

        recovered = tmp_path / "recovered"
        recovered.mkdir()
        (recovered / "claims.toml").write_text(
            (tmp_path / "claims.toml").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        restore(recovered)

        conn = open_db(recovered)
        try:
            assert "findings_no_delete" in schema_census_missing(conn), (
                "the restored graph forgot that a guard had been missing"
            )
        finally:
            conn.close()

    @pytest.mark.parametrize("stored", [
        '"COMPROMISED"',      # a JSON string: update() walks it per character
        '{"a": 1}',           # a JSON object: update() walks the keys
        "[1, 2]",             # a list of non-strings
        "17",                 # a bare number
    ])
    def test_a_hostile_census_row_names_no_guards(
        self, tmp_path: Path, stored: str,
    ) -> None:
        """The reader takes a list of names, and checks that it got one.

        ``set.update`` iterates whatever it is handed, so a stored JSON string
        reported one guard per character and a stored object reported one per
        key. The writer only ever stores a list, but the census travels in the
        backup now, so the value can arrive from a file somebody wrote.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a claim", generated_by="r1")
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute(
            "INSERT INTO schema_census(observed_at, missing) VALUES (?, ?)",
            ("2026-01-01T00:00:00+00:00", stored),
        )
        raw.commit()
        raw.close()

        conn = open_db(tmp_path)
        try:
            assert schema_census_missing(conn) == ()
        finally:
            conn.close()

    def test_zeroing_the_schema_version_does_not_skip_the_census(
        self, tmp_path: Path,
    ) -> None:
        """One PRAGMA must not buy the fresh-database path.

        ``open_db`` branches on ``user_version``, and zero means "fresh": it
        creates the schema, heals every guard, notes the seen set and returns
        without recording a census. ``user_version`` is a plain write no trigger
        can refuse, so on a populated graph that branch is a way to drop a guard
        and have the record never written, with the pragma restored to 1 on the
        way out so nothing looks touched afterwards.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a claim", generated_by="r1")
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("DROP TRIGGER findings_no_delete")
        raw.execute("PRAGMA user_version = 0")
        raw.commit()
        raw.close()

        conn = open_db(tmp_path)
        try:
            assert "findings_no_delete" in schema_census_missing(conn), (
                "zeroing user_version skipped the census"
            )
            assert conn.execute(
                "PRAGMA user_version"
            ).fetchone()[0] == _SCHEMA_VERSION
        finally:
            conn.close()

    def test_a_same_named_no_op_guard_is_reported_missing(
        self, tmp_path: Path,
    ) -> None:
        """A guard is its body, not its name.

        The census compared names, and the reconciler repairs a wrong body on
        the same open, so replacing a guard with a same-named no-op healed
        silently and the census stayed empty. A dropped write guard has to be
        reported rather than silently healed, and a guard that no longer guards
        is dropped in every sense that matters.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a claim", generated_by="r1")
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("DROP TRIGGER findings_no_delete")
        raw.execute(
            "CREATE TRIGGER findings_no_delete BEFORE DELETE ON findings "
            "BEGIN SELECT 1; END"
        )
        raw.commit()
        raw.close()

        conn = open_db(tmp_path)
        try:
            assert "findings_no_delete" in schema_census_missing(conn)
        finally:
            conn.close()

    def test_a_healthy_graph_reports_nothing(self, tmp_path: Path) -> None:
        """The premise. Comparing bodies must not flag every honest graph."""
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a claim", generated_by="r1")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("another", generated_by="r2")
        conn = open_db(tmp_path)
        try:
            assert schema_census_missing(conn) == ()
        finally:
            conn.close()

    def test_dropping_the_stores_own_guard_is_reported(
        self, tmp_path: Path,
    ) -> None:
        """A guard is only a guard if removing it is visible.

        The store's guards are created by _SCHEMA_CENSUS_SQL, which the census
        used to run before it read sqlite_master. So it healed a dropped store
        guard and then reported a healthy schema, and four statements erased
        the whole report: drop the two store guards to get past them, delete
        the rows behind. The census reads first now and creates second.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        _raw(tmp_path, "DROP TRIGGER findings_no_delete")
        open_db(tmp_path).close()                     # records the real tamper

        _raw(tmp_path,
             "DROP TRIGGER schema_census_no_delete",
             "DROP TRIGGER schema_guards_seen_no_delete",
             "DELETE FROM schema_census",
             "DELETE FROM schema_guards_seen")

        with mareforma.open(tmp_path, key_path=key) as g:
            root = g.trust_map(cid).get("trust_root")
        assert root.value == "TAMPERED"
        assert "schema_census_no_delete" in root.residual

    def test_it_keeps_reporting_after_the_store_is_rebuilt(
        self, tmp_path: Path,
    ) -> None:
        """One open catching it is not enough if the next one forgets."""
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        _raw(tmp_path,
             "DROP TRIGGER schema_census_no_delete",
             "DELETE FROM schema_census",
             "DROP TRIGGER schema_guards_seen_no_delete",
             "DELETE FROM schema_guards_seen")

        for _ in range(3):
            with mareforma.open(tmp_path, key_path=key) as g:
                assert g.trust_map(cid).get("trust_root").value == "TAMPERED"

    def test_a_first_open_does_not_invent_a_missing_store(
        self, tmp_path: Path,
    ) -> None:
        """Reading before creating must not turn "not built yet" into tamper.

        The store's guards cannot exist before its tables do, and on a file
        written without either that is not a finding, it is the file.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("PRAGMA legacy_alter_table = ON")
        for table in ("schema_census", "schema_guards_seen"):
            raw.execute(f"DROP TABLE IF EXISTS {table}")
        raw.commit()
        raw.close()

        with mareforma.open(tmp_path, key_path=key) as g:
            assert g.trust_map(cid).get("trust_root").value == "single trust domain"

    def test_the_store_carries_its_own_guards(self, tmp_path: Path) -> None:
        """Derived like the rest, so they are reconciled like the rest."""
        open_db(tmp_path).close()
        for guard in ("schema_census_no_delete", "schema_census_append_only",
                      "schema_guards_seen_no_delete",
                      "schema_guards_seen_append_only"):
            assert guard in _ALL_EXPECTED_TRIGGERS
            assert guard in _triggers(tmp_path)

    def test_recording_a_second_observation_is_an_append(
        self, tmp_path: Path,
    ) -> None:
        """observed_at lost its PRIMARY KEY so the delete guard can be absolute.

        It had one, with INSERT OR REPLACE behind it, and REPLACE resolves a
        conflict by deleting the row first. Guarding the table would have
        turned a rare collision into a refused open.
        """
        _bootstrap_key(tmp_path, "root.key")
        open_db(tmp_path).close()
        _raw(tmp_path, "DROP TRIGGER findings_no_delete")
        open_db(tmp_path).close()
        _raw(tmp_path, "DROP TRIGGER contradiction_verdicts_no_delete")
        open_db(tmp_path).close()

        conn = open_db(tmp_path)
        try:
            rows = conn.execute("SELECT COUNT(*) FROM schema_census").fetchone()[0]
            missing = schema_census_missing(conn)
        finally:
            conn.close()
        assert rows == 2
        assert "findings_no_delete" in missing
        assert "contradiction_verdicts_no_delete" in missing


class TestAGuardWithNoTableIsSkipped:
    def test_the_reconciler_does_not_try_to_build_it(
        self, tmp_path: Path,
    ) -> None:
        """Otherwise an older graph.db cannot be opened at all.

        The reconciler runs over every trigger in the schema now, including the
        ones on tables the additive script has yet to create on an inherited
        file. Attempting one is not a no-op, it is "no such table" out of the
        open.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")

        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("PRAGMA legacy_alter_table = ON")
        for table in ("schema_census", "schema_guards_seen"):
            raw.execute(f"DROP TABLE IF EXISTS {table}")
        raw.commit()
        raw.close()

        with mareforma.open(tmp_path, key_path=key) as g:
            assert g.trust_map(cid) is not None

    def test_every_guard_names_a_table_the_reconciler_can_check(self) -> None:
        """The skip reads _EXPECTED_TRIGGER_TABLES by name and would KeyError."""
        assert set(_EXPECTED_TRIGGER_TABLES) == {n for n, _ in _MANAGED_TRIGGERS}


class TestOnlyTheReconcilerCreatesGuards:
    def test_the_additive_script_creates_no_triggers(self) -> None:
        """Two creators, one owner of the wanted text, is how they drift.

        Three guards were created by _ADDITIVE_TABLES_SQL, which executes on
        every open, while the reconciler also owned them. Their text now lives
        in _RECONCILED_ONLY_TRIGGERS_SQL, which is parsed and never executed, so
        the DDL keeps a home and creation has exactly one path.

        The first assertion is the rule and holds for every guard, including
        ones authored into the reconciled-only home later: a table added to the
        additive script puts its guards there, never beside it. The three named
        below are the ones that were moved, pinned so they cannot drift back.
        """
        from mareforma.db._schema_sql import (
            _ADDITIVE_TABLES_SQL, _RECONCILED_ONLY_TRIGGERS_SQL, _extract_triggers,
        )
        assert _extract_triggers(_ADDITIVE_TABLES_SQL) == ()
        lifted = {n for n, _ in _extract_triggers(_RECONCILED_ONLY_TRIGGERS_SQL)}
        assert {"predictions_no_delete", "plan_retirements_append_only",
                "plan_retirements_no_delete"} <= lifted
        assert lifted <= set(_ALL_EXPECTED_TRIGGERS)

    def test_they_are_still_built_on_a_fresh_graph(self, tmp_path: Path) -> None:
        """Lifting the text out must not lift the guards out with it."""
        open_db(tmp_path).close()
        live = _triggers(tmp_path)
        for guard in ("predictions_no_delete", "plan_retirements_append_only",
                      "plan_retirements_no_delete"):
            assert guard in live
