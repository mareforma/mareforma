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
from mareforma.db.core import open_db, schema_census_missing
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
