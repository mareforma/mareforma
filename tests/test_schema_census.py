"""The schema census: what was missing when the file was opened.

A dropped write guard is the one tamper a read cannot infer afterwards, because
the repairs run silently on the way in. ``_ensure_managed_triggers`` reconciles
every trigger in the schema against ``sqlite_master`` on every open, and
``_ADDITIVE_TABLES_SQL`` re-runs its own ``CREATE TRIGGER IF NOT EXISTS``
statements on every open too. So by the time anything reads a claim, a guard
that was gone is back, and the deletes it permitted are indistinguishable from
rows that were never written.

There used to be guards that stayed gone, created once by a script that never
runs again, and a reader could at least see those missing in ``sqlite_master``.
There are none now. Healing every guard on every open is the right behaviour
and it costs the last witness a read had, which is what makes the census load
bearing rather than a nicety.

These tests pin the three properties that make it worth having: it runs BEFORE
the repairs, its record survives every later open, and it tells a table this
graph never had apart from one that was taken away.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

import mareforma
from mareforma.db._schema_sql import (
    _ADDITIVE_TABLES_SQL,
    _ALL_EXPECTED_TRIGGERS,
    _EXPECTED_TRIGGER_TABLES,
    _MANAGED_TRIGGERS,
)
from mareforma.db.core import open_db, schema_census_missing
from tests._helpers import _bootstrap_key

# One guard whose text is authored in Python, one whose text lives in the DDL.
# Both are reconciled now, so both heal before the open returns and neither is
# visible as missing to anything that reads afterwards. They are kept apart here
# because the two homes are still where a definition can drift.
_AUTHORED_GUARD = "validators_no_delete"
_DDL_GUARD = "contradiction_verdicts_no_delete"

# The tables _ADDITIVE_TABLES_SQL builds on the way into every open. A graph.db
# written by an earlier mareforma has none of them, and dropping them here is
# how these tests reproduce that file: adding a table is additive and never
# bumped user_version, so such a file walks straight through the version gate.
_ADDITIVE_TABLES = tuple(sorted(set(
    re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", _ADDITIVE_TABLES_SQL)
)))


def _graph_with_one_claim(root: Path) -> Path:
    """A signed project with one claim, and the root key that opened it.

    Signed rather than keyless on purpose: an unsigned project enrols no root,
    so the substrate axis reads "no trust root enrolled" and the planted-root
    attack has no row to copy.
    """
    key = _bootstrap_key(root, "root.key")
    with mareforma.open(root, key_path=key) as g:
        g.assert_claim("a claim")
    return key


def _drop(root: Path, *triggers: str) -> None:
    raw = sqlite3.connect(root / ".mareforma" / "graph.db")
    for t in triggers:
        raw.execute(f"DROP TRIGGER {t}")
    raw.commit()
    raw.close()


def _forget_the_bookkeeping(root: Path) -> None:
    """Remove the census tables, the way a file from an earlier build has none.

    Carrying no seen set is what marks a graph as inherited. It is also the
    only state in which an absent table is given the benefit of the doubt, so
    tests that mean "an older file" have to reach it, not just delete tables
    from a graph this build demonstrably made whole.
    """
    raw = sqlite3.connect(root / ".mareforma" / "graph.db")
    for table in ("schema_census", "schema_guards_seen"):
        raw.execute(f"DROP TABLE IF EXISTS {table}")
    raw.commit()
    raw.close()


def _make_legacy(root: Path) -> None:
    """A file written by a build that had none of the trust layer."""
    raw = sqlite3.connect(root / ".mareforma" / "graph.db")
    raw.execute("PRAGMA legacy_alter_table = ON")
    for table in _ADDITIVE_TABLES:
        raw.execute(f"DROP TABLE IF EXISTS {table}")
    raw.commit()
    raw.close()
    _forget_the_bookkeeping(root)


def _seen_guards(root: Path) -> set[str]:
    """The seen set, read raw.

    Not through ``open_db``: an open seeds this table, so reading it that way
    would report the state the read itself produced.
    """
    raw = sqlite3.connect(root / ".mareforma" / "graph.db")
    try:
        return {r[0] for r in raw.execute("SELECT name FROM schema_guards_seen")}
    finally:
        raw.close()


def _live_triggers(root: Path) -> set[str]:
    conn = open_db(root)
    try:
        return {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'")}
    finally:
        conn.close()


class TestTheExpectedSet:
    def test_matches_a_freshly_built_graph_exactly(self, tmp_path: Path) -> None:
        """Names AND text, because the text is what a reconciler compares."""
        conn = open_db(tmp_path)
        try:
            live = {r[0]: r[1] for r in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'")}
        finally:
            conn.close()
        assert set(live) == set(_ALL_EXPECTED_TRIGGERS)
        mismatched = [n for n in live if _ALL_EXPECTED_TRIGGERS[n] != live[n]]
        assert mismatched == []

    def test_carries_no_if_not_exists(self) -> None:
        """SQLite strips it from what it stores.

        A wanted text that keeps ``IF NOT EXISTS`` can never equal the stored
        text, so a reconciler keyed on equality would drop and recreate that
        trigger on every single open, forever, taking a write lock each time.
        """
        offenders = [n for n, sql in _ALL_EXPECTED_TRIGGERS.items()
                     if "IF NOT EXISTS" in sql]
        assert offenders == []

    def test_covers_the_managed_set(self) -> None:
        assert {n for n, _ in _MANAGED_TRIGGERS} <= set(_ALL_EXPECTED_TRIGGERS)

    def test_every_guard_is_paired_with_a_table_that_exists(
        self, tmp_path: Path,
    ) -> None:
        """The pairing is what lets the census skip a table that is not here yet.

        Parsed out of the DDL, so a trigger written with an unusual clause order
        could pair with the wrong word and silently take itself out of the
        expected set forever. Checking every pair against a real graph is what
        catches that.
        """
        assert set(_EXPECTED_TRIGGER_TABLES) == set(_ALL_EXPECTED_TRIGGERS)
        conn = open_db(tmp_path)
        try:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'")}
        finally:
            conn.close()
        unpaired = {n: t for n, t in _EXPECTED_TRIGGER_TABLES.items()
                    if t not in tables}
        assert unpaired == {}


class TestAnOlderGraphIsNotTamper:
    """The additive tables arrive on the way in, and their guards with them.

    ``_ADDITIVE_TABLES_SQL`` builds nine tables on every open, which is the
    whole reason it re-runs rather than being fresh-database-only, and sixteen
    expected guards hang off those tables. A graph.db written before they
    existed reaches the census without them, and it gets there legitimately:
    adding a table never bumped ``user_version``, so the version gate passes it
    through. Censused against the flat expected set it reports sixteen absent
    guards on the open that creates them, and because the record is a union
    that survives every later open, every claim in that file reads TAMPERED for
    good. An upgrade path is not an attack.
    """

    def test_a_file_this_build_made_gets_no_exemption(self, tmp_path: Path) -> None:
        """The same tables, taken from a graph that is known to have had them.

        Without this the legacy exemption would read as "absent tables are
        always fine", which is the hole the seen set exists to close.
        """
        _graph_with_one_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("PRAGMA legacy_alter_table = ON")
        for table in _ADDITIVE_TABLES:
            raw.execute(f"DROP TABLE IF EXISTS {table}")
        raw.commit()
        raw.close()

        conn = open_db(tmp_path)
        try:
            assert "findings_no_delete" in schema_census_missing(conn)
        finally:
            conn.close()

    def test_a_graph_predating_the_additive_tables_reports_nothing(
        self, tmp_path: Path,
    ) -> None:
        _graph_with_one_claim(tmp_path)
        _make_legacy(tmp_path)

        conn = open_db(tmp_path)
        try:
            assert schema_census_missing(conn) == ()
        finally:
            conn.close()

    def test_it_stays_clean_on_the_substrate_axis(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        _make_legacy(tmp_path)

        with mareforma.open(tmp_path, key_path=key) as g:
            root = g.trust_map(cid).get("trust_root")
        assert root.value == "single trust domain"

    def test_the_upgrade_leaves_a_graph_the_census_still_guards(
        self, tmp_path: Path,
    ) -> None:
        """Skipping absent tables is not the same as going quiet.

        Once the open has built the tables the guards belong to, they are back
        in the expected set, so the next drop is caught. Without this the fix
        for the upgrade case could be "expect nothing" and still pass above.
        """
        _graph_with_one_claim(tmp_path)
        _make_legacy(tmp_path)
        open_db(tmp_path).close()                    # the upgrade open
        _drop(tmp_path, _AUTHORED_GUARD)

        conn = open_db(tmp_path)
        try:
            assert _AUTHORED_GUARD in schema_census_missing(conn)
        finally:
            conn.close()

class TestTakingTheTableTooDoesNotHelp:
    """The escape hatch that pairing a guard to its table would otherwise open.

    A guard cannot outlive its table, so "expect it only while its table is
    here" hands an attacker a cheaper move than dropping the guard: drop the
    whole table and the guards leave the expected set with it, while
    ``_ADDITIVE_TABLES_SQL`` rebuilds the table empty on the same open. Quieter
    than dropping a guard, not louder. Measured before the seen set existed, a
    populated findings table could be destroyed and every axis went on
    reporting exactly what it reported before.

    The seen set is what closes it. A guard this graph has carried stays
    expected however its table is treated.
    """

    def test_dropping_the_table_is_still_tamper(self, tmp_path: Path) -> None:
        _graph_with_one_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("PRAGMA legacy_alter_table = ON")
        raw.execute("DROP TABLE findings")
        raw.commit()
        raw.close()

        conn = open_db(tmp_path)
        try:
            missing = schema_census_missing(conn)
        finally:
            conn.close()
        assert "findings_no_delete" in missing
        assert "findings_append_only" in missing

    def test_it_reaches_the_substrate_axis(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("PRAGMA legacy_alter_table = ON")
        raw.execute("DROP TABLE propositions")
        raw.commit()
        raw.close()

        with mareforma.open(tmp_path, key_path=key) as g:
            root = g.trust_map(cid).get("trust_root")
        assert root.value == "TAMPERED"
        assert "propositions_no_delete" in root.residual

    def test_the_seen_set_records_what_is_there_not_what_was_wanted(
        self, tmp_path: Path,
    ) -> None:
        """Recording the expected set instead would be circular.

        A guard would enrol itself the first time it was expected, and the
        graph would go on expecting it for a reason with nothing to do with
        ever having had it.

        Every guard heals now, so the only way to hold one absent past the
        repairs is to take its table with it, and rekor_inclusions is one of
        the five the additive script does not rebuild. With nothing seen yet,
        its guards are genuinely unknown to this graph and must stay unknown.
        """
        _graph_with_one_claim(tmp_path)
        _forget_the_bookkeeping(tmp_path)                # nothing seen yet
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("PRAGMA legacy_alter_table = ON")
        raw.execute("DROP TABLE rekor_inclusions")
        raw.commit()
        raw.close()

        open_db(tmp_path).close()
        seen = _seen_guards(tmp_path)
        assert "rekor_inclusions_no_delete" not in seen
        assert "rekor_inclusions_append_only" not in seen
        assert _AUTHORED_GUARD in seen           # and the rest were recorded

    def test_an_upgrade_records_the_trust_layer_it_just_built(
        self, tmp_path: Path,
    ) -> None:
        """The seeding runs after the repairs, and the window is why.

        The census has to look before anything heals. If the seen set looked at
        the same moment, the guards an upgrade creates would go unrecorded
        until the open after it, and a table dropped in between would walk out
        through the gap the seen set exists to close.
        """
        _graph_with_one_claim(tmp_path)
        _make_legacy(tmp_path)

        open_db(tmp_path).close()                        # the upgrade open
        assert "findings_no_delete" in _seen_guards(tmp_path)

        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("PRAGMA legacy_alter_table = ON")
        raw.execute("DROP TABLE findings")
        raw.commit()
        raw.close()
        conn = open_db(tmp_path)
        try:
            assert "findings_no_delete" in schema_census_missing(conn)
        finally:
            conn.close()

    def test_a_fresh_graph_is_seeded_before_it_can_be_touched(
        self, tmp_path: Path,
    ) -> None:
        """Otherwise a brand-new file has an empty baseline to attack.

        The seen set is normally filled by the first open of an existing file.
        A graph created and then tampered with before it is ever reopened would
        have nothing recorded, so the guards on a removed table would never
        have been expected. Seeding at creation closes that window for every
        graph this build makes; a file it inherits still relies on its first
        open, which is named in the sibling test below.
        """
        open_db(tmp_path).close()                        # creation, nothing more
        assert _seen_guards(tmp_path) == set(_ALL_EXPECTED_TRIGGERS)

    def test_an_inherited_graph_tampered_before_its_first_open_escapes(
        self, tmp_path: Path,
    ) -> None:
        """The residual window, named rather than left to be discovered.

        A graph.db written by an earlier build carries no seen set, so its
        first open under this one is where the baseline comes from. A table
        removed before that open was never seen here and its guards are never
        expected. Nothing observed it, so nothing can report it; the honest
        move is to have it on the record as a known limit.
        """
        _graph_with_one_claim(tmp_path)
        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("DROP TABLE schema_guards_seen")     # the inherited state
        raw.execute("PRAGMA legacy_alter_table = ON")
        raw.execute("DROP TABLE findings")
        raw.commit()
        raw.close()

        conn = open_db(tmp_path)
        try:
            assert schema_census_missing(conn) == ()
        finally:
            conn.close()


class TestTheCensusSeesWhatHeals:
    def test_untouched_graph_reports_nothing(self, tmp_path: Path) -> None:
        _graph_with_one_claim(tmp_path)
        conn = open_db(tmp_path)
        try:
            assert schema_census_missing(conn) == ()
        finally:
            conn.close()

    @pytest.mark.parametrize("guard", [_AUTHORED_GUARD, _DDL_GUARD])
    def test_records_a_guard_that_heals_on_the_same_open(
        self, tmp_path: Path, guard: str,
    ) -> None:
        """The case the ordering exists for, and now it is every case.

        The guard is back before the open returns, whichever home its text
        lives in. Re-deriving from sqlite_master at read time would answer
        "nothing is missing" on a graph that was demonstrably tampered with,
        and there is no longer any guard for which that answer happens to be
        right.
        """
        _graph_with_one_claim(tmp_path)
        _drop(tmp_path, guard)

        conn = open_db(tmp_path)
        try:
            assert guard in schema_census_missing(conn)
        finally:
            conn.close()
        assert guard in _live_triggers(tmp_path)     # healed, yet recorded

    def test_the_record_survives_later_opens(self, tmp_path: Path) -> None:
        """A guard that came back is not a guard that was never gone.

        Reporting only the most recent census would let one subsequent open
        bury the observation, which is the same disappearance the census exists
        to prevent, one level up.
        """
        _graph_with_one_claim(tmp_path)
        _drop(tmp_path, _AUTHORED_GUARD, _DDL_GUARD)

        seen = []
        for _ in range(3):
            conn = open_db(tmp_path)
            seen.append(schema_census_missing(conn))
            conn.close()

        for observed in seen:
            assert _AUTHORED_GUARD in observed
            assert _DDL_GUARD in observed

    def test_repeated_clean_opens_do_not_accumulate_rows(
        self, tmp_path: Path,
    ) -> None:
        """A row per open would grow without bound and bury the real one."""
        _graph_with_one_claim(tmp_path)
        _drop(tmp_path, _DDL_GUARD)
        for _ in range(4):
            open_db(tmp_path).close()

        conn = open_db(tmp_path)
        try:
            rows = conn.execute("SELECT COUNT(*) FROM schema_census").fetchone()[0]
        finally:
            conn.close()
        assert rows == 1


class TestTheSubstrateAxis:
    def test_a_dropped_guard_reads_tampered_on_the_trust_map(
        self, tmp_path: Path,
    ) -> None:
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        _drop(tmp_path, _DDL_GUARD)

        with mareforma.open(tmp_path, key_path=key) as g:
            root = g.trust_map(cid).get("trust_root")
        assert root.value == "TAMPERED"
        assert _DDL_GUARD in root.residual

    def test_a_planted_second_root_reads_tampered(self, tmp_path: Path) -> None:
        """validators blocks UPDATE and DELETE and permits INSERT.

        One statement turns every enrolment check in the graph False. Before
        this, the independence axis moved UP in response.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")

        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.row_factory = sqlite3.Row
        row = dict(raw.execute("SELECT * FROM validators LIMIT 1").fetchone())
        row["keyid"] = "d0" * 32
        row["enrolled_by_keyid"] = row["keyid"]
        raw.execute(
            f"INSERT INTO validators ({','.join(row)}) "
            f"VALUES ({','.join('?' * len(row))})",
            tuple(row.values()),
        )
        raw.commit()
        raw.close()

        with mareforma.open(tmp_path, key_path=key) as g:
            tmap = g.trust_map(cid)
        assert tmap.get("independence").value == "TAMPERED"
        assert tmap.get("trust_root").value == "TAMPERED"

    def test_a_clean_graph_keeps_the_ordinary_disclosure(
        self, tmp_path: Path,
    ) -> None:
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
            root = g.trust_map(cid).get("trust_root")
        assert root.value == "single trust domain"
        assert "disclosed, not established" in root.residual


class TestWhatTheCensusDoesNotReach:
    """The residual on the substrate axis names this, so it is measured here.

    A residual that describes behaviour nothing pins is a claim, and this map
    refuses to make claims it has not checked. The one place the census is
    silent is the search index: unlike every guard, it is built once by a script
    that does not run again, so nothing reconciles it and there is no repair for
    a record to be the memory of.
    """

    def test_an_emptied_index_under_reports_and_the_census_stays_clean(
        self, tmp_path: Path,
    ) -> None:
        """Search answers, and its answer is short by the rows that were removed.

        Worse than an error, because it looks like a result. Writes keep
        working and later claims are indexed as usual, so the graph reads
        healthy on every surface and the missing rows are only visible by
        comparing search against query.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a finding about mitochondria")

        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("DELETE FROM claims_fts")
        raw.commit()
        raw.close()

        with mareforma.open(tmp_path, key_path=key) as g:
            # Writing still works, so nothing announces the damage.
            cid = g.assert_claim("a second finding about mitochondria")
            found = [c["text"] for c in g.search("mitochondria")]
            assert found == ["a second finding about mitochondria"]
            assert len(g.query()) == 2
            root = g.trust_map(cid).get("trust_root")

        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        assert schema_census_missing(raw) == ()
        raw.close()
        assert root.value == "single trust domain", (
            "an emptied index is invisible to the census, which is what the "
            "residual has to say"
        )
        assert "search index" in root.residual

    def test_a_dropped_index_is_not_rebuilt_on_the_next_open(
        self, tmp_path: Path,
    ) -> None:
        """The other half, and it is loud rather than silent.

        Named beside the quiet one so the difference between them is on the
        record: a graph that loses the index outright cannot be written to
        either, and no open puts it back.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a finding")

        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.execute("DROP TABLE claims_fts")
        raw.commit()
        raw.close()

        with mareforma.open(tmp_path, key_path=key) as g:
            with pytest.raises(Exception, match="claims_fts"):
                g.assert_claim("a second finding")
            # query does not go through the index, so the claims are readable.
            assert len(g.query()) == 1

        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        present = raw.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'claims_fts'"
        ).fetchone()[0]
        assert schema_census_missing(raw) == ()
        raw.close()
        assert present == 0, "no open rebuilds it, which is why it is a residual"
