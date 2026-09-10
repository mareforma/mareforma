"""The contradiction answer stops being a column and becomes a replay.

``t_invalid`` carries no trigger. One UPDATE fabricates a contradiction with no
verdict behind it, and one UPDATE erases a real one from every read surface, and
a presenter over the row alone reports the edit as though it were the evidence.
The signed verdicts sit untouched in ``contradiction_verdicts`` the whole time.

So a read that holds the graph replays them: enrolled issuer, signature
verifying over the DSSE PAE rebuilt from the stored columns, the same bar the
recording path applies. What these tests pin is the disagreement, in both
directions, and the refusal to resolve it by un-flagging.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import mareforma
from mareforma.db.core import (
    REPLAY_TAMPER_SIGNALS,
    _verdict_invalidates,
    get_claim,
    open_db,
    refutation_status,
    replay_contradictions,
)
from tests._helpers import _bootstrap_key, _enroll_key


def _db(root: Path) -> Path:
    return root / ".mareforma" / "graph.db"


def _raw(root: Path, *statements) -> None:
    raw = sqlite3.connect(_db(root))
    for stmt in statements:
        raw.execute(stmt) if isinstance(stmt, str) else raw.execute(*stmt)
    raw.commit()
    raw.close()


def _contradicted_pair(root: Path) -> tuple[Path, str, str]:
    """Two claims and a real signed contradiction between them.

    The verdict issuer is a second enrolled key, because a verdict issuer must
    be an external witness whose keyid is not on the claim envelope. Returns the
    root key, the invalidated claim and the surviving one.
    """
    root_key = _bootstrap_key(root, "root.key")
    with mareforma.open(root, key_path=root_key) as g:
        older = g.assert_claim("the older claim", generated_by="run1")
        newer = g.assert_claim("the newer claim", generated_by="run2")

    witness = _bootstrap_key(root, "witness.key")
    _enroll_key(root, root_key, witness, identity="witness@example.org")
    with mareforma.open(root, key_path=witness) as g:
        g.record_contradiction_verdict(
            verdict_id="v1", member_claim_id=newer, other_claim_id=older,
        )
    return root_key, older, newer


class TestTheHonestCase:
    def test_a_real_contradiction_reads_as_a_signed_verdict(
        self, tmp_path: Path,
    ) -> None:
        key, older, _ = _contradicted_pair(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            ref = g.refutation_status(older)
        assert ref["state"] == "contradicted"
        assert ref["signal"] == "signed-verdict"

    def test_the_surviving_claim_is_clean(self, tmp_path: Path) -> None:
        key, _, newer = _contradicted_pair(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            ref = g.refutation_status(newer)
        assert ref["state"] == "clean"
        assert ref["signal"] == "none"

    def test_the_trust_map_agrees(self, tmp_path: Path) -> None:
        key, older, _ = _contradicted_pair(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            axis = g.trust_map(older).get("contestation")
        assert axis.value == "contradicted"
        assert "signed-verdict" in axis.residual

    def test_the_replay_agrees_with_the_trigger_on_which_claim_loses(
        self, tmp_path: Path,
    ) -> None:
        """The rule is restated in Python, so it can drift from the trigger.

        ``contradiction_invalidates_older`` picks the older claim by created_at,
        tie-broken on the smaller id. A replay that picked the other one would
        call every honest contradiction a suppressed verdict.
        """
        key, older, newer = _contradicted_pair(tmp_path)
        conn = open_db(tmp_path)
        try:
            verdict = conn.execute(
                "SELECT member_claim_id, other_claim_id FROM "
                "contradiction_verdicts LIMIT 1"
            ).fetchone()
            assert _verdict_invalidates(conn, verdict) == older
            assert get_claim(conn, older)["t_invalid"] is not None
            assert get_claim(conn, newer)["t_invalid"] is None
        finally:
            conn.close()

    def test_the_replay_agrees_with_the_trigger_when_timestamps_collide(
        self, tmp_path: Path,
    ) -> None:
        """The tie-break is the half of the rule a timestamp cannot decide.

        Two claims created in the same instant leave ``created_at`` no say, and
        the smaller claim id settles it on both sides. The two sides are written
        in different languages, so nothing but a test holds them together, and a
        replay that broke the tie the other way would report every honest
        contradiction in a collision as a suppressed verdict.

        The claim ids are random, so the expected loser is computed rather than
        named.
        """
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            first = g.assert_claim("one claim", generated_by="run1")
            second = g.assert_claim("the other claim", generated_by="run2")

        # Collisions are possible but rare at this timestamp resolution, so the
        # tie is forced. The guard that would refuse the edit comes off first,
        # which is why this stays a test about the rule and not about tamper.
        read = sqlite3.connect(_db(tmp_path))
        stamp = read.execute("SELECT MIN(created_at) FROM claims").fetchone()[0]
        read.close()
        _raw(
            tmp_path,
            "DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering",
            ("UPDATE claims SET created_at = ?", (stamp,)),
        )

        witness = _bootstrap_key(tmp_path, "witness.key")
        _enroll_key(tmp_path, root_key, witness, identity="witness@example.org")
        with mareforma.open(tmp_path, key_path=witness) as g:
            g.record_contradiction_verdict(
                verdict_id="v1", member_claim_id=second, other_claim_id=first,
            )

        loser, survivor = min(first, second), max(first, second)
        conn = open_db(tmp_path)
        try:
            verdict = conn.execute(
                "SELECT member_claim_id, other_claim_id FROM "
                "contradiction_verdicts LIMIT 1"
            ).fetchone()
            assert _verdict_invalidates(conn, verdict) == loser
            assert get_claim(conn, loser)["t_invalid"] is not None
            assert get_claim(conn, survivor)["t_invalid"] is None
        finally:
            conn.close()


class TestTheColumnWithoutTheEvidence:
    def test_an_invented_invalidation_is_named(self, tmp_path: Path) -> None:
        """One UPDATE, no verdict anywhere. The old answer was "contradicted"."""
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim nobody contradicted")
        _raw(tmp_path, ("UPDATE claims SET t_invalid = '2026-01-01T00:00:00+00:00' "
                        "WHERE claim_id = ?", (cid,)))

        with mareforma.open(tmp_path, key_path=key) as g:
            ref = g.refutation_status(cid)
        assert ref["signal"] == "unbacked-invalidation"
        assert ref["signal"] in REPLAY_TAMPER_SIGNALS

    def test_it_reaches_the_trust_map_as_tamper(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim nobody contradicted")
        _raw(tmp_path, ("UPDATE claims SET t_invalid = '2026-01-01T00:00:00+00:00' "
                        "WHERE claim_id = ?", (cid,)))

        with mareforma.open(tmp_path, key_path=key) as g:
            axis = g.trust_map(cid).get("contestation")
        assert axis.value == "TAMPERED"

    def test_the_claim_is_not_handed_back_as_clean(self, tmp_path: Path) -> None:
        """Un-flagging would finish the job for whoever set the column.

        The honest answer is that the column and the evidence disagree, not
        that the disagreement resolves in favour of either. A reader filtering
        contradicted claims out must still not see this one.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim nobody contradicted")
        _raw(tmp_path, ("UPDATE claims SET t_invalid = '2026-01-01T00:00:00+00:00' "
                        "WHERE claim_id = ?", (cid,)))

        with mareforma.open(tmp_path, key_path=key) as g:
            assert g.refutation_status(cid)["state"] == "contradicted"


class TestTheEvidenceWithoutTheColumn:
    def test_a_cleared_timestamp_does_not_clear_the_verdict(
        self, tmp_path: Path,
    ) -> None:
        """The other direction, and the one that suppresses a real refutation."""
        key, older, _ = _contradicted_pair(tmp_path)
        _raw(tmp_path, ("UPDATE claims SET t_invalid = NULL WHERE claim_id = ?",
                        (older,)))

        with mareforma.open(tmp_path, key_path=key) as g:
            ref = g.refutation_status(older)
        assert ref["state"] == "contradicted"
        assert ref["signal"] == "suppressed-verdict"

    def test_it_reaches_the_trust_map_as_tamper(self, tmp_path: Path) -> None:
        key, older, _ = _contradicted_pair(tmp_path)
        _raw(tmp_path, ("UPDATE claims SET t_invalid = NULL WHERE claim_id = ?",
                        (older,)))

        with mareforma.open(tmp_path, key_path=key) as g:
            axis = g.trust_map(older).get("contestation")
        assert axis.value == "TAMPERED"
        assert "suppressed-verdict" in axis.residual


class TestAVerdictThatDoesNotCheckOut:
    """The verdict table permits INSERT and refuses UPDATE and DELETE.

    So a hostile writer cannot edit a real verdict or remove one; the reachable
    move is to add a verdict of their own. The insert fires
    ``contradiction_invalidates_older``, which sets t_invalid, so a forged
    verdict suppresses a claim the same way a real one does. Nothing but
    checking the signature tells them apart.
    """

    def _plant(self, root: Path, victim: str, other: str, issuer: str) -> None:
        _raw(root, (
            "INSERT INTO contradiction_verdicts (verdict_id, member_claim_id, "
            "other_claim_id, confidence_json, issuer_keyid, signature, "
            "created_at) VALUES (?, ?, ?, '{}', ?, ?, ?)",
            ("planted", other, victim, issuer, b"not a signature",
             "2026-01-01T00:00:00+00:00"),
        ))

    def test_a_planted_verdict_suppresses_a_claim(self, tmp_path: Path) -> None:
        """What the attack buys without the replay: the column moves."""
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            victim = g.assert_claim("the older claim", generated_by="run1")
            other = g.assert_claim("the newer claim", generated_by="run2")
        conn = open_db(tmp_path)
        issuer = conn.execute("SELECT keyid FROM validators LIMIT 1").fetchone()[0]
        conn.close()

        self._plant(tmp_path, victim, other, issuer)
        conn = open_db(tmp_path)
        try:
            assert get_claim(conn, victim)["t_invalid"] is not None
        finally:
            conn.close()

    def test_a_forged_signature_is_tamper_not_weak_evidence(
        self, tmp_path: Path,
    ) -> None:
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            victim = g.assert_claim("the older claim", generated_by="run1")
            other = g.assert_claim("the newer claim", generated_by="run2")
        conn = open_db(tmp_path)
        issuer = conn.execute("SELECT keyid FROM validators LIMIT 1").fetchone()[0]
        conn.close()
        self._plant(tmp_path, victim, other, issuer)

        with mareforma.open(tmp_path, key_path=key) as g:
            ref = g.refutation_status(victim)
            axis = g.trust_map(victim).get("contestation")
        assert ref["signal"] == "unverifiable-verdict"
        assert "planted" in ref["reason"]
        assert axis.value == "TAMPERED"

    def test_an_unenrolled_issuer_is_tamper(self, tmp_path: Path) -> None:
        """A keyid the project never enrolled, past the foreign key."""
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            victim = g.assert_claim("the older claim", generated_by="run1")
            other = g.assert_claim("the newer claim", generated_by="run2")

        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("PRAGMA foreign_keys = OFF")
        raw.execute(
            "INSERT INTO contradiction_verdicts (verdict_id, member_claim_id, "
            "other_claim_id, confidence_json, issuer_keyid, signature, "
            "created_at) VALUES (?, ?, ?, '{}', ?, ?, ?)",
            ("planted", other, victim, "d0" * 32, b"sig",
             "2026-01-01T00:00:00+00:00"),
        )
        raw.commit()
        raw.close()

        with mareforma.open(tmp_path, key_path=key) as g:
            assert g.refutation_status(victim)["signal"] == "unverifiable-verdict"

    def test_a_real_verdict_beside_a_planted_one_still_reports_the_planted_one(
        self, tmp_path: Path,
    ) -> None:
        """Burying a forgery under a genuine verdict must not launder it."""
        key, older, newer = _contradicted_pair(tmp_path)
        conn = open_db(tmp_path)
        issuer = conn.execute("SELECT keyid FROM validators LIMIT 1").fetchone()[0]
        conn.close()
        self._plant(tmp_path, older, newer, issuer)

        with mareforma.open(tmp_path, key_path=key) as g:
            ref = g.refutation_status(older)
        assert ref["signal"] == "unverifiable-verdict"


class TestTheReplayNeverTakesAReadDown:
    """"Never raises" has to hold for more than a database error.

    The replay compares two ``created_at`` values. That column has TEXT
    affinity, so a value written around the write path keeps whatever type it
    was given, and comparing bytes with a string raises ``TypeError``, which is
    not a ``sqlite3.Error``. A guard refuses the edit and an attacker with file
    access drops the guard, so the read has to survive what it then finds.
    """

    def _blob_the_created_at(self, root: Path, claim_id: str) -> None:
        raw = sqlite3.connect(_db(root))
        for name in [
            r[0] for r in raw.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        ]:
            raw.execute(f"DROP TRIGGER IF EXISTS {name}")
        raw.execute(
            "UPDATE claims SET created_at = X'00' WHERE claim_id = ?",
            (claim_id,),
        )
        raw.commit()
        raw.close()

    def test_a_blob_timestamp_degrades_instead_of_raising(
        self, tmp_path: Path,
    ) -> None:
        key, older, _ = _contradicted_pair(tmp_path)
        self._blob_the_created_at(tmp_path, older)
        conn = open_db(tmp_path)
        try:
            status = refutation_status(dict(get_claim(conn, older)), conn)
        finally:
            conn.close()
        assert status["state"], "the read returned nothing at all"

    def test_a_replay_that_cannot_run_reads_as_tamper_not_as_silence(
        self, tmp_path: Path,
    ) -> None:
        """Degrading quietly would be the column speaking as the evidence.

        A replay that cannot run is a fact about the graph, not the absence of
        one. Reporting nothing let every caller fall through to ``t_invalid``,
        the column with no trigger that the replay exists to distrust.
        """
        key, older, _ = _contradicted_pair(tmp_path)
        self._blob_the_created_at(tmp_path, older)
        conn = open_db(tmp_path)
        try:
            status = refutation_status(dict(get_claim(conn, older)), conn)
        finally:
            conn.close()
        assert status["signal"] == "replay-unavailable"
        assert status["signal"] in REPLAY_TAMPER_SIGNALS
        assert "could not be" in status["reason"], status

    def test_a_listing_does_not_serve_a_suppressed_row_as_clean(
        self, tmp_path: Path,
    ) -> None:
        """The surface where falling through was worst.

        With the invalidation column cleared and the replay unable to run, a
        caller asking for clean claims was handed the row the signed verdict
        invalidates. The crash this replaced was at least visible.
        """
        key, older, newer = _contradicted_pair(tmp_path)
        _raw(tmp_path, ("UPDATE claims SET t_invalid = NULL "
                        "WHERE claim_id = ?", (older,)))
        self._blob_the_created_at(tmp_path, newer)
        with mareforma.open(tmp_path, key_path=key) as g:
            served = [c["claim_id"] for c in g.query(refutation_filter="clean")]
        assert older not in served, (
            "a suppressed contradiction was served as a clean claim"
        )


class TestAListingDoesNotHideAClaimInSilence:
    """A fabricated invalidation must fail on every read surface, not most.

    The per-claim surfaces replay and report. The listings put
    ``t_invalid IS NULL`` in SQL, so a fabricated invalidation drained the row
    before any replay could run: one UPDATE on a column with no trigger hid a
    claim from ``query`` and ``search`` alike, and nothing said so.
    """

    def test_a_fabricated_invalidation_is_disclosed(
        self, tmp_path: Path, caplog,
    ) -> None:
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            victim = g.assert_claim("the claim to hide", generated_by="r1")
            g.assert_claim("an untouched claim", generated_by="r2")
        # No verdict behind it: the timestamp is the whole of the attack.
        _raw(tmp_path, "DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering",
             ("UPDATE claims SET t_invalid = ? WHERE claim_id = ?",
              ("2026-01-01T00:00:00+00:00", victim)))

        import logging
        with caplog.at_level(logging.WARNING, logger="mareforma"):
            with mareforma.open(tmp_path, key_path=key) as g:
                served = [c["claim_id"] for c in g.query()]
        assert victim not in served, "premise: the filter hides it"
        assert any("no signed verdict supports" in r.getMessage()
                   for r in caplog.records), caplog.text

    def test_an_honest_invalidation_says_nothing(
        self, tmp_path: Path, caplog,
    ) -> None:
        """The other half, and the one that decides whether this is usable.

        A claim invalidated by a verdict that verifies is honestly hidden, and
        warning about it would cry wolf on every real contradiction.
        """
        key, older, _ = _contradicted_pair(tmp_path)
        import logging
        with caplog.at_level(logging.WARNING, logger="mareforma"):
            with mareforma.open(tmp_path, key_path=key) as g:
                served = [c["claim_id"] for c in g.query()]
        assert older not in served, "premise: a real contradiction hides it"
        assert not any("no signed verdict supports" in r.getMessage()
                       for r in caplog.records), caplog.text


class TestTheErasedDirectionIsDisclosedToo:
    """The erased direction is the one that reads as a clean answer.

    Clearing a real invalidation lets the row pass the SQL filter, so it is
    SERVED. The replay runs on it and reports ``suppressed-verdict``, but that
    reached a health counter and not the person reading the list.
    """

    def test_a_cleared_invalidation_is_disclosed(
        self, tmp_path: Path, caplog,
    ) -> None:
        import logging

        key, older, _ = _contradicted_pair(tmp_path)
        _raw(tmp_path, "DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering",
             ("UPDATE claims SET t_invalid = NULL WHERE claim_id = ?", (older,)))
        with caplog.at_level(logging.WARNING, logger="mareforma"):
            with mareforma.open(tmp_path, key_path=key) as g:
                served = [c["claim_id"] for c in g.query()]
        assert older in served, "premise: clearing it puts the row back in the list"
        assert any("signed verdicts contradict" in r.getMessage()
                   for r in caplog.records), caplog.text

    def test_an_honest_graph_says_nothing(self, tmp_path: Path, caplog) -> None:
        """The premise. A real contradiction must not warn on every read."""
        import logging

        key, _, _ = _contradicted_pair(tmp_path)
        with caplog.at_level(logging.WARNING, logger="mareforma"):
            with mareforma.open(tmp_path, key_path=key) as g:
                g.query()
        assert not any("signed verdicts contradict" in r.getMessage()
                       for r in caplog.records), caplog.text


class TestWithoutAConnection:
    def test_the_pure_form_still_answers_off_the_column(self) -> None:
        """Every existing caller passes a row and nothing else, and keeps its
        answer. What changes is that the answer no longer claims more than a
        column read: the signal says the verdicts were not replayed."""
        row = {"claim_id": "c1", "status": "active",
               "t_invalid": "2026-01-01T00:00:00+00:00"}
        ref = refutation_status(row)
        assert ref["state"] == "contradicted"
        assert ref["signal"] == "invalidation-recorded"
        assert ref["signal"] not in REPLAY_TAMPER_SIGNALS

    def test_a_partial_row_is_still_refused(self) -> None:
        with pytest.raises(ValueError):
            refutation_status({"t_invalid": None})

    def test_a_row_with_no_claim_id_falls_through_to_the_column(
        self, tmp_path: Path,
    ) -> None:
        """The replay needs an id to look up. A hand-built row without one gets
        the column answer rather than a crash, and says so."""
        conn = open_db(tmp_path)
        try:
            ref = refutation_status(
                {"status": "active", "t_invalid": "2026-01-01T00:00:00+00:00"},
                conn,
            )
        finally:
            conn.close()
        assert ref["signal"] == "invalidation-recorded"


class TestTheOrdinaryPathStaysCheap:
    def test_a_clean_claim_needs_no_verdict_work(self, tmp_path: Path) -> None:
        """The replay returns nothing to say and the status flags answer.

        Worth pinning because the common case is every claim in a graph, and a
        replay that reported a state here would put signature verification on
        the path of every clean read.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        conn = open_db(tmp_path)
        try:
            replay = replay_contradictions(conn, cid)
            assert replay == {"backed": False, "unverifiable": (), "checked": 0}
            assert refutation_status(get_claim(conn, cid), conn)["signal"] == "none"
        finally:
            conn.close()

    def test_the_lookup_uses_an_index_on_both_sides(self, tmp_path: Path) -> None:
        """Otherwise every clean read scans the whole verdict table.

        A verdict names a claim on either side, so the replay asks
        ``member_claim_id = ? OR other_claim_id = ?``. Only member_claim_id was
        indexed, and an OR the planner cannot satisfy from one index defeats the
        index it has: measured, the plan was SCAN contradiction_verdicts on
        every clean read. Nothing at ten verdicts and linear in a graph that
        argues with itself.
        """
        conn = open_db(tmp_path)
        try:
            plan = " ".join(str(r[-1]) for r in conn.execute(
                "EXPLAIN QUERY PLAN SELECT verdict_id FROM "
                "contradiction_verdicts WHERE member_claim_id = ? "
                "OR other_claim_id = ?", ("x", "x")))
        finally:
            conn.close()
        assert "SCAN" not in plan, plan
        assert "idx_contradiction_member" in plan
        assert "idx_contradiction_other" in plan

    def test_an_existing_graph_gains_the_index(self, tmp_path: Path) -> None:
        """It rides the additive script, not the fresh-database schema.

        Next to its table in _SCHEMA_SQL it would only ever reach graphs
        created after this, and the scan would stay for everyone who already
        had one.
        """
        open_db(tmp_path).close()
        raw = sqlite3.connect(_db(tmp_path))
        raw.execute("DROP INDEX idx_contradiction_other")
        raw.commit()
        raw.close()

        open_db(tmp_path).close()
        raw = sqlite3.connect(_db(tmp_path))
        try:
            assert raw.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' "
                "AND name = 'idx_contradiction_other'").fetchone()[0] == 1
        finally:
            raw.close()

    def test_editorial_states_are_untouched(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
            g.update_claim(cid, status="retracted")
            ref = g.refutation_status(cid)
        assert ref["state"] == "retracted"
        assert ref["signal"] == "editorial"


class TestTheVerdictReadsIt:
    """A tamper report the exit code ignores is a tamper report nobody sees.

    ``mareforma verify`` is what a CI gate runs, and its exit code is the only
    thing most callers ever read. The map said TAMPERED and the verdict exited 0
    beside it, which is the gap that made the whole contestation axis advisory.

    Only the per-claim signals reach the verdict. Each is a statement about this
    claim's own verdict record, checked and failed, which is what separates a
    definite NO from missing material in this module.
    """

    def test_an_unbacked_invalidation_fails_the_verdict(
        self, tmp_path: Path,
    ) -> None:
        from mareforma._verify import TAMPERED, classify_claim_verdict

        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim nobody contradicted")
        _raw(tmp_path, ("UPDATE claims SET t_invalid = '2026-01-01T00:00:00+00:00' "
                        "WHERE claim_id = ?", (cid,)))

        conn = open_db(tmp_path)
        try:
            verdict = classify_claim_verdict(conn, get_claim(conn, cid), cid)
        finally:
            conn.close()
        assert verdict.verdict == TAMPERED
        assert "unbacked-invalidation" in verdict.reason

    def test_a_suppressed_verdict_fails_the_verdict(self, tmp_path: Path) -> None:
        from mareforma._verify import TAMPERED, classify_claim_verdict

        key, older, _ = _contradicted_pair(tmp_path)
        _raw(tmp_path, ("UPDATE claims SET t_invalid = NULL WHERE claim_id = ?",
                        (older,)))

        conn = open_db(tmp_path)
        try:
            verdict = classify_claim_verdict(conn, get_claim(conn, older), older)
        finally:
            conn.close()
        assert verdict.verdict == TAMPERED
        assert "suppressed-verdict" in verdict.reason

    def test_an_honest_contradiction_does_not_fail_the_verdict(
        self, tmp_path: Path,
    ) -> None:
        """A claim that was genuinely refuted is not a claim that was tampered
        with. The signed verdict standing against it is the system working."""
        from mareforma._verify import TAMPERED, classify_claim_verdict

        key, older, _ = _contradicted_pair(tmp_path)
        conn = open_db(tmp_path)
        try:
            verdict = classify_claim_verdict(conn, get_claim(conn, older), older)
        finally:
            conn.close()
        assert verdict.verdict != TAMPERED

    def test_a_clean_claim_still_verifies(self, tmp_path: Path) -> None:
        from mareforma._verify import VERIFIED, classify_claim_verdict

        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        conn = open_db(tmp_path)
        try:
            assert classify_claim_verdict(conn, get_claim(conn, cid), cid).verdict == VERIFIED
        finally:
            conn.close()


class TestTheSubstrateReachesTheVerdictAsMissingMaterial:
    """A broken file is not proof this claim is bad, and not nothing either.

    A dropped write guard or a planted second root says evidence around the
    claim could have been removed with nothing left to say so. The claim's own
    signature may be perfect, so calling it TAMPERED accuses it of something
    nothing checked; leaving the verdict at 0 tells a CI gate the file is fine.
    UNVERIFIABLE is the reading that is true: something is missing, and no
    per-claim answer from this file is worth more than that.
    """

    def test_a_dropped_guard_makes_the_verdict_unverifiable(
        self, tmp_path: Path,
    ) -> None:
        from mareforma._verify import UNVERIFIABLE, classify_claim_verdict

        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        _raw(tmp_path, "DROP TRIGGER contradiction_verdicts_no_delete")

        conn = open_db(tmp_path)
        try:
            verdict = classify_claim_verdict(conn, get_claim(conn, cid), cid)
        finally:
            conn.close()
        assert verdict.verdict == UNVERIFIABLE
        assert "contradiction_verdicts_no_delete" in verdict.reason

    def test_it_does_not_read_as_tampered(self, tmp_path: Path) -> None:
        """The distinction is the whole point of the two exit codes."""
        from mareforma._verify import TAMPERED, classify_claim_verdict

        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        _raw(tmp_path, "DROP TRIGGER contradiction_verdicts_no_delete")

        conn = open_db(tmp_path)
        try:
            assert classify_claim_verdict(
                conn, get_claim(conn, cid), cid).verdict != TAMPERED
        finally:
            conn.close()

    def test_the_verdict_is_the_same_without_a_trust_map(
        self, tmp_path: Path,
    ) -> None:
        """Computed rather than read off the map, so asking for one cannot
        change the answer."""
        from mareforma._verify import classify_claim_verdict

        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        _raw(tmp_path, "DROP TRIGGER contradiction_verdicts_no_delete")

        conn = open_db(tmp_path)
        try:
            row = get_claim(conn, cid)
            with_map = classify_claim_verdict(conn, row, cid)
            without = classify_claim_verdict(conn, row, cid, with_trust_map=False)
        finally:
            conn.close()
        assert with_map.verdict == without.verdict
        assert without.trust_map is None


class TestACleanListingIsClean:
    """The SQL filter can only read t_invalid, and t_invalid carries no trigger.

    So a real contradiction erased from that column reads as clean to every
    statement in the file, and a caller asking for clean claims gets the
    suppressed one back. Replaying in SQL is not available (it is per-row
    crypto), and replaying the whole scan would cost in proportion to the graph
    rather than the page, so the check runs on the rows already being
    materialised toward the limit.
    """

    def test_a_suppressed_claim_is_not_served_as_clean(
        self, tmp_path: Path,
    ) -> None:
        key, older, _ = _contradicted_pair(tmp_path)
        _raw(tmp_path, ("UPDATE claims SET t_invalid = NULL WHERE claim_id = ?",
                        (older,)))

        with mareforma.open(tmp_path, key_path=key) as g:
            served = {c["claim_id"] for c in g.query(refutation_filter="clean")}
        assert older not in served

    def test_the_honest_claims_are_still_served(self, tmp_path: Path) -> None:
        key, older, newer = _contradicted_pair(tmp_path)
        _raw(tmp_path, ("UPDATE claims SET t_invalid = NULL WHERE claim_id = ?",
                        (older,)))

        with mareforma.open(tmp_path, key_path=key) as g:
            served = {c["claim_id"] for c in g.query(refutation_filter="clean")}
        assert newer in served

    def test_an_unfiltered_listing_is_untouched(self, tmp_path: Path) -> None:
        """The replay only runs where the answer can change what is served.

        Without the filter the caller has not said clean claims only, so
        dropping the row would be this function deciding for them, and paying
        for the decision on every listing in the graph.
        """
        key, older, _ = _contradicted_pair(tmp_path)
        _raw(tmp_path, ("UPDATE claims SET t_invalid = NULL WHERE claim_id = ?",
                        (older,)))

        with mareforma.open(tmp_path, key_path=key) as g:
            served = {c["claim_id"] for c in g.query()}
        assert older in served

    def test_a_graph_with_no_verdicts_serves_everything(
        self, tmp_path: Path,
    ) -> None:
        """The ordinary case, where the replay finds nothing and costs a lookup."""
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            with g.defer_backup():
                ids = {g.assert_claim(f"claim {i}") for i in range(20)}
            served = {c["claim_id"] for c in g.query(refutation_filter="clean",
                                                     limit=50)}
        assert ids <= served

    def test_the_disagreement_is_disclosed_even_unfiltered(
        self, tmp_path: Path,
    ) -> None:
        """A caller who did not ask for clean claims still has to be told.

        Dropping the row from an unfiltered listing would be the read deciding
        what the caller meant. Counting it nowhere would be the silence: a
        contradiction record the signed verdicts do not support, served as an
        ordinary row, with the only trace being a field nobody reads.
        """
        seen: list = []
        key, older, _ = _contradicted_pair(tmp_path)
        _raw(tmp_path, ("UPDATE claims SET t_invalid = NULL WHERE claim_id = ?",
                        (older,)))
        conn = open_db(tmp_path)
        try:
            from mareforma.db.core import query_claims

            rows = query_claims(conn, limit=20,
                                on_contested=lambda n: seen.append(n))
        finally:
            conn.close()
        assert older in {r["claim_id"] for r in rows}      # served, not withheld
        assert seen and sum(seen) >= 1                     # and disclosed

    def test_search_discloses_it_the_same_way_query_does(
        self, tmp_path: Path,
    ) -> None:
        """Two read surfaces, one graph, and they must not answer differently.

        Both go through the same projection, which replays the signed verdicts
        and hands the contested count back to each. query passed it on. search
        bound it to a local and dropped it, so ``on_contested`` sat in its
        signature with nothing to call it, and the graph handle wired a live
        callback into it that could never fire. A claim whose contradiction is
        signed and whose t_invalid somebody erased was served by search in
        silence and by query with a disclosure, in the same process.

        The whole class above covers query. That is why this survived.
        """
        from mareforma.db.core import query_claims, search_claims

        key, older, _ = _contradicted_pair(tmp_path)
        _raw(tmp_path, ("UPDATE claims SET t_invalid = NULL WHERE claim_id = ?",
                        (older,)))
        from_query: list = []
        from_search: list = []
        conn = open_db(tmp_path)
        try:
            q = query_claims(conn, limit=20,
                             on_contested=lambda n: from_query.append(n))
            s = search_claims(conn, "claim", limit=20,
                              on_contested=lambda n: from_search.append(n))
        finally:
            conn.close()
        assert older in {r["claim_id"] for r in q}
        assert older in {r["claim_id"] for r in s}, (
            "the fixture must have search serve the suppressed row, or this "
            "tests nothing"
        )
        assert sum(from_query) >= 1
        assert sum(from_search) >= 1, (
            "search served a contested row and told the caller nothing"
        )

    def test_it_is_not_counted_as_an_exclusion(self, tmp_path: Path) -> None:
        """A served row filed under "excluded" is a false sentence in the
        health record, and it inflates a count that answers a different
        question: how much of the list is missing."""
        excluded: list = []
        served: list = []
        key, older, _ = _contradicted_pair(tmp_path)
        _raw(tmp_path, ("UPDATE claims SET t_invalid = NULL WHERE claim_id = ?",
                        (older,)))
        conn = open_db(tmp_path)
        try:
            from mareforma.db.core import query_claims

            rows = query_claims(conn, limit=20,
                                on_verify_excluded=lambda n: excluded.append(n),
                                on_contested=lambda n: served.append(n))
        finally:
            conn.close()
        assert older in {r["claim_id"] for r in rows}
        assert sum(served) >= 1, "the fixture served no contested row"
        assert excluded == []


class TestTheRowOnlyFormIsDeprecated:
    """It answers off a column no trigger guards, and says so in `signal`.

    A caller reading only `state` cannot see that, which is what the warning is
    for. Suppressed suite-wide in pyproject because the tests above exercise the
    signature on purpose; escalated back to an error here so the suppression
    cannot quietly outlive the warning.
    """

    def test_it_warns(self) -> None:
        import warnings

        row = {"claim_id": "c1", "status": "active", "t_invalid": None}
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            with pytest.raises(DeprecationWarning, match="without a connection"):
                refutation_status(row)

    def test_the_warning_is_attributed_to_the_caller(self) -> None:
        """Emitting it is not the same as anyone seeing it.

        Python shows a DeprecationWarning by default only when it is attributed
        to ``__main__``. At the wrong stacklevel this one pointed at core.py,
        inside the library, so the default filter swallowed it and the row-only
        form would have been removed with no notice ever given. The test above
        cannot see that: it forces the filter to error, which fires whatever the
        attribution is.
        """
        import warnings

        row = {"claim_id": "c1", "status": "active", "t_invalid": None}
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            refutation_status(row)
        assert caught, "no warning was emitted at all"
        assert caught[0].filename == __file__, (
            f"attributed to {caught[0].filename}, not the calling file, so "
            "the default filter hides it"
        )

    def test_passing_a_connection_does_not(self, tmp_path: Path) -> None:
        import warnings

        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("a claim")
        conn = open_db(tmp_path)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("error", DeprecationWarning)
                refutation_status(get_claim(conn, cid), conn)
        finally:
            conn.close()

    def test_the_pure_column_path_does_not_warn(self) -> None:
        """_assemble is pure by contract and holds no graph. That is a
        legitimate absence, not a caller who should have passed one."""
        import warnings

        from mareforma.db.core import refutation_from_column

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            out = refutation_from_column(
                {"claim_id": "c1", "status": "active", "t_invalid": None})
        assert out["signal"] == "none"
