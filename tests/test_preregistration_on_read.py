"""Pre-registration is re-derived on read, not taken off the column.

``predictions.preregistered`` is deliberately excluded from the
content-addressed ``plan_id`` and no signature covers it, so on its own the flag
is an assertion. What makes it mean something lives at write time:
``assert_finding`` refuses to honor a ``preregistered = 1`` plan that was
registered after its run had already produced findings, because a rule chosen
once the outcomes are in view is not a pre-registration whatever the column
says.

Nothing re-derived that on read. ``post_hoc`` was computed straight off the
flag, so a stored 1 read back as though the refusal had been applied to it, and
the only thing standing between a post-hoc gate and a pre-registered label was
the ``predictions_append_only`` trigger. Every term of the write rule is in the
database, so the read asks the same question now.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import mareforma
from tests._helpers import _bootstrap_key, _est, _pred, _prop, _verdict

_CLAUDE = "claude-3-5-sonnet-20241022"


def _drop_guard(root: Path) -> None:
    """Remove the write guard, the way a process with SQL access would.

    The census records this and the next open puts it back, which is the point
    of that machinery. What it does not do is undo an edit made while the guard
    was down, and this is the edit.
    """
    raw = sqlite3.connect(root / ".mareforma" / "graph.db")
    raw.execute("DROP TRIGGER predictions_append_only")
    raw.commit()
    raw.close()


def _flip_to_preregistered(root: Path, content_id: str) -> None:
    raw = sqlite3.connect(root / ".mareforma" / "graph.db")
    raw.execute("UPDATE predictions SET preregistered = 1 WHERE content_id = ?",
                (content_id,))
    raw.commit()
    raw.close()


class TestTheHonestCases:
    def test_a_one_shot_plan_is_post_hoc(self, tmp_path: Path) -> None:
        prop = _prop()
        key = _bootstrap_key(tmp_path, "k.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_finding(prop, _pred(), _est(), data_id="d1",
                             generated_by="r1", grounding=_verdict(_CLAUDE))
            assert g.proposition_status(prop.content_id())["post_hoc"] is True

    def test_a_plan_registered_before_the_run_ran_is_not(
        self, tmp_path: Path,
    ) -> None:
        prop = _prop()
        key = _bootstrap_key(tmp_path, "k.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.register_plan(prop, _pred())
            g.submit_finding(prop, _pred(), _est(), data_id="d2",
                             generated_by="r2", grounding=_verdict(_CLAUDE))
            assert g.proposition_status(prop.content_id())["post_hoc"] is False


class TestTheFlipBuysNothing:
    """The attack the write guard was the only thing standing in front of."""

    def test_a_flipped_flag_is_still_post_hoc_on_read(
        self, tmp_path: Path,
    ) -> None:
        prop = _prop()
        key = _bootstrap_key(tmp_path, "k.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_finding(prop, _pred(), _est(), data_id="d1",
                             generated_by="r1", grounding=_verdict(_CLAUDE))
            assert g.proposition_status(prop.content_id())["post_hoc"] is True

        _drop_guard(tmp_path)
        _flip_to_preregistered(tmp_path, prop.content_id())

        with mareforma.open(tmp_path, key_path=key) as g:
            assert g.proposition_status(prop.content_id())["post_hoc"] is True

    def test_the_flag_really_did_flip(self, tmp_path: Path) -> None:
        """Otherwise the test above passes for the wrong reason."""
        prop = _prop()
        key = _bootstrap_key(tmp_path, "k.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_finding(prop, _pred(), _est(), data_id="d1",
                             generated_by="r1", grounding=_verdict(_CLAUDE))
        _drop_guard(tmp_path)
        _flip_to_preregistered(tmp_path, prop.content_id())

        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        try:
            stored = raw.execute(
                "SELECT preregistered FROM predictions WHERE content_id = ?",
                (prop.content_id(),),
            ).fetchone()[0]
        finally:
            raw.close()
        assert stored == 1

    def test_the_census_reports_the_dropped_guard_too(
        self, tmp_path: Path,
    ) -> None:
        """Belt and braces, and they cover different things.

        The read no longer trusts the flag, and the substrate axis says the
        guard was down. Neither makes the other redundant: the axis cannot say
        what was edited while it was gone, and the re-derivation cannot say
        that anything was.
        """
        prop = _prop()
        key = _bootstrap_key(tmp_path, "k.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_finding(prop, _pred(), _est(), data_id="d1",
                             generated_by="r1", grounding=_verdict(_CLAUDE))
            cid = g._conn.execute(
                "SELECT claim_id FROM findings LIMIT 1").fetchone()[0]
        _drop_guard(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            root = g.trust_map(cid).get("trust_root")
        assert root.value == "TAMPERED"
        assert "predictions_append_only" in root.residual


class TestTheTermsOfTheRule:
    def test_a_flag_with_no_registration_time_says_nothing(
        self, tmp_path: Path,
    ) -> None:
        """``registered_at`` is NOT NULL in the schema, so this is the shape a
        row reaches only by being written around it. A claim of
        pre-registration with no time behind it cannot be checked against the
        run, so it is not honored."""
        from mareforma.trust._gate import _preregistration_holds
        from mareforma.trust._gate import GateCache

        row = {"preregistered": 1, "plan_registered_at": None,
               "generated_by": "r1", "plan_id": "p1"}
        assert _preregistration_holds(None, row, GateCache()) is False

    def test_a_plan_with_no_signed_attestation_is_not_honored(
        self, tmp_path: Path,
    ) -> None:
        """A predictions row planted straight through SQL brings no attestation.

        register_plan writes one as an ordinary signed claim; nothing else
        does, so this is the term a writer with SQL access cannot satisfy
        without the project's key."""
        from mareforma.trust._gate import GateCache, _preregistration_holds
        from mareforma.db.core import open_db

        conn = open_db(tmp_path)
        try:
            row = {"preregistered": 1,
                   "plan_registered_at": "2026-01-01T00:00:00+00:00",
                   "generated_by": "a-run-with-no-findings",
                   "plan_id": "no-such-plan"}
            # No attestation for that plan_id, so the second term refuses it
            # before the timing term is reached. The timing term has its own
            # test below, on a plan that satisfies the first two.
            assert _preregistration_holds(conn, row, GateCache()) is False
        finally:
            conn.close()

    def test_a_plan_registered_after_its_run_started_is_not_honored(
        self, tmp_path: Path,
    ) -> None:
        """The timing rule the write path applies, asked again on read.

        ``assert_finding`` refuses a plan registered once its run was already
        producing findings, so this row reaches a reader only by being written
        around that refusal. The flag is set, the attestation is real and the
        registration time is honest; what fails is the comparison against the
        run. Without this term the read hands back a rule chosen with the
        outcomes in view as though the refusal had been honored.
        """
        from mareforma.db.core import open_db
        from mareforma.trust import Direction, Proposition
        from mareforma.trust._gate import GateCache, _preregistration_holds

        late = Proposition(
            subject="BRCA2", relation="affects", object="tumour growth",
            direction=Direction.DECREASES,
            scope={"population": "TNBC", "condition": "in vitro"},
        )
        key = _bootstrap_key(tmp_path, "k.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            # Run r1 is already emitting findings before the plan exists.
            g.assert_finding(_prop(), _pred(), _est(), data_id="d1",
                             generated_by="r1", grounding=_verdict(_CLAUDE))
            g.register_plan(late, _pred())

        raw = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        raw.row_factory = sqlite3.Row
        plan = raw.execute(
            "SELECT plan_id, registered_at FROM predictions "
            "WHERE content_id = ?", (late.content_id(),),
        ).fetchone()
        raw.close()

        # The run token is the one thing moved: this plan's evidence is claimed
        # for a run that started before the plan was written.
        row = {"preregistered": 1,
               "plan_registered_at": plan["registered_at"],
               "generated_by": "r1", "plan_id": plan["plan_id"]}
        conn = open_db(tmp_path)
        try:
            assert _preregistration_holds(conn, row, GateCache()) is False
        finally:
            conn.close()

    def test_an_attestation_written_after_the_row_does_not_honor_it(
        self, tmp_path: Path,
    ) -> None:
        """The attestation has to be the one that created the row.

        A one-shot registers and executes in the same breath, so the timing term
        passes on its own and the flag is the only thing left. Registering the
        same prediction later supplies an attestation for that plan_id through
        the public API with nothing tampered, and the flag then rides in through
        a backup, which replays the column raw where the live path refuses the
        same edit. ``register_plan`` commits its claim before the row, so a
        genuine pre-registration never attests after its own registered_at.
        """
        from mareforma.trust._gate import GateCache, _preregistration_holds
        from mareforma.db.core import open_db

        prop = _prop()
        key = _bootstrap_key(tmp_path, "k.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_finding(prop, _pred(), _est(), data_id="d1",
                             generated_by="r1", grounding=_verdict(_CLAUDE))
            assert g.proposition_status(prop.content_id())["post_hoc"] is True
            # The same prediction, registered after the fact.
            g.register_plan(prop, _pred())

        conn = open_db(tmp_path)
        try:
            row = conn.execute(
                "SELECT plan_id, registered_at FROM predictions "
                "WHERE content_id = ?", (prop.content_id(),),
            ).fetchone()
            # The flag as a backup would carry it: raised, everything else as
            # the one-shot wrote it.
            planted = {"preregistered": 1,
                       "plan_registered_at": row["registered_at"],
                       "generated_by": "r1",
                       "plan_id": row["plan_id"]}
            assert _preregistration_holds(conn, planted, GateCache()) is False
        finally:
            conn.close()

    def test_the_flag_still_has_to_be_set(self, tmp_path: Path) -> None:
        from mareforma.trust._gate import GateCache, _preregistration_holds

        row = {"preregistered": 0,
               "plan_registered_at": "2026-01-01T00:00:00+00:00",
               "generated_by": "r1", "plan_id": "p1"}
        assert _preregistration_holds(None, row, GateCache()) is False
