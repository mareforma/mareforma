"""tests/epistemic/test_plan_retirement.py: retire a plan the gates cannot run.

A release before the ``(0, 0.5)`` alpha bound could register a plan at, say,
``alpha=0.7``. The graph restores, but the read path rebuilds a ``Prediction``
from the stored columns and refuses that alpha, so every evidence line under the
plan drops out of the counts and the proposition reads UNTESTED. The row is
append-only and cannot be deleted, so there is nothing to correct in place.

``retire_plan`` is the recovery: the operator retires the un-gateable plan and
re-registers the same rule at a gateable alpha. The retirement is a record, the
replacement is a new registration, and the original row is untouched.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest import mock

import pytest

import mareforma
from mareforma.db.errors import RestoreError
from mareforma.trust import (
    NoRegisteredPlanError,
    PlanNotRetirableError,
    Prediction,
    Status,
    TrustError,
)

from tests.epistemic._builders import _prop, _smd, _superiority, open_graph


def _legacy(alpha: float = 0.7) -> Prediction:
    """A superiority plan a release before the ``(0, 0.5)`` bound could write.

    The constructor refuses the alpha today, which is the point: the value can
    only reach the row from an older release, so the test reproduces one by
    setting the field past the constructor, exactly the shape ``git show
    aba7181`` writes.
    """
    pred = _superiority()
    object.__setattr__(pred, "alpha", alpha)
    return pred


def _seed_legacy_finding(tmp_path: Path, *, alpha: float = 0.7):
    """Write one finding under a legacy-alpha plan; return (prop, plan_id).

    A legacy alpha can only reach the predictions row from a release before the
    ``(0, 0.5)`` write-boundary guard, so registering it is what that older
    release did. The guard is suspended for the seed write to reproduce that
    row; the guard itself is exercised directly in
    ``test_the_write_boundary_still_refuses_the_alpha``.
    """
    h = _prop()
    pred = _legacy(alpha)
    with open_graph(tmp_path) as graph:
        with mock.patch("mareforma.trust.prediction.validate_alpha", lambda a: None):
            plan_id = graph.register_plan(h, pred, generated_by="legacy-run")
            graph.submit_finding(
                h, pred, _smd(-0.8, p=0.01), data_id="dataset-A",
                generated_by="legacy-run",
            )
    return h, plan_id


def _health_ops(root: Path) -> list[dict]:
    path = root / ".mareforma" / "health.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


class TestTheDefect:
    def test_a_legacy_alpha_line_drops_out_of_every_recompute(
        self, tmp_path: Path
    ) -> None:
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            view = graph.proposition_status(h)
        assert view["status"] == Status.UNTESTED.value
        assert view["independent_support"] == 0
        assert view["lines_skipped"] == 1

    def test_the_skip_names_the_plan_to_retire(self, tmp_path: Path) -> None:
        """The operator cannot retire a plan whose id no read ever gives them."""
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            graph.proposition_status(h)
        skips = [e for e in _health_ops(tmp_path) if e["op"] == "ungateable_plan_skipped"]
        assert skips
        assert {e["plan_id"] for e in skips} == {plan_id}
        assert {e["content_id"] for e in skips} == {h.content_id()}
        assert {e["outcome"] for e in skips} == {"degraded"}
        # The generic recompute failure is for drift and corruption; a plan an
        # operator can retire is not that.
        assert not [
            e for e in _health_ops(tmp_path) if e["op"] == "bearing_recompute_skipped"
        ]

    def test_the_write_boundary_still_refuses_the_alpha(self) -> None:
        with pytest.raises(ValueError, match=r"alpha must be in \(0, 0.5\)"):
            _superiority(alpha=0.7)

    def test_assert_finding_refuses_a_bypassed_legacy_alpha(
        self, tmp_path: Path
    ) -> None:
        """The one-shot write path re-validates alpha too. register_plan guards its
        row; assert_finding synthesises its own plan, so a rule reaching it past the
        constructor (a frozen-instance bypass, the shape a pre-bound release wrote)
        must be refused there as well, or the un-gateable plan is minted anew rather
        than staying legacy-only."""
        with open_graph(tmp_path) as graph:
            with pytest.raises(ValueError, match=r"alpha must be in \(0, 0.5\)"):
                graph.assert_finding(
                    _prop(), _legacy(), _smd(-0.8, p=0.01), data_id="dataset-A",
                    generated_by="bypass-run",
                )


class TestRetireAndReRegister:
    def test_the_evidence_counts_again_under_the_replacement(
        self, tmp_path: Path
    ) -> None:
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            receipt = graph.retire_plan(
                plan_id, alpha=0.05,
                reason="registered at an alpha the gates cannot run",
            )
            view = graph.proposition_status(h)
        assert receipt["plan_id"] == plan_id
        assert receipt["superseded_by"] != plan_id
        assert receipt["lines_recovered"] == 1
        assert view["status"] == Status.PRELIMINARY.value
        assert view["independent_support"] == 1
        assert view["lines_skipped"] == 0

    def test_the_retired_row_is_neither_rewritten_nor_removed(
        self, tmp_path: Path
    ) -> None:
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            graph.retire_plan(plan_id, alpha=0.05, reason="un-gateable alpha")
            row = graph._conn.execute(
                "SELECT alpha FROM predictions WHERE plan_id = ?", (plan_id,)
            ).fetchone()
            assert row["alpha"] == 0.7
            # The append-only guarantees still hold over the retired row.
            with pytest.raises(sqlite3.IntegrityError, match="prediction_locked"):
                graph._conn.execute(
                    "UPDATE predictions SET alpha = 0.05 WHERE plan_id = ?",
                    (plan_id,),
                )
            with pytest.raises(
                sqlite3.IntegrityError, match="prediction_delete_blocked"
            ):
                graph._conn.execute(
                    "DELETE FROM predictions WHERE plan_id = ?", (plan_id,)
                )

    def test_the_record_carries_what_it_supersedes_and_why(
        self, tmp_path: Path
    ) -> None:
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            receipt = graph.retire_plan(
                plan_id, alpha=0.05, reason="alpha the gates cannot run",
            )
            row = graph._conn.execute(
                "SELECT * FROM plan_retirements WHERE plan_id = ?", (plan_id,)
            ).fetchone()
            assert row["superseded_by"] == receipt["superseded_by"]
            assert row["reason"] == "alpha the gates cannot run"
            # The replacement is not a pre-registration: it was registered
            # once the numbers were already in the graph.
            replacement = graph._conn.execute(
                "SELECT preregistered, alpha, direction_of_interest, test_type "
                "FROM predictions WHERE plan_id = ?", (receipt["superseded_by"],)
            ).fetchone()
            assert replacement["preregistered"] == 0
            assert replacement["alpha"] == 0.05
            # The rule itself is carried over, never re-chosen.
            assert replacement["direction_of_interest"] == "decrease"
            assert replacement["test_type"] == "superiority"
            # The retirement rides a signed claim naming both plans.
            claim = graph.get_claim(row["claim_id"])
            assert plan_id in claim["text"]
            assert receipt["superseded_by"] in claim["text"]
            assert "alpha the gates cannot run" in claim["text"]

    def test_retiring_twice_returns_the_same_record(self, tmp_path: Path) -> None:
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            first = graph.retire_plan(plan_id, alpha=0.05, reason="un-gateable")
            second = graph.retire_plan(plan_id, alpha=0.05, reason="un-gateable")
            assert second["superseded_by"] == first["superseded_by"]
            assert second["claim_id"] == first["claim_id"]
            assert graph._conn.execute(
                "SELECT COUNT(*) AS n FROM plan_retirements"
            ).fetchone()["n"] == 1

    def test_a_retired_plan_cannot_be_re_pointed_at_a_second_alpha(
        self, tmp_path: Path
    ) -> None:
        """Otherwise the operator shops for the alpha that reads best."""
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            graph.retire_plan(plan_id, alpha=0.05, reason="un-gateable")
            with pytest.raises(PlanNotRetirableError, match="already retired"):
                graph.retire_plan(plan_id, alpha=0.2, reason="second thoughts")


class TestRefusals:
    def test_a_plan_the_gates_can_still_run_is_not_retirable(
        self, tmp_path: Path
    ) -> None:
        h = _prop()
        with open_graph(tmp_path) as graph:
            out = graph.assert_finding(
                h, _superiority(), _smd(-0.8, p=0.01), data_id="dA",
                generated_by="run1",
            )
            with pytest.raises(PlanNotRetirableError, match="gates can still run"):
                graph.retire_plan(out["plan_id"], alpha=0.01, reason="tidy up")

    def test_an_unregistered_plan_raises(self, tmp_path: Path) -> None:
        with open_graph(tmp_path) as graph:
            with pytest.raises(NoRegisteredPlanError):
                graph.retire_plan("0" * 64, alpha=0.05, reason="nothing there")

    def test_the_replacement_alpha_must_be_gateable(self, tmp_path: Path) -> None:
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            with pytest.raises(ValueError, match=r"alpha must be in \(0, 0.5\)"):
                graph.retire_plan(plan_id, alpha=0.6, reason="still un-gateable")

    def test_a_retirement_that_would_recover_nothing_is_refused(
        self, tmp_path: Path
    ) -> None:
        """Retirement is one-way, so it must not be spent for no recovery."""
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            # The line's stored estimate is unreadable (the direct/foreign
            # writer case), so no alpha can gate it.
            graph._conn.execute(
                "UPDATE effect_estimates SET estimate_value = 'CORRUPT'"
            )
            graph._conn.commit()
            with pytest.raises(TrustError, match="recover"):
                graph.retire_plan(plan_id, alpha=0.05, reason="repair")

    def test_an_empty_reason_is_refused(self, tmp_path: Path) -> None:
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            with pytest.raises(ValueError, match="reason"):
                graph.retire_plan(plan_id, alpha=0.05, reason="   ")


class TestSubmittingUnderTheReplacement:
    def test_the_same_dataset_is_idempotent_not_a_fork(self, tmp_path: Path) -> None:
        """The dataset already stands under the replacement, by supersession."""
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            graph.retire_plan(plan_id, alpha=0.05, reason="un-gateable alpha")
            out = graph.submit_finding(
                h, _superiority(alpha=0.05), _smd(-0.8, p=0.01),
                data_id="dataset-A", generated_by="legacy-run",
            )
            view = graph.proposition_status(h)
        assert out["idempotent"] is True
        assert view["independent_support"] == 1
        assert view["lines_skipped"] == 0

    def test_a_second_dataset_lands_under_the_replacement(
        self, tmp_path: Path
    ) -> None:
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            receipt = graph.retire_plan(plan_id, alpha=0.05, reason="un-gateable")
            out = graph.submit_finding(
                h, _superiority(alpha=0.05), _smd(-0.9, p=0.02),
                data_id="dataset-B", generated_by="second-run",
            )
            view = graph.proposition_status(h)
        assert out["plan_id"] == receipt["superseded_by"]
        assert view["independent_support"] == 1  # one signer, two datasets
        assert view["lines_skipped"] == 0


class TestReadPathIsFailClosed:
    def test_a_retirement_never_re_points_a_plan_the_gates_can_run(
        self, tmp_path: Path
    ) -> None:
        """A planted retirement must not be able to erase a counted line.

        The trust rows are unsigned in the backup, so a tampered claims.toml
        could plant a retirement row. Resolution only ever applies to a plan the
        gates cannot run, and those lines count zero already, so a planted row
        can move no count that stands.
        """
        h = _prop()
        with open_graph(tmp_path) as graph:
            live = graph.assert_finding(
                h, _superiority(), _smd(-0.8, p=0.01), data_id="dA",
                generated_by="run1",
            )
            # A second, gateable plan to point the planted retirement at: at
            # this alpha the same line reads NEUTRAL, so resolution would drop
            # the support if it ever applied here.
            other = graph.register_plan(h, _superiority(alpha=0.0001))
            graph._conn.execute(
                "INSERT INTO plan_retirements "
                "(plan_id, superseded_by, reason, claim_id, retired_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (live["plan_id"], other, "planted", live["claim_id"],
                 "2026-01-01T00:00:00+00:00"),
            )
            graph._conn.commit()
            view = graph.proposition_status(h)
        assert view["independent_support"] == 1
        assert view["lines_skipped"] == 0


class TestRoundTrip:
    def test_a_retirement_survives_backup_and_restore(self, tmp_path: Path) -> None:
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            graph.retire_plan(plan_id, alpha=0.05, reason="un-gateable alpha")
        (tmp_path / ".mareforma" / "graph.db").unlink()
        mareforma.restore(tmp_path)
        with open_graph(tmp_path) as graph:
            view = graph.proposition_status(h)
            row = graph._conn.execute(
                "SELECT superseded_by, reason FROM plan_retirements WHERE plan_id = ?",
                (plan_id,),
            ).fetchone()
        assert view["status"] == Status.PRELIMINARY.value
        assert view["independent_support"] == 1
        assert row["reason"] == "un-gateable alpha"

    def test_a_reason_carrying_stripped_codepoints_still_restores(
        self, tmp_path: Path
    ) -> None:
        """The stored reason is the one the attestation signs, cleaned once."""
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            graph.retire_plan(
                plan_id, alpha=0.05, reason="  un​gateable alpha  ",
            )
        (tmp_path / ".mareforma" / "graph.db").unlink()
        mareforma.restore(tmp_path)
        with open_graph(tmp_path) as graph:
            assert graph.proposition_status(h)["independent_support"] == 1

    def test_a_rewritten_retirement_fails_restore(self, tmp_path: Path) -> None:
        """The pair the row carries is re-derived from the signed claim text."""
        h, plan_id = _seed_legacy_finding(tmp_path)
        with open_graph(tmp_path) as graph:
            graph.retire_plan(plan_id, alpha=0.05, reason="un-gateable alpha")
        toml_path = tmp_path / "claims.toml"
        toml_path.write_text(
            toml_path.read_text().replace(
                'reason = "un-gateable alpha"', 'reason = "the numbers were wrong"'
            ),
            encoding="utf-8",
        )
        (tmp_path / ".mareforma" / "graph.db").unlink()
        with pytest.raises(RestoreError) as exc:
            mareforma.restore(tmp_path)
        assert exc.value.kind == "claim_unverified"
