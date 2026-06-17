"""tests/epistemic/test_plan_finding.py: the register_plan / submit_finding split.

v0.3.5 splits the one-shot ``assert_finding`` into two earned steps:

  register_plan(prop, prediction)            -> pre-registers the decision rule
  submit_finding(prop, prediction, estimate) -> binds an outcome to that plan

The plan attestation is its own signed claim; the finding's signed ``supports[]``
cites the plan claim, so the plan -> finding edge is cryptographic, not just
denormalised metadata. ``assert_finding`` is preserved as a one-shot that
composes the two internally (and flags its synthesised plan ``preregistered=0``).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from mareforma.trust import (
    Direction,
    DirectionOfInterest,
    EffectEstimate,
    EffectType,
    FindingPlanForkError,
    NoRegisteredPlanError,
    NonFalsifiablePropositionError,
    Status,
)
from mareforma.trust import _store

from tests.epistemic._builders import _prop, _smd, _superiority, open_graph


# ---------------------------------------------------------------------------
# register_plan
# ---------------------------------------------------------------------------

class TestRegisterPlan:
    def test_happy_path_row_and_claim_land(self, tmp_path: Path) -> None:
        h = _prop()
        pred = _superiority()
        with open_graph(tmp_path) as graph:
            plan_id = graph.register_plan(h, pred, generated_by="lab_a")
            # The predictions row landed with preregistered=1.
            row = graph._conn.execute(
                "SELECT * FROM predictions WHERE plan_id = ?", (plan_id,)
            ).fetchone()
            assert row is not None
            assert row["preregistered"] == 1
            assert row["content_id"] == h.content_id()
            # The proposition row landed.
            assert graph.get_proposition(h.content_id()) is not None
            # Its own signed plan attestation landed under plan:{plan_id}.
            claim_id = _store.get_plan_claim_id(graph._conn, plan_id)
            assert claim_id is not None
            claim = graph.get_claim(claim_id)
            assert claim["idempotency_key"] == f"plan:{plan_id}"
            assert claim["signature_bundle"]  # signed like any other claim
            # No finding yet: the proposition is a dangling (registered) plan.
            assert graph.proposition_status(h)["status"] == Status.UNTESTED.value

    def test_plan_id_is_independent_of_preregistered_flag(self, tmp_path: Path) -> None:
        h = _prop()
        # Same rule, different preregistered flag -> same identity.
        a = _superiority(preregistered=True)
        b = _superiority(preregistered=False)
        cid = h.content_id()
        assert _store.compute_plan_id(cid, a) == _store.compute_plan_id(cid, b)

    def test_idempotent_reregister_no_duplicate(self, tmp_path: Path) -> None:
        h = _prop()
        pred = _superiority()
        with open_graph(tmp_path) as graph:
            p1 = graph.register_plan(h, pred, generated_by="lab_a")
            p2 = graph.register_plan(h, pred, generated_by="lab_a")
            assert p1 == p2
            n_rows = graph._conn.execute(
                "SELECT COUNT(*) AS c FROM predictions WHERE plan_id = ?", (p1,)
            ).fetchone()["c"]
            n_claims = graph._conn.execute(
                "SELECT COUNT(*) AS c FROM claims WHERE idempotency_key = ?",
                (f"plan:{p1}",),
            ).fetchone()["c"]
        assert n_rows == 1
        assert n_claims == 1

    def test_non_falsifiable_raises(self, tmp_path: Path) -> None:
        from mareforma.trust import Proposition
        bad = Proposition("X", "relates to", "Y")  # no direction, no scope
        with open_graph(tmp_path) as graph:
            with pytest.raises(NonFalsifiablePropositionError):
                graph.register_plan(bad, _superiority())


# ---------------------------------------------------------------------------
# submit_finding
# ---------------------------------------------------------------------------

class TestSubmitFinding:
    def test_happy_path_bearing_status_and_signed_edge(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        pred = _superiority()
        with open_graph(tmp_path) as graph:
            plan_id = graph.register_plan(h, pred, generated_by="lab_a")
            plan_claim = _store.get_plan_claim_id(graph._conn, plan_id)
            result = graph.submit_finding(
                h, pred, _smd(-2.6, p=0.003, n=842),
                data_id="dataA", generated_by="lab_a",
            )
            assert result["bearing"]["direction"] == "supports"
            assert result["idempotent"] is False
            assert result["plan_id"] == plan_id
            status = graph.proposition_status(h)
            assert status["status"] == Status.PRELIMINARY.value
            # The finding claim's SIGNED supports[] cites the plan claim.
            finding_claim = graph.get_claim(result["claim_id"])
            supports = json.loads(finding_claim["supports_json"])
            assert plan_claim in supports

    def test_no_registered_plan_raises(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        pred = _superiority()
        with open_graph(tmp_path) as graph:
            with pytest.raises(NoRegisteredPlanError):
                graph.submit_finding(
                    h, pred, _smd(-2.6, p=0.003), data_id="dataA", generated_by="a"
                )
            # Nothing was written: no proposition, no finding, no orphan claim.
            assert graph.get_proposition(h.content_id()) is None
            n = graph._conn.execute("SELECT COUNT(*) AS c FROM findings").fetchone()["c"]
            assert n == 0

    def test_idempotent_resubmit_same_dataset(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        pred = _superiority()
        with open_graph(tmp_path) as graph:
            graph.register_plan(h, pred)
            first = graph.submit_finding(
                h, pred, _smd(-2.6, p=0.003), data_id="dataA", generated_by="lab_a"
            )
            again = graph.submit_finding(
                h, pred, _smd(-2.6, p=0.003), data_id="dataA", generated_by="lab_b"
            )
            assert again["idempotent"] is True
            assert again["finding_id"] == first["finding_id"]
            status = graph.proposition_status(h)
            assert status["independent_support"] == 1  # not double-counted

    def test_inconsistent_estimate_raises_before_any_claim(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        pred = _superiority()
        with open_graph(tmp_path) as graph:
            graph.register_plan(h, pred)
            n_before = graph._conn.execute(
                "SELECT COUNT(*) AS c FROM claims"
            ).fetchone()["c"]
            # A superiority CI at the wrong level fails the gate.
            bad = EffectEstimate(2.0, EffectType.MD, ci_lower=0.5, ci_upper=3.5, ci_level=0.50)
            from mareforma.trust import InconsistentEstimateError
            with pytest.raises(InconsistentEstimateError):
                graph.submit_finding(h, _superiority(DirectionOfInterest.INCREASE), bad,
                                     data_id="dataA", generated_by="a")
            n_after = graph._conn.execute(
                "SELECT COUNT(*) AS c FROM claims"
            ).fetchone()["c"]
            assert n_after == n_before  # no finding claim written

    def test_fork_guard_changed_prediction_raises(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        plan_a = _superiority(alpha=0.05)
        plan_b = _superiority(alpha=0.01)  # a different rule -> different plan_id
        with open_graph(tmp_path) as graph:
            graph.register_plan(h, plan_a)
            graph.register_plan(h, plan_b)
            graph.submit_finding(h, plan_a, _smd(-2.6, p=0.003),
                                 data_id="dataA", generated_by="lab_a")
            # Same (content_id, data_id) but a DIFFERENT plan -> raise, not silent.
            with pytest.raises(FindingPlanForkError):
                graph.submit_finding(h, plan_b, _smd(-2.6, p=0.003),
                                     data_id="dataA", generated_by="lab_a")

    def test_two_independent_lines_corroborate(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        pred = _superiority()
        with open_graph(tmp_path) as graph:
            graph.register_plan(h, pred)
            graph.submit_finding(h, pred, _smd(-2.6, p=0.003), data_id="dataA", generated_by="lab_a")
            graph.submit_finding(h, pred, _smd(-2.4, p=0.01), data_id="dataB", generated_by="lab_b")
            status = graph.proposition_status(h)
        assert status["independent_support"] == 2
        assert status["status"] == Status.CORROBORATED.value

    def test_dangling_plan_is_untested(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        with open_graph(tmp_path) as graph:
            graph.register_plan(h, _superiority())
            status = graph.proposition_status(h)
        assert status is not None
        assert status["status"] == Status.UNTESTED.value
        assert status["independent_support"] == 0

    def test_concurrent_fork_does_not_strand_claim(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A fork seen only in-transaction (a concurrent writer landed a finding
        under a different plan between the pre-flight check and the write) must
        roll the finding claim back, never strand it on the chain."""
        h = _prop(Direction.DECREASES)
        pred = _superiority()
        with open_graph(tmp_path) as graph:
            graph.register_plan(h, pred)
            before = graph._conn.execute(
                "SELECT COUNT(*) FROM claims"
            ).fetchone()[0]

            calls = {"n": 0}

            def racing_find(conn, content_id, data_id):
                # Pre-flight sees nothing; the in-transaction re-check sees a
                # finding a concurrent writer landed under a DIFFERENT plan.
                calls["n"] += 1
                if calls["n"] == 1:
                    return None
                return {
                    "plan_id": "deadbeef" * 8,
                    "finding_id": "concurrent",
                    "claim_id": "concurrent-claim",
                }

            monkeypatch.setattr(_store, "find_existing_finding", racing_find)
            with pytest.raises(FindingPlanForkError):
                graph.submit_finding(
                    h, pred, _smd(-2.6, p=0.003),
                    data_id="dataA", generated_by="lab_a",
                )
            monkeypatch.undo()
            after = graph._conn.execute(
                "SELECT COUNT(*) FROM claims"
            ).fetchone()[0]
        # The refused fork wrote no claim: the transaction rolled it back.
        assert after == before

    def test_post_claim_write_failure_rolls_back_claim(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A failure AFTER the signed claim is written (here: insert_finding
        raises) must roll the claim back, not leave it committed and stranded.
        Regression: convergence detection used to commit the claim
        mid-transaction, so submit_finding's rollback was a no-op for it."""
        h = _prop(Direction.DECREASES)
        pred = _superiority()
        with open_graph(tmp_path) as graph:
            graph.register_plan(h, pred)
            before = graph._conn.execute(
                "SELECT COUNT(*) FROM claims"
            ).fetchone()[0]

            def boom(*args, **kwargs):
                raise RuntimeError("simulated failure after the claim write")

            monkeypatch.setattr(_store, "insert_finding", boom)
            with pytest.raises(RuntimeError, match="after the claim write"):
                graph.submit_finding(
                    h, pred, _smd(-2.6, p=0.003),
                    data_id="dataA", generated_by="lab_a",
                )
            monkeypatch.undo()
            after = graph._conn.execute(
                "SELECT COUNT(*) FROM claims"
            ).fetchone()[0]
        # The claim was written inside submit_finding's transaction; the
        # post-write failure rolled it back rather than committing under it.
        assert after == before


class TestSignedEdgeRoundTrip:
    def test_plan_to_finding_supports_edge_verifies(self, tmp_path: Path) -> None:
        """The plan->finding edge lives in the signed supports[], so a full
        sign/verify round-trip proves it (a row-level tamper would break it)."""
        from mareforma import signing as _signing
        key_path = tmp_path / "_test_key"
        h = _prop(Direction.DECREASES)
        pred = _superiority()
        with open_graph(tmp_path) as graph:
            plan_id = graph.register_plan(h, pred)
            plan_claim = _store.get_plan_claim_id(graph._conn, plan_id)
            result = graph.submit_finding(h, pred, _smd(-2.6, p=0.003),
                                          data_id="dataA", generated_by="lab_a")
            finding_claim = graph.get_claim(result["claim_id"])
            bundle = json.loads(finding_claim["signature_bundle"])
        # The envelope verifies cryptographically...
        verifier_key = _signing.load_private_key(key_path).public_key()
        assert _signing.verify_envelope(bundle, verifier_key) is True
        # ...and its signed predicate carries the plan->finding edge.
        predicate = _signing.claim_predicate_from_envelope(bundle)
        assert plan_claim in predicate["supports"]


# ---------------------------------------------------------------------------
# assert_finding one-shot regression: same shape, atomic, idempotent
# ---------------------------------------------------------------------------

class TestAssertFindingRegression:
    def test_one_shot_shape_preserved(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        with open_graph(tmp_path) as graph:
            result = graph.assert_finding(
                h, _superiority(), _smd(-2.6, p=0.003, n=842),
                data_id="dataA", generated_by="lab_a",
            )
        # Exact return shape preserved from v0.3.4.
        assert set(result) == {
            "finding_id", "content_id", "plan_id", "claim_id",
            "bearing", "status", "idempotent", "proposition_status",
        }
        assert result["bearing"]["direction"] == "supports"
        assert result["idempotent"] is False
        assert result["status"] == Status.PRELIMINARY.value

    def test_one_shot_synthesised_plan_is_not_preregistered(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        with open_graph(tmp_path) as graph:
            result = graph.assert_finding(
                h, _superiority(), _smd(-2.6, p=0.003),
                data_id="dataA", generated_by="lab_a",
            )
            row = graph._conn.execute(
                "SELECT preregistered FROM predictions WHERE plan_id = ?",
                (result["plan_id"],),
            ).fetchone()
        assert row["preregistered"] == 0  # one-shot, not a real pre-registration

    def test_one_shot_bad_estimate_leaves_nothing(self, tmp_path: Path) -> None:
        """A rejected one-shot finding writes no proposition, plan, or claim —
        v0.3.4's all-or-nothing behaviour is preserved through the refactor."""
        from mareforma.trust import InconsistentEstimateError
        h = _prop(Direction.DECREASES)
        bad = EffectEstimate(2.0, EffectType.MD, ci_lower=0.5, ci_upper=3.5, ci_level=0.50)
        with open_graph(tmp_path) as graph:
            n_claims_before = graph._conn.execute(
                "SELECT COUNT(*) AS c FROM claims"
            ).fetchone()["c"]
            with pytest.raises(InconsistentEstimateError):
                graph.assert_finding(
                    h, _superiority(DirectionOfInterest.INCREASE), bad,
                    data_id="dataA", generated_by="a",
                )
            assert graph.get_proposition(h.content_id()) is None
            assert graph._conn.execute(
                "SELECT COUNT(*) AS c FROM predictions"
            ).fetchone()["c"] == 0
            assert graph._conn.execute(
                "SELECT COUNT(*) AS c FROM claims"
            ).fetchone()["c"] == n_claims_before

    def test_one_shot_idempotent_on_content_and_data(self, tmp_path: Path) -> None:
        h = _prop(Direction.DECREASES)
        with open_graph(tmp_path) as graph:
            a = graph.assert_finding(h, _superiority(), _smd(-2.6, p=0.003),
                                     data_id="dataA", generated_by="lab_a")
            b = graph.assert_finding(h, _superiority(), _smd(-2.6, p=0.003),
                                     data_id="dataA", generated_by="lab_b")
            assert b["idempotent"] is True
            assert b["finding_id"] == a["finding_id"]
            assert b["claim_id"] == a["claim_id"]

    def test_observability_events_emitted(self, tmp_path: Path) -> None:
        """register_plan and submit_finding emit to the health/activity log."""
        h = _prop(Direction.DECREASES)
        pred = _superiority()
        with open_graph(tmp_path) as graph:
            graph.register_plan(h, pred)
            graph.submit_finding(h, pred, _smd(-2.6, p=0.003),
                                 data_id="dataA", generated_by="lab_a")
        log = tmp_path / ".mareforma" / "health.jsonl"
        ops = [json.loads(line)["op"] for line in log.read_text().splitlines() if line.strip()]
        assert "register_plan" in ops
        assert "submit_finding" in ops

    def test_one_shot_then_status_path_matches_two_step(self, tmp_path: Path) -> None:
        """A one-shot finding and a register_plan+submit_finding on the same
        (prop, prediction) land on the same plan_id (preregistered aside)."""
        h = _prop(Direction.DECREASES)
        pred = _superiority()
        with open_graph(tmp_path) as graph:
            one_shot = graph.assert_finding(h, pred, _smd(-2.6, p=0.003),
                                            data_id="dataA", generated_by="lab_a")
            # A second proposition+dataset via the two-step path.
            h2 = _prop(Direction.DECREASES, population="OTHER")
            graph.register_plan(h2, pred)
            two_step = graph.submit_finding(h2, pred, _smd(-2.6, p=0.003),
                                            data_id="dataB", generated_by="lab_b")
        assert one_shot["plan_id"] == _store.compute_plan_id(h.content_id(), pred)
        assert two_step["plan_id"] == _store.compute_plan_id(h2.content_id(), pred)
