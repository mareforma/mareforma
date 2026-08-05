"""tests/test_health.py — unit tests for mareforma/health.py."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from mareforma.db import add_claim, open_db
from mareforma.health import HealthReport, _compute_traffic_light, compute_health


def _open(tmp_path: Path) -> sqlite3.Connection:
    (tmp_path / ".mareforma").mkdir(parents=True, exist_ok=True)
    return open_db(tmp_path)


# ---------------------------------------------------------------------------
# Traffic light state derivation
# ---------------------------------------------------------------------------


class TestTrafficLight:
    def test_red_when_no_claims(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.traffic_light == "red"
        assert "claim" in report.rationale.lower()

    def test_yellow_when_all_preliminary(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Single agent finding")
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.traffic_light == "yellow"
        assert "PRELIMINARY" in report.rationale

    def test_green_when_replicated_claim_exists(self, tmp_path: Path) -> None:
        # Promotion keys on two distinct non-NULL asserter_keyid values over
        # a shared ESTABLISHED upstream, so each peer is signed with its own
        # key. Unsigned peers stay PRELIMINARY and never reach REPLICATED.
        import mareforma
        from tests._helpers import _bootstrap_key, _two_signers
        sa, sb = _two_signers(tmp_path)
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            prior = g.assert_claim("prior", generated_by="seed", seed=True)
            g.assert_claim("finding A", supports=[prior],
                           generated_by="agent_A", signer=sa)
            g.assert_claim("finding B", supports=[prior],
                           generated_by="agent_B", signer=sb)

        conn = _open(tmp_path)
        try:
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.traffic_light == "green"
        assert report.support_level_breakdown["REPLICATED"] == 2

    def test_green_when_only_replicated_claims_stand(self) -> None:
        # REPLICATED alone is a green light: the ESTABLISHED term must not be
        # the only thing carrying the verdict. Built directly because a graph
        # with REPLICATED and no ESTABLISHED anchor is unreachable through
        # the API.
        report = HealthReport(
            claims_open=1,
            support_level_breakdown={"REPLICATED": 1},
            standing_promoted=1,
        )
        light, rationale = _compute_traffic_light(report)
        assert light == "green"
        assert "replicated" in rationale.lower()

    def test_not_green_when_the_promoted_claim_is_retracted(
        self, tmp_path: Path,
    ) -> None:
        # Retraction is a terminal state the product expects to reach. A graph
        # whose only promoted claim was withdrawn must not read as healthy.
        import mareforma
        from tests._helpers import _bootstrap_key
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            prior = g.assert_claim("prior", generated_by="seed", seed=True)
            g.update_claim(prior, status="retracted")

        conn = _open(tmp_path)
        try:
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.traffic_light != "green"
        assert "retracted" in report.rationale
        # The census stays the full per-level count, it is a public field.
        assert report.support_level_breakdown["ESTABLISHED"] == 1

    def test_not_green_when_the_promoted_claim_is_invalidated(
        self, tmp_path: Path,
    ) -> None:
        # Same for a claim a signed contradiction verdict marked invalid.
        import mareforma
        from tests._helpers import _bootstrap_key, _pem_of, _two_signers
        _sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        val_key = _bootstrap_key(tmp_path, "val.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(_pem_of(val_key), identity="v")
            prior = g.assert_claim("prior", generated_by="seed", seed=True)
            counter = g.assert_claim("counter", generated_by="lab_w", signer=sb)
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_1", member_claim_id=prior,
                other_claim_id=counter, confidence={"stance": "refutes"},
            )

        conn = _open(tmp_path)
        try:
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.traffic_light != "green"
        assert "contradiction" in report.rationale


# ---------------------------------------------------------------------------
# Per-status + per-support-level counters
# ---------------------------------------------------------------------------


class TestCounts:
    def test_claims_open_vs_resolved(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Open claim", status="open")
            add_claim(conn, tmp_path, "Resolved claim", status="contested")
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.claims_open == 1
        assert report.claims_resolved == 1

    def test_claims_contradicted_counts_signed_invalidations(
        self, tmp_path: Path,
    ) -> None:
        # "contradicted" is the refutation taxonomy's word for a claim a
        # signed contradiction verdict marked invalid.
        import mareforma
        from tests._helpers import _bootstrap_key, _pem_of, _two_signers
        _sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        val_key = _bootstrap_key(tmp_path, "val.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(_pem_of(val_key), identity="v")
            older = g.assert_claim("older", generated_by="seed", seed=True)
            counter = g.assert_claim("counter", generated_by="lab_w", signer=sb)
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_1", member_claim_id=older,
                other_claim_id=counter, confidence={"stance": "refutes"},
            )

        conn = _open(tmp_path)
        try:
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.claims_contradicted == 1

    def test_asserting_a_contradiction_is_not_being_contradicted(
        self, tmp_path: Path,
    ) -> None:
        # A claim that disputes a DOI has not itself been refuted by
        # anyone, so it does not belong in the contradicted count.
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Disputing finding", contradicts=["10.1038/some"])
            add_claim(conn, tmp_path, "Normal finding")
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.claims_contradicted == 0

    def test_support_level_breakdown(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Claim 1")
            add_claim(conn, tmp_path, "Claim 2")
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.support_level_breakdown.get("PRELIMINARY", 0) == 2


# ---------------------------------------------------------------------------
# Never-raises contract
# ---------------------------------------------------------------------------


class TestNeverRaises:
    def test_empty_project_no_error(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            report = compute_health(tmp_path, conn)
            assert isinstance(report, HealthReport)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Corruption-vs-empty differentiation
# ---------------------------------------------------------------------------


class TestCorruptionVsEmpty:
    """A corrupted graph.db that cannot be read must surface as
    traffic_light='error' so an operator running ``mareforma health``
    sees a different signal than for a fresh, empty project. Before
    this distinction existed, a SELECT failure was silently swallowed
    and the resulting empty counters were folded into the standard
    ``red`` empty-graph branch — operationally indistinguishable.
    """

    def test_closed_connection_surfaces_as_error(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        conn.close()
        # SELECT against a closed connection raises ProgrammingError;
        # compute_health must catch and surface ``error``.
        report = compute_health(tmp_path, conn)
        assert report.traffic_light == "error"
        assert "Could not read" in report.rationale
        assert "not the same as an empty graph" in report.rationale

    def test_missing_claims_table_surfaces_as_error(self, tmp_path: Path) -> None:
        # Open a fresh DB, drop the claims table, then check that
        # compute_health surfaces the read failure as ``error``.
        conn = _open(tmp_path)
        try:
            conn.execute("DROP TABLE claims_fts")
            conn.execute("DROP TABLE claims")
            conn.commit()
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.traffic_light == "error"

    def test_empty_graph_still_red_not_error(self, tmp_path: Path) -> None:
        """Sanity: a legitimately-empty graph stays at ``red``, NOT
        ``error``. The distinction only fires for actual read failures.
        """
        conn = _open(tmp_path)
        try:
            report = compute_health(tmp_path, conn)
        finally:
            conn.close()
        assert report.traffic_light == "red"
        assert "No claims recorded" in report.rationale
