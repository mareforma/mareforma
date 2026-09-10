"""tests/test_health.py — unit tests for mareforma/health.py."""

from __future__ import annotations

import inspect
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
            report = compute_health(conn)
        finally:
            conn.close()
        assert report.traffic_light == "red"
        assert "claim" in report.rationale.lower()

    def test_yellow_when_all_preliminary(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Single agent finding")
            report = compute_health(conn)
        finally:
            conn.close()
        assert report.traffic_light == "yellow"
        assert "signed" in report.rationale

    def test_green_when_a_claim_carries_a_signed_validation(
        self, tmp_path: Path,
    ) -> None:
        """What turns the light green, now that no rung does.

        It used to be a claim that reached a rung, which happened on its
        own when two distinct signers converged. Nothing is promoted, so the
        light asks the only thing left that a person put their name to.
        """
        import mareforma
        from mareforma import signing as _sig
        from tests._helpers import _bootstrap_key

        key = _bootstrap_key(tmp_path, "root.key")
        val_key = tmp_path / "val.key"
        _sig.bootstrap_key(val_key)
        val_pem = _sig.public_key_to_pem(
            _sig.load_private_key(val_key).public_key(),
        )
        with mareforma.open(tmp_path, key_path=key) as g:
            claim_id = g.assert_claim("a finding", generated_by="agent_A")
            g.enroll_validator(val_pem, identity="v")
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.validate(claim_id)

        conn = _open(tmp_path)
        try:
            report = compute_health(conn)
        finally:
            conn.close()
        assert report.traffic_light == "green"
        assert report.standing_validated == 1

    def test_not_green_when_the_promoted_claim_is_retracted(
        self, tmp_path: Path,
    ) -> None:
        # Retraction is a terminal state the product expects to reach. A graph
        # whose only promoted claim was withdrawn must not read as healthy.
        import mareforma
        from tests._helpers import _bootstrap_key
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            prior = g.assert_claim("prior", generated_by="seed")
            g.update_claim(prior, status="retracted")

        conn = _open(tmp_path)
        try:
            report = compute_health(conn)
        finally:
            conn.close()
        assert report.traffic_light != "green"
        assert "retracted" in report.rationale
        # A retracted claim is not standing, whatever it carries.
        assert report.standing_validated == 0

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
            prior = g.assert_claim("prior", generated_by="seed")
            counter = g.assert_claim("counter", generated_by="lab_w", signer=sb)
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_1", member_claim_id=prior,
                other_claim_id=counter, confidence={"stance": "refutes"},
            )

        conn = _open(tmp_path)
        try:
            report = compute_health(conn)
        finally:
            conn.close()
        assert report.traffic_light != "green"
        assert "contradiction" in report.rationale

    def test_not_green_when_a_promotion_does_not_reverify(
        self, tmp_path: Path,
    ) -> None:
        # The census counts a row as standing when it carries a validation
        # envelope; the separate re-verification count is what catches an
        # envelope that no longer verifies, and the light cannot read green
        # over it.
        import mareforma
        from tests._helpers import _bootstrap_key, _pem_of, _two_signers
        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        val_key = _bootstrap_key(tmp_path, "val.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(_pem_of(val_key), identity="v")
            up = g.assert_claim("anchor", generated_by="seed")
            rep = g.assert_claim("A", supports=[up], generated_by="a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="b", signer=sb)
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.validate(rep)

        # Corrupt the validation envelope directly in sqlite, the way a process
        # with DB write access would. The trigger that makes a validation
        # terminal refuses this UPDATE, and the same writer can drop it, so the
        # tamper drops it first: what is under test here is the layer below,
        # the re-verification the health report runs for itself.
        conn = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        try:
            conn.execute("DROP TRIGGER IF EXISTS claims_validation_is_terminal")
            conn.execute(
                "UPDATE claims SET validation_signature = ? WHERE claim_id = ?",
                ('{"payloadType":"forged","payload":"x","signatures":[]}', rep),
            )
            conn.commit()
        finally:
            conn.close()

        conn = _open(tmp_path)
        try:
            report = compute_health(conn)
        finally:
            conn.close()
        # The tampered row no longer re-verifies, so the light is barred from
        # green and the failed count is surfaced.
        assert report.failed_verification == 1
        assert report.traffic_light != "green"
        assert "re-verify" in report.rationale


# ---------------------------------------------------------------------------
# Per-status + per-support-level counters
# ---------------------------------------------------------------------------


class TestCounts:
    def test_claims_open_vs_resolved(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Open claim", status="open")
            add_claim(conn, tmp_path, "Resolved claim", status="contested")
            report = compute_health(conn)
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
            older = g.assert_claim("older", generated_by="seed")
            counter = g.assert_claim("counter", generated_by="lab_w", signer=sb)
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_1", member_claim_id=older,
                other_claim_id=counter, confidence={"stance": "refutes"},
            )

        conn = _open(tmp_path)
        try:
            report = compute_health(conn)
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
            report = compute_health(conn)
        finally:
            conn.close()
        assert report.claims_contradicted == 0

    def test_counts_without_materialising_every_row(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        # Four numbers do not need every claim's text, signature bundle and
        # payloads boxed into Python. SQLite counts them in one pass, so the
        # census must not go through list_claims.
        import mareforma
        from mareforma import db as _db
        from tests._helpers import _bootstrap_key, _two_signers
        sa, sb = _two_signers(tmp_path)
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            prior = g.assert_claim("prior", generated_by="seed")
            g.assert_claim("finding A", supports=[prior],
                           generated_by="agent_A", signer=sa)
            g.assert_claim("finding B", supports=[prior],
                           generated_by="agent_B", signer=sb)

        conn = _open(tmp_path)
        try:
            add_claim(conn, tmp_path, "Contested claim", status="contested")
            baseline = compute_health(conn)

            def _refuse(*_args, **_kwargs):
                raise AssertionError("compute_health must not read whole rows")

            monkeypatch.setattr(_db, "list_claims", _refuse)
            report = compute_health(conn)
        finally:
            conn.close()
        assert report == baseline
        assert (report.claims_open, report.claims_resolved) == (3, 1)

class TestNeverRaises:
    def test_empty_project_no_error(self, tmp_path: Path) -> None:
        conn = _open(tmp_path)
        try:
            report = compute_health(conn)
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
        report = compute_health(conn)
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
            report = compute_health(conn)
        finally:
            conn.close()
        assert report.traffic_light == "error"

    def test_empty_graph_still_red_not_error(self, tmp_path: Path) -> None:
        """Sanity: a legitimately-empty graph stays at ``red``, NOT
        ``error``. The distinction only fires for actual read failures.
        """
        conn = _open(tmp_path)
        try:
            report = compute_health(conn)
        finally:
            conn.close()
        assert report.traffic_light == "red"
        assert "No claims recorded" in report.rationale


# ---------------------------------------------------------------------------
# Signature
# ---------------------------------------------------------------------------


class TestSignature:
    def test_snapshot_takes_only_the_connection(self) -> None:
        """The snapshot reads the claims table and nothing on disk, so it
        must not ask for a project root it cannot consult.
        """
        assert list(inspect.signature(compute_health).parameters) == ["conn"]


def test_a_policy_read_that_fails_is_not_reported_as_no_policy_stall(tmp_path):
    """A status command that cannot read the policy must say so, not answer no.

    The policy read caught ``sqlite3.DatabaseError``, the base class of nearly
    every sqlite failure including corruption, and set ``policy_unverified =
    False``, which prints as "no policy stall". Its three sibling reads set
    ``traffic_light = 'error'`` on the same exception tuple, so one unreadable
    graph produced a green-ish status beside three red ones.
    """
    import sqlite3

    import mareforma
    from mareforma import health as health_mod

    with mareforma.open(tmp_path) as g:
        conn = g._conn

        def boom(_conn):
            raise sqlite3.DatabaseError("database disk image is malformed")

        import mareforma.db as db_mod
        original = db_mod.project_policy_unverified
        db_mod.project_policy_unverified = boom
        try:
            report = health_mod.compute_health(conn)
        finally:
            db_mod.project_policy_unverified = original

    assert report.traffic_light == "error"
    assert "could not be read" in report.rationale
    assert report.policy_unverified is False  # unknown, and the light says so


def test_every_deprecation_warning_goes_through_the_one_emitter():
    """`_deprecation._emit` says it is "the one place the category and the warn
    call live". Four of six emitters bypassed it, which is how a wrong
    stacklevel shipped: a DeprecationWarning attributed to a mareforma file is
    hidden by Python's default filter from the person whose code needs changing.
    """
    import pathlib
    import re

    root = pathlib.Path(cli_source_root())
    offenders = []
    for path in root.rglob("*.py"):
        if path.name == "_deprecation.py":
            continue
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(r"warn\(\s*[^)]*DeprecationWarning", text):
            line = text[: match.start()].count("\n") + 1
            offenders.append(f"{path.name}:{line}")
    assert not offenders, (
        f"these sites emit a DeprecationWarning directly instead of through "
        f"_deprecation._emit: {offenders}"
    )


def cli_source_root():
    import mareforma
    import pathlib

    return pathlib.Path(mareforma.__file__).parent


def test_an_operational_error_that_is_not_a_missing_table_is_not_swallowed(tmp_path):
    """The narrowing has to be on the CASE, not on the exception class.

    OperationalError covers a locked database, a disk I/O error and a missing
    column as well as the older schema this tolerance was written for. Narrowing
    on the class alone left all of those reporting "no policy stall", which is
    the direction the branch exists to stop.
    """
    import sqlite3

    import mareforma
    import mareforma.db as db_mod
    from mareforma import health as health_mod

    def probe(exc):
        with mareforma.open(tmp_path) as g:
            original = db_mod.project_policy_unverified
            db_mod.project_policy_unverified = lambda _c: (_ for _ in ()).throw(exc)
            try:
                return health_mod.compute_health(g._conn)
            finally:
                db_mod.project_policy_unverified = original

    for exc in (sqlite3.OperationalError("disk I/O error"),
                sqlite3.OperationalError("database is locked"),
                sqlite3.DatabaseError("database disk image is malformed")):
        report = probe(exc)
        assert report.traffic_light == "error", exc
        assert "could not be read" in report.rationale, exc

    # The one case the tolerance is for stays tolerated.
    tolerated = probe(sqlite3.OperationalError("no such table: project_policy"))
    assert tolerated.traffic_light != "error"
    assert tolerated.policy_unverified is False

