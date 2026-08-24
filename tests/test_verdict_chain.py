"""claims.toml learns to say which rows were in it.

Every claim, validator and verdict in the backup carries its own signature, so
nobody can change what a row says. Nothing in the file signs *which rows are
there*. Delete a verdict's entry and restore rebuilds a graph that never had it,
reports clean, and nothing in the file disagrees. That is the half of
drop-guard-delete-verdict which survives both the guard reconciler and the
contestation replay, because those speak about rows that are still present.

The chain is what makes the absence speak, and these tests pin both what it
catches and what it does not. The two residuals have tests of their own, and
they assert the hole rather than papering over it: a residual nobody wrote down
is a residual somebody will later mistake for a guarantee.
"""

from __future__ import annotations

import base64
import warnings
import sqlite3
try:
    import tomllib          # 3.11+ stdlib
except ModuleNotFoundError:  # 3.10, where it is the tomli backport
    import tomli as tomllib  # type: ignore[no-redef]
from pathlib import Path

import pytest

import mareforma
from mareforma.db.core import (
    _append_verdict_chain_link,
    _verdict_chain_link_pae,
    _verdict_chain_tip,
    verdict_chain_coverage,
    verdict_chain_tip,
    verify_verdict_chain,
)
from mareforma.db.errors import FormatArtifactError
from mareforma.db.restore import restore
from tests._helpers import _bootstrap_key, _enroll_key, _load_signer


def _db(root: Path) -> Path:
    return root / ".mareforma" / "graph.db"


def _raw(root: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(_db(root))
    conn.row_factory = sqlite3.Row
    return conn


def _unguard(root: Path) -> None:
    """Drop the guards standing between an attacker and the chain.

    Every test that edits the chain does this first, which is the point: the
    census records the drop, and what these tests are about is the half of the
    attack that survives being recorded.
    """
    conn = _raw(root)
    for name in ("contradiction_verdicts_no_delete",
                 "contradiction_verdicts_append_only",
                 "verdict_chain_no_delete", "verdict_chain_append_only"):
        conn.execute(f"DROP TRIGGER IF EXISTS {name}")
    conn.commit()
    conn.close()


def _graph_with_verdicts(
    root: Path, count: int = 3, *, issuers: int = 1,
) -> tuple[Path, list[Path], list[str]]:
    """A graph with *count* contradictions, spread over *issuers* witness keys.

    Returns the root key, the witness keys, and the claim ids. Verdict ``n`` is
    ``v{n}`` so the tests can name links without reading them back.
    """
    root_key = _bootstrap_key(root, "root.key")
    claims: list[str] = []
    with mareforma.open(root, key_path=root_key) as g:
        for i in range(count * 2):
            claims.append(g.assert_claim(f"claim {i}", generated_by=f"run{i}"))

    witnesses = []
    for w in range(issuers):
        key = _bootstrap_key(root, f"w{w}.key")
        _enroll_key(root, root_key, key, identity=f"w{w}@example.org")
        witnesses.append(key)

    for n in range(count):
        with mareforma.open(root, key_path=witnesses[n % issuers]) as g:
            g.record_contradiction_verdict(
                verdict_id=f"v{n + 1}",
                member_claim_id=claims[n * 2 + 1],
                other_claim_id=claims[n * 2],
            )
    return root_key, witnesses, claims


class TestTheHonestCase:
    def test_a_verdict_gets_a_link_and_the_chain_verifies(
        self, tmp_path: Path,
    ) -> None:
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=3)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert verify_verdict_chain(g._conn) == ()
            assert verdict_chain_coverage(g._conn) == (3, 3)
            assert verdict_chain_tip(g._conn) != ""

    def test_an_empty_graph_has_an_empty_chain_and_no_complaint(
        self, tmp_path: Path,
    ) -> None:
        """A clean report on nothing, not a complaint about nothing."""
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a claim with nothing against it")
        with mareforma.open(tmp_path, key_path=key) as g:
            assert verify_verdict_chain(g._conn) == ()
            assert verdict_chain_coverage(g._conn) == (0, 0)
            assert verdict_chain_tip(g._conn) == ""

    def test_the_links_run_from_one_without_gaps(self, tmp_path: Path) -> None:
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=4)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            rows = g._conn.execute(
                "SELECT seq, prev_tip, tip FROM verdict_chain ORDER BY seq"
            ).fetchall()
        assert [r["seq"] for r in rows] == [1, 2, 3, 4]
        assert rows[0]["prev_tip"] == ""
        for earlier, later in zip(rows, rows[1:]):
            assert later["prev_tip"] == earlier["tip"]

    def test_the_verdict_and_its_link_commit_together(
        self, tmp_path: Path,
    ) -> None:
        """A verdict whose INSERT is refused leaves no link behind it.

        The two writes are one transaction. Were they not, a refused verdict
        would still advance the chain, and the next honest verdict would build
        on a tip covering something that is not in the graph.
        """
        root_key, witnesses, claims = _graph_with_verdicts(tmp_path, count=1)
        with mareforma.open(tmp_path, key_path=witnesses[0]) as g:
            with pytest.raises(Exception):
                # Same verdict_id twice: the PRIMARY KEY refuses the second.
                g.record_contradiction_verdict(
                    verdict_id="v1", member_claim_id=claims[3],
                    other_claim_id=claims[2],
                )
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert verdict_chain_coverage(g._conn) == (1, 1)
            assert verify_verdict_chain(g._conn) == ()


class TestWhatItCatches:
    def test_a_verdict_taken_from_the_middle_breaks_the_chain(
        self, tmp_path: Path,
    ) -> None:
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=3)
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        conn.execute("DELETE FROM contradiction_verdicts WHERE verdict_id='v2'")
        conn.execute("DELETE FROM verdict_chain WHERE verdict_id='v2'")
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            problems = verify_verdict_chain(g._conn)
        assert problems, "a removed verdict read as a clean chain"
        assert any("out of sequence" in p for p in problems)
        assert any("previous tip" in p for p in problems)

    def test_a_link_left_pointing_at_a_deleted_verdict_is_reported(
        self, tmp_path: Path,
    ) -> None:
        """Removing the verdict and leaving the link is the lazier attack."""
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=3)
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        conn.execute("DELETE FROM contradiction_verdicts WHERE verdict_id='v2'")
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            problems = verify_verdict_chain(g._conn)
        assert any("no longer in the graph" in p for p in problems)

    def test_an_edited_link_no_longer_produces_its_own_tip(
        self, tmp_path: Path,
    ) -> None:
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=2)
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        conn.execute(
            "UPDATE verdict_chain SET verdict_digest = ? WHERE seq = 1",
            ("0" * 64,),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            problems = verify_verdict_chain(g._conn)
        assert any("do not produce" in p for p in problems)

    def test_a_re_signed_verdict_does_not_satisfy_its_link(
        self, tmp_path: Path,
    ) -> None:
        """The link binds to the signature, not to the ids on the row.

        A verdict re-signed by another key keeps every id the link names and
        still fails it, which is why ``verdict_digest`` is taken over the one
        field only the issuer could have produced.
        """
        root_key, witnesses, _ = _graph_with_verdicts(
            tmp_path, count=2, issuers=2,
        )
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        row = conn.execute(
            "SELECT * FROM contradiction_verdicts WHERE verdict_id='v1'"
        ).fetchone()
        other = _load_signer(witnesses[1])
        from mareforma.db.core import _contradiction_verdict_pae
        import json
        forged = other.sign(_contradiction_verdict_pae({
            "verdict_id": row["verdict_id"],
            "member_claim_id": row["member_claim_id"],
            "other_claim_id": row["other_claim_id"],
            "confidence": json.loads(row["confidence_json"] or "{}"),
        }))
        conn.execute(
            "UPDATE contradiction_verdicts SET signature = ? "
            "WHERE verdict_id = 'v1'", (forged,),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            problems = verify_verdict_chain(g._conn)
        assert any("not the one the link was made over" in p for p in problems)

    def test_another_enrolled_peer_cannot_rewrite_a_link(
        self, tmp_path: Path,
    ) -> None:
        """Enrolment is not the bar. Issuing the covered verdict is.

        A verifier that checks only that a link's signature verifies and that
        its signer is enrolled lets any second enrolled validator delete a
        verdict, re-sign the following link over the gap, and hand back a chain
        that recomputes and reads clean. That is a valid signature over a set
        the signer had just emptied, which is the whole reason a project-level
        cover was rejected in favour of per-issuer links.
        """
        root_key, witnesses, _ = _graph_with_verdicts(
            tmp_path, count=3, issuers=1,
        )
        peer_key = _bootstrap_key(tmp_path, "peer.key")
        _enroll_key(tmp_path, root_key, peer_key, identity="peer@example.org")

        _unguard(tmp_path)
        conn = _raw(tmp_path)
        tip1 = conn.execute(
            "SELECT tip FROM verdict_chain WHERE seq = 1"
        ).fetchone()["tip"]
        link3 = conn.execute(
            "SELECT * FROM verdict_chain WHERE seq = 3"
        ).fetchone()
        conn.execute("DELETE FROM contradiction_verdicts WHERE verdict_id='v2'")
        conn.execute("DELETE FROM verdict_chain WHERE seq = 2")
        record = {
            "seq": 2, "prev_tip": tip1, "verdict_kind": "contradiction",
            "verdict_id": "v3", "verdict_digest": link3["verdict_digest"],
        }
        peer = _load_signer(peer_key)
        from mareforma import signing
        conn.execute(
            "UPDATE verdict_chain SET seq=?, prev_tip=?, tip=?, issuer_keyid=?, "
            "signature=? WHERE seq = 3",
            (2, tip1, _verdict_chain_tip(record),
             signing.public_key_id(peer.public_key()),
             peer.sign(_verdict_chain_link_pae(record))),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            problems = verify_verdict_chain(g._conn)
        assert any("did not issue the verdict it covers" in p for p in problems)

    def test_relabelling_the_issuer_column_does_not_satisfy_the_link(
        self, tmp_path: Path,
    ) -> None:
        """Setting both sides of the comparison is not enough.

        ``issuer_keyid`` on a verdict row carries no signature, so an attacker
        who rewrites a link can rewrite that column to match and the two agree.
        What they cannot produce is the covered verdict's own signature under
        the key they just named, so the link is checked against it. Without
        that check this attack reads as a clean chain over a set the peer had
        just emptied.
        """
        from mareforma import signing

        root_key, _, _ = _graph_with_verdicts(tmp_path, count=3, issuers=1)
        peer_key = _bootstrap_key(tmp_path, "peer.key")
        _enroll_key(tmp_path, root_key, peer_key, identity="peer@example.org")

        _unguard(tmp_path)
        conn = _raw(tmp_path)
        conn.execute("DROP TRIGGER IF EXISTS contradiction_verdicts_no_update")
        tip1 = conn.execute(
            "SELECT tip FROM verdict_chain WHERE seq = 1"
        ).fetchone()["tip"]
        link3 = conn.execute(
            "SELECT * FROM verdict_chain WHERE seq = 3"
        ).fetchone()
        conn.execute("DELETE FROM contradiction_verdicts WHERE verdict_id='v2'")
        conn.execute("DELETE FROM verdict_chain WHERE seq = 2")

        peer = _load_signer(peer_key)
        peer_keyid = signing.public_key_id(peer.public_key())
        # The verdict's signature is untouched, so the digest the link carries
        # stays the right one. Only the unsigned column moves.
        conn.execute(
            "UPDATE contradiction_verdicts SET issuer_keyid = ? "
            "WHERE verdict_id = 'v3'", (peer_keyid,),
        )
        record = {
            "seq": 2, "prev_tip": tip1, "verdict_kind": "contradiction",
            "verdict_id": "v3", "verdict_digest": link3["verdict_digest"],
            "issuer_keyid": peer_keyid,
        }
        conn.execute(
            "UPDATE verdict_chain SET seq=?, prev_tip=?, tip=?, issuer_keyid=?, "
            "signature=? WHERE seq = 3",
            (2, tip1, _verdict_chain_tip(record), peer_keyid,
             peer.sign(_verdict_chain_link_pae(record))),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            problems = verify_verdict_chain(g._conn)
        assert any(
            "does not verify under the key the link names" in p
            for p in problems
        ), problems

    def test_a_link_from_an_unenrolled_key_is_reported(
        self, tmp_path: Path,
    ) -> None:
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=2)
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        conn.execute(
            "UPDATE verdict_chain SET issuer_keyid = ? WHERE seq = 1",
            ("f" * 64,),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            problems = verify_verdict_chain(g._conn)
        assert any("not an enrolled validator" in p for p in problems)

    def test_a_link_whose_signature_does_not_verify_is_reported(
        self, tmp_path: Path,
    ) -> None:
        """The check that makes the chain a chain, and it had no test.

        Deleting the whole verify block left every test in this file green. The
        cases around it plant an unknown keyid or a rewritten issuer, and both
        take the enrolment branch before the signature is ever checked. This one
        keeps the issuer honest and moves a real signature onto the wrong link,
        so the only thing standing between it and a clean report is the verify.
        """
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=2)
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        # Link 2's own signature, made by the same enrolled issuer over a
        # different record. Enrolled, right issuer, wrong bytes.
        other = conn.execute(
            "SELECT signature FROM verdict_chain WHERE seq = 2"
        ).fetchone()["signature"]
        conn.execute(
            "UPDATE verdict_chain SET signature = ? WHERE seq = 1", (other,),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            problems = verify_verdict_chain(g._conn)
        assert any("does not verify against its issuer" in p for p in problems)

    def test_a_signer_that_does_not_chain_to_the_root_is_reported(
        self, tmp_path: Path,
    ) -> None:
        """Enrolment is a chain walk, not a row lookup.

        The unenrolled case above plants a keyid that is in no validators row,
        which a bare row lookup would also catch. This one puts a row in the
        table whose enrolment does not chain back to the root, so only the walk
        refuses it. Dropping the walk left the whole suite green.
        """
        root_key, witnesses, _ = _graph_with_verdicts(tmp_path, count=2)
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        # Break the enrolment envelope of the key that signed the links. The row
        # stays, so a lookup still finds it; the walk no longer reaches the root.
        conn.execute(
            "DROP TRIGGER IF EXISTS validators_append_only;"
        )
        conn.execute(
            "UPDATE validators SET enrolled_by_keyid = ? "
            "WHERE keyid = (SELECT issuer_keyid FROM verdict_chain WHERE seq=1)",
            ("f" * 64,),
        )
        conn.commit()
        rows = conn.execute("SELECT COUNT(*) FROM validators").fetchone()[0]
        conn.close()
        assert rows, "the signer's row must still be present for this to test"

        check = _raw(tmp_path)
        problems = verify_verdict_chain(check)
        check.close()
        assert any("not an enrolled validator" in p for p in problems)

    def test_an_unreadable_graph_reports_rather_than_raising(
        self, tmp_path: Path,
    ) -> None:
        """A read degrades; it does not take the caller down, or read clean.

        The enrolment walk and the covered-verdict lookup both touch tables the
        chain does not own. With those gone the check used to raise
        ``sqlite3.OperationalError`` straight through a function whose contract
        says it never does. Reporting the damage is the only safe answer: going
        quiet would turn a dismantled graph into a clean chain.
        """
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=2)
        conn = _raw(tmp_path)
        conn.executescript(
            "DROP TRIGGER IF EXISTS validators_no_delete;"
            "DROP TRIGGER IF EXISTS validators_append_only;"
            "DROP TABLE validators;"
        )
        conn.commit()
        conn.close()

        check = _raw(tmp_path)
        problems = verify_verdict_chain(check)
        check.close()
        assert problems, "a graph missing its validators read as a clean chain"
        assert any("not readable" in p for p in problems)

    def test_a_signature_column_holding_text_reports_rather_than_raising(
        self, tmp_path: Path,
    ) -> None:
        """The contract says never raises, and a scoped handler is not that.

        A signature column holding TEXT where the schema says BLOB reaches
        hashlib as a str and comes out a TypeError. The per-link handler was
        scoped to sqlite3.Error, so it walked past and took the caller down, on
        exactly the column an attacker edits. Reporting it is the contract.
        """
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=2)
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        conn.execute("DROP TRIGGER IF EXISTS contradiction_verdicts_append_only")
        conn.execute(
            "UPDATE contradiction_verdicts SET signature = 'not-bytes' "
            "WHERE verdict_id = 'v1'"
        )
        conn.commit()
        conn.close()

        check = _raw(tmp_path)
        problems = verify_verdict_chain(check)      # must not raise
        check.close()
        assert any("not readable" in p for p in problems)


class TestTheGuards:
    @pytest.mark.parametrize("statement, marker", [
        ("DELETE FROM verdict_chain", "no_delete"),
        ("UPDATE verdict_chain SET tip = 'x'", "append_only"),
    ])
    def test_the_chain_refuses_edits(
        self, tmp_path: Path, statement: str, marker: str,
    ) -> None:
        _graph_with_verdicts(tmp_path, count=1)
        conn = _raw(tmp_path)
        with pytest.raises(sqlite3.IntegrityError, match=marker):
            conn.execute(statement)
        conn.close()

    def test_the_guards_come_back_on_the_next_open(
        self, tmp_path: Path,
    ) -> None:
        """New table, same rule: the reconciler owns the guards.

        A guard created once on a fresh database and never again is a guard
        that stays dropped once somebody drops it. These are authored into the
        reconciled-only home, so the reconciler rebuilds them.
        """
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=1)
        _unguard(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key):
            pass
        conn = _raw(tmp_path)
        live = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'"
            )
        }
        conn.close()
        assert {"verdict_chain_no_delete", "verdict_chain_append_only"} <= live


class TestTheReadPathReportsIt:
    """A checker nothing calls reports to nobody.

    The chain shipped with ``verify_verdict_chain`` reachable only by a caller
    who imports it and holds a connection, which no product path did: not
    restore, not the trust map, not verify, not the CLI. So the exact tamper the
    chain was built for was detected by an API and reported on no surface, while
    a docstring said it was reported "on every read".
    """

    def test_a_broken_chain_reaches_the_trust_map(self, tmp_path: Path) -> None:
        from mareforma.trust_map import TAMPERED_VALUE

        root_key, _, claims = _graph_with_verdicts(tmp_path, count=3)
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        conn.execute("DELETE FROM contradiction_verdicts WHERE verdict_id='v2'")
        conn.execute("DELETE FROM verdict_chain WHERE verdict_id='v2'")
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            prop = next(
                p for p in g.trust_map(claims[0]).properties
                if p.name == "trust_root"
            )
        assert prop.value == TAMPERED_VALUE
        assert "verdict chain does not check out" in prop.residual

    def test_a_broken_chain_reaches_the_verdict_not_only_the_report(
        self, tmp_path: Path,
    ) -> None:
        """The exit code is what a gate reads, and it was not moving.

        Wiring the chain into the trust map put it in the printed report, since
        verify renders the map. It did not put it in the verdict, which verify
        computes itself: a graph somebody had just removed a verdict from
        rendered TAMPERED on the map and exited 0. A dropped write guard, the
        same class of damage, has always exited non-zero.

        Broken through the backup rather than by dropping a guard, so the census
        stays silent and the chain is the only thing with anything to say.
        """
        import shutil as _shutil

        import tomli_w

        from mareforma._verify import UNVERIFIABLE, classify_claim_verdict
        from mareforma.db.core import get_claim, open_db, schema_census_missing

        root_key, _, claims = _graph_with_verdicts(tmp_path, count=3)
        toml_path = tmp_path / "claims.toml"
        doc = tomllib.loads(toml_path.read_text())
        del doc["contradiction_verdicts"]["v2"]
        toml_path.write_text(
            tomli_w.dumps({k: v for k, v in doc.items() if k != "completeness"})
        )
        _shutil.rmtree(tmp_path / ".mareforma")
        restore(tmp_path)

        conn = open_db(tmp_path)
        try:
            assert schema_census_missing(conn) == (), (
                "the census must be silent or it, not the chain, is what fails"
            )
            assert verify_verdict_chain(conn) != ()
            result = classify_claim_verdict(
                conn, get_claim(conn, claims[0]), claims[0],
            )
        finally:
            conn.close()
        assert result.verdict == UNVERIFIABLE
        assert "verdict chain" in result.reason

    def test_verify_checks_the_chain_once_not_twice(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """The check is the expensive part, so paying twice is the whole cost.

        Checking a link costs an enrolment walk and two signature
        verifications, so the chain scales with the number of verdicts in the
        graph. Measured at forty milliseconds on two hundred verdicts, against
        a fifth of a millisecond for the rest of the map. verify ran it once
        for its own verdict and again inside the map it builds, which doubled
        that for an answer that cannot change in between.
        """
        from mareforma import _verify as _v
        from mareforma.db import core as _core
        from mareforma.db.core import get_claim, open_db

        root_key, _, claims = _graph_with_verdicts(tmp_path, count=3)
        calls = []
        real = _core.verify_verdict_chain

        def counting(conn):
            calls.append(1)
            return real(conn)

        monkeypatch.setattr(_core, "verify_verdict_chain", counting)
        conn = open_db(tmp_path)
        try:
            _v.classify_claim_verdict(
                conn, get_claim(conn, claims[0]), claims[0],
            )
        finally:
            conn.close()
        assert len(calls) == 1, (
            f"the chain was checked {len(calls)} times in one verify"
        )

    def test_a_healthy_chain_leaves_the_map_alone(self, tmp_path: Path) -> None:
        """The other half: a working chain must not paint every graph tampered."""
        root_key, _, claims = _graph_with_verdicts(tmp_path, count=2)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            prop = next(
                p for p in g.trust_map(claims[0]).properties
                if p.name == "trust_root"
            )
        assert prop.value == "single trust domain"
        assert "verdict chain" not in prop.residual

    def test_a_graph_predating_the_chain_is_not_reported(
        self, tmp_path: Path,
    ) -> None:
        """Verdicts with no links are uncovered, not broken.

        An upgraded graph carries verdicts written before the chain existed. The
        coverage pair is what makes that visible; painting it as tamper would
        brand every upgraded graph on its first open.

        The axis still reads tampered here, because removing the links needs the
        guards dropped and the census records that. What this asserts is the
        chain's own contribution: it must say nothing.
        """
        root_key, _, claims = _graph_with_verdicts(tmp_path, count=2)
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        conn.execute("DELETE FROM verdict_chain")
        conn.commit()
        assert verify_verdict_chain(conn) == (), (
            "verdicts with no links are uncovered, not a broken chain"
        )
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert verdict_chain_coverage(g._conn) == (0, 2)
            prop = next(
                p for p in g.trust_map(claims[0]).properties
                if p.name == "trust_root"
            )
        assert "verdict chain" not in prop.residual


class TestTheResiduals:
    """What the chain does not close, asserted so nobody assumes it does."""

    def test_an_enrolled_peer_can_still_take_a_verdict_from_the_middle(
        self, tmp_path: Path,
    ) -> None:
        """The residual the guarantee used to deny, pinned as behaviour.

        The chain once claimed to hold out "any other peer". It does not. A
        verdict's signed payload carries no issuer, so the column naming one is
        free: a peer puts its own keyid on a surviving verdict, re-signs the
        verdict, and re-signs the link to match. Every check passes on a chain
        it just shortened.

        What the fix did close is the cheaper version, where the verdict is left
        alone: the tip now commits to the link's author, and the covered verdict
        must verify under the key the link names. Both of those fail if either
        is skipped, and the two tests above prove it.

        This test exists so the day somebody closes the rest, it fails and they
        have to come here and say so.
        """
        import hashlib
        import json as _json

        from mareforma import signing as _sign
        from mareforma.db.core import _contradiction_verdict_pae

        root_key, _, _ = _graph_with_verdicts(tmp_path, count=3, issuers=1)
        attacker_key = _bootstrap_key(tmp_path, "attacker.key")
        _enroll_key(tmp_path, root_key, attacker_key,
                    identity="attacker@example.org")
        attacker = _load_signer(attacker_key)
        akey = _sign.public_key_id(attacker.public_key())

        _unguard(tmp_path)
        conn = _raw(tmp_path)
        tip1 = conn.execute(
            "SELECT tip FROM verdict_chain WHERE seq=1").fetchone()["tip"]
        v3 = dict(conn.execute(
            "SELECT * FROM contradiction_verdicts WHERE verdict_id='v3'"
        ).fetchone())

        conn.execute("DELETE FROM contradiction_verdicts WHERE verdict_id='v2'")
        conn.execute("DELETE FROM verdict_chain WHERE seq=2")

        # Claim the surviving verdict, exactly as the read path rebuilds it.
        new_sig = attacker.sign(_contradiction_verdict_pae({
            "verdict_id": v3["verdict_id"],
            "member_claim_id": v3["member_claim_id"],
            "other_claim_id": v3["other_claim_id"],
            "confidence": _json.loads(v3["confidence_json"] or "{}"),
        }))
        conn.execute(
            "UPDATE contradiction_verdicts SET issuer_keyid=?, signature=? "
            "WHERE verdict_id='v3'", (akey, new_sig),
        )
        record = {
            "seq": 2, "prev_tip": tip1, "verdict_kind": "contradiction",
            "verdict_id": "v3",
            "verdict_digest": hashlib.sha256(new_sig).hexdigest(),
            "issuer_keyid": akey,
        }
        conn.execute(
            "UPDATE verdict_chain SET seq=?, prev_tip=?, tip=?, "
            "verdict_digest=?, issuer_keyid=?, signature=? WHERE seq=3",
            (2, tip1, _verdict_chain_tip(record), record["verdict_digest"],
             akey, attacker.sign(_verdict_chain_link_pae(record))),
        )
        conn.commit()

        assert verify_verdict_chain(conn) == (), (
            "the residual is closed: update the guarantee in "
            "verify_verdict_chain and in docs/reference/data-model.mdx"
        )
        assert verdict_chain_coverage(conn) == (2, 2)
        conn.close()

    def test_a_removed_suffix_leaves_a_chain_that_verifies(
        self, tmp_path: Path,
    ) -> None:
        """The residual the release notes name, pinned as behaviour.

        Nothing inside the file records that the chain was ever longer. What
        moves is the coverage pair, which is why the completeness section
        reports it and why closing this needs the tip written somewhere the
        attacker does not hold.
        """
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=3)
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        conn.execute("DELETE FROM contradiction_verdicts WHERE verdict_id='v3'")
        conn.execute("DELETE FROM verdict_chain WHERE seq = 3")
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert verify_verdict_chain(g._conn) == ()
            assert verdict_chain_coverage(g._conn) == (2, 2)

    def test_an_issuer_can_rewrite_its_own_links(self, tmp_path: Path) -> None:
        """The other residual, and it cannot be closed.

        A key can always restate its own view of its own verdicts. On a graph
        where one issuer signed everything, that issuer can rewrite the whole
        chain. What the chain holds out is the project operator, an outside
        attacker with file access, and every other peer.
        """
        root_key, witnesses, _ = _graph_with_verdicts(
            tmp_path, count=3, issuers=1,
        )
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        tip1 = conn.execute(
            "SELECT tip FROM verdict_chain WHERE seq = 1"
        ).fetchone()["tip"]
        link3 = conn.execute(
            "SELECT * FROM verdict_chain WHERE seq = 3"
        ).fetchone()
        conn.execute("DELETE FROM contradiction_verdicts WHERE verdict_id='v2'")
        conn.execute("DELETE FROM verdict_chain WHERE seq = 2")
        record = {
            "seq": 2, "prev_tip": tip1, "verdict_kind": "contradiction",
            "verdict_id": "v3", "verdict_digest": link3["verdict_digest"],
            # Its own keyid, which the tip now commits to. That does not stop
            # the issuer: it is restating its own authorship, which is exactly
            # the thing no signature can hold it to.
            "issuer_keyid": link3["issuer_keyid"],
        }
        issuer = _load_signer(witnesses[0])
        conn.execute(
            "UPDATE verdict_chain SET seq=?, prev_tip=?, tip=?, signature=? "
            "WHERE seq = 3",
            (2, tip1, _verdict_chain_tip(record),
             issuer.sign(_verdict_chain_link_pae(record))),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert verify_verdict_chain(g._conn) == ()

    def test_verdicts_older_than_the_chain_show_as_uncovered(
        self, tmp_path: Path,
    ) -> None:
        """An upgraded graph carries verdicts with no link, and says so.

        Simulated by removing the links alone, which is the state a graph
        written before the table existed opens in. The pair is the whole point:
        the gap is a number an operator reads, not a silence in the middle of
        an artifact about absence.
        """
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=3)
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        conn.execute("DELETE FROM verdict_chain")
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert verify_verdict_chain(g._conn) == ()
            assert verdict_chain_coverage(g._conn) == (0, 3)


class TestTheBackupSections:
    def test_the_chain_and_the_witness_are_written(
        self, tmp_path: Path,
    ) -> None:
        _graph_with_verdicts(tmp_path, count=2)
        data = tomllib.loads((tmp_path / "claims.toml").read_text())
        assert set(data["verdict_chain"]) == {"1", "2"}
        link = data["verdict_chain"]["1"]
        assert link["prev_tip"] == ""
        assert link["verdict_id"] == "v1"
        base64.b64decode(link["signature"])

        witness = data["completeness"]
        assert witness["verdict_chain_covered"] == 2
        assert witness["verdicts_total"] == 2
        assert witness["sections"]["contradiction_verdicts"] == 2
        assert len(witness["digest"]) == 64

    def test_the_witness_counts_what_the_file_holds(
        self, tmp_path: Path,
    ) -> None:
        """Truncating the file leaves it disagreeing with itself.

        The digest is not a signature and nothing here pretends otherwise: an
        attacker recomputes it in a line. What it buys is that a file damaged
        or hand-edited without care stops adding up.
        """
        _graph_with_verdicts(tmp_path, count=2)
        data = tomllib.loads((tmp_path / "claims.toml").read_text())
        claimed = data["completeness"]["sections"]["claims"]
        assert claimed == len(data["claims"])

    def test_the_witness_is_last_so_its_digest_covers_the_chain(
        self, tmp_path: Path,
    ) -> None:
        """The digest is over the file's own bytes, above the table itself."""
        from mareforma.db.core import verify_completeness_digest

        _graph_with_verdicts(tmp_path, count=2)
        toml_path = tmp_path / "claims.toml"
        raw = toml_path.read_text()
        assert "verdict_chain" in tomllib.loads(raw)
        # The table is last, so its own bytes are not inside what it covers.
        # Asserted as a position, not as "the tail ends with the tail", which
        # is true of any file containing the word wherever it sits.
        head, sep, tail = raw.partition("[completeness]")
        assert sep, "no completeness section was written"
        assert "[" not in tail.split("]", 1)[1], (
            "another section follows [completeness], so the digest covers "
            "bytes that are not above it"
        )
        assert verify_completeness_digest(toml_path)

    def test_an_edited_backup_stops_adding_up(self, tmp_path: Path) -> None:
        """Not a signature, and the test says which threat it answers.

        An attacker recomputes the digest in a line. Truncation, corruption and
        hand-editing do not, which is the whole of what this buys.
        """
        from mareforma.db.core import verify_completeness_digest

        _graph_with_verdicts(tmp_path, count=2)
        toml_path = tmp_path / "claims.toml"
        raw = toml_path.read_text()

        # Truncated mid-body: the table is gone with everything after it.
        cut = tmp_path / "cut.toml"
        cut.write_text(raw[: len(raw) // 2])
        assert not verify_completeness_digest(cut)

        # Edited body, table intact: the digest no longer reproduces.
        edited = tmp_path / "edited.toml"
        edited.write_text(raw.replace("claim 0", "claim 0 tampered", 1))
        assert not verify_completeness_digest(edited)

    def test_restore_says_so_when_the_file_does_not_add_up(
        self, tmp_path: Path,
    ) -> None:
        """The table is only worth writing if a restore consults it.

        Nothing did, so a backup that had lost rows rebuilt a shorter graph and
        reported success, and afterwards a graph short a few claims looks
        exactly like a complete one. Disclosed rather than refused: this is the
        recovery path and an operator who edited the file on purpose still has
        to be able to recover from it.
        """
        # Plain claims, nothing referring to them, so the row can go without
        # tripping a foreign key on the way in: what is under test is the file
        # disagreeing with itself, not the graph refusing a dangling reference.
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            for i in range(3):
                g.assert_claim(f"claim {i}", generated_by=f"run{i}")
        raw = (tmp_path / "claims.toml").read_text(encoding="utf-8")

        # One claim entry removed, the completeness table left saying otherwise.
        doc = tomllib.loads(raw)
        victim = sorted(doc["claims"])[0]
        del doc["claims"][victim]
        import tomli_w
        body = tomli_w.dumps({k: v for k, v in doc.items() if k != "completeness"})
        tail = tomli_w.dumps({"completeness": doc["completeness"]})

        short = tmp_path / "recovered"
        short.mkdir()
        (short / "claims.toml").write_text(body + tail, encoding="utf-8")

        with pytest.warns(UserWarning, match="disagrees with itself"):
            restore(short)

    def test_a_truncation_that_takes_the_table_is_not_detectable(
        self, tmp_path: Path,
    ) -> None:
        """The boundary of the check above, pinned so nobody trusts it further.

        ``[completeness]`` is the last thing written, so a real truncation takes
        it. The count that would have caught the loss went with the bytes that
        were lost, and a file with no table is also what every backup written
        before the table existed looks like. This records that the recovery
        proceeds and says nothing, which is the honest state of it, not a bug
        waiting to be filed twice.
        """
        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            for i in range(3):
                g.assert_claim(f"claim {i}", generated_by=f"run{i}")
        raw = (tmp_path / "claims.toml").read_text(encoding="utf-8")

        short = tmp_path / "recovered"
        short.mkdir()
        (short / "claims.toml").write_text(
            raw[: raw.rindex("[claims.")], encoding="utf-8",
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            report = restore(short)
        assert report["claims_restored"] == 2
        assert not [
            w for w in caught if "disagrees with itself" in str(w.message)
        ], "the check claimed a truncation it cannot see"

    def test_a_backup_with_no_table_does_not_read_as_verified(
        self, tmp_path: Path,
    ) -> None:
        """Every backup written before the table existed lands here."""
        from mareforma.db.core import verify_completeness_digest

        legacy = tmp_path / "legacy.toml"
        legacy.write_text('[claims]\n[claims.abc]\ntext = "a claim"\n')
        assert not verify_completeness_digest(legacy)

    def test_a_claim_cannot_forge_the_digest_boundary(
        self, tmp_path: Path,
    ) -> None:
        """The split is a line, so claim text must not be able to write one.

        Serialized values keep their newlines escaped, so a claim whose text
        spells the header cannot move where a reader cuts the file.
        """
        from mareforma.db.core import verify_completeness_digest

        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim('text\n[completeness]\ndigest = "0"\n')
        toml_path = tmp_path / "claims.toml"
        assert toml_path.read_text().count("\n[completeness]\n") == 1
        assert verify_completeness_digest(toml_path)


class TestItSurvivesRecovery:
    def test_the_chain_round_trips_through_a_catastrophic_restore(
        self, tmp_path: Path,
    ) -> None:
        """The file is the recovery artifact, so the chain has to come back.

        A restore that dropped the chain would rebuild a graph whose verdicts
        are all uncovered, which reads exactly like a graph somebody stripped.
        """
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=3)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            before = verdict_chain_tip(g._conn)

        import shutil
        shutil.rmtree(tmp_path / ".mareforma")
        restore(tmp_path)

        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert verdict_chain_tip(g._conn) == before
            assert verify_verdict_chain(g._conn) == ()
            assert verdict_chain_coverage(g._conn) == (3, 3)

    def test_a_broken_chain_restores_broken_rather_than_absent(
        self, tmp_path: Path,
    ) -> None:
        """Restore replays the chain faithfully and does not sit in judgement.

        Dropping an unverifiable link on the way in would turn evidence of
        tampering into silence, which is the direction the whole artifact
        exists to close. The read path is what reports it, on every read.
        """
        root_key, _, _ = _graph_with_verdicts(tmp_path, count=3)
        _unguard(tmp_path)
        conn = _raw(tmp_path)
        conn.execute("DELETE FROM verdict_chain WHERE seq = 2")
        conn.commit()
        conn.close()
        # Rewrite the backup so the damaged chain is what the file carries.
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.assert_claim("a later claim that rewrites the backup")

        import shutil
        shutil.rmtree(tmp_path / ".mareforma")
        restore(tmp_path)

        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert verify_verdict_chain(g._conn) != ()

    @pytest.mark.parametrize("edit, match", [
        # A section set to a scalar. The sort helpers call .items() on it.
        (lambda d: d.__setitem__("verdict_chain", "not a table"),
         "verdict_chain"),
        (lambda d: d.__setitem__("grounding_attestations", "not a table"),
         "grounding_attestations"),
        # An entry missing a key the replay needs.
        (lambda d: d["verdict_chain"]["1"].pop("tip"),
         r"\[verdict_chain\] entry"),
        (lambda d: d["grounding_attestations"][
            next(iter(d["grounding_attestations"]))].pop("receipt_digest"),
         r"\[grounding_attestations\] entry"),
        # A signature base64 cannot decode. Note "!!!" would NOT do: b64decode
        # discards characters outside the alphabet, so it returns empty rather
        # than raising, and the link restores with a signature that fails
        # verification later. That is the design (restore replays, the read
        # path reports), so the case here is one that genuinely cannot decode.
        (lambda d: d["verdict_chain"]["1"].__setitem__("signature", "a"),
         r"\[verdict_chain\] entry"),
        # Two keys that normalise to one seq: the primary key refuses the
        # second, and that arrives as an IntegrityError from executemany.
        (lambda d: d["verdict_chain"].__setitem__(
            "01", dict(d["verdict_chain"]["1"])),
         "does not form a chain"),
    ], ids=["chain-scalar", "attestation-scalar", "chain-missing-key",
            "attestation-missing-key", "chain-bad-base64", "chain-duplicate-seq"])
    def test_a_hand_edited_section_is_refused_with_a_remedy(
        self, tmp_path: Path, edit, match: str,
    ) -> None:
        """Every refusal here has to be a RestoreError, not a raw exception.

        The CLI catches RestoreError and nothing else, so a leak past it prints
        a traceback and offers the operator nothing. These sections were outside
        both guards the other sections have: the shape check before the sort,
        and the IntegrityError translation around the insert. Both leaked, and
        a hand-edited backup is the shape they exist for.
        """
        import shutil
        import tomli_w

        from mareforma.db.errors import RestoreError

        from mareforma.observe import observe

        root_key, _, _ = _graph_with_verdicts(tmp_path, count=2)
        # One claim with an axis the observer earned, so the attestation
        # section is present and the parametrised edits above have something
        # to damage.
        dataset = tmp_path / "trial.csv"
        dataset.write_text("arm,outcome\ntreat,1\n")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            with observe(cites=str(dataset.resolve())) as handle:
                dataset.read_text()
            g.assert_claim(
                "an observed finding", classification="ANALYTICAL",
                predicate_payload={
                    "data_sources": [str(dataset.resolve())], "data_ids": [],
                },
                observed_grounding=handle.verdict.to_signed_dict(),
            )

        toml_path = tmp_path / "claims.toml"
        data = tomllib.loads(toml_path.read_text())
        assert data.get("grounding_attestations"), (
            "the fixture must produce an attestation, or these cases test "
            "nothing"
        )
        edit(data)
        toml_path.write_text(tomli_w.dumps(data))

        shutil.rmtree(tmp_path / ".mareforma")
        with pytest.raises(RestoreError, match=match):
            restore(tmp_path)


class TestAFailedVerdictWriteClosesItsTransaction:
    """A verdict write that fails must not take the next write down with it.

    Recording a verdict is two writes in one transaction now, the verdict and
    its link, and the second can fail where the first never could: a graph
    missing the table, or a signer that goes away between them. The handler
    caught only ``IntegrityError``, so anything else left the transaction open.

    That is not a lost verdict. It is a lost claim, later, somewhere else: the
    next ``add_claim`` on the same connection sees an open transaction, decides
    it does not own it, and never commits. The caller gets a claim id back and
    no exception, and the row is discarded when the connection closes.
    """

    @staticmethod
    def _fail_the_link(monkeypatch, exc: Exception) -> None:
        from mareforma.db import core as _core

        def boom(*args, **kwargs):
            raise exc

        monkeypatch.setattr(_core, "_verdict_chain_link_pae", boom)

    @pytest.mark.parametrize("exc", [
        RuntimeError("the signing device went away"),
        sqlite3.OperationalError("no such table: verdict_chain"),
    ], ids=["signer-died", "schema-gone"])
    def test_the_next_write_is_not_silently_discarded(
        self, tmp_path: Path, monkeypatch, exc: Exception,
    ) -> None:
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            a = g.assert_claim("claim one")
            b = g.assert_claim("claim two")
        witness = _bootstrap_key(tmp_path, "w0.key")
        _enroll_key(tmp_path, root_key, witness, identity="w0@example.org")

        conn = _raw(tmp_path)
        before = conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
        conn.close()

        with mareforma.open(tmp_path, key_path=witness) as g:
            self._fail_the_link(monkeypatch, exc)
            with pytest.raises(type(exc)):
                g.record_contradiction_verdict(
                    verdict_id="v1", member_claim_id=a, other_claim_id=b,
                )
            monkeypatch.undo()
            assert not g._conn.in_transaction, (
                "the failed verdict left its transaction open"
            )
            g.assert_claim("a claim written after the failure")

        conn = _raw(tmp_path)
        after = conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
        conn.close()
        assert after == before + 1, (
            "assert_claim returned an id for a row that reached no disk"
        )

    def test_the_verdict_itself_does_not_survive_a_failed_link(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """The two writes are one write, so half of it must not commit."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            a = g.assert_claim("claim one")
            b = g.assert_claim("claim two")
        witness = _bootstrap_key(tmp_path, "w0.key")
        _enroll_key(tmp_path, root_key, witness, identity="w0@example.org")

        with mareforma.open(tmp_path, key_path=witness) as g:
            self._fail_the_link(monkeypatch, RuntimeError("no signer"))
            with pytest.raises(RuntimeError):
                g.record_contradiction_verdict(
                    verdict_id="v1", member_claim_id=a, other_claim_id=b,
                )
            monkeypatch.undo()

        conn = _raw(tmp_path)
        verdicts = conn.execute(
            "SELECT COUNT(*) FROM contradiction_verdicts"
        ).fetchone()[0]
        links = conn.execute("SELECT COUNT(*) FROM verdict_chain").fetchone()[0]
        conn.close()
        assert (verdicts, links) == (0, 0)


class TestTheWriterDoesNotGoQuiet:
    """The backup swallows every failure but this one."""

    def test_a_format_failure_raises_instead_of_printing(
        self, tmp_path: Path, monkeypatch, capsys,
    ) -> None:
        """Every other section degrades to a stale backup and recovers.

        An absent ``[completeness]`` section cannot be told apart from a backup
        written before the section existed, so its silence has the same shape as
        the tamper it is there to catch.
        """
        from mareforma.db import core as _core

        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a first claim")

        def boom(conn, data):
            raise RuntimeError("the chain could not be read")

        monkeypatch.setattr(_core, "_backup_verdict_chain", boom)
        with mareforma.open(tmp_path, key_path=key) as g:
            with pytest.raises(FormatArtifactError, match="format section"):
                g.assert_claim("a claim whose backup cannot be completed")
        assert "ERROR: claims.toml backup failed" not in capsys.readouterr().err

    def test_a_failing_flush_still_closes_the_graph(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """Refusing to write must not turn into leaking the connection.

        Closing a graph drains any open deferral window, which writes the
        backup, which can now raise. With the close after the drain rather than
        in a finally, the connection stayed open, the handle read as not closed,
        and the caller got the format error and a leak with it.
        """
        from mareforma.db import core as _core

        key = _bootstrap_key(tmp_path, "root.key")
        graph = mareforma.open(tmp_path, key_path=key)
        _core.suspend_backup(graph._conn)
        graph.assert_claim("a claim written inside a deferral window")

        def boom(conn, data):
            raise RuntimeError("the chain could not be read")

        monkeypatch.setattr(_core, "_backup_verdict_chain", boom)
        with pytest.raises(FormatArtifactError):
            graph.close()

        assert graph._closed is True
        with pytest.raises(sqlite3.ProgrammingError):
            graph._conn.execute("SELECT 1")

    def test_other_backup_failures_still_degrade_to_stderr(
        self, tmp_path: Path, monkeypatch, capsys,
    ) -> None:
        """The existing contract is deliberate and stays.

        graph.db is authoritative and a failed backup must not fail the
        mutation that triggered it. Only the format sections are exempt.
        """
        from mareforma.db import core as _core

        key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a first claim")

        def boom(conn, data):
            raise RuntimeError("the trust tables could not be read")

        monkeypatch.setattr(_core, "_backup_trust_tables", boom)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("a claim whose backup degrades quietly")
        assert "ERROR: claims.toml backup failed" in capsys.readouterr().err
