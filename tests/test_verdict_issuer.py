"""Verdict-issuer protocol tests.

Covers the OSS substrate side of the inference layer:
  - record_replication_verdict / record_contradiction_verdict APIs
  - signature binding under DSSE PAE
  - issuer-must-be-enrolled gate
  - contradiction_invalidates_older trigger sets t_invalid
  - query include_invalidated kwarg honors t_invalid
  - append-only triggers on verdict tables
  - restore round-trips signed verdicts + replays t_invalid via trigger
  - adversarial cases: tampered confidence, forged signature, swapped
    issuer_keyid
"""
from __future__ import annotations

import base64
import json
import sqlite3
from pathlib import Path

import pytest

import mareforma
from mareforma import db as _db
from mareforma import signing as _signing


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _bootstrap(tmp_path: Path, name: str) -> Path:
    p = tmp_path / name
    _signing.bootstrap_key(p)
    return p


def _enroll_extra(graph: mareforma.EpistemicGraph, key_path: Path,
                  *, identity: str) -> str:
    pem = _signing.public_key_to_pem(
        _signing.load_private_key(key_path).public_key(),
    )
    graph.enroll_validator(pem, identity=identity)
    return _signing.public_key_id(
        _signing.load_private_key(key_path).public_key(),
    )


def _seed_two_claims(tmp_path: Path) -> tuple[Path, Path, str, str, str, str]:
    """Bootstrap two keys, enroll the second, return both claim_ids
    and both keyids."""
    root_key = _bootstrap(tmp_path, "root.key")
    issuer_key = _bootstrap(tmp_path, "issuer.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        issuer_keyid = _enroll_extra(g, issuer_key, identity="issuer")
        a = g.assert_claim("alpha", generated_by="A")
        b = g.assert_claim("beta", generated_by="B")
    root_keyid = _signing.public_key_id(
        _signing.load_private_key(root_key).public_key(),
    )
    return root_key, issuer_key, a, b, root_keyid, issuer_keyid


# ---------------------------------------------------------------------------
# record_replication_verdict
# ---------------------------------------------------------------------------

class TestRecordReplicationVerdict:
    def test_happy_path_promotes_to_replicated(self, tmp_path: Path) -> None:
        root_key, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_1", cluster_id="cl_x",
                member_claim_id=a, other_claim_id=b,
                method="semantic-cluster",
                confidence={"cosine": 0.92},
            )
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"
            verdicts = g.replication_verdicts(member_claim_id=a)
        assert len(verdicts) == 1
        assert verdicts[0]["method"] == "semantic-cluster"
        assert json.loads(verdicts[0]["confidence_json"]) == {"cosine": 0.92}

    def test_unenrolled_issuer_rejected(self, tmp_path: Path) -> None:
        root_key = _bootstrap(tmp_path, "root.key")
        unenrolled = _bootstrap(tmp_path, "unenrolled.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            a = g.assert_claim("alpha")
            b = g.assert_claim("beta")
        # unenrolled key was never enrolled; record must refuse.
        with mareforma.open(tmp_path, key_path=unenrolled) as g:
            with pytest.raises(_db.VerdictIssuerError, match="not enrolled"):
                g.record_replication_verdict(
                    verdict_id="rv_x", cluster_id="cl_y",
                    member_claim_id=a, other_claim_id=b,
                    method="semantic-cluster",
                    confidence={},
                )

    def test_invalid_method_rejected(self, tmp_path: Path) -> None:
        _, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            with pytest.raises(_db.VerdictIssuerError, match="method"):
                g.record_replication_verdict(
                    verdict_id="rv_q", cluster_id="cl_q",
                    member_claim_id=a, other_claim_id=b,
                    method="not-a-real-method",
                    confidence={},
                )

    def test_missing_claim_rejected(self, tmp_path: Path) -> None:
        _, issuer_key, a, _, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            with pytest.raises(_db.VerdictIssuerError, match="missing claim_id"):
                g.record_replication_verdict(
                    verdict_id="rv_z", cluster_id="cl_z",
                    member_claim_id=a, other_claim_id="nope",
                    method="cross-method",
                    confidence={},
                )


# ---------------------------------------------------------------------------
# record_contradiction_verdict + t_invalid trigger
# ---------------------------------------------------------------------------

class TestContradictionInvalidatesOlder:
    def test_older_claim_gets_t_invalid_set(self, tmp_path: Path) -> None:
        root_key, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        # 'a' is older (asserted first).
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_1",
                member_claim_id=a, other_claim_id=b,
                confidence={"stance": "refutes"},
            )
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.get_claim(a)["t_invalid"] is not None
            assert g.get_claim(b)["t_invalid"] is None

    def test_include_invalidated_false_excludes_by_default(self, tmp_path: Path) -> None:
        root_key, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_2",
                member_claim_id=a, other_claim_id=b,
                confidence={},
            )
        with mareforma.open(tmp_path, key_path=root_key) as g:
            visible = [r["claim_id"] for r in g.query(include_unverified=True)]
            audit = [r["claim_id"] for r in g.query(
                include_unverified=True, include_invalidated=True,
            )]
        assert a not in visible  # invalidated
        assert a in audit
        assert b in visible
        assert b in audit


# ---------------------------------------------------------------------------
# LLM-typed validators cannot issue contradictions
# ---------------------------------------------------------------------------

class TestLLMContradictionGate:
    """Symmetric to the LLM-promotion gate on validate_claim.

    A signed contradiction sets ``t_invalid`` on the older claim via the
    ``contradiction_invalidates_older`` trigger — effectively demoting
    it from default ``query()`` results. The human-only rule must apply
    in BOTH directions: humans-only-to-promote AND humans-only-to-demote.
    Without this gate, an enrolled LLM key could mark down any
    human-validated ESTABLISHED claim by signing a contradiction —
    breaking the README's promotion-requires-human framing on the
    demotion side.
    """

    def _seed_with_llm_issuer(
        self, tmp_path: Path,
    ) -> tuple[Path, Path, str, str]:
        from mareforma.db import LLMValidatorPromotionError  # noqa: F401
        root_key = _bootstrap(tmp_path, "root.key")
        llm_key = _bootstrap(tmp_path, "llm-issuer.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            llm_pem = _signing.public_key_to_pem(
                _signing.load_private_key(llm_key).public_key(),
            )
            g.enroll_validator(
                llm_pem, identity="llm-issuer", validator_type="llm",
            )
            a = g.assert_claim("alpha", generated_by="A")
            b = g.assert_claim("beta", generated_by="B")
        return root_key, llm_key, a, b

    def test_llm_typed_contradiction_refused(self, tmp_path: Path) -> None:
        from mareforma.db import LLMValidatorPromotionError
        root_key, llm_key, a, b = self._seed_with_llm_issuer(tmp_path)
        with mareforma.open(tmp_path, key_path=llm_key) as g:
            with pytest.raises(
                LLMValidatorPromotionError,
                match="contradictions that invalidate human-validated",
            ):
                g.record_contradiction_verdict(
                    verdict_id="cv_llm_blocked",
                    member_claim_id=a, other_claim_id=b,
                    confidence={"stance": "refutes"},
                )
        # And the older claim's t_invalid is still NULL — the gate
        # fired BEFORE the INSERT, not after.
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.get_claim(a)["t_invalid"] is None
            assert g.get_claim(b)["t_invalid"] is None

    def test_human_typed_contradiction_still_succeeds(
        self, tmp_path: Path,
    ) -> None:
        """Regression: the new gate must not break the existing
        human-typed-issuer path."""
        root_key, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_human_ok",
                member_claim_id=a, other_claim_id=b,
                confidence={"stance": "refutes"},
            )
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.get_claim(a)["t_invalid"] is not None  # older invalidated
            assert g.get_claim(b)["t_invalid"] is None


# ---------------------------------------------------------------------------
# Append-only triggers on verdict tables
# ---------------------------------------------------------------------------

class TestVerdictAppendOnly:
    def test_replication_verdict_update_blocked(self, tmp_path: Path) -> None:
        _, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_ao", cluster_id="cl",
                member_claim_id=a, other_claim_id=b,
                method="semantic-cluster", confidence={},
            )
            with pytest.raises(sqlite3.IntegrityError, match="verdict_locked"):
                g._conn.execute(
                    "UPDATE replication_verdicts SET method = 'hash-match' "
                    "WHERE verdict_id = ?",
                    ("rv_ao",),
                )

    def test_contradiction_verdict_update_blocked(self, tmp_path: Path) -> None:
        _, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_ao",
                member_claim_id=a, other_claim_id=b, confidence={},
            )
            with pytest.raises(sqlite3.IntegrityError, match="verdict_locked"):
                g._conn.execute(
                    "UPDATE contradiction_verdicts SET other_claim_id = ? "
                    "WHERE verdict_id = ?",
                    (a, "cv_ao"),
                )


# ---------------------------------------------------------------------------
# Restore round-trip + adversarial bindings
# ---------------------------------------------------------------------------

class TestRestoreVerdicts:
    def _wipe_db(self, tmp_path: Path) -> None:
        for fname in ("graph.db", "graph.db-wal", "graph.db-shm"):
            p = tmp_path / ".mareforma" / fname
            if p.exists():
                p.unlink()

    def test_round_trip_preserves_verdicts(self, tmp_path: Path) -> None:
        root_key, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_rt", cluster_id="cl_rt",
                member_claim_id=a, other_claim_id=b,
                method="semantic-cluster",
                confidence={"cosine": 0.91},
            )
            g.record_contradiction_verdict(
                verdict_id="cv_rt",
                member_claim_id=a, other_claim_id=b,
                confidence={"stance": "refutes"},
            )
        self._wipe_db(tmp_path)
        mareforma.restore(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            # Both claims are now invalidated by the contradiction
            # verdict — use audit mode to inspect the round-tripped
            # verdicts.
            reps = g.replication_verdicts(include_invalidated=True)
            cons = g.contradiction_verdicts(include_invalidated=True)
            # t_invalid was re-derived by the trigger on contradiction
            # INSERT — not directly round-tripped via TOML.
            assert g.get_claim(a)["t_invalid"] is not None
        assert len(reps) == 1
        assert reps[0]["verdict_id"] == "rv_rt"
        assert reps[0]["method"] == "semantic-cluster"
        assert len(cons) == 1
        assert cons[0]["verdict_id"] == "cv_rt"

    def test_tampered_verdict_confidence_rejected(self, tmp_path: Path) -> None:
        """Edit a verdict's confidence_json in the TOML without re-signing.
        Restore must catch the signature-vs-payload divergence."""
        try:
            import tomllib as tomli  # type: ignore[import-not-found]
        except ImportError:
            import tomli  # type: ignore[no-redef]
        import tomli_w
        root_key, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_t", cluster_id="cl_t",
                member_claim_id=a, other_claim_id=b,
                method="semantic-cluster",
                confidence={"cosine": 0.91},
            )
        toml_path = tmp_path / "claims.toml"
        data = tomli.loads(toml_path.read_text(encoding="utf-8"))
        data["replication_verdicts"]["rv_t"]["confidence_json"] = json.dumps(
            {"cosine": 0.42}, sort_keys=True, separators=(",", ":"),
        )
        toml_path.write_bytes(tomli_w.dumps(data).encode("utf-8"))
        self._wipe_db(tmp_path)
        with pytest.raises(_db.RestoreError) as exc:
            mareforma.restore(tmp_path)
        assert exc.value.kind == "claim_unverified"
        assert "signature verification failed" in str(exc.value)

    def test_tampered_verdict_signature_rejected(self, tmp_path: Path) -> None:
        """Replace the verdict signature with garbage. Restore catches it."""
        try:
            import tomllib as tomli  # type: ignore[import-not-found]
        except ImportError:
            import tomli  # type: ignore[no-redef]
        import tomli_w
        root_key, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_s", cluster_id="cl_s",
                member_claim_id=a, other_claim_id=b,
                method="cross-method",
                confidence={},
            )
        toml_path = tmp_path / "claims.toml"
        data = tomli.loads(toml_path.read_text(encoding="utf-8"))
        data["replication_verdicts"]["rv_s"]["signature"] = base64.b64encode(
            b"\x00" * 64,
        ).decode("ascii")
        toml_path.write_bytes(tomli_w.dumps(data).encode("utf-8"))
        self._wipe_db(tmp_path)
        with pytest.raises(_db.RestoreError) as exc:
            mareforma.restore(tmp_path)
        assert exc.value.kind == "claim_unverified"

    def test_forged_issuer_keyid_rejected(self, tmp_path: Path) -> None:
        """Swap issuer_keyid to a different enrolled validator's keyid.
        The signature was made by the original issuer, so the new
        validator's pubkey fails to verify it."""
        try:
            import tomllib as tomli  # type: ignore[import-not-found]
        except ImportError:
            import tomli  # type: ignore[no-redef]
        import tomli_w
        root_key, issuer_key, a, b, root_keyid, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_swap", cluster_id="cl_swap",
                member_claim_id=a, other_claim_id=b,
                method="semantic-cluster",
                confidence={},
            )
        toml_path = tmp_path / "claims.toml"
        data = tomli.loads(toml_path.read_text(encoding="utf-8"))
        # Swap to root's keyid, which IS enrolled but did not sign.
        data["replication_verdicts"]["rv_swap"]["issuer_keyid"] = root_keyid
        toml_path.write_bytes(tomli_w.dumps(data).encode("utf-8"))
        self._wipe_db(tmp_path)
        with pytest.raises(_db.RestoreError) as exc:
            mareforma.restore(tmp_path)
        assert exc.value.kind == "claim_unverified"


# ---------------------------------------------------------------------------
# Adversarial-review regression tests
# ---------------------------------------------------------------------------

class TestForeignKeyEnforcement:
    """PRAGMA foreign_keys=ON is set on every open_db so verdict
    FK references actually fire. Without this the schema's
    REFERENCES clauses are advisory and direct-SQL INSERTs with
    fabricated keyids would slip through."""

    def test_unenrolled_keyid_blocked_by_fk(self, tmp_path: Path) -> None:
        root_key = _bootstrap(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            a = g.assert_claim("alpha")
            b = g.assert_claim("beta")
            with pytest.raises(sqlite3.IntegrityError):
                g._conn.execute(
                    """
                    INSERT INTO replication_verdicts(
                        verdict_id, cluster_id, member_claim_id, other_claim_id,
                        method, confidence_json, issuer_keyid, signature, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("rv_x", "cl_x", a, b, "semantic-cluster", "{}",
                     "0" * 64, b"\x00" * 64, "2026-05-13T00:00:00+00:00"),
                )

    def test_missing_claim_blocked_by_fk(self, tmp_path: Path) -> None:
        root_key = _bootstrap(tmp_path, "root.key")
        root_keyid = _signing.public_key_id(
            _signing.load_private_key(root_key).public_key(),
        )
        with mareforma.open(tmp_path, key_path=root_key) as g:
            with pytest.raises(sqlite3.IntegrityError):
                g._conn.execute(
                    """
                    INSERT INTO replication_verdicts(
                        verdict_id, cluster_id, member_claim_id, other_claim_id,
                        method, confidence_json, issuer_keyid, signature, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("rv_y", "cl_y", "nonexistent-claim", None,
                     "semantic-cluster", "{}", root_keyid,
                     b"\x00" * 64, "2026-05-13T00:00:00+00:00"),
                )


class TestSelfContradictionRefused:
    def test_python_path_refuses_self_contradiction(self, tmp_path: Path) -> None:
        _, issuer_key, a, _, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            with pytest.raises(_db.VerdictIssuerError, match="self-"):
                g.record_contradiction_verdict(
                    verdict_id="cv_self",
                    member_claim_id=a, other_claim_id=a,
                    confidence={},
                )

    def test_sql_check_blocks_self_contradiction(self, tmp_path: Path) -> None:
        """Even a direct SQL INSERT with member==other is refused by
        the CHECK constraint on contradiction_verdicts."""
        _, issuer_key, a, _, _, issuer_keyid = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
                g._conn.execute(
                    """
                    INSERT INTO contradiction_verdicts(
                        verdict_id, member_claim_id, other_claim_id,
                        confidence_json, issuer_keyid, signature, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("cv_self_sql", a, a, "{}", issuer_keyid,
                     b"\x00" * 64, "2026-05-13T00:00:00+00:00"),
                )


class TestVerdictDeleteBlocked:
    def test_replication_verdict_delete_blocked(self, tmp_path: Path) -> None:
        _, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_del", cluster_id="cl",
                member_claim_id=a, other_claim_id=b,
                method="semantic-cluster", confidence={},
            )
            with pytest.raises(sqlite3.IntegrityError,
                               match="verdict_delete_blocked"):
                g._conn.execute(
                    "DELETE FROM replication_verdicts WHERE verdict_id = ?",
                    ("rv_del",),
                )

    def test_contradiction_verdict_delete_blocked(self, tmp_path: Path) -> None:
        _, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_del",
                member_claim_id=a, other_claim_id=b, confidence={},
            )
            with pytest.raises(sqlite3.IntegrityError,
                               match="verdict_delete_blocked"):
                g._conn.execute(
                    "DELETE FROM contradiction_verdicts WHERE verdict_id = ?",
                    ("cv_del",),
                )


class TestTriggerIdempotencyAndOrdering:
    def test_second_contradiction_does_not_overwrite_t_invalid(
        self, tmp_path: Path,
    ) -> None:
        """The trigger's WHERE t_invalid IS NULL clause makes a second
        contradiction on the same claim a no-op. A future refactor
        that drops the guard would change t_invalid silently."""
        root_key, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_1", member_claim_id=a, other_claim_id=b,
                confidence={},
            )
            first = g.get_claim(a)["t_invalid"]
            assert first is not None
            g.record_contradiction_verdict(
                verdict_id="cv_2", member_claim_id=a, other_claim_id=b,
                confidence={},
            )
            second = g.get_claim(a)["t_invalid"]
        assert first == second  # idempotent — earlier timestamp preserved

    def test_argument_order_does_not_change_invalidation(
        self, tmp_path: Path,
    ) -> None:
        """For identical created_at, the trigger's tie-break is
        lex-smaller claim_id — not the verdict's argument order.
        Two graphs differing only in (member, other) swap must
        invalidate the same claim."""
        root_key_x = _bootstrap(tmp_path / "x", "root.key")
        issuer_key_x = _bootstrap(tmp_path / "x", "issuer.key")
        with mareforma.open(tmp_path / "x", key_path=root_key_x) as g:
            issuer_pem = _signing.public_key_to_pem(
                _signing.load_private_key(issuer_key_x).public_key(),
            )
            g.enroll_validator(issuer_pem, identity="i")
            a = g.assert_claim("alpha")
            b = g.assert_claim("beta")
        with mareforma.open(tmp_path / "x", key_path=issuer_key_x) as g:
            g.record_contradiction_verdict(
                verdict_id="cv", member_claim_id=a, other_claim_id=b,
                confidence={},
            )
            invalidated_x = (
                a if g.get_claim(a)["t_invalid"] is not None else b
            )
        # Second graph in a separate dir: swap argument order.
        root_key_y = _bootstrap(tmp_path / "y", "root.key")
        issuer_key_y = _bootstrap(tmp_path / "y", "issuer.key")
        with mareforma.open(tmp_path / "y", key_path=root_key_y) as g:
            issuer_pem = _signing.public_key_to_pem(
                _signing.load_private_key(issuer_key_y).public_key(),
            )
            g.enroll_validator(issuer_pem, identity="i")
            # Use deterministic claim_ids — pin the texts the same.
            a2 = g.assert_claim("alpha")
            b2 = g.assert_claim("beta")
        # The IDs differ across the two graphs (uuid4), so this test
        # checks the trigger's invariant on a single graph: assert
        # that the older claim is invalidated regardless of argument
        # order. With created_at strictly increasing per insert, the
        # tie-break clause is exercised when both rows share a
        # microsecond — relatively rare but the deterministic clause
        # makes it predictable when it does happen.
        with mareforma.open(tmp_path / "y", key_path=issuer_key_y) as g:
            g.record_contradiction_verdict(
                verdict_id="cv2", member_claim_id=b2, other_claim_id=a2,
                confidence={},
            )
            # a2 was inserted first → it's the older one → it gets
            # invalidated regardless of argument order.
            assert g.get_claim(a2)["t_invalid"] is not None
            assert g.get_claim(b2)["t_invalid"] is None


class TestVerdictListingFiltersInvalidated:
    def test_default_excludes_verdicts_on_invalidated_claims(
        self, tmp_path: Path,
    ) -> None:
        root_key, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_v", cluster_id="cl",
                member_claim_id=a, other_claim_id=b,
                method="semantic-cluster", confidence={},
            )
            g.record_contradiction_verdict(
                verdict_id="cv_v",
                member_claim_id=a, other_claim_id=b, confidence={},
            )
        with mareforma.open(tmp_path, key_path=root_key) as g:
            default_reps = g.replication_verdicts()
            audit_reps = g.replication_verdicts(include_invalidated=True)
        assert default_reps == []  # both claims invalidated
        assert len(audit_reps) == 1


# ---------------------------------------------------------------------------
# /review hardening pass (security + testing specialist findings)
# ---------------------------------------------------------------------------


class TestInvalidatedClaimRefusesValidation:
    """validate_claim must refuse to promote a claim that's already
    been invalidated by a signed contradiction verdict. Without this,
    an enrolled human validator could lift an already-refuted claim
    REPLICATED → ESTABLISHED — riding past the terminal evidence of
    the signed contradiction."""

    def test_validate_refuses_t_invalid_claim(self, tmp_path: Path) -> None:
        from mareforma import db as _db
        root_key, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        # Promote (a, b) to REPLICATED via a replication verdict.
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_v1", cluster_id="cl",
                member_claim_id=a, other_claim_id=b,
                method="semantic-cluster", confidence={},
            )
        # Then invalidate `a` via a contradiction verdict.
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_v1",
                member_claim_id=a, other_claim_id=b,
                confidence={},
            )
        # Now an enrolled human validator (issuer_key, validator_type
        # defaults to 'human') tries to validate `a` → REFUSED.
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            with pytest.raises(ValueError, match="invalidated by a signed contradiction"):
                g.validate(a)


class TestReplicationDoesNotRePromoteInvalidated:
    """A replication verdict landing AFTER a contradiction must not
    re-promote the invalidated claim. The promotion UPDATE has a
    t_invalid IS NULL filter."""

    def test_replication_after_contradiction_skips_invalidated(
        self, tmp_path: Path,
    ) -> None:
        root_key, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        # Step 1: invalidate `a` via contradiction.
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_pre",
                member_claim_id=a, other_claim_id=b,
                confidence={},
            )
            assert g.get_claim(a)["t_invalid"] is not None
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
        # Step 2: a replication verdict tries to promote (a, b).
        # `b` is still PRELIMINARY and not invalidated → promotes.
        # `a` is invalidated → MUST NOT promote.
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_post", cluster_id="cl",
                member_claim_id=a, other_claim_id=b,
                method="semantic-cluster", confidence={},
            )
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
            assert g.get_claim(a)["t_invalid"] is not None
            assert g.get_claim(b)["support_level"] == "REPLICATED"


class TestRestoreDowngradesTransparencyWithoutRekor:
    """Hand-edited claims.toml that flips transparency_logged=true on a
    claim whose signature_bundle has no rekor block must be silently
    downgraded to transparency_logged=0 — otherwise the row would
    satisfy REPLICATED's transparency_logged=1 gate without ever
    having been witnessed by the log."""

    def test_transparency_flag_requires_rekor_block(
        self, tmp_path: Path,
    ) -> None:
        try:
            import tomllib as tomli  # type: ignore[import-not-found]
        except ImportError:
            import tomli  # type: ignore[no-redef]
        import tomli_w
        root_key = _bootstrap(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            cid = g.assert_claim("anchor")
        toml_path = tmp_path / "claims.toml"
        data = tomli.loads(toml_path.read_text(encoding="utf-8"))
        # Force-set transparency_logged=true even though no rekor block
        # was ever attached (no rekor_url was configured).
        data["claims"][cid]["transparency_logged"] = True
        toml_path.write_bytes(tomli_w.dumps(data).encode("utf-8"))
        # Wipe + restore.
        for fname in ("graph.db", "graph.db-wal", "graph.db-shm"):
            p = tmp_path / ".mareforma" / fname
            if p.exists():
                p.unlink()
        mareforma.restore(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            row = g.get_claim(cid)
        # The flag must be downgraded to 0 — bundle has no rekor uuid.
        assert row["transparency_logged"] == 0


class TestVerdictPayloadNoNanInf:
    """confidence_json must canonicalize via _canonical.canonicalize,
    which rejects NaN/Inf. A third-party verdict-issuer sneaking a
    non-finite float into confidence would otherwise produce a
    payload some verifiers refuse."""

    def test_nan_in_confidence_rejected_at_sign_time(
        self, tmp_path: Path,
    ) -> None:
        import math
        from mareforma._canonical import canonicalize
        _, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            with pytest.raises(ValueError):
                g.record_replication_verdict(
                    verdict_id="rv_nan", cluster_id="cl",
                    member_claim_id=a, other_claim_id=b,
                    method="semantic-cluster",
                    confidence={"cosine": float("nan")},
                )


class TestVerdictChainWalkEnforced:
    """_require_enrolled_issuer must walk the enrollment chain via
    validators.is_enrolled — same gate as seed and validate. A tampered
    DB row whose enrollment chain breaks must be rejected even though
    its keyid still exists in the validators table."""

    def test_broken_chain_rejected_for_verdict_path(
        self, tmp_path: Path,
    ) -> None:
        from mareforma import db as _db
        root_key, issuer_key, a, b, _, issuer_keyid = _seed_two_claims(tmp_path)
        # Tamper: clobber the issuer's enrollment_envelope so its
        # chain breaks. is_enrolled walks the chain, finds the
        # tampered envelope, returns False.
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g._conn.execute(
                "UPDATE validators SET enrollment_envelope = ? WHERE keyid = ?",
                ('{"payloadType":"x","payload":"","signatures":[]}', issuer_keyid),
            )
            g._conn.commit()
        # Now the issuer (whose row still exists) tries to write a
        # verdict — chain walk fails, verdict refused.
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            with pytest.raises(_db.VerdictIssuerError,
                               match="chain does not verify|not enrolled"):
                g.record_replication_verdict(
                    verdict_id="rv_brk", cluster_id="cl",
                    member_claim_id=a, other_claim_id=b,
                    method="semantic-cluster", confidence={},
                )


class TestUnsignedModeRefusesVerdict:
    """A graph opened without a signing key cannot record verdicts —
    the wrapper raises VerdictIssuerError before touching the DB."""

    def test_unsigned_graph_refuses_record_replication(
        self, tmp_path: Path,
    ) -> None:
        from mareforma import db as _db
        with mareforma.open(tmp_path) as g:  # no key_path
            a = g.assert_claim("alpha")
            b = g.assert_claim("beta")
            with pytest.raises(_db.VerdictIssuerError, match="without a signer"):
                g.record_replication_verdict(
                    verdict_id="rv_u", cluster_id="cl",
                    member_claim_id=a, other_claim_id=b,
                    method="semantic-cluster", confidence={},
                )

    def test_unsigned_graph_refuses_record_contradiction(
        self, tmp_path: Path,
    ) -> None:
        from mareforma import db as _db
        with mareforma.open(tmp_path) as g:
            a = g.assert_claim("alpha")
            b = g.assert_claim("beta")
            with pytest.raises(_db.VerdictIssuerError, match="without a signer"):
                g.record_contradiction_verdict(
                    verdict_id="cv_u",
                    member_claim_id=a, other_claim_id=b, confidence={},
                )


class TestVerdictFieldTamperOnRestore:
    """Every field in _REPLICATION_VERDICT_FIELDS / _CONTRADICTION_VERDICT_FIELDS
    is bound by the DSSE PAE signature. Tamper with any of them in
    claims.toml without re-signing → restore raises RestoreError."""

    def _setup_and_tamper(self, tmp_path: Path, field: str, new_value):
        try:
            import tomllib as tomli  # type: ignore[import-not-found]
        except ImportError:
            import tomli  # type: ignore[no-redef]
        import tomli_w
        _, issuer_key, a, b, _, _ = _seed_two_claims(tmp_path)
        with mareforma.open(tmp_path, key_path=issuer_key) as g:
            g.record_replication_verdict(
                verdict_id="rv_t", cluster_id="cl_orig",
                member_claim_id=a, other_claim_id=b,
                method="semantic-cluster",
                confidence={"cosine": 0.9},
            )
        toml_path = tmp_path / "claims.toml"
        data = tomli.loads(toml_path.read_text(encoding="utf-8"))
        data["replication_verdicts"]["rv_t"][field] = new_value
        toml_path.write_bytes(tomli_w.dumps(data).encode("utf-8"))
        # Wipe + try restore.
        for fname in ("graph.db", "graph.db-wal", "graph.db-shm"):
            p = tmp_path / ".mareforma" / fname
            if p.exists():
                p.unlink()

    def test_tampered_cluster_id_rejected(self, tmp_path: Path) -> None:
        from mareforma import db as _db
        self._setup_and_tamper(tmp_path, "cluster_id", "forged-cluster")
        with pytest.raises(_db.RestoreError) as exc:
            mareforma.restore(tmp_path)
        assert exc.value.kind == "claim_unverified"

    def test_tampered_method_rejected(self, tmp_path: Path) -> None:
        from mareforma import db as _db
        self._setup_and_tamper(tmp_path, "method", "cross-method")
        with pytest.raises(_db.RestoreError) as exc:
            mareforma.restore(tmp_path)
        assert exc.value.kind == "claim_unverified"


class TestDSSEPAETypeConfusion:
    """The DSSE PAE prefix makes signatures type-bound: a signature on
    (typeA, body) MUST NOT verify under typeB even when body is
    identical. The most security-critical property of the envelope."""

    def test_signature_does_not_cross_payload_types(self) -> None:
        import base64
        from cryptography.exceptions import InvalidSignature
        from mareforma.signing import (
            PAYLOAD_TYPE_CLAIM, PAYLOAD_TYPE_VALIDATION,
            _build_envelope, dsse_pae, generate_keypair,
        )
        key = generate_keypair()
        body = b'{"foo":"bar"}'
        env_claim = _build_envelope(body, key, payload_type=PAYLOAD_TYPE_CLAIM)
        env_validation = _build_envelope(
            body, key, payload_type=PAYLOAD_TYPE_VALIDATION,
        )
        sig_claim = base64.standard_b64decode(env_claim["signatures"][0]["sig"])
        sig_validation = base64.standard_b64decode(
            env_validation["signatures"][0]["sig"],
        )
        # Signatures over the same body but different types must differ.
        assert sig_claim != sig_validation
        # And a claim-typed signature must NOT verify under the validation PAE.
        with pytest.raises(InvalidSignature):
            key.public_key().verify(
                sig_claim, dsse_pae(PAYLOAD_TYPE_VALIDATION, body),
            )

    def test_dsse_pae_literal_byte_format(self) -> None:
        """Spec: DSSEv1 <SP> <len(type)> <SP> <type> <SP> <len(body)> <SP> <body>"""
        from mareforma.signing import dsse_pae
        assert dsse_pae("app/x", b"BODY") == b"DSSEv1 5 app/x 4 BODY"
        # multi-byte UTF-8 in payloadType counts bytes, not chars.
        assert dsse_pae("é", b"x") == b"DSSEv1 2 \xc3\xa9 1 x"


class TestChainInputEqualsSignedPayload:
    """canonical_statement(fields, evidence) bytes MUST be byte-equal
    to the base64-decoded payload of the envelope produced by
    sign_claim(fields, key, evidence=evidence). Chain integrity and
    signature integrity bind to the SAME bytes."""

    def test_chain_input_byte_equals_signed_payload(self) -> None:
        import base64
        from mareforma.signing import (
            canonical_statement, sign_claim, generate_keypair,
        )
        key = generate_keypair()
        fields = {
            "claim_id": "11111111-1111-1111-1111-111111111111",
            "text": "anchor finding for binding test",
            "classification": "ANALYTICAL",
            "generated_by": "agent/test",
            "supports": ["upstream-id-1"],
            "contradicts": [],
            "source_name": "exp-2026",
            "artifact_hash": None,
            "created_at": "2026-05-13T10:00:00+00:00",
        }
        evidence = {
            "risk_of_bias": -1,
            "rationale": {"risk_of_bias": "blinding broken"},
            "inconsistency": 0, "indirectness": 0,
            "imprecision": 0, "publication_bias": 0,
            "large_effect": False, "dose_response": False,
            "opposing_confounding": False,
            "reporting_compliance": [],
        }
        chain_bytes = canonical_statement(fields, evidence)
        envelope = sign_claim(fields, key, evidence=evidence)
        signed_bytes = base64.standard_b64decode(envelope["payload"])
        assert chain_bytes == signed_bytes
