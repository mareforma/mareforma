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
            reps = g.replication_verdicts()
            cons = g.contradiction_verdicts()
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
        import tomli, tomli_w
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
        import tomli, tomli_w
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
        import tomli, tomli_w
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
