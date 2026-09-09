"""tests/test_artifact_hash.py: the SHA256 artifact-hash collapse check.

Covers:
  - normalize_artifact_hash format check (length, hex, case)
  - artifact_hash is part of the signed payload (tamper-evidence)
  - the hash is a data-distinctness signal a caller records; the promotion
    gate that read it is gone with the ladder
  - distinct or absent hashes never block: the count then runs on distinct-
    signer convergence alone
  - a non-colliding third peer lifts a collapsed pair
  - CLI ``--artifact-hash`` flag round-trips through ``claim show --json``
  - assert_claim rejects malformed hashes with ValueError
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

import mareforma
from mareforma import db as _db
from mareforma import signing as _signing
from mareforma.cli import cli
from tests._helpers import _two_signers


# Convenience: a deterministic SHA256 for "artifact bytes".
HASH_A = hashlib.sha256(b"artifact-a").hexdigest()
HASH_B = hashlib.sha256(b"artifact-b").hexdigest()


class _MissingFirstSelect:
    """A connection wrapper that forces the FIRST idempotency SELECT to
    miss, then delegates everything else to the real connection.

    Used to drive the race-recovery branch in ``assert_claim``: the
    pre-INSERT idempotency probe returns nothing (as if no row existed),
    so the INSERT proceeds and trips the UNIQUE constraint a concurrent
    writer would have created.
    """

    def __init__(self, real):
        self._real = real
        self._missed = False

    def __getattr__(self, name):
        return getattr(self._real, name)

    def execute(self, sql, params=()):
        if (
            not self._missed
            and "WHERE idempotency_key = ?" in sql
            and "FROM claims" in sql
        ):
            self._missed = True
            class _Empty:
                def fetchone(_self):
                    return None
            return _Empty()
        return self._real.execute(sql, params)

    @property
    def in_transaction(self):
        return self._real.in_transaction


# ---------------------------------------------------------------------------
# Hash format validation
# ---------------------------------------------------------------------------

class TestNormalizeArtifactHash:
    def test_none_returns_none(self) -> None:
        assert _db.normalize_artifact_hash(None) is None

    def test_lowercase_hex_passes_through(self) -> None:
        assert _db.normalize_artifact_hash(HASH_A) == HASH_A

    def test_uppercase_is_normalised_to_lowercase(self) -> None:
        assert _db.normalize_artifact_hash(HASH_A.upper()) == HASH_A

    def test_whitespace_is_stripped(self) -> None:
        assert _db.normalize_artifact_hash(f"  {HASH_A}  ") == HASH_A

    def test_short_digest_rejected(self) -> None:
        with pytest.raises(ValueError, match="64-character"):
            _db.normalize_artifact_hash("abc")

    def test_off_by_one_lengths_rejected(self) -> None:
        # Pin the exact-length contract, 63 and 65 must both fail so a future
        # regex refactor to `{64,}` or `{63,65}` is caught.
        with pytest.raises(ValueError, match="64-character"):
            _db.normalize_artifact_hash("a" * 63)
        with pytest.raises(ValueError, match="64-character"):
            _db.normalize_artifact_hash("a" * 65)

    def test_all_zero_hash_accepted(self) -> None:
        # An all-zero SHA256 is a legitimate-but-suspicious sentinel.
        # Pin the behaviour: the regex layer accepts it; downstream policy
        # (if any) is the caller's responsibility.
        assert _db.normalize_artifact_hash("0" * 64) == "0" * 64

    def test_non_hex_rejected(self) -> None:
        # 64 chars but the trailing 'z' is not hex.
        with pytest.raises(ValueError, match="64-character"):
            _db.normalize_artifact_hash("z" + "a" * 63)

    def test_sha256_prefix_rejected(self) -> None:
        # "sha256:" prefix is a common multihash convention; we accept hex only.
        with pytest.raises(ValueError):
            _db.normalize_artifact_hash(f"sha256:{HASH_A}")

    def test_non_string_rejected(self) -> None:
        with pytest.raises(ValueError, match="string"):
            _db.normalize_artifact_hash(12345)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Tamper-evidence: artifact_hash is in the signed payload
# ---------------------------------------------------------------------------

class TestArtifactHashSigned:
    def test_hash_present_in_envelope_payload(self, tmp_path: Path) -> None:
        key_path = tmp_path / "key"
        _signing.bootstrap_key(key_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim("with hash", artifact_hash=HASH_A)
        row = self._load(tmp_path, cid)
        envelope = json.loads(row["signature_bundle"])
        # Statement v1: artifact_hash lives inside the predicate.
        predicate = _signing.claim_predicate_from_envelope(envelope)
        assert predicate["artifact_hash"] == HASH_A

    def test_hash_none_serialises_as_null(self, tmp_path: Path) -> None:
        key_path = tmp_path / "key"
        _signing.bootstrap_key(key_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim("no hash")
        row = self._load(tmp_path, cid)
        envelope = json.loads(row["signature_bundle"])
        predicate = _signing.claim_predicate_from_envelope(envelope)
        assert "artifact_hash" in predicate
        assert predicate["artifact_hash"] is None

    def test_tampered_hash_in_db_blocked_by_trigger(self, tmp_path: Path) -> None:
        """Direct-SQL artifact_hash tamper on a signed claim is refused by
        the append-only signed-fields trigger."""
        import sqlite3
        import pytest as _pytest
        key_path = tmp_path / "key"
        _signing.bootstrap_key(key_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim("with hash", artifact_hash=HASH_A)
            with _pytest.raises(sqlite3.IntegrityError) as exc:
                g._conn.execute(
                    "UPDATE claims SET artifact_hash = ? WHERE claim_id = ?",
                    (HASH_B, cid),
                )
            assert "signed_field_locked" in str(exc.value)
            # Row remains untouched.
            row = _db.get_claim(g._conn, cid)
            assert row["artifact_hash"] == HASH_A

    @staticmethod
    def _load(root: Path, claim_id: str) -> dict:
        with mareforma.open(root) as g:
            return _db.get_claim(g._conn, claim_id)


# ---------------------------------------------------------------------------
# Collapse check, opt-in hash agreement
# ---------------------------------------------------------------------------

class TestAssertClaimHashParam:
    def test_malformed_hash_raises_value_error(self, open_graph) -> None:
        with pytest.raises(ValueError, match="64-character"):
            open_graph.assert_claim("bad hash", artifact_hash="not-a-hash")

    def test_uppercase_hash_is_persisted_lowercase(self, open_graph) -> None:
        cid = open_graph.assert_claim(
            "upper hash", artifact_hash=HASH_A.upper(),
        )
        row = open_graph.get_claim(cid)
        assert row["artifact_hash"] == HASH_A


# ---------------------------------------------------------------------------
# Idempotency + artifact_hash interaction
# ---------------------------------------------------------------------------

class TestIdempotencyHashConflict:
    def test_same_key_same_hash_returns_existing(self, open_graph) -> None:
        a = open_graph.assert_claim(
            "x", idempotency_key="k1", artifact_hash=HASH_A,
        )
        b = open_graph.assert_claim(
            "x", idempotency_key="k1", artifact_hash=HASH_A,
        )
        assert a == b

    def test_same_key_conflicting_hash_raises(self, open_graph) -> None:
        open_graph.assert_claim(
            "x", idempotency_key="k1", artifact_hash=HASH_A,
        )
        with pytest.raises(_db.IdempotencyConflictError, match="artifact_hash"):
            open_graph.assert_claim(
                "x", idempotency_key="k1", artifact_hash=HASH_B,
            )

    def test_same_key_hash_then_nohash_raises(self, open_graph) -> None:
        """A replay that drops the hash is also a conflict — the absence of
        the hash is itself a semantic change the caller should see."""
        open_graph.assert_claim(
            "x", idempotency_key="k1", artifact_hash=HASH_A,
        )
        with pytest.raises(_db.IdempotencyConflictError):
            open_graph.assert_claim("x", idempotency_key="k1")

    def test_same_key_nohash_then_hash_raises(self, open_graph) -> None:
        open_graph.assert_claim("x", idempotency_key="k1")
        with pytest.raises(_db.IdempotencyConflictError):
            open_graph.assert_claim(
                "x", idempotency_key="k1", artifact_hash=HASH_A,
            )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

class TestCLIArtifactHash:
    def test_cli_round_trips_artifact_hash(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            add = runner.invoke(
                cli,
                ["claim", "add", "with hash", "--artifact-hash", HASH_A],
                catch_exceptions=False,
            )
            assert add.exit_code == 0
            claim_id = next(
                line.split("ID:")[-1].strip()
                for line in add.output.splitlines()
                if "ID:" in line
            )
            show = runner.invoke(
                cli, ["claim", "show", claim_id, "--json"],
                catch_exceptions=False,
            )
            data = json.loads(show.output)
        assert data["artifact_hash"] == HASH_A

    def test_cli_rejects_malformed_hash(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli, ["claim", "add", "bad", "--artifact-hash", "garbage"],
            )
        assert result.exit_code == 1
        assert "64-character" in result.output or "artifact_hash" in result.output


# ---------------------------------------------------------------------------
# Strict idempotency contract, every semantic field must match
# ---------------------------------------------------------------------------
#
# Tightened from the silent-merge anti-pattern. Prior
# behavior matched only on artifact_hash; this let two callers using the
# same key with different text + generated_by collapse into one row,
# destroying the second author's content and collapsing two lines into
# story (different generated_by converging on shared upstream). For
# cross-lab convergence, callers must assert two separate claims that
# share a supports[] entry, which is what convergence looks like.


class TestIdempotencyStrictContract:
    def test_same_key_different_text_raises(self, open_graph) -> None:
        open_graph.assert_claim("Lab A text", idempotency_key="k1",
                                generated_by="lab/a")
        with pytest.raises(_db.IdempotencyConflictError, match="text"):
            open_graph.assert_claim(
                "Lab B text", idempotency_key="k1", generated_by="lab/a",
            )

    def test_same_key_different_generated_by_raises(self, open_graph) -> None:
        open_graph.assert_claim("x", idempotency_key="k1",
                                generated_by="lab/a")
        with pytest.raises(_db.IdempotencyConflictError,
                           match="generated_by"):
            open_graph.assert_claim(
                "x", idempotency_key="k1", generated_by="lab/b",
            )

    def test_same_key_different_classification_raises(self, open_graph) -> None:
        open_graph.assert_claim("x", idempotency_key="k1",
                                classification="INFERRED")
        with pytest.raises(_db.IdempotencyConflictError,
                           match="classification"):
            open_graph.assert_claim(
                "x", idempotency_key="k1", classification="ANALYTICAL",
            )

    def test_same_key_different_supports_raises(self, open_graph) -> None:
        open_graph.assert_claim("x", idempotency_key="k1",
                                supports=["upstream_A"])
        with pytest.raises(_db.IdempotencyConflictError, match="supports"):
            open_graph.assert_claim(
                "x", idempotency_key="k1", supports=["upstream_B"],
            )

    def test_same_key_different_source_name_raises(self, open_graph) -> None:
        open_graph.assert_claim("x", idempotency_key="k1",
                                source_name="dataset_alpha")
        with pytest.raises(_db.IdempotencyConflictError, match="source_name"):
            open_graph.assert_claim(
                "x", idempotency_key="k1", source_name="dataset_beta",
            )

    def test_same_key_different_observed_grounding_raises(
        self, open_graph,
    ) -> None:
        """The grounding verdict signs into the envelope and the chain hash, so
        a replay carrying a different one is not a retry."""
        open_graph.assert_claim(
            "x", idempotency_key="k1",
            observed_grounding={
                "grounding": "UNGROUNDED", "reason": "no data read observed",
            },
        )
        with pytest.raises(_db.IdempotencyConflictError,
                           match="observed_grounding"):
            open_graph.assert_claim(
                "x", idempotency_key="k1",
                observed_grounding={
                    "grounding": "GROUNDED", "reason": "data read observed",
                },
            )

    def test_same_key_different_evidence_raises(self, open_graph) -> None:
        """The evidence vector signs in too, and it drives the GRADE
        downgrade, so a replay that changes it must not pass as a retry."""
        open_graph.assert_claim(
            "x", idempotency_key="k1", evidence={"risk_of_bias": -2},
        )
        with pytest.raises(_db.IdempotencyConflictError, match="evidence"):
            open_graph.assert_claim(
                "x", idempotency_key="k1", evidence={"risk_of_bias": 0},
            )

    def test_true_retry_survives_reordered_verdict_keys(
        self, open_graph,
    ) -> None:
        """Both columns hold canonical JSON, so the comparison is on the parsed
        record: a retry that spells the same verdict in a different key order
        still gets the first claim_id back."""
        a = open_graph.assert_claim(
            "x", idempotency_key="k1",
            observed_grounding={
                "grounding": "UNGROUNDED", "reason": "no data read observed",
            },
            evidence={"risk_of_bias": -1, "imprecision": -1},
        )
        b = open_graph.assert_claim(
            "x", idempotency_key="k1",
            observed_grounding={
                "reason": "no data read observed", "grounding": "UNGROUNDED",
            },
            evidence={"imprecision": -1, "risk_of_bias": -1},
        )
        assert a == b

    def test_true_retry_passes_silently(self, open_graph) -> None:
        """Every field identical → true retry. Returns the same claim_id,
        no INSERT."""
        a = open_graph.assert_claim(
            "x", classification="ANALYTICAL", generated_by="lab/a",
            supports=["upstream_A"], source_name="dataset_alpha",
            idempotency_key="k1",
        )
        b = open_graph.assert_claim(
            "x", classification="ANALYTICAL", generated_by="lab/a",
            supports=["upstream_A"], source_name="dataset_alpha",
            idempotency_key="k1",
        )
        assert a == b

    def test_multiple_mismatches_named_in_error(self, open_graph) -> None:
        """Conflict message names every mismatching field, not just the
        first one."""
        open_graph.assert_claim("Lab A text", idempotency_key="k1",
                                generated_by="lab/a")
        with pytest.raises(_db.IdempotencyConflictError) as exc:
            open_graph.assert_claim(
                "Lab B text", idempotency_key="k1", generated_by="lab/b",
            )
        msg = str(exc.value)
        assert "text" in msg and "generated_by" in msg

    def test_race_loss_translates_unique_violation(
        self, tmp_path, monkeypatch,
    ) -> None:
        """Two writers race past the pre-INSERT SELECT with the same
        idempotency_key. The loser must surface IdempotencyConflictError
        with the field-mismatch list, not a raw sqlite3.IntegrityError
        from idx_claims_idempotency_key.

        Simulated deterministically by wrapping the connection so the
        first idempotency SELECT returns None (as if the other writer
        hadn't committed yet), letting the INSERT trip the UNIQUE index
        and routing through the race-recovery branch.
        """
        # Land a real row that occupies idempotency_key="k1".
        with mareforma.open(tmp_path) as g:
            g.assert_claim(
                "Lab A text", idempotency_key="k1",
                classification="ANALYTICAL", generated_by="lab/a",
            )

        # Re-open and force the FIRST idempotency SELECT to miss, then
        # let the INSERT proceed to trip UNIQUE.
        with mareforma.open(tmp_path) as g:
            real_conn = g._conn
            wrapped = _MissingFirstSelect(real_conn)
            monkeypatch.setattr(g, "_conn", wrapped)
            with pytest.raises(_db.IdempotencyConflictError, match="text"):
                g.assert_claim(
                    "Lab B text", idempotency_key="k1",
                    classification="ANALYTICAL", generated_by="lab/a",
                )

    def test_race_loss_true_retry_returns_existing_id(
        self, tmp_path, monkeypatch,
    ) -> None:
        """Race-recovery happy path: if every field matches the row
        committed by the race winner, the loser gets the winner's
        claim_id back (idempotent retry), not an exception."""
        with mareforma.open(tmp_path) as g:
            winner_id = g.assert_claim(
                "shared text", idempotency_key="k1",
                classification="ANALYTICAL", generated_by="lab/a",
                source_name="dataset_alpha",
            )

        with mareforma.open(tmp_path) as g:
            monkeypatch.setattr(g, "_conn", _MissingFirstSelect(g._conn))
            loser_id = g.assert_claim(
                "shared text", idempotency_key="k1",
                classification="ANALYTICAL", generated_by="lab/a",
                source_name="dataset_alpha",
            )
            assert loser_id == winner_id
