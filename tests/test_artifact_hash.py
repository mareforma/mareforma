"""tests/test_artifact_hash.py — SHA256 artifact-hash gate for REPLICATED.

Covers:
  - normalize_artifact_hash format check (length, hex, case)
  - artifact_hash is part of the signed payload (tamper-evidence)
  - REPLICATED requires hash agreement when BOTH peers supply one
  - REPLICATED falls back to identity-only when either peer omits the hash
  - REPLICATED with three peers (mixed hash presence) handles each pair
    independently
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


# Convenience: a deterministic SHA256 for "artifact bytes".
HASH_A = hashlib.sha256(b"artifact-a").hexdigest()
HASH_B = hashlib.sha256(b"artifact-b").hexdigest()


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
        # Pin the exact-length contract — 63 and 65 must both fail so a future
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
        payload = _signing.envelope_payload(envelope)
        assert payload["artifact_hash"] == HASH_A

    def test_hash_none_serialises_as_null(self, tmp_path: Path) -> None:
        key_path = tmp_path / "key"
        _signing.bootstrap_key(key_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim("no hash")
        row = self._load(tmp_path, cid)
        envelope = json.loads(row["signature_bundle"])
        payload = _signing.envelope_payload(envelope)
        assert "artifact_hash" in payload
        assert payload["artifact_hash"] is None

    def test_tampered_hash_in_db_fails_signature_verify(self, tmp_path: Path) -> None:
        """Mutating the column without re-signing breaks the envelope check."""
        key_path = tmp_path / "key"
        _signing.bootstrap_key(key_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim("with hash", artifact_hash=HASH_A)
            signer = g._signer
            public_key = signer.public_key()
            # Tamper: rewrite the column to a different hash. The bundle
            # is unchanged so verification against the live row must fail.
            g._conn.execute(
                "UPDATE claims SET artifact_hash = ? WHERE claim_id = ?",
                (HASH_B, cid),
            )
            g._conn.commit()
            row = _db.get_claim(g._conn, cid)
        envelope = json.loads(row["signature_bundle"])
        # The envelope itself still verifies (signed bytes unchanged) — what
        # changed is the live row. Re-derive the canonical payload from the
        # tampered row and check it does NOT equal the envelope's payload.
        live = _signing.canonical_payload({
            "claim_id": cid,
            "text": row["text"],
            "classification": row["classification"],
            "generated_by": row["generated_by"],
            "supports": json.loads(row["supports_json"] or "[]"),
            "contradicts": json.loads(row["contradicts_json"] or "[]"),
            "source_name": row["source_name"],
            "artifact_hash": row["artifact_hash"],
            "created_at": row["created_at"],
        })
        envelope_payload_bytes = _signing.envelope_payload(envelope)
        # Encode the envelope's payload the same way for byte-equality compare.
        envelope_bytes = json.dumps(
            envelope_payload_bytes, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        assert live != envelope_bytes, (
            "tampered live row must not match the originally-signed payload"
        )

    @staticmethod
    def _load(root: Path, claim_id: str) -> dict:
        with mareforma.open(root) as g:
            return _db.get_claim(g._conn, claim_id)


# ---------------------------------------------------------------------------
# REPLICATED gating — opt-in hash agreement
# ---------------------------------------------------------------------------

class TestReplicatedHashGate:
    def test_matching_hashes_promote_to_replicated(self, open_graph) -> None:
        upstream = open_graph.assert_claim("upstream finding", generated_by="seed")
        a = open_graph.assert_claim(
            "agent A finding", supports=[upstream],
            generated_by="agent-A", artifact_hash=HASH_A,
        )
        b = open_graph.assert_claim(
            "agent B finding", supports=[upstream],
            generated_by="agent-B", artifact_hash=HASH_A,
        )
        assert open_graph.get_claim(a)["support_level"] == "REPLICATED"
        assert open_graph.get_claim(b)["support_level"] == "REPLICATED"

    def test_mismatched_hashes_block_replicated(self, open_graph) -> None:
        upstream = open_graph.assert_claim("upstream finding", generated_by="seed")
        a = open_graph.assert_claim(
            "agent A finding", supports=[upstream],
            generated_by="agent-A", artifact_hash=HASH_A,
        )
        b = open_graph.assert_claim(
            "agent B finding", supports=[upstream],
            generated_by="agent-B", artifact_hash=HASH_B,
        )
        assert open_graph.get_claim(a)["support_level"] == "PRELIMINARY"
        assert open_graph.get_claim(b)["support_level"] == "PRELIMINARY"

    def test_one_side_missing_hash_falls_back_to_identity_only(
        self, open_graph,
    ) -> None:
        """Back-compat: if EITHER peer lacks a hash, REPLICATED still fires
        on identity convergence alone. The hash signal is opt-in, not
        retroactive."""
        upstream = open_graph.assert_claim("upstream finding", generated_by="seed")
        a = open_graph.assert_claim(
            "agent A finding", supports=[upstream],
            generated_by="agent-A", artifact_hash=HASH_A,
        )
        b = open_graph.assert_claim(
            "agent B finding (no hash)", supports=[upstream],
            generated_by="agent-B",
        )
        assert open_graph.get_claim(a)["support_level"] == "REPLICATED"
        assert open_graph.get_claim(b)["support_level"] == "REPLICATED"

    def test_neither_has_hash_legacy_path_preserved(self, open_graph) -> None:
        upstream = open_graph.assert_claim("upstream finding", generated_by="seed")
        a = open_graph.assert_claim(
            "agent A finding", supports=[upstream], generated_by="agent-A",
        )
        b = open_graph.assert_claim(
            "agent B finding", supports=[upstream], generated_by="agent-B",
        )
        assert open_graph.get_claim(a)["support_level"] == "REPLICATED"
        assert open_graph.get_claim(b)["support_level"] == "REPLICATED"

    def test_third_matching_peer_promotes_after_blocked_pair(
        self, open_graph,
    ) -> None:
        """When A has HASH_A and B has HASH_B (blocked), a third claim C
        with HASH_A still converges with A — independent pair-by-pair."""
        upstream = open_graph.assert_claim("upstream finding", generated_by="seed")
        a = open_graph.assert_claim(
            "agent A", supports=[upstream],
            generated_by="agent-A", artifact_hash=HASH_A,
        )
        b = open_graph.assert_claim(
            "agent B", supports=[upstream],
            generated_by="agent-B", artifact_hash=HASH_B,
        )
        assert open_graph.get_claim(a)["support_level"] == "PRELIMINARY"
        assert open_graph.get_claim(b)["support_level"] == "PRELIMINARY"
        c = open_graph.assert_claim(
            "agent C", supports=[upstream],
            generated_by="agent-C", artifact_hash=HASH_A,
        )
        # A and C share a matching hash → both promote. B stays PRELIMINARY.
        assert open_graph.get_claim(a)["support_level"] == "REPLICATED"
        assert open_graph.get_claim(c)["support_level"] == "REPLICATED"
        assert open_graph.get_claim(b)["support_level"] == "PRELIMINARY"

    def test_same_agent_same_hash_does_not_promote(self, open_graph) -> None:
        """The hash gate must not bypass the same-agent independence check.
        Identity convergence requires distinct generated_by, full stop."""
        upstream = open_graph.assert_claim("upstream finding", generated_by="seed")
        a = open_graph.assert_claim(
            "first finding", supports=[upstream],
            generated_by="agent-A", artifact_hash=HASH_A,
        )
        b = open_graph.assert_claim(
            "second finding from same agent", supports=[upstream],
            generated_by="agent-A", artifact_hash=HASH_A,
        )
        assert open_graph.get_claim(a)["support_level"] == "PRELIMINARY"
        assert open_graph.get_claim(b)["support_level"] == "PRELIMINARY"

    def test_mark_claim_resolved_reapplies_hash_gate(self, open_graph) -> None:
        """When a DOI resolves late, the deferred REPLICATED re-check must
        consult the row's persisted artifact_hash — not bypass the gate."""
        # Peer A converges on upstream with HASH_A (no DOIs → resolved).
        upstream = open_graph.assert_claim("upstream", generated_by="seed")
        a = open_graph.assert_claim(
            "peer A", supports=[upstream],
            generated_by="agent-A", artifact_hash=HASH_A,
        )
        # Insert peer B with an unresolved flag forced on, hash=HASH_B.
        # We use the db layer directly so we can fix unresolved=True without
        # actually plumbing a fake DOI through the resolver.
        b = _db.add_claim(
            open_graph._conn, open_graph._root, "peer B",
            supports=[upstream], generated_by="agent-B",
            artifact_hash=HASH_B, unresolved=True,
        )
        # Confirm B is held back by unresolved AND would also be blocked by hash.
        assert open_graph.get_claim(b)["support_level"] == "PRELIMINARY"
        assert open_graph.get_claim(a)["support_level"] == "PRELIMINARY"
        # Clear unresolved flag — should re-fire REPLICATED check.
        _db.mark_claim_resolved(open_graph._conn, open_graph._root, b)
        # Hashes still mismatch → still PRELIMINARY.
        assert open_graph.get_claim(b)["support_level"] == "PRELIMINARY"
        assert open_graph.get_claim(a)["support_level"] == "PRELIMINARY"

    def test_mark_claim_resolved_promotes_when_hashes_agree(
        self, open_graph,
    ) -> None:
        """Mirror of the above: matching hash + late DOI resolution promotes."""
        upstream = open_graph.assert_claim("upstream", generated_by="seed")
        a = open_graph.assert_claim(
            "peer A", supports=[upstream],
            generated_by="agent-A", artifact_hash=HASH_A,
        )
        b = _db.add_claim(
            open_graph._conn, open_graph._root, "peer B",
            supports=[upstream], generated_by="agent-B",
            artifact_hash=HASH_A, unresolved=True,
        )
        assert open_graph.get_claim(b)["support_level"] == "PRELIMINARY"
        _db.mark_claim_resolved(open_graph._conn, open_graph._root, b)
        assert open_graph.get_claim(a)["support_level"] == "REPLICATED"
        assert open_graph.get_claim(b)["support_level"] == "REPLICATED"


# ---------------------------------------------------------------------------
# assert_claim parameter handling
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
