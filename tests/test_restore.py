"""Restore-from-claims.toml tests (spec.md #99).

``mareforma.restore(project_root)`` rebuilds a fresh graph.db from the
TOML state file written by every mutation. The rebuild is fresh-only
(refuses non-empty graph.db) and fail-all-or-nothing on signature
verification. The adversarial test class is the load-bearing one — it
documents what tampering the restore path must catch.
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
# Helpers
# ---------------------------------------------------------------------------

def _bootstrap_key(tmp_path: Path, name: str) -> Path:
    key_path = tmp_path / name
    _signing.bootstrap_key(key_path)
    return key_path


def _pem_of(key_path: Path) -> bytes:
    return _signing.public_key_to_pem(
        _signing.load_private_key(key_path).public_key(),
    )


def _build_full_graph(tmp_path: Path) -> dict:
    """Populate a project with the full v0.3.0 substrate: root validator,
    second validator, seed claim, REPLICATED pair, ESTABLISHED claim,
    one unsigned PRELIMINARY (in a separate unsigned-mode project).

    Returns identifiers used by tests for verification.
    """
    root_key = _bootstrap_key(tmp_path, "root.key")
    val_key = _bootstrap_key(tmp_path, "val.key")

    with mareforma.open(tmp_path, key_path=root_key) as g:
        seed_id = g.assert_claim("anchor", generated_by="seed", seed=True)
        rep_id = g.assert_claim("converged", supports=[seed_id], generated_by="A")
        g.assert_claim("converged", supports=[seed_id], generated_by="B")
        g.enroll_validator(_pem_of(val_key), identity="v")

    with mareforma.open(tmp_path, key_path=val_key) as g:
        g.validate(rep_id)
        assert g.get_claim(rep_id)["support_level"] == "ESTABLISHED"

    return {
        "root_key": root_key,
        "val_key": val_key,
        "seed_id": seed_id,
        "rep_id": rep_id,
    }


def _wipe_graph_db(tmp_path: Path) -> None:
    db_dir = tmp_path / ".mareforma"
    for f in db_dir.iterdir():
        f.unlink()
    db_dir.rmdir()


# ---------------------------------------------------------------------------
# Happy path: full round-trip
# ---------------------------------------------------------------------------

class TestRestoreHappyPath:
    def test_round_trip_preserves_claims_and_validators(
        self, tmp_path: Path,
    ) -> None:
        ctx = _build_full_graph(tmp_path)

        # Capture pre-state via the live graph.
        with mareforma.open(tmp_path, key_path=ctx["root_key"]) as g:
            pre_claims = sorted(
                g.query(include_unverified=True, limit=99),
                key=lambda c: c["created_at"],
            )
            from mareforma import validators as _validators
            pre_validators = _validators.list_validators(g._conn)
            pre_count = len(pre_claims)

        # Wipe graph.db; claims.toml survives.
        _wipe_graph_db(tmp_path)
        assert not (tmp_path / ".mareforma" / "graph.db").exists()

        result = mareforma.restore(tmp_path)
        assert result == {
            "validators_restored": len(pre_validators),
            "claims_restored": pre_count,
        }

        # Re-open the restored graph and confirm shape.
        with mareforma.open(tmp_path, key_path=ctx["root_key"]) as g:
            post_claims = sorted(
                g.query(include_unverified=True, limit=99),
                key=lambda c: c["created_at"],
            )
            post_validators = _validators.list_validators(g._conn)

        assert len(post_claims) == pre_count
        for pre, post in zip(pre_claims, post_claims):
            assert pre["claim_id"] == post["claim_id"]
            assert pre["text"] == post["text"]
            assert pre["support_level"] == post["support_level"]
            assert pre["signature_bundle"] == post["signature_bundle"]
            assert pre["validation_signature"] == post["validation_signature"]
            assert pre["validator_keyid"] == post["validator_keyid"]
            # prev_hash recomputed; must match because inputs and order
            # are identical and SHA256 is deterministic.
            assert pre["prev_hash"] == post["prev_hash"]
        assert {v["keyid"] for v in post_validators} == {
            v["keyid"] for v in pre_validators
        }

    def test_restore_rebuilds_fts_index(self, tmp_path: Path) -> None:
        """The INSERT triggers fire during restore, populating
        claims_fts. Search must work on the restored graph."""
        ctx = _build_full_graph(tmp_path)
        _wipe_graph_db(tmp_path)
        mareforma.restore(tmp_path)
        with mareforma.open(tmp_path, key_path=ctx["root_key"]) as g:
            results = g.search("converged")
        # Two REPLICATED claims share the text "converged".
        assert len(results) >= 1
        # And one carries the validator_reputation projection.
        ranked = [r for r in results if r["support_level"] == "ESTABLISHED"]
        if ranked:
            assert ranked[0]["validator_reputation"] >= 1

    def test_restore_returns_zeros_on_unsigned_empty_project(
        self, tmp_path: Path,
    ) -> None:
        """An unsigned project with claims still round-trips: no
        validators, claim signatures are NULL — that's mode-consistent."""
        with mareforma.open(tmp_path) as g:
            g.assert_claim("alpha")
            g.assert_claim("beta")
        _wipe_graph_db(tmp_path)
        result = mareforma.restore(tmp_path)
        assert result == {"validators_restored": 0, "claims_restored": 2}


# ---------------------------------------------------------------------------
# Refuse non-empty graph
# ---------------------------------------------------------------------------

class TestRestoreRefusesNonEmptyGraph:
    def test_refuses_when_graph_has_claims(self, tmp_path: Path) -> None:
        _build_full_graph(tmp_path)
        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "graph_not_empty"

    def test_accepts_when_graph_db_empty(self, tmp_path: Path) -> None:
        """Empty .mareforma/graph.db (claims table exists but has 0
        rows) is accepted — restore() proceeds normally."""
        ctx = _build_full_graph(tmp_path)
        # Wipe ROWS but keep the file. Re-open the live graph and delete
        # rows would trip the retracted-terminal trigger; easier: drop
        # the file entirely.
        _wipe_graph_db(tmp_path)
        # Calling open_db creates a fresh empty graph.db.
        conn = _db.open_db(tmp_path)
        conn.close()
        result = mareforma.restore(tmp_path)
        assert result["validators_restored"] >= 1


# ---------------------------------------------------------------------------
# Missing claims.toml
# ---------------------------------------------------------------------------

class TestRestoreMissingTOML:
    def test_missing_toml_raises(self, tmp_path: Path) -> None:
        # No graph, no TOML.
        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "toml_not_found"

    def test_explicit_toml_path_honored(self, tmp_path: Path) -> None:
        ctx = _build_full_graph(tmp_path)
        # Move claims.toml to a non-default location.
        moved = tmp_path / "backup" / "state.toml"
        moved.parent.mkdir()
        (tmp_path / "claims.toml").rename(moved)
        _wipe_graph_db(tmp_path)
        result = mareforma.restore(tmp_path, claims_toml=moved)
        assert result["claims_restored"] >= 1


# ---------------------------------------------------------------------------
# Adversarial: tampering is caught
# ---------------------------------------------------------------------------

class TestRestoreAdversarial:
    def _setup_and_wipe(self, tmp_path: Path) -> dict:
        ctx = _build_full_graph(tmp_path)
        _wipe_graph_db(tmp_path)
        return ctx

    def _read_toml(self, tmp_path: Path) -> dict:
        try:
            import tomllib  # type: ignore[import-not-found]
        except ImportError:
            import tomli as tomllib  # type: ignore[no-redef]
        return tomllib.loads(
            (tmp_path / "claims.toml").read_text(encoding="utf-8"),
        )

    def _write_toml(self, tmp_path: Path, data: dict) -> None:
        import tomli_w
        (tmp_path / "claims.toml").write_bytes(
            tomli_w.dumps(data).encode("utf-8"),
        )

    def test_tampered_claim_text_fails_verify(self, tmp_path: Path) -> None:
        """Edit a signed claim's text in claims.toml without re-signing.
        The signature_bundle remains the original bytes; restore must
        detect the field divergence and refuse."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        # Pick a signed claim (the seed has a bundle).
        signed_ids = [
            cid for cid, c in data["claims"].items()
            if c.get("signature_bundle")
        ]
        assert signed_ids
        victim = signed_ids[0]
        data["claims"][victim]["text"] = "TAMPERED — drug X causes effect Y"
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "claim_unverified"

    def test_tampered_signature_bytes_fail_verify(
        self, tmp_path: Path,
    ) -> None:
        """Mutate the base64 signature bytes; verify must fail."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        signed_ids = [
            cid for cid, c in data["claims"].items()
            if c.get("signature_bundle")
        ]
        victim = signed_ids[0]
        bundle = json.loads(data["claims"][victim]["signature_bundle"])
        # Flip a byte in the base64 signature.
        sig_bytes = bytearray(
            base64.standard_b64decode(bundle["signatures"][0]["sig"])
        )
        sig_bytes[0] ^= 0xFF
        bundle["signatures"][0]["sig"] = base64.standard_b64encode(
            bytes(sig_bytes)
        ).decode("ascii")
        data["claims"][victim]["signature_bundle"] = json.dumps(
            bundle, sort_keys=True, separators=(",", ":"),
        )
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "claim_unverified"

    def test_missing_signature_in_signed_mode_refused(
        self, tmp_path: Path,
    ) -> None:
        """Strip a signature_bundle from a signed-mode TOML — restore
        must refuse the mode-inconsistent graph."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        # Strip the bundle from one signed claim.
        for cid, c in data["claims"].items():
            if c.get("signature_bundle"):
                del c["signature_bundle"]
                break
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "mode_inconsistent"

    def test_orphan_signer_refused(self, tmp_path: Path) -> None:
        """A signature_bundle's keyid doesn't appear in the validators
        section — restore refuses the orphan signer."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        # Strip the validators section while leaving signed claims.
        # The signed claims now have signers not in the (empty)
        # validators set.
        del data["validators"]
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        # Either mode_inconsistent (the bundle exists but no validators)
        # or orphan_signer — both indicate the tampering.
        # The actual code path: signed_mode is False (no validators),
        # so claims with signature_bundle hit the orphan_signer check.
        assert exc_info.value.kind in ("orphan_signer", "mode_inconsistent")

    def test_tampered_validator_envelope_refused(
        self, tmp_path: Path,
    ) -> None:
        """Tamper with a validator's identity field in claims.toml —
        the enrollment envelope's signed payload no longer matches."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        non_root_keyids = [
            keyid for keyid, v in data["validators"].items()
            if v["enrolled_by_keyid"] != keyid
        ]
        assert non_root_keyids
        victim = non_root_keyids[0]
        data["validators"][victim]["identity"] = "TAMPERED-IDENTITY"
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "enrollment_unverified"

    def test_tampered_payload_type_raises_restore_error(
        self, tmp_path: Path,
    ) -> None:
        """A claim's signature_bundle with a swapped payloadType (e.g.
        validation envelope shoved into the claim-bundle slot) used to
        leak InvalidEnvelopeError past the restore contract. The
        verify_envelope call must be wrapped so RestoreError fires."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        signed_ids = [
            cid for cid, c in data["claims"].items()
            if c.get("signature_bundle")
        ]
        victim = signed_ids[0]
        bundle = json.loads(data["claims"][victim]["signature_bundle"])
        # Swap payloadType to the validation envelope type.
        bundle["payloadType"] = _signing.PAYLOAD_TYPE_VALIDATION
        data["claims"][victim]["signature_bundle"] = json.dumps(
            bundle, sort_keys=True, separators=(",", ":"),
        )
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "claim_unverified"

    def test_tampered_status_on_seed_blocked_at_replicated_gate(
        self, tmp_path: Path,
    ) -> None:
        """A born-retracted ESTABLISHED seed (planted via a hand-edited
        claims.toml) is restorable — the seed envelope binds claim_id +
        validator_keyid + seeded_at but NOT status. The substrate gate
        at _maybe_update_replicated_unlocked must refuse the retracted
        seed as an upstream anchor, blocking downstream REPLICATED."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        # Find the seed (ESTABLISHED + has validation_signature with
        # PAYLOAD_TYPE_SEED). Set its status to 'retracted' in TOML.
        for cid, c in data["claims"].items():
            if c.get("support_level") == "ESTABLISHED" and c.get(
                "validation_signature"
            ):
                env = json.loads(c["validation_signature"])
                if env.get("payloadType") == _signing.PAYLOAD_TYPE_SEED:
                    c["status"] = "retracted"
                    seed_id = cid
                    break
        self._write_toml(tmp_path, data)

        # Restore admits the row (it carries a valid envelope and the
        # status column has no envelope binding to fail against).
        result = mareforma.restore(tmp_path)
        assert result["claims_restored"] >= 1

        # Now try to plant a REPLICATED-via-retracted-seed convergence.
        # Two new agent claims cite the retracted seed; the convergence
        # check must refuse to promote them.
        with mareforma.open(tmp_path, key_path=ctx["root_key"]) as g:
            a = g.assert_claim(
                "downstream A", supports=[seed_id], generated_by="A",
            )
            g.assert_claim(
                "downstream B", supports=[seed_id], generated_by="B",
            )
            # Without the gate, both would be REPLICATED.
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"

    def test_missing_required_field_raises_restore_error(
        self, tmp_path: Path,
    ) -> None:
        """A hand-edited claims.toml that drops a required key (e.g.
        c['text']) used to leak KeyError past the documented RestoreError
        contract. The _required_field helper must translate it."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        victim = next(iter(data["claims"]))
        del data["claims"][victim]["text"]
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "toml_malformed"

    def test_missing_validator_required_field_raises_restore_error(
        self, tmp_path: Path,
    ) -> None:
        """Same for the validators section."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        victim = next(iter(data["validators"]))
        del data["validators"][victim]["identity"]
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "toml_malformed"

    def test_validation_envelope_swap_rejected(self, tmp_path: Path) -> None:
        """Copy a legitimate validation envelope from one ESTABLISHED
        claim onto a different (REPLICATED) row, set support_level to
        ESTABLISHED. The envelope verifies cryptographically (the bytes
        are unchanged), but its embedded claim_id no longer matches the
        new row. Restore must catch the row-vs-envelope divergence."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)

        # Find a legitimate ESTABLISHED claim with a validation envelope.
        donor_id, donor = next(
            (cid, c) for cid, c in data["claims"].items()
            if c.get("support_level") == "ESTABLISHED"
            and c.get("validation_signature")
        )
        legitimate_env_json = donor["validation_signature"]
        legitimate_validated_at = donor.get("validated_at")

        # Pick a different non-ESTABLISHED row as the victim. The fixture
        # has REPLICATED claims that lack validation_signature.
        victim_id, victim = next(
            (cid, c) for cid, c in data["claims"].items()
            if c.get("support_level") != "ESTABLISHED"
            and cid != donor_id
        )
        # Forge: copy envelope onto victim, flip to ESTABLISHED. Match
        # validated_at to the donor's so the timestamp check would
        # otherwise pass — the claim_id mismatch must be what trips us.
        victim["support_level"] = "ESTABLISHED"
        victim["validation_signature"] = legitimate_env_json
        victim["validated_at"] = legitimate_validated_at
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "claim_unverified"
        assert "different claim_id" in str(exc_info.value)

    def test_adversarial_text_round_trips(self, tmp_path: Path) -> None:
        """Newlines, quotes, control-like chars in source_name and text
        must round-trip through TOML and reload identically."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        adversarial_text = (
            'multi\nline\twith "quotes" and a backslash\\here'
        )
        adversarial_source = 'src "with quotes"'
        with mareforma.open(tmp_path, key_path=root_key) as g:
            cid = g.assert_claim(
                adversarial_text, source_name=adversarial_source,
            )

        # Round-trip.
        _wipe_graph_db(tmp_path)
        mareforma.restore(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            restored = g.get_claim(cid)
        assert restored["text"] == adversarial_text
        assert restored["source_name"] == adversarial_source

    def test_tampered_evidence_vector_rejected(self, tmp_path: Path) -> None:
        """Flip a domain in evidence_json without re-signing. The
        envelope binds the original evidence; restore must catch the
        signed-evidence vs row-evidence divergence."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        # Pick a signed claim whose evidence is the all-zero default
        # and flip one domain to -1. Signature_bundle stays unchanged.
        signed_ids = [
            cid for cid, c in data["claims"].items()
            if c.get("signature_bundle")
        ]
        assert signed_ids
        victim = signed_ids[0]
        # Build a tampered evidence dict — flip risk_of_bias to -1
        # with a fabricated rationale.
        tampered_evidence = {
            "risk_of_bias": -1,
            "inconsistency": 0,
            "indirectness": 0,
            "imprecision": 0,
            "publication_bias": 0,
            "large_effect": False,
            "dose_response": False,
            "opposing_confounding": False,
            "rationale": {"risk_of_bias": "tampered"},
            "reporting_compliance": [],
        }
        data["claims"][victim]["evidence_json"] = json.dumps(
            tampered_evidence, sort_keys=True, separators=(",", ":"),
        )
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "claim_unverified"
        # Either the evidence-vector binding catches it directly, or
        # the statement_cid cross-check does. Both messages are valid.
        msg = str(exc_info.value).lower()
        assert "evidence" in msg or "statement_cid" in msg

    def test_swapped_statement_cid_rejected(self, tmp_path: Path) -> None:
        """Forge statement_cid on a row. Restore re-derives the cid
        from the row's fields + evidence; the forged value must not
        match, raising RestoreError."""
        ctx = self._setup_and_wipe(tmp_path)
        data = self._read_toml(tmp_path)
        signed_ids = [
            cid for cid, c in data["claims"].items()
            if c.get("signature_bundle")
        ]
        assert signed_ids
        victim = signed_ids[0]
        # Overwrite statement_cid with all-zeros. The row's other
        # fields are unchanged, so SIGNED_FIELDS still match; only
        # the cid re-derivation catches it.
        data["claims"][victim]["statement_cid"] = "0" * 64
        self._write_toml(tmp_path, data)

        with pytest.raises(_db.RestoreError) as exc_info:
            mareforma.restore(tmp_path)
        assert exc_info.value.kind == "claim_unverified"
        assert "statement_cid" in str(exc_info.value)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

class TestRestoreCLI:
    def test_cli_restore_happy_path(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from click.testing import CliRunner
        from mareforma.cli import cli as mareforma_cli

        monkeypatch.chdir(tmp_path)
        ctx = _build_full_graph(tmp_path)
        _wipe_graph_db(tmp_path)

        runner = CliRunner()
        result = runner.invoke(mareforma_cli, ["restore"])
        assert result.exit_code == 0, result.output
        assert "validators_restored" in result.output
        assert "claims_restored" in result.output

    def test_cli_restore_refuses_non_empty(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from click.testing import CliRunner
        from mareforma.cli import cli as mareforma_cli

        monkeypatch.chdir(tmp_path)
        _build_full_graph(tmp_path)

        runner = CliRunner()
        result = runner.invoke(mareforma_cli, ["restore"])
        assert result.exit_code == 1
        assert "refuses to merge" in result.output
