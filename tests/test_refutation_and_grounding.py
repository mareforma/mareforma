"""Tests for refutation taxonomy + filter + grounding sensor."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import mareforma
from mareforma import db as _db
from mareforma import signing as _signing
from mareforma._evidence import EvidenceVector, EvidenceVectorError
from mareforma.verifiers import (
    MockNLIVerifier, Verifier, VerifierError, _validate_score,
)


# ----------------------------------------------------------------------------
# Refutation presenter
# ----------------------------------------------------------------------------


class TestRefutationStatusPresenter:
    def test_clean_row(self) -> None:
        row = {"t_invalid": None, "status": "open"}
        rs = mareforma.refutation_status(row)
        assert rs["state"] == "clean"
        assert rs["signal"] == "none"

    def test_contradicted_row(self) -> None:
        row = {"t_invalid": "2026-05-22T00:00:00+00:00", "status": "open"}
        rs = mareforma.refutation_status(row)
        assert rs["state"] == "contradicted"
        assert rs["signal"] == "signed-verdict"

    def test_retracted_row(self) -> None:
        row = {"t_invalid": None, "status": "retracted"}
        rs = mareforma.refutation_status(row)
        assert rs["state"] == "retracted"
        assert rs["signal"] == "editorial"

    def test_contested_row(self) -> None:
        row = {"t_invalid": None, "status": "contested"}
        rs = mareforma.refutation_status(row)
        assert rs["state"] == "contested"
        assert rs["signal"] == "editorial"

    def test_signed_verdict_beats_status(self) -> None:
        # If a signed contradiction verdict has fired, it takes
        # precedence over any editorial status — strongest signal wins.
        row = {"t_invalid": "2026-05-22T00:00:00+00:00", "status": "contested"}
        rs = mareforma.refutation_status(row)
        assert rs["state"] == "contradicted"

    def test_partial_row_raises(self) -> None:
        # A hand-crafted dict missing 'status' would otherwise fall
        # through to a confidently-wrong "clean" — refuse instead.
        with pytest.raises(ValueError, match="missing 'status'"):
            mareforma.refutation_status({"t_invalid": None})
        with pytest.raises(ValueError, match="must be a dict"):
            mareforma.refutation_status("not-a-dict")  # type: ignore[arg-type]


class TestRefutationStatusOnGraph:
    def test_unknown_claim_raises(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            with pytest.raises(_db.ClaimNotFoundError):
                graph.refutation_status(
                    "00000000-0000-4000-8000-000000000000"
                )

    def test_returns_clean_for_fresh_claim(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            cid = graph.assert_claim("test")
            assert graph.refutation_status(cid)["state"] == "clean"


# ----------------------------------------------------------------------------
# Refutation filter on query()
# ----------------------------------------------------------------------------


class TestRefutationFilter:
    def test_unknown_filter_raises(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            with pytest.raises(ValueError, match="Unknown refutation_filter"):
                graph.query(refutation_filter="totally-made-up")

    def test_clean_filter_excludes_retracted(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("clean claim")
            # Mark second claim retracted via update_claim.
            b = graph.assert_claim("to-retract claim")
            graph.update_claim(b, status="retracted")
            ids_clean = [
                r["claim_id"] for r in graph.query(
                    refutation_filter="clean", include_unverified=True,
                )
            ]
            assert a in ids_clean
            assert b not in ids_clean

    def test_retracted_filter_returns_only_retracted(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("clean")
            b = graph.assert_claim("to-retract")
            graph.update_claim(b, status="retracted")
            results = graph.query(
                refutation_filter="retracted", include_unverified=True,
            )
            ids = [r["claim_id"] for r in results]
            assert ids == [b]
            assert a not in ids

    def test_contested_filter_returns_only_contested(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("clean")
            b = graph.assert_claim("to-contest")
            graph.update_claim(b, status="contested")
            results = graph.query(
                refutation_filter="contested", include_unverified=True,
            )
            ids = [r["claim_id"] for r in results]
            assert ids == [b]
            assert a not in ids

    def test_any_filter_includes_all_states(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("clean")
            b = graph.assert_claim("retracted-one")
            graph.update_claim(b, status="retracted")
            c = graph.assert_claim("contested-one")
            graph.update_claim(c, status="contested")
            results = graph.query(
                refutation_filter="any", include_unverified=True, limit=100,
            )
            ids = {r["claim_id"] for r in results}
            assert {a, b, c} <= ids


# ----------------------------------------------------------------------------
# Verifier protocol + MockNLIVerifier
# ----------------------------------------------------------------------------


class TestMockNLIVerifier:
    def test_default_score_is_one(self) -> None:
        v = MockNLIVerifier()
        score, rationale = v.grounding_score("claim", ["upstream"])
        assert score == 1.0
        assert rationale

    def test_custom_score_returned(self) -> None:
        v = MockNLIVerifier(score=0.75, rationale="custom")
        score, rationale = v.grounding_score("c", [])
        assert score == 0.75
        assert rationale == "custom"

    def test_out_of_range_score_rejected_at_construction(self) -> None:
        with pytest.raises(VerifierError, match="out of"):
            MockNLIVerifier(score=1.5)
        with pytest.raises(VerifierError, match="out of"):
            MockNLIVerifier(score=-0.1)

    def test_nan_score_rejected(self) -> None:
        with pytest.raises(VerifierError, match="NaN"):
            MockNLIVerifier(score=float("nan"))

    def test_protocol_runtime_check(self) -> None:
        assert isinstance(MockNLIVerifier(), Verifier)

    def test_validate_score_coerces_int(self) -> None:
        assert _validate_score(1) == 1.0
        assert _validate_score(0) == 0.0


# ----------------------------------------------------------------------------
# EvidenceVector.grounding_score field
# ----------------------------------------------------------------------------


class TestGroundingScoreField:
    def test_default_omitted_from_to_dict(self) -> None:
        v = EvidenceVector()
        d = v.to_dict()
        assert "grounding_score" not in d
        assert "grounding_rationale" not in d

    def test_set_score_serialised(self) -> None:
        v = EvidenceVector(
            grounding_score=0.85,
            grounding_rationale="entailment",
        )
        d = v.to_dict()
        assert d["grounding_score"] == 0.85
        assert d["grounding_rationale"] == "entailment"

    def test_score_without_rationale_raises(self) -> None:
        with pytest.raises(EvidenceVectorError, match="rationale is required"):
            EvidenceVector(grounding_score=0.5)

    def test_rationale_without_score_raises(self) -> None:
        with pytest.raises(EvidenceVectorError, match="without grounding_score"):
            EvidenceVector(grounding_rationale="orphan")

    def test_out_of_range_score_raises(self) -> None:
        with pytest.raises(EvidenceVectorError, match="out of"):
            EvidenceVector(grounding_score=1.5, grounding_rationale="x")
        with pytest.raises(EvidenceVectorError, match="out of"):
            EvidenceVector(grounding_score=-0.1, grounding_rationale="x")

    def test_nan_score_raises(self) -> None:
        with pytest.raises(EvidenceVectorError, match="NaN"):
            EvidenceVector(
                grounding_score=float("nan"),
                grounding_rationale="x",
            )

    def test_bool_score_rejected(self) -> None:
        with pytest.raises(EvidenceVectorError, match="not a bool"):
            EvidenceVector(
                grounding_score=True,  # type: ignore[arg-type]
                grounding_rationale="x",
            )

    def test_round_trip_through_dict(self) -> None:
        v = EvidenceVector(
            grounding_score=0.42, grounding_rationale="round-trip",
        )
        restored = EvidenceVector.from_dict(v.to_dict())
        assert restored == v


# ----------------------------------------------------------------------------
# assert_claim(grounding_sensor=) snapshot at assertion time
# ----------------------------------------------------------------------------


class TestGroundingSensorPlumbing:
    def test_sensor_score_lands_in_signed_payload(
        self, tmp_path: Path,
    ) -> None:
        key_path = tmp_path / "asserter.key"
        _signing.save_private_key(_signing.generate_keypair(), key_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            cid = graph.assert_claim(
                "claim with grounding",
                grounding_sensor=MockNLIVerifier(
                    score=0.9, rationale="strong entailment",
                ),
            )
            row = graph.get_claim(cid)
        # The score lives inside the signed Statement's predicate.
        bundle = json.loads(row["signature_bundle"])
        import base64
        payload = json.loads(
            base64.standard_b64decode(bundle["payload"]).decode("utf-8")
        )
        predicate = payload["predicate"]
        assert predicate["evidence"]["grounding_score"] == 0.9
        assert predicate["evidence"]["grounding_rationale"] == "strong entailment"

    def test_sensor_failure_does_not_block_assert(
        self, tmp_path: Path,
    ) -> None:
        class _Broken:
            def grounding_score(self, claim, supports):
                raise VerifierError("model fell over")

        with mareforma.open(tmp_path) as graph:
            with pytest.warns(RuntimeWarning, match="grounding_sensor raised"):
                cid = graph.assert_claim(
                    "fallback claim", grounding_sensor=_Broken(),
                )
            row = graph.get_claim(cid)
        # Claim was still asserted, but with no grounding_score.
        evidence = json.loads(row["evidence_json"])
        assert "grounding_score" not in evidence

    def test_sensor_returning_bad_shape_does_not_block(
        self, tmp_path: Path,
    ) -> None:
        class _BadShape:
            def grounding_score(self, claim, supports):
                return "not a tuple"  # type: ignore[return-value]

        with mareforma.open(tmp_path) as graph:
            with pytest.warns(RuntimeWarning):
                cid = graph.assert_claim(
                    "claim", grounding_sensor=_BadShape(),
                )
            row = graph.get_claim(cid)
        evidence = json.loads(row["evidence_json"])
        assert "grounding_score" not in evidence

    def test_sensor_out_of_range_does_not_block(
        self, tmp_path: Path,
    ) -> None:
        class _OutOfRange:
            def grounding_score(self, claim, supports):
                return (1.5, "implausible")

        with mareforma.open(tmp_path) as graph:
            with pytest.warns(RuntimeWarning):
                cid = graph.assert_claim(
                    "claim", grounding_sensor=_OutOfRange(),
                )
            row = graph.get_claim(cid)
        evidence = json.loads(row["evidence_json"])
        assert "grounding_score" not in evidence

    def test_tampered_grounding_score_refused_by_restore(
        self, tmp_path: Path,
    ) -> None:
        # Lock-in regression: the score is BOUND into the signed
        # predicate. Tampering evidence_json in claims.toml after the
        # fact must trip restore's envelope-vs-row verification.
        from mareforma.db import RestoreError
        key_path = tmp_path / "asserter.key"
        _signing.save_private_key(_signing.generate_keypair(), key_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            graph.assert_claim(
                "tamper-target",
                grounding_sensor=MockNLIVerifier(score=0.3, rationale="r"),
            )
        # Rewrite evidence_json with a forged grounding_score.
        toml_path = tmp_path / "claims.toml"
        text = toml_path.read_text()
        # The evidence_json value contains "grounding_score":0.3 — flip
        # to 0.99 directly in the TOML.
        tampered = text.replace(
            '\\"grounding_score\\":0.3',
            '\\"grounding_score\\":0.99',
        )
        assert tampered != text  # confirm the replacement landed
        toml_path.write_text(tampered)
        (tmp_path / ".mareforma" / "graph.db").unlink()
        (tmp_path / ".mareforma" / "claim_supports_cache.db").unlink(
            missing_ok=True,
        )
        with pytest.raises(RestoreError) as ei:
            mareforma.restore(tmp_path)
        assert ei.value.kind == "claim_unverified"

    def test_score_is_immutable_after_assertion(
        self, tmp_path: Path,
    ) -> None:
        # The lock states future verifiers can re-run independently
        # but their verdicts are NOT stored on the claim — confirm by
        # writing one score, asserting, then verifying restore round-
        # trips the signed score unchanged.
        key_path = tmp_path / "asserter.key"
        _signing.save_private_key(_signing.generate_keypair(), key_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            cid = graph.assert_claim(
                "immutable test",
                grounding_sensor=MockNLIVerifier(score=0.3, rationale="r1"),
            )
        # Drop graph.db; restore from claims.toml; the round-trip
        # signature verification covers the grounding_score binding.
        (tmp_path / ".mareforma" / "graph.db").unlink()
        (tmp_path / ".mareforma" / "claim_supports_cache.db").unlink(
            missing_ok=True,
        )
        result = mareforma.restore(tmp_path)
        assert result["claims_restored"] == 1
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            row = graph.get_claim(cid)
        evidence = json.loads(row["evidence_json"])
        assert evidence["grounding_score"] == 0.3
        assert evidence["grounding_rationale"] == "r1"


# ----------------------------------------------------------------------------
# Legacy round-trip preserved
# ----------------------------------------------------------------------------


class TestVerifierHardening:
    """Regressions for the verifier sandboxing guarantees."""

    def test_verifier_cannot_mutate_supports_list(
        self, tmp_path: Path,
    ) -> None:
        # A hostile or buggy verifier must NOT be able to rewrite the
        # asserter's supports[] citations between assert_claim and
        # the signed envelope. The substrate hands the verifier a
        # tuple, not the live list.
        captured: list[object] = []

        class _MutatingVerifier:
            def grounding_score(self, claim, supports):
                captured.append(supports)
                try:
                    supports.append("10.0/forged")  # type: ignore[attr-defined]
                except (AttributeError, TypeError):
                    pass
                return (0.5, "tested mutation")

        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("upstream")
            cid = graph.assert_claim(
                "downstream",
                supports=[a],
                grounding_sensor=_MutatingVerifier(),
            )
            row = graph.get_claim(cid)
        # The verifier received a tuple (immutable), and the persisted
        # supports_json carries ONLY the asserter's original ref.
        assert isinstance(captured[0], tuple)
        persisted = json.loads(row["supports_json"])
        assert persisted == [a]
        assert "10.0/forged" not in persisted

    def test_verifier_oserror_does_not_block_assert(
        self, tmp_path: Path,
    ) -> None:
        # Real verifiers raise OSError / ConnectionError / RuntimeError
        # routinely (model load failure, network blip, OOM). The
        # substrate's contract is "claim still lands, score dropped."
        class _OSErrorVerifier:
            def grounding_score(self, claim, supports):
                raise OSError("model file missing")

        with mareforma.open(tmp_path) as graph:
            with pytest.warns(RuntimeWarning, match="OSError"):
                cid = graph.assert_claim(
                    "fallback", grounding_sensor=_OSErrorVerifier(),
                )
            evidence = json.loads(graph.get_claim(cid)["evidence_json"])
        assert "grounding_score" not in evidence

    def test_verifier_keyerror_does_not_block_assert(
        self, tmp_path: Path,
    ) -> None:
        class _KeyErrorVerifier:
            def grounding_score(self, claim, supports):
                raise KeyError("missing-model-key")

        with mareforma.open(tmp_path) as graph:
            with pytest.warns(RuntimeWarning, match="KeyError"):
                cid = graph.assert_claim(
                    "fallback", grounding_sensor=_KeyErrorVerifier(),
                )
            evidence = json.loads(graph.get_claim(cid)["evidence_json"])
        assert "grounding_score" not in evidence

    def test_verifier_non_string_rationale_does_not_block(
        self, tmp_path: Path,
    ) -> None:
        # Coercing rationale via str() would silently sign garbage
        # ("None", "b'abc'", "{...}"). The substrate now refuses
        # non-str rationale at the verifier-call site and falls
        # through to the warning path.
        class _BadRationale:
            def grounding_score(self, claim, supports):
                return (0.7, None)  # type: ignore[return-value]

        with mareforma.open(tmp_path) as graph:
            with pytest.warns(RuntimeWarning):
                cid = graph.assert_claim(
                    "fallback", grounding_sensor=_BadRationale(),
                )
            evidence = json.loads(graph.get_claim(cid)["evidence_json"])
        assert "grounding_score" not in evidence


class TestLegacyEvidenceVectorRoundTrip:
    """Adding grounding_score / grounding_rationale must NOT change the
    canonical bytes of any legacy EvidenceVector (no field, no
    rationale). Signed claims from before this change must still
    verify under the new code path."""

    def test_legacy_to_dict_unchanged(self) -> None:
        v = EvidenceVector(risk_of_bias=-1, rationale={"risk_of_bias": "x"})
        d = v.to_dict()
        assert "grounding_score" not in d
        assert "grounding_rationale" not in d
        # No study_design either (also new).
        assert "study_design" not in d
