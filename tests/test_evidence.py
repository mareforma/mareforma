"""GRADE EvidenceVector tests."""

from __future__ import annotations

import pytest

from mareforma._evidence import EvidenceVector, EvidenceVectorError


class TestDefaults:
    def test_all_zero_default(self) -> None:
        ev = EvidenceVector()
        assert ev.risk_of_bias == 0
        assert ev.inconsistency == 0
        assert ev.indirectness == 0
        assert ev.imprecision == 0
        assert ev.publication_bias == 0
        assert ev.large_effect is False
        assert ev.dose_response is False
        assert ev.opposing_confounding is False
        assert ev.rationale == {}
        assert ev.reporting_compliance == ()

    def test_frozen(self) -> None:
        ev = EvidenceVector()
        with pytest.raises(Exception):
            ev.risk_of_bias = -1  # type: ignore[misc]


class TestDomainBounds:
    @pytest.mark.parametrize("domain", [
        "risk_of_bias", "inconsistency", "indirectness",
        "imprecision", "publication_bias",
    ])
    def test_negative_three_rejected(self, domain: str) -> None:
        with pytest.raises(EvidenceVectorError) as exc:
            EvidenceVector(**{domain: -3, "rationale": {domain: "x"}})
        assert "out of range" in str(exc.value)

    @pytest.mark.parametrize("domain", [
        "risk_of_bias", "inconsistency", "indirectness",
        "imprecision", "publication_bias",
    ])
    def test_positive_one_rejected(self, domain: str) -> None:
        with pytest.raises(EvidenceVectorError) as exc:
            EvidenceVector(**{domain: 1})
        assert "out of range" in str(exc.value)

    def test_minus_two_accepted_with_rationale(self) -> None:
        ev = EvidenceVector(
            risk_of_bias=-2,
            rationale={"risk_of_bias": "blinding broken"},
        )
        assert ev.risk_of_bias == -2

    def test_bool_not_accepted_for_domain(self) -> None:
        # True is technically int in Python but should be rejected here —
        # GRADE downgrade levels are not bool.
        with pytest.raises(EvidenceVectorError):
            EvidenceVector(risk_of_bias=True)  # type: ignore[arg-type]


class TestRationaleRequired:
    def test_nonzero_without_rationale_rejected(self) -> None:
        with pytest.raises(EvidenceVectorError) as exc:
            EvidenceVector(risk_of_bias=-1)
        assert "rationale" in str(exc.value)
        assert "risk_of_bias" in str(exc.value)

    def test_nonzero_with_empty_rationale_string_rejected(self) -> None:
        with pytest.raises(EvidenceVectorError) as exc:
            EvidenceVector(
                inconsistency=-1, rationale={"inconsistency": "   "},
            )
        assert "rationale" in str(exc.value)

    def test_nonzero_with_wrong_key_rejected(self) -> None:
        # rationale dict has the wrong key
        with pytest.raises(EvidenceVectorError):
            EvidenceVector(
                indirectness=-1, rationale={"risk_of_bias": "x"},
            )

    def test_zero_domains_need_no_rationale(self) -> None:
        EvidenceVector()  # all-zero default, no rationale, fine

    def test_multiple_nonzero_each_needs_rationale(self) -> None:
        # one rationale missing → reject
        with pytest.raises(EvidenceVectorError):
            EvidenceVector(
                risk_of_bias=-1, inconsistency=-2,
                rationale={"risk_of_bias": "ok"},
            )
        # both rationales present → accept
        ev = EvidenceVector(
            risk_of_bias=-1, inconsistency=-2,
            rationale={
                "risk_of_bias": "blinding broken",
                "inconsistency": "I^2=0.85",
            },
        )
        assert ev.risk_of_bias == -1
        assert ev.inconsistency == -2


class TestUpgradeFlags:
    def test_bool_only(self) -> None:
        with pytest.raises(EvidenceVectorError):
            EvidenceVector(large_effect=1)  # type: ignore[arg-type]

    def test_all_three_flags(self) -> None:
        ev = EvidenceVector(
            large_effect=True, dose_response=True, opposing_confounding=True,
        )
        assert ev.large_effect is True
        assert ev.dose_response is True
        assert ev.opposing_confounding is True


class TestReportingCompliance:
    def test_default_empty_tuple(self) -> None:
        assert EvidenceVector().reporting_compliance == ()

    def test_non_tuple_rejected(self) -> None:
        with pytest.raises(EvidenceVectorError):
            EvidenceVector(reporting_compliance=["CONSORT"])  # type: ignore[arg-type]

    def test_non_string_entry_rejected(self) -> None:
        with pytest.raises(EvidenceVectorError):
            EvidenceVector(reporting_compliance=(1,))  # type: ignore[arg-type]

    def test_string_tuple_accepted(self) -> None:
        ev = EvidenceVector(
            reporting_compliance=("CONSORT", "PRISMA"),
        )
        assert ev.reporting_compliance == ("CONSORT", "PRISMA")


class TestRoundTrip:
    def test_to_from_dict_roundtrip(self) -> None:
        ev = EvidenceVector(
            risk_of_bias=-1,
            inconsistency=-2,
            large_effect=True,
            rationale={
                "risk_of_bias": "blinding broken",
                "inconsistency": "I^2=0.85",
            },
            reporting_compliance=("CONSORT",),
        )
        restored = EvidenceVector.from_dict(ev.to_dict())
        assert restored == ev

    def test_from_dict_validates(self) -> None:
        # Bad dict — nonzero domain without rationale
        with pytest.raises(EvidenceVectorError):
            EvidenceVector.from_dict({"risk_of_bias": -1})


# ---------------------------------------------------------------------------
# assert_claim(evidence=...) — end-to-end through the Graph API
# ---------------------------------------------------------------------------
#
# Before v0.3.0 launch tightening, EvidenceVector was plumbed through the
# entire signing stack but had no user-facing parameter — every signed
# claim carried the default all-zeros vector. The substrate signed a
# story it didn't let callers tell. These tests cover the new parameter
# surface end-to-end: caller-supplied vectors land in the signed
# predicate, the ev_* columns, and survive restore round-trip.


class TestAssertClaimEvidenceParameter:
    def _key(self, tmp_path):
        from mareforma import signing as _sig
        p = tmp_path / "k"
        _sig.bootstrap_key(p)
        return p

    def test_no_evidence_arg_defaults_to_zeros(self, tmp_path) -> None:
        import mareforma
        with mareforma.open(tmp_path, key_path=self._key(tmp_path)) as g:
            cid = g.assert_claim("baseline")
            row = g.get_claim(cid)
        assert row["ev_risk_of_bias"] == 0
        assert row["ev_inconsistency"] == 0
        import json
        ev = json.loads(row["evidence_json"])
        assert ev["rationale"] == {}
        assert ev["reporting_compliance"] == []

    def test_dict_evidence_lands_in_row_and_predicate(self, tmp_path) -> None:
        import mareforma, json
        from mareforma import signing as _sig
        with mareforma.open(tmp_path, key_path=self._key(tmp_path)) as g:
            cid = g.assert_claim(
                "downgraded analysis",
                evidence={
                    "risk_of_bias": -1,
                    "inconsistency": -2,
                    "rationale": {
                        "risk_of_bias": "single-blind only",
                        "inconsistency": "I^2 = 0.82 across studies",
                    },
                    "reporting_compliance": ["CONSORT"],
                },
            )
            row = g.get_claim(cid)
            envelope = json.loads(row["signature_bundle"])
            predicate = _sig.claim_predicate_from_envelope(envelope)
        assert row["ev_risk_of_bias"] == -1
        assert row["ev_inconsistency"] == -2
        ev = predicate["evidence"]
        assert ev["risk_of_bias"] == -1
        assert ev["inconsistency"] == -2
        assert ev["rationale"]["risk_of_bias"] == "single-blind only"
        assert ev["reporting_compliance"] == ["CONSORT"]

    def test_vector_instance_evidence_accepted(self, tmp_path) -> None:
        import mareforma
        ev = mareforma.EvidenceVector(
            risk_of_bias=-1,
            rationale={"risk_of_bias": "attrition not reported"},
            large_effect=True,
        )
        with mareforma.open(tmp_path, key_path=self._key(tmp_path)) as g:
            cid = g.assert_claim("with upgrade flag", evidence=ev)
            row = g.get_claim(cid)
        assert row["ev_risk_of_bias"] == -1
        import json
        stored = json.loads(row["evidence_json"])
        assert stored["large_effect"] is True

    def test_invalid_evidence_dict_raises_at_construction(
        self, tmp_path,
    ) -> None:
        """Nonzero domain without a rationale fails before any row lands."""
        import mareforma
        from mareforma._evidence import EvidenceVectorError
        with mareforma.open(tmp_path, key_path=self._key(tmp_path)) as g:
            with pytest.raises(EvidenceVectorError, match="rationale"):
                g.assert_claim(
                    "bad",
                    evidence={"risk_of_bias": -1},  # no rationale
                )

    def test_evidence_wrong_type_raises(self, tmp_path) -> None:
        import mareforma
        with mareforma.open(tmp_path, key_path=self._key(tmp_path)) as g:
            with pytest.raises(TypeError, match="evidence must be"):
                g.assert_claim("bad", evidence="not a dict")  # type: ignore[arg-type]

    def test_evidence_round_trips_through_restore(self, tmp_path) -> None:
        """Caller-supplied evidence survives claims.toml round-trip
        with byte-equal signed bytes (and statement_cid cross-check)."""
        import mareforma, json
        key_path = self._key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            cid = g.assert_claim(
                "round-trip test",
                evidence={
                    "imprecision": -1,
                    "rationale": {"imprecision": "n=12, CI wide"},
                },
            )
        # Wipe and restore.
        for fname in ("graph.db", "graph.db-wal", "graph.db-shm"):
            p = tmp_path / ".mareforma" / fname
            if p.exists():
                p.unlink()
        mareforma.restore(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as g:
            row = g.get_claim(cid)
        assert row["ev_imprecision"] == -1
        ev = json.loads(row["evidence_json"])
        assert ev["rationale"]["imprecision"] == "n=12, CI wide"
