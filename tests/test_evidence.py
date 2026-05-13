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
