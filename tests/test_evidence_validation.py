"""Evidence-vector input validation on the signed predicate.

The GRADE-shaped EvidenceVector class was retired, but its range checks are
load-bearing: an out-of-range or unjustified value must NOT sign into the
immutable predicate. These pin that _normalize_evidence rejects bad input.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from mareforma.db.core import _normalize_evidence


class TestEvidenceValidation:
    def test_out_of_range_grounding_score_rejected(self) -> None:
        with pytest.raises(ValueError):
            _normalize_evidence({"grounding_score": 5.0, "grounding_rationale": "x"})

    def test_grounding_score_requires_rationale(self) -> None:
        with pytest.raises(ValueError):
            _normalize_evidence({"grounding_score": 0.5})

    def test_bool_grounding_score_rejected(self) -> None:
        # bool is int in Python; True must not sign as a 1.0 score.
        with pytest.raises(ValueError):
            _normalize_evidence({"grounding_score": True, "grounding_rationale": "x"})

    def test_grounding_score_empty_rationale_rejected(self) -> None:
        with pytest.raises(ValueError):
            _normalize_evidence({"grounding_score": 0.5, "grounding_rationale": "  "})

    def test_downgrade_domain_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError):
            _normalize_evidence({"risk_of_bias": -3})
        with pytest.raises(ValueError):
            _normalize_evidence({"imprecision": 1})

    def test_non_bool_upgrade_flag_rejected(self) -> None:
        with pytest.raises(ValueError, match="large_effect"):
            _normalize_evidence({"large_effect": "probably"})

    def test_bare_string_reporting_compliance_rejected(self) -> None:
        # list("CONSORT") splats into seven single-letter guidelines, and the
        # claim signs compliance with all of them.
        with pytest.raises(ValueError, match="reporting_compliance"):
            _normalize_evidence({"reporting_compliance": "CONSORT"})

    def test_non_string_reporting_compliance_entry_rejected(self) -> None:
        with pytest.raises(ValueError, match="reporting_compliance"):
            _normalize_evidence({"reporting_compliance": ["CONSORT", 7]})

    def test_non_string_rationale_value_rejected(self) -> None:
        with pytest.raises(ValueError, match="rationale"):
            _normalize_evidence({"rationale": {"risk_of_bias": 3}})

    def test_non_string_study_design_rejected(self) -> None:
        with pytest.raises(ValueError, match="study_design"):
            _normalize_evidence({"study_design": {"kind": "rct"}})

    def test_valid_evidence_normalizes(self) -> None:
        out = _normalize_evidence(
            {"grounding_score": 0.8, "grounding_rationale": "read the cited file"}
        )
        assert out["grounding_score"] == 0.8
        assert out["grounding_rationale"] == "read the cited file"
        assert out["risk_of_bias"] == 0

    def test_empty_evidence_is_byte_stable(self) -> None:
        # The all-zeros default shape must be unchanged by the validation.
        assert _normalize_evidence(None) == _normalize_evidence({})
        out = _normalize_evidence(None)
        assert out["risk_of_bias"] == 0 and out["large_effect"] is False

    def test_assert_claim_refuses_before_writing(self, tmp_path: Path) -> None:
        """The refusal happens on the public path, with no claim written."""
        with mareforma.open(tmp_path) as graph:
            with pytest.raises(ValueError, match="reporting_compliance"):
                graph.assert_claim("x", evidence={"reporting_compliance": "CONSORT"})
            assert graph.query() == []
