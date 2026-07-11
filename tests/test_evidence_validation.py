"""Evidence-vector input validation on the signed predicate.

The GRADE-shaped EvidenceVector class was retired, but its range checks are
load-bearing: an out-of-range or unjustified value must NOT sign into the
immutable predicate. These pin that _normalize_evidence rejects bad input.
"""
from __future__ import annotations

import pytest

from mareforma.db.core import _normalize_evidence


class TestEvidenceValidation:
    def test_out_of_range_grounding_score_rejected(self) -> None:
        with pytest.raises(ValueError):
            _normalize_evidence({"grounding_score": 5.0, "grounding_rationale": "x"})

    def test_grounding_score_requires_rationale(self) -> None:
        with pytest.raises(ValueError):
            _normalize_evidence({"grounding_score": 0.5})

    def test_grounding_score_empty_rationale_rejected(self) -> None:
        with pytest.raises(ValueError):
            _normalize_evidence({"grounding_score": 0.5, "grounding_rationale": "  "})

    def test_downgrade_domain_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError):
            _normalize_evidence({"risk_of_bias": -3})
        with pytest.raises(ValueError):
            _normalize_evidence({"imprecision": 1})

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
