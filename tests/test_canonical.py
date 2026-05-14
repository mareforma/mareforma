"""Canonical JSON serializer tests."""

from __future__ import annotations

import math
import subprocess
import sys

import pytest

from mareforma._canonical import canonicalize


class TestCanonicalize:
    def test_sorts_keys(self) -> None:
        assert canonicalize({"b": 1, "a": 2}) == b'{"a":2,"b":1}'

    def test_no_whitespace(self) -> None:
        out = canonicalize({"a": [1, 2, 3], "b": {"c": "d"}})
        assert b" " not in out
        assert b"\n" not in out

    def test_unicode_passes_through(self) -> None:
        # raw UTF-8, not \uXXXX escapes
        assert canonicalize({"k": "résumé"}) == '{"k":"résumé"}'.encode("utf-8")

    def test_nfc_normalization(self) -> None:
        # composed é (U+00E9) and decomposed é (e + U+0301) must produce
        # the same canonical bytes
        composed = "café"  # U+00E9
        decomposed = "café"  # e + combining acute
        assert composed != decomposed  # different code points
        assert canonicalize({"k": composed}) == canonicalize({"k": decomposed})

    def test_nested_keys_sorted(self) -> None:
        out = canonicalize({"a": {"z": 1, "y": 2}, "b": [{"d": 4, "c": 3}]})
        assert out == b'{"a":{"y":2,"z":1},"b":[{"c":3,"d":4}]}'

    def test_null_serializes(self) -> None:
        assert canonicalize({"k": None}) == b'{"k":null}'

    def test_bool_serializes(self) -> None:
        assert canonicalize({"t": True, "f": False}) == b'{"f":false,"t":true}'

    def test_empty_object(self) -> None:
        assert canonicalize({}) == b"{}"

    def test_empty_array(self) -> None:
        assert canonicalize([]) == b"[]"


class TestRejectsNonFinite:
    def test_nan_rejected(self) -> None:
        with pytest.raises(ValueError):
            canonicalize({"p": float("nan")})

    def test_positive_infinity_rejected(self) -> None:
        with pytest.raises(ValueError):
            canonicalize({"p": math.inf})

    def test_negative_infinity_rejected(self) -> None:
        with pytest.raises(ValueError):
            canonicalize({"p": -math.inf})


class TestBytestableAcrossRuns:
    def test_subprocess_produces_identical_bytes(self) -> None:
        """The serializer must be byte-stable across Python interpreter
        runs. We run a subprocess that does the same canonicalization and
        compare its output to ours.
        """
        payload = {
            "z": 9,
            "a": "résumé",
            "nested": {"y": [3, 1, 2], "x": None},
            "bool": True,
        }
        in_process = canonicalize(payload)

        # The subprocess runs the same interpreter (`sys.executable`), so
        # `mareforma` is on its sys.path via whatever install method the
        # test runner used — no cwd or sys.path manipulation needed.
        script = (
            "import sys; "
            "from mareforma._canonical import canonicalize; "
            f"sys.stdout.buffer.write(canonicalize({payload!r}))"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            check=True,
        )
        assert result.stdout == in_process


class TestRfc8785NumberRules:
    """Verify the canonicalizer follows RFC 8785 ECMAScript Number rules
    for floats. The non-float schema in v0.3.0 happens to produce the
    same bytes as the prior stdlib canonicalizer — these tests guard
    the case where a future schema introduces a float field."""

    def test_integral_float_drops_decimal(self) -> None:
        """RFC 8785 §3.2.2.3: 1.0 → "1" (per ECMAScript Number.toString)."""
        assert canonicalize({"x": 1.0}) == b'{"x":1}'

    def test_non_integral_float_preserves(self) -> None:
        """Non-integer floats render with the minimum digits needed for
        round-trip recovery (ES shortest-roundtrip)."""
        assert canonicalize({"x": 2.5}) == b'{"x":2.5}'

    def test_large_float_no_exponent_in_range(self) -> None:
        """RFC 8785 §3.2.2.3: 1e10 → 10000000000 (no exponent for values
        in the ES Number.toString non-exponential range)."""
        assert canonicalize({"x": 1e10}) == b'{"x":10000000000}'

    def test_negative_zero_normalizes_to_zero(self) -> None:
        """-0.0 and 0.0 are equal under IEEE-754 but JSON has one zero."""
        assert canonicalize({"x": -0.0}) == canonicalize({"x": 0.0})


class TestRfc8785ByteCompatWithNoFloats:
    """The current v0.3.0 schema contains no float fields. Byte output
    for the existing field shapes must match the prior stdlib
    canonicalization exactly — otherwise every signed claim in any
    existing graph.db would fail re-verification on this release."""

    def test_int_only_payload_matches_legacy(self) -> None:
        """Signed ints in the EvidenceVector range round-trip the same way."""
        payload = {
            "risk_of_bias": -2,
            "inconsistency": -1,
            "indirectness": 0,
            "imprecision": 0,
            "publication_bias": -2,
        }
        out = canonicalize(payload)
        # All keys sorted, no whitespace, integer form unchanged.
        assert out == (
            b'{"imprecision":0,"inconsistency":-1,"indirectness":0,'
            b'"publication_bias":-2,"risk_of_bias":-2}'
        )

    def test_bool_only_payload_matches_legacy(self) -> None:
        """Upgrade flags (bools) serialize as ``true`` / ``false``."""
        payload = {
            "large_effect": True,
            "dose_response": False,
            "opposing_confounding": True,
        }
        assert canonicalize(payload) == (
            b'{"dose_response":false,"large_effect":true,'
            b'"opposing_confounding":true}'
        )

    def test_claim_envelope_shape_matches_legacy(self) -> None:
        """A realistic claim payload (text, classification, supports,
        timestamps, evidence) produces the same bytes the old
        canonicalizer would have produced."""
        payload = {
            "claim_id": "11111111-1111-4111-8111-111111111111",
            "text": "Cell type A shows property X (n=12, p=0.01)",
            "classification": "ANALYTICAL",
            "generated_by": "agent/model-a/lab_a",
            "supports": ["22222222-2222-4222-8222-222222222222"],
            "contradicts": [],
            "source_name": "depmap_24q2",
            "artifact_hash": None,
            "created_at": "2026-05-14T10:00:00+00:00",
            "evidence": {
                "risk_of_bias": -1,
                "inconsistency": 0,
                "indirectness": 0,
                "imprecision": -1,
                "publication_bias": 0,
                "large_effect": False,
                "dose_response": False,
                "opposing_confounding": False,
                "rationale": {"risk_of_bias": "single-lab study"},
                "reporting_compliance": [],
            },
        }
        out = canonicalize(payload)
        # Top-level keys are sorted; nested objects are also sorted.
        # The order below mirrors what the prior canonicalizer produced
        # and what any RFC 8785-conformant implementation produces.
        assert out.startswith(b'{"artifact_hash":null,"claim_id":')
        assert b'"classification":"ANALYTICAL"' in out
        assert b'"created_at":"2026-05-14T10:00:00+00:00"' in out
        assert b'"text":"Cell type A shows property X (n=12, p=0.01)"' in out
        # The whole envelope decodes back to the same dict.
        import json as _json
        assert _json.loads(out) == payload
