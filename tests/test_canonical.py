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
