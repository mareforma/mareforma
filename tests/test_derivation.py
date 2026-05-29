"""Smoke tests for :mod:`mareforma.derivation`.

The maqueta-grade unit suite (~145 tests) lives under
``tests/derivation/`` in the source-of-truth repo and runs when the
``[derivation]`` extra is installed. These tests verify the public
import surface plus graceful degradation when ``tree_sitter`` is
absent.

Conceptual clusters:

- :class:`TestPublicSurface` — every documented symbol is importable.
- :class:`TestGracefulDegradation` — clean error when the optional
  ``[derivation]`` extra is not installed.
- :class:`TestPureStdlibPath` — log_templates works without
  tree_sitter.
- :class:`TestVersionPin` — DERIVATION_VERSION is semver-shaped.
"""

from __future__ import annotations

import pytest

import mareforma.derivation as D


class TestPublicSurface:
    def test_imports(self):
        assert D.DERIVATION_VERSION
        assert D.derive_classification
        assert D.verify_classification
        assert D.extract_templates
        assert D.extract_source_profile
        assert D.extract_directory_profile
        assert hasattr(D, "HAS_TREE_SITTER")


class TestGracefulDegradation:
    def test_extract_source_profile_raises_clean_error_without_tree_sitter(self):
        """When [derivation] extra is uninstalled, the error names the extra."""
        if D.HAS_TREE_SITTER:
            pytest.skip("tree_sitter installed; skipping degradation check")

        with pytest.raises(ImportError) as ei:
            D.extract_source_profile("x = 1")
        msg = str(ei.value)
        assert "mareforma[derivation]" in msg, (
            f"degradation error must point at the extra: {msg!r}"
        )


class TestPureStdlibPath:
    def test_log_templates_extract_works_without_tree_sitter(self):
        """log_templates module is pure-Python; works regardless of extras."""
        result = D.extract_templates([
            "INFO connecting to database",
            "INFO connected to database",
            "INFO query returned 5 rows",
        ])
        assert isinstance(result.templates, list)
        assert len(result.templates) >= 1


class TestVersionPin:
    def test_derivation_version_is_pinned_semver(self):
        parts = D.DERIVATION_VERSION.split(".")
        assert len(parts) == 3
        for p in parts:
            int(p)  # raises ValueError if not numeric
