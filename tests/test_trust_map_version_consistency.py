"""The trust map fails closed when its code version drifts from the package.

The trust-map version stamp witnesses this module's property set and tier
semantics; the package version names the release it ships inside. A build that
packages a stale trust map beside a newer package renders a map whose logic does
not match the version it reports, so an axis can be under-named while the map
still reads as authoritative (a single-root finding could report an independence
count without naming the operator-Sybil residual). The map builder refuses to
assemble a map in that state rather than present one whose honesty it cannot
vouch for, and the stamped version stays pinned to the package version so a
drifted build is caught rather than shipped.
"""
from __future__ import annotations

import pytest

import mareforma
import mareforma.trust_map as trust_map
from mareforma.trust_map import (
    TRUST_MAP_VERSION,
    TrustMapVersionError,
    _assemble,
    _require_consistent_version,
)
from tests._helpers import _claim


class TestTrustMapVersionConsistency:
    def test_stamp_matches_the_package_version(self) -> None:
        """The stamped trust-map version tracks the package version, so a build
        that drifts one from the other is caught rather than shipped."""
        assert TRUST_MAP_VERSION.removeprefix("v") == mareforma.__version__

    def test_consistent_version_does_not_refuse(self) -> None:
        """On a consistent build the guard passes and a map assembles normally."""
        _require_consistent_version()
        tmap = _assemble(
            _claim(), n_roots=1, has_inclusion=False,
            effective_independence={"number": 2, "soft": False},
        )
        assert tmap.get("independence").value == "2"

    def test_drifted_stamp_refuses_to_build_a_map(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A trust map stamped at a different version than the package (the
        stale-build corruption) fails closed instead of emitting a map whose
        residuals may not match the shipped logic."""
        monkeypatch.setattr(
            trust_map, "TRUST_MAP_VERSION", f"v0.0.0-not-{mareforma.__version__}",
        )
        with pytest.raises(TrustMapVersionError):
            _require_consistent_version()
        # The map builder itself refuses, so the possibly under-named axis never
        # reaches a reader.
        with pytest.raises(TrustMapVersionError):
            _assemble(
                _claim(), n_roots=1, has_inclusion=False,
                effective_independence={"number": 2, "soft": False},
            )
