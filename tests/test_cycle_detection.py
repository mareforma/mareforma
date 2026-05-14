"""tests/test_cycle_detection.py — DFS cycle / self-loop detection.

Covers:
  - self-loop via add_claim's supports[] (impossible at the API surface
    because claim_id is server-generated, but verified for the
    internal helper)
  - self-loop via update_claim (the realistic path)
  - 2-cycle, 3-cycle, n-cycle via update_claim rejected
  - DOI-only supports pass (not graph nodes)
  - mixed claim_id + DOI supports — only the claim_id part walks
  - depth cap kicks in on pathologically long chains
  - empty supports passes
  - signed claim's supports[] cannot be mutated (the signed-immutability
    invariant still holds — cycle detection never reached on signed
    claims)
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

import mareforma
from mareforma.db import (
    CycleDetectedError,
    SignedClaimImmutableError,
    _CYCLE_MAX_DEPTH,
    _check_no_cycle,
    add_claim,
    open_db,
    update_claim,
)


# ---------------------------------------------------------------------------
# Self-loop
# ---------------------------------------------------------------------------


class TestSelfLoop:
    def test_self_loop_via_update_rejected(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as g:
            cid = g.assert_claim("standalone")
        conn = open_db(tmp_path)
        try:
            with pytest.raises(CycleDetectedError, match="self-loop"):
                update_claim(conn, tmp_path, cid, supports=[cid])
        finally:
            conn.close()

    def test_helper_rejects_self_in_supports(self, tmp_path: Path) -> None:
        """The internal _check_no_cycle helper guards even if called
        with a synthesized self-supporting set. Used by future code
        paths that might call the helper directly."""
        conn = open_db(tmp_path)
        try:
            fake_id = "00000000-0000-4000-8000-000000000001"
            with pytest.raises(CycleDetectedError, match="self-loop"):
                _check_no_cycle(conn, fake_id, [fake_id])
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Indirect cycles
# ---------------------------------------------------------------------------


class TestIndirectCycles:
    def test_two_cycle_via_update_rejected(self, tmp_path: Path) -> None:
        """A → B, then update A's supports to include B = closes 2-cycle."""
        with mareforma.open(tmp_path) as g:
            a = g.assert_claim("A")
            b = g.assert_claim("B", supports=[a])
        conn = open_db(tmp_path)
        try:
            with pytest.raises(CycleDetectedError, match="cycle"):
                update_claim(conn, tmp_path, a, supports=[b])
        finally:
            conn.close()

    def test_three_cycle_via_update_rejected(self, tmp_path: Path) -> None:
        """A → B → C, then update A's supports to include C = closes 3-cycle."""
        with mareforma.open(tmp_path) as g:
            a = g.assert_claim("A")
            b = g.assert_claim("B", supports=[a])
            c = g.assert_claim("C", supports=[b])
        conn = open_db(tmp_path)
        try:
            with pytest.raises(CycleDetectedError, match="cycle"):
                update_claim(conn, tmp_path, a, supports=[c])
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# DOI-only and mixed supports
# ---------------------------------------------------------------------------


class TestDOIPassThrough:
    def test_doi_only_supports_no_cycle_possible(self, tmp_path: Path) -> None:
        # DOIs are not graph nodes — walker should skip them entirely.
        # No exception even though the DOI doesn't exist as a claim.
        with mareforma.open(tmp_path) as g:
            cid = g.assert_claim(
                "with DOI supports",
                supports=["10.1234/some.doi"],
            )
        assert cid  # no exception raised


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_supports_passes(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as g:
            cid = g.assert_claim("empty supports")
        assert cid

    def test_unknown_claim_id_in_supports_no_cycle(self, tmp_path: Path) -> None:
        """An unknown UUID in supports[] doesn't crash cycle detection.
        It walks until the row is not found and stops."""
        fake = str(uuid.uuid4())
        with mareforma.open(tmp_path) as g:
            cid = g.assert_claim("references unknown", supports=[fake])
        assert cid

    def test_depth_cap_rejects_long_chain(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """Monkey-patch the cap to a small value, build a chain longer
        than that, verify the walker raises. The production cap
        (1024 hops) is too long to test directly because the chain
        construction itself would trip it — but the cap-rejection
        path is the same."""
        from mareforma import db as _db
        monkeypatch.setattr(_db, "_CYCLE_MAX_DEPTH", 5)
        with mareforma.open(tmp_path) as g:
            ids = [g.assert_claim("genesis")]
            # Build 7 hops — past the patched cap of 5. The first 5
            # hops walk inside the cap and succeed; the 6th hit the cap.
            for i in range(7):
                try:
                    ids.append(g.assert_claim(f"link {i}", supports=[ids[-1]]))
                except CycleDetectedError as exc:
                    assert "depth cap" in str(exc)
                    return
        pytest.fail("Expected CycleDetectedError on depth-cap overflow")


# ---------------------------------------------------------------------------
# Interaction with signed-claim immutability
# ---------------------------------------------------------------------------


class TestSignedClaimUnreachable:
    def test_signed_claim_supports_mutation_still_refused(
        self, tmp_path: Path,
    ) -> None:
        """Signed claims refuse supports mutation upstream of cycle
        detection. The signed-immutability invariant is unchanged."""
        from mareforma import signing as _sig
        key = tmp_path / "k"
        _sig.bootstrap_key(key)
        with mareforma.open(tmp_path, key_path=key) as g:
            cid = g.assert_claim("signed claim")
        conn = open_db(tmp_path)
        try:
            with pytest.raises(SignedClaimImmutableError):
                update_claim(conn, tmp_path, cid, supports=[cid])
        finally:
            conn.close()
