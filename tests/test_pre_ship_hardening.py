"""Regressions for the pre-ship hardening pass.

Locked-in invariants:

- supports[] and contradicts[] may not reference the same upstream
  claim (logically incoherent shape).
- query(text=...) treats SQLite LIKE wildcards (%, _) and the
  ESCAPE sentinel (\\) as literal characters in the substring
  filter — the README documents this as "case-insensitive
  substring filter" and the implementation must back that contract.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import mareforma


class TestSupportsContradictsIntersectionRefused:
    def test_same_uuid_in_both_lists_raises(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            upstream = graph.assert_claim(
                "upstream", classification="DERIVED",
                generated_by="seed",
            )
            with pytest.raises(ValueError, match="same upstream"):
                graph.assert_claim(
                    "logically incoherent — supports + contradicts u",
                    classification="DERIVED",
                    generated_by="X",
                    supports=[upstream],
                    contradicts=[upstream],
                )

    def test_disjoint_lists_still_pass(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            a = graph.assert_claim("a", classification="DERIVED",
                                   generated_by="seed")
            b = graph.assert_claim("b", classification="DERIVED",
                                   generated_by="seed")
            cid = graph.assert_claim(
                "supports a, contradicts b — legitimate",
                classification="DERIVED",
                generated_by="X",
                supports=[a],
                contradicts=[b],
            )
            assert cid

    def test_doi_overlap_allowed(self, tmp_path: Path) -> None:
        # Citing the same paper as both supporting evidence AND a
        # contrary point is legitimate at the citation level. The
        # gate filters on UUID-shaped refs only.
        with mareforma.open(tmp_path) as graph:
            cid = graph.assert_claim(
                "cites paper-X both for and against",
                classification="ANALYTICAL",
                generated_by="X",
                supports=["10.1234/paper-X"],
                contradicts=["10.1234/paper-X"],
            )
            assert cid


class TestQueryLikeWildcardEscape:
    def _seed(self, graph) -> None:
        graph.assert_claim("alpha finding", classification="INFERRED",
                           generated_by="X")
        graph.assert_claim("beta finding", classification="INFERRED",
                           generated_by="X")
        graph.assert_claim("gamma finding", classification="INFERRED",
                           generated_by="X")

    def test_percent_treated_as_literal_returns_zero(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            self._seed(graph)
            results = graph.query("%", include_unverified=True)
            assert results == [], (
                "query('%') should match nothing (no literal % in any "
                f"row's text), got {len(results)} hits"
            )

    def test_underscore_treated_as_literal_returns_zero(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            self._seed(graph)
            results = graph.query("_", include_unverified=True)
            assert results == []

    def test_empty_string_still_matches_all(self, tmp_path: Path) -> None:
        # An empty substring legitimately matches every row (substring
        # filter semantics). The leak in v0.3.1 was %/_ being treated
        # as wildcards, not the empty case.
        with mareforma.open(tmp_path) as graph:
            self._seed(graph)
            results = graph.query("", include_unverified=True)
            assert len(results) == 3

    def test_literal_percent_in_text_matched(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            graph.assert_claim(
                "study reports 47% reduction",
                classification="ANALYTICAL", generated_by="X",
            )
            graph.assert_claim(
                "no percentage reported here",
                classification="ANALYTICAL", generated_by="X",
            )
            results = graph.query("47%", include_unverified=True)
            assert len(results) == 1
            assert "47%" in results[0]["text"]

    def test_literal_underscore_in_text_matched(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            graph.assert_claim(
                "checkpoint_step finalised",
                classification="ANALYTICAL", generated_by="X",
            )
            graph.assert_claim(
                "no special chars here",
                classification="ANALYTICAL", generated_by="X",
            )
            results = graph.query("checkpoint_step", include_unverified=True)
            assert len(results) == 1
