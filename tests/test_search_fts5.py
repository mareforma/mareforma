"""FTS5 search tests.

``graph.search(query, ...)`` runs SQLite FTS5 with the unicode61
tokenizer over claim text. Sync is via three triggers on the
``claims`` table that mirror INSERT / DELETE / text-UPDATE into the
``claims_fts`` virtual table. Pure-wildcard queries are refused.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from mareforma import signing as _signing


def _bootstrap_key(tmp_path: Path, name: str = "root.key") -> Path:
    key_path = tmp_path / name
    _signing.bootstrap_key(key_path)
    return key_path


# ---------------------------------------------------------------------------
# Basic match
# ---------------------------------------------------------------------------

class TestSearchBasicMatch:
    def test_single_term_match(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("dopamine modulates striatal neurons")
            g.assert_claim("serotonin signaling in hippocampus")
            results = g.search("dopamine")
        assert len(results) == 1
        assert "dopamine" in results[0]["text"]

    def test_zero_match_returns_empty(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("dopamine modulates striatal neurons")
            results = g.search("acetylcholine")
        assert results == []

    def test_prefix_match(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("dopaminergic pathway implicated")
            g.assert_claim("dopamine receptor D2 antagonist")
            g.assert_claim("serotonin reuptake")
            results = g.search("dopamin*")
        assert len(results) == 2

    def test_phrase_match(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("epistemic graph for AI scientists")
            g.assert_claim("knowledge graph databases")
            results = g.search('"epistemic graph"')
        assert len(results) == 1


# ---------------------------------------------------------------------------
# Unicode / diacritics
# ---------------------------------------------------------------------------

class TestSearchUnicodeDiacritics:
    def test_diacritic_folding(self, tmp_path: Path) -> None:
        """unicode61 + remove_diacritics=2 should let 'gene' match
        'géné' and vice versa."""
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("Étude du gène CD4 chez la souris")
            results_unaccented = g.search("gene")
            results_accented = g.search("gène")
        assert len(results_unaccented) == 1
        assert len(results_accented) == 1


# ---------------------------------------------------------------------------
# Wildcard rejection
# ---------------------------------------------------------------------------

class TestSearchWildcardRejection:
    def test_empty_query_refused(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("anything")
            with pytest.raises(ValueError, match="Empty search query"):
                g.search("")

    def test_pure_wildcard_refused(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("anything")
            with pytest.raises(ValueError, match="just wildcards"):
                g.search("*")

    def test_multiple_wildcards_refused(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("anything")
            with pytest.raises(ValueError, match="just wildcards"):
                g.search("* ** ***")

    def test_term_plus_wildcard_allowed(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("dopaminergic pathway")
            # FTS5 prefix syntax: trailing * is fine.
            results = g.search("dopam*")
        assert len(results) == 1


# ---------------------------------------------------------------------------
# Filter composition (min_support, classification, include_unverified)
# ---------------------------------------------------------------------------

class TestSearchFilters:
    def test_min_support_filter(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            seed = g.assert_claim(
                "dopamine reference work",
                generated_by="seed", seed=True,
            )
            g.assert_claim(
                "dopamine modulates striatum",
                supports=[seed], generated_by="A",
            )
            g.assert_claim(
                "dopamine modulates striatum",
                supports=[seed], generated_by="B",
            )
            # Now we have 1 ESTABLISHED + 2 REPLICATED with 'dopamine'.
            replicated = g.search("dopamine", min_support="REPLICATED")
            established = g.search("dopamine", min_support="ESTABLISHED")
        # min_support=REPLICATED includes REPLICATED + ESTABLISHED.
        assert len(replicated) == 3
        # min_support=ESTABLISHED is just the seed.
        assert len(established) == 1

    def test_classification_filter(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim(
                "alpha finding", classification="INFERRED",
            )
            g.assert_claim(
                "alpha finding 2", classification="ANALYTICAL",
            )
            results = g.search(
                "alpha", classification="ANALYTICAL",
            )
        assert len(results) == 1
        assert results[0]["classification"] == "ANALYTICAL"

    def test_default_excludes_unverified_preliminary(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path) as g:  # unsigned
            g.assert_claim("alpha unverified")
            results = g.search("alpha")
        assert results == []

    def test_include_unverified_true_surfaces_unsigned(
        self, tmp_path: Path,
    ) -> None:
        with mareforma.open(tmp_path) as g:
            g.assert_claim("alpha unverified")
            results = g.search("alpha", include_unverified=True)
        assert len(results) == 1


# ---------------------------------------------------------------------------
# Index sync via triggers
# ---------------------------------------------------------------------------

class TestFTSIndexSync:
    def test_index_picks_up_inserts(self, tmp_path: Path) -> None:
        """The claims_fts_ai trigger fires on INSERT — a freshly added
        claim is searchable immediately."""
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("first")
            assert len(g.search("first")) == 1
            g.assert_claim("second")
            assert len(g.search("second")) == 1
            # Both are searchable.
            assert len(g.search("first")) == 1

    def test_index_reflects_text_update(self, tmp_path: Path) -> None:
        """An unsigned claim's text can be updated; the FTS index must
        reflect the new text and forget the old."""
        from mareforma import db as _db
        with mareforma.open(tmp_path) as g:
            cid = g.assert_claim("original text")
            assert len(g.search("original", include_unverified=True)) == 1
            _db.update_claim(g._conn, g._root, cid, text="revised body")
            assert g.search("original", include_unverified=True) == []
            assert len(g.search("revised", include_unverified=True)) == 1


# ---------------------------------------------------------------------------
# Reputation projection on search results
# ---------------------------------------------------------------------------

class TestSearchReputationProjection:
    def test_search_results_carry_reputation_fields(
        self, tmp_path: Path,
    ) -> None:
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("dopamine pathway")
            results = g.search("dopamine")
        assert len(results) == 1
        # Same projection as query().
        assert "validator_reputation" in results[0]
        assert "generator_enrolled" in results[0]
        assert results[0]["generator_enrolled"] is True
        # Not yet ESTABLISHED — reputation is 0.
        assert results[0]["validator_reputation"] == 0


# ---------------------------------------------------------------------------
# Closed-graph guard
# ---------------------------------------------------------------------------

class TestSearchClosedGuard:
    def test_search_on_closed_graph_raises(self, tmp_path: Path) -> None:
        key = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key) as g:
            g.assert_claim("anything")
        # g is closed now.
        with pytest.raises(RuntimeError, match="closed"):
            g.search("anything")
