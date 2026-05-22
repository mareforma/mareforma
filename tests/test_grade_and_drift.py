"""Tests for GRADE certainty + DOI drift detection."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import sqlite3

import mareforma
from mareforma import _evidence
from mareforma._evidence import EvidenceVector, EvidenceVectorError
from mareforma import doi_resolver as _doi


# ----------------------------------------------------------------------------
# EvidenceVector.study_design
# ----------------------------------------------------------------------------


class TestStudyDesignField:
    def test_default_is_none(self) -> None:
        v = EvidenceVector()
        assert v.study_design is None

    def test_accepts_known_designs(self) -> None:
        for d in mareforma.VALID_STUDY_DESIGNS:
            v = EvidenceVector(study_design=d)
            assert v.study_design == d

    def test_unknown_design_rejected(self) -> None:
        with pytest.raises(EvidenceVectorError, match="not one of"):
            EvidenceVector(study_design="meta-analysis")

    def test_non_string_design_rejected(self) -> None:
        with pytest.raises(EvidenceVectorError, match="string or None"):
            EvidenceVector(study_design=42)  # type: ignore[arg-type]

    def test_none_design_omitted_from_to_dict(self) -> None:
        # Legacy round-trip: vectors with study_design=None must
        # serialise to the exact pre-existing JSON shape so signed
        # statements keep their canonical bytes.
        v = EvidenceVector()
        assert "study_design" not in v.to_dict()

    def test_set_design_included_in_to_dict(self) -> None:
        v = EvidenceVector(study_design="randomised-trial")
        assert v.to_dict()["study_design"] == "randomised-trial"

    def test_from_dict_round_trip(self) -> None:
        original = EvidenceVector(study_design="observational")
        restored = EvidenceVector.from_dict(original.to_dict())
        assert restored == original

    def test_from_dict_legacy_no_design_field(self) -> None:
        legacy = {
            "risk_of_bias": 0, "inconsistency": 0, "indirectness": 0,
            "imprecision": 0, "publication_bias": 0,
            "large_effect": False, "dose_response": False,
            "opposing_confounding": False,
            "rationale": {}, "reporting_compliance": [],
        }
        v = EvidenceVector.from_dict(legacy)
        assert v.study_design is None


# ----------------------------------------------------------------------------
# EvidenceVector.certainty()
# ----------------------------------------------------------------------------


class TestCertainty:
    def test_default_is_high(self) -> None:
        # No design + no concerns → HIGH (legacy asserter posture).
        assert EvidenceVector().certainty() == "HIGH"

    def test_rct_no_downgrade_is_high(self) -> None:
        v = EvidenceVector(study_design="randomised-trial")
        assert v.certainty() == "HIGH"

    def test_observational_no_concerns_is_low(self) -> None:
        v = EvidenceVector(study_design="observational")
        assert v.certainty() == "LOW"

    def test_case_series_is_very_low(self) -> None:
        v = EvidenceVector(study_design="case-series")
        assert v.certainty() == "VERY_LOW"

    def test_rct_with_one_downgrade_is_moderate(self) -> None:
        v = EvidenceVector(
            study_design="randomised-trial",
            risk_of_bias=-1,
            rationale={"risk_of_bias": "open-label allocation"},
        )
        assert v.certainty() == "MODERATE"

    def test_rct_with_two_serious_downgrades_is_low(self) -> None:
        v = EvidenceVector(
            study_design="randomised-trial",
            risk_of_bias=-1,
            inconsistency=-1,
            rationale={
                "risk_of_bias": "open-label",
                "inconsistency": "heterogeneous effect across subgroups",
            },
        )
        assert v.certainty() == "LOW"

    def test_rct_with_max_downgrade_is_very_low(self) -> None:
        v = EvidenceVector(
            study_design="randomised-trial",
            risk_of_bias=-2,
            indirectness=-2,
            rationale={
                "risk_of_bias": "very serious bias",
                "indirectness": "surrogate outcome",
            },
        )
        assert v.certainty() == "VERY_LOW"

    def test_observational_with_large_effect_upgrades_to_moderate(self) -> None:
        v = EvidenceVector(
            study_design="observational",
            large_effect=True,
        )
        assert v.certainty() == "MODERATE"

    def test_observational_with_all_upgrades_is_high(self) -> None:
        v = EvidenceVector(
            study_design="observational",
            large_effect=True,
            dose_response=True,
            opposing_confounding=True,
        )
        # 2 (LOW baseline) + 3 upgrades → clamped to 4 → HIGH.
        assert v.certainty() == "HIGH"

    def test_observational_upgrade_blocked_when_downgraded(self) -> None:
        # GRADE forbids upgrading downgraded evidence.
        v = EvidenceVector(
            study_design="observational",
            large_effect=True,
            risk_of_bias=-1,
            rationale={"risk_of_bias": "selection bias"},
        )
        # 2 + (-1) = 1 → VERY_LOW. Upgrade does NOT apply because
        # downgrade_sum != 0.
        assert v.certainty() == "VERY_LOW"

    def test_not_applicable_design_is_asserter_high(self) -> None:
        v = EvidenceVector(study_design="not-applicable")
        assert v.certainty() == "HIGH"


# ----------------------------------------------------------------------------
# doi_cache.content_digest column + find_drifted_dois
# ----------------------------------------------------------------------------


class TestDoiCacheContentDigestColumn:
    def test_column_present_on_fresh_db(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            cols = {
                row[1] for row in graph._conn.execute(
                    "PRAGMA table_info(doi_cache)"
                ).fetchall()
            }
            assert "content_digest" in cols

    def test_legacy_db_missing_all_new_columns_upgrades_cleanly(
        self, tmp_path: Path,
    ) -> None:
        # The realistic v0.3.0 → v0.3.1 upgrade scenario: a graph.db
        # whose claims table lacks BOTH predicate_payload and
        # original_signature_bundle AND whose doi_cache lacks
        # content_digest. Open under v0.3.1 must succeed with all
        # three columns auto-added on the way through open_db.
        with mareforma.open(tmp_path) as graph:
            pass  # write a fresh v0.3.1 DB so we have a baseline
        db_path = tmp_path / ".mareforma" / "graph.db"
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute(
                "ALTER TABLE claims DROP COLUMN predicate_payload"
            )
            conn.execute(
                "ALTER TABLE claims DROP COLUMN original_signature_bundle"
            )
            conn.execute(
                "ALTER TABLE doi_cache DROP COLUMN content_digest"
            )
            conn.commit()
        except sqlite3.OperationalError:
            pytest.skip("SQLite < 3.35 cannot DROP COLUMN; test n/a")
        finally:
            conn.close()
        # Confirm the simulated legacy state.
        conn = sqlite3.connect(str(db_path))
        try:
            claims_cols = {
                r[1] for r in conn.execute(
                    "PRAGMA table_info(claims)"
                ).fetchall()
            }
            doi_cols = {
                r[1] for r in conn.execute(
                    "PRAGMA table_info(doi_cache)"
                ).fetchall()
            }
        finally:
            conn.close()
        assert "predicate_payload" not in claims_cols
        assert "original_signature_bundle" not in claims_cols
        assert "content_digest" not in doi_cols

        # Re-open under v0.3.1 — open_db must auto-migrate all three.
        with mareforma.open(tmp_path) as graph:
            claims_cols_after = {
                r[1] for r in graph._conn.execute(
                    "PRAGMA table_info(claims)"
                ).fetchall()
            }
            doi_cols_after = {
                r[1] for r in graph._conn.execute(
                    "PRAGMA table_info(doi_cache)"
                ).fetchall()
            }
        assert "predicate_payload" in claims_cols_after
        assert "original_signature_bundle" in claims_cols_after
        assert "content_digest" in doi_cols_after

    def test_column_added_to_legacy_db(self, tmp_path: Path) -> None:
        # Simulate a legacy DB by opening once, dropping the column via
        # raw SQL (SQLite ALTER TABLE DROP COLUMN requires 3.35+), then
        # reopening and confirming the column is auto-added.
        with mareforma.open(tmp_path) as graph:
            pass
        conn = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        try:
            conn.execute("ALTER TABLE doi_cache DROP COLUMN content_digest")
            conn.commit()
        except sqlite3.OperationalError:
            pytest.skip("SQLite < 3.35 cannot DROP COLUMN; test n/a")
        finally:
            conn.close()
        # Re-open: the column-presence check should re-add it.
        with mareforma.open(tmp_path) as graph:
            cols = {
                row[1] for row in graph._conn.execute(
                    "PRAGMA table_info(doi_cache)"
                ).fetchall()
            }
            assert "content_digest" in cols


class TestComputeContentDigest:
    def test_stable_across_field_order(self) -> None:
        a = {
            "title": ["Some Paper"],
            "issued": {"date-parts": [[2024, 5]]},
            "container-title": ["Nature"],
            "author": [{"family": "Smith"}, {"family": "Jones"}],
        }
        b = {
            "author": [{"family": "Smith"}, {"family": "Jones"}],
            "container-title": ["Nature"],
            "issued": {"date-parts": [[2024, 5]]},
            "title": ["Some Paper"],
        }
        assert _doi._compute_content_digest(a) == _doi._compute_content_digest(b)

    def test_title_change_drifts_digest(self) -> None:
        a = {"title": ["Original Title"], "author": [{"family": "Smith"}]}
        b = {"title": ["Retracted: Original Title"], "author": [{"family": "Smith"}]}
        assert _doi._compute_content_digest(a) != _doi._compute_content_digest(b)

    def test_author_change_drifts_digest(self) -> None:
        a = {"title": ["X"], "author": [{"family": "Smith"}]}
        b = {"title": ["X"], "author": [{"family": "Smith"}, {"family": "Jones"}]}
        assert _doi._compute_content_digest(a) != _doi._compute_content_digest(b)

    def test_unrelated_field_does_not_drift(self) -> None:
        # Abstract / license / indexed-by changes should not register.
        a = {"title": ["X"], "abstract": "v1", "license": ["CC-BY"]}
        b = {"title": ["X"], "abstract": "v2 substantially rewritten",
             "license": ["CC0"]}
        assert _doi._compute_content_digest(a) == _doi._compute_content_digest(b)

    def test_non_dict_returns_none(self) -> None:
        assert _doi._compute_content_digest(None) is None
        assert _doi._compute_content_digest("string") is None
        assert _doi._compute_content_digest(42) is None


class TestFindDriftedDois:
    """End-to-end with stubbed metadata fetch (no network)."""

    @staticmethod
    def _stub(metadata, registry="crossref", rate_limited=False):
        # Build a replacement for fetch_doi_metadata that returns the
        # canonical tuple the new contract requires.
        def _impl(doi, timeout=5.0, registry=None):
            return (metadata, registry or "crossref", rate_limited) if metadata else (None, None, rate_limited)
        return _impl

    def test_first_seen_seeds_digest_not_drift(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            conn = graph._conn
            conn.execute(
                "INSERT INTO doi_cache (doi, resolved, registry, "
                "last_checked_at) VALUES (?, 1, 'crossref', '2026-01-01T00:00:00+00:00')",
                ("10.1234/test",),
            )
            conn.commit()
            monkeypatch.setattr(
                _doi, "fetch_doi_metadata",
                self._stub({
                    "title": ["Stable Title"],
                    "author": [{"family": "Smith"}],
                }),
            )
            drifted = graph.find_drifted_dois()
            assert drifted == []
            # Digest was seeded.
            row = conn.execute(
                "SELECT content_digest FROM doi_cache WHERE doi = ?",
                ("10.1234/test",),
            ).fetchone()
            assert row["content_digest"] is not None

    def test_drift_detected_on_title_change(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            conn = graph._conn
            stored_digest = _doi._compute_content_digest({
                "title": ["Original"],
                "author": [{"family": "Smith"}],
            }, registry="crossref")
            conn.execute(
                "INSERT INTO doi_cache (doi, resolved, registry, "
                "last_checked_at, content_digest) VALUES "
                "(?, 1, 'crossref', '2026-01-01T00:00:00+00:00', ?)",
                ("10.1234/changed", stored_digest),
            )
            conn.commit()
            monkeypatch.setattr(
                _doi, "fetch_doi_metadata",
                self._stub({
                    "title": ["Retracted: Original"],
                    "author": [{"family": "Smith"}],
                }),
            )
            drifted = graph.find_drifted_dois()
            assert len(drifted) == 1
            assert drifted[0]["doi"] == "10.1234/changed"
            assert drifted[0]["stored_digest"] == stored_digest
            assert drifted[0]["current_digest"] != stored_digest

    def test_unchanged_metadata_not_drift(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            conn = graph._conn
            metadata = {
                "title": ["Same"],
                "author": [{"family": "Smith"}],
            }
            stored_digest = _doi._compute_content_digest(
                metadata, registry="crossref",
            )
            conn.execute(
                "INSERT INTO doi_cache (doi, resolved, registry, "
                "last_checked_at, content_digest) VALUES "
                "(?, 1, 'crossref', '2026-01-01T00:00:00+00:00', ?)",
                ("10.1234/stable", stored_digest),
            )
            conn.commit()
            monkeypatch.setattr(
                _doi, "fetch_doi_metadata", self._stub(metadata),
            )
            assert graph.find_drifted_dois() == []

    def test_unresolved_rows_skipped(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        # Only resolved=1 rows are inspected.
        with mareforma.open(tmp_path) as graph:
            conn = graph._conn
            conn.execute(
                "INSERT INTO doi_cache (doi, resolved, last_checked_at, "
                "content_digest) VALUES (?, 0, ?, ?)",
                ("10.1234/unresolved", "2026-01-01T00:00:00+00:00", "old"),
            )
            conn.commit()
            called: list[str] = []

            def _spy(doi, timeout=5.0, registry=None):
                called.append(doi)
                return ({"title": ["X"]}, "crossref", False)

            monkeypatch.setattr(_doi, "fetch_doi_metadata", _spy)
            graph.find_drifted_dois()
            assert called == []

    def test_fetch_failure_skips_silently(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            conn = graph._conn
            conn.execute(
                "INSERT INTO doi_cache (doi, resolved, last_checked_at, "
                "content_digest) VALUES (?, 1, ?, ?)",
                ("10.1234/network-down", "2026-01-01T00:00:00+00:00", "old"),
            )
            conn.commit()
            monkeypatch.setattr(
                _doi, "fetch_doi_metadata",
                lambda doi, timeout=5.0, registry=None: (None, None, False),
            )
            # No drift entries (fetch returned None) and no crash.
            assert graph.find_drifted_dois() == []

    def test_rate_limit_aborts_walk_early(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            conn = graph._conn
            for i in range(5):
                conn.execute(
                    "INSERT INTO doi_cache (doi, resolved, registry, "
                    "last_checked_at, content_digest) VALUES "
                    "(?, 1, 'crossref', ?, ?)",
                    (f"10.1234/rl-{i}", "2026-01-01T00:00:00+00:00", "old"),
                )
            conn.commit()
            calls: list[str] = []

            def _spy(doi, timeout=5.0, registry=None):
                calls.append(doi)
                # Second DOI hits 429 → walk must abort.
                if len(calls) == 2:
                    return (None, None, True)
                return ({"title": ["X"]}, "crossref", False)

            monkeypatch.setattr(_doi, "fetch_doi_metadata", _spy)
            graph.find_drifted_dois()
            # Walk aborted after the rate-limited fetch (the 2nd call).
            assert len(calls) == 2

    def test_default_limit_caps_walk(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            conn = graph._conn
            for i in range(150):
                conn.execute(
                    "INSERT INTO doi_cache (doi, resolved, registry, "
                    "last_checked_at, content_digest) VALUES "
                    "(?, 1, 'crossref', ?, ?)",
                    (f"10.1234/lim-{i}", "2026-01-01T00:00:00+00:00", "old"),
                )
            conn.commit()
            calls: list[str] = []

            def _spy(doi, timeout=5.0, registry=None):
                calls.append(doi)
                return ({"title": ["X"]}, "crossref", False)

            monkeypatch.setattr(_doi, "fetch_doi_metadata", _spy)
            graph.find_drifted_dois()
            # 100 = _DEFAULT_DRIFT_LIMIT.
            assert len(calls) == 100

    def test_walked_count_reflects_actual_inspection(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        # Walk 5 rows successfully, then 429 on the 6th → the
        # returned walked count is 5, aborted=True. Operator sees
        # the partial-coverage signal in health log.
        from mareforma import doi_resolver as _doi
        with mareforma.open(tmp_path) as graph:
            conn = graph._conn
            for i in range(10):
                conn.execute(
                    "INSERT INTO doi_cache (doi, resolved, registry, "
                    "last_checked_at, content_digest) VALUES "
                    "(?, 1, 'crossref', ?, ?)",
                    (f"10.1234/walk-{i}", "2026-01-01T00:00:00+00:00",
                     "old-digest"),
                )
            conn.commit()
            call_counter = {"n": 0}

            def _spy(doi, timeout=5.0, registry=None):
                call_counter["n"] += 1
                if call_counter["n"] == 6:
                    return (None, None, True)
                return ({"title": ["X"]}, "crossref", False)

            monkeypatch.setattr(_doi, "fetch_doi_metadata", _spy)
            drifted, walked, aborted = _doi.find_drifted_dois(conn)
        assert walked == 5
        assert aborted is True

    def test_graph_wrapper_emits_partial_outcome_on_rate_limit(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        # End-to-end: a rate-limited drift scan emits outcome=partial
        # with total_inspected reflecting actual coverage. Operator
        # running `mareforma stats` can distinguish "100 inspected /
        # 0 drifted" from "5 inspected, walk aborted / 0 drifted".
        from mareforma import doi_resolver as _doi
        from mareforma import health as _health
        with mareforma.open(tmp_path) as graph:
            conn = graph._conn
            for i in range(20):
                conn.execute(
                    "INSERT INTO doi_cache (doi, resolved, registry, "
                    "last_checked_at, content_digest) VALUES "
                    "(?, 1, 'crossref', ?, ?)",
                    (f"10.1234/part-{i}", "2026-01-01T00:00:00+00:00",
                     "old-digest"),
                )
            conn.commit()
            calls = {"n": 0}

            def _spy(doi, timeout=5.0, registry=None):
                calls["n"] += 1
                if calls["n"] == 4:
                    return (None, None, True)
                return ({"title": ["X"]}, "crossref", False)

            monkeypatch.setattr(_doi, "fetch_doi_metadata", _spy)
            graph.find_drifted_dois()
        stats = _health.compute_rolling_stats(tmp_path)
        drift = stats["ops"]["doi_drift_scan"]
        assert drift["partial"] == 1
        assert drift["ok"] == 0
        # walked=3 (calls 1, 2, 3 succeeded; 4 was rate-limited).
        assert drift["total_inspected"] == 3

    def test_pinned_to_original_registry(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        # A DOI originally resolved on DataCite must be re-fetched on
        # DataCite — not silently re-fetched on Crossref and produce
        # false drift.
        with mareforma.open(tmp_path) as graph:
            conn = graph._conn
            conn.execute(
                "INSERT INTO doi_cache (doi, resolved, registry, "
                "last_checked_at, content_digest) VALUES "
                "(?, 1, 'datacite', ?, 'irrelevant-digest')",
                ("10.1234/datacite-pinned", "2026-01-01T00:00:00+00:00"),
            )
            conn.commit()
            seen_registries: list[str | None] = []

            def _spy(doi, timeout=5.0, registry=None):
                seen_registries.append(registry)
                return ({"titles": [{"title": "X"}], "creators": [
                    {"familyName": "Smith"},
                ]}, "datacite", False)

            monkeypatch.setattr(_doi, "fetch_doi_metadata", _spy)
            graph.find_drifted_dois()
            assert seen_registries == ["datacite"]


class TestDataCiteShape:
    """find_drifted_dois must understand DataCite metadata shape; the
    earlier Crossref-only extractor produced empty subsets for every
    DataCite DOI."""

    def test_datacite_extractor_pulls_titles_creators_publication_year(
        self,
    ) -> None:
        datacite = {
            "titles": [{"title": "DataCite Paper"}],
            "creators": [
                {"familyName": "Curie"},
                {"familyName": "Meitner"},
            ],
            "publicationYear": 2023,
            "container": {"title": "Journal of Data"},
        }
        crossref = {
            "title": ["DataCite Paper"],
            "author": [{"family": "Curie"}, {"family": "Meitner"}],
            "issued": {"date-parts": [[2023]]},
            "container-title": ["Journal of Data"],
        }
        # Same semantic content → same digest across registries.
        assert _doi._compute_content_digest(datacite, registry="datacite") == (
            _doi._compute_content_digest(crossref, registry="crossref")
        )

    def test_datacite_title_change_drifts_digest(self) -> None:
        a = {"titles": [{"title": "Original"}], "creators": []}
        b = {"titles": [{"title": "Retracted: Original"}], "creators": []}
        assert _doi._compute_content_digest(a, registry="datacite") != (
            _doi._compute_content_digest(b, registry="datacite")
        )


class TestComputeContentDigestEmptySubset:
    """Refuse to seed a digest when every extracted field is empty —
    otherwise every degenerate-metadata DOI hashes to the same value
    and the drift detector cannot distinguish them."""

    def test_all_empty_returns_none(self) -> None:
        assert _doi._compute_content_digest({}) is None
        assert _doi._compute_content_digest({"abstract": "yes"}) is None
        assert _doi._compute_content_digest(
            {"title": "", "author": []}
        ) is None

    def test_one_field_present_succeeds(self) -> None:
        assert _doi._compute_content_digest(
            {"title": "X"}
        ) is not None


class TestNfcNormalisation:
    """NFC normalisation removes false-drift from registry-side flips
    between composed and decomposed Unicode forms for the same author
    name."""

    def test_nfc_vs_nfd_same_digest(self) -> None:
        # é as a single composed codepoint vs e + combining acute.
        composed = {"author": [{"family": "Curié"}]}
        decomposed = {"author": [{"family": "Curié"}]}
        assert _doi._compute_content_digest(composed) == (
            _doi._compute_content_digest(decomposed)
        )
