"""
tests/test_doi_resolver.py — DOI resolution and cache.

Covers:
  - is_doi format detection
  - extract_dois filter
  - resolve_doi: Crossref hit, DataCite fallback, both miss, timeout
  - resolve_dois_with_cache: cache hit avoids network, cache miss populates
  - clear_unresolved_cache: removes only resolved=0 entries
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import httpx
import pytest

from mareforma.db import open_db
from mareforma.doi_resolver import (
    clear_unresolved_cache,
    extract_dois,
    is_doi,
    resolve_doi,
    resolve_dois_with_cache,
)


CROSSREF = "https://api.crossref.org/works/{doi}"
DATACITE = "https://api.datacite.org/dois/{doi}"


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

class TestIsDoi:
    def test_valid_doi(self) -> None:
        assert is_doi("10.1038/s41586-023-06814-7")
        assert is_doi("10.1234/foo.bar")

    def test_uuid_not_a_doi(self) -> None:
        assert not is_doi("3f8a1b2c-1234-5678-9abc-def012345678")

    def test_arbitrary_string_not_a_doi(self) -> None:
        assert not is_doi("upstream-X")
        assert not is_doi("agent_alpha")
        assert not is_doi("")

    def test_extract_dois_filters_mixed_list(self) -> None:
        mixed = [
            "10.1038/foo",
            "3f8a1b2c-1234-5678-9abc-def012345678",
            "10.1234/bar",
            "upstream-X",
        ]
        assert extract_dois(mixed) == ["10.1038/foo", "10.1234/bar"]


# ---------------------------------------------------------------------------
# resolve_doi — direct registry calls
# ---------------------------------------------------------------------------

class TestResolveDoi:
    def test_crossref_200_returns_resolved(self, httpx_mock) -> None:
        httpx_mock.add_response(
            method="HEAD",
            url=CROSSREF.format(doi="10.1038/test"),
            status_code=200,
        )
        resolved, registry = resolve_doi("10.1038/test")
        assert resolved is True
        assert registry == "crossref"

    def test_crossref_404_then_datacite_200(self, httpx_mock) -> None:
        httpx_mock.add_response(
            method="HEAD",
            url=CROSSREF.format(doi="10.5061/dryad.test"),
            status_code=404,
        )
        httpx_mock.add_response(
            method="HEAD",
            url=DATACITE.format(doi="10.5061/dryad.test"),
            status_code=200,
        )
        resolved, registry = resolve_doi("10.5061/dryad.test")
        assert resolved is True
        assert registry == "datacite"

    def test_both_registries_404(self, httpx_mock) -> None:
        httpx_mock.add_response(
            method="HEAD",
            url=CROSSREF.format(doi="10.9999/fake"),
            status_code=404,
        )
        httpx_mock.add_response(
            method="HEAD",
            url=DATACITE.format(doi="10.9999/fake"),
            status_code=404,
        )
        resolved, registry = resolve_doi("10.9999/fake")
        assert resolved is False
        assert registry is None

    def test_network_error_falls_through(self, httpx_mock) -> None:
        httpx_mock.add_exception(httpx.ConnectTimeout("timeout"))
        httpx_mock.add_exception(httpx.ConnectTimeout("timeout"))
        resolved, registry = resolve_doi("10.1234/timeout")
        assert resolved is False
        assert registry is None


# ---------------------------------------------------------------------------
# resolve_dois_with_cache — uses doi_cache table
# ---------------------------------------------------------------------------

class TestResolveDoisWithCache:
    def test_cache_hit_avoids_network(self, tmp_path: Path, httpx_mock) -> None:
        conn = open_db(tmp_path)
        # Pre-populate cache as resolved.
        conn.execute(
            "INSERT INTO doi_cache (doi, resolved, registry, last_checked_at) "
            "VALUES (?, 1, 'crossref', '2026-05-12T00:00:00+00:00')",
            ("10.1038/cached",),
        )
        conn.commit()

        # No httpx mocks registered — if the resolver tries to hit the network, pytest-httpx will fail.
        results = resolve_dois_with_cache(conn, ["10.1038/cached"])
        assert results == {"10.1038/cached": True}
        conn.close()

    def test_cache_miss_populates(self, tmp_path: Path, httpx_mock) -> None:
        conn = open_db(tmp_path)
        httpx_mock.add_response(
            method="HEAD",
            url=CROSSREF.format(doi="10.1038/new"),
            status_code=200,
        )

        results = resolve_dois_with_cache(conn, ["10.1038/new"])
        assert results == {"10.1038/new": True}

        cached = conn.execute(
            "SELECT resolved, registry FROM doi_cache WHERE doi = ?",
            ("10.1038/new",),
        ).fetchone()
        assert cached["resolved"] == 1
        assert cached["registry"] == "crossref"
        conn.close()

    def test_clear_unresolved_cache_only_removes_unresolved(self, tmp_path: Path) -> None:
        conn = open_db(tmp_path)
        conn.executemany(
            "INSERT INTO doi_cache (doi, resolved, registry, last_checked_at) "
            "VALUES (?, ?, ?, '2026-05-12T00:00:00+00:00')",
            [
                ("10.1038/ok", 1, "crossref"),
                ("10.9999/bad", 0, None),
            ],
        )
        conn.commit()

        cleared = clear_unresolved_cache(conn)
        assert cleared == ["10.9999/bad"]

        remaining = {
            row["doi"] for row in conn.execute("SELECT doi FROM doi_cache").fetchall()
        }
        assert remaining == {"10.1038/ok"}
        conn.close()
