"""
tests/test_doi_resolver.py — DOI resolution and cache.

Covers:
  - is_doi format detection (with trailing whitespace tolerance)
  - extract_dois filter (strips whitespace)
  - resolve_doi: Crossref hit, DataCite fallback, both miss, timeout, 429,
    URL-encoded suffix (no host injection), no redirect follow
  - resolve_dois_with_cache: cache hit avoids network, cache miss populates,
    TTL expiry forces re-resolution, force=True bypasses cache,
    DataCite-fallback result is cached against its registry
  - clear_unresolved_cache: removes only resolved=0 entries
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from mareforma.db import open_db
from mareforma import doi_resolver
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

    def test_extract_dois_strips_surrounding_whitespace(self) -> None:
        mixed = ["10.1038/foo ", "  10.1234/bar", "\t10.5061/baz\n"]
        assert extract_dois(mixed) == ["10.1038/foo", "10.1234/bar", "10.5061/baz"]

    def test_is_doi_tolerates_surrounding_whitespace(self) -> None:
        assert is_doi(" 10.1038/foo ")
        assert is_doi("\t10.1234/bar\n")


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
        resolved, registry, _rate_limited = resolve_doi("10.1038/test")
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
        resolved, registry, _rate_limited = resolve_doi("10.5061/dryad.test")
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
        resolved, registry, _rate_limited = resolve_doi("10.9999/fake")
        assert resolved is False
        assert registry is None

    def test_network_error_falls_through(self, httpx_mock) -> None:
        httpx_mock.add_exception(httpx.ConnectTimeout("timeout"))
        httpx_mock.add_exception(httpx.ConnectTimeout("timeout"))
        resolved, registry, _rate_limited = resolve_doi("10.1234/timeout")
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

    def test_datacite_fallback_result_is_cached_against_registry(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        """A Crossref-404 → DataCite-200 resolution must persist with registry='datacite'.

        Without this regression: the cache write would lose the source-of-truth
        signal, and a later trust audit could not tell where a DOI was verified.
        """
        conn = open_db(tmp_path)
        httpx_mock.add_response(
            method="HEAD",
            url=CROSSREF.format(doi="10.5061/dryad.fallback"),
            status_code=404,
        )
        httpx_mock.add_response(
            method="HEAD",
            url=DATACITE.format(doi="10.5061/dryad.fallback"),
            status_code=200,
        )

        results = resolve_dois_with_cache(conn, ["10.5061/dryad.fallback"])
        assert results == {"10.5061/dryad.fallback": True}

        cached = conn.execute(
            "SELECT resolved, registry FROM doi_cache WHERE doi = ?",
            ("10.5061/dryad.fallback",),
        ).fetchone()
        assert cached["resolved"] == 1
        assert cached["registry"] == "datacite"
        conn.close()

    def test_expired_negative_cache_entry_re_resolves(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        """A 25-hour-old unresolved entry must be re-checked, not trusted."""
        conn = open_db(tmp_path)
        stale = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
        conn.execute(
            "INSERT INTO doi_cache (doi, resolved, registry, last_checked_at) "
            "VALUES (?, 0, NULL, ?)",
            ("10.1038/stale-neg", stale),
        )
        conn.commit()

        httpx_mock.add_response(
            method="HEAD",
            url=CROSSREF.format(doi="10.1038/stale-neg"),
            status_code=200,
        )
        results = resolve_dois_with_cache(conn, ["10.1038/stale-neg"])
        assert results == {"10.1038/stale-neg": True}

        cached = conn.execute(
            "SELECT resolved FROM doi_cache WHERE doi = ?", ("10.1038/stale-neg",),
        ).fetchone()
        assert cached["resolved"] == 1
        conn.close()

    def test_fresh_positive_cache_is_trusted_within_ttl(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        """A 1-day-old resolved entry is well within the 30-day TTL."""
        conn = open_db(tmp_path)
        fresh = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        conn.execute(
            "INSERT INTO doi_cache (doi, resolved, registry, last_checked_at) "
            "VALUES (?, 1, 'crossref', ?)",
            ("10.1038/fresh", fresh),
        )
        conn.commit()

        # No httpx_mock registrations — a network call here would fail the test.
        results = resolve_dois_with_cache(conn, ["10.1038/fresh"])
        assert results == {"10.1038/fresh": True}
        conn.close()

    def test_force_bypasses_cache(self, tmp_path: Path, httpx_mock) -> None:
        """``force=True`` must trigger a fresh HEAD even on a hot cache entry."""
        conn = open_db(tmp_path)
        fresh = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        conn.execute(
            "INSERT INTO doi_cache (doi, resolved, registry, last_checked_at) "
            "VALUES (?, 0, NULL, ?)",
            ("10.1038/retry-me", fresh),
        )
        conn.commit()

        httpx_mock.add_response(
            method="HEAD",
            url=CROSSREF.format(doi="10.1038/retry-me"),
            status_code=200,
        )
        results = resolve_dois_with_cache(conn, ["10.1038/retry-me"], force=True)
        assert results == {"10.1038/retry-me": True}
        conn.close()


# ---------------------------------------------------------------------------
# Hardening: URL encoding, redirect-blocking, rate-limit handling
# ---------------------------------------------------------------------------

class TestResolveDoiHardening:
    def test_doi_suffix_is_url_encoded(self, httpx_mock) -> None:
        """A DOI suffix with ``#``/``@`` must be percent-encoded into the URL.

        Without encoding, an attacker-controlled suffix like ``foo#@evil.com``
        would let the resolver redirect off-registry. The mock matches only
        the encoded URL — if the resolver issued the unencoded variant,
        pytest-httpx would raise.
        """
        httpx_mock.add_response(
            method="HEAD",
            url="https://api.crossref.org/works/10.1234/foo%23bar%40evil.com",
            status_code=200,
        )
        resolved, registry, _rate_limited = resolve_doi("10.1234/foo#bar@evil.com")
        assert resolved is True
        assert registry == "crossref"

    def test_429_does_not_count_as_resolved(self, httpx_mock) -> None:
        """A Crossref 429 must fall through to DataCite, not be cached as resolved."""
        httpx_mock.add_response(
            method="HEAD",
            url=CROSSREF.format(doi="10.1038/rate-limited"),
            status_code=429,
        )
        httpx_mock.add_response(
            method="HEAD",
            url=DATACITE.format(doi="10.1038/rate-limited"),
            status_code=200,
        )
        resolved, registry, _rate_limited = resolve_doi("10.1038/rate-limited")
        assert resolved is True
        assert registry == "datacite"

    def test_redirect_is_not_followed(self, httpx_mock) -> None:
        """A registry 301 must NOT be followed; it counts as unresolved.

        If the resolver followed redirects, a registry could (or be coerced to)
        301 us to an arbitrary host whose 200 we would trust as authoritative.
        """
        httpx_mock.add_response(
            method="HEAD",
            url=CROSSREF.format(doi="10.1038/redirect"),
            status_code=301,
            headers={"Location": "https://attacker.example.com/"},
        )
        httpx_mock.add_response(
            method="HEAD",
            url=DATACITE.format(doi="10.1038/redirect"),
            status_code=404,
        )
        resolved, registry, _rate_limited = resolve_doi("10.1038/redirect")
        assert resolved is False
        assert registry is None

    def test_request_carries_user_agent(self, httpx_mock) -> None:
        """Crossref polite-pool: the request must identify itself.

        We don't assert the exact UA string, only that one is present and
        mentions mareforma.
        """
        httpx_mock.add_response(
            method="HEAD",
            url=CROSSREF.format(doi="10.1038/ua-check"),
            status_code=200,
        )
        resolve_doi("10.1038/ua-check")
        sent = httpx_mock.get_requests()
        assert sent, "resolver did not issue an HTTP request"
        ua = sent[0].headers.get("user-agent", "")
        assert "mareforma" in ua.lower()

    def test_multi_slash_doi_preserves_inner_slashes(self, httpx_mock) -> None:
        """A real DOI like ``10.1093/imamat/35.3.337`` must keep its inner slash.

        DataCite historically requires slashes-as-slashes for hierarchical
        suffixes; over-encoding to ``%2F`` would 404 there. The mock matches
        only the slash-preserving form.
        """
        httpx_mock.add_response(
            method="HEAD",
            url="https://api.crossref.org/works/10.1093/imamat/35.3.337",
            status_code=200,
        )
        resolved, registry, _rate_limited = resolve_doi("10.1093/imamat/35.3.337")
        assert resolved is True
        assert registry == "crossref"

    def test_dual_429_marks_rate_limited(self, httpx_mock) -> None:
        """If BOTH registries return 429, resolve_doi reports rate_limited=True.

        Callers (resolve_dois_with_cache) use this to skip the cache write —
        a registry-wide throttling event should not poison the cache for 24h.
        """
        httpx_mock.add_response(
            method="HEAD",
            url=CROSSREF.format(doi="10.1038/both-throttled"),
            status_code=429,
        )
        httpx_mock.add_response(
            method="HEAD",
            url=DATACITE.format(doi="10.1038/both-throttled"),
            status_code=429,
        )
        resolved, registry, rate_limited = resolve_doi("10.1038/both-throttled")
        assert resolved is False
        assert registry is None
        assert rate_limited is True

    def test_dual_429_does_not_persist_to_cache(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        """A dual-429 outcome must NOT write a negative cache entry.

        Otherwise a transient registry-wide rate-limit would block REPLICATED
        promotion for the full 24h negative-TTL window after recovery.
        """
        conn = open_db(tmp_path)
        httpx_mock.add_response(
            method="HEAD",
            url=CROSSREF.format(doi="10.1038/throttle-poison"),
            status_code=429,
        )
        httpx_mock.add_response(
            method="HEAD",
            url=DATACITE.format(doi="10.1038/throttle-poison"),
            status_code=429,
        )

        results = resolve_dois_with_cache(conn, ["10.1038/throttle-poison"])
        assert results == {"10.1038/throttle-poison": False}

        cached = conn.execute(
            "SELECT doi FROM doi_cache WHERE doi = ?",
            ("10.1038/throttle-poison",),
        ).fetchone()
        assert cached is None, "throttled DOI must not be cached as unresolved"
        conn.close()

    def test_z_suffix_timestamp_parses_within_ttl(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        """A cache row with a ``Z``-suffixed timestamp must be honored.

        Python 3.10's fromisoformat doesn't parse ``Z`` natively; the helper
        must normalize it so externally-loaded rows behave the same as
        internally-written ``+00:00`` ones.
        """
        conn = open_db(tmp_path)
        fresh_z = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        conn.execute(
            "INSERT INTO doi_cache (doi, resolved, registry, last_checked_at) "
            "VALUES (?, 1, 'crossref', ?)",
            ("10.1038/z-suffix", fresh_z),
        )
        conn.commit()

        # No httpx mocks — network call here would fail the test.
        results = resolve_dois_with_cache(conn, ["10.1038/z-suffix"])
        assert results == {"10.1038/z-suffix": True}
        conn.close()
