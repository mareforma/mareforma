"""
doi_resolver.py — DOI resolution via Crossref and DataCite.

DOIs in claim ``supports[]`` and ``contradicts[]`` are HEAD-checked against
public registries at assertion time. Unresolved DOIs mark the claim as
``unresolved=True``, blocking promotion to REPLICATED.

Cache
-----
Results are persisted to the ``doi_cache`` table to avoid repeated network
calls. Resolved DOIs are cached for 30 days; unresolved entries for 24 hours.
Entries past their TTL are silently re-resolved. The TTL exists so that
retractions and registry outages eventually self-correct without operator
intervention.

Network compliance
------------------
- All requests carry a User-Agent identifying the project (Crossref polite-pool).
- Redirects are NOT followed: a DOI MUST resolve at the registry host itself.
- The DOI is URL-encoded before interpolation: a suffix containing ``#`` or
  ``@`` would otherwise let the caller redirect the resolver to another host.
- HTTP 429 falls through as unresolved; the caller retries after TTL.

Behavior
--------
- DOI format check (``10.\\d+/...``) before any network call; whitespace stripped.
- Try Crossref first, fall back to DataCite.
- On any HTTP error, timeout, or non-2xx, the DOI is treated as unresolved.
- Resolution is fail-closed at the claim level: any unresolved DOI in
  ``supports[]`` or ``contradicts[]`` sets ``claim.unresolved=True``.
"""

from __future__ import annotations

import re
import sqlite3
import threading
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Optional

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False


_DOI_PATTERN = re.compile(r"^10\.\d{4,}/.+")

_CROSSREF_URL = "https://api.crossref.org/works/{doi}"
_DATACITE_URL = "https://api.datacite.org/dois/{doi}"

_DEFAULT_TIMEOUT = 5.0

# Cache TTLs. Positive: a paper survives for years, but retractions do happen;
# 30 days bounds the staleness. Negative: registry blips heal quickly, 24 h
# lets a temporarily-unreachable DOI promote on its own.
_TTL_RESOLVED = timedelta(days=30)
_TTL_UNRESOLVED = timedelta(hours=24)

_USER_AGENT = (
    "mareforma/0.3.0 (+https://github.com/mareforma/mareforma; "
    "mailto:hello@mareforma.com)"
)


_client: Optional["httpx.Client"] = None
_client_lock = threading.Lock()


def _get_client() -> "httpx.Client":
    """Return the module-level httpx.Client, lazily constructing it.

    A single pooled client across the process amortises TCP+TLS setup across
    many DOI resolutions. ``follow_redirects=False`` is enforced here so a
    poisoned DOI cannot redirect the resolver off-registry.

    Initialization is locked so a multi-threaded harness cannot race two
    Client constructors and leak the loser's connection pool.
    """
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                _client = httpx.Client(
                    headers={"User-Agent": _USER_AGENT},
                    timeout=_DEFAULT_TIMEOUT,
                    follow_redirects=False,
                )
    return _client


def _reset_client_for_testing() -> None:
    """Drop the cached client so a test fixture can install fresh mocks."""
    global _client
    with _client_lock:
        if _client is not None:
            try:
                _client.close()
            except Exception:  # noqa: BLE001
                pass
            _client = None


def is_doi(s: str) -> bool:
    """Return True if string matches DOI format ``10.<registrant>/<suffix>``."""
    return bool(_DOI_PATTERN.match(s.strip()))


def extract_dois(values: list[str]) -> list[str]:
    """Filter a list to only DOIs, stripping surrounding whitespace."""
    return [v.strip() for v in values if is_doi(v)]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_iso() -> str:
    return _utcnow().isoformat()


def _encode_doi(doi: str) -> str:
    """Percent-encode the DOI so the suffix cannot escape the URL path.

    A DOI is ``<prefix>/<suffix>``. The suffix may contain almost any
    Unicode codepoint; an unencoded ``#`` or ``?`` would terminate the URL
    path and let a poisoned DOI redirect the resolver elsewhere.

    Slashes are PRESERVED inside the suffix: real-world DOIs commonly have
    multi-segment suffixes (e.g. ``10.1093/imamat/35.3.337``), and DataCite's
    /dois/ endpoint historically required slashes-as-slashes for hierarchical
    suffixes. ``urllib.parse.quote`` with ``safe='/'`` keeps slashes literal
    and still escapes the dangerous characters.
    """
    if "/" not in doi:
        return urllib.parse.quote(doi, safe="")
    prefix, _, suffix = doi.partition("/")
    return f"{urllib.parse.quote(prefix, safe='')}/{urllib.parse.quote(suffix, safe='/')}"


def resolve_doi(
    doi: str,
    *,
    timeout: float = _DEFAULT_TIMEOUT,
) -> tuple[bool, Optional[str], bool]:
    """HEAD-check a DOI against Crossref then DataCite.

    Returns
    -------
    (resolved, registry, rate_limited)
        ``resolved`` is True iff the DOI returned 2xx from either registry
        (no redirects followed). ``registry`` is ``"crossref"`` or
        ``"datacite"`` on success, ``None`` on failure. ``rate_limited`` is
        True if ANY registry returned 429 — callers should refrain from
        caching the result, since a transient rate-limit incident would
        otherwise poison the cache for the full negative-TTL.

    Exception handling
    ------------------
    Network/transport errors and OS errors are treated as a failed registry
    attempt and we fall through to the next one. Unexpected exceptions
    (TypeError, AttributeError, programmer bugs) propagate so they remain
    visible in tracebacks instead of being silently dropped to unresolved.
    """
    if not HAS_HTTPX:
        return (False, None, False)

    encoded = _encode_doi(doi.strip())
    client = _get_client()
    rate_limited = False

    for registry, url_template in (
        ("crossref", _CROSSREF_URL),
        ("datacite", _DATACITE_URL),
    ):
        url = url_template.format(doi=encoded)
        try:
            r = client.head(url, timeout=timeout)
        except (httpx.HTTPError, httpx.InvalidURL, OSError):
            continue
        if 200 <= r.status_code < 300:
            return (True, registry, rate_limited)
        # 429 = rate-limited. Note it and try the other registry, but do
        # NOT let the cache write below treat the final negative as
        # authoritative — a registry-wide throttling event would otherwise
        # block REPLICATED for 24 h after recovery.
        if r.status_code == 429:
            rate_limited = True
            continue

    return (False, None, rate_limited)


def _is_fresh(last_checked_at: str, resolved: bool) -> bool:
    """Return True if a cache entry is still within its TTL.

    Tolerates ``Z`` UTC suffix in addition to ``+00:00`` — Python 3.10's
    ``datetime.fromisoformat`` doesn't parse the ``Z`` form (3.11+ does),
    so an external tool that admin-loads a cache row with a ``Z``-suffixed
    timestamp would otherwise silently fail the parse and look expired.
    """
    if isinstance(last_checked_at, str) and last_checked_at.endswith("Z"):
        last_checked_at = last_checked_at[:-1] + "+00:00"
    try:
        ts = datetime.fromisoformat(last_checked_at)
    except (ValueError, TypeError):
        return False
    ttl = _TTL_RESOLVED if resolved else _TTL_UNRESOLVED
    return (_utcnow() - ts) < ttl


def resolve_dois_with_cache(
    conn: sqlite3.Connection,
    dois: list[str],
    *,
    timeout: float = _DEFAULT_TIMEOUT,
    force: bool = False,
) -> dict[str, bool]:
    """Resolve a list of DOIs using the ``doi_cache`` table.

    Returns a dict mapping each DOI to its resolved status. Cache hits
    within TTL avoid network calls; expired entries or ``force=True``
    re-resolve and overwrite the cache.

    Parameters
    ----------
    force:
        If True, ignore cache and re-resolve every DOI. Used by
        ``refresh_unresolved()`` for explicit retry semantics.

    Best-effort: cache failures do not crash; resolution still proceeds.
    """
    results: dict[str, bool] = {}
    for doi in dois:
        if not force:
            cached = conn.execute(
                "SELECT resolved, last_checked_at FROM doi_cache WHERE doi = ?",
                (doi,),
            ).fetchone()
            if cached is not None and _is_fresh(
                cached["last_checked_at"], bool(cached["resolved"])
            ):
                results[doi] = bool(cached["resolved"])
                continue

        resolved, registry, rate_limited = resolve_doi(doi, timeout=timeout)

        # Don't persist a rate-limited failure: a registry-wide throttling
        # event would otherwise pin the DOI as unresolved for the full
        # negative-TTL, blocking REPLICATED promotion after recovery.
        if not resolved and rate_limited:
            results[doi] = resolved
            continue

        try:
            conn.execute(
                "INSERT OR REPLACE INTO doi_cache "
                "(doi, resolved, registry, last_checked_at) "
                "VALUES (?, ?, ?, ?)",
                (doi, 1 if resolved else 0, registry, _utcnow_iso()),
            )
            conn.commit()
        except (sqlite3.OperationalError, sqlite3.IntegrityError):
            pass  # Caching is best-effort.

        results[doi] = resolved

    return results


def clear_unresolved_cache(conn: sqlite3.Connection) -> list[str]:
    """Delete cache entries for unresolved DOIs. Returns the list cleared.

    Retained for callers that want explicit cache invalidation. The
    ``refresh_unresolved()`` path no longer uses it (``force=True`` on
    ``resolve_dois_with_cache`` is per-DOI and avoids the thundering herd).
    """
    try:
        rows = conn.execute(
            "SELECT doi FROM doi_cache WHERE resolved = 0"
        ).fetchall()
        cleared = [r["doi"] for r in rows]
        conn.execute("DELETE FROM doi_cache WHERE resolved = 0")
        conn.commit()
        return cleared
    except sqlite3.OperationalError:
        return []
