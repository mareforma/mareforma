"""
doi_resolver.py — DOI resolution via Crossref and DataCite.

DOIs in claim ``supports[]`` and ``contradicts[]`` are HEAD-checked against
public registries at assertion time. Unresolved DOIs mark the claim as
``unresolved=True``, blocking promotion to REPLICATED.

Cache
-----
Results are persisted to the ``doi_cache`` table to avoid repeated network
calls. Resolved DOIs cache permanently; unresolved entries can be re-checked
via ``EpistemicGraph.refresh_unresolved()``.

Behavior
--------
- DOI format check (``10.\\d+/...``) before any network call.
- Try Crossref first, fall back to DataCite.
- On any HTTP error or timeout, the DOI is treated as unresolved.
- Resolution is fail-closed at the claim level: any unresolved DOI in
  ``supports[]`` or ``contradicts[]`` sets ``claim.unresolved=True``.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
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


def is_doi(s: str) -> bool:
    """Return True if string matches DOI format ``10.<registrant>/<suffix>``."""
    return bool(_DOI_PATTERN.match(s))


def extract_dois(values: list[str]) -> list[str]:
    """Filter a list to only DOIs."""
    return [v for v in values if is_doi(v)]


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_doi(
    doi: str,
    *,
    timeout: float = _DEFAULT_TIMEOUT,
) -> tuple[bool, Optional[str]]:
    """HEAD-check a DOI against Crossref then DataCite.

    Returns
    -------
    (resolved, registry)
        ``resolved`` is True if the DOI returned 200 from either registry.
        ``registry`` is ``"crossref"`` or ``"datacite"`` on success, ``None``
        on failure.
    """
    if not HAS_HTTPX:
        return (False, None)

    for registry, url in [
        ("crossref", _CROSSREF_URL.format(doi=doi)),
        ("datacite", _DATACITE_URL.format(doi=doi)),
    ]:
        try:
            r = httpx.head(url, timeout=timeout, follow_redirects=True)
            if r.status_code == 200:
                return (True, registry)
        except httpx.HTTPError:
            continue

    return (False, None)


def resolve_dois_with_cache(
    conn: sqlite3.Connection,
    dois: list[str],
    *,
    timeout: float = _DEFAULT_TIMEOUT,
) -> dict[str, bool]:
    """Resolve a list of DOIs using the ``doi_cache`` table.

    Returns a dict mapping each DOI to its resolved status. Cache hits
    avoid network calls. Misses trigger a resolution and update the cache.

    Best-effort: cache failures do not crash; resolution still proceeds.
    """
    results: dict[str, bool] = {}
    for doi in dois:
        cached = conn.execute(
            "SELECT resolved FROM doi_cache WHERE doi = ?",
            (doi,),
        ).fetchone()
        if cached is not None:
            results[doi] = bool(cached["resolved"])
            continue

        resolved, registry = resolve_doi(doi, timeout=timeout)

        try:
            conn.execute(
                "INSERT OR REPLACE INTO doi_cache "
                "(doi, resolved, registry, last_checked_at) "
                "VALUES (?, ?, ?, ?)",
                (doi, 1 if resolved else 0, registry, _utcnow_iso()),
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass  # Caching is best-effort.

        results[doi] = resolved

    return results


def clear_unresolved_cache(conn: sqlite3.Connection) -> list[str]:
    """Delete cache entries for unresolved DOIs. Returns the list cleared."""
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
