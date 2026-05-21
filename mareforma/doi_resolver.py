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
    "mareforma/0.3.1 (+https://github.com/mareforma/mareforma; "
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


def _extract_metadata_subset(
    metadata: dict, registry: str | None,
) -> dict | None:
    """Project Crossref OR DataCite metadata onto the drift-stable subset.

    Crossref and DataCite use different field names for the same data;
    a single extractor would silently produce empty subsets for one of
    the registries. The ``registry`` arg selects the right shape.
    When unspecified, both shapes are tried with Crossref first.

    Returns ``None`` when every extracted field is empty/None —
    refuses to seed an effectively-empty digest that would collide
    with every other empty-metadata DOI.
    """
    import unicodedata

    title: str | None = None
    year: object = None
    container: str | None = None
    authors: list[str] = []

    def _crossref(m: dict) -> None:
        nonlocal title, year, container, authors
        t = m.get("title")
        if isinstance(t, list) and t:
            t = t[0]
        if isinstance(t, str):
            title = unicodedata.normalize("NFC", t)
        issued = m.get("issued") or m.get("published")
        if isinstance(issued, dict):
            parts = issued.get("date-parts")
            if isinstance(parts, list) and parts and isinstance(parts[0], list):
                year = parts[0][0] if parts[0] else None
        c = m.get("container-title")
        if isinstance(c, list) and c:
            c = c[0]
        if isinstance(c, str):
            container = unicodedata.normalize("NFC", c)
        raw = m.get("author") or []
        if isinstance(raw, list):
            authors = [
                unicodedata.normalize("NFC", a["family"])
                for a in raw
                if isinstance(a, dict) and isinstance(a.get("family"), str)
            ]

    def _datacite(m: dict) -> None:
        nonlocal title, year, container, authors
        titles = m.get("titles")
        if isinstance(titles, list) and titles and isinstance(titles[0], dict):
            t = titles[0].get("title")
            if isinstance(t, str):
                title = unicodedata.normalize("NFC", t)
        py = m.get("publicationYear")
        if isinstance(py, (int, str)):
            try:
                year = int(py)
            except (TypeError, ValueError):
                year = None
        c = m.get("container")
        if isinstance(c, dict):
            ct = c.get("title")
            if isinstance(ct, str):
                container = unicodedata.normalize("NFC", ct)
        creators = m.get("creators")
        if isinstance(creators, list):
            authors = [
                unicodedata.normalize("NFC", a["familyName"])
                for a in creators
                if isinstance(a, dict) and isinstance(a.get("familyName"), str)
            ]

    if registry == "crossref":
        _crossref(metadata)
    elif registry == "datacite":
        _datacite(metadata)
    else:
        _crossref(metadata)
        if not title and not authors:
            _datacite(metadata)

    # Collapse empty strings to None so "" doesn't bypass the empty
    # check below.
    if title == "":
        title = None
    if container == "":
        container = None

    subset = {
        "title": title,
        "year": year,
        "container_title": container,
        "authors": authors,
    }
    # Refuse to seed when every field came back empty — those rows
    # would collide with every other empty-metadata DOI and produce a
    # single useless digest. Let the caller try again next pass.
    if title is None and year is None and container is None and not authors:
        return None
    return subset


def _compute_content_digest(
    metadata: dict | None,
    registry: str | None = None,
) -> str | None:
    """SHA-256 hex of a canonical subset of resolver metadata.

    Hashes a stable subset (title + year + container-title + author
    family names) so drift detection catches post-publication
    corrections / retractions without firing on benign churn (abstract
    edits, license updates, indexed-by changes).

    ``registry`` selects the field shape (``"crossref"`` or
    ``"datacite"``); pass ``None`` to try both. Returns ``None`` when
    the input is non-dict or the extracted subset is entirely empty —
    callers should treat that as "fetch indeterminate, try again".

    Strings are NFC-normalised before hashing so a registry that
    flips between NFC and NFD encoding for the same author name does
    not register as drift.
    """
    import hashlib
    import json as _json

    if not isinstance(metadata, dict):
        return None
    subset = _extract_metadata_subset(metadata, registry)
    if subset is None:
        return None
    canonical = _json.dumps(
        subset, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def fetch_doi_metadata(
    doi: str,
    *,
    timeout: float = _DEFAULT_TIMEOUT,
    registry: str | None = None,
) -> tuple[dict | None, str | None, bool]:
    """Fetch full metadata for *doi* from a specific registry (or both).

    Used by :func:`find_drifted_dois` to recompute the content digest
    of a previously-cached DOI. When ``registry`` is given (``"crossref"``
    or ``"datacite"``), only that registry is contacted — pin to the
    registry where the row was originally resolved to avoid producing
    false drift when Crossref blips and DataCite has the same DOI in a
    different shape. When ``None``, Crossref is tried first, then
    DataCite.

    Returns ``(metadata, registry_hit, rate_limited)``:
      * ``metadata`` is the parsed JSON body's ``message`` (Crossref
        shape) or ``data.attributes`` (DataCite shape), or ``None`` on
        any failure / non-2xx.
      * ``registry_hit`` is the registry that returned the metadata
        (``"crossref"`` / ``"datacite"``), or ``None`` on failure.
      * ``rate_limited`` is True if any registry returned 429.
        Callers MUST honour this by backing off and not iterating
        through additional DOIs in the same pass.
    """
    if not HAS_HTTPX:
        return (None, None, False)
    encoded = _encode_doi(doi.strip())
    client = _get_client()
    rate_limited = False
    candidates: list[tuple[str, str]]
    if registry == "crossref":
        candidates = [("crossref", _CROSSREF_URL)]
    elif registry == "datacite":
        candidates = [("datacite", _DATACITE_URL)]
    else:
        candidates = [
            ("crossref", _CROSSREF_URL),
            ("datacite", _DATACITE_URL),
        ]
    for reg, url_template in candidates:
        url = url_template.format(doi=encoded)
        try:
            r = client.get(url, timeout=timeout)
        except (httpx.HTTPError, httpx.InvalidURL, OSError):
            continue
        if r.status_code == 429:
            rate_limited = True
            continue
        if not (200 <= r.status_code < 300):
            continue
        try:
            body = r.json()
        except (ValueError, AttributeError):
            continue
        if reg == "crossref" and isinstance(body, dict):
            msg = body.get("message")
            if isinstance(msg, dict):
                return (msg, "crossref", rate_limited)
        if reg == "datacite" and isinstance(body, dict):
            data = body.get("data")
            if isinstance(data, dict):
                attrs = data.get("attributes")
                if isinstance(attrs, dict):
                    return (attrs, "datacite", rate_limited)
    return (None, None, rate_limited)


# Default cap on how many DOIs find_drifted_dois inspects per call.
# Two GETs per DOI (plus Crossref polite-pool guidance of ~50 req/sec)
# means an unbounded walk on a 10k-DOI graph would blow past the
# polite-pool ceiling and earn an IP ban. Bounded passes let the
# operator iterate by calling repeatedly with backoff between calls.
_DEFAULT_DRIFT_LIMIT = 100


def find_drifted_dois(
    conn: sqlite3.Connection,
    *,
    timeout: float = _DEFAULT_TIMEOUT,
    limit: int | None = None,
) -> list[dict]:
    """Walk the doi_cache and report DOIs whose metadata has drifted.

    For every cached resolved DOI carrying a stored content_digest,
    re-fetches the registry metadata (pinned to the registry that
    originally resolved the DOI when known), recomputes the digest,
    and appends an entry to the result when the two differ. DOIs that
    have never been digested (legacy rows) are also fetched and
    seeded with the current digest — those count as "first seen", not
    drifted, and are NOT included in the returned list.

    Returns a list of ``{doi, stored_digest, current_digest,
    last_checked_at}`` dicts for DOIs that drifted. Empty list when
    nothing changed, when httpx is unavailable, or when the walk
    aborted on a registry 429.

    The walk is read-and-update only; no row is deleted. A drifted
    digest signals to the operator that the referenced paper's
    metadata has changed (retraction, correction, indexing-host swap).
    Whether to update the cache or flag the citing claim as
    ``unresolved`` is a policy decision left to the caller.

    Rate limit / politeness
    -----------------------
    The walk aborts on the first 429 (rate-limit) response from any
    registry and returns whatever drift was detected before the
    abort. Crossref's polite-pool guidance is ~50 req/sec; with two
    GETs per DOI, this method caps the per-call walk at
    :data:`_DEFAULT_DRIFT_LIMIT` rows when ``limit=None``. Operators
    who want to inspect a larger graph should call repeatedly with
    pacing between calls.

    Parameters
    ----------
    limit
        Optional cap on how many DOIs to inspect this pass. Defaults
        to :data:`_DEFAULT_DRIFT_LIMIT` (100). Use a smaller value
        for faster health-check cycles; pass a larger value at your
        own (rate-limit-burning) risk.
    """
    if not HAS_HTTPX:
        return []
    effective_limit = limit if (limit is not None and limit > 0) else _DEFAULT_DRIFT_LIMIT
    sql = (
        "SELECT doi, registry, content_digest, last_checked_at "
        "FROM doi_cache WHERE resolved = 1 LIMIT ?"
    )
    try:
        rows = conn.execute(sql, (int(effective_limit),)).fetchall()
    except sqlite3.OperationalError:
        return []
    drifted: list[dict] = []
    seeded_any = False
    for row in rows:
        metadata, registry_hit, rate_limited = fetch_doi_metadata(
            row["doi"],
            timeout=timeout,
            registry=row["registry"] if row["registry"] else None,
        )
        if rate_limited:
            # Stop early — continuing would burn through the rate-limit
            # window on the operator's IP. Return what we have so far.
            break
        if metadata is None:
            continue
        current = _compute_content_digest(metadata, registry=registry_hit)
        if current is None:
            continue
        stored = row["content_digest"]
        if stored is None:
            # First-seen seed — accumulate; commit at end of loop.
            try:
                conn.execute(
                    "UPDATE doi_cache SET content_digest = ? WHERE doi = ?",
                    (current, row["doi"]),
                )
                seeded_any = True
            except sqlite3.OperationalError:
                pass
            continue
        if stored != current:
            drifted.append({
                "doi": row["doi"],
                "stored_digest": stored,
                "current_digest": current,
                "last_checked_at": row["last_checked_at"],
            })
    if seeded_any:
        try:
            conn.commit()
        except sqlite3.OperationalError:
            pass
    return drifted


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
