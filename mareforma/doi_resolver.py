"""
doi_resolver.py: DOI format helpers.

Detects DOIs in claim ``supports[]`` / ``contradicts[]`` so they can be
classified apart from local claim ids. Format-only: no network calls,
no registry lookups, no cache.
"""

from __future__ import annotations

import re


_DOI_PATTERN = re.compile(r"^10\.\d{4,}/.+")


def is_doi(s: str) -> bool:
    """Return True if string matches DOI format ``10.<registrant>/<suffix>``."""
    return bool(_DOI_PATTERN.match(s.strip()))


def extract_dois(values: list[str]) -> list[str]:
    """Filter a list to only DOIs, stripping surrounding whitespace."""
    return [v.strip() for v in values if is_doi(v)]
