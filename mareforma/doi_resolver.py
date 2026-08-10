"""
doi_resolver.py: DOI format helpers.

Detects DOIs in claim ``supports[]`` / ``contradicts[]`` so they can be
classified apart from local claim ids. Format-only: no network calls,
no registry lookups, no cache.
"""

from __future__ import annotations

import re


# End-anchored with \Z (not $, which would let a trailing newline
# through) and whitespace-free. The DOI Handbook allows printable
# Unicode in the suffix, including spaces, so \S+ is deliberately
# stricter than the spec: a string carrying prose or a second line is
# not the form the ``doi`` tag names.
_DOI_PATTERN = re.compile(r"^10\.\d{4,}/\S+\Z")

# DOIs have no length limit in the spec; real ones are far under this.
_DOI_MAX_LEN = 256


def is_doi(s: str) -> bool:
    """Return True if string is exactly ``10.<registrant>/<suffix>``.

    The whole string is tested as given. Surrounding whitespace is not
    trimmed, so a padded value classifies as external rather than
    reaching a consumer that builds ``https://doi.org/<value>``.
    """
    return len(s) <= _DOI_MAX_LEN and bool(_DOI_PATTERN.match(s))
