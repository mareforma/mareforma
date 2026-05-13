"""
_canonical.py — canonical JSON serialization for signed envelopes.

The signed payload of every claim is canonicalized before signing so the
same logical object always produces the same bytes — independent of
Python version, dict-insertion order, or whitespace conventions.

Rules
-----
- Keys sorted lexicographically at every nesting level.
- ``separators=(",", ":")`` — no whitespace.
- ``ensure_ascii=False`` — emit raw UTF-8 (the on-disk bytes are the same
  bytes that get signed; ASCII-only escaping inflates size and obscures
  diffs in claims.toml).
- ``allow_nan=False`` — NaN / Infinity are not valid JSON. A claim
  carrying a NaN p_value would otherwise produce non-portable output
  that some verifiers accept and others reject.
- Strings are NFC-normalized (Unicode Normalization Form C) before
  serialization so visually-identical text with different code-point
  decomposition produces the same canonical bytes.

This module is intentionally dependency-free: stdlib only. Used by
``_statement`` and by ``signing`` for the DSSE Pre-Authentication
Encoding (PAE).
"""
from __future__ import annotations

import json
import unicodedata
from typing import Any


def _normalize(obj: Any) -> Any:
    """Walk *obj* recursively, NFC-normalize every string, return a new tree.

    Numbers, booleans, and None pass through unchanged. Sequences become
    lists; mappings become dicts. The caller is responsible for refusing
    non-JSON types (functions, classes, etc.) — ``json.dumps`` will raise
    ``TypeError`` on those, which is the right behavior.
    """
    if isinstance(obj, str):
        return unicodedata.normalize("NFC", obj)
    if isinstance(obj, dict):
        return {_normalize(k): _normalize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_normalize(v) for v in obj]
    return obj


def canonicalize(obj: Any) -> bytes:
    """Serialize *obj* to canonical JSON bytes.

    Output is byte-stable: same input → same bytes, across Python versions
    and dict-insertion orders. Used for envelope payloads, statement_cid
    computation, and DSSE PAE construction.

    Raises
    ------
    TypeError
        If *obj* contains a value that ``json.dumps`` cannot serialize
        (e.g. a set, a custom class without ``__dict__``).
    ValueError
        If *obj* contains a float that is NaN or Infinity. JSON has no
        canonical representation for these, and accepting them would
        produce envelope bytes that some verifiers reject. Callers must
        convert non-finite floats to strings or omit them.
    """
    normalized = _normalize(obj)
    return json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
