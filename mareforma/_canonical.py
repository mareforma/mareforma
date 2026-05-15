"""
_canonical.py — RFC 8785 canonical JSON for signed envelopes.

The signed payload of every claim is canonicalized before signing so the
same logical object always produces the same bytes — independent of
Python version, dict-insertion order, language of the verifier, or
whitespace conventions.

Wire format
-----------
Output bytes follow RFC 8785 (JSON Canonicalization Scheme, JCS):

- Keys sorted lexicographically by UTF-16 code unit at every nesting
  level (RFC 8785 §3.2.3).
- ``separators=(",", ":")`` — no whitespace (RFC 8785 §3.2.2).
- UTF-8 output with the minimal JSON string escape set (RFC 8785 §3.2.1).
- Numbers serialized per the ECMAScript ``Number.prototype.toString``
  algorithm (RFC 8785 §3.2.2.3) — ``1.0`` becomes ``1``, ``1e10`` becomes
  ``10000000000``, exponent boundaries follow ES rules. This is the
  load-bearing difference vs. Python's ``json.dumps``: a verifier in
  Go / Rust / JavaScript that re-canonicalizes per RFC 8785 produces
  the same bytes for the same logical payload, including floats.
- NaN / Infinity are rejected (re-raised as ``ValueError``). JSON has
  no representation for these and RFC 8785 explicitly forbids them.

NFC normalization
-----------------
We apply Unicode Normalization Form C to every string *before* passing
the tree to ``rfc8785.dumps``. RFC 8785 does not mandate NFC — the spec
operates on whatever code points the input contains. We add NFC so that
visually-identical text with different code-point decomposition (e.g.
``é`` as U+00E9 vs ``e`` + U+0301) produces the same canonical bytes.
Decoupling NFC from JCS keeps the JCS layer interoperable with any
other RFC 8785 implementation; the NFC layer is a mareforma-internal
discipline that the canonical bytes happen to enjoy as a side effect.

Cross-language verification
---------------------------
Use any RFC 8785 implementation (e.g. ``rfc8785`` in Python,
``github.com/sigsum/sigsum-go/pkg/jcs`` in Go, ``serde_jcs`` in Rust,
``canonicalize`` in JS) to re-derive the bytes a mareforma signature
covers. The signature input is exactly ``rfc8785.dumps(claim_fields)``
after the caller has NFC-normalized strings in the payload.

This module depends only on ``rfc8785`` (added currently) and the
stdlib ``unicodedata``. Used by ``_statement`` for the signed predicate
and by ``signing`` for the DSSE Pre-Authentication Encoding (PAE).
"""
from __future__ import annotations

import unicodedata
from typing import Any

import rfc8785


def _normalize(obj: Any) -> Any:
    """Walk *obj* recursively, NFC-normalize every string, return a new tree.

    Numbers, booleans, and None pass through unchanged. Sequences become
    lists; mappings become dicts. The caller is responsible for refusing
    non-JSON types (functions, classes, etc.) — ``rfc8785.dumps`` will
    raise a domain-specific error on those, which is the right behavior.

    Dict-key NFC collisions
    -----------------------
    A naive ``{_normalize(k): _normalize(v) for k, v in items}`` would
    silently drop one value when two keys normalize to the same string
    (e.g. ``é`` U+00E9 and ``é`` U+0065+U+0301). Insertion order would
    determine which value survives, breaking the "same logical input →
    same bytes" contract under adversarial-but-shaped input. We
    explicitly detect the collision and raise ``ValueError`` so the
    caller fixes the source data instead of producing a non-
    deterministic envelope.
    """
    if isinstance(obj, str):
        return unicodedata.normalize("NFC", obj)
    if isinstance(obj, dict):
        out: dict[Any, Any] = {}
        for k, v in obj.items():
            nk = _normalize(k)
            if nk in out:
                # Show both the normalized form and the original keys
                # so the caller can pinpoint the collision source.
                colliding = [
                    repr(orig) for orig in obj.keys()
                    if isinstance(orig, str)
                    and unicodedata.normalize("NFC", orig) == nk
                ]
                raise ValueError(
                    f"Dict keys collide after NFC normalization to "
                    f"{nk!r}: {', '.join(colliding)}. Canonical JSON "
                    "requires distinct keys; pre-normalize the source "
                    "dict so the surviving value is unambiguous."
                )
            out[nk] = _normalize(v)
        return out
    if isinstance(obj, (list, tuple)):
        return [_normalize(v) for v in obj]
    return obj


def canonicalize(obj: Any) -> bytes:
    """Serialize *obj* to RFC 8785 canonical JSON bytes.

    Output is byte-stable: same input → same bytes, across Python
    versions, dict-insertion orders, and ANY RFC 8785-conformant
    implementation. Used for envelope payloads, ``statement_cid``
    computation, and DSSE PAE construction.

    Raises
    ------
    TypeError
        If *obj* contains a value that is not JSON-serializable
        (e.g. a set, a custom class). ``rfc8785.dumps`` raises a
        descendant of ``TypeError`` or ``rfc8785.CanonicalizationError``;
        we re-raise as ``TypeError`` for caller continuity with the
        previous stdlib-only contract.
    ValueError
        If *obj* contains a float that is NaN or Infinity, or an
        integer outside the IEEE-754 double-precision safe-integer
        range that RFC 8785 forbids in canonical output (JCS verifiers
        in other languages would reject these too), OR if a dict
        contains two keys that NFC-normalize to the same string —
        canonical JSON requires distinct keys and the substrate refuses
        to silently drop one value.
    """
    normalized = _normalize(obj)
    try:
        return rfc8785.dumps(normalized)
    except rfc8785.FloatDomainError as exc:
        raise ValueError(str(exc)) from exc
    except rfc8785.IntegerDomainError as exc:
        raise ValueError(str(exc)) from exc
    except rfc8785.CanonicalizationError as exc:
        # Generic fallback for non-numeric domain errors (unsupported
        # types). Keep the previous-version contract that non-JSON types
        # surface as TypeError, not the implementation's exception class.
        raise TypeError(str(exc)) from exc
