"""Canonicalization — byte-stable forms for arbitrary Python values.

The default canonicalizer (``json-c14n-v1``) is JCS (RFC 8785) via the
``rfc8785`` package: a number's encoding does not depend on whether it
came in as ``1`` or ``1.0``, non-finite floats are rejected, dict
keys are sorted, no platform-dependent ``e+``/``e-`` variance.

Specialty canonicalizers register themselves on first import of
:mod:`mareforma.canonicalize.specialty` and cover RDKit canonical
SMILES (with NFC fallback when ``rdkit`` is unavailable), FASTA
sequence NFC + uppercase + strip, and PDB ATOM/HETATM serial-sorted.
"""

from __future__ import annotations

import hashlib
import math
from typing import Any, Callable

import rfc8785


__all__ = [
    "CanonicalizationError",
    "DEFAULT_CANONICALIZER",
    "canonicalize",
    "canonicalize_default",
    "digest_bytes",
    "fingerprint_tool_config",
    "register_canonicalizer",
    "registered_canonicalizers",
]


class CanonicalizationError(ValueError):
    """Raised when a value cannot be canonicalized to byte-stable form."""


DEFAULT_CANONICALIZER = "json-c14n-v1"


def canonicalize_default(value: Any) -> bytes:
    """Default JCS-shaped canonicalizer.

    Non-finite floats (``NaN``, ``Inf``) have no canonical JSON form
    under RFC 8785; this raises :class:`CanonicalizationError` with a
    diagnostic pointer rather than the bare ``rfc8785`` error.
    """
    _check_finite(value)
    try:
        return rfc8785.dumps(value)
    except rfc8785.CanonicalizationError as exc:
        raise CanonicalizationError(str(exc)) from exc


def _check_finite(value: Any) -> None:
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalizationError(
                f"non-finite floats are not byte-stable; values must be "
                f"finite (got {value!r})"
            )
    elif isinstance(value, dict):
        for v in value.values():
            _check_finite(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            _check_finite(v)


_REGISTRY: dict[str, Callable[[Any], bytes]] = {
    DEFAULT_CANONICALIZER: canonicalize_default,
}


def register_canonicalizer(name: str, fn: Callable[[Any], bytes]) -> None:
    """Register a specialty canonicalizer under ``name``.

    Names must be non-empty and contain only ASCII letters, digits,
    hyphens, or underscores. The name ends up in a claim's
    ``result_canonical_form`` field so replay can pick the matching
    canonicalizer.
    """
    if not name or not name.replace("-", "").replace("_", "").isalnum():
        raise ValueError(
            "canonicalizer name must be non-empty kebab-case or "
            "underscored alphanumeric"
        )
    _REGISTRY[name] = fn


def registered_canonicalizers() -> tuple[str, ...]:
    """Return the registered canonicalizer names in registration order."""
    return tuple(_REGISTRY.keys())


def canonicalize(value: Any, *, form: str = DEFAULT_CANONICALIZER) -> bytes:
    """Apply the named canonicalizer to ``value``.

    Raises :class:`CanonicalizationError` when ``form`` is not
    registered (the message lists the registered names).
    """
    try:
        fn = _REGISTRY[form]
    except KeyError as exc:
        raise CanonicalizationError(
            f"unknown canonicalizer form {form!r} "
            f"(registered: {', '.join(_REGISTRY.keys())})"
        ) from exc
    return fn(value)


def digest_bytes(b: bytes) -> str:
    """SHA-256 of bytes as 64-char lowercase hex."""
    return hashlib.sha256(b).hexdigest()


def fingerprint_tool_config(config: dict[str, Any]) -> str:
    """Return ``sha256:<hex>`` of the canonicalized tool config.

    Configs that aren't JSON-canonicalizable (e.g. containing callable
    objects) should be summarised to a clean dict before fingerprinting.
    """
    return "sha256:" + digest_bytes(canonicalize(config))
