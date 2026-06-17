"""Canonicalization: byte-stable forms for arbitrary Python values.

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
import threading
from typing import Any, Callable

import rfc8785


__all__ = [
    "CanonicalizationError",
    "DEFAULT_CANONICALIZER",
    "DSSE_JCS_NFC_V1",
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

# NFC-normalising JCS — mareforma's signed-envelope canonicalizer
# exposed under a registered name so adapters that need the same
# byte-for-byte form as the envelope layer can opt in by form name
# instead of importing the private mareforma._canonical module.
DSSE_JCS_NFC_V1 = "dsse-jcs-nfc-v1"


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

# Lock guards _REGISTRY against the dictionary-changed-size-during-
# iteration race: canonicalize() formats _REGISTRY.keys() inside its
# KeyError path while register_canonicalizer() may be mutating the
# dict from another thread.
_LOCK = threading.Lock()


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
    with _LOCK:
        _REGISTRY[name] = fn


def registered_canonicalizers() -> tuple[str, ...]:
    """Return the registered canonicalizer names in registration order."""
    with _LOCK:
        return tuple(_REGISTRY.keys())


def canonicalize(value: Any, *, form: str = DEFAULT_CANONICALIZER) -> bytes:
    """Apply the named canonicalizer to ``value``.

    Raises :class:`CanonicalizationError` when ``form`` is not
    registered (the message lists the registered names).
    """
    with _LOCK:
        fn = _REGISTRY.get(form)
        known = tuple(_REGISTRY.keys()) if fn is None else ()
    if fn is None:
        raise CanonicalizationError(
            f"unknown canonicalizer form {form!r} "
            f"(registered: {', '.join(known)})"
        )
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


# Register mareforma's NFC-normalising envelope canonicalizer as a
# named form so adapters that need the SAME bytes the envelope layer
# signs can opt in by form name. Delegates to mareforma._canonical
# (the canonical envelope canonicaliser) — keeps a single
# implementation; this is just an alias under a registered name.
def _canonicalize_dsse_jcs_nfc_v1(value: Any) -> bytes:
    """NFC-normalising JCS: same bytes as the signed-envelope layer."""
    from mareforma._canonical import canonicalize as _envelope_canonicalize
    return _envelope_canonicalize(value)


register_canonicalizer(DSSE_JCS_NFC_V1, _canonicalize_dsse_jcs_nfc_v1)


# Auto-import specialty canonicalizers so the documented forms
# (rdkit-canonical-smiles-v1, fasta-nfc-v1, pdb-atom-sorted-v1) are
# available the moment `mareforma.canonicalize` is imported. Without
# this the docstring promise — "specialty canonicalizers register
# themselves" — is false: users have to discover the submodule import.
from mareforma.canonicalize import specialty as _specialty  # noqa: E402,F401
