"""Reflective registry of mareforma predicate types.

Adapters (`mareforma_tooluniverse`, `mareforma_gemini`, `mareforma_wet_lab`,
`mareforma_peer_review`, `mareforma_elo`, ...) call :func:`register` at
import time with the URI they ship. The substrate validates URI shape
(`urn:mareforma:predicate:<name>:v<N>`) but does not constrain payload
semantics — adapters own the predicate body shape; this registry only
asserts that the URI is well-formed and visible.

:func:`predicates` returns the full list of registered URIs for
introspection (e.g. an agent that wants to know which adapter packages
are available in the current Python environment without import-walking).

Built-in URIs registered at import:

* ``urn:mareforma:predicate:claim:v1`` — the substrate's default
  single-claim envelope.
* ``urn:mareforma:predicate:epistemic-graph:v1`` — the signed-bundle
  envelope produced by ``mareforma export --bundle``.
* ``urn:mareforma:predicate:claim-with-roles:v1`` — the multi-signature
  variant; the actual writer lives in ``mareforma.signing``, this
  module reserves the URI.

The registry is process-local (a Python ``dict``) and does NOT
persist. The columnar ``predicate_payload`` field on the claims table
is the on-disk projection; the registry is only the URI-shape
contract.
"""

from __future__ import annotations

import re
import threading
from typing import Iterable


__all__ = [
    "register",
    "predicates",
    "is_registered",
    "unregister",
    "PredicateTypeError",
    "BUILTIN_URIS",
]


class PredicateTypeError(ValueError):
    """Raised when a URI fails the predicate-type-shape check.

    Subclass of :class:`ValueError` so existing callers that catch
    ``ValueError`` continue to work; new code that wants to react
    specifically to predicate-type-shape failures can catch this
    narrower class.
    """


# urn:mareforma:predicate:<name>:v<N>
# <name> = lowercase letters, digits, dot, dash, slash, underscore.
# Slash supports the wet-lab-assay/<class> hierarchy (e.g.
# wet-lab-assay/flow-cytometry). <N> = positive integer.
_URI_RE = re.compile(
    r"^urn:mareforma:predicate:[a-z0-9][a-z0-9._/\-]*:v[1-9][0-9]*$"
)


# Built-in URIs the substrate ships with. Adapters MUST NOT
# re-register these; doing so raises ``PredicateTypeError``.
BUILTIN_URIS: tuple[str, ...] = (
    # Core substrate predicates (writers live in the substrate itself).
    "urn:mareforma:predicate:claim:v1",
    "urn:mareforma:predicate:epistemic-graph:v1",
    "urn:mareforma:predicate:claim-with-roles:v1",
    # Reserved namespaces for upstream adapters. The substrate ships
    # no writer for these; an adapter that emits one of these
    # predicateTypes opts into the shape contract documented in the
    # adapter's own docs. Listed here so the substrate's URI registry
    # cannot be poisoned by a third-party module silently
    # re-registering one of these slots.
    "urn:mareforma:predicate:tool-call:v1",
    "urn:mareforma:predicate:ingested-trace:v1",
    "urn:mareforma:predicate:agent-trace:v1",
    "urn:mareforma:predicate:llm-output:v1",
    "urn:mareforma:predicate:review:v1",
    "urn:mareforma:predicate:peer-review:v1",
    "urn:mareforma:predicate:elo-match:v1",
    "urn:mareforma:predicate:tournament-bracket:v1",
    "urn:mareforma:predicate:wet-lab-assay/flow-cytometry:v1",
    "urn:mareforma:predicate:wet-lab-assay/sequencing:v1",
    "urn:mareforma:predicate:wet-lab-assay/imaging:v1",
    "urn:mareforma:predicate:wet-lab-assay/proteomics:v1",
    "urn:mareforma:predicate:wet-lab-assay/electrophysiology:v1",
    "urn:mareforma:predicate:replication-attestation:v1",
    "urn:mareforma:predicate:compounding-attestation:v1",
    "urn:mareforma:predicate:semantic-grounding:v1",
    "urn:mareforma:predicate:doi-resolution:v1",
)


_lock = threading.Lock()
_registry: dict[str, str | None] = {}


def _validate_uri(uri: str) -> None:
    if not isinstance(uri, str):
        raise PredicateTypeError(
            f"predicate URI must be str, got {type(uri).__name__}"
        )
    if not _URI_RE.match(uri):
        raise PredicateTypeError(
            f"Invalid predicate URI shape: {uri!r}. Expected "
            "urn:mareforma:predicate:<name>:v<N> where <name> uses "
            "[a-z0-9._/-] and <N> is a positive integer."
        )


def register(uri: str, owner: str | None = None) -> None:
    """Register a predicate URI.

    ``owner`` is an optional human-readable hint about the adapter that
    ships this URI (e.g. ``"mareforma_tooluniverse 0.1.0"``). Stored
    only for introspection; the substrate does not act on it.

    Re-registering an already-known URI with the same ``owner`` is a
    no-op. Re-registering with a different ``owner`` raises
    :class:`PredicateTypeError` so two adapters cannot silently claim
    the same URI.

    Built-in URIs (:data:`BUILTIN_URIS`) are reserved and cannot be
    re-registered.
    """
    _validate_uri(uri)
    with _lock:
        if uri in BUILTIN_URIS and uri in _registry:
            existing = _registry[uri]
            if owner is not None and owner != existing:
                raise PredicateTypeError(
                    f"Cannot re-register built-in URI {uri!r} with "
                    f"owner={owner!r} (substrate owns this URI)"
                )
            return
        if uri in _registry:
            existing = _registry[uri]
            if existing is not None and owner is not None and existing != owner:
                raise PredicateTypeError(
                    f"URI {uri!r} already registered by {existing!r}; "
                    f"refusing re-register by {owner!r}"
                )
            return
        _registry[uri] = owner


def predicates() -> list[str]:
    """Return all registered predicate URIs, sorted for determinism."""
    with _lock:
        return sorted(_registry)


def is_registered(uri: str) -> bool:
    """Return True if ``uri`` has been registered."""
    with _lock:
        return uri in _registry


def unregister(uri: str) -> None:
    """Remove a URI from the registry.

    Refuses to remove built-in URIs. Intended for test setup/teardown,
    not production use — adapters that go out of scope generally leave
    their URIs registered.
    """
    if uri in BUILTIN_URIS:
        raise PredicateTypeError(
            f"Cannot unregister built-in URI {uri!r}"
        )
    with _lock:
        _registry.pop(uri, None)


def _seed_builtins(uris: Iterable[str] = BUILTIN_URIS) -> None:
    with _lock:
        for uri in uris:
            _registry.setdefault(uri, "mareforma (built-in)")


_seed_builtins()
