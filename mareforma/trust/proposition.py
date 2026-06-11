"""proposition.py: the unit of sameness (the frozen identity kernel).

A scientific assertion is accountable to a world that exists independently of
the asserter. The unit that is true or false is the *proposition*, its
meaning, not the sentence used to express it, and not the act of asserting
it. The legacy ``claims`` row fuses three separable things: WHAT is asserted
(the proposition), WHO asserted it (the signed claim, which remains the
attestation underneath), and the EVIDENCE it rests on. ``Proposition``
is the first of those, content-addressed, so two agents who assert the same
thing in different words converge on one node and the graph can reason over
meaning instead of byte-identical prose.

Identity (frozen as ``content_id@v1``; changing it requires ``content_id@v2``)
-------------------------------------------------------------------
- ``content_id = sha256(canon(subject, relation, object, scope, direction,
  magnitude))``, the answer. ``magnitude`` participates (increases-by-20% and
  increases-by-80% are different propositions about the world).
- ``frame_id = sha256(canon(subject, relation, object, scope))``, the
  *question*. It drops BOTH ``direction`` and ``magnitude`` (both are part of
  the answer). Two propositions share a frame iff same question; they are
  the
  same proposition iff also same direction and magnitude; they contradict iff
  same frame and contrary directions.
- ``canon`` normalizes every string token by NFC + casefold +
  whitespace-collapse, renders ``scope`` as its key/value tokens, and the
  byte serialization reuses :func:`mareforma._canonical.canonicalize`
  (RFC 8785) so the same logical proposition produces the same bytes on any
  host, in any language, regardless of dict order. This normalization is the
  frozen kernel.

Grounding is opaque: a leaf is whatever token the caller passes. If the caller
grounds a leaf to a CURIE/IRI, mareforma stores and hashes that string like any
other token, it requires no ontology and ships none. Ungrounded (plain-token)
leaves still produce a valid ``content_id`` and pass falsifiability; grounding
only improves cross-agent convergence. Convergence on synonyms is the
ecosystem's job (ontologies) and an explicit, contestable ``same_as``
assertion, not mareforma's.
"""
from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

from mareforma._canonical import canonicalize


class Direction(str, Enum):
    """The sign of the asserted relation, the falsifiable core of a claim.

    Directions come in *contrary sets*: within a set, exactly one can hold of
    the world, so asserting two different ones of the same frame is a
    contradiction. ``UNSPECIFIED`` is the rejection path, a proposition with
    an unspecified direction forbids no observation and is not falsifiable.
    """

    INCREASES = "INCREASES"
    DECREASES = "DECREASES"
    NO_EFFECT = "NO_EFFECT"
    PRESENT = "PRESENT"
    ABSENT = "ABSENT"
    UNSPECIFIED = "UNSPECIFIED"

    @property
    def contrary_set(self) -> frozenset["Direction"]:
        """The mutually-exclusive family this direction belongs to."""
        if self in _MAGNITUDE_FAMILY:
            return _MAGNITUDE_FAMILY
        if self in _PRESENCE_FAMILY:
            return _PRESENCE_FAMILY
        return frozenset({self})

    def contradicts(self, other: "Direction") -> bool:
        """True iff *self* and *other* cannot both hold of the same frame."""
        if Direction.UNSPECIFIED in (self, other):
            return False  # unfalsifiable, no truth-conditional conflict
        if self == other:
            return False
        return other in self.contrary_set


# The mutually-exclusive direction families, hoisted to module level so
# contradicts() (the contradiction-scan hot path) does not rebuild them per call.
_MAGNITUDE_FAMILY: frozenset[Direction] = frozenset(
    {Direction.INCREASES, Direction.DECREASES, Direction.NO_EFFECT}
)
_PRESENCE_FAMILY: frozenset[Direction] = frozenset(
    {Direction.PRESENT, Direction.ABSENT}
)


# The closed set of directions that are valid in a *registered* proposition.
# UNSPECIFIED is deliberately excluded: it is the rejection sentinel, never a
# stored value. The SQL CHECK constraint mirrors this exact set.
REGISTRABLE_DIRECTIONS: frozenset[Direction] = frozenset(
    d for d in Direction if d is not Direction.UNSPECIFIED
)


def normalize_token(s: str) -> str:
    """NFC-normalise, casefold, and collapse whitespace.

    Sameness of meaning must survive cosmetic variation in the surface text
    (casing, padding, internal whitespace runs, Unicode decomposition). We do
    NOT attempt synonym resolution here, that is a judgement for a verdict
    issuer with a domain ontology. We guarantee the weaker, decidable
    property: two tokens that normalise identically ARE the same token.
    """
    return " ".join(unicodedata.normalize("NFC", s).casefold().split())


def _canon_scope(scope: Mapping[str, Any]) -> dict[str, str]:
    """Normalise scope keys and values to tokens.

    Keys that collide after normalisation are refused rather than silently
    dropped, a collision would make ``content_id`` depend on dict-insertion
    order, breaking the byte-stability contract.
    """
    out: dict[str, str] = {}
    for k, v in scope.items():
        nk = normalize_token(str(k))
        if nk in out:
            raise ValueError(
                f"scope keys collide after normalisation to {nk!r}; "
                "distinct keys are required so identity is order-independent"
            )
        out[nk] = normalize_token(str(v))
    return out


@dataclass(frozen=True)
class Proposition:
    """A truth-apt, falsifiable, observer-independent claim about the world.

    Frozen + value-typed: two Propositions built from the same truth
    conditions are ``==`` and share a :meth:`content_id`, so the graph stores
    one node per proposition no matter how many agents assert it.
    """

    subject: str
    relation: str
    object: str
    direction: Direction = Direction.UNSPECIFIED
    # Values may be any token-able type; they are normalised to string tokens
    # (via str()) at identity time, matching to_dict/from_dict which accept Any.
    scope: Mapping[str, Any] = field(default_factory=dict)
    # Optional quantitative refinement of the truth conditions. When present it
    # PARTICIPATES in identity: "increases by 20%" and "increases by 80%" are
    # different propositions, but share a frame (same question).
    magnitude: str | None = None

    def __post_init__(self) -> None:
        for f in ("subject", "relation", "object"):
            v = getattr(self, f)
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"Proposition.{f} must be a non-empty string")
        if not isinstance(self.direction, Direction):
            object.__setattr__(self, "direction", Direction(self.direction))
        if not isinstance(self.scope, Mapping):
            raise ValueError("Proposition.scope must be a mapping")
        if self.magnitude is not None:
            if not isinstance(self.magnitude, str) or not self.magnitude.strip():
                raise ValueError(
                    "Proposition.magnitude, when given, must be a non-empty string"
                )

    # -- falsifiability --------------------------------------------------

    def is_falsifiable(self) -> bool:
        """Popper's demarcation, made operational and decidable.

        Falsifiable iff (a) it commits to a direction, there is an
        observation that would count against it, and (b) it states a scope,
        so the conditions under which it is meant to hold are pinned down. An
        unscoped, directionless assertion forbids no possible observation.
        """
        return self.direction is not Direction.UNSPECIFIED and bool(self.scope)

    # -- identity --------------------------------------------------------

    def _frame_payload(self) -> dict[str, Any]:
        """The frame: everything EXCEPT direction and magnitude."""
        return {
            "subject": normalize_token(self.subject),
            "relation": normalize_token(self.relation),
            "object": normalize_token(self.object),
            "scope": _canon_scope(self.scope),
        }

    def frame_id(self) -> str:
        """Content id of the frame (direction- and magnitude-free).

        Two propositions with the same ``frame_id`` are about the same
        *question*. They are the same proposition iff their direction and
        magnitude also match; they CONTRADICT iff their directions are
        contraries.
        """
        return hashlib.sha256(canonicalize(self._frame_payload())).hexdigest()

    def content_id(self) -> str:
        """The unit of SAMENESS. Same truth conditions ⇒ same id."""
        payload = self._frame_payload()
        payload["direction"] = self.direction.value
        payload["magnitude"] = (
            normalize_token(self.magnitude) if self.magnitude else None
        )
        return hashlib.sha256(canonicalize(payload)).hexdigest()

    # -- relations -------------------------------------------------------

    def same_as(self, other: "Proposition") -> bool:
        return self.content_id() == other.content_id()

    def contradicts(self, other: "Proposition") -> bool:
        """Decidable contradiction: same frame, contrary directions.

        No embedding model, no NLI threshold. If two propositions share a
        frame and their directions cannot both hold of the world, they
        contradict, full stop.
        """
        return self.frame_id() == other.frame_id() and self.direction.contradicts(
            other.direction
        )

    # -- serialisation ---------------------------------------------------

    def text(self) -> str:
        """A human-readable rendering. NOT used for identity."""
        scope = ", ".join(f"{k}={v}" for k, v in sorted(self.scope.items()))
        mag = f" ({self.magnitude})" if self.magnitude else ""
        scope_s = f" [{scope}]" if scope else ""
        return (
            f"{self.subject} {self.relation} {self.direction.value} "
            f"{self.object}{mag}{scope_s}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "subject": self.subject,
            "relation": self.relation,
            "object": self.object,
            "direction": self.direction.value,
            "scope": dict(self.scope),
            "magnitude": self.magnitude,
            "content_id": self.content_id(),
            "frame_id": self.frame_id(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Proposition":
        return cls(
            subject=data["subject"],
            relation=data["relation"],
            object=data["object"],
            direction=Direction(data.get("direction", "UNSPECIFIED")),
            scope=dict(data.get("scope") or {}),
            magnitude=data.get("magnitude"),
        )
