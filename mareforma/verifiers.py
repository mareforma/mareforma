"""Verifier protocol for claim-grounding sensors.

A *grounding sensor* takes a claim's text + its declared upstream
``supports[]`` and produces a numeric score (in ``[0.0, 1.0]``)
estimating how well the claim is grounded in those supports. The
canonical realisation is an NLI (natural-language inference) model that
checks each (support → claim) pair for entailment.

In mareforma the score is computed at *assertion time*, snapshotted
into the signed Statement v1 predicate, and immutable thereafter. A
future re-run of the same or a different verifier may produce a
different score; that recomputed verdict is NOT persisted on the
claim. The signed score is the asserter's verdict at the moment they
made the claim — same posture as the GRADE EvidenceVector.

The substrate does not bundle any model dependencies. Callers wire
their own concrete verifiers; :class:`MockNLIVerifier` is provided as
a reference implementation for tests and for adapter scaffolding.

Protocol contract
-----------------
Implementations must be pure functions (no I/O hidden in __init__).
The substrate calls :meth:`Verifier.grounding_score` synchronously
inside :meth:`mareforma.EpistemicGraph.assert_claim`; long-running
implementations should batch upstream prefetch BEFORE handing the
verifier to the substrate.
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable


__all__ = ["Verifier", "MockNLIVerifier", "VerifierError"]


class VerifierError(Exception):
    """Raised when a verifier returns a structurally-invalid result.

    The substrate catches this exception around the verifier call so
    a broken sensor doesn't take down the assertion path; the claim
    is still asserted, the score is dropped, and the caller sees a
    warning in the WARNING stream.
    """


@runtime_checkable
class Verifier(Protocol):
    """Protocol for claim-grounding sensors.

    A conforming implementation accepts the claim text + the list of
    supports references (UUIDs of local claims or external strings
    such as DOIs) and returns a ``(score, rationale)`` tuple. The
    substrate is opinion-free about WHAT the verifier checks — it
    only enforces the return shape.
    """

    def grounding_score(
        self,
        claim_text: str,
        supports: Sequence[str],
    ) -> tuple[float, str]:
        """Return ``(score, rationale)``.

        ``score`` must be a float in ``[0.0, 1.0]`` where ``1.0`` is
        "fully grounded" (every support entails the claim) and
        ``0.0`` is "ungrounded" (no support entails the claim).
        ``rationale`` is a short human-readable string explaining the
        score; it lands inside the signed payload alongside the
        score.

        Raises
        ------
        VerifierError
            If the implementation cannot produce a result (model
            failure, malformed input).
        """
        ...


def _validate_score(score: float) -> float:
    """Coerce + validate a verifier score to [0.0, 1.0]."""
    try:
        s = float(score)
    except (TypeError, ValueError) as exc:
        raise VerifierError(
            f"grounding_score must be a float; got {score!r}"
        ) from exc
    if s != s:  # NaN
        raise VerifierError("grounding_score must not be NaN")
    if s < 0.0 or s > 1.0:
        raise VerifierError(
            f"grounding_score={s} out of [0.0, 1.0]"
        )
    return s


class MockNLIVerifier:
    """Deterministic stub verifier for tests and adapter scaffolding.

    Returns a fixed score for every call. Useful for exercising the
    substrate's grounding_sensor plumbing without bundling a real NLI
    model. Production adapters should ship a real verifier (e.g.
    sentence-transformers + an entailment model) and pass it via
    ``assert_claim(grounding_sensor=...)``.

    Parameters
    ----------
    score
        Fixed score this verifier returns (in ``[0.0, 1.0]``).
    rationale
        Fixed rationale string returned alongside the score.
    """

    def __init__(
        self,
        score: float = 1.0,
        *,
        rationale: str = "MockNLIVerifier: deterministic test stub",
    ) -> None:
        self._score = _validate_score(score)
        self._rationale = rationale

    def grounding_score(
        self,
        claim_text: str,
        supports: Sequence[str],
    ) -> tuple[float, str]:
        # No-op: the stub does not actually inspect the inputs.
        return (self._score, self._rationale)
