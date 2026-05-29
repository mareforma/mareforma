"""Gemini-for-Science adapter — read-only ingest of public outputs.

Gemini for Science (AlphaEvolve+ERA, Co-Scientist, NotebookLM,
Antigravity Science Skills) is a closed platform with no upstream
cooperation path: mareforma can read public outputs, sign them as
inferred claims, and let downstream agents reason about them.

The v0.3.3 surface is intentionally minimal:

- :class:`OutputIngester` — accepts a single Gemini output dict
  (capability + payload) and asserts one signed mareforma claim.
- URI constants — re-exported from :mod:`mareforma.predicate_types`
  for ergonomic import:

  * ``CODE_VARIATION_V1`` (AlphaEvolve+ERA)
  * ``HYPOTHESIS_V1`` (Co-Scientist debate / argument synthesis)
  * ``LITERATURE_INSIGHT_V1`` (NotebookLM cells)
  * ``SCIENCE_SKILL_V1`` (Antigravity Science Skills database calls)

Full surface-specific adapters (per-cell NotebookLM ingest, full
Co-Scientist debate transcripts, AlphaEvolve population trees) are
queued for v0.3.4+ when adoption signal warrants the surface area.

Install: ``pip install mareforma[gemini]``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mareforma.predicate_types import (
    CODE_VARIATION_V1,
    HYPOTHESIS_V1,
    LITERATURE_INSIGHT_V1,
    SCIENCE_SKILL_V1,
)


if TYPE_CHECKING:
    from mareforma._graph import EpistemicGraph


__all__ = [
    "CODE_VARIATION_V1",
    "HYPOTHESIS_V1",
    "LITERATURE_INSIGHT_V1",
    "SCIENCE_SKILL_V1",
    "OutputIngester",
    "SUPPORTED_CAPABILITIES",
]


# Capability → URI table. Used at ingest time to validate the caller
# passed a recognised capability and to pick the predicate URI for the
# resulting claim.
SUPPORTED_CAPABILITIES: dict[str, str] = {
    "code-variation": CODE_VARIATION_V1,
    "hypothesis": HYPOTHESIS_V1,
    "literature-insight": LITERATURE_INSIGHT_V1,
    "science-skill": SCIENCE_SKILL_V1,
}


class OutputIngester:
    """Read-only ingest of Gemini-for-Science capability outputs.

    Each ``ingest()`` call produces one signed mareforma claim with a
    predicate URI matching the capability. Calls are INFERRED by
    default — a Gemini output is a single source's claim, not a
    cross-host replication; downstream code is responsible for
    promoting findings that converge with claims from other sources.
    """

    def __init__(self, *, graph: "EpistemicGraph | None" = None) -> None:
        self._graph = graph

    # ------------------------------------------------------------------
    # Convention helpers (used by the coexistence test)
    # ------------------------------------------------------------------

    def predicate_uris(self) -> tuple[str, ...]:
        """Return the URIs this ingester may emit on the graph."""
        return tuple(SUPPORTED_CAPABILITIES.values())

    def emit_sample(self) -> str:
        """Emit one synthetic literature-insight claim and return its id.

        Convention helper for the cross-adapter coexistence test.
        Real callers use :meth:`ingest` with actual Gemini output.
        """
        if self._graph is None:
            raise RuntimeError(
                "emit_sample() needs the OutputIngester to be "
                "constructed with a graph=... argument"
            )
        return self.ingest(
            capability="literature-insight",
            payload={"sample": True, "summary": "Gemini literature-insight (sample)"},
            generated_by="adapter:gemini",
        )

    # ------------------------------------------------------------------
    # Real ingest path
    # ------------------------------------------------------------------

    def ingest(
        self,
        *,
        capability: str,
        payload: dict[str, Any],
        generated_by: str = "adapter:gemini",
        supports: list[str] | None = None,
    ) -> str:
        """Assert one INFERRED claim from a Gemini output payload.

        ``capability`` must be one of :data:`SUPPORTED_CAPABILITIES`
        keys; the resulting claim's predicate URI is set accordingly.
        ``payload`` is recorded as ``predicate_payload`` so a downstream
        verifier can re-derive shape and contents. The claim text is a
        short summary; the canonical bytes live in the predicate.
        """
        if self._graph is None:
            raise RuntimeError(
                "ingest() needs the OutputIngester to be constructed "
                "with a graph=... argument"
            )
        if capability not in SUPPORTED_CAPABILITIES:
            raise ValueError(
                f"unsupported capability {capability!r}; choose from "
                f"{sorted(SUPPORTED_CAPABILITIES)}"
            )
        uri = SUPPORTED_CAPABILITIES[capability]
        summary = (
            payload.get("summary")
            or f"Gemini {capability} ingest"
        )
        return self._graph.assert_claim(
            str(summary),
            classification="INFERRED",
            generated_by=generated_by,
            supports=supports,
            predicate_payload={
                "predicate_type": uri,
                "capability": capability,
                **payload,
            },
        )
