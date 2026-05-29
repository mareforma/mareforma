"""Gemini-for-Science adapter — read-only ingest of public outputs.

Gemini for Science (AlphaEvolve+ERA, Co-Scientist, NotebookLM,
Antigravity Science Skills) is a closed platform with no upstream
cooperation path: mareforma reads public outputs, sanitises them,
validates the per-capability payload shape, and signs them as
INFERRED claims so downstream agents can reason about provenance.

The v0.3.3 surface is intentionally minimal:

- :class:`OutputIngester` — accepts a single Gemini output dict
  (capability + payload). Per-capability required-field validation
  catches malformed inputs at ingest time; string payload values
  flow through :func:`mareforma.sanitize_for_llm` so prompt-injection
  vectors cannot survive into the claim. One INFERRED claim is
  asserted with a predicate URI matching the capability.

- URI constants — re-exported from :mod:`mareforma.predicate_types`
  for ergonomic import:

  * ``CODE_VARIATION_V1`` (AlphaEvolve+ERA)
  * ``HYPOTHESIS_V1`` (Co-Scientist debate / argument synthesis)
  * ``LITERATURE_INSIGHT_V1`` (NotebookLM cells)
  * ``SCIENCE_SKILL_V1`` (Antigravity Science Skills database calls)

Full surface-specific producers (per-cell NotebookLM ingest, full
Co-Scientist debate transcripts, AlphaEvolve population-tree
reconstruction, deterministic replay) are queued for v0.3.4+ when
adoption signal warrants the surface area.

Install: ``pip install mareforma[gemini]``.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Mapping

from mareforma import sanitize_for_llm
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
    "REQUIRED_FIELDS",
    "SUPPORTED_CAPABILITIES",
]


# Capability → URI table. Frozen via MappingProxyType so a downstream
# `del SUPPORTED_CAPABILITIES['hypothesis']` cannot poison the registry
# process-wide. Use the constants from mareforma.predicate_types if you
# need URI strings directly.
SUPPORTED_CAPABILITIES: Mapping[str, str] = MappingProxyType({
    "code-variation": CODE_VARIATION_V1,
    "hypothesis": HYPOTHESIS_V1,
    "literature-insight": LITERATURE_INSIGHT_V1,
    "science-skill": SCIENCE_SKILL_V1,
})

# Cached at module load so predicate_uris() is allocation-free.
_PREDICATE_URIS: tuple[str, ...] = tuple(SUPPORTED_CAPABILITIES.values())

# Per-capability required-field schema. Kept minimal in v0.3.3 — the
# field sets mirror the atelier predicate builders' load-bearing keys
# so a v0.3.4 full-adapter promotion will not break existing claims.
# Each capability's required fields are validated at ingest time;
# missing fields raise ValueError BEFORE assert_claim runs.
REQUIRED_FIELDS: Mapping[str, frozenset[str]] = MappingProxyType({
    "code-variation": frozenset({
        "input_problem_digest",
        "code_variation_source_digest",
        "score",
        "model_version",
    }),
    "hypothesis": frozenset({
        "final_hypothesis_text_digest",
        "model_version",
    }),
    "literature-insight": frozenset({
        "cell_value_digest",
        "cited_paper_dois",
        "model_version",
    }),
    "science-skill": frozenset({
        "db_name",
        "query_digest",
        "result_digest",
        "result_canonical_form",
        "provider",
    }),
})


# Reserved keys callers MAY NOT supply in `payload` — they are owned
# by the adapter and must not be overrideable. The previous spread
# order (`{..., **payload}`) allowed a hostile caller to override
# predicate_type with an arbitrary URI; we now reject collisions
# loudly instead.
_RESERVED_PAYLOAD_KEYS: frozenset[str] = frozenset({
    "predicate_type", "capability",
})


def _sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Recursively apply sanitize_for_llm to every string in ``payload``.

    Sanitisation runs on string values only — digests, version
    strings, and free-text summaries — leaving numbers, bools, and
    nested digest lists untouched. Mirrors the clawinstitute
    sanitisation contract so all adapters scrub external content
    before it reaches the signed envelope.
    """
    out: dict[str, Any] = {}
    for k, v in payload.items():
        out[k] = _sanitize_value(v)
    return out


def _sanitize_value(v: Any) -> Any:
    if isinstance(v, str):
        return sanitize_for_llm(v) or ""
    if isinstance(v, dict):
        return _sanitize_payload(v)
    if isinstance(v, (list, tuple)):
        cls = type(v)
        return cls(_sanitize_value(x) for x in v)
    return v


class OutputIngester:
    """Read-only ingest of Gemini-for-Science capability outputs.

    Each ``ingest()`` call:

    1. Validates the capability against :data:`SUPPORTED_CAPABILITIES`.
    2. Validates required fields against :data:`REQUIRED_FIELDS`.
    3. Refuses payloads that try to set reserved keys (predicate_type,
       capability) — those are adapter-owned.
    4. Sanitises every string in the payload via
       :func:`mareforma.sanitize_for_llm`.
    5. Asserts ONE INFERRED claim under the matching capability URI.

    Claims are INFERRED by default — a Gemini output is a single
    source's claim, not a cross-host replication. Downstream code is
    responsible for promoting findings that converge with claims from
    other sources.
    """

    def __init__(self, *, graph: "EpistemicGraph | None" = None) -> None:
        self._graph = graph

    # ------------------------------------------------------------------
    # Convention helpers (used by the coexistence test)
    # ------------------------------------------------------------------

    def predicate_uris(self) -> tuple[str, ...]:
        """Return the URIs this ingester may emit on the graph."""
        return _PREDICATE_URIS

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
            payload={
                "summary": "Gemini literature-insight (sample)",
                "cell_value_digest": "sha256:" + "0" * 64,
                "cited_paper_dois": [],
                "model_version": "sample",
            },
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
        """Assert one INFERRED claim from a Gemini output payload."""
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

        # Reserved-key collision check: predicate_type / capability
        # are adapter-owned and MUST NOT be overrideable by caller.
        collisions = _RESERVED_PAYLOAD_KEYS & set(payload)
        if collisions:
            raise ValueError(
                f"payload may not set reserved keys {sorted(collisions)}; "
                "predicate_type and capability are adapter-owned"
            )

        # Required-field validation BEFORE assert_claim runs.
        missing = REQUIRED_FIELDS[capability] - set(payload)
        if missing:
            raise ValueError(
                f"capability {capability!r} requires fields "
                f"{sorted(REQUIRED_FIELDS[capability])}; "
                f"missing: {sorted(missing)}"
            )

        sanitised = _sanitize_payload(payload)
        uri = SUPPORTED_CAPABILITIES[capability]
        summary = sanitised.get("summary") or f"Gemini {capability} ingest"

        return self._graph.assert_claim(
            str(summary),
            classification="INFERRED",
            generated_by=generated_by,
            supports=supports,
            predicate_payload={
                # Spread payload FIRST so the canonical adapter-owned
                # keys (predicate_type, capability) cannot be
                # overridden even if the reserved-key check above is
                # ever loosened.
                **sanitised,
                "predicate_type": uri,
                "capability": capability,
            },
        )
