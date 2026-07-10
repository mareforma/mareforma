"""Model/method lineage captured at the call boundary, tiered like ``data_id``.

The observer records WHICH model and method authored a finding, computed from the
request the producer actually sent rather than from a self-declaration. The tier
mirrors the ``data_id`` axis exactly:

- ``COMPUTED``      — the model came from a body-parse at the socket seam (a
                      wrapped ``httpx`` POST). The producer does not control this
                      path, so it is the trustworthy tier, analogous to a
                      content-addressed ``data_id``.
- ``PROXY``         — a cooperating producer declared the model out of band
                      (``declare_model``). Agent-attested and soft, analogous to
                      a string-fallback ``data_id``: it never reads as COMPUTED.
- ``UNVERIFIABLE``  — the lineage is soft: a hosted fine-tune, a moving alias, or
                      a wrapper whose base model is not declarable. A distinct
                      model STRING does not, by itself, read as a distinct model
                      — it family-roots to its base where declarable, else it is
                      UNVERIFIABLE, never a fabricated distinct model.

The model interior is identity-only. This records the model/method identity; it
makes no claim about training-time contamination, which is a later boundary.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, replace
from enum import Enum

# Model families whose base is declarable from the model string. A name outside
# this set (a wrapper, an internal router, an unknown provider) cannot be rooted
# to a declarable base, so it is UNVERIFIABLE rather than a fabricated distinct
# model.
_KNOWN_FAMILY_PREFIXES: tuple[str, ...] = (
    "claude-",
    "gpt-",
    "o1",
    "o3",
    "o4-",
    "chatgpt-",
)

# A trailing release token: an 8-digit Anthropic date (``20241022``) or an
# ISO OpenAI date (``2024-08-06``). Stripped to yield the family root so two
# date-distinct strings of one base collapse to the same root.
_VERSION_SUFFIX = re.compile(r"^(?P<root>.+?)-(?P<ver>\d{8}|\d{4}-\d{2}-\d{2})$")


class ModelLineageTier(str, Enum):
    """The computed model-lineage tier. Mirrors the ``data_id`` trust axis."""

    COMPUTED = "COMPUTED"
    PROXY = "PROXY"
    UNVERIFIABLE = "UNVERIFIABLE"


@dataclass(frozen=True)
class ModelLineage:
    """The model/method identity behind a finding, with its computed tier.

    ``model_id`` is the raw model string as seen. ``family_root`` is the base the
    string roots to when declarable (else ``None`` — the UNVERIFIABLE marker).
    ``decoding`` carries the sampling parameters (``temperature``, ``top_p``,
    ``seed``) the request declared. ``method`` is the tool/pipeline identity: the
    request path for a socket-seam capture, or the producer's tag for a
    declaration.
    """

    tier: ModelLineageTier
    model_id: str
    family_root: str | None
    provider: str | None
    version: str | None
    method: str | None
    decoding: dict

    def to_dict(self) -> dict:
        return {
            "tier": self.tier.value,
            "model_id": self.model_id,
            "family_root": self.family_root,
            "provider": self.provider,
            "version": self.version,
            "method": self.method,
            "decoding": dict(self.decoding),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ModelLineage":
        return cls(
            tier=ModelLineageTier(d.get("tier", "UNVERIFIABLE")),
            model_id=d.get("model_id", ""),
            family_root=d.get("family_root"),
            provider=d.get("provider"),
            version=d.get("version"),
            method=d.get("method"),
            decoding=dict(d.get("decoding") or {}),
        )


def _family_root(model_id: str) -> tuple[str | None, str | None, bool]:
    """``(root, version, declarable)`` for a model string.

    ``declarable`` is False for a hosted fine-tune (base weights not declarable),
    a moving alias (``-latest``), or a name outside the known families (a
    wrapper). A declarable string roots to its base with any trailing release
    date stripped.
    """
    m = (model_id or "").strip()
    if not m:
        return (None, None, False)
    lower = m.lower()
    # A hosted fine-tune: the base weights behind the alias are not declarable.
    if lower.startswith("ft:") or "::" in m:
        return (None, None, False)
    # A moving alias: the concrete weights it points at are not declarable.
    if lower.endswith("-latest") or lower == "latest":
        return (None, None, False)
    # A name outside the known families (a wrapper / unknown provider) cannot be
    # rooted to a declarable base.
    if not lower.startswith(_KNOWN_FAMILY_PREFIXES):
        return (None, None, False)
    mo = _VERSION_SUFFIX.match(m)
    if mo:
        return (mo.group("root"), mo.group("ver"), True)
    return (m, None, True)


def resolve_lineage(
    model_id: str,
    *,
    source: str,
    method: str | None,
    decoding: dict,
    provider: str | None = None,
) -> ModelLineage:
    """Tier a captured or declared model.

    ``source`` is ``"socket"`` for a body-parse at the seam (COMPUTED) or
    ``"declared"`` for a producer declaration (PROXY). Either way, a string whose
    base is not declarable is UNVERIFIABLE — soft lineage dominates the source
    tier, so a fine-tune or alias never reads as COMPUTED or PROXY.
    """
    root, version, declarable = _family_root(model_id)
    if not declarable:
        tier = ModelLineageTier.UNVERIFIABLE
        root, version = None, None
    elif source == "socket":
        tier = ModelLineageTier.COMPUTED
    else:
        tier = ModelLineageTier.PROXY
    return ModelLineage(
        tier=tier,
        model_id=(model_id or "").strip(),
        family_root=root,
        provider=provider,
        version=version,
        method=method,
        decoding=dict(decoding),
    )


def collapse_lineage(records: list[ModelLineage]) -> ModelLineage | None:
    """The single finding-level lineage for a scope's captured model records.

    One record is returned as-is. When a scope captured several, the tier is the
    most conservative present (UNVERIFIABLE if any is soft or the roots disagree,
    else PROXY if any is agent-declared, else COMPUTED), so a mixed authoring
    span never over-claims a clean single model.
    """
    if not records:
        return None
    if len(records) == 1:
        return records[0]
    distinct_roots = {r.family_root for r in records if r.family_root is not None}
    tiers = {r.tier for r in records}
    if ModelLineageTier.UNVERIFIABLE in tiers or len(distinct_roots) > 1:
        tier = ModelLineageTier.UNVERIFIABLE
    elif ModelLineageTier.PROXY in tiers:
        tier = ModelLineageTier.PROXY
    else:
        tier = ModelLineageTier.COMPUTED
    root = next(iter(distinct_roots)) if len(distinct_roots) == 1 else None
    return replace(records[0], tier=tier, family_root=root)
