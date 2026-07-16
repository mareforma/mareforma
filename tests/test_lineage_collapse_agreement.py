"""A declaration that agrees with a seam capture corroborates, never downgrades.

``collapse_lineage`` resolves a scope's finding-level tier to the most
conservative present. A cooperating producer who both wraps ``httpx`` (earning a
COMPUTED socket capture) and calls ``declare_model`` for the SAME model produces
two records that share one family root. That is redundant agreement, not
conflict, so the collapse must keep COMPUTED. Downgrading it to PROXY erases the
independence unit the seam capture earned and penalises the most cooperative
producer.
"""
from __future__ import annotations

from mareforma.observe._lineage import (
    ModelLineage,
    ModelLineageTier,
    collapse_lineage,
    independence_model_key,
)

_ROOT = "claude-3-5-sonnet"


def _computed(root: str = _ROOT) -> ModelLineage:
    return ModelLineage(
        tier=ModelLineageTier.COMPUTED,
        model_id=f"{root}-20241022",
        family_root=root,
        provider="anthropic",
        version="20241022",
        method="/v1/messages",
        decoding={},
        attestor="provider-host",
    )


def _declared(root: str = _ROOT) -> ModelLineage:
    return ModelLineage(
        tier=ModelLineageTier.PROXY,
        model_id=f"{root}-20241022",
        family_root=root,
        provider=None,
        version="20241022",
        method="declared",
        decoding={},
        attestor="declared",
    )


def test_agreeing_declaration_keeps_computed() -> None:
    collapsed = collapse_lineage([_computed(), _declared()])
    assert collapsed is not None
    assert collapsed.tier is ModelLineageTier.COMPUTED
    assert independence_model_key(collapsed.to_dict()) == ("model", _ROOT)


def test_declaration_before_capture_also_keeps_computed() -> None:
    # Order independent: the seam capture wins regardless of record order.
    collapsed = collapse_lineage([_declared(), _computed()])
    assert collapsed.tier is ModelLineageTier.COMPUTED


def test_disagreeing_declaration_still_downgrades() -> None:
    # A declaration naming a DIFFERENT model than the seam captured is a real
    # conflict: the span is mixed and stays UNVERIFIABLE.
    collapsed = collapse_lineage([_computed("claude-3-5-sonnet"), _declared("gpt-4o")])
    assert collapsed.tier is ModelLineageTier.UNVERIFIABLE
    assert independence_model_key(collapsed.to_dict()) == ("soft",)


def test_all_declared_stays_proxy() -> None:
    # No seam capture present: agreeing declarations remain PROXY (soft).
    collapsed = collapse_lineage([_declared(), _declared()])
    assert collapsed.tier is ModelLineageTier.PROXY
    assert independence_model_key(collapsed.to_dict()) == ("soft",)
