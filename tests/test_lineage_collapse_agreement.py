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


_LOCAL = "qwen3:8b"
_DIGEST = "sha256:abc"


def _digest_capture(model_id: str = _LOCAL, digest: str = _DIGEST) -> ModelLineage:
    # A local inference server: the seam capture keys on the weights digest and
    # roots to no remote family, so family_root is None by design.
    return ModelLineage(
        tier=ModelLineageTier.COMPUTED,
        model_id=model_id,
        family_root=None,
        provider=None,
        version=None,
        method="/api/chat",
        decoding={},
        attestor="weights-digest",
        digest=digest,
    )


def _declared_local(model_id: str = _LOCAL) -> ModelLineage:
    return ModelLineage(
        tier=ModelLineageTier.PROXY,
        model_id=model_id,
        family_root="qwen-3",
        provider=None,
        version=None,
        method="declared",
        decoding={},
        attestor="declared",
    )


def test_agreeing_declaration_keeps_the_weights_digest() -> None:
    # The declaration names the model the digest capture already verified. Its
    # family root is the same identity in another space, not a second model, so
    # the digest identity survives the collapse.
    collapsed = collapse_lineage([_digest_capture(), _declared_local()])
    assert collapsed.tier is ModelLineageTier.COMPUTED
    assert independence_model_key(collapsed.to_dict()) == ("model", "digest:" + _DIGEST)


def test_declaration_before_digest_capture_also_keeps_it() -> None:
    collapsed = collapse_lineage([_declared_local(), _digest_capture()])
    assert collapsed.tier is ModelLineageTier.COMPUTED
    assert independence_model_key(collapsed.to_dict()) == ("model", "digest:" + _DIGEST)


def test_declaration_of_another_model_downgrades_the_digest() -> None:
    # A declaration naming a DIFFERENT model is a real conflict: two identities
    # in one span, so nothing is certified.
    collapsed = collapse_lineage([_digest_capture(), _declared("gpt-4o")])
    assert collapsed.tier is ModelLineageTier.UNVERIFIABLE
    assert independence_model_key(collapsed.to_dict()) == ("soft",)


def test_two_distinct_digests_stay_unverifiable() -> None:
    collapsed = collapse_lineage(
        [_digest_capture(), _digest_capture("llama3:8b", "sha256:def")]
    )
    assert collapsed.tier is ModelLineageTier.UNVERIFIABLE
    assert independence_model_key(collapsed.to_dict()) == ("soft",)
