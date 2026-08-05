"""Model/method lineage captured at the call boundary, tiered like ``data_id``.

The observer records WHICH model and method authored a finding, computed from the
request the producer actually sent rather than from a self-declaration. The tier
mirrors the ``data_id`` axis exactly:

- ``COMPUTED``     , the model came from a body-parse at the socket seam (a
                      wrapped ``httpx`` POST) addressed to a RECOGNIZED provider
                      host. The producer cannot name an arbitrary model to an
                      arbitrary endpoint, so this is the trustworthy tier,
                      analogous to a content-addressed ``data_id``. Its residual:
                      it is still the model field of the producer's own request,
                      read off the wire, not a response-attested fact. A
                      body-parse to an unrecognized host is fully
                      producer-controlled, so it is UNVERIFIABLE, never COMPUTED.
- ``PROXY``        , a cooperating producer declared the model out of band
                      (``declare_model``). Agent-attested and soft, analogous to
                      a string-fallback ``data_id``: it never reads as COMPUTED.
- ``UNVERIFIABLE`` , the lineage is soft: a hosted fine-tune, a moving alias, or
                      a wrapper whose base model is not declarable. A distinct
                      model STRING does not, by itself, read as a distinct model
                     , it family-roots to its base where declarable, else it is
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
# model. These are the closed provider families, whose strings are stable; the
# open-weight families root through ``_open_family_root`` below instead.
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

# A family release number: digits with an optional minor, and never a
# parameter-count size (the lookahead refuses ``70b``/``0.6b``), so
# ``llama-3.1-70b`` roots on the 3.1, not the 70.
_OPEN_VER = r"(\d+(?:\.\d+)?)(?![\d.]*b)"

# Open-weight (and open-string) families, rooted to family + release. One open
# release is served under provider-specific names (``llama-3.1-70b-versatile``,
# ``meta-llama/Llama-3.1-70B-Instruct``, ``llama-v3p1-70b-instruct``), so the
# root is deliberately COARSE: variants of one release collapse to one model on
# the independence axis. The rounding direction is safety, naming variance can
# under-claim distinctness (two sizes of one release read as one model), never
# mint a fake distinct model; a string no pattern parses stays UNVERIFIABLE.
_OPEN_FAMILY_PATTERNS: tuple[tuple[str, "re.Pattern[str]"], ...] = tuple(
    (family, re.compile(pattern))
    for family, pattern in (
        ("llama", rf"^(?:meta-)?llama-?v?{_OPEN_VER}(?=$|[-:.])"),
        ("qwen", rf"^qwen-?{_OPEN_VER}(?=$|[-:.])"),
        ("mixtral", r"^mixtral(?:-(\d+x\d+b))?(?=$|[-:.])"),
        ("mistral", r"^mistral(?=$|[-:.])"),
        ("gemma", rf"^gemma-?{_OPEN_VER}(?=$|[-:.])"),
        ("deepseek", r"^deepseek(?:-(v\d+(?:\.\d+)?|r\d+))?(?=$|[-:.])"),
        ("phi", rf"^phi-?{_OPEN_VER}(?=$|[-:.])"),
        ("gemini", rf"^gemini-{_OPEN_VER}(?=$|[-:.])"),
        ("grok", rf"^grok-?{_OPEN_VER}(?=$|[-:.])"),
        ("glm", rf"^glm-?{_OPEN_VER}(?=$|[-:.])"),
        ("kimi", r"^kimi(?:-?(k\d+))?(?=$|[-:.])"),
    )
)

# Fireworks release notation: ``v3p1`` means v3.1.
_P_NOTATION = re.compile(r"v(\d+)p(\d+)")


def _open_family_root(lower: str) -> "str | None":
    """The family-release root of an open-model string, or None.

    Works on the last path segment (hosted ids are namespaced,
    ``meta-llama/...``, ``accounts/fireworks/models/...``), with provider
    release notation normalized first.
    """
    seg = lower.rsplit("/", 1)[-1]
    seg = _P_NOTATION.sub(r"v\1.\2", seg)
    for family, pattern in _OPEN_FAMILY_PATTERNS:
        mo = pattern.match(seg)
        if mo is not None:
            ver = next((g for g in mo.groups() if g), None)
            return f"{family}-{ver}" if ver else family
    return None


class ModelLineageTier(str, Enum):
    """The computed model-lineage tier. Mirrors the ``data_id`` trust axis."""

    COMPUTED = "COMPUTED"
    PROXY = "PROXY"
    UNVERIFIABLE = "UNVERIFIABLE"


@dataclass(frozen=True)
class ModelLineage:
    """The model/method identity behind a finding, with its computed tier.

    ``model_id`` is the raw model string as seen. ``family_root`` is the base the
    string roots to when declarable (else ``None``, the UNVERIFIABLE marker).
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
    # How the model identity was established, recorded so the trust map never
    # blurs a third-party-attested identity with a self-attested one:
    #   ``provider-host`` , observed at the seam to a recognized remote host.
    #   ``weights-digest``, a local inference server's content digest of the
    #                        served weights (self-attested: the producer controls
    #                        the machine, so it is COMPUTED-grade DISCRIMINATION,
    #                        not third-party attestation, the operator-Sybil
    #                        residual is named, not closed).
    #   ``declared``      , a producer declaration (PROXY).
    attestor: str | None = None
    # The content digest of the served weights for a ``weights-digest`` lineage
    # (``sha256:...``); the distinctness key for local models, which root to no
    # known remote family.
    digest: str | None = None

    def to_dict(self) -> dict:
        return {
            "tier": self.tier.value,
            "model_id": self.model_id,
            "family_root": self.family_root,
            "provider": self.provider,
            "version": self.version,
            "method": self.method,
            "decoding": dict(self.decoding),
            "attestor": self.attestor,
            "digest": self.digest,
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
            attestor=d.get("attestor"),
            digest=d.get("digest"),
        )


def _family_root(model_id: str) -> tuple[str | None, str | None, bool]:
    """``(root, version, declarable)`` for a model string.

    ``declarable`` is False for a hosted fine-tune (base weights not declarable),
    a moving alias (``-latest``), or a name outside the known families (a
    wrapper). A closed-family string roots to its base with any trailing release
    date stripped; an open-family string roots to its family release via
    :func:`_open_family_root`.
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
    if lower.startswith(_KNOWN_FAMILY_PREFIXES):
        mo = _VERSION_SUFFIX.match(m)
        if mo:
            return (mo.group("root"), mo.group("ver"), True)
        return (m, None, True)
    # Not a closed family: try the open-weight family release patterns. A name
    # neither set can root (a wrapper / unknown provider) is not declarable.
    open_root = _open_family_root(lower)
    if open_root is not None:
        return (open_root, None, True)
    return (None, None, False)


def resolve_lineage(
    model_id: str,
    *,
    source: str,
    method: str | None,
    decoding: dict,
    provider: str | None = None,
    digest: str | None = None,
) -> ModelLineage:
    """Tier a captured or declared model.

    ``source`` is ``"socket"`` for a body-parse at the seam (COMPUTED) or
    ``"declared"`` for a producer declaration (PROXY). Either way, a string whose
    base is not declarable is UNVERIFIABLE, soft lineage dominates the source
    tier, so a fine-tune or alias never reads as COMPUTED or PROXY.

    ``digest`` is the content digest of the served weights for a LOCAL inference
    server (Ollama), resolved by the observer from the running server. A local
    model roots to no known remote family, so its digest, not a family root, is
    its verifiable distinct identity: a seam capture WITH a digest is COMPUTED via
    the ``weights-digest`` attestor, bypassing the family-prefix gate.
    """
    if source == "socket" and digest:
        # Local content-addressed identity. COMPUTED-grade discrimination: two
        # distinct digests are provably distinct weights; the same digest is
        # provably the same. Self-attested (the producer's own machine), recorded
        # as such via the attestor field, not third-party host attestation.
        return ModelLineage(
            tier=ModelLineageTier.COMPUTED,
            model_id=(model_id or "").strip(),
            family_root=None,
            provider=None,
            version=None,
            method=method,
            decoding=dict(decoding),
            attestor="weights-digest",
            digest=digest,
        )
    root, version, declarable = _family_root(model_id)
    attestor = None
    if not declarable:
        tier = ModelLineageTier.UNVERIFIABLE
        root, version = None, None
    elif source == "socket" and provider:
        # Observed at the seam AND addressed to a recognized provider host: the
        # producer cannot name an arbitrary model to an arbitrary endpoint, so
        # this is the trustworthy COMPUTED tier (still the model field of their
        # own request read off the wire, not a response-attested fact).
        tier = ModelLineageTier.COMPUTED
        attestor = "provider-host"
    elif source == "socket":
        # Observed at the seam but addressed to an UNRECOGNIZED host. The
        # producer chose the endpoint, so a "model" field in a body they sent
        # anywhere is producer-controlled and cannot certify a real model call.
        # Soft, never COMPUTED, this is what stops a forged POST to an arbitrary
        # host from minting a distinct model and faking independence.
        tier = ModelLineageTier.UNVERIFIABLE
        root, version = None, None
    else:
        tier = ModelLineageTier.PROXY
        attestor = "declared"
    return ModelLineage(
        tier=tier,
        model_id=(model_id or "").strip(),
        family_root=root,
        provider=provider,
        version=version,
        method=method,
        decoding=dict(decoding),
        attestor=attestor,
        digest=None,
    )


def independence_model_key(lineage: "dict | None") -> tuple:
    """The independence-relevant identity of a captured model lineage.

    Returns one of three keys, mirroring the tier's trust:

    - ``("model", root)``, COMPUTED lineage rooted to a declarable base: a
      verifiable distinct model. Two lines collapse on this key iff their roots
      match, so a same-model rerun (even under distinct signers) is not a second
      independent line.
    - ``("soft",)``, PROXY / UNVERIFIABLE lineage (or COMPUTED without a root,
      defensively): present but not a verifiable model, so it can never certify a
      distinct model and never earns an independent unit.
    - ``("absent",)``, no observed model call. The line made no model claim, so
      it imposes no model constraint and keeps the legacy signer axis; every
      pre-observer finding lands here.

    ``lineage`` is the parsed ``model_lineage`` record (a dict) or ``None``.
    Only ``None`` reads as absent (no model call observed); a present-but-empty
    or non-conforming record (including a non-dict from a tampered column) reads
    as soft (fail-closed, a record we cannot make sense of never certifies a
    distinct model, and never crashes the count).
    """
    if lineage is None:
        return ("absent",)
    if not isinstance(lineage, dict):
        return ("soft",)
    tier = lineage.get("tier")
    if tier == ModelLineageTier.COMPUTED.value:
        # A local content-addressed model roots to no known remote family; its
        # weights digest is its verifiable distinct identity.
        digest = lineage.get("digest")
        if lineage.get("attestor") == "weights-digest" and digest:
            return ("model", "digest:" + digest)
        root = lineage.get("family_root")
        if root:
            return ("model", root)
    return ("soft",)


def model_distinct_pair(a: "dict | None", b: "dict | None") -> bool:
    """Whether two lineages count as model-distinct for an independence pair.

    ``False`` when the pair is UNVERIFIABLE, soft lineage on either side, so a
    distinct model cannot be certified, or when both are the SAME COMPUTED model
    (equal family roots). ``True`` when both are COMPUTED with distinct roots, or
    when at least one side made no model claim (legacy absent lineage imposes no
    constraint). Soft never a silent pass: a soft side always reads ``False``.
    """
    ka, kb = independence_model_key(a), independence_model_key(b)
    if ka[0] == "soft" or kb[0] == "soft":
        return False
    if ka[0] == "model" and kb[0] == "model":
        return ka[1] != kb[1]
    return True


def _model_key(record: "ModelLineage") -> str:
    """The record's model string, normalized for identity comparison."""
    return (record.model_id or "").strip().casefold()


def collapse_lineage(records: list[ModelLineage]) -> ModelLineage | None:
    """The single finding-level lineage for a scope's captured model records.

    One record is returned as-is. When a scope captured several, the tier is
    UNVERIFIABLE if any record is soft or the identities disagree; otherwise every
    record shares one identity, and a seam-verified COMPUTED wins over a producer
    declaration of the SAME model (redundant agreement, not conflict), so PROXY
    survives only for an all-declared span. A mixed authoring span never
    over-claims a clean single model, and an agreeing declaration never downgrades
    a capture it corroborates.

    A model identity is a remote family root OR a local weights digest, two
    distinct LOCAL models have ``family_root is None`` and are told apart only by
    their digests, so distinctness counts BOTH. A declaration of the same
    ``model_id`` as a digest capture names that one identity in the root space,
    so it corroborates it; only a different model, or a second digest, is mixed.
    A mixed or downgraded span drops the identity fields (root, digest, attestor)
    so the finding-level lineage never carries a stale digest that
    ``independence_model_key`` would key on.
    """
    if not records:
        return None
    if len(records) == 1:
        return records[0]
    digests = {r.digest for r in records if r.digest is not None}
    # A local model captured at the seam is keyed by its weights digest and has
    # no family root, while the producer's declaration of the SAME model roots
    # normally. Same model_id: one identity in two spaces, so the rooted record
    # corroborates the digest instead of counting as a second model.
    digested_ids = {_model_key(r) for r in records if r.digest is not None}
    roots = {
        r.family_root for r in records
        if r.family_root is not None and _model_key(r) not in digested_ids
    }
    tiers = {r.tier for r in records}
    # More than one distinct model identity (an unreconciled root and a digest
    # are different models) is a mixed span, never a single clean model.
    mixed = (len(roots) + len(digests)) > 1
    if ModelLineageTier.UNVERIFIABLE in tiers or mixed:
        return replace(
            records[0], tier=ModelLineageTier.UNVERIFIABLE,
            family_root=None, digest=None, attestor=None,
        )
    # Past the mixed/UNVERIFIABLE guard every record shares one identity, so a
    # PROXY record here is a producer RE-declaring what a COMPUTED capture already
    # verified, redundant agreement, not conflict. A seam capture wins: COMPUTED
    # stands whenever any record carries it, and PROXY only survives when no seam
    # capture backs the shared identity (an all-declared span).
    tier = ModelLineageTier.COMPUTED if ModelLineageTier.COMPUTED in tiers else ModelLineageTier.PROXY
    # Not mixed: at most one root and one digest, and never both, every record
    # shares the single surviving identity.
    root = next(iter(roots)) if roots else None
    digest = next(iter(digests)) if digests else None
    # The digest-carrying record is the base when there is one: collapsing onto
    # records[0] would stamp the surviving digest with the declaring record's
    # attestor, and independence_model_key reads that as soft.
    base = next((r for r in records if r.digest is not None), records[0])
    return replace(base, tier=tier, family_root=root, digest=digest)
