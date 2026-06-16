"""Shared builders for the epistemic test suite.

These were duplicated, byte-for-byte, across the epistemic test files.
``open_graph``/``open_signed_graph``/``_bootstrap_validator_key`` open
graphs with a bootstrapped signing key; ``_prop``/``_superiority``/``_smd``
construct the trust-layer value objects the finding tests build on.

The ``_superiority`` and ``_smd`` factories are the SUPERSET forms: the
``preregistered=`` flag and the ``n=`` keyword default to a no-op, so they
also serve the narrower call sites that never pass them.
"""

from __future__ import annotations

from pathlib import Path

import mareforma
from mareforma.trust import (
    Direction,
    DirectionOfInterest,
    EffectEstimate,
    EffectType,
    Prediction,
    Proposition,
    TestType,
)


# ---------------------------------------------------------------------------
# Graph openers
# ---------------------------------------------------------------------------

def open_graph(tmp_path: Path):
    """Open a graph with a bootstrapped key so ``seed=True`` works.

    A loaded signing key is needed because ESTABLISHED-upstream promotion
    and ``assert_finding`` both write signed claims. Returns the unclosed
    graph; callers use ``with open_graph(tmp_path) as g:``.
    """
    from mareforma import signing as _signing
    key_path = tmp_path / "_test_key"
    if not key_path.exists():
        _signing.bootstrap_key(key_path)
    return mareforma.open(tmp_path, key_path=key_path)


def open_signed_graph(tmp_path: Path):
    """Open a graph whose loaded key auto-enrolls as the root validator.

    Required for tests that exercise ``graph.validate()`` — the auto-enrolled
    root is the prerequisite for promoting a claim to ESTABLISHED.
    """
    from mareforma import signing as _signing
    key_path = tmp_path / "mareforma.key"
    _signing.bootstrap_key(key_path)
    return mareforma.open(tmp_path, key_path=key_path)


def _bootstrap_validator_key(tmp_path: Path) -> Path:
    """Bootstrap a second signing key and return its path.

    The graph refuses self-validation, so promoting a REPLICATED claim needs
    a key distinct from the one that signed the claim.
    """
    from mareforma import signing as _signing
    key_path = tmp_path / "validator.key"
    if not key_path.exists():
        _signing.bootstrap_key(key_path)
    return key_path


# ---------------------------------------------------------------------------
# Trust-layer value objects
# ---------------------------------------------------------------------------

def _prop(direction: Direction = Direction.DECREASES, **scope) -> Proposition:
    return Proposition(
        subject="BRCA1",
        relation="affects",
        object="tumour growth",
        direction=direction,
        scope=scope or {"population": "TNBC", "condition": "in vitro"},
    )


def _superiority(
    direction: DirectionOfInterest = DirectionOfInterest.DECREASE,
    alpha: float = 0.05,
    preregistered: bool = False,
) -> Prediction:
    return Prediction(
        TestType.SUPERIORITY,
        direction_of_interest=direction,
        alpha=alpha,
        preregistered=preregistered,
    )


def _smd(value: float, *, p=None, ci=None, ci_level=None, n=None) -> EffectEstimate:
    kw: dict = {}
    if p is not None:
        kw["p_value"] = p
    if ci is not None:
        kw["ci_lower"], kw["ci_upper"] = ci
        kw["ci_level"] = ci_level
    if n is not None:
        kw["n_total"] = n
    return EffectEstimate(value, EffectType.SMD, **kw)
