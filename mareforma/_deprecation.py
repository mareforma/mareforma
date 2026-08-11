"""_deprecation.py: shared deprecation-warning emitters.

A neutral module both ``trust/`` and ``db/`` import without pulling one into
the other. ``db/core.py`` reaches ``trust`` only once, lazily; keeping the
``warnings.warn`` boilerplate here means the two subpackages emit one warning
shape (category, stacklevel discipline, message form) from one implementation
rather than two copies drifting apart.
"""
from __future__ import annotations

import warnings


def _emit(message: str, stacklevel: int) -> None:
    """Emit a single ``DeprecationWarning``. The one place the category and the
    warn call live, so every deprecation in the package reads the same.

    ``stacklevel`` is passed to :func:`warnings.warn` UNCHANGED, and this
    function is itself a frame, so a caller counts from here, not from itself:
    a site that wants the warning attributed to its own caller passes 3, not 2.
    Getting this wrong points a DeprecationWarning at a mareforma file, where
    Python's default filter hides it from the person whose code needs changing,
    which is the failure mode that made this module worth having.
    """
    warnings.warn(message, DeprecationWarning, stacklevel=stacklevel)


def warn_retired_status(old: str, new: str, *, stacklevel: int = 4) -> None:
    """Warn that a retired ``Status`` label resolves to ``new`` for one release.

    ``old`` named a corroboration/independence verdict, but distinct-model is
    necessary, not sufficient, for independence, so the word over-claimed.
    """
    _emit(
        f"Status.{old} is retired: it named a corroboration/independence "
        f"verdict, but distinct-model is necessary, not sufficient, for "
        f"independence. Use Status.{new}, a convergence marker for two or more "
        f"lineage-distinct supporting lines converging. This alias resolves "
        f"this release and is removed in a future release.",
        stacklevel,
    )


def warn_deprecated_seed(*, stacklevel: int = 6) -> None:
    """Warn that ``assert_claim(seed=True)`` is deprecated and removed in v0.4.0.

    The seed path stays functional this release: it still writes a signed
    ESTABLISHED claim and it is the only anchor that bootstraps a fresh trust
    chain. The replacement anchor is designed for v0.4.0.

    The default is 6, the depth from the one call site (``db.core.add_claim``,
    reached through the graph's ``assert_claim``) out to the user's own line. It
    used to be 3, which named no call site that exists: a default no caller can
    use is a default that points a warning at mareforma's own frames.
    """
    _emit(
        "assert_claim(seed=True) is deprecated and will be removed in v0.4.0. "
        "It stays the seed-anchor bootstrap for this release and still writes a "
        "signed ESTABLISHED claim; a replacement anchor arrives in v0.4.0.",
        stacklevel,
    )
