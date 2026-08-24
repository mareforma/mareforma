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


def warn_refutation_status_without_conn(*, stacklevel: int = 4) -> None:
    """Warn that the row-only ``refutation_status`` cannot replay the verdicts.

    Four frames: this function, ``_emit``, ``refutation_status``, the caller.
    Any fewer attributes the warning to ``core.py`` rather than to the code that
    called it, and Python's default filter only shows a DeprecationWarning
    attributed to ``__main__``, so a warning that stops short reaches nobody. A
    deprecation nobody sees is a removal with no notice.

    The signature that takes a row and nothing else answers off ``t_invalid``,
    a column no trigger guards, so a caller on this path is told a
    contradiction exists (or does not) on the strength of an edit nobody
    signed. Passing the connection lets the same function hold the signed
    verdicts against their issuers instead. The old form still answers, and it
    still says in its ``signal`` that nothing was replayed; the warning is here
    because a caller reading only ``state`` cannot see that.
    """
    _emit(
        "refutation_status(row) without a connection reports the t_invalid "
        "column, which no trigger guards, so a fabricated or erased "
        "contradiction reads back as fact. Pass the graph connection, "
        "refutation_status(row, conn), to replay the signed contradiction "
        "verdicts instead. The row-only form is removed in a future release.",
        stacklevel,
    )
