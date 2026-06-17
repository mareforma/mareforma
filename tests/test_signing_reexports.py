"""Re-export guard for the ``mareforma.signing`` subpackage.

When a new symbol is added to ``mareforma/signing/core.py`` or
``mareforma/signing/rekor.py``, ``mareforma/signing/__init__.py``
must re-export it so that callers writing ``from mareforma.signing
import X`` and code accessing ``signing.X`` via attribute lookup
continue to work after the carve.

This test parses the two submodule source files for every
module-level name (functions, classes, constants) — including
underscore-prefixed names — and asserts each is importable AND
accessible via ``getattr`` on the package. Failing this test means
``__init__.py`` is missing a re-export.
"""

from __future__ import annotations

import importlib
import inspect
from pathlib import Path

import pytest

import mareforma.signing as signing_pkg
from tests._helpers import _module_level_names


_CORE = Path(inspect.getfile(importlib.import_module("mareforma.signing.core")))
_REKOR = Path(inspect.getfile(importlib.import_module("mareforma.signing.rekor")))


@pytest.mark.parametrize("name", _module_level_names(_CORE))
def test_core_name_reexported(name: str) -> None:
    """Every name defined in signing/core.py is reachable via mareforma.signing."""
    assert hasattr(signing_pkg, name), (
        f"mareforma/signing/__init__.py is missing a re-export for "
        f"{name!r} (defined in signing/core.py). Add it to the "
        f"`from .core import (...)` block AND, if public, to __all__."
    )
    # Also assert `from mareforma.signing import <name>` works — the
    # import-statement path is observably distinct from getattr() in
    # at least one Python edge case (lazy attribute hooks).
    module = importlib.import_module("mareforma.signing")
    assert hasattr(module, name)


@pytest.mark.parametrize("name", _module_level_names(_REKOR))
def test_rekor_name_reexported(name: str) -> None:
    """Every name defined in signing/rekor.py is reachable via mareforma.signing."""
    assert hasattr(signing_pkg, name), (
        f"mareforma/signing/__init__.py is missing a re-export for "
        f"{name!r} (defined in signing/rekor.py). Add it to the "
        f"`from .rekor import (...)` block AND, if public, to __all__."
    )
    module = importlib.import_module("mareforma.signing")
    assert hasattr(module, name)


def test_all_lists_only_public_names() -> None:
    """__all__ must list only public names (no underscore-prefixed entries).

    Underscore-prefixed names are intentionally bound in the package
    namespace (so ``from mareforma.signing import _X`` and
    ``getattr(signing, '_X')`` both work for internal callers) but
    deliberately omitted from __all__ so ``from mareforma.signing
    import *`` does not surface them.
    """
    underscore_in_all = [n for n in signing_pkg.__all__ if n.startswith("_")]
    assert underscore_in_all == [], (
        f"__all__ contains underscore-prefixed names which should be "
        f"internal-only: {underscore_in_all}"
    )


def test_all_lists_match_actually_defined() -> None:
    """Every name in __all__ must actually be bound in the package."""
    missing = [n for n in signing_pkg.__all__ if not hasattr(signing_pkg, n)]
    assert missing == [], (
        f"__all__ lists names that are not bound in mareforma.signing: "
        f"{missing}"
    )
