"""Observer coverage self-report: what is wrapped here, what forces OPAQUE.

The honest-trust thesis applied to the tool itself. Before you trust a verdict,
you should be able to ask the observer what it can and cannot see IN THIS
ENVIRONMENT: which loaders are wrapped (so their reads reach GROUNDED), which
third-party loaders are importable but not yet active, which seam kinds force
OPAQUE, and the standing coverage bounds. It is also the natural first line of a
diagnose run.

The report reflects the LIVE process: it installs the wrappers (the same lazy
install ``observe()`` does) and reads back what took, so it never claims coverage
the running interpreter does not actually have.
"""
from __future__ import annotations

import sys

from . import _loaders, _scope

# The loaders the observer wraps are declared in ``_loaders`` beside the wrap
# functions themselves (``STDLIB_WRAPS`` / ``THIRD_PARTY_WRAPS``), so this report
# cannot claim coverage the wrappers do not install, or omit one they do.

# What each seam kind means for a verdict. The kinds themselves come from
# ``_scope.SEAM_KINDS``, beside the classifier that records them, so this report
# cannot omit a kind the classifier can raise; only the wording lives here.
_SEAM_EFFECTS = {
    "socket": "network read; forces OPAQUE for URL / content-address citations",
    "subprocess": "child process; forces OPAQUE (see attach for coverage)",
    "thread": "library thread; forces OPAQUE",
    "coverage-gap": "an uninstrumented or C-runtime reader; forces OPAQUE",
    _scope.ABORT_SEAM: "the observed target exited before the scope closed; "
                       "the observation is truncated, so it forces OPAQUE",
    "failed-open": "an observed open of the cited source that raised; names "
                   "the failure, does not force OPAQUE",
}
# A kind the classifier gained and the wording above has not caught up with. An
# unmodelled kind blocks every citation, so OPAQUE is what it forces.
_UNNAMED_SEAM = "forces OPAQUE; this build carries no description for it"

_KNOWN_BOUNDS = (
    "A resource opened BEFORE the scope (a module-level or pooled handle other "
    "than the wrapped HTTP sessions) is not swapped, so its reads are invisible.",
    "The scope reaches only the asyncio tasks created inside it; a task that "
    "predates the scope is seamed at entry and lands OPAQUE.",
    "A read on a fork-started multiprocessing child is not observed (fork skips "
    "interpreter startup); it lands OPAQUE via the subprocess seam.",
    "Foreign-runtime readers (R, Julia, a CLI subprocess) are OPAQUE, not "
    "GROUNDED — the observer instruments Python I/O only.",
    "For a plain file, GROUNDED means the cited file was opened and is non-empty; "
    "the file path proxies flow by size, it does not prove the bytes were read.",
)


def coverage_report() -> dict:
    """Compute the live coverage report for the current environment.

    Installs the wrappers (idempotent, the same install ``observe()`` performs),
    then reports which are active, which third-party loaders are importable but
    inactive, the seam kinds, and the known bounds.
    """
    _loaders.ensure_installed()
    _loaders.refresh_third_party()
    reals = _loaders._reals

    stdlib = [
        {"loader": label, "wrapped": any(k in reals for k in keys)}
        for label, keys in _loaders.STDLIB_WRAPS.items()
    ]
    third_party = []
    for label, keys in _loaders.THIRD_PARTY_WRAPS.items():
        top = keys[0].split(".", 1)[0]
        third_party.append(
            {
                "loader": label,
                "wrapped": any(k in reals for k in keys),
                "importable": top in sys.modules or _is_importable(top),
            }
        )
    # Described kinds first, in the order they are written; any kind the
    # classifier records without a description still gets a row.
    described = [k for k in _SEAM_EFFECTS if k in _scope.SEAM_KINDS]
    rest = sorted(_scope.SEAM_KINDS.difference(_SEAM_EFFECTS))
    return {
        "stdlib_wrapped": stdlib,
        "third_party": third_party,
        "seam_kinds": [
            {"kind": k, "effect": _SEAM_EFFECTS.get(k, _UNNAMED_SEAM)}
            for k in described + rest
        ],
        "known_bounds": list(_KNOWN_BOUNDS),
    }


def _is_importable(name: str) -> bool:
    import importlib.util

    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return False
