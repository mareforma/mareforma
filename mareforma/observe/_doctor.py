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

from . import _loaders

# The loaders the observer wraps, grouped for the report. Keys are the entries
# that appear in ``_loaders._reals`` once wrapped; the label is human-facing.
_STDLIB_WRAPS = {
    "open": "builtins.open (file reads)",
    "sqlite3.connect": "sqlite3 (query rows)",
}
_THIRD_PARTY_WRAPS = {
    "pandas.read_csv": "pandas readers",
    "requests.get": "requests.get",
    "requests.Session.get": "requests.Session (pooled)",
    "httpx.get": "httpx.get",
    "httpx.Client.get": "httpx.Client / AsyncClient (pooled)",
    "aiohttp.ClientSession._request": "aiohttp (recorded as a network seam)",
    "h5py.File": "h5py (HDF5)",
    "pyarrow.parquet.read_table": "pyarrow (Parquet / Arrow)",
    "netCDF4.Dataset": "netCDF4",
}

# The seam kinds the classifier can raise, and what each means for a verdict.
_SEAM_KINDS = {
    "socket": "network read; forces OPAQUE for URL / content-address citations",
    "subprocess": "child process; forces OPAQUE (see attach for coverage)",
    "thread": "library thread; forces OPAQUE",
    "coverage-gap": "an uninstrumented or C-runtime reader; forces OPAQUE",
}

_KNOWN_BOUNDS = (
    "A resource opened BEFORE the scope (a module-level or pooled handle other "
    "than the wrapped HTTP sessions) is not swapped, so its reads are invisible.",
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
        {"loader": label, "wrapped": key in reals}
        for key, label in _STDLIB_WRAPS.items()
    ]
    third_party = []
    for key, label in _THIRD_PARTY_WRAPS.items():
        top = key.split(".", 1)[0]
        third_party.append(
            {
                "loader": label,
                "wrapped": key in reals,
                "importable": top in sys.modules or _is_importable(top),
            }
        )
    return {
        "stdlib_wrapped": stdlib,
        "third_party": third_party,
        "seam_kinds": [{"kind": k, "effect": v} for k, v in _SEAM_KINDS.items()],
        "known_bounds": list(_KNOWN_BOUNDS),
    }


def _is_importable(name: str) -> bool:
    import importlib.util

    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return False
