"""Read-to-citation binding: does a captured read MATCH the finding's source.

GROUNDED is not "some loader returned data." It is "a read that matches the
finding's cited source returned data." Without this, an incidental read of a
config file, a tokenizer, a ``.env``, or a template through the same wrapped
``open()`` would falsely count as grounding. The match is what makes UNGROUNDED
mean the CITED data did not arrive, not merely that nothing was read.

Two match modes:

- Identifier match (default). The read's normalized identifier equals a cited
  source: same absolute file path, same database connection target, or same
  ``scheme://host/path`` for a URL. Cheap, no hashing of large reads on the
  common path.
- Content-address match (opt-in). When the finding cites a ``sha256:`` data_id,
  a read matches if the content-address of its returned bytes equals that
  data_id. Used when the citation is the dataset's content, not its location.
"""
from __future__ import annotations

import os
from urllib.parse import urlsplit

from ..trust._store import is_content_addressed


def normalize_identifier(raw: str) -> str:
    """Normalize a read target or a cited source into a comparable identifier.

    File paths collapse to an absolute, symlink-resolved form so ``./data.csv``
    and ``/abs/data.csv`` compare equal. URLs collapse to ``scheme://host/path``
    with the query and fragment dropped and the host lowercased, so a cited URL
    matches the read regardless of transient query params. A ``sha256:`` content
    address passes through unchanged (it is already canonical). Anything else
    (a DB connection target, an opaque handle) is returned stripped.
    """
    if not isinstance(raw, str) or not raw:
        return ""
    if is_content_addressed(raw):
        return raw
    parts = urlsplit(raw)
    if parts.scheme in ("http", "https", "ftp") and parts.netloc:
        path = parts.path or "/"
        return f"{parts.scheme}://{parts.netloc.lower()}{path}"
    if parts.scheme in ("file",) and parts.path:
        return _normalize_path(parts.path)
    # A bare path (no scheme, or a Windows drive letter mis-parsed as a scheme).
    if not parts.scheme or len(parts.scheme) == 1:
        return _normalize_path(raw)
    # A DB URL or other opaque target: lowercase the scheme+netloc, keep path.
    return raw.strip()


def _normalize_path(path: str) -> str:
    """Absolute, normalized filesystem path. realpath when it resolves, else
    a lexical absolute normalization (a not-yet-existing path still compares).
    """
    try:
        return os.path.realpath(path)
    except OSError:
        return os.path.normpath(os.path.abspath(path))


def cited_set(cites) -> tuple[str, ...]:
    """Normalize the caller's cited sources into a comparable tuple.

    Accepts a single string or an iterable of strings. Empty / non-string
    entries are dropped. Order is preserved for a stable receipt but matching
    is order-free.
    """
    if cites is None:
        return ()
    if isinstance(cites, (str, bytes)):
        items = [cites]
    else:
        try:
            items = list(cites)
        except TypeError:
            items = [cites]
    out: list[str] = []
    for c in items:
        if isinstance(c, bytes):
            c = c.decode("utf-8", "replace")
        norm = normalize_identifier(c) if isinstance(c, str) else ""
        if norm:
            out.append(norm)
    return tuple(out)


# File extensions whose bytes are read through a C runtime (HDF5, netCDF, Arrow),
# which opens via the C library's own open(2)/fopen and emits NO Python PEP-578
# audit event. A cited read of one is invisible to the open-coverage hook, so its
# absence cannot be trusted: the classifier floors it to OPAQUE unless a wrapped
# reader recorded the read directly. The formats of natural-science pipelines.
_C_EXTENSION_SUFFIXES: frozenset[str] = frozenset(
    {".h5", ".hdf5", ".he5", ".nc", ".nc4", ".cdf", ".parquet", ".pq",
     ".feather", ".arrow"}
)


def citation_kind(identifier: str) -> str:
    """Classify a normalized cited identifier into a coverage kind.

    - ``"content-address"``, a ``sha256:`` data_id. Its bytes can arrive over any
      channel (disk or network), so a socket seam is relevant to it.
    - ``"url"``, an ``http``/``https``/``ftp`` location. Delivered over a socket.
    - ``"c-extension-file"``, a local path whose suffix is read through a C
      runtime, invisible to the PEP-578 open hook.
    - ``"file"``, any other local path. An in-process read of it hits the open
      audit event, so a socket seam cannot have hidden it.
    - ``"unknown"``, anything else (an opaque DB target); treated fail-closed.
    """
    if not isinstance(identifier, str) or not identifier:
        return "unknown"
    if is_content_addressed(identifier):
        return "content-address"
    parts = urlsplit(identifier)
    if parts.scheme in ("http", "https", "ftp") and parts.netloc:
        return "url"
    if identifier.startswith("/") or (parts.scheme in ("", "file")):
        lower = identifier.lower()
        if any(lower.endswith(suf) for suf in _C_EXTENSION_SUFFIXES):
            return "c-extension-file"
        return "file"
    return "unknown"


def read_norm_matches(norm_read: str, read_content_address, cited) -> bool:
    """True iff an ALREADY-normalized read identifier binds to a cited source.

    The cited set is normalized once at citation time, so matching is a plain
    comparison. Callers that hold the normalized read identifier (the classifier,
    which normalizes each read once per scope) use this directly to avoid
    re-running ``os.path.realpath`` on every read on every pass.
    """
    for c in cited:
        if norm_read and norm_read == c:
            return True
        if (
            read_content_address
            and is_content_addressed(c)
            and read_content_address == c
        ):
            return True
    return False


def read_matches_citation(read_identifier: str, read_content_address, cited) -> bool:
    """True iff this read binds to one of the cited sources.

    Identifier match on the normalized read identifier, OR content-address
    match when the read carries a ``sha256:`` digest and a cited source is that
    same content address. Either mode is sufficient; both are checked so a
    location-cited source and a content-cited source both resolve.
    """
    return read_norm_matches(
        normalize_identifier(read_identifier), read_content_address, cited
    )
