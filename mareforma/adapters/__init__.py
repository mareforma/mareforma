"""Optional adapter packages bridging external platforms into mareforma.

Each submodule under ``mareforma.adapters`` is gated behind a
matching install extra: ``mareforma[clawinstitute]``,
``mareforma[tooluniverse]``, ``mareforma[gemini]``. Installing the
extra pulls the platform-specific dependencies; importing the
submodule registers the adapter's predicate URIs and provides an
:class:`EventSource` (per :mod:`mareforma.events`) or tool-wrapper
ergonomic that emits signed mareforma claims.

The adapter framework keeps mareforma slim: ``mareforma`` core
has no opinion on which AI platforms exist, and adding a new adapter
does not bloat the default install.
"""

__all__: list[str] = []
