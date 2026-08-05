"""Optional adapter packages bridging external platforms into mareforma.

Every submodule under ``mareforma.adapters`` runs on core
dependencies, so none needs an install extra. Importing a submodule
registers the adapter's predicate URIs and provides an
:class:`EventSource` (per :mod:`mareforma.events`) or tool-wrapper
ergonomic that emits signed mareforma claims.

The adapter framework keeps mareforma slim: ``mareforma`` core
has no opinion on which AI platforms exist, and an adapter costs
nothing until a caller imports it.
"""

__all__: list[str] = []
