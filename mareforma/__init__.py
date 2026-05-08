"""Mareforma — The epistemic layer AI scientists run on."""

__description__ = "Mareforma — The epistemic layer AI scientists run on."
__version__ = "0.3.0"

from pathlib import Path
from typing import TYPE_CHECKING

from mareforma.transforms import transform, registry
from mareforma.initializer import initialize
from mareforma.pipeline.context import BuildContext

if TYPE_CHECKING:
    from mareforma._graph import EpistemicGraph


def open(path: "str | Path | None" = None) -> "EpistemicGraph":  # noqa: A001
    """Open the epistemic graph at *path* and return an EpistemicGraph.

    Parameters
    ----------
    path:
        Project root directory. Defaults to the current working directory.
        The graph is stored at <path>/.mareforma/graph.db and is created
        on first use.

    Returns
    -------
    EpistemicGraph
        Agent-native interface for asserting and querying claims.
        Use as a context manager to ensure the connection is closed.

    Example
    -------
    >>> graph = mareforma.open()
    >>> claim_id = graph.assert_claim("...", stated_confidence=0.8)
    >>> results = graph.query("...", min_support="REPLICATED")
    >>> graph.close()

    Or with a context manager::

        with mareforma.open() as graph:
            graph.assert_claim("...")
    """
    from mareforma._graph import EpistemicGraph
    from mareforma.db import open_db

    root = Path(path) if path is not None else Path.cwd()
    conn = open_db(root)
    return EpistemicGraph(conn, root)


__all__ = [
    "open",
    "transform", "registry", "initialize", "BuildContext",
    "__version__",
]