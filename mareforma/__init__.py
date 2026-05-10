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
    >>> claim_id = graph.assert_claim("...", classification="ANALYTICAL")
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


def schema() -> dict:
    """Return the mareforma epistemic schema — valid values and state transitions.

    Intended for agents that need to reason about the system before calling it.
    The returned dict is stable across patch releases; fields are only added,
    never removed, within a major version.

    Returns
    -------
    dict with keys:
        schema_version  : int — schema version stored in graph.db
        classifications : list[str] — valid classification values
        support_levels  : list[str] — valid support_level values, ordered low→high
        statuses        : list[str] — valid claim status values
        defaults        : dict — default value for each field at assert_claim() time
        transitions     : list[dict] — valid support_level state transitions

    Example
    -------
    >>> s = mareforma.schema()
    >>> s["classifications"]
    ['INFERRED', 'ANALYTICAL', 'DERIVED']
    >>> s["transitions"]
    [{'from': 'PRELIMINARY', 'to': 'REPLICATED', ...}, ...]
    """
    from mareforma.db import (
        _SCHEMA_VERSION,
        VALID_CLASSIFICATIONS,
        VALID_SUPPORT_LEVELS,
        VALID_STATUSES,
    )

    return {
        "schema_version": _SCHEMA_VERSION,
        "classifications": list(VALID_CLASSIFICATIONS),
        "support_levels": list(VALID_SUPPORT_LEVELS),
        "statuses": list(VALID_STATUSES),
        "defaults": {
            "classification": "INFERRED",
            "support_level": "PRELIMINARY",
            "status": "open",
            "generated_by": "agent",
        },
        "transitions": [
            {
                "from": "PRELIMINARY",
                "to": "REPLICATED",
                "trigger": "automatic",
                "condition": (
                    "≥2 claims with different generated_by share the same "
                    "upstream claim_id in supports[]"
                ),
            },
            {
                "from": "REPLICATED",
                "to": "ESTABLISHED",
                "trigger": "human",
                "condition": "graph.validate(claim_id, validated_by=...) — no automated path",
            },
        ],
    }


__all__ = [
    "open",
    "schema",
    "transform", "registry", "initialize", "BuildContext",
    "__version__",
]