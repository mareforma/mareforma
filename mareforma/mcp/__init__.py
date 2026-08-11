"""Model Context Protocol surface for mareforma: read and verify, never write.

An agent reaches one mareforma project over MCP to look claims up and audit
them. It cannot assert a claim across that boundary: a claim written over a
transport carries no observed grounding, and the record exists to hold claims to
the grounding they earned, so the server exposes query and verify tools and no
write path at all. That refusal is a designed bound, not a missing feature.

The server lives behind the optional ``mcp`` extra
(``pip install mareforma[mcp]``) so the base install stays light. Import it only
through :func:`run_server`, which the ``mareforma mcp serve`` command calls.
"""
from __future__ import annotations

from .server import MCPServerError, run_server

__all__ = ["run_server", "MCPServerError"]
