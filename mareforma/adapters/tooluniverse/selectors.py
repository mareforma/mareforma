"""Selective tool wrapping: which tools get provenance, which pass through.

Phase 2 ships the substantive / skip / unknown three-bucket rule. Tools
declare a ``category`` attribute (matching ToolUniverse's tool_config
``category`` field); the default selector accepts the wrap or skip
decision from the bucket the category lands in, and warns when a
category is unknown (default-wrap with a flag the operator should
audit).
"""

from __future__ import annotations

import warnings
from typing import Any


__all__ = [
    "SUBSTANTIVE_CATEGORIES",
    "SKIP_CATEGORIES",
    "UnknownCategoryWarning",
    "default_should_wrap",
]


# Substantive categories — tools that produce information whose
# provenance is load-bearing for downstream agent reasoning.
SUBSTANTIVE_CATEGORIES: frozenset[str] = frozenset({
    "biological",
    "medical",
    "literature",
    "pharmacology",
    "genomics",
    "cheminformatics",
    "structure",
    "ml_inference",
    "analysis",
    # Common ToolUniverse aliases.
    "biology",
    "drug",
    "disease",
    "compound",
    "target",
    "protein",
    "OpenTargets",
})


# Skip categories — tools that just format / display / hook existing
# results. Wrapping them would clutter the graph with claims that are
# not load-bearing.
SKIP_CATEGORIES: frozenset[str] = frozenset({
    "hook",
    "format",
    "display",
    "summarization",
    "discovery",  # the tool finder itself; its output is metadata, not findings
    "metadata",
})


class UnknownCategoryWarning(UserWarning):
    """Emitted when a tool's category is not in either bucket.

    The selector defaults to *wrapping* unknown categories (safer
    default, record provenance for tools we don't yet recognise), but
    the warning lets the operator audit the gap and either add the
    category to SUBSTANTIVE_CATEGORIES or override the selector.
    """


def default_should_wrap(tool: Any) -> bool:
    """Return whether ``tool`` should be wrapped by ProvenanceToolAdapter.

    Reads ``tool.category`` (matching the ToolUniverse tool_config
    field). Returns:

    - ``True`` if category is in SUBSTANTIVE_CATEGORIES.
    - ``False`` if category is in SKIP_CATEGORIES.
    - ``True`` with an :class:`UnknownCategoryWarning` if the category
      is unknown OR the tool has no category attribute.

    The third bucket is the safer default: record provenance for what
    we don't recognise; operators can override the selector per call site
    when they decide a category is genuinely skip-class.
    """

    category = getattr(tool, "category", None)
    if category in SUBSTANTIVE_CATEGORIES:
        return True
    if category in SKIP_CATEGORIES:
        return False
    name = getattr(tool, "name", "<unnamed>")
    warnings.warn(
        f"unknown category {category!r} for tool {name!r}; "
        "defaulting to wrap. Add to SUBSTANTIVE_CATEGORIES or override "
        "the selector to silence.",
        UnknownCategoryWarning,
        stacklevel=2,
    )
    return True
