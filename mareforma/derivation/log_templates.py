"""
log_templates: Drain-style log-template extraction engine.

Production-grade pure-Python Drain algorithm (He et al., ICWS 2017) that
reduces unstructured log lines to parameterized templates deterministically.

Features:
  - Fixed-depth parse tree with configurable depth (default 4)
  - Token-count-based bucketing, similarity threshold for cluster merging
  - Built-in masking for timestamps, UUIDs, hex strings, IP addresses,
    file paths, numeric values, URLs
  - Template-set serialization: deterministic JSON with content-addressed
    digest (SHA-256)
  - Data-access signal extraction: DB queries, HTTP requests, file I/O, API calls
  - Streaming interface: process log lines one-at-a-time for memory efficiency
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field

# ---------- Pre-processing masks ----------

# Order matters: longer / more specific patterns first.
_MASKS: list[tuple[re.Pattern[str], str]] = [
    # ISO-8601 timestamps (with or without timezone)
    (re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"), "<*>"),
    # UUIDs
    (re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"), "<*>"),
    # Hex strings (8+ chars)
    (re.compile(r"\b[0-9a-fA-F]{8,}\b"), "<*>"),
    # IP addresses (v4)
    (re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"), "<*>"),
    # URLs (http/https)
    (re.compile(r"https?://\S+"), "<*>"),
    # File paths (Unix-style, at least two segments)
    (re.compile(r"(?:/[\w.~-]+){2,}"), "<*>"),
    # Standalone numbers (integers and floats)
    (re.compile(r"\b\d+(?:\.\d+)?\b"), "<*>"),
]

# Data-access signal keywords detected in templates
_DATA_ACCESS_KEYWORDS: list[str] = [
    "select", "insert", "update", "delete", "from",  # SQL
    "http", "get", "post", "put", "patch",  # HTTP
    "returned", "response", "status",  # HTTP responses
    "query", "fetch", "connect", "connection",  # Database
    "read", "write", "open", "file",  # File I/O
]


@dataclass
class LogTemplate:
    """A parameterized log template."""

    pattern: str
    count: int = 1
    is_data_access: bool = False


@dataclass
class TemplateResult:
    """Result of log-template extraction."""

    templates: list[LogTemplate] = field(default_factory=list)
    digest: str = ""

    def to_json(self) -> str:
        """Serialize to deterministic JSON with content-addressed digest.

        Returns a JSON string with sorted keys and compact separators.
        The ``digest`` field is SHA-256 of the canonical templates payload.
        """
        templates_payload = [
            {"pattern": t.pattern, "count": t.count, "is_data_access": t.is_data_access}
            for t in self.templates
        ]
        return json.dumps(
            {"templates": templates_payload, "digest": self.digest},
            sort_keys=True,
            separators=(",", ":"),
        )


def _preprocess(line: str) -> str:
    """Mask variable parts of a log line."""
    result = line.strip()
    for pattern, replacement in _MASKS:
        result = pattern.sub(replacement, result)
    return result


def _collapse_placeholders(text: str) -> str:
    """Collapse consecutive <*> tokens into one."""
    return re.sub(r"(<\*>\s*)+", "<*> ", text).strip()


def _tokenize(line: str) -> list[str]:
    """Split a preprocessed line into tokens."""
    return line.split()


def _compute_similarity(tokens_a: list[str], tokens_b: list[str]) -> float:
    """Compute token-level similarity between two token sequences."""
    if len(tokens_a) != len(tokens_b):
        return 0.0
    if not tokens_a:
        return 1.0
    matches = sum(1 for a, b in zip(tokens_a, tokens_b) if a == b)
    return matches / len(tokens_a)


def _merge_templates(tokens_a: list[str], tokens_b: list[str]) -> list[str]:
    """Merge two token sequences into a template (differing tokens become <*>)."""
    return [a if a == b else "<*>" for a, b in zip(tokens_a, tokens_b)]


def _is_data_access_template(pattern: str) -> bool:
    """Check whether a template pattern contains data-access signals."""
    lower = pattern.lower()
    return any(kw in lower for kw in _DATA_ACCESS_KEYWORDS)


def _compute_digest(templates: list[LogTemplate]) -> str:
    """Compute SHA-256 digest of the canonical JSON template representation."""
    digest_payload = json.dumps(
        [{"pattern": t.pattern, "count": t.count} for t in templates],
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(digest_payload.encode("utf-8")).hexdigest()


class _DrainNode:
    """Internal Drain parse-tree node."""

    __slots__ = ("children", "clusters")

    def __init__(self) -> None:
        self.children: dict[str, _DrainNode] = {}
        self.clusters: list[tuple[list[str], int]] = []


class _DrainCore:
    """Internal fixed-depth Drain core.

    Depth=4 (configurable). Tokens at depth positions are used as tree
    keys; leaf nodes hold cluster lists. Similarity threshold=0.5
    for merging into an existing cluster.
    """

    def __init__(self, depth: int = 4, sim_threshold: float = 0.5) -> None:
        self._depth = depth
        self._sim_threshold = sim_threshold
        # Buckets keyed by token count
        self._buckets: dict[int, _DrainNode] = {}

    def add_tokens(self, tokens: list[str]) -> None:
        """Add a preprocessed, tokenized log line to the parse tree."""
        if not tokens:
            return

        n = len(tokens)
        if n not in self._buckets:
            self._buckets[n] = _DrainNode()

        node = self._buckets[n]

        # Walk fixed-depth prefix
        for i in range(min(self._depth, n)):
            tok = tokens[i]
            key = tok if tok != "<*>" else "<*>"
            if key not in node.children:
                node.children[key] = _DrainNode()
            node = node.children[key]

        # Try to merge with an existing cluster at the leaf
        best_idx = -1
        best_sim = -1.0
        for idx, (cluster_tokens, _count) in enumerate(node.clusters):
            sim = _compute_similarity(cluster_tokens, tokens)
            if sim > best_sim:
                best_sim = sim
                best_idx = idx

        if best_sim >= self._sim_threshold and best_idx >= 0:
            old_tokens, old_count = node.clusters[best_idx]
            merged = _merge_templates(old_tokens, tokens)
            node.clusters[best_idx] = (merged, old_count + 1)
        else:
            node.clusters.append((list(tokens), 1))

    def get_clusters(self) -> list[tuple[str, int]]:
        """Return all clusters as (pattern_string, count) pairs."""
        results: list[tuple[str, int]] = []
        self._collect(self._buckets, results)
        # Sort for determinism
        results.sort(key=lambda x: (x[0], x[1]))
        return results

    def _collect(
        self,
        obj: dict[int, _DrainNode] | _DrainNode,
        out: list[tuple[str, int]],
    ) -> None:
        if isinstance(obj, dict):
            for _key in sorted(obj.keys()):
                self._collect(obj[_key], out)
        else:
            for tokens, count in obj.clusters:
                pattern = _collapse_placeholders(" ".join(tokens))
                out.append((pattern, count))
            for _key in sorted(obj.children.keys()):
                self._collect(obj.children[_key], out)


class DrainParser:
    """Public streaming Drain parser.

    Processes log lines one-at-a-time for memory efficiency.
    Call ``add_log_line(raw_line)`` for each log line, then
    ``get_result()`` to obtain the ``TemplateResult``.

    ``get_result()`` can be called at any point to get an intermediate
    snapshot -- useful for monitoring or early stopping.

    Args:
        depth: Drain parse-tree depth (default 4).
        sim_threshold: Similarity threshold for cluster merging (default 0.5).
    """

    def __init__(self, depth: int = 4, sim_threshold: float = 0.5) -> None:
        self._core = _DrainCore(depth=depth, sim_threshold=sim_threshold)

    def add_log_line(self, raw_line: str) -> None:
        """Preprocess, tokenize, and add a single raw log line."""
        preprocessed = _preprocess(raw_line)
        tokens = _tokenize(preprocessed)
        if tokens:
            self._core.add_tokens(tokens)

    def get_result(self) -> TemplateResult:
        """Return the current TemplateResult (deterministic snapshot).

        Safe to call multiple times -- returns a fresh snapshot each call.
        """
        clusters = self._core.get_clusters()
        if not clusters:
            empty_digest = hashlib.sha256(b"[]").hexdigest()
            return TemplateResult(templates=[], digest=empty_digest)

        templates: list[LogTemplate] = []
        for pattern, count in clusters:
            templates.append(
                LogTemplate(
                    pattern=pattern,
                    count=count,
                    is_data_access=_is_data_access_template(pattern),
                )
            )
        return TemplateResult(templates=templates, digest=_compute_digest(templates))


def extract_templates(
    lines: list[str],
    *,
    depth: int = 4,
    sim_threshold: float = 0.5,
) -> TemplateResult:
    """Extract parameterized log templates from raw log lines.

    Args:
        lines: Raw log lines (stdout/stderr).
        depth: Drain parse-tree depth (default 4).
        sim_threshold: Similarity threshold for cluster merging (default 0.5).

    Returns:
        TemplateResult with deterministic templates and a content-addressed digest.
    """
    if not lines:
        empty_digest = hashlib.sha256(b"[]").hexdigest()
        return TemplateResult(templates=[], digest=empty_digest)

    parser = DrainParser(depth=depth, sim_threshold=sim_threshold)
    for line in lines:
        parser.add_log_line(line)

    return parser.get_result()
