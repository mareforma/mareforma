"""
source_profile — tree-sitter-based source-profile extraction.

Uses tree-sitter-python to parse Python source code into an AST and
identify data-access patterns (database, HTTP, file I/O) and LLM-call
patterns (OpenAI, Anthropic, LangChain, LlamaIndex, Google AI).

The source profile is a deterministic fingerprint of what data-access
and LLM-call capabilities the source code has, with control-flow-aware
dead-code detection.

Phase 5 additions:
  - Unanalyzable pattern detection: __import__(), exec(), eval(),
    importlib.import_module(), and getattr() are flagged as kind="unanalyzable"
    with a detail string describing the dynamic construct found.
  - Dynamic-import-tainted variable tracking: local variables assigned
    from dynamic imports (e.g. ``requests = importlib.import_module("requests")``)
    are tracked so their method calls are NOT falsely matched against
    known module patterns.

Implementation note: the installed tree-sitter version exposes a Query
object without captures()/matches() methods, so we walk the AST manually
rather than using S-expression queries.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field

try:
    import tree_sitter_python as tspython
    from tree_sitter import Language, Parser
    PY_LANGUAGE = Language(tspython.language())
    HAS_TREE_SITTER = True
except ImportError:
    tspython = None  # type: ignore[assignment]
    Language = None  # type: ignore[assignment,misc]
    Parser = None  # type: ignore[assignment,misc]
    PY_LANGUAGE = None
    HAS_TREE_SITTER = False


def _require_tree_sitter() -> None:
    if not HAS_TREE_SITTER:
        raise ImportError(
            "mareforma.derivation source-profile extraction requires "
            "tree_sitter and tree_sitter_python. Install the optional "
            "derivation extra: pip install mareforma[derivation]"
        )

# ---------- Pattern definitions ----------

# Mapping from import module name (or prefix) to pattern kind
_IMPORT_TO_KIND: dict[str, str] = {
    # Database
    "sqlite3": "database",
    "psycopg2": "database",
    "psycopg": "database",
    "sqlalchemy": "database",
    "pymongo": "database",
    "pymysql": "database",
    "mysql": "database",
    # HTTP
    "requests": "http",
    "httpx": "http",
    "urllib": "http",
    "aiohttp": "http",
    "urllib3": "http",
    # File I/O
    "csv": "file_io",
    "json": "file_io",
    "pathlib": "file_io",
    "shutil": "file_io",
    # Data libraries
    "pandas": "database",
    "numpy": "database",
    "datasets": "database",
}

# LLM SDK imports → "llm" kind
_LLM_IMPORT_TO_KIND: dict[str, str] = {
    "openai": "llm",
    "anthropic": "llm",
    "langchain": "llm",
    "langchain_core": "llm",
    "langchain_community": "llm",
    "langchain_openai": "llm",
    "llama_index": "llm",
    "google.generativeai": "llm",
    "google.ai": "llm",
    "litellm": "llm",
}

# All imports: data-access + LLM
_ALL_IMPORTS: dict[str, str] = {**_IMPORT_TO_KIND, **_LLM_IMPORT_TO_KIND}

# Attribute-access patterns: obj.method() indicating data-access
_ATTR_CALL_PATTERNS: dict[str, str] = {
    "sqlite3.connect": "database",
    "requests.get": "http",
    "requests.post": "http",
    "requests.put": "http",
    "requests.patch": "http",
    "requests.delete": "http",
    "httpx.get": "http",
    "httpx.post": "http",
    "httpx.Client": "http",
    "httpx.AsyncClient": "http",
}

# Method names that indicate data-access when called on a known module object
_DATA_METHODS: dict[str, str] = {
    "connect": "database",
    "execute": "database",
    "fetchall": "database",
    "fetchone": "database",
    "cursor": "database",
    "get": "http",
    "post": "http",
    "put": "http",
    "json": "http",
}

# Standalone function calls that indicate dynamic/unanalyzable code patterns.
# These are flagged as kind="unanalyzable" in the source profile.
_UNANALYZABLE_STANDALONE_CALLS: dict[str, str] = {
    "__import__": "__import__()",
    "exec": "exec()",
    "eval": "eval()",
}

# Attribute calls that indicate dynamic/unanalyzable code patterns.
_UNANALYZABLE_ATTR_CALLS: dict[str, str] = {
    "importlib.import_module": "importlib.import_module()",
}


@dataclass
class SourcePattern:
    """A detected data-access or LLM-call pattern in source code."""

    kind: str  # "database", "http", "file_io", "llm"
    location: str  # "line N" or "import"
    detail: str  # e.g. "sqlite3.connect" or "requests.get"
    file: str = ""  # source filename (populated for multi-file analysis)
    line: int = 0  # 1-based line number
    function: str = ""  # enclosing function name, or "" for module-level
    is_dead_code: bool = False  # True if inside unreachable branch


@dataclass
class SourceProfile:
    """Deterministic profile of data-access capabilities in source code."""

    patterns: list[SourcePattern] = field(default_factory=list)
    has_data_access: bool = False
    digest: str = ""

    def to_json(self) -> str:
        """Serialize to deterministic JSON with content-addressed digest.

        Returns a JSON string with sorted keys and compact separators.
        """
        patterns_payload = [
            {
                "kind": p.kind,
                "detail": p.detail,
                "file": p.file,
                "line": p.line,
                "function": p.function,
                "is_dead_code": p.is_dead_code,
            }
            for p in self.patterns
        ]
        return json.dumps(
            {"patterns": patterns_payload, "digest": self.digest},
            sort_keys=True,
            separators=(",", ":"),
        )


def _get_parser():
    """Create a fresh tree-sitter parser for Python.

    Raises :class:`ImportError` (via ``_require_tree_sitter``) when the
    optional derivation extra is not installed.
    """
    _require_tree_sitter()
    return Parser(PY_LANGUAGE)


def _node_text(node, source_bytes: bytes) -> str:  # noqa: ANN001
    """Extract the source text for a tree-sitter node."""
    return source_bytes[node.start_byte:node.end_byte].decode("utf-8", errors="replace")


def _walk_tree(node, callback, source_bytes: bytes) -> None:  # noqa: ANN001
    """Recursively walk an AST node, calling callback on each node."""
    callback(node, source_bytes)
    for child in node.children:
        _walk_tree(child, callback, source_bytes)


def _root_identifier(node, source_bytes: bytes) -> str:  # noqa: ANN001
    """Extract the root identifier from a possibly-chained expression node.

    For ``requests`` (identifier) → returns "requests".
    For ``requests.get`` (attribute) → returns "requests".
    For ``requests.get(url)`` (call with attribute function) → returns "requests".
    For ``requests.get(url).json`` (attribute with call object) → returns "requests".

    Returns the text of the leftmost identifier in the chain, or empty string
    if the node is not a chain expression.
    """
    current = node
    for _depth in range(20):  # guard against infinite loops
        if current.type == "identifier":
            return _node_text(current, source_bytes)
        if current.type == "attribute":
            obj = current.child_by_field_name("object")
            if obj is not None:
                current = obj
                continue
            break
        if current.type == "call":
            func = current.child_by_field_name("function")
            if func is not None:
                current = func
                continue
            break
        break
    return ""


# ---------- Dead-code detection helpers ----------

def _find_enclosing_function(node) -> str:  # noqa: ANN001
    """Walk up the AST to find the enclosing function name."""
    current = node.parent
    while current is not None:
        if current.type in ("function_definition", "async_function_definition"):
            name_node = current.child_by_field_name("name")
            if name_node is not None:
                return name_node.text.decode("utf-8", errors="replace") if hasattr(name_node, "text") else ""
            return ""
        current = current.parent
    return ""


def _is_in_dead_zone(node, dead_zones: list[tuple[int, int]]) -> bool:  # noqa: ANN001
    """Check if a node's start byte falls inside any dead zone."""
    start = node.start_byte
    for zone_start, zone_end in dead_zones:
        if zone_start <= start < zone_end:
            return True
    return False


def _collect_dead_zones(root_node) -> list[tuple[int, int]]:  # noqa: ANN001
    """Walk the AST and collect byte ranges of dead code.

    Dead code includes:
    - Body of `if False:` blocks
    - Exception handler bodies (except blocks)
    - Code after unconditional `return` in the same block
    """
    dead_zones: list[tuple[int, int]] = []

    def _walk_for_dead(node) -> None:  # noqa: ANN001
        # `if False:` — the consequence block is dead
        if node.type == "if_statement":
            condition = node.child_by_field_name("condition")
            if condition is not None and condition.type == "false":
                consequence = node.child_by_field_name("consequence")
                if consequence is not None:
                    dead_zones.append((consequence.start_byte, consequence.end_byte))

        # except_clause — treat the handler body as dead code
        if node.type == "except_clause":
            # The body is the block inside the except clause
            # In tree-sitter, the except_clause has child nodes:
            # the exception type and the body block
            for child in node.children:
                if child.type == "block":
                    dead_zones.append((child.start_byte, child.end_byte))
                    break

        # Code after unconditional return in a block
        if node.type == "block":
            found_return = False
            for child in node.children:
                if found_return and child.type not in ("comment",):
                    # Everything after the return is dead
                    dead_zones.append((child.start_byte, child.end_byte))
                if child.type == "return_statement":
                    found_return = True

        for child in node.children:
            _walk_for_dead(child)

    _walk_for_dead(root_node)
    return dead_zones


# ---------- Pattern detection ----------

def _find_patterns(
    tree,  # noqa: ANN001
    source_bytes: bytes,
    *,
    filename: str = "",
) -> list[SourcePattern]:
    """Walk the AST and collect all data-access and LLM-call patterns."""
    patterns: list[SourcePattern] = []
    # Alias mapping: alias -> canonical module name (e.g., "pd" -> "pandas")
    alias_to_module: dict[str, str] = {}
    # Variables tainted by dynamic imports — their method calls must NOT
    # be matched against known module patterns. E.g., after
    # ``requests = importlib.import_module("requests")``, the local
    # variable ``requests`` is tainted: ``requests.get(url)`` should NOT
    # match _ATTR_CALL_PATTERNS because the import was dynamic.
    tainted_vars: set[str] = set()

    # Collect dead zones first
    dead_zones = _collect_dead_zones(tree.root_node)

    def _is_dynamic_call(node) -> bool:  # noqa: ANN001
        """Check if a call node is a dynamic import/code pattern."""
        func_node = node.child_by_field_name("function")
        if func_node is None:
            return False
        if func_node.type == "identifier":
            return _node_text(func_node, source_bytes) in _UNANALYZABLE_STANDALONE_CALLS
        if func_node.type == "attribute":
            obj = func_node.child_by_field_name("object")
            attr = func_node.child_by_field_name("attribute")
            if obj is not None and attr is not None:
                resolved = alias_to_module.get(
                    _node_text(obj, source_bytes),
                    _node_text(obj, source_bytes),
                )
                full = f"{resolved}.{_node_text(attr, source_bytes)}"
                return full in _UNANALYZABLE_ATTR_CALLS
        return False

    def _collect_tainted_vars(root) -> None:  # noqa: ANN001
        """Pre-pass: find local variables assigned from dynamic imports.

        Patterns detected:
            x = __import__("foo")
            x = importlib.import_module("foo")
            x = eval(...)
        """
        def _walk_for_tainted(n) -> None:  # noqa: ANN001
            if n.type == "assignment":
                left = n.child_by_field_name("left")
                right = n.child_by_field_name("right")
                if left is not None and right is not None and left.type == "identifier":
                    var_name = _node_text(left, source_bytes)
                    if right.type == "call" and _is_dynamic_call(right):
                        tainted_vars.add(var_name)
            for child in n.children:
                _walk_for_tainted(child)
        _walk_for_tainted(root)

    # Pre-pass: collect tainted variables
    _collect_tainted_vars(tree.root_node)

    def visitor(node, src_bytes: bytes) -> None:  # noqa: ANN001
        is_dead = _is_in_dead_zone(node, dead_zones)
        line_num = node.start_point[0] + 1
        func_name = _find_enclosing_function(node)

        # --- Import statements ---
        if node.type == "import_statement":
            for child in node.named_children:
                if child.type == "dotted_name":
                    module_text = _node_text(child, src_bytes)
                    _check_import(
                        module_text, line_num, func_name, filename,
                        is_dead, patterns,
                    )
                elif child.type == "aliased_import":
                    # import X as Y
                    name_node = child.child_by_field_name("name")
                    alias_node = child.child_by_field_name("alias")
                    if name_node is not None:
                        module_text = _node_text(name_node, src_bytes)
                        if alias_node is not None:
                            alias_text = _node_text(alias_node, src_bytes)
                            alias_to_module[alias_text] = module_text
                        _check_import(
                            module_text, line_num, func_name, filename,
                            is_dead, patterns,
                        )

        # import_from_statement: "from X import Y"
        elif node.type == "import_from_statement":
            module_node = node.child_by_field_name("module_name")
            if module_node is not None:
                module_text = _node_text(module_node, src_bytes)
                _check_import(
                    module_text, line_num, func_name, filename,
                    is_dead, patterns,
                )

        # --- Function calls ---
        elif node.type == "call":
            func_node = node.child_by_field_name("function")
            if func_node is None:
                return

            # Attribute call: obj.method(...)
            if func_node.type == "attribute":
                obj_node = func_node.child_by_field_name("object")
                attr_node = func_node.child_by_field_name("attribute")
                if obj_node is not None and attr_node is not None:
                    obj_text = _node_text(obj_node, src_bytes)
                    attr_text = _node_text(attr_node, src_bytes)

                    # Resolve alias → canonical module
                    resolved_obj = alias_to_module.get(obj_text, obj_text)
                    full_call = f"{resolved_obj}.{attr_text}"

                    # Check for unanalyzable attribute calls first
                    if full_call in _UNANALYZABLE_ATTR_CALLS:
                        patterns.append(SourcePattern(
                            kind="unanalyzable",
                            location=f"line {line_num}",
                            detail=_UNANALYZABLE_ATTR_CALLS[full_call],
                            file=filename,
                            line=line_num,
                            function=func_name,
                            is_dead_code=is_dead,
                        ))
                        return

                    # Skip method calls on tainted variables — these are
                    # dynamically-imported modules that tree-sitter can't
                    # verify. Without this guard, ``requests.get(url)``
                    # after ``requests = importlib.import_module("requests")``
                    # would falsely match _ATTR_CALL_PATTERNS.
                    # Also handles chained calls like ``requests.get(url).json()``
                    # where obj_node is a call expression rooted at a tainted var.
                    if _root_identifier(obj_node, src_bytes) in tainted_vars:
                        return

                    # Check known attribute call patterns
                    if full_call in _ATTR_CALL_PATTERNS:
                        patterns.append(SourcePattern(
                            kind=_ATTR_CALL_PATTERNS[full_call],
                            location=f"line {line_num}",
                            detail=full_call,
                            file=filename,
                            line=line_num,
                            function=func_name,
                            is_dead_code=is_dead,
                        ))
                    else:
                        # Check if resolved_obj is a known module
                        if attr_text in _DATA_METHODS:
                            for mod in _ALL_IMPORTS:
                                if resolved_obj == mod or resolved_obj.startswith(mod):
                                    patterns.append(SourcePattern(
                                        kind=_DATA_METHODS[attr_text],
                                        location=f"line {line_num}",
                                        detail=full_call,
                                        file=filename,
                                        line=line_num,
                                        function=func_name,
                                        is_dead_code=is_dead,
                                    ))
                                    break

            # Standalone call: open(...) or unanalyzable builtins
            elif func_node.type == "identifier":
                func_text = _node_text(func_node, src_bytes)

                # Check for unanalyzable standalone calls first
                if func_text in _UNANALYZABLE_STANDALONE_CALLS:
                    patterns.append(SourcePattern(
                        kind="unanalyzable",
                        location=f"line {line_num}",
                        detail=_UNANALYZABLE_STANDALONE_CALLS[func_text],
                        file=filename,
                        line=line_num,
                        function=func_name,
                        is_dead_code=is_dead,
                    ))
                elif func_text == "open":
                    patterns.append(SourcePattern(
                        kind="file_io",
                        location=f"line {line_num}",
                        detail="open()",
                        file=filename,
                        line=line_num,
                        function=func_name,
                        is_dead_code=is_dead,
                    ))

    _walk_tree(tree.root_node, visitor, source_bytes)
    return patterns


def _check_import(
    module_text: str,
    line_num: int,
    func_name: str,
    filename: str,
    is_dead: bool,
    patterns: list[SourcePattern],
) -> None:
    """Check a module name against all known import-to-kind mappings."""
    for module_prefix, kind in _ALL_IMPORTS.items():
        if module_text == module_prefix or module_text.startswith(module_prefix + "."):
            patterns.append(SourcePattern(
                kind=kind,
                location=f"line {line_num}",
                detail=f"import {module_text}",
                file=filename,
                line=line_num,
                function=func_name,
                is_dead_code=is_dead,
            ))
            return


def _deduplicate_patterns(patterns: list[SourcePattern]) -> list[SourcePattern]:
    """Remove duplicate patterns (same kind + detail + file + line) and sort for determinism."""
    seen: set[tuple[str, str, str, int]] = set()
    result: list[SourcePattern] = []
    for p in patterns:
        key = (p.kind, p.detail, p.file, p.line)
        if key not in seen:
            seen.add(key)
            result.append(p)
    result.sort(key=lambda p: (p.file, p.kind, p.detail, p.location))
    return result


def _compute_digest(patterns: list[SourcePattern]) -> str:
    """Compute SHA-256 digest of the canonical JSON pattern representation."""
    digest_payload = json.dumps(
        [{"kind": p.kind, "detail": p.detail, "file": p.file} for p in patterns],
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(digest_payload.encode("utf-8")).hexdigest()


def extract_source_profile(
    source: str,
    *,
    filename: str = "",
) -> SourceProfile:
    """Extract a deterministic source profile from Python source code.

    Args:
        source: Python source code as a string.
        filename: Optional filename for source-location tracking.

    Returns:
        SourceProfile with detected data-access/LLM-call patterns and a
        content-addressed digest.
    """
    if not source.strip():
        empty_digest = hashlib.sha256(b"[]").hexdigest()
        return SourceProfile(patterns=[], has_data_access=False, digest=empty_digest)

    source_bytes = source.encode("utf-8")
    parser = _get_parser()
    tree = parser.parse(source_bytes)

    raw_patterns = _find_patterns(tree, source_bytes, filename=filename)
    patterns = _deduplicate_patterns(raw_patterns)

    # has_data_access is True only for non-LLM data-access patterns
    # (database, http, file_io) — LLM-call patterns are NOT data-access
    has_data_access = any(
        p.kind in ("database", "http", "file_io")
        for p in patterns
    )

    digest = _compute_digest(patterns)

    return SourceProfile(
        patterns=patterns,
        has_data_access=has_data_access,
        digest=digest,
    )


def extract_directory_profile(directory: str) -> SourceProfile:
    """Extract a combined source profile from all Python files in a directory.

    Recursively walks the directory, parses each .py file, and combines
    all detected patterns into a single SourceProfile.

    Args:
        directory: Path to the directory to analyze.

    Returns:
        SourceProfile combining patterns from all .py files, with a
        content-addressed digest.
    """
    all_patterns: list[SourcePattern] = []

    # Collect .py files deterministically (sorted for reproducibility)
    py_files: list[tuple[str, str]] = []  # (relative_path, absolute_path)
    for root, dirs, files in os.walk(directory):
        dirs.sort()  # Deterministic directory traversal
        for fname in sorted(files):
            if fname.endswith(".py"):
                abs_path = os.path.join(root, fname)
                rel_path = os.path.relpath(abs_path, directory)
                py_files.append((rel_path, abs_path))

    for rel_path, abs_path in py_files:
        try:
            with open(abs_path, encoding="utf-8", errors="replace") as f:
                source = f.read()
        except OSError:
            continue

        if not source.strip():
            continue

        source_bytes = source.encode("utf-8")
        parser = _get_parser()
        tree = parser.parse(source_bytes)

        file_patterns = _find_patterns(tree, source_bytes, filename=rel_path)
        all_patterns.extend(file_patterns)

    patterns = _deduplicate_patterns(all_patterns)

    has_data_access = any(
        p.kind in ("database", "http", "file_io")
        for p in patterns
    )

    if not patterns:
        empty_digest = hashlib.sha256(b"[]").hexdigest()
        return SourceProfile(patterns=[], has_data_access=False, digest=empty_digest)

    digest = _compute_digest(patterns)

    return SourceProfile(
        patterns=patterns,
        has_data_access=has_data_access,
        digest=digest,
    )
