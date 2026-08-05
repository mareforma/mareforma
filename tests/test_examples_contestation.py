"""Example 03's console listing, pinned so the demo cannot rot.

``query_graph`` is the agent tool surface, so it wraps claim text in
``<untrusted_data>`` for the LLM. A human console must not show that wrapper:
printed raw it breaks the line after the support level and spends a quarter of
the truncation budget on delimiters. The example imports langchain-core at
module scope, so the display helper is lifted out of the source rather than
imported.
"""
from __future__ import annotations

import ast
from pathlib import Path

from mareforma.prompt_safety import safe_for_llm

_EXAMPLE_DIR = (
    Path(__file__).resolve().parents[1] / "examples" / "03_documented_contestation"
)
_EXAMPLE = _EXAMPLE_DIR / "03_documented_contestation.py"


def _display_helper():
    """The example's console-display helper, compiled on its own."""
    body = ast.parse(_EXAMPLE.read_text(encoding="utf-8")).body
    func = next(
        (
            node
            for node in body
            if isinstance(node, ast.FunctionDef) and node.name == "_for_console"
        ),
        None,
    )
    assert func is not None, (
        "the example prints query_graph text with no console-display helper, so "
        "the LLM wrapper reaches the terminal"
    )
    namespace: dict = {}
    exec(compile(ast.Module(body=[func], type_ignores=[]), str(_EXAMPLE), "exec"), namespace)
    return namespace["_for_console"]


def test_console_listing_drops_the_llm_wrapper() -> None:
    """the printed row shows claim text only, all 65 truncated characters."""
    text = "Treatment X reduces outcome Y in population P (cohort_1, n=500, p=0.003)"
    shown = _display_helper()(safe_for_llm(text))[:65]
    assert "untrusted_data" not in shown, shown
    assert shown == text[:65], shown


def test_readme_transcript_shows_no_wrapper() -> None:
    """the published transcript records what the fixed script prints."""
    readme = (_EXAMPLE_DIR / "README.md").read_text(encoding="utf-8")
    assert "untrusted_data" not in readme, (
        "the README transcript still shows the LLM wrapper on the console"
    )
