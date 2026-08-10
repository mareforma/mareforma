"""Offline structural checks for the documentation site.

There is no docs build, link check or MDX lint in CI, so a page listed in
``docs.json`` but missing on disk, or an ``<Accordion>`` opened and never closed,
ships silently and only breaks when the site is next built. These checks run with
no network and no Mintlify toolchain: they validate that every navigation entry
resolves to a real page and that the block components on each page are balanced.
It is deliberately narrow, the lightest real check that catches the two failure
shapes a heavy docs edit introduces, not a substitute for a full render.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_DOCS = Path(__file__).resolve().parent.parent / "docs"
_DOCS_JSON = _DOCS / "docs.json"

# The sdist ships tests but not docs/, so this module has nothing to read there.
# The skip has to be module-level and it has to run HERE, above the parametrize
# decorators: those call _load_pages() at import time, so a function-level skip
# or a pytestmark is far too late. Without it the whole shipped suite dies at
# collection with FileNotFoundError and runs zero tests, which is what a distro
# packager sees. tests/test_packaging.py has the end-to-end guard for this, but
# it carries the `sdist` marker that pyproject's addopts deselects, so the
# ordinary green run cannot catch a regression here.
if not _DOCS_JSON.exists():  # pragma: no cover - only true outside a checkout
    pytest.skip(
        "reads the docs/ tree, which the sdist does not ship",
        allow_module_level=True,
    )

# Block components that must open and close in pairs. A self-closing tag
# (``<Tag ... />``) counts as neither, so it never unbalances the pair.
_PAIRED_COMPONENTS = (
    "Accordion",
    "AccordionGroup",
    "Tabs",
    "Tab",
    "CodeGroup",
    "Card",
    "CardGroup",
    "Steps",
    "Step",
    "Frame",
    "Expandable",
)


def _nav_pages(node: object) -> list[str]:
    """Every page slug referenced anywhere in the navigation tree."""
    pages: list[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "pages" and isinstance(value, list):
                for entry in value:
                    if isinstance(entry, str):
                        pages.append(entry)
                    else:
                        pages.extend(_nav_pages(entry))
            else:
                pages.extend(_nav_pages(value))
    elif isinstance(node, list):
        for entry in node:
            pages.extend(_nav_pages(entry))
    return pages


def _load_pages() -> list[str]:
    data = json.loads(_DOCS_JSON.read_text(encoding="utf-8"))
    return _nav_pages(data)


def test_docs_json_is_valid_json() -> None:
    # A malformed docs.json fails the whole site build; catch it here first.
    json.loads(_DOCS_JSON.read_text(encoding="utf-8"))


@pytest.mark.parametrize("page", _load_pages())
def test_navigation_page_exists(page: str) -> None:
    target = _DOCS / f"{page}.mdx"
    assert target.exists(), (
        f"docs.json navigation lists {page!r} but {target} does not exist"
    )


def test_navigation_has_no_duplicate_pages() -> None:
    pages = _load_pages()
    seen = [p for p in pages if pages.count(p) > 1]
    assert not seen, f"docs.json lists these pages more than once: {sorted(set(seen))}"


def _unbalanced_components(text: str) -> list[str]:
    problems = []
    for comp in _PAIRED_COMPONENTS:
        opens = len(re.findall(rf"<{comp}(?=[\s/>])", text))
        self_closing = len(re.findall(rf"<{comp}\b[^>]*/>", text))
        closes = len(re.findall(rf"</{comp}>", text))
        if opens - self_closing != closes:
            problems.append(
                f"{comp}: {opens - self_closing} open vs {closes} close"
            )
    return problems


@pytest.mark.parametrize(
    "mdx", sorted(_DOCS.rglob("*.mdx")), ids=lambda p: str(p.relative_to(_DOCS)),
)
def test_block_components_are_balanced(mdx: Path) -> None:
    problems = _unbalanced_components(mdx.read_text(encoding="utf-8"))
    assert not problems, (
        f"{mdx.relative_to(_DOCS)} has unbalanced block components: "
        + "; ".join(problems)
    )
