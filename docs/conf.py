"""Sphinx configuration for mareforma documentation."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# ---------------------------------------------------------------------------
# Project
# ---------------------------------------------------------------------------

project = "mareforma"
author = "Mareforma"
copyright = "2026, Mareforma"

from mareforma import __version__  # noqa: E402
release = __version__
version = __version__

# ---------------------------------------------------------------------------
# Extensions
# ---------------------------------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",       # NumPy / Google docstring styles
    "sphinx.ext.viewcode",       # [source] links in API reference
    "sphinx.ext.intersphinx",    # cross-reference Python stdlib
    "myst_parser",               # Markdown support
    "sphinx_autodoc_typehints",  # type hints in signatures
]

# ---------------------------------------------------------------------------
# Autodoc
# ---------------------------------------------------------------------------

autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
    "member-order": "bysource",
}
autodoc_typehints = "description"
autodoc_typehints_description_target = "documented"

# ---------------------------------------------------------------------------
# Napoleon (docstring style)
# ---------------------------------------------------------------------------

napoleon_numpy_docstring = True
napoleon_google_docstring = False
napoleon_use_param = True
napoleon_use_rtype = True

# ---------------------------------------------------------------------------
# MyST (Markdown)
# ---------------------------------------------------------------------------

myst_enable_extensions = [
    "colon_fence",
    "deflist",
]
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# ---------------------------------------------------------------------------
# Intersphinx
# ---------------------------------------------------------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# ---------------------------------------------------------------------------
# HTML output
# ---------------------------------------------------------------------------

html_theme = "furo"
html_title = "mareforma"
html_static_path = ["_static"]
html_theme_options = {
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
}

# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
nitpicky = False
