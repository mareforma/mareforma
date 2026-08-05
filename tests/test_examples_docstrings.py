"""Every example's module docstring, pinned against the file it sits in.

The docstring is the first thing a reader copies, so a Run block naming a
path that does not exist fails before the example teaches anything. Both
guards read the example tree, so they fail whenever a script moves.
"""
from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path

import pytest

from tests._helpers import _example_files, _requires_repo_checkout

_EXAMPLES = Path(__file__).resolve().parents[1] / "examples"

# examples/ is not in the sdist, so the shipped suite skips this module.
pytestmark = _requires_repo_checkout

_RUN_COMMAND_RE = re.compile(r"^\s*python3?\s+(\S+)", re.MULTILINE)

_WALKTHROUGH = _EXAMPLES / "01_api_walkthrough" / "01_api_walkthrough.py"

# Index entries are "  N. Label   description", two spaces before the description.
_INDEX_ENTRY_RE = re.compile(r"^\s*(\d+)\.\s+(.+?)(?:\s{2,}\S.*)?$", re.MULTILINE)
_BANNER_RE = re.compile(r'sep\("(\d+)\.\s+([^"]+)"\)')


def _docstring(path: Path) -> str:
    return ast.get_docstring(ast.parse(path.read_text(encoding="utf-8"))) or ""


@pytest.mark.parametrize("path", _example_files(), ids=lambda p: p.name)
def test_docstring_names_its_own_file(path: Path) -> None:
    """The identity line says which script the reader has open."""
    first = next(line for line in _docstring(path).splitlines() if line.strip())
    assert first.startswith(path.name), (
        f"{path} opens its docstring with {first!r}, not with {path.name}"
    )


@pytest.mark.parametrize("path", _example_files(), ids=lambda p: p.name)
def test_docstring_run_commands_resolve(path: Path) -> None:
    """Every documented ``python <script>`` runs from the example's directory."""
    for target in _RUN_COMMAND_RE.findall(_docstring(path)):
        assert (path.parent / target).exists(), (
            f"{path} documents `python {target}`, which does not exist"
        )


def test_example_files_skips_what_an_example_installs(tmp_path: Path) -> None:
    """The walk above must list tracked examples, not a vendored tree.

    Example 05's README tells the reader to run ``--install``, which clones
    a repository and builds a virtualenv inside the example directory. Both
    are gitignored, so a reader who runs the example and then the suite gets
    hundreds of failures named after third-party modules.
    """
    example = tmp_path / "examples" / "01_demo"
    (example / "medea_env").mkdir(parents=True)
    (example / "01_demo.py").write_text('"""01_demo.py"""\n', encoding="utf-8")
    (example / ".gitignore").write_text("medea_env/\n", encoding="utf-8")
    (example / "medea_env" / "vendored.py").write_text("x = 1\n", encoding="utf-8")
    (example / "medea_env" / "setup.toml").write_text("[build]\n", encoding="utf-8")
    for command in (["init", "-q", str(tmp_path)], ["-C", str(tmp_path), "add", "-A"]):
        subprocess.run(["git", *command], check=True, capture_output=True)

    assert _example_files(root=tmp_path) == [example / "01_demo.py"]
    assert _example_files(".toml", root=tmp_path) == []


def test_walkthrough_index_lists_every_section_it_prints() -> None:
    """The Sections index is the table of contents for the banners the run prints."""
    source = _WALKTHROUGH.read_text(encoding="utf-8")
    index = _INDEX_ENTRY_RE.findall(_docstring(_WALKTHROUGH).split("Sections")[1])
    banners = _BANNER_RE.findall(source)

    assert [number for number, _ in index] == [number for number, _ in banners]
    for (_, label), (number, title) in zip(index, banners):
        assert title.startswith(label), (
            f"index entry {number} reads {label!r}, the banner reads {title!r}"
        )
