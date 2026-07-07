"""Packaging invariants: sdist completeness, extras hygiene, Dependabot.

Regression guards for the packaging issues:

- #45  the sdist must ship a *runnable* test suite (conftest, shared
       helpers, and every subpackage) or none at all. Completeness is the
       shipped choice, so this pins the complete tree.
- #56  no dead ``[git]`` optional-dependency extra and no ``gitpython``
       in the dev extra (nothing imports it).
- #55  the Dependabot config produces real updates and never advertises a
       lockfile that is not committed.

Each guard fails on the pre-fix tree.
"""

from __future__ import annotations

import os
import pathlib
import re
import tarfile
import tempfile

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"
DEPENDABOT = REPO_ROOT / ".github" / "dependabot.yml"

# Files whose absence from an sdist turns the shipped suite into an
# unrunnable pile of imports.
_REQUIRED_SDIST_TEST_FILES = ("tests/conftest.py", "tests/_helpers.py")
_REQUIRED_SDIST_TEST_SUBDIRS = ("tests/adapters/", "tests/epistemic/", "tests/integration/")

_KNOWN_LOCKFILES = ("uv.lock", "poetry.lock", "Pipfile.lock", "pdm.lock", "pixi.lock")


def _build_sdist_names():
    """Build the sdist in-process (no network, no build frontend) and
    return the archive member paths relative to the sdist root."""
    # setuptools is a BUILD-system dependency (pyproject [build-system].requires),
    # not a runtime or test dependency, and modern pip no longer seeds it into a
    # venv. Skip rather than hard-fail when a clean environment lacks it — the
    # PEP 517 build supplies it at build time regardless.
    build_meta = pytest.importorskip("setuptools.build_meta")

    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as out:
        os.chdir(REPO_ROOT)
        try:
            name = build_meta.build_sdist(out)
        finally:
            os.chdir(cwd)
        with tarfile.open(os.path.join(out, name)) as tf:
            members = tf.getnames()
    # strip the leading "<pkg>-<version>/" component
    return {m.split("/", 1)[1] for m in members if "/" in m}


def test_sdist_ships_complete_runnable_suite():
    """#45: conftest + helpers + every test subpackage ride in the sdist."""
    names = _build_sdist_names()
    missing = [f for f in _REQUIRED_SDIST_TEST_FILES if f not in names]
    assert not missing, f"sdist omits runnable-suite files: {missing}"
    for subdir in _REQUIRED_SDIST_TEST_SUBDIRS:
        assert any(n.startswith(subdir) for n in names), (
            f"sdist omits the {subdir} test subpackage — suite is not runnable"
        )


def test_no_dead_git_extra_and_no_gitpython_dev():
    """#56: the [git] extra is gone and gitpython is out of dev."""
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    extras = data["project"].get("optional-dependencies", {})
    assert "git" not in extras, "the dead [git] extra must be removed"
    dev = extras.get("dev", [])
    assert not any(req.lower().startswith("gitpython") for req in dev), (
        "gitpython is unused; it must not sit in the dev extra"
    )


def test_dependabot_produces_real_updates():
    """#55: both ecosystems scheduled, and no uncommitted lockfile claim."""
    text = DEPENDABOT.read_text(encoding="utf-8")
    ecosystems = set(re.findall(r"package-ecosystem:\s*[\"']?([\w-]+)", text))
    assert {"github-actions", "pip"} <= ecosystems, (
        f"Dependabot must track github-actions + pip; found {ecosystems}"
    )
    intervals = re.findall(r"interval:\s*[\"']?([\w-]+)", text)
    assert len(intervals) >= 2, "each ecosystem needs a schedule interval"
    # A config that names a lockfile it does not commit produces no updates
    # from that lockfile — the #55 defect. Guard against reintroducing it.
    for lockfile in _KNOWN_LOCKFILES:
        if lockfile in text:
            assert (REPO_ROOT / lockfile).exists(), (
                f"dependabot.yml references {lockfile} but it is not committed"
            )
