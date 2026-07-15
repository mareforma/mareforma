"""Packaging invariants: sdist completeness, extras hygiene, Dependabot.

Regression guards for the packaging issues:

- the sdist must ship a *runnable* test suite (conftest, shared
       helpers, and every subpackage) or none at all. Completeness is the
       shipped choice, so this pins the complete tree.
- no dead ``[git]`` optional-dependency extra and no ``gitpython``
       in the dev extra (nothing imports it).
- the Dependabot config produces real updates and never advertises a
       lockfile that is not committed.
- a PEP 639 string ``project.license`` requires a setuptools>=77 build
       floor; the floor must not permit versions that reject the string form.
- the ``test-heavy`` extra installs exactly the loader libs the grounding
       tests skip on, and carries no heavy dep no test references.

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
TESTS_DIR = REPO_ROOT / "tests"

# ``setuptools`` is a build backend, not a heavy loader; tests importorskip it
# to build the sdist, and it never belongs in the test-heavy runtime extra.
_BUILD_ONLY_IMPORTS = frozenset({"setuptools"})

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
    # venv. Skip rather than hard-fail when a clean environment lacks it, the
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
    """conftest + helpers + every test subpackage ride in the sdist."""
    names = _build_sdist_names()
    missing = [f for f in _REQUIRED_SDIST_TEST_FILES if f not in names]
    assert not missing, f"sdist omits runnable-suite files: {missing}"
    for subdir in _REQUIRED_SDIST_TEST_SUBDIRS:
        assert any(n.startswith(subdir) for n in names), (
            f"sdist omits the {subdir} test subpackage, suite is not runnable"
        )


def test_no_dead_git_extra_and_no_gitpython_dev():
    """the [git] extra is gone and gitpython is out of dev."""
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    extras = data["project"].get("optional-dependencies", {})
    assert "git" not in extras, "the dead [git] extra must be removed"
    dev = extras.get("dev", [])
    assert not any(req.lower().startswith("gitpython") for req in dev), (
        "gitpython is unused; it must not sit in the dev extra"
    )


def _setuptools_floor(requires):
    """Return the ``setuptools>=X.Y`` build floor as an int tuple, or None."""
    for req in requires:
        match = re.match(r"\s*setuptools\s*>=\s*([\d.]+)", req)
        if match:
            return tuple(int(part) for part in match.group(1).split("."))
    return None


def test_license_string_requires_setuptools_77_floor():
    """setuptools accepts a string ``project.license`` (PEP 639 SPDX)
    only from 77.0.0 on; every earlier version rejects it. A string license
    with a floor below 77 cannot build with its own stated minimum toolchain.
    """
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    license_field = data["project"]["license"]
    # Table form ({"file": ...}/{"text": ...}) predates PEP 639 and builds on
    # old setuptools; this guard only binds the SPDX string form.
    if not isinstance(license_field, str):
        pytest.skip("license is a table; the setuptools>=77 floor does not bind")
    floor = _setuptools_floor(data["build-system"]["requires"])
    assert floor is not None and floor >= (77,), (
        f"project.license is a PEP 639 string but the setuptools build floor "
        f"is {floor}; the string form needs setuptools>=77"
    )


def _requirement_name(req):
    """Return the base distribution name of a requirement string.

    ``netCDF4>=1.6`` -> ``netCDF4``; ``polars`` -> ``polars``.
    """
    return re.split(r"[<>=!~;\s\[]", req, maxsplit=1)[0]


def _importorskipped_modules():
    """Every top-level module the test suite gates on via ``importorskip``."""
    modules = set()
    for path in TESTS_DIR.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        modules.update(re.findall(r"importorskip\(\s*[\"']([A-Za-z0-9_]+)", text))
    return modules


def _tests_reference(name):
    """True when the distribution name is used by a test under ``tests/``.

    This packaging module is skipped: it names every heavy dep in prose (docstrings,
    assertion messages), so counting it would let any extra reference itself.
    """
    this_file = pathlib.Path(__file__).resolve()
    return any(
        name in path.read_text(encoding="utf-8")
        for path in TESTS_DIR.rglob("*.py")
        if path.resolve() != this_file
    )


def test_test_heavy_extra_matches_the_loaders_it_exercises():
    """the heavy leg must install every loader the grounding tests skip on
    (polars, duckdb were importorskip'd but installed by no extra), and must not
    carry a heavy dep no test references (netCDF4 was shipped with zero test use).
    """
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    heavy = {_requirement_name(req) for req in data["project"]["optional-dependencies"]["test-heavy"]}

    # Direction 1: no importorskip'd loader is missing from the heavy extra.
    gated = _importorskipped_modules() - _BUILD_ONLY_IMPORTS
    missing = sorted(gated - heavy)
    assert not missing, (
        f"the test-heavy extra omits libs the grounding tests importorskip, so "
        f"those pins skip on every CI leg: {missing}"
    )

    # Direction 2: no heavy dep sits in the extra with nothing referencing it.
    unreferenced = sorted(name for name in heavy if not _tests_reference(name))
    assert not unreferenced, (
        f"the test-heavy extra installs deps no test references, so CI pays to "
        f"build wheels that guard nothing: {unreferenced}"
    )


def test_dependabot_produces_real_updates():
    """both ecosystems scheduled, and no uncommitted lockfile claim."""
    text = DEPENDABOT.read_text(encoding="utf-8")
    ecosystems = set(re.findall(r"package-ecosystem:\s*[\"']?([\w-]+)", text))
    assert {"github-actions", "pip"} <= ecosystems, (
        f"Dependabot must track github-actions + pip; found {ecosystems}"
    )
    intervals = re.findall(r"interval:\s*[\"']?([\w-]+)", text)
    assert len(intervals) >= 2, "each ecosystem needs a schedule interval"
    # A config that names a lockfile it does not commit produces no updates
    # from that lockfile. Guard against reintroducing it.
    for lockfile in _KNOWN_LOCKFILES:
        if lockfile in text:
            assert (REPO_ROOT / lockfile).exists(), (
                f"dependabot.yml references {lockfile} but it is not committed"
            )
