"""Packaging invariants: sdist completeness, extras hygiene, Dependabot.

Regression guards for the packaging issues:

- the sdist must ship a *runnable* test suite (conftest, shared
       helpers, and every subpackage) or none at all. Completeness is the
       shipped choice, so this pins the complete tree.
- no dead ``[git]`` optional-dependency extra and no ``gitpython``
       in the dev extra (nothing imports it).
- every core dependency is imported somewhere under ``mareforma/``
       (``rich`` outlived the subcommands it was added for).
- the Dependabot config produces real updates, never advertises a
       lockfile that is not committed, and does not ratchet the pip version
       floors up on every upstream release.
- a PEP 639 string ``project.license`` requires a setuptools>=77 build
       floor; the floor must not permit versions that reject the string form.
- the ``dev`` extra declares the build backend the sdist guard imports, so
       that guard runs instead of skipping on interpreters without setuptools.
- the ``test-heavy`` extra installs exactly the loader libs the grounding
       tests skip on, and carries no heavy dep no test references.
- every optional dep a test skips on behind a module-level ``HAS_*`` flag
       is installed by an extra some workflow leg names, or the test runs
       nowhere (``rdkit`` was skipped on all four legs).
- ``mareforma.__version__`` matches the version the build publishes; the
       two literals are independent and nothing else compares them.
- every marker ``addopts`` deselects is selected back by some workflow
       step, so a marked test has a job that runs it.
- a test fixture that needs SQL newer than the declared SQLite floor
       carries a version skipif, so it skips instead of erroring on a
       build inside the supported window.

Each guard fails on the pre-fix tree.
"""

from __future__ import annotations

import ast
import functools
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
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"
TESTS_DIR = REPO_ROOT / "tests"
PACKAGE_DIR = REPO_ROOT / "mareforma"

# ``setuptools`` is a build backend, not a heavy loader; tests importorskip it
# to build the sdist, and it never belongs in the test-heavy runtime extra.
_BUILD_ONLY_IMPORTS = frozenset({"setuptools"})

# Files whose absence from an sdist turns the shipped suite into an
# unrunnable pile of imports.
_REQUIRED_SDIST_TEST_FILES = ("tests/conftest.py", "tests/_helpers.py")

_KNOWN_LOCKFILES = ("uv.lock", "poetry.lock", "Pipfile.lock", "pdm.lock", "pixi.lock")

# ``ALTER TABLE ... DROP COLUMN`` first shipped in SQLite 3.35, above the
# ``_MIN_SQLITE`` floor ``open_db`` accepts, so a fixture using it must carry
# the shared skipif that gates on the linked version.
_ABOVE_FLOOR_SQL = re.compile(r"ALTER\s+TABLE\b.*\bDROP\s+COLUMN", re.IGNORECASE)
_ABOVE_FLOOR_MARKER = "_requires_drop_column"


def _build_sdist_names():
    """Build the sdist in-process (no network, no build frontend) and
    return the archive member paths relative to the sdist root."""
    # The dev extra declares setuptools, so every CI leg and every documented
    # dev install runs this guard. Skip rather than hard-fail for the one case
    # left, a bare environment installed without the dev extra.
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


def _required_sdist_test_subdirs():
    """Every test subpackage, as an sdist member prefix.

    Read from the tree rather than listed, so a subpackage added later cannot
    ship untested.
    """
    return sorted(
        f"{path.parent.relative_to(REPO_ROOT).as_posix()}/"
        for path in TESTS_DIR.rglob("__init__.py")
        if path.parent != TESTS_DIR
    )


def test_sdist_ships_complete_runnable_suite():
    """conftest + helpers + every test subpackage ride in the sdist."""
    names = _build_sdist_names()
    missing = [f for f in _REQUIRED_SDIST_TEST_FILES if f not in names]
    assert not missing, f"sdist omits runnable-suite files: {missing}"
    for subdir in _required_sdist_test_subdirs():
        assert any(n.startswith(subdir) for n in names), (
            f"sdist omits the {subdir} test subpackage, suite is not runnable"
        )


def test_every_test_subpackage_is_required_in_the_sdist():
    """the guard above promises every test subpackage, so the set it walks must
    come from the tree. A hardcoded list goes stale on the next subpackage:
    ``tests/fixtures/killswitch`` landed after it and rode in on ``graft tests``
    alone, with nothing left to fail if a prune line dropped it.
    """
    required = set(_required_sdist_test_subdirs())
    assert "tests/fixtures/killswitch/" in required, (
        "tests/test_killswitch_pilot.py imports tests.fixtures.killswitch, but "
        f"the sdist guard does not require it: {sorted(required)}"
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


def test_dev_extra_declares_the_build_backend():
    """the sdist guard builds with ``setuptools.build_meta``, and pip no longer
    seeds setuptools into a venv, so a dev extra that does not declare it leaves
    the only sdist guard skipped on every interpreter without the ensurepip copy.
    """
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    dev = data["project"]["optional-dependencies"]["dev"]
    build_floor = _setuptools_floor(data["build-system"]["requires"])
    dev_floor = _setuptools_floor(dev)
    assert dev_floor is not None and dev_floor >= build_floor, (
        f"the dev extra declares setuptools {dev_floor}, the build floor is "
        f"{build_floor}; without it the sdist guard skips instead of running"
    )


def test_version_literals_agree():
    """``mareforma.__version__`` is a second literal the build never reads, so a
    bump that touches only pyproject still produces a correctly named wheel. The
    drifted value is stamped into signed export bundles and JSON-LD exports,
    which cannot be corrected after the fact.
    """
    import mareforma

    declared = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]["version"]
    assert mareforma.__version__ == declared, (
        f"mareforma.__version__ is {mareforma.__version__} but pyproject publishes "
        f"{declared}; exports would carry a version that was never released"
    )


def test_deselected_markers_have_a_job_that_selects_them():
    """a marker deselected by ``addopts`` and selected by no workflow step is
    dead weight: no invocation reaches it, ``-k`` cannot override the marker
    filter, and the tests behind it rot uncompiled while reading as coverage.
    """
    ini = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["tool"]["pytest"]["ini_options"]
    excluded = set(re.findall(r"not\s+([A-Za-z_]\w*)", ini.get("addopts", "")))
    workflows = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(WORKFLOWS_DIR.glob("*.yml"))
    )
    unreachable = sorted(
        marker
        for marker in excluded
        if not re.search(rf"-m\s+[\"']?{re.escape(marker)}\b", workflows)
    )
    assert not unreachable, (
        f"addopts deselects markers no workflow step selects back: {unreachable}"
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


@functools.lru_cache(maxsize=1)
def _tests_use_modules():
    """Every top-level module the test suite imports or gates on.

    Read from the syntax tree, not the source text: every heavy dep is named in
    prose somewhere (docstrings, comments, assertion messages), so a substring
    scan counts a dep nothing imports as used.
    """
    modules = set(_importorskipped_modules())
    for path in TESTS_DIR.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                modules.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules.add(node.module.split(".")[0])
    return frozenset(modules)


def _tests_reference(name):
    """True when the distribution is imported by a test under ``tests/``.

    Every current pin's import name is its distribution name; a pin whose two
    names differ needs an explicit alias here.
    """
    return name.replace("-", "_") in _tests_use_modules()


def _package_imports():
    """Every top-level module name imported anywhere under ``mareforma/``.

    Matches indented imports too: the optional-stdlib fallbacks sit inside
    ``try`` blocks and function bodies.
    """
    names = set()
    for path in PACKAGE_DIR.rglob("*.py"):
        names.update(
            re.findall(
                r"^\s*(?:import|from)\s+([A-Za-z_]\w*)",
                path.read_text(encoding="utf-8"),
                re.M,
            )
        )
    return names


def test_every_core_dependency_is_imported():
    """a core dependency nothing imports is supply-chain surface every
    ``pip install mareforma`` pays for and no code path uses. The extras carry
    an unreferenced-dep guard; ``[project] dependencies`` carried none, which
    is how ``rich`` outlived the subcommands it shipped for.
    """
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    imported = _package_imports()
    unused = sorted(
        name for name in map(_requirement_name, data["project"]["dependencies"])
        if name.replace("-", "_") not in imported
    )
    assert not unused, (
        f"core dependencies nothing under mareforma/ imports, so every install "
        f"carries them and their transitive deps for no code path: {unused}"
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


def _flag_gated_modules():
    """Every optional dep the suite skips on via a module-level ``HAS_*`` flag.

    The other gate shape is ``pytest.importorskip``; a test that imports
    ``HAS_RDKIT`` and skips on it is invisible to a scan for that one.
    """
    this_file = pathlib.Path(__file__).resolve()
    modules = set()
    for path in TESTS_DIR.rglob("*.py"):
        if path.resolve() == this_file:
            continue
        text = path.read_text(encoding="utf-8")
        modules.update(name.lower() for name in _HAS_FLAG.findall(text))
    return modules


def _workflow_installed_extras():
    """Every optional-dependency extra some workflow leg installs."""
    extras = set()
    for path in WORKFLOWS_DIR.glob("*.yml"):
        for command in re.findall(r"pip install[^\n]*", path.read_text(encoding="utf-8")):
            extras |= _extras_named(command)
    return extras


def test_flag_gated_deps_are_installed_by_a_workflow_leg():
    """a test gated on a ``HAS_*`` flag runs only where the dep behind it is
    installed. rdkit sat in the ``chem`` extra no leg named, so the form's
    refusal test and its canonicalisation test were both decided by the same
    absent import and neither proved the form produces the bytes it promises.
    """
    extras = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"][
        "optional-dependencies"
    ]
    installed = {
        _requirement_name(req).replace("-", "_").lower()
        for name in _workflow_installed_extras()
        for req in extras.get(name, ())
    }
    missing = sorted(_flag_gated_modules() - installed)
    assert not missing, (
        f"these deps gate a test behind a HAS_* flag and no workflow leg "
        f"installs the extra that provides them, so the test skips "
        f"everywhere: {missing}"
    )


def test_dep_use_is_measured_by_imports_not_prose():
    """direction 2 above delegates to ``_tests_reference``. A scan over test
    source text counts comments and docstrings, so a pin named only in prose
    reads as used and the unreferenced-dep guard reports a success it never
    earned (``aioresponses`` was named in one comment saying it is unused).
    """
    assert _tests_reference("responses"), (
        "the HTTP mocking tests import responses; a use scan must see it"
    )
    assert not _tests_reference("aioresponses"), (
        "no test imports aioresponses; a scan that counts prose as use guards "
        "nothing, since any name in a comment satisfies it"
    )


def test_above_floor_sql_in_tests_carries_a_version_skipif():
    """a fixture using SQL above the declared SQLite floor must skip on the
    older builds ``open_db`` still accepts, not error with a raw syntax error.
    """
    this_file = pathlib.Path(__file__).resolve()
    unguarded = []
    for path in TESTS_DIR.rglob("*.py"):
        if path.resolve() == this_file:
            continue
        text = path.read_text(encoding="utf-8")
        if _ABOVE_FLOOR_SQL.search(text) and _ABOVE_FLOOR_MARKER not in text:
            unguarded.append(str(path.relative_to(REPO_ROOT)))
    assert not unguarded, (
        f"these tests use SQL newer than the SQLite floor mareforma declares "
        f"and carry no {_ABOVE_FLOOR_MARKER} skipif: {sorted(unguarded)}"
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


def _dependabot_entry(ecosystem):
    """Return the text of the update entry for one package ecosystem, or None."""
    text = DEPENDABOT.read_text(encoding="utf-8")
    for block in re.split(r"^\s*- package-ecosystem:", text, flags=re.M)[1:]:
        head, _, rest = block.partition("\n")
        if head.strip().strip("\"'") == ecosystem:
            return rest
    return None


def test_dependabot_pip_does_not_ratchet_version_floors():
    """the pip entry must pin a versioning strategy that leaves satisfied
    constraints alone. Without it Dependabot rewrites the lower bound on every
    upstream release, so a floor no call site needs (cryptography>=48) ships in
    immutable PyPI metadata and makes the install unsolvable for pipelines
    holding an older but perfectly working version.
    """
    entry = _dependabot_entry("pip")
    assert entry is not None, "dependabot.yml declares no pip update entry"
    assert re.search(r"versioning-strategy:\s*[\"']?increase-if-necessary", entry), (
        "the pip entry must declare versioning-strategy: increase-if-necessary, "
        "or every upstream release ratchets the declared floors"
    )
