"""Packaging invariants: sdist completeness, extras hygiene, Dependabot.

Regression guards for the packaging issues:

- the sdist must ship a *runnable* test suite (conftest, shared
       helpers, and every subpackage) or none at all. Completeness is the
       shipped choice, so this pins the complete tree.
- the shipped suite must run green from the unpacked archive, so a test
       reading a repo tree the sdist leaves out (docs/, examples/,
       ``.github/``) skips downstream instead of failing a fine release.
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
- every exporter module uses each name it imports (``sqlite3`` sat unused
       in the RO-Crate exporter, implying it owned a connection).
- no test module keeps its own copy of a helper ``tests/_helpers.py``
       already exports, so the shared fixture has one definition to edit.

Each guard fails on the pre-fix tree.
"""

from __future__ import annotations

import ast
import functools
import os
import pathlib
import re
import subprocess
import sys
import tarfile
import tempfile

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

import pytest

from tests._helpers import _requires_repo_checkout

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"
README = REPO_ROOT / "README.md"
CONTRIBUTING = REPO_ROOT / "CONTRIBUTING.md"
DEPENDABOT = REPO_ROOT / ".github" / "dependabot.yml"
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"
TESTS_DIR = REPO_ROOT / "tests"
PACKAGE_DIR = REPO_ROOT / "mareforma"

# ``setuptools`` is a build backend, not a heavy loader; tests importorskip it
# to build the sdist, and it never belongs in the test-heavy runtime extra.
_BUILD_ONLY_IMPORTS = frozenset({"setuptools"})

# Libs an importorskip gates on that are NOT grounding loaders and live in their
# own extra with their own CI leg, so they are installed and their pins run,
# just not on the test-heavy leg. ``mcp`` (the Model Context Protocol SDK) sits
# in the ``mcp`` extra and installs on the ``mcp`` workflow job.
_DEDICATED_LEG_IMPORTS = frozenset({"mcp"})

# Files whose absence from an sdist turns the shipped suite into an
# unrunnable pile of imports.
_REQUIRED_SDIST_TEST_FILES = ("tests/conftest.py", "tests/_helpers.py")

_KNOWN_LOCKFILES = ("uv.lock", "poetry.lock", "Pipfile.lock", "pdm.lock", "pixi.lock")

# ``ALTER TABLE ... DROP COLUMN`` first shipped in SQLite 3.35, above the
# ``_MIN_SQLITE`` floor ``open_db`` accepts, so a fixture using it must carry
# the shared skipif that gates on the linked version.
_ABOVE_FLOOR_SQL = re.compile(r"ALTER\s+TABLE\b.*\bDROP\s+COLUMN", re.IGNORECASE)
_ABOVE_FLOOR_MARKER = "_requires_drop_column"

# A module-level availability flag a test skips on, ``HAS_RDKIT`` -> ``rdkit``.
_HAS_FLAG = re.compile(r"\bHAS_([A-Z0-9]+(?:_[A-Z0-9]+)*)\b")

# Marks pytest registers itself, so the contributor guide may name one
# without pyproject listing it.
_PYTEST_BUILTIN_MARKERS = frozenset(
    {"parametrize", "skip", "skipif", "xfail", "usefixtures", "filterwarnings"}
)

# A marker the contributor guide prescribes, written either as a decorator
# or as the argument of a documented ``pytest -m`` invocation.
_DOC_MARKER = re.compile(r"@pytest\.mark\.(\w+)|pytest\s+-m\s+'?(\w+)")


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


@pytest.mark.sdist
@_requires_repo_checkout
def test_sdist_suite_runs_green_from_the_archive(tmp_path):
    """the shipped suite must pass from the unpacked archive, not just collect.

    Member names cannot see this: a test that reads a repo tree the archive
    leaves out (docs/, examples/, .github/) hands a packager a red build for a
    release that is fine. Such tests skip downstream, and this catches the next
    one that does not. Marked ``sdist`` because it costs about 45 seconds.
    """
    build_meta = pytest.importorskip("setuptools.build_meta")

    cwd = os.getcwd()
    os.chdir(REPO_ROOT)
    try:
        name = build_meta.build_sdist(str(tmp_path))
    finally:
        os.chdir(cwd)
    with tarfile.open(tmp_path / name) as tf:
        tf.extractall(tmp_path, filter="data")

    run = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"],
        cwd=tmp_path / name.removesuffix(".tar.gz"),
        capture_output=True,
        text=True,
    )
    assert run.returncode == 0, run.stdout[-4000:]


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


def test_empty_extras_carry_a_justifying_comment():
    """an extra with no requirements is still published: ``Provides-Extra``
    rides in the wheel metadata, so ``pip install mareforma[name]`` succeeds
    silently instead of warning that the extra does not exist. That reads as
    "already satisfied" when the truth may be "wired to nothing". An empty
    extra must say on its line why it earns the empty list.
    """
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    extras = data["project"].get("optional-dependencies", {})
    text = PYPROJECT.read_text(encoding="utf-8")
    _, _, block = text.partition("[project.optional-dependencies]\n")
    block, _, _ = block.partition("\n[")
    unexplained = sorted(
        name
        for name, reqs in extras.items()
        if not reqs
        and not re.search(rf"^{re.escape(name)}\s*=\s*\[\s*\]\s*#\s*\S", block, re.M)
    )
    assert not unexplained, (
        f"these extras install nothing and say nothing about why: {unexplained}. "
        "Delete them, or state the reason in a trailing comment"
    )


@_requires_repo_checkout
def test_no_doc_prescribes_installing_an_empty_extra():
    """``pip install mareforma[name]`` for an extra that resolves to nothing
    installs nothing and still succeeds, so a doc that prints it teaches a
    reader that the extra is what supplies the feature. Every install
    instruction must name an extra that actually pulls a dependency.
    """
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    extras = data["project"].get("optional-dependencies", {})
    empty = sorted(name for name, reqs in extras.items() if not reqs)
    surfaces = [
        *(REPO_ROOT / "docs").rglob("*.mdx"),
        *(REPO_ROOT / "mareforma").rglob("*.py"),
        *(REPO_ROOT / "examples").rglob("*.md"),
        REPO_ROOT / "README.md",
        REPO_ROOT / "ARCHITECTURE.md",
        REPO_ROOT / "AGENTS.md",
        REPO_ROOT / "CONTRIBUTING.md",
    ]
    # The changelog records extras as they were, including ones since removed.
    changelog = REPO_ROOT / "docs" / "reference" / "changelog.mdx"
    offenders = sorted(
        f"{path.relative_to(REPO_ROOT)}: mareforma[{name}]"
        for path in surfaces
        if path != changelog
        for name in empty
        if f"pip install mareforma[{name}]" in path.read_text(encoding="utf-8")
    )
    assert not offenders, (
        f"these docs tell a reader to install an extra that pulls nothing: "
        f"{offenders}. Give the extra a dependency, or drop it and the "
        "install instruction"
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


@_requires_repo_checkout
def test_deselected_markers_have_a_job_that_selects_them():
    """a marker deselected by ``addopts`` and selected by no workflow step is
    dead weight: no invocation reaches it, ``-k`` cannot override the marker
    filter, and the tests behind it rot uncompiled while reading as coverage.

    The sdist ships no .github/, where this would read an empty workflow set
    and pass on every marker; it skips there instead.
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


def test_markers_named_in_the_contributor_guide_are_registered():
    """the guide prescribed ``@pytest.mark.requires_model`` under a rule that
    the full suite must download nothing, and nothing registered or deselected
    it: a test carrying it emits a warning and then runs, downloading weights
    on every CI leg. A marker the guide names must be one pytest knows.
    """
    registered = {
        entry.split(":", 1)[0].strip()
        for entry in tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["tool"]["pytest"][
            "ini_options"
        ]["markers"]
    }
    named = {
        marker or selected
        for marker, selected in _DOC_MARKER.findall(CONTRIBUTING.read_text(encoding="utf-8"))
    }
    unwired = sorted(named - registered - _PYTEST_BUILTIN_MARKERS)
    assert not unwired, (
        f"CONTRIBUTING.md prescribes markers pyproject does not register, so "
        f"they select and skip nothing: {unwired}"
    )


def test_addopts_makes_an_unregistered_marker_an_error():
    """without ``--strict-markers`` a typo'd or never-registered marker is a
    ``PytestUnknownMarkWarning`` in a verbose log and the test runs unguarded,
    which is how the guide's opt-in rule stayed inert. Strict turns it into a
    collection error.
    """
    ini = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["tool"]["pytest"]["ini_options"]
    assert "--strict-markers" in ini.get("addopts", ""), (
        "addopts must carry --strict-markers so an unregistered marker fails "
        "collection instead of warning and running"
    )


def _workflow_job(workflow_name, job_name):
    """Return the body of one job block from a workflow file, or None.

    A job is a bare two-space key under ``jobs:``; its body runs to the next
    one. Text, not YAML: pyyaml is not a dependency here.
    """
    text = (WORKFLOWS_DIR / workflow_name).read_text(encoding="utf-8")
    _, _, jobs = text.partition("\njobs:\n")
    for block in re.split(r"^  (?=[\w-]+:\s*$)", jobs, flags=re.M)[1:]:
        head, _, body = block.partition("\n")
        if head.strip().rstrip(":") == job_name:
            return body
    return None


@_requires_repo_checkout
def test_publish_is_gated_on_the_test_suite():
    """a PyPI upload is irreversible per version, and ``twine check`` reads
    metadata rendering only, so a publish job with no test gate ships whatever
    the tagged commit contains. ``tests.yml`` fires on the tag push but nothing
    reads its conclusion: the upload finishes long before the matrix reports.

    The sdist ships no .github/; this skips there.
    """
    publish = _workflow_job("publish.yml", "publish")
    assert publish is not None, "publish.yml declares no publish job"
    gated = re.search(r"^\s*needs:", publish, flags=re.M) or re.search(
        r"^\s*run:.*\bpytest\b", publish, flags=re.M
    )
    assert gated, (
        "the publish job neither declares needs: nor runs pytest, so an "
        "irreversible upload can ship from a commit with a red suite"
    )
    if re.search(r"^\s*needs:", publish, flags=re.M):
        gate = re.search(r"^\s*needs:\s*\[?\s*([\w-]+)", publish, flags=re.M).group(1)
        gate_job = _workflow_job("publish.yml", gate)
        assert gate_job is not None and re.search(
            r"uses:\s*\./\.github/workflows/tests\.yml", gate_job
        ), f"the publish job waits on '{gate}', which does not run the suite"


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


_FENCED_CODE = re.compile(r"^```.*?^```", re.DOTALL | re.MULTILINE)
_HTML_URL_ATTR = re.compile(r'(?:src|srcset|href)\s*=\s*"([^"]+)"')
_MARKDOWN_LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")


def _readme_reference_targets():
    """Every URL the README points at, from HTML attributes and Markdown links.

    Fenced code blocks are stripped before the Markdown pass so a bracketed
    expression in an example script cannot read as a link.
    """
    text = README.read_text(encoding="utf-8")
    return _HTML_URL_ATTR.findall(text) + _MARKDOWN_LINK.findall(_FENCED_CODE.sub("", text))


def test_readme_references_nothing_by_repo_relative_path():
    """``project.readme`` makes this file the PyPI long description, and PyPI
    resolves relative paths against the project page, where no repo tree
    exists. A relative wordmark renders broken and a relative doc link 404s.
    """
    relative = sorted(
        {
            target
            for target in _readme_reference_targets()
            if not target.startswith(("http://", "https://", "#"))
        }
    )
    assert not relative, (
        f"these README references resolve against the PyPI project page and "
        f"404 there; make them absolute: {relative}"
    )


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


def _setup_install_commands():
    """Every install command in the contributor guide's Setup block.

    Each match stops at the next ``&&``, comment or newline, so an
    alternative offered behind ``# or:`` is checked on its own terms.
    """
    text = CONTRIBUTING.read_text(encoding="utf-8")
    _, _, block = text.partition("\n## Setup\n")
    block, _, _ = block.partition("\n## ")
    return re.findall(r"(?:uv sync|pip install)[^\n&#]*", block)


def _extras_named(command):
    """The optional-dependency extras an install command opts into."""
    named = {
        part.strip() for group in re.findall(r"\[([^\]]+)\]", command) for part in group.split(",")
    }
    return named | set(re.findall(r"--extra\s+([\w-]+)", command))


def test_contributing_setup_installs_the_extra_that_provides_pytest():
    """the Setup block runs pytest on the line after the install, and the same
    file makes a green suite a hard gate twice. pytest sits in an extra, and
    neither ``uv sync`` nor ``pip install -e .`` installs one: uv installs a
    dependency *group* by default, never an optional-dependency extra.
    """
    extras = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"][
        "optional-dependencies"
    ]
    provides_pytest = {
        name
        for name, reqs in extras.items()
        if any(_requirement_name(req) == "pytest" for req in reqs)
    }
    assert provides_pytest, "no extra declares pytest; the guard has nothing to pin"
    commands = _setup_install_commands()
    assert commands, "the Setup block documents no install command"
    silent = [cmd.strip() for cmd in commands if not _extras_named(cmd) & provides_pytest]
    assert not silent, (
        f"these documented install commands leave pytest out of the "
        f"environment, so the next line fails: {silent}"
    )


def test_test_heavy_extra_matches_the_loaders_it_exercises():
    """the heavy leg must install every loader the grounding tests skip on
    (polars, duckdb were importorskip'd but installed by no extra), and must not
    carry a heavy dep no test references (netCDF4 was shipped with zero test use).
    """
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    heavy = {_requirement_name(req) for req in data["project"]["optional-dependencies"]["test-heavy"]}

    # Direction 1: no importorskip'd loader is missing from the heavy extra.
    # A lib installed by its own dedicated CI leg is covered elsewhere.
    gated = _importorskipped_modules() - _BUILD_ONLY_IMPORTS - _DEDICATED_LEG_IMPORTS
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


@_requires_repo_checkout
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


def _top_level_definitions(path):
    """Map every top-level ``def``/``class`` in *path* to its shape.

    The shape is the syntax tree with the docstring dropped, so a copy
    that lost the original's docstring still compares equal to it.
    """
    shapes = {}
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        body = node.body
        if ast.get_docstring(node) is not None:
            body = body[1:]
        shapes[node.name] = ast.dump(ast.Module(body=body, type_ignores=[]))
    return shapes


def test_no_test_module_redefines_a_shared_helper():
    """a fixture the shared helpers already export must not also live in a
    test module: the next person to change the canonical proposition edits
    ``tests/_helpers.py``, sees the importing siblings follow, and never
    learns that one module quietly kept the old one.
    """
    shared = _top_level_definitions(TESTS_DIR / "_helpers.py")
    copies = {}
    for path in sorted(TESTS_DIR.rglob("*.py")):
        if path.name == "_helpers.py":
            continue
        duplicated = sorted(
            name for name, shape in _top_level_definitions(path).items()
            if shared.get(name) == shape
        )
        if duplicated:
            copies[str(path.relative_to(REPO_ROOT))] = duplicated
    assert not copies, (
        f"these modules redefine helpers tests/_helpers.py already exports; "
        f"import them instead: {copies}"
    )


def _unused_import_names(path):
    """Names *path* imports and then never mentions again.

    ``from __future__`` bindings are compiler directives rather than
    names, so they are excluded.
    """
    source = path.read_text(encoding="utf-8")
    imported = []
    import_lines = set()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, (ast.Import, ast.ImportFrom)):
            continue
        import_lines.update(range(node.lineno, node.end_lineno + 1))
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            continue
        imported.extend(a.asname or a.name.split(".")[0] for a in node.names)
    body = "\n".join(
        line for n, line in enumerate(source.splitlines(), 1)
        if n not in import_lines
    )
    return [
        name for name in imported
        if not re.search(rf"\b{re.escape(name)}\b", body)
    ]


def test_exporters_import_only_what_they_use():
    """an import a module never uses misstates what the module does:
    ``import sqlite3`` in the RO-Crate exporter implied it opened its own
    connection, when it borrows one from ``mareforma.db``. CI runs tests
    and no lint step, so nothing else catches F401. Scoped to the
    exporters; ``__init__`` is skipped because it exists to re-export.
    """
    offenders = {}
    for path in sorted((PACKAGE_DIR / "exporters").glob("*.py")):
        if path.name == "__init__.py":
            continue
        unused = _unused_import_names(path)
        if unused:
            offenders[path.name] = unused
    assert not offenders, (
        f"exporter modules importing names they never use, which reads as a "
        f"dependency the module does not have: {offenders}"
    )


@_requires_repo_checkout
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


@_requires_repo_checkout
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


# The read-side model-lineage authentication. Independence keys the distinct-
# model axis off the SIGNED lineage the claim's envelope binds, never the
# denormalised, unsigned ``evidence_lines.model_lineage`` column. Without it a
# direct or foreign writer rewrites that column to a fabricated distinct model
# and a single-source proposition promotes to CONVERGENT (independence 2). A
# built artifact that predates this authentication carries the inflation, so
# any wheel or sdist in ``dist/`` lacking these functions must not pass the
# pre-publish suite.
#
# Searched across the whole bundled package rather than one path. The original
# guard pinned ``mareforma/trust/_store.py``, which is where both functions
# lived when it was written; they now live in ``mareforma/trust/_gate.py``, so
# a pinned path would clear a stale artifact and fail a correct one. What has
# to be true is that the shipped code contains them, not where.
_MODEL_LINEAGE_AUTH_MARKERS = ("_authentic_model_key", "_signed_model_lineage")


def _bundled_package_defines(artifact: pathlib.Path, markers) -> "set | None":
    """Which of *markers* the artifact's bundled package defines.

    Returns None when the artifact ships no Python at all, so an unrelated
    archive dropped in ``dist/`` cannot fail the guard.
    """
    import zipfile

    found: set = set()
    saw_python = False

    def scan(name: str, blob: bytes) -> None:
        nonlocal saw_python
        if not name.endswith(".py"):
            return
        saw_python = True
        text = blob.decode("utf-8", "replace")
        for marker in markers:
            if f"def {marker}" in text:
                found.add(marker)

    if artifact.name.endswith(".whl"):
        with zipfile.ZipFile(artifact) as zf:
            for member in zf.namelist():
                scan(member, zf.read(member))
    elif artifact.name.endswith(".tar.gz"):
        with tarfile.open(artifact) as tf:
            for member in tf.getmembers():
                if not member.isfile():
                    continue
                fh = tf.extractfile(member)
                if fh is not None:
                    scan(member.name, fh.read())
    else:
        return None
    return found if saw_python else None


def test_built_artifacts_carry_model_lineage_authentication():
    """Every wheel and sdist in ``dist/`` binds the model-lineage authentication.

    A stale artifact, one built before the read-side authentication landed,
    ships a trust layer that keys independence off the unsigned
    ``evidence_lines.model_lineage`` column. A forged column then inflates a
    single-source proposition to CONVERGENT, and ``verify`` passes it. This
    runs in the pre-publish suite, with ``dist/`` populated by the release
    build, and fails any artifact missing the authentication so the inflation
    cannot ship. It skips cleanly when nothing is built.
    """
    dist_dir = REPO_ROOT / "dist"
    if not dist_dir.is_dir():
        pytest.skip("no dist/ built; nothing to check")
    artifacts = sorted(
        p for p in dist_dir.iterdir()
        if p.name.endswith(".whl") or p.name.endswith(".tar.gz")
    )
    if not artifacts:
        pytest.skip("dist/ holds no wheel or sdist to check")
    stale = []
    for artifact in artifacts:
        defined = _bundled_package_defines(
            artifact, _MODEL_LINEAGE_AUTH_MARKERS,
        )
        if defined is None:
            continue
        missing = [m for m in _MODEL_LINEAGE_AUTH_MARKERS if m not in defined]
        if missing:
            stale.append(f"{artifact.name} (missing {', '.join(missing)})")
    assert not stale, (
        "these built artifacts ship without the read-side model-lineage "
        "authentication, so a forged unsigned model_lineage column inflates "
        f"independence to CONVERGENT: {stale}. Rebuild dist/ from current "
        "source before publishing."
    )
