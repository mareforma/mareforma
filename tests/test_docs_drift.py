"""Doc-drift guards: reference docs must track the code they describe.

- the documented status-policy stamp must equal ``STATUS_POLICY`` in
       code (the docs lagged at ``@v2`` while the code moved to ``@v3``).
- every ``mareforma export --format`` choice defined in the CLI must
       be documented, and the blanket "NOT PROV-O-conformant" claim must
       be scoped now that a ``prov-o`` format exists.

Both read the source of truth from code, so they fail whenever the docs
fall behind a future change, not just today's drift.
"""

from __future__ import annotations

import ast
import builtins
import dataclasses
import importlib
import inspect
import itertools
import json
import pathlib
import re
import shutil
import warnings

import click
import pytest

import mareforma
import mareforma.cli as cli_module
import mareforma.trust
from mareforma.cli import (
    _VERIFY_FAIL,
    _VERIFY_OK,
    _VERIFY_UNVERIFIABLE,
    _VERIFY_USAGE,
    cli,
)
from mareforma.observe.oracle import perturbation_oracle
from mareforma.trust import STATUS_POLICY
from tests._helpers import _bootstrap_key, _est, _pred, _prop, _requires_repo_checkout
from tests.test_distinct_signer_model import _build_established

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

# This module reads docs/ and examples/, trees the sdist does not ship, so it
# skips as a unit when the shipped suite runs from an unpacked archive.
pytestmark = _requires_repo_checkout


def _prose(text: str) -> str:
    """Return *text* without fenced blocks.

    Comment lines inside a fence start with ``#``, so ``_section`` reads them
    as headings and cuts a section short of the prose that follows.
    """
    return re.sub(r"```.*?```", "", text, flags=re.DOTALL)


def _python_blocks(text: str) -> list[str]:
    """Return the bodies of every fenced ``python`` block in *text*."""
    return re.findall(r"```python\n(.*?)```", text, re.DOTALL)

# Reference/concept pages that state the *current* policy stamp. The
# changelog legitimately records superseded stamps, so it is excluded.
_POLICY_PAGES = (
    DOCS / "reference" / "api.mdx",
    DOCS / "concepts" / "findings.mdx",
    DOCS / "reference" / "data-model.mdx",
)

_POLICY_STAMP_RE = re.compile(r"status_policy@v\d+")

# Root keys the Mintlify docs.json schema accepts. Every branch of that schema
# sets ``additionalProperties: false``, so anything else is a setting the docs
# build never reads. Transcribed here to keep the guard offline.
_DOCS_JSON_ROOT_KEYS = frozenset(
    {
        "$schema", "api", "appearance", "background", "banner", "colors",
        "contextual", "description", "errors", "favicon", "fonts", "footer",
        "icons", "integrations", "interaction", "logo", "markdown", "metadata",
        "name", "navbar", "navigation", "public", "redirects", "search", "seo",
        "styling", "theme", "thumbnails", "variables",
    }
)


def test_docs_json_keys_are_in_the_declared_schema():
    """docs.json declares a schema; a key outside it is dead config that
    schema-aware editors flag and the docs build silently ignores.
    """
    config = json.loads((DOCS / "docs.json").read_text(encoding="utf-8"))
    stray = sorted(set(config) - _DOCS_JSON_ROOT_KEYS)
    assert not stray, (
        f"docs.json sets keys its declared schema forbids: {stray}"
    )


def _export_format_choices():
    export = cli.commands["export"]
    for param in export.params:
        if param.name == "fmt" and isinstance(param.type, click.Choice):
            return list(param.type.choices)
    raise AssertionError("export command has no --format Choice option")


def test_status_policy_stamp_documented_matches_code():
    """each current-policy page names the live stamp and no stale one."""
    for page in _POLICY_PAGES:
        text = page.read_text(encoding="utf-8")
        stamps = set(_POLICY_STAMP_RE.findall(text))
        assert STATUS_POLICY in stamps, (
            f"{page.name} does not document the live policy stamp {STATUS_POLICY!r}"
        )
        stale = stamps - {STATUS_POLICY}
        assert not stale, f"{page.name} still documents stale policy stamps {stale}"


# ``canonical_payload`` was removed because it produced different bytes from
# the production encoding. Private helpers with the word inside a longer name
# (``_verdict_canonical_payload``) sign verdict envelopes, not claims, so the
# guard matches the bare name only.
_DEAD_SIGNING_NAME_RE = re.compile(r"(?<![\w])canonical_payload")

# The changelog records the rename itself, so it keeps the old name.
_SIGNING_NAME_EXEMPT = frozenset({"CHANGELOG.md", "changelog.mdx"})


def _signing_name_sources():
    """Yield every doc and package file the signing-name guard reads."""
    for name in ("AGENTS.md", "ARCHITECTURE.md", "README.md"):
        yield ROOT / name
    yield from sorted(DOCS.rglob("*.mdx"))
    yield from sorted((ROOT / "mareforma").rglob("*.py"))


def test_signed_bytes_are_named_by_the_symbol_that_exists():
    """docs and comments must name the signed bytes ``canonical_statement``.

    ``canonical_statement`` is the only exported name; importing
    ``canonical_payload`` raises. The passages that still spell the removed
    name are written for external verifier authors and tell them to re-derive
    bytes by a name that resolves to nothing.
    """
    from mareforma import signing as _signing

    assert hasattr(_signing, "canonical_statement")
    assert not hasattr(_signing, "canonical_payload")

    stale = []
    for path in _signing_name_sources():
        if path.name in _SIGNING_NAME_EXEMPT:
            continue
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), 1
        ):
            if _DEAD_SIGNING_NAME_RE.search(line):
                stale.append(f"{path.relative_to(ROOT)}:{lineno}")
    assert not stale, (
        "these lines name the removed `canonical_payload`; the signed bytes "
        f"are `canonical_statement`: {stale}"
    )

# Every page that describes `mareforma export` to a reader. AGENTS.md is the
# in-repo agent contract, so it drifts out of the published pair unless it is
# held to the same guards.
_EXPORT_PAGES = (
    DOCS / "reference" / "cli.mdx",
    DOCS / "for-agents" / "agents.mdx",
    ROOT / "AGENTS.md",
)


def test_export_format_choices_documented():
    """every --format choice from the CLI appears on every export page."""
    for page in _EXPORT_PAGES:
        name = page.relative_to(ROOT)
        text = page.read_text(encoding="utf-8")
        assert "--format" in text, f"{name} does not document the --format option"
        for choice in _export_format_choices():
            assert choice in text, f"{name} does not document --format={choice}"


def _cli_section(name: str) -> str:
    """The body of one ``## <name>`` section of cli.mdx."""
    text = (DOCS / "reference" / "cli.mdx").read_text(encoding="utf-8")
    match = re.search(rf"^## {re.escape(name)}\n(.*?)(?=^## )",
                      text, re.M | re.DOTALL)
    assert match is not None, f"cli.mdx has no {name} section"
    return match.group(1)


def test_export_options_table_lists_every_option():
    """the export options table must name every flag the command takes.

    A reader signing a bundle for a project whose root validator is not the
    XDG bootstrap key needs ``--key``, and an option missing from the table
    reads as an option that does not exist. The page then sends that reader
    looking for a way to pin a signer the command already accepts.
    """
    table = _cli_section("export").split("**Options**", 1)[1]
    missing = [
        param.opts[0]
        for param in cli.commands["export"].params
        if isinstance(param, click.Option)
        and not param.hidden
        and not any(opt in table for opt in param.opts)
    ]
    assert not missing, f"cli.mdx export options table omits {missing}"


def test_verify_documents_the_payload_types_it_routes_a_file_by():
    """the verify page must describe file routing the way the code does.

    ``_verify_signed_file`` dispatches on the DSSE ``payloadType``: an audit
    receipt and an audit run record reach their own verifiers before the
    bundle verifier is ever tried, and the filename decides nothing. A page
    that says an existing file is verified as a signed bundle points the CI
    gate author who wired ``mareforma audit`` at the wrong verdict.
    """
    from mareforma import signing as _signing

    section = _cli_section("verify")
    assert "payloadType" in section, (
        "cli.mdx does not say verify routes a file by its DSSE payloadType"
    )
    for payload_type in (_signing.PAYLOAD_TYPE_AUDIT_RECEIPT,
                         _signing.PAYLOAD_TYPE_AUDIT_RUN):
        assert payload_type in section, (
            f"cli.mdx does not document the {payload_type} target kind"
        )


def test_every_cli_command_documented():
    """every visible top-level command is documented in cli.mdx.

    Reads the command list from ``cli.commands`` in code, the same source of
    truth ``_export_format_choices`` uses, so it fails whenever a future
    command drifts out of the reference, not only for today's ``audit`` and
    ``reexec`` gaps. Hidden/deprecated commands (e.g. ``stats``) are excluded
    because they are absent from ``--help`` and the reference by design. The
    check requires a section heading, not a substring: a row in the summary
    table satisfies any substring check, which is how six commands came to be
    listed with their arguments, options and defaults documented nowhere.
    """
    cli_doc = (DOCS / "reference" / "cli.mdx").read_text(encoding="utf-8")
    visible = [name for name, cmd in cli.commands.items()
               if not getattr(cmd, "hidden", False)]
    missing = sorted(
        name for name in visible
        if not re.search(rf"^#{{2,3}} {re.escape(name)}\b", cli_doc, re.M)
    )
    assert not missing, f"cli.mdx has no section for command(s): {missing}"


# Releases from this one on are mirrored section for section on the docs page.
# The page rewrote 0.3.0 and the 0.2.x line as themed summaries, and says so.
_MIRROR_FLOOR = (0, 3, 1)


def _release_sections(text: str, version_pattern: str) -> dict[str, list[str]]:
    """Map each release heading to its ``###`` section names, in order."""
    releases: dict[str, list[str]] = {}
    current: list[str] | None = None
    for line in text.splitlines():
        release = re.match(version_pattern, line)
        if release:
            current = releases.setdefault(release.group(1), [])
            continue
        section = re.match(r"### (.+)", line)
        if section is not None and current is not None:
            current.append(section.group(1).strip())
    return releases


def test_changelog_mirror_carries_every_section_of_each_release():
    """the docs changelog page must not drop a section of a mirrored release.

    The page says it mirrors ``CHANGELOG.md``, and it is the surface a user
    reaching the docs site first sees. A dropped Security block reads as a
    release with no security content.
    """
    changelog = _release_sections(
        (ROOT / "CHANGELOG.md").read_text(encoding="utf-8"),
        r"## \[([0-9][0-9.]*)\]",
    )
    mirror = _release_sections(
        (DOCS / "reference" / "changelog.mdx").read_text(encoding="utf-8"),
        r"## v([0-9][0-9.]*)",
    )
    drift = {
        version: (sections, mirror.get(version))
        for version, sections in changelog.items()
        if tuple(int(p) for p in version.split(".")) >= _MIRROR_FLOOR
        and mirror.get(version) != sections
    }
    assert not drift, f"the changelog mirror drifted from CHANGELOG.md: {drift}"


_CHANGELOG_PAGES = (
    ROOT / "CHANGELOG.md",
    DOCS / "reference" / "changelog.mdx",
)


def test_changelog_says_when_the_supports_counter_bumps(tmp_path):
    """the counter entry must name every write that moves the revision.

    ``record_supports_edges`` bumps the revision for a claim that carries no
    supports at all, because the cache stamps the claim count alongside it. An
    entry that scopes the counter to supports edges sends an implementer
    debugging a stale cache looking for an edge write that never happened.
    """
    from mareforma import _supports

    key = _bootstrap_key(tmp_path, "lab_a.key")
    with mareforma.open(tmp_path, key_path=key) as graph:
        before = _supports.supports_revision(graph._conn)
        graph.assert_claim("no supports here", generated_by="agent/a")
        after = _supports.supports_revision(graph._conn)
    assert after == before + 1, (
        "a claim with no supports no longer bumps the revision; re-read the "
        "changelog entry against the counter before changing this test"
    )
    for page in _CHANGELOG_PAGES:
        prose = " ".join(page.read_text(encoding="utf-8").split())
        assert (
            "a counter bumped by every claim insert and every supports-edge "
            "change" in prose
        ), f"{page.name} does not say when the supports counter bumps"


_RELEASE_0_3_11 = (
    (ROOT / "CHANGELOG.md", "## [0.3.11]"),
    (DOCS / "reference" / "changelog.mdx", "## v0.3.11"),
)


def test_changelog_names_every_extra_this_release_removed():
    """the release note must account for every install name that stopped resolving.

    0.3.10 published ``clawinstitute``, ``tooluniverse``, ``gemini`` and
    ``docs``. All four are gone, and an extra a distribution does not declare
    is ignored rather than refused, so a requirements file naming one keeps
    installing silently. The note is the only surface that can tell a reader
    auditing the upgrade why the name went away.
    """
    for page, heading in _RELEASE_0_3_11:
        text = page.read_text(encoding="utf-8")
        body = " ".join(_section(text, heading).split())
        missing = [
            extra
            for extra in ("clawinstitute", "tooluniverse", "gemini", "docs")
            if f"`{extra}`" not in body
        ]
        assert not missing, (
            f"the 0.3.11 entry in {page.name} does not name the removed "
            f"extras {missing}"
        )


def test_changelog_names_the_rekor_verifier_signature_change():
    """the release note must name the exported helper that gained a parameter.

    ``verify_rekor_inclusion`` is in ``mareforma.signing.__all__`` and now
    takes the signed envelope it is proving inclusion for, so every direct
    call written against 0.3.10 raises ``TypeError`` on the first upgrade.
    The release note is the only surface that connects that failure to the
    binding it enforces.
    """
    from mareforma.signing import verify_rekor_inclusion

    # Only the positional parameters carry the breaking change. A keyword-only
    # option with a default leaves every 0.3.10 call site working, so adding one
    # must not fail this guard.
    sig = inspect.signature(verify_rekor_inclusion)
    positional = [
        name
        for name, param in sig.parameters.items()
        if param.kind is param.POSITIONAL_OR_KEYWORD
    ]
    assert positional == ["rekor_body", "log_pubkey_pem", "envelope"], (
        "verify_rekor_inclusion no longer takes the envelope third; re-read "
        f"the release note against the signature before changing this test: {positional}"
    )
    for page, heading in _RELEASE_0_3_11:
        body = " ".join(_section(page.read_text(encoding="utf-8"), heading).split())
        assert "verify_rekor_inclusion" in body, (
            f"the 0.3.11 entry in {page.name} does not name "
            "verify_rekor_inclusion, whose call signature changed"
        )


def test_changelog_names_the_pre_registration_gate_on_the_default_token():
    """the release note must carry the gate that now fires without a run token.

    The guard resolves an omitted ``generated_by`` to the default run token,
    so a project that never sets one cannot submit a ``preregistered=1`` plan
    once any finding exists. That turns a working 0.3.10 script into a
    ``PostHocPlanError``, and the note is where an upgrading reader gets the
    name to search for.
    """
    for page, heading in _RELEASE_0_3_11:
        body = " ".join(_section(page.read_text(encoding="utf-8"), heading).split())
        assert "PostHocPlanError" in body, (
            f"the 0.3.11 entry in {page.name} does not name PostHocPlanError, "
            "which the pre-registration gate now raises on the default run token"
        )


def test_docs_scope_the_one_shot_pre_registration_exemption():
    """the docs must not promise the one-shot can never hit the post-hoc gate.

    The exemption is carried by the plan row, not by the call path: a one-shot
    that lands on a plan someone already registered with ``preregistered=1``
    submits under that claim and is refused, which
    ``tests/epistemic/test_plan_finding.py`` pins. A reader who mixes the two
    APIs in one run meets an exception both surfaces rule out.
    """
    from mareforma.trust import PostHocPlanError

    surfaces = (
        ("PostHocPlanError docstring", PostHocPlanError.__doc__),
        ("AGENTS.md", (ROOT / "AGENTS.md").read_text(encoding="utf-8")),
    )
    for name, text in surfaces:
        prose = " ".join(text.split())
        assert "synthesised plan is the one on record" in prose, (
            f"{name} does not scope the one-shot exemption to the plan row it "
            "owns, so it reads as an exemption by call path"
        )


def test_changelog_names_the_failed_open_verdict(tmp_path):
    """the release note must carry the verdict a failed cited open now lands.

    The 0.3.10 entry says a failed open still floors to OPAQUE, never
    UNGROUNDED. This release reversed that: when the observed failures account
    for every open of the cited path, the scope lands UNGROUNDED. Without a
    0.3.11 bullet the 0.3.10 sentence is the most recent word in the file, and
    a reader who wrote a gate on it has the two states swapped.
    """
    from mareforma import observe as obs
    from mareforma.observe import ObservedGrounding

    missing = str(tmp_path / "missing.csv")
    with obs.observe(cites=missing) as h:
        try:
            open(missing)
        except FileNotFoundError:
            pass
    assert h.verdict.grounding is ObservedGrounding.UNGROUNDED, (
        "a failed cited open no longer lands UNGROUNDED; re-read the release "
        "note against the classifier before changing this test"
    )
    for page, heading in _RELEASE_0_3_11:
        body = " ".join(_section(page.read_text(encoding="utf-8"), heading).split())
        assert "failed open" in body and "UNGROUNDED" in body, (
            f"the 0.3.11 entry in {page.name} does not say a failed open of a "
            "cited source now lands UNGROUNDED"
        )


_VALIDATORS_DDL_RE = re.compile(
    r"CREATE TABLE IF NOT EXISTS validators \((.*?)\n\);", re.DOTALL
)


def _validators_columns():
    """Return the ``validators`` column names in schema order."""
    from mareforma.db import _schema_sql

    body = _VALIDATORS_DDL_RE.search(_schema_sql._SCHEMA_SQL).group(1)
    return [
        line.split()[0]
        for line in body.strip().splitlines()
        if line.startswith("    ") and not line.strip().startswith("CHECK")
    ]


def test_data_model_documents_every_validators_column():
    """the column-level schema page must list all seven validators columns.

    The page is the authority a reader reconstructs an enrollment payload
    from, and ``validator_type`` is bound into the signed envelope, so a
    payload rebuilt from a six-column table never verifies.
    """
    section = _section(
        (DOCS / "reference" / "data-model.mdx").read_text(encoding="utf-8"),
        "## validators table",
    )
    missing = [c for c in _validators_columns() if f"`{c}`" not in section]
    assert not missing, (
        f"the validators table section omits column(s): {missing}"
    )


def test_data_model_documents_every_model_lineage_key():
    """the ``model_lineage`` row must list every key the observer stores.

    The page is what a reader rebuilds or audits a lineage record from, and
    ``independence_model_key`` reads ``attestor`` and ``digest`` first: a
    record rebuilt without them cannot express a local weights-digest
    identity at all, so it silently loses its distinct-model credit.
    """
    from mareforma.observe._lineage import ModelLineage

    section = _section(
        (DOCS / "reference" / "data-model.mdx").read_text(encoding="utf-8"),
        "### evidence_lines table",
    )
    row = next(line for line in section.splitlines()
               if line.startswith("| `model_lineage`"))
    stored = ModelLineage.from_dict({}).to_dict()
    missing = [key for key in stored if key not in row]
    assert not missing, (
        f"the model_lineage row omits stored key(s): {missing}"
    )


_STATE_CODE_RE = re.compile(r"RAISE\(ABORT, '(mareforma:state:[^']+)'\)")
_WATCHED_COLUMNS_RE = re.compile(r"BEFORE UPDATE OF\n(.*?)\nON claims", re.DOTALL)


def _laundering_prose(page):
    """The passage in *page* that lists what the laundering trigger watches."""
    lines = [
        line for line in page.read_text(encoding="utf-8").splitlines()
        if "claims_signed_fields_no_laundering" in line and "`created_at`" in line
    ]
    assert lines, (
        f"{page.name} no longer describes what "
        "claims_signed_fields_no_laundering watches"
    )
    return " ".join(lines)


def test_docs_name_every_column_the_laundering_trigger_watches():
    """the watched list must be the trigger's, not a shorter one.

    Both pages read as exhaustive. ``observed_grounding`` and
    ``predicate_payload`` are watched because flipping one to GROUNDED lifts
    the claims the observer refused to promote and clearing the other turns a
    binding violation into a clean verdict. An auditor reading a short list
    concludes those two columns are unguarded.
    """
    from mareforma.db import _schema_sql

    body = _WATCHED_COLUMNS_RE.search(_schema_sql._SIGNED_FIELDS_TRIGGER_SQL)
    columns = [c.strip() for c in body.group(1).replace("\n", " ").split(",")]
    # The pages write the five GRADE columns as the `ev_*` family.
    watched = {"ev_*" if c.startswith("ev_") else c for c in columns}
    for page in (DOCS / "reference" / "data-model.mdx", ROOT / "ARCHITECTURE.md"):
        prose = _laundering_prose(page)
        missing = sorted(c for c in watched if f"`{c}`" not in prose)
        assert not missing, (
            f"{page.name} omits watched column(s) {missing} from the "
            "claims_signed_fields_no_laundering list"
        )


def test_data_model_names_every_state_code_the_triggers_raise():
    """the page must carry the codes the triggers actually raise.

    ``RAISE()`` cannot concatenate a column value below SQLite 3.46, so every
    code is a static suffix rather than a from/to pair. A reader who writes a
    handler against the documented format matches a string the code has never
    emitted.
    """
    from mareforma.db import _schema_sql

    source = pathlib.Path(_schema_sql.__file__).read_text(encoding="utf-8")
    codes = set(_STATE_CODE_RE.findall(source))
    assert codes, "no mareforma:state: codes found in the schema module"
    page = (DOCS / "reference" / "data-model.mdx").read_text(encoding="utf-8")
    missing = sorted(code for code in codes if code not in page)
    assert not missing, (
        f"data-model.mdx does not name state code(s): {missing}"
    )


def test_data_model_documents_every_table_the_schema_creates():
    """the schema reference must not omit a table the release added.

    ``supports_revision`` carries the counter a stale supports cache is
    detected against and ``claims_fts`` backs every text search. A reader
    auditing ``graph.db`` against this page finds tables the page never
    mentions.
    """
    from mareforma.db import _schema_sql

    source = pathlib.Path(_schema_sql.__file__).read_text(encoding="utf-8")
    tables = set(re.findall(
        r"CREATE (?:VIRTUAL )?TABLE IF NOT EXISTS (\w+)", source
    ))
    page = (DOCS / "reference" / "data-model.mdx").read_text(encoding="utf-8")
    missing = sorted(name for name in tables if name not in page)
    assert not missing, f"data-model.mdx documents no table named: {missing}"


def test_bundle_pages_state_the_completeness_bound():
    """every page that describes what a verified bundle proves says what it
    does not prove about the claim set.

    ``verify_bundle`` walks the nodes the bundle carries and never the other
    way, so a claim removed with its subject entry and re-signed by the same
    key verifies clean (pinned by
    ``test_export_bundle.py::test_dropped_claim_still_verifies``). A page
    that stops at "an edited predicate fails" tells a reviewer the opposite.
    """
    pages = (
        ROOT / "ARCHITECTURE.md",
        ROOT / "AGENTS.md",
        DOCS / "for-agents" / "agents.mdx",
    )
    for page in pages:
        # Collapse the hard wrapping so the sentence is matched as prose.
        text = " ".join(page.read_text(encoding="utf-8").split())
        assert "not that they are all the claims in the graph" in text, (
            f"{page.name} describes bundle verification without the "
            "completeness bound"
        )


def test_cli_module_docstring_carries_no_partial_command_index():
    """the in-file command index is all or nothing.

    ``mareforma --help`` is generated from ``cli.commands`` and cli.mdx is
    guarded by the test above, so a hand-kept list in the module docstring
    is a third source of truth with nothing holding it to the code. It
    either names every visible command or names none and points at the two
    surfaces that are covered.
    """
    doc = cli_module.__doc__
    visible = [name for name, cmd in cli.commands.items()
               if not getattr(cmd, "hidden", False)]
    indexed = [name for name in visible if f"mareforma {name}" in doc]
    missing = sorted(set(visible) - set(indexed))
    assert not indexed or not missing, (
        f"cli.__doc__ indexes {len(indexed)} of {len(visible)} commands, "
        f"omitting {missing}"
    )


def test_every_verify_exit_code_is_tabled():
    """every page tabling verify's exit codes tables all four.

    A gate written from a table that stops at 2 has no branch for a usage
    error, so a typo'd flag reads as a pass. That is the misread
    ``_VerifyCommand`` remaps click's exit 2 to 3 to prevent, so a page
    that omits the row hands back the hole the code closed.
    """
    codes = (_VERIFY_OK, _VERIFY_FAIL, _VERIFY_UNVERIFIABLE, _VERIFY_USAGE)
    for page in (DOCS / "reference" / "cli.mdx",
                 DOCS / "concepts" / "trust.mdx"):
        text = page.read_text(encoding="utf-8")
        rows = set(re.findall(r"^\| `(\d+)` \|", text, re.M))
        missing = sorted(str(c) for c in codes if str(c) not in rows)
        assert not missing, f"{page.name} omits verify exit code(s): {missing}"


def test_trust_page_routes_verify_by_payload_type():
    """the page carrying the exit-code table must route a file as the CLI does.

    ``_verify_signed_file`` reads the DSSE ``payloadType`` and sends an audit
    receipt and a signed audit run to their own verifiers before the bundle
    fallback. A CI-gate author who reads "an existing file is a signed bundle"
    takes exit 2 on a receipt for "this artifact could not be checked" rather
    than "pin the auditor's key with --key".
    """
    # The prose name for each payload type the router branches on. A branch
    # with no entry here fails the guard rather than passing unnamed.
    labels = {
        "PAYLOAD_TYPE_AUDIT_RECEIPT": "audit receipt",
        "PAYLOAD_TYPE_AUDIT_RUN": "audit-run record",
    }
    routed = set(
        re.findall(
            r"PAYLOAD_TYPE_\w+", inspect.getsource(cli_module._verify_signed_file)
        )
    )
    page = " ".join(
        (DOCS / "concepts" / "trust.mdx").read_text(encoding="utf-8").split()
    )
    assert "payloadType" in page, (
        "trust.mdx does not say a file is routed by its DSSE payloadType"
    )
    for const in sorted(routed):
        label = labels.get(const)
        assert label is not None, (
            f"_verify_signed_file routes {const}, which this guard has no "
            "prose name for; name it here and on the page"
        )
        assert label in page, f"trust.mdx does not name the {label} target"


_CARDINAL_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
}


def test_architecture_trust_map_count_matches_its_command_bullets():
    """a spelled-out count goes stale the moment a bullet is appended.

    ARCHITECTURE.md is the design reference, so a reader who trusts the
    count over the list is left guessing which listed command does not
    ship. The section may state no count at all; it may not state a wrong
    one.
    """
    text = (ROOT / "ARCHITECTURE.md").read_text(encoding="utf-8")
    section = text.split("\n## Trust map\n", 1)[1].split("\n## ", 1)[0]
    bullets = re.findall(r"^- `mareforma \w+", section, re.MULTILINE)
    opening = section.strip().split(":", 1)[0].lower()
    stated = [
        _CARDINAL_WORDS[word]
        for word in re.findall(r"[a-z]+", opening)
        if word in _CARDINAL_WORDS
    ]
    assert all(count == len(bullets) for count in stated), (
        f"the Trust map opener counts {stated} commands but lists "
        f"{len(bullets)}: {opening}"
    )


def test_no_page_promises_a_cli_only_established_promotion():
    """ESTABLISHED needs a seed anchor, and no CLI command writes one.

    REPLICATED requires an ESTABLISHED upstream and ``claim validate``
    refuses anything below REPLICATED, so a walkthrough that never leaves
    the CLI dead-ends on a fresh graph. Only ``assert_claim(seed=True)``
    breaks the deadlock, and ``claim add`` exposes no ``--seed``. The guard
    lifts on its own the day the CLI grows that option.
    """
    add = cli.commands["claim"].commands["add"]
    if any("--seed" in param.opts for param in add.params):
        return
    pages = [ROOT / "AGENTS.md", *sorted(DOCS.rglob("*.mdx"))]
    offenders = sorted(
        str(page.relative_to(ROOT))
        for page in pages
        if "CLI only" in page.read_text(encoding="utf-8")
    )
    assert not offenders, (
        "pages promise a CLI-only path to ESTABLISHED, but `mareforma claim "
        f"add` cannot write the seed anchor: {offenders}"
    )


# Pages that carry a REPLICATED convergence recipe in prose.
_REPLICATION_PAGES = (
    ROOT / "AGENTS.md",
    DOCS / "for-agents" / "agents.mdx",
    DOCS / "concepts" / "trust.mdx",
)

# ``min_support="REPLICATED"`` is a query filter, not a promotion claim.
_MIN_SUPPORT_RE = re.compile(r"""min_support\s*=\s*["']REPLICATED["']""")


def test_replicated_recipes_sign_the_peers_with_distinct_keys():
    """a convergence recipe run under one key stays PRELIMINARY, silently.

    Promotion counts independence by distinct ``asserter_keyid``, so two
    peers asserted through one open graph share one keyid and never
    converge. A snippet that annotates that pair as REPLICATED teaches the
    reader that a same-key rerun counts as corroboration.
    """
    unsigned = []
    for page in _REPLICATION_PAGES:
        for block in _python_blocks(page.read_text(encoding="utf-8")):
            if "assert_claim" not in block:
                continue
            if "REPLICATED" not in _MIN_SUPPORT_RE.sub("", block):
                continue
            if "signer=" in block or block.count("key_path=") >= 2:
                continue
            unsigned.append(str(page.relative_to(ROOT)))
    assert not unsigned, (
        "REPLICATED recipes assert both peers under one key in: "
        + ", ".join(sorted(set(unsigned)))
    )


def test_findings_convergent_example_uses_a_distinct_signer(tmp_path):
    """the findings.mdx CONVERGENT recipe must reach CONVERGENT when run.

    The status counts independence by distinct signer, so two supporting lines
    converge only when a second signing key backs the second finding. A recipe
    that reuses one open graph (one key) stays PRELIMINARY, so the concept page
    must show the second finding signed under a distinct ``key_path``.
    """
    prop, plan, est = _prop(), _pred(), _est()

    # Reusing one signer stays PRELIMINARY: that is the trap the old snippet set.
    ka = _bootstrap_key(tmp_path, "lab_a.key")
    with mareforma.open(tmp_path, key_path=ka) as g:
        g.assert_finding(prop, plan, est, data_id="dataset_alpha",
                         generated_by="analyst/model-a/lab_a")
        g.assert_finding(prop, plan, est, data_id="dataset_beta",
                         generated_by="analyst/model-b/lab_b")
        assert g.proposition_status(prop)["status"] == "PRELIMINARY"

    # A distinct signer on a distinct dataset is the second independent line.
    kb = _bootstrap_key(tmp_path, "lab_b.key")
    with mareforma.open(tmp_path, key_path=kb) as g:
        g.assert_finding(prop, plan, est, data_id="dataset_gamma",
                         generated_by="analyst/model-c/lab_c")
        assert g.proposition_status(prop)["status"] == "CONVERGENT"

    # The page's CONVERGENT snippet must show the distinct-signer form.
    findings = (DOCS / "concepts" / "findings.mdx").read_text(encoding="utf-8")
    convergent = [b for b in _python_blocks(findings) if '"CONVERGENT"' in b]
    assert convergent, "findings.mdx has no CONVERGENT example block"
    assert all("key_path" in b for b in convergent), (
        "the CONVERGENT example must open the graph under a second signing key"
    )


def _call_arguments(text: str, name: str) -> list[str]:
    """Return the argument text of every ``name(...)`` call in *text*."""
    found = []
    for match in re.finditer(rf"\b{name}\(", text):
        depth = 1
        for index in range(match.end(), len(text)):
            if text[index] == "(":
                depth += 1
            elif text[index] == ")":
                depth -= 1
                if depth == 0:
                    found.append(text[match.end():index])
                    break
    return found


def test_docs_grounding_snippets_give_the_verdict_something_to_bind(tmp_path):
    """every docs snippet passing ``grounding=`` must cite a matchable source.

    The verdict is cross-checked against the finding's own citation. A
    plain-string ``data_id`` is an opaque token, not a matchable path or
    content address, so a snippet that omits ``data_source=`` (or
    ``data_bytes=``) stores the verdict unbound and never exercises the gate.
    A reader copying such a snippet signs a GROUNDED that was never checked.
    """
    from mareforma.observe import observe

    cited = tmp_path / "trial.csv"
    cited.write_text("a,b\n1,2\n", encoding="utf-8")
    key = _bootstrap_key(tmp_path, "lab_a.key")
    with mareforma.open(tmp_path, key_path=key) as graph:
        with observe(cites=str(cited)) as obs:
            cited.read_bytes()
        unbound = graph.assert_finding(_prop(), _pred(), _est(),
                                       data_id="dataset_alpha",
                                       generated_by="analyst/model-a/lab_a",
                                       grounding=obs.verdict)
        bound = graph.assert_finding(_prop(), _pred(), _est(),
                                     data_id="dataset_beta",
                                     data_source=str(cited),
                                     generated_by="analyst/model-a/lab_a",
                                     grounding=obs.verdict)
    assert "no finding citation to bind" in unbound["grounding"]["reason"]
    assert "no finding citation to bind" not in bound["grounding"]["reason"]

    unbindable = []
    for page in sorted(DOCS.rglob("*.mdx")):
        for block in _python_blocks(page.read_text(encoding="utf-8")):
            for arguments in _call_arguments(block, "assert_finding"):
                if "grounding=" not in arguments:
                    continue
                if "data_source=" in arguments or "data_bytes=" in arguments:
                    continue
                unbindable.append(str(page.relative_to(ROOT)))
    assert not unbindable, (
        "docs snippets bind a verdict with nothing to bind it to: "
        + ", ".join(sorted(set(unbindable)))
    )


_IDEMPOTENCY_KEY_RE = re.compile(r"idempotency_key=(\"[^\"]*\"|'[^']*')")


def test_idempotency_docs_do_not_teach_key_sharing_as_convergence(tmp_path):
    """no page may sell ``idempotency_key`` as a cross-agent merge.

    A replay carrying the same key with a divergent semantic field raises,
    on purpose: collapsing two authors into one row would discard the
    second contribution. So a snippet that replays a key must replay the
    same arguments, and no prose may call the key a convergence convention.
    """
    key = _bootstrap_key(tmp_path, "lab_a.key")
    with mareforma.open(tmp_path, key_path=key) as graph:
        graph.assert_claim("Target T elevated (cohort_1)",
                           idempotency_key="target_T_condition_C",
                           generated_by="agent/a")
        with pytest.raises(mareforma.IdempotencyConflictError) as raised:
            graph.assert_claim("Target T elevated (cohort_2)",
                               idempotency_key="target_T_condition_C",
                               generated_by="agent/b")
    assert "different text, generated_by" in str(raised.value)

    diverging = []
    for page in sorted(DOCS.rglob("*.mdx")):
        for block in _python_blocks(page.read_text(encoding="utf-8")):
            replays: dict[str, set[str]] = {}
            for arguments in _call_arguments(block, "assert_claim"):
                found = _IDEMPOTENCY_KEY_RE.search(arguments)
                if found:
                    replays.setdefault(found.group(1), set()).add(
                        " ".join(arguments.split())
                    )
            if any(len(calls) > 1 for calls in replays.values()):
                diverging.append(str(page.relative_to(ROOT)))
    assert not diverging, (
        "docs snippets replay one idempotency_key with different arguments, "
        "which raises: " + ", ".join(sorted(set(diverging)))
    )

    # Example 05 installs a virtualenv beside its script, so the examples
    # listing comes from git: a walk of the working tree hands this guard
    # every vendored source in that tree, including files no reader wrote
    # and some that are not UTF-8.
    suffixes = (".mdx", ".md", ".py")
    pages = [p for p in DOCS.rglob("*") if p.is_file() and p.suffix in suffixes]
    pages += [p for suffix in suffixes for p in _example_files(suffix)]
    convergence = sorted(
        str(p.relative_to(ROOT))
        for p in pages
        if "convergence convention" in p.read_text(encoding="utf-8").lower()
    )
    assert not convergence, (
        "pages still teach idempotency_key as a convergence convention: "
        + ", ".join(convergence)
    )


def test_quickstart_python_blocks_run_end_to_end(tmp_path, monkeypatch):
    """the quickstart must run as written, up to and including ESTABLISHED.

    Promotion refuses a validator whose keyid signed the claim or any peer
    in the converging set, so a page that promotes with the key that
    asserted the claim ends in ``SelfValidationError`` at its last step.
    The blocks are executed in order in a temp project; the only stub is
    ``analyze``, the reader's own pipeline.
    """
    from mareforma import signing

    project = tmp_path / "project"
    project.mkdir()
    monkeypatch.chdir(project)
    signing.bootstrap_key(signing.default_key_path())

    blocks = _python_blocks(
        (DOCS / "introduction" / "quickstart.mdx").read_text(encoding="utf-8")
    )
    namespace: dict = {"analyze": lambda source: 1.0}
    for block in blocks:
        exec(compile(block, "quickstart.mdx", "exec"), namespace)

    with mareforma.open(project, key_path=signing.default_key_path()) as graph:
        assert graph.get_claim(namespace["id_a"])["support_level"] == "ESTABLISHED"


def test_replicated_accordion_scopes_the_witnessing_policy_to_recovery(tmp_path):
    """the witnessing policy is a recovery rule, not a live promotion gate.

    ``require_rekor_witnessing()`` root-signs a policy row. The insert path
    never reads it: convergence keys on ``transparency_logged``, which is
    set at insert unless the graph was opened with a ``rekor_url``. So the
    page that defines REPLICATED must name ``restore`` wherever it names
    the policy, or it sells recovery enforcement as a live gate.
    """
    from mareforma import signing

    root = _bootstrap_key(tmp_path, "root.key")
    peer = signing.load_private_key(_bootstrap_key(tmp_path, "lab_b.key"))
    with mareforma.open(tmp_path, key_path=root) as graph:
        assert graph.require_rekor_witnessing()["rekor_required"] == 1
        graph.enroll_validator(signing.public_key_to_pem(peer.public_key()),
                               identity="lab_b")
        seed = graph.assert_claim("upstream", classification="DERIVED",
                                  generated_by="agent/seed", seed=True)
        a = graph.assert_claim("A", supports=[seed], generated_by="lab_a")
        b = graph.assert_claim("B", supports=[seed], generated_by="lab_b",
                               signer=peer)
        # No rekor_url, so both are born flagged and converge unwitnessed.
        assert graph.get_claim(a)["transparency_logged"] == 1
        assert graph.get_claim(a)["support_level"] == "REPLICATED"
        assert graph.get_claim(b)["support_level"] == "REPLICATED"

    trust = (DOCS / "concepts" / "trust.mdx").read_text(encoding="utf-8")
    accordion = re.search(
        r'<Accordion title="REPLICATED">(.*?)</Accordion>', trust, re.DOTALL
    )
    assert accordion, "trust.mdx has no REPLICATED accordion"
    body = accordion.group(1)
    if "require_rekor_witnessing" in body:
        assert "restore" in body, (
            "the REPLICATED accordion presents require_rekor_witnessing() as a "
            "live convergence gate; it is enforced on restore"
        )


def test_quickstart_signing_key_step_is_not_labelled_optional(tmp_path):
    """the key is optional for two sections of the page and required for the rest.

    Without a loaded signer ``assert_claim`` stores an unsigned claim that
    ``query`` hides unless the caller opts in with ``include_unverified``,
    and ``seed=True``, ``enroll_validator`` and ``validate`` all raise,
    since independence and promotion key on ``asserter_keyid``. The
    bootstrap step must not read as skippable.
    """
    with mareforma.open(tmp_path) as graph:
        graph.assert_claim("Cell type A receives more inhibitory input",
                           classification="ANALYTICAL",
                           source_name="dataset_alpha")
        assert graph.query("cell type A", min_support="PRELIMINARY") == []
        assert graph.query("cell type A", min_support="PRELIMINARY",
                           include_unverified=True)
        for call in (
            lambda: graph.assert_claim("upstream", classification="DERIVED",
                                       seed=True),
            lambda: graph.enroll_validator(b"pem", identity="lab_b"),
            lambda: graph.validate("00000000-0000-4000-8000-000000000000"),
        ):
            with pytest.raises(ValueError, match="signing key"):
                call()

    page = (DOCS / "introduction" / "quickstart.mdx").read_text(encoding="utf-8")
    heading = re.search(r"^##.*signing key.*$", page, re.MULTILINE | re.IGNORECASE)
    assert heading, "quickstart has no signing-key step"
    assert "optional" not in heading.group(0).lower(), (
        "the quickstart labels the signing key optional, yet its later "
        "sections seed, enroll and validate, all of which need one"
    )
    step = _section(page, heading.group(0))
    for required in ("seed", "enroll_validator", "validate", "include_unverified"):
        assert required in step, (
            f"the signing-key step must say {required} needs a key"
        )


def test_prov_o_claim_is_scoped_to_default_format():
    """the 'NOT PROV-O-conformant' line no longer reads as a blanket
    claim now that --format=prov-o emits real W3C PROV-O."""
    for page in _EXPORT_PAGES:
        text = page.read_text(encoding="utf-8")
        # If the page still warns about PROV-O non-conformance, it must also
        # point at the prov-o format so the claim reads as scope, not denial.
        if "PROV-O-conformant" in text:
            assert "--format=prov-o" in text or "`prov-o`" in text, (
                f"{page.relative_to(ROOT)}: the PROV-O-conformance caveat "
                "must name the prov-o format"
            )


def test_prov_o_overview_tracks_whether_the_export_path_validates():
    """the module overview must not promise a check the exporter skips.

    A reader consults it before trusting ``mareforma export --format=prov-o``
    output. While ``build_prov_o`` returns the document as built, the prose
    has to say the validator is the caller's to run; wire the validator into
    the export path and the disclaimer goes away with it.
    """
    from mareforma.exporters import prov_o

    source = inspect.getsource(prov_o.build_prov_o)
    overview = " ".join((prov_o.__doc__ or "").split())
    if "validate_prov_o(" not in source:
        assert "the exporter does not run it" in overview, (
            "build_prov_o returns the document unvalidated, so the module "
            "overview must say validate_prov_o is the consumer's to call"
        )


def test_replicated_promotion_docs_do_not_overclaim_the_model_gate(tmp_path):
    """the REPLICATED promotion prose must not claim a distinct-model gate.

    On the primary path a claim's finding model lineage is written after
    promotion runs, so the promotion-time ``model_distinct_pair`` filter reads
    absent on both sides and passes everything through. The load-bearing
    model-independence signal is the read-side effective-independence number.
    So two distinct signers on a shared ESTABLISHED upstream still promote to
    REPLICATED regardless of model, and the docs must not say otherwise.
    """
    from mareforma import signing as _signing

    ka = _bootstrap_key(tmp_path, "root.key")
    sa = _signing.load_private_key(_bootstrap_key(tmp_path, "a.key"))
    sb = _signing.load_private_key(_bootstrap_key(tmp_path, "b.key"))
    with mareforma.open(tmp_path, key_path=ka) as g:
        up = g.assert_claim("anchor", generated_by="seed", seed=True)
        a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
        b = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
        # No finding lineage on these claims, so the model gate is a no-op: a
        # same-(absent-)model pair under distinct keys promotes all the same.
        assert g.get_claim(a)["support_level"] == "REPLICATED"
        assert g.get_claim(b)["support_level"] == "REPLICATED"

    for name in ("AGENTS.md", "ARCHITECTURE.md"):
        text = (ROOT / name).read_text(encoding="utf-8")
        assert "genuinely different model" not in text, (
            f"{name} overclaims a distinct-model REPLICATED promotion gate; the "
            "read-side effective-independence number is the model signal"
        )


def test_deprecated_label_note_matches_deprecated_labels(tmp_path=None):
    """the "deprecated public labels" note must list only the real aliases.

    Only ``REPLICATED`` and ``ESTABLISHED`` are retired public labels resolved as
    one-release aliases (``mareforma._DEPRECATED_SUPPORT_LABELS``); ``PRELIMINARY``
    was never a module attribute and is not deprecated. The note on both pages
    must name exactly the aliases the code honors.
    """
    known = {"PRELIMINARY", "REPLICATED", "ESTABLISHED"}
    expected = set(mareforma._DEPRECATED_SUPPORT_LABELS)
    for page in (DOCS / "concepts" / "trust.mdx",
                 DOCS / "for-agents" / "agents.mdx"):
        text = page.read_text(encoding="utf-8")
        idx = text.index("are deprecated public labels")
        window = text[idx - 160:idx]
        listed = {label for label in known if f"`{label}`" in window}
        assert listed == expected, (
            f"{page.name} lists {listed} as deprecated public labels; the code "
            f"deprecates only {expected}"
        )


def _section(text: str, heading: str) -> str:
    """Return *text* from *heading* up to the next same-or-higher heading."""
    start = text.index(heading)
    level = heading[: len(heading) - len(heading.lstrip("#"))]
    rest = text[start + len(heading):]
    ahead = re.search(rf"^#{{1,{len(level)}}} ", rest, re.MULTILINE)
    return rest[: ahead.start()] if ahead else rest


def _documented_signature(text: str, name: str) -> tuple[list[str], dict[str, str]]:
    """Return the positional names and the keyword defaults a heading spells.

    Headings on the API reference carry the real call signature, so a reader
    can take one as the whole parameter set. The keyword map holds only the
    keywords the heading writes a default for; a heading that lists a bare
    name maps it to ``None``.
    """
    match = re.search(rf"^#+ `{re.escape(name)}\((.*?)\)`", text, re.MULTILINE)
    assert match, f"the reference has no signature heading for {name}"
    positional: list[str] = []
    keywords: dict[str, str] = {}
    seen_star = False
    for part in (p.strip() for p in match.group(1).split(",")):
        if part == "*":
            seen_star = True
            continue
        param, _, default = part.partition("=")
        if seen_star:
            keywords[param] = default or None
        else:
            positional.append(param)
    return positional, keywords


def _same_default(written: str, real) -> bool:
    """Whether a heading's written default is the value the code defaults to.

    The headings quote strings the way the page does; what has to match is the
    value, not the quote character.
    """
    try:
        return ast.literal_eval(written) == real
    except (ValueError, SyntaxError):
        return written == repr(real)


_SIGNATURE_HEADINGS = (
    ("perturbation_oracle", perturbation_oracle),
    ("assert_claim", mareforma.EpistemicGraph.assert_claim),
    ("search", mareforma.EpistemicGraph.search),
    ("get_tools", mareforma.EpistemicGraph.get_tools),
)


@pytest.mark.parametrize("name, func", _SIGNATURE_HEADINGS)
def test_api_signature_headings_carry_every_parameter(name, func):
    """A heading that drops a keyword hides a knob from the one page to check.

    ``perturbation_oracle`` lost ``multiplicity`` and ``thin_sigma_guard``,
    the two controls that widen the decision threshold before the influence
    call, and ``assert_claim`` lost the per-call ``signer`` that decides which
    key a claim is attributed to.
    """
    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    documented_positional, documented_keywords = _documented_signature(api, name)
    params = [p for p in inspect.signature(func).parameters.values() if p.name != "self"]
    real_positional = [p.name for p in params if p.kind is not p.KEYWORD_ONLY]
    real_keywords = {p.name: p.default for p in params if p.kind is p.KEYWORD_ONLY}

    assert documented_positional == real_positional, (
        f"the {name} heading lists positional parameters "
        f"{documented_positional}, the code takes {real_positional}"
    )
    assert set(documented_keywords) == set(real_keywords), (
        f"the {name} heading omits keywords "
        f"{sorted(set(real_keywords) - set(documented_keywords))} and invents "
        f"{sorted(set(documented_keywords) - set(real_keywords))}"
    )
    wrong = {
        param: (written, repr(real_keywords[param]))
        for param, written in documented_keywords.items()
        if written is not None and not _same_default(written, real_keywords[param])
    }
    assert not wrong, f"the {name} heading states defaults the code does not: {wrong}"


@pytest.mark.parametrize(
    "page, heading",
    [
        (DOCS / "reference" / "api.mdx", "### `assert_claim("),
        (DOCS / "for-agents" / "agents.mdx", "## `graph.assert_claim("),
        (ROOT / "AGENTS.md", "### `graph.assert_claim("),
    ],
)
def test_assert_claim_tables_document_every_parameter(page, heading):
    """The parameter table is where a reader looks up what a keyword does.

    A keyword named in the heading but absent from the table reads as a typo
    rather than a documented control. AGENTS.md is the contract an agent is
    pointed at, so a row missing there is a keyword it never learns to pass:
    without ``artifact_hash`` it cannot write a claim a strict-promotion
    project will promote.
    """
    documented = _table_rows(page.read_text(encoding="utf-8"), heading)
    params = inspect.signature(mareforma.EpistemicGraph.assert_claim).parameters
    missing = [
        name
        for name in params
        if name != "self" and name not in documented
    ]
    assert not missing, (
        f"the {page.name} assert_claim table has no row for: {missing}"
    )


def test_api_search_section_spells_its_own_parameter_set():
    """Describing one read path as "same as the other" hands over the wrong set.

    ``query()`` takes ``refutation_filter`` and ``search()`` refuses it, so a
    section that borrows ``query()``'s parameter table documents a call that
    raises ``TypeError``.
    """
    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    section = _section(_prose(api), "### `search(")
    search_params = set(inspect.signature(mareforma.EpistemicGraph.search).parameters)
    borrowed = sorted(
        name
        for name, param in inspect.signature(mareforma.EpistemicGraph.query).parameters.items()
        if param.kind is param.KEYWORD_ONLY and name not in search_params
    )
    unexplained = [name for name in borrowed if not re.search(rf"`{name}`.*only", section)]
    assert not unexplained, (
        "the search() section describes its parameters as query()'s, so it "
        f"documents kwargs search() rejects: {unexplained}"
    )
    undescribed = sorted(
        name
        for name in search_params
        if name not in ("self", "query") and f"`{name}`" not in section
    )
    assert not undescribed, (
        f"the search() section never names its own parameters: {undescribed}"
    )


def _documented_exceptions(api: str) -> dict[str, str]:
    """Return ``{exception name: submodule}`` for every exception the page names.

    Two places name one: the table under ``## Exceptions`` and the **Raises**
    block of ``enroll_validator``, which lists validator errors the table
    leaves out. Builtins are skipped; the page documents those as the stdlib
    names they are.
    """
    section = _section(api, "## Exceptions")
    documented = dict(
        re.findall(r"^\| `(\w+)` \| `(mareforma[\w.]*)` \|", section, re.MULTILINE)
    )
    raises = _section(_prose(api), "### `enroll_validator(")
    for name in re.findall(r"^- `(\w+Error)`:", raises, re.MULTILINE):
        if not hasattr(builtins, name):
            documented.setdefault(name, "mareforma.validators")
    return documented


def test_api_exceptions_resolve_where_the_page_says_they_do():
    """The page tells a caller to catch without remembering the submodule.

    A documented exception that only resolves inside its submodule turns that
    sentence into an ImportError at the top of the caller's file, and a
    top-level name bound to a different object than the submodule one would
    make the two import paths disagree at runtime.
    """
    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    documented = _documented_exceptions(api)
    assert documented, "no exceptions parsed out of reference/api.mdx"

    missing = sorted(name for name in documented if not hasattr(mareforma, name))
    assert not missing, (
        "reference/api.mdx promises every documented exception is re-exported "
        f"at the top level, but `from mareforma import` fails for: {missing}"
    )
    unlisted = sorted(name for name in documented if name not in mareforma.__all__)
    assert not unlisted, f"documented exceptions absent from __all__: {unlisted}"
    mismatched = {
        name: module
        for name, module in documented.items()
        if getattr(importlib.import_module(module), name, None)
        is not getattr(mareforma, name)
    }
    assert not mismatched, (
        "top-level name and documented submodule resolve to different objects "
        f"for: {mismatched}"
    )


def _docstring_raises(func) -> list[str]:
    """Return the exception names the numpydoc ``Raises`` section of *func* lists."""
    section = re.search(r"Raises\n-+\n(.*)", inspect.getdoc(func), re.DOTALL)
    return re.findall(r"^(\w+Error)$", section.group(1), re.MULTILINE)


@pytest.mark.parametrize(
    "page, heading",
    [
        (DOCS / "reference" / "api.mdx", "### `validate("),
        (ROOT / "AGENTS.md", "### `graph.validate("),
    ],
)
def test_docs_list_every_refusal_validate_can_raise(page, heading):
    """A handler written from a short Raises list misses the promotion gates.

    ``SelfValidationError`` is the default outcome on a single-key project and
    ``LLMValidatorPromotionError`` fires for any LLM-typed validator. Neither
    subclasses ``ValueError``, so a caller who caught what the page listed
    catches neither.
    """
    section = _section(_prose(page.read_text(encoding="utf-8")), heading)
    missing = [
        name
        for name in _docstring_raises(mareforma.EpistemicGraph.validate)
        if f"`{name}`" not in section
    ]
    assert not missing, (
        f"{page.name} documents validate() without the refusals it can raise: "
        + ", ".join(missing)
    )


def test_api_get_tools_section_names_the_shipped_tools(tmp_path):
    """Frameworks take a tool's name from ``fn.__name__``.

    An integrator who keys a dispatch table or a system prompt on a name from
    this page targets a tool the default surface does not expose.
    """
    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    section = _section(_prose(api), "### `get_tools(")
    documented = set(re.findall(r"^- `(\w+)\(", section, re.MULTILINE))
    for listed in re.findall(r"`\[([\w, ]+)\]`", section):
        documented.update(listed.split(", "))
    assert documented, "no tool names parsed out of the get_tools section"

    with warnings.catch_warnings():
        # Opening a fresh graph auto-enrolls the root validator and says so.
        warnings.simplefilter("ignore", UserWarning)
        with mareforma.open(tmp_path, key_path=_bootstrap_key(tmp_path)) as graph:
            shipped = {fn.__name__ for fn in graph.get_tools()}
    assert documented == shipped, (
        f"the get_tools section documents {sorted(documented - shipped)} and "
        f"omits {sorted(shipped - documented)}"
    )


def test_api_trust_errors_list_every_exported_trust_error():
    """The trust section carries one error list, and it reads as the whole set.

    ``PostHocPlanError`` is the pre-registration gate: it refuses a plan
    registered after the run's first finding. A reader who takes the list as
    complete never learns the gate exists until it fires.
    """
    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    block = re.search(r"^Errors: (.*?)\n\n", api, re.DOTALL | re.MULTILINE)
    documented = set(re.findall(r"`(\w+Error)`", block.group(1)))
    exported = {name for name in mareforma.trust.__all__ if name.endswith("Error")}
    assert documented == exported, (
        "the reference trust-error list omits "
        f"{sorted(exported - documented)} and invents {sorted(documented - exported)}"
    )
    section = _section(_prose(api), "#### `submit_finding(")
    assert "`PostHocPlanError`" in section, (
        "the submit_finding section documents its other refusals but not the "
        "pre-registration gate it raises"
    )


def test_api_exception_table_covers_every_exported_exception():
    """The table is the one index of what a caller can catch.

    An exported exception with no row is invisible to anyone writing error
    handling from the reference.
    """
    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    documented = _documented_exceptions(api)
    missing = sorted(
        name
        for name in mareforma.__all__
        if name.endswith("Error") and name not in documented
    )
    assert not missing, (
        "the reference/api.mdx exception table has no row for: " + ", ".join(missing)
    )


def test_api_tables_every_exception_it_names():
    """A name that reaches only prose is checked by nothing on this page.

    Six trust errors were named in the trust section and in no table, so the
    re-export guard never saw them and the page kept promising an import that
    raises ImportError.
    """
    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    named = {
        name
        for name in re.findall(r"`(\w+Error)`", api)
        if not hasattr(builtins, name)
    }
    missing = sorted(named - set(_documented_exceptions(api)))
    assert not missing, (
        "reference/api.mdx names exceptions no table of its own lists: "
        + ", ".join(missing)
    )


# The packages whose exceptions sit under ``MareformaError``. The Exceptions
# prose names them and the table's module column places each name, so both
# have to agree with the class tree.
_MAREFORMA_ERROR_PACKAGES = ("mareforma.db", "mareforma.trust")


def test_api_exception_table_splits_the_mareforma_error_tree_correctly():
    """A caller who mis-reads the tree writes a handler that catches nothing.

    The page tells readers which packages ``except MareformaError`` covers. It
    named ``mareforma.db`` alone while every trust error subclasses
    ``MareformaError``, so a reader hand-rolled a catch the base already made.
    """
    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    documented = _documented_exceptions(api)
    wrong = {
        name: module
        for name, module in documented.items()
        if issubclass(getattr(mareforma, name), mareforma.MareformaError)
        != (module in _MAREFORMA_ERROR_PACKAGES)
    }
    named = " and ".join(f"`{package}`" for package in _MAREFORMA_ERROR_PACKAGES)
    assert not wrong, (
        f"reference/api.mdx says the {named} exceptions subclass "
        f"MareformaError; the tree disagrees for: {wrong}"
    )
    prose = " ".join(_section(_prose(api), "## Exceptions").split())
    assert f"the {named} exceptions subclass `MareformaError`" in prose, (
        "the Exceptions prose does not name the packages under MareformaError"
    )


def test_api_documents_every_public_graph_member():
    """The site nominates the API reference as the per-method documentation.

    A public ``EpistemicGraph`` member with no heading there leaves a reader
    who followed that pointer with nowhere to go but the source.
    """
    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    undocumented = [
        name
        for name in sorted(vars(mareforma.EpistemicGraph))
        if not name.startswith("_")
        and not re.search(rf"^#+ .*`{name}[(`]", api, re.MULTILINE)
    ]
    assert not undocumented, (
        "reference/api.mdx has no heading for EpistemicGraph members: "
        + ", ".join(undocumented)
    )


def test_api_keeps_the_declared_and_observed_grounding_axes_apart():
    """The reference must not sell a self-declaration as a computed verdict.

    ``grounding_sensor`` writes the asserter's own score into the signed
    evidence vector and never touches the ``observed_grounding`` column, the
    axis that gates promotion. A table row saying the sensor computes the
    observed verdict collapses the one distinction the product rests on.
    """
    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    section = _section(api, "### `assert_claim(")
    sensor_row = next(
        line for line in section.splitlines() if line.startswith("| `grounding_sensor`")
    )
    assert "observed-grounding verdict for the claim" not in sensor_row, (
        "the grounding_sensor row states the sensor computes the observed "
        "verdict; it writes a declared score into the evidence vector"
    )
    assert "grounding_score" in sensor_row and "declar" in sensor_row.lower(), (
        "the grounding_sensor row must name the declared grounding_score it "
        "actually writes"
    )
    observed_row = next(
        line for line in section.splitlines() if line.startswith("| `observed_grounding`")
    )
    assert "promotion" in observed_row, (
        "the observed_grounding row must say the record gates promotion, not "
        "only that it is stored in a queryable column"
    )


def _importable(dotted: str) -> bool:
    """Whether *dotted* names a real module or an attribute of one."""
    try:
        importlib.import_module(dotted)
        return True
    except ImportError:
        pass
    module, _, attr = dotted.rpartition(".")
    try:
        return hasattr(importlib.import_module(module), attr)
    except ImportError:
        return False


def test_api_headings_name_importable_paths():
    """A heading is the path a reader copies, so it has to be the real one.

    ``open_db_from_db_path`` was headed as a top-level symbol while it lives
    only in ``mareforma.db``, and the package ``__getattr__`` turns every
    unlisted name into an ``AttributeError``.
    """
    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    headings = re.findall(r"^#+ `(mareforma[\w.]*\w)", api, re.MULTILINE)
    assert headings, "no mareforma headings parsed out of reference/api.mdx"
    unresolved = [name for name in headings if not _importable(name)]
    assert not unresolved, (
        "reference/api.mdx headings name paths that do not import: "
        + ", ".join(unresolved)
    )


_NUMBER_WORDS = ("no", "one", "two", "three", "four", "five", "six", "seven", "eight")

# The two ``health()`` keys that size the graph rather than count drift.
_POPULATION_KEYS = {"claim_count", "validator_count"}


def test_api_health_counts_its_own_drift_counters(tmp_path):
    """A miscounted summary sentence leaves an off-by-one a reader cannot settle.

    ``health()`` is the operator's audit summary and this page is what an
    integrator reads when wiring a green/red gate, so the sentence that says
    what "healthy" means has to name every counter it covers.
    """
    with mareforma.open(tmp_path) as graph:
        counters = sorted(set(graph.health()) - _POPULATION_KEYS)

    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    section = _section(_prose(api), "### `health()`")
    assert f"the {_NUMBER_WORDS[len(counters)]} drift counters" in section, (
        f"reference/api.mdx must say health() has {len(counters)} drift counters"
    )
    missing = [name for name in counters if f"`{name}`" not in section]
    assert not missing, (
        "the reference/api.mdx health() section never names the drift counters: "
        + ", ".join(missing)
    )


def test_api_documents_every_grounding_verdict_field_and_method():
    """A short field list hides state a caller reads off ``obs.verdict``.

    The enumeration is written as the whole object, so a name missing from it
    is a name a reader has no way to learn from the reference. ``model_lineage``
    is the one that costs: it carries the independence axis to the evidence
    line, and ``from_receipt`` is where it silently goes missing.
    """
    from mareforma.observe import GroundingVerdict

    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    section = _section(_prose(api), "### `GroundingVerdict`")
    names = [f.name for f in dataclasses.fields(GroundingVerdict)] + [
        name
        for name in vars(GroundingVerdict)
        if not name.startswith("_") and callable(getattr(GroundingVerdict, name))
    ]
    missing = [
        name for name in names if not re.search(rf"`{name}[`(]", section)
    ]
    assert not missing, (
        "the reference/api.mdx GroundingVerdict section never names: "
        + ", ".join(sorted(missing))
    )


def test_api_reference_names_every_registered_canonicalizer():
    """The reference is where an adapter author picks a canonical form.

    A form the page omits is one nobody names in ``result_canonical_form``, so
    digests get recorded against the older form's bytes. Some forms are shown
    under their constant, so a page naming either spelling counts.
    """
    from mareforma.canonicalize import registered_canonicalizers

    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    registered = sorted(registered_canonicalizers())
    assert registered, "no canonical forms registered; the guard checks nothing"
    missing = [
        name
        for name in registered
        if name not in api and name.upper().replace("-", "_") not in api
    ]
    assert not missing, (
        "reference/api.mdx names no canonical form for: " + ", ".join(missing)
    )


def _table_rows(text: str, heading: str) -> set[str]:
    """Return the first-column names of the first table under *heading*."""
    lines = text[text.index(heading) + len(heading):].splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("|"))
    block = itertools.takewhile(lambda line: line.startswith("|"), lines[start:])
    return {m.group(1) for m in (re.match(r"\| `(\w+)` \|", line) for line in block) if m}


def _assert_claim_keywords_used(text: str) -> set[str]:
    """Return the ``assert_claim`` keywords the samples in *text* pass."""
    real = set(inspect.signature(mareforma.EpistemicGraph.assert_claim).parameters)
    used: set[str] = set()
    for match in re.finditer(r"graph\.assert_claim\(", text):
        end, depth = match.end(), 1
        while depth:
            depth += (text[end] == "(") - (text[end] == ")")
            end += 1
        call = text[match.end():end - 1]
        used |= {
            name
            for name in re.findall(r"(?:^|,)\s*([a-z_]+)=", call, re.MULTILINE)
            if name in real
        }
    return used


def test_agents_page_defines_the_keywords_it_demonstrates():
    """The agents page states it mirrors AGENTS.md; the tables must agree.

    Its core sample is built around ``observed_grounding``, and the table
    below defines neither that nor the rest of what AGENTS.md carries. An
    agent reading the page it was pointed at has no contract for the keyword
    the page is organised around.
    """
    agents_mdx = (DOCS / "for-agents" / "agents.mdx").read_text(encoding="utf-8")
    canonical = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    documented = _table_rows(agents_mdx, "## `graph.assert_claim(")

    demonstrated = _assert_claim_keywords_used(agents_mdx) - documented
    assert not demonstrated, (
        "agents.mdx passes assert_claim keywords its table never defines: "
        + ", ".join(sorted(demonstrated))
    )
    unmirrored = _table_rows(canonical, "### `graph.assert_claim(") - documented
    assert not unmirrored, (
        "agents.mdx claims to mirror AGENTS.md but drops the rows: "
        + ", ".join(sorted(unmirrored))
    )


@pytest.mark.parametrize(
    "page, heading",
    [
        (DOCS / "reference" / "api.mdx", "## `mareforma.open("),
        (DOCS / "for-agents" / "agents.mdx", "## `mareforma.open("),
        (ROOT / "AGENTS.md", "### `mareforma.open("),
    ],
)
def test_open_tables_document_every_keyword(page, heading):
    """Every page presents the open() table as the whole option set.

    A missing row is a knob a reader generating an ``open()`` call never
    learns exists, and the omitted ones gate promotion and decide whether
    the caller's key is enrolled at all.
    """
    documented = _table_rows(page.read_text(encoding="utf-8"), heading)
    missing = [
        name
        for name in inspect.signature(mareforma.open).parameters
        if name not in documented
    ]
    assert not missing, (
        f"{page.name} open() table has no row for: {', '.join(missing)}"
    )

def test_unresolved_row_describes_the_restore_path_that_sets_it(tmp_path):
    """the `unresolved` row must not read as inert.

    No write path sets the flag, but restore replays it from a claims.toml
    that carries ``unresolved = true``, and a set flag holds the claim out of
    REPLICATED with no error. An operator who reads "always 0" rules out the
    one thing keeping the claim at PRELIMINARY.
    """
    from mareforma import db as _db
    from mareforma import signing as _signing
    from tests._helpers import _pem_of

    source = tmp_path / "source"
    source.mkdir()
    root_key = _bootstrap_key(source, "root.key")
    key_a, key_b = _bootstrap_key(source, "a.key"), _bootstrap_key(source, "b.key")
    with mareforma.open(source, key_path=root_key) as g:
        g.enroll_validator(_pem_of(key_a), identity="a")
        g.enroll_validator(_pem_of(key_b), identity="b")
        up = g.assert_claim("anchor", generated_by="seed", seed=True)
        first = g.assert_claim("A", supports=[up], generated_by="lab_a",
                               signer=_signing.load_private_key(key_a))

    # A pre-v0.3.10 capture that quarantined the claim.
    project = tmp_path / "restored"
    project.mkdir()
    toml = (source / "claims.toml").read_text(encoding="utf-8")
    (project / "claims.toml").write_text(
        toml.replace(f"[claims.{first}]", f"[claims.{first}]\nunresolved = true"),
        encoding="utf-8",
    )
    mareforma.restore(project)

    with mareforma.open(project, key_path=root_key) as g:
        assert g.get_claim(first)["unresolved"] == 1
        # The second independent line lands, and neither side promotes: the
        # convergence candidate query skips the quarantined row silently.
        second = g.assert_claim("B", supports=[up], generated_by="lab_b",
                                signer=_signing.load_private_key(key_b))
        assert g.get_claim(first)["support_level"] == "PRELIMINARY"
        assert g.get_claim(second)["support_level"] == "PRELIMINARY"

        assert [c["claim_id"] for c in _db.list_unresolved_claims(g._conn)] == [first]
        _db.mark_claim_resolved(g._conn, project, first)
        assert g.get_claim(first)["support_level"] == "REPLICATED"
        assert g.get_claim(second)["support_level"] == "REPLICATED"

    page = (DOCS / "reference" / "data-model.mdx").read_text(encoding="utf-8")
    row = next(line for line in page.splitlines()
               if line.startswith("| `unresolved` |"))
    assert "always `0`" not in row and "inert" not in row, (
        "the unresolved row calls the flag inert; restore replays it"
    )
    assert "mark_claim_resolved" in row, (
        "the unresolved row must name the clearing path"
    )
    index_row = next(line for line in page.splitlines()
                     if line.startswith("| `idx_claims_unresolved` |"))
    assert "inert" not in index_row, "the index row repeats the inert claim"


def test_api_compute_status_counts_independence_by_signer():
    """the compute_status reference must count independence by signer.

    Independence keys on the claim's ``asserter_keyid``, with ``generated_by``
    only the fallback for legacy or unsigned lines. The reference elsewhere says
    so (the REPLICATED rows), so the compute_status paragraph must not claim
    ``generated_by`` is the primary counting axis.
    """
    api = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    section = _section(api, "#### `compute_status(")
    assert "asserter_keyid" in section, (
        "compute_status must describe independence in signer (asserter_keyid) terms"
    )
    collapsed = " ".join(section.split())
    assert "counted by distinct run (`generated_by`)" not in collapsed, (
        "compute_status must not name generated_by as the primary independence axis"
    )


def test_example_05_promotion_prose_matches_the_signer_gate(tmp_path):
    """example 05 must state the gate it would actually meet, and promise no more.

    Promotion keys on distinct non-NULL ``asserter_keyid`` values over a shared
    ESTABLISHED upstream; ``generated_by`` is a display label. The script writes
    both forks through one open handle with no ``supports``, so both stay
    PRELIMINARY and the run report must not promise a promotion.
    """
    from mareforma import signing as _signing

    ka = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=ka) as g:
        # The way run_experiment.py writes: one key, no upstream.
        ra = g.assert_claim("RA target", generated_by="medea/gpt-4o/ra_cd4")
        sle = g.assert_claim("SLE target", generated_by="medea/gpt-4o/sle_cd4")
        assert g.get_claim(ra)["support_level"] == "PRELIMINARY"
        assert g.get_claim(sle)["support_level"] == "PRELIMINARY"

        # The gate the README must describe: distinct keys, shared anchor.
        sa = _signing.load_private_key(_bootstrap_key(tmp_path, "a.key"))
        sb = _signing.load_private_key(_bootstrap_key(tmp_path, "b.key"))
        up = g.assert_claim("anchor", generated_by="seed", seed=True)
        a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
        assert g.get_claim(a)["support_level"] == "PRELIMINARY"
        b = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
        assert g.get_claim(b)["support_level"] == "REPLICATED"

    example = ROOT / "examples" / "05_drug_target_provenance"
    readme = (example / "README.md").read_text(encoding="utf-8")
    section = " ".join(_section(readme, "## Promoting a finding").split())
    assert "asserter_keyid" in section, (
        "example 05 must state promotion in signer (asserter_keyid) terms"
    )
    assert "different `generated_by` fork" not in section, (
        "example 05 names generated_by as the independence axis; it is a label"
    )

    script = (example / "run_experiment.py").read_text(encoding="utf-8")
    assert "REPLICATED fires automatically" not in script, (
        "the run report promises a promotion these single-key writes cannot reach"
    )


def test_example_05_recorded_run_is_out_of_the_backup_writer_reach(tmp_path):
    """the recorded Case B claims must survive a run of the example.

    ``assert_claim`` backs the project up to ``claims.toml`` in the project
    root, so a recorded run parked there is overwritten by the first write the
    README's ``--run`` step makes. The recorded file lives under ``recorded/``,
    which no writer targets.
    """
    example = ROOT / "examples" / "05_drug_target_provenance"
    recorded = example / "recorded" / "case_b.claims.toml"
    assert recorded.is_file(), "the recorded Case B run must be committed"
    assert not (example / "claims.toml").exists(), (
        "a committed claims.toml in the project root is the backup writer's target"
    )

    project = tmp_path / "ex05"
    (project / "recorded").mkdir(parents=True)
    shutil.copy2(recorded, project / "recorded" / "case_b.claims.toml")
    before = recorded.read_bytes()

    with mareforma.open(project) as graph:
        graph.assert_claim(
            "RA target", generated_by="medea/gpt-4o/ra_cd4", source_name="medeadb"
        )

    assert (project / "recorded" / "case_b.claims.toml").read_bytes() == before, (
        "a run of the example rewrote the recorded Case B claims"
    )


def test_example_05_recorded_run_restores_as_the_readme_describes(tmp_path):
    """the README must state how the recorded capture restores.

    The capture predates signing, so ``restore`` rebuilds four unsigned claims
    that the default ``query`` drops. That is only honest if the README says
    so rather than presenting the file as a working recovery source.
    """
    example = ROOT / "examples" / "05_drug_target_provenance"
    recorded = example / "recorded" / "case_b.claims.toml"

    project = tmp_path / "restored"
    project.mkdir()
    shutil.copy2(recorded, project / "claims.toml")
    assert mareforma.restore(project)["claims_restored"] == 4

    with mareforma.open(project) as graph:
        assert graph.query("target") == []
        assert len(graph.query("target", include_unverified=True)) == 4

    section = " ".join(
        _section((example / "README.md").read_text(encoding="utf-8"), "## What this caught").split()
    )
    assert "recorded/case_b.claims.toml" in section, (
        "the README must point at the recorded capture by path"
    )
    assert "include_unverified=True" in section, (
        "the README must state that the capture is unsigned and needs include_unverified"
    )


def test_examples_carry_no_config_file_the_package_never_reads():
    """examples must not ship a project file no parser reads.

    claims.toml is the package's only TOML reader, and root discovery keys on
    ``.mareforma/graph.db`` rather than a marker file. A project.toml beside an
    example teaches a configuration mechanism mareforma does not have.
    """
    stray = sorted(
        str(p.relative_to(ROOT))
        for p in _example_files(".toml")
        if not p.name.endswith("claims.toml")
    )
    assert stray == [], f"no reader parses {stray}"


def test_get_tools_docstring_names_the_signing_key_as_the_axis():
    """the agent-framework entry point must not sell the label as the axis.

    Every tool from one ``get_tools`` binding signs with the key the graph was
    opened with, so varying ``generated_by`` per run yields one asserter keyid
    and never promotes. A docstring that ties the label to REPLICATED costs the
    integrator the run and teaches that a producer-controlled string is
    corroboration.
    """
    doc = " ".join((mareforma.EpistemicGraph.get_tools.__doc__ or "").split())
    conflated = [
        s for s in doc.split(". ")
        if "generated_by" in s and "REPLICATED" in s
        and "asserter_keyid" not in s and "signer" not in s
    ]
    assert not conflated, (
        "get_tools ties generated_by to REPLICATED without naming the signing "
        f"key: {conflated}"
    )
    assert "asserter_keyid" in doc, (
        "get_tools must name asserter_keyid as the independence axis"
    )


def test_predicate_registry_docstring_does_not_ask_adapters_to_register():
    """The extension contract in prose must match the one the suite enforces.

    ``tests/adapters/test_coexistence.py`` fails any adapter that registers a
    predicate URI as an import side effect, and every adapter-owned URI is
    seeded from ``BUILTIN_URIS`` at import. An author who follows a docstring
    that says otherwise gets a DeprecationWarning, a discarded owner, and a
    failing test.
    """
    doc = " ".join((mareforma.predicate_types.__doc__ or "").split())
    assert not re.search(r"register\W* at import time", doc), (
        "predicate_types tells adapters to call register() at import time, "
        "which test_adapter_imports_do_not_pollute_predicate_registry forbids"
    )
    assert "available in the current Python environment" not in doc, (
        "predicate_types claims predicates() reveals which adapter packages "
        "are installed, but every adapter URI is a seeded builtin"
    )


def test_build_statement_docstring_lists_the_real_classifications():
    """The signed-envelope reference must name the three values the code takes.

    ``build_statement`` is where a contributor reads what goes into the
    predicate. A classification it names that ``add_claim`` and the table
    CHECK both refuse costs a write and invents a tier the trust model does
    not have.
    """
    from mareforma._statement import build_statement
    from mareforma.db import VALID_CLASSIFICATIONS

    doc = inspect.getdoc(build_statement) or ""
    block = re.search(r"\nclassification\n(.*?)\n\S", doc, re.DOTALL)
    assert block, "build_statement no longer documents its classification arg"
    named = set(re.findall(r"``([A-Z]+)``", block.group(1)))
    assert named == set(VALID_CLASSIFICATIONS), (
        "build_statement documents classifications the write path refuses: "
        f"{sorted(named - set(VALID_CLASSIFICATIONS))}"
    )


def test_pre_ship_hardening_summary_names_only_paths_it_tests():
    """A suite header that names untested paths reads as coverage that is not there.

    Someone auditing which paths are pinned reads module summaries first, so a
    path word in this one has to be findable in the body below it.
    """
    import tests.test_pre_ship_hardening as module

    doc = module.__doc__ or ""
    source = pathlib.Path(module.__file__).read_text(encoding="utf-8")
    body = source.split('"""', 2)[-1]  # everything after the module docstring
    claimed = [
        word for word in ("convergence", "backup", "restore", "export", "bundle")
        if word in doc.lower() and word not in body.lower()
    ]
    assert not claimed, (
        "test_pre_ship_hardening advertises paths it does not test: "
        + ", ".join(claimed)
    )


_DOC_PATH_RE = re.compile(r"docs/[A-Za-z0-9_./-]+\.mdx?")


def test_source_doc_pointers_resolve():
    """A ``docs/...`` path in the package must name a page that exists.

    These pointers are a reader's route from a module to the schema it
    writes on the wire, and for some fields they are the only route. One
    that resolves to nothing costs a lookup and reads as "undocumented",
    so every path is pinned to the tree it names.
    """
    dead = [
        f"{path.relative_to(ROOT)}:{lineno} -> {ref}"
        for path in sorted((ROOT / "mareforma").rglob("*.py"))
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), 1
        )
        for ref in _DOC_PATH_RE.findall(line)
        if not (ROOT / ref).is_file()
    ]
    assert dead == [], "source points at docs pages that do not exist: " + ", ".join(dead)

def _restore_error_kinds() -> set[str]:
    """Every ``kind=`` literal restore.py raises a ``RestoreError`` with."""
    import ast

    path = ROOT / "mareforma" / "db" / "restore.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
    kinds = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if name != "RestoreError":
            continue
        for keyword in node.keywords:
            if keyword.arg == "kind" and isinstance(keyword.value, ast.Constant):
                kinds.add(keyword.value.value)
    return kinds


def test_every_restore_error_kind_is_documented():
    """A kind missing from the docs is a failure mode callers cannot match on.

    ``RestoreError.kind`` exists so a caller can branch without parsing the
    message. Any kind the rebuild raises but no reader lists is a branch
    nobody writes, and the policy and Rekor kinds are exactly the ones that
    fire once transparency-log enforcement is on.
    """
    from mareforma.db.errors import RestoreError

    readers = {
        "AGENTS.md": (ROOT / "AGENTS.md").read_text(encoding="utf-8"),
        "docs/reference/api.mdx": (DOCS / "reference" / "api.mdx").read_text(
            encoding="utf-8"
        ),
        "docs/reference/cli.mdx": (DOCS / "reference" / "cli.mdx").read_text(
            encoding="utf-8"
        ),
        "RestoreError docstring": RestoreError.__doc__ or "",
        "mareforma.restore docstring": mareforma.restore.__doc__ or "",
    }
    missing = sorted(
        f"{reader} is missing {kind!r}"
        for kind in _restore_error_kinds()
        for reader, text in readers.items()
        if kind not in text
    )
    assert not missing, "undocumented RestoreError kinds: " + ", ".join(missing)


def test_agents_restore_signature_names_every_keyword():
    """AGENTS.md is the in-repo agent contract; a short signature hides options.

    An agent that reads only the contract cannot pass a parameter the
    contract never names, so it stays on the default without knowing there
    is a choice.
    """
    import inspect

    text = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    heading = next(
        line for line in text.splitlines() if line.startswith("### `mareforma.restore(")
    )
    keywords = [
        name
        for name, param in inspect.signature(mareforma.restore).parameters.items()
        if param.kind is inspect.Parameter.KEYWORD_ONLY
    ]
    absent = [name for name in keywords if name not in heading]
    assert not absent, (
        f"AGENTS.md restore heading omits keyword parameters {absent}: {heading}"
    )


# The two references that enumerate what a ``query()`` result dict holds,
# keyed by the heading each one gives the method.
_QUERY_PAGES = {
    "AGENTS.md": (ROOT / "AGENTS.md", "### `graph.query("),
    "docs/reference/api.mdx": (DOCS / "reference" / "api.mdx", "### `query("),
}

# A backticked identifier, allowing the ``name: type`` form the reputation
# projections are written in.
_RESULT_KEY_RE = re.compile(r"`([a-z_][a-z0-9_]*)(?::[^`]*)?`")


def _documented_query_keys(path: pathlib.Path) -> set[str]:
    """The result keys a page names in its ``query()`` returns block."""
    text = path.read_text(encoding="utf-8")
    start = text.index("ach dict contains:")
    return set(_RESULT_KEY_RE.findall(text[start:text.index("**Raises", start)]))


def test_query_result_keys_match_the_projection(tmp_path):
    """Both references must name every key ``query()`` hands back.

    A key no reader lists is a key an agent reopens the database to recover,
    and the undocumented set held ``observed_grounding`` and
    ``asserter_keyid``, the grounding and independence signals. Equality also
    catches the reverse drift, a page still listing a dropped column.
    """
    root_key, _, _, _ = _build_established(tmp_path)
    with mareforma.open(tmp_path, key_path=root_key) as g:
        rows = g.query(min_support="ESTABLISHED", limit=9)
    assert rows, "no ESTABLISHED row to read the full projection from"
    returned = set(rows[0])

    drift = {}
    for name, (path, _heading) in _QUERY_PAGES.items():
        documented = _documented_query_keys(path)
        if documented != returned:
            drift[name] = {
                "undocumented": sorted(returned - documented),
                "not returned": sorted(documented - returned),
            }
    assert not drift, f"query() result keys drifted from the docs: {drift}"


def _documented_return_keys(name: str) -> set[str]:
    """The keys the API reference's ``**Returns**`` block names for *name*."""
    text = (DOCS / "reference" / "api.mdx").read_text(encoding="utf-8")
    block = text[text.index(f"#### `{name}("):]
    returns = re.search(r"\*\*Returns\*\* `dict`(.*?)\n\n", block, re.DOTALL)
    assert returns is not None, f"api.mdx has no Returns block for {name}"
    return set(_RESULT_KEY_RE.findall(returns.group(1)))


def test_finding_return_keys_match_the_reference(tmp_path):
    """The two finding surfaces must name every key they hand back.

    A Returns list reads as closed, so a key it leaves out is one a caller
    finds only by reading the source. ``model_lineage`` is the independence
    signal on a written finding, and ``lines_skipped`` is the disclosure
    that evidence dropped out of the counts, the case that key exists to
    make visible.
    """
    root_key = _bootstrap_key(tmp_path, "lab_a.key")
    with mareforma.open(tmp_path, key_path=root_key) as graph:
        result = graph.assert_finding(
            _prop(), _pred(), _est(),
            data_id="dataset_alpha", generated_by="analyst/a",
        )

    surfaces = {
        "assert_finding": set(result),
        "proposition_status": set(result["proposition_status"]),
    }
    drift = {}
    for name, returned in surfaces.items():
        documented = _documented_return_keys(name)
        if documented != returned:
            drift[name] = {
                "undocumented": sorted(returned - documented),
                "not returned": sorted(documented - returned),
            }
    assert not drift, f"api.mdx return keys drifted from the code: {drift}"


def test_query_parameters_documented_in_both_references():
    """A parameter no reference names is a filter nobody passes.

    ``refutation_filter`` is the refutation-state filter; an agent reading a
    table without it queries everything and filters by hand.
    """
    import inspect

    parameters = list(inspect.signature(mareforma.EpistemicGraph.query).parameters)[1:]
    absent = {}
    for name, (path, heading) in _QUERY_PAGES.items():
        section = _section(path.read_text(encoding="utf-8"), heading)
        missing = [p for p in parameters if f"`{p}`" not in section]
        if missing:
            absent[name] = missing
    assert not absent, f"query() parameters missing from the docs: {absent}"


_GRAPH_CALL_RE = re.compile(r"graph\.([A-Za-z_][A-Za-z0-9_]*)\(")


def _graph_calls_in_strings():
    """Yield ``(path, lineno, name)`` for every ``graph.<name>(`` in a literal."""
    import ast

    package = ROOT / "mareforma"
    for path in sorted(package.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
                continue
            for match in _GRAPH_CALL_RE.finditer(node.value):
                yield path.relative_to(ROOT), node.lineno, match.group(1)


def test_message_strings_name_real_graph_methods():
    """Advice text must not send an operator to a method that does not exist.

    Error messages and docstrings that spell ``graph.<name>()`` are read as
    a call to run. Every such name has to resolve on ``EpistemicGraph``, or
    the reader gets an AttributeError at the moment they need the fix.
    """
    phantom = [
        f"{path}:{lineno} -> graph.{name}()"
        for path, lineno, name in _graph_calls_in_strings()
        if not hasattr(mareforma.EpistemicGraph, name)
    ]
    assert not phantom, (
        "message strings name methods EpistemicGraph does not have: "
        + ", ".join(phantom)
    )


def test_conn_cache_tests_name_the_connection_type_they_run_on(tmp_path):
    """The class summary is where a maintainer learns what the cache runs on.

    ``open_db`` builds every connection as the attribute-accepting subclass, so
    the tests can observe the cache on the real type. A summary that names a
    different type, or a stand-in connection class in the body, teaches the
    maintainer to test something the product never builds.
    """
    from mareforma.db import open_db
    from tests.test_validators import TestConnCacheInvalidation

    conn = open_db(tmp_path)
    try:
        conn_type = type(conn).__name__
    finally:
        conn.close()

    doc = TestConnCacheInvalidation.__doc__ or ""
    assert conn_type in doc, (
        f"the conn-cache tests run on {conn_type}, which their summary does "
        "not name"
    )
    stand_ins = sorted(
        name
        for name, value in vars(TestConnCacheInvalidation).items()
        if isinstance(value, type)
    )
    assert not stand_ins, (
        "the conn-cache tests define connection stand-ins instead of using "
        f"{conn_type}: " + ", ".join(stand_ins)
    )


def test_epistemic_builders_expose_one_graph_opener():
    """The shared builders exist to collapse duplicated graph openers.

    Every opener here bootstraps a key and opens the project, and the key
    file's name carries no meaning to the package, so a second opener reads
    as a capability difference the graph does not have.
    """
    import tests.epistemic._builders as builders

    openers = sorted(name for name in vars(builders) if name.startswith("open_"))
    assert openers == ["open_graph"], (
        "tests/epistemic/_builders.py defines more than one graph opener: "
        + ", ".join(openers)
    )
    named = set(re.findall(r"``(open_[A-Za-z_]+)``", builders.__doc__ or ""))
    assert named == set(openers), (
        "the builders docstring names openers the module does not define: "
        + ", ".join(sorted(named.symmetric_difference(openers)))
    )


def _scenarios(doc: str) -> dict[str, str]:
    """Return ``{heading: bullet text}`` for a "Scenarios covered" list.

    Headings sit at two spaces of indent, their bullets at four.
    """
    scenarios: dict[str, str] = {}
    heading = None
    for line in doc.split("Scenarios covered", 1)[-1].splitlines():
        if re.fullmatch(r" {2}\S.*", line):
            heading = line.strip()
            scenarios[heading] = ""
        elif heading and line.strip():
            scenarios[heading] += " " + line.strip()
    return scenarios


def test_trust_ladder_summary_matches_the_scenarios_it_pins():
    """The ship gate's own summary is where a reader learns the ladder's edges.

    A scenario with no section under it sends the reader hunting for coverage
    that is not there, and a scenario the tests refuse must read as a refusal,
    or the summary teaches the inverse of what ships.
    """
    import tests.epistemic.test_trust_ladder as module

    body = pathlib.Path(module.__file__).read_text(encoding="utf-8")
    body = body.split('"""', 2)[-1]  # everything after the module docstring
    scenarios = _scenarios(module.__doc__ or "")
    assert scenarios, "test_trust_ladder lists no scenarios"

    missing = [head for head in scenarios if head not in body]
    assert not missing, (
        "test_trust_ladder advertises scenarios it does not test: "
        + ", ".join(missing)
    )

    # Banner comments carry the headings; the piece after a banner is its code.
    pieces = re.split(r"\n# -{10,}", body)
    inverted = [
        head
        for head, bullets in scenarios.items()
        for i, piece in enumerate(pieces[:-1])
        if head in piece
        and "pytest.raises" in pieces[i + 1]
        and not re.search(r"refus|reject", bullets, re.IGNORECASE)
    ]
    assert not inverted, (
        "test_trust_ladder describes refused shapes as permitted: "
        + ", ".join(inverted)
    )


# HTTP clients and raw sockets, the only ways a module here reaches the network.
_NETWORK_CLIENT = re.compile(r"\b(?:httpx|requests|urllib|aiohttp|socket)\b")


def test_security_policy_names_no_doi_registry_as_a_network_upstream():
    """the out-of-scope list scoped network attacks against Crossref and
    DataCite. DOI handling is format-only: ``doi_resolver`` reaches no HTTP
    client and the assert path never resolves a DOI, so those registries are
    an attack surface the package cannot have. Naming them invents a trust
    boundary and hides the upstreams that do carry one.
    """
    resolver = (ROOT / "mareforma" / "doi_resolver.py").read_text(encoding="utf-8")
    assert not _NETWORK_CLIENT.search(resolver), (
        "doi_resolver reaches the network now; re-derive the upstream list in "
        "SECURITY.md before relaxing this guard"
    )
    security = (ROOT / "SECURITY.md").read_text(encoding="utf-8")
    named = sorted(registry for registry in ("Crossref", "DataCite") if registry in security)
    assert not named, (
        f"SECURITY.md names DOI registries as network upstreams, but mareforma "
        f"never contacts one: {named}"
    )
