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

import inspect
import json
import pathlib
import re
import shutil

import click
import pytest

import mareforma
import mareforma.cli as cli_module
from mareforma.cli import (
    _VERIFY_FAIL,
    _VERIFY_OK,
    _VERIFY_UNVERIFIABLE,
    _VERIFY_USAGE,
    cli,
)
from mareforma.trust import STATUS_POLICY
from tests._helpers import _bootstrap_key, _est, _pred, _prop, _requires_repo_checkout

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

# This module reads docs/ and examples/, trees the sdist does not ship, so it
# skips as a unit when the shipped suite runs from an unpacked archive.
pytestmark = _requires_repo_checkout


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


def test_export_format_choices_documented():
    """every --format choice from the CLI appears in cli.mdx."""
    cli_doc = (DOCS / "reference" / "cli.mdx").read_text(encoding="utf-8")
    assert "--format" in cli_doc, "cli.mdx does not document the --format option"
    for choice in _export_format_choices():
        assert choice in cli_doc, f"cli.mdx does not document --format={choice}"


def test_every_cli_command_documented():
    """every visible top-level command is documented in cli.mdx.

    Reads the command list from ``cli.commands`` in code, the same source of
    truth ``_export_format_choices`` uses, so it fails whenever a future
    command drifts out of the reference, not only for today's ``audit`` and
    ``reexec`` gaps. Hidden/deprecated commands (e.g. ``stats``) are excluded
    because they are absent from ``--help`` and the reference by design. The
    check requires the canonical ``mareforma <cmd>`` form rather than a bare
    substring, so a command whose name is also a common word (``key``,
    ``map``, ``status``) cannot read as documented by coincidence.
    """
    cli_doc = (DOCS / "reference" / "cli.mdx").read_text(encoding="utf-8")
    visible = [name for name, cmd in cli.commands.items()
               if not getattr(cmd, "hidden", False)]
    missing = sorted(name for name in visible
                     if f"mareforma {name}" not in cli_doc)
    assert not missing, f"cli.mdx omits visible command(s): {missing}"


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
    cli_doc = (DOCS / "reference" / "cli.mdx").read_text(encoding="utf-8")
    # If the page still warns about PROV-O non-conformance, it must also
    # point at the prov-o format so the claim reads as scope, not denial.
    if "PROV-O-conformant" in cli_doc:
        assert "--format=prov-o" in cli_doc or "`prov-o`" in cli_doc, (
            "the PROV-O-conformance caveat must name the prov-o format"
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
