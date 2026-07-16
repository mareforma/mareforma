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

import pathlib
import re

import click

import mareforma
from mareforma.cli import cli
from mareforma.trust import STATUS_POLICY
from tests._helpers import _bootstrap_key, _est, _pred, _prop

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"


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
