"""Doc-drift guards: reference docs must track the code they describe.

- #44  the documented status-policy stamp must equal ``STATUS_POLICY`` in
       code (the docs lagged at ``@v2`` while the code moved to ``@v3``).
- #54  every ``mareforma export --format`` choice defined in the CLI must
       be documented, and the blanket "NOT PROV-O-conformant" claim must
       be scoped now that a ``prov-o`` format exists.

Both read the source of truth from code, so they fail whenever the docs
fall behind a future change — not just today's drift.
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
    """#44: each current-policy page names the live stamp and no stale one."""
    for page in _POLICY_PAGES:
        text = page.read_text(encoding="utf-8")
        stamps = set(_POLICY_STAMP_RE.findall(text))
        assert STATUS_POLICY in stamps, (
            f"{page.name} does not document the live policy stamp {STATUS_POLICY!r}"
        )
        stale = stamps - {STATUS_POLICY}
        assert not stale, f"{page.name} still documents stale policy stamps {stale}"


def test_export_format_choices_documented():
    """#54: every --format choice from the CLI appears in cli.mdx."""
    cli_doc = (DOCS / "reference" / "cli.mdx").read_text(encoding="utf-8")
    assert "--format" in cli_doc, "cli.mdx does not document the --format option"
    for choice in _export_format_choices():
        assert choice in cli_doc, f"cli.mdx does not document --format={choice}"


def test_findings_convergent_example_uses_a_distinct_signer(tmp_path):
    """#28: the findings.mdx CONVERGENT recipe must reach CONVERGENT when run.

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
    """#54: the 'NOT PROV-O-conformant' line no longer reads as a blanket
    claim now that --format=prov-o emits real W3C PROV-O."""
    cli_doc = (DOCS / "reference" / "cli.mdx").read_text(encoding="utf-8")
    # If the page still warns about PROV-O non-conformance, it must also
    # point at the prov-o format so the claim reads as scope, not denial.
    if "PROV-O-conformant" in cli_doc:
        assert "--format=prov-o" in cli_doc or "`prov-o`" in cli_doc, (
            "the PROV-O-conformance caveat must name the prov-o format"
        )
