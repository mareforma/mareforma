"""Example 04's answers, pinned so the example cannot rot.

The example imports langchain-core at module scope, so it cannot be imported
here. Its sections are delimited by ``sep(...)`` banners, and the tests below
run a section's shipped statements verbatim against a graph they build, which
is what keeps the printed verdicts honest about the graph on screen.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path

import mareforma
from mareforma import signing as _signing
from tests._helpers import _requires_repo_checkout

_EXAMPLE = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "04_private_data_public_findings"
    / "04_private_data_public_findings.py"
)

# examples/ is not in the sdist, so the shipped suite skips this module.
pytestmark = _requires_repo_checkout


def _banner(node: ast.stmt) -> str | None:
    """The title of a ``sep("...")`` section banner, if this node is one."""
    if (
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "sep"
        and node.value.args
        and isinstance(node.value.args[0], ast.Constant)
    ):
        return node.value.args[0].value
    return None


def _section(marker: str) -> str:
    """The example's statements under the section titled *marker*."""
    body = ast.parse(_EXAMPLE.read_text()).body
    banners = [(i, _banner(n)) for i, n in enumerate(body)]
    banners = [(i, title) for i, title in banners if title is not None]
    start = next(i for i, title in banners if title.startswith(marker))
    end = next((i for i, _ in banners if i > start), len(body))
    return ast.unparse(ast.Module(body=body[start + 1 : end], type_ignores=[]))


def _run_section(marker: str, **names) -> None:
    """Execute a section of the example with *names* bound."""
    show = names.pop("show", lambda label, value: print(f"  {label}: {value}"))
    exec(compile(_section(marker), str(_EXAMPLE), "exec"), {"show": show, **names})


def test_q1_one_key_two_labels_is_not_independent(tmp_path: Path, capsys) -> None:
    """One key signing both labs is not two lines, whatever the labels say.

    ``generated_by`` is a display label the producer picks and it plays no part
    in the promotion gate; the independence axis is a distinct non-NULL
    ``asserter_keyid``. A single-key operator writing two labels must not read
    as independent corroboration.
    """
    key = tmp_path / "_key"
    _signing.bootstrap_key(key)
    graph = mareforma.open(tmp_path, key_path=key)
    try:
        graph.assert_claim(
            "Candidate target T shows elevated activity in condition C",
            classification="ANALYTICAL",
            generated_by="lab_a/model-a",
            source_name="private_dataset_A",
        )
        graph.assert_claim(
            "Target T activity in condition C is specific to cell subtype S",
            classification="ANALYTICAL",
            generated_by="lab_b/model-b",
            source_name="private_dataset_B",
        )
        _run_section("Q1,", graph=graph)
    finally:
        graph.close()

    out = capsys.readouterr().out
    assert "not genuinely independent" in out
    assert "Two independent data sources" not in out


def _walk(graph, claim_id: str) -> list[str]:
    """The claim ids under *claim_id*, first support per hop, oldest first."""
    hops = []
    while claim_id:
        claim = graph.get_claim(claim_id)
        hops.append(claim_id)
        supports = json.loads(claim.get("supports_json") or "[]")
        claim_id = supports[0] if supports else None
    return list(reversed(hops))


def test_q3_prints_the_chain_the_graph_holds(tmp_path: Path, capsys) -> None:
    """Q3's chains must be walked from supports, not written by hand.

    Lab B's second claim cites Lab A's second claim, so it descends through Lab
    A's private-dataset chain. A diagram that shows it hanging off Lab B's own
    first claim hides the dependence the section teaches readers to look for.
    """
    key = tmp_path / "_key"
    _signing.bootstrap_key(key)
    graph = mareforma.open(tmp_path, key_path=key)
    try:
        upstream = graph.assert_claim(
            "Prior literature on Target T in condition C",
            classification="DERIVED",
            generated_by="agent_seed/literature",
            seed=True,
        )
        step_1 = graph.assert_claim(
            "Candidate target T shows elevated activity in condition C",
            classification="ANALYTICAL",
            supports=[upstream],
            source_name="private_dataset_A",
        )
        step_2 = graph.assert_claim(
            "Target T activity in condition C is specific to cell subtype S",
            classification="ANALYTICAL",
            supports=[step_1],
            source_name="private_dataset_A",
        )
        rep_1 = graph.assert_claim(
            "Candidate target T shows elevated activity in condition C (n=580)",
            classification="ANALYTICAL",
            supports=[upstream],
            source_name="private_dataset_B",
        )
        rep_2 = graph.assert_claim(
            "Target T activity in condition C is specific to cell subtype S"
            " (pathway analysis)",
            classification="ANALYTICAL",
            supports=[step_2],
            source_name="private_dataset_B",
        )
        walked = _walk(graph, rep_2)
        _run_section(
            "Q3,", graph=graph, json=json, step_2=step_2, rep_1=rep_1, rep_2=rep_2
        )
    finally:
        graph.close()

    out = capsys.readouterr().out
    printed = [line for line in out.splitlines() if rep_2[:8] in line]
    assert len(printed) == 1, f"no printed chain names rep_2:\n{out}"
    for claim_id in walked:
        assert claim_id[:8] in printed[0], printed[0]
    assert rep_1[:8] not in printed[0], printed[0]
