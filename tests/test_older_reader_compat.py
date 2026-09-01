"""A graph this code wrote still opens under the last published release.

The additive rule is what buys the format work its place in this release, and it
is a claim about a program that is already shipped, so it cannot be checked by
reading this tree. It is checked by extracting 0.3.12 from git, putting it first
on ``sys.path`` in a subprocess, and handing it a graph and a backup this code
produced.

Three things break an older reader, and all three are refusals rather than
mistakes: a new column on ``claims`` fails the exact column-set match, a bumped
``user_version`` fails the version gate, and either one tells the operator to
delete graph.db. A new table and a new ``claims.toml`` section are the two moves
that do not, which is why the chain and the witness are shaped the way they are.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import mareforma
from tests._helpers import _bootstrap_key, _enroll_key


_REPO_ROOT = Path(__file__).resolve().parent.parent
_V0312_COMMIT = "94e0fc7"


def _v0312_available() -> bool:
    try:
        subprocess.run(
            ["git", "-C", str(_REPO_ROOT), "rev-parse", "--verify",
             f"{_V0312_COMMIT}^{{commit}}"],
            capture_output=True, check=True,
        )
        return True
    except (OSError, subprocess.CalledProcessError):
        return False


pytestmark = pytest.mark.skipif(
    not _v0312_available(),
    reason="builds the 0.3.12 tree from git; needs the commit reachable",
)


def _extract_v0312(work: Path) -> Path:
    """Unpack the 0.3.12 package tree, beside the project rather than in it."""
    pkg_dir = work / "v0312"
    pkg_dir.mkdir(parents=True, exist_ok=True)
    archive = subprocess.run(
        ["git", "-C", str(_REPO_ROOT), "archive", _V0312_COMMIT, "mareforma"],
        capture_output=True, check=True,
    ).stdout
    subprocess.run(["tar", "-x", "-C", str(pkg_dir)], input=archive, check=True)
    return pkg_dir


def _run_under_v0312(pkg_dir: Path, body: str, *args: str) -> str:
    """Run *body* with the extracted 0.3.12 package shadowing the installed one.

    Third-party dependencies are shared; only the package source differs.
    """
    script = textwrap.dedent(
        """
        import sys
        sys.path.insert(0, sys.argv[1])
        from pathlib import Path
        import mareforma
        assert mareforma.__version__.startswith("0.3.12"), mareforma.__version__
        root = Path(sys.argv[2])
        """
    ) + textwrap.dedent(body)
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    proc = subprocess.run(
        [sys.executable, "-c", script, str(pkg_dir), *args],
        capture_output=True, text=True, env=env,
    )
    if proc.returncode != 0:
        raise AssertionError(
            "the 0.3.12 subprocess refused a graph this code wrote:\n"
            f"{proc.stdout}\n{proc.stderr}"
        )
    return proc.stdout.strip()


def _graph_with_a_chain(root: Path) -> Path:
    """A graph carrying both new tables and every new claims.toml section.

    Populated, not merely present: an empty table would let this pass on a
    change that only creates schema, and what has to stay readable is a file
    with rows in the parts an older reader has never heard of.
    """
    from mareforma.observe import observe

    root_key = _bootstrap_key(root, "root.key")
    data = root / "trial.csv"
    data.write_text("arm,outcome\ntreat,1\n")
    with mareforma.open(root, key_path=root_key) as g:
        with observe(cites=str(data.resolve())) as handle:
            data.read_text()
        g.assert_claim(
            "an observed finding", classification="ANALYTICAL",
            predicate_payload={
                "data_sources": [str(data.resolve())], "data_ids": [],
            },
            observed_grounding=handle.verdict.to_signed_dict(),
        )
        older = g.assert_claim("the older claim", generated_by="run1")
        newer = g.assert_claim("the newer claim", generated_by="run2")
    witness = _bootstrap_key(root, "witness.key")
    _enroll_key(root, root_key, witness, identity="witness@example.org")
    with mareforma.open(root, key_path=witness) as g:
        g.record_contradiction_verdict(
            verdict_id="v1", member_claim_id=newer, other_claim_id=older,
        )

    # A census entry, so the section that carries one is in the file an older
    # reader is handed. Without a guard having gone missing the section is
    # absent, and the compatibility question about it would go unasked.
    import sqlite3
    raw = sqlite3.connect(root / ".mareforma" / "graph.db")
    raw.execute("DROP TRIGGER findings_no_delete")
    raw.commit()
    raw.close()
    with mareforma.open(root, key_path=root_key) as g:
        g.assert_claim("written after the guard went", generated_by="run3")
    return root_key


def test_the_new_table_and_sections_are_actually_there(tmp_path: Path) -> None:
    """Guards the two tests below against passing on an empty change."""
    import sqlite3
    try:
        import tomllib          # 3.11+ stdlib
    except ModuleNotFoundError:  # 3.10, where it is the tomli backport
        import tomli as tomllib  # type: ignore[no-redef]

    _graph_with_a_chain(tmp_path)
    conn = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
    assert conn.execute(
        "SELECT COUNT(*) FROM verdict_chain"
    ).fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM grounding_attestations"
    ).fetchone()[0] == 1
    conn.close()
    data = tomllib.loads((tmp_path / "claims.toml").read_text())
    assert "verdict_chain" in data
    assert "grounding_attestations" in data
    assert "schema_census" in data
    assert "completeness" in data
    # A top-level key rather than a table, so it is the one addition here that
    # is not a section at all. The restore below is what says an older reader
    # walks past it: those readers reach named sections and nothing else.
    assert "backup_format" in data


def test_0312_is_refused_a_graph_this_code_wrote(tmp_path: Path) -> None:
    """The schema version moved, so an older reader stops here. By design.

    This is the half of the compatibility rule that a version bump spends, and
    spending it once is the reason the bump is one release rather than several.

    What the refusal says is not this code's to choose, and that is the point
    worth recording. The sentence comes out of a reader that shipped long ago:
    it calls itself a dev branch, and it tells the operator to delete the file
    holding the chain and every signature, on a graph a newer reader opens
    without complaint. Nothing written here can change it. The only thing that
    can is being on a reader whose refusal was already fixed, which is the
    release before this one, and that is an argument about upgrade order rather
    than about code.
    """
    project = tmp_path / "project"
    project.mkdir()
    _graph_with_a_chain(project)
    out = _run_under_v0312(
        _extract_v0312(tmp_path / "work"),
        """
        try:
            with mareforma.open(root, key_path=root / "root.key") as g:
                print("opened", len(g.query(include_invalidated=True)))
        except Exception as exc:
            print("refused", type(exc).__name__)
            print("said", str(exc))
        """,
        str(project),
    )
    assert "refused DatabaseError" in out
    assert "user_version" in out
    # Pinned rather than lamented. A test that only asserted the refusal would
    # let this read as a clean stop, and it is not one.
    assert "Delete .mareforma/graph.db" in out


def test_0312_restores_a_backup_this_code_wrote(tmp_path: Path) -> None:
    """Restore reads named sections and never rejects an unknown one.

    So ``[verdict_chain]`` and ``[completeness]`` are inert to 0.3.12: it
    rebuilds the graph without them and reports clean. That is exactly the
    property which makes the manifest data rather than a guarantee, and the
    reason the refusal that binds it is a later release rather than this one.
    """
    project = tmp_path / "project"
    project.mkdir()
    _graph_with_a_chain(project)
    import shutil
    shutil.rmtree(project / ".mareforma")

    out = _run_under_v0312(
        _extract_v0312(tmp_path / "work"),
        """
        from mareforma.db.restore import restore
        report = restore(root)
        print("restored", report["claims_restored"])
        """,
        str(project),
    )
    assert "restored 4" in out
