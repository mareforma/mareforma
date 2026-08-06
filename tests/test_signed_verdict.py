"""The five demonstrated attacks, carried verbatim, plus the named residual.

The security pass that motivated this work ran five attacks against a live graph
and recorded a before and after for each. This module carries every one of them
as an executable test: each performs the attack exactly as it was demonstrated
and asserts either that nothing changed or that the line dropped with its reason
named on the disclosure channel. The threat model is a process with raw SQL
access to ``graph.db`` (foreign keys off, no custom functions registered) or a
hand-edited ``claims.toml`` backup; the durable write guards are a speed bump a
writer with that access can drop, so every read-path test drops them first and
proves the re-derivation, not the trigger, catches the forgery.

| Attack | Covered | How |
|---|---|---|
| Alter an estimate | Yes | The signed estimates digest no longer matches the live rows |
| Delete an estimate row | Yes | The LEFT-JOINed count keeps the finding visible; the digest catches the gap |
| Edit one number in claims.toml | Yes | Restore re-derives the digest and refuses the recovery |
| Fabricate signers | Yes | A line whose signer does not authenticate counts on no axis |
| Rewrite a proposition's text | Yes | The finding's signed claim text must render the live proposition row |
| Delete the signed finding row | No (residual) | Nothing is left to re-derive against; pinned, not hidden |

The last row is the residual the design names and defers to an external anchor.
It is asserted here as NOT caught, so the boundary is explicit rather than
forgotten.
"""
from __future__ import annotations

import base64
import json
import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import mareforma
import mareforma.db as _db
from mareforma.trust import EffectEstimate, EffectType
from mareforma.trust._gate import (
    GateCache,
    GateInputRefused,
    verified_gate_inputs,
    verify_gate_inputs_or_refuse,
)
from mareforma.trust._store import effective_independence

from tests._helpers import (
    _bootstrap_key,
    _enroll_key,
    _est,
    _pred,
    _prop,
    _verdict,
    _wipe_db,
)

_CLAUDE = "claude-3-5-sonnet-20241022"
_GPT = "gpt-4o-2024-08-06"

# Every append-only / no-delete guard the read-path re-derivation stands behind.
# The SQL adversary drops these before writing, so each attack below drops them
# and proves the re-derivation is what catches the forgery.
_WRITE_GUARDS = (
    "findings_append_only", "findings_no_delete",
    "evidence_lines_append_only", "evidence_lines_no_delete",
    "effect_estimates_append_only", "effect_estimates_no_delete",
    "contrasts_append_only", "contrasts_no_delete",
    "propositions_append_only", "propositions_no_delete",
    "claims_signed_fields_no_laundering",
)


def _adversary(tmp_path: Path) -> sqlite3.Connection:
    """A raw connection to graph.db with foreign keys off, no custom functions.

    The threat model's attacker: a co-resident process opening the file directly.
    Foreign keys default to OFF, so a referenced row can be deleted out from
    under its children and the read path, not a foreign key the attacker never
    turned on, is what must catch it.
    """
    conn = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
    conn.execute("PRAGMA foreign_keys = OFF")
    for name in _WRITE_GUARDS:
        conn.execute(f"DROP TRIGGER IF EXISTS {name}")
    return conn


def _health_ops(tmp_path: Path) -> list[str]:
    log = tmp_path / ".mareforma" / "health.jsonl"
    if not log.exists():
        return []
    return [
        json.loads(line)["op"]
        for line in log.read_text().splitlines()
        if line.strip()
    ]


def _contested_graph(tmp_path: Path) -> tuple[Path, str]:
    """One proposition, one SUPPORTING and one REFUTING line, distinct enrolled
    signers. Reads CONTESTED 1/1 with nothing skipped, so a silent flip or drop
    of either line moves an observable count.
    """
    ka = _bootstrap_key(tmp_path, "ka.key")
    kb = _bootstrap_key(tmp_path, "kb.key")
    _enroll_key(tmp_path, ka, kb)
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=ka) as g:
        g.assert_finding(
            prop, pred, _est(), data_id="ds_sup", generated_by="run_sup",
            grounding=_verdict(_CLAUDE),
        )
    with mareforma.open(tmp_path, key_path=kb) as g:
        g.assert_finding(
            prop, pred, EffectEstimate(0.8, EffectType.SMD, p_value=0.001),
            data_id="ds_ref", generated_by="run_ref", grounding=_verdict(_GPT),
        )
    return ka, prop.content_id()


def _single_finding_graph(tmp_path: Path) -> tuple[Path, str]:
    """One clean, counted single-line supporting finding. Reads support 1."""
    ka = _bootstrap_key(tmp_path, "ka.key")
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=ka) as g:
        g.assert_finding(
            prop, pred, _est(), data_id="ds1", generated_by="run1",
            grounding=_verdict(_CLAUDE),
        )
    return ka, prop.content_id()


# ---------------------------------------------------------------------------
# Attack 1: alter an estimate
# ---------------------------------------------------------------------------


class TestAlterEstimate:
    """``UPDATE effect_estimates SET estimate_value = 0.8`` flipped a proposition
    from convergent to refuted with nothing disclosed. The signed estimates
    digest no longer matches the live rows, so the finding drops and discloses."""

    def test_alter_estimate_is_disclosed_on_read(self, tmp_path: Path) -> None:
        root, cid = _single_finding_graph(tmp_path)
        conn = _adversary(tmp_path)
        conn.execute("UPDATE effect_estimates SET estimate_value = 0.8")
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(cid)
            # The altered line does not count and is not re-flipped into a clean
            # refutation: it drops, and the drop is disclosed.
            assert view["independent_support"] == 0
            assert view["independent_refute"] == 0
            assert view["lines_skipped"] == 1
            with pytest.raises(GateInputRefused):
                verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())
        assert "estimates_digest_skipped" in _health_ops(tmp_path)


# ---------------------------------------------------------------------------
# Attack 2: delete an estimate row
# ---------------------------------------------------------------------------


class TestDeleteEstimate:
    """Deleting one estimate row erased a refutation and the proposition read as
    consensus with the dropped-evidence counter reporting zero, the one outcome
    the trust model cannot tolerate. The count now LEFT JOINs down from the
    signed finding, so the finding stays visible and its digest catches the gap:
    disclosed on read, refused on restore."""

    def test_delete_estimate_is_disclosed_and_refused(
        self, tmp_path: Path,
    ) -> None:
        root, cid = _single_finding_graph(tmp_path)
        conn = _adversary(tmp_path)
        conn.execute("DELETE FROM effect_estimates")
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(cid)
            assert view["independent_support"] == 0
            assert view["lines_skipped"] == 1
            with pytest.raises(GateInputRefused):
                verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())
        ops = _health_ops(tmp_path)
        assert "estimates_digest_skipped" in ops


# ---------------------------------------------------------------------------
# Attack 3: edit one number in claims.toml and restore
# ---------------------------------------------------------------------------


class TestEditClaimsToml:
    """Editing one number in ``claims.toml`` and restoring produced a reversed
    verdict through the full restore verification suite; every signature
    verified. The finding now signs a digest over its estimate set, so an edited
    estimate no longer matches and restore refuses the whole recovery."""

    def test_edited_estimate_in_backup_refuses_restore(
        self, tmp_path: Path,
    ) -> None:
        root, _cid = _single_finding_graph(tmp_path)
        toml_path = tmp_path / "claims.toml"
        toml = toml_path.read_text()
        assert toml.count("-0.8") == 1, "the estimate number is not where expected"
        # Reverse the verdict: flip the supporting -0.8 to a refuting 0.8.
        toml_path.write_text(toml.replace("-0.8", "0.8", 1))
        _wipe_db(tmp_path)
        with pytest.raises(_db.RestoreError) as exc:
            mareforma.restore(tmp_path)
        # The recovery is refused, not silently reversed. The graph.db is not
        # left behind as a half-restored artefact.
        assert exc.value.kind == "claim_unverified"
        assert not (tmp_path / ".mareforma" / "graph.db").exists()

    def test_untampered_backup_still_restores(self, tmp_path: Path) -> None:
        """The refusal is specific to the edit: an honest backup of the same
        graph restores and reads back its original verdict, so the guard does not
        cost the operator a legitimate recovery."""
        root, cid = _single_finding_graph(tmp_path)
        with mareforma.open(tmp_path, key_path=root) as g:
            before = g.proposition_status(cid)["independent_support"]
        _wipe_db(tmp_path)
        mareforma.restore(tmp_path)
        with mareforma.open(tmp_path, key_path=root) as g:
            assert g.proposition_status(cid)["independent_support"] == before


# ---------------------------------------------------------------------------
# Attack 4: fabricate signers
# ---------------------------------------------------------------------------


class TestFabricateSigners:
    """Claims with all-zero signatures under keyids that never existed counted as
    independent supporting sources. A line whose signer does not authenticate
    (an unenrolled keyid, or no bundle at all) now counts on NO axis: the read
    sees the true count of what authenticates, not the inflated one, and the
    fabricated line is disclosed as skipped rather than silently counted."""

    def _two_genuine_supporters(self, tmp_path: Path) -> tuple[Path, str, str]:
        """Two genuinely distinct enrolled signers supporting one proposition:
        the true effective count is 2. Returns (root_key, content_id, the second
        finding's claim_id, so a test can forge that one line)."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_GPT),
            )
            claim_id = g._conn.execute(
                "SELECT c.claim_id FROM findings f "
                "JOIN claims c ON c.claim_id = f.claim_id "
                "JOIN evidence_lines el ON el.finding_id = f.finding_id "
                "WHERE el.data_id = 'ds2'",
            ).fetchone()[0]
        return ka, prop.content_id(), claim_id

    def test_all_zero_signature_under_unenrolled_keyid_does_not_count(
        self, tmp_path: Path,
    ) -> None:
        root, cid, claim_id = self._two_genuine_supporters(tmp_path)
        with mareforma.open(tmp_path, key_path=root) as g:
            # The genuine baseline: two authenticated distinct-model supporters.
            assert effective_independence(g._conn, cid)["number"] == 2

        # Forge one of the two: an all-zero signature under a keyid that never
        # enrolled. The bundle is present, so the read verifies it and finds a
        # signer it cannot authenticate.
        conn = _adversary(tmp_path)
        env = json.loads(
            conn.execute(
                "SELECT signature_bundle FROM claims WHERE claim_id = ?",
                (claim_id,),
            ).fetchone()[0]
        )
        env["signatures"][0]["sig"] = base64.standard_b64encode(b"\x00" * 64).decode()
        env["signatures"][0]["keyid"] = "never-enrolled-fabricated-keyid"
        conn.execute(
            "UPDATE claims SET signature_bundle = ?, asserter_keyid = ? "
            "WHERE claim_id = ?",
            (json.dumps(env), "never-enrolled-fabricated-keyid", claim_id),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=root) as g:
            eff = effective_independence(g._conn, cid)
        # The true count of what authenticates (1), not the inflated 2 that would
        # have counted the fabricated line. The drop is disclosed.
        assert eff["number"] == 1
        assert eff["lines_skipped"] == 1

    def test_no_bundle_claim_cannot_manufacture_support(
        self, tmp_path: Path,
    ) -> None:
        """The no-bundle variant: a claim carrying a fabricated signer keyid but
        no signature bundle at all. It cannot be authenticated, so it counts on no
        axis and cannot manufacture a distinct supporting source."""
        root, cid, claim_id = self._two_genuine_supporters(tmp_path)
        conn = _adversary(tmp_path)
        conn.execute(
            "UPDATE claims SET signature_bundle = NULL, asserter_keyid = ? "
            "WHERE claim_id = ?",
            ("never-enrolled-fabricated-keyid", claim_id),
        )
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=root) as g:
            eff = effective_independence(g._conn, cid)
        assert eff["number"] == 1
        assert eff["lines_skipped"] == 1


# ---------------------------------------------------------------------------
# Attack 5: rewrite a proposition's text
# ---------------------------------------------------------------------------


class TestRewriteProposition:
    """Rewriting a proposition's text left the verdict untouched, so the evidence
    supported a different sentence. The finding's signed claim text is the
    rendering of the proposition it attests, so the live proposition row must
    render back to it: a rewrite drops the finding on the read path, not only on
    restore, and the benign case variant still reads and restores."""

    def test_rewrite_is_caught_on_the_read_path(self, tmp_path: Path) -> None:
        root, cid = _single_finding_graph(tmp_path)
        conn = _adversary(tmp_path)
        # Rewrite the subject: the finding now attests "TP53 ..." though its
        # signed claim text renders "BRCA1 ...".
        conn.execute("UPDATE propositions SET subject = 'TP53'")
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(cid)
            assert view["independent_support"] == 0
            assert view["lines_skipped"] == 1
            # Caught on the read path, not only on restore.
            with pytest.raises(GateInputRefused):
                verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())
        assert "proposition_rebind_skipped" in _health_ops(tmp_path)

    def test_benign_case_variant_still_reads_and_restores(
        self, tmp_path: Path,
    ) -> None:
        """Two agents naming one proposition with different capitalisation
        converge on a single row whose stored strings are the first writer's,
        while the second writer's finding renders the second capitalisation. The
        binding normalises both sides, so this honest, tamper-free case is not
        false-dropped on read and not false-refused on restore."""
        from mareforma.trust import Direction, Proposition

        lower = Proposition(
            subject="brca1", relation="affects", object="tumour growth",
            direction=Direction.DECREASES,
            scope={"population": "TNBC", "condition": "in vitro"},
        )
        upper = Proposition(
            subject="BRCA1", relation="affects", object="tumour growth",
            direction=Direction.DECREASES,
            scope={"population": "TNBC", "condition": "in vitro"},
        )
        assert lower.content_id() == upper.content_id()
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                lower, _pred(), _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                upper, _pred(), _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_GPT),
            )
            # Read: both lines count, neither is false-dropped as a rewrite.
            view = g.proposition_status(upper.content_id())
        assert view["independent_support"] == 2
        assert view["lines_skipped"] == 0
        # Restore: the case variant does not false-refuse the whole recovery.
        _wipe_db(tmp_path)
        mareforma.restore(tmp_path)
        with mareforma.open(tmp_path, key_path=ka) as g:
            after = g.proposition_status(upper.content_id())
        assert after["independent_support"] == 2
        assert after["lines_skipped"] == 0


# ---------------------------------------------------------------------------
# The residual: deleting the signed finding row itself
# ---------------------------------------------------------------------------


class TestResidualDeletedFinding:
    """Deleting the signed ``findings`` row removes the anchor the count
    enumerates from, so nothing is left to re-derive against. This is the residual
    the design accepts and defers to an external anchor (v0.4.0 territory). It is
    asserted here as NOT caught, so the boundary is pinned rather than mistaken
    for a covered case."""

    def test_deleted_finding_row_is_the_named_residual(
        self, tmp_path: Path,
    ) -> None:
        root, cid = _single_finding_graph(tmp_path)
        conn = _adversary(tmp_path)
        conn.execute("DELETE FROM findings")
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(cid)
            # No row carries the signed record, so the read cannot see the
            # deletion and restore does not refuse it. This is the residual.
            assert view["independent_support"] == 0
            assert view["lines_skipped"] == 0
            verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())


# ---------------------------------------------------------------------------
# Read and restore agree on the same graph, for every corruption reason
# ---------------------------------------------------------------------------


class TestReadRestoreAgree:
    """The one-boundary property the design turns on: the live read
    (:func:`verified_gate_inputs`) and restore (:func:`verify_gate_inputs_or_refuse`)
    run the one verifier. For every corruption a direct writer can land, the read
    drops-and-discloses exactly the lines restore refuses over, so a tampered
    graph never reads clean where restore would refuse it, and vice versa."""

    def _tamper_alter_estimate(self, conn: sqlite3.Connection) -> None:
        conn.execute("UPDATE effect_estimates SET estimate_value = 0.8")

    def _tamper_delete_estimate(self, conn: sqlite3.Connection) -> None:
        conn.execute("DELETE FROM effect_estimates")

    def _tamper_rewrite_proposition(self, conn: sqlite3.Connection) -> None:
        conn.execute("UPDATE propositions SET subject = 'TP53'")

    @pytest.mark.parametrize(
        "tamper_name",
        ["_tamper_alter_estimate", "_tamper_delete_estimate",
         "_tamper_rewrite_proposition"],
    )
    def test_read_skips_exactly_what_restore_refuses(
        self, tmp_path: Path, tamper_name: str,
    ) -> None:
        root, cid = _single_finding_graph(tmp_path)
        conn = _adversary(tmp_path)
        getattr(self, tamper_name)(conn)
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=root) as g:
            read = verified_gate_inputs(g._conn, cid, cache=GateCache())
            # The read dropped every tampered line (none survived to be counted)
            # and disclosed the drop.
            assert read.units == ()
            assert read.skipped, "the read did not disclose the tampered line"
            # Restore runs the same verifier and refuses on the same corruption.
            with pytest.raises(GateInputRefused):
                verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())

    def test_clean_graph_read_and_restore_agree(self, tmp_path: Path) -> None:
        """The converse: on an untampered graph both entry points hand back
        identical verified lines and identical (empty) skip sets, so the shared
        verifier has not drifted into two rules."""
        root, cid = _contested_graph(tmp_path)
        with mareforma.open(tmp_path, key_path=root) as g:
            read = verified_gate_inputs(g._conn, cid, cache=GateCache())
            restore = verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())
        assert read.units == restore.units
        assert read.skipped == restore.skipped == ()
        assert len(read.units) == 2


# ---------------------------------------------------------------------------
# Each corruption reason refuses restore with its OWN message branch
# ---------------------------------------------------------------------------


class TestRestoreRefusalMessageBranches:
    """A corruption reason added to ``_CORRUPTION_REASONS`` without a matching
    branch in ``verify_gate_inputs_or_refuse`` falls through to the generic
    bearing-recompute message and reports the wrong error to an operator
    mid-recovery. Each reason this release added carries its own branch; these pin
    that a refusal names the corruption it actually found, not a stand-in."""

    def test_altered_estimate_names_the_digest(self, tmp_path: Path) -> None:
        root, cid = _single_finding_graph(tmp_path)
        conn = _adversary(tmp_path)
        conn.execute("UPDATE effect_estimates SET estimate_value = 0.8")
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=root) as g:
            with pytest.raises(GateInputRefused, match="digest"):
                verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())

    def test_rewritten_proposition_names_the_edge(self, tmp_path: Path) -> None:
        root, cid = _single_finding_graph(tmp_path)
        conn = _adversary(tmp_path)
        conn.execute("UPDATE propositions SET subject = 'TP53'")
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=root) as g:
            with pytest.raises(
                GateInputRefused, match="does not render the proposition",
            ):
                verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())

    def test_failed_signature_under_enrolled_signer_names_the_signature(
        self, tmp_path: Path,
    ) -> None:
        """An enrolled validator's finding whose bundle payload was edited so its
        signature no longer covers it: restore names the failed signature, not a
        generic bearing error. Distinct from the unenrolled forgery, which is a
        disclosed skip restore accepts (an honest backup can carry an unenrolled
        participant's finding)."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_id="ds2", generated_by="run2",
                grounding=_verdict(_GPT),
            )
            claim_id = g._conn.execute(
                "SELECT c.claim_id FROM findings f "
                "JOIN claims c ON c.claim_id = f.claim_id "
                "JOIN evidence_lines el ON el.finding_id = f.finding_id "
                "WHERE el.data_id = 'ds2'",
            ).fetchone()[0]
        conn = _adversary(tmp_path)
        env = json.loads(
            conn.execute(
                "SELECT signature_bundle FROM claims WHERE claim_id = ?",
                (claim_id,),
            ).fetchone()[0]
        )
        payload = json.loads(base64.standard_b64decode(env["payload"]))
        payload["predicate"]["observed_grounding"]["model_lineage"][
            "model_id"
        ] = "tampered"
        env["payload"] = base64.standard_b64encode(
            json.dumps(payload).encode()
        ).decode()
        conn.execute(
            "UPDATE claims SET signature_bundle = ? WHERE claim_id = ?",
            (json.dumps(env), claim_id),
        )
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=ka) as g:
            with pytest.raises(
                GateInputRefused, match="signature no longer verifies",
            ):
                verify_gate_inputs_or_refuse(
                    g._conn, prop.content_id(), cache=GateCache(),
                )


# ---------------------------------------------------------------------------
# A v0.3.10 graph, built by the v0.3.10 code, opens and reads under this code
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent
# The v0.3.10 release, before the signed finding record existed. A finding it
# wrote carries no record and must be grandfathered, not read as corruption.
_V0310_COMMIT = "aba7181"


def _v0310_available() -> bool:
    """True when this checkout can build the v0.3.10 tree from git.

    The test constructs the old graph with the OLD code, so it needs the commit
    reachable. It skips (rather than fails) from an unpacked sdist, which ships
    no .git, the same reason the repo-tree tests skip there.
    """
    if not (_REPO_ROOT / ".git").is_dir():
        return False
    try:
        subprocess.run(
            ["git", "-C", str(_REPO_ROOT), "rev-parse", "--verify",
             f"{_V0310_COMMIT}^{{commit}}"],
            capture_output=True, check=True,
        )
        return True
    except (OSError, subprocess.CalledProcessError):
        return False


@pytest.mark.skipif(
    not _v0310_available(),
    reason="builds the v0.3.10 tree from git; needs the commit reachable",
)
class TestCrossVersionByConstruction:
    """A graph created by the actual v0.3.10 code, read under this one.

    The compatibility claim is not that this code writes bytes v0.3.10 writes; it
    is that a graph v0.3.10 already wrote keeps reading. Its findings predate the
    signed finding record, so they carry none: this code must grandfather them,
    reading their verdict and restoring them, not dropping them as an unsigned or
    corrupt edge. Constructed rather than asserted by argument, the design's own
    bar: extract the v0.3.10 package, drive it in a subprocess to lay down a
    graph, then exercise the lifecycle here.
    """

    def _build_v0310_graph(self, root: Path, work: Path) -> str:
        """Extract the v0.3.10 package and drive it to write one finding.

        Returns the finding's content_id. The subprocess puts the extracted tree
        first on ``sys.path`` so its ``mareforma`` shadows the installed one; the
        third-party deps (cryptography and the rest) are shared, so only the
        package source differs.
        """
        pkg_dir = work / "v0310"
        pkg_dir.mkdir()
        archive = subprocess.run(
            ["git", "-C", str(_REPO_ROOT), "archive", _V0310_COMMIT, "mareforma"],
            capture_output=True, check=True,
        ).stdout
        subprocess.run(
            ["tar", "-x", "-C", str(pkg_dir)],
            input=archive, check=True,
        )
        script = textwrap.dedent(
            """
            import sys
            sys.path.insert(0, sys.argv[1])
            from pathlib import Path
            import mareforma
            assert mareforma.__version__.startswith("0.3.10"), mareforma.__version__
            from mareforma import signing
            from mareforma.trust import (
                Direction, Proposition, Prediction, TestType,
                DirectionOfInterest, EffectEstimate, EffectType,
            )
            root = Path(sys.argv[2])
            key = root / "m.key"
            signing.bootstrap_key(key)
            prop = Proposition(
                subject="BRCA1", relation="affects", object="tumour growth",
                direction=Direction.DECREASES,
                scope={"population": "TNBC", "condition": "in vitro"},
            )
            pred = Prediction(
                TestType.SUPERIORITY,
                direction_of_interest=DirectionOfInterest.DECREASE, alpha=0.05,
            )
            est = EffectEstimate(-0.8, EffectType.SMD, p_value=0.001)
            with mareforma.open(root, key_path=key) as g:
                g.assert_finding(
                    prop, pred, est, data_id="ds1", generated_by="run1",
                )
            print(prop.content_id())
            """
        )
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        proc = subprocess.run(
            [sys.executable, "-c", script, str(pkg_dir), str(root)],
            capture_output=True, text=True, env=env,
        )
        if proc.returncode != 0:
            raise AssertionError(
                f"the v0.3.10 subprocess failed to build a graph:\n{proc.stderr}"
            )
        return proc.stdout.strip().splitlines()[-1]

    def test_v0310_finding_is_grandfathered_and_restores(
        self, tmp_path: Path,
    ) -> None:
        graph_root = tmp_path / "graph"
        graph_root.mkdir()
        cid = self._build_v0310_graph(graph_root, tmp_path)
        key = graph_root / "m.key"

        # This code reads the legacy finding: it predates the signed record, so
        # it is grandfathered, its verdict counts, and nothing is skipped as
        # corruption.
        with mareforma.open(graph_root, key_path=key) as g:
            view = g.proposition_status(cid)
            assert view["independent_support"] == 1
            assert view["lines_skipped"] == 0
            # Restore's stricter verifier accepts the legacy finding too: a
            # graph the old code wrote is not corruption.
            verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())

        # And it survives a backup/restore round trip under this code.
        _wipe_db(graph_root)
        mareforma.restore(graph_root)
        with mareforma.open(graph_root, key_path=key) as g:
            after = g.proposition_status(cid)
        assert after["independent_support"] == 1
        assert after["lines_skipped"] == 0
