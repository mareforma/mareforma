"""Read-path guards: the count sees a deletion, and a single-row read verifies
only the evidence its own row needs.

Two independent regressions land here.

* The count enumerates from the signed ``findings`` and LEFT JOINs downward, so
  a finding whose ``effect_estimates`` / ``contrasts`` / ``evidence_lines`` row
  was deleted stays visible as a NULL-downward row the per-finding digest check
  catches. The old inner-join chain erased the whole finding, and for the modal
  single-line finding the signed digest never ran, so a deleted estimate read as
  if the line never existed. Deleting the ``findings`` row itself is the named
  residual and is asserted as NOT caught, so the boundary is pinned.

* ``get_claim`` re-derives a promoted row's rung through ``_CorroborationIndex``.
  The index used to verify one signature per claim in the whole corroboration
  set to answer a single-row question. Both probes now gather and verify only
  the evidence the served row needs, so the verification a single read does not
  grow with the number of unrelated promoted rows in the graph. The unbacked row
  is exercised too: it has no qualifying peer, so it must examine every candidate
  on its own anchors, and a promoted-row-only baseline would early-exit and hide
  the regression.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import mareforma
from mareforma.db import core as _core
from mareforma.trust._gate import (
    GateCache,
    GateInputRefused,
    verified_gate_inputs,
    verify_gate_inputs_or_refuse,
)

from tests._helpers import _bootstrap_key, _est, _pred, _prop, _two_signers, _verdict

_CLAUDE = "claude-3-5-sonnet-20241022"


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------


def _one_finding_graph(tmp_path: Path) -> tuple[Path, str, str]:
    """A graph with one clean, counted single-line finding.

    Returns (root_key, content_id, finding_id). The finding carries a signed
    estimates digest (assert_finding binds it), so a later deletion is caught as
    a digest mismatch rather than a grandfathered one.
    """
    root = _bootstrap_key(tmp_path, "root.key")
    prop = _prop()
    with mareforma.open(tmp_path, key_path=root) as g:
        g.assert_finding(
            prop, _pred(), _est(), data_id="ds_1", generated_by="run_1",
            grounding=_verdict(_CLAUDE),
        )
        fid = g._conn.execute("SELECT finding_id FROM findings").fetchone()[0]
    return root, prop.content_id(), fid


def _health_ops(tmp_path: Path) -> list[str]:
    log = tmp_path / ".mareforma" / "health.jsonl"
    if not log.exists():
        return []
    import json
    return [
        json.loads(line)["op"]
        for line in log.read_text().splitlines()
        if line.strip()
    ]


def _adversary_conn(tmp_path: Path) -> sqlite3.Connection:
    """A raw connection to graph.db with foreign keys off.

    The design's threat model: an attacker with SQL access opens the file
    without the constraints the library enforces. Foreign keys off lets a parent
    row be deleted out from under its children, the deletion the read path must
    catch on re-derivation.
    """
    conn = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
    conn.execute("PRAGMA foreign_keys = OFF")
    return conn


# ----------------------------------------------------------------------------
# The count anchors on findings and LEFT JOINs downward
# ----------------------------------------------------------------------------


class TestCleanGraphUnchanged:
    """Anchoring on findings and LEFT JOINing downward must not change a read on
    an untampered graph: every finding still has its full line/contrast/estimate
    tree, so the LEFT JOINs yield exactly what the inner joins did."""

    def test_clean_finding_counts_and_skips_zero(self, tmp_path: Path) -> None:
        root, cid, _fid = _one_finding_graph(tmp_path)
        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(cid)
        assert view["independent_support"] == 1
        assert view["independent_refute"] == 0
        assert view["lines_skipped"] == 0

    def test_read_and_restore_agree_on_a_clean_graph(
        self, tmp_path: Path,
    ) -> None:
        root, cid, _fid = _one_finding_graph(tmp_path)
        with mareforma.open(tmp_path, key_path=root) as g:
            read = verified_gate_inputs(g._conn, cid, cache=GateCache())
            restore = verify_gate_inputs_or_refuse(
                g._conn, cid, cache=GateCache(),
            )
        assert read.units == restore.units
        assert read.skipped == restore.skipped
        assert len(read.units) == 1


class TestDeletionIsDisclosedAndRefused:
    """A deleted estimate, contrast, or evidence line on a single-line finding
    used to vanish the whole finding before its signed digest could run. The
    LEFT JOINs keep the finding visible as a NULL-downward row, and the digest
    check drops and discloses it on read and refuses it on restore."""

    def _assert_caught(self, tmp_path: Path, root: Path, cid: str) -> None:
        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(cid)
            # The deletion is disclosed, not silent: the line does not count and
            # is not manufactured into a clean, empty consensus.
            assert view["independent_support"] == 0
            assert view["lines_skipped"] == 1
            # Restore refuses the whole recovery: the reason is a corruption one.
            with pytest.raises(GateInputRefused):
                verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())
        ops = _health_ops(tmp_path)
        assert any(
            op in ("estimates_digest_skipped", "bearing_recompute_skipped")
            for op in ops
        ), ops

    def test_deleted_estimate_is_caught(self, tmp_path: Path) -> None:
        root, cid, _fid = _one_finding_graph(tmp_path)
        conn = _adversary_conn(tmp_path)
        # The no-delete guard is the adversary's first obstacle; the read-path
        # re-derivation is the guarantee, so drop the trigger and prove the
        # deletion is still caught on read.
        conn.execute("DROP TRIGGER IF EXISTS effect_estimates_no_delete")
        conn.execute("DROP TRIGGER IF EXISTS effect_estimates_append_only")
        conn.execute("DELETE FROM effect_estimates")
        conn.commit()
        conn.close()
        self._assert_caught(tmp_path, root, cid)

    def test_deleted_contrast_is_caught(self, tmp_path: Path) -> None:
        root, cid, _fid = _one_finding_graph(tmp_path)
        conn = _adversary_conn(tmp_path)
        conn.execute("DROP TRIGGER IF EXISTS contrasts_no_delete")
        conn.execute("DROP TRIGGER IF EXISTS contrasts_append_only")
        conn.execute("DELETE FROM contrasts")
        conn.commit()
        conn.close()
        self._assert_caught(tmp_path, root, cid)

    def test_deleted_evidence_line_is_caught(self, tmp_path: Path) -> None:
        root, cid, _fid = _one_finding_graph(tmp_path)
        conn = _adversary_conn(tmp_path)
        conn.execute("DROP TRIGGER IF EXISTS evidence_lines_no_delete")
        conn.execute("DROP TRIGGER IF EXISTS evidence_lines_append_only")
        conn.execute("DELETE FROM evidence_lines")
        conn.commit()
        conn.close()
        self._assert_caught(tmp_path, root, cid)

    def test_one_deleted_estimate_drops_the_whole_multiline_finding(
        self, tmp_path: Path,
    ) -> None:
        """Deleting one estimate of a two-line finding must drop BOTH lines, not
        just the NULL-downward one. The digest commits to the whole line set, so
        recomputing it over the live rows (a ``None`` where the estimate was)
        differs and the finding is refused as a unit. If the recompute raised
        instead of folding the NULL, the digest check would be abandoned and the
        surviving line would count, exactly the silent slip-through the LEFT JOIN
        closes."""
        from mareforma.trust import EffectEstimate, EffectType, EvidenceLine

        root = _bootstrap_key(tmp_path, "root.key")
        prop, pred = _prop(), _pred()
        lines = [
            EvidenceLine(estimate=_est(), data_id="ds_a"),
            EvidenceLine(
                estimate=EffectEstimate(-0.7, EffectType.SMD, p_value=0.002),
                data_id="ds_b",
            ),
        ]
        with mareforma.open(tmp_path, key_path=root) as g:
            g.register_plan(prop, pred)
            g.submit_finding(
                prop, pred, lines=lines, generated_by="run",
                grounding=_verdict(_CLAUDE),
            )
            assert g.proposition_status(prop.content_id())[
                "independent_support"
            ] == 1
        conn = _adversary_conn(tmp_path)
        conn.execute("DROP TRIGGER IF EXISTS effect_estimates_no_delete")
        conn.execute("DROP TRIGGER IF EXISTS effect_estimates_append_only")
        conn.execute(
            "DELETE FROM effect_estimates WHERE estimate_id IN "
            "(SELECT estimate_id FROM effect_estimates LIMIT 1)"
        )
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(prop.content_id())
            assert view["independent_support"] == 0
            assert view["lines_skipped"] == 2
            with pytest.raises(GateInputRefused):
                verify_gate_inputs_or_refuse(
                    g._conn, prop.content_id(), cache=GateCache(),
                )

    def test_deleting_the_finding_row_is_the_named_residual(
        self, tmp_path: Path,
    ) -> None:
        """Deleting the ``findings`` row itself removes the anchor the count
        enumerates from: nothing is left to re-derive against, so the line is
        gone with nothing disclosed. This is the residual the design names and
        defers to an external anchor; it is pinned here so the gap is not
        mistaken for a covered case."""
        root, cid, _fid = _one_finding_graph(tmp_path)
        conn = _adversary_conn(tmp_path)
        conn.execute("DROP TRIGGER IF EXISTS findings_no_delete")
        conn.execute("DROP TRIGGER IF EXISTS findings_append_only")
        conn.execute("DELETE FROM findings")
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(cid)
            # The proposition still exists; its finding does not. The read cannot
            # see the deletion (no row to carry the signed digest), and restore
            # does not refuse it.
            assert view["independent_support"] == 0
            assert view["lines_skipped"] == 0
            verify_gate_inputs_or_refuse(g._conn, cid, cache=GateCache())


# ----------------------------------------------------------------------------
# The corroboration probes narrow to the served row
# ----------------------------------------------------------------------------


def _count_participant_verifies(monkeypatch) -> list[int]:
    """Install a counting wrapper around the peer-bundle verifier.

    The corroboration peer probe and the served row's own participant check both
    go through ``_verify_participant_bundle_on_read``; counting its calls
    measures the signature work a single read does. Returns a one-element list
    the caller reads after the read.
    """
    calls = [0]
    real = _core._verify_participant_bundle_on_read

    def counting(conn, row, cache):
        calls[0] += 1
        return real(conn, row, cache)

    monkeypatch.setattr(_core, "_verify_participant_bundle_on_read", counting)
    return calls


