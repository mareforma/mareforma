"""No drop is silent: two paths that dropped a line without disclosing it.

The count's whole guarantee is that a line the verifier cannot count is
returned as a skip, because a dropped refutation reads as consensus. Two
routes bypassed that.

* The per-finding digest check classified a row as "cannot rebuild" by
  catching ANY exception from ``estimates_digest_from_rows([row])``, then
  exempted it from the mismatch skip on the reasoning that the per-line
  bearing check downstream would name the concrete error. That reasoning
  holds only for a row whose ESTIMATE fails to rebuild. The digest also
  canonicalizes ``[data_id, control_type, estimate]``, and canonical JSON
  refuses a value JSON cannot hold. ``evidence_lines.data_id`` is TEXT with
  no CHECK and SQLite stores a BLOB there verbatim, so a BLOB ``data_id``
  raised from the data_id rather than the estimate: the row was exempted
  from the mismatch, fell through to a bearing check that recomputed
  cleanly, and was COUNTED. A refutation whose estimate was rewritten to
  support became a second supporting unit with ``lines_skipped`` at zero.

* The count query LEFT JOINs downward to ``evidence_lines`` / ``contrasts``
  / ``effect_estimates`` precisely so a deleted row surfaces as NULLs the
  digest check catches, but ``predictions`` and ``claims`` were INNER
  JOINed. Those two rows are deletable by exactly the adversary the LEFT
  JOINs were added for, and an inner join erases the whole finding before
  any check runs. Deleting the estimate disclosed a skip; deleting the
  prediction under the same finding disclosed nothing.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import mareforma
from mareforma.trust import EffectEstimate, EffectType
from mareforma.trust._gate import GateCache, verified_gate_inputs

from tests._helpers import _bootstrap_key, _est, _pred, _prop, _verdict

_CLAUDE = "claude-3-5-sonnet-20241022"
_GPT = "gpt-4o-2024-08-06"


def _adversary_conn(tmp_path: Path) -> sqlite3.Connection:
    """A raw connection to graph.db with foreign keys off.

    The threat model: a writer with SQL access opens the file without the
    constraints the library enforces, so a parent row can be deleted out
    from under its children.
    """
    conn = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
    conn.execute("PRAGMA foreign_keys = OFF")
    return conn


def _drop_guards(conn: sqlite3.Connection, *triggers: str) -> None:
    """Drop the write guards. They raise the cost of writing the bad row; the
    read-path re-derivation is the guarantee, so every case here proves the
    read catches the forgery with the triggers already gone."""
    for name in triggers:
        conn.execute(f"DROP TRIGGER IF EXISTS {name}")


def _health_ops(tmp_path: Path) -> list[str]:
    log = tmp_path / ".mareforma" / "health.jsonl"
    if not log.exists():
        return []
    return [
        json.loads(line)["op"]
        for line in log.read_text().splitlines()
        if line.strip()
    ]


def _contested_graph(tmp_path: Path) -> tuple[Path, str, str, str]:
    """One proposition, one supporting line and one refuting line.

    Returns (root_key, content_id, refuting_line_id, refuting_estimate_id).
    Both findings carry a signed estimates digest, so a later edit to either
    is caught as a mismatch rather than grandfathered.
    """
    root = _bootstrap_key(tmp_path, "root.key")
    prop, pred = _prop(), _pred()
    with mareforma.open(tmp_path, key_path=root) as g:
        g.register_plan(prop, pred)
        g.assert_finding(
            prop, pred, _est(), data_id="ds_a", generated_by="run_1",
            grounding=_verdict(_CLAUDE),
        )
        g.assert_finding(
            prop, pred,
            # The proposition runs DECREASES, so a positive estimate refutes.
            EffectEstimate(0.9, EffectType.SMD, p_value=0.001),
            data_id="ds_b", generated_by="run_2",
            grounding=_verdict(_GPT),
        )
        line_id, estimate_id = g._conn.execute(
            "SELECT el.line_id, est.estimate_id FROM evidence_lines el "
            "JOIN contrasts c ON c.line_id = el.line_id "
            "JOIN effect_estimates est ON est.contrast_id = c.contrast_id "
            "WHERE el.data_id = 'ds_b'"
        ).fetchone()
    return root, prop.content_id(), line_id, estimate_id


def _one_finding_graph(tmp_path: Path) -> tuple[Path, str]:
    """A graph with one clean, counted single-line finding."""
    root = _bootstrap_key(tmp_path, "root.key")
    prop = _prop()
    with mareforma.open(tmp_path, key_path=root) as g:
        g.assert_finding(
            prop, _pred(), _est(), data_id="ds_1", generated_by="run_1",
            grounding=_verdict(_CLAUDE),
        )
    return root, prop.content_id()


class TestUndigestableLineIsDisclosed:
    """A row the digest cannot canonicalize must be dropped and disclosed, not
    exempted from the mismatch and then counted."""

    def test_contested_baseline(self, tmp_path: Path) -> None:
        """The untampered graph reads as a genuine contest, nothing skipped."""
        root, cid, _line, _est_id = _contested_graph(tmp_path)
        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(cid)
        assert view["independent_support"] == 1
        assert view["independent_refute"] == 1
        assert view["lines_skipped"] == 0

    def test_blob_data_id_does_not_buy_a_free_edit_of_the_estimate(
        self, tmp_path: Path,
    ) -> None:
        """Rewriting a refuting estimate to support is a digest mismatch. Making
        the same line's ``data_id`` a BLOB must not exempt it from that skip."""
        root, cid, line_id, estimate_id = _contested_graph(tmp_path)
        conn = _adversary_conn(tmp_path)
        _drop_guards(
            conn,
            "effect_estimates_no_delete", "effect_estimates_append_only",
            "evidence_lines_no_delete", "evidence_lines_append_only",
        )
        conn.execute(
            "UPDATE effect_estimates SET estimate_value = -0.9 "
            "WHERE estimate_id = ?", (estimate_id,),
        )
        conn.execute(
            "UPDATE evidence_lines SET data_id = ? WHERE line_id = ?",
            (sqlite3.Binary(b"ds_b"), line_id),
        )
        conn.commit()
        assert conn.execute(
            "SELECT typeof(data_id) FROM evidence_lines WHERE line_id = ?",
            (line_id,),
        ).fetchone()[0] == "blob"
        conn.close()

        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(cid)
            derived = verified_gate_inputs(g._conn, cid, cache=GateCache())
        # The refutation is gone from the count either way; what must never
        # happen is that it goes quietly.
        assert view["lines_skipped"] == 1, view
        assert not any(
            u.data_id == b"ds_b" for u in derived.units
        ), f"the tampered line was counted: {derived.units}"
        assert any(
            s.line_id == line_id for s in derived.skipped
        ), f"the tampered line was not disclosed: {derived.skipped}"
        assert view["independent_refute"] == 0
        assert "estimates_digest_skipped" in _health_ops(tmp_path)

    def test_blob_data_id_alone_is_disclosed(self, tmp_path: Path) -> None:
        """Even with the estimate untouched, a line the digest cannot rebuild
        does not count: the recomputed digest cannot be compared against the
        signed one, so the finding no longer matches what was signed."""
        root, cid, line_id, _estimate_id = _contested_graph(tmp_path)
        conn = _adversary_conn(tmp_path)
        _drop_guards(
            conn, "evidence_lines_no_delete", "evidence_lines_append_only",
        )
        conn.execute(
            "UPDATE evidence_lines SET data_id = ? WHERE line_id = ?",
            (sqlite3.Binary(b"ds_b"), line_id),
        )
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(cid)
            derived = verified_gate_inputs(g._conn, cid, cache=GateCache())
        assert view["lines_skipped"] == 1, view
        assert not any(u.data_id == b"ds_b" for u in derived.units)

    def test_a_corrupt_estimate_still_names_its_own_error(
        self, tmp_path: Path,
    ) -> None:
        """The narrowing must not cost the case it was written for: a row whose
        ESTIMATE no longer rebuilds still falls through to the bearing check, so
        the disclosure names the concrete error rather than the digest."""
        root, cid, line_id, estimate_id = _contested_graph(tmp_path)
        conn = _adversary_conn(tmp_path)
        _drop_guards(
            conn,
            "effect_estimates_no_delete", "effect_estimates_append_only",
        )
        conn.execute(
            "UPDATE effect_estimates SET estimate_value = ? "
            "WHERE estimate_id = ?", (sqlite3.Binary(b"nope"), estimate_id),
        )
        conn.commit()
        conn.close()
        with mareforma.open(tmp_path, key_path=root) as g:
            derived = verified_gate_inputs(g._conn, cid, cache=GateCache())
        skips = {s.line_id: s.op for s in derived.skipped}
        assert skips.get(line_id) == "bearing_recompute_skipped", derived.skipped


class TestDeletedParentRowIsDisclosed:
    """Controlled A/B from one state. Deleting the estimate under a finding
    discloses a skip; deleting the prediction or the claim it hangs on must
    disclose one too, rather than erasing the finding from the query."""

    def _assert_disclosed(self, tmp_path: Path, root: Path, cid: str) -> None:
        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(cid)
        assert view["independent_support"] == 0
        assert view["lines_skipped"] == 1, view
        assert _health_ops(tmp_path)[-1].endswith("_skipped"), _health_ops(
            tmp_path
        )

    def test_deleted_estimate_is_disclosed(self, tmp_path: Path) -> None:
        """The A of the A/B: this one already discloses."""
        root, cid = _one_finding_graph(tmp_path)
        conn = _adversary_conn(tmp_path)
        _drop_guards(
            conn, "effect_estimates_no_delete", "effect_estimates_append_only",
        )
        conn.execute("DELETE FROM effect_estimates")
        conn.commit()
        conn.close()
        self._assert_disclosed(tmp_path, root, cid)

    def test_deleted_prediction_is_disclosed(self, tmp_path: Path) -> None:
        root, cid = _one_finding_graph(tmp_path)
        conn = _adversary_conn(tmp_path)
        _drop_guards(
            conn, "predictions_no_delete", "predictions_append_only",
        )
        conn.execute("DELETE FROM predictions")
        conn.commit()
        conn.close()
        self._assert_disclosed(tmp_path, root, cid)

    def test_deleted_claim_is_disclosed(self, tmp_path: Path) -> None:
        root, cid = _one_finding_graph(tmp_path)
        conn = _adversary_conn(tmp_path)
        _drop_guards(conn, "claims_signed_no_delete")
        conn.execute("DELETE FROM claims")
        conn.commit()
        conn.close()
        self._assert_disclosed(tmp_path, root, cid)

    def test_clean_graph_is_unchanged(self, tmp_path: Path) -> None:
        """Every finding on an untampered graph has its plan and its claim, so
        the outer joins yield exactly the rows the inner joins did."""
        root, cid = _one_finding_graph(tmp_path)
        with mareforma.open(tmp_path, key_path=root) as g:
            view = g.proposition_status(cid)
            derived = verified_gate_inputs(g._conn, cid, cache=GateCache())
        assert view["independent_support"] == 1
        assert view["lines_skipped"] == 0
        assert len(derived.units) == 1
        assert derived.skipped == ()
