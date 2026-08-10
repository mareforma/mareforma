"""A schema-driven tamper sweep over every input a trust gate reads.

The trust layer decides a proposition's status by reading database rows, and most
of the columns it reads are denormalised and unsigned: a process with SQL access
can rewrite them. The guarantee this module proves is the design invariant, that
every such mutation either leaves the derived status unchanged or drops the line
AND discloses the drop, and that no mutation ever silently changes a count. A
dropped refutation reads as consensus, so a silent drop is the one outcome the
whole trust model cannot tolerate.

Two layers, deliberately:

* :class:`TestSchemaColumnSweep` is the loop the design asks for: it walks every
  column of every gate-input table straight off ``PRAGMA table_info`` and mutates
  each one, so a column nobody wrote a case for, including one added in a later
  release, is covered by construction. It uses a value the column's own
  constraints accept where one exists (a different enum member), and a
  constraint-violating value otherwise, so a column protected only by a foreign
  key or CHECK is proven protected by it. Any mutation that lands must read as
  unchanged or as a disclosed drop.

* The targeted classes carry the forgeries a dumb perturbation cannot express:
  re-pointing a column at another *valid* row (a second plan, a foreign
  proposition), which lands and so exercises the read-path re-derivation rather
  than a foreign-key refusal. These are named in ``_READ_PATH_PROVEN_SEPARATELY``
  so the sweep's reliance on a constraint refusal for those columns is not a
  silent gap.

The append-only triggers on ``findings`` and ``evidence_lines`` refuse a naive
UPDATE or DELETE outright. A writer with database access can drop a durable
trigger, though, so every tamper here drops the write guards first and asserts
the read path itself catches the forgery: the trigger raises the cost of writing
the bad row, the re-derivation is the guarantee.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest import mock

import pytest

import mareforma
from mareforma.trust import (
    Direction,
    DirectionOfInterest,
    EffectEstimate,
    EffectType,
    PlanNotRetirableError,
    Prediction,
    Proposition,
    _store,
)
from mareforma.trust import TestType as _TestType
from mareforma.trust._gate import (
    GateCache,
    verified_gate_inputs,
    verify_gate_inputs_or_refuse,
)

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

# The six tables a trust gate reads, per the design's section 1 inventory. The
# first five feed the independence-count gate through ``INDEPENDENCE_COUNTS_SQL``
# and are swept against ``proposition_status``; ``replication_verdicts`` feeds
# the separate corroboration gate and is swept against ``support_level`` in its
# own class.
_COUNT_GATE_TABLES = (
    "findings",
    "evidence_lines",
    "propositions",
    "predictions",
    "plan_retirements",
    "contrasts",
    "effect_estimates",
)

# Every append-only / no-delete guard on a gate-input table. A SQL-access
# adversary drops these before mutating, so the read path, not the trigger, is
# what each tamper here must be caught by.
_WRITE_GUARD_TRIGGERS = (
    "findings_append_only",
    "findings_no_delete",
    "evidence_lines_append_only",
    "evidence_lines_no_delete",
    "predictions_append_only",
    "predictions_no_delete",
    "plan_retirements_append_only",
    "plan_retirements_no_delete",
    "replication_verdicts_append_only",
    "replication_verdicts_no_delete",
    "propositions_append_only",
    "propositions_no_delete",
    "contrasts_append_only",
    "contrasts_no_delete",
    "effect_estimates_append_only",
    "effect_estimates_no_delete",
    "validators_append_only",
    "validators_no_delete",
)

# CHECK-constrained columns and their full domain, so the sweep can pick a
# different *valid* member (a mutation that lands) rather than a value the CHECK
# would reject. A column whose domain has a single member has no valid
# alternative; the sweep falls back to an invalid value there and asserts the
# CHECK refuses it.
_ENUM_DOMAINS = {
    ("propositions", "direction"): (
        "INCREASES", "DECREASES", "NO_EFFECT", "PRESENT", "ABSENT",
    ),
    ("findings", "bearing_direction"): ("supports", "refutes", "neutral"),
    ("predictions", "inference_regime"): ("frequentist",),
    ("predictions", "test_type"): ("superiority", "equivalence"),
    ("predictions", "direction_of_interest"): ("increase", "decrease"),
    ("predictions", "preregistered"): (0, 1),
    ("contrasts", "control_type"): (
        "positive", "negative", "vehicle", "sham", "comparative",
    ),
    ("effect_estimates", "effect_type"): (
        "SMD", "Hedges_g", "OR", "logOR", "RR", "HR", "COR", "ZCOR",
        "MD", "ROM", "beta", "log2FC", "GEN",
    ),
    ("effect_estimates", "scale"): ("raw", "log"),
}

# Columns whose *valid-target* forgery a dumb perturbation cannot express (it
# repoints one row's identity at another real row), proven in the targeted
# classes below rather than in the schema loop. Named here so the loop's reliance
# on a foreign-key / content-address refusal for them is explicit, not a silent
# skip: the loop proves a naive rewrite is refused, the named test proves the
# read path catches the valid-target repoint.
_READ_PATH_PROVEN_SEPARATELY = {
    ("findings", "plan_id"): "TestValidRepointAttacks",       # -> a second plan
    ("findings", "content_id"): "TestValidRepointAttacks",    # -> a foreign prop
    ("predictions", "direction_of_interest"): "TestValidRepointAttacks",
    ("plan_retirements", "superseded_by"): "TestPlanLifecycleKeyInteractions",
}

# Columns a gate reads for DISCLOSURE (not for the support/refute count) that the
# read path cannot re-derive, so the only guard is the write trigger. ``preregistered``
# feeds ``proposition_status.post_hoc`` (a reader telling a pre-registered gate from a
# post-hoc one) but is deliberately excluded from ``compute_plan_id``: whether a plan
# was registered before the numbers is a historical fact, not a property of the rule,
# so nothing on read can reconstruct it. Its integrity rests on ``predictions_append_only``
# (which watches ``preregistered``) alone — the same trigger-not-signature limit ``plan_id``
# carries. The sweep therefore keeps the guard for such a column and proves the write is
# refused, rather than dropping the guard and expecting a read-path drop that cannot exist.
_WRITE_TRIGGER_PROTECTED_ONLY = {
    ("predictions", "preregistered"): "predictions_append_only",
}


# ---------------------------------------------------------------------------
# Known-state builders
# ---------------------------------------------------------------------------

def _prop_donor() -> Proposition:
    """A second, distinct proposition, the valid target a repoint steals to."""
    return Proposition(
        subject="TP53", relation="affects", object="apoptosis",
        direction=Direction.INCREASES,
        scope={"population": "TNBC", "condition": "in vitro"},
    )


def _prop_retired() -> Proposition:
    """A third proposition, carrying a retired-plan line so the sweep reaches
    ``plan_retirements`` and the replacement ``predictions`` row."""
    return Proposition(
        subject="EGFR", relation="affects", object="proliferation",
        direction=Direction.DECREASES,
        scope={"population": "NSCLC", "condition": "in vitro"},
    )


def _pred_at(alpha: float) -> Prediction:
    """The canonical superiority rule at a chosen alpha: a distinct, valid plan
    to repoint a line at."""
    return Prediction(
        _TestType.SUPERIORITY,
        direction_of_interest=DirectionOfInterest.DECREASE,
        alpha=alpha,
    )


def _pred_increase(alpha: float = 0.05) -> Prediction:
    """The canonical rule with its direction of interest flipped: a valid,
    registrable plan whose rule differs from the retired rule beyond alpha."""
    return Prediction(
        _TestType.SUPERIORITY,
        direction_of_interest=DirectionOfInterest.INCREASE,
        alpha=alpha,
    )


def _legacy_pred(alpha: float = 0.7) -> Prediction:
    """A superiority rule at an alpha no gate can run, as a pre-bound release
    wrote it: the constructor refuses the value today, so it is set past it."""
    pred = _pred()
    object.__setattr__(pred, "alpha", alpha)
    return pred


def _known_state(tmp_path: Path) -> dict:
    """Build one graph in a known, fully-counted state and return its handles.

    ``h_main`` carries a SUPPORTING line (SMD -0.8, signed by an enrolled key,
    claude) and a REFUTING line (SMD +0.8, a second enrolled key, gpt): distinct
    signers and models, so both count and the proposition reads CONTESTED 1/1. A
    silent flip of either line is therefore observable as a count that moves with
    nothing disclosed. ``h_ret`` carries a line under a legacy-alpha plan that is
    retired and recovered under a replacement, so ``plan_retirements`` and the
    replacement ``predictions`` row exist for the sweep to mutate. ``h_donor`` is
    an unrelated proposition whose finding is the valid target a content_id
    repoint steals to.
    """
    ka = _bootstrap_key(tmp_path, "ka.key")
    kb = _bootstrap_key(tmp_path, "kb.key")
    _enroll_key(tmp_path, ka, kb)
    main, donor, retired = _prop(), _prop_donor(), _prop_retired()
    pred = _pred()
    refute_est = EffectEstimate(0.8, EffectType.SMD, p_value=0.001)

    with mareforma.open(tmp_path, key_path=ka) as g:
        sup = g.assert_finding(
            main, pred, _est(), data_id="ds_sup", generated_by="run_sup",
            grounding=_verdict(_CLAUDE),
        )
        donor_finding = g.assert_finding(
            donor, pred, _est(), data_id="ds_donor", generated_by="run_donor",
            grounding=_verdict(_CLAUDE),
        )
    with mareforma.open(tmp_path, key_path=kb) as g:
        ref = g.assert_finding(
            main, pred, refute_est, data_id="ds_ref", generated_by="run_ref",
            grounding=_verdict(_GPT),
        )
    # The retired-plan line: register a legacy-alpha plan (the write-boundary
    # guard is suspended only for this seed, exactly as a pre-guard release would
    # have written it), submit a line under it, then retire it so the line counts
    # again under the replacement.
    legacy = _legacy_pred()
    with mareforma.open(tmp_path, key_path=ka) as g:
        with mock.patch(
            "mareforma.trust.prediction.validate_alpha", lambda a: None
        ):
            legacy_plan = g.register_plan(retired, legacy, generated_by="legacy")
            g.submit_finding(
                retired, legacy, _est(), data_id="ds_ret",
                generated_by="legacy",
            )
        receipt = g.retire_plan(
            legacy_plan, alpha=0.05, reason="registered at an un-gateable alpha",
        )

    return {
        "root": ka,
        "second": kb,
        "main_cid": main.content_id(),
        "donor_cid": donor.content_id(),
        "retired_cid": retired.content_id(),
        "main": main,
        "retired": retired,
        "sup_claim": sup["claim_id"],
        "ref_claim": ref["claim_id"],
        "ref_plan": ref["plan_id"],
        "donor_claim": donor_finding["claim_id"],
        "legacy_plan": legacy_plan,
        "replacement_plan": receipt["superseded_by"],
    }


# ---------------------------------------------------------------------------
# Observation helpers
# ---------------------------------------------------------------------------

def _status_tuple(view: "dict | None") -> tuple:
    """(status, support, refute, lines_skipped) from a proposition view."""
    if view is None:
        return ("<absent>", 0, 0, 0)
    return (
        view["status"],
        view["independent_support"],
        view["independent_refute"],
        view["lines_skipped"],
    )


def _health_count(tmp_path: Path) -> int:
    log = tmp_path / ".mareforma" / "health.jsonl"
    if not log.exists():
        return 0
    return sum(1 for line in log.read_text().splitlines() if line.strip())


def _health_ops(tmp_path: Path) -> list[dict]:
    log = tmp_path / ".mareforma" / "health.jsonl"
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text().splitlines() if line.strip()]


def _drop_write_guards(conn: sqlite3.Connection) -> None:
    """Drop every append-only / no-delete guard, the SQL adversary's first move."""
    for name in _WRITE_GUARD_TRIGGERS:
        conn.execute(f"DROP TRIGGER IF EXISTS {name}")


def _observe_mutation(
    tmp_path: Path, root_key: Path, cid: str, mutate, *, drop_guards: bool = True,
) -> tuple[tuple, int]:
    """Apply *mutate(conn)* on a fresh handle inside a savepoint, read the
    proposition, roll back, and return (status_tuple, disclosures_this_read).

    A fresh handle per call means a fresh disclosure de-dup set, so a drop is
    re-emitted to the health channel every time and the per-read delta is a
    faithful "did this read disclose" signal. The savepoint is rolled back, so
    the mutation, and the dropped guards, never persist past the observation.

    ``drop_guards`` models the SQL adversary's first move (drop the append-only
    triggers so the read-path re-derivation, not the trigger, is what must catch
    the forgery). A column with no read-path re-derivation keeps its guard
    (``drop_guards=False``): the trigger is its only defence, and the mutation
    must be refused rather than caught on read.
    """
    with mareforma.open(tmp_path, key_path=root_key) as g:
        conn = g._conn
        conn.execute("SAVEPOINT tamper")
        try:
            if drop_guards:
                _drop_write_guards(conn)
            mutate(conn)
            before = _health_count(tmp_path)
            view = g.proposition_status(cid)
            after = _health_count(tmp_path)
        finally:
            conn.execute("ROLLBACK TO tamper")
            conn.execute("RELEASE tamper")
    return _status_tuple(view), after - before


def _alternative_value(table: str, column, current):
    """A value distinct from *current* for a mutation of *table.column*.

    Prefers a different CHECK-domain member (a value the column accepts, so the
    write lands and the read-path re-derivation is what must catch it). For an
    unconstrained column it perturbs by type. For a foreign-key or primary-key
    text column the perturbed string is not a real target, so the mutation is
    refused by the constraint, which is the guard the sweep then records.
    """
    name = column["name"]
    domain = _ENUM_DOMAINS.get((table, name))
    if domain is not None:
        for member in domain:
            if member != current:
                return member
        return "not-a-valid-enum-member"
    col_type = (column["type"] or "").upper()
    if "INT" in col_type:
        return current + 1 if isinstance(current, int) else 1
    if "REAL" in col_type:
        return current + 0.5 if isinstance(current, (int, float)) else 0.5
    if current is None:
        return "tampered"
    return f"{current}~tampered"


def _row_cid(conn: sqlite3.Connection, table: str, pk_col: str, pk_val) -> str:
    """The content_id of the proposition the given row belongs to."""
    if table in ("findings", "predictions"):
        return conn.execute(
            f"SELECT content_id FROM {table} WHERE {pk_col} = ?", (pk_val,)
        ).fetchone()["content_id"]
    if table == "propositions":
        return pk_val
    if table == "evidence_lines":
        return conn.execute(
            "SELECT f.content_id FROM findings f "
            "JOIN evidence_lines el ON el.finding_id = f.finding_id "
            "WHERE el.line_id = ?", (pk_val,),
        ).fetchone()["content_id"]
    if table == "plan_retirements":
        return conn.execute(
            "SELECT p.content_id FROM plan_retirements r "
            "JOIN predictions p ON p.plan_id = r.plan_id WHERE r.plan_id = ?",
            (pk_val,),
        ).fetchone()["content_id"]
    if table == "contrasts":
        return conn.execute(
            "SELECT f.content_id FROM findings f "
            "JOIN evidence_lines el ON el.finding_id = f.finding_id "
            "JOIN contrasts c ON c.line_id = el.line_id "
            "WHERE c.contrast_id = ?", (pk_val,),
        ).fetchone()["content_id"]
    if table == "effect_estimates":
        return conn.execute(
            "SELECT f.content_id FROM findings f "
            "JOIN evidence_lines el ON el.finding_id = f.finding_id "
            "JOIN contrasts c ON c.line_id = el.line_id "
            "JOIN effect_estimates est ON est.contrast_id = c.contrast_id "
            "WHERE est.estimate_id = ?", (pk_val,),
        ).fetchone()["content_id"]
    raise AssertionError(f"no content_id mapping for {table}")


_PRIMARY_KEY = {
    "findings": "finding_id",
    "evidence_lines": "line_id",
    "propositions": "content_id",
    "predictions": "plan_id",
    "plan_retirements": "plan_id",
    "contrasts": "contrast_id",
    "effect_estimates": "estimate_id",
}


class TestSchemaColumnSweep:
    """Mutate every column of every count-gate table and assert no silent change.

    Driven off ``PRAGMA table_info`` rather than a hand list, so a column added
    to any of these tables in a later release is swept the day it lands. For each
    row, each column is set to a distinct value (a different valid enum member
    where the CHECK allows one, an invalid value otherwise) with the write guards
    dropped first. The mutation must leave the derived status untouched, or drop
    the line and disclose it, or be refused by a constraint. A count that moves
    with nothing on the health channel is the failure this sweep exists to catch.
    """

    @pytest.mark.parametrize("table", _COUNT_GATE_TABLES)
    def test_no_column_mutation_silently_changes_a_count(
        self, tmp_path: Path, table: str,
    ) -> None:
        state = _known_state(tmp_path)
        root = state["root"]
        pk_col = _PRIMARY_KEY[table]

        with mareforma.open(tmp_path, key_path=root) as g:
            conn = g._conn
            columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
            rows = conn.execute(f"SELECT {pk_col} FROM {table}").fetchall()
            pk_values = [r[pk_col] for r in rows]
            cids = {pk: _row_cid(conn, table, pk_col, pk) for pk in pk_values}

        assert pk_values, f"{table} has no rows to sweep; the known state is thin"
        swept = 0
        for pk_val in pk_values:
            cid = cids[pk_val]
            baseline, _ = _observe_mutation(
                tmp_path, root, cid, lambda conn: None
            )
            for column in columns:
                name = column["name"]

                def mutate(conn, _name=name, _pk=pk_val, _col=column):
                    current = conn.execute(
                        f"SELECT {_name} FROM {table} WHERE {pk_col} = ?",
                        (_pk,),
                    ).fetchone()[_name]
                    conn.execute(
                        f"UPDATE {table} SET {_name} = ? WHERE {pk_col} = ?",
                        (_alternative_value(table, _col, current), _pk),
                    )

                swept += 1
                if (table, name) in _WRITE_TRIGGER_PROTECTED_ONLY:
                    # A disclosure column the read path cannot re-derive: its guard
                    # is the write trigger, so keep the trigger in place and assert
                    # the mutation is refused, rather than dropping the guard and
                    # expecting a read-path drop that cannot exist for it.
                    with pytest.raises(sqlite3.Error):
                        _observe_mutation(
                            tmp_path, root, cid, mutate, drop_guards=False,
                        )
                    continue
                try:
                    after, disclosures = _observe_mutation(
                        tmp_path, root, cid, mutate
                    )
                except sqlite3.Error:
                    # A CHECK / foreign-key / uniqueness constraint refused the
                    # write. The row never changed, so the count cannot have: the
                    # constraint is the guard, and that is a pass.
                    continue

                counted_before = baseline[1] + baseline[2]
                counted_after = after[1] + after[2]
                if after[:3] == baseline[:3]:
                    # Unchanged: a column the gate does not read, or a value that
                    # did not move the derivation.
                    continue
                # The status moved. The only acceptable reason is a disclosed
                # drop: strictly fewer counted lines, the skip tally up, and a
                # health event emitted on this very read.
                assert counted_after < counted_before, (
                    f"{table}.{name}: mutation inflated or reflipped a count "
                    f"silently ({baseline} -> {after})"
                )
                assert after[3] > baseline[3], (
                    f"{table}.{name}: a counted line vanished without "
                    f"lines_skipped rising ({baseline} -> {after})"
                )
                assert disclosures > 0, (
                    f"{table}.{name}: a line dropped with nothing disclosed on "
                    f"the health channel ({baseline} -> {after})"
                )
        assert swept >= len(columns), "the sweep mutated no columns"

    def test_the_read_path_allowlist_names_real_columns_and_tests(
        self, tmp_path: Path,
    ) -> None:
        """Every column the sweep leans on a constraint refusal for (rather than a
        read-path drop) is named in ``_READ_PATH_PROVEN_SEPARATELY`` with the test
        class that proves its valid-target repoint IS caught. This keeps the
        allowlist honest: a named column must be a real column, and the class it
        cites must exist, so the allowlist cannot silently paper over a gap."""
        module = globals()
        with mareforma.open(tmp_path, key_path=_bootstrap_key(tmp_path)) as g:
            for (table, column), test_class in _READ_PATH_PROVEN_SEPARATELY.items():
                cols = {
                    r["name"]
                    for r in g._conn.execute(
                        f"PRAGMA table_info({table})"
                    ).fetchall()
                }
                assert column in cols, f"{table}.{column} is not a real column"
                assert test_class in module, f"{test_class} is not defined"

    def test_the_write_trigger_allowlist_names_real_columns_and_triggers(
        self, tmp_path: Path,
    ) -> None:
        """Every column the sweep protects with a write trigger alone (no read-path
        re-derivation) is named in ``_WRITE_TRIGGER_PROTECTED_ONLY`` with the trigger
        that guards it. A named column must be a real column, and its trigger must be
        installed after an open, so the allowlist cannot quietly excuse a column the
        read path should have caught but does not."""
        with mareforma.open(tmp_path, key_path=_bootstrap_key(tmp_path)) as g:
            installed = {
                r["name"]
                for r in g._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                ).fetchall()
            }
            for (table, column), trigger in _WRITE_TRIGGER_PROTECTED_ONLY.items():
                cols = {
                    r["name"]
                    for r in g._conn.execute(
                        f"PRAGMA table_info({table})"
                    ).fetchall()
                }
                assert column in cols, f"{table}.{column} is not a real column"
                assert trigger in installed, f"{trigger} is not installed"

    def test_post_hoc_disclosure_rests_on_the_write_trigger(
        self, tmp_path: Path,
    ) -> None:
        """``preregistered`` feeds ``post_hoc`` but is not re-derivable on read, so
        its guard is ``predictions_append_only``. With the trigger in place the flip
        that would relabel a post-hoc gate as pre-registered is refused; the schema
        sweep proves the same for every predictions row. This pins the write-side
        guarantee the disclosure integrity of ``post_hoc`` rests on."""
        state = _known_state(tmp_path)
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            cid = state["main_cid"]
            assert g.proposition_status(cid)["post_hoc"] is True
            with pytest.raises(sqlite3.IntegrityError, match="prediction_locked"):
                g._conn.execute(
                    "UPDATE predictions SET preregistered = 1 "
                    "WHERE content_id = ?",
                    (cid,),
                )


class TestTriggersRefuseNaiveWrites:
    """The write-guard half: the append-only triggers refuse an in-place edit.

    Re-derivation is the guarantee, but the trigger is what a writer without SQL
    access hits first. These assert the append-only guards on ``findings`` and
    ``evidence_lines`` (which carried none before this release) refuse both an
    UPDATE and a DELETE, and that every gate-input table carries its guards after
    an open.
    """

    def test_findings_update_and_delete_are_refused(self, tmp_path: Path) -> None:
        state = _known_state(tmp_path)
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            with pytest.raises(sqlite3.IntegrityError, match="finding_locked"):
                g._conn.execute(
                    "UPDATE findings SET plan_id = plan_id WHERE content_id = ?",
                    (state["main_cid"],),
                )
            with pytest.raises(
                sqlite3.IntegrityError, match="finding_delete_blocked"
            ):
                g._conn.execute(
                    "DELETE FROM findings WHERE content_id = ?",
                    (state["main_cid"],),
                )

    def test_evidence_lines_update_and_delete_are_refused(
        self, tmp_path: Path,
    ) -> None:
        state = _known_state(tmp_path)
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            with pytest.raises(
                sqlite3.IntegrityError, match="evidence_line_locked"
            ):
                g._conn.execute(
                    "UPDATE evidence_lines SET data_id = 'x'"
                )
            with pytest.raises(
                sqlite3.IntegrityError, match="evidence_line_delete_blocked"
            ):
                g._conn.execute("DELETE FROM evidence_lines")

    def test_every_gate_input_table_carries_its_write_guards(
        self, tmp_path: Path,
    ) -> None:
        """A guard reconciled onto the schema is what makes the naive edit cost
        a trigger drop. Every append-only gate-input table carries both halves,
        including the four the read path leans on that carried none before this
        release: propositions, contrasts, effect_estimates and validators."""
        state = _known_state(tmp_path)
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            names = {
                r["name"]
                for r in g._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                ).fetchall()
            }
        for guarded in (
            "findings", "evidence_lines", "predictions", "plan_retirements",
            "replication_verdicts", "propositions", "contrasts",
            "effect_estimates", "validators",
        ):
            assert f"{guarded}_append_only" in names, guarded
            assert f"{guarded}_no_delete" in names, guarded

    def test_predictions_plan_id_is_inside_its_own_write_guard(
        self, tmp_path: Path,
    ) -> None:
        """Rewriting ``predictions.plan_id`` re-points a whole rule at another
        identity and drops every line gated under it with nothing disclosed. The
        primary key was missing from the append-only watch list; a naive UPDATE
        of it must now be refused by the trigger."""
        state = _known_state(tmp_path)
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            with pytest.raises(
                sqlite3.IntegrityError, match="prediction_locked"
            ):
                g._conn.execute(
                    "UPDATE predictions SET plan_id = plan_id WHERE content_id = ?",
                    (state["main_cid"],),
                )


class TestNewlyGuardedTablesRefuseAdversaryWrites:
    """propositions, contrasts, effect_estimates and validators carried no write
    guard before this release, so a direct UPDATE flipped an estimate or a
    proposition's text and a DELETE dropped a refutation.

    The adversary these model is a co-resident process opening graph.db with
    plain sqlite3: it registers no custom SQL functions and leaves foreign keys
    at SQLite's default (OFF), so a DELETE of a referenced row is refused by the
    guard trigger, not by a foreign key the attacker never turned on. Each table
    is swept for both an UPDATE of a real column and a DELETE, and the message
    is asserted so the refusal is the append-only guard and not an incidental
    error.
    """

    # (table, a real column, a distinct value) for the UPDATE half.
    _UPDATE_PROBE = (
        ("propositions", "subject", "TAMPERED", "proposition_locked"),
        ("contrasts", "control_type", "positive", "contrast_locked"),
        ("effect_estimates", "estimate_value", 0.8, "effect_estimate_locked"),
        ("validators", "pubkey_pem", "TAMPERED", "validator_locked"),
    )

    def _adversary(self, tmp_path: Path) -> sqlite3.Connection:
        """A raw connection to graph.db with foreign keys OFF and no functions."""
        conn = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        conn.execute("PRAGMA foreign_keys = OFF")
        return conn

    def test_update_of_each_guarded_table_is_refused(
        self, tmp_path: Path,
    ) -> None:
        _known_state(tmp_path)
        adv = self._adversary(tmp_path)
        try:
            for table, column, value, message in self._UPDATE_PROBE:
                assert adv.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], (
                    f"{table} has no rows for the guard to fire on"
                )
                with pytest.raises(sqlite3.IntegrityError, match=message):
                    adv.execute(f"UPDATE {table} SET {column} = ?", (value,))
        finally:
            adv.close()

    def test_delete_of_each_guarded_table_is_refused(
        self, tmp_path: Path,
    ) -> None:
        _known_state(tmp_path)
        adv = self._adversary(tmp_path)
        try:
            for table, *_ in self._UPDATE_PROBE:
                with pytest.raises(
                    sqlite3.IntegrityError, match="delete_blocked"
                ):
                    adv.execute(f"DELETE FROM {table}")
        finally:
            adv.close()

    def test_the_demonstrated_estimate_flip_is_refused_at_the_write(
        self, tmp_path: Path,
    ) -> None:
        """The attack the design opens on: ``UPDATE effect_estimates SET
        estimate_value = 0.8`` flipped a proposition from convergent to refuted
        with nothing disclosed. The read path re-derives the digest and catches
        it, but the guard now refuses the write outright, so the careless edit
        does not land in the first place."""
        state = _known_state(tmp_path)
        adv = self._adversary(tmp_path)
        try:
            with pytest.raises(
                sqlite3.IntegrityError, match="effect_estimate_locked"
            ):
                adv.execute("UPDATE effect_estimates SET estimate_value = 0.8")
        finally:
            adv.close()
        # The value did not change, so the proposition still reads as it did.
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            assert _status_tuple(
                g.proposition_status(state["main_cid"])
            ) == ("CONTESTED", 1, 1, 0)

    def test_honest_writes_survive_the_new_guards(
        self, tmp_path: Path,
    ) -> None:
        """A wrong full-table lock would refuse a legitimate write. The honest
        lifecycle, enrolling a validator, registering a plan, and submitting
        findings that write propositions, contrasts and estimates, must still
        complete and read correctly with every guard installed."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.register_plan(prop, pred)
            g.submit_finding(
                prop, pred, _est(), data_id="ds1", generated_by="run1",
                grounding=_verdict(_CLAUDE),
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.submit_finding(
                prop, pred, EffectEstimate(-0.9, EffectType.SMD, p_value=0.001),
                data_id="ds2", generated_by="run2", grounding=_verdict(_GPT),
            )
        with mareforma.open(tmp_path, key_path=ka) as g:
            view = g.proposition_status(prop.content_id())
            # Two distinct-signer supporting lines wrote through every guarded
            # table and read back with nothing dropped.
            assert view["independent_support"] == 2
            assert view["lines_skipped"] == 0


class TestValidRepointAttacks:
    """The forgeries a dumb perturbation cannot express: repoint a column at
    another real row, which lands, so the read-path re-derivation, not a
    constraint, is what must catch it."""

    def test_findings_plan_id_repoint_to_another_plan_drops_and_discloses(
        self, tmp_path: Path,
    ) -> None:
        """The pre-existing critical: an unsigned ``findings.plan_id`` gates
        every line, and repointing it at another registered plan whose rule reads
        the estimate the other way would flip a REFUTED line to SUPPORTS. The
        finding recorded the plan it was filed under in its own claim, so the
        column must match that record; a repoint drops the line as
        ``plan_rebind_skipped`` rather than reflipping the count."""
        state = _known_state(tmp_path)
        cid = state["main_cid"]
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            # A second, valid, differently-ruled plan on the same proposition to
            # steal the refuting line to.
            other_plan = g.register_plan(
                state["main"], _pred_at(0.01),
            )
            before = g.proposition_status(cid)
            assert _status_tuple(before) == ("CONTESTED", 1, 1, 0)

            _drop_write_guards(g._conn)
            g._conn.execute(
                "UPDATE findings SET plan_id = ? WHERE claim_id = ?",
                (other_plan, state["ref_claim"]),
            )
            g._conn.commit()
            after = g.proposition_status(cid)

        assert not (after["status"] == "CONVERGENT" and after["lines_skipped"] == 0)
        assert after["independent_refute"] == 0
        assert after["lines_skipped"] == 1
        ops = [e["op"] for e in _health_ops(tmp_path)]
        assert "plan_rebind_skipped" in ops

    def test_predictions_rule_flip_drops_and_discloses(
        self, tmp_path: Path,
    ) -> None:
        """A plan_id is content-addressed over its rule, so rewriting a rule-
        bearing ``predictions`` column (here the direction of interest) re-points
        the row at a rule whose hash is a different plan_id. The read query gates
        on the columns, not that binding, so an unguarded read would reflip the
        supporting line to refuting. The rule must hash to the plan_id keying it;
        a mismatch drops the line as ``plan_rule_rebind_skipped``."""
        state = _known_state(tmp_path)
        cid = state["main_cid"]
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            before = g.proposition_status(cid)
            assert _status_tuple(before) == ("CONTESTED", 1, 1, 0)

            _drop_write_guards(g._conn)
            g._conn.execute(
                "UPDATE predictions SET direction_of_interest = 'increase' "
                "WHERE plan_id = ?",
                (state["ref_plan"],),
            )
            g._conn.commit()
            after = g.proposition_status(cid)

        # Both lines shared the plan, so both drop when its rule is rewritten:
        # the point is neither reflips silently.
        assert after["independent_support"] == 0
        assert after["independent_refute"] == 0
        assert after["lines_skipped"] >= 1
        ops = [e["op"] for e in _health_ops(tmp_path)]
        assert "plan_rule_rebind_skipped" in ops

    def test_content_id_plant_into_a_proposition_is_caught(
        self, tmp_path: Path,
    ) -> None:
        """Repointing a foreign finding's ``content_id`` at this proposition
        would plant a line the proposition never earned. The planted finding's
        signed claim text renders the donor proposition, not this one, so the
        proposition-binding re-derivation drops it as ``proposition_rebind_skipped``
        before it can count; the plan_id, hashed over content_id too, is a second
        backstop (``plan_rule_rebind_skipped``) for a plant whose text coincides.
        Either way the count does not inflate."""
        state = _known_state(tmp_path)
        cid = state["main_cid"]
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            before = g.proposition_status(cid)
            _drop_write_guards(g._conn)
            g._conn.execute(
                "UPDATE findings SET content_id = ? WHERE claim_id = ?",
                (cid, state["donor_claim"]),
            )
            g._conn.commit()
            after = g.proposition_status(cid)

        assert after["independent_support"] == before["independent_support"]
        assert after["independent_refute"] == before["independent_refute"]
        assert after["lines_skipped"] > before["lines_skipped"]
        ops = [e["op"] for e in _health_ops(tmp_path)]
        assert (
            "proposition_rebind_skipped" in ops
            or "plan_rule_rebind_skipped" in ops
        )

    def test_content_id_move_away_is_refused_by_the_write_guard(
        self, tmp_path: Path,
    ) -> None:
        """Repointing a finding's ``content_id`` AWAY removes the line from the
        victim proposition's query entirely: a row that is no longer in the
        result set cannot be re-derived on that per-proposition read, so the
        guarantee against a move-away is the append-only write guard (and
        restore's re-derivation of a backup). The naive UPDATE is refused."""
        state = _known_state(tmp_path)
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            with pytest.raises(sqlite3.IntegrityError, match="finding_locked"):
                g._conn.execute(
                    "UPDATE findings SET content_id = ? WHERE claim_id = ?",
                    (state["donor_cid"], state["ref_claim"]),
                )


class TestReadRestoreParity:
    """The one-boundary property: the live read and restore run the same verifier
    and agree on the same graph."""

    def test_read_and_restore_verifier_return_identical_units(
        self, tmp_path: Path,
    ) -> None:
        """``verified_gate_inputs`` (read) and ``verify_gate_inputs_or_refuse``
        (restore) are the two entry points into one verifier. On a clean graph
        they must hand back byte-identical verified lines: if they ever diverged,
        the two would have drifted into two rules, which is the failure the
        boundary exists to prevent."""
        state = _known_state(tmp_path)
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            conn = g._conn
            for cid in (state["main_cid"], state["retired_cid"]):
                read = verified_gate_inputs(conn, cid, cache=GateCache())
                restore = verify_gate_inputs_or_refuse(
                    conn, cid, cache=GateCache()
                )
                assert read.units == restore.units
                assert read.skipped == restore.skipped

    def test_full_round_trip_status_is_identical(self, tmp_path: Path) -> None:
        """The same graph read live, then wiped and rebuilt from its backup,
        must derive the same status for every proposition. Both derivations run
        the shared verifier, so a divergence would be a drift between the two
        paths."""
        state = _known_state(tmp_path)
        cids = (state["main_cid"], state["retired_cid"], state["donor_cid"])
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            before = {cid: _status_tuple(g.proposition_status(cid)) for cid in cids}

        _wipe_db(tmp_path)
        mareforma.restore(tmp_path)

        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            after = {cid: _status_tuple(g.proposition_status(cid)) for cid in cids}
        assert after == before
        assert before[state["main_cid"]] == ("CONTESTED", 1, 1, 0)


class TestPlanLifecycleKeyInteractions:
    """The spec's plan-lifecycle interactions, the half the binding boundary does
    not close."""

    def test_post_hoc_names_a_count_resting_on_a_one_shot_plan(
        self, tmp_path: Path,
    ) -> None:
        """A one-shot ``assert_finding`` synthesises its plan after the estimate
        is in hand, so its count is post-hoc; an up-front ``register_plan`` is
        pre-registered. ``proposition_status.post_hoc`` is what lets a reader tell
        the two apart, byte-identical counts notwithstanding."""
        one_shot, pre_reg = _prop(), _prop_donor()
        ka = _bootstrap_key(tmp_path, "ka.key")
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                one_shot, _pred(), _est(), data_id="d1", generated_by="r1",
                grounding=_verdict(_CLAUDE),
            )
            g.register_plan(pre_reg, _pred())
            g.submit_finding(
                pre_reg, _pred(), _est(), data_id="d2", generated_by="r2",
                grounding=_verdict(_CLAUDE),
            )
            assert g.proposition_status(one_shot.content_id())["post_hoc"] is True
            assert g.proposition_status(pre_reg.content_id())["post_hoc"] is False

    def test_post_hoc_is_true_for_a_line_recovered_by_a_retirement(
        self, tmp_path: Path,
    ) -> None:
        """A retirement's replacement alpha is chosen with the estimates in view,
        so a line gated under it is post-hoc even though it once stood under a
        pre-registration."""
        state = _known_state(tmp_path)
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            view = g.proposition_status(state["retired_cid"])
        assert view["independent_support"] == 1
        assert view["post_hoc"] is True

    def test_retiring_onto_a_pre_existing_replacement_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """If the replacement plan already exists, ``register_plan``'s idempotent
        write would reuse it and the ``preregistered=0`` / supersedes disclosure
        would be dropped silently, recording a post-hoc repair as a
        pre-registration. Retirement refuses with a typed error naming the
        pre-existing plan instead."""
        retired = _prop_retired()
        legacy = _legacy_pred()
        ka = _bootstrap_key(tmp_path, "ka.key")
        with mareforma.open(tmp_path, key_path=ka) as g:
            with mock.patch(
                "mareforma.trust.prediction.validate_alpha", lambda a: None
            ):
                plan_id = g.register_plan(retired, legacy, generated_by="legacy")
                g.submit_finding(
                    retired, legacy, _est(), data_id="ds", generated_by="legacy",
                )
            # Pre-register the exact rule the retirement would mint as its
            # replacement (same rule at alpha 0.05).
            g.register_plan(retired, _pred())
            with pytest.raises(PlanNotRetirableError, match="already-registered"):
                g.retire_plan(plan_id, alpha=0.05, reason="repair")

    def test_a_replacement_rule_rewritten_beyond_alpha_is_caught_on_read(
        self, tmp_path: Path,
    ) -> None:
        """A retirement carries the retired rule over unchanged except alpha, and
        that identity is a property of the read, not only the write: rewriting the
        replacement ``predictions`` row's direction of interest makes the
        replacement no longer reproduce the retired rule at its alpha, so the
        recovered line drops and is disclosed rather than gating on a re-chosen
        rule that reflips its bearing."""
        state = _known_state(tmp_path)
        cid = state["retired_cid"]
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            assert g.proposition_status(cid)["independent_support"] == 1
            _drop_write_guards(g._conn)
            g._conn.execute(
                "UPDATE predictions SET direction_of_interest = 'increase' "
                "WHERE plan_id = ?",
                (state["replacement_plan"],),
            )
            g._conn.commit()
            after = g.proposition_status(cid)
        assert not (after["independent_refute"] == 1 and after["lines_skipped"] == 0)
        assert after["independent_support"] == 0
        assert after["independent_refute"] == 0
        assert after["lines_skipped"] >= 1
        ops = [e["op"] for e in _health_ops(tmp_path)]
        assert "ungateable_plan_skipped" in ops

    def test_a_retirement_repointed_at_a_differently_ruled_plan_drops_the_line(
        self, tmp_path: Path,
    ) -> None:
        """Repointing ``plan_retirements.superseded_by`` at another registered
        plan whose rule differs beyond alpha (here the direction of interest is
        flipped) would gate the recovered line under a rule the retirement never
        sanctioned, reflipping a SUPPORTS to a REFUTES. A retirement carries the
        retired rule over unchanged except alpha, checked on the read: a
        replacement that re-chose the rule drops the line rather than re-gating
        it, so the flip cannot land silently."""
        state = _known_state(tmp_path)
        cid = state["retired_cid"]
        with mareforma.open(tmp_path, key_path=state["root"]) as g:
            before = g.proposition_status(cid)
            assert before["independent_support"] == 1
            # A second registered plan on the retired proposition whose rule
            # differs from the retired rule in more than alpha.
            other = g.register_plan(state["retired"], _pred_increase())
            _drop_write_guards(g._conn)
            g._conn.execute(
                "UPDATE plan_retirements SET superseded_by = ? WHERE plan_id = ?",
                (other, state["legacy_plan"]),
            )
            g._conn.commit()
            after = g.proposition_status(cid)
        assert not (after["independent_refute"] == 1 and after["lines_skipped"] == 0)
        assert after["independent_support"] == 0
        assert after["independent_refute"] == 0
        assert after["lines_skipped"] >= 1
        ops = [e["op"] for e in _health_ops(tmp_path)]
        assert "ungateable_plan_skipped" in ops


class TestReplicationVerdictSweep:
    """The corroboration gate reads ``replication_verdicts`` to re-derive a claim's
    support level. A planted or edited verdict must not lift a lone claim: the
    index verifies every verdict it counts, so an unsigned or mismatched row
    backs no promotion."""

    def test_no_verdict_column_mutation_silently_promotes_a_lone_claim(
        self, tmp_path: Path,
    ) -> None:
        """A single PRELIMINARY claim, plus a genuine signed verdict (issued by an
        external enrolled witness) that names an unrelated cluster, is the
        baseline. Mutating any column of the verdict row (issuer, member, method,
        signature, confidence) must not promote the lone claim above PRELIMINARY:
        the level is re-derived from verified material, not read off the table."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        _enroll_key(tmp_path, ka, kb)
        with mareforma.open(tmp_path, key_path=ka) as g:
            lone = g.assert_claim(
                "a lone analytical finding", classification="ANALYTICAL",
            )
            other = g.assert_claim(
                "an unrelated finding", classification="ANALYTICAL",
            )
        # The witness (kb) issued neither claim, so its verdict is not a
        # self-verdict.
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.record_replication_verdict(
                verdict_id="v1", cluster_id="c1", member_claim_id=other,
                method="semantic-cluster", confidence={"cosine": 0.9},
            )
        with mareforma.open(tmp_path, key_path=ka) as g:
            assert g.get_claim(lone)["support_level"] == "PRELIMINARY"

            conn = g._conn
            columns = conn.execute(
                "PRAGMA table_info(replication_verdicts)"
            ).fetchall()
            swept = 0
            for column in columns:
                name = column["name"]
                conn.execute("SAVEPOINT v")
                _drop_write_guards(conn)
                try:
                    current = conn.execute(
                        f"SELECT {name} FROM replication_verdicts WHERE verdict_id = 'v1'"
                    ).fetchone()[name]
                    # Aim the verdict at the lone claim where the column allows.
                    value = (
                        lone if name in ("member_claim_id", "other_claim_id")
                        else _alternative_value(
                            "replication_verdicts", column, current
                        )
                    )
                    conn.execute(
                        f"UPDATE replication_verdicts SET {name} = ? WHERE verdict_id = 'v1'",
                        (value,),
                    )
                    swept += 1
                    assert g.get_claim(lone)["support_level"] == "PRELIMINARY", (
                        f"replication_verdicts.{name}: a verdict edit promoted a "
                        "lone claim"
                    )
                except sqlite3.Error:
                    pass
                finally:
                    conn.execute("ROLLBACK TO v")
                    conn.execute("RELEASE v")
            assert swept > 0


class TestVerifyCacheHoldsNoNegative:
    """The verify cache carries positive results only, so a resolution that fails
    before an enrolment is not pinned as an answer the enrolment would change."""

    def test_an_enrolment_after_a_soft_read_lifts_the_count(
        self, tmp_path: Path,
    ) -> None:
        """Two distinct-model findings whose second signer is NOT yet enrolled:
        the second line counts on no axis (an unregistered signer is dropped and
        disclosed), so the effective count sits at one and the drop shows in
        lines_skipped. Enrolling that signer and reading again must lift the count
        to two: had the miss been cached as 'not enrolled', the later read would
        still serve one, the exact stale-negative the cache is built to avoid."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        cid = prop.content_id()
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
            eff = _store.effective_independence(g._conn, cid)
        assert eff["number"] == 1
        assert eff["soft"] is False
        assert eff["lines_skipped"] == 1

        # Enroll the second signer; the same graph now counts its line.
        _enroll_key(tmp_path, ka, kb)
        with mareforma.open(tmp_path, key_path=ka) as g:
            eff = _store.effective_independence(g._conn, cid)
        assert eff["number"] == 2
        assert eff["soft"] is False
