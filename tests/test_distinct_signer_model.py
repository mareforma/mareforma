"""New-behaviour tests for the v0.3.7 distinct-signer trust model.

These cover the model change directly (not migrated from older expectations):

  * REPLICATED keys on two distinct, non-NULL ``asserter_keyid`` values sharing
    an ESTABLISHED+open anchor, not on distinct ``generated_by``.
  * artifact_hash is an EQUAL-data COLLAPSE, not a convergence reward.
  * data_id content-addressing collapses byte-identical reruns.
  * the ESTABLISHED boundary refuses a validator that asserted any claim in
    the converging set.
  * trust-layer counting agrees with promotion on the asserter_keyid axis,
    with the legacy NULL-keyid generated_by fallback preserved.
  * verify-on-read excludes forged high-trust rows from ``query`` and flags
    them ``verified=False`` in ``get_claim`` without raising.
  * single_trust_domain disclosure on a solo-operator ESTABLISHED row.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

import mareforma
from mareforma import signing as _signing
from mareforma.db import UnverifiedClaimError, list_claims
from mareforma.trust import _store
from tests._helpers import (
    _bootstrap_key, _enroll_key, _est, _pem_of, _pred, _prop,
    _requires_drop_column, _two_signers, _verdict,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_path(tmp_path: Path) -> Path:
    return tmp_path / ".mareforma" / "graph.db"


def _open_root_graph(tmp_path: Path):
    """Open a graph whose loaded key auto-enrolls as the root validator."""
    key_path = _bootstrap_key(tmp_path, "root.key")
    return mareforma.open(tmp_path, key_path=key_path), key_path


# ===========================================================================
# REPLICATED promotion keys on distinct asserter_keyid
# ===========================================================================

class TestReplicatedKeysOnSigner:
    def test_distinct_signers_shared_anchor_promote(self, tmp_path: Path) -> None:
        sa, sb = _two_signers(tmp_path)
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            b = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"

    def test_same_signer_does_not_promote(self, tmp_path: Path) -> None:
        sa, _ = _two_signers(tmp_path)
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            # Distinct generated_by but the SAME signer -> same asserter_keyid.
            a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            b = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sa)
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"

    def test_unsigned_peer_does_not_promote(self, tmp_path: Path) -> None:
        """A signed claim converging with an unsigned (NULL keyid) peer does
        not promote: one of the two asserters is NULL."""
        sa, _ = _two_signers(tmp_path)
        # Open with NO loaded key so the second claim is unsigned (NULL keyid),
        # but seed needs a key, so build the seed in a signed handle first.
        key_path = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key_path) as g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
        # Reopen WITHOUT a key: the next claim is unsigned (NULL asserter_keyid).
        with mareforma.open(tmp_path) as g:
            b = g.assert_claim("B", supports=[up], generated_by="lab_b")
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"

    def test_two_null_peers_are_not_distinct_signers(self, tmp_path: Path) -> None:
        """Two unsigned (NULL keyid) peers are NOT two distinct signers, the
        legacy guard: NULL != NULL for convergence purposes."""
        # Seed must be ESTABLISHED, which needs a signed seed. Build it signed,
        # then write both converging peers unsigned.
        key_path = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=key_path) as g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
        with mareforma.open(tmp_path) as g:
            a = g.assert_claim("A", supports=[up], generated_by="lab_a")
            b = g.assert_claim("B", supports=[up], generated_by="lab_b")
            assert g.get_claim(a)["asserter_keyid"] is None
            assert g.get_claim(b)["asserter_keyid"] is None
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"

    def test_already_replicated_peers_are_not_rewritten(
        self, tmp_path: Path,
    ) -> None:
        """A converging insert promotes only PRELIMINARY rows.

        Peers already at REPLICATED stay in the candidate set (they still
        corroborate), but the promotion UPDATE must not touch them: rewriting
        their ``updated_at`` would date an old claim to the moment a stranger
        cited the same anchor, and that field is exported as the claim's
        end time.
        """
        sa, sb = _two_signers(tmp_path)
        sc = _signing.load_private_key(_bootstrap_key(tmp_path, "_signer_c.key"))
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            b = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"
            before = {
                cid: g.get_claim(cid)["updated_at"] for cid in (a, b)
            }

            c = g.assert_claim("C", supports=[up], generated_by="lab_c", signer=sc)
            assert g.get_claim(c)["support_level"] == "REPLICATED"
            assert {
                cid: g.get_claim(cid)["updated_at"] for cid in (a, b)
            } == before


# ===========================================================================
# artifact_hash: equal-data collapse (inverted from old convergence reward)
# ===========================================================================

class TestArtifactHashCollapse:
    def test_equal_hash_collapses_no_promote(self, tmp_path: Path) -> None:
        """Two distinct-signer peers that BOTH supply an EQUAL non-NULL
        artifact_hash are the same output and collapse, they do NOT promote."""
        sa, sb = _two_signers(tmp_path)
        h = hashlib.sha256(b"same-artifact").hexdigest()
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim(
                "A", supports=[up], generated_by="lab_a", signer=sa, artifact_hash=h,
            )
            b = g.assert_claim(
                "B", supports=[up], generated_by="lab_b", signer=sb, artifact_hash=h,
            )
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"

    def test_distinct_hash_does_not_block_promotion(self, tmp_path: Path) -> None:
        sa, sb = _two_signers(tmp_path)
        ha = hashlib.sha256(b"artifact-a").hexdigest()
        hb = hashlib.sha256(b"artifact-b").hexdigest()
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim(
                "A", supports=[up], generated_by="lab_a", signer=sa, artifact_hash=ha,
            )
            b = g.assert_claim(
                "B", supports=[up], generated_by="lab_b", signer=sb, artifact_hash=hb,
            )
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"

    def test_absent_hash_does_not_block_promotion(self, tmp_path: Path) -> None:
        sa, sb = _two_signers(tmp_path)
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            b = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            # No artifact_hash on either side: distinct signers still promote.
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"

    def test_double_null_hash_promotes_only_via_distinct_signer(
        self, tmp_path: Path,
    ) -> None:
        """Two absent (NULL) artifact hashes do not promote "on hash alone" , 
        promotion only ever fires via two distinct signers. Same signer +
        absent hashes -> no promote."""
        sa, _ = _two_signers(tmp_path)
        g, _ = _open_root_graph(tmp_path)
        with g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            b = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sa)
            assert g.get_claim(a)["support_level"] == "PRELIMINARY"
            assert g.get_claim(b)["support_level"] == "PRELIMINARY"


# ===========================================================================
# ESTABLISHED boundary: validator cannot equal any converging asserter
# ===========================================================================

class TestEstablishedBoundary:
    def test_validator_equal_to_asserter_refused(self, tmp_path: Path) -> None:
        from mareforma.db import SelfValidationError

        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        # Enroll sa and sb as validators so a self-validation attempt is gated
        # by the converging-set check (not merely the not-enrolled check).
        pem_a = _signing.public_key_to_pem(sa.public_key())
        pem_b = _signing.public_key_to_pem(sb.public_key())
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(pem_a, identity="a")
            g.enroll_validator(pem_b, identity="b")
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            assert g.get_claim(rep)["support_level"] == "REPLICATED"

        # sb asserted a peer in the converging set behind `rep`, it cannot
        # witness its own convergence into ESTABLISHED.
        sb_key = tmp_path / "_signer_b.key"
        with mareforma.open(tmp_path, key_path=sb_key) as g:
            with pytest.raises(SelfValidationError):
                g.validate(rep)

    def test_independent_validator_promotes(self, tmp_path: Path) -> None:
        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        val_key = _bootstrap_key(tmp_path, "val.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(_pem_of(val_key), identity="v")
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
        with mareforma.open(tmp_path, key_path=val_key) as g:
            g.validate(rep)
            assert g.get_claim(rep)["support_level"] == "ESTABLISHED"


# ===========================================================================
# Trust-layer counting agrees with promotion on the asserter_keyid axis
# ===========================================================================

def _prop():
    from mareforma.trust import Direction, Proposition
    return Proposition(
        subject="BRCA1", relation="affects", object="tumour growth",
        direction=Direction.DECREASES,
        scope={"population": "TNBC", "condition": "in vitro"},
    )


def _pred():
    from mareforma.trust import DirectionOfInterest, Prediction, TestType
    return Prediction(
        TestType.SUPERIORITY,
        direction_of_interest=DirectionOfInterest.DECREASE,
        alpha=0.05,
    )


def _est():
    from mareforma.trust import EffectEstimate, EffectType
    return EffectEstimate(-0.8, EffectType.SMD, p_value=0.001)


class TestTrustCounting:
    def test_same_signer_findings_count_as_one(self, tmp_path: Path) -> None:
        """Two findings written through ONE graph handle share one signer and
        count as a single independent support (not CONVERGENT)."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1")
            g.assert_finding(prop, pred, _est(), data_id="ds2", generated_by="run2")
            status = g.proposition_status(prop.content_id())
        # One distinct signer -> at most one independent support.
        assert status["independent_support"] == 1
        assert status["status_policy"] == "status_policy@v4"

    def test_distinct_signer_findings_corroborate(self, tmp_path: Path) -> None:
        """Each finding written through a graph handle opened with a DISTINCT
        key carries a distinct signer -> two independent supports."""
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1")
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(prop, pred, _est(), data_id="ds2", generated_by="run2")
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 2
        assert status["status"] == "CONVERGENT"

    def test_legacy_null_keyid_findings_count_under_generated_by(
        self, tmp_path: Path,
    ) -> None:
        """Unsigned findings (NULL asserter_keyid) fall back to the generated_by
        axis: two with distinct generated_by + distinct data_id still
        corroborate, no silent CONVERGENT downgrade."""
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path) as g:  # no key -> unsigned findings
            g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="run1")
            g.assert_finding(prop, pred, _est(), data_id="ds2", generated_by="run2")
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 2
        assert status["status"] == "CONVERGENT"


# ===========================================================================
# A withdrawn or invalidated claim stops counting as a supporting line
# ===========================================================================

def _two_converging_findings(tmp_path: Path) -> dict:
    """Two cross-model findings from distinct ENROLLED signers: CONVERGENT, 2.

    Returns ``{"content_id", "a", "b", "root", "third"}``, where ``third`` is an
    enrolled key that asserted nothing (so it may issue a verdict on the pair).
    """
    ka = _bootstrap_key(tmp_path, "ka.key")
    kb = _bootstrap_key(tmp_path, "kb.key")
    kc = _bootstrap_key(tmp_path, "kc.key")
    _enroll_key(tmp_path, ka, kb)
    _enroll_key(tmp_path, ka, kc, identity="third@lab.example")
    prop, pred = _prop(), _pred()
    findings = []
    for key, data_id, run, model in (
        (ka, "ds1", "run1", "claude-3-5-sonnet-20241022"),
        (kb, "ds2", "run2", "gpt-4o-2024-08-06"),
    ):
        with mareforma.open(tmp_path, key_path=key) as g:
            findings.append(g.assert_finding(
                prop, pred, _est(), data_id=data_id, generated_by=run,
                grounding=_verdict(model),
            ))
    return {
        "content_id": prop.content_id(),
        "a": findings[0]["claim_id"], "b": findings[1]["claim_id"],
        "root": ka, "third": kc,
    }


def _contested_proposition(tmp_path: Path) -> dict:
    """Two supports and two refutes from four distinct ENROLLED signers.

    Returns ``{"content_id", "refutes", "root"}``, where ``refutes`` is the pair
    of refuting claim_ids. The proposition reads CONTESTED, two each way.
    """
    from mareforma.trust import EffectEstimate, EffectType

    root = _bootstrap_key(tmp_path, "k0.key")
    prop, pred = _prop(), _pred()
    refutes = []
    for i, (value, model) in enumerate((
        (-0.8, "claude-3-5-sonnet-20241022"), (-0.9, "gpt-4o-2024-08-06"),
        (+0.8, "claude-3-5-sonnet-20241022"), (+0.9, "gpt-4o-2024-08-06"),
    )):
        key = root
        if i:
            key = _bootstrap_key(tmp_path, f"k{i}.key")
            _enroll_key(tmp_path, root, key, identity=f"lab{i}@lab.example")
        with mareforma.open(tmp_path, key_path=key) as g:
            finding = g.assert_finding(
                prop, pred, EffectEstimate(value, EffectType.SMD, p_value=0.001),
                data_id=f"ds{i}", generated_by=f"run{i}",
                grounding=_verdict(model),
            )
        if value > 0:
            refutes.append(finding["claim_id"])
    return {"content_id": prop.content_id(), "refutes": refutes, "root": root}


class TestWithdrawnLinesStopCounting:
    def test_retracted_finding_no_longer_corroborates(
        self, tmp_path: Path,
    ) -> None:
        """Retraction is the documented withdrawal path, so a retracted claim
        must stop counting as an independent supporting line: the proposition
        falls back to PRELIMINARY and the effective number to 1."""
        setup = _two_converging_findings(tmp_path)
        with mareforma.open(tmp_path, key_path=setup["root"]) as g:
            assert g.proposition_status(setup["content_id"])["status"] == "CONVERGENT"
            g.update_claim(setup["b"], status="retracted")
            status = g.proposition_status(setup["content_id"])
            assert status["independent_support"] == 1
            assert status["status"] == "PRELIMINARY"
            assert _store.effective_independence(
                g._conn, setup["content_id"]
            )["number"] == 1

    def test_contradicted_finding_no_longer_corroborates(
        self, tmp_path: Path,
    ) -> None:
        """A signed contradiction verdict from a non-participating enrolled
        validator invalidates the older claim (``t_invalid``). That claim must
        stop counting too, including on the surviving sibling's trust map, which
        would otherwise read independence 2 with nothing disclosing why."""
        setup = _two_converging_findings(tmp_path)
        with mareforma.open(tmp_path, key_path=setup["third"]) as g:
            g.record_contradiction_verdict(
                verdict_id="cv_1", member_claim_id=setup["a"],
                other_claim_id=setup["b"], confidence={"stance": "refutes"},
            )
        with mareforma.open(tmp_path, key_path=setup["root"]) as g:
            assert g.get_claim(setup["a"])["t_invalid"] is not None
            status = g.proposition_status(setup["content_id"])
            assert status["independent_support"] == 1
            assert status["status"] == "PRELIMINARY"
            assert g.trust_map(setup["b"]).get("independence").value == "1"

    def test_status_flip_cannot_silently_erase_a_refutation(
        self, tmp_path: Path,
    ) -> None:
        """``status`` is an unsigned column any handle holding the graph may
        rewrite, including one carrying no key at all. Dropping the refuting
        lines it names must therefore be disclosed: a proposition that reads
        CONVERGENT with ``lines_skipped == 0`` after two keyless flips is a
        manufactured consensus with nothing on the read saying so."""
        setup = _contested_proposition(tmp_path)
        with mareforma.open(tmp_path, key_path=setup["root"]) as g:
            before = g.proposition_status(setup["content_id"])
        assert before["status"] == "CONTESTED"
        assert before["independent_refute"] == 2
        assert before["lines_skipped"] == 0
        with mareforma.open(tmp_path) as g:  # no key: cannot sign anything
            for claim_id in setup["refutes"]:
                g.update_claim(claim_id, status="contested")
            after = g.proposition_status(setup["content_id"])
        assert not (after["status"] == "CONVERGENT" and after["lines_skipped"] == 0)
        assert after["lines_skipped"] == 2

    def test_repeated_reads_do_not_re_record_the_same_drop(
        self, tmp_path: Path,
    ) -> None:
        """A withdrawal is a state, not an event per read. ``lines_skipped``
        carries it back on every read, so the health channel records each
        dropped line once: an agent polling ``proposition_status`` while it
        works must not grow ``health.jsonl`` in proportion to reads."""
        setup = _contested_proposition(tmp_path)
        log = tmp_path / ".mareforma" / "health.jsonl"
        with mareforma.open(tmp_path, key_path=setup["root"]) as g:
            for claim_id in setup["refutes"]:
                g.update_claim(claim_id, status="retracted")
            before = len(log.read_text(encoding="utf-8").splitlines())
            for _ in range(10):
                view = g.proposition_status(setup["content_id"])
                assert view["lines_skipped"] == 2
            after = len(log.read_text(encoding="utf-8").splitlines())
        # One disclosure per withdrawn line, not one per line per read.
        assert after - before == 2


# ===========================================================================
# data_id content-addressing
# ===========================================================================

class TestContentAddressing:
    def test_equal_bytes_collapse(self, tmp_path: Path) -> None:
        """Equal dataset bytes content-address to the same data_id -> the
        independence guard collapses them to one unit."""
        prop, pred = _prop(), _pred()
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_bytes=b"identical", generated_by="run1",
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            # Same bytes -> same content-addressed data_id -> idempotent reuse,
            # collapses to ONE unit even across two distinct signers.
            g.assert_finding(
                prop, pred, _est(), data_bytes=b"identical", generated_by="run2",
            )
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 1

    def test_distinct_bytes_count_with_distinct_signers(self, tmp_path: Path) -> None:
        prop, pred = _prop(), _pred()
        ka = _bootstrap_key(tmp_path, "ka.key")
        kb = _bootstrap_key(tmp_path, "kb.key")
        with mareforma.open(tmp_path, key_path=ka) as g:
            g.assert_finding(
                prop, pred, _est(), data_bytes=b"dataset-a", generated_by="run1",
            )
        with mareforma.open(tmp_path, key_path=kb) as g:
            g.assert_finding(
                prop, pred, _est(), data_bytes=b"dataset-b", generated_by="run2",
            )
            status = g.proposition_status(prop.content_id())
        assert status["independent_support"] == 2

    def test_data_id_and_data_bytes_mutually_exclusive(self, tmp_path: Path) -> None:
        prop, pred = _prop(), _pred()
        with mareforma.open(tmp_path, key_path=_bootstrap_key(tmp_path)) as g:
            with pytest.raises(ValueError, match="data_id|data_bytes|both"):
                g.assert_finding(
                    prop, pred, _est(), data_id="x", data_bytes=b"y",
                    generated_by="run1",
                )

    def test_content_address_helpers(self) -> None:
        did = _store.content_address_data_id(b"hello")
        assert did == "sha256:" + hashlib.sha256(b"hello").hexdigest()
        assert _store.is_content_addressed(did)
        assert not _store.is_content_addressed("plain-string")


# ===========================================================================
# verify-on-read
# ===========================================================================

def _build_established(tmp_path: Path, *, rep_text: str = "A"):
    """Build an ESTABLISHED claim; return (root_key, val_key, rep_id, peer_id).

    ``rep_text`` sets the promoted claim's text so a search-side test can find
    it by a distinctive term.
    """
    sa, sb = _two_signers(tmp_path)
    root_key = _bootstrap_key(tmp_path, "root.key")
    val_key = _bootstrap_key(tmp_path, "val.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.enroll_validator(_pem_of(val_key), identity="v")
        up = g.assert_claim("anchor", generated_by="seed", seed=True)
        rep = g.assert_claim(rep_text, supports=[up], generated_by="lab_a", signer=sa)
        peer = g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
    with mareforma.open(tmp_path, key_path=val_key) as g:
        g.validate(rep)
    return root_key, val_key, rep, peer


class TestVerifyOnRead:
    def test_tampered_established_excluded_and_flagged(self, tmp_path: Path) -> None:
        root_key, _, rep, _ = _build_established(tmp_path)
        # Forge: corrupt the validation_signature directly in sqlite.
        conn = sqlite3.connect(_db_path(tmp_path))
        try:
            conn.execute(
                "UPDATE claims SET validation_signature = ? WHERE claim_id = ?",
                ('{"payloadType":"forged","payload":"x","signatures":[]}', rep),
            )
            conn.commit()
        finally:
            conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            # get_claim never raises; flags verified=False.
            c = g.get_claim(rep)
            assert c["verified"] is False
            # query excludes the forged high-trust row.
            ids = {r["claim_id"] for r in g.query(min_support="ESTABLISHED", limit=99)}
            assert rep not in ids

    def test_read_exclusion_is_counted_and_logged(self, tmp_path: Path) -> None:
        """A shorter result set reads the same as an empty graph. The exclusion
        must reach the operator as a signal: a counter on the graph and a
        health event, not only a missing row."""
        root_key, _, rep, _ = _build_established(
            tmp_path, rep_text="quasarflux marker term",
        )
        conn = sqlite3.connect(_db_path(tmp_path))
        try:
            conn.execute(
                "UPDATE claims SET validation_signature = ? WHERE claim_id = ?",
                ('{"payloadType":"forged","payload":"x","signatures":[]}', rep),
            )
            conn.commit()
        finally:
            conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.read_verify_exclusions == 0
            g.query(min_support="ESTABLISHED", limit=99)
            assert g.read_verify_exclusions == 1
            g.search("quasarflux", limit=99)
            assert g.read_verify_exclusions == 2

        events = [
            json.loads(line)
            for line in (_db_path(tmp_path).parent / "health.jsonl")
            .read_text().splitlines() if line.strip()
        ]
        excluded = [e for e in events if e["op"] == "read_verify_excluded"]
        assert [e["n"] for e in excluded] == [1, 1]

    def test_legacy_unsigned_replicated_is_verify_exempt(self, tmp_path: Path) -> None:
        """A REPLICATED row whose asserter is not enrolled (no pubkey to check)
        is verify-exempt: returned as-is, never falsely excluded."""
        sa, sb = _two_signers(tmp_path)  # NOT enrolled as validators
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            c = g.get_claim(rep)
            assert c["support_level"] == "REPLICATED"
            assert c["verified"] is True
            ids = {r["claim_id"] for r in g.query(min_support="REPLICATED", limit=99)}
            assert rep in ids

    def test_tampered_enrolled_asserter_bundle_excluded(self, tmp_path: Path) -> None:
        """A tampered participant bundle on a REPLICATED row whose asserter IS an
        enrolled validator is excluded from query."""
        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        pem_a = _signing.public_key_to_pem(sa.public_key())
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(pem_a, identity="a")  # sa is now enrolled
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            assert g.get_claim(rep)["support_level"] == "REPLICATED"

        # Tamper the asserter bundle's signature bytes.
        conn = sqlite3.connect(_db_path(tmp_path))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT signature_bundle FROM claims WHERE claim_id = ?", (rep,),
            ).fetchone()
            bundle = json.loads(row["signature_bundle"])
            import base64
            sig = bytearray(base64.standard_b64decode(bundle["signatures"][0]["sig"]))
            sig[0] ^= 0xFF
            bundle["signatures"][0]["sig"] = base64.standard_b64encode(
                bytes(sig)
            ).decode("ascii")
            conn.execute(
                "UPDATE claims SET signature_bundle = ? WHERE claim_id = ?",
                (json.dumps(bundle, sort_keys=True, separators=(",", ":")), rep),
            )
            conn.commit()
        finally:
            conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            c = g.get_claim(rep)
            assert c["verified"] is False
            ids = {r["claim_id"] for r in g.query(min_support="REPLICATED", limit=99)}
            assert rep not in ids

    def test_search_excludes_a_high_trust_row_that_query_excludes(
        self, tmp_path: Path,
    ) -> None:
        """Every read surface gates a high-trust row on re-verification, not just
        query(). Build an ESTABLISHED row, break its validation envelope in the
        DB, and assert search() excludes it exactly as query() does."""
        root_key, _, rep, _ = _build_established(
            tmp_path, rep_text="quasarflux marker term",
        )
        conn = sqlite3.connect(_db_path(tmp_path))
        try:
            conn.execute(
                "UPDATE claims SET validation_signature = ? WHERE claim_id = ?",
                ('{"payloadType":"x","payload":"x","signatures":[]}', rep),
            )
            conn.commit()
        finally:
            conn.close()
        with mareforma.open(tmp_path, key_path=root_key) as g:
            q_ids = {r["claim_id"] for r in g.query(min_support="ESTABLISHED", limit=99)}
            s_ids = {r["claim_id"] for r in g.search("quasarflux", limit=99)}
        assert rep not in q_ids
        assert rep not in s_ids

    def test_search_returns_a_genuine_established_row_with_disclosure(
        self, tmp_path: Path,
    ) -> None:
        """The read-path gate must not over-exclude: a genuine ESTABLISHED row is
        still served by search, and carries the same trust-domain disclosure
        query attaches (search promises the same projection as query_claims)."""
        root_key, _, rep, _ = _build_established(
            tmp_path, rep_text="pulsarwidth marker term",
        )
        with mareforma.open(tmp_path, key_path=root_key) as g:
            hits = {r["claim_id"]: r for r in g.search("pulsarwidth", limit=99)}
        assert rep in hits
        assert hits[rep]["single_trust_domain"] is True


# ===========================================================================
# single_trust_domain disclosure
# ===========================================================================

class TestSingleTrustDomain:
    def test_solo_operator_established_row_discloses_single_domain(
        self, tmp_path: Path,
    ) -> None:
        root_key, _, rep, _ = _build_established(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            c = g.get_claim(rep)
        assert c["support_level"] == "ESTABLISHED"
        assert c["single_trust_domain"] is True
        assert c["trust_domain_root"] is not None

    def test_export_bundle_predicate(self, tmp_path: Path) -> None:
        """The export bundle carries the mare:singleTrustDomain predicate on the
        ESTABLISHED row."""
        from mareforma import export_bundle as _eb

        _build_established(tmp_path)
        statement = _eb.build_statement(tmp_path)
        assert statement["predicate"]["mare:singleTrustDomain"] is True
        assert statement["predicate"]["mare:trustDomainRoot"] is not None

    def test_validators_module_predicates(self, tmp_path: Path) -> None:
        from mareforma import validators as _validators

        root_key, _, _, _ = _build_established(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert _validators.single_trust_domain(g._conn) is True
            assert _validators.trust_domain_root(g._conn) is not None
            assert len(_validators.enrollment_roots(g._conn)) == 1


class TestVerifyOnReadCacheBinding:
    """Regression: the verify-on-read cache must key on the per-row identity.

    The ESTABLISHED verify result depends on a payload-binds-this-claim check, so
    the cache key must include the row's claim_id. Without it, an attacker who
    copies a genuine validation_signature onto a second row (which they sort
    first via a chosen created_at) would poison the shared query cache and censor
    the legitimate ESTABLISHED claim.
    """

    def test_copied_validation_signature_does_not_censor_the_real_row(
        self, tmp_path
    ):
        import uuid

        kv = tmp_path / "mareforma.key"
        _signing.bootstrap_key(kv)
        sa, sb = _two_signers(tmp_path)
        with mareforma.open(tmp_path, key_path=kv) as g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim("legit A", generated_by="x", supports=[up], signer=sa)
            g.assert_claim("peer B", generated_by="y", supports=[up], signer=sb)
            g.validate(a)  # A -> ESTABLISHED
            vs_a = g.get_claim(a)["validation_signature"]

        # Forge a second ESTABLISHED row that reuses A's validation envelope and
        # sorts FIRST by carrying a far-future created_at.
        conn = sqlite3.connect(tmp_path / ".mareforma" / "graph.db")
        conn.execute(
            "INSERT INTO claims (claim_id, text, support_level, status, "
            "validation_signature, created_at, updated_at) "
            "VALUES (?, ?, 'ESTABLISHED', 'open', ?, ?, ?)",
            (str(uuid.uuid4()), "forged F", vs_a,
             "2099-01-01T00:00:00+00:00", "2099-01-01T00:00:00+00:00"),
        )
        conn.commit()
        conn.close()

        with mareforma.open(tmp_path, key_path=kv) as g:
            texts = {c["text"] for c in g.query(limit=99)}
            # The forged row is excluded (its envelope does not bind its claim_id)
            assert "forged F" not in texts
            # ...and the legitimate ESTABLISHED claim is NOT censored by the
            # forged row sharing its envelope bytes.
            assert "legit A" in texts
            assert g.get_claim(a)["verified"] is True


class TestParticipantBundleBinding:
    """Verify-on-read for REPLICATED rows binds the bundle to the claim, so a
    genuine bundle cannot be stapled onto a forged row (the P1 review gap)."""

    def test_copied_bundle_onto_other_claim_excluded(self, tmp_path: Path) -> None:
        """A genuine enrolled-key bundle copied onto a different REPLICATED row
        fails the claim_id binding: get_claim flags verified=False and query
        excludes it, while the genuine row is still served."""
        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        pem_a = _signing.public_key_to_pem(sa.public_key())
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(pem_a, identity="a")
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            victim = g.assert_claim("totally fabricated", generated_by="x")
            assert g.get_claim(rep)["support_level"] == "REPLICATED"

        conn = sqlite3.connect(str(_db_path(tmp_path)))
        conn.row_factory = sqlite3.Row
        try:
            # The adversary in scope holds DB write access, so the append-only
            # trigger is theirs to drop. The read path is the defence under test.
            conn.execute("DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering")
            conn.execute("DROP TRIGGER IF EXISTS claims_signed_promotion_backed")
            r = conn.execute(
                "SELECT signature_bundle, asserter_keyid FROM claims "
                "WHERE claim_id = ?", (rep,),
            ).fetchone()
            # Staple rep's genuine (enrolled-key) bundle + keyid onto the
            # fabricated row and flip it to REPLICATED via raw SQL.
            conn.execute(
                "UPDATE claims SET signature_bundle = ?, asserter_keyid = ?, "
                "support_level = 'REPLICATED' WHERE claim_id = ?",
                (r["signature_bundle"], r["asserter_keyid"], victim),
            )
            conn.commit()
        finally:
            conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            c = g.get_claim(victim)
            assert c["support_level"] == "REPLICATED"  # forged level persists
            assert c["verified"] is False               # but flagged unverified
            ids = {row["claim_id"]
                   for row in g.query(min_support="REPLICATED", limit=99)}
            assert victim not in ids                     # excluded from query
            assert rep in ids                            # genuine row still served

    def test_copied_bundle_with_null_keyid_excluded(self, tmp_path: Path) -> None:
        """asserter_keyid is an unsigned denormalisation, so the read path must
        derive the signer from the bundle instead of letting the column decide
        whether the check runs. NULLing it must not buy the legacy exemption."""
        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        pem_a = _signing.public_key_to_pem(sa.public_key())
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(pem_a, identity="a")
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            victim = g.assert_claim("totally fabricated", generated_by="x")

        conn = sqlite3.connect(str(_db_path(tmp_path)))
        conn.row_factory = sqlite3.Row
        try:
            # The adversary in scope holds DB write access, so the append-only
            # trigger is theirs to drop. The read path is the defence under test.
            conn.execute("DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering")
            conn.execute("DROP TRIGGER IF EXISTS claims_signed_promotion_backed")
            bundle = conn.execute(
                "SELECT signature_bundle FROM claims WHERE claim_id = ?", (rep,),
            ).fetchone()["signature_bundle"]
            conn.execute(
                "UPDATE claims SET signature_bundle = ?, asserter_keyid = NULL, "
                "support_level = 'REPLICATED' WHERE claim_id = ?",
                (bundle, victim),
            )
            conn.commit()
        finally:
            conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.get_claim(victim)["verified"] is False
            ids = {row["claim_id"]
                   for row in g.query(min_support="REPLICATED", limit=99)}
            assert victim not in ids
            assert rep in ids

    def test_keyid_disagreeing_with_bundle_excluded(self, tmp_path: Path) -> None:
        """Rewriting asserter_keyid to a non-enrolled string must not skip the
        signature check: the row is refused when it contradicts its bundle."""
        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        pem_a = _signing.public_key_to_pem(sa.public_key())
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(pem_a, identity="a")
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            assert g.get_claim(rep)["support_level"] == "REPLICATED"

        conn = sqlite3.connect(str(_db_path(tmp_path)))
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering")
            env = json.loads(conn.execute(
                "SELECT signature_bundle FROM claims WHERE claim_id = ?", (rep,),
            ).fetchone()["signature_bundle"])
            sig = env["signatures"][0]["sig"]
            env["signatures"][0]["sig"] = ("B" if sig[0] != "B" else "C") + sig[1:]
            conn.execute(
                "UPDATE claims SET signature_bundle = ?, "
                "asserter_keyid = '0123456789abcdef' WHERE claim_id = ?",
                (json.dumps(env), rep),
            )
            conn.commit()
        finally:
            conn.close()

        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.get_claim(rep)["verified"] is False
            ids = {row["claim_id"]
                   for row in g.query(min_support="REPLICATED", limit=99)}
            assert rep not in ids

    def test_junk_bundle_unenrolled_keyid_excluded(self, tmp_path: Path) -> None:
        """A non-enrolled keyid with a non-claim bundle no longer slips through
        the exempt path: the structural binding rejects it."""
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            victim = g.assert_claim("fabricated", generated_by="x")
        conn = sqlite3.connect(str(_db_path(tmp_path)))
        try:
            # The adversary in scope holds DB write access, so the append-only
            # trigger is theirs to drop. The read path is the defence under test.
            conn.execute("DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering")
            conn.execute("DROP TRIGGER IF EXISTS claims_signed_promotion_backed")
            conn.execute(
                "UPDATE claims SET signature_bundle = ?, asserter_keyid = ?, "
                "support_level = 'REPLICATED' WHERE claim_id = ?",
                ('{"not":"a claim envelope"}', "deadbeefdeadbeef", victim),
            )
            conn.commit()
        finally:
            conn.close()
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.get_claim(victim)["verified"] is False
            ids = {row["claim_id"]
                   for row in g.query(min_support="REPLICATED", limit=99)}
            assert victim not in ids


class TestPublishingSurfacesGateHighTrustRows:
    """``list_claims`` and the exports behind it gate a high-trust row too.

    The interop exports are the one read path that leaves the machine, so a
    forged support level does the most damage there. ``list_claims`` flags the
    row and the exports refuse to publish it.
    """

    def _forged_replicated_row(self, tmp_path: Path) -> tuple[Path, str, str]:
        """Build a graph where *victim* wears a bundle copied off *rep*."""
        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        pem_a = _signing.public_key_to_pem(sa.public_key())
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(pem_a, identity="a")
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            victim = g.assert_claim("totally fabricated", generated_by="x")
            assert g.get_claim(rep)["support_level"] == "REPLICATED"

        conn = sqlite3.connect(str(_db_path(tmp_path)))
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering")
            conn.execute("DROP TRIGGER IF EXISTS claims_signed_promotion_backed")
            r = conn.execute(
                "SELECT signature_bundle, asserter_keyid FROM claims "
                "WHERE claim_id = ?", (rep,),
            ).fetchone()
            conn.execute(
                "UPDATE claims SET signature_bundle = ?, asserter_keyid = ?, "
                "support_level = 'REPLICATED' WHERE claim_id = ?",
                (r["signature_bundle"], r["asserter_keyid"], victim),
            )
            conn.commit()
        finally:
            conn.close()
        return root_key, rep, victim

    def test_list_claims_flags_the_forged_row(self, tmp_path: Path) -> None:
        root_key, rep, victim = self._forged_replicated_row(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            by_id = {c["claim_id"]: c for c in list_claims(g._conn)}
        assert by_id[victim]["support_level"] == "REPLICATED"
        assert by_id[victim]["verified"] is False
        assert by_id[rep]["verified"] is True

    def test_exports_refuse_to_publish_the_forged_row(self, tmp_path: Path) -> None:
        from mareforma.export_bundle import build_statement
        from mareforma.exporters.jsonld import JSONLDExporter
        from mareforma.exporters.prov_o import build_prov_o
        from mareforma.exporters.ro_crate import build_crate

        _, _, victim = self._forged_replicated_row(tmp_path)
        for build in (
            lambda: JSONLDExporter(tmp_path).export(),
            lambda: build_prov_o(tmp_path),
            lambda: build_crate(tmp_path),
            lambda: build_statement(tmp_path),
        ):
            with pytest.raises(UnverifiedClaimError) as exc:
                build()
            assert victim in str(exc.value)

    def test_rows_that_never_saw_verify_on_read_are_refused(
        self, tmp_path: Path,
    ) -> None:
        """A row fetched with a plain select carries no ``verified`` key, so a
        flag test alone reads it as clean. The gate refuses it instead: the
        only way past is a row that verify-on-read passed."""
        from mareforma.db import refuse_unverified_claims

        root_key, rep, _ = self._forged_replicated_row(tmp_path)
        with mareforma.open(tmp_path, key_path=root_key) as g:
            unflagged = [
                {k: v for k, v in c.items() if k != "verified"}
                for c in list_claims(g._conn)
            ]
        with pytest.raises(UnverifiedClaimError) as exc:
            refuse_unverified_claims(unflagged)
        assert rep in str(exc.value)

    def test_claim_list_marks_the_forged_row(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from click.testing import CliRunner

        from mareforma.cli import cli

        _, _, victim = self._forged_replicated_row(tmp_path)
        monkeypatch.chdir(tmp_path)
        res = CliRunner().invoke(cli, ["claim", "list"])
        assert res.exit_code == 0, res.output
        marked = [
            line for line in res.output.splitlines()
            if "UNVERIFIED" in line and "totally fabricated" in line
        ]
        assert marked, res.output
        assert victim in res.output


class TestVerifyOnReadContentBinding:
    """Verify-on-read binds the signature to the CONTENT, not only the claim id.

    A process with DB write access is in scope for this gate, so it may drop the
    append-only trigger first. Rewriting ``text`` under a genuine bundle, or
    clearing the bundle and then rewriting ``text``, must both leave the row
    unverified at every gated tier.
    """

    def _launder(self, tmp_path: Path, claim_id: str, *, drop_bundle: bool) -> None:
        conn = sqlite3.connect(str(_db_path(tmp_path)))
        try:
            conn.execute("DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering")
            if drop_bundle:
                conn.execute(
                    "UPDATE claims SET signature_bundle = NULL WHERE claim_id = ?",
                    (claim_id,),
                )
            conn.execute(
                "UPDATE claims SET text = 'LAUNDERED' WHERE claim_id = ?",
                (claim_id,),
            )
            conn.commit()
        finally:
            conn.close()

    def _replicated(self, tmp_path: Path) -> tuple[Path, str]:
        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            g.enroll_validator(_signing.public_key_to_pem(sa.public_key()),
                               identity="a")
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            assert g.get_claim(rep)["support_level"] == "REPLICATED"
        return root_key, rep

    def _assert_unverified(
        self, tmp_path: Path, root_key: Path, claim_id: str, level: str,
    ) -> None:
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.get_claim(claim_id)["verified"] is False
            ids = {r["claim_id"] for r in g.query(min_support=level, limit=99)}
            assert claim_id not in ids

    def test_replicated_text_rewrite_excluded(self, tmp_path: Path) -> None:
        root_key, rep = self._replicated(tmp_path)
        self._launder(tmp_path, rep, drop_bundle=False)
        self._assert_unverified(tmp_path, root_key, rep, "REPLICATED")

    def test_replicated_text_rewrite_without_bundle_excluded(
        self, tmp_path: Path,
    ) -> None:
        root_key, rep = self._replicated(tmp_path)
        self._launder(tmp_path, rep, drop_bundle=True)
        self._assert_unverified(tmp_path, root_key, rep, "REPLICATED")

    def test_established_text_rewrite_excluded(self, tmp_path: Path) -> None:
        root_key, _, rep, _ = _build_established(tmp_path)
        self._launder(tmp_path, rep, drop_bundle=False)
        self._assert_unverified(tmp_path, root_key, rep, "ESTABLISHED")

    def test_established_text_rewrite_without_bundle_excluded(
        self, tmp_path: Path,
    ) -> None:
        root_key, _, rep, _ = _build_established(tmp_path)
        self._launder(tmp_path, rep, drop_bundle=True)
        self._assert_unverified(tmp_path, root_key, rep, "ESTABLISHED")


class TestPromotionDataAxis:
    """The data axis is a secondary collapse, never a gate: absent data never
    blocks promotion on the distinct-signer axis."""

    def test_distinct_signers_one_null_hash_promote(self, tmp_path: Path) -> None:
        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            a = g.assert_claim(
                "A", supports=[up], generated_by="lab_a", signer=sa,
                artifact_hash="a" * 64,
            )
            b = g.assert_claim(  # no artifact_hash -> absent data
                "B", supports=[up], generated_by="lab_b", signer=sb,
            )
            assert g.get_claim(a)["support_level"] == "REPLICATED"
            assert g.get_claim(b)["support_level"] == "REPLICATED"


class TestGrandfatherMigration:
    """A pre-asserter_keyid graph.db (v0.3.6) keeps its REPLICATED rows on
    upgrade: they are grandfathered, not mass-downgraded, with a durable
    legacy_promotion health event recorded exactly once."""

    def _health_events(self, tmp_path: Path) -> list[dict]:
        path = _db_path(tmp_path).parent / "health.jsonl"
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()
                if line.strip()]

    @_requires_drop_column
    def test_legacy_replicated_survives_upgrade_with_health_event(
        self, tmp_path: Path,
    ) -> None:
        sa, sb = _two_signers(tmp_path)
        root_key = _bootstrap_key(tmp_path, "root.key")
        with mareforma.open(tmp_path, key_path=root_key) as g:
            up = g.assert_claim("anchor", generated_by="seed", seed=True)
            rep = g.assert_claim("A", supports=[up], generated_by="lab_a", signer=sa)
            g.assert_claim("B", supports=[up], generated_by="lab_b", signer=sb)
            assert g.get_claim(rep)["support_level"] == "REPLICATED"

        # Simulate a pre-asserter_keyid (v0.3.6) graph.db: drop the index + column
        # so the genuine REPLICATED rows look legacy (NULL keyid) on reopen. The
        # append-only trigger references the column, so it goes first; open_db
        # recreates it after the upgrade re-adds the column.
        conn = sqlite3.connect(str(_db_path(tmp_path)))
        try:
            conn.execute("DROP TRIGGER IF EXISTS claims_signed_fields_no_laundering")
            conn.execute("DROP INDEX IF EXISTS idx_claims_asserter_keyid")
            conn.execute("ALTER TABLE claims DROP COLUMN asserter_keyid")
            conn.commit()
        finally:
            conn.close()

        # Reopen under v0.3.7: column re-added (NULL everywhere), so the genuine
        # REPLICATED rows must be grandfathered, not downgraded.
        with mareforma.open(tmp_path, key_path=root_key) as g:
            assert g.get_claim(rep)["support_level"] == "REPLICATED"
        gf = [e for e in self._health_events(tmp_path)
              if e.get("op") == "legacy_promotion"]
        assert len(gf) == 1
        assert gf[0]["replicated_grandfathered"] >= 1

        # Idempotent: a second open does not re-fire the grandfather.
        with mareforma.open(tmp_path, key_path=root_key) as g:
            pass
        gf2 = [e for e in self._health_events(tmp_path)
               if e.get("op") == "legacy_promotion"]
        assert len(gf2) == 1
