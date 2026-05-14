"""in-toto Statement v1 envelope tests."""

from __future__ import annotations

from mareforma._statement import (
    PREDICATE_TYPE,
    STATEMENT_TYPE,
    SUBJECT_NAME_PREFIX,
    build_statement,
    statement_cid,
    text_sha256,
)


def _minimal_statement(**overrides) -> dict:
    base = dict(
        claim_id="c_abc123",
        text="rats lose weight on intermittent fasting",
        classification="INFERRED",
        generated_by="agent/v1",
        supports=[],
        contradicts=[],
        source_name=None,
        artifact_hash=None,
        created_at="2026-05-13T12:00:00.000000+00:00",
        evidence={},
    )
    base.update(overrides)
    return build_statement(**base)


# ---------------------------------------------------------------------------
# Statement v1 envelope shape
# ---------------------------------------------------------------------------


class TestStatementShape:
    def test_type_and_predicate_type(self) -> None:
        stmt = _minimal_statement()
        assert stmt["_type"] == STATEMENT_TYPE
        assert stmt["_type"] == "https://in-toto.io/Statement/v1"
        assert stmt["predicateType"] == PREDICATE_TYPE
        assert stmt["predicateType"] == "https://mareforma.dev/claim/v1"

    def test_subject_shape(self) -> None:
        stmt = _minimal_statement()
        assert len(stmt["subject"]) == 1
        subj = stmt["subject"][0]
        assert subj["name"].startswith(SUBJECT_NAME_PREFIX)
        assert subj["name"] == "mareforma:claim:c_abc123"
        assert "sha256" in subj["digest"]
        assert len(subj["digest"]["sha256"]) == 64

    def test_subject_digest_is_text_sha256(self) -> None:
        text = "rats lose weight on intermittent fasting"
        stmt = _minimal_statement(text=text)
        assert stmt["subject"][0]["digest"]["sha256"] == text_sha256(text)

    def test_predicate_carries_all_claim_fields(self) -> None:
        stmt = _minimal_statement(
            supports=["c_u1"], contradicts=["c_u2"],
            source_name="notebook.ipynb", artifact_hash="abc" * 21 + "d",
        )
        p = stmt["predicate"]
        for k in (
            "claim_id", "text", "classification", "generated_by",
            "supports", "contradicts", "source_name", "artifact_hash",
            "created_at", "evidence",
        ):
            assert k in p

    def test_predicate_evidence_passes_through(self) -> None:
        evidence = {
            "risk_of_bias": -1,
            "rationale": {"risk_of_bias": "blinding broken"},
        }
        stmt = _minimal_statement(evidence=evidence)
        assert stmt["predicate"]["evidence"] == evidence


# ---------------------------------------------------------------------------
# text_sha256 helper
# ---------------------------------------------------------------------------


class TestTextSha256:
    def test_nfc_normalization(self) -> None:
        import unicodedata
        composed = unicodedata.normalize("NFC", "café")
        decomposed = unicodedata.normalize("NFD", "café")
        assert composed != decomposed
        assert text_sha256(composed) == text_sha256(decomposed)

    def test_different_text_different_hash(self) -> None:
        assert text_sha256("a") != text_sha256("b")

    def test_hex_length_64(self) -> None:
        assert len(text_sha256("anything")) == 64


# ---------------------------------------------------------------------------
# statement_cid — content identifier derivation
# ---------------------------------------------------------------------------


class TestStatementCid:
    def test_byte_stable_same_input(self) -> None:
        stmt1 = _minimal_statement()
        stmt2 = _minimal_statement()
        assert statement_cid(stmt1) == statement_cid(stmt2)

    def test_different_text_different_cid(self) -> None:
        a = _minimal_statement(text="one")
        b = _minimal_statement(text="two")
        assert statement_cid(a) != statement_cid(b)

    def test_different_classification_different_cid(self) -> None:
        a = _minimal_statement(classification="INFERRED")
        b = _minimal_statement(classification="ANALYTICAL")
        assert statement_cid(a) != statement_cid(b)

    def test_evidence_change_changes_cid(self) -> None:
        a = _minimal_statement(evidence={})
        b = _minimal_statement(evidence={
            "risk_of_bias": -1,
            "rationale": {"risk_of_bias": "x"},
        })
        assert statement_cid(a) != statement_cid(b)

    def test_supports_order_invariant(self) -> None:
        # canonicalize sorts dict keys but not list elements — supports
        # IS a list whose order is meaningful (chronological / authorial).
        # Different supports-order → different cid. Documenting this.
        a = _minimal_statement(supports=["c_a", "c_b"])
        b = _minimal_statement(supports=["c_b", "c_a"])
        assert statement_cid(a) != statement_cid(b)

    def test_hex_length_64(self) -> None:
        assert len(statement_cid(_minimal_statement())) == 64
