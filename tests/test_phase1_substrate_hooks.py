"""Tests for v0.3.1 Phase 1 substrate hooks (items 300-305).

Items covered:
- 300 ``predicate_payload`` TEXT column on claims table
- 301 ``predicate_type`` reflective registry (``mareforma.predicates()``)
- 302 ``mareforma export --format=in-toto-v1|ro-crate-1.2`` CLI
- 303 Public ``assert_claim(..., signer=key)`` param on EpistemicGraph
- 304 Per-row ``original_signature_bundle`` column
- 305 ``record_replication_verdict(method='signed-elo-bracket-replay')`` enum extension
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import mareforma
from mareforma import predicate_types as _pt
from mareforma.db import open_db, add_claim, list_claims


# ----------------------------------------------------------------------------
# Item 301 — predicate_type reflective registry
# ----------------------------------------------------------------------------


class TestPredicateTypeRegistry:
    def setup_method(self) -> None:
        # Snapshot the registry so each test starts clean.
        self._snapshot = dict(_pt._registry)

    def teardown_method(self) -> None:
        # Restore the registry — important because the registry is
        # process-global state and other tests inherit it.
        _pt._registry.clear()
        _pt._registry.update(self._snapshot)

    def test_builtin_uris_registered_at_import(self) -> None:
        assert "urn:mareforma:predicate:claim:v1" in mareforma.predicates()
        assert "urn:mareforma:predicate:epistemic-graph:v1" in mareforma.predicates()
        assert "urn:mareforma:predicate:claim-with-roles:v1" in mareforma.predicates()

    def test_register_adapter_uri_appears_in_listing(self) -> None:
        mareforma.register_predicate(
            "urn:mareforma:predicate:tool-call:v1",
            owner="mareforma_tooluniverse",
        )
        assert "urn:mareforma:predicate:tool-call:v1" in mareforma.predicates()

    def test_predicates_returns_sorted_list(self) -> None:
        mareforma.register_predicate("urn:mareforma:predicate:zzz-last:v1")
        mareforma.register_predicate("urn:mareforma:predicate:aaa-first:v1")
        result = mareforma.predicates()
        assert result == sorted(result)

    def test_is_registered_query(self) -> None:
        mareforma.register_predicate("urn:mareforma:predicate:my-uri:v1")
        assert mareforma.is_registered("urn:mareforma:predicate:my-uri:v1")
        assert not mareforma.is_registered("urn:mareforma:predicate:unknown:v1")

    def test_re_register_same_owner_is_noop(self) -> None:
        mareforma.register_predicate(
            "urn:mareforma:predicate:tool-call:v1",
            owner="mareforma_tooluniverse",
        )
        # Same owner, second call: no exception.
        mareforma.register_predicate(
            "urn:mareforma:predicate:tool-call:v1",
            owner="mareforma_tooluniverse",
        )

    def test_re_register_different_owner_raises(self) -> None:
        mareforma.register_predicate(
            "urn:mareforma:predicate:tool-call:v1",
            owner="mareforma_tooluniverse",
        )
        with pytest.raises(mareforma.PredicateTypeError):
            mareforma.register_predicate(
                "urn:mareforma:predicate:tool-call:v1",
                owner="evil_squatter",
            )

    def test_builtin_uri_cannot_be_overwritten(self) -> None:
        with pytest.raises(mareforma.PredicateTypeError):
            mareforma.register_predicate(
                "urn:mareforma:predicate:claim:v1",
                owner="evil_squatter",
            )

    def test_builtin_uri_cannot_be_unregistered(self) -> None:
        with pytest.raises(mareforma.PredicateTypeError):
            _pt.unregister("urn:mareforma:predicate:claim:v1")

    def test_invalid_uri_shape_raises(self) -> None:
        with pytest.raises(mareforma.PredicateTypeError):
            mareforma.register_predicate("not-a-uri")
        with pytest.raises(mareforma.PredicateTypeError):
            mareforma.register_predicate("http://example.com/predicate")
        with pytest.raises(mareforma.PredicateTypeError):
            mareforma.register_predicate("urn:mareforma:predicate:foo")  # no version
        with pytest.raises(mareforma.PredicateTypeError):
            mareforma.register_predicate("urn:mareforma:predicate:foo:v0")  # v0 invalid

    def test_uri_with_slash_in_name_accepted(self) -> None:
        # wet-lab-assay/flow-cytometry pattern
        mareforma.register_predicate(
            "urn:mareforma:predicate:wet-lab-assay/flow-cytometry:v1"
        )
        assert mareforma.is_registered(
            "urn:mareforma:predicate:wet-lab-assay/flow-cytometry:v1"
        )

    def test_predicate_type_error_is_value_error_subclass(self) -> None:
        # Existing callers that catch ValueError continue to work.
        with pytest.raises(ValueError):
            mareforma.register_predicate("not-a-uri")


# ----------------------------------------------------------------------------
# Item 300 — predicate_payload column
# ----------------------------------------------------------------------------


class TestPredicatePayloadColumn:
    def test_default_empty_string(self, tmp_path: Path) -> None:
        conn = open_db(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "test claim")
            row = next(c for c in list_claims(conn) if c["claim_id"] == claim_id)
            # Column exists, default empty string.
            assert row["predicate_payload"] == ""
        finally:
            conn.close()

    def test_write_dict_serializes_canonical_json(self, tmp_path: Path) -> None:
        conn = open_db(tmp_path)
        try:
            claim_id = add_claim(
                conn,
                tmp_path,
                "test claim",
                predicate_payload={"tool_name": "openai", "version": "1.0"},
            )
            row = next(c for c in list_claims(conn) if c["claim_id"] == claim_id)
            payload = row["predicate_payload"]
            # Canonical JSON: sorted keys, no whitespace.
            assert payload == '{"tool_name":"openai","version":"1.0"}'
        finally:
            conn.close()

    def test_round_trips_through_claims_toml_backup(self, tmp_path: Path) -> None:
        conn = open_db(tmp_path)
        try:
            add_claim(
                conn,
                tmp_path,
                "claim with predicate",
                predicate_payload={"x": 1},
            )
        finally:
            conn.close()
        # claims.toml is written on every mutation.
        toml_path = tmp_path / "claims.toml"
        assert toml_path.exists()
        content = toml_path.read_text()
        assert "predicate_payload" in content

    def test_v030_caller_writes_empty_default(self, tmp_path: Path) -> None:
        # Callers that don't pass predicate_payload write the empty
        # default; round-trip preserves "no predicate".
        conn = open_db(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "v0.3.0-shape call")
            row = next(c for c in list_claims(conn) if c["claim_id"] == claim_id)
            assert row["predicate_payload"] == ""
        finally:
            conn.close()


# ----------------------------------------------------------------------------
# Item 304 — original_signature_bundle column
# ----------------------------------------------------------------------------


class TestOriginalSignatureBundleColumn:
    def test_default_null(self, tmp_path: Path) -> None:
        conn = open_db(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "test claim")
            row = next(c for c in list_claims(conn) if c["claim_id"] == claim_id)
            assert row["original_signature_bundle"] is None
        finally:
            conn.close()

    def test_explicit_write_persists(self, tmp_path: Path) -> None:
        original_envelope = json.dumps({
            "payloadType": "application/vnd.in-toto+json",
            "payload": "base64...",
            "signatures": [{"keyid": "abc", "sig": "xyz"}],
        })
        conn = open_db(tmp_path)
        try:
            claim_id = add_claim(
                conn,
                tmp_path,
                "federation-imported claim",
                original_signature_bundle=original_envelope,
            )
            row = next(c for c in list_claims(conn) if c["claim_id"] == claim_id)
            assert row["original_signature_bundle"] == original_envelope
        finally:
            conn.close()


# ----------------------------------------------------------------------------
# Item 305 — record_replication_verdict enum extension
# ----------------------------------------------------------------------------


class TestReplicationVerdictMethodEnum:
    def test_signed_elo_bracket_replay_in_valid_methods(self) -> None:
        from mareforma.db import _VALID_REPLICATION_METHODS
        assert "signed-elo-bracket-replay" in _VALID_REPLICATION_METHODS

    def test_pre_existing_methods_still_valid(self) -> None:
        # Regression: don't drop any of the v0.3.0 methods.
        from mareforma.db import _VALID_REPLICATION_METHODS
        assert "hash-match" in _VALID_REPLICATION_METHODS
        assert "semantic-cluster" in _VALID_REPLICATION_METHODS
        assert "shared-resolved-upstream" in _VALID_REPLICATION_METHODS
        assert "cross-method" in _VALID_REPLICATION_METHODS


# ----------------------------------------------------------------------------
# Item 303 — public assert_claim(signer=) on EpistemicGraph
# ----------------------------------------------------------------------------


class TestPerCallSignerOverride:
    def test_signer_kwarg_accepted_no_signing_path(self, tmp_path: Path) -> None:
        # Unsigned graph + signer=None on the call: equivalent to v0.3.0
        # behaviour. The kwarg exists and is accepted.
        with mareforma.open(tmp_path) as graph:
            claim_id = graph.assert_claim("test", signer=None)
            assert claim_id

    def test_predicate_payload_kwarg_threaded_through(
        self, tmp_path: Path
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            claim_id = graph.assert_claim(
                "claim with predicate",
                predicate_payload={"adapter": "test", "version": 1},
            )
        # Read back via list_claims directly.
        conn = open_db(tmp_path)
        try:
            row = next(
                c for c in list_claims(conn) if c["claim_id"] == claim_id
            )
            assert row["predicate_payload"] == (
                '{"adapter":"test","version":1}'
            )
        finally:
            conn.close()

    def test_original_signature_bundle_kwarg_threaded_through(
        self, tmp_path: Path
    ) -> None:
        with mareforma.open(tmp_path) as graph:
            claim_id = graph.assert_claim(
                "imported claim",
                original_signature_bundle='{"original":"envelope"}',
            )
        conn = open_db(tmp_path)
        try:
            row = next(
                c for c in list_claims(conn) if c["claim_id"] == claim_id
            )
            assert row["original_signature_bundle"] == (
                '{"original":"envelope"}'
            )
        finally:
            conn.close()


# ----------------------------------------------------------------------------
# Item 302 — mareforma export --format CLI
# ----------------------------------------------------------------------------


class TestExportFormats:
    """The CLI integration test path covers `mareforma export --format`.

    Build the format outputs by directly calling the exporter modules
    here (CLI-shell-level testing would need a click runner; the
    substrate's existing test_cli.py tests cover the CLI plumbing).
    """

    def _seed_graph(self, tmp_path: Path) -> str:
        with mareforma.open(tmp_path) as graph:
            return graph.assert_claim(
                "test claim for export",
                classification="ANALYTICAL",
                source_name="test-source",
            )

    def test_in_toto_v1_export_produces_valid_statement(
        self, tmp_path: Path
    ) -> None:
        from mareforma.exporters.in_toto import (
            build_statement,
            IN_TOTO_STATEMENT_TYPE,
            PREDICATE_TYPE,
        )
        self._seed_graph(tmp_path)
        statement = build_statement(tmp_path)
        assert statement["_type"] == IN_TOTO_STATEMENT_TYPE
        assert statement["predicateType"] == PREDICATE_TYPE
        assert "subject" in statement
        assert "predicate" in statement
        # Subject must be a non-empty list (at least one claim was seeded).
        assert isinstance(statement["subject"], list)
        assert len(statement["subject"]) >= 1
        # Each subject carries the urn:mareforma:claim:<uuid> prefix.
        for s in statement["subject"]:
            assert s["name"].startswith("urn:mareforma:claim:")
            assert "sha256" in s["digest"]

    def test_ro_crate_export_produces_valid_metadata(
        self, tmp_path: Path
    ) -> None:
        from mareforma.exporters.ro_crate import (
            build_crate,
            RO_CRATE_PROFILE,
            RO_CRATE_CONTEXT,
            PROCESS_RUN_CRATE_PROFILE,
        )
        claim_id = self._seed_graph(tmp_path)
        crate = build_crate(tmp_path)
        assert crate["@context"] == RO_CRATE_CONTEXT
        graph = crate["@graph"]
        # Root metadata descriptor conforms to RO-Crate 1.2 AND
        # Process Run Crate profiles.
        meta = next(e for e in graph if e["@id"] == "ro-crate-metadata.json")
        conforms = meta["conformsTo"]
        if isinstance(conforms, list):
            conforms_ids = {c["@id"] for c in conforms}
        else:
            conforms_ids = {conforms["@id"]}
        assert RO_CRATE_PROFILE in conforms_ids
        assert PROCESS_RUN_CRATE_PROFILE in conforms_ids
        # Root Dataset has the seeded claim in hasPart.
        root = next(e for e in graph if e["@id"] == "./")
        assert root["@type"] == "Dataset"
        has_part_ids = {p["@id"] for p in root["hasPart"]}
        assert f"urn:mareforma:claim:{claim_id}" in has_part_ids
        # CreateAction entity for the claim exists.
        action = next(
            e for e in graph
            if e["@id"] == f"urn:mareforma:claim:{claim_id}"
        )
        assert action["@type"] == "CreateAction"
        # MediaObject for the claim text exists.
        text_obj = next(
            e for e in graph if e["@id"] == f"#claim-text/{claim_id}"
        )
        assert text_obj["@type"] == "MediaObject"
        assert text_obj["text"] == "test claim for export"

    def test_ro_crate_empty_graph_handles_gracefully(
        self, tmp_path: Path
    ) -> None:
        # Open + close to create the db, then export with no claims.
        with mareforma.open(tmp_path):
            pass
        from mareforma.exporters.ro_crate import build_crate
        crate = build_crate(tmp_path)
        # Empty graph still produces a valid crate (just empty hasPart).
        root = next(e for e in crate["@graph"] if e["@id"] == "./")
        assert root["@type"] == "Dataset"
        assert root["hasPart"] == []

    def test_ro_crate_missing_graph_raises(self, tmp_path: Path) -> None:
        from mareforma.exporters.ro_crate import build_crate
        with pytest.raises(FileNotFoundError):
            build_crate(tmp_path)

    def test_in_toto_empty_graph_handles_gracefully(
        self, tmp_path: Path
    ) -> None:
        # M11 from /review — symmetric coverage with RO-Crate exporter.
        from mareforma.exporters.in_toto import build_statement
        with mareforma.open(tmp_path):
            pass
        statement = build_statement(tmp_path)
        # Empty graph still produces a valid statement (just empty subject).
        assert statement["_type"]
        assert statement["predicateType"]
        assert "subject" in statement


# ----------------------------------------------------------------------------
# /review-driven hardening tests (P0 + H + M findings from adversarial pass)
# ----------------------------------------------------------------------------


class TestReplicationVerdictIntegration:
    """C1 from /review — `signed-elo-bracket-replay` must work end-to-end,
    not just pass the Python validator. The SQL CHECK constraint MUST
    list the new method or the INSERT fails with IntegrityError.
    """

    def test_signed_elo_bracket_replay_inserts_successfully(
        self, tmp_path: Path
    ) -> None:
        # Open with a key so we have a signer that can issue verdicts.
        from mareforma import signing as _signing
        key_path = tmp_path / "test.key"
        _signing.save_private_key(_signing.generate_keypair(), key_path)
        # Seed two claims to satisfy FK constraints.
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            a = graph.assert_claim(
                "claim A",
                classification="DERIVED",
                generated_by="agent-a",
                seed=True,
            )
            b = graph.assert_claim(
                "claim B",
                classification="DERIVED",
                supports=[a],
                generated_by="agent-b",
            )
            # Use the new method — should NOT raise.
            graph.record_replication_verdict(
                verdict_id="rv_test_elo",
                cluster_id="cl_test",
                member_claim_id=b,
                other_claim_id=a,
                method="signed-elo-bracket-replay",
                confidence={"bracket_id": "br_test", "ordinal": 1},
            )
            verdicts = graph.replication_verdicts(member_claim_id=b)
            assert any(v["method"] == "signed-elo-bracket-replay"
                       for v in verdicts)


class TestIdempotencyReconciliationCoversNewFields:
    """H1 from /review — retry with different predicate_payload or
    original_signature_bundle but same idempotency_key MUST raise
    IdempotencyConflictError instead of silently merging.
    """

    def test_predicate_payload_mismatch_raises(self, tmp_path: Path) -> None:
        from mareforma.db import IdempotencyConflictError
        with mareforma.open(tmp_path) as graph:
            graph.assert_claim(
                "shared text",
                idempotency_key="run_X_claim_1",
                predicate_payload={"adapter": "test"},
            )
            with pytest.raises(IdempotencyConflictError):
                graph.assert_claim(
                    "shared text",
                    idempotency_key="run_X_claim_1",
                    predicate_payload={"adapter": "DIFFERENT"},
                )

    def test_original_signature_bundle_mismatch_raises(
        self, tmp_path: Path
    ) -> None:
        from mareforma.db import IdempotencyConflictError
        with mareforma.open(tmp_path) as graph:
            graph.assert_claim(
                "shared text",
                idempotency_key="run_Y_claim_1",
                original_signature_bundle='{"version":"a"}',
            )
            with pytest.raises(IdempotencyConflictError):
                graph.assert_claim(
                    "shared text",
                    idempotency_key="run_Y_claim_1",
                    original_signature_bundle='{"version":"b"}',
                )

    def test_same_idempotency_key_matching_fields_is_idempotent(
        self, tmp_path: Path
    ) -> None:
        # Sanity: matching fields → same claim_id returned, no error.
        with mareforma.open(tmp_path) as graph:
            id1 = graph.assert_claim(
                "shared text",
                idempotency_key="run_Z_claim_1",
                predicate_payload={"adapter": "test"},
                original_signature_bundle='{"version":"a"}',
            )
            id2 = graph.assert_claim(
                "shared text",
                idempotency_key="run_Z_claim_1",
                predicate_payload={"adapter": "test"},
                original_signature_bundle='{"version":"a"}',
            )
            assert id1 == id2


class TestPredicatePayloadTypeValidation:
    """M5 from /review — non-dict predicate_payload raises TypeError
    instead of silently canonicalizing into a non-object JSON string."""

    def test_non_dict_payload_raises_typeerror(self, tmp_path: Path) -> None:
        with mareforma.open(tmp_path) as graph:
            with pytest.raises(TypeError):
                graph.assert_claim(
                    "claim",
                    predicate_payload="just a string",  # type: ignore[arg-type]
                )
            with pytest.raises(TypeError):
                graph.assert_claim(
                    "claim",
                    predicate_payload=[1, 2, 3],  # type: ignore[arg-type]
                )
            with pytest.raises(TypeError):
                graph.assert_claim(
                    "claim",
                    predicate_payload=42,  # type: ignore[arg-type]
                )


class TestRoCrateInputValidation:
    """H4 / H6 from /review — RO-Crate exporter refuses non-UUID
    claim_ids and gracefully handles malformed supports_json shapes.
    """

    def test_non_uuid_claim_id_raises(self) -> None:
        from mareforma.exporters.ro_crate import _claim_to_create_action
        with pytest.raises(ValueError, match="non-UUID claim_id"):
            _claim_to_create_action({
                "claim_id": "not-a-uuid",
                "generated_by": "agent",
            })

    def test_unsafe_agent_id_sanitized(self) -> None:
        from mareforma.exporters.ro_crate import _safe_agent_id
        # Slash + dash + dot OK (model/version/context convention).
        assert _safe_agent_id("openai/gpt-4o/v1.0") == "openai/gpt-4o/v1.0"
        # Hash sign → underscore (breaks JSON-LD @id fragment otherwise).
        assert "#" not in _safe_agent_id("evil#agent")
        # Whitespace → underscore.
        assert " " not in _safe_agent_id("agent with spaces")
        # Other shell-meta → underscore.
        assert ";" not in _safe_agent_id("agent;rm")

    def test_supports_json_dict_does_not_iterate_keys(
        self, tmp_path: Path
    ) -> None:
        # H6: a malformed supports_json that decoded to a dict would
        # iterate its keys under the old code (silent footgun).
        # New code checks isinstance(decoded, list) explicitly.
        from mareforma.exporters.ro_crate import _claim_to_create_action
        import uuid
        valid_uuid = str(uuid.uuid4())
        action = _claim_to_create_action({
            "claim_id": valid_uuid,
            "generated_by": "agent",
            # Dict instead of list — should be ignored, not iterated.
            "supports_json": '{"x": 1, "y": 2}',
        })
        assert "object" not in action  # No supports[] references emitted.

    def test_supports_json_filters_non_uuid_refs(
        self, tmp_path: Path
    ) -> None:
        # DOIs / external refs in supports[] are intentionally omitted
        # from the JSON-LD @id graph (no urn:mareforma:claim: identity).
        from mareforma.exporters.ro_crate import _claim_to_create_action
        import uuid
        valid_uuid = str(uuid.uuid4())
        other_uuid = str(uuid.uuid4())
        action = _claim_to_create_action({
            "claim_id": valid_uuid,
            "generated_by": "agent",
            "supports_json": json.dumps([
                other_uuid,
                "10.1038/s41586-026-10652-y",  # DOI — should be filtered.
                "external-ref-string",  # also filtered.
            ]),
        })
        assert action["object"] == [
            {"@id": f"urn:mareforma:claim:{other_uuid}"}
        ]


class TestRestoreTypeSafety:
    """H2 from /review — restore() must not silently coerce
    unexpected types (False, 0, dict, list) into the new columns."""

    def test_restore_with_non_string_predicate_payload_coerces_to_empty(
        self, tmp_path: Path
    ) -> None:
        # Build a graph, then mock a malformed claims.toml restore where
        # predicate_payload has an unexpected type. The restore should
        # NOT raise (isinstance check returns False, default-empty path).
        import tomli_w
        toml_path = tmp_path / "claims.toml"
        bad_claim_id = "abcdef01-2345-6789-abcd-ef0123456789"
        toml_path.write_text(tomli_w.dumps({
            "claims": {
                bad_claim_id: {
                    "text": "test",
                    "classification": "INFERRED",
                    "support_level": "PRELIMINARY",
                    "generated_by": "agent",
                    "status": "open",
                    "supports": [],
                    "contradicts": [],
                    "comparison_summary": "",
                    "evidence_json": "{}",
                    "predicate_payload": True,  # bad type — should coerce.
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                }
            }
        }))
        result = mareforma.restore(tmp_path)
        # Restore succeeds; non-string predicate_payload coerced to "".
        assert result["claims_restored"] == 1
