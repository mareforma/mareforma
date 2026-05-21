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
