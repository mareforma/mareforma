"""SQLite-backed epistemic graph for mareforma.

Submodules:

- :mod:`core`: live-write path (claim CRUD, convergence detection,
  validation gates, Rekor saga, verdict protocol, FTS search, TOML backup)
- :mod:`_schema_sql`: DDL constant and column contract
- :mod:`errors`: exception hierarchy
- :mod:`restore`: ``restore()`` disaster-recovery path

Every name previously importable as ``from mareforma.db import X``
continues to work after the carve: the submodule layout is an
internal organisation, not a public API change.
"""

# Re-export the full surface. The CI guard test (test_db_reexports.py)
# walks each submodule's AST and asserts every defined name is
# accessible here via import AND getattr.

from ._schema_sql import (
    _ADDITIVE_TABLES_SQL,
    _SCHEMA_SQL,
    _CLAIM_COLUMNS,
    _CLAIM_SELECT,
)
from .errors import (
    MareformaError,
    DatabaseError,
    ClaimNotFoundError,
    SignedClaimImmutableError,
    IdempotencyConflictError,
    IllegalStateTransitionError,
    ChainIntegrityError,
    LLMValidatorPromotionError,
    SelfValidationError,
    EvidenceCitationError,
    InvalidValidationEnvelopeError,
    RestoreError,
    CycleDetectedError,
    GraphTooLargeError,
    VerdictIssuerError,
    RekorSidecarSectionAbsentWarning,
    RekorSidecarEntryMissingWarning,
)
from .core import (
    # Constants.
    DB_FILENAME,
    _SCHEMA_VERSION,
    _MAX_CLAIM_TEXT_LEN,
    VALID_STATUSES,
    VALID_CLASSIFICATIONS,
    VALID_SUPPORT_LEVELS,
    _SUPPORT_LEVEL_TIERS,
    _SHA256_HEX_RE,
    _CLAIM_ID_RE,
    _REACHABLE_CLAIM_CAP,
    SUPPORT_TYPE_CLAIM,
    SUPPORT_TYPE_DOI,
    SUPPORT_TYPE_EXTERNAL,
    _VALID_SUPPORT_TYPES,
    _VALID_REPLICATION_METHODS,
    _REPLICATION_VERDICT_FIELDS,
    _CONTRADICTION_VERDICT_FIELDS,
    # Connection management.
    open_db,
    open_db_from_db_path,
    _db_path,
    _ensure_claims_columns_for_upgrade,
    _ensure_evidence_lines_columns,
    _attach_supports_cache,
    # Serialization.
    _serialize_predicate_payload,
    _serialize_observed_grounding,
    _observed_grounding_promotes,
    # Chain hash.
    _chain_input_for_claim,
    _compute_prev_hash,
    # Evidence-vector normalization.
    _EVIDENCE_DOWNGRADE_DOMAINS,
    _EVIDENCE_UPGRADE_FLAGS,
    _normalize_evidence,
    # Support classification + cycle detection.
    _is_claim_id,
    classify_support,
    classify_supports,
    _check_no_cycle,
    _GraphConnection,
    # State-machine helpers.
    _signed_delete_error,
    _state_error_from_integrity,
    validate_status,
    normalize_artifact_hash,
    # Claims CRUD.
    _reconcile_idempotency_row,
    add_claim,
    update_claim,
    delete_claim,
    get_claim,
    list_claims,
    delete_claims_by_generated_by,
    # Convergence detection.
    _claim_model_lineage,
    _maybe_update_replicated_unlocked,
    _maybe_update_replicated,
    _maybe_update_replicated_best_effort,
    list_convergence_retry_claims,
    clear_convergence_retry_flag,
    find_dangling_supports,
    # Validation gates.
    _extract_validation_signer_keyid,
    _refuse_llm_validator,
    _refuse_llm_contradiction_issuer,
    _canonical_envelope,
    _refuse_self_verdict,
    _claim_signer_keyids,
    _refuse_self_validation,
    _verify_evidence_seen,
    validate_claim,
    # DOI helpers.
    list_unresolved_claims,
    mark_claim_resolved,
    # Rekor saga.
    _attempt_rekor_saga,
    _record_rekor_inclusion,
    get_rekor_inclusion,
    list_unlogged_claims,
    mark_claim_logged,
    # Verdict protocol.
    _verdict_canonical_payload,
    _require_enrolled_issuer,
    _require_claim_exists,
    record_replication_verdict,
    record_contradiction_verdict,
    list_replication_verdicts,
    list_contradiction_verdicts,
    # Refutation + queries.
    REFUTATION_STATES,
    VALID_REFUTATION_FILTERS,
    refutation_status,
    verify_claim_signatures,
    _verify_role_signatures,
    _json_list,
    query_claims,
    search_claims,
    _read_scan_ceiling,
    _read_path_row,
    _project_verified_rows,
    _row_verified_on_read,
    _verify_validation_on_read,
    _verify_participant_bundle_on_read,
    _trust_domain_disclosure,
    _grandfather_legacy_replicated,
    _refuse_self_validation_across_set,
    _extract_signature_bundle_keyid,
    _enrolled_validator_keyids,
    _compute_validator_reputation,
    _validate_fts5_query,
    get_validator_reputation,
    # Internal helpers.
    _now,
    _backup_claims_toml,
    _backup_suspended,
    suspend_backup,
    resume_backup,
    _drain_backup_window,
    get_project_policy,
    set_project_policy,
)
from .restore import (
    restore,
    _rekor_body_binds_to_claim,
    _restore_predicate_payload,
    _restore_original_signature_bundle,
    _parse_observed_grounding,
    _verify_grounding_binding_on_read,
    _verify_replicated_corroboration,
    _verify_and_insert_replication_verdict,
    _verify_and_insert_contradiction_verdict,
    _verify_and_insert_project_policy,
    _required_field,
    _verify_claim_signatures_on_restore,
)


__all__ = [
    # Schema.
    "_SCHEMA_SQL",
    "_ADDITIVE_TABLES_SQL",
    "_CLAIM_COLUMNS",
    "_CLAIM_SELECT",
    # Exceptions.
    "MareformaError",
    "DatabaseError",
    "ClaimNotFoundError",
    "SignedClaimImmutableError",
    "IdempotencyConflictError",
    "IllegalStateTransitionError",
    "ChainIntegrityError",
    "LLMValidatorPromotionError",
    "SelfValidationError",
    "EvidenceCitationError",
    "InvalidValidationEnvelopeError",
    "RestoreError",
    "CycleDetectedError",
    "GraphTooLargeError",
    "VerdictIssuerError",
    # Constants.
    "DB_FILENAME",
    "VALID_STATUSES",
    "VALID_CLASSIFICATIONS",
    "VALID_SUPPORT_LEVELS",
    "SUPPORT_TYPE_CLAIM",
    "SUPPORT_TYPE_DOI",
    "SUPPORT_TYPE_EXTERNAL",
    # Connection.
    "open_db",
    "open_db_from_db_path",
    # Support classification.
    "classify_support",
    "classify_supports",
    # Validation.
    "validate_status",
    "normalize_artifact_hash",
    "validate_claim",
    # Claims CRUD.
    "add_claim",
    "update_claim",
    "delete_claim",
    "get_claim",
    "list_claims",
    "delete_claims_by_generated_by",
    # Convergence.
    "list_convergence_retry_claims",
    "clear_convergence_retry_flag",
    "find_dangling_supports",
    # DOI.
    "list_unresolved_claims",
    "mark_claim_resolved",
    # Rekor.
    "get_rekor_inclusion",
    "list_unlogged_claims",
    "mark_claim_logged",
    # Verdicts.
    "record_replication_verdict",
    "record_contradiction_verdict",
    "list_replication_verdicts",
    "list_contradiction_verdicts",
    # Refutation + queries.
    "REFUTATION_STATES",
    "VALID_REFUTATION_FILTERS",
    "refutation_status",
    "verify_claim_signatures",
    "query_claims",
    "search_claims",
    "get_validator_reputation",
    # Restore.
    "restore",
    # Internal helpers (public for TOML backup).
    "_backup_claims_toml",
    "_now",
]
