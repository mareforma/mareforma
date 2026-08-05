"""Restore a graph from claims.toml: catastrophic-loss recovery path.

This module contains the ``restore()`` function and its verification
helpers. The restore path is fail-all-or-nothing: every enrollment
envelope, every claim signature, every validation envelope is verified
before the transaction commits. The first failure rolls back to
pre-restore state.

Separated from the live-write path (``db/core.py``) because restore
is a one-shot disaster-recovery operation with a distinct invariant
set (the rebuild proves "what was signed is what was written") while
the live path proves "what is being written is being signed."
"""

from __future__ import annotations

import base64
import json
import sqlite3
import warnings
from pathlib import Path
from typing import Any

from .errors import (
    LLMValidatorPromotionError,
    RestoreError,
    SelfValidationError,
    VerdictIssuerError,
)
from .core import (
    open_db,
    _CorroborationIndex,
    _promotion_window,
    _compute_prev_hash,
    _is_claim_id,
    _refuse_llm_contradiction_issuer,
    _refuse_llm_validator,
    _refuse_self_validation,
    _refuse_self_verdict,
    _extract_validation_signer_keyid,
    _extract_signature_bundle_keyid,
    _serialize_observed_grounding,
    _validate_claim_text,
    _replication_verdict_pae,
    _verdict_canonical_payload,
    _CONTRADICTION_VERDICT_FIELDS,
    _TRUST_TABLE_BACKUP,
)


def _parse_observed_grounding(value) -> dict | None:
    """Parse the restored ``observed_grounding`` record into a dict, or None.

    The column is stored as canonical JSON (or omitted). Restore rebuilds the
    signed statement from it, so a malformed value must abort loudly rather than
    silently drop the field and let statement_cid mismatch masquerade as tamper.
    Absent / empty → None (a claim asserted without the observer).
    """
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError) as exc:
            raise RestoreError(
                "A claim's observed_grounding is not valid JSON; "
                "claims.toml is malformed.",
                kind="claim_unverified",
            ) from exc
        if not isinstance(parsed, dict):
            raise RestoreError(
                "A claim's observed_grounding must be a JSON object; "
                "claims.toml is malformed.",
                kind="claim_unverified",
            )
        return parsed
    raise RestoreError(
        f"A claim's observed_grounding has unexpected type "
        f"{type(value).__name__}; claims.toml is malformed.",
        kind="claim_unverified",
    )


def _verify_grounding_binding_on_read(claim_id, record, predicate) -> None:
    """Re-check a GROUNDED verdict's grounded set against the finding's citation.

    Only a GROUNDED v0.3.9 record (one carrying ``grounded_sources``) is checked;
    a v0.3.8 record omits it and is "not checkable," and OPAQUE / UNGROUNDED never
    assert the data arrived. The binding re-check uses ``grounded_sources``, the
    cited sources a read was actually observed for, not the declared
    ``cited_sources``, so it matches the write-side gate exactly. The finding's
    citation identifiers are taken from the predicate this restore parsed, the
    normalized ``data_sources`` and any content-addressed ``data_ids``, so the
    comparison is pure string equality with no filesystem access. A disjoint
    match is a binding violation. Statement v1 binds neither key, so a claim
    whose citation set lives only in the ``predicate_payload`` column is not
    checkable here; the live audit path reads that column instead
    (:func:`mareforma.cli._claim_bound_sources`).
    """
    if not isinstance(record, dict) or record.get("grounding") != "GROUNDED":
        return
    grounded_sources = record.get("grounded_sources")
    if grounded_sources is None:  # pre-v0.3.9 axis: binding was not checkable
        return

    from mareforma.observe._binding import (
        BindingState,
        check_grounding_binding,
        predicate_citation_sources,
    )

    finding_sources = predicate_citation_sources(predicate)
    result = check_grounding_binding(tuple(grounded_sources), finding_sources)
    if result.state is BindingState.DISJOINT:
        raise RestoreError(
            f"Claim {claim_id} stores a GROUNDED observed-grounding verdict whose "
            "cited set is disjoint from the finding's citation, binding violation.",
            kind="claim_unverified",
        )


def _claim_envelope(claim: dict) -> dict | None:
    """Parse a claim's signature bundle, or None when it has none."""
    bundle_json = claim.get("signature_bundle")
    if not bundle_json:
        return None
    try:
        bundle = json.loads(bundle_json)
    except (json.JSONDecodeError, TypeError):
        return None
    return bundle if isinstance(bundle, dict) else None


def _restore_predicate_payload(c: dict, claim_id: str) -> str:
    """Coerce restored ``predicate_payload`` per the add_claim write contract.

    ``add_claim`` rejects non-dict / non-string values at write time.
    Restore must be at least as strict; a tampered TOML carrying an int
    or list for this field would otherwise land as ``""`` (silent data
    loss). Either the field is a string (passed through) or absent
    (default empty); anything else is a malformed TOML and aborts.
    """
    val = c.get("predicate_payload")
    if val is None:
        return ""
    if isinstance(val, str):
        return val
    raise RestoreError(
        f"Claim {claim_id} predicate_payload is not a string "
        f"(got {type(val).__name__}); claims.toml is malformed.",
        kind="claim_unverified",
    )


def _restore_original_signature_bundle(c: dict, claim_id: str) -> str | None:
    """Coerce restored ``original_signature_bundle`` consistently.

    Same posture as :func:`_restore_predicate_payload`. Non-string,
    non-null values are TOML corruption and abort the restore.
    """
    val = c.get("original_signature_bundle")
    if val is None:
        return None
    if isinstance(val, str):
        return val
    raise RestoreError(
        f"Claim {claim_id} original_signature_bundle is not a string "
        f"(got {type(val).__name__}); claims.toml is malformed.",
        kind="claim_unverified",
    )


def _restore_evidence_domain(
    evidence: dict, domain: str, claim_id: str,
) -> int:
    """Coerce one restored GRADE domain to an int.

    Same posture as :func:`_restore_predicate_payload`. A bare ``int()``
    over a tampered TOML leaks a ``ValueError`` or a ``TypeError`` past
    restore's documented error surface. An absent or falsy value keeps the
    0 default, a number or a digit string passes through, anything else
    (booleans included) aborts. Out-of-range scores stay with the CHECK
    constraint on insert, which already reports them.
    """
    val = evidence.get(domain)
    if not isinstance(val, bool):
        if not val:
            return 0
        if isinstance(val, (int, float)):
            return int(val)
        if isinstance(val, str) and val.isdigit():
            return int(val)
    raise RestoreError(
        f"Claim {claim_id} evidence_json domain {domain!r} is not a GRADE "
        f"score (got {type(val).__name__}); claims.toml is malformed.",
        kind="claim_unverified",
    )


def _discard_created_paths(
    mareforma_dir: Path, dir_existed: bool, files_existed: set[str],
) -> None:
    """Remove what a refused restore created under ``.mareforma/``.

    ``restore`` opens the db before it verifies anything, so a refusal would
    otherwise leave an empty ``graph.db`` and supports cache behind and every
    later read command would report a live empty project instead of no project
    at all. Only paths this call created are removed. An ``OSError`` here is
    swallowed: cleanup must not mask the ``RestoreError`` that is the answer.
    """
    try:
        for path in mareforma_dir.iterdir():
            if path.is_file() and path.name not in files_existed:
                path.unlink()
        if not dir_existed:
            mareforma_dir.rmdir()
    except OSError:
        pass


def restore(
    project_root: Path | str,
    *,
    claims_toml: Path | str | None = None,
    rekor_log_pubkey_pem: bytes | None = None,
    enforce_rekor_policy: bool = False,
) -> dict:
    """Rebuild a fresh graph.db from claims.toml.

    Reverse of :func:`_backup_claims_toml`. Intended for catastrophic-
    loss recovery: ``graph.db`` is missing or corrupt, the operator
    has a recent ``claims.toml``, the project must be reconstructable.

    Parameters
    ----------
    enforce_rekor_policy:
        When True, the operator asserts this project requires Rekor
        witnessing. restore then requires a valid root-signed
        ``[project_policy]`` (``rekor_required``) and a pinned
        ``rekor_log_pubkey_pem``, and marks a signed claim
        convergence-eligible only when it carries a verified, claim-bound
        inclusion proof. This closes the strip-route: an edited
        claims.toml cannot make an unwitnessed claim convergence-eligible,
        because the signed policy cannot be forged and its absence is
        refused. Off by default (best-effort, matching the pinned-key
        opt-in posture).
    rekor_log_pubkey_pem:
        PEM-encoded Rekor log operator public key. When supplied, every
        ``[rekor_inclusions]`` entry is cryptographically verified via
        :func:`mareforma.signing.verify_rekor_inclusion` before INSERT.
        Verification failure raises :class:`RestoreError` with
        ``kind='rekor_inclusion_invalid'``. When ``None``, entries are
        replayed unverified (matching the submit-path opt-in posture).

    The rebuild is **fresh-only**. ``restore`` refuses to run if
    ``.mareforma/graph.db`` already contains claims; merge semantics
    are out of scope for the current release (status drift, supports[] divergence,
    and validator chain conflicts have no clean answers). Wipe
    ``graph.db`` first if you really mean to overwrite.

    Signature verification is fail-all-or-nothing. Every enrollment
    envelope is verified against its parent key; every claim
    ``signature_bundle`` is verified against the enrolled signer key;
    every ``validation_signature`` is verified against its signer key.
    The first failure rolls back the entire transaction: the project
    stays in its pre-restore state.

    Parameters
    ----------
    project_root:
        Project directory. ``graph.db`` is reconstructed under
        ``<project_root>/.mareforma/``.
    claims_toml:
        Path to the source TOML. Defaults to
        ``<project_root>/claims.toml``.

    Returns
    -------
    dict
        ``{"validators_restored": N, "claims_restored": M}``.

    Raises
    ------
    RestoreError
        With a ``.kind`` field. See :class:`RestoreError`.
    """
    # TOML parser: stdlib `tomllib` on Python 3.11+, PyPI `tomli` on 3.10.
    # Both share the same `loads` + `TOMLDecodeError` API. The previous
    # code imported `tomli` unconditionally; pyproject only declares it
    # for Python < 3.11, so a 3.11+ install hit ModuleNotFoundError the
    # moment restore() ran, silently breaking the catastrophic-loss
    # recovery path on the most common modern Python.
    try:
        import tomllib  # Python 3.11+ stdlib
    except ImportError:  # pragma: no cover  -- Python 3.10 path
        import tomli as tomllib  # type: ignore[no-redef]
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    root = Path(project_root).resolve()
    toml_path = (
        Path(claims_toml) if claims_toml is not None else root / "claims.toml"
    )
    if not toml_path.exists():
        raise RestoreError(
            f"claims.toml not found at {toml_path}",
            kind="toml_not_found",
        )

    try:
        raw_text = toml_path.read_text(encoding="utf-8")
    except OSError as exc:
        # The path exists but cannot be read (permission denied, or it resolves
        # to a directory). Surface the documented RestoreError rather than a raw
        # OSError/IsADirectoryError traceback to an operator mid-recovery.
        raise RestoreError(
            f"claims.toml at {toml_path} could not be read: {exc}",
            kind="toml_unreadable",
        ) from exc
    try:
        data = tomllib.loads(raw_text)
    except tomllib.TOMLDecodeError as exc:
        raise RestoreError(
            f"claims.toml at {toml_path} is malformed: {exc}",
            kind="toml_malformed",
        ) from exc

    # Validate section shapes before any sort touches them: a tampered
    # claims.toml can set a section to a scalar or plant a scalar entry, and the
    # sort helpers call .items()/.get() on them and would leak a raw
    # AttributeError past the documented RestoreError contract.
    for _section_name in (
        "validators", "claims", "replication_verdicts", "contradiction_verdicts",
        "rekor_inclusions",
    ):
        _validate_section_shape(data.get(_section_name), _section_name)
    # [project_policy] holds fields, not rows, so only the section shape is
    # checked; _required_field reports a missing or malformed field.
    _validate_section_shape(
        data.get("project_policy"), "project_policy", allow_scalar_entries=True,
    )
    # [graph_meta] holds fields too: the supports-edge revision counter.
    _validate_section_shape(
        data.get("graph_meta"), "graph_meta", allow_scalar_entries=True,
    )

    validators_section: dict = data.get("validators", {}) or {}
    claims_section: dict = data.get("claims", {}) or {}

    # Note what the project already had: open_db creates .mareforma/ and its
    # files, and a refused restore has to hand the directory back untouched.
    mareforma_dir = root / ".mareforma"
    dir_existed = mareforma_dir.is_dir()
    files_existed = (
        {p.name for p in mareforma_dir.iterdir()} if dir_existed else set()
    )

    conn = open_db(root)
    try:
        signed_mode = bool(validators_section)

        # Order validators by enrolled_at so the root (earliest) lands
        # first and chain-walk parent lookups always succeed in-table.
        ordered_validators = sorted(
            validators_section.items(),
            key=lambda kv: kv[1].get("enrolled_at", ""),
        )

        # BEGIN IMMEDIATE first, THEN re-check emptiness. The write lock
        # closes the window between "check" and "act", a concurrent
        # writer cannot slip a row in between the SELECT and the
        # restore INSERTs.
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT COUNT(*) AS n FROM claims"
            ).fetchone()
            if existing["n"] > 0:
                raise RestoreError(
                    f"graph.db at {root}/.mareforma/graph.db already has "
                    f"{existing['n']} claim(s). restore() refuses to merge, "
                    "wipe graph.db first, or use a fresh project root.",
                    kind="graph_not_empty",
                )
            for keyid, v in ordered_validators:
                ctx_v = f"Validator {keyid[:12]}…"
                row = {
                    "keyid": keyid,
                    "pubkey_pem": _required_field(v, "pubkey_pem", ctx_v),
                    "identity": _required_field(v, "identity", ctx_v),
                    "validator_type": _required_field(
                        v, "validator_type", ctx_v,
                    ),
                    "enrolled_at": _required_field(v, "enrolled_at", ctx_v),
                    "enrolled_by_keyid": _required_field(
                        v, "enrolled_by_keyid", ctx_v,
                    ),
                    "enrollment_envelope": _required_field(
                        v, "enrollment_envelope", ctx_v,
                    ),
                }
                if row["enrolled_by_keyid"] == keyid:
                    parent_pem_b64 = row["pubkey_pem"]
                else:
                    parent_v = validators_section.get(row["enrolled_by_keyid"])
                    if parent_v is None:
                        raise RestoreError(
                            f"Validator {keyid[:12]}… claims to be enrolled "
                            f"by {row['enrolled_by_keyid'][:12]}… but that "
                            "parent is missing from claims.toml.",
                            kind="enrollment_unverified",
                        )
                    parent_pem_b64 = _required_field(
                        parent_v, "pubkey_pem",
                        f"Parent validator {row['enrolled_by_keyid'][:12]}…",
                    )
                try:
                    parent_pem = base64.standard_b64decode(parent_pem_b64)
                except (ValueError, TypeError) as exc:
                    raise RestoreError(
                        f"Parent pubkey_pem for validator "
                        f"{keyid[:12]}… is not valid base64.",
                        kind="enrollment_unverified",
                    ) from exc
                if not _validators.verify_enrollment(row, parent_pem):
                    raise RestoreError(
                        f"Enrollment envelope for validator "
                        f"{keyid[:12]}… failed verification.",
                        kind="enrollment_unverified",
                    )
                try:
                    conn.execute(
                        "INSERT INTO validators "
                        "(keyid, pubkey_pem, identity, validator_type, "
                        " enrolled_at, enrolled_by_keyid, "
                        " enrollment_envelope) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            keyid, row["pubkey_pem"], row["identity"],
                            row["validator_type"], row["enrolled_at"],
                            row["enrolled_by_keyid"],
                            row["enrollment_envelope"],
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    # Duplicate keyid PK, bad validator_type CHECK, or any
                    # other validator-table integrity violation. Translate
                    # to RestoreError so callers honour the documented
                    # contract.
                    raise RestoreError(
                        f"Validator {keyid[:12]}… could not be restored: "
                        f"{exc}",
                        kind="enrollment_unverified",
                    ) from exc

            # The per-row envelope check above is weaker than what every live
            # write path enforces: one self-signed root, every other validator
            # chained beneath it. A hand-edited claims.toml can carry a second
            # self-signed block whose own envelope verifies, which would leave
            # a table the chain walk refuses to trust at all. Re-run the live
            # walk over the just-inserted rows.
            _validators.invalidate_conn_cache(conn)
            roots = _validators.enrollment_roots(conn)
            if len(roots) > 1:
                extra = ", ".join(f"{k[:12]}…" for k in roots[1:])
                raise RestoreError(
                    "claims.toml carries more than one self-signed root "
                    f"validator (extra: {extra}). A project has exactly one "
                    "root of trust.",
                    kind="enrollment_unverified",
                )
            for keyid, _ in ordered_validators:
                if not _validators.is_enrolled(conn, keyid):
                    raise RestoreError(
                        f"Validator {keyid[:12]}… does not chain back to the "
                        "project root of trust.",
                        kind="enrollment_unverified",
                    )

            # Project policy: verify the root-signed [project_policy] envelope
            # (if present) and round-trip it into the restored graph. A present
            # but unverifiable policy is tampered signed material and aborts.
            policy = _verify_and_insert_project_policy(
                conn, data.get("project_policy"), validators_section, _signing,
            )
            policy_rekor_required = bool(policy and policy["rekor_required"])
            # Enforcement is the operator's out-of-band assertion, like the
            # pinned log pubkey: only when they pass enforce_rekor_policy does
            # restore refuse to reconstruct convergence-eligible state for a
            # signed claim that lacks a verified, claim-bound inclusion proof.
            policy_enforced = False
            if enforce_rekor_policy:
                if not policy_rekor_required:
                    raise RestoreError(
                        "enforce_rekor_policy=True but claims.toml carries no "
                        "root-signed policy requiring Rekor witnessing.",
                        kind="policy_absent",
                    )
                if rekor_log_pubkey_pem is None:
                    raise RestoreError(
                        "enforce_rekor_policy=True requires rekor_log_pubkey_pem "
                        "so inclusion proofs can be verified.",
                        kind="policy_unverifiable",
                    )
                policy_enforced = True

            # Order claims by created_at so prev_hash reconstruction
            # matches the original chain. SHA256 is deterministic, same
            # inputs in the same order produce the same chain.
            ordered_claims = sorted(
                claims_section.items(),
                key=lambda kv: kv[1].get("created_at", ""),
            )

            # The [rekor_inclusions] sidecar is the authoritative record of
            # which claims the transparency log witnessed: each entry carries
            # the log's own inclusion proof (verified below when a log pubkey
            # is pinned). transparency_logged is derived from it per claim.
            rekor_sidecar = data.get("rekor_inclusions") or {}

            for claim_id, c in ordered_claims:
                ctx_c = f"Claim {claim_id}"
                # Pull required fields up-front via the helper so any
                # missing key surfaces as RestoreError(kind="toml_malformed")
                # instead of a bare KeyError past the contract.
                c_text = _required_field(c, "text", ctx_c)
                c_classification = _required_field(c, "classification", ctx_c)
                c_generated_by = _required_field(c, "generated_by", ctx_c)
                c_created_at = _required_field(c, "created_at", ctx_c)
                c_updated_at = _required_field(c, "updated_at", ctx_c)
                c_status = _required_field(c, "status", ctx_c)
                target_level = _required_field(c, "support_level", ctx_c)
                _verify_claim_signatures_on_restore(
                    conn, claim_id, c, validators_section, signed_mode,
                    _signing,
                )
                # Reconstruct supports/contradicts JSON.
                supports_list = c.get("supports", []) or []
                contradicts_list = c.get("contradicts", []) or []
                # Evidence-vector round-trip. The TOML carries the
                # canonical JSON; we re-derive ev_* + chain_input from
                # it so the chain_hash matches the original.
                # An unparseable vector is a refusal, not a zeroed row: the
                # signed path already treats it that way, and swallowing it
                # here restored a claim whose ev_* columns contradict the
                # evidence_json blob written back beside them.
                evidence_json_str = c.get("evidence_json") or "{}"
                try:
                    evidence_dict = json.loads(evidence_json_str)
                except (ValueError, TypeError) as exc:
                    raise RestoreError(
                        f"Claim {claim_id} evidence_json is malformed.",
                        kind="toml_malformed",
                    ) from exc
                if not isinstance(evidence_dict, dict):
                    raise RestoreError(
                        f"Claim {claim_id} evidence_json must be a JSON "
                        f"object (got {type(evidence_dict).__name__}); "
                        "claims.toml is malformed.",
                        kind="claim_unverified",
                    )
                # Observed grounding verdict (optional/versioned). Parse the
                # stored JSON record so the chain hash and statement_cid rebuild
                # from the same bytes the original signing path bound. Absent for
                # every pre-observer claim, keeping those chain links identical.
                observed_grounding = _parse_observed_grounding(
                    c.get("observed_grounding")
                )
                chain_fields = {
                    "claim_id": claim_id,
                    "text": c_text,
                    "classification": c_classification,
                    "generated_by": c_generated_by,
                    "supports": supports_list,
                    "contradicts": contradicts_list,
                    "source_name": c.get("source_name"),
                    "artifact_hash": c.get("artifact_hash"),
                    "created_at": c_created_at,
                }
                if observed_grounding is not None:
                    chain_fields["observed_grounding"] = observed_grounding
                prev_hash = _compute_prev_hash(
                    conn, chain_fields, evidence_dict,
                )
                val_sig = c.get("validation_signature")
                validator_keyid = (
                    _extract_validation_signer_keyid(val_sig)
                    if val_sig else None
                )
                # The INSERT trigger only accepts PRELIMINARY or
                # ESTABLISHED as initial values, REPLICATED is reached
                # via the convergence detection path inside add_claim,
                # never as a born state. Restore inserts REPLICATED rows
                # as PRELIMINARY first, then UPDATEs into REPLICATED.
                # The UPDATE trigger accepts PRELIMINARY → REPLICATED.
                insert_level = (
                    "PRELIMINARY" if target_level == "REPLICATED"
                    else target_level
                )
                # ESTABLISHED rows born here carry validation_signature
                # (the CHECK constraint and the INSERT trigger both
                # require it). PRELIMINARY-during-promotion rows must
                # NOT carry validated_by / validated_at, the INSERT
                # trigger refuses that combination. We hold those
                # back to the UPDATE phase below for REPLICATED.
                insert_validated_by = (
                    c.get("validated_by") if insert_level == "ESTABLISHED"
                    else None
                )
                insert_validated_at = (
                    c.get("validated_at") if insert_level == "ESTABLISHED"
                    else None
                )
                insert_validation_signature = (
                    val_sig if insert_level == "ESTABLISHED" else None
                )
                insert_validator_keyid = (
                    validator_keyid if insert_level == "ESTABLISHED"
                    else None
                )
                # Denormalize ev_* from the canonical evidence_dict so
                # the row's CHECK constraints + the evidence_json blob
                # stay aligned. statement_cid is rebuilt from the same
                # chain_fields + evidence_dict and serves as restore's
                # adversarial anchor, any TOML tamper of an ev_* field
                # produces a different statement_cid here than the one
                # the original signing path computed.
                from mareforma import _statement as _stmt_mod
                statement_cid_str = _stmt_mod.statement_cid(
                    _stmt_mod.build_statement(
                        claim_id=claim_id,
                        text=c_text,
                        classification=c_classification,
                        generated_by=c_generated_by,
                        supports=supports_list,
                        contradicts=contradicts_list,
                        source_name=c.get("source_name"),
                        artifact_hash=c.get("artifact_hash"),
                        created_at=c_created_at,
                        evidence=evidence_dict,
                        observed_grounding=observed_grounding,
                    )
                ) if c.get("signature_bundle") else None
                # transparency_logged gates convergence eligibility. Anchor it
                # to the [rekor_inclusions] sidecar, not the `rekor` block inside
                # signature_bundle: that block is attached after signing and the
                # claim signature does not cover it, so a lone bundle edit must
                # not confer witnessed state. A claim whose bundle asserts a
                # rekor uuid counts as witnessed only when a matching sidecar
                # entry is present (and, when a log pubkey is pinned, that entry
                # is cryptographically verified below). A claim with no rekor
                # coords keeps the honest default: ready unless the TOML recorded
                # it pending. This preserves the non-Rekor case (the flag
                # defaults to 1 and the backup omits it, so absence restores as
                # 1) and fails closed when the sidecar cannot corroborate a
                # witnessed claim (the operator re-establishes it via
                # refresh_unsigned). Full anti-forgery of witnessed state relies
                # on a pinned log pubkey; without one the sidecar is replayed
                # unverified, matching the submit-path opt-in posture.
                bundle_rekor_uuid = None
                if c.get("signature_bundle"):
                    try:
                        _env = json.loads(c["signature_bundle"])
                        _rekor = _env.get("rekor") or {}
                        bundle_rekor_uuid = _rekor.get("uuid") or None
                    except (json.JSONDecodeError, AttributeError, TypeError):
                        bundle_rekor_uuid = None
                if policy_enforced and c.get("signature_bundle"):
                    # Under an enforced witnessing policy a signed claim is
                    # convergence-eligible only with a sidecar entry, which the
                    # replay loop below verifies (Merkle) and binds to the claim
                    # or aborts the whole restore. No entry, no eligibility , 
                    # stripping the bundle rekor block cannot buy readiness.
                    resolved_transparency = 1 if claim_id in rekor_sidecar else 0
                elif bundle_rekor_uuid is not None:
                    _sidecar = rekor_sidecar.get(claim_id) or {}
                    resolved_transparency = (
                        1 if _sidecar.get("uuid") == bundle_rekor_uuid else 0
                    )
                elif policy_rekor_required:
                    # The project's signed policy requires witnessing. A claim
                    # with no rekor coords and no verified sidecar entry is not
                    # witnessed, so it restores pending regardless of the TOML
                    # flag: stripping a `transparency_logged = false` line
                    # cannot buy convergence-eligibility even when the operator
                    # did not pass enforce_rekor_policy. (A genuinely witnessed
                    # claim carries rekor coords and is resolved above.)
                    resolved_transparency = 0
                else:
                    resolved_transparency = (
                        0 if c.get("transparency_logged") is False else 1
                    )
                try:
                    conn.execute(
                        """
                        INSERT INTO claims
                            (claim_id, text, classification, support_level,
                             idempotency_key, validated_by, validated_at,
                             status, source_name, generated_by,
                             supports_json, contradicts_json,
                             comparison_summary, unresolved,
                             signature_bundle, transparency_logged,
                             validation_signature, validator_keyid,
                             asserter_keyid,
                             artifact_hash, prev_hash,
                             ev_risk_of_bias, ev_inconsistency,
                             ev_indirectness, ev_imprecision, ev_pub_bias,
                             evidence_json, statement_cid,
                             convergence_retry_needed,
                             predicate_payload, original_signature_bundle,
                             observed_grounding,
                             created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            claim_id, c_text, c_classification,
                            insert_level,
                            c.get("idempotency_key"),
                            insert_validated_by, insert_validated_at,
                            c_status, c.get("source_name"),
                            c_generated_by,
                            json.dumps(supports_list, sort_keys=True,
                                       separators=(",", ":")),
                            json.dumps(contradicts_list, sort_keys=True,
                                       separators=(",", ":")),
                            c.get("comparison_summary") or "",
                            1 if c.get("unresolved") else 0,
                            c.get("signature_bundle"),
                            resolved_transparency,
                            insert_validation_signature,
                            insert_validator_keyid,
                            # Re-derive the asserter keyid from the preserved
                            # bundle so the denormalization stays in sync with
                            # the authoritative signature_bundle after restore.
                            _extract_signature_bundle_keyid(
                                c.get("signature_bundle")
                            ),
                            c.get("artifact_hash"), prev_hash,
                            _restore_evidence_domain(
                                evidence_dict, "risk_of_bias", claim_id,
                            ),
                            _restore_evidence_domain(
                                evidence_dict, "inconsistency", claim_id,
                            ),
                            _restore_evidence_domain(
                                evidence_dict, "indirectness", claim_id,
                            ),
                            _restore_evidence_domain(
                                evidence_dict, "imprecision", claim_id,
                            ),
                            _restore_evidence_domain(
                                evidence_dict, "publication_bias", claim_id,
                            ),
                            evidence_json_str,
                            statement_cid_str,
                            1 if c.get("convergence_retry_needed") else 0,
                            _restore_predicate_payload(c, claim_id),
                            _restore_original_signature_bundle(c, claim_id),
                            _serialize_observed_grounding(observed_grounding),
                            c_created_at, c_updated_at,
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    # Trigger refusals (illegal initial support_level,
                    # ESTABLISHED without validation_signature) and CHECK
                    # violations (bad classification / support_level /
                    # status enum, duplicate prev_hash) all surface here.
                    # Translate to RestoreError so callers honour the
                    # documented contract.
                    raise RestoreError(
                        f"Claim {claim_id} could not be restored: {exc}",
                        kind="claim_unverified",
                    ) from exc
                if target_level == "REPLICATED":
                    # PRELIMINARY → REPLICATED, the UPDATE trigger
                    # accepts the transition. No validation_signature
                    # required on REPLICATED rows. Wrap the UPDATE so
                    # any trigger refusal surfaces as RestoreError.
                    try:
                        conn.execute(
                            "UPDATE claims SET support_level = 'REPLICATED' "
                            "WHERE claim_id = ?",
                            (claim_id,),
                        )
                    except sqlite3.IntegrityError as exc:
                        raise RestoreError(
                            f"Claim {claim_id} promote-to-REPLICATED "
                            f"refused: {exc}",
                            kind="claim_unverified",
                        ) from exc

            # Verdict-table replay. Each verdict envelope carries its
            # own signature binding; we verify before INSERT. The
            # contradiction trigger fires on the contradiction INSERT
            # and re-derives t_invalid, restore doesn't need to
            # round-trip t_invalid separately.
            #
            # Sort by created_at before replay so the contradiction
            # trigger (WHERE t_invalid IS NULL) sets t_invalid to the
            # earliest contradiction's timestamp, preserving the
            # truthful first-invalidation moment. Without sorting,
            # tomli's insertion-order iteration lets a hand-edited
            # TOML reorder contradictions to backdate or postdate the
            # invalidation timestamp.
            rep_section = data.get("replication_verdicts") or {}
            rep_ordered = sorted(
                rep_section.items(),
                key=lambda kv: kv[1].get("created_at") or "",
            )
            for verdict_id, v in rep_ordered:
                _verify_and_insert_replication_verdict(
                    conn, verdict_id, v, validators_section,
                )
            con_section = data.get("contradiction_verdicts") or {}
            con_ordered = sorted(
                con_section.items(),
                key=lambda kv: kv[1].get("created_at") or "",
            )
            for verdict_id, v in con_ordered:
                _verify_and_insert_contradiction_verdict(
                    conn, verdict_id, v, validators_section,
                )

            # Rekor inclusion sidecar. Replay entries so post-restore
            # graphs carry the same Rekor proof data as the original.
            # When rekor_log_pubkey_pem was supplied at open(), verify
            # each entry's inclusion proof against the pinned key.
            rekor_section = data.get("rekor_inclusions") or {}
            has_rekor_section = "rekor_inclusions" in data
            rekor_logged_claim_ids = set()
            for cid, c in ordered_claims:
                bundle_str = c.get("signature_bundle")
                if bundle_str:
                    try:
                        bundle = json.loads(bundle_str)
                        if bundle.get("rekor"):
                            rekor_logged_claim_ids.add(cid)
                    except (ValueError, TypeError):
                        pass

            if not has_rekor_section and rekor_logged_claim_ids:
                from .errors import RekorSidecarSectionAbsentWarning
                warnings.warn(
                    f"claims.toml has no [rekor_inclusions] section but "
                    f"{len(rekor_logged_claim_ids)} claim(s) have Rekor "
                    "coords in their signature_bundle. This is expected "
                    "when restoring from a pre-v0.3.2 TOML. Those claims "
                    "restore as not-yet-witnessed (transparency_logged=0) "
                    "and stay out of convergence until re-established. Run "
                    "refresh_unsigned() to re-fetch inclusion proofs.",
                    RekorSidecarSectionAbsentWarning,
                    stacklevel=2,
                )

            for cid, entry in rekor_section.items():
                if cid not in claims_section:
                    raise RestoreError(
                        f"rekor_inclusions entry references claim_id "
                        f"{cid!r} which is not in the [claims] section",
                        kind="rekor_inclusion_invalid",
                    )
                r_uuid = entry.get("uuid")
                r_log_index = entry.get("log_index")
                r_raw = entry.get("raw_response_b64")
                r_itime = entry.get("integrated_time")
                r_recorded = entry.get("recorded_at")
                # Reject an empty raw_response too, not just a missing one: an
                # empty body would slip past verification below (the pinned-key
                # block is guarded on a truthy raw) while the claim still counts
                # as witnessed by presence, forging convergence-eligible state.
                # log_index and recorded_at are NOT NULL in the sidecar table.
                # Left to the INSERT they would be dropped without a word while
                # the claim already counts as witnessed by presence, leaving the
                # recovered graph disagreeing with itself and no way back: the
                # refresh_unsigned retry only revisits transparency_logged=0.
                if (
                    not r_uuid or not r_raw
                    or not isinstance(r_log_index, int) or not r_recorded
                ):
                    raise RestoreError(
                        f"rekor_inclusions entry for {cid!r} is missing "
                        "required fields (uuid, raw_response_b64, "
                        "log_index as an integer, recorded_at)",
                        kind="rekor_inclusion_invalid",
                    )
                if rekor_log_pubkey_pem is not None and r_raw:
                    try:
                        raw_json = base64.standard_b64decode(r_raw).decode("utf-8")
                        rekor_body = json.loads(raw_json)
                        entry_val = next(iter(rekor_body.values())) if isinstance(rekor_body, dict) and rekor_body else rekor_body
                    except Exception as exc:
                        raise RestoreError(
                            f"Rekor inclusion entry for claim {cid!r} is "
                            f"unparseable: {exc}",
                            kind="rekor_inclusion_invalid",
                        ) from exc
                    # A row recorded without a pinned log key holds the entry
                    # coordinates only, no proof to check. Name that instead of
                    # falling through to the binding refusal, which would blame
                    # the backup for a gap in how the row was written.
                    if not isinstance(entry_val, dict) or not entry_val.get("body"):
                        raise RestoreError(
                            f"Rekor inclusion entry for claim {cid!r} carries no "
                            "inclusion proof; it was recorded without a pinned "
                            "log key, so there is nothing to verify. Restore "
                            "without rekor_log_pubkey_pem to accept the sidecar "
                            "coordinates unverified.",
                            kind="rekor_inclusion_invalid",
                        )
                    from mareforma.signing import (
                        rekor_entry_binds_to_envelope, verify_rekor_inclusion,
                    )
                    # Bind the entry to THIS claim before trusting its proof: a
                    # valid inclusion proof only shows its body is in the log,
                    # not that the body is about this claim. Without this a real
                    # proof copied from another claim would confer witnessed
                    # state on a row it never covered.
                    envelope = _claim_envelope(claims_section[cid])
                    if envelope is None or not rekor_entry_binds_to_envelope(
                        entry_val, envelope,
                    ):
                        raise RestoreError(
                            f"Rekor inclusion proof for claim {cid!r} does not "
                            "bind to this claim's signed payload; the entry "
                            "belongs to a different claim.",
                            kind="rekor_inclusion_invalid",
                        )
                    try:
                        verify_rekor_inclusion(
                            entry_val, rekor_log_pubkey_pem, envelope,
                        )
                    except Exception as exc:
                        raise RestoreError(
                            f"Rekor inclusion proof verification failed for "
                            f"claim {cid!r}: {exc}",
                            kind="rekor_inclusion_invalid",
                        ) from exc
                try:
                    conn.execute(
                        "INSERT INTO rekor_inclusions "
                        "(claim_id, uuid, log_index, integrated_time, "
                        "raw_response_b64, recorded_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (cid, r_uuid, r_log_index, r_itime, r_raw, r_recorded),
                    )
                except sqlite3.IntegrityError as exc:
                    raise RestoreError(
                        f"rekor_inclusions entry for {cid!r} could not be "
                        f"restored: {exc}",
                        kind="rekor_inclusion_invalid",
                    ) from exc

            if has_rekor_section and rekor_logged_claim_ids:
                from .errors import RekorSidecarEntryMissingWarning
                for cid in rekor_logged_claim_ids:
                    if cid not in rekor_section:
                        warnings.warn(
                            f"Claim {cid[:12]}... has Rekor coords in its "
                            "signature_bundle but no matching entry in "
                            "[rekor_inclusions]. The section exists (not a "
                            "pre-v0.3.2 upgrade), investigate whether the "
                            "entry was removed from claims.toml.",
                            RekorSidecarEntryMissingWarning,
                            stacklevel=2,
                        )

            # Trust layer: replay the finding tree (propositions, predictions,
            # findings, evidence_lines, contrasts, effect_estimates). Runs after
            # the claims loop so findings.claim_id foreign keys resolve. The
            # rows hang off finding attestation claims that restore already
            # verified; the signed model lineage each line carries also lives in
            # the finding claim's observed_grounding, which round-trips on the
            # claims path, so the independence read re-authenticates it as before.
            _restore_trust_tables(conn, data)

            # Resume the supports-edge revision counter the backup recorded.
            _restore_supports_revision(conn, data.get("graph_meta"))

            # Refuse a retirement that names a plan pair its attestation does
            # not. The row decides which rule a proposition's stranded evidence
            # is gated under, and none of its columns is signed.
            _verify_plan_retirement_binding(conn)

            # Refuse a finding attached to a proposition its claim never made.
            # The edge itself is unsigned, so it is re-derived from the claim's
            # signed text, the same posture as the REPLICATED re-derivation below.
            _verify_finding_proposition_binding(conn)

            # Refuse a REPLICATED level no distinct-signer corroboration backs.
            # support_level is not signed, so this runs after the full graph +
            # verdicts are in place and re-derives the promotion invariant from
            # signed material (supports edges + verified asserter identities).
            _verify_replicated_corroboration(conn)

            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            raise

        # Restore inserted many validator rows; drop any per-connection
        # chain-verification cache so the next is_enrolled walk operates
        # against the fresh state. (Restore opens its own connection and
        # closes it on the next line, so this is technically belt-and-
        # suspenders, but the symmetric treatment is the right invariant
        # for any future restore caller that reuses the connection.)
        from mareforma.validators import invalidate_conn_cache
        invalidate_conn_cache(conn)
        # Rebuild the claim_supports cache from the freshly-replayed
        # chain. The cache file lives outside the signed graph; if
        # the rebuild fails (disk full, attached file locked) the
        # main restore has already committed and the next open() will
        # re-detect staleness and rebuild. Surface a warning so the
        # operator knows restore succeeded but the cache is dirty
        # until next open.
        from mareforma import _supports
        try:
            _supports.rebuild_cache(conn)
        except sqlite3.Error as exc:
            warnings.warn(
                "restore: claim_supports cache rebuild failed "
                f"({exc}); the next mareforma.open() will rebuild it. "
                "Restore itself succeeded.",
                RuntimeWarning,
                stacklevel=2,
            )
        return {
            "validators_restored": len(ordered_validators),
            "claims_restored": len(ordered_claims),
        }
    except BaseException:
        # Close first so the files are unlocked, then drop the residue.
        conn.close()
        _discard_created_paths(mareforma_dir, dir_existed, files_existed)
        raise
    finally:
        conn.close()


def _restore_supports_revision(conn: sqlite3.Connection, section: Any) -> None:
    """Resume the supports-edge revision counter from ``[graph_meta]``.

    The counter is not signed material: it decides only whether the
    rebuildable supports cache is trusted or rebuilt. A backup written before
    the counter existed carries no section and leaves the restored graph at 0.
    A present but malformed value is refused rather than silently dropped,
    because restoring at 0 lets the counter climb back through values a
    surviving cache file already stamped, and stale edges would read as fresh.
    """
    if not isinstance(section, dict) or "supports_revision" not in section:
        return
    revision = section["supports_revision"]
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise RestoreError(
            "claims.toml field [graph_meta].supports_revision must be a "
            f"non-negative integer, got {revision!r}.",
            kind="toml_malformed",
        )
    from mareforma import _supports
    _supports.set_supports_revision(conn, revision)


def _restore_trust_tables(conn: sqlite3.Connection, data: dict) -> None:
    """Replay the trust-layer finding tree from claims.toml.

    Walks ``_TRUST_TABLE_BACKUP`` in foreign-key order (parents before children)
    and inserts each populated section. NULL-valued columns were omitted from the
    backup, so each column reads with a NULL default. The section shapes are
    validated first (restore's tamper threat model), and foreign-key or CHECK
    violations translate to RestoreError with kind='trust_row_rejected' rather
    than a raw IntegrityError. That kind is deliberately not 'claim_unverified':
    no signature was checked here, and telling an operator mid-recovery that
    their signed material failed verification points at no fix. None
    of these columns is signed: the finding -> proposition edge is re-derived
    from signed material afterwards by
    :func:`_verify_finding_proposition_binding`, and the model axis is read on
    the finding claim's signed ``observed_grounding``, which round-trips on the
    claims path, whether or not the replayed column survived: an edited backup
    can drop a ``model_lineage`` entry, and the count must not move.
    """
    for section, table, pk, cols in _TRUST_TABLE_BACKUP:
        section_data = data.get(section)
        if not section_data:
            continue
        _validate_section_shape(section_data, section)
        all_cols = (pk, *cols)
        placeholders = ", ".join("?" * len(all_cols))
        insert_sql = (
            f"INSERT INTO {table} ({', '.join(all_cols)}) "
            f"VALUES ({placeholders})"
        )
        for pk_value, entry in section_data.items():
            values = [pk_value, *(entry.get(col) for col in cols)]
            try:
                conn.execute(insert_sql, values)
            except sqlite3.IntegrityError as exc:
                raise RestoreError(
                    f"Trust-layer row {pk_value!r} in [{section}] could not be "
                    f"restored: {exc}. The row breaks a schema constraint, not "
                    f"a signature: correct or remove it in claims.toml and run "
                    f"restore again.",
                    kind="trust_row_rejected",
                ) from exc


def _verify_plan_retirement_binding(conn: sqlite3.Connection) -> None:
    """Refuse a restored retirement its attestation does not name.

    A retirement moves the evidence under an un-runnable plan onto the plan that
    supersedes it, and every column of the row is unsigned, so a tampered
    claims.toml could point one somewhere else or rewrite why it was retired.
    The triple is re-derivable from signed material: the retirement attestation's
    ``text`` renders plan, replacement and reason (see
    :func:`mareforma.trust._store.retirement_claim_text`), and restore verified
    that text's signature on the claims path. A row whose claim does not render
    it fails the whole restore (kind='claim_unverified').
    """
    from mareforma.trust import _store

    rows = conn.execute(
        "SELECT r.plan_id, r.superseded_by, r.reason, c.text AS claim_text "
        "FROM plan_retirements r JOIN claims c ON c.claim_id = r.claim_id"
    ).fetchall()
    for r in rows:
        expected = _store.retirement_claim_text(
            r["plan_id"], r["superseded_by"], r["reason"],
        )
        if expected != r["claim_text"]:
            raise RestoreError(
                f"The retirement of plan {r['plan_id'][:12]}… names a "
                "replacement or a reason its attestation claim does not; the "
                "retirement in claims.toml was rewritten.",
                kind="claim_unverified",
            )


def _verify_finding_proposition_binding(conn: sqlite3.Connection) -> None:
    """Refuse a restored finding attached to a proposition its claim never made.

    ``findings.content_id`` is not a signed field, so a tampered claims.toml can
    re-point a genuinely signed finding at a proposition it says nothing about
    and inflate that proposition's independence count. The edge is still
    re-derivable from signed material: a finding claim's ``text`` is the
    rendering of the proposition it attests. A finding whose proposition does
    not render its own claim text is a rewritten edge and fails the whole
    restore (kind='claim_unverified').
    """
    from mareforma.trust import Proposition

    rows = conn.execute(
        "SELECT f.finding_id, c.text AS claim_text, p.subject, p.relation, "
        " p.object, p.direction, p.scope_json, p.magnitude "
        "FROM findings f "
        "JOIN claims c ON c.claim_id = f.claim_id "
        "JOIN propositions p ON p.content_id = f.content_id"
    ).fetchall()
    for r in rows:
        try:
            proposition = Proposition.from_dict({
                "subject": r["subject"],
                "relation": r["relation"],
                "object": r["object"],
                "direction": r["direction"],
                "scope": json.loads(r["scope_json"] or "{}"),
                "magnitude": r["magnitude"],
            })
            expected = _validate_claim_text(proposition.text())
        except (ValueError, TypeError) as exc:
            raise RestoreError(
                f"The proposition finding {r['finding_id'][:12]}… points at "
                f"cannot be read back: {exc}",
                kind="claim_unverified",
            ) from exc
        if expected != r["claim_text"]:
            raise RestoreError(
                f"Finding {r['finding_id'][:12]}… points at a proposition its "
                "attestation claim does not attest; the finding edge in "
                "claims.toml was rewritten.",
                kind="claim_unverified",
            )


def _verify_replicated_corroboration(conn: sqlite3.Connection) -> None:
    """Refuse a restored promotion no signed evidence backs.

    ``support_level`` is not a signed field, so a tampered claims.toml can flip
    a lone PRELIMINARY claim to REPLICATED while its signature still verifies.
    :class:`_CorroborationIndex` re-derives the rung from signed material, the
    same rule the live read path applies before it serves a row; here an
    unbacked row fails the whole restore rather than degrading one read.

    A project whose root-signed policy requires strict promotion is held to it
    here too: a claim created after that declaration must carry data, and so
    must the peer backing it. Claims created before the declaration keep their
    level, the policy is not retroactive, and both timestamps are signed so the
    grandfathering window cannot be widened, neither by editing the backup nor
    by declaring a second, unrelated rule later.
    """
    rows = conn.execute(
        "SELECT claim_id, support_level, asserter_keyid, supports_json, "
        "artifact_hash, observed_grounding, transparency_logged, "
        "created_at, validation_signature FROM claims "
        "WHERE support_level IN ('REPLICATED', 'ESTABLISHED')"
    ).fetchall()
    if not rows:
        return
    index = _CorroborationIndex(conn, {})
    for r in rows:
        failure = index.failure(r)
        if failure is None:
            continue
        if failure == "strict_promotion_without_data":
            raise RestoreError(
                f"Claim {r['claim_id']} is stored as {r['support_level']} with "
                "no artifact_hash, which this project's root-signed "
                "strict-promotion policy forbids for a claim created after "
                "the declaration.",
                kind="policy_violation",
            )
        raise RestoreError(
            f"Claim {r['claim_id']} is stored as {r['support_level']} but no "
            "distinct-signer corroboration on a shared ESTABLISHED anchor "
            "backs the REPLICATED rung it stands on: a peer "
            "must carry a different artifact hash, and neither side may "
            "carry a non-promoting grounding verdict. A replication verdict "
            "naming the claim does not settle it either: a verdict names "
            "every member of its cluster and promotes only the qualifying "
            "ones. The support level is not a signed field; this one is "
            "unverifiable and the backup may be tampered.",
            kind="claim_unverified",
        )


def _gate_replayed_verdict_issuer(
    conn: sqlite3.Connection,
    issuer_keyid: str,
    claims: tuple[tuple[str, str], ...],
    *,
    verdict_kind: str,
    ctx: str,
    refuse_llm_issuer: bool = False,
) -> None:
    """Hold a replayed verdict to the issuer-identity gates the live path runs.

    *claims* pairs each referenced claim_id with its relation name. A verified
    signature proves the issuer signed the verdict; these gates decide whether
    that issuer was entitled to. ``refuse_llm_issuer`` adds the llm ceiling the
    contradiction path carries, since a contradiction invalidates the older
    claim through the insert trigger. Both refusals become RestoreError, the
    failure mode restore's callers handle.
    """
    try:
        if refuse_llm_issuer:
            _refuse_llm_contradiction_issuer(conn, issuer_keyid)
        for claim_id, relation in claims:
            _refuse_self_verdict(
                conn, issuer_keyid, claim_id,
                relation=relation, verdict_kind=verdict_kind,
            )
    except (LLMValidatorPromotionError, VerdictIssuerError) as exc:
        raise RestoreError(
            f"{ctx} was issued by a key the live path refuses: {exc}",
            kind="claim_unverified",
        ) from exc


def _verify_and_insert_replication_verdict(
    conn: sqlite3.Connection,
    verdict_id: str,
    v: dict,
    validators_section: dict,
) -> None:
    """Cryptographically verify + INSERT a replication verdict from TOML.

    The signed payload binds (verdict_id, cluster_id, member_claim_id,
    other_claim_id, method, confidence) under DSSE PAE with
    payloadType ``application/vnd.mareforma.replication-verdict+json``.
    The issuer_keyid is looked up in the restored validators_section;
    forged keyids without a matching enrollment fail verification.

    A verified signature says who signed the verdict, not that they were
    entitled to issue it, so the self-verdict gate the live path runs is
    applied here too, in the same order: enrollment, signature, identity.
    """
    from mareforma import signing as _signing

    ctx = f"Replication verdict {verdict_id}"
    cluster_id = _required_field(v, "cluster_id", ctx)
    member_claim_id = _required_field(v, "member_claim_id", ctx)
    other_claim_id = v.get("other_claim_id")
    method = _required_field(v, "method", ctx)
    confidence_json = _required_field(v, "confidence_json", ctx)
    issuer_keyid = _required_field(v, "issuer_keyid", ctx)
    signature_b64 = _required_field(v, "signature", ctx)
    created_at = _required_field(v, "created_at", ctx)

    try:
        signature_bytes = base64.b64decode(signature_b64)
    except (ValueError, TypeError) as exc:
        raise RestoreError(
            f"{ctx} signature is not valid base64.",
            kind="claim_unverified",
        ) from exc

    enrollment = validators_section.get(issuer_keyid)
    if enrollment is None:
        raise RestoreError(
            f"{ctx} issuer_keyid {issuer_keyid!r} is not in the validators "
            "section, verdict signer is not enrolled.",
            kind="claim_unverified",
        )
    try:
        pem_bytes = base64.standard_b64decode(enrollment["pubkey_pem"])
        pubkey = _signing.public_key_from_pem(pem_bytes)
    except (KeyError, ValueError, TypeError, _signing.SigningError) as exc:
        raise RestoreError(
            f"{ctx} validator PEM unparseable: {exc}",
            kind="claim_unverified",
        ) from exc

    try:
        confidence_dict = json.loads(confidence_json or "{}")
    except (ValueError, TypeError) as exc:
        raise RestoreError(
            f"{ctx} confidence_json unparseable: {exc}",
            kind="claim_unverified",
        ) from exc

    record = {
        "verdict_id": verdict_id,
        "cluster_id": cluster_id,
        "member_claim_id": member_claim_id,
        "other_claim_id": other_claim_id,
        "method": method,
        "confidence": confidence_dict,
    }
    payload = _verdict_canonical_payload(_REPLICATION_VERDICT_FIELDS, record)
    pae = _signing.dsse_pae(
        "application/vnd.mareforma.replication-verdict+json", payload,
    )
    from cryptography.exceptions import InvalidSignature
    try:
        pubkey.verify(signature_bytes, pae)
    except InvalidSignature as exc:
        raise RestoreError(
            f"{ctx} signature verification failed, TOML tampered or "
            "signature forged.",
            kind="claim_unverified",
        ) from exc

    referenced = ((member_claim_id, "member_claim_id"),)
    if other_claim_id is not None:
        referenced += ((other_claim_id, "other_claim_id"),)
    _gate_replayed_verdict_issuer(
        conn, issuer_keyid, referenced,
        verdict_kind="replication", ctx=ctx,
    )

    try:
        conn.execute(
            """
            INSERT INTO replication_verdicts(
                verdict_id, cluster_id, member_claim_id, other_claim_id,
                method, confidence_json, issuer_keyid, signature, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict_id, cluster_id, member_claim_id, other_claim_id,
                method, confidence_json, issuer_keyid, signature_bytes,
                created_at,
            ),
        )
    except sqlite3.IntegrityError as exc:
        raise RestoreError(
            f"{ctx} INSERT refused: {exc}",
            kind="claim_unverified",
        ) from exc


def _verify_and_insert_contradiction_verdict(
    conn: sqlite3.Connection,
    verdict_id: str,
    v: dict,
    validators_section: dict,
) -> None:
    """Cryptographically verify + INSERT a contradiction verdict from TOML.

    Same shape as the replication verdict path, plus the llm-issuer ceiling
    the live path adds here. The ``contradiction_invalidates_older`` trigger
    fires on this INSERT and re-derives ``claims.t_invalid`` automatically,
    so an ungated replay would demote a claim through the backup.
    """
    from mareforma import signing as _signing

    ctx = f"Contradiction verdict {verdict_id}"
    member_claim_id = _required_field(v, "member_claim_id", ctx)
    other_claim_id = _required_field(v, "other_claim_id", ctx)
    confidence_json = _required_field(v, "confidence_json", ctx)
    issuer_keyid = _required_field(v, "issuer_keyid", ctx)
    signature_b64 = _required_field(v, "signature", ctx)
    created_at = _required_field(v, "created_at", ctx)

    try:
        signature_bytes = base64.b64decode(signature_b64)
    except (ValueError, TypeError) as exc:
        raise RestoreError(
            f"{ctx} signature is not valid base64.",
            kind="claim_unverified",
        ) from exc

    enrollment = validators_section.get(issuer_keyid)
    if enrollment is None:
        raise RestoreError(
            f"{ctx} issuer_keyid {issuer_keyid!r} is not in the validators "
            "section, verdict signer is not enrolled.",
            kind="claim_unverified",
        )
    try:
        pem_bytes = base64.standard_b64decode(enrollment["pubkey_pem"])
        pubkey = _signing.public_key_from_pem(pem_bytes)
    except (KeyError, ValueError, TypeError, _signing.SigningError) as exc:
        raise RestoreError(
            f"{ctx} validator PEM unparseable: {exc}",
            kind="claim_unverified",
        ) from exc

    try:
        confidence_dict = json.loads(confidence_json or "{}")
    except (ValueError, TypeError) as exc:
        raise RestoreError(
            f"{ctx} confidence_json unparseable: {exc}",
            kind="claim_unverified",
        ) from exc

    record = {
        "verdict_id": verdict_id,
        "member_claim_id": member_claim_id,
        "other_claim_id": other_claim_id,
        "confidence": confidence_dict,
    }
    payload = _verdict_canonical_payload(_CONTRADICTION_VERDICT_FIELDS, record)
    pae = _signing.dsse_pae(
        "application/vnd.mareforma.contradiction-verdict+json", payload,
    )
    from cryptography.exceptions import InvalidSignature
    try:
        pubkey.verify(signature_bytes, pae)
    except InvalidSignature as exc:
        raise RestoreError(
            f"{ctx} signature verification failed, TOML tampered or "
            "signature forged.",
            kind="claim_unverified",
        ) from exc

    _gate_replayed_verdict_issuer(
        conn, issuer_keyid,
        ((member_claim_id, "member_claim_id"),
         (other_claim_id, "other_claim_id")),
        verdict_kind="contradiction", ctx=ctx, refuse_llm_issuer=True,
    )

    try:
        conn.execute(
            """
            INSERT INTO contradiction_verdicts(
                verdict_id, member_claim_id, other_claim_id,
                confidence_json, issuer_keyid, signature, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict_id, member_claim_id, other_claim_id,
                confidence_json, issuer_keyid, signature_bytes,
                created_at,
            ),
        )
    except sqlite3.IntegrityError as exc:
        raise RestoreError(
            f"{ctx} INSERT refused: {exc}",
            kind="claim_unverified",
        ) from exc


def _verify_and_insert_project_policy(
    conn: sqlite3.Connection,
    policy_section: dict | None,
    validators_section: dict,
    _signing,
) -> dict | None:
    """Verify a ``[project_policy]`` envelope and round-trip it into the graph.

    The policy must be signed by the project's root validator (self-enrolled).
    A present-but-unverifiable policy is tampered signed material and aborts the
    restore. The flat row fields must match the signed payload, under the field
    list of the payload's own version: an envelope written before a flag existed
    verifies under the rules it was signed with, and one written before the
    per-flag declaration times existed cannot have them edited in, since the
    match is checked both ways. Returns the restored policy row as a dict, or
    None when the section is absent.
    """
    if not policy_section:
        return None
    ctx = "Project policy"
    signer_keyid = _required_field(policy_section, "signer_keyid", ctx)
    envelope_json = _required_field(policy_section, "envelope", ctx)
    created_at = _required_field(policy_section, "created_at", ctx)
    rekor_required = bool(policy_section.get("rekor_required"))
    strict_promotion = bool(policy_section.get("strict_promotion_required"))
    rekor_declared_at = policy_section.get("rekor_declared_at")
    strict_declared_at = policy_section.get("strict_promotion_declared_at")

    enrollment = validators_section.get(signer_keyid)
    if enrollment is None:
        raise RestoreError(
            f"{ctx} is signed by keyid {signer_keyid[:12]}… which is not "
            "in the validators section.",
            kind="policy_unverified",
        )
    # The signer must be the project's SINGLE root of trust, matching the
    # write-side check in require_rekor_witnessing. A self-enrolled-but-not-sole
    # root (two trust domains in one TOML) has no single authority to speak for
    # the project, so a policy signed by one of several roots is refused.
    roots = [
        k for k, v in validators_section.items()
        if v.get("enrolled_by_keyid") == k
    ]
    if len(roots) != 1 or signer_keyid != roots[0]:
        raise RestoreError(
            f"{ctx} must be signed by the project's single root validator; "
            f"{signer_keyid[:12]}… is not that root.",
            kind="policy_unverified",
        )
    try:
        env = json.loads(envelope_json)
    except (ValueError, TypeError) as exc:
        raise RestoreError(
            f"{ctx} envelope is not valid JSON.", kind="policy_unverified",
        ) from exc
    try:
        pem = base64.standard_b64decode(enrollment["pubkey_pem"])
        pub = _signing.public_key_from_pem(pem)
    except (KeyError, ValueError, TypeError, _signing.SigningError) as exc:
        raise RestoreError(
            f"{ctx} root pubkey unparseable: {exc}", kind="policy_unverified",
        ) from exc
    try:
        ok = _signing.verify_envelope(
            env, pub,
            expected_payload_type=_signing.PAYLOAD_TYPE_PROJECT_POLICY,
        )
    except _signing.InvalidEnvelopeError as exc:
        raise RestoreError(
            f"{ctx} envelope is structurally invalid: {exc}",
            kind="policy_unverified",
        ) from exc
    if not ok:
        raise RestoreError(
            f"{ctx} signature failed verification, TOML tampered.",
            kind="policy_unverified",
        )
    # Bind the flat row to the signed payload so a tampered cache is caught.
    # The payload's own version fixes which fields it is allowed to carry, so
    # a v1 envelope cannot be read as declaring a flag it never signed.
    payload = _signing.envelope_payload(env)
    try:
        signed_fields = _signing._project_policy_fields(
            payload.get("version", 1)
        )
    except _signing.InvalidEnvelopeError as exc:
        raise RestoreError(f"{ctx}: {exc}", kind="policy_unverified") from exc
    if set(payload) != set(signed_fields):
        raise RestoreError(
            f"{ctx} signed payload does not carry the fields its version "
            "declares, TOML tampered.",
            kind="policy_unverified",
        )
    if (
        bool(payload.get("rekor_required")) != rekor_required
        or bool(payload.get("strict_promotion_required")) != strict_promotion
        or payload.get("created_at") != created_at
        or payload.get("rekor_declared_at") != rekor_declared_at
        or payload.get("strict_promotion_declared_at") != strict_declared_at
    ):
        raise RestoreError(
            f"{ctx} row fields do not match the signed envelope, "
            "TOML tampered.",
            kind="policy_unverified",
        )
    conn.execute(
        "INSERT INTO project_policy "
        "(id, rekor_required, strict_promotion_required, signer_keyid, "
        "envelope, created_at, rekor_declared_at, "
        "strict_promotion_declared_at) VALUES (1, ?, ?, ?, ?, ?, ?, ?)",
        (
            1 if rekor_required else 0,
            1 if strict_promotion else 0,
            signer_keyid, envelope_json, created_at,
            rekor_declared_at, strict_declared_at,
        ),
    )
    return {
        "rekor_required": rekor_required,
        "strict_promotion_required": strict_promotion,
        "created_at": created_at,
        "rekor_declared_at": rekor_declared_at,
        "strict_promotion_declared_at": strict_declared_at,
    }


def _validate_section_shape(
    section: Any, name: str, *, allow_scalar_entries: bool = False,
) -> None:
    """Confirm a claims.toml section is a table of tables before iteration.

    An absent section (None) is fine. A present section must be a table whose
    every entry is itself a table, since the restore sort helpers call
    ``.items()`` on the section and ``.get()`` on each entry. A scalar in place
    of either would leak a raw ``AttributeError`` past the documented
    ``RestoreError`` contract, so name the offending section/entry and raise
    ``RestoreError(kind='toml_malformed')`` instead.

    ``allow_scalar_entries`` covers a table of scalars such as
    ``[project_policy]``, whose values are fields rather than rows: the section
    itself is still checked, the per-entry check is skipped.
    """
    if section is None:
        return
    if not isinstance(section, dict):
        raise RestoreError(
            f"claims.toml section [{name}] must be a table, got "
            f"{type(section).__name__}.",
            kind="toml_malformed",
        )
    if allow_scalar_entries:
        return
    for entry_key, entry_val in section.items():
        if not isinstance(entry_val, dict):
            raise RestoreError(
                f"claims.toml entry [{name}.{entry_key}] must be a table, got "
                f"{type(entry_val).__name__}.",
                kind="toml_malformed",
            )


def _required_field(d: dict, key: str, context: str) -> Any:
    """Look up a required field on a TOML-deserialized row.

    Raises :class:`RestoreError` with ``kind='toml_malformed'`` when the
    field is missing. Direct ``d[key]`` would raise ``KeyError`` past
    the documented ``RestoreError`` contract.
    """
    if key not in d:
        raise RestoreError(
            f"{context}: required field {key!r} is missing from "
            "claims.toml.",
            kind="toml_malformed",
        )
    return d[key]


def _verify_claim_signatures_on_restore(
    conn: sqlite3.Connection,
    claim_id: str,
    c: dict,
    validators_section: dict,
    signed_mode: bool,
    _signing,
) -> None:
    """Verify a single claim's signatures during restore.

    Raises :class:`RestoreError` with the appropriate ``kind`` on
    any of: orphan signer keyid, signature_bundle verification
    failure, validation_signature verification failure, or
    mixed-mode (signed-mode graph with an unsigned claim that
    isn't a benign PRELIMINARY-from-pre-signing-era row).
    """
    sig_bundle_json = c.get("signature_bundle")
    if sig_bundle_json:
        try:
            bundle = json.loads(sig_bundle_json)
            all_sigs = bundle["signatures"]
            if not isinstance(all_sigs, list) or not all_sigs:
                raise ValueError("empty or non-list signatures")
            bundle_keyid = all_sigs[0]["keyid"]
        except (
            json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError,
        ) as exc:
            raise RestoreError(
                f"Claim {claim_id} signature_bundle is malformed.",
                kind="claim_unverified",
            ) from exc
        if bundle_keyid not in validators_section:
            raise RestoreError(
                f"Claim {claim_id} is signed by keyid "
                f"{bundle_keyid[:12]}… which is not in the validators "
                "section. Restore refuses orphan signers.",
                kind="orphan_signer",
            )
        try:
            signer_pem = base64.standard_b64decode(
                validators_section[bundle_keyid]["pubkey_pem"],
            )
            signer_pub = _signing.public_key_from_pem(signer_pem)
        except (ValueError, TypeError, _signing.SigningError) as exc:
            raise RestoreError(
                f"Signer pubkey for keyid {bundle_keyid[:12]}… is not "
                "a valid PEM.",
                kind="claim_unverified",
            ) from exc
        # verify_envelope returns False on signature mismatch but raises
        # InvalidEnvelopeError on payloadType/structural mismatch (e.g.
        # a tampered TOML that swaps a validation envelope into the
        # claim-bundle slot). Wrap both into the documented RestoreError
        # contract so callers don't have to catch SigningError too.
        try:
            envelope_ok = _signing.verify_envelope(bundle, signer_pub)
        except _signing.InvalidEnvelopeError as exc:
            raise RestoreError(
                f"Claim {claim_id} signature_bundle is structurally "
                f"invalid: {exc}",
                kind="claim_unverified",
            ) from exc
        if not envelope_ok:
            raise RestoreError(
                f"Claim {claim_id} signature_bundle failed verification.",
                kind="claim_unverified",
            )
        # Multi-signature envelopes (claim-with-roles:v1) carry N
        # signatures; verify_envelope only checked signatures[0]. Walk
        # the remaining signatures and verify each one individually
        # against its claimed signer's enrolled pubkey. An attacker
        # who attached forged extra signatures would otherwise sneak
        # them past restore and into core-trusted role
        # attestations.
        #
        # Enforce the same role contract sign_claim_with_roles /
        # verify_envelope_multi apply at write/verify time: every
        # signature beyond the asserter MUST carry a role in
        # VALID_CLAIM_ROLES, and roles must be unique across the
        # envelope. Tampered TOML carrying two planner-tagged sigs or
        # a fabricated "superuser" role gets refused here so the
        # downstream query_provenance / unverified-role attestation
        # set stays trustworthy.
        from mareforma.signing import VALID_CLAIM_ROLES as _ROLES
        seen_roles: set[str] = set()
        for extra_sig in all_sigs[1:]:
            if not isinstance(extra_sig, dict):
                raise RestoreError(
                    f"Claim {claim_id} signature entry is not an object.",
                    kind="claim_unverified",
                )
            extra_keyid = extra_sig.get("keyid")
            if not isinstance(extra_keyid, str):
                raise RestoreError(
                    f"Claim {claim_id} signature entry missing keyid.",
                    kind="claim_unverified",
                )
            extra_role = extra_sig.get("role")
            if not isinstance(extra_role, str) or extra_role not in _ROLES:
                raise RestoreError(
                    f"Claim {claim_id} multi-sig entry carries role "
                    f"{extra_role!r} which is not in {_ROLES}.",
                    kind="claim_unverified",
                )
            if extra_role in seen_roles:
                raise RestoreError(
                    f"Claim {claim_id} multi-sig envelope has duplicate "
                    f"role {extra_role!r}; each role may sign at most once.",
                    kind="claim_unverified",
                )
            seen_roles.add(extra_role)
            if extra_keyid not in validators_section:
                raise RestoreError(
                    f"Claim {claim_id} carries an extra signature from "
                    f"keyid {extra_keyid[:12]}… which is not in the "
                    "validators section. Restore refuses orphan signers.",
                    kind="orphan_signer",
                )
            try:
                extra_pem = base64.standard_b64decode(
                    validators_section[extra_keyid]["pubkey_pem"],
                )
                extra_pub = _signing.public_key_from_pem(extra_pem)
                extra_sig_bytes = base64.standard_b64decode(extra_sig["sig"])
                pae = _signing.dsse_pae(
                    _signing.PAYLOAD_TYPE_CLAIM,
                    base64.standard_b64decode(bundle["payload"]),
                )
                extra_pub.verify(extra_sig_bytes, pae)
            except Exception as exc:
                raise RestoreError(
                    f"Claim {claim_id} extra signature from keyid "
                    f"{extra_keyid[:12]}… failed verification: {exc}",
                    kind="claim_unverified",
                ) from exc
        # Defense in depth: every signed-predicate field must equal the
        # claim's restored field. Tampering with the row but reusing a
        # legitimate envelope is caught here. Statement v1 puts these
        # fields one level deeper under ``predicate``.
        try:
            predicate = _signing.claim_predicate_from_envelope(bundle)
        except _signing.InvalidEnvelopeError as exc:
            raise RestoreError(
                f"Claim {claim_id} envelope payload is unparseable.",
                kind="claim_unverified",
            ) from exc
        ctx_c = f"Claim {claim_id}"
        expected = {
            "claim_id": claim_id,
            "text": _required_field(c, "text", ctx_c),
            "classification": _required_field(c, "classification", ctx_c),
            "generated_by": _required_field(c, "generated_by", ctx_c),
            "supports": c.get("supports") or [],
            "contradicts": c.get("contradicts") or [],
            "source_name": c.get("source_name"),
            "artifact_hash": c.get("artifact_hash"),
            "created_at": _required_field(c, "created_at", ctx_c),
        }
        for field in _signing.SIGNED_FIELDS:
            if predicate.get(field) != expected[field]:
                raise RestoreError(
                    f"Claim {claim_id} signed-predicate field {field!r} "
                    "does not match the row, TOML tampered.",
                    kind="claim_unverified",
                )

        # Evidence-vector binding. The predicate carries the canonical
        # evidence dict that was signed; restore the row's TOML
        # evidence_json must round-trip to the same dict. Without this,
        # a TOML editor could flip ``risk_of_bias`` from -2 to 0 (a
        # quality upgrade by tamper) and the SIGNED_FIELDS loop above
        # would not catch it because evidence is not in SIGNED_FIELDS.
        try:
            row_evidence = json.loads(c.get("evidence_json") or "{}")
        except (ValueError, TypeError) as exc:
            raise RestoreError(
                f"Claim {claim_id} evidence_json is malformed.",
                kind="claim_unverified",
            ) from exc
        if predicate.get("evidence") != row_evidence:
            raise RestoreError(
                f"Claim {claim_id} signed evidence vector does not match "
                "evidence_json on the row, TOML tampered.",
                kind="claim_unverified",
            )

        # Observed grounding binding. The optional/versioned field lives inside
        # the signed predicate; the row's observed_grounding column must match
        # it. Absence on both sides is the pre-observer case and passes. A
        # verdict present in the envelope but flipped (or dropped) on the row is
        # caught here, the same posture as the evidence check above. Parse both
        # sides so key ordering does not create a false mismatch.
        row_grounding = _parse_observed_grounding(c.get("observed_grounding"))
        if predicate.get("observed_grounding") != row_grounding:
            raise RestoreError(
                f"Claim {claim_id} signed observed-grounding verdict does not "
                "match the observed_grounding column on the row, TOML tampered.",
                kind="claim_unverified",
            )

        # Re-run the verdict↔citation binding on read. A v0.3.9 record carries the
        # cited set inside the signed statement, so verify-on-read can re-confirm
        # that a stored GROUNDED actually binds to the finding's own citation,
        # rather than trusting the write-time result, a hand-edited-then-re-signed
        # row whose GROUNDED cites data the finding never names is caught here.
        # Pure string comparison over stored normalized identifiers: no realpath,
        # no filesystem, so an honest cross-host claim whose paths do not exist on
        # the verifier is never false-flagged.
        _verify_grounding_binding_on_read(claim_id, row_grounding, predicate)

        # statement_cid cross-check. The row carries the cid the
        # original signing path computed. Restore re-derives the cid
        # from the row's fields + evidence and compares. A bare TOML
        # edit that leaves the bundle in place but flips any predicate
        # field is caught here as a second defense after SIGNED_FIELDS.
        if c.get("statement_cid"):
            from mareforma import _statement as _stmt_mod
            recomputed_cid = _stmt_mod.statement_cid(
                _stmt_mod.build_statement(
                    claim_id=claim_id,
                    text=expected["text"],
                    classification=expected["classification"],
                    generated_by=expected["generated_by"],
                    supports=expected["supports"],
                    contradicts=expected["contradicts"],
                    source_name=expected["source_name"],
                    artifact_hash=expected["artifact_hash"],
                    created_at=expected["created_at"],
                    evidence=row_evidence,
                    observed_grounding=row_grounding,
                )
            )
            if recomputed_cid != c["statement_cid"]:
                raise RestoreError(
                    f"Claim {claim_id} statement_cid mismatch: row stores "
                    f"{c['statement_cid']!r} but re-derived {recomputed_cid!r}. "
                    "TOML tampered.",
                    kind="claim_unverified",
                )
    elif signed_mode:
        raise RestoreError(
            f"Claim {claim_id} has no signature_bundle but the graph "
            "is in signed mode (validators are enrolled). Restore "
            "refuses mixed-mode reconstruction.",
            kind="mode_inconsistent",
        )

    val_sig = c.get("validation_signature")
    if val_sig:
        try:
            val_env = json.loads(val_sig)
            val_keyid = val_env["signatures"][0]["keyid"]
            declared_type = val_env["payloadType"]
        except (json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
            raise RestoreError(
                f"Claim {claim_id} validation_signature is malformed.",
                kind="claim_unverified",
            ) from exc
        # The validation_signature column carries either a validation
        # envelope (REPLICATED→ESTABLISHED promotion) or a seed envelope
        # (born-ESTABLISHED bootstrap). Both are legitimate; pass the
        # declared type back to verify_envelope so a mismatch surfaces
        # any tampering between row and column.
        if declared_type not in (
            _signing.PAYLOAD_TYPE_VALIDATION,
            _signing.PAYLOAD_TYPE_SEED,
        ):
            raise RestoreError(
                f"Claim {claim_id} validation_signature has unexpected "
                f"payloadType {declared_type!r}.",
                kind="claim_unverified",
            )
        if val_keyid not in validators_section:
            raise RestoreError(
                f"Claim {claim_id} validation envelope is signed by "
                f"keyid {val_keyid[:12]}… which is not enrolled.",
                kind="orphan_signer",
            )
        try:
            val_signer_pem = base64.standard_b64decode(
                validators_section[val_keyid]["pubkey_pem"],
            )
            val_signer_pub = _signing.public_key_from_pem(val_signer_pem)
        except (ValueError, TypeError, _signing.SigningError) as exc:
            raise RestoreError(
                f"Validation signer pubkey for keyid {val_keyid[:12]}… "
                "is not a valid PEM.",
                kind="claim_unverified",
            ) from exc
        try:
            val_ok = _signing.verify_envelope(
                val_env, val_signer_pub,
                expected_payload_type=declared_type,
            )
        except _signing.InvalidEnvelopeError as exc:
            raise RestoreError(
                f"Claim {claim_id} validation_signature is structurally "
                f"invalid: {exc}",
                kind="claim_unverified",
            ) from exc
        if not val_ok:
            raise RestoreError(
                f"Claim {claim_id} validation_signature failed "
                "verification.",
                kind="claim_unverified",
            )
        # Cryptographic verify_envelope only proves the validator signed
        # the embedded payload, it does NOT prove the embedded payload
        # is about THIS row. A hand-edited claims.toml could copy a
        # legitimate validation/seed envelope onto a different row;
        # without the field-equality check the row would inherit a
        # forged ESTABLISHED stamp anchored by a real validator
        # signature it never authorized for that claim. Mirror the
        # SIGNED_FIELDS cross-check the signature_bundle branch does.
        try:
            val_payload = _signing.envelope_payload(val_env)
        except _signing.InvalidEnvelopeError as exc:
            raise RestoreError(
                f"Claim {claim_id} validation envelope payload is "
                "unparseable.",
                kind="claim_unverified",
            ) from exc
        if val_payload.get("claim_id") != claim_id:
            raise RestoreError(
                f"Claim {claim_id} validation envelope binds a different "
                f"claim_id ({val_payload.get('claim_id')!r}); TOML "
                "tampered or envelope copy-pasted from another row.",
                kind="claim_unverified",
            )
        if val_payload.get("validator_keyid") != val_keyid:
            raise RestoreError(
                f"Claim {claim_id} validation envelope binds a different "
                "validator_keyid than the signing keyid; TOML tampered.",
                kind="claim_unverified",
            )
        # The promotion gates the live path runs once the signer is known
        # authentic. Both read signed material only, the claim's own
        # signature bundle and the validator's signed enrollment, so a row
        # that fails them here could not have been promoted there. Seed
        # envelopes are exempt: a born-ESTABLISHED claim is attested by its
        # own asserter by design and never climbs the ladder.
        if declared_type == _signing.PAYLOAD_TYPE_VALIDATION:
            try:
                _refuse_llm_validator(conn, val_keyid)
                _refuse_self_validation(claim_id, sig_bundle_json, val_keyid)
            except (LLMValidatorPromotionError, SelfValidationError) as exc:
                raise RestoreError(
                    f"Claim {claim_id} validation envelope fails a promotion "
                    f"gate the live path enforces: {exc}",
                    kind="claim_unverified",
                ) from exc
        # Validation envelopes bind validated_at; seed envelopes bind
        # seeded_at. Both must match the row's validated_at column , 
        # the seed path writes seeded_at INTO validated_at at INSERT
        # time, so the comparison is uniform across envelope types.
        timestamp_field = (
            "validated_at"
            if declared_type == _signing.PAYLOAD_TYPE_VALIDATION
            else "seeded_at"
        )
        if val_payload.get(timestamp_field) != c.get("validated_at"):
            raise RestoreError(
                f"Claim {claim_id} validation envelope timestamp "
                f"({timestamp_field}={val_payload.get(timestamp_field)!r}) "
                f"does not match the row's validated_at "
                f"({c.get('validated_at')!r}); TOML tampered.",
                kind="claim_unverified",
            )
        # evidence_seen verification, only relevant for the
        # PAYLOAD_TYPE_VALIDATION case (seed envelopes don't carry
        # evidence_seen). Every cited claim_id must already exist in
        # the restored graph and predate the validation timestamp.
        # Since claims are inserted in created_at order and validations
        # cite earlier claims, the cited entries should be present by
        # the time this row's validation is checked.
        if declared_type == _signing.PAYLOAD_TYPE_VALIDATION:
            cited = val_payload.get("evidence_seen")
            if cited is None:
                raise RestoreError(
                    f"Claim {claim_id} validation envelope is missing "
                    "the evidence_seen field; current envelopes always "
                    "bind this field (use [] for the no-review case).",
                    kind="claim_unverified",
                )
            if not isinstance(cited, list):
                raise RestoreError(
                    f"Claim {claim_id} validation envelope's "
                    f"evidence_seen is not a list: {cited!r}.",
                    kind="claim_unverified",
                )
            row_validated_at = c.get("validated_at")
            for entry in cited:
                if not isinstance(entry, str) or not _is_claim_id(entry):
                    raise RestoreError(
                        f"Claim {claim_id} evidence_seen entry "
                        f"{entry!r} is not a strict-v4 UUID.",
                        kind="claim_unverified",
                    )
                cited_row = conn.execute(
                    "SELECT created_at FROM claims WHERE claim_id = ?",
                    (entry,),
                ).fetchone()
                if cited_row is None:
                    raise RestoreError(
                        f"Claim {claim_id} evidence_seen cites "
                        f"'{entry}' which does not exist in the "
                        "restored graph.",
                        kind="claim_unverified",
                    )
                if cited_row["created_at"] > row_validated_at:
                    raise RestoreError(
                        f"Claim {claim_id} evidence_seen cites "
                        f"'{entry}' (created_at "
                        f"{cited_row['created_at']!r}) which post-dates "
                        f"the validation (validated_at "
                        f"{row_validated_at!r}).",
                        kind="claim_unverified",
                    )


