"""Restore a graph from claims.toml — catastrophic-loss recovery path.

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

from .errors import RestoreError
from .core import (
    open_db,
    _compute_prev_hash,
    _is_claim_id,
    _extract_validation_signer_keyid,
    _verdict_canonical_payload,
    _REPLICATION_VERDICT_FIELDS,
    _CONTRADICTION_VERDICT_FIELDS,
)


def _restore_predicate_payload(c: dict, claim_id: str) -> str:
    """Coerce restored ``predicate_payload`` per the add_claim write contract.

    ``add_claim`` rejects non-dict / non-string values at write time.
    Restore must be at least as strict; a tampered TOML carrying an int
    or list for this field would otherwise land as ``""`` (silent data
    loss). Either the field is a string (passed through) or absent
    (default empty) — anything else is a malformed TOML and aborts.
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



def restore(
    project_root: Path | str,
    *,
    claims_toml: Path | str | None = None,
) -> dict:
    """Rebuild a fresh graph.db from claims.toml.

    Reverse of :func:`_backup_claims_toml`. Intended for catastrophic-
    loss recovery: ``graph.db`` is missing or corrupt, the operator
    has a recent ``claims.toml``, the project must be reconstructable.

    The rebuild is **fresh-only**. ``restore`` refuses to run if
    ``.mareforma/graph.db`` already contains claims — merge semantics
    are out of scope for the current release (status drift, supports[] divergence,
    and validator chain conflicts have no clean answers). Wipe
    ``graph.db`` first if you really mean to overwrite.

    Signature verification is fail-all-or-nothing. Every enrollment
    envelope is verified against its parent key; every claim
    ``signature_bundle`` is verified against the enrolled signer key;
    every ``validation_signature`` is verified against its signer key.
    The first failure rolls back the entire transaction — the project
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
    # moment restore() ran — silently breaking the catastrophic-loss
    # recovery path on the most common modern Python.
    try:
        import tomllib  # Python 3.11+ stdlib
    except ImportError:  # pragma: no cover  -- Python 3.10 path
        import tomli as tomllib  # type: ignore[no-redef]
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    root = Path(project_root)
    toml_path = (
        Path(claims_toml) if claims_toml is not None else root / "claims.toml"
    )
    if not toml_path.exists():
        raise RestoreError(
            f"claims.toml not found at {toml_path}",
            kind="toml_not_found",
        )

    try:
        data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise RestoreError(
            f"claims.toml at {toml_path} is malformed: {exc}",
            kind="toml_malformed",
        ) from exc

    validators_section: dict = data.get("validators", {}) or {}
    claims_section: dict = data.get("claims", {}) or {}

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
        # closes the window between "check" and "act" — a concurrent
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
                    f"{existing['n']} claim(s). restore() refuses to merge — "
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

            # Order claims by created_at so prev_hash reconstruction
            # matches the original chain. SHA256 is deterministic — same
            # inputs in the same order produce the same chain.
            ordered_claims = sorted(
                claims_section.items(),
                key=lambda kv: kv[1].get("created_at", ""),
            )

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
                # EvidenceVector round-trip. The TOML carries the
                # canonical JSON; we re-derive ev_* + chain_input from
                # it so the chain_hash matches the original.
                evidence_json_str = c.get("evidence_json") or "{}"
                try:
                    evidence_dict = json.loads(evidence_json_str)
                except (ValueError, TypeError):
                    evidence_dict = {}
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
                prev_hash = _compute_prev_hash(
                    conn, chain_fields, evidence_dict,
                )
                val_sig = c.get("validation_signature")
                validator_keyid = (
                    _extract_validation_signer_keyid(val_sig)
                    if val_sig else None
                )
                # The INSERT trigger only accepts PRELIMINARY or
                # ESTABLISHED as initial values — REPLICATED is reached
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
                # NOT carry validated_by / validated_at — the INSERT
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
                # adversarial anchor — any TOML tamper of an ev_* field
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
                    )
                ) if c.get("signature_bundle") else None
                # transparency_logged: trust the TOML flag ONLY when the
                # bundle actually carries a rekor block with a uuid.
                # Otherwise a hand-edited claims.toml could flip the
                # flag to true and the row would then satisfy the
                # REPLICATED-detection gate (transparency_logged=1)
                # without ever having been witnessed by the log.
                toml_logged = c.get("transparency_logged")
                bundle_has_rekor = False
                if c.get("signature_bundle"):
                    try:
                        _env = json.loads(c["signature_bundle"])
                        _rekor = _env.get("rekor") or {}
                        bundle_has_rekor = bool(_rekor.get("uuid"))
                    except (json.JSONDecodeError, AttributeError, TypeError):
                        bundle_has_rekor = False
                resolved_transparency = (
                    1 if (toml_logged is not False and bundle_has_rekor)
                    else 0
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
                             artifact_hash, prev_hash,
                             ev_risk_of_bias, ev_inconsistency,
                             ev_indirectness, ev_imprecision, ev_pub_bias,
                             evidence_json, statement_cid,
                             convergence_retry_needed,
                             predicate_payload, original_signature_bundle,
                             created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            claim_id, c_text, c_classification,
                            insert_level,
                            None,  # idempotency_key — TOML doesn't carry it
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
                            c.get("artifact_hash"), prev_hash,
                            int(evidence_dict.get("risk_of_bias", 0) or 0),
                            int(evidence_dict.get("inconsistency", 0) or 0),
                            int(evidence_dict.get("indirectness", 0) or 0),
                            int(evidence_dict.get("imprecision", 0) or 0),
                            int(evidence_dict.get("publication_bias", 0) or 0),
                            evidence_json_str,
                            statement_cid_str,
                            1 if c.get("convergence_retry_needed") else 0,
                            _restore_predicate_payload(c, claim_id),
                            _restore_original_signature_bundle(c, claim_id),
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
                    # PRELIMINARY → REPLICATED — the UPDATE trigger
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
            # and re-derives t_invalid — restore doesn't need to
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
                    "when restoring from a pre-v0.3.2 TOML. Run "
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
                if not r_uuid or r_raw is None:
                    raise RestoreError(
                        f"rekor_inclusions entry for {cid!r} is missing "
                        "required fields (uuid, raw_response_b64)",
                        kind="rekor_inclusion_invalid",
                    )
                conn.execute(
                    "INSERT OR IGNORE INTO rekor_inclusions "
                    "(claim_id, uuid, log_index, integrated_time, "
                    "raw_response_b64, recorded_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (cid, r_uuid, r_log_index, r_itime, r_raw, r_recorded),
                )

            if has_rekor_section and rekor_logged_claim_ids:
                from .errors import RekorSidecarEntryMissingWarning
                for cid in rekor_logged_claim_ids:
                    if cid not in rekor_section:
                        warnings.warn(
                            f"Claim {cid[:12]}... has Rekor coords in its "
                            "signature_bundle but no matching entry in "
                            "[rekor_inclusions]. The section exists (not a "
                            "pre-v0.3.2 upgrade) — investigate whether the "
                            "entry was removed from claims.toml.",
                            RekorSidecarEntryMissingWarning,
                            stacklevel=2,
                        )

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
    finally:
        conn.close()


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
            "section — verdict signer is not enrolled.",
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
            f"{ctx} signature verification failed — TOML tampered or "
            "signature forged.",
            kind="claim_unverified",
        ) from exc

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

    Same shape as the replication verdict path. The
    ``contradiction_invalidates_older`` trigger fires on this INSERT
    and re-derives ``claims.t_invalid`` automatically.
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
            "section — verdict signer is not enrolled.",
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
            f"{ctx} signature verification failed — TOML tampered or "
            "signature forged.",
            kind="claim_unverified",
        ) from exc

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
        # them past restore and into substrate-trusted role
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
                    "does not match the row — TOML tampered.",
                    kind="claim_unverified",
                )

        # EvidenceVector binding. The predicate carries the canonical
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
                "evidence_json on the row — TOML tampered.",
                kind="claim_unverified",
            )

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
        # the embedded payload — it does NOT prove the embedded payload
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
        # Validation envelopes bind validated_at; seed envelopes bind
        # seeded_at. Both must match the row's validated_at column —
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
        # evidence_seen verification — only relevant for the
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


