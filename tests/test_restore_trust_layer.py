"""Restore reconstructs the trust layer, and derives witnessed-state honestly.

``restore()`` is the catastrophic-loss recovery path. It must rebuild the state
that promotion depends on (support level, invalidation, transparency, signer
identity) so the recovered graph behaves identically, and it must derive
"transparency-logged" from the verifiable ``[rekor_inclusions]`` sidecar rather
than the unsigned ``rekor`` block inside a claim's signature bundle. Tampering
with claims.toml is restore's threat model.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import tomli_w

import mareforma
from mareforma import signing as _signing
from mareforma.db import (
    LLMValidatorPromotionError,
    RestoreError,
    SelfValidationError,
)
from tests._helpers import _bootstrap_key, _pem_of

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover -- 3.10 path
    import tomli as tomllib  # type: ignore[no-redef]


def _wipe_graph_db(tmp_path: Path) -> None:
    db_dir = tmp_path / ".mareforma"
    for f in db_dir.iterdir():
        f.unlink()
    db_dir.rmdir()


def _trust_columns(tmp_path: Path) -> dict:
    """Read the trust-relevant columns for every claim, keyed by claim_id."""
    from mareforma.db import open_db
    conn = open_db(tmp_path)
    try:
        rows = conn.execute(
            "SELECT claim_id, transparency_logged, t_invalid, "
            "asserter_keyid, validator_keyid, validated_by, validated_at "
            "FROM claims"
        ).fetchall()
    finally:
        conn.close()
    return {r["claim_id"]: dict(r) for r in rows}


def test_signed_non_rekor_claim_keeps_transparency_and_still_converges(
    tmp_path: Path,
) -> None:
    """A signed claim in a non-Rekor project is transparency-ready by default.
    Restore must preserve that so a peer added afterward still converges. The
    bug: restore recomputed the flag from the (absent) bundle rekor block and
    demoted the claim to 0, silently blocking all future convergence."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    val_key = _bootstrap_key(tmp_path, "val.key")
    root_signer = _signing.load_private_key(root_key)
    val_signer = _signing.load_private_key(val_key)

    with mareforma.open(tmp_path, key_path=root_key) as g:
        seed = g.assert_claim("anchor", generated_by="seed")
        g.enroll_validator(_pem_of(val_key), identity="v")
        c1 = g.assert_claim(
            "converged", supports=[seed], generated_by="A", signer=root_signer,
        )

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path, trust_unaccounted_backup=True)

    # The honest transparency flag survives the round-trip.
    assert _trust_columns(tmp_path)[c1]["transparency_logged"] == 1

    # A second distinct-signer peer added post-restore promotes BOTH to
    # REPLICATED, only possible if c1 stayed transparency-eligible.
    with mareforma.open(tmp_path, key_path=root_key) as g:
        c2 = g.assert_claim(
            "converged", supports=[seed], generated_by="B", signer=val_signer,
        )


def test_restore_refuses_a_forged_rekor_uuid_in_the_bundle(
    tmp_path: Path,
) -> None:
    """Injecting a rekor block into a claim's signature_bundle forges
    "witnessed" state: the block is attached after signing, so the claim
    signature still verifies. Restore must derive transparency from the
    verifiable [rekor_inclusions] sidecar, not this field, and refuse the
    forge when no matching sidecar entry exists."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        cid = g.assert_claim("witness me", generated_by="x")

    # Honest state: an unwitnessed claim awaiting its inclusion proof.
    from mareforma.db import open_db, _backup_claims_toml
    conn = open_db(tmp_path)
    conn.execute(
        "UPDATE claims SET transparency_logged = 0 WHERE claim_id = ?", (cid,),
    )
    conn.commit()
    _backup_claims_toml(conn, tmp_path)
    conn.close()

    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    entry = data["claims"][cid]
    bundle = json.loads(entry["signature_bundle"])
    bundle["rekor"] = {"uuid": "forged0000deadbeef"}  # attached post-signing
    entry["signature_bundle"] = json.dumps(bundle)
    entry.pop("transparency_logged", None)  # flip the honest =false away
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path, trust_unaccounted_backup=True)

    # No [rekor_inclusions] entry backs the forged uuid: not witnessed.
    assert _trust_columns(tmp_path)[cid]["transparency_logged"] == 0


def _forge_established_and_restore(
    tmp_path: Path, claim_id: str, signer,
) -> None:
    """Stamp a claim ESTABLISHED in claims.toml with a real validation envelope.

    ``support_level`` is unsigned, so the level and the envelope are both
    hand-written; the envelope is genuinely signed by *signer* and binds this
    claim_id, which is what makes it survive every signature check restore runs.
    """
    from mareforma.db import _backup_claims_toml, open_db
    conn = open_db(tmp_path)
    _backup_claims_toml(conn, tmp_path)
    conn.close()

    keyid = _signing.public_key_id(signer.public_key())
    validated_at = "2099-01-01T00:00:00Z"
    envelope = _signing.sign_validation(
        {
            "claim_id": claim_id,
            "validator_keyid": keyid,
            "validated_at": validated_at,
            "evidence_seen": [],
        },
        signer,
    )
    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    entry = data["claims"][claim_id]
    entry["support_level"] = "ESTABLISHED"
    entry["validation_signature"] = json.dumps(
        envelope, sort_keys=True, separators=(",", ":"),
    )
    entry["validated_at"] = validated_at
    entry["validator_keyid"] = keyid
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path, trust_unaccounted_backup=True)


def test_restore_refuses_a_self_validated_established(tmp_path: Path) -> None:
    """Promotion needs a witnessing validator whose keyid is not on the claim
    envelope. The live path raises SelfValidationError; restore must refuse the
    same envelope rather than rebuild the row it names."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    val_key = _bootstrap_key(tmp_path, "val.key")
    root_signer = _signing.load_private_key(root_key)
    val_signer = _signing.load_private_key(val_key)
    with mareforma.open(tmp_path, key_path=root_key) as g:
        seed = g.assert_claim("anchor", generated_by="seed")
        g.enroll_validator(_pem_of(val_key), identity="v")
        c1 = g.assert_claim(
            "converged", supports=[seed], generated_by="A", signer=root_signer,
        )
        g.assert_claim(
            "converged", supports=[seed], generated_by="B", signer=val_signer,
        )
        with pytest.raises(SelfValidationError):
            g.validate(c1)

    with pytest.raises(RestoreError):
        _forge_established_and_restore(tmp_path, c1, root_signer)


def test_restore_refuses_an_established_validated_by_an_llm(
    tmp_path: Path,
) -> None:
    """An LLM-typed validator may sign a validation envelope but cannot promote
    past REPLICATED. The type is bound into the signed enrollment, so restore
    can hold the replayed envelope to the same ceiling the live path does."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    val_key = _bootstrap_key(tmp_path, "val.key")
    llm_key = _bootstrap_key(tmp_path, "llm.key")
    root_signer = _signing.load_private_key(root_key)
    val_signer = _signing.load_private_key(val_key)
    llm_signer = _signing.load_private_key(llm_key)
    with mareforma.open(tmp_path, key_path=root_key) as g:
        seed = g.assert_claim("anchor", generated_by="seed")
        g.enroll_validator(_pem_of(val_key), identity="v")
        g.enroll_validator(
            _pem_of(llm_key), identity="bot", validator_type="llm",
        )
        c1 = g.assert_claim(
            "converged", supports=[seed], generated_by="A", signer=root_signer,
        )
        g.assert_claim(
            "converged", supports=[seed], generated_by="B", signer=val_signer,
        )

    with mareforma.open(tmp_path, key_path=llm_key) as g:
        with pytest.raises(LLMValidatorPromotionError):
            g.validate(c1)

    with pytest.raises(RestoreError):
        _forge_established_and_restore(tmp_path, c1, llm_signer)


def test_restore_pends_an_unwitnessed_claim_under_a_rekor_policy(
    tmp_path: Path,
) -> None:
    """A project whose signed policy requires witnessing must not restore an
    unwitnessed claim as convergence-ready, even without enforce_rekor_policy.
    The claim carries no rekor coords and no verified sidecar entry, so an
    absent/stripped transparency flag cannot default it to ready."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        cid = g.assert_claim("must be witnessed", generated_by="x")
        g.require_rekor_witnessing()

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path, trust_unaccounted_backup=True)  # deliberately without enforce_rekor_policy
    assert _trust_columns(tmp_path)[cid]["transparency_logged"] == 0


def test_project_policy_round_trips_through_restore(tmp_path: Path) -> None:
    """A root-signed witnessing policy is emitted to claims.toml and rebuilt on
    restore, so the recovered graph carries the same trust declaration."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("anchored", generated_by="x")
        g.require_rekor_witnessing()

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path, trust_unaccounted_backup=True)

    from mareforma.db import open_db, get_project_policy
    conn = open_db(tmp_path)
    try:
        policy = get_project_policy(conn)
    finally:
        conn.close()
    assert policy is not None and policy["rekor_required"] == 1


def test_restore_accepts_a_policy_envelope_signed_before_the_strict_field(
    tmp_path: Path,
) -> None:
    """A v1 policy envelope, the shape signed before strict promotion existed,
    still verifies and still restores. Deployed projects carry these; adding a
    field to the declaration must not invalidate them."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("anchored", generated_by="x")
        g.require_rekor_witnessing()

    # Re-sign the declaration the way the pre-versioning code did: the payload
    # is exactly {created_at, rekor_required}, with no version field.
    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    created_at = data["project_policy"]["created_at"]
    payload = json.dumps(
        {"created_at": created_at, "rekor_required": True},
        sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    envelope = _signing._build_envelope(
        payload, _signing.load_private_key(root_key),
        payload_type=_signing.PAYLOAD_TYPE_PROJECT_POLICY,
    )
    data["project_policy"] = {
        "rekor_required": True,
        "signer_keyid": data["project_policy"]["signer_keyid"],
        "envelope": json.dumps(envelope, sort_keys=True, separators=(",", ":")),
        "created_at": created_at,
    }
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path, trust_unaccounted_backup=True)

    from mareforma.db import get_project_policy, open_db
    conn = open_db(tmp_path)
    try:
        policy = get_project_policy(conn)
    finally:
        conn.close()
    assert policy["rekor_required"] == 1
    # A v1 envelope declares nothing about strict promotion, and restore must
    # not read a rule into it that the root never signed.
    assert policy["strict_promotion_required"] == 0


def test_restore_dates_an_undated_policy_envelope_by_its_created_at(
    tmp_path: Path,
) -> None:
    """A policy envelope signed before the per-flag declaration times existed
    still verifies and still restores, and its flags are dated by the one
    timestamp it does carry, so a project that upgrades is held to exactly the
    window it was already held to."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("anchored", generated_by="x")
        # A declaration has to exist before it can be re-signed. Witnessing is
        # the one a caller can still make; the strict-promotion flag that made
        # the other went with the promotion it gated.
        g.require_rekor_witnessing()

    # Re-sign the declaration the way the pre-v3 code did: the payload carries
    # the flags and one created_at, and nothing dating either flag.
    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    created_at = data["project_policy"]["created_at"]
    payload = json.dumps(
        {
            "version": 2,
            "rekor_required": False,
            "strict_promotion_required": True,
            "created_at": created_at,
        },
        sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    envelope = _signing._build_envelope(
        payload, _signing.load_private_key(root_key),
        payload_type=_signing.PAYLOAD_TYPE_PROJECT_POLICY,
    )
    data["project_policy"] = {
        "rekor_required": False,
        "strict_promotion_required": True,
        "signer_keyid": data["project_policy"]["signer_keyid"],
        "envelope": json.dumps(envelope, sort_keys=True, separators=(",", ":")),
        "created_at": created_at,
    }
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path, trust_unaccounted_backup=True)

    from mareforma.db import (
        get_project_policy, open_db, project_policy_declared_at,
    )
    conn = open_db(tmp_path)
    try:
        policy = get_project_policy(conn)
    finally:
        conn.close()
    assert policy["strict_promotion_required"] == 1
    assert policy["strict_promotion_declared_at"] is None
    assert project_policy_declared_at(policy) == (None, created_at)


def test_restore_refuses_a_declaration_time_the_envelope_does_not_carry(
    tmp_path: Path,
) -> None:
    """The declaration times are signed material. Editing one into the flat
    fields of an envelope that does not carry it moves the grandfathering
    cutoff, so restore aborts rather than honouring the edit."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("anchored", generated_by="x")
        g.require_rekor_witnessing()

    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    data["project_policy"]["rekor_declared_at"] = "2099-01-01T00:00:00+00:00"
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    with pytest.raises(RestoreError) as exc:
        mareforma.restore(tmp_path, trust_unaccounted_backup=True)
    assert "not match the signed envelope" in str(exc.value)


def test_restore_refuses_a_stripped_strict_promotion_flag(tmp_path: Path) -> None:
    """Dropping the strict flag from the flat fields leaves it in the signed
    envelope, so the cache no longer matches the declaration and restore
    aborts. The gate cannot be edited off the project."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("anchored", generated_by="x")
        g.require_rekor_witnessing()

    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    # The flat field and the signed envelope have to disagree; which flag it is
    # does not matter, only that one of them was edited off the declaration.
    data["project_policy"]["rekor_required"] = False
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    with pytest.raises(RestoreError) as exc:
        mareforma.restore(tmp_path, trust_unaccounted_backup=True)
    assert "not match the signed envelope" in str(exc.value)


def test_enforced_policy_fails_closed_on_an_unwitnessed_signed_claim(
    tmp_path: Path,
) -> None:
    """Under an enforced witnessing policy, a signed claim with no inclusion
    proof restores as not convergence-eligible even if the TOML says otherwise.
    This is the strip-route defeat: an edited claims.toml cannot buy readiness."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        cid = g.assert_claim("must be witnessed", generated_by="x")
        g.require_rekor_witnessing()
    # Non-Rekor claim: transparency_logged defaults to 1. The signed policy
    # says witnessing is required, and no sidecar entry backs this claim.

    _wipe_graph_db(tmp_path)
    log_pubkey = _pem_of(_bootstrap_key(tmp_path, "log.key"))
    mareforma.restore(
        tmp_path, rekor_log_pubkey_pem=log_pubkey, enforce_rekor_policy=True,
    )
    assert _trust_columns(tmp_path)[cid]["transparency_logged"] == 0


def test_enforce_requires_a_signed_policy(tmp_path: Path) -> None:
    """enforce_rekor_policy=True on a project with no policy is refused, so the
    operator's assertion cannot be silently satisfied by an absent declaration."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("no policy here", generated_by="x")

    _wipe_graph_db(tmp_path)
    log_pubkey = _pem_of(_bootstrap_key(tmp_path, "log.key"))
    with pytest.raises(RestoreError) as exc:
        mareforma.restore(
            tmp_path, rekor_log_pubkey_pem=log_pubkey,
            enforce_rekor_policy=True, trust_unaccounted_backup=True,
        )
    assert "no root-signed policy" in str(exc.value)


def test_enforce_requires_a_pinned_log_key(tmp_path: Path) -> None:
    """Enforcement without a pinned log key is refused: unverified inclusion
    proofs cannot back a fail-closed guarantee."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("anchored", generated_by="x")
        g.require_rekor_witnessing()

    _wipe_graph_db(tmp_path)
    with pytest.raises(RestoreError) as exc:
        mareforma.restore(tmp_path, enforce_rekor_policy=True, trust_unaccounted_backup=True)
    assert "requires rekor_log_pubkey_pem" in str(exc.value)


def test_restore_refuses_a_tampered_project_policy(tmp_path: Path) -> None:
    """A [project_policy] whose flat fields disagree with its signed envelope is
    tampered material and aborts the restore, even without enforcement."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("anchored", generated_by="x")
        g.require_rekor_witnessing()

    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    data["project_policy"]["rekor_required"] = False  # envelope still says True
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    with pytest.raises(RestoreError) as exc:
        mareforma.restore(tmp_path, trust_unaccounted_backup=True)
    assert "not match the signed envelope" in str(exc.value)


def test_enforced_policy_rejects_an_empty_inclusion_body(tmp_path: Path) -> None:
    """An empty raw_response_b64 must not slip past verification: it would skip
    both the Merkle check and the claim binding while the claim counts as
    witnessed by mere presence in the sidecar. Restore must refuse it."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        cid = g.assert_claim("must be witnessed", generated_by="x")
        g.require_rekor_witnessing()

    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    data["rekor_inclusions"] = {
        cid: {
            "uuid": "fake-uuid",
            "log_index": 1,
            "raw_response_b64": "",  # empty body: would bypass verification
            "recorded_at": "2026-05-27T01:00:00Z",
        }
    }
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    log_pubkey = _pem_of(_bootstrap_key(tmp_path, "log.key"))
    with pytest.raises(RestoreError) as exc:
        mareforma.restore(
            tmp_path, rekor_log_pubkey_pem=log_pubkey,
            enforce_rekor_policy=True, trust_unaccounted_backup=True,
        )
    assert "missing required fields" in str(exc.value)


def _hashedrekord_raw_response(payload_hash: str, sig_b64: str, uuid: str) -> str:
    """Build a base64 raw_response_b64 whose logged body records the given
    payload hash + signature, in the ``{uuid: {body,...}}`` shape restore reads."""
    import base64
    record = {
        "apiVersion": "0.0.1",
        "kind": "hashedrekord",
        "spec": {
            "data": {"hash": {"algorithm": "sha256", "value": payload_hash}},
            "signature": {"content": sig_b64, "publicKey": {"content": "x"}},
        },
    }
    body = base64.b64encode(
        json.dumps(record, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    raw = {uuid: {"body": body, "integratedTime": 1700000000, "logIndex": 5}}
    return base64.b64encode(json.dumps(raw).encode("utf-8")).decode("ascii")


def test_restore_rejects_a_rekor_proof_copied_from_another_claim(
    tmp_path: Path,
) -> None:
    """A valid inclusion proof covers one claim's signed payload. Copying
    another claim's sidecar entry onto this row must fail under a pinned log
    key: the proof body does not bind to this claim's payload hash + signature."""
    import base64
    import hashlib

    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        a = g.assert_claim("claim A original", generated_by="x")
        g.assert_claim("claim B original", generated_by="x")

    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    # Take claim B's real logged material and plant it under claim A's id.
    b_id = next(k for k in data["claims"] if k != a)
    b_bundle = json.loads(data["claims"][b_id]["signature_bundle"])
    b_hash = hashlib.sha256(
        base64.standard_b64decode(b_bundle["payload"])
    ).hexdigest()
    b_sig = b_bundle["signatures"][0]["sig"]
    data["rekor_inclusions"] = {
        a: {
            "uuid": "b-real-uuid",
            "log_index": 5,
            "raw_response_b64": _hashedrekord_raw_response(b_hash, b_sig, "b-real-uuid"),
            "recorded_at": "2026-05-27T01:00:00Z",
        }
    }
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    log_pubkey = _pem_of(_bootstrap_key(tmp_path, "log.key"))
    with pytest.raises(RestoreError) as exc:
        mareforma.restore(tmp_path, rekor_log_pubkey_pem=log_pubkey, trust_unaccounted_backup=True)
    assert "does not bind" in str(exc.value)


def test_restore_names_a_sidecar_that_carries_no_proof(tmp_path: Path) -> None:
    """A sidecar recorded without a pinned log key holds the entry coordinates
    only. Restoring it under a pinned key must refuse, and say the row has no
    proof rather than accuse the backup of carrying another claim's entry."""
    import base64

    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        cid = g.assert_claim("witnessed without a pinned key", generated_by="x")

    coords = {"uuid": "aa01bb02", "logIndex": 5, "integratedTime": 1700000000}
    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    data["rekor_inclusions"] = {
        cid: {
            "uuid": "aa01bb02",
            "log_index": 5,
            "raw_response_b64": base64.b64encode(
                json.dumps(coords).encode("utf-8"),
            ).decode("ascii"),
            "recorded_at": "2026-05-27T01:00:00Z",
        }
    }
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    log_pubkey = _pem_of(_bootstrap_key(tmp_path, "log.key"))
    with pytest.raises(RestoreError) as exc:
        mareforma.restore(tmp_path, rekor_log_pubkey_pem=log_pubkey, trust_unaccounted_backup=True)
    assert "no inclusion proof" in str(exc.value)


@pytest.mark.parametrize("broken", [
    {"log_index": None},
    {"recorded_at": None},
    {"log_index": "five"},
])
def test_restore_refuses_an_incomplete_sidecar_entry(
    tmp_path: Path, broken: dict,
) -> None:
    """``log_index`` and ``recorded_at`` are NOT NULL in the sidecar table, so
    an entry missing either was skipped by INSERT OR IGNORE without a word,
    while transparency_logged was already resolved to 1 from the entry's
    presence. That leaves the recovered graph permanently disagreeing with
    itself and nothing to reconcile against, so restore must refuse."""
    import base64

    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        cid = g.assert_claim("witnessed claim", generated_by="x")

    coords = {"uuid": "aa01bb02", "logIndex": 5, "integratedTime": 1700000000}
    entry = {
        "uuid": "aa01bb02",
        "log_index": 5,
        "raw_response_b64": base64.b64encode(
            json.dumps(coords).encode("utf-8"),
        ).decode("ascii"),
        "recorded_at": "2026-05-27T01:00:00Z",
    }
    for field, value in broken.items():
        if value is None:
            del entry[field]
        else:
            entry[field] = value
    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    data["rekor_inclusions"] = {cid: entry}
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    with pytest.raises(RestoreError) as exc:
        mareforma.restore(tmp_path, trust_unaccounted_backup=True)
    assert exc.value.kind == "rekor_inclusion_invalid"


def test_restore_preserves_the_full_trust_layer(tmp_path: Path) -> None:
    """Round-trip a graph carrying every trust-layer state promotion reads , 
    REPLICATED level, contradiction invalidation, signer identity, transparency
   , and assert the recovered columns are identical to the originals."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    val_key = _bootstrap_key(tmp_path, "val.key")
    val2_key = _bootstrap_key(tmp_path, "val2.key")
    root_signer = _signing.load_private_key(root_key)
    val_signer = _signing.load_private_key(val_key)

    with mareforma.open(tmp_path, key_path=root_key) as g:
        seed = g.assert_claim("anchor", generated_by="seed")
        g.enroll_validator(_pem_of(val_key), identity="v")
        g.enroll_validator(_pem_of(val2_key), identity="v2")
        rep = g.assert_claim(
            "converged", supports=[seed], generated_by="A", signer=root_signer,
        )
        g.assert_claim(
            "converged", supports=[seed], generated_by="B", signer=val_signer,
        )
        # Two standalone claims to be contradicted by a third-party witness.
        older = g.assert_claim("older statement", generated_by="A",
                               signer=root_signer)
        newer = g.assert_claim("newer statement", generated_by="B",
                               signer=val_signer)

    # val2 signed neither claim, so it is a valid external contradiction
    # witness (self-verdicts are refused) and the promoting validator.
    with mareforma.open(tmp_path, key_path=val2_key) as g:
        g.validate(rep)
        g.record_contradiction_verdict(
            verdict_id="v-contra-1",
            member_claim_id=newer,
            other_claim_id=older,
        )
        assert g.get_claim(older)["t_invalid"] is not None

    pre = _trust_columns(tmp_path)
    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path, trust_unaccounted_backup=True)
    post = _trust_columns(tmp_path)

    assert set(pre) == set(post)
    for cid in pre:
        assert pre[cid] == post[cid], f"trust columns drifted for {cid}"


def test_restore_rebuilds_the_finding_evidence_tree(tmp_path: Path) -> None:
    """A finding's proposition / prediction / findings / evidence tree must
    survive the documented delete-and-restore recovery, including the signed
    model lineage denormalized onto the evidence line.

    The prior claims.toml round-trip predated the v0.3.9 trust tables and never extended to
    them, so after restore proposition_status returned None and the findings /
    evidence_lines rows were gone though the finding's signed claim survived.
    """
    from tests._helpers import _est, _pred, _prop, _verdict

    key = _bootstrap_key(tmp_path, "root.key")
    prop, pred = _prop(), _pred()
    content_id = prop.content_id()

    with mareforma.open(tmp_path, key_path=key) as g:
        g.assert_finding(
            prop, pred, _est(), data_id="ds1", generated_by="run1",
            grounding=_verdict("gpt-4o-2024-08-06", source="declared"),
        )
        conn = g._conn
        pre_findings = conn.execute("SELECT COUNT(*) FROM findings").fetchone()[0]
        pre_lines = conn.execute(
            "SELECT COUNT(*) FROM evidence_lines"
        ).fetchone()[0]
        pre_contrasts = conn.execute(
            "SELECT COUNT(*) FROM contrasts"
        ).fetchone()[0]
        pre_estimates = conn.execute(
            "SELECT COUNT(*) FROM effect_estimates"
        ).fetchone()[0]
        pre_lineage = conn.execute(
            "SELECT model_lineage FROM evidence_lines "
            "WHERE model_lineage IS NOT NULL LIMIT 1"
        ).fetchone()[0]
        assert g.proposition_status(content_id) is not None
        assert pre_findings >= 1 and pre_lines >= 1
        assert pre_lineage is not None

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path, trust_unaccounted_backup=True)

    with mareforma.open(tmp_path, key_path=key) as g:
        conn = g._conn
        assert g.proposition_status(content_id) is not None
        assert conn.execute(
            "SELECT COUNT(*) FROM findings"
        ).fetchone()[0] == pre_findings
        assert conn.execute(
            "SELECT COUNT(*) FROM evidence_lines"
        ).fetchone()[0] == pre_lines
        assert conn.execute(
            "SELECT COUNT(*) FROM contrasts"
        ).fetchone()[0] == pre_contrasts
        assert conn.execute(
            "SELECT COUNT(*) FROM effect_estimates"
        ).fetchone()[0] == pre_estimates
        post_lineage = conn.execute(
            "SELECT model_lineage FROM evidence_lines "
            "WHERE model_lineage IS NOT NULL LIMIT 1"
        ).fetchone()[0]
        assert post_lineage == pre_lineage


def test_restore_refuses_a_second_self_signed_root(tmp_path: Path) -> None:
    """A hand-edited claims.toml can append a second self-signed validator
    block whose own envelope verifies. Every live write path keeps exactly one
    root, so restore must too: two roots break the singleton-root invariant the
    chain walk requires, and the recovered project would refuse validate() and
    enroll_validator() for the honest root forever."""
    victim = tmp_path / "victim"
    attacker = tmp_path / "attacker"
    victim.mkdir()
    attacker.mkdir()

    with mareforma.open(victim, key_path=_bootstrap_key(victim, "root.key")) as g:
        g.assert_claim("honest finding", generated_by="x")
    with mareforma.open(attacker, key_path=_bootstrap_key(attacker, "a.key")) as g:
        g.assert_claim("attacker anchor", generated_by="a")

    victim_toml = victim / "claims.toml"
    data = tomllib.loads(victim_toml.read_text(encoding="utf-8"))
    rogue = tomllib.loads(
        (attacker / "claims.toml").read_text(encoding="utf-8")
    )["validators"]
    data["validators"].update(rogue)
    victim_toml.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(victim)
    with pytest.raises(RestoreError) as exc:
        mareforma.restore(victim)
    assert exc.value.kind == "enrollment_unverified"
    assert "more than one self-signed root" in str(exc.value)
    assert next(iter(rogue))[:12] in str(exc.value)


def test_restore_refuses_a_repointed_finding_edge(tmp_path: Path) -> None:
    """``findings.content_id`` is not part of the signed claim, so a hand-edited
    claims.toml can re-attach another signer's genuinely signed finding to a
    proposition it says nothing about and inflate that proposition's
    independence count. Restore must refuse an edge whose claim text is not the
    proposition the row points at."""
    from tests._helpers import _est, _pred, _prop
    from mareforma.trust import Direction, Proposition

    prop, pred = _prop(), _pred()
    other = Proposition(
        subject="TP53", relation="affects", object="apoptosis",
        direction=Direction.INCREASES,
        scope={"population": "TNBC", "condition": "in vitro"},
    )
    root_key = _bootstrap_key(tmp_path, "root.key")
    val_key = _bootstrap_key(tmp_path, "val.key")

    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.enroll_validator(_pem_of(val_key), identity="v")
        g.assert_finding(prop, pred, _est(), data_id="ds1", generated_by="A")
    with mareforma.open(tmp_path, key_path=val_key) as g:
        g.assert_finding(other, pred, _est(), data_id="ds2", generated_by="B")
        assert g.proposition_status(prop.content_id())["status"] == "PRELIMINARY"

    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    for entry in data["findings"].values():
        if entry["content_id"] == other.content_id():
            entry["content_id"] = prop.content_id()
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    with pytest.raises(RestoreError) as exc:
        mareforma.restore(tmp_path, trust_unaccounted_backup=True)
    assert exc.value.kind == "claim_unverified"
    assert "does not attest" in str(exc.value)


_PREDICTIONS_DDL_SQL = (
    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'predictions'"
)
_ALPHA_COLUMN_RE = re.compile(r"^[ \t]*alpha\b.*$", re.MULTILINE)

# The permissive CHECK an older graph carries. Written out rather than derived
# from the shipped DDL, so the legacy graph keeps this bound however the
# current schema spells its own.
_LEGACY_ALPHA_COLUMN = "    alpha REAL NOT NULL CHECK (alpha > 0 AND alpha < 1),"


def _set_alpha_check(db_path: Path, column_sql: str) -> None:
    """Replace the predictions alpha column definition in an existing graph.

    Only the CHECK expression changes, so the stored rows still match the
    recorded DDL. The rewrite asserts its own result: a step that leaves the
    bound it found is a step that proves nothing.
    """
    import sqlite3

    raw = sqlite3.connect(db_path, isolation_level=None)
    try:
        ddl = raw.execute(_PREDICTIONS_DDL_SQL).fetchone()[0]
        rewritten, count = _ALPHA_COLUMN_RE.subn(column_sql, ddl, count=1)
        assert count == 1, f"no alpha column in the predictions DDL:\n{ddl}"
        raw.execute("PRAGMA writable_schema=ON")
        raw.execute(
            "UPDATE sqlite_master SET sql = ? "
            "WHERE type = 'table' AND name = 'predictions'",
            (rewritten,),
        )
        raw.execute("PRAGMA writable_schema=RESET")
        assert column_sql in raw.execute(_PREDICTIONS_DDL_SQL).fetchone()[0], (
            "the alpha rewrite was inert: the graph still carries the bound it "
            "was created with"
        )
    finally:
        raw.close()


def test_the_legacy_alpha_bound_does_not_depend_on_the_shipped_spelling(
    tmp_path: Path,
) -> None:
    """The legacy step must install the older bound outright.

    Deriving it from the current DDL by matching one spelling of the current
    bound goes inert the moment that spelling changes, and an inert step leaves
    the restore test below building a graph identical to the fresh schema.
    """
    key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=key):
        pass
    db_path = tmp_path / ".mareforma" / "graph.db"

    # Stand in for a future schema that tightens alpha in its own words.
    _set_alpha_check(
        db_path, "    alpha REAL NOT NULL CHECK (alpha > 0 AND alpha <= 0.4),"
    )
    _set_alpha_check(db_path, _LEGACY_ALPHA_COLUMN)

    from mareforma.db import open_db

    conn = open_db(tmp_path)
    try:
        assert "alpha > 0 AND alpha < 1" in conn.execute(
            _PREDICTIONS_DDL_SQL
        ).fetchone()[0]
    finally:
        conn.close()


def test_restore_recovers_a_plan_registered_under_the_older_alpha_bound(
    tmp_path: Path,
) -> None:
    """A graph created before alpha tightened to (0, 0.5) keeps the permissive
    CHECK its schema was created with, so its predictions table still holds
    alpha >= 0.5. Restore rebuilds a fresh database and replays every backed-up
    row into it, so the fresh schema must accept whatever an existing graph
    accepts. The bug: the tightened CHECK rejected the legacy plan, the single
    restore transaction rolled back, and every claim in the backup was lost at
    the one moment graph.db was already gone."""
    from mareforma.db import _backup_claims_toml, open_db
    from tests._helpers import _est, _pred, _prop, _verdict

    key = _bootstrap_key(tmp_path, "root.key")
    prop = _prop()
    with mareforma.open(tmp_path, key_path=key) as g:
        finding = g.assert_finding(
            prop, _pred(), _est(), data_id="ds1", generated_by="run1",
            grounding=_verdict("gpt-4o-2024-08-06", source="declared"),
        )
    claim_id = finding["claim_id"]

    # Every statement in the schema is CREATE TABLE IF NOT EXISTS, so an
    # existing project keeps the bound it was created with and can hold a plan
    # the current API would refuse.
    _set_alpha_check(tmp_path / ".mareforma" / "graph.db", _LEGACY_ALPHA_COLUMN)

    conn = open_db(tmp_path)
    try:
        conn.execute(
            "INSERT INTO predictions (plan_id, content_id, inference_regime, "
            "test_type, direction_of_interest, alpha, preregistered, "
            "registered_at) VALUES ('PL-legacy', ?, 'frequentist', "
            "'superiority', 'decrease', 0.8, 0, '2026-01-01T00:00:00Z')",
            (prop.content_id(),),
        )
        conn.commit()
        _backup_claims_toml(conn, tmp_path)
    finally:
        conn.close()

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path, trust_unaccounted_backup=True)

    with mareforma.open(tmp_path, key_path=key) as g:
        assert g.get_claim(claim_id) is not None
        assert g.proposition_status(prop.content_id()) is not None
        assert g._conn.execute(
            "SELECT alpha FROM predictions WHERE plan_id = 'PL-legacy'"
        ).fetchone()[0] == 0.8


def test_restore_names_a_rejected_trust_row_rather_than_a_bad_signature(
    tmp_path: Path,
) -> None:
    """A trust-layer row the schema refuses is a broken backup, not a claim
    whose signature failed. Reporting it as claim_unverified tells an operator
    recovering from disk loss that their signed material was tampered with, and
    points at no fix that would work."""
    from tests._helpers import _est, _pred, _prop

    key = _bootstrap_key(tmp_path, "root.key")
    prop = _prop()
    with mareforma.open(tmp_path, key_path=key) as g:
        g.assert_finding(
            prop, _pred(), _est(), data_id="ds1", generated_by="run1",
        )

    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    for entry in data["predictions"].values():
        entry["content_id"] = "no-such-proposition"
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    with pytest.raises(RestoreError) as exc:
        mareforma.restore(tmp_path, trust_unaccounted_backup=True)
    assert exc.value.kind == "trust_row_rejected"
    assert "claims.toml" in str(exc.value)
