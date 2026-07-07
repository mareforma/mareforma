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
from pathlib import Path

import pytest
import tomli_w

import mareforma
from mareforma import signing as _signing
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
            "SELECT claim_id, support_level, transparency_logged, t_invalid, "
            "asserter_keyid, validator_keyid, validated_by, validated_at, "
            "convergence_retry_needed FROM claims"
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
        seed = g.assert_claim("anchor", generated_by="seed", seed=True)
        g.enroll_validator(_pem_of(val_key), identity="v")
        c1 = g.assert_claim(
            "converged", supports=[seed], generated_by="A", signer=root_signer,
        )
        assert g.get_claim(c1)["support_level"] == "PRELIMINARY"

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path)

    # The honest transparency flag survives the round-trip.
    assert _trust_columns(tmp_path)[c1]["transparency_logged"] == 1

    # A second distinct-signer peer added post-restore promotes BOTH to
    # REPLICATED — only possible if c1 stayed transparency-eligible.
    with mareforma.open(tmp_path, key_path=root_key) as g:
        c2 = g.assert_claim(
            "converged", supports=[seed], generated_by="B", signer=val_signer,
        )
        assert g.get_claim(c1)["support_level"] == "REPLICATED"
        assert g.get_claim(c2)["support_level"] == "REPLICATED"


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
    mareforma.restore(tmp_path)

    # No [rekor_inclusions] entry backs the forged uuid: not witnessed.
    assert _trust_columns(tmp_path)[cid]["transparency_logged"] == 0


def test_restore_refuses_a_forged_replicated_support_level(
    tmp_path: Path,
) -> None:
    """``support_level`` is not a signed field. A tampered claims.toml can flip
    a lone PRELIMINARY claim to REPLICATED, forging distinct-signer
    corroboration that never happened — the claim signature still verifies.
    Restore must re-derive REPLICATED from the signed supports graph + verified
    asserter identities and refuse a level no corroboration backs."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        cid = g.assert_claim("lonely claim, no converging peer", generated_by="x")
        assert g.get_claim(cid)["support_level"] == "PRELIMINARY"

    from mareforma.db import open_db, _backup_claims_toml, RestoreError
    conn = open_db(tmp_path)
    _backup_claims_toml(conn, tmp_path)
    conn.close()

    toml_path = tmp_path / "claims.toml"
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    # Forge the level only; the signature bundle is left untouched and valid.
    data["claims"][cid]["support_level"] = "REPLICATED"
    toml_path.write_text(tomli_w.dumps(data), encoding="utf-8")

    _wipe_graph_db(tmp_path)
    with pytest.raises(RestoreError):
        mareforma.restore(tmp_path)


def test_restore_preserves_a_corroborated_replicated(tmp_path: Path) -> None:
    """A genuinely converged REPLICATED pair — distinct signers on a shared
    ESTABLISHED anchor — survives backup + restore. The corroboration is
    re-derivable from the signed supports graph, so the level is kept and the
    forgery check does not false-reject an honest promotion."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    val_key = _bootstrap_key(tmp_path, "val.key")
    root_signer = _signing.load_private_key(root_key)
    val_signer = _signing.load_private_key(val_key)
    with mareforma.open(tmp_path, key_path=root_key) as g:
        seed = g.assert_claim("anchor", generated_by="seed", seed=True)
        g.enroll_validator(_pem_of(val_key), identity="v")
        c1 = g.assert_claim(
            "converged", supports=[seed], generated_by="A", signer=root_signer,
        )
        c2 = g.assert_claim(
            "converged", supports=[seed], generated_by="B", signer=val_signer,
        )
        assert g.get_claim(c1)["support_level"] == "REPLICATED"
        assert g.get_claim(c2)["support_level"] == "REPLICATED"

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path)
    cols = _trust_columns(tmp_path)
    assert cols[c1]["support_level"] == "REPLICATED"
    assert cols[c2]["support_level"] == "REPLICATED"


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
    mareforma.restore(tmp_path)  # deliberately without enforce_rekor_policy
    assert _trust_columns(tmp_path)[cid]["transparency_logged"] == 0


def test_project_policy_round_trips_through_restore(tmp_path: Path) -> None:
    """A root-signed witnessing policy is emitted to claims.toml and rebuilt on
    restore, so the recovered graph carries the same trust declaration."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("anchored", generated_by="x")
        g.require_rekor_witnessing()

    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path)

    from mareforma.db import open_db, get_project_policy
    conn = open_db(tmp_path)
    try:
        policy = get_project_policy(conn)
    finally:
        conn.close()
    assert policy is not None and policy["rekor_required"] == 1


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
    with pytest.raises(Exception) as exc:
        mareforma.restore(
            tmp_path, rekor_log_pubkey_pem=log_pubkey, enforce_rekor_policy=True,
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
    with pytest.raises(Exception) as exc:
        mareforma.restore(tmp_path, enforce_rekor_policy=True)
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
    with pytest.raises(Exception) as exc:
        mareforma.restore(tmp_path)
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
    with pytest.raises(Exception) as exc:
        mareforma.restore(
            tmp_path, rekor_log_pubkey_pem=log_pubkey, enforce_rekor_policy=True,
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
    with pytest.raises(Exception) as exc:
        mareforma.restore(tmp_path, rekor_log_pubkey_pem=log_pubkey)
    assert "does not bind" in str(exc.value)


def test_restore_preserves_the_full_trust_layer(tmp_path: Path) -> None:
    """Round-trip a graph carrying every trust-layer state promotion reads —
    REPLICATED level, contradiction invalidation, signer identity, transparency
    — and assert the recovered columns are identical to the originals."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    val_key = _bootstrap_key(tmp_path, "val.key")
    val2_key = _bootstrap_key(tmp_path, "val2.key")
    root_signer = _signing.load_private_key(root_key)
    val_signer = _signing.load_private_key(val_key)

    with mareforma.open(tmp_path, key_path=root_key) as g:
        seed = g.assert_claim("anchor", generated_by="seed", seed=True)
        g.enroll_validator(_pem_of(val_key), identity="v")
        g.enroll_validator(_pem_of(val2_key), identity="v2")
        rep = g.assert_claim(
            "converged", supports=[seed], generated_by="A", signer=root_signer,
        )
        g.assert_claim(
            "converged", supports=[seed], generated_by="B", signer=val_signer,
        )
        assert g.get_claim(rep)["support_level"] == "REPLICATED"
        # Two standalone claims to be contradicted by a third-party witness.
        older = g.assert_claim("older statement", generated_by="A",
                               signer=root_signer)
        newer = g.assert_claim("newer statement", generated_by="B",
                               signer=val_signer)

    # val2 signed neither claim, so it is a valid external contradiction
    # witness (self-verdicts are refused) and the promoting validator.
    with mareforma.open(tmp_path, key_path=val2_key) as g:
        g.validate(rep)
        assert g.get_claim(rep)["support_level"] == "ESTABLISHED"
        g.record_contradiction_verdict(
            verdict_id="v-contra-1",
            member_claim_id=newer,
            other_claim_id=older,
        )
        assert g.get_claim(older)["t_invalid"] is not None

    pre = _trust_columns(tmp_path)
    _wipe_graph_db(tmp_path)
    mareforma.restore(tmp_path)
    post = _trust_columns(tmp_path)

    assert set(pre) == set(post)
    for cid in pre:
        assert pre[cid] == post[cid], f"trust columns drifted for {cid}"
