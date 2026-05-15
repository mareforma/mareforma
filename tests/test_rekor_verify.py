"""tests/test_rekor_verify.py — RFC 6962 Merkle inclusion + checkpoint
signature verification (live functions in :mod:`mareforma.signing`).

Synthesizes Merkle trees of varying sizes and known Ed25519 / ECDSA
signing keys, then drives the verifier through happy-path and every
documented failure mode.
"""

from __future__ import annotations

import base64
import hashlib

import pytest
from cryptography.hazmat.primitives import hashes as crypto_hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from mareforma import signing as _signing


# ---------------------------------------------------------------------------
# Merkle tree synthesis helpers (RFC 6962)
# ---------------------------------------------------------------------------


def _leaf_hash(leaf_bytes: bytes) -> bytes:
    return hashlib.sha256(b"\x00" + leaf_bytes).digest()


def _node_hash(left: bytes, right: bytes) -> bytes:
    return hashlib.sha256(b"\x01" + left + right).digest()


def _largest_pow2_strict_le(n: int) -> int:
    """Largest power of two strictly less than n (RFC 6962 §2.1 'k')."""
    k = 1
    while k * 2 < n:
        k *= 2
    return k


def _merkle_root(leaves: list[bytes]) -> bytes:
    if len(leaves) == 1:
        return _leaf_hash(leaves[0])
    k = _largest_pow2_strict_le(len(leaves))
    left = _merkle_root(leaves[:k])
    right = _merkle_root(leaves[k:])
    return _node_hash(left, right)


def _merkle_inclusion_path(leaves: list[bytes], index: int) -> list[bytes]:
    n = len(leaves)
    if n == 1:
        return []
    k = _largest_pow2_strict_le(n)
    if index < k:
        return _merkle_inclusion_path(leaves[:k], index) + [_merkle_root(leaves[k:])]
    return _merkle_inclusion_path(leaves[k:], index - k) + [_merkle_root(leaves[:k])]


# ---------------------------------------------------------------------------
# Checkpoint synthesis
# ---------------------------------------------------------------------------


def _sign_checkpoint_ed25519(
    *, origin: str, tree_size: int, root_hash: bytes,
    signer_name: str, key: Ed25519PrivateKey,
) -> str:
    body = (
        f"{origin}\n"
        f"{tree_size}\n"
        f"{base64.standard_b64encode(root_hash).decode('ascii')}\n"
    )
    body_bytes = body.encode("utf-8")
    sig = key.sign(body_bytes)
    sig_blob = b"\x00\x00\x00\x00" + sig
    sig_b64 = base64.standard_b64encode(sig_blob).decode("ascii")
    return body + "\n" + f"— {signer_name} {sig_b64}\n"


def _sign_checkpoint_ecdsa(
    *, origin: str, tree_size: int, root_hash: bytes,
    signer_name: str, key: ec.EllipticCurvePrivateKey,
) -> str:
    body = (
        f"{origin}\n"
        f"{tree_size}\n"
        f"{base64.standard_b64encode(root_hash).decode('ascii')}\n"
    )
    body_bytes = body.encode("utf-8")
    sig = key.sign(body_bytes, ec.ECDSA(crypto_hashes.SHA256()))
    sig_blob = b"\x00\x00\x00\x00" + sig
    sig_b64 = base64.standard_b64encode(sig_blob).decode("ascii")
    return body + "\n" + f"— {signer_name} {sig_b64}\n"


def _pubkey_pem(key) -> bytes:
    return key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


# ---------------------------------------------------------------------------
# compute_rekor_leaf_hash
# ---------------------------------------------------------------------------


class TestComputeLeafHash:
    def test_matches_rfc6962_spec(self) -> None:
        leaf = b"hello world"
        b64 = base64.standard_b64encode(leaf).decode("ascii")
        expected = hashlib.sha256(b"\x00" + leaf).digest()
        assert _signing.compute_rekor_leaf_hash(b64) == expected

    def test_distinct_leaves_distinct_hashes(self) -> None:
        a = base64.standard_b64encode(b"a").decode("ascii")
        b = base64.standard_b64encode(b"b").decode("ascii")
        assert _signing.compute_rekor_leaf_hash(a) != _signing.compute_rekor_leaf_hash(b)


# ---------------------------------------------------------------------------
# Merkle inclusion proof — happy paths
# ---------------------------------------------------------------------------


class TestMerkleInclusionHappy:
    @pytest.mark.parametrize("tree_size", [1, 2, 3, 4, 5, 7, 8, 13, 16, 21, 100])
    def test_inclusion_at_every_index(self, tree_size: int) -> None:
        leaves = [f"leaf-{i}".encode() for i in range(tree_size)]
        root = _merkle_root(leaves)
        for idx in range(tree_size):
            path = _merkle_inclusion_path(leaves, idx)
            assert _signing.verify_merkle_inclusion_proof(
                _leaf_hash(leaves[idx]), idx, tree_size, path, root,
            ) is True, f"failed at index {idx} of tree_size {tree_size}"

    def test_singleton_tree(self) -> None:
        leaves = [b"only"]
        assert _signing.verify_merkle_inclusion_proof(
            _leaf_hash(b"only"), 0, 1, [], _merkle_root(leaves),
        ) is True


# ---------------------------------------------------------------------------
# Merkle inclusion proof — adversarial
# ---------------------------------------------------------------------------


class TestMerkleInclusionAdversarial:
    def test_tampered_leaf_hash_refused(self) -> None:
        leaves = [f"l{i}".encode() for i in range(8)]
        root = _merkle_root(leaves)
        path = _merkle_inclusion_path(leaves, 3)
        assert _signing.verify_merkle_inclusion_proof(
            _leaf_hash(b"forged"), 3, 8, path, root,
        ) is False

    def test_tampered_sibling_refused(self) -> None:
        leaves = [f"l{i}".encode() for i in range(8)]
        root = _merkle_root(leaves)
        path = _merkle_inclusion_path(leaves, 3)
        bad = bytearray(path[0])
        bad[0] ^= 0xFF
        path[0] = bytes(bad)
        assert _signing.verify_merkle_inclusion_proof(
            _leaf_hash(leaves[3]), 3, 8, path, root,
        ) is False

    def test_proof_too_long_refused(self) -> None:
        leaves = [f"l{i}".encode() for i in range(4)]
        root = _merkle_root(leaves)
        path = _merkle_inclusion_path(leaves, 1)
        path.append(b"\x00" * 32)
        assert _signing.verify_merkle_inclusion_proof(
            _leaf_hash(leaves[1]), 1, 4, path, root,
        ) is False

    def test_proof_too_short_refused(self) -> None:
        leaves = [f"l{i}".encode() for i in range(4)]
        root = _merkle_root(leaves)
        path = _merkle_inclusion_path(leaves, 1)[:-1]
        assert _signing.verify_merkle_inclusion_proof(
            _leaf_hash(leaves[1]), 1, 4, path, root,
        ) is False

    def test_index_out_of_range_refused(self) -> None:
        leaves = [f"l{i}".encode() for i in range(4)]
        root = _merkle_root(leaves)
        path = _merkle_inclusion_path(leaves, 0)
        assert _signing.verify_merkle_inclusion_proof(
            _leaf_hash(leaves[0]), 4, 4, path, root,
        ) is False

    def test_negative_index_refused(self) -> None:
        assert _signing.verify_merkle_inclusion_proof(
            b"\x00" * 32, -1, 4, [], b"\x00" * 32,
        ) is False

    def test_zero_tree_size_refused(self) -> None:
        assert _signing.verify_merkle_inclusion_proof(
            b"\x00" * 32, 0, 0, [], b"\x00" * 32,
        ) is False

    def test_wrong_leaf_hash_length_refused(self) -> None:
        leaves = [b"x", b"y"]
        root = _merkle_root(leaves)
        path = _merkle_inclusion_path(leaves, 0)
        assert _signing.verify_merkle_inclusion_proof(
            b"\x00" * 31, 0, 2, path, root,
        ) is False

    def test_non_32_byte_sibling_refused(self) -> None:
        leaves = [f"l{i}".encode() for i in range(4)]
        root = _merkle_root(leaves)
        path = _merkle_inclusion_path(leaves, 1)
        path[0] = b"\x00" * 16  # wrong length
        assert _signing.verify_merkle_inclusion_proof(
            _leaf_hash(leaves[1]), 1, 4, path, root,
        ) is False


# ---------------------------------------------------------------------------
# Checkpoint parsing
# ---------------------------------------------------------------------------


class TestCheckpointParsing:
    def test_well_formed_round_trip(self) -> None:
        key = Ed25519PrivateKey.generate()
        root = b"\x11" * 32
        text = _sign_checkpoint_ed25519(
            origin="rekor.test - 0001",
            tree_size=42,
            root_hash=root,
            signer_name="rekor.test",
            key=key,
        )
        parsed = _signing.parse_rekor_checkpoint(text)
        assert parsed["origin"] == "rekor.test - 0001"
        assert parsed["tree_size"] == 42
        assert parsed["root_hash"] == root
        assert len(parsed["signatures"]) == 1
        name, key_hash, sig_bytes = parsed["signatures"][0]
        assert name == "rekor.test"
        assert len(key_hash) == 4
        assert len(sig_bytes) == 64  # Ed25519 sig

    def test_non_integer_tree_size_refused(self) -> None:
        text = (
            "origin\nnot-a-number\n"
            + base64.standard_b64encode(b"\x00" * 32).decode()
            + "\n\n— name "
            + base64.standard_b64encode(b"\x00" * 4 + b"\x00" * 64).decode()
            + "\n"
        )
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.parse_rekor_checkpoint(text)
        assert exc_info.value.reason == "checkpoint_malformed"

    def test_short_root_hash_refused(self) -> None:
        text = (
            "origin\n1\n"
            + base64.standard_b64encode(b"\x00" * 16).decode()
            + "\n\n— name "
            + base64.standard_b64encode(b"\x00" * 4 + b"\x00" * 64).decode()
            + "\n"
        )
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.parse_rekor_checkpoint(text)
        assert exc_info.value.reason == "checkpoint_malformed"

    def test_missing_separator_refused(self) -> None:
        text = (
            "origin\n1\n"
            + base64.standard_b64encode(b"\x00" * 32).decode()
            + "\n— name AAAAAA\n"
        )
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.parse_rekor_checkpoint(text)
        assert exc_info.value.reason == "checkpoint_malformed"

    def test_no_signature_line_refused(self) -> None:
        text = (
            "origin\n1\n"
            + base64.standard_b64encode(b"\x00" * 32).decode()
            + "\n\n"
        )
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.parse_rekor_checkpoint(text)
        assert exc_info.value.reason == "checkpoint_unsigned"

    def test_non_em_dash_signature_refused(self) -> None:
        text = (
            "origin\n1\n"
            + base64.standard_b64encode(b"\x00" * 32).decode()
            + "\n\n- name "
            + base64.standard_b64encode(b"\x00" * 4 + b"\x00" * 64).decode()
            + "\n"
        )
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.parse_rekor_checkpoint(text)
        assert exc_info.value.reason == "checkpoint_malformed"


# ---------------------------------------------------------------------------
# Checkpoint signature verification — Ed25519
# ---------------------------------------------------------------------------


class TestCheckpointSignatureEd25519:
    def test_valid_signature_verifies(self) -> None:
        key = Ed25519PrivateKey.generate()
        root = b"\x22" * 32
        text = _sign_checkpoint_ed25519(
            origin="rekor.test - 0001", tree_size=10,
            root_hash=root, signer_name="rekor.test", key=key,
        )
        # Should not raise.
        _signing.verify_rekor_checkpoint(
            text, _pubkey_pem(key),
            expected_root_hash=root, expected_tree_size=10,
        )

    def test_wrong_key_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        other = Ed25519PrivateKey.generate()
        root = b"\x22" * 32
        text = _sign_checkpoint_ed25519(
            origin="rekor.test - 0001", tree_size=10,
            root_hash=root, signer_name="rekor.test", key=key,
        )
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_checkpoint(
                text, _pubkey_pem(other),
                expected_root_hash=root, expected_tree_size=10,
            )
        assert exc_info.value.reason == "checkpoint_bad_sig"

    def test_root_mismatch_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        root = b"\x22" * 32
        text = _sign_checkpoint_ed25519(
            origin="rekor.test", tree_size=10,
            root_hash=root, signer_name="rekor.test", key=key,
        )
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_checkpoint(
                text, _pubkey_pem(key),
                expected_root_hash=b"\x33" * 32, expected_tree_size=10,
            )
        assert exc_info.value.reason == "checkpoint_root_mismatch"

    def test_tree_size_mismatch_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        root = b"\x22" * 32
        text = _sign_checkpoint_ed25519(
            origin="rekor.test", tree_size=10,
            root_hash=root, signer_name="rekor.test", key=key,
        )
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_checkpoint(
                text, _pubkey_pem(key),
                expected_root_hash=root, expected_tree_size=11,
            )
        assert exc_info.value.reason == "checkpoint_root_mismatch"


# ---------------------------------------------------------------------------
# Checkpoint signature verification — ECDSA-P256
# ---------------------------------------------------------------------------


class TestCheckpointSignatureECDSA:
    def test_valid_signature_verifies(self) -> None:
        key = ec.generate_private_key(ec.SECP256R1())
        root = b"\x55" * 32
        text = _sign_checkpoint_ecdsa(
            origin="rekor.test", tree_size=10,
            root_hash=root, signer_name="rekor.test", key=key,
        )
        _signing.verify_rekor_checkpoint(
            text, _pubkey_pem(key),
            expected_root_hash=root, expected_tree_size=10,
        )

    def test_wrong_key_refused(self) -> None:
        key = ec.generate_private_key(ec.SECP256R1())
        other = ec.generate_private_key(ec.SECP256R1())
        root = b"\x55" * 32
        text = _sign_checkpoint_ecdsa(
            origin="rekor.test", tree_size=10,
            root_hash=root, signer_name="rekor.test", key=key,
        )
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_checkpoint(
                text, _pubkey_pem(other),
                expected_root_hash=root, expected_tree_size=10,
            )
        assert exc_info.value.reason == "checkpoint_bad_sig"


# ---------------------------------------------------------------------------
# Full response verification (end-to-end)
# ---------------------------------------------------------------------------


def _build_rekor_response(
    *,
    leaves: list[bytes],
    target_index: int,
    log_key,
    sign_fn=_sign_checkpoint_ed25519,
    origin: str = "rekor.test - 0001",
    signer_name: str = "rekor.test",
) -> dict:
    root = _merkle_root(leaves)
    path = _merkle_inclusion_path(leaves, target_index)
    checkpoint = sign_fn(
        origin=origin, tree_size=len(leaves), root_hash=root,
        signer_name=signer_name, key=log_key,
    )
    body_b64 = base64.standard_b64encode(leaves[target_index]).decode("ascii")
    return {
        "body": body_b64,
        "integratedTime": 1700000000,
        "logIndex": target_index,
        "logID": "deadbeef",
        "verification": {
            "inclusionProof": {
                "checkpoint": checkpoint,
                "hashes": [h.hex() for h in path],
                "logIndex": target_index,
                "rootHash": root.hex(),
                "treeSize": len(leaves),
            },
        },
    }


class TestFullResponseVerification:
    def test_happy_path_ed25519(self) -> None:
        key = Ed25519PrivateKey.generate()
        leaves = [f"e{i}".encode() for i in range(11)]
        resp = _build_rekor_response(leaves=leaves, target_index=7, log_key=key)
        assert _signing.verify_rekor_inclusion(resp, _pubkey_pem(key)) is True

    def test_happy_path_ecdsa(self) -> None:
        key = ec.generate_private_key(ec.SECP256R1())
        leaves = [f"e{i}".encode() for i in range(11)]
        resp = _build_rekor_response(
            leaves=leaves, target_index=7, log_key=key,
            sign_fn=_sign_checkpoint_ecdsa,
        )
        assert _signing.verify_rekor_inclusion(resp, _pubkey_pem(key)) is True

    def test_tree_size_one(self) -> None:
        key = Ed25519PrivateKey.generate()
        leaves = [b"only"]
        resp = _build_rekor_response(leaves=leaves, target_index=0, log_key=key)
        assert _signing.verify_rekor_inclusion(resp, _pubkey_pem(key)) is True

    def test_root_mismatch_proof_vs_checkpoint_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        leaves = [f"e{i}".encode() for i in range(8)]
        resp = _build_rekor_response(leaves=leaves, target_index=3, log_key=key)
        resp["verification"]["inclusionProof"]["rootHash"] = "ff" * 32
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_inclusion(resp, _pubkey_pem(key))
        # Either the merkle walk reaches a different root, or the
        # checkpoint cross-check refuses.
        assert exc_info.value.reason in (
            "merkle_root_mismatch", "checkpoint_root_mismatch",
        )

    def test_tampered_leaf_body_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        leaves = [f"e{i}".encode() for i in range(8)]
        resp = _build_rekor_response(leaves=leaves, target_index=3, log_key=key)
        resp["body"] = base64.standard_b64encode(b"forged").decode("ascii")
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_inclusion(resp, _pubkey_pem(key))
        assert exc_info.value.reason == "merkle_root_mismatch"

    def test_missing_verification_block_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        leaves = [f"e{i}".encode() for i in range(4)]
        resp = _build_rekor_response(leaves=leaves, target_index=0, log_key=key)
        del resp["verification"]
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_inclusion(resp, _pubkey_pem(key))
        assert exc_info.value.reason == "missing_proof"

    def test_unsigned_checkpoint_refused(self) -> None:
        """Checkpoint signed by attacker's key, not the real log key."""
        real_key = Ed25519PrivateKey.generate()
        attacker = Ed25519PrivateKey.generate()
        leaves = [f"e{i}".encode() for i in range(8)]
        resp = _build_rekor_response(leaves=leaves, target_index=2, log_key=attacker)
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_inclusion(resp, _pubkey_pem(real_key))
        assert exc_info.value.reason == "checkpoint_bad_sig"

    def test_log_index_mismatch_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        leaves = [f"e{i}".encode() for i in range(8)]
        resp = _build_rekor_response(leaves=leaves, target_index=3, log_key=key)
        resp["verification"]["inclusionProof"]["logIndex"] = 5
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_inclusion(resp, _pubkey_pem(key))
        assert exc_info.value.reason == "merkle_root_mismatch"

    def test_tree_size_mismatch_proof_vs_checkpoint_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        leaves = [f"e{i}".encode() for i in range(8)]
        resp = _build_rekor_response(leaves=leaves, target_index=3, log_key=key)
        resp["verification"]["inclusionProof"]["treeSize"] = 12
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_inclusion(resp, _pubkey_pem(key))
        # Either merkle walk fails or checkpoint tree_size mismatch fires.
        assert exc_info.value.reason in (
            "merkle_root_mismatch", "checkpoint_root_mismatch",
        )

    def test_missing_body_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        leaves = [f"e{i}".encode() for i in range(4)]
        resp = _build_rekor_response(leaves=leaves, target_index=0, log_key=key)
        del resp["body"]
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_inclusion(resp, _pubkey_pem(key))
        assert exc_info.value.reason == "malformed_proof"


# ---------------------------------------------------------------------------
# Post-review hardening regressions
# ---------------------------------------------------------------------------


class TestEcCurveRestriction:
    """M1: ECDSA log keys must be P-256. Other curves surface as
    unsupported_key with a precise reason, not a generic bad-sig."""

    def test_p384_log_key_refused(self) -> None:
        key = ec.generate_private_key(ec.SECP384R1())
        cp = _sign_checkpoint_ecdsa(
            origin="rekor.test", tree_size=10, root_hash=b"\x55" * 32,
            signer_name="rekor.test", key=key,
        )
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_checkpoint(
                cp, _pubkey_pem(key),
                expected_root_hash=b"\x55" * 32, expected_tree_size=10,
            )
        assert exc_info.value.reason == "unsupported_key"


class TestL2StrictIntegerParsing:
    """L2: hostile Rekor responses with float / bool logIndex / treeSize
    surface as ``malformed_proof``, not as a misleading
    ``merkle_root_mismatch``."""

    def _build(self, log_key):
        leaves = [f"e{i}".encode() for i in range(8)]
        root = _merkle_root(leaves)
        path = _merkle_inclusion_path(leaves, 3)
        cp = _sign_checkpoint_ed25519(
            origin="rekor.test", tree_size=len(leaves),
            root_hash=root, signer_name="rekor.test", key=log_key,
        )
        return {
            "body": base64.standard_b64encode(leaves[3]).decode("ascii"),
            "integratedTime": 1700000000,
            "logIndex": 3,
            "logID": "deadbeef",
            "verification": {
                "inclusionProof": {
                    "checkpoint": cp,
                    "hashes": [h.hex() for h in path],
                    "logIndex": 3,
                    "rootHash": root.hex(),
                    "treeSize": len(leaves),
                },
            },
        }

    def test_float_log_index_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        resp = self._build(key)
        resp["verification"]["inclusionProof"]["logIndex"] = 3.5
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_inclusion(resp, _pubkey_pem(key))
        assert exc_info.value.reason == "malformed_proof"

    def test_bool_log_index_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        resp = self._build(key)
        resp["verification"]["inclusionProof"]["logIndex"] = True
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_inclusion(resp, _pubkey_pem(key))
        assert exc_info.value.reason == "malformed_proof"

    def test_float_tree_size_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        resp = self._build(key)
        resp["verification"]["inclusionProof"]["treeSize"] = 8.0
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_inclusion(resp, _pubkey_pem(key))
        assert exc_info.value.reason == "malformed_proof"


class TestL3CarriageReturnInCheckpoint:
    """L3: a checkpoint body containing CR (e.g., proxy that rewrote
    LF→CRLF) surfaces as ``checkpoint_malformed`` rather than the more
    misleading ``checkpoint_bad_sig`` after the signed bytes mismatch."""

    def test_cr_in_body_refused(self) -> None:
        key = Ed25519PrivateKey.generate()
        text = _sign_checkpoint_ed25519(
            origin="rekor.test", tree_size=10, root_hash=b"\x22" * 32,
            signer_name="rekor.test", key=key,
        )
        # Inject CR into the body half.
        idx = text.index("\n\n")
        body = text[:idx + 1].replace("\n", "\r\n")
        corrupted = body + text[idx + 1:]
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.parse_rekor_checkpoint(corrupted)
        assert exc_info.value.reason == "checkpoint_malformed"


class TestM4UuidValidation:
    """M4: a hostile Rekor returning a uuid with path-traversal /
    query-string characters cannot smuggle requests via
    fetch_inclusion_proof's URL substitution."""

    def test_uuid_with_query_string_refused(self) -> None:
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.fetch_inclusion_proof(
                "deadbeef?evil=1",
                "https://rekor.test.example/api/v1/log/entries",
            )
        assert exc_info.value.reason == "malformed_proof"

    def test_uuid_with_path_traversal_refused(self) -> None:
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.fetch_inclusion_proof(
                "../etc/passwd",
                "https://rekor.test.example/api/v1/log/entries",
            )
        assert exc_info.value.reason == "malformed_proof"

    def test_uuid_non_hex_refused(self) -> None:
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.fetch_inclusion_proof(
                "abc-not-hex",
                "https://rekor.test.example/api/v1/log/entries",
            )
        assert exc_info.value.reason == "malformed_proof"

    def test_uuid_hex_with_tree_id_accepted(self) -> None:
        """The tree-id-prefixed form ``<treehex>-<entryhex>`` is the
        Rekor shard-aware uuid shape and must NOT be rejected by the
        validator."""
        # Construct a valid hex tree-id-prefixed uuid; the actual
        # network call will fail (no mock) — we only test that it
        # passes the regex.
        import httpx as _httpx
        try:
            _signing.fetch_inclusion_proof(
                "1234567890abcdef-fedcba0987654321",
                "https://rekor.test.example/api/v1/log/entries",
            )
        except _signing.RekorInclusionError as exc:
            # Must be a network failure (missing_proof), not a
            # malformed_proof from regex refusal.
            assert exc.reason in ("missing_proof", "malformed_proof"), (
                f"expected network failure, got reason={exc.reason!r}"
            )
            # If it's malformed_proof, it must not be from the uuid
            # regex — message text should NOT mention uuid format.
            if exc.reason == "malformed_proof":
                assert "hex string" not in str(exc), (
                    f"valid tree-id uuid was rejected by regex: {exc}"
                )


class TestL1RekorUrlRevalidation:
    """L1: fetch_inclusion_proof + fetch_log_pubkey re-validate
    rekor_url against the SSRF / scheme defense, not just rely on
    mareforma.open() having done so. Direct callers (tests, scripts)
    can't bypass."""

    def test_fetch_inclusion_proof_refuses_http(self) -> None:
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.fetch_inclusion_proof(
                "deadbeef" * 8,
                "http://rekor.test.example/api/v1/log/entries",
            )
        assert exc_info.value.reason == "malformed_proof"

    def test_fetch_log_pubkey_refuses_loopback(self) -> None:
        with pytest.raises(_signing.SigningError):
            _signing.fetch_log_pubkey("https://127.0.0.1/api/v1/log/entries")

    def test_fetch_inclusion_proof_refuses_loopback(self) -> None:
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.fetch_inclusion_proof(
                "deadbeef" * 8,
                "https://localhost/api/v1/log/entries",
            )
        assert exc_info.value.reason == "malformed_proof"


class TestH1ExceptionContract:
    """H1: verify_rekor_inclusion's base64-fallback path must re-raise
    the documented RekorInclusionError, not the raw decode exception
    it catches in the inner except."""

    def test_unparseable_checkpoint_surfaces_typed_error(self) -> None:
        """A checkpoint that fails both direct-parse AND base64-decode
        re-raises RekorInclusionError, not ValueError/UnicodeDecodeError."""
        key = Ed25519PrivateKey.generate()
        leaves = [f"e{i}".encode() for i in range(4)]
        root = _merkle_root(leaves)
        path = _merkle_inclusion_path(leaves, 1)
        # Checkpoint is structurally invalid AND not valid base64
        # (contains characters outside the b64 alphabet).
        bad_cp = "garbage that won't parse @@@@ ###"
        resp = {
            "body": base64.standard_b64encode(leaves[1]).decode("ascii"),
            "integratedTime": 1700000000,
            "logIndex": 1,
            "logID": "deadbeef",
            "verification": {
                "inclusionProof": {
                    "checkpoint": bad_cp,
                    "hashes": [h.hex() for h in path],
                    "logIndex": 1,
                    "rootHash": root.hex(),
                    "treeSize": 4,
                },
            },
        }
        # Must raise RekorInclusionError, never ValueError /
        # binascii.Error / UnicodeDecodeError leaking from the fallback.
        with pytest.raises(_signing.RekorInclusionError) as exc_info:
            _signing.verify_rekor_inclusion(resp, _pubkey_pem(key))
        # The reason should be checkpoint_malformed (from the inner
        # parser), preserving the original failure type rather than
        # masquerading as a decode error.
        assert exc_info.value.reason == "checkpoint_malformed"
