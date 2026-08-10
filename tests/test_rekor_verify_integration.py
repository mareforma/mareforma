"""tests/test_rekor_verify_integration.py, end-to-end Merkle inclusion
verification through ``mareforma.open()``.

Drives the full submit-time saga with a mocked Rekor that returns both
the submit response AND the re-fetched inclusion proof, signed by a
known log key. Verifies:

  - happy path: verification passes, transparency_logged=1
  - failure: log signs the WRONG root → transparency_logged stays 0
  - failure: log key rotation → TOFU pin refuses
  - opt-out: rekor_log_pubkey_pem=None preserves the prior behavior
"""

from __future__ import annotations

import base64
import json

import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

import mareforma
from mareforma import signing as _signing


_TEST_REKOR_URL = "https://rekor.test.example/api/v1/log/entries"


# Reuse the Merkle helpers from the unit-test file.
from tests._helpers import _bootstrap_key, _rekor_response_for
from tests.test_rekor_verify import (
    _merkle_root, _merkle_inclusion_path,
    _sign_checkpoint_ed25519, _sign_checkpoint_ecdsa, _pubkey_pem,
)


def _build_rekor_get_response_with_proof(
    *, post_body_dict: dict, log_key,
    tree_leaves: int = 11, target_index: int = 5,
    sign_fn=_sign_checkpoint_ed25519,
    origin: str = "rekor.test - 0001",
    signer_name: str = "rekor.test",
) -> dict:
    """The body Rekor returns from GET /log/entries/{uuid}: same shape
    as POST but with a verification.inclusionProof block attached.

    ``post_body_dict`` is the dict whose uuid → entry, we pull the
    entry, place it at ``target_index`` in a synthetic tree of
    ``tree_leaves`` leaves, build the audit path, and sign a checkpoint
    over the resulting root.
    """
    uuid_key = next(iter(post_body_dict))
    entry = post_body_dict[uuid_key]
    # The leaf bytes for the inclusion proof are the base64-decoded
    # body bytes (i.e., the canonical record bytes).
    decoded_target = base64.standard_b64decode(entry["body"])
    # Build a tree with our target at target_index, fillers elsewhere.
    leaves = [f"filler-{i}".encode() for i in range(tree_leaves)]
    leaves[target_index] = decoded_target
    root = _merkle_root(leaves)
    path = _merkle_inclusion_path(leaves, target_index)
    checkpoint = sign_fn(
        origin=origin, tree_size=tree_leaves, root_hash=root,
        signer_name=signer_name, key=log_key,
    )
    return {
        uuid_key: {
            **entry,
            "verification": {
                "inclusionProof": {
                    "checkpoint": checkpoint,
                    "hashes": [h.hex() for h in path],
                    "logIndex": target_index,
                    "rootHash": root.hex(),
                    "treeSize": tree_leaves,
                },
            },
        },
    }


def _wire_rekor_mock(
    httpx_mock, *, log_key,
    sign_fn=_sign_checkpoint_ed25519,
    override_root: bytes | None = None,
    uuid: str = "abc01deadbeef02", log_index: int = 5,
) -> None:
    """Register POST + GET callbacks. Each POST mirrors the submission
    in its response (so submit_to_rekor passes), and each GET on the
    matching uuid returns a verification.inclusionProof signed by
    ``log_key``.

    If ``override_root`` is set, the checkpoint is signed over a
    DIFFERENT root than the proof actually proves, used to simulate
    a hostile log that signs an inconsistent checkpoint.
    """
    import httpx

    captured_entries: dict[str, dict] = {}

    def post_callback(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        spec = body["spec"]
        hash_value = spec["data"]["hash"]["value"]
        sig_content = spec["signature"]["content"]
        body_dict = _rekor_response_for(
            payload_hash=hash_value, sig_b64=sig_content,
            uuid=uuid, log_index=log_index,
        )
        captured_entries[uuid] = body_dict[uuid]
        return httpx.Response(201, json=body_dict)

    def get_callback(request: httpx.Request) -> httpx.Response:
        if uuid not in captured_entries:
            return httpx.Response(404, json={"error": "not found"})
        entry = captured_entries[uuid]
        wrapped = {uuid: entry}
        full = _build_rekor_get_response_with_proof(
            post_body_dict=wrapped, log_key=log_key,
            tree_leaves=11, target_index=log_index,
            sign_fn=sign_fn,
        )
        if override_root is not None:
            # Mutate the checkpoint to sign a different root.
            cp = sign_fn(
                origin="rekor.test - 0001", tree_size=11,
                root_hash=override_root, signer_name="rekor.test", key=log_key,
            )
            full[uuid]["verification"]["inclusionProof"]["checkpoint"] = cp
            # Leave the proof's hashes pointing at the real root, so the
            # checkpoint disagrees with the proof.
        return httpx.Response(200, json=full)

    # ``is_optional=True``: matching is allowed (auto-fetch happy path
    # consumes both); but if a test exercises the "TOFU fetch fails →
    # degrade to unverified" branch, the GET on the entry endpoint
    # never fires and httpx_mock would otherwise complain at teardown.
    httpx_mock.add_callback(
        post_callback, method="POST", url=_TEST_REKOR_URL,
        is_reusable=True, is_optional=True,
    )
    httpx_mock.add_callback(
        get_callback, method="GET",
        url=f"{_TEST_REKOR_URL}/{uuid}",
        is_reusable=True, is_optional=True,
    )


# ---------------------------------------------------------------------------
# The hostile-checkpoint wiring itself
# ---------------------------------------------------------------------------


class TestWireRekorMockOverrideRoot:
    @pytest.mark.parametrize("forged_root", [bytes(range(32)), b"REGEN"])
    def test_checkpoint_signs_the_given_root_not_the_proof_root(
        self, httpx_mock, forged_root,
    ):
        """``override_root`` is signed as-is, no byte string is special,
        and the proof's hashes keep pointing at the real root."""
        import httpx

        log_key = Ed25519PrivateKey.generate()
        _wire_rekor_mock(httpx_mock, log_key=log_key, override_root=forged_root)

        httpx.post(_TEST_REKOR_URL, json={
            "spec": {
                "data": {"hash": {"value": "ab" * 32}},
                "signature": {"content": "c2ln"},
            },
        })
        uuid = "abc01deadbeef02"
        proof = httpx.get(f"{_TEST_REKOR_URL}/{uuid}").json()[uuid][
            "verification"]["inclusionProof"]

        signed_root = proof["checkpoint"].splitlines()[2]
        assert signed_root == base64.standard_b64encode(forged_root).decode("ascii")
        assert bytes.fromhex(proof["rootHash"]) != forged_root


# ---------------------------------------------------------------------------
# Happy path, submit-time verification succeeds
# ---------------------------------------------------------------------------


class TestSubmitTimeVerificationHappy:
    def test_verified_inclusion_sets_transparency_flag(self, tmp_path, httpx_mock):
        log_key = Ed25519PrivateKey.generate()
        log_pem = _pubkey_pem(log_key)
        _wire_rekor_mock(httpx_mock, log_key=log_key)

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path,
            key_path=key_path,
            rekor_url=_TEST_REKOR_URL,
            trust_insecure_rekor=True,
            rekor_log_pubkey_pem=log_pem,
        ) as graph:
            cid = graph.assert_claim("verified finding", classification="ANALYTICAL")
            claim = graph.get_claim(cid)
        assert claim["transparency_logged"] == 1

    def test_ecdsa_log_key_supported(self, tmp_path, httpx_mock):
        log_key = ec.generate_private_key(ec.SECP256R1())
        log_pem = _pubkey_pem(log_key)
        _wire_rekor_mock(httpx_mock, log_key=log_key, sign_fn=_sign_checkpoint_ecdsa)

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path,
            key_path=key_path,
            rekor_url=_TEST_REKOR_URL,
            trust_insecure_rekor=True,
            rekor_log_pubkey_pem=log_pem,
        ) as graph:
            cid = graph.assert_claim("ecdsa verified", classification="ANALYTICAL")
            claim = graph.get_claim(cid)
        assert claim["transparency_logged"] == 1


# ---------------------------------------------------------------------------
# Adversarial, submit-time verification fails
# ---------------------------------------------------------------------------


class TestSubmitTimeVerificationAdversarial:
    def test_hostile_checkpoint_wrong_root_leaves_unlogged(self, tmp_path, httpx_mock):
        """Log signs an inconsistent checkpoint (different root than the
        proof's hashes reconstruct). Submit-time verification refuses;
        transparency_logged stays 0."""
        log_key = Ed25519PrivateKey.generate()
        log_pem = _pubkey_pem(log_key)
        _wire_rekor_mock(
            httpx_mock, log_key=log_key, override_root=b"\xff" * 32,
        )

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path,
            key_path=key_path,
            rekor_url=_TEST_REKOR_URL,
            trust_insecure_rekor=True,
            rekor_log_pubkey_pem=log_pem,
        ) as graph:
            with pytest.warns(UserWarning, match="inclusion-proof verification failed"):
                cid = graph.assert_claim("forged", classification="ANALYTICAL")
            claim = graph.get_claim(cid)
        # Claim persisted but transparency_logged=0; refresh_unsigned
        # would retry.
        assert claim["transparency_logged"] == 0

    def test_wrong_log_pubkey_leaves_unlogged(self, tmp_path, httpx_mock):
        """Caller pins a DIFFERENT log key than the one actually signing
        checkpoints. All proofs fail signature verification."""
        real_log_key = Ed25519PrivateKey.generate()
        wrong_log_key = Ed25519PrivateKey.generate()
        _wire_rekor_mock(httpx_mock, log_key=real_log_key)

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path,
            key_path=key_path,
            rekor_url=_TEST_REKOR_URL,
            trust_insecure_rekor=True,
            rekor_log_pubkey_pem=_pubkey_pem(wrong_log_key),
        ) as graph:
            with pytest.warns(UserWarning, match="inclusion-proof verification failed"):
                cid = graph.assert_claim("wrong key", classification="ANALYTICAL")
            claim = graph.get_claim(cid)
        assert claim["transparency_logged"] == 0


# ---------------------------------------------------------------------------
# TOFU pin
# ---------------------------------------------------------------------------


class TestTOFUPin:
    def test_first_use_persists_pin(self, tmp_path, httpx_mock):
        log_key = Ed25519PrivateKey.generate()
        log_pem = _pubkey_pem(log_key)
        _wire_rekor_mock(httpx_mock, log_key=log_key)
        key_path = _bootstrap_key(tmp_path)
        pin_path = tmp_path / ".mareforma" / "rekor_log_pubkey.pem"

        assert not pin_path.exists()
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
            rekor_log_pubkey_pem=log_pem,
        ) as graph:
            # Consume the wired POST + GET mocks so httpx_mock doesn't
            # complain about unused responses at teardown.
            graph.assert_claim("pin-check", classification="ANALYTICAL")
        assert pin_path.exists()
        assert pin_path.read_bytes().strip() == log_pem.strip()

    def test_second_use_loads_pinned(self, tmp_path, httpx_mock):
        log_key = Ed25519PrivateKey.generate()
        log_pem = _pubkey_pem(log_key)
        _wire_rekor_mock(httpx_mock, log_key=log_key)
        key_path = _bootstrap_key(tmp_path)
        pin_path = tmp_path / ".mareforma" / "rekor_log_pubkey.pem"
        pin_path.parent.mkdir(parents=True, exist_ok=True)
        pin_path.write_bytes(log_pem)

        # Re-open with NO explicit pubkey; the pinned file should drive verification.
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
        ) as graph:
            cid = graph.assert_claim("uses pinned", classification="ANALYTICAL")
            claim = graph.get_claim(cid)
        assert claim["transparency_logged"] == 1

    def test_rotation_silent_refused(self, tmp_path):
        """A different pubkey than the pinned one raises SigningError , 
        refusing silent log-operator-key rotation."""
        log_key_old = Ed25519PrivateKey.generate()
        log_key_new = Ed25519PrivateKey.generate()
        pin_path = tmp_path / ".mareforma" / "rekor_log_pubkey.pem"
        pin_path.parent.mkdir(parents=True, exist_ok=True)
        pin_path.write_bytes(_pubkey_pem(log_key_old))

        with pytest.raises(_signing.SigningError, match="pins a different key"):
            mareforma.open(
                tmp_path,
                rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
                rekor_log_pubkey_pem=_pubkey_pem(log_key_new),
            )

    def test_truncated_pin_refused_on_read(self, tmp_path):
        """A zero-byte pin (crash mid-write) must not be trusted: it
        would send every signed claim into inclusion-proof verification
        with an unparseable key and stall the project silently."""
        pin_path = tmp_path / ".mareforma" / "rekor_log_pubkey.pem"
        pin_path.parent.mkdir(parents=True, exist_ok=True)
        pin_path.write_bytes(b"")

        with pytest.raises(_signing.SigningError) as exc_info:
            mareforma.open(
                tmp_path,
                rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
            )
        assert str(pin_path) in str(exc_info.value)

    def test_failed_pin_write_leaves_no_stub_behind(self, tmp_path, monkeypatch):
        """A write that fails must remove the O_EXCL'd file. Leaving it
        behind pins an empty key that later opens read back and trust."""
        import os as _os

        def _short_write(fd, data):
            return 0

        monkeypatch.setattr(_os, "write", _short_write)
        pin_path = tmp_path / ".mareforma" / "rekor_log_pubkey.pem"

        with pytest.raises(OSError):
            mareforma.open(
                tmp_path,
                rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
                rekor_log_pubkey_pem=_pubkey_pem(Ed25519PrivateKey.generate()),
            )
        assert not pin_path.exists()

    def test_pem_and_path_mutually_exclusive(self, tmp_path):
        log_key = Ed25519PrivateKey.generate()
        pem_file = tmp_path / "x.pem"
        pem_file.write_bytes(_pubkey_pem(log_key))
        with pytest.raises(ValueError, match="mutually exclusive"):
            mareforma.open(
                tmp_path,
                rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
                rekor_log_pubkey_pem=_pubkey_pem(log_key),
                rekor_log_pubkey_path=pem_file,
            )


class TestOpenRefusesBeforeCreatingAnything:
    """An open() that refuses must leave no project and no live handle."""

    def test_pem_and_path_conflict_creates_no_project(self, tmp_path):
        log_key = Ed25519PrivateKey.generate()
        pem_file = tmp_path / "x.pem"
        pem_file.write_bytes(_pubkey_pem(log_key))

        with pytest.raises(ValueError, match="mutually exclusive"):
            mareforma.open(
                tmp_path,
                rekor_log_pubkey_pem=_pubkey_pem(log_key),
                rekor_log_pubkey_path=pem_file,
            )
        assert not (tmp_path / ".mareforma").exists()

    def test_unreadable_pubkey_path_raises_signing_error(self, tmp_path):
        """A missing PEM file is a typed SigningError, like every other
        pubkey failure in open(), not a bare FileNotFoundError."""
        with pytest.raises(_signing.SigningError) as exc_info:
            mareforma.open(
                tmp_path,
                rekor_log_pubkey_path=tmp_path / "absent.pem",
            )
        assert "absent.pem" in str(exc_info.value)
        assert not (tmp_path / ".mareforma").exists()

    def test_pin_mismatch_closes_the_connection(self, tmp_path, monkeypatch):
        """Checks that need the open store still must not strand the
        connection they opened when they refuse."""
        import sqlite3

        import mareforma.db as _db

        opened = []
        real_open_db = _db.open_db

        def _tracking_open_db(root, *args, **kwargs):
            conn = real_open_db(root, *args, **kwargs)
            opened.append(conn)
            return conn

        monkeypatch.setattr(_db, "open_db", _tracking_open_db)

        pin_path = tmp_path / ".mareforma" / "rekor_log_pubkey.pem"
        pin_path.parent.mkdir(parents=True, exist_ok=True)
        pin_path.write_bytes(_pubkey_pem(Ed25519PrivateKey.generate()))

        with pytest.raises(_signing.SigningError, match="different key"):
            mareforma.open(
                tmp_path,
                rekor_log_pubkey_pem=_pubkey_pem(Ed25519PrivateKey.generate()),
            )

        assert len(opened) == 1
        with pytest.raises(sqlite3.ProgrammingError):
            opened[0].execute("select 1")


# ---------------------------------------------------------------------------
# Opt-in vs opt-out, pubkey supplied vs omitted, pin behavior on first use
# ---------------------------------------------------------------------------


class TestOptInAndOptOut:
    def test_no_pubkey_means_no_verification_no_fetch(self, tmp_path, httpx_mock):
        """With ``rekor_url`` set but NO ``rekor_log_pubkey_pem`` /
        ``rekor_log_pubkey_path`` supplied (and no pinned file from a
        prior session), the graph does NOT auto-fetch the log
        pubkey and does NOT Merkle-verify inclusions. The claim still
        gets ``transparency_logged=1`` via the submit-time response
        binding, which catches the most common tampering classes; the
        residual "log forked or rotated after submit" risk is the
        documented opt-out posture in README "Limits of the Rekor
        integration". To opt in, pass ``rekor_log_pubkey_pem`` or
        ``rekor_log_pubkey_path``."""
        log_key = Ed25519PrivateKey.generate()  # not used by mareforma
        _wire_rekor_mock(httpx_mock, log_key=log_key)
        key_path = _bootstrap_key(tmp_path)
        pin_path = tmp_path / ".mareforma" / "rekor_log_pubkey.pem"

        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
        ) as graph:
            cid = graph.assert_claim("opted-out", classification="ANALYTICAL")
            claim = graph.get_claim(cid)
        # Submit-time binding still gates the row; transparency_logged=1.
        assert claim["transparency_logged"] == 1
        # No pin was persisted; no /publicKey fetch was attempted.
        assert not pin_path.exists()
        get_urls = [
            str(r.url) for r in httpx_mock.get_requests() if r.method == "GET"
        ]
        assert not any("/publicKey" in u for u in get_urls), (
            f"unexpected GET on /publicKey: {get_urls!r}"
        )

    def test_explicit_pubkey_pins_on_first_use(self, tmp_path, httpx_mock):
        """Passing ``rekor_log_pubkey_pem`` explicitly persists the
        bytes to ``.mareforma/rekor_log_pubkey.pem`` (the TOFU pin).
        Subsequent opens without the explicit kwarg load the pin and
        refuse silent rotation."""
        log_key = Ed25519PrivateKey.generate()
        log_pem = _pubkey_pem(log_key)
        _wire_rekor_mock(httpx_mock, log_key=log_key)
        key_path = _bootstrap_key(tmp_path)
        pin_path = tmp_path / ".mareforma" / "rekor_log_pubkey.pem"

        assert not pin_path.exists()
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
            rekor_log_pubkey_pem=log_pem,
        ) as graph:
            cid = graph.assert_claim("verified", classification="ANALYTICAL")
            claim = graph.get_claim(cid)
        assert claim["transparency_logged"] == 1
        assert pin_path.exists()
        assert pin_path.read_bytes() == log_pem

    def test_no_rekor_url_no_verification_no_fetch(self, tmp_path, httpx_mock):
        """When ``rekor_url`` is unset, Rekor is fully disabled, no
        submit, no fetch, no pin. The claim persists locally signed but
        ``transparency_logged=1`` (the default for non-Rekor flows)."""
        key_path = _bootstrap_key(tmp_path)
        pin_path = tmp_path / ".mareforma" / "rekor_log_pubkey.pem"
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            cid = graph.assert_claim("local only", classification="ANALYTICAL")
            claim = graph.get_claim(cid)
        # No Rekor wiring → transparency_logged defaults to 1
        # (REPLICATED gating skipped for non-Rekor signed claims).
        assert claim["transparency_logged"] == 1
        assert not pin_path.exists()
        # No HTTP requests issued.
        assert httpx_mock.get_requests() == []


# ---------------------------------------------------------------------------
# Post-review hardening regressions (integration-level)
# ---------------------------------------------------------------------------


class TestM2DerComparison:
    """M2: TOFU pin comparison uses canonical DER, so the same key with
    different PEM line-wrap width / line endings still matches."""

    def test_same_key_different_pem_formatting_accepted(self, tmp_path):
        log_key = Ed25519PrivateKey.generate()
        canonical_pem = _pubkey_pem(log_key)
        # Reformat the PEM to 32-char line wrap (instead of cryptography's
        # default 64). Same DER, different bytes.
        from cryptography.hazmat.primitives import serialization
        der = log_key.public_key().public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        b64 = base64.standard_b64encode(der).decode("ascii")
        reformatted = "-----BEGIN PUBLIC KEY-----\n"
        for i in range(0, len(b64), 32):
            reformatted += b64[i:i + 32] + "\n"
        reformatted += "-----END PUBLIC KEY-----\n"
        reformatted_pem = reformatted.encode("ascii")
        assert reformatted_pem.strip() != canonical_pem.strip()  # bytes differ
        key_path = _bootstrap_key(tmp_path)
        # First open: pin the canonical PEM.
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
            rekor_log_pubkey_pem=canonical_pem,
        ):
            pass
        # Second open: supply the differently-wrapped PEM. Must NOT
        # raise, DER bytes match.
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
            rekor_log_pubkey_pem=reformatted_pem,
        ):
            pass

    def test_different_key_refused_on_second_open(self, tmp_path):
        """Genuine key rotation IS refused, the operator must delete
        the pin file to rotate."""
        key_a = Ed25519PrivateKey.generate()
        key_b = Ed25519PrivateKey.generate()
        key_path = _bootstrap_key(tmp_path)

        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
            rekor_log_pubkey_pem=_pubkey_pem(key_a),
        ):
            pass
        with pytest.raises(_signing.SigningError, match="different key"):
            with mareforma.open(
                tmp_path, key_path=key_path,
                rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
                rekor_log_pubkey_pem=_pubkey_pem(key_b),
            ):
                pass


class TestM3AtomicPinWrite:
    """M3: pin write is atomic (O_CREAT|O_EXCL). A pin file that
    already exists is detected, caller is routed through the
    mismatch-check branch even on the "first use" code path."""

    def test_pre_existing_pin_file_triggers_mismatch_check(self, tmp_path):
        """Simulate a race winner: a pin file already exists holding a
        DIFFERENT key. Our open() with our key must surface as
        SigningError rather than overwrite the winner's pin."""
        race_winner_key = Ed25519PrivateKey.generate()
        our_key = Ed25519PrivateKey.generate()
        # Pre-write the race winner's pin to the canonical location.
        pin_path = tmp_path / ".mareforma" / "rekor_log_pubkey.pem"
        pin_path.parent.mkdir(parents=True, exist_ok=True)
        pin_path.write_bytes(_pubkey_pem(race_winner_key))
        key_path = _bootstrap_key(tmp_path)
        # Our open() with our DIFFERENT key must NOT silently clobber.
        with pytest.raises(_signing.SigningError, match="different key"):
            with mareforma.open(
                tmp_path, key_path=key_path,
                rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
                rekor_log_pubkey_pem=_pubkey_pem(our_key),
            ):
                pass
        # Race winner's pin is untouched.
        assert pin_path.read_bytes() == _pubkey_pem(race_winner_key)


class TestH2SidecarBeforeMarkClaim:
    """H2: refresh_unsigned's re-submit path writes the
    rekor_inclusions sidecar BEFORE calling mark_claim_logged. Without
    this, a mark_claim_logged failure (drift refusal, transient
    IntegrityError, contention) would leave the entry in Rekor with no
    local sidecar record; the next refresh_unsigned would re-submit
    and create a duplicate Rekor entry. Writing the sidecar first
    routes any retry through the saved-entry replay path."""

    def test_resubmit_path_writes_sidecar(self, tmp_path, httpx_mock):
        """Submit a claim while Rekor is down (claim persisted with
        transparency_logged=0). Bring Rekor back and run
        refresh_unsigned. Confirm the sidecar row gets written so a
        subsequent retry uses the saved-entry replay path."""
        log_key = Ed25519PrivateKey.generate()
        key_path = _bootstrap_key(tmp_path)
        # First open: Rekor 500s, so the claim is persisted unlogged.
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL,
            status_code=503, is_optional=True,
        )
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
        ) as graph:
            cid = graph.assert_claim("pending", classification="ANALYTICAL")
            assert graph.get_claim(cid)["transparency_logged"] == 0

        # Second open: Rekor is back. refresh_unsigned should
        # re-submit AND write the sidecar.
        _wire_rekor_mock(httpx_mock, log_key=log_key)
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
        ) as graph:
            result = graph.refresh_unsigned()
            assert result["logged"] == 1, result
            from mareforma import db as _db
            entry = _db.get_rekor_inclusion(graph._conn, cid)
            assert entry is not None, (
                "sidecar must be populated after re-submit so a retry "
                "can route through the saved-entry replay path"
            )
            assert "uuid" in entry


def _assert_claim_while_rekor_is_down(tmp_path, httpx_mock, key_path) -> str:
    """Persist one claim with transparency_logged=0 by 503ing the submit.

    The starting state for every refresh_unsigned case: the operator
    recovery path only has work to do once a claim is unlogged.
    """
    httpx_mock.add_response(
        method="POST", url=_TEST_REKOR_URL,
        status_code=503, is_optional=True,
    )
    with mareforma.open(
        tmp_path, key_path=key_path,
        rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
    ) as graph:
        cid = graph.assert_claim("pending", classification="ANALYTICAL")
        assert graph.get_claim(cid)["transparency_logged"] == 0
    return cid


class TestRefreshUnsignedVerification:
    """refresh_unsigned is the recovery path after a Rekor outage, so it
    is where a silent promotion would land: the row would read
    transparency_logged=1 with no verified inclusion proof behind it.
    When the graph carries a pinned log key, only a proof that verifies
    may promote the claim."""

    def test_verified_proof_promotes_and_reaches_the_sidecar(
        self, tmp_path, httpx_mock,
    ):
        log_key = Ed25519PrivateKey.generate()
        key_path = _bootstrap_key(tmp_path)
        cid = _assert_claim_while_rekor_is_down(tmp_path, httpx_mock, key_path)

        _wire_rekor_mock(httpx_mock, log_key=log_key)
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
            rekor_log_pubkey_pem=_pubkey_pem(log_key),
        ) as graph:
            assert graph.refresh_unsigned()["logged"] == 1
            assert graph.get_claim(cid)["transparency_logged"] == 1
            raw = graph._conn.execute(
                "SELECT raw_response_b64 FROM rekor_inclusions "
                "WHERE claim_id = ?", (cid,),
            ).fetchone()["raw_response_b64"]
        # The proof the recovery path verified is what restore reads.
        stored = json.loads(base64.standard_b64decode(raw))
        entry = next(iter(stored.values()))
        assert "inclusionProof" in entry["verification"]

    def test_hostile_checkpoint_leaves_the_claim_unlogged(
        self, tmp_path, httpx_mock,
    ):
        log_key = Ed25519PrivateKey.generate()
        key_path = _bootstrap_key(tmp_path)
        cid = _assert_claim_while_rekor_is_down(tmp_path, httpx_mock, key_path)

        # Rekor is back but signs a checkpoint over a root the proof's
        # hashes do not reconstruct.
        _wire_rekor_mock(
            httpx_mock, log_key=log_key, override_root=b"\xff" * 32,
        )
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
            rekor_log_pubkey_pem=_pubkey_pem(log_key),
        ) as graph:
            with pytest.warns(
                UserWarning, match="inclusion-proof verification.*failed",
            ):
                result = graph.refresh_unsigned()
            assert result == {"checked": 1, "logged": 0, "still_unlogged": 1}
            assert graph.get_claim(cid)["transparency_logged"] == 0

    def test_submit_response_without_uuid_leaves_the_claim_unlogged(
        self, tmp_path, httpx_mock,
    ):
        """No uuid means no re-fetch, so there is nothing to verify."""
        log_key = Ed25519PrivateKey.generate()
        key_path = _bootstrap_key(tmp_path)
        cid = _assert_claim_while_rekor_is_down(tmp_path, httpx_mock, key_path)

        _wire_rekor_mock(httpx_mock, log_key=log_key, uuid="")
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
            rekor_log_pubkey_pem=_pubkey_pem(log_key),
        ) as graph:
            with pytest.warns(UserWarning, match="response had no uuid"):
                result = graph.refresh_unsigned()
            assert result == {"checked": 1, "logged": 0, "still_unlogged": 1}
            assert graph.get_claim(cid)["transparency_logged"] == 0


class TestSubmitResponseWithoutUuid:
    """Submit-time counterpart: a response carrying no uuid cannot be
    re-fetched, so the inclusion proof the caller asked for can never be
    obtained. require_rekor=True refuses the write; otherwise the claim
    persists unlogged."""

    def test_require_rekor_raises(self, tmp_path, httpx_mock):
        log_key = Ed25519PrivateKey.generate()
        _wire_rekor_mock(httpx_mock, log_key=log_key, uuid="")

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
            rekor_log_pubkey_pem=_pubkey_pem(log_key),
            require_rekor=True,
        ) as graph:
            with pytest.raises(_signing.SigningError, match="no uuid"):
                graph.assert_claim("no uuid", classification="ANALYTICAL")

    def test_without_require_rekor_the_claim_stays_unlogged(
        self, tmp_path, httpx_mock,
    ):
        log_key = Ed25519PrivateKey.generate()
        _wire_rekor_mock(httpx_mock, log_key=log_key, uuid="")

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, trust_insecure_rekor=True,
            rekor_log_pubkey_pem=_pubkey_pem(log_key),
        ) as graph:
            cid = graph.assert_claim("no uuid", classification="ANALYTICAL")
            assert graph.get_claim(cid)["transparency_logged"] == 0


# ---------------------------------------------------------------------------
# The sidecar must carry a proof restore can verify
# ---------------------------------------------------------------------------


class TestSidecarHoldsTheInclusionProof:
    """The sidecar is what claims.toml carries into restore. When the graph
    was opened with a pinned log key, the proof it verified must be the thing
    stored, or restore under the same pinned key cannot verify anything."""

    def test_restore_verifies_the_sidecar_under_a_pinned_log_key(
        self, tmp_path, httpx_mock,
    ):
        log_key = Ed25519PrivateKey.generate()
        log_pem = _pubkey_pem(log_key)
        _wire_rekor_mock(httpx_mock, log_key=log_key)

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path,
            key_path=key_path,
            rekor_url=_TEST_REKOR_URL,
            trust_insecure_rekor=True,
            rekor_log_pubkey_pem=log_pem,
        ) as graph:
            cid = graph.assert_claim("witnessed finding", classification="ANALYTICAL")
            assert graph.get_claim(cid)["transparency_logged"] == 1

        db_dir = tmp_path / ".mareforma"
        for f in db_dir.iterdir():
            f.unlink()
        db_dir.rmdir()

        mareforma.restore(tmp_path, rekor_log_pubkey_pem=log_pem)

        with mareforma.open(tmp_path, key_path=key_path) as graph:
            assert graph.get_claim(cid)["transparency_logged"] == 1


# ---------------------------------------------------------------------------
# The proof must be about THIS claim
# ---------------------------------------------------------------------------


def _wire_foreign_entry_mock(
    httpx_mock, *, log_key, uuid: str = "abc01deadbeef02", log_index: int = 5,
) -> None:
    """POST mirrors the submission, so submit_to_rekor accepts it. The GET
    answers with a DIFFERENT entry that is genuinely in the log: a real audit
    path under a checkpoint signed by the real log key, recording material
    that belongs to another claim. This is what a hostile or compromised log
    returns to make a claim look witnessed after mutating its entry."""
    import httpx

    def post_callback(request: httpx.Request) -> httpx.Response:
        spec = json.loads(request.content)["spec"]
        return httpx.Response(201, json=_rekor_response_for(
            payload_hash=spec["data"]["hash"]["value"],
            sig_b64=spec["signature"]["content"],
            uuid=uuid, log_index=log_index,
        ))

    def get_callback(request: httpx.Request) -> httpx.Response:
        foreign = _rekor_response_for(
            payload_hash="ab" * 32, sig_b64="Zm9yZWlnbi1zaWduYXR1cmU=",
            uuid=uuid, log_index=log_index,
        )
        return httpx.Response(200, json=_build_rekor_get_response_with_proof(
            post_body_dict=foreign, log_key=log_key,
            tree_leaves=11, target_index=log_index,
        ))

    httpx_mock.add_callback(
        post_callback, method="POST", url=_TEST_REKOR_URL,
        is_reusable=True, is_optional=True,
    )
    httpx_mock.add_callback(
        get_callback, method="GET", url=f"{_TEST_REKOR_URL}/{uuid}",
        is_reusable=True, is_optional=True,
    )


class TestProofMustBindToTheClaim:
    def test_foreign_entry_leaves_the_claim_unlogged(self, tmp_path, httpx_mock):
        log_key = Ed25519PrivateKey.generate()
        _wire_foreign_entry_mock(httpx_mock, log_key=log_key)

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path,
            key_path=key_path,
            rekor_url=_TEST_REKOR_URL,
            trust_insecure_rekor=True,
            rekor_log_pubkey_pem=_pubkey_pem(log_key),
        ) as graph:
            with pytest.warns(UserWarning, match="inclusion-proof verification failed"):
                cid = graph.assert_claim("substituted", classification="ANALYTICAL")
            claim = graph.get_claim(cid)
        assert claim["transparency_logged"] == 0

    def test_require_rekor_raises_on_a_foreign_entry(self, tmp_path, httpx_mock):
        log_key = Ed25519PrivateKey.generate()
        _wire_foreign_entry_mock(httpx_mock, log_key=log_key)

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path,
            key_path=key_path,
            rekor_url=_TEST_REKOR_URL,
            trust_insecure_rekor=True,
            rekor_log_pubkey_pem=_pubkey_pem(log_key),
            require_rekor=True,
        ) as graph:
            with pytest.raises(_signing.SigningError, match="verification failed"):
                graph.assert_claim("substituted", classification="ANALYTICAL")
