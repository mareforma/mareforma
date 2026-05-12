"""
tests/test_signing_hardening.py — regression suite for the adversarial-review fixes.

One-finding-per-class, names map back to the review report:

  Finding 1   update_claim refuses to mutate signed-surface fields
  Finding 2   submit_to_rekor verifies the response body records our submission
  Finding 3   refresh_unsigned refuses to log a stale signature for a drifted row
  Finding 4   mark_claim_logged rejects a bundle whose payload claim_id mismatches
  Finding 5+6 save_private_key tightens parent dir to 0o700;
              bootstrap_key uses O_CREAT|O_EXCL to close the TOCTOU race
  Finding 7   submit_to_rekor caps Rekor response size
  Finding 10  validate_rekor_url rejects non-https + private IPs; opt-out works
  Finding 12  one peer logged + one peer not → neither REPLICATES; both promote after refresh
  Finding 13  refresh_unsigned warns + skips on keyid mismatch (key rotation)
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import stat
import threading
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

import mareforma
from mareforma import signing as _signing
from mareforma.db import (
    DatabaseError,
    SignedClaimImmutableError,
    add_claim,
    get_claim,
    list_unlogged_claims,
    mark_claim_logged,
    open_db,
    update_claim,
)


_TEST_REKOR_URL = "https://rekor.test.example/api/v1/log/entries"


def _bootstrap_key(tmp_path: Path) -> Path:
    key_path = tmp_path / "mareforma.key"
    _signing.bootstrap_key(key_path)
    return key_path


def _rekor_mirror(httpx_mock, *, uuid_prefix: str = "h") -> None:
    """Reusable Rekor 201 callback that mirrors the submission's hash + sig."""
    counter = {"n": 0}

    def callback(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        spec = body["spec"]
        counter["n"] += 1
        record = {
            "apiVersion": "0.0.1",
            "kind": "hashedrekord",
            "spec": {
                "data": {
                    "hash": {
                        "algorithm": "sha256",
                        "value": spec["data"]["hash"]["value"],
                    },
                },
                "signature": {
                    "content": spec["signature"]["content"],
                    "publicKey": {"content": "<not-checked>"},
                },
            },
        }
        encoded = base64.standard_b64encode(
            json.dumps(record, separators=(",", ":")).encode("utf-8"),
        ).decode("ascii")
        return httpx.Response(
            201,
            json={
                f"{uuid_prefix}-{counter['n']}": {
                    "body": encoded,
                    "integratedTime": 1700000000 + counter["n"],
                    "logIndex": counter["n"],
                }
            },
        )

    httpx_mock.add_callback(
        callback, method="POST", url=_TEST_REKOR_URL, is_reusable=True,
    )


# ---------------------------------------------------------------------------
# Finding 1 — update_claim signed-surface immutability
# ---------------------------------------------------------------------------

class TestUpdateClaimSignedSurface:
    def test_text_change_on_signed_claim_raises(self, tmp_path):
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("original text")
        conn = open_db(tmp_path)
        try:
            with pytest.raises(SignedClaimImmutableError, match="signed"):
                update_claim(conn, tmp_path, claim_id, text="tampered text")
        finally:
            conn.close()

    def test_supports_change_on_signed_claim_raises(self, tmp_path):
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("anchor", supports=["upstream-1"])
        conn = open_db(tmp_path)
        try:
            with pytest.raises(SignedClaimImmutableError):
                update_claim(conn, tmp_path, claim_id, supports=["upstream-2"])
        finally:
            conn.close()

    def test_contradicts_change_on_signed_claim_raises(self, tmp_path):
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("anchor")
        conn = open_db(tmp_path)
        try:
            with pytest.raises(SignedClaimImmutableError):
                update_claim(conn, tmp_path, claim_id, contradicts=["xx"])
        finally:
            conn.close()

    def test_status_change_on_signed_claim_allowed(self, tmp_path):
        """status is not part of the signed payload — must still be editable."""
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("retract me")
        conn = open_db(tmp_path)
        try:
            update_claim(conn, tmp_path, claim_id, status="retracted")
            assert get_claim(conn, claim_id)["status"] == "retracted"
        finally:
            conn.close()

    def test_comparison_summary_on_signed_claim_allowed(self, tmp_path):
        """comparison_summary is not part of the signed payload."""
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("with summary")
        conn = open_db(tmp_path)
        try:
            update_claim(
                conn, tmp_path, claim_id, comparison_summary="reviewed 2026-05-12",
            )
            assert get_claim(conn, claim_id)["comparison_summary"] == "reviewed 2026-05-12"
        finally:
            conn.close()

    def test_unsigned_claim_can_still_mutate_freely(self, tmp_path):
        conn = open_db(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "unsigned")
            update_claim(conn, tmp_path, claim_id, text="freely edited")
            assert get_claim(conn, claim_id)["text"] == "freely edited"
        finally:
            conn.close()

    def test_redundant_signed_field_set_is_a_noop(self, tmp_path):
        """Passing supports=<existing supports> on a signed claim must NOT raise.

        The refuse logic compares old vs new; identical values shouldn't trip it.
        """
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("redundant", supports=["u1"])
        conn = open_db(tmp_path)
        try:
            update_claim(conn, tmp_path, claim_id, supports=["u1"], status="contested")
            assert get_claim(conn, claim_id)["status"] == "contested"
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Finding 2 — Rekor response verification
# ---------------------------------------------------------------------------

class TestRekorResponseVerification:
    def _build_envelope(self):
        key = _signing.generate_keypair()
        envelope = _signing.sign_claim(
            {
                "claim_id": "c-1",
                "text": "x",
                "classification": "INFERRED",
                "generated_by": "a",
                "supports": [],
                "contradicts": [],
                "source_name": None,
                "created_at": "2026-05-12T00:00:00+00:00",
            },
            key,
        )
        return key, envelope

    def test_hash_mismatch_in_response_is_rejected(self, httpx_mock):
        key, envelope = self._build_envelope()
        # Build a Rekor body claiming a DIFFERENT hash.
        record = {
            "apiVersion": "0.0.1",
            "kind": "hashedrekord",
            "spec": {
                "data": {"hash": {"algorithm": "sha256", "value": "00" * 32}},
                "signature": {
                    "content": envelope["signatures"][0]["sig"],
                    "publicKey": {"content": "x"},
                },
            },
        }
        encoded = base64.standard_b64encode(
            json.dumps(record).encode("utf-8"),
        ).decode("ascii")
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL,
            status_code=201, json={"u": {"body": encoded, "logIndex": 1}},
        )
        logged, entry = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url=_TEST_REKOR_URL,
        )
        assert logged is False
        assert entry is None

    def test_signature_mismatch_in_response_is_rejected(self, httpx_mock):
        key, envelope = self._build_envelope()
        payload_bytes = base64.standard_b64decode(envelope["payload"])
        true_hash = hashlib.sha256(payload_bytes).hexdigest()
        record = {
            "apiVersion": "0.0.1",
            "kind": "hashedrekord",
            "spec": {
                "data": {"hash": {"algorithm": "sha256", "value": true_hash}},
                "signature": {
                    "content": base64.standard_b64encode(b"not-our-sig").decode("ascii"),
                    "publicKey": {"content": "x"},
                },
            },
        }
        encoded = base64.standard_b64encode(
            json.dumps(record).encode("utf-8"),
        ).decode("ascii")
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL,
            status_code=201, json={"u": {"body": encoded, "logIndex": 1}},
        )
        logged, _ = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url=_TEST_REKOR_URL,
        )
        assert logged is False

    def test_unparseable_response_body_is_rejected(self, httpx_mock):
        key, envelope = self._build_envelope()
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL,
            status_code=201, json={"u": {"body": "not-base64!", "logIndex": 1}},
        )
        logged, _ = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url=_TEST_REKOR_URL,
        )
        assert logged is False


# ---------------------------------------------------------------------------
# Finding 3 — refresh_unsigned drift guard
# ---------------------------------------------------------------------------

class TestRefreshUnsignedDrift:
    def test_drifted_row_is_not_submitted_to_rekor(self, tmp_path, httpx_mock):
        """A row whose text was tampered after assert_claim must not be
        logged to Rekor by refresh_unsigned — the public log would otherwise
        cement a stale signature."""
        # Initial Rekor down so the claim lands transparency_logged=0.
        httpx_mock.add_response(method="POST", url=_TEST_REKOR_URL, status_code=503)

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path, key_path=key_path, rekor_url=_TEST_REKOR_URL,
        ) as graph:
            claim_id = graph.assert_claim("original text")
            assert graph.get_claim(claim_id)["transparency_logged"] == 0

        # Tamper at the sqlite layer (simulates corruption or attacker).
        import sqlite3
        conn = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        conn.execute(
            "UPDATE claims SET text = ? WHERE claim_id = ?",
            ("tampered text", claim_id),
        )
        conn.commit()
        conn.close()

        # refresh_unsigned: even if Rekor would have accepted, the drift
        # check must fire first. No Rekor request should be made.
        with pytest.warns(UserWarning, match="drifted"):
            with mareforma.open(
                tmp_path, key_path=key_path, rekor_url=_TEST_REKOR_URL,
            ) as graph:
                result = graph.refresh_unsigned()

        assert result == {"checked": 1, "logged": 0, "still_unlogged": 1}
        # No POST should have been issued.
        rekor_posts = [
            r for r in httpx_mock.get_requests() if r.method == "POST"
        ]
        # Only the original failed POST during assert_claim — no second one
        # from refresh_unsigned.
        assert len(rekor_posts) == 1


# ---------------------------------------------------------------------------
# Finding 4 — mark_claim_logged claim_id verification
# ---------------------------------------------------------------------------

class TestMarkClaimLoggedVerification:
    def test_bundle_for_wrong_claim_id_is_rejected(self, tmp_path):
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            alice_id = graph.assert_claim("alice claim")
            bob_id = graph.assert_claim("bob claim")
            alice_bundle = graph.get_claim(alice_id)["signature_bundle"]

        conn = open_db(tmp_path)
        try:
            with pytest.raises(DatabaseError, match="does not match"):
                mark_claim_logged(conn, tmp_path, bob_id, alice_bundle)
        finally:
            conn.close()

    def test_malformed_bundle_is_rejected(self, tmp_path):
        conn = open_db(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "unsigned host")
            with pytest.raises(DatabaseError, match="malformed bundle"):
                mark_claim_logged(conn, tmp_path, claim_id, "{not valid json")
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Findings 5 + 6 — key file + parent dir hardening
# ---------------------------------------------------------------------------

class TestKeyFileHardening:
    def test_parent_dir_is_0o700_on_posix(self, tmp_path):
        key_path = tmp_path / "deep" / "nest" / "mareforma" / "key"
        _signing.bootstrap_key(key_path)
        if os.name == "posix":
            mode = stat.S_IMODE(key_path.parent.stat().st_mode)
            assert mode == 0o700, f"expected 0o700, got {oct(mode)}"

    def test_bootstrap_concurrent_calls_only_one_wins(self, tmp_path):
        """Two threads racing on the same path: O_CREAT|O_EXCL must let
        exactly one succeed; the other raises SigningError. This closes
        the TOCTOU between exists() and rename."""
        key_path = tmp_path / "racy.key"
        results: list[object] = []

        def runner():
            try:
                _signing.bootstrap_key(key_path)
                results.append("ok")
            except _signing.SigningError as exc:
                results.append(exc)

        threads = [threading.Thread(target=runner) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        oks = [r for r in results if r == "ok"]
        errs = [r for r in results if isinstance(r, _signing.SigningError)]
        assert len(oks) == 1, f"expected exactly one winner, got {oks}"
        assert len(errs) == 3, f"expected three losers, got {errs}"

    def test_save_private_key_exclusive_refuses_to_replace(self, tmp_path):
        key_a = _signing.generate_keypair()
        key_b = _signing.generate_keypair()
        path = tmp_path / "key"
        _signing.save_private_key(key_a, path, exclusive=True)
        with pytest.raises(FileExistsError):
            _signing.save_private_key(key_b, path, exclusive=True)
        # First key still on disk, untouched.
        loaded = _signing.load_private_key(path)
        assert _signing.public_key_id(loaded.public_key()) == \
               _signing.public_key_id(key_a.public_key())


# ---------------------------------------------------------------------------
# Finding 7 — Rekor response size cap
# ---------------------------------------------------------------------------

class TestRekorResponseSizeCap:
    def test_huge_content_length_header_rejected(self, httpx_mock):
        key = _signing.generate_keypair()
        envelope = _signing.sign_claim(
            {"claim_id": "c", "text": "x", "classification": "INFERRED",
             "generated_by": "a", "supports": [], "contradicts": [],
             "source_name": None, "created_at": "2026-05-12T00:00:00+00:00"},
            key,
        )
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL, status_code=201,
            content=b"{}",
            headers={"content-length": "10485760"},  # 10 MiB
        )
        logged, _ = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url=_TEST_REKOR_URL,
        )
        assert logged is False

    def test_oversized_actual_body_rejected(self, httpx_mock):
        key = _signing.generate_keypair()
        envelope = _signing.sign_claim(
            {"claim_id": "c", "text": "x", "classification": "INFERRED",
             "generated_by": "a", "supports": [], "contradicts": [],
             "source_name": None, "created_at": "2026-05-12T00:00:00+00:00"},
            key,
        )
        # 128 KB filler — exceeds the 64 KB cap.
        huge = "A" * (128 * 1024)
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL, status_code=201,
            content=huge.encode("utf-8"),
            headers={"content-type": "application/json"},
        )
        logged, _ = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url=_TEST_REKOR_URL,
        )
        assert logged is False


# ---------------------------------------------------------------------------
# Finding 10 — rekor_url SSRF validation
# ---------------------------------------------------------------------------

class TestRekorUrlValidation:
    @pytest.mark.parametrize("url", [
        "http://rekor.example.com/api/v1/log/entries",   # not https
        "https://127.0.0.1/api/v1/log/entries",          # loopback
        "https://10.0.0.5/api/v1/log/entries",           # private RFC1918
        "https://192.168.1.1/api/v1/log/entries",        # private RFC1918
        "https://169.254.169.254/latest/meta-data",      # link-local (metadata)
    ])
    def test_unsafe_url_rejected_at_open(self, tmp_path, url):
        with pytest.raises(_signing.SigningError):
            mareforma.open(tmp_path, rekor_url=url)

    def test_https_dns_hostname_accepted(self, tmp_path):
        # DNS hostnames are allowed — TLS to the resolved host is the
        # actual authentication.
        with mareforma.open(
            tmp_path, rekor_url="https://rekor.sigstore.dev/api/v1/log/entries",
        ) as graph:
            assert graph._rekor_url.startswith("https://rekor.sigstore.dev")

    def test_trust_insecure_rekor_bypasses_validation(self, tmp_path):
        # Internal Rekor on a private IP works WITH the explicit opt-in.
        with mareforma.open(
            tmp_path,
            rekor_url="http://10.0.0.5/api/v1/log/entries",
            trust_insecure_rekor=True,
        ) as graph:
            assert graph._rekor_url == "http://10.0.0.5/api/v1/log/entries"


# ---------------------------------------------------------------------------
# Finding 12 — one peer logged, one peer not
# ---------------------------------------------------------------------------

class TestOnePeerLoggedOneNot:
    def test_neither_replicates_until_both_logged(self, tmp_path, httpx_mock):
        """Agent A succeeds at Rekor; agent B never does. Agent A is
        transparency_logged=1 alone, but REPLICATED requires a logged peer
        AND the new claim's transparency_logged=1 — agent B's continued
        unlogged state must keep both at PRELIMINARY."""
        key_path = _bootstrap_key(tmp_path)

        # First three POSTs: upstream OK (mirror), agent A OK (mirror),
        # agent B FAIL (503). We need to alternate.
        # Simpler: use add_response for explicit ordering.
        # 1: upstream  — success (mirror)
        # 2: agent A   — success (mirror)
        # 3: agent B   — 503
        def make_mirror_response(httpx_mock, count):
            # Queue `count` mirror responses
            for _ in range(count):
                # Each call adds one reusable callback? No — non-reusable.
                pass
        # Easier: register the mirror as reusable and then ALSO queue a 503.
        # pytest-httpx matches in FIFO order; reusable kicks in after queued
        # ones exhaust.

        # Plan:
        #   - first 2 POSTs satisfied by 2 mirror callbacks
        #   - third POST satisfied by a 503

        def one_shot_mirror(httpx_mock):
            def cb(request: httpx.Request) -> httpx.Response:
                body = json.loads(request.content)
                spec = body["spec"]
                record = {
                    "apiVersion": "0.0.1", "kind": "hashedrekord",
                    "spec": {
                        "data": {"hash": {
                            "algorithm": "sha256",
                            "value": spec["data"]["hash"]["value"],
                        }},
                        "signature": {
                            "content": spec["signature"]["content"],
                            "publicKey": {"content": "x"},
                        },
                    },
                }
                encoded = base64.standard_b64encode(
                    json.dumps(record).encode("utf-8"),
                ).decode("ascii")
                return httpx.Response(
                    201,
                    json={"uu": {"body": encoded, "logIndex": 1, "integratedTime": 1}},
                )
            httpx_mock.add_callback(
                cb, method="POST", url=_TEST_REKOR_URL,
            )

        one_shot_mirror(httpx_mock)  # upstream succeeds
        one_shot_mirror(httpx_mock)  # agent A succeeds
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL, status_code=503,
        )  # agent B fails

        with mareforma.open(
            tmp_path, key_path=key_path, rekor_url=_TEST_REKOR_URL,
        ) as graph:
            upstream = graph.assert_claim("upstream", generated_by="seed")
            id_a = graph.assert_claim(
                "agent A", supports=[upstream], generated_by="agent/a",
            )
            id_b = graph.assert_claim(
                "agent B", supports=[upstream], generated_by="agent/b",
            )

            # A is logged, B is not. The REPLICATED filter requires the
            # CURRENT claim's transparency_logged=1 — agent B's add_claim
            # never triggered convergence detection. So both PRELIMINARY.
            assert graph.get_claim(id_a)["transparency_logged"] == 1
            assert graph.get_claim(id_b)["transparency_logged"] == 0
            assert graph.get_claim(id_a)["support_level"] == "PRELIMINARY"
            assert graph.get_claim(id_b)["support_level"] == "PRELIMINARY"

            # When B's refresh_unsigned finally succeeds, both must promote.
            _rekor_mirror(httpx_mock, uuid_prefix="late-b")
            result = graph.refresh_unsigned()
            assert result["logged"] == 1  # only B was pending

            assert graph.get_claim(id_a)["support_level"] == "REPLICATED"
            assert graph.get_claim(id_b)["support_level"] == "REPLICATED"


# ---------------------------------------------------------------------------
# Finding 13 — keyid mismatch in refresh_unsigned
# ---------------------------------------------------------------------------

class TestKeyIdMismatchOnRefresh:
    def test_rotated_key_skipped_with_warning(self, tmp_path, httpx_mock):
        """Bootstrap key A, assert (Rekor down), bootstrap key B with
        --overwrite, then refresh_unsigned: the claim signed by A cannot
        be re-logged by B; refresh must warn and skip."""
        httpx_mock.add_response(method="POST", url=_TEST_REKOR_URL, status_code=503)

        key_path = tmp_path / "rotating.key"
        _signing.bootstrap_key(key_path)
        with mareforma.open(
            tmp_path, key_path=key_path, rekor_url=_TEST_REKOR_URL,
        ) as graph:
            claim_id = graph.assert_claim("signed by A")
            assert graph.get_claim(claim_id)["transparency_logged"] == 0

        # Rotate the key.
        _signing.bootstrap_key(key_path, overwrite=True)

        # Even if Rekor would have accepted, the keyid guard must fire first.
        with pytest.warns(UserWarning, match="signed by keyid"):
            with mareforma.open(
                tmp_path, key_path=key_path, rekor_url=_TEST_REKOR_URL,
            ) as graph:
                result = graph.refresh_unsigned()

        assert result == {"checked": 1, "logged": 0, "still_unlogged": 1}
        # Only the original failed POST — no retry POST from refresh.
        rekor_posts = [
            r for r in httpx_mock.get_requests() if r.method == "POST"
        ]
        assert len(rekor_posts) == 1
