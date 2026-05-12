"""
tests/test_rekor.py — Rekor transparency-log integration.

Covers:
  - submit_to_rekor happy path (hashedrekord POST → 201 with uuid/logIndex)
  - submit_to_rekor failure modes (network error, non-2xx, malformed JSON)
  - attach_rekor_entry preserves verification
  - assert_claim with rekor_url=None: transparency_logged auto-set to 1
  - assert_claim with rekor_url + signer + Rekor 200: transparency_logged=1,
    augmented bundle stored
  - assert_claim with rekor_url + signer + Rekor down: transparency_logged=0,
    bundle stored without rekor block
  - REPLICATED gating: signed-but-unlogged claim blocks convergence
  - refresh_unsigned retries pending and clears the flag on success
  - refresh_unsigned no-op when rekor_url is None
  - require_rekor=True without rekor_url raises at open() time
  - require_rekor=True with rekor_url that 500s raises at assert_claim time
"""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

import pytest

import mareforma
from mareforma import signing as _signing


_TEST_REKOR_URL = "https://rekor.test.example/api/v1/log/entries"


def _bootstrap_key(tmp_path: Path) -> Path:
    """Generate a key inside tmp_path and return its absolute path."""
    key_path = tmp_path / "mareforma.key"
    _signing.bootstrap_key(key_path)
    return key_path


def _rekor_response_for(
    *,
    payload_hash: str,
    sig_b64: str,
    uuid: str = "abc-uuid",
    log_index: int = 42,
    integrated_time: int = 1700000000,
) -> dict:
    """Build a realistic Rekor 201 body whose `body` field actually
    records the submitted hash + signature.

    submit_to_rekor now verifies the response — a generic mock without a
    matching body fails the equality check.
    """
    record = {
        "apiVersion": "0.0.1",
        "kind": "hashedrekord",
        "spec": {
            "data": {"hash": {"algorithm": "sha256", "value": payload_hash}},
            "signature": {
                "content": sig_b64,
                "publicKey": {"content": "<not-checked>"},
            },
        },
    }
    encoded = base64.standard_b64encode(
        json.dumps(record, separators=(",", ":")).encode("utf-8"),
    ).decode("ascii")
    return {
        uuid: {
            "body": encoded,
            "integratedTime": integrated_time,
            "logIndex": log_index,
        }
    }


def _hash_and_sig(envelope: dict) -> tuple[str, str]:
    """Extract (sha256 hex of payload, base64 sig) from an envelope."""
    payload_bytes = base64.standard_b64decode(envelope["payload"])
    return (
        hashlib.sha256(payload_bytes).hexdigest(),
        envelope["signatures"][0]["sig"],
    )


def _rekor_response_for_envelope(
    envelope: dict,
    *,
    uuid: str = "abc-uuid",
    log_index: int = 42,
) -> dict:
    payload_hash, sig_b64 = _hash_and_sig(envelope)
    return _rekor_response_for(
        payload_hash=payload_hash,
        sig_b64=sig_b64,
        uuid=uuid,
        log_index=log_index,
    )


def _mirror_rekor(httpx_mock, *, uuid_prefix: str = "m") -> None:
    """Register a reusable Rekor 201 callback that mirrors the inbound
    submission's hash + signature back in the response body.

    submit_to_rekor verifies that the returned entry actually records OUR
    submission; this helper produces a body that satisfies that check for
    any number of subsequent POSTs (auto-incrementing uuid + logIndex).
    """
    import httpx

    counter = {"n": 0}

    def callback(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        spec = body["spec"]
        hash_value = spec["data"]["hash"]["value"]
        sig_content = spec["signature"]["content"]
        counter["n"] += 1
        return httpx.Response(
            201,
            json=_rekor_response_for(
                payload_hash=hash_value,
                sig_b64=sig_content,
                uuid=f"{uuid_prefix}-{counter['n']}",
                log_index=counter["n"],
            ),
        )

    httpx_mock.add_callback(
        callback, method="POST", url=_TEST_REKOR_URL, is_reusable=True,
    )


# ---------------------------------------------------------------------------
# submit_to_rekor — direct
# ---------------------------------------------------------------------------

class TestSubmitToRekor:
    def test_happy_path(self, httpx_mock) -> None:
        key = _signing.generate_keypair()
        envelope = _signing.sign_claim(
            {
                "claim_id": "c-1",
                "text": "finding",
                "classification": "INFERRED",
                "generated_by": "agent",
                "supports": [],
                "contradicts": [],
                "source_name": None,
                "created_at": "2026-05-12T00:00:00+00:00",
            },
            key,
        )
        # submit_to_rekor verifies the response's encoded body records our
        # hash + sig, so the mock must be built from this envelope.
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL,
            status_code=201,
            json=_rekor_response_for_envelope(envelope, uuid="uuid-A", log_index=7),
        )
        logged, entry = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url=_TEST_REKOR_URL,
        )
        assert logged is True
        assert entry == {"uuid": "uuid-A", "integratedTime": 1700000000, "logIndex": 7}

    def test_network_error_returns_false(self, httpx_mock) -> None:
        import httpx as _httpx
        httpx_mock.add_exception(_httpx.ConnectError("down"))
        key = _signing.generate_keypair()
        envelope = _signing.sign_claim(
            {"claim_id": "c-2", "text": "x", "classification": "INFERRED",
             "generated_by": "a", "supports": [], "contradicts": [],
             "source_name": None, "created_at": "2026-05-12T00:00:00+00:00"},
            key,
        )
        logged, entry = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url=_TEST_REKOR_URL,
        )
        assert logged is False
        assert entry is None

    def test_5xx_returns_false(self, httpx_mock) -> None:
        httpx_mock.add_response(method="POST", url=_TEST_REKOR_URL, status_code=503)
        key = _signing.generate_keypair()
        envelope = _signing.sign_claim(
            {"claim_id": "c-3", "text": "x", "classification": "INFERRED",
             "generated_by": "a", "supports": [], "contradicts": [],
             "source_name": None, "created_at": "2026-05-12T00:00:00+00:00"},
            key,
        )
        logged, _ = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url=_TEST_REKOR_URL,
        )
        assert logged is False

    def test_attach_rekor_entry_preserves_signature(self) -> None:
        key = _signing.generate_keypair()
        envelope = _signing.sign_claim(
            {"claim_id": "c-4", "text": "x", "classification": "INFERRED",
             "generated_by": "a", "supports": [], "contradicts": [],
             "source_name": None, "created_at": "2026-05-12T00:00:00+00:00"},
            key,
        )
        augmented = _signing.attach_rekor_entry(
            envelope, {"uuid": "u", "integratedTime": 1, "logIndex": 0},
        )
        # Rekor block present; signature still verifies (was over the
        # original payload bytes, unchanged).
        assert augmented["rekor"]["uuid"] == "u"
        assert _signing.verify_envelope(augmented, key.public_key()) is True


# ---------------------------------------------------------------------------
# assert_claim integration
# ---------------------------------------------------------------------------

class TestAssertClaimWithRekor:
    def test_no_rekor_url_yields_transparency_logged_true_by_default(
        self, tmp_path: Path,
    ) -> None:
        """With rekor_url=None, the column defaults to 1 so REPLICATED is
        not gated even when claims are signed locally."""
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("local-only signed")
            claim = graph.get_claim(claim_id)
        assert claim["transparency_logged"] == 1
        assert claim["signature_bundle"] is not None

    def test_rekor_success_attaches_log_entry_and_flips_flag(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        _mirror_rekor(httpx_mock, uuid_prefix="first")
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path, key_path=key_path, rekor_url=_TEST_REKOR_URL,
        ) as graph:
            claim_id = graph.assert_claim("with rekor")
            claim = graph.get_claim(claim_id)

        assert claim["transparency_logged"] == 1
        envelope = json.loads(claim["signature_bundle"])
        assert envelope["rekor"]["uuid"] == "first-1"
        assert envelope["rekor"]["logIndex"] == 1

    def test_rekor_failure_persists_with_transparency_logged_false(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        """Mirror DOI fail-closed pattern: claim persists, REPLICATED waits."""
        httpx_mock.add_response(method="POST", url=_TEST_REKOR_URL, status_code=503)
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path, key_path=key_path, rekor_url=_TEST_REKOR_URL,
        ) as graph:
            claim_id = graph.assert_claim("rekor down")
            claim = graph.get_claim(claim_id)

        assert claim["transparency_logged"] == 0
        # Bundle is stored (signed locally) but lacks the rekor block.
        envelope = json.loads(claim["signature_bundle"])
        assert "rekor" not in envelope

    def test_unsigned_claim_stays_transparency_logged_true(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        """No signer → no Rekor submission attempt, transparency_logged=1."""
        with mareforma.open(
            tmp_path, key_path=tmp_path / "absent", rekor_url=_TEST_REKOR_URL,
        ) as graph:
            claim_id = graph.assert_claim("unsigned")
            claim = graph.get_claim(claim_id)
        assert claim["transparency_logged"] == 1
        assert claim["signature_bundle"] is None


# ---------------------------------------------------------------------------
# REPLICATED gating
# ---------------------------------------------------------------------------

class TestReplicatedGating:
    def test_signed_but_unlogged_blocks_replicated(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        """Two agents converge but one is still pending Rekor inclusion;
        both must stay PRELIMINARY until that one logs."""
        # Three claims will be asserted (upstream, agent A, agent B); Rekor
        # is down for all three.
        for _ in range(3):
            httpx_mock.add_response(
                method="POST", url=_TEST_REKOR_URL, status_code=503,
            )
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path, key_path=key_path, rekor_url=_TEST_REKOR_URL,
        ) as graph:
            upstream = graph.assert_claim("upstream", generated_by="seed")
            assert graph.get_claim(upstream)["transparency_logged"] == 0

            id_a = graph.assert_claim(
                "agent A", supports=[upstream], generated_by="agent/a",
            )
            id_b = graph.assert_claim(
                "agent B", supports=[upstream], generated_by="agent/b",
            )

            # Without Rekor confirmation, no REPLICATED promotion fires.
            assert graph.get_claim(id_a)["support_level"] == "PRELIMINARY"
            assert graph.get_claim(id_b)["support_level"] == "PRELIMINARY"

    def test_logged_claims_replicate_normally(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        _mirror_rekor(httpx_mock)
        key_path = _bootstrap_key(tmp_path)
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

            assert graph.get_claim(id_a)["support_level"] == "REPLICATED"
            assert graph.get_claim(id_b)["support_level"] == "REPLICATED"


# ---------------------------------------------------------------------------
# refresh_unsigned
# ---------------------------------------------------------------------------

class TestRefreshUnsigned:
    def test_refresh_unsigned_no_rekor_url_is_noop(self, tmp_path: Path) -> None:
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            graph.assert_claim("some claim")
            result = graph.refresh_unsigned()
        assert result == {"checked": 0, "logged": 0, "still_unlogged": 0}

    def test_refresh_unsigned_clears_pending_claims(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        # Initial: two assert_claim calls hit Rekor=down.
        httpx_mock.add_response(method="POST", url=_TEST_REKOR_URL, status_code=503)
        httpx_mock.add_response(method="POST", url=_TEST_REKOR_URL, status_code=503)
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path, key_path=key_path, rekor_url=_TEST_REKOR_URL,
        ) as graph:
            id_1 = graph.assert_claim("first")
            id_2 = graph.assert_claim("second")
            assert graph.get_claim(id_1)["transparency_logged"] == 0
            assert graph.get_claim(id_2)["transparency_logged"] == 0

            # Rekor recovers — register a reusable mirror callback.
            _mirror_rekor(httpx_mock)
            result = graph.refresh_unsigned()

            assert result["checked"] == 2
            assert result["logged"] == 2
            assert result["still_unlogged"] == 0
            assert graph.get_claim(id_1)["transparency_logged"] == 1
            assert graph.get_claim(id_2)["transparency_logged"] == 1

    def test_refresh_unsigned_promotes_replicated_after_log(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        """After refresh_unsigned succeeds for both peer claims, their shared
        upstream's REPLICATED check fires."""
        # First three asserts: Rekor down.
        for _ in range(3):
            httpx_mock.add_response(
                method="POST", url=_TEST_REKOR_URL, status_code=503,
            )
        key_path = _bootstrap_key(tmp_path)
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
            assert graph.get_claim(id_a)["support_level"] == "PRELIMINARY"

            # Rekor recovers — register a reusable mirror callback.
            _mirror_rekor(httpx_mock, uuid_prefix="late")
            result = graph.refresh_unsigned()
            assert result == {"checked": 3, "logged": 3, "still_unlogged": 0}

            assert graph.get_claim(id_a)["support_level"] == "REPLICATED"
            assert graph.get_claim(id_b)["support_level"] == "REPLICATED"


# ---------------------------------------------------------------------------
# require_rekor
# ---------------------------------------------------------------------------

class TestRequireRekor:
    def test_require_rekor_without_url_raises_at_open(self, tmp_path: Path) -> None:
        with pytest.raises(_signing.SigningError, match="needs an explicit rekor_url"):
            mareforma.open(tmp_path, require_rekor=True)

    def test_require_rekor_with_failing_rekor_raises_at_assert(
        self, tmp_path: Path, httpx_mock,
    ) -> None:
        httpx_mock.add_response(method="POST", url=_TEST_REKOR_URL, status_code=503)
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path, key_path=key_path,
            rekor_url=_TEST_REKOR_URL, require_rekor=True,
        ) as graph:
            with pytest.raises(_signing.SigningError, match="Rekor submission"):
                graph.assert_claim("must be logged")
