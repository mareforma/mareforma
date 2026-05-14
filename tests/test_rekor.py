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
        """When Rekor is unreachable, the claim still persists locally with
        transparency_logged=0; REPLICATED promotion waits until a later
        refresh_unsigned() succeeds."""
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
            upstream = graph.assert_claim("upstream", generated_by="seed", seed=True)
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
            upstream = graph.assert_claim("upstream", generated_by="seed", seed=True)
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
            upstream = graph.assert_claim("upstream", generated_by="seed", seed=True)
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


# ---------------------------------------------------------------------------
# Rekor response trust boundary
# ---------------------------------------------------------------------------

class TestRekorResponseVerification:
    """submit_to_rekor must verify that the returned entry actually records
    our submission. A buggy or hostile registry that hands back an arbitrary
    uuid/logIndex but a body that doesn't match our hash+sig should NOT be
    treated as proof of inclusion.
    """

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
        import base64
        key, envelope = self._build_envelope()
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
        import base64
        import hashlib
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
# refresh_unsigned drift guard
# ---------------------------------------------------------------------------

class TestRefreshUnsignedDrift:
    def test_drifted_row_is_not_submitted_to_rekor(self, tmp_path, httpx_mock):
        """A row whose text was tampered after assert_claim must not be
        logged to Rekor by refresh_unsigned — the public log would otherwise
        cement a stale signature."""
        httpx_mock.add_response(method="POST", url=_TEST_REKOR_URL, status_code=503)

        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(
            tmp_path, key_path=key_path, rekor_url=_TEST_REKOR_URL,
        ) as graph:
            claim_id = graph.assert_claim("original text")
            assert graph.get_claim(claim_id)["transparency_logged"] == 0

        # Direct-SQL tamper on a signed claim's text is refused by the
        # claims_signed_fields_no_laundering trigger — the substrate
        # gate is the primary defense. The Python-side drift check
        # in refresh_unsigned is a defense-in-depth backstop for
        # cases the trigger somehow doesn't catch (e.g. an attacker
        # bypassing SQLite triggers via the pragma path).
        import sqlite3
        conn = sqlite3.connect(str(tmp_path / ".mareforma" / "graph.db"))
        with pytest.raises(sqlite3.IntegrityError) as exc:
            conn.execute(
                "UPDATE claims SET text = ? WHERE claim_id = ?",
                ("tampered text", claim_id),
            )
        assert "signed_field_locked" in str(exc.value)
        conn.close()

        # The row was not mutated; refresh_unsigned should now succeed
        # against Rekor (httpx_mock is configured for 503 in this test,
        # so the second attempt also fails but for the network reason).
        # The relevant assertion is that the tamper was blocked.
        rekor_posts = [r for r in httpx_mock.get_requests() if r.method == "POST"]
        # Only the original failed POST during assert_claim.
        assert len(rekor_posts) == 1


# ---------------------------------------------------------------------------
# mark_claim_logged caller verification
# ---------------------------------------------------------------------------

class TestMarkClaimLoggedVerification:
    """mark_claim_logged is a low-level helper that writes a caller-supplied
    bundle onto a row. Internal mix-ups (Alice's bundle onto Bob's row) and
    malformed inputs must surface as DatabaseError, not silent corruption
    or an AttributeError leaking from envelope_payload.
    """

    def test_bundle_for_wrong_claim_id_is_rejected(self, tmp_path):
        from mareforma.db import DatabaseError, mark_claim_logged
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            alice_id = graph.assert_claim("alice claim")
            bob_id = graph.assert_claim("bob claim")
            alice_bundle = graph.get_claim(alice_id)["signature_bundle"]

        from mareforma.db import open_db
        conn = open_db(tmp_path)
        try:
            with pytest.raises(DatabaseError, match="does not match"):
                mark_claim_logged(conn, tmp_path, bob_id, alice_bundle)
        finally:
            conn.close()

    def test_malformed_bundle_is_rejected(self, tmp_path):
        """A signed claim cannot have its bundle replaced by malformed JSON."""
        from mareforma.db import DatabaseError, mark_claim_logged, open_db
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("signed host")
        conn = open_db(tmp_path)
        try:
            with pytest.raises(DatabaseError, match="malformed bundle"):
                mark_claim_logged(conn, tmp_path, claim_id, "{not valid json")
        finally:
            conn.close()

    def test_bundle_whose_payload_is_a_json_string_surfaces_DatabaseError(self, tmp_path):
        """End-to-end: envelope_payload's dict-only contract must propagate
        through mark_claim_logged as DatabaseError (not AttributeError)."""
        import base64
        from mareforma.db import DatabaseError, mark_claim_logged, open_db
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("host")
        bad_bundle = json.dumps({
            "payloadType": "application/vnd.mareforma.claim+json",
            "payload": base64.standard_b64encode(b'"nope"').decode("ascii"),
            "signatures": [{"keyid": "x", "sig": "y"}],
        })
        conn = open_db(tmp_path)
        try:
            with pytest.raises(DatabaseError, match="malformed bundle"):
                mark_claim_logged(conn, tmp_path, claim_id, bad_bundle)
        finally:
            conn.close()

    def test_unsigned_claim_refuses_rekor_attachment(self, tmp_path):
        """Rekor inclusion attaches to an already-signed envelope. An
        unsigned claim (signature_bundle IS NULL) cannot be retroactively
        log-stamped via mark_claim_logged — the call refuses up-front
        rather than silently writing a bundle onto an unsigned row.
        """
        from mareforma.db import (
            DatabaseError, add_claim, mark_claim_logged, open_db,
        )
        conn = open_db(tmp_path)
        try:
            claim_id = add_claim(conn, tmp_path, "unsigned host")
            with pytest.raises(DatabaseError, match="no existing signature_bundle"):
                mark_claim_logged(conn, tmp_path, claim_id, "{}")
        finally:
            conn.close()

    def test_smuggled_top_level_keys_are_rejected(self, tmp_path):
        """A bundle whose payload/payloadType/signatures match but which
        adds unexpected top-level keys (e.g. an opaque 'metadata' blob)
        is refused. Only payload/payloadType/signatures/rekor are
        whitelisted — substantive substitution beyond the Rekor block
        is not allowed, even if the new content is structurally inert."""
        from mareforma.db import DatabaseError, mark_claim_logged, open_db
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("the claim")
            existing = graph.get_claim(claim_id)["signature_bundle"]

        env = json.loads(existing)
        env["bonus_metadata"] = {"some": "payload"}
        smuggled = json.dumps(env, sort_keys=True, separators=(",", ":"))

        conn = open_db(tmp_path)
        try:
            with pytest.raises(DatabaseError, match="unexpected top-level keys"):
                mark_claim_logged(conn, tmp_path, claim_id, smuggled)
        finally:
            conn.close()

    def test_substitute_bundle_with_different_signer_is_rejected(self, tmp_path):
        """A bundle whose predicate.claim_id matches but whose payload +
        signatures differ from the row's existing bundle is refused —
        mark_claim_logged is for Rekor attachment, not envelope swap.
        """
        from mareforma.db import DatabaseError, mark_claim_logged, open_db
        # Two different signing keys → two different claim envelopes on
        # the same claim_id string is impossible to forge organically,
        # but a hostile caller could hand-craft. Build one with a fresh
        # key by re-signing the same claim fields and verify rejection.
        key_path = _bootstrap_key(tmp_path)
        with mareforma.open(tmp_path, key_path=key_path) as graph:
            claim_id = graph.assert_claim("the claim")
            existing = graph.get_claim(claim_id)["signature_bundle"]

        # Hand-craft a syntactically valid bundle that names the same
        # claim_id but in payload/signatures bytes differs from the
        # existing one. Easiest: parse, tweak signatures, re-serialize.
        env = json.loads(existing)
        env["signatures"] = [{"keyid": "0" * 64, "sig": "AA=="}]
        bad_bundle = json.dumps(env, sort_keys=True, separators=(",", ":"))

        conn = open_db(tmp_path)
        try:
            with pytest.raises(DatabaseError, match="differ from the existing row"):
                mark_claim_logged(conn, tmp_path, claim_id, bad_bundle)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Rekor response size cap (header pre-check + actual bytes)
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
        """No Content-Length header — the streaming accumulator must abort
        once it has read past the cap, before the body fully lands."""
        key = _signing.generate_keypair()
        envelope = _signing.sign_claim(
            {"claim_id": "c", "text": "x", "classification": "INFERRED",
             "generated_by": "a", "supports": [], "contradicts": [],
             "source_name": None, "created_at": "2026-05-12T00:00:00+00:00"},
            key,
        )
        # 256 KB filler — well past the 64 KB cap.
        huge = b"X" * (256 * 1024)
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL, status_code=201,
            content=huge,
        )
        logged, _ = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url=_TEST_REKOR_URL,
        )
        assert logged is False


# ---------------------------------------------------------------------------
# Sidecar parser hostility tolerance
# ---------------------------------------------------------------------------

class TestRecordRekorInclusionParseTolerance:
    """A buggy or hostile Rekor response with non-integer logIndex /
    integratedTime must NOT propagate ValueError out of add_claim. The
    sidecar write is treated as a miss and the caller's claim INSERT
    stays committed; the next refresh_unsigned() re-submits.
    """

    def _make_entry(self, **overrides):
        entry = {
            "uuid": "abc",
            "logIndex": 42,
            "integratedTime": 1_700_000_000,
        }
        entry.update(overrides)
        return entry

    def test_non_integer_logIndex_treated_as_sidecar_miss(self, tmp_path):
        from mareforma.db import _record_rekor_inclusion, add_claim, open_db
        conn = open_db(tmp_path)
        try:
            cid = add_claim(conn, tmp_path, "host claim")
            # Hostile registry returns a string.
            ok = _record_rekor_inclusion(
                conn, cid, self._make_entry(logIndex="not-a-number"),
            )
            assert ok is False
            # No sidecar row was written.
            rows = conn.execute(
                "SELECT 1 FROM rekor_inclusions WHERE claim_id = ?", (cid,),
            ).fetchall()
            assert rows == []
        finally:
            conn.close()

    def test_non_integer_integratedTime_falls_back_to_null(self, tmp_path):
        from mareforma.db import _record_rekor_inclusion, add_claim, open_db
        conn = open_db(tmp_path)
        try:
            cid = add_claim(conn, tmp_path, "host claim 2")
            ok = _record_rekor_inclusion(
                conn, cid, self._make_entry(integratedTime="garbage"),
            )
            # logIndex is fine → sidecar succeeds with NULL integratedTime.
            assert ok is True
            row = conn.execute(
                "SELECT integrated_time, log_index "
                "FROM rekor_inclusions WHERE claim_id = ?", (cid,),
            ).fetchone()
            assert row["integrated_time"] is None
            assert row["log_index"] == 42
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# rekor_url validation (SSRF defense)
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

    @pytest.mark.parametrize("url", [
        "https://localhost/api/v1/log/entries",        # plain DNS loopback
        "https://localhost.localdomain/api/v1",        # legacy alias
        "https://ip6-localhost/api/v1",                 # /etc/hosts entry
        "https://127.1/api/v1/log/entries",             # short IPv4 form
        "https://0/api/v1/log/entries",                 # 0 → 0.0.0.0
        "https://2130706433/api/v1/log/entries",        # decimal of 127.0.0.1
        "https://0177.0.0.1/api/v1/log/entries",        # octal of 127.0.0.1
        "https://0.0.0.0/api/v1/log/entries",           # unspecified
    ])
    def test_dns_shortcut_bypasses_rejected(self, tmp_path, url):
        """DNS shortcuts that ipaddress.ip_address() rejects but kernels
        resolve to loopback/private must NOT pass validation. Classic SSRF
        bypass payloads."""
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
# Signature equality tolerates base64 alphabet and padding variants
# ---------------------------------------------------------------------------

class TestSignatureBase64Tolerance:
    """submit_to_rekor compares signatures by decoded bytes, not literal
    base64 strings. Real Rekor servers may canonicalize the entry-body
    base64 differently than what we POSTed (URL-safe alphabet, padding
    stripped); wire-equivalent representations must still be accepted.
    """

    def _envelope_and_hash(self):
        import base64
        import hashlib
        key = _signing.generate_keypair()
        envelope = _signing.sign_claim(
            {"claim_id": "c", "text": "x", "classification": "INFERRED",
             "generated_by": "a", "supports": [], "contradicts": [],
             "source_name": None, "created_at": "2026-05-12T00:00:00+00:00"},
            key,
        )
        payload_bytes = base64.standard_b64decode(envelope["payload"])
        true_hash = hashlib.sha256(payload_bytes).hexdigest()
        return key, envelope, true_hash

    def test_padding_stripped_signature_in_response_still_accepted(self, httpx_mock):
        import base64
        key, envelope, true_hash = self._envelope_and_hash()
        sig_no_pad = envelope["signatures"][0]["sig"].rstrip("=")
        record = {
            "apiVersion": "0.0.1", "kind": "hashedrekord",
            "spec": {
                "data": {"hash": {"algorithm": "sha256", "value": true_hash}},
                "signature": {"content": sig_no_pad, "publicKey": {"content": "x"}},
            },
        }
        encoded = base64.standard_b64encode(
            json.dumps(record).encode("utf-8"),
        ).decode("ascii")
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL, status_code=201,
            json={"u": {"body": encoded, "integratedTime": 1, "logIndex": 1}},
        )
        logged, entry = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url=_TEST_REKOR_URL,
        )
        assert logged is True
        assert entry["uuid"] == "u"

    def test_urlsafe_alphabet_signature_in_response_still_accepted(self, httpx_mock):
        import base64
        key, envelope, true_hash = self._envelope_and_hash()
        raw_sig = base64.standard_b64decode(envelope["signatures"][0]["sig"])
        sig_urlsafe = base64.urlsafe_b64encode(raw_sig).decode("ascii")
        record = {
            "apiVersion": "0.0.1", "kind": "hashedrekord",
            "spec": {
                "data": {"hash": {"algorithm": "sha256", "value": true_hash}},
                "signature": {"content": sig_urlsafe, "publicKey": {"content": "x"}},
            },
        }
        encoded = base64.standard_b64encode(
            json.dumps(record).encode("utf-8"),
        ).decode("ascii")
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL, status_code=201,
            json={"u": {"body": encoded, "integratedTime": 1, "logIndex": 1}},
        )
        logged, entry = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url=_TEST_REKOR_URL,
        )
        assert logged is True


# ---------------------------------------------------------------------------
# REPLICATED gating: one peer logged, one peer not
# ---------------------------------------------------------------------------

class TestOnePeerLoggedOneNot:
    def test_neither_replicates_until_both_logged(self, tmp_path, httpx_mock):
        """Agent A succeeds at Rekor; agent B never does. Agent A is
        transparency_logged=1 alone, but REPLICATED requires the NEW
        claim's transparency_logged=1 as well — agent B's continued
        unlogged state keeps both at PRELIMINARY."""
        import base64
        key_path = _bootstrap_key(tmp_path)

        def one_shot_mirror(httpx_mock):
            def cb(request: "httpx.Request") -> "httpx.Response":
                import httpx as _httpx
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
                return _httpx.Response(
                    201,
                    json={"uu": {"body": encoded, "logIndex": 1, "integratedTime": 1}},
                )
            httpx_mock.add_callback(cb, method="POST", url=_TEST_REKOR_URL)

        one_shot_mirror(httpx_mock)  # upstream succeeds
        one_shot_mirror(httpx_mock)  # agent A succeeds
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL, status_code=503,
        )  # agent B fails

        with mareforma.open(
            tmp_path, key_path=key_path, rekor_url=_TEST_REKOR_URL,
        ) as graph:
            upstream = graph.assert_claim("upstream", generated_by="seed", seed=True)
            id_a = graph.assert_claim(
                "agent A", supports=[upstream], generated_by="agent/a",
            )
            id_b = graph.assert_claim(
                "agent B", supports=[upstream], generated_by="agent/b",
            )

            assert graph.get_claim(id_a)["transparency_logged"] == 1
            assert graph.get_claim(id_b)["transparency_logged"] == 0
            assert graph.get_claim(id_a)["support_level"] == "PRELIMINARY"
            assert graph.get_claim(id_b)["support_level"] == "PRELIMINARY"

            # When B's refresh_unsigned succeeds, both must promote.
            _mirror_rekor(httpx_mock, uuid_prefix="late-b")
            result = graph.refresh_unsigned()
            assert result["logged"] == 1  # only B was pending

            assert graph.get_claim(id_a)["support_level"] == "REPLICATED"
            assert graph.get_claim(id_b)["support_level"] == "REPLICATED"


# ---------------------------------------------------------------------------
# Key rotation: refresh_unsigned skips claims signed by the prior key
# ---------------------------------------------------------------------------

class TestKeyIdMismatchOnRefresh:
    def test_rotated_key_skipped_with_warning(self, tmp_path, httpx_mock):
        """Bootstrap key A, assert (Rekor down), bootstrap key B with
        overwrite, then refresh_unsigned: the claim signed by A cannot
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

        with pytest.warns(UserWarning, match="signed by keyid"):
            with mareforma.open(
                tmp_path, key_path=key_path, rekor_url=_TEST_REKOR_URL,
            ) as graph:
                result = graph.refresh_unsigned()

        assert result == {"checked": 1, "logged": 0, "still_unlogged": 1}
        rekor_posts = [r for r in httpx_mock.get_requests() if r.method == "POST"]
        assert len(rekor_posts) == 1


# ---------------------------------------------------------------------------
# Streaming response body (oversized aborts mid-read, not after full buffer)
# ---------------------------------------------------------------------------

class TestSubmitToRekorStreaming:
    def test_oversized_chunked_body_aborts_during_read(self, httpx_mock):
        """No Content-Length, 256 KB of garbage past the 64 KB cap.
        submit_to_rekor must reject without buffering the whole body
        — the streaming accumulator is the only line of defense."""
        key = _signing.generate_keypair()
        envelope = _signing.sign_claim(
            {"claim_id": "c", "text": "x", "classification": "INFERRED",
             "generated_by": "a", "supports": [], "contradicts": [],
             "source_name": None, "created_at": "2026-05-12T00:00:00+00:00"},
            key,
        )
        huge = b"X" * (256 * 1024)
        httpx_mock.add_response(
            method="POST", url=_TEST_REKOR_URL, status_code=201,
            content=huge,
        )
        logged, entry = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url=_TEST_REKOR_URL,
        )
        assert logged is False
        assert entry is None
