"""tests/test_rekor_ssrf_hardening.py, SSRF-guard hardening for rekor.py.

Covers:
  - validate_rekor_url rejects non-base-10 numeric IP shortcuts (hex,
    mixed-hex-dotted), Unicode-digit host forms, and the NAT64 literal
    that embeds an internal IPv4.
  - submit_to_rekor re-validates rekor_url at entry so direct callers
    cannot bypass the SSRF / scheme defense the fetch paths enforce.
  - the trust_insecure_rekor session opt-in reaches every submit and
    fetch call, so a private Rekor instance is actually usable.
"""

from __future__ import annotations

import json

import pytest

import mareforma
from mareforma import signing as _signing
from mareforma._urlguard import _numeric_shortcut_ipv4
from tests._helpers import _bootstrap_key, _rekor_response_for


def _sample_envelope():
    key = _signing.generate_keypair()
    envelope = _signing.sign_claim(
        {
            "claim_id": "ssrf-1",
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
    return envelope, key


# ---------------------------------------------------------------------------
# numeric-shortcut / Unicode-digit / NAT64 bypasses
# ---------------------------------------------------------------------------

class TestSsrfRadixAndEmbeddedBypasses:
    @pytest.mark.parametrize("url", [
        "https://0x7f000001/api/v1/log/entries",          # hex 127.0.0.1
        "https://0x7f.0.0.1/api/v1/log/entries",           # mixed hex-dotted
        "https://①②⑧.0.0.1/api/v1/log/entries",  # Unicode digits
        "https://[64:ff9b::169.254.169.254]/api/v1/log/entries",  # NAT64 -> metadata
    ])
    def test_bypass_forms_rejected(self, url):
        with pytest.raises(_signing.SigningError):
            _signing.validate_rekor_url(url)

    def test_public_hostname_still_accepted(self):
        # A normal public DNS host must keep passing (no false positive,
        # no network resolution).
        _signing.validate_rekor_url(
            "https://rekor.sigstore.dev/api/v1/log/entries",
        )

    def test_public_numeric_form_is_not_blocked(self):
        # A numeric shortcut for a PUBLIC address (8.8.8.8) is unusual but
        # not an SSRF target; the guard blocks internal addresses, not all
        # numeric forms.
        _signing.validate_rekor_url("https://0x08080808/api/v1/log/entries")


# ---------------------------------------------------------------------------
# submit_to_rekor must re-validate at entry
# ---------------------------------------------------------------------------

class TestSubmitToRekorValidatesUrl:
    def test_unsafe_url_returns_false_without_posting(self, httpx_mock):
        # No mock is registered: the fix must reject the URL before any
        # request is issued. Before the fix, submit_to_rekor POSTs to the
        # metadata endpoint and pytest-httpx raises on the unmatched call.
        envelope, key = _sample_envelope()

        logged, entry = _signing.submit_to_rekor(
            envelope,
            key.public_key(),
            rekor_url="http://169.254.169.254/api/v1/log/entries",
        )

        assert logged is False
        assert entry is None
        assert httpx_mock.get_requests() == []

    def test_allow_insecure_lets_private_submit_proceed(self, httpx_mock):
        import httpx

        unsafe = "http://10.0.0.5/api/v1/log/entries"

        def mirror(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            spec = body["spec"]
            return httpx.Response(
                201,
                json=_rekor_response_for(
                    payload_hash=spec["data"]["hash"]["value"],
                    sig_b64=spec["signature"]["content"],
                    uuid="priv",
                    log_index=3,
                ),
            )

        httpx_mock.add_callback(mirror, url=unsafe, is_reusable=True)
        envelope, key = _sample_envelope()

        logged, entry = _signing.submit_to_rekor(
            envelope,
            key.public_key(),
            rekor_url=unsafe,
            allow_insecure=True,
        )

        assert logged is True
        assert entry["uuid"] == "priv"


# ---------------------------------------------------------------------------
# trust_insecure_rekor must reach the network calls, not just open()
# ---------------------------------------------------------------------------

_PRIVATE_REKOR_URL = "http://10.0.0.5/api/v1/log/entries"


def _wire_private_rekor(httpx_mock, *, uuid: str = "priv01") -> None:
    """Mirror POSTs to the private Rekor address so submit_to_rekor's
    response-binding check passes."""
    import httpx

    def mirror(request: httpx.Request) -> httpx.Response:
        spec = json.loads(request.content)["spec"]
        return httpx.Response(
            201,
            json=_rekor_response_for(
                payload_hash=spec["data"]["hash"]["value"],
                sig_b64=spec["signature"]["content"],
                uuid=uuid,
                log_index=3,
            ),
        )

    httpx_mock.add_callback(
        mirror, method="POST", url=_PRIVATE_REKOR_URL, is_reusable=True,
    )


class TestSessionThreadsTrustInsecureRekor:
    def test_assert_claim_reaches_private_rekor(self, tmp_path, httpx_mock):
        _wire_private_rekor(httpx_mock)
        key_path = _bootstrap_key(tmp_path)

        with mareforma.open(
            tmp_path,
            key_path=key_path,
            rekor_url=_PRIVATE_REKOR_URL,
            trust_insecure_rekor=True,
        ) as graph:
            cid = graph.assert_claim("private finding", classification="ANALYTICAL")
            claim = graph.get_claim(cid)

        assert claim["transparency_logged"] == 1

    def test_refresh_unsigned_reaches_private_rekor(self, tmp_path, httpx_mock):
        # The first submit fails at the log, so the claim stays in the
        # backlog; the retry has to reach the same private address.
        httpx_mock.add_response(
            method="POST", url=_PRIVATE_REKOR_URL, status_code=503,
        )
        _wire_private_rekor(httpx_mock, uuid="priv02")
        key_path = _bootstrap_key(tmp_path)

        with mareforma.open(
            tmp_path,
            key_path=key_path,
            rekor_url=_PRIVATE_REKOR_URL,
            trust_insecure_rekor=True,
        ) as graph:
            graph.assert_claim("backlog finding", classification="ANALYTICAL")
            result = graph.refresh_unsigned()

        assert result["logged"] == 1
        assert result["still_unlogged"] == 0

    def test_open_without_the_flag_still_refuses(self, tmp_path):
        with pytest.raises(_signing.SigningError):
            mareforma.open(tmp_path, rekor_url=_PRIVATE_REKOR_URL)


class TestFetchPathsAcceptAllowInsecure:
    def test_fetch_inclusion_proof_reaches_private_rekor(self, httpx_mock):
        uuid = "deadbeef" * 8
        httpx_mock.add_response(
            method="GET",
            url=f"{_PRIVATE_REKOR_URL}/{uuid}",
            json={uuid: {"body": "e30=", "logIndex": 3}},
        )

        entry = _signing.fetch_inclusion_proof(
            uuid, _PRIVATE_REKOR_URL, allow_insecure=True,
        )

        assert entry["logIndex"] == 3

    def test_fetch_log_pubkey_reaches_private_rekor(self, httpx_mock):
        key = _signing.generate_keypair()
        pem = _signing.public_key_to_pem(key.public_key())
        httpx_mock.add_response(
            method="GET",
            url="http://10.0.0.5/api/v1/log/publicKey",
            content=pem,
        )

        assert _signing.fetch_log_pubkey(
            _PRIVATE_REKOR_URL, allow_insecure=True,
        ) == pem

    def test_fetch_paths_still_refuse_without_the_flag(self):
        with pytest.raises(_signing.RekorInclusionError):
            _signing.fetch_inclusion_proof("deadbeef" * 8, _PRIVATE_REKOR_URL)
        with pytest.raises(_signing.SigningError):
            _signing.fetch_log_pubkey(_PRIVATE_REKOR_URL)
