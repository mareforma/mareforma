"""tests/test_rekor_ssrf_hardening.py — SSRF-guard hardening for rekor.py.

Covers:
  - validate_rekor_url rejects non-base-10 numeric IP shortcuts (hex,
    mixed-hex-dotted), Unicode-digit host forms, and the NAT64 literal
    that embeds an internal IPv4 (#37).
  - submit_to_rekor re-validates rekor_url at entry so direct callers
    cannot bypass the SSRF / scheme defense the fetch paths enforce (#38).
"""

from __future__ import annotations

import json

import pytest

from mareforma import signing as _signing
from tests.test_rekor import _rekor_response_for


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
# #37 — numeric-shortcut / Unicode-digit / NAT64 bypasses
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
# #38 — submit_to_rekor must re-validate at entry
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
