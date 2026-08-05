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

    @pytest.mark.parametrize("separator", [
        "。",  # ideographic full stop
        "．",  # fullwidth full stop
        "｡",  # halfwidth ideographic full stop
    ])
    @pytest.mark.parametrize("quad", ["127.0.0.1", "169.254.169.254"])
    def test_unicode_label_separators_rejected(self, separator, quad):
        # IDNA maps these three to '.', so httpx resolves the dotted quad
        # the guard never saw unless it normalizes first.
        host = quad.replace(".", separator)
        with pytest.raises(_signing.SigningError):
            _signing.validate_rekor_url(f"https://{host}/api/v1/log/entries")

    def test_unparseable_url_raises_the_callers_error_type(self):
        # urlsplit raises ValueError on an unclosed IPv6 bracket. The guard
        # documents one error type, so the parse must be inside it.
        with pytest.raises(_signing.SigningError):
            _signing.validate_rekor_url("https://[::1/x")

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
# the inet_aton radix parser, reachable on its own
# ---------------------------------------------------------------------------

class TestNumericShortcutParser:
    @pytest.mark.parametrize("host", [
        "0x7f000001",   # hex, one part
        "2130706433",   # decimal, one part
        "127.1",        # two parts, the tail is the low 24 bits
        "0177.0.0.1",   # octal first part
    ])
    def test_shortcut_forms_resolve_to_loopback(self, host):
        assert str(_numeric_shortcut_ipv4(host)) == "127.0.0.1"

    @pytest.mark.parametrize("host", [
        "256.1.1.1",    # part out of range
        "0x",           # hex marker with no body
        "08",           # leading zero, not octal
        "rekor.sigstore.dev",
    ])
    def test_non_numeric_forms_return_none(self, host):
        assert _numeric_shortcut_ipv4(host) is None


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

    def test_unparseable_url_returns_false_without_posting(self, httpx_mock):
        # submit_to_rekor promises (False, None) for any bad rekor_url and
        # guards on SigningError, so the parse failure must arrive as one.
        envelope, key = _sample_envelope()

        logged, entry = _signing.submit_to_rekor(
            envelope, key.public_key(), rekor_url="https://[::1/x",
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
