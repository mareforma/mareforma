"""tests/test_rekor_ssrf_hardening.py — SSRF-guard hardening for rekor.py.

Covers:
  - validate_rekor_url rejects non-base-10 numeric IP shortcuts (hex,
    mixed-hex-dotted), Unicode-digit host forms, and the NAT64 literal
    that embeds an internal IPv4 (#37).
"""

from __future__ import annotations

import pytest

from mareforma import signing as _signing


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
