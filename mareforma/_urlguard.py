"""_urlguard.py: shared transport guard for outbound service URLs.

Two call sites need the same precondition on a URL taken from config or
the environment: it must reach a public host over TLS. The Rekor client
uses it as an SSRF defense; the ClawInstitute adapter uses it because
the base URL decides whether the Bearer token travels encrypted.

The checks are identical, so they live here once and each caller passes
its own parameter name, exception type and opt-out hint.
"""

from __future__ import annotations

import ipaddress
import unicodedata
from typing import Optional
from urllib.parse import urlparse


_LOOPBACK_DNS_NAMES = frozenset({
    "localhost",
    "localhost.localdomain",
    "ip6-localhost",
    "ip6-loopback",
})

# IDNA (RFC 3490 section 3.1, UTS-46) reads three more code points as
# label separators, so an HTTP client resolves '127。0。0。1' as the dotted
# quad. Fold them to '.' before any classification or the whole host
# arrives here as one unrecognized label.
_LABEL_SEPARATORS = str.maketrans({
    "。": ".",  # ideographic full stop
    "．": ".",  # fullwidth full stop
    "｡": ".",  # halfwidth ideographic full stop
})


_ASCII_DIGITS = "0123456789"


def _is_blocked_ip(
    ip: "ipaddress.IPv4Address | ipaddress.IPv6Address",
) -> bool:
    # The address classes a public service must never be.
    return bool(
        ip.is_loopback or ip.is_private or ip.is_link_local
        or ip.is_multicast or ip.is_unspecified
    )


def _embedded_ipv4(
    ip: "ipaddress.IPv4Address | ipaddress.IPv6Address",
) -> Optional[ipaddress.IPv4Address]:
    # IPv4-mapped (::ffff:0:0/96), 6to4 (2002::/16), and NAT64
    # (64:ff9b::/96) each wrap a routable IPv4 the outer-address
    # flags (is_link_local etc.) do not see.
    if not isinstance(ip, ipaddress.IPv6Address):
        return None
    if ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    if ip.sixtofour is not None:
        return ip.sixtofour
    if ip in ipaddress.ip_network("64:ff9b::/96"):
        return ipaddress.IPv4Address(ip.packed[-4:])
    return None


def _parse_c_uint(part: str) -> Optional[int]:
    # One host part the way C strtoul (base 0) reads it: 0x -> hex,
    # leading 0 -> octal, else decimal. ASCII digits only (Python's
    # int otherwise accepts Unicode digits). None if not a number.
    if not part:
        return None
    low = part.lower()
    if low.startswith("0x"):
        body = low[2:]
        if body and all(c in "0123456789abcdef" for c in body):
            return int(body, 16)
        return None
    if part.startswith("0") and len(part) > 1:
        if all(c in "01234567" for c in part):
            return int(part, 8)
        return None
    if all(c in _ASCII_DIGITS for c in part):
        return int(part, 10)
    return None


def _numeric_shortcut_ipv4(host: str) -> Optional[ipaddress.IPv4Address]:
    # Resolve an inet_aton-style numeric host (1-4 dot-separated
    # parts, any radix) that ipaddress rejects but getaddrinfo
    # honors: 0x7f000001, 2130706433, 127.1, 0177.0.0.1 -> loopback.
    parts = host.split(".")
    if not 1 <= len(parts) <= 4:
        return None
    values: list[int] = []
    for part in parts:
        value = _parse_c_uint(part)
        if value is None:
            return None
        values.append(value)
    n = len(values)
    if n == 1:
        packed = values[0]
    elif n == 2:
        a, b = values
        if a > 0xFF or b > 0xFFFFFF:
            return None
        packed = (a << 24) | b
    elif n == 3:
        a, b, c = values
        if a > 0xFF or b > 0xFF or c > 0xFFFF:
            return None
        packed = (a << 24) | (b << 16) | c
    else:  # n == 4
        if any(v > 0xFF for v in values):
            return None
        packed = (
            (values[0] << 24) | (values[1] << 16)
            | (values[2] << 8) | values[3]
        )
    if not 0 <= packed <= 0xFFFFFFFF:
        return None
    return ipaddress.IPv4Address(packed)


def _has_non_ascii_digit(host: str) -> bool:
    # Fullwidth / circled / other decimal-digit forms (e.g. U+2460
    # ①) are read as numbers by some C resolvers, so ①②⑧.0.0.1 can
    # reach an internal address; no public DNS name uses them.
    return any(
        ch not in _ASCII_DIGITS and unicodedata.digit(ch, None) is not None
        for ch in host
    )


def validate_public_https_url(
    url: str,
    *,
    param_name: str,
    error_cls: type[Exception],
    bypass_hint: str,
    allow_insecure: bool = False,
) -> None:
    """Reject URLs that are not plain ``https://`` to a public host.

    Enforces ``https://`` and rejects:

    - Loopback / private / link-local / multicast / unspecified (``0.0.0.0``,
      ``::``) IP literals, INCLUDING the IPv4 embedded in an IPv6 form
      (IPv4-mapped ``::ffff:a.b.c.d``, 6to4, and the NAT64 well-known
      prefix ``64:ff9b::/96``), which the outer-address flags miss.
    - Numeric IP shortcuts in any radix that resolve to an internal
      address: ``127.1`` and ``2130706433`` (decimal), ``0177.0.0.1``
      (octal), ``0x7f000001`` and ``0x7f.0.0.1`` (hex). These bypass
      :func:`ipaddress.ip_address` because Python rejects the shortcut
      form, but ``socket.getaddrinfo`` resolves them to the internal
      address on most kernels.
    - Hostnames carrying non-ASCII Unicode digits (``①②⑧.0.0.1``), which
      a C resolver may read as a numeric address.
    - ``localhost`` and its ``/etc/hosts`` aliases.

    DNS hostnames that don't look like loopback shortcuts are accepted;
    defending against a DNS rebind at connect-time would need ahead-of-time
    resolution which is fragile: TLS at the remote host is the actual
    authentication boundary.

    ``param_name`` names the setting in every message, ``bypass_hint``
    tells the operator which flag opts out, and ``error_cls`` is the
    caller's own error type so its taxonomy stays intact.

    Pass ``allow_insecure=True`` to skip all checks (only useful against
    a private instance on a non-public address).

    Raises
    ------
    error_cls
        If the URL does not parse or fails any check, and
        ``allow_insecure`` is False.
    """
    if allow_insecure:
        return

    try:
        parsed = urlparse(url)
    except ValueError as exc:
        # urlsplit refuses a malformed authority (an unclosed IPv6 bracket)
        # with a bare ValueError. Callers only handle error_cls, so the
        # parse belongs under the same contract as every check below.
        raise error_cls(
            f"{param_name} is not a parseable URL: {url!r} ({exc})"
        ) from exc
    if parsed.scheme != "https":
        raise error_cls(
            f"{param_name} must use https:// (got {parsed.scheme!r}). "
            f"{bypass_hint}"
        )
    hostname = parsed.hostname
    if hostname is None:
        raise error_cls(f"{param_name} is missing a hostname: {url!r}")
    try:
        ip = ipaddress.ip_address(hostname)
    except ValueError:
        # Not a strict IP literal, apply the DNS-shortcut bypass guards.
        hl = hostname.lower()
        if (
            hl in _LOOPBACK_DNS_NAMES
            or hl.endswith(".localhost")
            or hl.startswith("localhost.")
        ):
            raise error_cls(
                f"{param_name} hostname {hostname!r} resolves to loopback. "
                f"{bypass_hint}"
            )
        if has_non_ascii_digit(hostname):
            # ①②⑧.0.0.1 and friends: a resolver may read the Unicode
            # digits as a numeric address; no public DNS name uses them.
            raise error_cls(
                f"{param_name} hostname {hostname!r} contains non-ASCII "
                "digit characters that a resolver may read as a numeric IP "
                f"shortcut. {bypass_hint}"
            )
        shortcut_ip = numeric_shortcut_ipv4(hostname)
        if shortcut_ip is not None and is_blocked_ip(shortcut_ip):
            # 127.1, 2130706433, 0x7f000001, 0177.0.0.1 etc., ipaddress
            # rejects these but socket.getaddrinfo (any radix) resolves
            # them to the internal address below.
            raise error_cls(
                f"{param_name} hostname {hostname!r} is a numeric IP "
                f"shortcut for {shortcut_ip}, a non-public address. "
                f"{bypass_hint}"
            )
        return
    # IP literal path, classify the literal AND any IPv4 embedded in an
    # IPv6 form (IPv4-mapped, 6to4, NAT64), which the outer flags miss.
    candidates: list["ipaddress.IPv4Address | ipaddress.IPv6Address"] = [ip]
    embedded = _embedded_ipv4(ip)
    if embedded is not None:
        candidates.append(embedded)
    for candidate in candidates:
        if _is_blocked_ip(candidate):
            raise error_cls(
                f"{param_name} resolves to a non-public address "
                f"({candidate}). {bypass_hint}"
            )
