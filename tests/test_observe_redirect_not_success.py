"""A 3xx redirect is delivery the observer never saw, not a success.

httpx does not follow redirects by default, so a ``.get``/``.post`` to a host
that redirects returns the 3xx stub to the wrapper. The redirect body is a short
"moved" page, so a lax success gate would ground a cited URL off bytes that never
arrived and mint COMPUTED model lineage off a call the provider bounced. The
shared ``_response_ok`` gate must admit only 2xx.
"""
from __future__ import annotations

import pytest

from mareforma.observe import _loaders, _scope


class _FakeResp:
    def __init__(self, status_code: int, content: bytes = b"Moved"):
        self.status_code = status_code
        self.content = content
        self.text = content.decode()


_MODEL_URL = "https://api.openai.com/v1/chat/completions"
_BODY = {"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}


@pytest.mark.parametrize("code", [301, 302, 303, 307, 308])
def test_redirect_is_not_a_success(code: int) -> None:
    assert _loaders._response_ok(_FakeResp(code)) is not True


@pytest.mark.parametrize("code", [301, 302, 303, 307, 308])
def test_redirect_body_does_not_ground_a_read(code: int) -> None:
    assert _loaders._resp_nonempty(_FakeResp(code)) is False


@pytest.mark.parametrize("code", [301, 302, 303, 307, 308])
def test_redirected_model_post_mints_no_lineage(code: int) -> None:
    scope = _scope.Scope(cited=())
    _loaders._record_model_lineage(scope, _BODY, _MODEL_URL, _FakeResp(code))
    assert scope.models == []


def test_2xx_still_reads_as_success() -> None:
    ok = _FakeResp(200, content=b"the cited bytes")
    assert _loaders._response_ok(ok) is True
    assert _loaders._resp_nonempty(ok) is True
    scope = _scope.Scope(cited=())
    _loaders._record_model_lineage(scope, _BODY, _MODEL_URL, ok)
    assert scope.models  # a real 2xx model call still mints lineage
