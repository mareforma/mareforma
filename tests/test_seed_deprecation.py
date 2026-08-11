"""The seed-anchor deprecation fires, once, and its escalation is scoped.

``assert_claim(seed=True)`` is deprecated and removed in v0.4.0. It still writes
a signed ESTABLISHED claim this release, so the warning is the only change on the
seed path. The suite ignores this warning by message (pyproject ``filterwarnings``)
because ~180 seed=True call sites are incidental scaffolding; these tests are where
the warning is proved to fire. They also prove the escalation is scoped to this
one message: a blanket ``error`` would raise on the irrevocable root-enrollment
notice, which is a distinct warning that must not be suppressible.
"""
from __future__ import annotations

import warnings

import pytest

import mareforma
from tests._helpers import _bootstrap_key

_SEED_MATCH = r"assert_claim\(seed=True\) is deprecated"
_SEED_SUBSTR = "assert_claim(seed=True) is deprecated"
_SEED_FILTER = r"error:assert_claim\(seed=True\) is deprecated:DeprecationWarning"


def test_seed_emits_deprecation_warning_and_still_writes_established(tmp_path):
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        with pytest.warns(DeprecationWarning, match=_SEED_MATCH):
            cid = g.assert_claim("anchor", generated_by="seed", seed=True)
        # R2: the seed path is unchanged apart from the warning.
        assert g.get_claim(cid)["support_level"] == "ESTABLISHED"


def test_seed_warns_exactly_once_per_call(tmp_path):
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            g.assert_claim("anchor", generated_by="seed", seed=True)
        seed_warnings = [w for w in caught if _SEED_SUBSTR in str(w.message)]
        assert len(seed_warnings) == 1


def test_honest_path_emits_no_seed_warning(tmp_path):
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            g.assert_claim("ordinary finding", generated_by="agent")
        assert not [w for w in caught if _SEED_SUBSTR in str(w.message)]


@pytest.mark.filterwarnings(_SEED_FILTER)
def test_scoped_escalation_makes_a_working_seed_call_raise(tmp_path):
    # The mitigation the release ships for a caller running under an escalated
    # filter: a working seed call raises rather than silently proceeding.
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        with pytest.raises(DeprecationWarning, match=_SEED_MATCH):
            g.assert_claim("anchor", generated_by="seed", seed=True)


@pytest.mark.filterwarnings(_SEED_FILTER)
def test_scoped_escalation_spares_the_root_enrollment_notice(tmp_path):
    # E9: escalating the seed message to an error must not turn the irrevocable
    # root-enrollment notice (a separate warning) into an error. Opening the
    # graph enrolls the root and emits that notice; under this message-scoped
    # filter the open still succeeds, where a blanket `error` would raise.
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        assert g.assert_claim("ordinary finding", generated_by="agent")

    # And the assertion that actually proves the filter is MESSAGE-scoped: the
    # root-enrollment notice is a UserWarning, so a CATEGORY-scoped filter over
    # DeprecationWarning would spare it too and pass this test identically. What
    # only a message-scoped filter allows is a sibling DeprecationWarning
    # reaching the caller as a warning rather than an error.
    import warnings

    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        warnings.filterwarnings(
            "error", message=r"assert_claim\(seed=True\) is deprecated",
            category=DeprecationWarning,
        )
        warnings.warn("an unrelated deprecation", DeprecationWarning)
    assert [w.category for w in seen] == [DeprecationWarning], (
        "the seed filter escalated a DeprecationWarning it does not name, so it "
        "is category-scoped, not message-scoped"
    )
