"""REPLICATED / ESTABLISHED retired as public support-level labels.

The trust map now leads with the effective-independence number, not a single
support word. The old public labels ``mareforma.REPLICATED`` and
``mareforma.ESTABLISHED`` keep working for one release as string aliases that
warn on read; a future release removes them. This is a public-label
retirement, not a schema rename: the stored ``support_level`` strings and the
promotion machinery are unchanged.
"""

from __future__ import annotations

import pytest

import mareforma


def test_replicated_label_deprecation_warning() -> None:
    # Still works: each retired label resolves to its string value, so a caller
    # reading the old public name is not broken this release...
    with pytest.warns(DeprecationWarning, match="deprecated"):
        replicated = mareforma.REPLICATED
    assert replicated == "REPLICATED"

    with pytest.warns(DeprecationWarning, match="deprecated"):
        established = mareforma.ESTABLISHED
    assert established == "ESTABLISHED"


def test_unknown_public_attribute_still_raises() -> None:
    # The label hook must not swallow every miss into a warning: a genuine
    # typo on the public surface stays an AttributeError.
    with pytest.raises(AttributeError):
        mareforma.NOT_A_REAL_LABEL
