"""Retired trust vocabulary keeps working for one release, and warns.

Two retirements share this file. REPLICATED / ESTABLISHED were public
support-level labels; the trust map now leads with the effective-independence
number, not a single support word. CORROBORATED was the top Status verdict
word; it is renamed to the convergence marker CONVERGENT, because distinct-model
is necessary but not sufficient for independence, so the word over-claimed. Both
old names keep resolving for one release and warn on read; a future release
removes them. Neither is a schema rename: the stored ``support_level`` strings
and the promotion machinery are unchanged, and Status is recomputed on read, so
no stored status string exists to migrate.
"""

from __future__ import annotations

import pytest

import mareforma
from mareforma.trust import Status


def test_corroborated_value_lookup_deprecated() -> None:
    # The old string still resolves, so external code deserialising a
    # "CORROBORATED" value is not broken this release, and it maps to the new
    # member with a warning that names the replacement.
    with pytest.warns(DeprecationWarning, match="CONVERGENT"):
        resolved = Status("CORROBORATED")
    assert resolved is Status.CONVERGENT
    assert resolved.value == "CONVERGENT"


def test_corroborated_attribute_deprecated() -> None:
    # Attribute access to the old member name resolves to the new member and
    # warns; a genuine typo on the enum still raises AttributeError.
    with pytest.warns(DeprecationWarning, match="CONVERGENT"):
        member = Status.CORROBORATED
    assert member is Status.CONVERGENT
    with pytest.raises(AttributeError):
        Status.NOT_A_REAL_STATUS


def test_convergent_value_lookup_does_not_warn(recwarn) -> None:
    # The live name resolves cleanly, with no deprecation noise.
    assert Status("CONVERGENT") is Status.CONVERGENT
    assert not [w for w in recwarn.list if issubclass(w.category, DeprecationWarning)]


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
