"""The scramble library: shape dispatch, the null families, and a bare import.

The library picks the null family from the finding's data shape so no caller
chooses one, and it must import with the standard library alone: numpy and pandas
are test-only extras, so a shipped module that imported them would break a bare
install.
"""
from __future__ import annotations

import subprocess
import sys

import pytest

from mareforma.observe.scrambles import Scramble, scramble_family


def _named(family):
    return {s.name: s.perturbed for s in family}


def test_scalar_family_replaces_the_value_every_way():
    fam = _named(scramble_family(10.0))
    # Every null is a distinct value replacement, and none equals the input.
    assert 10.0 not in fam.values()
    assert fam["zeroed"] == 0.0
    assert fam["negated"] == -10.0


def test_scalar_family_drops_nulls_equal_to_the_input():
    # base 0.0: zeroed and negated both equal 0.0 and must be dropped, leaving
    # only nulls that actually perturb.
    fam = scramble_family(0.0)
    assert all(s.perturbed != 0.0 for s in fam)
    assert len(fam) >= 1


def test_sequence_family_has_destroying_and_marginal_preserving_nulls():
    fam = _named(scramble_family([1.0, 2.0, 3.0, 4.0]))
    assert fam["zeroed"] == [0.0, 0.0, 0.0, 0.0]
    assert fam["constant"] == [1.0e6] * 4
    # permuted and reversed hold the multiset invariant.
    assert sorted(fam["permuted"]) == [1.0, 2.0, 3.0, 4.0]
    assert fam["reversed"] == [4.0, 3.0, 2.0, 1.0]


def test_sequence_family_preserves_the_container_type():
    fam = scramble_family((1.0, 2.0, 3.0))
    assert all(isinstance(s.perturbed, tuple) for s in fam)


def test_sequence_family_dedupes_coincident_nulls():
    # On a length-2 sequence permute and reverse are the same operation, so the
    # family must not carry the null twice (it would run one measurement twice
    # and inflate the family-size correction).
    fam = scramble_family([5.0, 6.0])
    names = [s.name for s in fam]
    assert names.count("permuted") + names.count("reversed") == 1


def test_mapping_family_scrambles_the_values():
    fam = _named(scramble_family({"a": 1.0, "b": 2.0, "c": 3.0}))
    assert fam["zeroed"] == {"a": 0.0, "b": 0.0, "c": 0.0}
    # permuted keeps the keys and the value multiset, breaks the pairing.
    assert set(fam["permuted"]) == {"a", "b", "c"}
    assert sorted(fam["permuted"].values()) == [1.0, 2.0, 3.0]


@pytest.mark.parametrize(
    "unsupported",
    ["a string", b"bytes", True, False, [], {}, ["not", "numbers"],
     {"a": "x"}, [1.0, "mixed"]],
)
def test_unsupported_shapes_have_no_family(unsupported):
    assert scramble_family(unsupported) is None


def test_scramble_is_a_named_pair():
    s = Scramble("zeroed", [0.0])
    assert s.name == "zeroed"
    assert s.perturbed == [0.0]


def test_module_imports_without_numpy_or_pandas():
    # A bare install has neither. Import the module in a subprocess whose import
    # machinery raises on numpy/pandas, so an accidental top-level import fails
    # here loudly instead of on a user's machine.
    code = (
        "import builtins\n"
        "_imp = builtins.__import__\n"
        "def _blocked(name, *a, **k):\n"
        "    if name.split('.')[0] in ('numpy', 'pandas'):\n"
        "        raise ImportError('blocked ' + name)\n"
        "    return _imp(name, *a, **k)\n"
        "builtins.__import__ = _blocked\n"
        "import mareforma.observe.scrambles as s\n"
        "assert s.scramble_family(1.0) is not None\n"
        "assert s.scramble_family([1.0, 2.0]) is not None\n"
        "print('ok')\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "ok"


def test_numpy_array_is_handled_when_present():
    numpy = pytest.importorskip("numpy")
    arr = numpy.asarray([1.0, 2.0, 3.0, 4.0])
    fam = scramble_family(arr)
    assert fam is not None
    names = {s.name for s in fam}
    assert {"zeroed", "constant"} <= names
    # The perturbed inputs are rebuilt as numpy arrays so the pipeline gets what
    # it expects.
    assert all(hasattr(s.perturbed, "__array__") for s in fam)


def test_numpy_integer_array_is_handled_not_silently_dropped():
    # A numpy int scalar is not a Python int, so an int-dtype array would be
    # rejected by an isinstance(int, float) check and silently read NOT_TESTED.
    # It is an ordinary numeric finding (counts, indices) and gets a full family.
    numpy = pytest.importorskip("numpy")
    fam = scramble_family(numpy.asarray([1, 2, 3, 4]))  # int64 dtype
    assert fam is not None
    assert {"zeroed", "constant", "permuted", "reversed"} == {s.name for s in fam}


def test_numpy_bool_array_has_no_family():
    # A bool array is a mask, not a scalar metric, and must not be scrambled.
    numpy = pytest.importorskip("numpy")
    assert scramble_family(numpy.asarray([True, False, True])) is None


# -- a null identical to the base is not a perturbation ---------------------

def test_a_mapping_never_gets_a_null_equal_to_its_own_values():
    # An all-zero mapping was handed its own values back under the name "zeroed".
    # A finding cannot move under a null identical to its input, so the row read
    # as "a provable invariant of a valid null" off a null that perturbed nothing.
    fam = scramble_family({"a": 0.0, "b": 0.0})
    assert [s.name for s in fam] == ["constant"]
    assert "zeroed" in fam.dropped
    assert all(s.perturbed != {"a": 0.0, "b": 0.0} for s in fam)


def test_a_constant_sequence_reports_the_marginal_preserving_nulls_it_lost():
    # Permuting or reversing a constant sequence produces the input, so both
    # marginal-preserving nulls are unrunnable. The family must say so: without
    # them it cannot tell an honest invariant from a hollow finding, and a verdict
    # over what remains is narrower than the same verdict over ordinary data.
    fam = scramble_family([5.0, 5.0, 5.0])
    assert [s.name for s in fam] == ["zeroed", "constant"]
    assert fam.dropped == ("permuted", "reversed")
    assert fam.dropped_marginal_preserving == ("permuted", "reversed")


def test_a_length_one_sequence_reports_the_same_loss():
    fam = scramble_family([7.0])
    assert fam.dropped_marginal_preserving == ("permuted", "reversed")


def test_a_one_key_mapping_reports_that_it_cannot_permute():
    fam = scramble_family({"only": 3.0})
    assert fam.dropped_marginal_preserving == ("permuted",)


def test_a_deduped_coincident_null_is_not_reported_as_a_loss():
    # On a length-2 sequence permute and reverse are the same operation. Running
    # it twice would inflate the family-size correction, but nothing is lost, so
    # the dedup must not read as the family losing a marginal-preserving null.
    fam = scramble_family([5.0, 6.0])
    assert fam.dropped_marginal_preserving == ()
    assert "permuted" in [s.name for s in fam]


def test_mapping_values_are_gated_like_sequence_elements():
    # float() succeeding is not the gate the rest of the module applies: a bool
    # carries one bit and a numeric string is not a number. Both had a family.
    assert scramble_family({"a": True, "b": False}) is None
    assert scramble_family({"a": "3.5"}) is None


# -- the perturbed input reaches the pipeline as the type it expects --------

def test_a_namedtuple_is_rebuilt_as_itself_not_as_a_plain_tuple():
    from collections import namedtuple

    point = namedtuple("Point", "x y z")
    fam = scramble_family(point(1.0, 2.0, 3.0))
    assert all(isinstance(s.perturbed, point) for s in fam)
    # Rebuilt as a plain tuple, the pipeline's own field access raises and the
    # oracle records the TARGET as having crashed under the null, blaming the
    # target for what the harness handed it.
    assert fam[0].perturbed.x == 0.0


def test_a_tuple_subclass_that_cannot_be_rebuilt_has_no_family():
    class Weird(tuple):
        def __new__(cls, a, b):  # not constructible from a value list
            return super().__new__(cls, (a, b))

    assert scramble_family(Weird(1.0, 2.0)) is None


def test_a_labelled_series_keeps_its_labels():
    pandas = pytest.importorskip("pandas")
    series = pandas.Series([1.0, 2.0, 3.0], index=["a", "b", "c"])
    fam = scramble_family(series)
    assert fam is not None
    for s in fam:
        assert isinstance(s.perturbed, pandas.Series)
        assert list(s.perturbed.index) == ["a", "b", "c"]


def test_a_decimal_mapping_still_has_a_family():
    # decimal.Decimal is a Number but not a numbers.Real, and it is an ordinary
    # way to carry a measured quantity. Gating on Real alone stopped measuring a
    # shape that used to be measured.
    from decimal import Decimal

    fam = scramble_family({"a": Decimal("1.5"), "b": Decimal("2.5")})
    assert fam is not None
    assert [s.name for s in fam] == ["zeroed", "constant", "permuted"]


def test_a_sequence_protocol_index_method_is_not_read_as_a_label_index():
    # The Sequence protocol mandates an index() METHOD. Testing for the
    # attribute name alone rebuilt an ordinary array-like as
    # type(x)(values, index=<bound method>), which raises, and the finding then
    # read as the null failing to build.
    from mareforma.observe.scrambles import _rebuilder

    class Labelled:
        index = ["a", "b", "c"]           # sized, not callable: a label index

        def __array__(self, *a, **k):     # pragma: no cover - shape only
            raise AssertionError("not called")

    class SequenceLike:
        def index(self, value):           # the protocol's method
            return 0

        def __array__(self, *a, **k):     # pragma: no cover - shape only
            raise AssertionError("not called")

    assert _rebuilder(Labelled()).__name__ == "rebuild_labelled"
    assert _rebuilder(SequenceLike()).__name__ == "rebuild"
