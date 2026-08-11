"""The scramble library: null perturbations chosen by the finding's data shape.

The oracle measures influence by destroying the cited data's content and re-running
the pipeline: if the finding moves, it depended on the data. The *null* is the
particular way the content is destroyed, and this module builds the family of nulls
for a finding automatically, from the SHAPE of its input, so no caller chooses one.
A choice is a place to fish: pick the null a quantity is provably invariant to and
the finding reads NOT_INFLUENCED however honest it is. Running the whole family
every time removes the choice, and the profile of effects across the family is what
the oracle routes on (see :func:`mareforma.observe.oracle.perturbation_oracle`).

Two families of null carry a specific job:

- *content-destroying* nulls (zero the data, replace it with a constant) move any
  finding that reads the data at all, so a finding flat under every one is hollow;
- *marginal-preserving* nulls (permute the data, reverse it) hold a statistic like a
  mean or a sum invariant while changing the ordering. A genuine mean is flat under
  these and moves under the destroying ones, which reads as *moves under some nulls,
  not others*, the honest-invariant case the oracle routes to UNDECIDABLE rather than
  calling a real computation hollow.

The module imports with the standard library alone. ``numpy`` and ``pandas`` are
test-only extras, never dependencies, so a shipped module that imported them would
break a bare install; a numpy array passed at run time is handled by a lazy import
inside the one branch that needs it, which only runs when numpy is already present.
"""
from __future__ import annotations

import numbers
import random
from dataclasses import dataclass
from typing import Any

# A fixed seed so a permutation null is reproducible run to run: the oracle
# compares a base run against a perturbed run, and a null that reshuffled itself
# on every call would add noise the oracle would read as the pipeline's own. The
# seed is a constant, NOT derived from the input: the input must not choose or
# influence its own null.
_SEED = 0x5CA3B1E


@dataclass(frozen=True)
class Scramble:
    """One null: a name for the record and the already-perturbed input to run."""

    name: str
    perturbed: Any


def scramble_family(base_input: Any) -> "list[Scramble] | None":
    """Build the family of nulls for ``base_input`` from its data shape.

    Returns the list of scrambles to run the pipeline against, or ``None`` when
    no family fits the shape (the caller reads that as a NOT_TESTED verdict with
    reason ``unsupported-shape``). The shapes handled:

    - a real scalar (``int`` / ``float``, never ``bool``): value-replacement nulls;
    - a mapping of scalars: nulls over the values (zero, constant, permute across
      keys);
    - a sequence of scalars (``list`` / ``tuple``, or a numpy array): content-
      destroying and marginal-preserving nulls.

    A string, a boolean, an empty container, or a container of non-scalars has no
    family and yields ``None``.
    """
    if isinstance(base_input, bool):
        # bool is an int subclass but not a metric: it carries one bit, so there
        # is no distribution to scramble and no scale to move.
        return None
    if isinstance(base_input, numbers.Real):
        # Any real scalar, including a numpy scalar (numpy.float64 / numpy.int64),
        # which registers as numbers.Real without importing numpy here.
        return _scalar_family(float(base_input))
    if isinstance(base_input, dict):
        return _mapping_family(base_input)
    values = _as_scalar_sequence(base_input)
    if values is not None:
        return _sequence_family(base_input, values)
    return None


def _scalar_family(x: float) -> "list[Scramble] | None":
    """Value-replacement nulls for a bare scalar.

    A scalar has no internal structure to permute, so every null simply replaces
    it with a different value: a finding that depends on it moves under all of
    them, a constant fallback under none. The replacements are de-duplicated and
    forced to differ from ``x`` (a null equal to the input is not a perturbation),
    so a scalar near one of the sentinels still gets a real family.
    """
    candidates = [0.0, -x, x + 1.0, x * 2.0, 1.0e6]
    seen: set[float] = set()
    family: list[Scramble] = []
    names = ["zeroed", "negated", "offset", "scaled", "replaced"]
    for name, value in zip(names, candidates):
        if value == x or value in seen:
            continue
        seen.add(value)
        family.append(Scramble(name, value))
    if not family:
        # x coincided with every sentinel (only possible for a degenerate set);
        # fall back to a single guaranteed-distinct null.
        family.append(Scramble("offset", x + 1.0))
    return family


def _mapping_family(mapping: "dict") -> "list[Scramble] | None":
    """Nulls over the values of a mapping of scalars.

    Zero and constant destroy the values; permuting the values across the keys
    holds the value multiset invariant while breaking the key-to-value pairing,
    so a finding that reads one key's value moves but a finding over the whole
    multiset (a sum, a mean) does not.
    """
    if not mapping:
        return None
    keys = list(mapping)
    try:
        values = [float(mapping[k]) for k in keys]
    except (TypeError, ValueError):
        return None
    family = [
        Scramble("zeroed", {k: 0.0 for k in keys}),
        Scramble("constant", {k: 1.0e6 for k in keys}),
    ]
    if len(keys) > 1:
        shuffled = list(values)
        random.Random(_SEED).shuffle(shuffled)
        # A permutation that leaves the key-to-value pairing unchanged is not a
        # perturbation; skip it rather than run a null identical to the base.
        if shuffled != values:
            family.append(Scramble("permuted", dict(zip(keys, shuffled))))
    return family


def _sequence_family(
    base_input: Any, values: "list[float]"
) -> "list[Scramble] | None":
    """Content-destroying and marginal-preserving nulls for a scalar sequence.

    Zero and constant destroy the content; permute and reverse hold the multiset
    invariant. The perturbed input is rebuilt in the container's own type so the
    pipeline receives what it expects: a list stays a list, a tuple a tuple, and a
    numpy array is rebuilt through a lazy import that only runs for a numpy input.
    """
    if not values:
        return None
    rebuild = _rebuilder(base_input)
    if rebuild is None:
        return None
    n = len(values)
    # Build (name, value-list) pairs and de-duplicate on the value list before
    # rebuilding: a null equal to the input is not a perturbation, and two nulls
    # that coincide (permute and reverse agree on a length-2 sequence) would run
    # the same measurement twice and inflate the family size. Comparison is on
    # plain float lists, so a numpy input never hits an ambiguous array truth.
    candidates = [
        ("zeroed", [0.0] * n),
        ("constant", [1.0e6] * n),
    ]
    if n > 1:
        shuffled = list(values)
        random.Random(_SEED).shuffle(shuffled)
        candidates.append(("permuted", shuffled))
        candidates.append(("reversed", list(reversed(values))))
    family: list[Scramble] = []
    seen: list[list[float]] = [values]
    for name, vs in candidates:
        if vs in seen:
            continue
        seen.append(vs)
        family.append(Scramble(name, rebuild(vs)))
    return family or None


def _as_scalar_sequence(base_input: Any) -> "list[float] | None":
    """The sequence's elements as floats, or None if it is not a scalar sequence.

    A string is a sequence of characters, never a scalar sequence. A numpy array
    is accepted by duck typing (it is iterable of numbers) without importing numpy
    here.
    """
    if isinstance(base_input, (str, bytes, dict)):
        return None
    if isinstance(base_input, (list, tuple)):
        items = base_input
    elif hasattr(base_input, "__array__") and hasattr(base_input, "__iter__"):
        # A numpy (or array-like) input: iterate it without importing numpy.
        try:
            items = list(base_input)
        except TypeError:
            return None
    else:
        return None
    try:
        out = []
        for item in items:
            if isinstance(item, bool):
                # A bool element makes this a mask, not a scalar metric.
                return None
            if not isinstance(item, numbers.Real):
                # numbers.Real covers int, float, and numpy integer/float scalars
                # (numpy registers them), so an int-dtype array is handled; a
                # numpy bool array element is not Real and correctly rejected.
                return None
            out.append(float(item))
    except TypeError:
        return None
    return out


def _rebuilder(base_input: Any):
    """A callable that rebuilds a scrambled value list in the input's own type.

    Returns None when the type cannot be rebuilt from a list of floats. A numpy
    array is rebuilt through a lazy import so the module still imports without
    numpy installed; the import only runs when a numpy array was actually passed,
    which means numpy is present.
    """
    if isinstance(base_input, list):
        return lambda vs: list(vs)
    if isinstance(base_input, tuple):
        return lambda vs: tuple(vs)
    if hasattr(base_input, "__array__"):
        def rebuild(vs):
            import numpy  # lazy: only for a numpy input, never at module import

            return numpy.asarray(vs, dtype=getattr(base_input, "dtype", float))

        return rebuild
    return None
