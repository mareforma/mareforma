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


# The nulls that hold a distribution's marginal invariant while changing the
# ordering. They are what separates a genuine mean (flat under these, moves under
# the destroying ones) from a hollow finding (flat under everything), so a family
# that lost them cannot make that distinction and the oracle has to say so.
MARGINAL_PRESERVING = frozenset({"permuted", "reversed"})


@dataclass(frozen=True)
class ScrambleFamily:
    """The nulls that will run, and the ones the input itself ruled out.

    A null identical to the base input is not a perturbation, so it is dropped
    rather than run. Which nulls were dropped is part of the measurement, not
    bookkeeping: a constant-valued sequence drops both marginal-preserving nulls
    (permuting constants changes nothing), so a verdict over what remains rests on
    a narrower family than the shape normally supplies. ``dropped`` carries those
    names so the oracle can say which nulls its verdict never tried, instead of
    reporting "under every null" about a family the data quietly shrank.

    Iterating, indexing and ``len`` see the nulls that will run, so a caller that
    only wants the family reads it like the sequence it replaces.
    """

    scrambles: "tuple[Scramble, ...]"
    dropped: "tuple[str, ...]" = ()

    def __iter__(self):
        return iter(self.scrambles)

    def __len__(self) -> int:
        return len(self.scrambles)

    def __getitem__(self, index):
        return self.scrambles[index]

    @property
    def dropped_marginal_preserving(self) -> "tuple[str, ...]":
        """The marginal-preserving nulls the input ruled out, if any.

        Non-empty means the family cannot separate an honest invariant from a
        hollow finding, because the nulls that would have shown the difference
        could not be built from this input.
        """
        return tuple(n for n in self.dropped if n in MARGINAL_PRESERVING)


def scramble_family(base_input: Any) -> "ScrambleFamily | None":
    """Build the family of nulls for ``base_input`` from its data shape.

    Returns a :class:`ScrambleFamily` (the nulls to run, plus the ones the input
    ruled out), or ``None`` when no family fits the shape (the caller reads that
    as a NOT_TESTED verdict with reason ``unsupported-shape``). The shapes handled:

    - a real scalar (``int`` / ``float``, never ``bool``): value-replacement nulls;
    - a mapping of scalars: nulls over the values (zero, constant, permute across
      keys);
    - a sequence of scalars (``list`` / ``tuple``, a namedtuple, or a numpy array):
      content-destroying and marginal-preserving nulls.

    A string, a boolean, an empty container, a container of non-scalars, or a
    container that cannot be rebuilt in its own type has no family and yields
    ``None``. Every family drops a null that would equal the base input, since
    running the base against itself measures nothing; the dropped names stay on
    the family so the verdict can name what it never tried.
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


def _scalar_family(x: float) -> "ScrambleFamily | None":
    """Value-replacement nulls for a bare scalar.

    A scalar has no internal structure to permute, so every null simply replaces
    it with a different value: a finding that depends on it moves under all of
    them, a constant fallback under none. The replacements are de-duplicated and
    forced to differ from ``x`` (a null equal to the input is not a perturbation),
    so a scalar near one of the sentinels still gets a real family. A scalar has
    no marginal-preserving null at all, so a dropped sentinel narrows the family
    without changing what it can distinguish.
    """
    candidates = [0.0, -x, x + 1.0, x * 2.0, 1.0e6]
    seen: set[float] = set()
    family: list[Scramble] = []
    dropped: list[str] = []
    names = ["zeroed", "negated", "offset", "scaled", "replaced"]
    for name, value in zip(names, candidates):
        if value == x:
            dropped.append(name)  # identical to the base: nothing to perturb
            continue
        if value in seen:
            continue  # coincident with a null already here: a dedup, not a loss
        seen.add(value)
        family.append(Scramble(name, value))
    if not family:
        # Unreachable today: the candidates include both 0.0 and 1.0e6 and one
        # float cannot equal both. Kept as a guard so a future sentinel change
        # cannot silently return an empty family.
        family.append(Scramble("offset", x + 1.0))
    return ScrambleFamily(tuple(family), tuple(dropped))


def _mapping_family(mapping: "dict") -> "ScrambleFamily | None":
    """Nulls over the values of a mapping of scalars.

    Zero and constant destroy the values; permuting the values across the keys
    holds the value multiset invariant while breaking the key-to-value pairing,
    so a finding that reads one key's value moves but a finding over the whole
    multiset (a sum, a mean) does not.

    Every null is compared against the base values and dropped when it matches:
    an all-zero mapping would otherwise be handed its own values back under the
    name ``zeroed``, and a finding that necessarily fails to move under a null
    identical to its input would read as invariant under a null nothing perturbed.
    The mapping values are gated on ``numbers.Real`` (never ``bool``), the same
    rule the sequence path applies to its elements, so a mapping of flags or of
    numeric strings has no family rather than a silently coerced one.
    """
    if not mapping:
        return None
    keys = list(mapping)
    values: list[float] = []
    for k in keys:
        v = mapping[k]
        if isinstance(v, bool) or not isinstance(v, numbers.Real):
            return None
        values.append(float(v))
    shuffled = list(values)
    random.Random(_SEED).shuffle(shuffled)
    # ``permuted`` is always a candidate, including on a one-key mapping where it
    # cannot differ from the base, so a family that loses it reports the loss
    # instead of quietly shipping without a marginal-preserving null.
    candidates = [
        ("zeroed", [0.0] * len(keys)),
        ("constant", [1.0e6] * len(keys)),
        ("permuted", shuffled),
    ]
    family: list[Scramble] = []
    dropped: list[str] = []
    seen: list[list[float]] = []
    for name, vs in candidates:
        if vs == values:
            dropped.append(name)  # identical to the base: a real loss
            continue
        if vs in seen:
            continue  # coincident with a null already here: a dedup, not a loss
        seen.append(vs)
        family.append(Scramble(name, dict(zip(keys, vs))))
    if not family:
        return None
    return ScrambleFamily(tuple(family), tuple(dropped))


def _sequence_family(
    base_input: Any, values: "list[float]"
) -> "ScrambleFamily | None":
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
    shuffled = list(values)
    random.Random(_SEED).shuffle(shuffled)
    # The marginal-preserving nulls are always CANDIDATES, even on a length-1
    # sequence where they cannot differ from the base. Listing them and dropping
    # them is what lets the family report that it lost them: silently skipping
    # them would leave a narrowed family indistinguishable from a full one.
    candidates.append(("permuted", shuffled))
    candidates.append(("reversed", list(reversed(values))))
    family: list[Scramble] = []
    dropped: list[str] = []
    seen: list[list[float]] = []
    for name, vs in candidates:
        if vs == values:
            # Identical to the base: this null cannot perturb anything, and the
            # family is genuinely narrower for losing it. Recorded.
            dropped.append(name)
            continue
        if vs in seen:
            # Coincident with a null already in the family (permute and reverse
            # agree on a length-2 sequence). Running it twice would measure the
            # same thing twice and inflate the family-size correction, but
            # nothing is lost, so this is a dedup, not a drop.
            continue
        seen.append(vs)
        family.append(Scramble(name, rebuild(vs)))
    if not family:
        return None
    return ScrambleFamily(tuple(family), tuple(dropped))


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

    Returns None when the type cannot be rebuilt from a list of floats, which the
    caller reads as a shape with no family. Refusing is the honest outcome: a
    container rebuilt as the wrong type reaches the pipeline as something it did
    not expect, and the AttributeError that follows would be recorded as the
    TARGET crashing under a null when the harness handed it the wrong thing.

    A numpy array and a pandas-style labelled series are rebuilt through a lazy
    import so the module still imports with the standard library alone; the import
    only runs when such an input was actually passed, which means the library that
    produced it is already installed.
    """
    if isinstance(base_input, list):
        return lambda vs: list(vs)
    if isinstance(base_input, tuple):
        fields = getattr(type(base_input), "_fields", None)
        if fields is None:
            if type(base_input) is not tuple:
                # A tuple subclass we cannot construct positionally from values.
                return None
            return lambda vs: tuple(vs)
        # A namedtuple: rebuild it as its own type so the pipeline still reads
        # its fields by name.
        return lambda vs: type(base_input)(*vs)
    if hasattr(base_input, "__array__"):
        index = getattr(base_input, "index", None)
        if index is not None:
            # A labelled series: the labels are part of what the pipeline reads,
            # so a null must keep them rather than hand back a bare array.
            def rebuild_labelled(vs):
                return type(base_input)(vs, index=index)

            return rebuild_labelled

        def rebuild(vs):
            import numpy  # lazy: only for a numpy input, never at module import

            return numpy.asarray(vs, dtype=getattr(base_input, "dtype", float))

        return rebuild
    return None
