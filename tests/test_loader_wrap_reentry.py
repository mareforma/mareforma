"""A loader that arrives during the wrap pass still gets wrapped.

The pass runs the wrappers in a fixed order and is guarded against re-entry,
because a lazily loading module can import another wrappable name from inside
the pass and arrive back through the import hook on the same thread. Taking the
install lock again would hang the host inside an ordinary import, so the nested
pass is skipped.

Skipping alone is not enough. A module that arrives after its own line has
already run is not picked up by the pass in flight: that line looked for it, did
not find it, and does not look again. The loader is then unwrapped for the life
of the process, and an unwrapped reader is invisible to the observer, so a cited
source reads as never opened and an honest finding floors to UNGROUNDED.

polars imported from inside the duckdb wrapper is the live case, since duckdb
runs after polars. This pins the general invariant with a stub, so it holds for
any pair and does not need the heavy loaders installed.
"""
from __future__ import annotations

import sys
import types

from mareforma.observe import _loaders


def _fake_module(name: str):
    mod = types.ModuleType(name)
    mod.read_thing = lambda *a, **k: None
    return mod


def test_a_loader_imported_during_the_pass_is_still_wrapped(monkeypatch):
    """The pass goes round again rather than assuming it covered the newcomer."""
    seen: list[str] = []
    arrived: dict = {}

    def early_wrapper() -> None:
        # Stands in for polars: its line runs before the newcomer exists.
        seen.append("early")
        if "late_arrival" in arrived:
            arrived["wrapped"] = True

    def later_wrapper() -> None:
        # Stands in for duckdb: importing from inside it brings the newcomer in
        # after the early line has already run and found nothing.
        seen.append("later")
        if not arrived:
            arrived["late_arrival"] = True
            _loaders._wrap_third_party_locked()

    monkeypatch.setattr(_loaders, "_wrap_polars_if_present", early_wrapper)
    monkeypatch.setattr(_loaders, "_wrap_duckdb_if_present", later_wrapper)

    _loaders._wrap_third_party_locked()

    assert arrived.get("wrapped"), (
        "a loader that arrived during the pass was left unwrapped: the pass ran "
        f"{seen} and never revisited the early line after the newcomer appeared"
    )


def test_the_reentrant_pass_terminates(monkeypatch):
    """A module that keeps asking must not spin the pass forever."""
    rounds = {"n": 0}

    def always_asks_again() -> None:
        rounds["n"] += 1
        _loaders._wrap_third_party_locked()

    monkeypatch.setattr(_loaders, "_wrap_duckdb_if_present", always_asks_again)

    _loaders._wrap_third_party_locked()

    assert rounds["n"] <= _loaders._MAX_WRAP_ROUNDS, (
        f"the pass ran {rounds['n']} rounds against a cap of "
        f"{_loaders._MAX_WRAP_ROUNDS}"
    )
    assert not getattr(_loaders._in_wrap, "active", False), (
        "the re-entry flag survived the pass, so every later pass is skipped"
    )
    assert not getattr(_loaders._in_wrap, "pending", False), (
        "the retry flag survived the pass"
    )


def test_the_flag_is_cleared_when_a_wrapper_raises(monkeypatch):
    """A wrapper that raises must not leave the pass permanently disabled."""
    def boom() -> None:
        raise RuntimeError("a loader's attribute read blew up")

    monkeypatch.setattr(_loaders, "_wrap_duckdb_if_present", boom)

    try:
        _loaders._wrap_third_party_locked()
    except RuntimeError:
        pass

    assert not getattr(_loaders._in_wrap, "active", False), (
        "a raising wrapper left the re-entry flag set, so every later pass is "
        "skipped and no loader is ever wrapped again"
    )
