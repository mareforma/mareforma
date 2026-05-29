"""Cross-adapter coexistence: every adapter must share a single graph
without colliding on predicate URIs, registry entries, or schema.

This is the contract test for the ``mareforma.adapters.*`` namespace:
a user who installs more than one extra must be able to instantiate
them all against the same ``EpistemicGraph`` and have every adapter's
claims persist with distinct predicate URIs.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import mareforma


pytestmark = pytest.mark.adapters


def _try_import(name: str):
    """Import an adapter or skip — extras may be uninstalled in CI."""
    try:
        return __import__(name, fromlist=["*"])
    except ImportError as exc:
        pytest.skip(f"adapter not installed: {name} ({exc})")


def test_three_adapters_share_one_graph(tmp_path: Path):
    """Instantiate all three adapters against one graph; verify no
    predicate-URI collisions and that every adapter's emission is
    durably recorded."""
    claw = _try_import("mareforma.adapters.clawinstitute")
    tu = _try_import("mareforma.adapters.tooluniverse")
    gem = _try_import("mareforma.adapters.gemini")

    from mareforma import signing as _signing
    key_path = tmp_path / "mareforma.key"
    _signing.bootstrap_key(key_path)

    with mareforma.open(tmp_path, key_path=key_path) as graph:
        claw_emitter = claw.EventHook(graph=graph)
        tu_emitter = tu.ToolCallRecorder(graph=graph)
        gem_emitter = gem.OutputIngester(graph=graph)

        claw_uris = set(claw_emitter.predicate_uris())
        tu_uris = set(tu_emitter.predicate_uris())
        gem_uris = set(gem_emitter.predicate_uris())

        # Capability-shaped URN form across the board.
        for uri in claw_uris | tu_uris | gem_uris:
            assert uri.startswith("urn:mareforma:predicate:"), (
                f"predicate {uri!r} is not URN-form"
            )

        # No accidental collisions across adapters.
        assert claw_uris.isdisjoint(tu_uris), (
            f"clawinstitute and tooluniverse share predicates: "
            f"{claw_uris & tu_uris}"
        )
        assert claw_uris.isdisjoint(gem_uris), (
            f"clawinstitute and gemini share predicates: "
            f"{claw_uris & gem_uris}"
        )
        assert tu_uris.isdisjoint(gem_uris), (
            f"tooluniverse and gemini share predicates: "
            f"{tu_uris & gem_uris}"
        )

        # Each adapter records at least one claim against the shared graph.
        claw_id = claw_emitter.emit_sample()
        tu_id = tu_emitter.emit_sample()
        gem_id = gem_emitter.emit_sample()

        all_ids = {claw_id, tu_id, gem_id}
        assert len(all_ids) == 3, "adapters produced duplicate claim ids"

        for cid in all_ids:
            row = graph.get_claim(cid)
            assert row is not None, f"claim {cid} not persisted"


def test_adapter_imports_do_not_pollute_predicate_registry():
    """Importing an adapter must not register predicates as a side
    effect — registration is the emitter's job, not import-time."""
    from mareforma import predicate_types as _pt
    before = set(_pt._registry)

    for name in (
        "mareforma.adapters.clawinstitute",
        "mareforma.adapters.tooluniverse",
        "mareforma.adapters.gemini",
    ):
        _try_import(name)

    after = set(_pt._registry)
    assert before == after, (
        f"adapter import polluted predicate registry: {after - before}"
    )
