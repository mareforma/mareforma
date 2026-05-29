"""Smoke tests for the promoted tooluniverse adapter surface.

Conceptual clusters:

- :class:`TestUriForm` — every URI exposed by the adapter is URN-form.
- :class:`TestProvenanceToolAdapter` — wrap a tool, verify the
  recorded claim shape.
- :class:`TestToolCallRecorder` — coexistence convention shim.
- :class:`TestImportHygiene` — import-time registry pollution check.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from mareforma.predicate_types import TOOL_CALL_V1


@pytest.fixture()
def graph(tmp_path: Path):
    from mareforma import signing as _signing
    key_path = tmp_path / "key"
    _signing.bootstrap_key(key_path)
    with mareforma.open(tmp_path, key_path=key_path) as g:
        yield g


class TestUriForm:
    def test_predicate_uri_is_urn_form(self):
        from mareforma.adapters.tooluniverse import PREDICATE_TYPE_V1
        assert PREDICATE_TYPE_V1 == TOOL_CALL_V1
        assert PREDICATE_TYPE_V1.startswith("urn:mareforma:predicate:")

    def test_container_exec_uri_is_urn_form(self):
        from mareforma.adapters.tooluniverse.exec_routing import (
            CONTAINER_EXEC_PREDICATE_TYPE,
        )
        assert CONTAINER_EXEC_PREDICATE_TYPE.startswith(
            "urn:mareforma:predicate:"
        )


class TestProvenanceToolAdapter:
    def test_wraps_a_demo_tool(self, graph):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        from mareforma.adapters.tooluniverse.demo_tool import (
            OpenTargetsSearchTargetsMock,
        )
        pta = ProvenanceToolAdapter(
            tool=OpenTargetsSearchTargetsMock(), graph=graph,
        )
        result = pta.call(target="EGFR")
        assert "mareforma_claim_id" in result["metadata"]
        assert result["data"]["args_echo"]["target"] == "EGFR"


class TestToolCallRecorder:
    def test_emits_claim(self, graph):
        from mareforma.adapters.tooluniverse import ToolCallRecorder
        rec = ToolCallRecorder(graph=graph)
        cid = rec.emit_sample()
        assert cid
        row = graph.get_claim(cid)
        assert row is not None
        assert "tool-call" in row["text"]

    def test_predicate_uris(self):
        from mareforma.adapters.tooluniverse import ToolCallRecorder
        assert ToolCallRecorder().predicate_uris() == (TOOL_CALL_V1,)


class TestImportHygiene:
    def test_import_does_not_pollute_predicate_registry(self):
        from mareforma.predicate_types import predicates
        before = len(predicates())
        import mareforma.adapters.tooluniverse  # noqa: F401
        after = len(predicates())
        assert before == after
