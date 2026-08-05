"""Smoke tests for the promoted tooluniverse adapter surface.

Conceptual clusters:

- :class:`TestUriForm` — every URI exposed by the adapter is URN-form.
- :class:`TestProvenanceToolAdapter` — wrap a tool, verify the
  recorded claim shape.
- :class:`TestExecClassClaim` — exec-class tools attest only the
  execution environment they actually reported.
- :class:`TestCacheLineage` — a callee's declared cache origin is
  recorded, never turned into a graph edge.
- :class:`TestToolCallRecorder` — coexistence convention shim.
- :class:`TestImportHygiene` — import-time registry pollution check.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import mareforma
from mareforma.predicate_types import TOOL_CALL_V1
from mareforma.tools import ToolCallError
from tests._helpers import _bootstrap_key, _two_signers


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


class _PythonExecMock:
    """Exec-class stand-in whose reported environment metadata is settable."""

    name = "python_exec"
    version = "0.1.0"
    category = "python_exec"

    def __init__(self, metadata: dict[str, Any]) -> None:
        self._metadata = metadata

    def call(self, **kwargs: Any) -> dict[str, Any]:
        return {"data": {"x": 1}, "metadata": dict(self._metadata)}


_OBSERVED_ENVIRONMENT = {
    "image_digest": "sha256:" + "a" * 64,
    "source_digest": "sha256:" + "b" * 64,
    "runtime": "gvisor",
    "variance_mode": "nondeterministic",
}


class TestExecClassClaim:
    def test_refuses_to_sign_unreported_environment(self, graph):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        pta = ProvenanceToolAdapter(tool=_PythonExecMock({}), graph=graph)
        with pytest.raises(ToolCallError, match="image_digest"):
            pta.call(code="print(1)")
        assert graph.query(include_unverified=True) == []

    def test_refuses_when_one_field_is_missing(self, graph):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        partial = dict(_OBSERVED_ENVIRONMENT)
        del partial["runtime"]
        pta = ProvenanceToolAdapter(tool=_PythonExecMock(partial), graph=graph)
        with pytest.raises(ToolCallError, match="runtime"):
            pta.call(code="print(1)")

    def test_records_the_reported_environment(self, graph):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        from mareforma.adapters.tooluniverse.exec_routing import (
            decode_container_exec_predicate,
        )
        pta = ProvenanceToolAdapter(
            tool=_PythonExecMock(_OBSERVED_ENVIRONMENT), graph=graph,
        )
        result = pta.call(code="print(1)")
        row = graph.get_claim(result["metadata"]["mareforma_claim_id"])
        predicate = decode_container_exec_predicate(row["text"])
        for field, value in _OBSERVED_ENVIRONMENT.items():
            assert predicate[field] == value


class _CacheHitMock:
    """Retrieval tool that declares a cache hit against a claim it names."""

    name = "cached_search"
    version = "1.0.0"
    category = "pharmacology"

    def __init__(self, original_claim_id: str) -> None:
        self._original_claim_id = original_claim_id

    def call(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "data": {"hits": []},
            "metadata": {
                "cache_hit": True,
                "cache_origin": "external-cache",
                "cache_original_claim_id": self._original_claim_id,
            },
        }


class TestCacheLineage:
    def test_declared_cache_origin_writes_no_supports_edge(
        self, tmp_path: Path,
    ):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        from mareforma.adapters.tooluniverse.predicate import (
            decode_predicate_from_text,
        )
        _sa, sb = _two_signers(tmp_path)
        with mareforma.open(
            tmp_path, key_path=_bootstrap_key(tmp_path, "root.key"),
        ) as graph:
            anchor = graph.assert_claim(
                "anchor", generated_by="seed", seed=True,
            )
            peer = graph.assert_claim(
                "an honest peer finding", supports=[anchor],
                generated_by="lab_b", signer=sb,
            )
            assert graph.get_claim(peer)["support_level"] == "PRELIMINARY"

            pta = ProvenanceToolAdapter(
                tool=_CacheHitMock(anchor), graph=graph,
            )
            result = pta.call(target="EGFR")

            row = graph.get_claim(result["metadata"]["mareforma_claim_id"])
            assert json.loads(row["supports_json"]) == []
            assert row["support_level"] == "PRELIMINARY"
            assert graph.get_claim(peer)["support_level"] == "PRELIMINARY"
            predicate = decode_predicate_from_text(row["text"])
            assert predicate["cache_original_claim_id"] == anchor


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
