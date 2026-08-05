"""Smoke tests for the promoted tooluniverse adapter surface.

Conceptual clusters:

- :class:`TestUriForm` — every URI exposed by the adapter is URN-form.
- :class:`TestProvenanceToolAdapter` — wrap a tool, verify the
  recorded claim shape.
- :class:`TestIdentitySanitization` — the tool name, role and
  namespace are scrubbed before they are signed.
- :class:`TestResultSizeCap` — over-cap results are refused, never
  truncated into a digest no replayer can re-derive.
- :class:`TestMissingToolVersion` — a version-less tool warns and
  records `unknown`.
- :class:`TestExecClassClaim` — exec-class tools attest only the
  execution environment they actually reported.
- :class:`TestCacheLineage` — a callee's declared cache origin is
  recorded, never turned into a graph edge.
- :class:`TestAsyncCall` — the async path records the task id and
  writes no claim when the call fails.
- :class:`TestToolCallRecorder` — coexistence convention shim.
- :class:`TestImportHygiene` — import-time registry pollution check.
"""

from __future__ import annotations

import asyncio
import importlib
import json
from pathlib import Path
from typing import Any

import pytest

import mareforma
from mareforma.predicate_types import TOOL_CALL_V1
from mareforma.tools import ToolCallError
from tests._helpers import (
    _bootstrap_key, _import_registry_delta, _two_signers,
)


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


class TestDemoToolPayloadIsolation:
    """The pinned payload stays pinned however a caller treats the result."""

    def test_call_does_not_hand_out_the_module_constant(self):
        from mareforma.adapters.tooluniverse.demo_tool import (
            MOCK_OPEN_TARGETS_PAYLOAD,
            OpenTargetsSearchTargetsMock,
        )
        result = OpenTargetsSearchTargetsMock().call(target="EGFR")
        search = result["data"]["search"]
        assert search is not MOCK_OPEN_TARGETS_PAYLOAD["search"]
        search["total"] = 999
        fresh = OpenTargetsSearchTargetsMock().call(target="EGFR")
        assert fresh["data"]["search"]["total"] == 2


class _NamedMock:
    """Minimal tool whose reported name is caller-chosen."""

    version = "0.1.0"

    def __init__(self, name: str) -> None:
        self.name = name

    def call(self, **kwargs: Any) -> dict[str, Any]:
        return {"data": {"ok": True}, "metadata": {}}


class TestIdentitySanitization:
    """The identity a tool reports is untrusted, and it gets signed."""

    def test_tool_name_scrubbed_in_signed_identity(self, graph):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        pta = ProvenanceToolAdapter(
            tool=_NamedMock("Evil​Tool"), graph=graph,
        )
        claim_id = pta.call()["metadata"]["mareforma_claim_id"]

        row = graph.get_claim(claim_id)
        assert row["generated_by"] == "adapter/executor/EvilTool"
        assert row["source_name"] == "tooluniverse/EvilTool"

    def test_role_and_namespace_scrubbed(self, graph):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        pta = ProvenanceToolAdapter(
            tool=_NamedMock("PlainTool"),
            graph=graph,
            role="exec​utor",
            tool_namespace="regis​try",
        )
        claim_id = pta.call()["metadata"]["mareforma_claim_id"]

        row = graph.get_claim(claim_id)
        assert row["generated_by"] == "adapter/executor/PlainTool"
        assert row["source_name"] == "registry/PlainTool"
        assert "​" not in row["text"]

    def test_non_string_tool_name_is_recorded_as_text(self, graph):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        pta = ProvenanceToolAdapter(tool=_NamedMock(1234), graph=graph)
        claim_id = pta.call()["metadata"]["mareforma_claim_id"]

        assert graph.get_claim(claim_id)["source_name"] == "tooluniverse/1234"

    def test_tool_name_that_scrubs_to_nothing_is_refused(self, graph):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        with pytest.raises(ValueError, match="tool.name sanitised to empty"):
            ProvenanceToolAdapter(tool=_NamedMock("​"), graph=graph)


class _OversizeMock:
    """Tool whose canonical result overruns any modest byte cap."""

    name = "bulk_fetch"
    version = "1.0.0"

    def call(self, **kwargs: Any) -> dict[str, Any]:
        return {"data": {"rows": ["x" * 256]}, "metadata": {}}


class TestResultSizeCap:
    def test_refuses_to_sign_an_over_cap_result(self, graph):
        from mareforma.adapters.tooluniverse import (
            ProvenanceToolAdapter, ResultTooLargeError,
        )
        pta = ProvenanceToolAdapter(
            tool=_OversizeMock(), graph=graph, max_result_bytes=64,
        )
        with pytest.raises(ResultTooLargeError, match="cap is 64"):
            pta.call()
        assert graph.query(include_unverified=True) == []


class _VersionlessMock:
    """Tool that reports no version at all."""

    name = "versionless_tool"

    def call(self, **kwargs: Any) -> dict[str, Any]:
        return {"data": {"ok": True}, "metadata": {}}


class TestMissingToolVersion:
    def test_warns_and_records_unknown(self, graph):
        from mareforma.adapters.tooluniverse import (
            MissingToolVersionWarning, ProvenanceToolAdapter,
        )
        from mareforma.adapters.tooluniverse.predicate import (
            decode_predicate_from_text,
        )
        with pytest.warns(MissingToolVersionWarning, match="versionless_tool"):
            pta = ProvenanceToolAdapter(tool=_VersionlessMock(), graph=graph)

        claim_id = pta.call()["metadata"]["mareforma_claim_id"]
        predicate = decode_predicate_from_text(graph.get_claim(claim_id)["text"])
        assert predicate["tool_version"] == "unknown"


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

    def test_refuses_a_close_tag_smuggled_through_arguments(self, graph):
        """The code an exec-class tool runs is signed verbatim.

        A caller who plants the close marker in it would otherwise
        split the claim text into a second, caller-written predicate
        block.
        """
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        from mareforma.tools import PredicateBoundaryError
        pta = ProvenanceToolAdapter(
            tool=_PythonExecMock(_OBSERVED_ENVIRONMENT), graph=graph,
        )
        with pytest.raises(PredicateBoundaryError):
            pta.call(code="print('</predicate>')")
        assert graph.query(include_unverified=True) == []


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

    def test_only_the_operator_parent_becomes_a_supports_edge(self, graph):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        from mareforma.adapters.tooluniverse.predicate import (
            decode_predicate_from_text,
        )
        parent = graph.assert_claim("the step that decided to call the tool")
        origin = graph.assert_claim("the finding the cache says it served")
        pta = ProvenanceToolAdapter(
            tool=_CacheHitMock(origin), graph=graph, parent_claim_id=parent,
        )
        result = pta.call(target="EGFR")

        row = graph.get_claim(result["metadata"]["mareforma_claim_id"])
        assert json.loads(row["supports_json"]) == [parent]
        predicate = decode_predicate_from_text(row["text"])
        assert predicate["parent_claim_id"] == parent
        assert predicate["cache_original_claim_id"] == origin


class _TaskManagerMock:
    """TaskManager shape: ``start_call`` hands back a task id and an awaitable."""

    name = "async_search"
    version = "2.0.0"

    def __init__(self, fail_at: str | None = None) -> None:
        self.task_id = "task-42"
        self._fail_at = fail_at

    async def start_call(self, **kwargs: Any) -> tuple[str, Any]:
        if self._fail_at == "start":
            raise RuntimeError("task manager refused the call")

        async def _await_result() -> dict[str, Any]:
            if self._fail_at == "await":
                raise RuntimeError("task failed after dispatch")
            return {"data": {"args_echo": dict(kwargs)}, "metadata": {}}

        return self.task_id, _await_result()


class TestAsyncCall:
    def test_records_the_task_id_as_the_tool_call_id(self, graph):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        from mareforma.adapters.tooluniverse.predicate import (
            decode_predicate_from_text,
        )
        tool = _TaskManagerMock()
        pta = ProvenanceToolAdapter(tool=tool, graph=graph)
        result = asyncio.run(pta.call_async(target="EGFR"))

        assert result["metadata"]["tool_call_id"] == tool.task_id
        row = graph.get_claim(result["metadata"]["mareforma_claim_id"])
        predicate = decode_predicate_from_text(row["text"])
        assert predicate["tool_call_id"] == tool.task_id
        assert predicate["arguments_canonical"] == {"target": "EGFR"}

    def test_a_refused_dispatch_writes_no_claim(self, graph):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        pta = ProvenanceToolAdapter(
            tool=_TaskManagerMock(fail_at="start"), graph=graph,
        )
        with pytest.raises(ToolCallError, match="start_call raised"):
            asyncio.run(pta.call_async(target="EGFR"))
        assert graph.query(include_unverified=True) == []

    def test_a_failed_await_writes_no_claim(self, graph):
        from mareforma.adapters.tooluniverse import ProvenanceToolAdapter
        pta = ProvenanceToolAdapter(
            tool=_TaskManagerMock(fail_at="await"), graph=graph,
        )
        with pytest.raises(ToolCallError, match="await raised"):
            asyncio.run(pta.call_async(target="EGFR"))
        assert graph.query(include_unverified=True) == []


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
        assert _import_registry_delta("mareforma.adapters.tooluniverse") == 0


class TestSelectiveWrappingRemoved:
    """The adapter wraps whatever tool the caller hands it, and says so."""

    def test_selectors_module_gone(self):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("mareforma.adapters.tooluniverse.selectors")

    def test_docstring_promises_no_selector(self):
        from mareforma.adapters.tooluniverse import adapter
        assert "selective wrapping" not in (adapter.__doc__ or "")


class TestTelemetryRemoved:
    """The adapter has one jsonl writer, `mareforma.health`, not two."""

    def test_telemetry_module_gone(self):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("mareforma.adapters.tooluniverse.telemetry")
