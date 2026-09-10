"""The support ladder is gone from every read a caller can reach.

It was deprecated on the path callers actually take, which is a plain string
passed to ``query(min_support=...)`` rather than the module attribute nobody
reads. That warning is what this removal was announced through, so these tests
stand where the warning's tests used to.

What is checked here is that the parameter is gone from all four public reads
and from the agent tool's own schema, and that the reads still work without it.
The stored column and the storage layer's filter are a separate step and still
answer; nothing public reaches them.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import mareforma


@pytest.fixture()
def seeded_graph(open_graph):
    """One signed claim to read back, on the suite's canonical open graph."""
    open_graph.assert_claim("a finding to read back", generated_by="run")
    return open_graph


class TestTheParameterIsGone:
    """Every public read that used to take a level, and the tool schema."""

    def test_query_does_not_take_it(self, seeded_graph) -> None:
        with pytest.raises(TypeError, match="min_support"):
            seeded_graph.query(min_support="REPLICATED")

    def test_search_does_not_take_it(self, seeded_graph) -> None:
        with pytest.raises(TypeError, match="min_support"):
            seeded_graph.search("finding", min_support="REPLICATED")

    def test_query_for_llm_does_not_take_it(self, seeded_graph) -> None:
        with pytest.raises(TypeError, match="min_support"):
            seeded_graph.query_for_llm("finding", min_support="REPLICATED")

    def test_the_agent_tool_does_not_take_it(self, seeded_graph) -> None:
        """The tool an agent is handed, which is a published schema.

        An agent author who wrote the level into a call gets an error naming
        the argument rather than a silently ignored filter.
        """
        query_graph = seeded_graph.get_tools()[0]
        with pytest.raises(TypeError, match="min_support"):
            query_graph(topic="finding", min_support="REPLICATED")

    def test_the_tool_schema_no_longer_advertises_it(self, seeded_graph) -> None:
        """The signature and docstring are the schema an SDK builds from.

        Leaving the level in either would have every agent keep passing an
        argument the call now refuses, which is worse than never offering it.
        """
        import inspect

        for tool in seeded_graph.get_tools():
            assert "min_support" not in inspect.signature(tool).parameters
            assert "min_support" not in (tool.__doc__ or "")


class TestWhereTheRefusalDoesNotReach:
    """The limit of the guarantee above, written down rather than assumed.

    A direct call refuses an argument the function does not take. Wrapped in a
    tool decorator that builds a pydantic schema from the signature, which is
    the wrapping the docs recommend, an unknown key is dropped before the call
    is ever made. Nothing on this side can see it happen, so nothing here can
    refuse it.

    That matters because it is the path an agent takes. An agent still passing
    a level gets a full unfiltered read rather than an error, and a caller
    treating the result as filtered would be reading rows the filter would have
    excluded. The honest statement is that the refusal holds for the callable
    and not for every wrapper around it.
    """

    def test_the_wrapper_drops_it_instead_of_refusing(self, seeded_graph) -> None:
        tool = pytest.importorskip("langchain_core.tools").tool
        import json

        query_graph = tool(seeded_graph.get_tools()[0])
        # No error, and the read is the same one an honest call gets.
        wrapped = json.loads(
            query_graph.invoke({"topic": "finding", "min_support": "ESTABLISHED"})
        )
        honest = json.loads(query_graph.invoke({"topic": "finding"}))
        assert wrapped == honest

    def test_the_bare_callable_still_refuses(self, seeded_graph) -> None:
        """The half that does hold, kept beside the half that does not."""
        with pytest.raises(TypeError, match="min_support"):
            seeded_graph.get_tools()[0](topic="finding", min_support="ESTABLISHED")


class TestTheReadsStillWork:
    """The direction that matters more: removing a filter removed only it."""

    def test_query_returns_the_claim(self, seeded_graph) -> None:
        rows = seeded_graph.query("finding")
        assert [r["text"] for r in rows] == ["a finding to read back"]

    def test_search_returns_the_claim(self, seeded_graph) -> None:
        rows = seeded_graph.search("finding")
        assert [r["text"] for r in rows] == ["a finding to read back"]

    def test_the_agent_tool_returns_the_claim(self, seeded_graph) -> None:
        import json

        query_graph = seeded_graph.get_tools()[0]
        assert json.loads(query_graph(topic="finding"))

    def test_nothing_warns_about_a_ladder_any_more(
        self, seeded_graph, recwarn,
    ) -> None:
        """The deprecation went with the thing it announced.

        A warning that outlives its removal is noise the caller cannot act on.
        """
        seeded_graph.query("finding")
        seeded_graph.search("finding")
        assert not [w for w in recwarn if "min_support" in str(w.message)]


class TestTheStorageLayerStillAnswers:
    """Where the filter still lives, and why that is not a leak.

    The stored column and the reads under it go in the step that changes the
    schema. Until then the storage layer keeps its filter, and the point here
    is that no public surface reaches it: an argument that cannot be passed
    cannot be depended on, whatever still exists beneath it.
    """

    def test_no_public_read_forwards_a_level(self, tmp_path: Path) -> None:
        import inspect

        from mareforma._graph import EpistemicGraph

        for name in ("query", "search", "query_for_llm"):
            signature = inspect.signature(getattr(EpistemicGraph, name))
            assert "min_support" not in signature.parameters, name
