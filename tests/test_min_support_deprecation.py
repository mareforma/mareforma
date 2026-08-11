"""The support-ladder retirement reaches the path callers actually take.

The retirement warned only on ``mareforma.REPLICATED``, the module attribute.
Nobody reads the ladder that way: callers pass the level as a plain string to
``query(min_support=...)``. So the announcement fired on the one path nobody
takes and stayed silent on the path everybody does, and the v0.4.0 removal
would have arrived unannounced for every real caller.

These tests are where the ``min_support`` warning is proved to fire. The suite
ignores it by message (pyproject ``filterwarnings``) because ~40 call sites are
scaffolding for tests about something else, the same treatment the seed-anchor
deprecation gets and for the same reason.
"""
from __future__ import annotations

import warnings

import pytest


_MATCH = r"query\(min_support=\.\.\.\) is deprecated"


@pytest.fixture()
def seeded_graph(open_graph):
    """One signed claim to read back, on the suite's canonical open graph.

    Named apart from the canonical ``graph`` fixture rather than shadowing it:
    a local redefinition of a shared fixture name reads as the shared one to
    anyone skimming the file.
    """
    open_graph.assert_claim("a finding", generated_by="agent/x")
    return open_graph


def test_query_with_min_support_warns(seeded_graph):
    with pytest.warns(DeprecationWarning, match=_MATCH):
        seeded_graph.query(min_support="PRELIMINARY")


def test_search_with_min_support_warns(seeded_graph):
    with pytest.warns(DeprecationWarning, match=_MATCH):
        seeded_graph.search("finding", min_support="PRELIMINARY")


def test_query_for_llm_warns_through_the_query_it_delegates_to(seeded_graph):
    with pytest.warns(DeprecationWarning, match=_MATCH):
        seeded_graph.query_for_llm(min_support="PRELIMINARY")


def test_the_warning_names_what_replaces_the_ladder(seeded_graph):
    with pytest.warns(DeprecationWarning, match=_MATCH) as caught:
        seeded_graph.query(min_support="PRELIMINARY")
    message = str(caught[0].message)
    # A deprecation that does not say what to do instead is a dead end.
    assert "v0.4.0" in message
    assert "independence" in message


def test_a_read_that_does_not_filter_on_the_ladder_is_silent(seeded_graph):
    """The warning must fire on the retired argument, not on every read."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        seeded_graph.query()
        seeded_graph.search("finding")


def test_the_agent_tool_does_not_warn_when_no_level_was_named(seeded_graph):
    """The deprecation must fire on the caller's choice, not a library default.

    ``get_tools()``'s ``query_graph`` defaulted ``min_support`` to
    ``"PRELIMINARY"``. That is the floor of the ladder, so it filtered nothing,
    but it still reached ``query`` as an explicit argument: every call warned
    about a retired feature the caller never asked for, naming a default the
    agent author cannot change. Passing nothing now means asking for nothing.
    """
    query_graph = next(t for t in seeded_graph.get_tools() if t.__name__ == "query_graph")
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        query_graph("finding")


def test_the_agent_tool_still_warns_when_a_level_is_named(seeded_graph):
    """The control: an explicit level is the caller's choice, so it warns."""
    query_graph = next(t for t in seeded_graph.get_tools() if t.__name__ == "query_graph")
    with pytest.warns(DeprecationWarning, match=_MATCH):
        query_graph("finding", min_support="REPLICATED")


def test_dropping_the_default_does_not_change_which_claims_come_back(seeded_graph):
    """support_level is NOT NULL CHECK(IN the three), so the floor filters nothing."""
    query_graph = next(t for t in seeded_graph.get_tools() if t.__name__ == "query_graph")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        assert query_graph("finding") == query_graph("finding", min_support="PRELIMINARY")


def test_the_warning_is_attributed_to_the_caller_not_the_library(seeded_graph):
    """Emission is not the property that matters; attribution is.

    Python's default filter ignores a DeprecationWarning unless it comes from
    __main__, so a warning attributed to a frame inside mareforma is invisible
    to every real caller, and every call site collapses onto one dedup key so
    only the first reports. Every test above uses pytest.warns, which forces
    the "always" filter and therefore cannot see the difference. This one can.

    The delegating paths are the point: query_for_llm and the get_tools tool
    both reach `query` through library frames, so any fixed stacklevel that is
    right for a direct call is wrong for them.
    """
    query_graph = next(
        t for t in seeded_graph.get_tools() if t.__name__ == "query_graph"
    )
    paths = {
        "query": lambda: seeded_graph.query(min_support="PRELIMINARY"),
        "search": lambda: seeded_graph.search("finding", min_support="PRELIMINARY"),
        "query_for_llm": lambda: seeded_graph.query_for_llm(
            min_support="PRELIMINARY"),
        "query_graph": lambda: query_graph("finding", min_support="REPLICATED"),
    }
    for name, call in paths.items():
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            call()
        emitted = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert emitted, f"{name} emitted no DeprecationWarning at all"
        assert emitted[0].filename == __file__, (
            f"{name} attributed its warning to {emitted[0].filename}, inside the "
            "library, where the default filter hides it from every caller"
        )
