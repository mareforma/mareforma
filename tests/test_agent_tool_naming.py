"""the claim-recording agent tool is renamed off the shadowed name.

``get_tools`` returned an inner closure named ``assert_finding`` — the same
name as :meth:`EpistemicGraph.assert_finding`, a different one-shot finding
path. The LLM-facing tool is now ``record_claim``; ``assert_finding`` stays
one release as a deprecated alias that warns on use.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import mareforma
from tests._helpers import _bootstrap_key


def test_default_tool_is_record_claim_not_assert_finding(tmp_path: Path) -> None:
    key_path = _bootstrap_key(tmp_path)
    with mareforma.open(tmp_path, key_path=key_path) as graph:
        tools = graph.get_tools(generated_by="agent/a")
        names = [t.__name__ for t in tools]
        assert names == ["query_graph", "record_claim"]
        assert "assert_finding" not in names  # no longer shadows the method

        _, record_claim = tools
        cid = record_claim(text="a finding via the renamed tool")
        assert isinstance(cid, str) and cid
        # The graph method of the same old name is a distinct, live surface.
        assert callable(graph.assert_finding)


def test_deprecated_alias_warns_and_records(tmp_path: Path) -> None:
    key_path = _bootstrap_key(tmp_path)
    with mareforma.open(tmp_path, key_path=key_path) as graph:
        tools = graph.get_tools(
            generated_by="agent/a", include_deprecated_aliases=True
        )
        names = [t.__name__ for t in tools]
        assert names == ["query_graph", "record_claim", "assert_finding"]

        assert_finding = tools[-1]
        with pytest.warns(DeprecationWarning, match="renamed to 'record_claim'"):
            cid = assert_finding(text="a finding via the deprecated alias")
        assert isinstance(cid, str) and cid


def test_deprecated_alias_absent_by_default(tmp_path: Path) -> None:
    key_path = _bootstrap_key(tmp_path)
    with mareforma.open(tmp_path, key_path=key_path) as graph:
        assert len(graph.get_tools()) == 2
