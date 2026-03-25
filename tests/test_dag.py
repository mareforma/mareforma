"""
tests/test_dag.py — unit tests for pipeline/dag.py.

Covers:
  - topological ordering for linear, branching, and diamond graphs
  - deterministic output for equal-priority nodes
  - cycle detection with named error message
  - missing dependency detection
  - empty input
  - single node
"""

from __future__ import annotations

import pytest

from mareforma.pipeline.dag import (
    CyclicDependencyError,
    MissingDependencyError,
    resolve,
)


class TestResolveOrdering:
    def test_empty_returns_empty(self, make_record) -> None:
        assert resolve([]) == []

    def test_single_node_no_deps(self, make_record) -> None:
        a = make_record("a.load")
        result = resolve([a])
        assert [r.name for r in result] == ["a.load"]

    def test_linear_chain(self, make_record) -> None:
        # a → b → c: a must run first
        a = make_record("a.load")
        b = make_record("a.register", depends_on=["a.load"])
        c = make_record("a.features", depends_on=["a.register"])

        result = resolve([c, b, a])  # intentionally shuffled input
        names = [r.name for r in result]
        assert names.index("a.load") < names.index("a.register")
        assert names.index("a.register") < names.index("a.features")

    def test_two_independent_roots(self, make_record) -> None:
        a = make_record("src1.load")
        b = make_record("src2.load")
        result = resolve([b, a])
        # Both roots, order determined alphabetically for determinism
        assert sorted([r.name for r in result]) == ["src1.load", "src2.load"]

    def test_diamond_dependency(self, make_record) -> None:
        #       root
        #      /    \
        #    left  right
        #      \    /
        #      merge
        root  = make_record("d.root")
        left  = make_record("d.left",  depends_on=["d.root"])
        right = make_record("d.right", depends_on=["d.root"])
        merge = make_record("d.merge", depends_on=["d.left", "d.right"])

        result = resolve([merge, right, left, root])
        names = [r.name for r in result]
        assert names.index("d.root")  < names.index("d.left")
        assert names.index("d.root")  < names.index("d.right")
        assert names.index("d.left")  < names.index("d.merge")
        assert names.index("d.right") < names.index("d.merge")

    def test_deterministic_for_equal_priority(self, make_record) -> None:
        """Two independent roots must always come out in the same order."""
        nodes = [
            make_record("z.step"),
            make_record("a.step"),
            make_record("m.step"),
        ]
        r1 = [r.name for r in resolve(nodes)]
        r2 = [r.name for r in resolve(list(reversed(nodes)))]
        assert r1 == r2  # same order regardless of input order
        assert r1 == sorted(r1)  # alphabetical

    def test_fan_out(self, make_record) -> None:
        # One root, three dependents
        root = make_record("src.load")
        a    = make_record("src.proc_a", depends_on=["src.load"])
        b    = make_record("src.proc_b", depends_on=["src.load"])
        c    = make_record("src.proc_c", depends_on=["src.load"])

        result = resolve([c, b, a, root])
        names = [r.name for r in result]
        for dep in ["src.proc_a", "src.proc_b", "src.proc_c"]:
            assert names.index("src.load") < names.index(dep)

    def test_multi_source_with_merge(self, make_record) -> None:
        # Two independent source chains merged at the end
        a_load   = make_record("a.load")
        b_load   = make_record("b.load")
        a_proc   = make_record("a.proc", depends_on=["a.load"])
        b_proc   = make_record("b.proc", depends_on=["b.load"])
        combined = make_record("combined.merge", depends_on=["a.proc", "b.proc"])

        result = resolve([combined, b_proc, a_proc, b_load, a_load])
        names = [r.name for r in result]
        assert names.index("a.load")  < names.index("a.proc")
        assert names.index("b.load")  < names.index("b.proc")
        assert names.index("a.proc")  < names.index("combined.merge")
        assert names.index("b.proc")  < names.index("combined.merge")


class TestCycleDetection:
    def test_self_loop(self, make_record) -> None:
        a = make_record("cyc.self", depends_on=["cyc.self"])
        with pytest.raises(CyclicDependencyError):
            resolve([a])

    def test_two_node_cycle_names_participant(self, make_record) -> None:
        a = make_record("cyc.a", depends_on=["cyc.b"])
        b = make_record("cyc.b", depends_on=["cyc.a"])
        with pytest.raises(CyclicDependencyError, match="[Cc]ircular") as exc_info:
            resolve([a, b])
        msg = str(exc_info.value)
        assert "cyc.a" in msg or "cyc.b" in msg


class TestMissingDependency:
    def test_missing_dep_raises(self, make_record) -> None:
        a = make_record("miss.step", depends_on=["miss.nonexistent"])
        with pytest.raises(MissingDependencyError) as exc_info:
            resolve([a])
        msg = str(exc_info.value)
        assert "miss.nonexistent" in msg
        assert "miss.step" in msg

    def test_missing_dep_lists_registered(self, make_record) -> None:
        a = make_record("reg.real")
        b = make_record("reg.broken", depends_on=["reg.ghost"])
        with pytest.raises(MissingDependencyError) as exc_info:
            resolve([a, b])
        assert "reg.real" in str(exc_info.value)

    def test_all_deps_present_no_error(self, make_record) -> None:
        a = make_record("ok.load")
        b = make_record("ok.proc", depends_on=["ok.load"])
        result = resolve([a, b])  # must not raise
        assert len(result) == 2