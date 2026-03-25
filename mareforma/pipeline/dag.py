"""
pipeline/dag.py — DAG builder and resolver.

Takes a list of TransformRecords from the registry and produces a
topologically sorted execution order, detecting cycles and missing
dependency references.

Algorithm: Kahn's algorithm (BFS-based topological sort).
  - O(V + E) where V = transforms, E = dependency edges
  - Detects cycles by checking for nodes remaining after sort
  - Preserves deterministic order for nodes with equal priority
    (sorted by name) so builds are reproducible

ASCII diagram:

  TransformRecord list
        │
        ▼
  _build_graph()           adjacency list + in-degree map
        │
        ▼
  _kahn_sort()             BFS topological sort
        │                  ├── cycle? → CyclicDependencyError (names cycle path)
        │                  └── missing dep? → MissingDependencyError
        ▼
  list[TransformRecord]    execution order (roots first)
"""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mareforma.transforms import TransformRecord


class CyclicDependencyError(Exception):
    """Raised when the transform DAG contains a cycle."""


class MissingDependencyError(Exception):
    """Raised when a depends_on reference names an unregistered transform."""


def resolve(records: list["TransformRecord"]) -> list["TransformRecord"]:
    """Return *records* in topological execution order.

    Raises
    ------
    MissingDependencyError
        If any depends_on string references a name not in *records*.
    CyclicDependencyError
        If the dependency graph contains a cycle.
    """
    if not records:
        return []

    name_to_record = {r.name: r for r in records}

    # Validate all depends_on references exist
    for record in records:
        for dep in record.depends_on:
            if dep not in name_to_record:
                raise MissingDependencyError(
                    f"Transform '{record.name}' depends on '{dep}', "
                    f"but '{dep}' is not registered.\n"
                    f"Registered transforms: {sorted(name_to_record.keys())}"
                )

    return _kahn_sort(records, name_to_record)


def _kahn_sort(
    records: list["TransformRecord"],
    name_to_record: dict[str, "TransformRecord"],
) -> list["TransformRecord"]:
    """Kahn's algorithm. Returns sorted list or raises CyclicDependencyError."""

    # Build: dependents[A] = {B, C} means A must run before B and C
    # Build: in_degree[X] = number of transforms X depends on
    dependents: dict[str, list[str]] = {r.name: [] for r in records}
    in_degree: dict[str, int] = {r.name: 0 for r in records}

    for record in records:
        for dep in record.depends_on:
            dependents[dep].append(record.name)
            in_degree[record.name] += 1

    # Seed queue with all roots (no dependencies), sorted for determinism
    queue: deque[str] = deque(
        sorted(name for name, deg in in_degree.items() if deg == 0)
    )

    order: list["TransformRecord"] = []

    while queue:
        name = queue.popleft()
        order.append(name_to_record[name])

        # Reduce in-degree for all nodes that depend on this one
        for dependent in sorted(dependents[name]):  # sorted for determinism
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(order) != len(records):
        # Some nodes remain — there's a cycle. Find it for a useful error.
        cycle_names = sorted(
            name for name, deg in in_degree.items() if deg > 0
        )
        cycle_path = _find_cycle(cycle_names, name_to_record)
        raise CyclicDependencyError(
            f"Circular dependency detected among transforms: {cycle_path}\n"
            "Check the depends_on declarations for these transforms."
        )

    return order


def _find_cycle(
    candidates: list[str],
    name_to_record: dict[str, "TransformRecord"],
) -> str:
    """Return a human-readable cycle path string, e.g. 'A → B → C → A'."""
    # Simple DFS to find one cycle path among candidates
    visited: set[str] = set()
    path: list[str] = []

    def dfs(name: str) -> bool:
        if name in path:
            # Found the cycle — extract it
            idx = path.index(name)
            cycle = path[idx:] + [name]
            path[:] = cycle
            return True
        if name in visited:
            return False
        visited.add(name)
        path.append(name)
        record = name_to_record.get(name)
        if record:
            for dep in record.depends_on:
                if dep in name_to_record and dfs(dep):
                    return True
        path.pop()
        return False

    for start in candidates:
        path.clear()
        visited.clear()
        if dfs(start):
            return " → ".join(path)

    return " → ".join(candidates)  # fallback: just list them