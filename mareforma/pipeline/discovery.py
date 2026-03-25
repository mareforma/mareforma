"""
pipeline/discovery.py — Discover and import build_transform.py files.

Import side effects are the mechanism: when build_transform.py is imported,
its @transform decorations fire and register into the global TransformRegistry.

Discovery order
---------------
1. If mareforma.project.toml has [sources.<n>.entry_point], import that path.
2. Otherwise, auto-discover data/<n>/preprocessing/build_transform.py for
   every registered source.
3. Filter to sources matching the requested source filter (or all if None).

Error policy
------------
- File not found    → warning, skip (source may not have transforms yet)
- Import error      → error, re-raise with context (broken transform file)
- CyclicDependency  → error, re-raise (invalid DAG)
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from mareforma.transforms import registry as _registry, TransformRecord


class DiscoveryError(Exception):
    """Raised when a build_transform.py file cannot be imported."""


def discover(
    root: Path,
    registry_data: dict,
    source_filter: str | None = None,
) -> list[TransformRecord]:
    """Import build_transform files and return all registered TransformRecords.

    Parameters
    ----------
    root:
        Project root.
    registry_data:
        Parsed mareforma.project.toml.
    source_filter:
        If given, only import transforms whose name starts with this source.
        e.g. ``"morphology"`` → only ``"morphology.*"`` transforms.

    Returns
    -------
    list[TransformRecord]
        All records registered after discovery (may include records from
        previous imports if registry was not cleared — runner handles this).
    """
    sources = registry_data.get("sources", {})

    for source_name, source_cfg in sources.items():
        if source_filter and source_name != source_filter:
            continue

        # Check for explicit entry_point override in TOML
        entry_point = source_cfg.get("entry_point")
        if entry_point:
            module_path = Path(entry_point)
            if not module_path.is_absolute():
                module_path = root / module_path
        else:
            module_path = root / "data" / source_name / "preprocessing" / "build_transform.py"

        if not module_path.exists():
            # Not an error — source may have no transforms yet
            continue

        _import_module(module_path, source_name)

    records = _registry.all()

    # Filter to requested source if specified
    if source_filter:
        records = [r for r in records if r.name.startswith(f"{source_filter}.")]

    return records


def _import_module(path: Path, source_name: str) -> None:
    """Import a Python file as a module by path."""
    module_name = f"_mareforma_build_{source_name}"

    # If already imported in this Python session, reload to pick up changes.
    if module_name in sys.modules:
        try:
            importlib.reload(sys.modules[module_name])
        except Exception as exc:
            raise DiscoveryError(
                f"Error reloading '{path}': {exc}\n"
                f"Fix the syntax/import error in that file and try again."
            ) from exc
        return

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise DiscoveryError(f"Could not load module spec from '{path}'.")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module

    try:
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    except Exception as exc:
        del sys.modules[module_name]
        raise DiscoveryError(
            f"Error importing '{path}':\n  {type(exc).__name__}: {exc}\n"
            "Fix the error in your build_transform.py and try again."
        ) from exc