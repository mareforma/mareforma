"""
transforms.py — @transform decorator and global TransformRegistry.

Usage
-----
    from mareforma import transform
    from mareforma.pipeline.context import BuildContext

    @transform("morphology.register", depends_on=["morphology.load"])
    def register(ctx: BuildContext) -> None:
        df = ctx.load("morphology.load")
        # ... process ...
        ctx.save("registered", df)

DAG edges
---------
depends_on is a list of transform name strings. mareforma build resolves
execution order and only re-runs stale nodes. When depends_on is omitted
the transform is a DAG root (no prerequisites).

Registry
--------
Every @transform-decorated function is registered in the module-level
TransformRegistry singleton at decoration time. mareforma build discovers
transforms by importing build_transform.py files, which triggers decoration,
which populates the registry.

Logging
-------
Each call (standalone or via pipeline) appends one JSON record to
.mareforma/commits/transforms.jsonl. Silently skipped outside a project.
"""

from __future__ import annotations

import dataclasses
import functools
import inspect
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

_COMMITS_DIR = Path(".mareforma") / "commits"
_TRANSFORMS_LOG = _COMMITS_DIR / "transforms.jsonl"


# ---------------------------------------------------------------------------
# TransformRecord — metadata for one registered transform
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class TransformRecord:
    """All metadata mareforma needs to reason about one transform."""
    name: str
    fn: Callable[..., Any]
    depends_on: list[str]
    source_file: str          # absolute path to the file where fn is defined
    source_code: str          # fn source code — used for staleness hashing

    def __repr__(self) -> str:
        deps = ", ".join(self.depends_on) or "—"
        return f"<TransformRecord name={self.name!r} depends_on=[{deps}]>"


# ---------------------------------------------------------------------------
# TransformRegistry — module-level singleton
# ---------------------------------------------------------------------------

class TransformRegistry:
    """Singleton registry populated by @transform decorations at import time.

    mareforma.build() reads this registry after importing all
    build_transform.py files to construct the DAG.
    """

    def __init__(self) -> None:
        self._transforms: dict[str, TransformRecord] = {}

    def register(self, record: TransformRecord) -> None:
        self._transforms[record.name] = record

    def get(self, name: str) -> TransformRecord | None:
        return self._transforms.get(name)

    def all(self) -> list[TransformRecord]:
        return list(self._transforms.values())

    def names(self) -> list[str]:
        return list(self._transforms.keys())

    def clear(self) -> None:
        """Clear all registrations. Used between test runs."""
        self._transforms.clear()


# Module-level singleton — imported by pipeline modules
registry = TransformRegistry()


# ---------------------------------------------------------------------------
# @transform decorator
# ---------------------------------------------------------------------------

def transform(
    name: str,
    depends_on: list[str] | None = None,
) -> Callable[[F], F]:
    """Decorator factory. Registers *fn* in the global registry and wraps it
    to log every call.

    Parameters
    ----------
    name:
        Dotted identifier, e.g. ``"morphology.register"``.
        Convention: ``"<source_name>.<step_name>"``.
    depends_on:
        List of transform name strings that must complete before this one.
        Used by ``mareforma build`` to resolve execution order.
        Omit for DAG root transforms (no prerequisites).
    """
    if not name or not name.strip():
        raise ValueError("transform() requires a non-empty name string.")

    _depends_on: list[str] = depends_on or []

    def decorator(fn: F) -> F:
        # Capture source code at decoration time for staleness hashing.
        try:
            src = inspect.getsource(fn)
        except OSError:
            src = ""

        # Register in the global registry.
        record = TransformRecord(
            name=name,
            fn=fn,
            depends_on=_depends_on,
            source_file=inspect.getfile(fn),
            source_code=src,
        )
        registry.register(record)

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.monotonic()
            entry: dict[str, Any] = {
                "name": name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": None,
                "duration_ms": None,
                "error": None,
            }

            try:
                result = fn(*args, **kwargs)
                entry["status"] = "success"
                return result

            except Exception as exc:
                entry["status"] = "failed"
                entry["error"] = f"{type(exc).__name__}: {exc}"
                raise  # always re-raise

            finally:
                entry["duration_ms"] = round((time.monotonic() - start) * 1000)
                _append_log(entry)

        # Attach metadata to the wrapper for introspection.
        wrapper._mare_record = record  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    return decorator


# ---------------------------------------------------------------------------
# JSONL logging
# ---------------------------------------------------------------------------

def _append_log(entry: dict[str, Any]) -> None:
    """Append *entry* as a JSON line to transforms.jsonl.

    Silently skips if the commits directory does not exist (not in a project).
    Silently skips on any I/O error to avoid masking the original exception.
    """
    if not _COMMITS_DIR.exists():
        return
    try:
        with _TRANSFORMS_LOG.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
    except OSError:
        pass