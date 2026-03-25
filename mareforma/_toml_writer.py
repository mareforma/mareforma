"""
_toml_writer.py — Minimal TOML serialiser for the subset mareforma uses.

Supports: str, int, float, bool, list of str/int/float, nested dicts.
This covers everything needed for mareforma.project.toml without requiring
the external tomli-w package (though tomli-w is preferred when available).

Not a general-purpose TOML writer. Do not use outside mareforma.
"""

from __future__ import annotations

from typing import Any


def dumps(data: dict[str, Any]) -> str:
    """Serialize *data* to a TOML string."""
    lines: list[str] = []
    _write_table(lines, data, prefix="")
    return "\n".join(lines) + "\n"


def _write_table(lines: list[str], table: dict[str, Any], prefix: str) -> None:
    # Write scalar/list values first, then nested tables.
    deferred: list[tuple[str, dict]] = []

    for key, value in table.items():
        full_key = f"{prefix}.{key}" if prefix else key

        if isinstance(value, dict):
            deferred.append((full_key, value))
        elif isinstance(value, list):
            items = ", ".join(_scalar(v) for v in value)
            lines.append(f"{key} = [{items}]")
        else:
            lines.append(f"{key} = {_scalar(value)}")

    for full_key, sub in deferred:
        lines.append("")
        lines.append(f"[{full_key}]")
        _write_table(lines, sub, prefix=full_key)


def _scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        return f'"{escaped}"'
    raise TypeError(f"Unsupported TOML value type: {type(value)}")
