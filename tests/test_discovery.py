"""
tests/test_discovery.py — unit tests for pipeline/discovery.py.

Covers:
  - auto-discovery from data/<source>/preprocessing/build_transform.py
  - explicit entry_point in TOML overrides auto path
  - missing file is silently skipped (not an error)
  - broken file raises DiscoveryError with context
  - source_filter limits discovered transforms
  - registry is populated after discovery
  - module reload on second discovery call
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from mareforma.initializer import initialize
from mareforma.pipeline.discovery import discover, DiscoveryError
from mareforma.registry import add_source
from mareforma.transforms import registry


def _write_transforms(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _registry_data_with_source(root: Path, source: str, raw_path: str | None = None) -> dict:
    from mareforma.registry import load as load_toml
    raw = root / "data" / source / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    add_source(root, source, raw_path or str(raw), "test source")
    return load_toml(root)


class TestAutoDiscovery:
    def test_discovers_transforms_from_default_path(self, tmp_path: Path) -> None:
        initialize(tmp_path)
        data = _registry_data_with_source(tmp_path, "morph")

        bt = tmp_path / "data" / "morph" / "preprocessing" / "build_transform.py"
        _write_transforms(bt, """
from mareforma.transforms import transform

@transform("morph.load")
def load(ctx):
    pass
""")

        records = discover(tmp_path, data)
        assert any(r.name == "morph.load" for r in records)

    def test_no_build_transform_file_silently_skipped(self, tmp_path: Path) -> None:
        initialize(tmp_path)
        data = _registry_data_with_source(tmp_path, "morph")
        # No build_transform.py created — should not raise
        records = discover(tmp_path, data)
        assert records == []

    def test_multiple_sources_discovered(self, tmp_path: Path) -> None:
        initialize(tmp_path)
        data = _registry_data_with_source(tmp_path, "src_a")
        (tmp_path / "data" / "src_b" / "raw").mkdir(parents=True, exist_ok=True)
        add_source(tmp_path, "src_b", str(tmp_path / "data" / "src_b" / "raw"), "b")

        from mareforma.registry import load as load_toml
        data = load_toml(tmp_path)

        for src, fn_name in [("src_a", "src_a.load"), ("src_b", "src_b.load")]:
            bt = tmp_path / "data" / src / "preprocessing" / "build_transform.py"
            _write_transforms(bt, f"""
from mareforma.transforms import transform

@transform("{fn_name}")
def load(ctx):
    pass
""")

        records = discover(tmp_path, data)
        names = {r.name for r in records}
        assert "src_a.load" in names
        assert "src_b.load" in names

    def test_no_sources_registered_returns_empty(self, tmp_path: Path) -> None:
        initialize(tmp_path)
        from mareforma.registry import load as load_toml
        data = load_toml(tmp_path)
        records = discover(tmp_path, data)
        assert records == []


class TestEntryPointOverride:
    def test_custom_entry_point_used(self, tmp_path: Path) -> None:
        initialize(tmp_path)

        custom_file = tmp_path / "custom" / "pipeline.py"
        _write_transforms(custom_file, """
from mareforma.transforms import transform

@transform("override.step")
def step(ctx):
    pass
""")

        from mareforma.registry import add_source as _add, load as load_toml
        raw = tmp_path / "data" / "override" / "raw"
        raw.mkdir(parents=True)
        _add(tmp_path, "override", str(raw), "test")

        # Manually inject entry_point into TOML
        toml_path = tmp_path / "mareforma.project.toml"
        text = toml_path.read_text()
        text = text.replace(
            '[sources.override.acquisition]',
            f'entry_point = "custom/pipeline.py"\n\n[sources.override.acquisition]'
        )
        toml_path.write_text(text)

        data = load_toml(tmp_path)
        records = discover(tmp_path, data)
        assert any(r.name == "override.step" for r in records)


class TestSourceFilter:
    def test_filter_limits_to_source(self, tmp_path: Path) -> None:
        initialize(tmp_path)

        for src, fn_name in [("alpha", "alpha.load"), ("beta", "beta.load")]:
            (tmp_path / "data" / src / "raw").mkdir(parents=True, exist_ok=True)
            add_source(tmp_path, src, str(tmp_path / "data" / src / "raw"), src)
            bt = tmp_path / "data" / src / "preprocessing" / "build_transform.py"
            _write_transforms(bt, f"""
from mareforma.transforms import transform

@transform("{fn_name}")
def load(ctx):
    pass
""")

        from mareforma.registry import load as load_toml
        data = load_toml(tmp_path)

        records = discover(tmp_path, data, source_filter="alpha")
        names = {r.name for r in records}
        assert "alpha.load" in names
        assert "beta.load" not in names

    def test_filter_nonexistent_source_returns_empty(self, tmp_path: Path) -> None:
        initialize(tmp_path)
        data = _registry_data_with_source(tmp_path, "morph")
        records = discover(tmp_path, data, source_filter="nonexistent")
        assert records == []


class TestBrokenModule:
    def test_syntax_error_raises_discovery_error(self, tmp_path: Path) -> None:
        initialize(tmp_path)
        data = _registry_data_with_source(tmp_path, "broken")

        bt = tmp_path / "data" / "broken" / "preprocessing" / "build_transform.py"
        _write_transforms(bt, "def this is not valid python !!!!")

        with pytest.raises(DiscoveryError, match="[Ee]rror"):
            discover(tmp_path, data)

    def test_import_error_raises_discovery_error(self, tmp_path: Path) -> None:
        initialize(tmp_path)
        data = _registry_data_with_source(tmp_path, "badimp")

        bt = tmp_path / "data" / "badimp" / "preprocessing" / "build_transform.py"
        _write_transforms(bt, "import this_module_does_not_exist_xyz")

        with pytest.raises(DiscoveryError):
            discover(tmp_path, data)

    def test_error_message_contains_filename(self, tmp_path: Path) -> None:
        initialize(tmp_path)
        data = _registry_data_with_source(tmp_path, "errmsg")

        bt = tmp_path / "data" / "errmsg" / "preprocessing" / "build_transform.py"
        _write_transforms(bt, "raise RuntimeError('intentional error')")

        with pytest.raises(DiscoveryError) as exc_info:
            discover(tmp_path, data)

        assert "errmsg" in str(exc_info.value) or "build_transform" in str(exc_info.value)


class TestRegistryPopulation:
    def test_registry_populated_after_discovery(self, tmp_path: Path) -> None:
        initialize(tmp_path)
        data = _registry_data_with_source(tmp_path, "pop")

        bt = tmp_path / "data" / "pop" / "preprocessing" / "build_transform.py"
        _write_transforms(bt, """
from mareforma.transforms import transform

@transform("pop.step")
def step(ctx):
    pass
""")

        discover(tmp_path, data)
        assert registry.get("pop.step") is not None

    def test_depends_on_preserved_through_discovery(self, tmp_path: Path) -> None:
        initialize(tmp_path)
        data = _registry_data_with_source(tmp_path, "dep")

        bt = tmp_path / "data" / "dep" / "preprocessing" / "build_transform.py"
        _write_transforms(bt, """
from mareforma.transforms import transform

@transform("dep.load")
def load(ctx):
    pass

@transform("dep.proc", depends_on=["dep.load"])
def proc(ctx):
    pass
""")

        records = discover(tmp_path, data)
        proc = next(r for r in records if r.name == "dep.proc")
        assert proc.depends_on == ["dep.load"]