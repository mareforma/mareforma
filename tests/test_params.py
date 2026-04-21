"""
tests/test_params.py — tests for ctx.params (BuildContext.params).

Verifies that [params] from mareforma.project.toml is readable
inside a transform via ctx.params.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mareforma.initializer import initialize
from mareforma.pipeline.runner import TransformRunner
from mareforma.registry import load as load_toml, save as save_toml
from mareforma.transforms import TransformRecord


def _runner(root: Path) -> TransformRunner:
    return TransformRunner(root=root, registry_data=load_toml(root))


def _make_record(name: str, fn) -> TransformRecord:
    return TransformRecord(
        name=name, fn=fn, depends_on=[], source_file="<test>", source_code="def f(): pass"
    )


@pytest.fixture()
def proj(tmp_path: Path) -> Path:
    initialize(tmp_path)
    from mareforma.registry import add_source
    raw = tmp_path / "data" / "demo" / "raw"
    raw.mkdir(parents=True)
    add_source(tmp_path, "demo", str(raw), "test source")
    return tmp_path


class TestCtxParams:
    def test_params_empty_when_no_section(self, proj: Path) -> None:
        captured = {}

        def fn(ctx):
            captured["params"] = ctx.params

        _runner(proj).run([_make_record("demo.check", fn)])
        assert captured["params"] == {}

    def test_params_reads_toml_section(self, proj: Path) -> None:
        data = load_toml(proj)
        data["params"] = {"hypothesis": "PV+ cells have higher out-degree", "domain": "neuroscience"}
        save_toml(proj, data)

        captured = {}

        def fn(ctx):
            captured["params"] = ctx.params

        _runner(proj).run([_make_record("demo.check", fn)])
        assert captured["params"]["hypothesis"] == "PV+ cells have higher out-degree"
        assert captured["params"]["domain"] == "neuroscience"

    def test_params_returns_copy(self, proj: Path) -> None:
        data = load_toml(proj)
        data["params"] = {"key": "value"}
        save_toml(proj, data)

        captured = {}

        def fn(ctx):
            p = ctx.params
            p["injected"] = True
            captured["second_call"] = ctx.params

        _runner(proj).run([_make_record("demo.check", fn)])
        assert "injected" not in captured["second_call"]
