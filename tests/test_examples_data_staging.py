"""Example 05's data stage, pinned so a partial download is never called done.

huggingface_hub creates each file's parent directory before it writes any
bytes, so the top-level directory names exist minutes into a multi-hour
transfer. Completeness read off those names would report a truncated dataset
as finished and never resume it.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

from tests._helpers import _requires_repo_checkout

_EXAMPLE = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "05_drug_target_provenance"
    / "05_drug_target_provenance.py"
)

# examples/ is not in the sdist, so the shipped suite skips this module.
pytestmark = _requires_repo_checkout


def _load() -> ModuleType:
    """The example imported as a module, its name being no identifier."""
    spec = importlib.util.spec_from_file_location("example_05", _EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        del sys.modules[spec.name]
    return module


def test_stage_data_downloads_over_empty_expected_directories(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """Directories with no files in them are a started download, not a done one."""
    module = _load()
    data_dir = tmp_path / "raw"
    for name in ("compass", "depmap_24q2", "pinnacle_embeds", "transcriptformer_embedding"):
        (data_dir / name).mkdir(parents=True)
    hf = tmp_path / "hf"
    hf.touch()

    commands = []
    monkeypatch.setattr(module, "DATA_DIR", data_dir)
    monkeypatch.setattr(module, "VENV_HF", hf)
    monkeypatch.setattr(module, "run", lambda cmd, **kwargs: commands.append(cmd))

    module.stage_data()
    capsys.readouterr()

    assert commands, "stage_data returned without invoking hf download"
    assert commands[0][:2] == [str(hf), "download"]


def test_run_stage_does_not_resolve_the_installer(monkeypatch, capsys) -> None:
    """``--run`` repeats the experiment against a venv that already exists.

    uv builds that venv and does nothing else, so a box where uv is gone (or
    lives somewhere ``find_uv`` does not probe) must still reach stage 3.
    """
    module = _load()

    def _no_uv():
        raise AssertionError("--run resolved uv, which only the install stage uses")

    stages = []
    monkeypatch.setattr(sys, "argv", [str(_EXAMPLE), "--run"])
    monkeypatch.setattr(module, "find_uv", _no_uv)
    monkeypatch.setattr(module, "stage_run", lambda: stages.append("run"))

    module.main()
    capsys.readouterr()

    assert stages == ["run"]
