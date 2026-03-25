"""
tests/test_registry.py — unit tests for registry read/write operations.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mareforma.registry import (
    MareformaError,
    ProjectNotFoundError,
    SourceAlreadyExistsError,
    SourceNotFoundError,
    TOMLParseError,
    add_source,
    get_project,
    get_source,
    list_sources,
    load,
    validate,
)
from mareforma.initializer import initialize


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    """Return an initialised project root."""
    initialize(tmp_path)
    return tmp_path


class TestLoad:
    def test_loads_valid_toml(self, project: Path) -> None:
        data = load(project)
        assert "project" in data

    def test_raises_project_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(ProjectNotFoundError):
            load(tmp_path)

    def test_raises_toml_parse_error(self, project: Path) -> None:
        toml = project / "mareforma.project.toml"
        toml.write_text("[[invalid\n")
        with pytest.raises(TOMLParseError):
            load(project)


class TestAddSource:
    def test_adds_source_entry(self, project: Path) -> None:
        add_source(project, "morphology", "data/morphology/raw/", "Skeletons")
        sources = list_sources(project)
        assert "morphology" in sources

    def test_duplicate_raises_without_force(self, project: Path) -> None:
        add_source(project, "morphology", "data/morphology/raw/")
        with pytest.raises(SourceAlreadyExistsError):
            add_source(project, "morphology", "data/morphology/raw/")

    def test_force_overwrites(self, project: Path) -> None:
        add_source(project, "morphology", "data/morphology/raw/", "old")
        add_source(project, "morphology", "data/morphology/raw/", "new", force=True)
        src = get_source(project, "morphology")
        assert src["description"] == "new"

    def test_default_fields_present(self, project: Path) -> None:
        add_source(project, "ephys", "data/ephys/raw/")
        src = get_source(project, "ephys")
        assert "status" in src
        assert src["status"] == "raw"
        assert "limitations" not in src
        assert "design_decisions" not in src

    def test_added_by_populated_from_git_config(self, project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "mareforma.git.get_user_config",
            lambda: {"name": "Test User", "email": "test@example.com"},
        )
        add_source(project, "morphology", "data/morphology/raw/")
        src = get_source(project, "morphology")
        assert src["added_by"] == "Test User <test@example.com>"

    def test_added_by_empty_when_git_unavailable(self, project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "mareforma.git.get_user_config",
            lambda: {"name": "", "email": ""},
        )
        add_source(project, "ephys", "data/ephys/raw/")
        src = get_source(project, "ephys")
        assert src["added_by"] == ""

    def test_multiple_sources(self, project: Path) -> None:
        add_source(project, "a", "data/a/raw/")
        add_source(project, "b", "data/b/raw/")
        add_source(project, "c", "data/c/raw/")
        assert set(list_sources(project)) == {"a", "b", "c"}


class TestGetSource:
    def test_returns_source(self, project: Path) -> None:
        add_source(project, "morphology", "data/morphology/raw/", "Skeletons")
        src = get_source(project, "morphology")
        assert src["description"] == "Skeletons"

    def test_raises_not_found(self, project: Path) -> None:
        with pytest.raises(SourceNotFoundError, match="not found"):
            get_source(project, "nonexistent")

    def test_error_lists_registered_sources(self, project: Path) -> None:
        add_source(project, "morphology", "data/morphology/raw/")
        with pytest.raises(SourceNotFoundError, match="morphology"):
            get_source(project, "nonexistent")


class TestValidate:
    def test_empty_project_has_warnings(self, project: Path) -> None:
        issues = validate(project)
        assert len(issues) > 0

    def test_missing_path_is_warned(self, project: Path) -> None:
        add_source(project, "ghost", "/nonexistent/path")
        issues = validate(project)
        sources_warned = [i["source"] for i in issues if "does not exist" in i["message"]]
        assert "ghost" in sources_warned

    def test_empty_description_is_warned(self, project: Path) -> None:
        add_source(project, "morphology", "data/morphology/raw/", "")
        issues = validate(project)
        warned = [i for i in issues if i["source"] == "morphology" and "description" in i["message"]]
        assert warned

    def test_clean_source_no_issues(self, project: Path, tmp_path: Path) -> None:
        raw = tmp_path / "data" / "clean" / "raw"
        raw.mkdir(parents=True)
        add_source(project, "clean", str(raw), "A described source")

        # Also fill project description and format
        toml_path = project / "mareforma.project.toml"
        text = toml_path.read_text()
        text = text.replace('description = ""', 'description = "My project"', 1)
        text = text.replace('format = ""', 'format = "CSV"')
        toml_path.write_text(text)

        issues = validate(project)
        assert issues == []
