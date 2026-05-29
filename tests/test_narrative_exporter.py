"""Tests for the narrative Markdown exporter
(:mod:`mareforma.exporters.narrative`).

Conceptual clusters:

- :class:`TestStructure` — Markdown header + empty-db placeholder.
- :class:`TestContent` — DOIs, claim text, confidence-percentage
  rendering.
- :class:`TestContradictions` — structural / heuristic contradiction
  flagging surfaces in the output.
"""

from __future__ import annotations


class TestStructure:
    def test_narrative_is_markdown(self, populated_db):
        from mareforma.exporters.narrative import export_narrative
        md = export_narrative(populated_db)
        assert md.startswith("# Literature Summary")

    def test_narrative_empty_db(self, db):
        from mareforma.exporters.narrative import export_narrative
        md = export_narrative(db)
        assert "No claims ingested yet" in md


class TestContent:
    def test_lists_sources(self, populated_db):
        from mareforma.exporters.narrative import export_narrative
        md = export_narrative(populated_db)
        assert "10.1234/study-a-2024" in md
        assert "10.5678/study-b-2025" in md

    def test_contains_doi(self, populated_db):
        from mareforma.exporters.narrative import export_narrative
        md = export_narrative(populated_db)
        assert "10.1234/study-a-2024" in md

    def test_contains_claim_text(self, populated_db):
        from mareforma.exporters.narrative import export_narrative
        md = export_narrative(populated_db)
        assert "CRP" in md

    def test_renders_confidence_percentage(self, populated_db):
        from mareforma.exporters.narrative import export_narrative
        md = export_narrative(populated_db)
        assert "%" in md


class TestContradictions:
    def test_flags_contradictions_inline(self, populated_db):
        from mareforma.exporters.narrative import export_narrative
        md = export_narrative(populated_db)
        assert "contradicted" in md or "Contradictions detected" in md
