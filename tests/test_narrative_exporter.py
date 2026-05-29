"""
test_narrative_exporter.py — tests for the narrative Markdown exporter.
"""


def test_narrative_is_markdown(populated_db):
    from mareforma.exporters.narrative import export_narrative
    md = export_narrative(populated_db)
    assert md.startswith("# Literature Summary")


def test_narrative_empty_db(db):
    from mareforma.exporters.narrative import export_narrative
    md = export_narrative(db)
    assert "No claims ingested yet" in md


def test_narrative_lists_sources(populated_db):
    from mareforma.exporters.narrative import export_narrative
    md = export_narrative(populated_db)
    assert "10.1234/study-a-2024" in md
    assert "10.5678/study-b-2025" in md


def test_narrative_contains_doi(populated_db):
    from mareforma.exporters.narrative import export_narrative
    md = export_narrative(populated_db)
    assert "10.1234/study-a-2024" in md


def test_narrative_flags_contradictions(populated_db):
    from mareforma.exporters.narrative import export_narrative
    md = export_narrative(populated_db)
    assert "contradicted" in md or "Contradictions detected" in md


def test_narrative_confidence_percentage(populated_db):
    from mareforma.exporters.narrative import export_narrative
    md = export_narrative(populated_db)
    assert "%" in md


def test_narrative_contains_claim_text(populated_db):
    from mareforma.exporters.narrative import export_narrative
    md = export_narrative(populated_db)
    assert "CRP" in md
