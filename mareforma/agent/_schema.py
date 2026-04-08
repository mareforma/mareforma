"""
agent/_schema.py — DDL for the agent_events table.

Created lazily by MareformaObserver on first use via CREATE TABLE IF NOT EXISTS.
This table is outside the versioned schema (user_version=1 in db.py) — it is
created per-project on demand without requiring re-initialisation.

ERD addition:
  transform_runs ──< agent_events   (run_id, soft FK — not enforced)
"""

AGENT_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS agent_events (
    event_id      TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    name          TEXT NOT NULL,
    timestamp     TEXT NOT NULL,
    status        TEXT NOT NULL,
    duration_ms   INTEGER,
    input_hash    TEXT,
    output_hash   TEXT,
    metadata_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_agent_events_run_id
    ON agent_events(run_id);
"""
