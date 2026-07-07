"""#31: re-ingesting a paper must not orphan FTS rows.

INSERT OR REPLACE on ``literature_claims`` fired the external-content FTS
delete trigger only under ``recursive_triggers`` (off by default), so the
superseded rowid stayed in ``literature_claims_fts`` and search returned
stale, orphaned hits. The ingest path now deletes a document's prior
claims (firing the delete trigger) before inserting the fresh set.
"""

from __future__ import annotations

from pathlib import Path

from mareforma.db import open_db
from mareforma.ingest_command import ingest_file

_TEMPLATE = """TITLE: {title}
DOI: 10.1234/reingest-test
CLAIMS:
{claims}
"""


def _write(tmp_path: Path, title: str, claims: list[str]) -> Path:
    body = "\n".join(f"- {c}" for c in claims)
    f = tmp_path / "paper.txt"
    f.write_text(_TEMPLATE.format(title=title, claims=body), encoding="utf-8")
    return f


def _fts_rowids(conn) -> set[int]:
    return {r[0] for r in conn.execute("SELECT rowid FROM literature_claims_fts")}


def _table_rowids(conn) -> set[int]:
    return {r[0] for r in conn.execute("SELECT rowid FROM literature_claims")}


def test_reingest_leaves_no_orphaned_fts_rowids(tmp_path: Path) -> None:
    conn = open_db(tmp_path)
    try:
        _write(tmp_path, "paper", ["alpha unique original claim about widgets"])
        ingest_file(_write(tmp_path, "paper", ["alpha unique original claim about widgets"]), conn)

        # Re-ingest the SAME document (same DOI+title → same doc_id) with
        # different claim text.
        ingest_file(_write(tmp_path, "paper", ["beta unique revised claim about gadgets"]), conn)

        # Every FTS rowid must still resolve to a live literature_claims row.
        assert _fts_rowids(conn) == _table_rowids(conn)

        # Searching the retired text returns nothing; the current text hits.
        stale = conn.execute(
            "SELECT count(*) FROM literature_claims_fts WHERE claim_text MATCH 'widgets'"
        ).fetchone()[0]
        assert stale == 0, "orphaned FTS row still matches the retired text"
        fresh = conn.execute(
            "SELECT count(*) FROM literature_claims_fts WHERE claim_text MATCH 'gadgets'"
        ).fetchone()[0]
        assert fresh == 1
    finally:
        conn.close()


def test_reingest_with_fewer_claims_drops_the_tail(tmp_path: Path) -> None:
    conn = open_db(tmp_path)
    try:
        ingest_file(_write(tmp_path, "p", ["one claim aaa", "two claim bbb", "three claim ccc"]), conn)
        ingest_file(_write(tmp_path, "p", ["one claim aaa"]), conn)
        # The dropped claims leave no rows and no orphaned FTS entries.
        assert _table_rowids(conn) == _fts_rowids(conn)
        assert conn.execute(
            "SELECT count(*) FROM literature_claims"
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_reingest_with_zero_claims_purges_prior_rows(tmp_path: Path) -> None:
    # The empty-re-extraction case: re-ingesting the same paper with NO claims
    # must STILL purge the prior version's rows and their FTS entries. Deriving
    # the purge set from the freshly produced claims skipped the delete when the
    # set was empty, leaving orphaned stale hits (the #31 bug's residual arm).
    conn = open_db(tmp_path)
    try:
        ingest_file(_write(tmp_path, "p", ["alpha claim aaa", "beta claim bbb"]), conn)
        assert conn.execute(
            "SELECT count(*) FROM literature_claims"
        ).fetchone()[0] == 2
        # Re-ingest the same document (same DOI+title) with zero claims.
        ingest_file(_write(tmp_path, "p", []), conn)
        assert conn.execute(
            "SELECT count(*) FROM literature_claims"
        ).fetchone()[0] == 0
        # No orphaned FTS rowids, and a search for the old text finds nothing.
        assert _table_rowids(conn) == _fts_rowids(conn)
        hits = conn.execute(
            "SELECT count(*) FROM literature_claims_fts "
            "WHERE literature_claims_fts MATCH ?",
            ("aaa",),
        ).fetchone()[0]
        assert hits == 0
    finally:
        conn.close()
