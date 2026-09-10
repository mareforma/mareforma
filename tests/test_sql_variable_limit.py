"""IN-list parameter counts must not grow with the graph.

SQLite caps the bound variables one statement may carry (999 on 3.30 and 3.31,
the oldest builds ``open_db`` accepts). The convergence candidate query, its
promotion UPDATE and ``find_dangling_supports`` each spliced one placeholder per
row, so a well-cited anchor or a large graph pushed them past the cap.
Convergence then failed silently and for good (the error is swallowed into a
retry flag, and the retry rebuilds the same statement), and the audit surface
crashed with a raw sqlite3 error. The lowered limit here stands in for the
row counts that reach the real cap.
"""
from __future__ import annotations

import sqlite3
import sys

import pytest

import mareforma
from tests._helpers import _bootstrap_key, _two_signers

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 11), reason="Connection.setlimit needs Python 3.11"
)

_LIMIT = 50
_PEERS = 60


def test_find_dangling_supports_past_the_sql_variable_limit(tmp_path):
    """The audit query must not scale its parameter count with the graph."""
    phantom = "12345678-1234-4234-8234-123456789012"
    with mareforma.open(tmp_path) as g:
        # The query binds one variable per distinct cited claim_id, so the
        # citations have to be distinct, not merely numerous.
        cited = [
            g.assert_claim(f"upstream {i}", generated_by="lab_a")
            for i in range(_PEERS)
        ]
        cid = g.assert_claim(
            "cites-phantom", supports=[*cited, phantom], generated_by="lab_x",
        )

        g._conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, _LIMIT)
        assert g.find_dangling_supports() == [
            {"claim_id": cid, "dangling_ref": phantom},
        ]


def test_find_dangling_supports_raises_database_error_on_sqlite_failure(tmp_path):
    """A SQLite failure on the audit surface surfaces as DatabaseError, not a
    raw sqlite3 error callers have no reason to catch."""
    with mareforma.open(tmp_path) as g:
        cid = g.assert_claim("claim", generated_by="lab_a")
        # Unparseable supports_json is only reachable by hand-editing the DB,
        # but json_each refuses it and the audit surface must not leak that.
        g._conn.execute(
            "UPDATE claims SET supports_json = ? WHERE claim_id = ?",
            ("not json", cid),
        )
        with pytest.raises(mareforma.DatabaseError):
            g.find_dangling_supports()
