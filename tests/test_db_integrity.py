"""Integrity regressions for the db core: open paths, write invariants, threads.

Groups the low-level db/core.py fixes that guard the graph against silent
breakage: a literal-path open that cannot write, a corrupt-file open that leaks
a raw sqlite3 error, cross-thread transaction merges, and the update_claim write
path drifting from add_claim's invariants.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from mareforma.db import (
    DatabaseError,
    add_claim,
    get_claim,
    open_db,
    open_db_from_db_path,
)


def test_literal_path_open_can_write_a_claim(tmp_path: Path) -> None:
    """A db opened at a non-conventional literal path must accept writes.

    open_db_from_db_path's literal-path branch skipped the supports-cache
    attach, so add_claim's unconditional supports-edge maintenance hit
    'no such table: supports_cache.cache_meta' on every write, including a
    claim with no supports.
    """
    db_file = tmp_path / "custom.db"
    conn = open_db_from_db_path(db_file)
    try:
        # A claim with no supports still touches the attached cache counter.
        cid = add_claim(conn, db_file.parent, "a claim with no supports")
        assert get_claim(conn, cid) is not None
        # A second claim citing the first also writes a supports edge.
        cid2 = add_claim(
            conn, db_file.parent, "a claim that supports the first",
            supports=[cid],
        )
        assert get_claim(conn, cid2) is not None
    finally:
        conn.close()
