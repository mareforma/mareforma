"""Read-path paging: the default query sorts the table once, not per batch.

When the verified-claim filter drains most rows (a default query over many
unsigned PRELIMINARY claims), the read loop keeps pulling until it has enough
survivors. Paging with a growing OFFSET re-runs the ordering query per batch,
so a table that cannot use an index for the ordering is fully re-scanned and
re-sorted on every batch. A single forward scan runs the ordering query once.
"""
from __future__ import annotations

import mareforma
from tests._helpers import _bootstrap_key, _two_signers


def test_query_sorts_the_table_once_not_per_batch(tmp_path):
    sa, _ = _two_signers(tmp_path)  # unenrolled: its claims drain by default
    root_key = _bootstrap_key(tmp_path, "root.key")
    with mareforma.open(tmp_path, key_path=root_key) as g:
        for i in range(60):
            g.assert_claim(
                f"drained claim number {i}", generated_by="x", signer=sa,
            )

    with mareforma.open(tmp_path, key_path=root_key) as g:
        seen: list[str] = []
        g._conn.set_trace_callback(seen.append)
        try:
            # include_unverified=False, so every unenrolled-PRELIMINARY row is
            # drained and the loop scans the whole table for survivors.
            g.query(limit=10)
        finally:
            g._conn.set_trace_callback(None)

    orderings = [s for s in seen if "ORDER BY CASE support_level" in s]
    assert len(orderings) == 1, (
        f"expected one scan-and-sort over the claims table, got {len(orderings)}"
    )


def _count_materialised_rows(g, call):
    """Run *call* and return how many rows the connection materialised.

    Wraps the row_factory so every row pulled into Python is counted. The
    counter is reset immediately before *call* so only that call's fetches are
    measured (the helper queries a read runs are few and constant)."""
    count = {"n": 0}
    base = g._conn.row_factory

    def counting(cursor, row):
        count["n"] += 1
        return base(cursor, row)

    g._conn.row_factory = counting
    try:
        call()
    finally:
        g._conn.row_factory = base
    return count["n"]


def test_common_path_does_not_materialise_the_whole_ceiling(tmp_path):
    """When the first `limit` rows all survive projection, the read must stop
    fetching there, not pull the whole scan ceiling into Python."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    n_rows = 300
    with mareforma.open(tmp_path, key_path=root_key) as g:
        # Root-signed claims: the generator is the enrolled root, so every row
        # survives the default read filter and the first `limit` all survive.
        for i in range(n_rows):
            g.assert_claim(f"surviving claim number {i}", generated_by="x")

    with mareforma.open(tmp_path, key_path=root_key) as g:
        # Warm the query once so the row_factory count reflects only the fetch,
        # not one-time setup, then measure.
        assert len(g.query(limit=5)) == 5
        fetched = _count_materialised_rows(g, lambda: g.query(limit=5))

    # The old eager `.fetchall()` pulled every row up to the ceiling (all 300
    # here); the lazy cursor stops at the handful it needs. A small constant of
    # helper-query rows is expected on top of the ~5 survivors.
    assert fetched < 50, (
        f"read materialised {fetched} rows for limit=5 over {n_rows} claims; "
        "the whole scan ceiling was pulled instead of stopping at the survivors"
    )

