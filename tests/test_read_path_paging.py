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
