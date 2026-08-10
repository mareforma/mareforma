"""restore's REPLICATED corroboration check must not scan the graph per row.

``_verify_replicated_corroboration`` re-derives the promotion invariant for
every signed REPLICATED row. It used to run two unindexed probes per row: a
full scan of ``replication_verdicts`` (the ``member_claim_id OR
other_claim_id`` filter has no index to serve it) and a
``claims c, json_each(c.supports_json)`` cross join that JSON-expands every
signed claim in the graph. That is O(R x N) inside restore's open
transaction, on the catastrophic-recovery path. The same question is
answerable in one grouped pass, so the SQL work must not grow with the
number of REPLICATED rows.
"""
from __future__ import annotations

import json
from pathlib import Path

from mareforma import signing
from mareforma.db import open_db
from mareforma.db.core import _promotion_window
from mareforma.db.restore import _verify_replicated_corroboration


def _peer_row(claim_id: str, key, keyid: str) -> tuple:
    """A claim row citing the anchor, carrying the bundle its columns claim.

    A peer only corroborates once its own bundle verifies and binds its row, so
    the fixture signs each one for real. ``evidence_json`` is taken back out of
    the signed predicate: the read path holds the two against each other.
    """
    fields = {
        "claim_id": claim_id,
        "text": f"peer {claim_id}",
        "classification": "INFERRED",
        "generated_by": "lab",
        "supports": ["anchor"],
        "contradicts": [],
        "source_name": None,
        "artifact_hash": None,
        "created_at": "t",
    }
    envelope = signing.sign_claim(fields, key)
    predicate = signing.claim_predicate_from_envelope(envelope)
    return (
        claim_id, fields["text"], fields["classification"],
        fields["generated_by"], json.dumps(fields["supports"]),
        json.dumps(fields["contradicts"]), keyid, json.dumps(envelope),
        json.dumps(predicate["evidence"]),
    )


def _seed(conn, peers: int) -> None:
    """One ESTABLISHED anchor plus ``peers`` REPLICATED rows citing it.

    Two distinct asserter keyids, so every peer is genuinely corroborated and
    the check passes; only the amount of SQL it takes to say so is under test.
    REPLICATED is not an insertable born state, so the peers land PRELIMINARY
    and are promoted, the same two-step restore itself uses.
    """
    keys = [signing.generate_keypair(), signing.generate_keypair()]
    keyids = [signing.public_key_id(k.public_key()) for k in keys]
    conn.execute(
        "INSERT INTO claims (claim_id, text, support_level, supports_json, "
        "validation_signature, created_at, updated_at) "
        "VALUES ('anchor', 'anchor', 'ESTABLISHED', '[]', 'x', 't', 't')"
    )
    conn.executemany(
        "INSERT INTO claims (claim_id, text, classification, generated_by, "
        "supports_json, contradicts_json, asserter_keyid, signature_bundle, "
        "evidence_json, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 't', 't')",
        [_peer_row(f"c{i}", keys[i % 2], keyids[i % 2]) for i in range(peers)],
    )
    with _promotion_window(conn):
        conn.execute(
            "UPDATE claims SET support_level = 'REPLICATED' "
            "WHERE claim_id != 'anchor'"
        )
    conn.commit()


def _statements(tmp_path: Path, peers: int) -> list[str]:
    conn = open_db(tmp_path)
    try:
        _seed(conn, peers)
        seen: list[str] = []
        conn.set_trace_callback(seen.append)
        try:
            _verify_replicated_corroboration(conn)
        finally:
            conn.set_trace_callback(None)
        return seen
    finally:
        conn.close()


def test_corroboration_sql_does_not_grow_with_replicated_rows(
    tmp_path: Path,
) -> None:
    small = _statements(tmp_path / "small", 20)
    large = _statements(tmp_path / "large", 200)
    assert len(large) == len(small), (
        f"corroboration check ran {len(small)} statements for 20 rows and "
        f"{len(large)} for 200; it still probes per row"
    )
