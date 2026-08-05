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
    update_claim,
)


def test_open_db_wraps_a_corrupt_file_as_database_error(tmp_path: Path) -> None:
    """A corrupt graph.db must raise mareforma's DatabaseError, not a raw
    sqlite3 error, and the message must point at the claims.toml recovery.

    sqlite3.DatabaseError ('file is not a database') is the PARENT of
    OperationalError, so the old narrow catch let it escape open() and every
    CLI command as a bare traceback, and automation catching mareforma's
    DatabaseError never saw it.
    """
    import sqlite3

    mdir = tmp_path / ".mareforma"
    mdir.mkdir(parents=True, exist_ok=True)
    (mdir / "graph.db").write_bytes(b"this is not a sqlite database at all")

    with pytest.raises(DatabaseError) as exc_info:
        open_db(tmp_path)
    # Not the raw sqlite3 exception the docstring never promised.
    assert not isinstance(exc_info.value, sqlite3.Error)
    assert "claims.toml" in str(exc_info.value)


def test_open_db_from_db_path_wraps_a_corrupt_file(tmp_path: Path) -> None:
    """open_db_from_db_path must uphold the same contract on a corrupt file."""
    import sqlite3

    db_file = tmp_path / "custom.db"
    db_file.write_bytes(b"garbage bytes, definitely not sqlite")

    with pytest.raises(DatabaseError) as exc_info:
        open_db_from_db_path(db_file)
    assert not isinstance(exc_info.value, sqlite3.Error)


def test_cross_thread_writes_do_not_merge_and_lose(tmp_path: Path) -> None:
    """A write on one thread must not silently join a transaction open on
    another thread and be erased by that thread's rollback.

    open_db builds the connection with check_same_thread=False, so a graph can
    be driven from more than one thread. Ownership is decided by
    ``not conn.in_transaction``, a connection-wide property. Without
    serialization thread B reads thread A's open BEGIN IMMEDIATE as its own,
    skips its own transaction, and joins A's. When A rolls back, B's claim is
    gone though B's assert_claim reported a claim_id as persisted.
    """
    import sqlite3
    import threading

    import mareforma
    import mareforma._supports as _supports
    from tests._helpers import _bootstrap_key

    key = _bootstrap_key(tmp_path, "root.key")

    orig = _supports.record_supports_edges
    a_in_txn = threading.Event()
    release_a = threading.Event()

    def patched(conn, claim_id, supports):
        # Pause thread A INSIDE its owned transaction (after BEGIN IMMEDIATE),
        # then force a rollback. This is the window in which a second thread
        # could observe the open transaction and merge into it.
        if threading.current_thread().name == "writerA":
            a_in_txn.set()
            release_a.wait(timeout=5.0)
            raise sqlite3.OperationalError("forced rollback of A's transaction")
        return orig(conn, claim_id, supports)

    with mareforma.open(tmp_path, key_path=key) as g:
        _supports.record_supports_edges = patched
        try:
            def run_a() -> None:
                with pytest.raises(DatabaseError):
                    g.assert_claim("claim authored by thread A", generated_by="a")

            b_result: list[str] = []

            def run_b() -> None:
                b_result.append(
                    g.assert_claim("claim authored by thread B", generated_by="b")
                )

            t_a = threading.Thread(target=run_a, name="writerA")
            t_a.start()
            assert a_in_txn.wait(timeout=5.0), "thread A never entered its txn"

            t_b = threading.Thread(target=run_b, name="writerB")
            t_b.start()
            # Give B a chance to (wrongly) join A's open transaction.
            t_b.join(timeout=1.0)

            release_a.set()
            t_a.join(timeout=5.0)
            t_b.join(timeout=5.0)
        finally:
            _supports.record_supports_edges = orig

        assert b_result, "thread B never returned a claim_id"
        # B's claim must survive A's rollback: it belongs to B's own committed
        # transaction, not A's aborted one.
        assert g.get_claim(b_result[0]) is not None


def test_update_claim_sanitizes_text_on_write(tmp_path: Path) -> None:
    """update_claim must strip zero-width / bidi codepoints like add_claim.

    Sanitize-on-write is defense in depth so any consumer reading text directly
    gets a clean string. update_claim wrote text verbatim, so an edit could
    re-introduce the injection surface add_claim exists to close, and the FTS
    trigger copied it into the search index too.
    """
    (tmp_path / ".mareforma").mkdir(parents=True, exist_ok=True)
    conn = open_db(tmp_path)
    try:
        cid = add_claim(conn, tmp_path, "clean text")
        # U+202E (RTL override) + U+200B (zero-width space) inside the edit.
        update_claim(conn, tmp_path, cid, text="x ‮vil​ y")
        stored = get_claim(conn, cid)["text"]
        assert "‮" not in stored
        assert "​" not in stored
    finally:
        conn.close()


def test_update_claim_enforces_the_text_cap(tmp_path: Path) -> None:
    """update_claim must reject text past the cap add_claim enforces."""
    (tmp_path / ".mareforma").mkdir(parents=True, exist_ok=True)
    conn = open_db(tmp_path)
    try:
        cid = add_claim(conn, tmp_path, "clean text")
        with pytest.raises(ValueError):
            update_claim(conn, tmp_path, cid, text="A" * 200_000)
    finally:
        conn.close()


def test_update_claim_refuses_supports_contradicts_overlap(tmp_path: Path) -> None:
    """update_claim must refuse a support+contradict on the same upstream,
    the same logically-incoherent state add_claim rejects."""
    (tmp_path / ".mareforma").mkdir(parents=True, exist_ok=True)
    conn = open_db(tmp_path)
    try:
        up = add_claim(conn, tmp_path, "an upstream claim")
        cid = add_claim(conn, tmp_path, "a downstream claim")
        with pytest.raises(ValueError):
            update_claim(conn, tmp_path, cid, supports=[up], contradicts=[up])
    finally:
        conn.close()


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


def test_sanitized_text_is_signed_and_stored_as_one_string(
    tmp_path: Path,
) -> None:
    """Sanitize-on-write must leave no whitespace the signature then strips.

    strip() runs before sanitize_for_llm, so a zero-width space in front of a
    real space survived the strip and sanitized to a leading blank. The INSERT
    bound that value while the signature covered text.strip(), so an honest
    write verified as tampered and restore refused to rebuild the graph.
    """
    import base64
    import json

    import mareforma
    from mareforma.db import verify_claim_signatures
    from tests._helpers import _bootstrap_key

    key_path = _bootstrap_key(tmp_path, "root.key")
    # U+200B (zero-width space) followed by an ordinary space: the usual
    # copy-paste residue from a web page or a PDF.
    text = "​ a finding pasted from a web page"

    with mareforma.open(tmp_path, key_path=key_path) as g:
        cid = g.assert_claim(text)
        row = g.get_claim(cid)
        bundle = json.loads(row["signature_bundle"])
        payload = json.loads(base64.b64decode(bundle["payload"]))
        assert payload["predicate"]["text"] == row["text"]
        assert verify_claim_signatures(g._conn, row) == (True, "")

    db_dir = tmp_path / ".mareforma"
    for f in db_dir.iterdir():
        f.unlink()
    db_dir.rmdir()
    assert mareforma.restore(tmp_path)["claims_restored"] == 1


def test_text_of_only_invisible_characters_is_refused(tmp_path: Path) -> None:
    """Zero-width plus whitespace has no visible content and must not store."""
    (tmp_path / ".mareforma").mkdir(parents=True, exist_ok=True)
    conn = open_db(tmp_path)
    try:
        with pytest.raises(ValueError, match="no visible content"):
            add_claim(conn, tmp_path, "​ ​")
    finally:
        conn.close()


def test_sanitized_text_compares_equal_on_retry_and_signed_edit(
    tmp_path: Path,
) -> None:
    """The stored text is what idempotency and signed-immutability compare.

    Both compared the caller's text.strip() against the row, so a genuine
    retry of the same string raised IdempotencyConflictError and re-supplying
    the original text to update_claim wrongly tripped the immutability guard.
    """
    import mareforma
    from tests._helpers import _bootstrap_key

    key_path = _bootstrap_key(tmp_path, "root.key")
    text = "​ a finding pasted from a web page"

    with mareforma.open(tmp_path, key_path=key_path) as g:
        cid = g.assert_claim(text, idempotency_key="k1")
        assert g.assert_claim(text, idempotency_key="k1") == cid
        # Re-supplying the original text is a no-op edit, not a mutation.
        update_claim(g._conn, tmp_path, cid, text=text, status="contested")
        assert g.get_claim(cid)["status"] == "contested"
