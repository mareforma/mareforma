"""Backup and restore integrity: the claims.toml DR artifact and its restore.

claims.toml is the source of truth for ``mareforma restore`` after loss of
graph.db. It must survive a crash during its own rewrite, and restore must
reconstruct the state promotion depends on rather than trust an unsigned field.
"""
from __future__ import annotations

import os

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover -- 3.10 path
    import tomli as tomllib  # type: ignore[no-redef]

import mareforma
from tests._helpers import _bootstrap_key


def test_backup_survives_a_crash_at_the_atomic_rename(tmp_path, monkeypatch):
    """A crash during the backup rewrite must leave the previous good
    claims.toml intact, never a truncated or empty file."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"

    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("first claim alpha", generated_by="x")
        first = out.read_bytes()
        assert b"first claim alpha" in first

        def _boom_replace(src, dst, *a, **k):
            raise OSError("simulated crash before the rename completes")

        monkeypatch.setattr(os, "replace", _boom_replace)
        # The next mutation's backup fails at the atomic-commit step. The backup
        # is non-fatal, so the claim write itself still succeeds.
        g.assert_claim("second claim beta", generated_by="x")

    after = out.read_bytes()
    assert after == first
    assert b"first claim alpha" in after
    assert b"second claim beta" not in after
    # No half-written temp file lingers in the project root.
    assert not list(tmp_path.glob(".claims.toml.*.tmp"))


def test_backup_does_not_capture_a_rolled_back_claim(tmp_path):
    """A claim written inside a caller's open transaction that then rolls back
    must not persist in claims.toml. The backup snapshots committed state only,
    so it runs only when add_claim owns the transaction, not when it joined a
    caller's open one."""
    import mareforma.db.core as _core

    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"

    with mareforma.open(tmp_path, key_path=root_key) as g:
        g.assert_claim("committed alpha claim", generated_by="x")
        conn = g._conn
        # A caller owns the transaction; add_claim joins it (own_transaction is
        # False), writes the claim, then the caller rolls the whole thing back.
        conn.execute("BEGIN IMMEDIATE")
        _core.add_claim(
            conn, tmp_path, "phantom rolled-back claim", generated_by="x",
        )
        conn.rollback()

    toml = out.read_text()
    assert "committed alpha claim" in toml
    assert "phantom rolled-back claim" not in toml


def _count_claims_toml_writes(monkeypatch, out):
    """Count real claims.toml writes by intercepting the atomic rename."""
    counter = {"n": 0}
    real = os.replace

    def _counting(src, dst, *a, **k):
        if str(dst) == str(out):
            counter["n"] += 1
        return real(src, dst, *a, **k)

    monkeypatch.setattr(os, "replace", _counting)
    return counter


def test_refresh_convergence_writes_the_backup_once_not_per_row(
    tmp_path, monkeypatch,
):
    """A refresh pass over many flagged rows writes claims.toml once, not once
    per row."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"
    with mareforma.open(tmp_path, key_path=root_key) as g:
        for i in range(4):
            g.assert_claim(f"claim number {i}", generated_by="x")
        g._conn.execute("UPDATE claims SET convergence_retry_needed = 1")
        g._conn.commit()

        writes = _count_claims_toml_writes(monkeypatch, out)
        g.refresh_convergence()

    assert writes["n"] == 1


def test_refresh_unsigned_writes_the_backup_once_not_per_claim(
    tmp_path, monkeypatch, httpx_mock,
):
    """Clearing a backlog of unlogged claims writes claims.toml once, not once
    per claim. The sidecar-replay path has no network call to hide the cost."""
    from mareforma.db import _record_rekor_inclusion

    rekor_url = "https://rekor.test.example/api/v1/log/entries"
    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"
    with mareforma.open(tmp_path, key_path=root_key, rekor_url=rekor_url) as g:
        # Rekor is down at assert time, so every claim is signed but unlogged.
        for _ in range(4):
            httpx_mock.add_response(method="POST", url=rekor_url, status_code=503)
        ids = [
            g.assert_claim(f"pending claim {i}", generated_by="x")
            for i in range(4)
        ]
        # A sidecar entry per claim routes refresh_unsigned through the replay
        # path, which re-logs the rows without touching the network.
        for i, cid in enumerate(ids):
            assert _record_rekor_inclusion(
                g._conn,
                cid,
                {
                    "uuid": f"uuid-{i}",
                    "logIndex": i,
                    "integratedTime": 1_700_000_000,
                },
            )

        writes = _count_claims_toml_writes(monkeypatch, out)
        result = g.refresh_unsigned()

    assert result == {"checked": 4, "logged": 4, "still_unlogged": 0}
    assert writes["n"] == 1


def test_defer_backup_batches_writes_and_backup_forces_one(tmp_path, monkeypatch):
    """defer_backup groups mutations under one write and leaves claims.toml
    current on exit; backup() forces a write on demand."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"
    with mareforma.open(tmp_path, key_path=root_key) as g:
        writes = _count_claims_toml_writes(monkeypatch, out)
        with g.defer_backup():
            for i in range(5):
                g.assert_claim(f"deferred claim {i}", generated_by="x")
            assert writes["n"] == 0  # nothing written inside the window
        assert writes["n"] == 1  # one write when the window closes

        toml = out.read_text()
        for i in range(5):
            assert f"deferred claim {i}" in toml

        g.backup()
        assert writes["n"] == 2


def test_nested_defer_backup_keeps_batching_until_the_outer_window_closes(
    tmp_path, monkeypatch,
):
    """A defer_backup window nested inside another writes claims.toml once, when
    the outermost window closes, not when the inner one exits."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"
    with mareforma.open(tmp_path, key_path=root_key) as g:
        writes = _count_claims_toml_writes(monkeypatch, out)
        with g.defer_backup():
            g.assert_claim("outer claim one", generated_by="x")
            with g.defer_backup():
                g.assert_claim("inner claim two", generated_by="x")
                assert writes["n"] == 0  # inner window still batching
            # Inner window closed, but the outer one is still open: no write yet.
            assert writes["n"] == 0
            g.assert_claim("outer claim three", generated_by="x")
        assert writes["n"] == 1  # single write when the outermost window closes

        toml = out.read_text()
        assert "outer claim one" in toml
        assert "inner claim two" in toml
        assert "outer claim three" in toml


def test_close_inside_a_nested_defer_backup_still_flushes_the_batch(
    tmp_path, monkeypatch,
):
    """Closing the graph mid-batch (inside nested defer_backup windows) flushes
    the pending write once and leaves no window keyed on the connection id. The
    unwinding context managers then resume on a drained connection as no-ops."""
    import mareforma.db.core as _core

    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"
    g = mareforma.open(tmp_path, key_path=root_key)
    conn_id = id(g._conn)
    writes = _count_claims_toml_writes(monkeypatch, out)
    with g.defer_backup():
        with g.defer_backup():
            g.assert_claim("claim written mid batch", generated_by="x")
            assert writes["n"] == 0  # nothing written while windows are open
            g.close()  # drains every level at once, before the conn closes
            assert writes["n"] == 1  # the batched write happened on close
            assert conn_id not in _core._backup_suspended  # no leaked key

    assert "claim written mid batch" in out.read_text()


def test_null_other_claim_id_verdict_does_not_break_the_backup(tmp_path, capsys):
    """A single-row cross-method verdict stores other_claim_id=None. The backup
    must not choke on the NULL: tomli_w cannot serialize None, and the blanket
    except turned that into a permanent silent stall where claims.toml froze."""
    from tests._helpers import _load_signer

    root_key = _bootstrap_key(tmp_path, "root.key")
    member_key = _bootstrap_key(tmp_path, "member.key")
    out = tmp_path / "claims.toml"

    with mareforma.open(tmp_path, key_path=root_key) as g:
        # The member claim is signed by a distinct key: a verdict issuer cannot
        # verdict a claim it authored (self-verdicts are refused).
        member = g.assert_claim("a claim that got a cross-method verdict",
                                generated_by="x", signer=_load_signer(member_key))
        g.record_replication_verdict(
            verdict_id="v_single", cluster_id="c1", member_claim_id=member,
            other_claim_id=None, method="cross-method",
        )
        # A claim asserted AFTER the NULL verdict must still land in the backup.
        later = g.assert_claim("a claim asserted after the verdict",
                               generated_by="x")

    err = capsys.readouterr().err
    assert "claims.toml backup failed" not in err

    data = tomllib.loads(out.read_text())
    assert later in data["claims"]
    assert "v_single" in data.get("replication_verdicts", {})
    # The NULL key is simply omitted, not serialized as a value.
    assert "other_claim_id" not in data["replication_verdicts"]["v_single"]


def test_null_integrated_time_rekor_entry_does_not_break_the_backup(
    tmp_path, capsys,
):
    """A rekor inclusion row can carry a NULL integrated_time (a malformed
    integratedTime in the log response). The backup must omit the key rather
    than hand None to the TOML serializer."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    out = tmp_path / "claims.toml"

    with mareforma.open(tmp_path, key_path=root_key) as g:
        cid = g.assert_claim("a witnessed claim", generated_by="x")
        g._conn.execute(
            "INSERT INTO rekor_inclusions "
            "(claim_id, uuid, log_index, integrated_time, raw_response_b64, "
            " recorded_at) VALUES (?, ?, ?, NULL, ?, ?)",
            (cid, "uuid-abc", 1, "cmF3", "2026-01-01T00:00:00Z"),
        )
        g._conn.commit()
        # Trigger a fresh backup that reads the NULL-integrated_time row.
        g.assert_claim("another claim to force a backup", generated_by="x")

    err = capsys.readouterr().err
    assert "claims.toml backup failed" not in err

    data = tomllib.loads(out.read_text())
    assert cid in data.get("rekor_inclusions", {})
    assert "integrated_time" not in data["rekor_inclusions"][cid]


def test_a_relative_root_stays_anchored_across_a_chdir(tmp_path, monkeypatch):
    """``mareforma.open("proj")`` must bind its sidecars to the directory the
    connection opened, not re-resolve them against the cwd of the moment. A
    caller (or an observed target) that chdirs into a sibling holding an
    identically named subdirectory would otherwise split the corpus in two."""
    root_key = _bootstrap_key(tmp_path, "root.key")
    here = tmp_path / "a" / "proj"
    there = tmp_path / "b" / "proj"
    here.mkdir(parents=True)
    there.mkdir(parents=True)

    monkeypatch.chdir(tmp_path / "a")
    with mareforma.open("proj", key_path=root_key) as g:
        g.assert_claim("first claim alpha", generated_by="x")
        monkeypatch.chdir(tmp_path / "b")
        cid = g.assert_claim("second claim beta", generated_by="x")
        g.query_provenance(cid)

    data = tomllib.loads((here / "claims.toml").read_text())
    texts = {c["text"] for c in data["claims"].values()}
    assert texts == {"first claim alpha", "second claim beta"}
    assert not (there / "claims.toml").exists()
    assert not (there / ".mareforma").exists()
