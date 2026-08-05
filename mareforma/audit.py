"""``mareforma audit``: per-finding grounding receipts for a third-party pipeline.

``diagnose`` answers one question about one run; ``audit`` answers one question
per FINDING. The target runs IN-PROCESS under the same observer diagnose uses
(a subprocess would hide it behind the observer's own seam) and never has to
import mareforma. A findings mapping supplies ``finding_id`` → cited source(s);
on exit one verdict per finding is computed from the SHARED observed evidence
against that finding's cited set, with the same bind-time citation semantics a
cooperating producer gets. Each verdict is emitted twice:

- ``receipts.jsonl``, one plain verdict receipt per finding, directly
  consumable by ``mareforma measure`` and ``summarize_pilot``;
- ``envelopes/<n>-<finding_id>.json``, the same record DSSE-signed with the
  auditor's key, checkable by ``mareforma verify`` from public material alone.

No self-report: nothing the target prints, writes, or declares enters a
verdict. The mapping names what each finding CLAIMS to cite; the observer alone
supplies what happened. A crashing target still yields receipts over the
partial observation, and the run record carries the target's own exit code.

The trust boundary is the shared interpreter. Running the target in-process is
what lets the observer see its reads at all, and it is also the limit: a target
written to defeat the audit can import the observer's internals and fabricate
reads, or patch the auditor itself. The receipts therefore grade a pipeline
that does not attack its auditor, the silent fallback, the unread citation , 
and the signature attests the auditor's observation, not the target's honesty.

Corpus mode iterates run specs with one fresh interpreter per run, a target
cannot poison the observation of the next, and is resumable: a run is skipped
on re-invocation only when its record is complete AND verifies against the
auditor's key. The key never enters a child. Children hand their records to
the parent over an anonymous pipe, keyed with a nonce they send before the
target starts, and the parent signs those, so a target can neither reach the
key through the frame stack nor rewrite what the auditor signs on its way
out, in the run directory or on the channel itself.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import traceback
from pathlib import Path
from typing import Callable

import click

from mareforma.diagnose import _exit_code_of, _run_target

# The receipts file and per-run record the auditor emits. The run record is
# written LAST, as a DSSE envelope under the auditor's key: the resume key is a
# VERIFIED ``completed`` flag, so a killed run never reads as complete and a
# record the target forged on disk never skips a run.
RECEIPTS_FILE = "receipts.jsonl"
RUN_RECORD_FILE = "run.json"
ENVELOPES_DIR = "envelopes"


def load_findings(path: Path) -> dict[str, tuple[str, ...]]:
    """Load and normalize the ``finding_id`` → cited source(s) mapping.

    The file is a JSON object; each value is one source string or a list of
    them. Sources are normalized here, once, the same way the scope normalizes
    its cited set, so receipts carry comparable identifiers and the read-side
    binding re-check stays pure string comparison.
    """
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise click.UsageError(f"could not read the findings mapping {path}: {exc}")
    if not isinstance(raw, dict) or not raw:
        raise click.UsageError(
            "the findings mapping must be a non-empty JSON object of "
            "finding_id -> cited source(s)"
        )
    from mareforma.observe._citation import cited_set

    findings: dict[str, tuple[str, ...]] = {}
    for fid, sources in raw.items():
        cited = cited_set(sources)
        if not cited:
            raise click.UsageError(
                f"finding {fid!r} names no usable cited source"
            )
        findings[str(fid)] = cited
    return findings


def _resolve_key(key_path) -> "Path":
    """The auditor's signing key path, or a usage error naming the fix.

    An audit receipt is only worth forwarding if it verifies, so audit refuses
    to run unsigned rather than silently degrading the contract.
    """
    from mareforma import signing

    key_file = Path(key_path) if key_path else signing.default_key_path()
    if not key_file.exists():
        raise click.UsageError(
            f"no signing key at {key_file}; audit receipts must be signed. "
            "Run `mareforma bootstrap` or pass --key."
        )
    return key_file


def _finding_verdict(scope, cited: tuple[str, ...]):
    """One finding's verdict from the shared observed evidence.

    Classification is pure and should not raise, but if it ever does the
    receipt degrades to an honest OPAQUE rather than losing the finding.
    ``KeyboardInterrupt`` and ``SystemExit`` propagate: this loop runs after
    the scope has closed, so an abort the operator asked for must end the
    audit, not turn one finding OPAQUE and write the records anyway.
    """
    from mareforma.observe import GroundingVerdict, ObservedGrounding

    try:
        return scope.classify_against(cited)
    except Exception as exc:  # noqa: BLE001
        return GroundingVerdict(
            grounding=ObservedGrounding.OPAQUE,
            reason=(
                "verdict computation failed during audit: "
                f"{type(exc).__name__}"
            ),
            cited_sources=cited,
        )


def _safe_name(finding_id: str) -> str:
    """A filesystem-safe fragment of a finding id for the envelope filename.

    The index prefix on the filename keeps two ids that sanitize identically
    from colliding; the id itself is authoritative inside the signed record.
    """
    return re.sub(r"[^A-Za-z0-9._-]", "_", finding_id)[:80] or "finding"


# The receipt's signature does not verify because the ENVELOPE names a
# different signing key than the one supplied (keyid mismatch), as opposed to
# a signature that fails over the right key (a tampered payload). Both make
# verify_envelope return False; only the keyid tells them apart. The caller
# treats a key mismatch as unverifiable (wrong key), not tamper.
RECEIPT_KEY_MISMATCH_REASON = (
    "receipt signed by a key other than the one supplied"
)


def verify_audit_receipt(envelope: dict, public_key) -> tuple[bool, str]:
    """Verify one signed audit receipt from public material alone.

    Two checks, both required:

    - the DSSE signature over the audit-receipt payload type;
    - the grounding→citation binding, re-checked as pure string comparison
      over the STORED normalized identifiers (never the filesystem): a
      GROUNDED record must ground on at least one source the finding cites,
      or the signature would attest an unbound GROUNDED.

    Returns ``(ok, reason)``. A signature that fails because the envelope
    names a different key returns :data:`RECEIPT_KEY_MISMATCH_REASON` so the
    caller can report a wrong key as unverifiable rather than tamper.
    Structural problems raise :class:`mareforma.signing.InvalidEnvelopeError`
    so the caller can distinguish a malformed envelope from a wrong key.
    """
    import base64

    from mareforma.observe._binding import check_grounding_binding
    from mareforma.signing import (
        PAYLOAD_TYPE_AUDIT_RECEIPT,
        public_key_id,
        verify_envelope,
    )

    # A keyid mismatch and a bad signature both make verify_envelope return
    # False; separate them here so a wrong verification key does not read as
    # tamper. The payload is untouched, so the keyid still identifies the
    # actual signer.
    try:
        sig_keyid = envelope["signatures"][0]["keyid"]
    except (KeyError, IndexError, TypeError):
        sig_keyid = None
    if sig_keyid is not None and sig_keyid != public_key_id(public_key):
        return False, RECEIPT_KEY_MISMATCH_REASON

    if not verify_envelope(
        envelope, public_key, expected_payload_type=PAYLOAD_TYPE_AUDIT_RECEIPT
    ):
        return False, "signature does not verify (receipt payload may be tampered)"
    record = json.loads(base64.standard_b64decode(envelope["payload"]))
    if record.get("grounding") == "GROUNDED":
        binding = check_grounding_binding(
            tuple(record.get("grounded_sources") or ()),
            tuple(record.get("cited_sources") or ()),
        )
        if binding.disjoint:
            return False, f"grounding binding violation: {binding.reason}"
    return True, (
        f"audit receipt verified (finding {record.get('finding_id')!r})"
    )


def run_audit(
    command: list[str],
    *,
    findings_path,
    out_dir,
    key_path,
    as_json: bool,
    redact_home: "Callable[[str], str] | None" = None,
    defer_signing: bool = False,
) -> int:
    """Run COMMAND under the observer and emit per-finding signed receipts.

    Returns the target's own exit code, like diagnose: the audit succeeding is
    signalled by the receipts on disk, not by masking what the target did.

    With *defer_signing* no key is loaded and the outputs land unsigned: this
    is the corpus-child protocol, where the parent signs after the child exits
    so the auditor's key never shares an interpreter with a target. The records
    also go back to the parent on the handoff descriptor, which is what the
    parent signs.
    """
    from mareforma import signing
    from mareforma.observe import _loaders, _scope
    from mareforma.observe._audit import ensure_installed as _ensure_hook
    from mareforma.observe._citation import cited_set

    # Opened before the target runs, so the nonce the parent keys on is on the
    # channel before the target could write anything to it. Dropping the
    # variable does not hide the descriptor, /proc still shows it, the nonce is
    # what tells the parent which bytes are the observer's.
    handoff = _open_handoff(os.environ.pop(HANDOFF_FD_ENV, None))
    signer = None
    if not defer_signing:
        signer = signing.load_private_key(_resolve_key(key_path))
    findings = load_findings(Path(findings_path))
    # Resolved to an absolute path BEFORE the target runs: the target executes
    # in-process and may chdir, and where the receipts land must stay the
    # auditor's choice, anchored to the invocation directory.
    out = Path(out_dir).resolve()

    # One observed run against the union of every finding's cited sources; the
    # per-finding cited set is applied at classification time. Recording does
    # not depend on the cited set (every read and seam is captured either way),
    # so the union changes nothing the observer sees.
    union = cited_set([c for cited in findings.values() for c in cited])
    # Hash read bytes when any finding cites its data by content: without
    # hashing no read can ever match a ``sha256:`` citation, and the classifier
    # floors such findings to OPAQUE rather than a false UNGROUNDED.
    from mareforma.trust._store import is_content_addressed

    hash_reads = any(is_content_addressed(c) for c in union)

    exit_code = 0
    crashed = False
    tb_text = None
    _ensure_hook()
    _loaders.ensure_installed()
    _loaders.refresh_third_party()
    scope = _scope.enter(union, content_address=hash_reads)
    try:
        try:
            _run_target(list(command))
        except click.UsageError:
            # Bad invocation of audit itself, re-raise so click reports it.
            raise
        except SystemExit as exc:
            # A non-zero exit is an aborted run, the same event as a raised
            # exception; only a clean sys.exit(0) leaves the run complete.
            exit_code = _exit_code_of(exc)
            crashed = exit_code != 0
        except BaseException:  # noqa: BLE001, a target crash is expected input
            crashed = True
            exit_code = 1
            tb_text = traceback.format_exc()
        if crashed:
            _scope.record_abort(exit_code)
    finally:
        _scope.exit(scope)

    # Verdicts and receipts are computed AFTER the scope closes, only from what
    # the observer recorded. The target's stdout, files, and exit status never
    # feed a verdict, that is the no-self-report guarantee.
    records = []
    for fid, cited in findings.items():
        verdict = _finding_verdict(scope, cited)
        records.append({
            "finding_id": fid,
            **verdict.receipt(),
            "target": list(command),
            "exit_code": exit_code,
            "partial": crashed,
        })

    out.mkdir(parents=True, exist_ok=True)
    # The envelopes are these same verdicts signed, so the directory gets the
    # same truncation receipts.jsonl gets. Re-auditing into a used --out would
    # otherwise leave an earlier run's envelopes beside the current ones, still
    # signed and still verifying, with nothing on disk saying which run is
    # current. Unconditional: deferred signing writes no envelopes, and a stale
    # set surviving an unsigned run is the same mixed evidence.
    for stale in (out / ENVELOPES_DIR).glob("*.json"):
        stale.unlink()
    _write_receipts(out / RECEIPTS_FILE, records)
    if signer is not None:
        _write_receipt_envelopes(out / ENVELOPES_DIR, records, signer)

    reads = [
        {"kind": r.kind, "identifier": r.identifier, "nonempty": r.nonempty}
        for r in scope.reads
    ]
    seams = [{"kind": s.kind, "detail": s.detail} for s in scope.seams]
    reads_seen, opens_detected = scope.coverage_counts()
    run_record = {
        "target": list(command),
        "exit_code": exit_code,
        "partial": crashed,
        "findings": list(findings),
        "reads": reads,
        "seams": seams,
        "coverage": {
            "reads_seen": reads_seen,
            "opens_detected": opens_detected,
            "read_coverage_fraction": (
                None if opens_detected <= 0 else reads_seen / opens_detected
            ),
        },
        "completed": True,
    }
    if tb_text:
        run_record["traceback"] = tb_text
    # The run record lands last: its ``completed`` flag is the resume key, so a
    # run killed mid-write never reads as complete. Signed here on the direct
    # path, left unsigned for the corpus parent to sign when deferring, and
    # resume honors the flag only inside an envelope the auditor's key
    # verifies, the record sits where the audited target could write.
    (out / RUN_RECORD_FILE).write_text(
        json.dumps(
            run_record if signer is None
            else signing.sign_audit_run(run_record, signer),
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    if handoff is not None:
        handoff.emit(run_record, records)

    verdict_lines = [
        {"finding_id": r["finding_id"], "grounding": r["grounding"],
         "reason": r["reason"]}
        for r in records
    ]
    if as_json:
        text = json.dumps(
            {**run_record, "out": str(out), "verdicts": verdict_lines},
            indent=2,
        )
        click.echo(redact_home(text) if redact_home else text)
    else:
        if crashed and tb_text:
            # Print the traceback the way Python would, before the summary.
            click.echo(tb_text, err=True, nl=False)
        _echo_summary(run_record, verdict_lines, out, redact_home)
    return exit_code


def _write_receipts(path: Path, records: list[dict]) -> None:
    """Write one plain verdict receipt per line."""
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, sort_keys=True) + "\n")


def _open_handoff(fd_text: "str | None") -> "_ChildHandoff | None":
    """The handoff channel the corpus parent opened for this child, if any."""
    return None if fd_text is None else _ChildHandoff(int(fd_text))


class _ChildHandoff:
    """A corpus child's end of the handoff pipe: nonce first, records once.

    The pipe is anonymous, so there is no path the audited target can name and
    rewrite, the way it can rewrite the run directory. The nonce goes out
    before the target starts and the parent accepts only a frame carrying it,
    so what the target writes to the descriptor once it is running, while it
    runs or from the ``atexit`` hook that fires after the observer has closed,
    reads as noise rather than as the observer's records. The nonce is what
    marks the frame, not where the frame falls on the stream: the target
    shares the descriptor and can leave its own bytes unterminated in front of
    the frame, and none of that is its decision to make. The frame ends at a
    newline, which the JSON body cannot contain, so a partial write from a
    killed child lacks the terminator and reads as nothing handed over.

    A target that reads the nonce out of this object can still forge a frame,
    the same in-process reach that lets it fabricate the reads themselves.
    """

    def __init__(self, fd: int) -> None:
        self._fh = os.fdopen(fd, "wb")
        self._nonce = os.urandom(16).hex()
        self._write(self._nonce)

    def emit(self, run_record: dict, records: list[dict]) -> None:
        """Hand the records over and close the channel."""
        self._write(self._nonce + " " + json.dumps(
            {"run_record": run_record, "receipts": records}
        ))
        self._fh.close()

    def _write(self, line: str) -> None:
        self._fh.write(line.encode("utf-8") + b"\n")
        self._fh.flush()


def _write_receipt_envelopes(env_dir: Path, records: list[dict], signer) -> None:
    """DSSE-sign each receipt into ``envelopes/<n>-<finding_id>.json``."""
    from mareforma import signing

    env_dir.mkdir(parents=True, exist_ok=True)
    for i, rec in enumerate(records, start=1):
        name = f"{i:03d}-{_safe_name(rec['finding_id'])}.json"
        (env_dir / name).write_text(
            json.dumps(signing.sign_audit_receipt(rec, signer), indent=2) + "\n",
            encoding="utf-8",
        )


def _echo_summary(run_record: dict, verdicts: list[dict], out: Path,
                  redact: "Callable[[str], str] | None") -> None:
    """Print the audit summary in human-readable form."""
    def line(text: str) -> None:
        click.echo(redact(text) if redact else text)

    click.echo(click.style("AUDIT REPORT", bold=True, fg="cyan"))
    line("  target: " + " ".join(run_record["target"]))
    if run_record["partial"]:
        click.echo(click.style(
            "  target exited with error, partial observation", fg="yellow"))
    click.echo(
        f"  reads: {len(run_record['reads'])}  "
        f"seams: {len(run_record['seams'])}  "
        f"findings: {len(verdicts)}"
    )
    for v in verdicts:
        color = {"GROUNDED": "green", "UNGROUNDED": "red",
                 "OPAQUE": "yellow"}.get(v["grounding"], "white")
        click.echo(
            f"    {v['finding_id']}: "
            + click.style(v["grounding"], fg=color, bold=True)
        )
        line(f"      {v['reason']}")
    line(f"  receipts: {out / RECEIPTS_FILE}")
    line(f"  signed envelopes: {out / ENVELOPES_DIR}/")


# -- corpus mode --------------------------------------------------------------

def _run_completed(run_dir: Path, public_key) -> bool:
    """Whether this run already holds a complete, auditor-signed record.

    The record sits where an audited target could write, so the ``completed``
    flag is honored only inside a run-record envelope that verifies against
    the auditor's key. That key never enters a process that runs a target
    (:func:`_sign_run_outputs` signs here, in the parent, after the child
    exits), so a hostile run A cannot mint the signature run B's record needs:
    whatever it plants reads as not complete and the run re-executes.
    """
    import base64

    from mareforma.signing import PAYLOAD_TYPE_AUDIT_RUN, verify_envelope

    try:
        envelope = json.loads(
            (run_dir / RUN_RECORD_FILE).read_text(encoding="utf-8")
        )
        if not verify_envelope(
            envelope, public_key, expected_payload_type=PAYLOAD_TYPE_AUDIT_RUN
        ):
            return False
        record = json.loads(base64.standard_b64decode(envelope["payload"]))
        return bool(record.get("completed"))
    except Exception:  # noqa: BLE001, any defect in the record means re-run
        return False


def _load_spec(spec_path: Path) -> dict:
    """Load one run spec: ``{"command": [...], "findings": {...}}``.

    A malformed spec aborts the corpus with the spec named, the corpus is the
    auditor's own input, so failing fast beats auditing the wrong thing.
    """
    try:
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise click.UsageError(f"could not read run spec {spec_path}: {exc}")
    command = spec.get("command") if isinstance(spec, dict) else None
    findings = spec.get("findings") if isinstance(spec, dict) else None
    if (
        not isinstance(command, list)
        or not command
        or not all(isinstance(t, str) for t in command)
        or not isinstance(findings, dict)
        or not findings
    ):
        raise click.UsageError(
            f"run spec {spec_path} must carry a non-empty 'command' list of "
            "strings and a non-empty 'findings' mapping"
        )
    return spec


def _execute_run(spec: dict, run_dir: Path) -> tuple[int, "dict | None"]:
    """Run one corpus spec in a fresh interpreter (the per-run isolation).

    A fresh process guarantees one target cannot poison the observation of the
    next: no shared module cache, no leaked global state, no half-crashed
    interpreter. The child gets no signing key and hands its records back on a
    pipe this process opened, so a target can neither reach the key through
    the frame stack nor rewrite the records after the observer wrote them. The
    child's exit code is the target's own, and its stderr (the target's
    traceback on a crash) is persisted next to the record so nothing is
    swallowed.

    Returns the child's exit code and the records it handed over, or ``None``
    when it handed over nothing usable.
    """
    from concurrent.futures import ThreadPoolExecutor

    import mareforma

    cmd = [
        sys.executable, "-m", "mareforma", "audit",
        "--findings", str(run_dir / "findings.json"),
        "--out", str(run_dir),
        "--defer-signing",
        "--", *spec["command"],
    ]
    env = dict(os.environ)
    # The child must import the same mareforma this process runs, including a
    # source checkout that was never pip-installed.
    pkg_root = str(Path(mareforma.__file__).resolve().parents[1])
    env["PYTHONPATH"] = (
        pkg_root + os.pathsep + env["PYTHONPATH"]
        if env.get("PYTHONPATH") else pkg_root
    )
    # An anonymous pipe rather than a temp file: the target runs in-process in
    # the child and can reopen any path it can name, so a file leaves the
    # records rewritable after the observer wrote them. The parent drains the
    # pipe while the child runs, a buffer deep enough to hold the records is
    # not something either side can promise.
    read_fd, write_fd = os.pipe()
    env[HANDOFF_FD_ENV] = str(write_fd)
    try:
        proc = subprocess.Popen(
            cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, pass_fds=(write_fd,),
        )
    except BaseException:
        os.close(read_fd)
        raise
    finally:
        # The child holds the only write end from here, so the read end sees
        # EOF when it exits however it exits.
        os.close(write_fd)
    with ThreadPoolExecutor(max_workers=1) as pool:
        reading = pool.submit(_read_handoff, read_fd)
        try:
            _, stderr = proc.communicate()
        except BaseException:
            # An interrupt leaves the child running and the reader waiting on
            # EOF that would never come.
            proc.kill()
            proc.wait()
            raise
        handoff = reading.result()
    if stderr:
        (run_dir / "stderr.txt").write_text(stderr, encoding="utf-8")
    return proc.returncode, handoff


def _read_handoff(fd: int) -> "dict | None":
    """The records a child handed over, or ``None`` if it handed none.

    The child writes its nonce before the target runs and then one frame
    carrying it; everything else on the pipe was written by the audited
    target, which does not know the nonce, and is discarded. Reading runs to
    EOF either way: the target shares the descriptor, and a write of its own
    left blocking on a full pipe would keep the child from exiting.

    Where the frame sits is not the target's to decide. It shares the
    descriptor and owes it no terminator, so its last bytes can run straight
    into the front of the frame; keying on the frame starting a line would let
    any target suppress its own record, fail the run closed, and repeat that
    on every re-invocation. The nonce marker is found wherever it lands.

    A child killed before it wrote leaves the channel empty or a frame without
    its terminator; that reads as nothing handed over, the run stays unsigned,
    and resume re-executes it.
    """
    handoff = None
    with os.fdopen(fd, "rb") as fh:
        nonce = fh.readline(_HANDOFF_NONCE_MAX)
        if nonce.endswith(b"\n"):
            for frame in _handoff_frames(fh, nonce[:-1] + b" "):
                # The observer emits once, after the target has finished
                # running, so its frame is the last marked one on the channel;
                # anything marked ahead of it was there while the target still
                # held the descriptor. Later wins, and a frame that does not
                # parse leaves the last one that did in place.
                handoff = _parse_handoff(frame) or handoff
    return handoff


def _handoff_frames(fh: BinaryIO, marker: bytes) -> Iterator[bytes]:
    """Yield the body of each terminated frame carrying *marker*, in order.

    Scanning, not line reading: the marker is matched wherever it appears and
    the bytes ahead of it are dropped as they are read, so a target writing
    without a terminator cannot grow this buffer. Only what follows a matched
    marker is held, and only up to :data:`_HANDOFF_FRAME_MAX`; past that the
    match is abandoned and scanning resumes, since the observer's frame is one
    flushed write and does not arrive that far from its terminator.
    """
    # Held back on every discard: a marker split across two reads is still
    # whole in the join. A one-byte marker would make this zero, and ``[-0:]``
    # keeps everything, so the empty case is spelled out.
    tail = len(marker) - 1
    keep = (lambda buf: buf[-tail:]) if tail else (lambda buf: b"")
    buf = b""
    framing = False
    while True:
        chunk = fh.read(_HANDOFF_CHUNK)
        if not chunk:
            return
        buf += chunk
        while True:
            if not framing:
                at = buf.find(marker)
                if at < 0:
                    buf = keep(buf)
                    break
                buf = buf[at + len(marker):]
                framing = True
            end = buf.find(b"\n")
            if end < 0:
                if len(buf) > _HANDOFF_FRAME_MAX:
                    buf, framing = keep(buf), False
                    continue
                break
            yield buf[:end]
            buf = buf[end + 1:]
            framing = False


def _parse_handoff(frame: bytes) -> "dict | None":
    """The records inside a handoff frame, or ``None`` if it is not one.

    The parent signs this, so the shape is checked rather than trusted: keys
    it does not know are a refusal, at the top level and in the run record it
    signs whole, so nothing the child composed beyond the record it observed
    can ride into a signature.
    """
    try:
        handoff = json.loads(frame)
    except ValueError:
        return None
    if (
        not isinstance(handoff, dict)
        or set(handoff) != {"run_record", "receipts"}
        or not isinstance(handoff["run_record"], dict)
        or not isinstance(handoff["receipts"], list)
        or not set(handoff["run_record"]) <= RUN_RECORD_KEYS
    ):
        return None
    return handoff


def _clear_run_outputs(run_dir: Path) -> None:
    """Delete a run's records before it executes.

    What sits there is either a killed earlier attempt or state another run's
    target planted. Clearing first means a run that never finishes leaves no
    earlier records behind to read as its own.
    """
    import shutil

    for name in (RUN_RECORD_FILE, RECEIPTS_FILE):
        (run_dir / name).unlink(missing_ok=True)
    shutil.rmtree(run_dir / ENVELOPES_DIR, ignore_errors=True)


def _sign_run_outputs(run_dir: Path, handoff: "dict | None", signer) -> None:
    """Sign what the child handed over, here in the parent, and write it out.

    The child observed the target in its own interpreter and passed its
    records back on the handoff descriptor. Signing after it exits keeps the
    auditor's key out of every process that runs untrusted code, so no target
    can produce a signature another run's resume check would accept. The run
    directory is not the channel: the target executes in-process in the child
    and can rewrite those files on its way out, so the parent overwrites them
    from the handoff rather than reading them back. A child that handed
    nothing over leaves the run unsigned, which reads as not complete and
    re-runs.
    """
    from mareforma import signing

    if handoff is None:
        return
    receipts = handoff["receipts"]
    _write_receipts(run_dir / RECEIPTS_FILE, receipts)
    _write_receipt_envelopes(run_dir / ENVELOPES_DIR, receipts, signer)
    (run_dir / RUN_RECORD_FILE).write_text(
        json.dumps(
            signing.sign_audit_run(handoff["run_record"], signer), indent=2,
        ) + "\n",
        encoding="utf-8",
    )


def run_corpus(corpus_dir, *, out_dir, key_path) -> int:
    """Iterate a corpus of run specs, resumably. Returns the corpus exit code.

    A crashed TARGET is data, not an audit failure: its run still completes
    with partial receipts and its exit code recorded. The corpus fails (exit 1)
    only when a run produced no complete record, the audit machinery itself
    broke, and re-invocation will retry exactly those runs.
    """
    from mareforma import signing

    corpus = Path(corpus_dir)
    specs = sorted(corpus.glob("*.json"))
    if not specs:
        raise click.UsageError(f"no run specs (*.json) in {corpus}")
    # Resolved once, up front: every run directory hangs off the invocation's
    # own out dir, whatever any child target does to its working directory.
    out = Path(out_dir).resolve()
    # The key stays in this process, which never runs a target: children emit
    # unsigned records and the parent signs them. Its public half is what
    # resume trusts when deciding a run is already complete.
    signer = signing.load_private_key(_resolve_key(key_path))
    public_key = signer.public_key()

    failed: list[str] = []
    for spec_path in specs:
        run_id = spec_path.stem
        run_dir = out / run_id
        if _run_completed(run_dir, public_key):
            click.echo(f"  skip {run_id}: already complete")
            continue
        spec = _load_spec(spec_path)
        run_dir.mkdir(parents=True, exist_ok=True)
        _clear_run_outputs(run_dir)
        (run_dir / "findings.json").write_text(
            json.dumps(spec["findings"], sort_keys=True), encoding="utf-8"
        )
        rc, handoff = _execute_run(spec, run_dir)
        _sign_run_outputs(run_dir, handoff, signer)
        if _run_completed(run_dir, public_key):
            click.echo(f"  run {run_id}: target exit {rc}, receipts written")
        else:
            failed.append(run_id)
            click.echo(
                f"  run {run_id}: no complete audit record (child exit {rc})",
                err=True,
            )
    if failed:
        click.echo(
            "corpus incomplete; re-invoke to retry: " + ", ".join(failed),
            err=True,
        )
        return 1
    return 0
