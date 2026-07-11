"""``mareforma audit``: per-finding grounding receipts for a third-party pipeline.

``diagnose`` answers one question about one run; ``audit`` answers one question
per FINDING. The target runs IN-PROCESS under the same observer diagnose uses
(a subprocess would hide it behind the observer's own seam) and never has to
import mareforma. A findings mapping supplies ``finding_id`` → cited source(s);
on exit one verdict per finding is computed from the SHARED observed evidence
against that finding's cited set, with the same bind-time citation semantics a
cooperating producer gets. Each verdict is emitted twice:

- ``receipts.jsonl`` — one plain verdict receipt per finding, directly
  consumable by ``mareforma measure`` and ``summarize_pilot``;
- ``envelopes/<n>-<finding_id>.json`` — the same record DSSE-signed with the
  auditor's key, checkable by ``mareforma verify`` from public material alone.

No self-report: nothing the target prints, writes, or declares enters a
verdict. The mapping names what each finding CLAIMS to cite; the observer alone
supplies what happened. A crashing target still yields receipts over the
partial observation, and the run record carries the target's own exit code.

The trust boundary is the shared interpreter. Running the target in-process is
what lets the observer see its reads at all, and it is also the limit: a target
written to defeat the audit can import the observer's internals and fabricate
reads, or patch the auditor itself. The receipts therefore grade a pipeline
that does not attack its auditor — the silent fallback, the unread citation —
and the signature attests the auditor's observation, not the target's honesty.

Corpus mode iterates run specs with one fresh interpreter per run — a target
cannot poison the observation of the next — and is resumable: a run is skipped
on re-invocation only when its record is complete AND verifies against the
auditor's key, so state a target planted on disk cannot mark a run done.
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
    receipt degrades to an honest OPAQUE rather than losing the finding —
    the same posture as the ``observe()`` teardown.
    """
    from mareforma.observe import GroundingVerdict, ObservedGrounding

    try:
        return scope.classify_against(cited)
    except BaseException as exc:  # noqa: BLE001
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


def verify_audit_receipt(envelope: dict, public_key) -> tuple[bool, str]:
    """Verify one signed audit receipt from public material alone.

    Two checks, both required:

    - the DSSE signature over the audit-receipt payload type;
    - the grounding→citation binding, re-checked as pure string comparison
      over the STORED normalized identifiers (never the filesystem): a
      GROUNDED record must ground on at least one source the finding cites,
      or the signature would attest an unbound GROUNDED.

    Returns ``(ok, reason)``. Structural problems raise
    :class:`mareforma.signing.InvalidEnvelopeError` so the caller can
    distinguish a malformed envelope from a wrong key.
    """
    import base64

    from mareforma.observe._binding import check_grounding_binding
    from mareforma.signing import PAYLOAD_TYPE_AUDIT_RECEIPT, verify_envelope

    if not verify_envelope(
        envelope, public_key, expected_payload_type=PAYLOAD_TYPE_AUDIT_RECEIPT
    ):
        return False, "signature does not verify against this key"
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
) -> int:
    """Run COMMAND under the observer and emit per-finding signed receipts.

    Returns the target's own exit code, like diagnose: the audit succeeding is
    signalled by the receipts on disk, not by masking what the target did.
    """
    from mareforma import signing
    from mareforma.observe import _loaders, _scope
    from mareforma.observe._audit import ensure_installed as _ensure_hook
    from mareforma.observe._citation import cited_set

    key_file = _resolve_key(key_path)
    signer = signing.load_private_key(key_file)
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
            # Bad invocation of audit itself — re-raise so click reports it.
            raise
        except SystemExit as exc:
            exit_code = _exit_code_of(exc)
        except BaseException:  # noqa: BLE001 — a target crash is expected input
            crashed = True
            exit_code = 1
            tb_text = traceback.format_exc()
    finally:
        _scope.exit(scope)

    # Verdicts and receipts are computed AFTER the scope closes, only from what
    # the observer recorded. The target's stdout, files, and exit status never
    # feed a verdict — that is the no-self-report guarantee.
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

    env_dir = out / ENVELOPES_DIR
    env_dir.mkdir(parents=True, exist_ok=True)
    with (out / RECEIPTS_FILE).open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, sort_keys=True) + "\n")
    for i, rec in enumerate(records, start=1):
        envelope = signing.sign_audit_receipt(rec, signer)
        name = f"{i:03d}-{_safe_name(rec['finding_id'])}.json"
        (env_dir / name).write_text(
            json.dumps(envelope, indent=2) + "\n", encoding="utf-8"
        )

    reads = [
        {"kind": r.kind, "identifier": r.identifier, "nonempty": r.nonempty}
        for r in scope.reads
    ]
    seams = [{"kind": s.kind, "detail": s.detail} for s in scope.seams]
    reads_seen = sum(1 for r in scope.reads if r.kind == "file")
    opens_detected = len(scope.opens)
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
    # The run record lands last, signed: its ``completed`` flag is the resume
    # key, so a run killed mid-write never reads as complete, and resume honors
    # the flag only inside an envelope the auditor's key verifies — the record
    # sits where the audited target could write.
    (out / RUN_RECORD_FILE).write_text(
        json.dumps(signing.sign_audit_run(run_record, signer), indent=2) + "\n",
        encoding="utf-8",
    )

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
    the auditor's key. A plain, unsigned, or unverifiable ``run.json`` — the
    state a hostile run A could plant in run B's directory — reads as not
    complete, and the run re-executes.
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
    except Exception:  # noqa: BLE001 — any defect in the record means re-run
        return False


def _load_spec(spec_path: Path) -> dict:
    """Load one run spec: ``{"command": [...], "findings": {...}}``.

    A malformed spec aborts the corpus with the spec named — the corpus is the
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


def _execute_run(spec: dict, run_dir: Path, key_path: Path) -> int:
    """Run one corpus spec in a fresh interpreter (the per-run isolation).

    A fresh process guarantees one target cannot poison the observation of the
    next: no shared module cache, no leaked global state, no half-crashed
    interpreter. The child is the single-run audit path, so its record and
    receipts are identical to a direct invocation, and its exit code is the
    target's own. The child's stderr (the target's traceback on a crash) is
    persisted next to the record so nothing is swallowed.
    """
    import mareforma

    cmd = [
        sys.executable, "-m", "mareforma", "audit",
        "--findings", str(run_dir / "findings.json"),
        "--out", str(run_dir),
        "--key", str(key_path),
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
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if proc.stderr:
        (run_dir / "stderr.txt").write_text(proc.stderr, encoding="utf-8")
    return proc.returncode


def run_corpus(corpus_dir, *, out_dir, key_path) -> int:
    """Iterate a corpus of run specs, resumably. Returns the corpus exit code.

    A crashed TARGET is data, not an audit failure: its run still completes
    with partial receipts and its exit code recorded. The corpus fails (exit 1)
    only when a run produced no complete record — the audit machinery itself
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
    key_file = _resolve_key(key_path)
    # The child signs each run record with this key; its public half is what
    # resume trusts when deciding a run is already complete.
    public_key = signing.load_private_key(key_file).public_key()

    failed: list[str] = []
    for spec_path in specs:
        run_id = spec_path.stem
        run_dir = out / run_id
        if _run_completed(run_dir, public_key):
            click.echo(f"  skip {run_id}: already complete")
            continue
        spec = _load_spec(spec_path)
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "findings.json").write_text(
            json.dumps(spec["findings"], sort_keys=True), encoding="utf-8"
        )
        rc = _execute_run(spec, run_dir, key_file)
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
