# Silent Failure Catch

The smallest end-to-end demonstration of what mareforma checks: whether a finding
was actually backed by the data it cites. No model, no API key, no download. One
`pip install` and one command.

```bash
pip install mareforma pandas
```

Two pipelines report the same number. One reads the data. One silently falls back
to a baked-in prior because its read broke. The printed output is identical, so a
transcript cannot tell them apart.

## The honest run

```bash
mareforma diagnose --cites data.csv -- analysis.py
```

`analysis.py` reads `data.csv`, so the observer sees the data flow and the finding
is grounded:

```text
  Grounding: GROUNDED
    the cited source was opened for reading and is non-empty (file; the open path proxies data flow by file size, it does not observe the bytes read)
```

## The silent failure

```bash
mareforma diagnose --cites data.csv -- analysis_fallback.py
```

`analysis_fallback.py` prints the same ratio, but its read failed and it fell back
to a constant. `data.csv` is cited and never opened, so the finding is ungrounded:

```text
  Grounding: UNGROUNDED
    scope fully observed; no read matching the cited source returned data
```

## Why it matters

The number a pipeline prints is not evidence that it read the data. A moved file,
a broken path, or a fallback branch produces a confident finding backed by nothing,
and the transcript looks the same as the honest run. `mareforma diagnose` observes
the reads a run actually performs and reports GROUNDED, UNGROUNDED, or OPAQUE (when
a read happens through a seam the observer cannot see) for the sources it cites.

The verdict is signed and re-checkable from public material with `mareforma verify`,
so a reader who was not present for the run can still tell a grounded finding from a
fabricated one.
