# Verify in CI

`mareforma verify` is the trust gate, and it is built to sit inside a real gate.
Its exit codes are stable, so a CI job can branch on them without parsing text:

| Code | Meaning | CI action |
|---|---|---|
| `0` | verified | pass |
| `1` | tamper or binding violation, a definite no | fail the build |
| `2` | unverifiable, the check could not run | fail, or warn (read below first) |
| `3` | usage error (bad flag or missing argument) | fail the build |

The workflow below verifies a claim on every push. Copy it to
`.github/workflows/verify.yml` and set `CLAIM_ID` to the claim your pipeline
produces (or loop over several).

```yaml
name: verify findings
on: [push, pull_request]

jobs:
  verify:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install mareforma
      # The claim id your pipeline recorded. A run that regenerates the
      # graph would assert claims first, then verify them here.
      - name: verify claim
        run: |
          : "${CLAIM_ID:?CLAIM_ID is not set}"
          mareforma verify "$CLAIM_ID"
        env:
          CLAIM_ID: ${{ vars.CLAIM_ID }}
```

`mareforma verify` exits non-zero on a tampered or unverifiable claim, so the
step fails the job with no extra scripting.

## Parse the verdict

For a gate that treats "tampered" and "unverifiable" differently, read the exit
code directly and use `--json` for the details:

```yaml
      - name: verify claim (split tamper vs unverifiable)
        run: |
          : "${CLAIM_ID:?CLAIM_ID is not set}"
          set +e
          mareforma verify "$CLAIM_ID" --json > verdict.json
          code=$?
          cat verdict.json
          if [ "$code" = "1" ]; then
            echo "::error::claim tampered or grounding binding violated"
            exit 1
          elif [ "$code" = "2" ]; then
            echo "::warning::claim unverifiable, nothing to check it against"
            exit 0
          elif [ "$code" != "0" ]; then
            echo "::error::mareforma verify usage error (exit $code)"
            exit 1
          fi
        env:
          CLAIM_ID: ${{ vars.CLAIM_ID }}
```

The `--json` payload carries the verdict and the full trust map, so a
downstream step can post the map as a comment or archive it as the run's audit
receipt.

## Before you warn on exit 2

Exit 2 says the check could not run, not that it ran and found nothing wrong.
A claim lands there when the material to reach a verdict is missing: the claim
id is not in this graph, the graph cannot be read, or the claim is signed by a
key that is not an enrolled validator on the project.

That last case is the one to think about. `verify` works from public material,
which is the project's enrolled validator pubkeys, so a signature under a key
nobody enrolled cannot be checked against anything, including a signature of 64
zero bytes. A gate that warns on 2 lets that claim through. Fail on 2 unless
you have a reason not to, and enroll the signer's key so its claims can reach a
real verdict.
