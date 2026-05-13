# Contributing to Mareforma

Mareforma is the local epistemic graph AI scientists use to record
findings with cryptographic provenance. The substrate (signing,
validators, the REPLICATED trust ladder) decides what consumers can
trust, so changes that touch it land differently from a bug fix.

Bug fixes, doc corrections, test coverage, and new examples are
welcome. For new public API surface, new CLI commands, or anything
touching the signing / validator / Rekor / state-machine paths, open
a discussion before a PR so we can think about it together before
code lands.

## Setup

```bash
git clone https://github.com/mareforma/mareforma.git
cd mareforma
uv sync               # or: python -m venv .venv && pip install -e .
pytest                # full suite must pass before any commit
```

Python ≥ 3.10. Dependencies are minimal (`click`, `tomli-w`, `tomli`,
`httpx`, `cryptography`); we add new runtime deps reluctantly.

## What to send a PR for

Yes:

- Bug fixes with a regression test that fails on `main` and passes on
  the branch
- Documentation corrections (factual fixes; broken cross-links; stale
  API references)
- Test coverage for under-tested paths
- New examples that demonstrate a real workflow (not toys)

Talk first, then PR:

- New API surface on `EpistemicGraph` or new top-level helpers
- New CLI subcommands
- Schema changes
- Anything touching the signing / validator / Rekor substrate

Probably no:

- Adding dependencies for marginal convenience
- Wrapper layers over the existing API ("Mareforma but Async", "Mareforma
  for Django", etc.) — these belong in a separate package
- Stylistic refactors without a behavioural reason

## Security

Do NOT open a public issue for security defects. See
[SECURITY.md](SECURITY.md) for the GitHub Private Vulnerability
Reporting channel and the response targets.

## Workflow

1. Branch from `main`.
2. Make the change with tests.
3. `pytest` — must be green.
4. Self-review the full diff against the checklist below.
5. Update any docs that describe the changed surface (`AGENTS.md`,
   `docs/reference/*.mdx`, `CHANGELOG.md`).
6. Open the PR.

## Self-review checklist

Before opening a PR, confirm:

- [ ] Full test suite passes
- [ ] New behaviour has a test that would fail without the fix
- [ ] No internal planning labels in shipped code / public docstrings
  / comments / commit-message subjects (see "Internal labels" below)
- [ ] If the change touches the public API, `AGENTS.md` and
  `docs/reference/api.mdx` reflect the new surface
- [ ] If the change touches the CLI, `docs/reference/cli.mdx` reflects
  it and `mareforma <command> --help` reads cleanly
- [ ] `CHANGELOG.md` has a one-line entry under "Unreleased" in the
  right section (Added / Changed / Fixed / Removed)

## Commit style

Conventional commits with a tight subject (≤ 70 chars) and a body
that explains *why*:

```
feat(P1.7): ESTABLISHED-upstream gate + seed-claim bootstrap (8 tests)

REPLICATED detection now requires at least one ESTABLISHED claim in
the converging peer's supports[]. Matches Cochrane / GRADE evidence
chains; stops replication-of-noise. Bootstrap is the seed=True
parameter on assert_claim, gated by validator enrollment.
```

Prefixes in use: `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`,
`revert:`. Pick the one that describes the **primary** intent of the
diff — a refactor that ships a bug fix is still `fix:`.

## Internal labels

Internal planning artifacts (`P0.x`, `P1.5`, `Phase A/B`, "wave",
"hardening pass", "adversarial review", subagent names) belong in
spec docs and substrate-decision artifacts only. They MUST NOT appear
in:

- Shipped code or public docstrings
- Inline comments
- Commit-message subjects (bodies are allowed when load-bearing — see
  the `feat(P1.7):` example above; the body cites the work for
  archaeology, the subject stays user-facing)
- Public documentation

These labels are noise for users and a maintenance liability when
renumbered.

## Trust-substrate changes

Changes to the signing, validator-enrollment, Rekor, or
state-machine paths require:

- Direct test of the new behavior
- Adversarial test: a regression test that demonstrates the attack
  the change is supposed to block, run against `main` to verify it
  passes pre-fix
- Documentation: update `AGENTS.md` "Signing and transparency log",
  "Validators", or "Cycle / self-loop detection" sections as
  applicable

Substrate over surface: when a defect surfaces, fix it at the root
layer (DB trigger, signing payload, state machine) rather than
patching the wrapper. The trust ladder must not be bypassable via a
public API path the wrapper happens to not expose.

## Examples

Examples in `examples/` must:

- Run end-to-end against a fresh checkout with no manual setup beyond
  what their own README documents (and the API keys their own README
  declares as required)
- Fail fast with a clear message when a required external resource
  (API key, dataset, model weights) is missing — see
  `examples/05_drug_target_provenance/run_experiment.py:_require_llm_key`
  for the pattern
- Use `seed=True` to bootstrap an ESTABLISHED upstream when the
  example demonstrates REPLICATED convergence (the
  ESTABLISHED-upstream rule means a plain string anchor or an
  unsigned upstream will not trigger REPLICATED)

## Licence

By contributing, you agree your contribution is under the project's
MIT licence.
