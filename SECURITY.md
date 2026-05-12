# Security Policy

Mareforma is pre-1.0 software maintained by a single author. The threat model
matters: mareforma builds the local epistemic record AI scientists rely on
for cross-agent replication, so a defect that lets an attacker forge a
`REPLICATED` claim, a signed envelope, or a validator enrollment is a
trust-substrate failure, not a cosmetic bug. Reports here get priority.

## Supported versions

Only the latest released version receives security fixes. Pre-1.0 means
the API is still shifting and back-porting to older minors is not
sustainable for a single maintainer. If you are pinned to an older
version, the fix is "upgrade."

| Version | Supported          |
|---------|--------------------|
| 0.3.x   | ✅ current         |
| < 0.3   | ❌ upgrade required |

## Reporting a vulnerability

**Do not open a public GitHub issue.** Use GitHub Private Vulnerability
Reporting:

  https://github.com/mareforma/mareforma/security/advisories/new

If the form is unavailable (PVR not yet enabled, GitHub outage, etc.),
the fallback is to open an empty public issue titled "Security contact
needed" — the maintainer will respond with a private channel within 72
hours. Do not include exploit details in the public issue.

### What to include

- Affected version (`mareforma --version` or `pip show mareforma`)
- Reproduction: minimum code or CLI commands that demonstrate the issue
- Impact: what an attacker can do (forge `REPLICATED`, mutate a signed
  claim without detection, bypass identity gates, etc.)
- Suggested fix or mitigation, if you have one

### Response targets

These are best-effort for a single-maintainer pre-1.0 project, not
contractual SLAs:

- Acknowledgement: target **3 business days**.
- First technical reply (triage outcome, severity, rough fix ETA):
  target **10 business days**.
- Coordinated disclosure: the advisory and the fix release land
  together. We will agree on a disclosure date with you before any
  public posting.
- Credit in the advisory (or anonymous attribution if you prefer).

We will not pursue legal action against good-faith researchers who
follow this policy.

## Supply-chain integrity

### PyPI Trusted Publishing

Releases to PyPI are published via OIDC-based GitHub Actions, not
long-lived API tokens. The workflow is in
[.github/workflows/publish.yml](.github/workflows/publish.yml) and uses
`pypa/gh-action-pypi-publish` pinned to a specific commit SHA (not a
floating tag). The PyPI project is bound to the GitHub repo —
`mareforma` on PyPI can only be published from this repo's release
workflow.

### Verifying a release

After installing, you can confirm the package came from PyPI's
Trusted-Publisher path by checking the PyPI provenance attestation:

```bash
pip install mareforma==0.3.0
python -m pip show mareforma           # confirms the installed version
# Provenance: https://pypi.org/project/mareforma/#files
```

If you find a `mareforma` package on PyPI whose attestation chain does
**not** lead back to this repository, that is a supply-chain incident —
report it via the channel above.

### Typosquat reservation

Common misspellings of the project name will be reserved on PyPI to
prevent typosquat attacks. If you encounter a `mareforma`-adjacent
package published by anyone other than this project, it is hostile;
report it.

## Cryptographic substrate

Mareforma 0.3.0 introduces local Ed25519 signing, Sigstore-Rekor
transparency logging, validator enrollment, and SHA-256 artifact
hashing. The substrate is documented in [AGENTS.md](AGENTS.md). Known
trust boundaries:

- The local signing key at `~/.config/mareforma/key` is mode `0600`.
  Anyone with read access to that file can forge claims as you.
- The first key opened against a fresh project's `graph.db`
  auto-enrolls as the root validator. This is **irrevocable in 0.3.x**.
  Open a fresh project with the intended key.
- Sigstore-Rekor inclusion is opt-in (`rekor_url=` parameter on
  `mareforma.open`). Without it, claims are signed but not
  transparency-logged.
- DOI resolution hits Crossref and DataCite. URLs are validated
  against SSRF probes (private IPs, DNS shortcuts to loopback).

Defects in any of these are P0 by definition. Report them.

## Out of scope

- Bugs that require local code execution as the same user (mareforma
  is a library, not a sandbox).
- DoS via pathologically large inputs to `assert_claim` (use rate
  limiting at your agent layer; mareforma will validate and reject
  but cannot prevent disk fill).
- Network-level attacks against Crossref, DataCite, or Sigstore
  (those are the upstreams' responsibility; mareforma's job is to
  fail closed when they misbehave).
