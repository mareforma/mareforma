# Security Policy

Mareforma is pre-1.0 software maintained by the Mareforma team. The threat model
matters: mareforma builds the local epistemic record AI scientists rely on
for cross-agent replication, so a defect that lets an attacker forge cross-agent
convergence (making one line of evidence read as two independent ones), a
signed envelope, or a validator
enrollment is a trust-layer failure, not a cosmetic bug. Reports here get
priority.

## Supported versions

Only the latest released version receives security fixes. Pre-1.0 means
the API is still shifting and back-porting to older minors is not
sustainable for the team at this stage. If you are pinned to an older
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
needed", the team will respond with a private channel within 72
hours. Do not include exploit details in the public issue.

### What to include

- Affected version (`mareforma --version` or `pip show mareforma`)
- Reproduction: minimum code or CLI commands that demonstrate the issue
- Impact: what an attacker can do (make one line of evidence read as two,
  mutate a signed claim without detection, bypass identity gates, etc.)
- Suggested fix or mitigation, if you have one

### Response targets

These are best-effort for a small team on a pre-1.0 project, not
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
floating tag). The PyPI project is bound to the GitHub repo:
`mareforma` on PyPI can only be published from this repo's release
workflow.

### Verifying a release

After installing, you can confirm the package came from PyPI's
Trusted-Publisher path by checking the PyPI provenance attestation:

```bash
pip install mareforma                  # latest release
python -m pip show mareforma           # confirms the installed version
# Provenance: https://pypi.org/project/mareforma/#files
```

If you find a `mareforma` package on PyPI whose attestation chain does
**not** lead back to this repository, that is a supply-chain incident,
report it via the channel above.

### Typosquat reservation

Common misspellings and adjacent names are reserved on PyPI as
defensive placeholders. Installing any of them raises `ImportError`
and points the user back to the canonical `mareforma` package.

Reserved names:

- [`maraforma`](https://pypi.org/project/maraforma/)
- [`mareform`](https://pypi.org/project/mareform/)
- [`mareforma-cli`](https://pypi.org/project/mareforma-cli/)
- [`mareforma-py`](https://pypi.org/project/mareforma-py/)
- [`mareforma-agent`](https://pypi.org/project/mareforma-agent/)

Names too close to `mareforma` to register at all (PyPI rejects new
registrations with *"too similar to an existing project"*):

- `mare-forma` / `mare_forma` / `mare.forma`: all PEP-503-collapse to
  `mare-forma`, which PyPI blocks as confusable with `mareforma`. No
  defensive claim was possible; PyPI's confusable-name check provides
  the defense automatically.

If you encounter a `mareforma`-adjacent package published by anyone
other than this project, it is hostile, report it via the channel
above.

## Cryptographic core

Mareforma provides local Ed25519 signing, Sigstore-Rekor
transparency logging, validator enrollment, and SHA-256 artifact
hashing. The cryptographic core is documented in [AGENTS.md](AGENTS.md). Known
trust boundaries:

- The local signing key at `~/.config/mareforma/key` is mode `0600`.
  Anyone with read access to that file can forge claims as you.
- The first key opened against a fresh project's `graph.db`
  auto-enrolls as the root validator. This is **irrevocable**.
  Open a fresh project with the intended key.
- Sigstore-Rekor inclusion is opt-in (`rekor_url=` parameter on
  `mareforma.open`). Without it, claims are signed but not
  transparency-logged.
- What a claim is worth is derived on read, never stored: a row is served
  as verified only when the signed material on it verifies. Nothing on the
  row can be edited to raise it, because there is no such field. A direct
  writer can still remove signed material, and the read path reports that
  rather than hiding it. Local write access to the graph is the boundary,
  the same residual as the signing key above.
- A local model's lineage is the served weights' digest, resolved from
  the producer's own inference server through a scope-detached probe
  that never follows a redirect off the loopback host and accepts only
  a content-addressed digest (a compatible surface answering with a
  sentinel or a name hash yields none). It is content-addressed for an
  honest producer but self-attested against an operator who controls
  that server, the same residual as the signing key above.
- **A backup's `[completeness]` table is a witness against accident, not
  against intent.** It records what `claims.toml` holds, so a file that no
  longer holds it says so, and restore refuses. Nothing signs that table, and
  recomputing it is free, so an editor who removes rows and rewrites the table
  to match leaves a file restore accepts. Three edits are known to survive it,
  all of them requiring the table to be rewritten: deleting a verdict from the
  tail of the chain, stripping the `verdict_chain_withheld` key, and dropping
  the `[schema_census]` section. Every signature in the file is verified
  whatever the table says, so a forged claim or a stapled envelope is refused
  either way. Closing the rest needs a whole-file signature over
  `claims.toml`, and there is none.

Defects in any of these are P0 by definition. Report them, except the
`[completeness]` bound above, which is documented rather than open.

## Out of scope

- Bugs that require local code execution as the same user (mareforma
  is a library, not a sandbox).
- DoS via pathologically large inputs to `assert_claim` (use rate
  limiting at your agent layer; mareforma will validate and reject
  but cannot prevent disk fill).
- Network-level attacks against the upstreams mareforma contacts,
  Sigstore-Rekor and the optional ClawInstitute API (those are the
  upstreams' responsibility; mareforma's job is to fail closed when
  they misbehave).
