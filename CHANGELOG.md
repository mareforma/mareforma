# Changelog

All notable changes to this project will be documented in this file.

## [0.3.12] - 2026-08-11

An agent can now read and audit a project over the Model Context Protocol,
without being able to write to it. The rest of the release is the work that
makes that safe, and the largest part of it is that `verify` no longer blesses
what it did not check: a claim carrying no signature was reported verified, on
the default path, in every release up to and including 0.3.11.

### Added

- **A read-and-verify MCP server.** `mareforma mcp serve` serves one project
  over the Model Context Protocol behind the optional `mcp` extra. Six tools:
  `query_claims`, `search_claims`, `get_claim`, `proposition_status`,
  `trust_map`, `verify_claim`. There is no write path and there will not be one:
  a claim asserted across a transport carries no observed grounding, and the
  record exists to hold claims to the grounding they earned. The project root is
  pinned once at startup rather than taken per request, and one graph is held for
  the process lifetime.
- **Claim text reaches a model as data, not as instructions.** Every row the
  server returns is sanitized and wrapped in `<untrusted_data>` markers, and the
  server states that contract in its instructions so the reader knows what the
  markers mean. Claim text is written by whoever produced the claim, which on a
  shared record is usually another agent.
- **`proposition_status` reports `question_status`.** `consistent` or `divided`
  names the state of the question, beside `status` for the state of the answer.
  Both derive from one frame computation, so they cannot disagree about whether
  a frame is contested.
- **An example that plants a silent failure and catches it.** `examples/07` runs
  two pipelines that print the same number, and shows only the one that read its
  data reading GROUNDED.

### Changed

- **`diagnose` refuses a target that is not a Python program.** A JSON run spec
  is a valid Python dict literal, so it used to compile, exit cleanly, read
  nothing, and draw `UNGROUNDED` with `scope fully observed` beside it: a false
  accusation carrying a false completeness claim. It is now a usage error, exit
  2, before the target runs. `audit` already refused these; both commands make
  the same promise about what they watched, so both now enforce it. Anything
  runpy can run is untouched: `.py`, `.pyw`, `.pyz`, `.pyc`, `.zip`, a directory
  with a `__main__`, an extensionless script, and `-m module`.
- **The MCP page size is capped at 200 rows,** and a capped page says so with a
  `limit_capped_from` field. The graph sizes its scan ceiling from the limit it
  is handed, so an unbounded limit was an unbounded scan holding the one lock
  every other tool call waits on. A short page that does not report the cap
  reads as "that is all there is", which is a false answer about the record.
- **`mareforma mcp serve` refuses a project it cannot write to,** and says which
  path to fix. The server signs nothing and writes no claims, but SQLite opens
  the graph read-write and journals beside it even on a pure read. A readable
  project that was not writable used to pass startup and then die with "attempt
  to write a readonly database", which names no directory to fix.

- **`mareforma.open()` refuses two path mistakes** that used to create a nested
  project: a path that resolves to an existing file, and a path named
  `graph.db` or sitting inside a `.mareforma/` directory that is not already a
  project. Both raise `ValueError`. Pointing the library at the database file
  rather than the project directory is the mistake this catches.
- **`mareforma status` can read yellow on a project that meets every green
  condition.** A stored project policy whose root signature no longer verifies
  now overlays the traffic light and appends its reason. The support-level
  breakdown also prints the retired-ladder notice.
- **`JSONLDExporter` requires `classification` and `support_level` on every row
  it is handed.** It used to default them silently. First-party callers pass
  full `list_claims` rows and are unaffected; a caller passing a projected row
  to `export(claims=...)` now gets an error instead of a fabricated node.

- **`mareforma mcp serve` refuses to derive a target carrying more than 1000
  evidence lines,** configurable with `--max-evidence-lines`. Derivation cost is
  linear in that count and runs holding the one read lock every tool call
  shares, so an unbounded target stalled every other caller. The refusal names
  the count and the way to raise it. It refuses rather than answering over a
  subset, because a trust map derived over part of the evidence is a wrong
  trust map.

### Fixed

- **`verify` no longer reports a claim verified when nothing was checked.** A
  signature bundle whose `signatures` array is empty or malformed names no
  signer, so the pubkey check was skipped and the command fell through to exit
  0. A bundle that names no signer is now refused. The bundle-file path already
  refused these; the stored-claim path now agrees with it.
- **`verify` no longer reports an unsigned claim as verified.** A claim with
  no signature reached none of the signature checks, so the verdict fell
  through to `verified` and exit 0. It is now `unverifiable`, exit 2. This was
  reachable with no database access and no attacker: open a project, assert a
  claim, never run `bootstrap`. That is the path the quickstart teaches and the
  path `examples/06_ci_verify` gates CI on, so anyone following that example had
  a gate that never fired. Present in every release up to 0.3.11. The trust map
  had the right answer throughout and rendered attributability as `unsigned`;
  the verdict never read its own map.
- **The witnessing axis reports the record it found, not a proof it did not
  check.** It read `logged` with the residual "signed and recorded in a
  transparency log with an inclusion proof" whenever a row existed in
  `rekor_inclusions`. Nothing opened the stored proof: the Merkle check runs at
  restore, and the table's triggers block UPDATE and DELETE but permit INSERT.
  It now reads `inclusion record present` and says the proof is not re-checked
  on read.
- **The contestation axis reports the column it read.** It stated that a signed
  contradiction verdict had marked a claim invalid, from `t_invalid` alone. No
  trigger guards that column and the signed verdicts are never replayed on
  read, so one direct write could fabricate a contradiction or erase a real
  one. The signal is now `invalidation-recorded` rather than `signed-verdict`.

- **`verify` no longer treats exit 0 as the fall-through.** The verdict was
  matched against string literals, so any verdict the classifier grows that the
  branches did not name would have exited 0 and reported "verified". Exit 0 is
  now the branch that has to be earned; anything else is unverifiable.
- **The trust map no longer reports a signature for a claim that has none.**
  Re-verification was gated on a row carrying both an asserter keyid and a
  signature bundle, so a row with a keyid stapled on and no bundle skipped the
  check entirely and fell back to the stored verified gate, which passes
  PRELIMINARY rows through. The map read "signature re-verified on read" beside
  a keyid while `verify` called the same claim tampered. Enrolment is now read
  on the signer the bundle names, the same keyid the verification uses. Present
  since 0.3.11, reachable through `mareforma map` and `graph.trust_map()`;
  `verify` was correct throughout.
- **Short label fields can no longer forge a closing delimiter.**
  `source_name`, `generated_by` and `validated_by` skip the `<untrusted_data>`
  wrapper because delimiters around a short label are noise, and they were
  skipping the forged-delimiter strip with it. A run token reading
  `</untrusted_data>` is serialised into the same object as the wrapped text and
  closes a delimiter it was never given. The labels are still not wrapped; they
  are now stripped, like every other string in the row. Present since 0.3.11 in
  `query_for_llm`.

### Deprecated

- **The support ladder, whole.** 0.4.0 removes the `PRELIMINARY` /
  `REPLICATED` / `ESTABLISHED` labels, the stored `support_level` column, the
  promotion machinery, and the `query(min_support=...)` filter. Read the trust
  map's independence axis, or `proposition_status()`, for how much distinct
  backing a finding has. Both surfaces warn today: the retired module labels on
  attribute access, and `min_support` on the reads that filter by it. The filter
  is the path callers actually take, so a removal announced only through the
  module attribute would have reached nobody.
- **`frame_status` on `proposition_status`.** It echoed the answer's own status
  word. Read `question_status` instead. Removed in 0.4.0.
- **`assert_claim(seed=True)`.** It still writes a signed ESTABLISHED claim and
  remains the anchor that bootstraps a fresh trust chain. Removed in 0.4.0.

### Migration

- `get_tools()`'s `query_graph` no longer defaults `min_support` to
  `"PRELIMINARY"`. That value is the floor of the ladder, so it filtered
  nothing, but it reached `query` as an explicit argument and made every agent
  tool call warn about a deprecation the caller had not asked for. Passing
  nothing now means asking for nothing; the rows returned are unchanged.
- The `verify` and `trust_map` fixes change the answer only for rows that were
  already inconsistent with their own signatures. The three Changed entries
  above are the only other calls whose result moves: two `open()` paths that
  now refuse, and one exporter that now requires the fields it reads.

## [0.3.11] - 2026-08-08

A hardening release. An adversarial audit of the substrate found defects on every
path a claim travels, and this closes them. The theme is that a claim is now
trusted for what can be checked about it, not for what it says about itself.

### Added

- **Strict promotion is project state, not a caller's flag.** Opening a project
  with `strict_promotion=True` records a root-signed, one-way policy that every
  write path, the CLI and restore read. Before, the flag lived on the handle, so
  a second opener promoted the rows the strict opener had refused, and nothing
  recorded the bypass. Declaring the policy requires the root key; a keyless or
  non-root caller is refused with a typed error. The policy envelope carries a
  version, so envelopes signed before the field keep verifying.
- **A supports revision counter.** `graph.db` records a counter bumped by every
  claim insert and every supports-edge change, and the cache records the value
  it was built from. A torn commit that loses the cache half of an in-place edge
  edit is now detected and rebuilt instead of serving pre-edit lineage
  indefinitely.
- **A wrap-invariant FASTA canonical form**, and the rdkit SMILES form split from
  its no-rdkit fallback. One recorded form name used to mean different bytes on
  different hosts; the rdkit form now refuses without rdkit, the string form is
  registered under its own name, and a `chem` extra installs rdkit.
- **Retire a plan whose gates cannot run.** `graph.retire_plan(plan_id, alpha=,
  reason=)` registers a replacement carrying the retired plan's own rule at a
  usable alpha, records a signed retirement attestation, and re-gates the
  stranded evidence under the replacement. Only the alpha may move, so a repair
  cannot re-choose the side of the null once the numbers are known, and the
  retired row keeps its original values. A release before the `(0, 0.5)` bound
  could register a plan the gates cannot evaluate; its evidence lines dropped out
  of every count with no way to recover them.
- **A `post_hoc` flag on `proposition_status`.** A count resting on a plan chosen
  after the estimates were in view, either a one-shot plan or the replacement a
  retirement resolved a stranded line to, now says so. A reader can tell a
  pre-registered gate from one picked once the numbers were known.
- **Publishing gates on the test suite**, and the release job checks the packaged
  version against the tag it is publishing.
- **The trust map refuses to render when its engine version drifts from the
  package.** Every map carries the engine version that computed it, so a stored
  record names the build behind its tiers. That stamp is now bound to the package
  version, and a build where the two disagree raises `TrustMapVersionError`
  instead of emitting a map. A map whose tiers came from one engine while naming
  another is worse than no map, because the name is what a later reader checks
  the tiers against.
- **The pre-publish suite fails an artifact missing the read-side lineage
  authentication.** A wheel or sdist in `dist/` built before independence keyed
  off the signed model lineage ships a trust layer a forged `model_lineage`
  column can inflate to `CONVERGENT`. Any such artifact now fails the suite
  rather than being published.

### Breaking

- **Enrolling a key stops a project counting the claims it wrote before that
  key.** A project can start unsigned, and its claims count on a run axis keyed
  on `generated_by`. The moment a validator enrols, that axis is withdrawn:
  `generated_by` is a value the writer picks, so once a project signs, a writer
  with database access can add an unsigned claim under any run token and the gate
  cannot tell it from an honest pre-key one. That closed two measured bypasses,
  stripping the signature off an existing claim and inserting a fabricated
  unsigned tree, both of which read as convergent support with nothing disclosed.
  The cost lands on an honest operator who enrolled late: those findings stay in
  the graph and stop counting, the proposition reads `UNTESTED`, `lines_skipped`
  rises, and each drop is named `unregistered_signer_skipped` in
  `.mareforma/health.jsonl`. There is no un-enrol. Open a project with its key
  from the first write, or keep it unsigned.
- **`restore` accepts a recovery it used to refuse, and reports it.** A claim
  with no signature in a project that enrols a validator no longer fails the
  whole rebuild. One keyless write after enrolment, an unattended run or a
  collaborator without the key, used to make every later backup unrestorable,
  and nothing said so until recovery day. Such claims come back, the return value
  carries `unsigned_in_signed_mode` counting them, and a warning names the first
  few. They still count on no axis, so the read path and `restore` now describe
  the same graph. A claim carrying a `statement_cid` with no `signature_bundle`
  is still refused: it was signed once and the signature is gone, which is
  tampering rather than a keyless write.
- **A retirement resolves only behind a verified attestation.** The read path
  re-derives a plan retirement from the signed record, so a graph carrying a
  hand-planted `plan_retirements` row stops resolving stranded lines through it.
  A row whose claim does not render the plan, replacement and reason it names
  resolves nothing, and `restore` refuses it.
- **Upgrade every writer on a project together.** Once a writer opens a project
  with this release, an older release can still open it (the schema version is
  not bumped), but its promotion now trips the newer promotion guard and is left
  pending instead of landing. The older release used to swallow that failure and
  lose the promotion in silence. `mareforma status` now names the stuck count as
  `convergence_retry_pending`, and a writer on this release clears it. See the
  migration note below.
- **`mareforma verify <claim-id>` exits 2, not 0, for a signed claim whose signer
  is not an enrolled validator.** Auditor-mode verification uses public material
  only, so a signer that never enrolled cannot be authenticated: the claim's
  binding is all that could be checked, never its signature. The old exit 0 let a
  CI gate pass a 64-zero-byte signature under a keyid that was never enrolled.
  Exit 2 is the unverifiable verdict, not a failure: enrol the signer's key and
  the same claim reaches a definite one. A CI gate that treats any non-zero exit
  as tampering will now fail on this, and should route 2 to "cannot tell yet."
- **A signer that does not authenticate contributes no distinct-signer unit.** A
  finding whose claim names a signer that is not an enrolled validator, or that
  carries no signature bundle at all, counts on no axis, where before an
  unenrolled or bundle-less signer still read as a distinct source. A project that
  leaned on unenrolled participants for its independence count sees those lines
  drop and disclosed as skipped; enroll the signer to count the line again.

- **`Prediction` no longer accepts `preregistered`.** The constructor keyword is
  gone and `to_dict()` no longer carries the key; the store owns the flag. Delete
  the argument from any call site. The v0.3.10 reference documented it and the
  shipped compounding example used it, so copied code needs the edit.
- **`Prediction` refuses an alpha of 0.5 or above.** The bound narrowed from
  `(0, 1)` to `(0, 0.5)`, so a plan that constructed on the previous release now
  raises `ValueError` at construction. The gate is one-sided, and at an alpha of
  half or more it cannot separate the sides, which is the state a release before
  the bound could register and then fail to evaluate. Re-register any such plan
  at a usable alpha, or retire it with `retire_plan`, which exists for the plans
  already recorded above the bound.
- **Receipts written before this release no longer summarize.** The grounding axis
  version moved, so `summarize_receipts` raises `GroundingAxisMismatchError` on any
  receipt from v0.3.9 or v0.3.10 and `mareforma measure` exits 1 on a stored
  `receipts.jsonl`. Re-run the observation to produce receipts on the current axis.
- **`perturbation_oracle` returns different verdicts for identical inputs.** The
  effect size is now the largest single perturbation move rather than the pooled
  mean, the multiple-comparison family includes the perturbation count, and thin
  noise is reported at one repeat. An influence rate computed on v0.3.10 does not
  carry over.
- **`mareforma verify` reports a foreign-key bundle as unverifiable, not tampered.**
  A bundle signed with a key other than the local one now exits 2 with verdict
  `unverifiable`, where it exited 1 with `tampered`. A CI gate keyed on the
  documented 1-versus-2 split takes the opposite branch. Pass `--key` to verify
  against the signing key.
- **`mareforma reexec` usage errors exit 3, not 2.** A missing file or an unknown
  flag now returns the usage code rather than the could-not-re-execute code, so a
  gate that treated 2 as inconclusive-and-continue aborts on 3.
- **`claim add` writes to the nearest ancestor project.** `claim add`, `claim
  update`, `claim validate` and `validator add` now join the project above the
  current directory instead of creating a new one in place. The same command run
  in a subdirectory writes to a different graph than it did before.
- **`health.claims_contradicted` counts a different population.** It now counts
  claims marked invalid by a signed contradiction verdict, where it counted claims
  that assert a contradiction. Same name, same type, different number, so a
  dashboard or gate reading it silently changes meaning.
- **The exporters refuse a document rather than returning a partial one.** JSON-LD,
  PROV-O and RO-Crate raise `UnverifiedClaimError` naming the claims when any row
  fails verification, and `mareforma export` exits 1. They also raise
  `FileNotFoundError` on a root with no graph instead of returning an empty
  document, as does `export_bundle.build_statement`.
- **The RO-Crate `signature` property is an object, not a string.** It is now the
  parsed DSSE envelope where it was the envelope as a JSON string, so a consumer
  calling `json.loads` on it raises. A malformed bundle is omitted rather than
  passed through opaquely.
- **Agent identifiers in the RO-Crate and PROV-O exports are percent-encoded.**
  A model name carrying a colon now renders as `%3A` where it rendered as `_`, so
  documents exported by different releases do not join on the agent node.
- **`mareforma export --bundle --json` exits 1.** The combination was a documented
  no-op and is now refused as mutually exclusive, so a script passing a blanket
  `--json` to every export fails.
- **`write_bundle` refuses projects it used to sign.** A signing key that is not
  the graph's trust root, and any graph containing a claim with no asserter
  signature, are both refused with `BundleExportError`. Projects whose earliest
  claims predate their signing key are the common case.
- **`register_canonicalizer` refuses a duplicate name.** Re-registering an existing
  form now raises; pass `override=True` to replace it deliberately. A module that
  registered a form and is then reloaded fails on the second import.
- **The JSON-LD export declares `@version: 1.1`.** A JSON-LD 1.0 processor is
  required to reject the document. The context also maps `evidence` as a JSON
  literal, so it survives expansion where it was previously dropped.
- **`mareforma validator list` can exit 1.** A read-only listing now fails when the
  validator rows do not chain back to a single self-signed root, which aborts a
  `set -e` script that only meant to list them.
- **`mareforma audit --out` replaces the whole directory.** Envelopes from an
  earlier run in the same output directory are deleted before the new ones are
  written. The deleted artifacts are signed evidence, so point re-runs at a fresh
  directory to keep them.
- **`partial` is true for any non-zero exit.** In `diagnose --json`, audit receipts
  and run records, a target that exits non-zero is now reported as a truncated
  observation, where the flag was set only when the target raised.
- **`is_doi` no longer trims whitespace and requires a stricter form.** A padded or
  multi-token supports entry classifies as an external reference rather than a DOI,
  which moves it between predicates in the JSON-LD export. `graph.classify_supports`
  returns a different answer for unchanged stored data.
- **Declaring the strict-promotion policy requires the project's root key.** Opening
  with `strict_promotion=True` from a keyless or non-root caller raises
  `ProjectPolicyError`, where the flag was previously accepted on any handle. This
  is the enforcement half of the policy entry under Added.

### Changed

- `verify_claim_signatures` returns `False` for a row that carries a signer keyid
  but no signature bundle, instead of exempting it as legacy.
- A finding's verdict inputs are signed. The record it already carried, the
  proposition it addresses, the plan that gated it, the datasets behind it, its
  bearing, and a digest over its ordered estimate line set, is bound into the
  claim's signed statement, and a verdict re-derives against that copy where it is
  read. A finding written before the record carries none and signs to
  byte-identical bytes, so every claim already on disk keeps verifying.
- Altering or deleting an estimate, a contrast, or an evidence line is disclosed
  on read and refused on restore. The independence count enumerates from the
  signed finding and joins downward, so a removed row leaves the finding visible
  and its signed digest catches the gap, instead of erasing the whole finding from
  an inner join and reading a dropped refutation as consensus.
- A rewritten proposition is caught where the verdict is read, not only on
  restore. A finding's signed claim text is the rendering of the proposition it
  attests, so the live proposition row must render back to it; a mismatch drops
  the finding rather than counting evidence for a sentence the finding never made.
  Both renderings are normalised, so two agents naming one proposition with
  different capitalisation still read and restore without a false refusal.
- A failed signature is distinct from an absent one. A finding signed by an
  enrolled validator whose signature no longer verifies is a disclosed skip and a
  restore refusal; a finding that never carried a signature keeps the previous
  fallback. Before, a swapped stored pubkey changed a verdict with the skip
  counter reading zero.
- `mareforma status` cannot read green while a promoted claim fails
  re-verification, and it reports the count of promotions left pending by a
  swallowed convergence retry.
- `effect_estimates`, `contrasts`, `propositions` and `validators` are
  append-only and undeletable, and `predictions.plan_id`, the primary key, joins
  its own trigger's watch list. A direct in-place edit or delete of any of them is
  refused at the storage layer, the same guard `findings` and `evidence_lines`
  already carried.
- `claims.toml` keeps a `.prev` copy on each write. A bad rewrite, or a graph lost
  along with its only backup, still leaves one recovery point, at constant cost
  and with no per-write signature work.
- The detached bundle verifier gained the self-validation refusal restore already
  applied, so the live read, restore, and the bundle verifier agree on what a
  promotion needs; a test fails if the three rules diverge again.
- `list_claims` accepts a `limit`, so a caller can bound the verify-on-read work
  on a large graph the way `query` and `search` already do.
- `effective_independence` reports `lines_skipped`, so the independence axis does
  not read a confident number off a line set that silently lost lines.
- Omitting `generated_by` no longer exempts a submit from the pre-registration
  gate. The write resolves it to the default run token and the gate asks about
  that same token, so a project that never sets a run token raises
  `PostHocPlanError` on a `preregistered=1` plan once any finding exists under
  the default. Register the plan before the run executes, or submit under a
  fresh run token.
- A failed open of a cited source no longer floors the verdict to `OPAQUE`.
  When the observed failures account for every open of the cited path, the open
  provably returned no file object and nothing is left unexplained, so the scope
  lands `UNGROUNDED` with the failure and its exception type named in the
  reason. This reverses the 0.3.10 sentence below, which said a failed open
  still floors to `OPAQUE`. One open more than the observed failures still lands
  `OPAQUE` on the hidden-reader gap.
- A support level above `PRELIMINARY` is served as verified only when the signed
  evidence behind it verifies too: a signature-checked replication verdict naming
  the claim, or distinct-signer convergence on a shared anchor. A row whose level
  has no such backing reads `verified=False` and stays out of gated queries. The
  live read path and restore share one derivation, so they cannot drift apart.
- The project policy records when each rule was first declared, so extending it
  with a second rule leaves the first rule's window where it was. A policy
  envelope signed before this release still verifies and keeps its own date.
- The export bundle is described as what it emits, an in-toto Statement v1 in a
  DSSE envelope. Earlier releases called it SCITT-style; SCITT names a COSE
  signed statement, which this package does not produce.
- The trust map drops the topology flag no caller could act on, and
  `TRUST_MAP_VERSION` is stamped for the new shape.
- `compute_health` no longer takes the project root it never used.
- `verify_rekor_inclusion` takes the signed claim envelope as a third argument,
  so a call written against 0.3.10 raises `TypeError` until it passes the
  envelope the entry is meant to witness.
- The exceptions the API reference names are exported at the top level.
- Dead surfaces are gone: the selective-wrapping selectors, the unreachable
  telemetry writer, the unread role-attestation sidecar, an unauthenticated model
  key helper, and a root identity parameter no caller could set. The `rich`
  dependency was never imported and is dropped.
- All four empty extras are gone: `clawinstitute`, `tooluniverse`, `gemini` and
  `docs`. Every adapter runs on core dependencies, so an extra per adapter
  installed nothing and still succeeded, reading as "already satisfied" to
  anyone who ran it. Adapters are opt-in by import.

### Fixed

- `restore` reports a malformed trust-layer row instead of crashing. TOML can
  hold arrays, inline tables, local times and integers wider than SQLite takes,
  and a hand-edited backup carrying one raised `ProgrammingError`, or
  `OverflowError`, straight out of `restore`. Every trust table now rejects a
  non-scalar with `RestoreError(kind='trust_row_rejected')` naming the row and
  the column, which is the documented error surface a recovery script catches.
- Verify-on-read binds a signed claim's stored fields to its envelope. Rewritten
  text, a copied bundle, a removed bundle and a forged signer keyid are all
  refused at both gated levels instead of being served as verified, and the
  append-only triggers watch the bundle and the keyid so the rewrite is refused
  at the storage layer first.
- A single statement can no longer forge a grounding verdict or promote a claim.
- Every input a trust gate counts is re-derived where it is read. The read path
  and restore share one verifier, so a finding's plan is checked against the plan
  its own claim recorded, and a plan's rule columns against the identifier that
  keys them. A direct-SQL rewrite that would reflip a bearing drops the line and
  discloses it instead of moving the count in silence, and restore refuses the
  recovery outright.
- `findings` and `evidence_lines` are append-only and undeletable. A finding's
  plan and bearing, and a line's data and model lineage, gate every count above
  them, so an in-place edit is refused at the storage layer.
- `retire_plan` refuses two repairs that used to succeed quietly: one that
  recovers no evidence line at all, and one whose replacement plan already
  exists, which would drop the disclosure the recovered count depends on.
- A one-shot finding's synthesised plan is held to the same `(0, 0.5)` alpha
  bound as a registered one, so no write path mints a plan the gates cannot
  evaluate.
- Restore verifies what it replays: a second self-signed root in `claims.toml`
  cannot void the trust layer, an unsigned finding edge cannot replay as an
  independent line, the durable promotion gates are re-applied to rebuilt rows,
  and an incomplete Rekor sidecar entry is refused.
- Independence counts only what can be authenticated. Withdrawn and invalidated
  claims stop contributing, a null lineage column falls back to the signed record
  rather than skipping re-authentication, and a defaulted validator type no
  longer certifies an independent line.
- The observer fails closed. The HTTP transport allowlist refuses an unknown
  transport, a nested scope replays its evidence into its parent instead of
  blinding it, reads are recorded under absolute urls with credentials stripped,
  and an unreadable response floors to opaque.
- A tool adapter signs only what it observed. Environment facts it did not watch
  are refused, and callee cache metadata no longer writes a supports edge.
- Read-only commands stay read-only: `verify` and `map` no longer enroll the
  caller's key as the root validator, and an audited child process cannot reach
  the corpus signing key.
- Every read surface verifies a high-trust row, including `list_claims`, so the
  JSON-LD, PROV-O and RO-Crate exporters and `mareforma claim list` no longer
  publish a REPLICATED or ESTABLISHED row whose signature does not re-verify.
- An enrolled validator is bound to its own key: a row whose keyid is not the
  public key id of its own `pubkey_pem` is refused, so one key cannot hold two
  identities on the independence axis.
- Two paths that grounded a cited source without reading it are closed: an
  in-process transport that answers without a socket, and a database connection
  that creates the file it claims to read. The boundary stays the process rather
  than the caller's honesty inside it, because the scope recorder is reachable
  from the observer's own exports, so code in the same interpreter can still have
  a verdict minted for a read that did not happen.
- An audit receipt attests what the observer recorded, not what the run directory
  held afterwards, so an audited target cannot rewrite its own verdict into the
  auditor's signature.
- Transactions end where they start. A refused delete releases its transaction
  instead of discarding later writes, and the Rekor sidecar write stays inside
  the caller's transaction.
- Reads are bounded and serialized: a read truncated by the scan ceiling is
  refused rather than served short, unbounded IN-lists bind as one json array,
  the acyclicity walk runs in sql, and reads and close run under the graph lock.
- An export bundle is refused unless the trust root signed it, bundle
  verification checks the validation envelope's signer rather than its label, and
  an exported artifact lands through an atomic replace so a failed write cannot
  destroy the previous one.
- A Rekor inclusion proof is bound to the claim it witnesses.
  `verify_rekor_inclusion` refuses a proof whose proven body records another
  entry's payload hash or signature, so a valid proof over some other claim no
  longer verifies this one.
- The insecure-Rekor flag reaches submit and fetch instead of being read once and
  dropped, and the log-pubkey pin is written durably and checked when it is read.
- `open()` resolves the project root once, so changing directory under a live
  graph cannot split the signed corpus across two trees.
- The documentation describes the shipped behavior. Every `open()` keyword, the
  full oracle and `assert_claim` signatures, six undocumented graph methods, the
  refusals `validate()` can raise, the restore signature and its error kinds, and
  six CLI commands that had no section are now in the reference, and the
  quickstart follows a path that reaches the established level.
- The example walkthroughs print what a reader's own run prints. Two transcripts
  carried a claim id and a validation date from the run that recorded them,
  values no other run reproduces, and both are now elided the way the rest of
  the transcripts already were.
- The shipped sdist runs its own suite: the guards that read repository files
  skip there instead of failing where those files do not exist.

### Migration

Upgrade every writer on a project to this release together. The schema version is
not bumped, so a project stays openable in both directions, and a graph this
release writes still reads under the older one. What changes is promotion: a
convergence check run by the older release now trips the newer promotion guard and
leaves the claim below the level its evidence earns, flagged for retry rather than
lost. The older release swallowed that failure silently. Run `mareforma status` to
see the pending count (`convergence_retry_pending`); a writer on this release
re-runs the check and clears it. The retry has to come from this release: the
older one reports the count in `health()` but its own `refresh_convergence()`
promotes nothing and leaves the count where it was.

A human validation is the harder case, and it is not queued. `validate()` called
from the older release on an upgraded project raises
`mareforma:append_only:promotion_unmarked`, the transaction rolls back whole, and
the claim keeps its level with `validated_by` still empty and nothing added to
the retry count. The validation is lost rather than deferred, and the message the
older release prints is the internal sentinel with no remedy attached. Re-run the
validation from this release. No data migration, no
reindex, and no re-sign: the only action is to move the remaining writers onto
this release.

## [0.3.10] - 2026-07-16

### Added

- **Model and method lineage on the evidence line.** The observer records which
  model and method authored a finding, computed from the request the producer
  actually sent. Tiered like `data_id`: COMPUTED (a body-parse at the socket seam
  to a recognized provider host), PROXY (a producer declaration), UNVERIFIABLE (a
  fine-tune, alias, or wrapper whose base is not declarable). Corroboration counts
  distinct models, not distinct signers alone, and a human check counts as the
  strongest independent source. Persisted additively on `evidence_lines`.
- **Local models earn content-addressed lineage.** A call to a local inference
  server (Ollama) is COMPUTED via a `weights-digest` attestor: the observer
  resolves the served weights' sha256 from the running server and keys model
  distinctness on the digest, so two local models are told apart by their weights,
  not a self-chosen name. The `attestor` field (`provider-host`, `weights-digest`,
  `declared`) records how each identity was established.
- **Open-weight model lineage.** An open model roots to its family release
  (`llama-3.1`, `qwen-2.5`, `deepseek-v3`), so one release served under
  provider-specific names (a namespaced hub id, a suffixed serving alias)
  collapses to one model on the independence axis, and naming variance can only
  under-claim distinctness, never mint a fake independent line. The recognized
  provider hosts extend to the open-weight inference providers (Groq, Together,
  Fireworks, Mistral, DeepSeek), matched on their registered domains; a router
  stays unrecognized because its host does not pin which upstream served the
  weights.
- **Wider execution observation.** Grounding and lineage now fire on the idioms
  real pipelines use: reads through `io.open` (pathlib `open`/`read_text`/
  `read_bytes`, zipfile), model calls at `httpx` `Client`/`AsyncClient.send` and
  the `aiohttp` request seam (the paths the provider SDKs and litellm take), and a
  cited read through polars is GROUNDED. A grounded read and a computed lineage are
  gated on a successful response, so an error body never grounds a cited URL and a
  failed call never mints a model. A duckdb query, whose read path lives in the SQL
  beyond the observer's view, floors a cited read to OPAQUE rather than a false
  UNGROUNDED.
- **`mareforma audit`.** A post-hoc auditor that signs one grounding receipt per
  finding, verifiable independently of the producer, with corpus resume.
- **Re-execution faithfulness proxy.** A `FaithfulnessVerdict` placed on the trust
  map that re-runs a recorded step and reports whether the result reproduces.
- **Effective independence and the prevalence pilot.** The independence arm reports
  the effective-independence distribution and the same-model-collapse rate, with a
  causal oracle prose path, kill-switch fixtures, and a slim pilot harness that
  states its OPAQUE-coverage bound instead of reading a blind run as a prevalence.
- **Pre-registration gate.** A plan registered after the run's first execution is
  refused, so a plan cannot be back-dated to a run it did not precede.

### Deprecated

- **The public `REPLICATED` and `ESTABLISHED` labels.** They read as settled
  conclusions the substrate does not compute. The internal DB support levels are
  unchanged; only the public module attributes warn.

### Removed

- **The GRADE evidence vector** (`EvidenceVector`, `EvidenceVectorError`,
  `VALID_STUDY_DESIGNS`), **literature ingest** (the `ingest`, `ask`, and
  `narrative` commands and their exporters), **DOI network resolution** (the
  regex-only `is_doi` format helper stays), and **activity hooks**. These were
  surface the core does not need; their removal is asserted by tests.

### Changed

- **A failed open of a cited source is named as such.** A wrapped read-mode
  open that raises is recorded with its exception type, and when those
  failures account for every unexplained open of the cited paths the
  coverage-gap seam says the open failed and names the type, instead of
  blaming an uninstrumented reader. One open more than the observed failures
  keeps the hidden-reader message. The verdict is unchanged: a failed open
  still floors to OPAQUE, never UNGROUNDED, because a failed wrapped open
  does not rule out a hidden successful one.
- **The top status label `CORROBORATED` is renamed `CONVERGENT`.** The state and
  its rule are unchanged: two or more independent-lineage supporting lines, none
  refuting. The word changes because it over-claimed. Distinct-model is
  necessary, not sufficient, for independence: a kill-switch measured
  distinct-provider model pairs as error-correlated as any pair (rho 0.484 vs
  0.485). `CONVERGENT` states the structural fact that lineage-distinct lines
  converge and names cross-model error correlation as the residual, rather than
  reading as a corroboration or independence verdict. Reading
  `Status("CORROBORATED")` or `Status.CORROBORATED` still resolves to
  `Status.CONVERGENT` this release and emits a `DeprecationWarning`; a future
  release removes the alias. Status is recomputed on read and never stored, so
  there is no stored-value migration. The policy stamp moves from
  `status_policy@v3` to `status_policy@v4`.

### Security

- **Model lineage is bound into the signed finding and authenticated on read.**
  The lineage that drives independence was a denormalized column a writer could
  forge; it is now part of the signed payload and the independence read checks it,
  so a forged column no longer moves the count. Legacy unbound findings read as
  UNVERIFIABLE rather than being rejected.
- **A producer-controlled transport is classified PROXY, not COMPUTED**, and an
  agreeing producer declaration can no longer pull a seam-verified COMPUTED model
  down. Absent model lineage reads as UNVERIFIABLE, never as a confident
  independent line.
- **The local weights probe accepts only content-addressed digests.** Other local
  servers ship Ollama-compatible surfaces whose `digest` is a constant sentinel or
  the sha256 of the model name; either would have minted a COMPUTED weights-digest
  lineage off a fabricated identity, and the fake digest would have scored as a
  distinct model and forged cross-model independence. The probe now requires a
  well-formed sha256 payload that is not the hash of the model's own name, and
  fails closed to no digest, so the call stays UNVERIFIABLE.
- **Single-operator topology is named on the independence axis.** When every
  signer traces to one trust root the count says so and rests on distinct-model or
  human lines within that domain.
- **Replication promotion is gated on grounding and a distinct signer**, and the
  invalidation gate is re-asserted on the validation write, so an invalidated or
  ungrounded claim cannot promote past a concurrent contradiction.
- **Rekor URL validation runs at submit entry** like the fetch paths, and rejects
  non-decimal and IPv6-embedded SSRF host forms.
- **Key rotation writes durably.** The rotated key is written to an unpredictable
  temp, fsynced before the rename, so a crash cannot leave a zero-byte key and two
  rotations cannot clobber a shared temp.

### Fixed

- Backup no longer crashes on a null verdict or Rekor field, restore round-trips
  the finding evidence tree and validates section shapes, and a corrupt graph
  opens as a typed `DatabaseError`.
- The supports cache is maintained when a claim's supports are edited or the claim
  is deleted, so `query_provenance` stops serving pre-edit or dangling lineage.
- Graph mutations serialize across threads, `update_claim` applies the same write
  invariants as `add_claim`, and an established seed stays out of its own
  convergence promotion.
- A read grounds only on a 2xx response, so an error body or redirect never grounds
  a cited URL or mints lineage.
- `mareforma verify` reads a signed audit `run.json` as unverifiable (trusted via
  resume), not as tampered.
- CLI: `export` and `activity` discover the enclosing project from a subdirectory,
  a wrong-key audit receipt reads as unverifiable, a reexec map lookup failure
  exits as a usage error, and the no-project hint points at a real command.
- The CLI reference documents `audit` and `reexec`, and a drift guard fails when
  any shipped command leaves the reference. `TRUST_MAP_VERSION` moves to `v0.3.10`.

### Performance

- The findings table carries a `claim_id` index the write path and trust map need,
  convergence peer lookup no longer full-scans on every insert, per-frame
  independence counts and per-scope reads are memoized, and the local Ollama
  weights-digest probe is cached per server and model.

### Packaging

- The `setuptools` build floor is raised for the string license metadata, and the
  `test-heavy` extra is synced with the loaders it exercises.

## [0.3.9] - 2026-07-08

Three passes that make trust honest and legible. The grounding verdict now binds to
the data a finding actually read. The trust a claim carries is read as a structured
map instead of one word, with three commands that make it legible to a stranger in a
minute. And the open issue burn-down closes, with the core-derived classification
engine removed. Additive on the schema.

### Added

- **Verdict-to-citation binding.** An observed GROUNDED verdict now proves it
  attests the finding's own data. The verdict carries the sources a read was
  actually observed for into the signed record (grounding axis `v0.3.9`), and that
  set is cross-checked against the finding's citation at write time and re-checked
  on read. A producer who names a dataset in `observe(cites=...)` but reads only an
  incidental decoy no longer earns GROUNDED for it: a disjoint GROUNDED downgrades to
  OPAQUE with a signed reason and a `grounding_citation_mismatch` health event, or
  raises in strict mode (`grounding_strict=True`). Evidence lines gain `data_source=`
  so the honest workflow, cite a path and content-address the data_id, binds. This
  makes the v0.3.8 promise, "this finding read the data it claims to," true.
- **The per-finding trust map.** `graph.trust_map(claim)` and `mareforma map
  <claim>` place every trust property (attributability, provenance, grounding,
  methodological validity, leakage, independence, contestation, standing,
  trust-root, witnessing) at its tier (`COMPUTED` / `PROXIED` / `DEFERRED`) with the
  residual named. A property Mareforma cannot observe is stated, never inferred.
  `--json` emits a canonicalizable record; `--html` writes one self-contained page.
- **`mareforma verify <target>`.** The audit receipt at the moment of the check. It
  re-verifies signatures, the grounding-to-citation binding, and support level, then
  prints the trust map. It detects its target by shape (claim id, signed bundle, or
  export directory) and verifies a claim from public material alone. Exit codes are
  stable for CI: `0` verified, `1` tamper or binding violation, `2` unverifiable, `3`
  usage error, so a bad flag never masquerades as a verdict. `--json` emits the
  verdict for a gate to parse.
- **`mareforma diagnose -- <cmd>`.** Runs a Python target in-process under the
  grounding observer and reports the reads, seams, and coverage it saw. With
  `--cites` it also computes the grounding verdict; without one it reports
  observation only and never guesses a citation. A crashing target still prints its
  partial observation and exits with the target's own code.
- **`mareforma observe --doctor` and `mareforma measure`.** `--doctor` self-reports
  which loaders are wrapped and which seams force OPAQUE in the current environment.
  `measure` aggregates a run's verdicts into the reported split, OPAQUE bucketed by
  seam kind. `--redact-home` rewrites `$HOME` in emitted artifacts, never in a signed
  receipt.
- **Keep-alive HTTP and C-extension coverage.** Pooled `requests.Session`,
  `httpx.Client` / `AsyncClient`, and `aiohttp` sessions, and the C-runtime readers
  (`h5py`, `pyarrow`, `netCDF4`), are wrapped only-if-imported, so a retrieval or an
  HDF5 / netCDF / Arrow read reaches GROUNDED instead of a false UNGROUNDED. A loader
  imported inside an open scope is wrapped too.
- **The independence axis.** The trust map reports independence separately from the
  support ladder, marking it `UNVERIFIABLE` whenever fewer than two trust roots are
  enrolled, the honest reading when one operator could mint every key.
- **`strict_promotion` option.** `mareforma.open(strict_promotion=True)` gates
  REPLICATED on non-NULL data on both sides of a converging pair. Off by default; it
  only ever adds the requirement.
- **Declared metric reducer for the causal oracle.** `declared_reducer(...)` names
  the reduction a prose finding needs and records whether it reinserts a model into
  the ground truth. (The oracle itself shipped in v0.3.8.)

### Changed

- **UNGROUNDED means genuine absence, not blindness.** A seam blocks the UNGROUNDED
  verdict only when it could have hidden a read of a citation kind actually present.
  A socket seam no longer forces OPAQUE on a file-cited finding, while URL and
  content-address citations stay blocked. Thread, subprocess, and coverage-gap seams
  block everything; unknown kinds fail closed.
- **REPLICATED is a convergence marker, not a claim of independence.** The README
  and trust docs stop leading with it: signing keys are operator-mintable, so
  distinct signatures are a weak prior. Independence lives on its own axis.
- **The claim-recording agent tool is renamed `record_claim`.** It was
  `assert_finding`, which shadowed the `EpistemicGraph.assert_finding` method. A
  deprecated `assert_finding` alias is available for one release via
  `get_tools(include_deprecated_aliases=True)` and warns on use. (#51)
- **The cycle check runs as one recursive query** instead of one per ancestor, and
  an oversized reachable graph raises a distinct `GraphTooLargeError` rather than a
  false "cycle." (#33)
- **RO-Crate and PROV-O exports align with the profile shapes.** The RO-Crate root
  entity carries a license and a non-null `datePublished` and separates data entities
  (`hasPart`) from provenance actions (`mentions`); PROV-O labels use `rdfs:label`.
  The tests check shape and label vocabulary, not full validator conformance. (#29,
  #48)
- **`mareforma verify` subsumes the old bundle-path command.** The prior `mareforma
  verify <bundle>` invocation keeps working as the file-detection case; a missing
  local key exits `2` (unverifiable) rather than `1`.
- **The per-connection validator chain-verification cache now persists**, so a
  repeated enrollment check skips the chain walk. (#50)

### Fixed

- **Re-ingesting a paper no longer leaves orphaned full-text-search rows**, even when
  the re-extraction is empty. The ingest path deletes a document's prior claims by
  document id before inserting the fresh set, so the FTS delete trigger fires. (#31)
- **Multi-role signatures are re-verified on the live read path**, so a forged role
  attestation is caught on read and by `mareforma verify`, not only at restore.
- **Deleting a signed claim raises the typed `SignedClaimImmutableError`** for both
  `delete_claim` and `delete_claims_by_generated_by`. (#42)
- **The API-version probe rejects a neighbouring major.** The clawinstitute check
  matched "v10" and "v1beta2" against "v1"; it now matches the exact major or a minor
  under it. (#49)
- **`independence_counts` stops full-scanning `effect_estimates`**, via an index on
  `contrasts(line_id)`. (#32)
- **The sdist ships a complete, runnable test suite** (conftest, shared helpers, and
  every test subpackage). (#45)
- **The Dependabot config no longer advertises a lockfile the repo does not commit.**
  (#55)
- **The reference docs match the code.** The status-policy stamp, the `export
  --format` choices, and the default-format PROV-O scope note are corrected. (#44,
  #54)

### Removed

- **The core-derived classification engine.** `mareforma.derivation` (the keyword and
  log-template classifier), its `[derivation]` install extra, and the `tree_sitter`
  dependencies are gone; execution-observed grounding computes the same signal from
  observed reads. (#19, #30, #38)
- **The dead `[git]` install extra** and the unused `gitpython` dev dependency. (#56)

## [0.3.8] - 2026-07-06

Execution-observed grounding, and a trust-layer hardening pass across every path a
finding travels: read, convergence, backup, restore, and the export bundle. The
substrate now proves more of what it shows. A bundle's claims are checked against
their own asserters and earned support level, restore rebuilds trust state from
verifiable material rather than agent-set flags, and a project can require
transparency-log witnessing before its findings converge. Additive on the schema
(one new nullable column and one additive table, no migration).

### Added

- **Execution-observed grounding.** An optional observer watches a finding's
  computation and records whether real data actually flowed into it (GROUNDED /
  UNGROUNDED / OPAQUE). The verdict is bound into the signed claim and re-derived on
  restore, so "this finding read the data it claims to" becomes part of what the
  signature attests.
- **Required Rekor witnessing.** `graph.require_rekor_witnessing()` writes a
  root-signed, one-way project policy; `restore(enforce_rekor_policy=True)` then
  reconstructs convergence-eligibility only for claims carrying a verified,
  claim-bound transparency-log inclusion proof.
- **Per-claim verification in the export bundle.** `verify_bundle` checks each
  claim's own asserter signature bound to its presented content, the enrolled
  validator chain to a single root (which must be the bundle signer), and the
  displayed support level (ESTABLISHED against a validator-signed validation
  envelope, REPLICATED against distinct-signer corroboration). The bundle signs over
  the DSSE PAE encoding, so it verifies with standard DSSE tooling. Editorial status
  (retracted / contested) carries no signature and stays exporter-attested.

### Changed

- **Search re-verifies high-trust rows.** `search()` applies the same
  verify-on-read and trust-domain disclosure as `query()`, and both read paths sort
  the table once instead of per batch.
- **Convergence respects contradiction verdicts.** A claim invalidated by a signed
  contradiction no longer rides an honest peer into REPLICATED, and a transiently
  failed convergence re-check is retained and retried rather than lost.
- **Durable, coherent backup.** `claims.toml` is written atomically, so a crash
  cannot truncate the sole recovery artifact; the backup and the Rekor commit run
  only when the call owns the transaction; and many mutations can share one rewrite
  via `graph.defer_backup()` / `graph.backup()`.
- **Restore reconstructs the trust layer.** Support level, invalidation, signer
  identity, and witnessed state round-trip so promotion outcomes match the original.
  Witnessed state is derived from the transparency-log inclusion sidecar, not an
  unsigned field.
- **Honest machine schema.** `schema()` describes the distinct-signer convergence
  rule an agent must follow; a test pins the transition text so it cannot drift.

### Fixed

- The publish workflow fails when the release tag and built version disagree, so a
  forgotten version bump cannot ship a tagged release with no package.
- Read-only CLI commands discover an existing project by walking up from the working
  directory and refuse when none exists, instead of creating a stray `graph.db`.
- Exporting to a path outside the project reports success, not a false failure.
- Root auto-enrollment is announced on stderr, not only a filterable warning.
- A naive DOI cache timestamp is treated as UTC rather than crashing the write path.
- `mareforma ingest --help` now documents the required `TITLE` / `DOI` / `CLAIMS`
  layout, and a structured-mode file that yields no claims exits non-zero and
  names the expected layout, instead of a silent success on an unparseable file.

## [0.3.7] - 2026-06-30

Verified independence. REPLICATED now keys on the signing key, not a free-text
`generated_by` string: two claims converge only when distinct keys sign them, so
a single operator can no longer manufacture REPLICATED with a string trick. And
high-trust rows are re-verified on read, so a forged REPLICATED or ESTABLISHED row
in a shared `graph.db` is caught at query time instead of trusted. Additive on the
schema (one new nullable column, no migration); existing REPLICATED rows are
grandfathered, not downgraded.

### Added

- **Verify on read.** `get_claim`, `query_claims`, and `query_provenance` re-verify
  a REPLICATED or ESTABLISHED row's signatures before serving it. `query_*` excludes
  a row whose signature does not verify; `get_claim` returns it flagged
  `verified=False`. Neither raises: a verification miss degrades the read, it does
  not crash it. The check binds the signed payload to the row, so a genuine envelope
  copied onto a different row is rejected.
- **`single_trust_domain` disclosure.** Query results and the exported bundle carry,
  on each ESTABLISHED row, whether every validator traces to one root of trust. It
  discloses trust-domain concentration so a consumer can discount intra-operator
  ESTABLISHED. It is a disclosure, not a Sybil guard.
- **`asserter_keyid` column.** Denormalized from the signature bundle onto each
  claim, indexed, with the envelope authoritative. The REPLICATED promotion query
  and the trust-layer independence count both read it. Added to existing graphs by
  `ALTER TABLE ADD COLUMN` on first open; legacy rows stay NULL.

### Changed

- **REPLICATED keys on the signing key, not `generated_by`.** Two claims sharing an
  ESTABLISHED upstream converge only when distinct keys sign them (distinct
  `asserter_keyid`). `generated_by` becomes a display label and plays no part in
  promotion. An unsigned claim, or two claims signed by the same key, no longer
  reach REPLICATED. Existing REPLICATED rows promoted under the old rule are
  grandfathered on first open with a durable `legacy_promotion` health event.
  Distinct keys prove the asserters are cryptographically separate, not that their
  data is independent, so REPLICATED reads as a convergence signal, not proof.
- **Status independence keys on the signing key.** `independent_support` and
  `independent_refute` count distinct `asserter_keyid`, the same axis the promotion
  path uses, with a `data_id` guard. Unsigned evidence lines fall back to the retired
  distinct-`generated_by` run axis, so their counts are preserved. The policy version
  moves from `status_policy@v2` to `status_policy@v3`; Status is recomputed on read,
  no migration.
- **`artifact_hash` is a collapse check, not a match requirement.** Two converging
  peers with equal data collapse to one independent line, so an equal-hash pair does
  not promote on data alone; distinct content-addressed data counts as two lines.
  Absent data never blocks: distinct signers alone promote. This reverses the prior
  rule, where matching hashes were required for convergence.
- **`data_id` is content-addressed.** When a finding supplies the dataset bytes,
  mareforma hashes them into `data_id` (`sha256:` prefix), so equal data collapses
  and an agent cannot fabricate distinctness with a made-up string. A reference-only
  `data_id` stays an agent-attested fallback.

## [0.3.6] - 2026-06-17

The multi-line evidence tree. A finding can now carry several evidence lines
instead of one, and Status counts independence by distinct run rather than
distinct dataset. Additive on the schema (stays at v1, no migration). The
single-line API is unchanged, and so are its Status outcomes for findings from
distinct runs.

### Added

- **Multi-line findings.** `submit_finding` and `assert_finding` take a
  `lines=[EvidenceLine, ...]` argument in place of the single `estimate` +
  `data_id` pair. One finding then records several datasets or arms under one
  proposition and prediction. The two forms are mutually exclusive. A finding
  with no lines raises `ValueError`; a finding where any line fails the gate
  rolls back whole, leaving no claim and no rows. A finding's identity is its
  full `data_id` set: re-submitting the same set is idempotent, a partial overlap
  or a different plan raises `FindingPlanForkError`. The return dict gains
  `bearings`, the per-line bearing list (a single-line finding carries one entry).
- **Per-line bearing.** Each evidence line's bearing is recomputed on read from
  its stored estimate and the finding's prediction. A multi-line finding whose
  lines disagree reads as CONTESTED, not as one finding-level label.

### Changed

- **Status independence is now run-distinct.** `independent_support` and
  `independent_refute` count distinct `generated_by` (run) with a `data_id`
  guard, in place of the distinct-`data_id` count. One run contributes at most
  one support and one refute, so a single run cannot reach CORROBORATED by
  submitting several datasets and a multi-line finding cannot self-corroborate.
  Corroboration accrues across runs. Two findings on one proposition from the
  *same* run that previously read CORROBORATED now read PRELIMINARY; findings
  from distinct runs are unaffected. The policy version moves from
  `status_policy@v1` to `status_policy@v2`. Status is recomputed on read, so
  stored findings pick up the new count without a migration.
- **`generated_by` is checked at the finding write.** A blank or whitespace-only
  run token raises `ValueError`; a missing or default token writes but emits a
  health event, because distinct-run independence needs a real per-run identity.

## [0.3.5] - 2026-06-15

The pre-registration split. v0.3.4 shipped the trust layer as a single
`assert_finding` call. v0.3.5 separates the two earned steps of the
hypothetico-deductive method: register the decision rule *before* the numbers
are seen, then submit the outcome against it. This makes the plan → finding edge
cryptographic. Additive only: no schema migration, schema stays at v1, and
`assert_finding` / `assert_claim` keep working unchanged.

### Added

- **`EpistemicGraph.register_plan(proposition, prediction)`**, pre-register a
  decision rule against a proposition. Registers the proposition, writes the
  append-only `predictions` row with `preregistered=1`, and writes its own
  signed claim (the **plan attestation**) via the normal `assert_claim` path
  under idempotency key `plan:{plan_id}`. The plan claim is Rekor-anchorable
  like any other claim (no special-casing). Idempotent: re-registering the same
  prediction is a no-op on both the claim and the row. Returns the
  content-addressed `plan_id`.
- **`EpistemicGraph.submit_finding(proposition, prediction, estimate, *, data_id,
  ...)`**, submit a finding against an already-registered plan. Computes the
  `plan_id` and requires the plan to exist (else `NoRegisteredPlanError`),
  computes the Bearing, and writes the finding's signed claim whose `supports[]`
  cites the **plan attestation's claim_id**, so the plan → finding edge is
  signed, not merely denormalised metadata. **Fork-guard:** a finding already
  recorded for `(content_id, data_id)` but under a *different* `plan_id` raises
  `FindingPlanForkError` rather than silently returning the prior bearing. The
  authoritative existence check and the row writes run in one transaction (no
  TOCTOU).
- **`mareforma.trust` errors** `NoRegisteredPlanError` and `FindingPlanForkError`
  (both subclass `TrustError`).
- **`mareforma.trust` gates[] chain**, `Gate`, `gates_for(prediction)`, and
  `evaluate_gates(estimate, gates)` re-express the DecisionRule as an ordered
  short-circuit chain over the existing prediction columns. The single binary
  gate shipped in v0.3.4 is the one-element chain; a one-element chain produces a
  Bearing identical to `compute_bearing` for superiority and for equivalence/TOST
  (parity-tested). A pure Python structure: no new schema column, no migration.

### Changed

- **`assert_finding` now composes `register_plan` + `submit_finding`
  internally.** It synthesises a plan flagged `preregistered=0` (so a genuine
  up-front pre-registration stays distinguishable from a one-shot) and delegates.
  Its return shape, idempotency on `(content_id, data_id)`, atomicity, and
  derived Status are all preserved unchanged.
- **The previously-unused `predictions.preregistered` column is now set:** `1`
  by `register_plan`, `0` by the plan `assert_finding` synthesises.
- **`plan_id` is content-addressed over the prediction's identity fields,
  excluding `preregistered`.** The flag is provenance about how a row was
  created, not part of the decision rule's identity, so a finding binds to a
  pre-registered plan regardless of the flag.
- **`register_plan` and `submit_finding` emit to the health/activity log**
  (`.mareforma/health.jsonl`), alongside the existing operational signals.

### Notes

- **Float determinism.** The gate compares floats (`p_value`, CI bounds,
  `alpha`), but that is not a cross-host hazard: each IEEE-754 primitive op is a
  single correctly-rounded result. Divergence risk comes from accumulated
  computation (the pooling a meta-analysis does), which v0.3.5 does not perform.
  The `abs(ci_level - expected_level) > 1e-9` check is a float-equality guard on
  caller input, not a status-driving reduction: it never softens or flips a
  bearing.
- Single-line evidence model only. Multi-line evidence trees, per-line bearing,
  pooling / I2 / tau2, GRADE certainty, and the deferred gate regimes
  (multiplicity, magnitude bands, non-inferiority, dose-response, Bayesian) are
  not in this release.

## [0.3.4] - 2026-06-11

The trust layer: structured findings with a computed bearing and a derived
status. A free-text claim becomes a content-addressed proposition bound to a
pre-registered prediction; the direction of evidence is computed from the
registered rule and the result, never self-declared; and a count over
independent data derives the status. Additive only: six new tables, schema
stays at v1, and every finding still rides a signed claim as its attestation.

### Added

- **`mareforma.trust`**: the trust layer.
  - `Proposition`: a content-addressed, falsifiable claim. `content_id` is
    the answer (subject, relation, object, scope, direction, magnitude);
    `frame_id` is the question (direction and magnitude dropped). The same
    truth conditions collapse to one node across hosts and languages
    (NFC + casefold + whitespace, RFC 8785 bytes). `contradicts` is decidable:
    same frame, contrary directions.
  - `Prediction`: a pre-registered decision rule, bound to a proposition
    before the numbers are seen. Superiority (a predicted side of the null)
    and equivalence (TOST) gates.
  - `EffectEstimate` / `EvidenceLine` / `Contrast`: the one-line evidence
    tree, with metafor-named effect fields. Rejects inconsistent input
    (non-finite values, a confidence interval that does not bracket the
    estimate, an out-of-range p-value).
  - `compute_bearing`: the gate. Returns supports / refutes / neutral,
    computed from the prediction and the estimate rather than declared.
  - `compute_status` / `compute_frame_status`: the count-based status
    (`UNTESTED`, `PRELIMINARY`, `CORROBORATED`, `REFUTED`, `CONTESTED`) over
    independent data, plus the frame-level contest. Versioned as a policy
    (`status_policy@v1`) over stored counts, not baked into the schema.
- **`EpistemicGraph` trust methods**: `register_proposition`,
  `assert_finding`, `proposition_status`, `get_proposition`, `query_frame`.
  `assert_finding` validates the input, computes the bearing, writes a signed
  claim, persists the evidence tree, and derives the status in one call;
  idempotent on `(content_id, data_id)`.
- **Schema**: six additive tables (`propositions`, `predictions`, `findings`,
  `evidence_lines`, `contrasts`, `effect_estimates`), with the prediction table
  append-only. Schema stays at v1; an existing v0.3.3 `graph.db` gains them on
  next `open_db()`.
- **Docs**: a Findings concept page, the trust API surface, and the six tables
  in the data model.

### Notes

- The superiority gate is one-sided at `alpha`. A supplied p-value is read as
  two-sided (the metafor/escalc convention), so significance is `p < 2*alpha`,
  matching the `(1 - 2*alpha)` confidence-interval path.

## [0.3.3] - 2026-05-29

Adapter framework and substrate primitives. Five new primitives in
core (events, tools, canonicalize, derivation, hooks) plus three
opt-in adapters under `mareforma.adapters.*` and a literature-ingest
CLI. Schema stays at v1; existing v0.3.2 graph.db
auto-applies the new `literature_claims` and `agent_activities`
tables on next `open_db()`.

### Added

- **Substrate primitives**
  - `mareforma.events`, `EventSource` / `EventHandler` Protocols,
    typed `EventPayload` and `ClaimResult`, source-name constants
    (`SOURCE_CLAWINSTITUTE`, `SOURCE_TOOLUNIVERSE`, `SOURCE_GEMINI`,
    `SOURCE_CLAUDE_CODE_PRETOOLUSE`) so adapters dispatch on
    constants, not string literals.
  - `mareforma.tools`, `Tool` Protocol (`name`, `version`,
    `call(**kwargs) -> ToolResult`), `ToolResult` TypedDict,
    `ReplayResult` dataclass. The structural contract any wrappable
    callable satisfies.
  - `mareforma.canonicalize`, registry-based canonicalizer surface
    for adapter authors. Default `json-c14n-v1` (RFC 8785 JCS) plus
    `dsse-jcs-nfc-v1` (same bytes the signed-envelope layer produces).
    `mareforma.canonicalize.specialty` registers
    `rdkit-canonical-smiles-v1`, `fasta-nfc-v1`, `pdb-atom-sorted-v1`
    on import; registry is lock-guarded for thread safety.
  - `mareforma.derivation`, substrate-derived classification.
    Deterministically derives `ANALYTICAL` vs `INFERRED` from a static
    profile of the agent's source code plus dynamic templates extracted
    from runtime logs (Drain parser). Source-profile extraction
    requires the `[derivation]` extra (tree-sitter); log-template
    extraction is pure stdlib.
  - `mareforma.hooks`, Claude Code `PreToolUse` handler
    (`python -m mareforma.hooks`) records every tool invocation as a
    `prov:Activity` row. The `agent_activities` table is part of the
    canonical schema; the hook routes through `mareforma.db.open_db`
    so it inherits foreign-keys PRAGMA + schema validation.
- **Capability-shaped predicate URI constants** on
  `mareforma.predicate_types` (re-exported at the top level):
  `CONTAINER_EXEC_V1`, `CODE_VARIATION_V1`, `HYPOTHESIS_V1`,
  `LITERATURE_INSIGHT_V1`, `SCIENCE_SKILL_V1`, `META_CLAIM_V1`,
  `WORKSHOP_EVENT_V1`. Adapters import the constants, a typo on a
  constant name fails at import; a typo on a URI string would
  silently mis-classify a claim.
- **Three opt-in adapters under `mareforma.adapters.*`** (each
  behind an install extra so the default install stays slim):
  - `mareforma.adapters.clawinstitute`, generic ClawInstitute
    workshop-event hook. `EventHook` implements the EventSource
    Protocol; `HttpxClient` uses a pooled `httpx.Client` with
    `follow_redirects=False` and quotes URL path segments to refuse
    `..` traversal. Eight typed exceptions all share
    `ClawInstituteApiError` as parent. Untrusted post content runs
    through three sanitisation layers (raw-byte cap →
    `sanitize_for_llm` → `wrap_untrusted`) before any handler sees
    it. Handler exceptions during `dispatch()` are caught and
    returned as `ClaimResult(error=...)` so a misbehaving
    subscriber cannot block peers.
  - `mareforma.adapters.tooluniverse`, wrap any
    `mareforma.tools.Tool` so each `.call(**kwargs)` records a
    signed `urn:mareforma:predicate:tool-call:v1` claim with
    arguments digest, result digest, tool config fingerprint,
    timing. Container-exec class tools route to
    `urn:mareforma:predicate:container-exec:v1`. Over-cap results
    raise `ResultTooLargeError` rather than truncating mid-byte
    (truncated canonical JSON produces a digest no replayer can
    re-derive).
  - `mareforma.adapters.gemini`, read-only ingest for Gemini for
    Science outputs (4 capabilities: AlphaEvolve code-variation,
    Co-Scientist hypothesis, NotebookLM literature-insight,
    Antigravity science-skill). Per-capability `REQUIRED_FIELDS`
    validation runs before `assert_claim`; string payload values
    flow through `sanitize_for_llm`; reserved keys (`predicate_type`,
    `capability`) are adapter-owned and a caller that tries to set
    them in `payload` raises `ValueError`.
- **Literature ingest CLI:** `mareforma ingest <file>`,
  `mareforma ask "<query>"`, `mareforma narrative`. Paper claim
  drafts live in their own `literature_claims` table (separate from
  the signed `claims` table) so most ingested assertions stay drafts
  pending review. FTS5 BM25 search escapes embedded quotes; the
  narrative exporter flags structural and polarity-heuristic
  contradictions inline.
- **`mareforma.db.open_db_from_db_path()`**, opens a graph DB from
  a direct file path (the CLI accepts `--db <root>/.mareforma/graph.db`
  or any non-conventional location). Honours the supplied filename
  instead of silently re-deriving `<root>/.mareforma/graph.db`.
- **`rich` is now a core dep** (used by ingest / ask / narrative
  output formatting).

### Changed

- **Schema is additive on every `open_db()`.** `literature_claims`,
  `literature_claims_fts` (with insert / delete / **update**
  triggers, the update trigger is new), and `agent_activities`
  tables are created via an `_ADDITIVE_TABLES_SQL` script that
  runs on both fresh and v1-initialised graphs. Existing v0.3.2
  databases pick up the new tables on first open with no migration
  required.
- **`cli.py` lazy-loads ingest / ask / narrative subcommands** so
  `mareforma --help` / `--version` / `bootstrap` / `validator add`
  do not pay the rich + tomli_w import cost.

### Fixed

- `mareforma.derivation.source_profile`: import guard now catches
  `Exception` (tree-sitter ABI mismatch surfaces as `TypeError` /
  `RuntimeError`, not `ImportError`). `_require_tree_sitter` includes
  the underlying error in the install hint.
- `mareforma.derivation.source_profile`: module-prefix matching
  requires a dot separator so `urllib_legacy.get` no longer matches
  the `urllib` import.
- `mareforma.derivation.source_profile`: dead-zone walker no longer
  marks `except` clause bodies as dead. The prior behaviour silently
  demoted ANALYTICAL agents to INFERRED on any error-handling path.

### Removed

- `truncate_oversized=True` constructor option on
  `mareforma.adapters.tooluniverse.ProvenanceToolAdapter`. Truncating
  canonicalised JSON at an arbitrary byte boundary produces bytes no
  replayer can re-derive; the adapter now always raises
  `ResultTooLargeError` when results exceed `max_result_bytes`.
- Unused `canonicalizer_fallback` and `result_truncated` parameters
  from `build_tool_call_predicate` (always-False fields signed into
  every envelope; permanent on-disk noise).

## [0.3.2] - 2026-05-27

Internal restructure + one restore-time verification improvement.
Schema stays at v1; no migration required. All existing
`from mareforma.db import X` and `from mareforma.signing import Y`
import paths continue to work unchanged.

### Changed

- **`mareforma/signing.py` split into `mareforma/signing/` subpackage.**
  `signing/core.py` carries DSSE PAE, canonical Statement v1, key
  management, envelope sign/verify, and `bootstrap_key`.
  `signing/rekor.py` carries Rekor submission, RFC 6962 Merkle
  inclusion-proof verification, checkpoint parsing, SSRF defense, and
  log-pubkey fetch. `signing/__init__.py` re-exports every public and
  underscore-prefixed name with explicit `__all__` (PEP 561).
- **`mareforma/db.py` split into `mareforma/db/` subpackage.**
  `db/core.py` (~3960 LOC) carries the live-write path, queries,
  verdicts, Rekor saga, and TOML backup, the threat-model locality
  stays in one buffer. `db/_schema_sql.py` carries the DDL constant
  and column contract. `db/errors.py` carries the exception hierarchy.
  `db/restore.py` carries `restore()` and its verification helpers.
  `db/__init__.py` re-exports the full surface with explicit `__all__`.
- **Warnings idiom normalised.** All function-body lazy
  `import warnings as _warnings` hoisted to module-top
  `import warnings`; `_warnings.warn()` rewritten to `warnings.warn()`
  across 7 call sites.

### Added

- **`rekor_inclusions` sidecar round-trip through `claims.toml`.**
  `_backup_claims_toml` now emits a `[rekor_inclusions]` section
  carrying each sidecar row's uuid, log\_index, integrated\_time,
  raw\_response\_b64, and recorded\_at. `restore()` replays entries
  into the sidecar table after the corresponding claim INSERT, inside
  the same fail-all-or-nothing transaction. Closes the restore-time
  gap where Merkle inclusion proofs could not be re-verified
  post-restore.
- **Two drift-warning classes** for the sidecar restore path:
  `RekorSidecarSectionAbsentWarning` fires once per restore when the
  TOML has no `[rekor_inclusions]` section (expected for pre-v0.3.2
  files); `RekorSidecarEntryMissingWarning` fires per Rekor-logged
  claim when the section exists but lacks an entry for that claim
  (suspicious). The two are distinct so operators can tell a
  legitimate upgrade from a TOML edit.
- **CI guard tests** (`test_signing_reexports.py`,
  `test_db_reexports.py`) walk each submodule source file via AST and
  assert every defined name is importable AND accessible via
  `getattr` on the package. Fails CI if a future contributor adds a
  name without mirroring it in `__init__.py`.
- **Restore-time sidecar validation.** Orphan `rekor_inclusions`
  entries (referencing a claim\_id not in `[claims]`) and entries
  missing required fields (uuid, raw\_response\_b64) raise
  `RestoreError`. Sequential inclusion-proof verification costs ~1ms
  per entry (~50s for a 50k-claim Rekor-logged graph on the one-shot
  disaster-recovery path).

### Compatibility

- All `from mareforma.db import X` and `from mareforma.signing import Y`
  import paths work unchanged. External callers need no code changes.
- `claims.toml` files from v0.3.0 / v0.3.1 (no `[rekor_inclusions]`
  section) restore successfully on v0.3.2 with a
  `RekorSidecarSectionAbsentWarning`. Run `refresh_unsigned()` after
  restore to re-fetch inclusion proofs from the log.

## [0.3.1] - 2026-05-22

Additive release. The substrate's versioned schema stays at v1; new
columns land via in-place `ALTER TABLE ADD COLUMN` on the
non-signed-integrity surface. On first `mareforma.open()` after the
upgrade the substrate auto-adds three columns: `claims.predicate_payload`
(`TEXT NOT NULL DEFAULT ''`), `claims.original_signature_bundle`
(`TEXT NULL`), and `doi_cache.content_digest` (`TEXT NULL`). None of
these are part of the signed envelope or chain hash, so every
existing claim's signed bytes round-trip byte-equal and signatures
re-verify under the new code. A new rebuildable cache file lives at
`.mareforma/claim_supports_cache.db`; the file is created on first
open and auto-rebuilt on detection of count-mismatch, missing, or
corrupt state.

### Added

- **`EpistemicGraph.query_provenance(claim_id, depth=4)`** , 
  agent-readable lineage view of a claim: focal row + role-actor
  signatures + recursive upstream / downstream walks + inbound
  contradictions + replication verdicts in one deterministic dict.
- **Rebuildable `claim_supports` cache.** Edge denormalisation in a
  separate SQLite file (`.mareforma/claim_supports_cache.db`).
  Recursive-CTE walkers serve provenance queries in O(depth * deg).
  Auto-rebuilt on stale / missing detection; 50k-claim p99 < 300ms.
- **`claim-with-roles:v1` multi-signature DSSE envelopes.** New
  `mareforma.signing.sign_claim_with_roles` + `verify_envelope_multi`
  let asserters carry per-role (planner / executor / reviewer /
  validator) signatures inside one envelope. Legacy single-sig
  envelopes verify under the existing `verify_envelope` unchanged.
- **PROV-O JSON-LD exporter** + four-invariant hand-rolled
  validator. `mareforma export --format=prov-o`.
- **Self-validation defense-in-depth.** `_refuse_self_validation`
  walks every signature on the envelope (not just signatures[0]);
  new `_refuse_self_verdict` blocks replication / contradiction
  verdict issuers from signing either referenced claim.
- **GRADE certainty surface.** Optional `study_design` field on
  `EvidenceVector` (`randomised-trial` / `observational` /
  `case-series` / `not-applicable`) + new `EvidenceVector.certainty()`
  returning the GRADE four-tier band.
- **DOI metadata drift detection.** New
  `doi_cache.content_digest` column +
  `EpistemicGraph.find_drifted_dois(limit=N)` (Crossref AND DataCite
  shapes, NFC-normalised, rate-limit-aware, registry-pinned).
- **Refutation taxonomy + filter.** New `refutation_status()`
  presenter (clean / contradicted / contested / retracted) and a
  composable `refutation_filter` kwarg on `query()` / `search()`.
- **Grounding sensor protocol.** New `mareforma.Verifier` Protocol +
  `MockNLIVerifier` reference impl. `EpistemicGraph.assert_claim(
  grounding_sensor=verifier)` snapshots the verdict (score +
  rationale) into the signed Statement v1 predicate at assertion
  time. Broken sensors fall back to no-score with a RuntimeWarning;
  the claim still asserts.
- **Predicate URI reservations.** `BUILTIN_URIS` expanded from 3 to
  21 entries reserving the substrate-owned slots (claim,
  epistemic-graph, claim-with-roles) plus 18 adapter URIs
  (tool-call, ingested-trace, agent-trace, llm-output, review,
  peer-review, elo-match, tournament-bracket, wet-lab-assay/{
  flow-cytometry, sequencing, imaging, proteomics,
  electrophysiology}, replication-attestation,
  compounding-attestation, semantic-grounding, doi-resolution, and
  the wet-lab-assay umbrella).
- **Operational health log + stats CLI.** Append-only
  `.mareforma/health.jsonl` records per-op operational signal
  (provenance queries, grounding verdicts, DOI drift scans,
  refresh retries). New `mareforma stats [--last N] [--json]`
  command renders rolling rates (avg score, pass rate,
  availability, average drift). Bounded reads use a
  fixed-capacity deque so the reader stays O(last_n) on long logs.
- **Public `EpistemicGraph.update_claim`** wrapper around
  `db.update_claim`. Status mutations are EDITORIAL, the
  docstring documents the unsigned trust posture and recommends
  the retract-and-supersede pattern for cryptographically-traceable
  retractions.
- **Public `EpistemicGraph.refutation_status(claim_id)`** thin
  wrapper exposing the presenter.
- **`mareforma export --format` adds `prov-o`** alongside the
  existing `in-toto-v1` / `ro-crate-1.2` / `jsonld` shapes.
- **`assert_claim(signer=key, predicate_payload=dict,
  original_signature_bundle=str, grounding_sensor=verifier)`** , 
  four new kwargs on the public assertion path for adapter
  scaffolding.
- **`record_replication_verdict(method='signed-elo-bracket-replay')`**
 , new method enum value alongside hash-match, semantic-cluster,
  shared-resolved-upstream, cross-method.

### Changed

- **CLI: `mareforma stats` renamed to `mareforma activity`.** The
  old `stats` name was one letter from the unrelated `status`
  command (snapshot vs. rolling-rate), and the homonym was a
  source of confusion. `mareforma activity` carries the same
  flags (`--json`, `--last=N`) and reads the same on-disk log.
  The `mareforma stats` alias still works for one release and
  emits a `DeprecationWarning` pointing at the new name; v0.4
  removes the alias.
- **Wheels now ship `mareforma/py.typed`** (PEP 561). Downstream
  type-checkers (mypy / pyright) now honour mareforma's
  annotations instead of treating every imported symbol as
  `Any`. No source change required by callers; existing typed
  integration code starts seeing real errors against the
  substrate's signatures.
- **`mareforma.EpistemicGraph` is now part of the public surface**
  (added to `__all__`, importable via `mareforma.EpistemicGraph`).
  Type-hint callers no longer need to reach into the private
  `mareforma._graph` module to annotate function signatures that
  accept a graph handle.

### Hardening

- Mixed journal mode bug fixed, both `graph.db` and
  `claim_supports_cache.db` now run WAL so cross-DB transactions
  share atomicity guarantees.
- Multi-sig envelopes on `signature_bundle` get every signature
  verified on `restore()`, not just signatures[0]. Forged extra
  signatures are rejected.
- Self-validation / self-verdict gates refuse claims whose
  `signature_bundle.signatures` is empty or non-list (would
  otherwise let a tamperer drop their keyid from the gate).
- `find_drifted_dois` aborts on the first 429 and defaults to a
  cap of 100 inspected DOIs per call (Crossref polite-pool
  guidance ~50 req/sec with two GETs per DOI).
- Grounding sensor receives `supports` as an immutable tuple so a
  hostile or buggy verifier cannot mutate the citation list before
  the predicate is signed.
- Grounding sensor exception catch widened from
  `(VerifierError, AttributeError, TypeError, ValueError)` to
  `Exception`, real-world verifiers raise OSError /
  ConnectionError / KeyError / RuntimeError, and the substrate's
  documented "claim still lands" contract now actually holds for
  those.
- `health.jsonl` writes use `json.dumps(allow_nan=False)` so a NaN
  score never produces non-portable JSONL that breaks `jq` /
  browser `JSON.parse`.
- PROV-O exporter refuses non-UUID claim_ids (parity with
  RO-Crate); `_extract_metadata_subset` returns None on empty
  subsets so first-seen seeding doesn't collapse every empty-
  metadata DOI to the same digest.

### Compatibility

- Legacy single-signature claim envelopes verify under
  `verify_envelope` unchanged.
- `EvidenceVector.to_dict()` omits `study_design` /
  `grounding_score` / `grounding_rationale` when None so the
  canonical bytes of a previously-signed claim round-trip
  byte-equal under the new verifier.
- Re-registering one of the 18 newly-reserved adapter URIs
  (anything other than `claim:v1`, `epistemic-graph:v1`,
  `claim-with-roles:v1`) by a foreign owner is downgraded from
  raise to DeprecationWarning for one release, adapters that
  pre-registered before promotion get one cycle to drop the
  call before the next version refuses outright. The three core
  substrate-owned URIs still raise hard on foreign re-registration.
- `predicate_payload` is intentionally NOT part of the
  idempotency reconciliation surface, a retry with the same
  `idempotency_key` but divergent `predicate_payload` silently
  returns the first writer's `claim_id` and discards the second
  payload. The field is a query-side denormalisation, not
  cryptographic identity; adapters that need predicate-body
  integrity must encode the body in the claim text instead.

## [0.3.0] - 2026-05-13

Breaking change from v0.2.x. Schema does not migrate from older
versions; delete `.mareforma/graph.db` to start fresh.

`claims.toml` at the project root is the canonical source for
`mareforma.restore()`, it rebuilds `graph.db` and re-verifies every
per-claim signature against the enrolled signer's pubkey. The
`prev_hash` chain is **regenerated** during restore (not preserved):
claims are replayed in `created_at` order, and the chain is rebuilt
from canonical-statement bytes. Signatures survive because they bind
the canonical statement, not the chain position. The most recent row
may lag SQLite commit on a process crash because `_backup_claims_toml`
runs after `COMMIT`, outside the SQLite transaction; the v0.4
performance rewrite moves this off the foreground commit path and
addresses the crash gap.

What ships in v0.3.0:

- **Ed25519 claim signing** with optional Sigstore-Rekor transparency log
- **Artifact-hash gate** on REPLICATED, converging peers that both supply a SHA-256 must agree
- **Identity-gated `graph.validate()`** with a per-project validators table and signed enrollment chain
- **DOI resolution** against Crossref + DataCite with a persistent cache
- **DB-layer state-machine triggers** + append-only `prev_hash` chain, the storage layer rejects illegal transitions
- **Cycle / self-loop detection** on `supports[]` at INSERT and UPDATE
- **ESTABLISHED-upstream requirement** for REPLICATED + signed seed-claim bootstrap (Cochrane / GRADE evidence chains; no replication-of-noise)
- **JSON-LD export** in a mareforma-native vocabulary
- **SCITT-style signed export bundle** + `mareforma verify` CLI
- **In-toto Statement v1 + DSSE v1 PAE envelope** on every signed claim, GRADE 5-domain `EvidenceVector` inside every signed predicate, signed verdict-issuer protocol that any third party can integrate against (see below)

Envelope upgrade + verdict-issuer protocol (substrate-launch additions):
- **In-toto Statement v1 + DSSE v1 PAE envelope**, every signed claim is now a DSSE envelope (`payloadType=application/vnd.in-toto+json`) wrapping an in-toto Statement v1 (`predicateType=urn:mareforma:predicate:claim:v1`). Standards-aligned; introspectable by `cosign`, GUAC, and any in-toto-aware tool without a mareforma-specific verifier. URN (not DNS), the identifier is a stable name, not a fetched document, avoiding a perpetual-ownership commitment on any DNS name. The signature covers the DSSE Pre-Authentication Encoding (PAE), not the payload bytes alone, a signature on `(typeA, payload)` cannot be replayed as a signature on `(typeB, payload)`.
- **GRADE 5-domain EvidenceVector** carried inside every signed claim's predicate. Five downgrade domains (`risk_of_bias`, `inconsistency`, `indirectness`, `imprecision`, `publication_bias`) each in `[-2, 0]`, three upgrade flags (`large_effect`, `dose_response`, `opposing_confounding`), `rationale` dict (required for any nonzero domain), and `reporting_compliance` list. Bound into the signature; denormalized into `ev_*` columns for queryable filters; restore re-derives the canonical bytes and refuses any TOML-tampered upgrade.
- **Verdict-issuer protocol.** Two new tables, `replication_verdicts` and `contradiction_verdicts`, accept signed verdicts from any enrolled validator. The OSS substrate ratifies what enrolled identities sign; the predicates that PRODUCE verdicts (semantic-cluster, cross-method, hash-match, shared-resolved-upstream, contradiction-detection) live outside the OSS and call `Graph.record_replication_verdict()` / `Graph.record_contradiction_verdict()`. New `VerdictIssuerError` exception covers the gates: signer must be enrolled (chain walk back to a self-signed root), referenced claim must exist, method must be in the allowed enum, contradiction `member != other`.
- **`t_invalid` derived state.** New nullable column on `claims`. The `contradiction_invalidates_older` AFTER INSERT trigger on `contradiction_verdicts` sets `t_invalid` on the older of the two referenced claims (lex-smaller `claim_id` as deterministic tie-break when timestamps collide; idempotent via `WHERE t_invalid IS NULL`). `validate_claim` refuses to promote a `t_invalid` claim, a signed contradiction is terminal evidence.
- **New `include_invalidated` kwarg** on `graph.query()`, `graph.search()`, `graph.replication_verdicts()`, `graph.contradiction_verdicts()`. Defaults to `False`, invalidated claims and the verdicts that reference them are excluded from default reads. Pass `True` for audit / history queries.
- **Append-only over the signed predicate.** New `claims_signed_fields_no_laundering` BEFORE UPDATE trigger refuses direct-SQL mutation of any signed-predicate column (`text`, `classification`, `generated_by`, `supports_json`, `contradicts_json`, `source_name`, `artifact_hash`, `ev_*`, `evidence_json`, `statement_cid`, `prev_hash`, `created_at`) on rows whose `signature_bundle IS NOT NULL`. Value-comparison fires only when something actually changed, so multi-column UPDATEs that re-emit unchanged values pass through. A tampered Python interpreter cannot relax this.
- **Append-only verdicts.** `replication_verdicts_append_only` + `replication_verdicts_no_delete` triggers refuse UPDATE on signed columns and any DELETE. Same for contradictions. The envelope is the source of truth; rows cannot drift from what was signed.
- **PRAGMA foreign_keys = ON.** Set on every `open_db()`. The verdict tables' FK references to `validators(keyid)` and `claims(claim_id)` are now enforced, direct-SQL INSERTs with fabricated keyids fail at the SQL layer, not just in Python.
- **Subject ↔ predicate consistency.** `claim_predicate_from_envelope()` refuses envelopes where `subject[0].name` or `subject[0].digest.sha256` disagree with the predicate's `claim_id` or `text`. The signature would still verify in such an envelope, but the two halves would describe different claims, in-toto consumers keying off `subject` would see a different identity than mareforma's predicate. Caught at the envelope-decode layer.
- **Restore extensions.** `claims.toml` round-trip now covers `replication_verdicts` and `contradiction_verdicts` sections (signatures base64-encoded). Each verdict's signature is cryptographically verified against the enrolled issuer's pubkey before INSERT. Verdicts are replayed in `created_at` order so the contradiction trigger's `WHERE t_invalid IS NULL` guard preserves the truthful first-invalidation moment. `transparency_logged=true` in TOML is downgraded to `0` when the bundle has no `rekor` block, hand-edited TOML cannot fake a Rekor inclusion. New adversarial tests: tampered `EvidenceVector`, swapped `statement_cid`, tampered verdict fields (`cluster_id`, `method`, `confidence_json`, `signature`), forged `issuer_keyid`.
- **New modules**, `mareforma._canonical` (NFC + sorted-keys + no-whitespace + `allow_nan=False` canonical JSON), `mareforma._statement` (in-toto Statement v1 builder + `statement_cid` computation), `mareforma._evidence` (stdlib-dataclass `EvidenceVector` with `__post_init__` validator). No pydantic dependency added; mareforma stays at 5 runtime deps.
- **`mareforma.signing.dsse_pae()`** is public so external verifiers can independently re-derive the bytes the signature covers. `canonical_statement(claim_fields, evidence)` replaces the legacy `canonical_payload` for chain-hash + signature inputs; old shim is removed because it silently desynced from production bytes.

### Added

- Ed25519 claim signing. New `mareforma/signing.py` module: keypair gen + PEM save/load + DSSE-style envelope sign/verify. Private key lives at `~/.config/mareforma/key` (XDG-compliant, mode 0600). Public-key id is SHA-256 of the raw Ed25519 public bytes.
- `mareforma bootstrap` CLI command: one-time identity setup. Generates a fresh keypair, prints the public-key id. Refuses to overwrite an existing key unless `--overwrite` (avoids orphaning every previously-signed claim).
- `mareforma.open(key_path=..., require_signed=...)` parameters. When a key exists at the XDG path (or `key_path`), claims are automatically signed before INSERT and the envelope is persisted to a new `signature_bundle` TEXT column. `require_signed=True` raises `KeyNotFoundError` if no key is found, high-assurance opt-in.
- Signed payload binds `claim_id`, `text`, `classification`, `generated_by`, `supports`, `contradicts`, `source_name`, `artifact_hash`, `created_at`. Any tamper with the row breaks verification.
- `artifact_hash` parameter on `assert_claim` (Python API) and `--artifact-hash` flag on `mareforma claim add` (CLI). Accepts a SHA256 hex digest of the output bytes (figure, CSV, model) backing the claim. Normalised to lowercase, validated as 64-char hex, persisted to a new `artifact_hash TEXT` column and bound into the signed payload. Restores the v0.1 artifact-hashing discipline that was dropped in v0.2.
- **Signed-payload change.** `canonical_payload` now always emits an `artifact_hash` key (`null` when absent), so envelopes signed before this commit on the v0.3.0 dev branch no longer re-derive byte-for-byte. Any signed claim from an earlier v0.3.0 dev checkout must be re-asserted on a fresh `graph.db`. v0.2.x → v0.3.0 already requires a fresh `graph.db`, so end-users on a tagged release are unaffected.
- REPLICATED detection now consults `artifact_hash` as a parallel signal. When two converging peers BOTH supply a hash, the hashes must match for `REPLICATED` to fire. When either side omits the hash, the gate is bypassed and identity-only `REPLICATED` applies, the signal is opt-in, not retroactive.
- New `IdempotencyConflictError` raised when `add_claim` replays the same `idempotency_key` with a different `artifact_hash` (in either direction, including hash-then-omit). Silently returning the first claim_id would let a caller believe their new hash was registered when it was not, defeating tamper-evidence. Use a different `idempotency_key` or omit the conflicting field.
- `idx_claims_artifact_hash` partial index (only rows with a non-NULL hash) accelerates the REPLICATED query without bloating the index for users who don't supply hashes.
- New `mareforma.prompt_safety` module + `EpistemicGraph.query_for_llm()` method. Sanitize-and-wrap helpers for feeding retrieved claim text into an LLM prompt. Strips zero-width / bidi-override / C0-C1 control characters, caps oversized fields at 100k chars with a visible truncation marker, and wraps free-text fields (`text`, `comparison_summary`) in `<untrusted_data>...</untrusted_data>` delimiters. Forged opening/closing tags inside the content are replaced with `[stripped]` so a hostile claim cannot break out of the wrapper (case-insensitive, whitespace-tolerant). The metadata labels (`source_name`, `generated_by`, `validated_by`) are sanitized but not wrapped. `mareforma.sanitize_for_llm`, `mareforma.wrap_untrusted`, and the composed `mareforma.safe_for_llm` are public for one-off use.
- Stripping also covers known steganographic prompt-injection vectors: the Unicode "tags" plane (U+E0000-E007F) used by Goodside-style ASCII-smuggler attacks, variation selectors (U+FE00-FE0F, U+E0100-E01EF, U+180B-180D), interlinear annotation anchors (U+FFF9-FFFB), and the fullwidth `<`/`>`/`/` lookalikes (U+FF1C/E/F) that could survive both sanitize and wrap if a downstream NFKC normaliser folds them to ASCII.
- New `SECURITY.md` documents the disclosure channel (GitHub Private Vulnerability Reporting), supported-versions policy (latest pre-1.0 only), PyPI Trusted Publishing setup, cryptographic trust boundaries, and out-of-scope categories. **Operator note:** Private Vulnerability Reporting must be enabled in repo Settings → Security for the referenced URL to work.
- `EpistemicGraph.get_tools()` now routes through `query_for_llm` internally. The `query_graph` tool that ships to LangChain / LangGraph / CrewAI / AutoGen / LlamaIndex / PydanticAI / Smol Agents / OpenAI SDK previously returned raw claim text, a stored prompt-injection planted by a prior agent would have been delivered verbatim to the consuming LLM. The tool now returns `text` wrapped in `<untrusted_data>...</untrusted_data>` with sanitization applied, matching the documented safe-retrieval contract.
- Sanitize-on-write at the DB layer: `assert_claim` runs `prompt_safety.sanitize_for_llm(text)` before signing and persisting. Defense in depth, any consumer that reads `claim.text` directly (custom analytics, claims.toml restore, third-party tooling) gets a clean string. The signed payload binds the sanitized form, so downstream verifiers see what the LLM sees. Claims that consist entirely of zero-width / control characters are rejected with `ValueError`.
- Hard cap on claim text at 100,000 characters (`_MAX_CLAIM_TEXT_LEN` in `db.py`). Matches the truncation point in `prompt_safety._MAX_FIELD_LEN` so claim text never silently degrades when consumed by an LLM. Multi-MB writes are rejected at `assert_claim` time.
- `.github/workflows/*.yml` first-party actions pinned by commit SHA: `actions/checkout@34e1148…` and `actions/setup-python@a26af69…`. Closes the tag-squat / maintainer-compromise vector against the Trusted Publishing OIDC token. The third-party `pypa/gh-action-pypi-publish` was already SHA-pinned.
- `.github/CODEOWNERS` and `.github/dependabot.yml`. CODEOWNERS documents the required-review surface for the release pipeline and SECURITY.md (operator must enable "Require review from Code Owners" in repo Branch protection rules for enforcement). Dependabot keeps the Action SHAs and Python deps current.
- Sigstore-Rekor transparency-log integration. New `mareforma.open(rekor_url=..., require_rekor=...)` parameters. When a Rekor URL is set, every signed claim is submitted to the transparency log at INSERT time using the `hashedrekord` entry kind; the entry uuid + logIndex are attached to the bundle and `transparency_logged` flips to 1. Submission failure persists the claim with `transparency_logged=0` and blocks REPLICATED promotion, mirroring the DOI `unresolved` pattern.
- New `EpistemicGraph.refresh_unsigned()` retries Rekor submission for every signed-but-unlogged claim. Mirrors `refresh_unresolved()`. Returns `{checked, logged, still_unlogged}`.
- REPLICATED detection now requires `transparency_logged = 1` alongside `unresolved = 0`. Unsigned claims and Rekor-disabled mode (no `rekor_url`) keep the default `transparency_logged=1`, so they REPLICATE unchanged.
- `transparency_logged INTEGER NOT NULL DEFAULT 1 CHECK(IN (0,1))` column on the claims table + `idx_claims_transparency_logged` index.
- `mareforma.signing.PUBLIC_REKOR_URL` constant points to the public sigstore Rekor instance for users who want it without typing the URL.
- Signed-claim append-only invariant. `update_claim` raises `SignedClaimImmutableError` when asked to mutate `text` / `supports` / `contradicts` on a claim with a non-NULL `signature_bundle`. Mutating signed-surface fields would silently invalidate the signature without surfacing the change. To revise a signed claim, retract it (`status='retracted'`) and assert a new one citing the old via `contradicts=[<old_claim_id>]`. `status` and `comparison_summary` remain editable since neither is part of the signed payload.
- `submit_to_rekor` now verifies the Rekor response actually records OUR submission: the encoded `entry.body` is base64-decoded, parsed, and its `spec.data.hash.value` and `spec.signature.content` must match what we sent. A hostile or buggy registry can no longer hand back an arbitrary uuid/logIndex that mareforma accepts as proof of inclusion.
- `submit_to_rekor` caps Rekor responses at 64 KB (both the `Content-Length` header and the actually-received bytes). A multi-MB JSON blob from a hostile endpoint can no longer land in `graph.db` and amplify through every subsequent backup.
- `mareforma.open(rekor_url=...)` validates the URL at open() time: only `https://` is accepted, and loopback / private RFC1918 / link-local IP literals are rejected. `mareforma.open(trust_insecure_rekor=True)` is the explicit opt-out for internal Rekor instances on private networks.
- `refresh_unsigned` drift + key-rotation guards. Before re-submitting a stored envelope to Rekor, the canonical payload bytes are compared against the live row's signed fields, a tampered row is quarantined as still-unlogged with a warning rather than cementing a stale signature in the public log. Likewise, an envelope whose keyid does not match the graph's current signer (key was rotated since assert_claim) is skipped with a warning instead of retrying forever.
- `mark_claim_logged` decodes the supplied bundle and verifies its payload's `claim_id` matches the row before writing. A buggy caller cannot silently write Alice's bundle onto Bob's row.
- `save_private_key` chmods the leaf parent directory to `0o700` on POSIX so the per-user mareforma config directory is not enumerable by other local users. `bootstrap_key` now uses `O_CREAT|O_EXCL` for the no-overwrite path: two concurrent bootstraps can no longer both pass an `exists()` check and race to overwrite each other.
- `load_private_key` emits a `UserWarning` on non-POSIX platforms (Windows etc.) where file-mode bits are largely advisory and mareforma does not configure ACLs.
- `add_claim` now warns when Rekor accepts the submission but the local follow-up UPDATE fails, operators can no longer miss the `transparency_logged=0` divergence and learn that running `refresh_unsigned()` will reconcile.
- `mareforma.signing.SIGNED_FIELDS` and `mareforma.signing.canonical_payload(...)` are now public so verifiers can independently re-derive the bytes that should be signed.
- `validate_rekor_url` now rejects DNS-shortcut SSRF bypasses: `localhost`, `localhost.localdomain`, `ip6-localhost`, `ip6-loopback`, and numeric-only hostnames (`127.1`, `2130706433`, `0177.0.0.1`). Python's `ipaddress.ip_address` rejects these forms but `socket.getaddrinfo` resolves them to loopback, a DNS-shortcut SSRF gap that bypassed the earlier IP-literal-only check.
- `envelope_payload` raises `InvalidEnvelopeError` when the decoded payload is not a JSON object (was: bare JSON string/list/number passed through, then crashed downstream callers with `AttributeError` on `payload.get(...)`).
- `save_private_key(exclusive=True)` unlinks the file on a mid-write `OSError` so the next bootstrap retry can succeed instead of hitting a misleading "key already exists" on a zero-byte leftover.
- `submit_to_rekor` now streams the Rekor response via `httpx.stream(...)` with a running-byte accumulator that aborts at 64 KB during the read, a hostile registry can no longer cost 100 MB of RSS before the size cap fires.
- `submit_to_rekor` compares signatures by decoding both sides to raw bytes (`base64.urlsafe_b64decode`, which transparently accepts standard and URL-safe alphabets, with or without padding). Wire-equivalent base64 representations from real Rekor instances no longer false-reject.
- `mareforma bootstrap --overwrite` help text and `signing.bootstrap_key` docstring now call out the destructive consequence: every signed-but-not-yet-Rekor'd claim becomes permanently un-loggable when the prior key is gone. Documented safe rotation path: back up the old key, `refresh_unsigned()` to drain the pending queue, then rotate.
- Identity-gated validation. `graph.validate()` now requires a loaded signing key AND that key must be enrolled in the project's `validators` table. The first key opened against a fresh graph auto-enrolls as the root validator (silent self-signed enrollment). Additional validators are added via `mareforma validator add --pubkey ... --identity ...` (CLI) or `mareforma.validators.enroll_validator(...)` (library). Removal is intentionally unsupported in v0.3.0, validator history is append-only.
- New `validators` table on `graph.db`: `keyid`, `pubkey_pem`, `identity`, `enrolled_at`, `enrolled_by_keyid`, `enrollment_envelope`. Each enrollment is signed by the parent validator (root self-signs).
- New `mareforma/validators.py` module: `auto_enroll_root`, `enroll_validator`, `is_enrolled`, `get_validator`, `list_validators`, `count_validators`, `verify_enrollment`.
- `mareforma validator add` + `mareforma validator list` CLI subcommands.
- `graph.validate()` now signs the validation event itself: a DSSE-style envelope binding `(claim_id, validator_keyid, validated_at)` is persisted to a new `validation_signature` column on the claim. Tampering with `validated_by` / `validated_at` post-hoc is detectable.
- `validated_by` is now documented as a cosmetic display label. The authenticated validator identity is the `validator_keyid` embedded in `validation_signature`; consumers that care about who validated must check the signed envelope.
- New `mareforma.signing.sign_validator_enrollment(...)` and `mareforma.signing.sign_validation(...)` for the two new envelope kinds.
- `mareforma.signing.verify_envelope(envelope, public_key, *, expected_payload_type=...)` requires the envelope's `payloadType` to match the expected type; the default is the claim type. Callers verifying validator-enrollment or validation envelopes pass the explicit type. Cross-type swaps (e.g. a validation envelope substituted for a claim envelope) are refused.
- `auto_enroll_root` runs the check + insert inside `BEGIN IMMEDIATE` so two simultaneous opens of a fresh `graph.db` cannot both become roots. Root self-enrollment emits a `UserWarning` with the keyid prefix so an operator who opened the project with the wrong key has a chance to notice before the (irrevocable) root is cemented.
- `is_enrolled` now walks the enrollment chain back to a self-signed root and verifies every link's envelope against the parent's persisted pubkey. A row planted via direct sqlite INSERT with a fabricated parent does not pass. Chain-verification results are cached per-connection.
- `enroll_validator` and `auto_enroll_root` sanitize the `identity` field: 256-char cap, rejects control characters (codepoints < 0x20 except space) and NULs via the new `InvalidIdentityError`. Prevents ANSI escapes from spoofing the `(root)` marker in `mareforma validator list` output, and bounds the bytes signed into the enrollment envelope.
- `graph.validate()` threads ONE timestamp through to both the signed envelope's `validated_at` field and the row's `validated_at` column. Previously a second `_now()` was computed inside `db.validate_claim` and the two timestamps drifted by microseconds on every call, defeating the documented tamper-evidence property.
- New `EpistemicGraph.enroll_validator(pubkey_pem, *, identity)` and `EpistemicGraph.list_validators()` public methods so docs and library callers no longer need to reach into `graph._conn` / `graph._signer`.
- `mareforma validator add --pubkey <path>` caps the PEM file read at 64 KB. An oversized file (or a path the operator typo'd at a system log file) is rejected before parsing.
- Chain walk enforces a **singleton-root invariant**: if two rows in the `validators` table have `keyid == enrolled_by_keyid`, neither is trusted. An attacker with sqlite write access who plants a fresh self-signed row with their own key now invalidates the table for everyone rather than gaining validator power. The chain walk is also capped at 64 hops to defend against DoS from a pathologically long planted chain.
- `mareforma claim validate` now routes through `EpistemicGraph.validate()`, which means the CLI gets the same identity + signing guarantees as the library API: the loaded XDG key must be an enrolled validator, and the validation event is signed and persisted to the row.
- `verify_enrollment` now binds every field in the signed payload (`keyid`, `pubkey_pem`, `identity`, `enrolled_at`, `enrolled_by_keyid`) against the persisted row. A future refactor that lets `identity` or `pubkey_pem` drift between the envelope and the row will be caught.
- Identity sanitizer extended to reject Unicode display-spoofing characters (RTL/LTR overrides, zero-width spaces, BOM/ZWNBSP) on top of C0/C1 controls. Operators can no longer plant an identity that visually disguises the `(root)` marker in `mareforma validator list` output.
- `EpistemicGraph.__init__` warns when the loaded key is not an enrolled validator on this project (e.g. opened with the wrong key, or lost the bootstrap race). Surfaces immediately instead of failing on the first `validate()` call.
- `enroll_validator` raises `ValidatorAlreadyEnrolledError` with a "chain broken" message when the row exists but its chain doesn't verify, instead of leaking a raw `sqlite3.IntegrityError` from the PK conflict.
- `mareforma.open()`, returns `EpistemicGraph`; no `@transform` required
- `EpistemicGraph.assert_claim()`, assert claims directly from any agent
- `EpistemicGraph.query()`, query by text, support level, or classification
- `EpistemicGraph.get_claim()`, fetch a single claim by ID
- `EpistemicGraph.validate()`, human gate to ESTABLISHED
- `mareforma claim validate`, CLI command to promote REPLICATED → ESTABLISHED; `--validated-by` optional
- DOI resolution: every DOI in `supports[]`/`contradicts[]` is HEAD-checked against Crossref and DataCite at assert time. Unresolved DOIs mark the claim `unresolved=True` and block REPLICATED promotion. `EpistemicGraph.refresh_unresolved()` retries previously-failed resolutions.
- DOI resolver network contract: DOI suffix URL-encoded before interpolation (prevents host injection via `#`/`@`, preserves inner `/` for hierarchical suffixes like `10.1093/imamat/35.3.337`); `follow_redirects=False` (registry must answer directly); pooled `httpx.Client` with `User-Agent` and threading lock around lazy init (Crossref polite-pool, FD-leak-safe under concurrency); HTTP 429 from EITHER registry skips the cache write (a registry-wide throttling event no longer poisons the cache for 24h); tight exception clause (`httpx.HTTPError`, `httpx.InvalidURL`, `OSError`) so programmer bugs surface in tracebacks instead of silently becoming "unresolved".
- `doi_cache` table: persistent cache of DOI resolution results to avoid repeated network calls. TTLs: 30 days for resolved entries, 24 hours for unresolved (so retractions and registry blips self-correct).
- `httpx` is now a required dependency (was `paper` extra)
- `EpistemicGraph.get_tools()`, returns `[query_graph, assert_finding]` as plain Python callables; `generated_by` baked into closure; wraps in one line for any framework
- `mareforma.schema()`, runtime introspection of valid values and state transitions
- Claims schema v1: `classification`, `support_level`, `idempotency_key`, `validated_by`, `validated_at`, `branch_id`, `unresolved`; CHECK constraints on `classification`, `support_level`, `status`, `unresolved`
- Schema validation: `open_db()` enforces an exact column-set match against `_CLAIM_COLUMNS`. Replaces the version-number compare. Missing columns instruct the user to delete `graph.db`; **extras-only** is treated as a downgrade attempt and instructs the user to upgrade mareforma instead.
- `mark_claim_resolved()` is atomic: the unresolved-flag clear and the REPLICATED re-evaluation run in the same SQLite transaction; convergence detection remains best-effort within the transaction (transient lock errors no longer roll back the flag-clear).
- `update_claim()` re-resolves DOIs only when `supports`/`contradicts` actually change (diff-check against prior JSON), and re-runs REPLICATED convergence inside the update transaction when a claim transitions from `unresolved=1` to `0`, otherwise a claim cured via `update_claim` would stay PRELIMINARY forever even with a sibling waiting on it.
- `refresh_unresolved()` quarantines claims with corrupt `supports_json`/`contradicts_json` instead of aborting the entire refresh.
- DOI cache TTL parsing tolerates `Z` UTC suffix as well as `+00:00` (Python 3.10 compatibility for externally-loaded rows).
- REPLICATED auto-trigger: fires automatically when ≥2 claims share the same upstream in `supports[]` with different `generated_by`
- Framework integrations: AGENTS.md table covering Anthropic SDK, OpenAI SDK, LangChain, LangGraph, CrewAI, AutoGen, LlamaIndex, PydanticAI, Smol Agents
- Mintlify docs at `docs.mareforma.com`
- 5 runnable examples (API walkthrough, compounding agents, documented contestation, private data / public findings, MEDEA drug target)
- **DB-layer state-machine + append-only hash chain.** `claims.prev_hash TEXT UNIQUE` column carries a SHA-256 chain (`sha256(prev_chain_link || canonical_payload)`); BEGIN IMMEDIATE + UNIQUE constraint together prevent branched chains. Two `BEFORE` triggers enforce state transitions at the storage layer: insert trigger refuses ESTABLISHED-without-validation, update trigger refuses illegal transitions with translatable `mareforma:state:<from>-><to>` error codes. A separate `BEFORE UPDATE OF status` trigger makes `retracted` terminal (transitions out of retracted are refused, to resurrect a withdrawn finding, assert a new claim citing the old). New CHECK constraint requires `validation_signature` on every ESTABLISHED row. New exceptions `IllegalStateTransitionError` and `ChainIntegrityError`. Defense in depth: a tampered Python interpreter cannot relax the rules.
- **Simple-DFS cycle detection on supports[].** A claim that supports itself (directly or via a chain) is rejected at INSERT (`add_claim`) and at UPDATE (`update_claim` on unsigned claims, signed claims refuse supports mutation upstream of this check via `SignedClaimImmutableError`). Forward-walk DFS with a visited set, depth-capped at 1024 hops. DOI strings in supports[] are not graph nodes and skipped. New `CycleDetectedError` exception.
- **ESTABLISHED-upstream requirement for REPLICATED + seed-claim bootstrap.** REPLICATED promotion now requires at least one ESTABLISHED claim in the peer's supports[]. Matches Cochrane/GRADE evidence-chain methodology, stops replication-of-noise. Bootstrap path: `assert_claim(text=..., seed=True)` inserts the claim directly at ESTABLISHED with a signed seed envelope (payload type `application/vnd.mareforma.seed+json`, binds `claim_id + validator_keyid + seeded_at`). Only enrolled validators can produce seed envelopes. Strict by default, no opt-in flag.
- **JSON-LD export, mareforma-native vocabulary.** Removed PROV-O references (`prov:wasGeneratedBy`, `prov:used`) from the JSON-LD `@context`, the previous export name-dropped the vocabulary without populating the full PROV-O graph (no `prov:Activity`, no `prov:wasAssociatedWith`, no model identity or prompt/response hashes). The export now declares `@type='mare:Graph'` and `mare:mediaType='application/x-mareforma-graph+json'`. The `used` key on source-bearing claims was renamed to `usedSource` (aliased to `mare:usedSource`). Every `SIGNED_FIELDS` member is always emitted on each claim node so downstream consumers (e.g. the bundle verifier below) can re-derive `canonical_payload` from a node alone.
- **SCITT-style signed export bundle + `mareforma verify`.** New `mareforma/export_bundle.py` produces an in-toto Statement v1 wrapper around the JSON-LD export, with `predicateType='urn:mareforma:predicate:epistemic-graph:v1'` and a DSSE-style signature over the whole bundle. Subject names use the `urn:mareforma:claim:<uuid>` namespace; URN (not DNS) avoids a perpetual-ownership commitment on `mareforma.dev`. New CLI: `mareforma export --bundle [-o path]` writes a signed bundle (requires bootstrapped XDG key); `mareforma verify <bundle.json>` checks the bundle DSSE signature AND every per-claim subject digest. New `BundleVerificationError` names the first failing check so callers can route between "corrupt" and "cross-version skew".
- **Validator type signal.** `validators.validator_type TEXT CHECK IN ('human','llm')` column, bound into the signed enrollment envelope. Default `'human'`; pass `validator_type='llm'` to `graph.enroll_validator` or `--type llm` to `mareforma validator add`. Self-declared honesty disclosure, no external attestation. New substrate gate: `validate_claim` refuses LLM-typed signers (raises `LLMValidatorPromotionError`) and refuses self-validation when the validation signer keyid equals the claim's signature_bundle keyid (raises `SelfValidationError`). Bound at the substrate layer; wrapper code cannot bypass.
- **Reputation-aware retrieval.** `graph.query()` gains `include_unverified: bool = False`, PRELIMINARY claims whose signing keyid is not enrolled are excluded by default. Every result dict carries a derived `validator_reputation` (count of ESTABLISHED claims the validator has signed) and `generator_enrolled` (bool). New `graph.get_validator_reputation()` returns the bulk `{keyid: count}` map for all enrolled validators. `claims.validator_keyid TEXT` column denormalizes the validator from `validation_signature`'s payload for indexable reputation aggregation; partial index `idx_claims_validator_keyid WHERE NOT NULL` keeps storage scoped to ESTABLISHED rows.
- **FTS5 full-text search.** New `claims_fts` virtual table (`unicode61` tokenizer, diacritics folded) kept in lockstep with `claims` via three triggers (INSERT / DELETE / UPDATE OF text). New `graph.search(query, ...)` method ranks by FTS5 score; supports FTS5 query grammar (phrase, prefix, boolean, NEAR). Pure-wildcard queries refused. Same per-row projection as `query()`.
- **`mareforma.restore(project_root)`, claims.toml rebuild for catastrophic-loss recovery.** Fresh-only (refuses if `graph.db` has any claims), fail-all-or-nothing on signature verification. Validators verified first (every enrollment envelope against its parent key), then claims (every `signature_bundle` against the enrolled signer, every `validation_signature` against the validator). The CLI surface is `mareforma restore [path/to/claims.toml]`. New `RestoreError` with a `.kind` field naming the failure mode: `graph_not_empty`, `toml_not_found`, `toml_malformed`, `enrollment_unverified`, `claim_unverified`, `mode_inconsistent`, `orphan_signer`. Adversarial test class proves the round-trip catches tampered text, mutated signature bytes, missing signatures in signed-mode graphs, orphan signers, and validator-row tampering.
- **`EpistemicGraph.convergence_errors`**, read-only int property that mirrors swallowed SQLite errors from `_maybe_update_replicated`. Convergence detection runs after every successful INSERT and swallows trigger errors so a misconfigured trigger or contention pattern cannot crash a write; a WARNING is logged, and this counter now ticks alongside it so the failure is observable without log parsing. Resets per `mareforma.open()`.
- **`EpistemicGraph.find_dangling_supports()`**, returns `[{"claim_id", "dangling_ref"}, ...]` for every UUID-shaped entry in some claim's `supports[]` whose referenced claim does not exist in the graph. DOIs and other free-form strings are external references and are NOT flagged. REPLICATED detection already refuses to promote on a dangling reference; this helper is for auditing integrity, not enforcement.
- **`EpistemicGraph.refresh_all_dois()`**, force-re-resolves every DOI referenced anywhere in the graph, bypassing the 30-day positive cache. Use when you suspect a referenced DOI has been retracted. Returns `{checked, still_resolved, now_unresolved, newly_failed}`, `newly_failed` is the count of DOIs whose cache state flipped from resolved to unresolved (the drift signal operators usually want). Does NOT mutate `support_level` or per-claim `unresolved` flags; re-running a HEAD check is not strong enough evidence to demote across the trust ladder.
- **`EpistemicGraph.health()`**, single-call audit summary aggregating substrate counters: `{claim_count, validator_count, unsigned_claims, unresolved_claims, dangling_supports, convergence_errors, convergence_retry_pending}`. Pure observability over existing surfaces, no side effects. A "healthy" graph has zeros across the drift counters; non-zero values do not by themselves indicate a defect, they indicate something the operator should look at.
- **`EpistemicGraph.refresh_convergence()`**, retry convergence detection (PRELIMINARY → REPLICATED) for every claim flagged `convergence_retry_needed=1`. Returns `{checked, promoted, still_pending}`. Convergence detection runs after every successful claim INSERT; when a SQLite trigger or contention pattern causes it to raise, the substrate swallows the error so writes never crash, logs a WARNING, and flips the per-claim retry flag. Without `refresh_convergence`, a swallowed error left the claim stuck at PRELIMINARY forever.
- **`EpistemicGraph.classify_supports(values)`**, three-way classifier for `supports[]` / `contradicts[]` entries: `claim` (strict-v4 UUID, candidate graph-node edge), `doi` (Crossref/DataCite syntax, external citation), `external` (anything else, stored verbatim). Returns `[{"value", "type"}, ...]` in input order. Pure-function (no network, no DB read). Same classification the substrate uses internally for cycle detection, REPLICATED anchoring, and dangling-reference audit.
- **`EpistemicGraph.validate(claim_id, evidence_seen=[...])`**, `evidence_seen` keyword now accepted on validation. The substrate verifies every cited entry is a strict-v4 UUID matching an existing claim with `created_at <= validated_at`, then binds the list (defaults to `[]` for the "I reviewed nothing" admission) into the signed validation envelope. The signed payload of every validation event now binds `(claim_id, validator_keyid, validated_at, evidence_seen)`. The validator's enumeration is self-declared, but the envelope shifts "a human pressed a button" to "a human pressed a button AND named the evidence they consulted." New `EvidenceCitationError` exception.
- **Rekor saga + `rekor_inclusions` sidecar.** Two-write saga closes the divergence window where Rekor would have a public record of a claim but the local row still said `transparency_logged=0`. Step 3 (sidecar INSERT) persists the Rekor coords BEFORE step 4 (the claims-row UPDATE). When step 4 fails, `refresh_unsigned()` reads the sidecar and replays the UPDATE from stored coords instead of re-submitting, no duplicate Rekor entry. The drift guard applies uniformly to both replay and re-submit paths: a tampered row cannot launder a stale signature through either path. New table `rekor_inclusions`, helpers `_record_rekor_inclusion` / `get_rekor_inclusion`. **`rekor_inclusions_append_only`** and **`rekor_inclusions_no_delete`** triggers refuse UPDATE and DELETE on every row, mirroring the verdict-table protections; the sidecar write uses `INSERT ON CONFLICT(claim_id) DO NOTHING` so legitimate retries are crash-free and a SQL-writer cannot launder forged Rekor coords through the recovery path.
- **Validation envelope / `evidence_seen` kwarg agreement gate.** `db.validate_claim` now decodes the supplied validation envelope, extracts its `evidence_seen` field, and refuses the call if it disagrees with the `evidence_seen` kwarg. The substrate validates what the caller passes, without this gate, a direct `db.validate_claim` caller could embed a fraudulent populated list in the signed envelope on disk while passing an empty kwarg, persisting an envelope claiming citations the substrate never validated. Closes the gap between "what the substrate validated" and "what the on-disk envelope claims."
- **Strict UUIDv4 in `_CLAIM_ID_RE`.** The substrate's claim_id pattern now requires the version nibble (`4`) and the RFC 4122 variant nibble (`{8, 9, a, b}`). Non-v4 UUID-shapes in `supports[]` (v1/v3/v5/zero UUIDs) are no longer treated as graph-node candidates, they fall through to the `external` bucket, matching how DOIs are handled. Tightening from the looser "any hex-shape" pattern makes the shape-vs-version check explicit instead of accidental. New `SUPPORT_TYPE_CLAIM`/`SUPPORT_TYPE_DOI`/`SUPPORT_TYPE_EXTERNAL` constants and `classify_support` helper.
- **JSON-LD export emits typed buckets.** Each claim node now carries `supportsClaim` / `supportsDoi` / `supportsReference` (and `contradictsClaim` / `contradictsDoi` / `contradictsReference`) alongside the flat `supports` / `contradicts` arrays. The flat arrays stay byte-identical to what was signed (canonical_statement digest still matches); the typed buckets are a derived view a downstream consumer can route on. New `mare:` predicates declared in the `@context`.
- **End-to-end verifiable Rekor inclusion (opt-in).** Pass `rekor_log_pubkey_pem=...` (or `rekor_log_pubkey_path=...`) to `mareforma.open()` and every signed-claim submit + every `refresh_unsigned()` re-fetches the entry and cryptographically verifies the RFC 6962 Merkle audit path against the log's signed checkpoint. Closes the gap left by submit-time response binding alone (which proves "Rekor returned an entry recording OUR hash + signature" but NOT "the log committed the entry and didn't mutate / remove / reposition it after"). Verification failure refuses to set `transparency_logged=1`; the claim stays at `transparency_logged=0` and `refresh_unsigned()` retries. The supplied pubkey is persisted to `.mareforma/rekor_log_pubkey.pem` as a TOFU pin, silent rotation is refused (delete the pin file to rotate). Supports Ed25519 (private Rekor deployments) and ECDSA secp256r1 (Sigstore public-good Rekor) log keys; other curves and RSA refuse with `unsupported_key`. New `mareforma.signing.RekorInclusionError` exception (re-exported at top-level) with a stable `.reason` token taxonomy (`missing_proof`, `malformed_proof`, `bad_root_hex`, `bad_proof_hex`, `merkle_root_mismatch`, `checkpoint_missing`, `checkpoint_malformed`, `checkpoint_root_mismatch`, `checkpoint_unsigned`, `checkpoint_bad_sig`, `unsupported_key`). New public helpers in `mareforma.signing`: `verify_merkle_inclusion_proof` (RFC 6962 §2.1.1 path walk, handles unbalanced trees), `compute_rekor_leaf_hash`, `parse_rekor_checkpoint` (Sigsum-style signed note), `verify_rekor_checkpoint`, `verify_rekor_inclusion` (end-to-end), `fetch_inclusion_proof` (re-fetch by uuid), `fetch_log_pubkey` (TOFU fetcher). Restore-time verification of stored proofs is on the deferred-features backlog, it needs the `rekor_inclusions` sidecar round-tripped through `claims.toml`.
- **Hardening on the Rekor verification surface** (post-implementation review). Nine findings closed in one pass: (1) `verify_rekor_inclusion`'s base64 fallback now re-raises the documented `RekorInclusionError` instead of leaking the raw decode exception; (2) `refresh_unsigned()`'s re-submit path writes the `rekor_inclusions` sidecar BEFORE calling `mark_claim_logged` (without this, a row UPDATE failure would leave the entry in Rekor with no local record and the next refresh would create a duplicate); (3) ECDSA log keys are restricted to secp256r1 with `unsupported_key` for other curves; (4) TOFU pin comparison uses canonical DER bytes, not stripped PEM text, so two semantically identical keys with different line-wrap width or LF/CRLF endings no longer raise spurious mismatch errors; (5) first-pin write is atomic via `O_CREAT|O_EXCL`, two concurrent `mareforma.open()` calls with different keys can no longer silently overwrite each other's pin; (6) `fetch_inclusion_proof` validates the uuid against a hex regex before URL substitution (a hostile Rekor cannot smuggle `?` / `#` / path-traversal characters into the GET URL); (7) `fetch_inclusion_proof` and `fetch_log_pubkey` both re-validate `rekor_url` against the SSRF / scheme defense at function entry rather than relying on the call graph; (8) `inclusionProof.logIndex` and `treeSize` strict-parse, floats and bools surface as `malformed_proof` rather than misleading `merkle_root_mismatch`; (9) checkpoint parser rejects CR characters in the body half with `checkpoint_malformed` (a proxy rewriting LF→CRLF no longer surfaces as the more misleading `checkpoint_bad_sig`).

### Changed

- **RFC 8785-strict canonicalization.** `mareforma/_canonical.py` now uses the `rfc8785` PyPI library instead of Python's stdlib `json.dumps`. Output is byte-identical for the current schema (no float fields), so every existing signed claim re-verifies, verified by 7 new tests in `tests/test_canonical.py::TestRfc8785NumberRules` and `TestRfc8785ByteCompatWithNoFloats`. The forward-looking property: when a future schema introduces a float field, any RFC 8785-conformant verifier in another language (Go, Rust, JS) reads the same bytes. NaN/Infinity rejection preserved. NFC normalization still applied. Added `rfc8785>=0.1` to runtime deps (5 → 6).
- **`_VALIDATION_FIELDS` extended with `evidence_seen`.** Every validation envelope signed before this change no longer re-verifies. Pre-1.0 substrate evolves in place; the v0.2.x → v0.3.0 "delete `graph.db`" posture covers this.
- **`_backup_claims_toml` extended with `convergence_retry_needed`.** The audit flag is preserved across restore so the operator's TODO list of "claims whose convergence detection still needs a retry" doesn't reset to empty on a rebuild.
- **`refresh_unsigned()` replay path verifies drift first.** When a sidecar inclusion exists for a claim, the replay path now goes through the same drift guard the re-submit path uses. A tampered row cannot attach valid Rekor coords to invalid payload bytes through either path.

- `mareforma status`, rewritten to show epistemic health by support level (red/yellow/green); no pipeline dependency
- `mareforma export`, rewritten to produce claims-only JSON-LD
- `mareforma claim` group: added `validate` subcommand; `--generated-by` default changed from `"human"` to `"agent"`
- `generated_by` default unified to `"agent"` across `db.add_claim`, CLI, and schema DDL
- `claims.toml` format extended with a `[validators]` section (signed enrollment envelopes round-trip through restore). Old files with no `[validators]` section continue to work and are read as unsigned-mode.
- `_ENROLLMENT_FIELDS` extended to include `validator_type`, existing enrollment envelopes signed before this change no longer re-verify. Pre-1.0 substrate evolves in place; test fixtures were updated, no migration is provided.
- `_backup_claims_toml` failure logged to stderr at ERROR level (was `warnings.warn`, which production loggers routinely suppress). graph.db remains authoritative; this only changes the visibility of a divergence between the two.

### Removed

- `@transform` decorator and `BuildContext`, pipeline layer removed
- `MareformaObserver`, `LangChainAdapter`, execution tracing removed
- Pipeline CLI commands: `init`, `add-source`, `explain`, `build`, `log`, `diff`, `cross-diff`, `trace`
- `MareformaError` moved from `registry.py` into `db.py`

## [0.2.1] - 2026-05-08

### Added
- `ctx.params`, runtime parameter injection from TOML for transforms
- `query_claims()`, read primitive for the epistemic graph
- `delete_claims_by_generated_by()`, delete claims by their source agent

### Fixed
- `LangChainAdapter` updated to use `langchain_core.callbacks.base` (replaces deprecated import path)
- Blank line after each transform's done line in build output

### Changed
- Removed verbose logging of recorded claims in `BuildContext`

## [0.2.0] - 2026-04-08

### Added
- `mareforma.agent`, framework-agnostic agent provenance module
- `AgentEvent`, canonical dataclass for one AI scientist provenance event (LLM call, tool call, chain step, or custom)
- `MareformaObserver`, context manager that records `AgentEvent`s to `graph.db` (`agent_events` table) and full payloads to `.mareforma/artifacts/agent_payloads/`; works with any AI scientist framework
- `mareforma.agent.adapters.langchain.LangChainAdapter`, LangChain `BaseCallbackHandler` adapter; hooks `on_llm_start/end/error`, `on_tool_start/end/error`, `on_chain_end/error`
- `mareforma agent-log [run_id]`, CLI command to inspect recorded agent events
- `ctx.root` and `ctx.run_id` public properties on `BuildContext`

### Changed
- `open_db()` now uses `check_same_thread=False`, safe for LangChain's background callback threads under WAL mode

## [0.1.0] - 2026-03-25

### Added
- `@transform` decorator, wrap any Python function to capture provenance automatically
- SQLite epistemic graph (`graph.db`) storing transform runs, artifacts, claims, and evidence links
- `ctx.save()`, save intermediate artifacts per run with sha256 hashing
- `ctx.claim()`, assert scientific claims from inside a transform, linked to the current run
- `mareforma build`, execute all transforms, resolving the DAG
- `mareforma cross-diff TRANSFORM_A TRANSFORM_B`, compare latest runs of two transforms by artifact, showing SAME / CHANGED / ONLY_IN_A / ONLY_IN_B per artifact and attached claims
- `mareforma diff <transform>`, compare the two most recent runs of a single transform
- `mareforma status`, epistemic health dashboard with traffic-light (green/yellow/red), claim counts, unclaimed transforms, and confidence breakdown (`--json`)
- `mareforma trace <transform>`, ASCII ancestry tree showing transform class and support level (`--json`)
- `mareforma claim` command group: `add`, `list`, `show`, `update`
- `mareforma log`, run history
- `mareforma init`, scaffold a new mareforma project
- `mareforma add-source`, register a data source
- Automatic transform classification: RAW / PROCESSED / ANALYSED / INFERRED
- Epistemic distance: BFS over transform DAG weighted by class
- Support levels: SINGLE → REPLICATED → CONVERGED → CONSISTENT → ESTABLISHED
- `claims.toml` auto-backup, survives `graph.db` deletion, committed to git
- `ontology.jsonld` export, JSON-LD with `schema.org`, `prov`, and `mare` terms
- Schema versioning, databases auto-initialise on first use; future versions migrate automatically
- MEDEA example (`examples/ai_agent_drug_target/`), wraps the MEDEA AI scientist in `@transform` to compare drug target findings across diseases with `cross-diff`
