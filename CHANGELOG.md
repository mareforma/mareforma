# Changelog

All notable changes to this project will be documented in this file.

## [0.3.0] - 2026-05-13

Breaking change from v0.2.x. Schema does not migrate from older
versions; delete `.mareforma/graph.db` to start fresh. Claims are
backed up in `claims.toml` for restoration.

Full P0+P1 substrate per the 2026-05-12 security-substrate decision:
- **P0.1** Ed25519 signing + Sigstore-Rekor
- **P0.2** Artifact-hash gate on REPLICATED
- **P0.3** Identity-gated `validate()` with validator enrollment
- **P0.4** DOI resolution + cache
- **P1.5** DB-layer state-machine triggers + append-only hash chain
- **P1.6** Simple-DFS cycle / self-loop detection on supports[]
- **P1.7** ESTABLISHED-upstream requirement for REPLICATED + seed-claim bootstrap
- **P1.8** Path A: JSON-LD export renamed to mareforma-native vocabulary
- **P1.9** SCITT-style signed export bundle + verify CLI

### Added

- Ed25519 claim signing. New `mareforma/signing.py` module: keypair gen + PEM save/load + DSSE-style envelope sign/verify. Private key lives at `~/.config/mareforma/key` (XDG-compliant, mode 0600). Public-key id is SHA-256 of the raw Ed25519 public bytes.
- `mareforma bootstrap` CLI command: one-time identity setup. Generates a fresh keypair, prints the public-key id. Refuses to overwrite an existing key unless `--overwrite` (avoids orphaning every previously-signed claim).
- `mareforma.open(key_path=..., require_signed=...)` parameters. When a key exists at the XDG path (or `key_path`), claims are automatically signed before INSERT and the envelope is persisted to a new `signature_bundle` TEXT column. `require_signed=True` raises `KeyNotFoundError` if no key is found — high-assurance opt-in.
- Signed payload binds `claim_id`, `text`, `classification`, `generated_by`, `supports`, `contradicts`, `source_name`, `artifact_hash`, `created_at`. Any tamper with the row breaks verification.
- `artifact_hash` parameter on `assert_claim` (Python API) and `--artifact-hash` flag on `mareforma claim add` (CLI). Accepts a SHA256 hex digest of the output bytes (figure, CSV, model) backing the claim. Normalised to lowercase, validated as 64-char hex, persisted to a new `artifact_hash TEXT` column and bound into the signed payload. Restores the v0.1 artifact-hashing discipline that was dropped in v0.2.
- **Signed-payload change.** `canonical_payload` now always emits an `artifact_hash` key (`null` when absent), so envelopes signed before this commit on the v0.3.0 dev branch no longer re-derive byte-for-byte. Any signed claim from a pre-P0.2 v0.3.0 dev checkout must be re-asserted on a fresh `graph.db`. v0.2.x → v0.3.0 already requires a fresh `graph.db`, so end-users on a tagged release are unaffected.
- REPLICATED detection now consults `artifact_hash` as a parallel signal. When two converging peers BOTH supply a hash, the hashes must match for `REPLICATED` to fire. When either side omits the hash, the gate is bypassed and identity-only `REPLICATED` applies — the signal is opt-in, not retroactive.
- New `IdempotencyConflictError` raised when `add_claim` replays the same `idempotency_key` with a different `artifact_hash` (in either direction, including hash-then-omit). Silently returning the first claim_id would let a caller believe their new hash was registered when it was not, defeating tamper-evidence. Use a different `idempotency_key` or omit the conflicting field.
- `idx_claims_artifact_hash` partial index (only rows with a non-NULL hash) accelerates the REPLICATED query without bloating the index for users who don't supply hashes.
- New `mareforma.prompt_safety` module + `EpistemicGraph.query_for_llm()` method. Sanitize-and-wrap helpers for feeding retrieved claim text into an LLM prompt. Strips zero-width / bidi-override / C0-C1 control characters, caps oversized fields at 100k chars with a visible truncation marker, and wraps free-text fields (`text`, `comparison_summary`) in `<untrusted_data>...</untrusted_data>` delimiters. Forged opening/closing tags inside the content are replaced with `[stripped]` so a hostile claim cannot break out of the wrapper (case-insensitive, whitespace-tolerant). The metadata labels (`source_name`, `generated_by`, `validated_by`) are sanitized but not wrapped. `mareforma.sanitize_for_llm`, `mareforma.wrap_untrusted`, and the composed `mareforma.safe_for_llm` are public for one-off use.
- Stripping also covers known steganographic prompt-injection vectors: the Unicode "tags" plane (U+E0000-E007F) used by Goodside-style ASCII-smuggler attacks, variation selectors (U+FE00-FE0F, U+E0100-E01EF, U+180B-180D), interlinear annotation anchors (U+FFF9-FFFB), and the fullwidth `<`/`>`/`/` lookalikes (U+FF1C/E/F) that could survive both sanitize and wrap if a downstream NFKC normaliser folds them to ASCII.
- New `SECURITY.md` documents the disclosure channel (GitHub Private Vulnerability Reporting), supported-versions policy (latest pre-1.0 only), PyPI Trusted Publishing setup, cryptographic trust boundaries, and out-of-scope categories. **Operator note:** Private Vulnerability Reporting must be enabled in repo Settings → Security for the referenced URL to work.
- `EpistemicGraph.get_tools()` now routes through `query_for_llm` internally. The `query_graph` tool that ships to LangChain / LangGraph / CrewAI / AutoGen / LlamaIndex / PydanticAI / Smol Agents / OpenAI SDK previously returned raw claim text — a stored prompt-injection planted by a prior agent would have been delivered verbatim to the consuming LLM. The tool now returns `text` wrapped in `<untrusted_data>...</untrusted_data>` with sanitization applied, matching the documented safe-retrieval contract.
- Sanitize-on-write at the DB layer: `assert_claim` runs `prompt_safety.sanitize_for_llm(text)` before signing and persisting. Defense in depth — any consumer that reads `claim.text` directly (custom analytics, claims.toml restore, third-party tooling) gets a clean string. The signed payload binds the sanitized form, so downstream verifiers see what the LLM sees. Claims that consist entirely of zero-width / control characters are rejected with `ValueError`.
- Hard cap on claim text at 100,000 characters (`_MAX_CLAIM_TEXT_LEN` in `db.py`). Matches the truncation point in `prompt_safety._MAX_FIELD_LEN` so claim text never silently degrades when consumed by an LLM. Multi-MB writes are rejected at `assert_claim` time.
- `.github/workflows/*.yml` first-party actions pinned by commit SHA: `actions/checkout@34e1148…` and `actions/setup-python@a26af69…`. Closes the tag-squat / maintainer-compromise vector against the Trusted Publishing OIDC token. The third-party `pypa/gh-action-pypi-publish` was already SHA-pinned.
- `.github/CODEOWNERS` and `.github/dependabot.yml`. CODEOWNERS documents the required-review surface for the release pipeline and SECURITY.md (operator must enable "Require review from Code Owners" in repo Branch protection rules for enforcement). Dependabot keeps the Action SHAs and Python deps current.
- Sigstore-Rekor transparency-log integration. New `mareforma.open(rekor_url=..., require_rekor=...)` parameters. When a Rekor URL is set, every signed claim is submitted to the transparency log at INSERT time using the `hashedrekord` entry kind; the entry uuid + logIndex are attached to the bundle and `transparency_logged` flips to 1. Submission failure persists the claim with `transparency_logged=0` and blocks REPLICATED promotion — mirroring the DOI `unresolved` pattern.
- New `EpistemicGraph.refresh_unsigned()` retries Rekor submission for every signed-but-unlogged claim. Mirrors `refresh_unresolved()`. Returns `{checked, logged, still_unlogged}`.
- REPLICATED detection now requires `transparency_logged = 1` alongside `unresolved = 0`. Unsigned claims and Rekor-disabled mode (no `rekor_url`) keep the default `transparency_logged=1`, so they REPLICATE unchanged.
- `transparency_logged INTEGER NOT NULL DEFAULT 1 CHECK(IN (0,1))` column on the claims table + `idx_claims_transparency_logged` index.
- `mareforma.signing.PUBLIC_REKOR_URL` constant points to the public sigstore Rekor instance for users who want it without typing the URL.
- Signed-claim append-only invariant. `update_claim` raises `SignedClaimImmutableError` when asked to mutate `text` / `supports` / `contradicts` on a claim with a non-NULL `signature_bundle`. Mutating signed-surface fields would silently invalidate the signature without surfacing the change. To revise a signed claim, retract it (`status='retracted'`) and assert a new one citing the old via `contradicts=[<old_claim_id>]`. `status` and `comparison_summary` remain editable since neither is part of the signed payload.
- `submit_to_rekor` now verifies the Rekor response actually records OUR submission: the encoded `entry.body` is base64-decoded, parsed, and its `spec.data.hash.value` and `spec.signature.content` must match what we sent. A hostile or buggy registry can no longer hand back an arbitrary uuid/logIndex that mareforma accepts as proof of inclusion.
- `submit_to_rekor` caps Rekor responses at 64 KB (both the `Content-Length` header and the actually-received bytes). A multi-MB JSON blob from a hostile endpoint can no longer land in `graph.db` and amplify through every subsequent backup.
- `mareforma.open(rekor_url=...)` validates the URL at open() time: only `https://` is accepted, and loopback / private RFC1918 / link-local IP literals are rejected. `mareforma.open(trust_insecure_rekor=True)` is the explicit opt-out for internal Rekor instances on private networks.
- `refresh_unsigned` drift + key-rotation guards. Before re-submitting a stored envelope to Rekor, the canonical payload bytes are compared against the live row's signed fields — a tampered row is quarantined as still-unlogged with a warning rather than cementing a stale signature in the public log. Likewise, an envelope whose keyid does not match the graph's current signer (key was rotated since assert_claim) is skipped with a warning instead of retrying forever.
- `mark_claim_logged` decodes the supplied bundle and verifies its payload's `claim_id` matches the row before writing. A buggy caller cannot silently write Alice's bundle onto Bob's row.
- `save_private_key` chmods the leaf parent directory to `0o700` on POSIX so the per-user mareforma config directory is not enumerable by other local users. `bootstrap_key` now uses `O_CREAT|O_EXCL` for the no-overwrite path: two concurrent bootstraps can no longer both pass an `exists()` check and race to overwrite each other.
- `load_private_key` emits a `UserWarning` on non-POSIX platforms (Windows etc.) where file-mode bits are largely advisory and mareforma does not configure ACLs.
- `add_claim` now warns when Rekor accepts the submission but the local follow-up UPDATE fails — operators can no longer miss the `transparency_logged=0` divergence and learn that running `refresh_unsigned()` will reconcile.
- `mareforma.signing.SIGNED_FIELDS` and `mareforma.signing.canonical_payload(...)` are now public so verifiers can independently re-derive the bytes that should be signed.
- `validate_rekor_url` now rejects DNS-shortcut SSRF bypasses: `localhost`, `localhost.localdomain`, `ip6-localhost`, `ip6-loopback`, and numeric-only hostnames (`127.1`, `2130706433`, `0177.0.0.1`). Python's `ipaddress.ip_address` rejects these forms but `socket.getaddrinfo` resolves them to loopback — a DNS-shortcut SSRF gap that bypassed the earlier IP-literal-only check.
- `envelope_payload` raises `InvalidEnvelopeError` when the decoded payload is not a JSON object (was: bare JSON string/list/number passed through, then crashed downstream callers with `AttributeError` on `payload.get(...)`).
- `save_private_key(exclusive=True)` unlinks the file on a mid-write `OSError` so the next bootstrap retry can succeed instead of hitting a misleading "key already exists" on a zero-byte leftover.
- `submit_to_rekor` now streams the Rekor response via `httpx.stream(...)` with a running-byte accumulator that aborts at 64 KB during the read — a hostile registry can no longer cost 100 MB of RSS before the size cap fires.
- `submit_to_rekor` compares signatures by decoding both sides to raw bytes (`base64.urlsafe_b64decode`, which transparently accepts standard and URL-safe alphabets, with or without padding). Wire-equivalent base64 representations from real Rekor instances no longer false-reject.
- `mareforma bootstrap --overwrite` help text and `signing.bootstrap_key` docstring now call out the destructive consequence: every signed-but-not-yet-Rekor'd claim becomes permanently un-loggable when the prior key is gone. Documented safe rotation path: back up the old key, `refresh_unsigned()` to drain the pending queue, then rotate.
- Identity-gated validation. `graph.validate()` now requires a loaded signing key AND that key must be enrolled in the project's `validators` table. The first key opened against a fresh graph auto-enrolls as the root validator (silent self-signed enrollment). Additional validators are added via `mareforma validator add --pubkey ... --identity ...` (CLI) or `mareforma.validators.enroll_validator(...)` (library). Removal is intentionally unsupported in v0.3.0 — validator history is append-only.
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
- `mareforma.open()` — returns `EpistemicGraph`; no `@transform` required
- `EpistemicGraph.assert_claim()` — assert claims directly from any agent
- `EpistemicGraph.query()` — query by text, support level, or classification
- `EpistemicGraph.get_claim()` — fetch a single claim by ID
- `EpistemicGraph.validate()` — human gate to ESTABLISHED
- `mareforma claim validate` — CLI command to promote REPLICATED → ESTABLISHED; `--validated-by` optional
- DOI resolution: every DOI in `supports[]`/`contradicts[]` is HEAD-checked against Crossref and DataCite at assert time. Unresolved DOIs mark the claim `unresolved=True` and block REPLICATED promotion. `EpistemicGraph.refresh_unresolved()` retries previously-failed resolutions.
- DOI resolver network contract: DOI suffix URL-encoded before interpolation (prevents host injection via `#`/`@`, preserves inner `/` for hierarchical suffixes like `10.1093/imamat/35.3.337`); `follow_redirects=False` (registry must answer directly); pooled `httpx.Client` with `User-Agent` and threading lock around lazy init (Crossref polite-pool, FD-leak-safe under concurrency); HTTP 429 from EITHER registry skips the cache write (a registry-wide throttling event no longer poisons the cache for 24h); tight exception clause (`httpx.HTTPError`, `httpx.InvalidURL`, `OSError`) so programmer bugs surface in tracebacks instead of silently becoming "unresolved".
- `doi_cache` table: persistent cache of DOI resolution results to avoid repeated network calls. TTLs: 30 days for resolved entries, 24 hours for unresolved (so retractions and registry blips self-correct).
- `httpx` is now a required dependency (was `paper` extra)
- `EpistemicGraph.get_tools()` — returns `[query_graph, assert_finding]` as plain Python callables; `generated_by` baked into closure; wraps in one line for any framework
- `mareforma.schema()` — runtime introspection of valid values and state transitions
- Claims schema v1: `classification`, `support_level`, `idempotency_key`, `validated_by`, `validated_at`, `branch_id`, `unresolved`; CHECK constraints on `classification`, `support_level`, `status`, `unresolved`
- Schema validation: `open_db()` enforces an exact column-set match against `_CLAIM_COLUMNS`. Replaces the version-number compare. Missing columns instruct the user to delete `graph.db`; **extras-only** is treated as a downgrade attempt and instructs the user to upgrade mareforma instead.
- `mark_claim_resolved()` is atomic: the unresolved-flag clear and the REPLICATED re-evaluation run in the same SQLite transaction; convergence detection remains best-effort within the transaction (transient lock errors no longer roll back the flag-clear).
- `update_claim()` re-resolves DOIs only when `supports`/`contradicts` actually change (diff-check against prior JSON), and re-runs REPLICATED convergence inside the update transaction when a claim transitions from `unresolved=1` to `0` — otherwise a claim cured via `update_claim` would stay PRELIMINARY forever even with a sibling waiting on it.
- `refresh_unresolved()` quarantines claims with corrupt `supports_json`/`contradicts_json` instead of aborting the entire refresh.
- DOI cache TTL parsing tolerates `Z` UTC suffix as well as `+00:00` (Python 3.10 compatibility for externally-loaded rows).
- REPLICATED auto-trigger: fires automatically when ≥2 claims share the same upstream in `supports[]` with different `generated_by`
- Framework integrations: AGENTS.md table covering Anthropic SDK, OpenAI SDK, LangChain, LangGraph, CrewAI, AutoGen, LlamaIndex, PydanticAI, Smol Agents
- Mintlify docs at `docs.mareforma.com`
- 5 runnable examples (API walkthrough, compounding agents, documented contestation, private data / public findings, MEDEA drug target)
- **P1.5 — DB-layer state-machine + append-only hash chain.** `claims.prev_hash TEXT UNIQUE` column carries a SHA-256 chain (`sha256(prev_chain_link || canonical_payload)`); BEGIN IMMEDIATE + UNIQUE constraint together prevent branched chains. Two `BEFORE` triggers enforce state transitions at the storage layer: insert trigger refuses ESTABLISHED-without-validation, update trigger refuses illegal transitions with translatable `mareforma:state:<from>-><to>` error codes. A separate `BEFORE UPDATE OF status` trigger makes `retracted` terminal (transitions out of retracted are refused — to resurrect a withdrawn finding, assert a new claim citing the old). New CHECK constraint requires `validation_signature` on every ESTABLISHED row. New exceptions `IllegalStateTransitionError` and `ChainIntegrityError`. Defense in depth: a tampered Python interpreter cannot relax the rules.
- **P1.6 — Simple-DFS cycle detection on supports[].** A claim that supports itself (directly or via a chain) is rejected at INSERT (`add_claim`) and at UPDATE (`update_claim` on unsigned claims — signed claims refuse supports mutation upstream of this check via `SignedClaimImmutableError`). Forward-walk DFS with a visited set, depth-capped at 1024 hops. DOI strings in supports[] are not graph nodes and skipped. New `CycleDetectedError` exception. Closes MF-007.
- **P1.7 — ESTABLISHED-upstream requirement for REPLICATED + seed-claim bootstrap.** REPLICATED promotion now requires at least one ESTABLISHED claim in the peer's supports[]. Matches Cochrane/GRADE evidence-chain methodology — stops replication-of-noise. Bootstrap path: `assert_claim(text=..., seed=True)` inserts the claim directly at ESTABLISHED with a signed seed envelope (payload type `application/vnd.mareforma.seed+json`, binds `claim_id + validator_keyid + seeded_at`). Only enrolled validators can produce seed envelopes. Strict by default — no opt-in flag. Closes MF-013.
- **P1.8 Path A — JSON-LD export renamed to mareforma-native vocabulary.** Removed PROV-O references (`prov:wasGeneratedBy`, `prov:used`) from the JSON-LD `@context` — the previous export name-dropped the vocabulary without populating the full PROV-O graph (no `prov:Activity`, no `prov:wasAssociatedWith`, no model identity or prompt/response hashes). The export now declares `@type='mare:Graph'` and `mare:mediaType='application/x-mareforma-graph+json'`. The `used` key on source-bearing claims was renamed to `usedSource` (aliased to `mare:usedSource`). Every `SIGNED_FIELDS` member is always emitted on each claim node so downstream consumers (e.g. P1.9 bundle verification) can re-derive `canonical_payload` from a node alone. Closes MF-008 via honest scoping.
- **P1.9 — SCITT-style signed export bundle + `mareforma verify`.** New `mareforma/export_bundle.py` produces an in-toto Statement v1 wrapper around the JSON-LD export, with `predicateType='urn:mareforma:predicate:epistemic-graph:v1'` and a DSSE-style signature over the whole bundle. Subject names use the `urn:mareforma:claim:<uuid>` namespace; URN (not DNS) avoids a perpetual-ownership commitment on `mareforma.dev`. New CLI: `mareforma export --bundle [-o path]` writes a signed bundle (requires bootstrapped XDG key); `mareforma verify <bundle.json>` checks the bundle DSSE signature AND every per-claim subject digest. New `BundleVerificationError` names the first failing check so callers can route between "corrupt" and "cross-version skew". Closes MF-008 alternate + MF-018 partial.

### Changed

- `mareforma status` — rewritten to show epistemic health by support level (red/yellow/green); no pipeline dependency
- `mareforma export` — rewritten to produce claims-only JSON-LD
- `mareforma claim` group: added `validate` subcommand; `--generated-by` default changed from `"human"` to `"agent"`
- `generated_by` default unified to `"agent"` across `db.add_claim`, CLI, and schema DDL

### Removed

- `@transform` decorator and `BuildContext` — pipeline layer removed
- `MareformaObserver`, `LangChainAdapter` — execution tracing removed
- Pipeline CLI commands: `init`, `add-source`, `explain`, `build`, `log`, `diff`, `cross-diff`, `trace`
- `MareformaError` moved from `registry.py` into `db.py`

## [0.2.1] - 2026-05-08

### Added
- `ctx.params` — runtime parameter injection from TOML for transforms
- `query_claims()` — read primitive for the epistemic graph
- `delete_claims_by_generated_by()` — delete claims by their source agent

### Fixed
- `LangChainAdapter` updated to use `langchain_core.callbacks.base` (replaces deprecated import path)
- Blank line after each transform's done line in build output

### Changed
- Removed verbose logging of recorded claims in `BuildContext`

## [0.2.0] - 2026-04-08

### Added
- `mareforma.agent` — framework-agnostic agent provenance module
- `AgentEvent` — canonical dataclass for one AI scientist provenance event (LLM call, tool call, chain step, or custom)
- `MareformaObserver` — context manager that records `AgentEvent`s to `graph.db` (`agent_events` table) and full payloads to `.mareforma/artifacts/agent_payloads/`; works with any AI scientist framework
- `mareforma.agent.adapters.langchain.LangChainAdapter` — LangChain `BaseCallbackHandler` adapter; hooks `on_llm_start/end/error`, `on_tool_start/end/error`, `on_chain_end/error`
- `mareforma agent-log [run_id]` — CLI command to inspect recorded agent events
- `ctx.root` and `ctx.run_id` public properties on `BuildContext`

### Changed
- `open_db()` now uses `check_same_thread=False` — safe for LangChain's background callback threads under WAL mode

## [0.1.0] - 2026-03-25

### Added
- `@transform` decorator — wrap any Python function to capture provenance automatically
- SQLite epistemic graph (`graph.db`) storing transform runs, artifacts, claims, and evidence links
- `ctx.save()` — save intermediate artifacts per run with sha256 hashing
- `ctx.claim()` — assert scientific claims from inside a transform, linked to the current run
- `mareforma build` — execute all transforms, resolving the DAG
- `mareforma cross-diff TRANSFORM_A TRANSFORM_B` — compare latest runs of two transforms by artifact, showing SAME / CHANGED / ONLY_IN_A / ONLY_IN_B per artifact and attached claims
- `mareforma diff <transform>` — compare the two most recent runs of a single transform
- `mareforma status` — epistemic health dashboard with traffic-light (green/yellow/red), claim counts, unclaimed transforms, and confidence breakdown (`--json`)
- `mareforma trace <transform>` — ASCII ancestry tree showing transform class and support level (`--json`)
- `mareforma claim` command group: `add`, `list`, `show`, `update`
- `mareforma log` — run history
- `mareforma init` — scaffold a new mareforma project
- `mareforma add-source` — register a data source
- Automatic transform classification: RAW / PROCESSED / ANALYSED / INFERRED
- Epistemic distance: BFS over transform DAG weighted by class
- Support levels: SINGLE → REPLICATED → CONVERGED → CONSISTENT → ESTABLISHED
- `claims.toml` auto-backup — survives `graph.db` deletion, committed to git
- `ontology.jsonld` export — JSON-LD with `schema.org`, `prov`, and `mare` terms
- Schema versioning — databases auto-initialise on first use; future versions migrate automatically
- MEDEA example (`examples/ai_agent_drug_target/`) — wraps the MEDEA AI scientist in `@transform` to compare drug target findings across diseases with `cross-diff`
