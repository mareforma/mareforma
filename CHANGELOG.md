# Changelog

All notable changes to this project will be documented in this file.

## [0.3.0] - 2026-05-12

Breaking change from v0.2.x. No migration path — delete `graph.db` to start
fresh. Claims are backed up in `claims.toml`.

### Added

- Ed25519 claim signing. New `mareforma/signing.py` module: keypair gen + PEM save/load + DSSE-style envelope sign/verify. Private key lives at `~/.config/mareforma/key` (XDG-compliant, mode 0600). Public-key id is SHA-256 of the raw Ed25519 public bytes.
- `mareforma bootstrap` CLI command: one-time identity setup. Generates a fresh keypair, prints the public-key id. Refuses to overwrite an existing key unless `--overwrite` (avoids orphaning every previously-signed claim).
- `mareforma.open(key_path=..., require_signed=...)` parameters. When a key exists at the XDG path (or `key_path`), claims are automatically signed before INSERT and the envelope is persisted to a new `signature_bundle` TEXT column. `require_signed=True` raises `KeyNotFoundError` if no key is found — high-assurance opt-in.
- Signed payload binds `claim_id`, `text`, `classification`, `generated_by`, `supports`, `contradicts`, `source_name`, `created_at`. Any tamper with the row breaks verification.
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
- `validate_rekor_url` now rejects DNS-shortcut SSRF bypasses: `localhost`, `localhost.localdomain`, `ip6-localhost`, `ip6-loopback`, and numeric-only hostnames (`127.1`, `2130706433`, `0177.0.0.1`). Python's `ipaddress.ip_address` rejects these forms but `socket.getaddrinfo` resolves them to loopback — the gap that defeated the first SSRF defense pass.
- `envelope_payload` raises `InvalidEnvelopeError` when the decoded payload is not a JSON object (was: bare JSON string/list/number passed through, then crashed downstream callers with `AttributeError` on `payload.get(...)`).
- `save_private_key(exclusive=True)` unlinks the file on a mid-write `OSError` so the next bootstrap retry can succeed instead of hitting a misleading "key already exists" on a zero-byte leftover.
- `submit_to_rekor` now streams the Rekor response via `httpx.stream(...)` with a running-byte accumulator that aborts at 64 KB during the read — a hostile registry can no longer cost 100 MB of RSS before the size cap fires.
- `submit_to_rekor` compares signatures by decoding both sides to raw bytes (`base64.urlsafe_b64decode`, which transparently accepts standard and URL-safe alphabets, with or without padding). Wire-equivalent base64 representations from real Rekor instances no longer false-reject.
- `mareforma bootstrap --overwrite` help text and `signing.bootstrap_key` docstring now call out the destructive consequence: every signed-but-not-yet-Rekor'd claim becomes permanently un-loggable when the prior key is gone. Documented safe rotation path: back up the old key, `refresh_unsigned()` to drain the pending queue, then rotate.
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
