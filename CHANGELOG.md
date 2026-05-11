# Changelog

All notable changes to this project will be documented in this file.

## [0.3.0] - 2026-05-11

Breaking change from v0.2.x. No migration path — delete `graph.db` to start
fresh. Claims are backed up in `claims.toml`.

### Added

- `mareforma.open()` — returns `EpistemicGraph`; no `@transform` required
- `EpistemicGraph.assert_claim()` — assert claims directly from any agent
- `EpistemicGraph.query()` — query by text, support level, or classification
- `EpistemicGraph.get_claim()` — fetch a single claim by ID
- `EpistemicGraph.validate()` — human gate to ESTABLISHED
- `EpistemicGraph.get_tools()` — returns `[query_graph, assert_finding]` as plain Python callables; `generated_by` baked into closure; wraps in one line for any framework
- `mareforma.schema()` — runtime introspection of valid values and state transitions
- Claims schema v1: `classification`, `support_level`, `idempotency_key`, `validated_by`, `validated_at`, `branch_id`
- REPLICATED auto-trigger: fires automatically when ≥2 claims share the same upstream in `supports[]` with different `generated_by`
- Framework integrations: AGENTS.md table covering Anthropic SDK, OpenAI SDK, LangChain, LangGraph, CrewAI, AutoGen, LlamaIndex, PydanticAI, Smol Agents
- Mintlify docs at `docs.mareforma.com`
- 5 runnable examples (API walkthrough, compounding agents, documented contestation, private data / public findings, MEDEA drug target)

### Changed

- `mareforma status` — rewritten to show epistemic health by support level (red/yellow/green); no pipeline dependency
- `mareforma export` — rewritten to produce claims-only JSON-LD
- `mareforma claim` group unchanged

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
