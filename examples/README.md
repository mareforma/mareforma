# Examples

Runnable mareforma examples, from API basics to real AI scientists.

| # | Example | What it shows |
|---|---------|---------------|
| 01 | [API Walkthrough](01_api_walkthrough/) | Full API: open(), assert_claim(), query(), idempotency, the stored support-level ladder (REPLICATED / ESTABLISHED labels deprecated for v0.4.0), validate(), anti-patterns |
| 02 | [Compounding Agents](02_compounding_agents/) | Two agents working sequentially: findings compound instead of evaporating; a finding whose analysis step never ran reads UNGROUNDED, and same-model checks answered from a producer-supplied transport leave effective independence UNVERIFIABLE rather than counted |
| 03 | [Documented Contestation](03_documented_contestation/) | Agent challenges an ESTABLISHED finding with stronger methodology; both coexist in the graph |
| 04 | [Private Data, Public Findings](04_private_data_public_findings/) | Two labs share provenance traces without sharing raw data; effective independence separates real corroboration from distinct signatures alone |
| 05 | [Drug Target Provenance](05_drug_target_provenance/) | AI scientist drug target identification: ANALYTICAL vs INFERRED distinguishes real data from LLM prior |
| 06 | [Verify in CI](06_ci_verify/) | `mareforma verify` as a GitHub Actions gate, keyed on stable exit codes (0 verified / 1 tampered / 2 unverifiable / 3 usage error) |
| 07 | [Silent Failure Catch](07_silent_failure_catch/) | The minimal grounding catch: two pipelines print the same number, `mareforma diagnose` reads GROUNDED for the one that read its data and UNGROUNDED for the one that silently fell back |
