# Examples

Runnable mareforma examples, from API basics to real AI scientists.

| # | Example | What it shows |
|---|---------|---------------|
| 01 | [API Walkthrough](01_api_walkthrough/) | Full API: open(), assert_claim(), query(), idempotency, support levels, validate(), anti-patterns |
| 02 | [Compounding Agents](02_compounding_agents/) | Two agents working sequentially: findings compound instead of evaporating; two same-model checks do not converge to an independence claim, while two lineage-distinct lines read CONVERGENT, a structural convergence marker with cross-model error correlation named as the residual |
| 03 | [Documented Contestation](03_documented_contestation/) | Agent challenges a CONVERGENT finding with stronger methodology; both coexist in the graph |
| 04 | [Private Data, Public Findings](04_private_data_public_findings/) | Two labs share provenance traces without sharing raw data; effective independence separates real corroboration from distinct signatures alone |
| 05 | [Drug Target Provenance](05_drug_target_provenance/) | AI scientist drug target identification: ANALYTICAL vs INFERRED distinguishes real data from LLM prior |
| 06 | [Verify in CI](06_ci_verify/) | `mareforma verify` as a GitHub Actions gate, keyed on stable exit codes (0 verified / 1 tampered / 2 unverifiable / 3 usage error) |
