---
name: analyze-dbx-job
description: Analyze Azure Databricks job/cluster metrics for cost-aware, right-sized configuration (D/E/F family, workers/autoscale, topology).
agent: databricks-job-run-optimizer
argument-hint: Paste job-run metrics JSON and optional pipeline or cluster configuration context
---

## Input
The user will provide one or more of the following:
 * Databricks job or cluster run metrics (JSON or text)
 * Autoscale configuration (min/max workers)
 * Optional pipeline context (YAML, SQL, notebook settings, or narrative)
Metrics may be partial or from a single run.

## Task
Use the **databricks-efficiency** skill for **cost and configuration fit** from metrics (**Objective** and **Procedure**: family/SKU first, workers/autoscale second, **`single_node`** last resort).

## Guidance
 * Treat **current run metrics** as the primary source of truth.
 * Use historical summaries only if explicitly provided.
 * Follow **Procedure** in the skill (family/SKU first) **do not** defer **D/E/F** or SKU sizing when metrics support a better fit.
 * For **vm_family D/E**, **`ads`/`ds`/`s`** ordering applies **per skill** **Eligible-SKU naming order** (**size band**: same nominal size digits + **`_vN`**, preferably from **`system.compute.node_types`** vCPU/RAM rows). Highest **`_vN`** among **literal** **`eligible`** strings **first**, then **`ads` → `ds` → `s`**. **`azure_node_type`** must match **allow-list ∩ `system.compute.node_types`** verbatim.
 * Enforce Databricks availability first: recommended `azure_node_type` must come from (allow-list ∩ workspace/region `system.compute.node_types`). Apply generation/local-temp preferences only after this eligibility filter and only as ranking tie-breakers.
 * Include workspace/region `system.compute.node_types` evidence in the input whenever possible. If missing, avoid unverified SKU changes and use `INSUFFICIENT_EVIDENCE` with notes.
 * Do not infer single-node topology from autoscale settings alone.
 * Interpret **`workflow_task_count`**, **allocated-vs-utilized** fields, percentages (`_pct`), and percentiles (`p95_` / `p99_`) **only** as defined in **databricks-efficiency** (input uses the flat ingest names from that skill; no ingest version field expected).
 * Do not assume prompts create files or directories; engineers manually save generated JSON under `copilot-results/` per repo conventions.

## Output
Return **strict JSON only** as defined in **databricks-efficiency**, **Output Expectations** (output contract plus sample schema). Prefer raw JSON **without** surrounding prose when the chat UI supports it. Use **rich** **`comparison.rationale`** (multiple bullets, ingest keys, why-not alternatives) and substantive **`analysis_summary`** text per the skill.
