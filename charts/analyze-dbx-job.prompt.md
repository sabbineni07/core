---
name: analyze-dbx-job
description: Analyze Azure Databricks job and cluster efficiency with emphasis on autoscale over-provisioning, per-node utilization, Azure VM family fit (D / E / F), and single-node versus multi-node topology.
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
Use the **databricks-efficiency** skill to analyze the provided metrics and determine the safest cost- and performance-aware recommendation for cluster sizing, Azure VM family fit, autoscale bounds, and cluster topology.

## Guidance
 * Treat **current run metrics** as the primary source of truth.
 * Use historical summaries only if explicitly provided.
 * Prefer reducing node count or tightening autoscale bounds **before** changing VM size or VM family. When worker count is **already minimal**, use **Core Optimization Principles, section 2** in the skill: rightsizing SKU or **D / E / F** fit is appropriate when metrics justify it.
 * Do not infer single-node topology from autoscale settings alone.
 * Interpret **`workflow_task_count`**, **allocated-vs-utilized** fields, percentages (`_pct`), and percentiles (`p95_` / `p99_`) **only** as defined in **databricks-efficiency** (input uses the flat ingest names from that skill; no ingest version field expected).
 * Do not assume prompts create files or directories; engineers manually save generated JSON under `copilot-results/` per repo conventions.

## Output
Return **strict JSON only** as defined in **databricks-efficiency**, **Output Expectations** (output contract plus sample schema). Prefer raw JSON **without** surrounding prose when the chat UI supports it.
