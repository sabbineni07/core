---
name: databricks-efficiency
description: Azure Databricks cost-aware cluster optimization; ingest metrics in body; JSON schema_version 2.0.0.
---

### Objective

**Primary goals:** **reduce unnecessary cost** and recommend **right-sized, well-fitting cluster configuration** (VM series/SKU, workers/autoscale, topology) **using the ingest metrics**. Respect workload safety: use **`confidence_notes`** when uncertain **do not** invent metrics.

**Default stance:** **Prefer** evaluating **cheaper or better-matching** family/SKU when utilization supports it **avoid** keeping the current **`azure_worker_vm_size`** / **`vm_family`** without **metric-backed** justification (same as recommending a change).

**Platform preference (when SKU choice is otherwise equivalent):** Prefer **newer-generation** Azure VM families (**v5** over **v3**, etc., for the same series/size intent) and SKUs that include **local ephemeral (temp) SSD** where Azure naming applies (**`d`** in the size name, e.g. **Eds**/**Dds**-style sizes **not** literally every SKU).

**Azure size-pair preference (explicit):** For the same family and capacity intent, prefer **`*ds_v*`** (or equivalent local-temp-disk variant such as `*ads_v*` / `*pdsv*` where naming applies) over **`*s_v*`** (for example **`Standard_D4ds_v5`** over **`Standard_D4s_v5`**). Treat recommending the no-local-temp variant when a viable local-temp variant exists as an exception path: explain the blocker in **`recommended_configuration.notes`** and/or **`confidence_notes`** (policy/region availability, workload incompatibility, measured risk, or stronger metric-backed cost/perf reason).

**AMD `ads` vs Intel `ds` (same capacity tier):** When **eligible** candidates include **`Standard_*ads_v*`** and **`Standard_*ds_v*`** with the **same** nominal **`vcpus_per_node`** and **`memory_gb_per_node`** (for example **`Standard_D2ads_v6`** vs **`Standard_D2ds_v6`**), rank **`ads`** **above** **`ds`** (often lower hourly list price verify with org/table if attached). When **`ads`** is **not** in **`eligible_node_types`**, pick the **least expensive** intersection SKU that satisfies the buffered CPU/memory envelope and note the substitution.

**Feature-2 availability guardrail:** Do **not** assume every D/E/F subfamily has a Feature-2 (local temp disk) sibling. Some subfamilies are intentionally no-local-disk and documented as better value when local temp storage is not required. In those cases, prefer **newest generation within the available subfamily**, and document that local-temp-disk was unavailable or not required.

**Exception gate:** Do **not** downgrade from a newer local-temp-disk class (for example **`*ds_v*`**) to older/non-temp-SSD classes by default. If that recommendation is made, **`comparison.rationale`** must include explicit metric-backed and constraint-backed justification (not generic savings language).

**Cost levers (full picture):** Savings can come from **(1)** **family / SKU / generation** (for example **D** or **F** instead of **E** when utilization indicates compute-heavy or balanced fit and lower $/hr applies **or** a smaller SKU within series **without** starving memory **or** right-sized **E** when memory-bound), **(2)** **worker count / autoscale bounds**, **(3)** topology (**`single_node`** only when procedure allows). **Follow Procedure order** when reasoning **do not** collapse the narrative to **only** “fewer nodes” or “lower max_workers.”

**Primary lever wording:** **Do not** call **reducing workers or nodes** the **primary** cost lever unless **family/SKU fit is already addressed** (metrics support staying on the current **`vm_family`** / **`azure_worker_vm_size`** class, or **`NO_CHANGE`** there) **and** leftover waste is clearly from **autoscale/topology**. If **`VM_FAMILY_MISMATCH`** applies or a **cheaper/better-fit family or SKU** is plausible from **`cluster_avg_cpu_utilization_pct_of_ceiling_capacity`**, **`cluster_avg_memory_utilization_pct_of_ceiling_capacity`**, **`avg_vcpus_allocated_active_cluster`** vs **`avg_vcpus_utilized_by_workload`**, **`avg_memory_gb_allocated_active_cluster`** vs **`avg_memory_gb_utilized_by_workload`**, **`workflow_task_count`**, **`avg_cpu_wait_pct`**, then **`comparison.rationale`**, **`recommended_configuration.notes`**, and **`analysis_summary`** **must** surface **family/SKU** as a **first-class lever** (alongside any worker change **not** buried after node-only language).

---

### Scope

**Azure Databricks on Azure VMs only.** Emit **one** JSON object per **`Output Expectations`**. Cite ingest keys in **`analysis_summary.key_evidence`** and **`comparison.rationale`**.

Historical summaries (if user attaches them): **secondary only** **never** replace missing live metrics.

---

### Procedure (order matters)

1. **Family / SKU (`vm_family` D / E / F):** From **CPU vs memory** utilization (**allocated vs utilized**, **`cluster_avg_*_pct_of_ceiling_capacity`**, **`avg_cpu_*`**, **`avg_cpu_wait_pct`**, **`workflow_task_count`** context), infer memory-heavy (**E**), compute-heavy vs balanced (**F** vs **D**). Emit **`VM_FAMILY_MISMATCH`** when current family does not fit **use metrics + normal Spark/Azure reasoning**. Within the chosen family/size intent, prefer **newer gen** first pick local-temp variants (**`*ads_v*`** before **`*ds_v*`** at same tier **`*ds_v*`** before **`*s_v*`**) only from **eligible** SKUs per **Databricks availability enforcement** **do not** invent names.
2. **Workers and autoscaler:** **`min_workers`**/**`max_workers`**, **`max_worker_nodes_cluster_ceiling`** vs consumed tails (**`OVERPROVISIONED_AUTOSCALE`**), low utilized vs allocated (**`PER_NODE_UNDERUTILIZED`**), orchestration (**`LOW_PARALLELISM`**).
3. **`single_node` topology:** **Last resort only.** **Default `multi_node`.** Recommend **`single_node`** **only** when true single-VM (driver+executors colocated) is clearly appropriate **do not** infer from **`autoscale` min=max=1** alone. **`SINGLE_NODE_*`** codes should be **rare.**

---

### Hard rules

- **Minimum `vcpus_per_node`:** **4** (see schema).
- **No peak CPU/memory fields in ingest** **do not** cite **`peak_*_utilization_pct_sample_window`**.
- **Planning buffer** (no peaks): per CPU and memory, **`utilized_pct_of_allocated = ratio * 100`**, plan using **`min(100, utilized_pct + 20)`** unless **`confidence_notes`** override.
- **Bundled telemetry:** **`azure_worker_vm_size`** applies to **driver and workers**. Node-count and **`avg_*_allocated_active_cluster`** fields **include the driver** **not** workers-only splits. **`max_worker_nodes_cluster_ceiling` = `1`** = single-node **ingest posture** classic **`min=max=1`** alone **not** true single-node.

### Databricks availability enforcement (required)

- **Recommend only Databricks-available node types:** **`comparison.recommended_configuration.azure_node_type`** must exist in **`system.compute.node_types`** for the target workspace/region (or equivalent user-provided export of that table).
- **Allow-list gate (in addition to system table):** Recommended node type must be in this allow-list:
  - **D-Series:** `Standard_D2ds_v6`, `Standard_D2ads_v6`, `Standard_D4ds_v5`, `Standard_D3_v2`, `Standard_D4ads_v6`, `Standard_D4ads_v5`, `Standard_D12_v2`, `Standard_D4s_v5`, `Standard_D4ds_v6`, `Standard_D4as_v5`, `Standard_D4_v2`, `Standard_D3s_v5`, `Standard_D8ads_v6`, `Standard_D8ds_v5`, `Standard_D13_v2`, `Standard_D8ds_v6`, `Standard_D8ads_v5`, `Standard_D8as_v5`, `Standard_D16as_v5`, `Standard_D5_v2`, `Standard_D16ads_v5`, `Standard_D16s_v5`, `Standard_D16ads_v6`, `Standard_D16ds_v6`, `Standard_D14_v2`, `Standard_D16ds_v5`
  - **E-Series:** `Standard_E2ads_v6`, `Standard_E2ds_v6`, `Standard_E4as_v5`, `Standard_E4ds_v5`, `Standard_E4s_v5`, `Standard_E4ads_v5`, `Standard_E4ads_v6`, `Standard_E4ds_v6`, `Standard_E8as_v5`, `Standard_E8ads_v6`, `Standard_E8ds_v5`, `Standard_E8ads_v5`, `Standard_E8ds_v6`, `Standard_E8s_v5`, `Standard_E16ds_v5`, `Standard_E16as_v5`, `Standard_E16ads_v5`, `Standard_E16s_v5`, `Standard_E16ads_v6`, `Standard_E16ds_v6`
  - **F-Series:** `Standard_F4s_v2`, `Standard_F4s`, `Standard_F4`, `Standard_F8s_v2`, `Standard_F8s`, `Standard_F8`, `Standard_F16s`, `Standard_F16`, `Standard_F16s_v2`
- **Dual filter behavior:** If a SKU is on allow-list but missing in **`system.compute.node_types`**, **do not recommend it**. If a SKU is in system table but not in allow-list, **do not recommend it**.
- **Closed set:** Treat the allow-list as a strict closed set. **Never** output a recommended node type outside these exact values.
- **Fallback behavior:** If preferred SKU is filtered out, pick the next closest SKU that passes both gates and explain in **`recommended_configuration.notes`** and/or **`confidence_notes`**.
- **No availability evidence case:** If no workspace-specific **`system.compute.node_types`** evidence is provided, avoid changing to an unverified SKU. Prefer keeping current node type with **`INSUFFICIENT_EVIDENCE`** or explicitly note uncertainty in **`confidence_notes`**.
- **Deterministic selection order (must follow):**
  1. Build eligible candidates using strict intersection (**allow-list** ∩ **`system.compute.node_types`**).
  2. Restrict to chosen **`vm_family`** and required capacity envelope (CPU/memory fit from metrics + planning buffer).
  3. Rank eligible candidates by: (**a**) **closest fit** (minimal overshoot of required vCPU/mem after planning buffer); (**b**) at the **same** nominal **`vcpus_per_node` / `memory_gb_per_node` tier**, prefer **`Standard_*ads_v*`** before **`Standard_*ds_v*`** (AMD local-temp sibling **before** Intel local-temp when both intersect); (**c**) if **`ads`** is absent, prefer the **lowest hourly cost** SKU in the intersection that still satisfies (**a**) (use attached org pricing row or `system.compute.node_types`-supplied hourly fields when present otherwise rely on (**a**)(**b**)); (**d**) then newer generation break ties (**e**) local-temp over no-local-disk where applicable.
  4. Emit only the top ranked value from that eligible set.
- **Preference safety rule:** Version/local-disk preferences are tie-breakers **after** eligibility filtering; they must never synthesize or mutate SKU names.
- **Mandatory final validity check (before emitting JSON):**
  - Compute `eligible_node_types = allow_list ∩ system.compute.node_types`.
  - Verify `comparison.recommended_configuration.azure_node_type` is an **exact string match** in `eligible_node_types`.
  - If check fails, **do not emit** the invalid SKU. Re-select from `eligible_node_types`; if none fit, set **`change_required: false`** with **`INSUFFICIENT_EVIDENCE`** and explain in **`confidence_notes`**.
- **No guessed availability:** Never assume workspace availability from Azure family naming alone. Only trust provided `system.compute.node_types` evidence for eligibility.

---

### Ingest field names

Flat JSON **exact keys below** (omit if unknown **unknown key names** → **`null`** / ignore).

**Identity/time (do not drive sizing alone):** **`workspace_id`**, **`workspace_name`**, **`cluster_id`**, **`job_id`**, **`job_name`**, **`run_*`**, **`job_run_date`**, **`metrics_ingest_timestamp_utc`**

**Sizing / utilization:** **`azure_worker_vm_size`**, **`max_worker_nodes_cluster_ceiling`**, **`total_vcpus_cluster_ceiling`**, **`total_memory_gb_cluster_ceiling`**, **`avg_worker_nodes_consumed`**, **`p95_worker_nodes_consumed`**, **`p99_worker_nodes_consumed`**, **`avg_vcpus_allocated_active_cluster`**, **`avg_memory_gb_allocated_active_cluster`**, **`avg_vcpus_utilized_by_workload`**, **`avg_memory_gb_utilized_by_workload`**, **`cluster_avg_cpu_utilization_pct_of_ceiling_capacity`**, **`cluster_avg_memory_utilization_pct_of_ceiling_capacity`**, **`workflow_task_count`**, **`duration_seconds`**, **`processed_row_count`**, **`processed_bytes`**

**Diagnostics:** **`avg_cpu_user_pct`**, **`avg_cpu_system_pct`**, **`avg_cpu_wait_pct`**

**Secondary (validation only):** **`provisioning_efficiency_pct`**, **`cpu_utilization_efficiency_pct`**, **`memory_utilization_efficiency_pct`**

**Ratios** use denominators **`> 0`** **else** **`INSUFFICIENT_EVIDENCE`**.

**Future:** When driver/worker-split fields arrive in ingest, prefer **worker-scoped** fields for worker/autoscale **driver-scoped** for driver SKU **document** in **`confidence_notes`** if both exist and disagree.

---

### Reason codes

Emit **`reason_codes`** (length **>= 1**) **only** from:

`OVERPROVISIONED_AUTOSCALE` | `PER_NODE_UNDERUTILIZED` | `VM_FAMILY_MISMATCH` | `LOW_PARALLELISM` | `SINGLE_NODE_ELIGIBLE` | `SINGLE_NODE_RECOMMENDED` | `NO_CHANGE_RECOMMENDED` | `INSUFFICIENT_EVIDENCE`

**`NO_CHANGE_RECOMMENDED`:** Use when no safe change is justified **`change_required`: false**, mirror **current** to **recommended**.

**`INSUFFICIENT_EVIDENCE`:** Missing or unusable metrics **`change_required`: false**, explain in **`confidence_notes`**.

---

### Recommendation ID

`<job_name>__<job_run_date>` then optional **`__<job_id>`**, **`__<cluster_id>`**.

---

### Lifecycle & maturity

| `lifecycle.status` | `maturity.maturity_score` | `maturity.maturity_level` |
|-------------------|------------------|------------------|
| `RECOMMENDED` | 0 | `generated` |
| `ACCEPTED` | 33 | `accepted` |
| `APPLIED` | 66 | `applied` |
| `VERIFIED` | 100 | `verified` |
| `REJECTED` | 0 | `rejected` |

---

### Output Expectations

Return **strict JSON once**. Prefer stable key order as in sample.

**MUST:** All SKU/topo/autoscale deltas live under **`comparison.current_configuration`** / **`comparison.recommended_configuration`**. Gate with **`comparison.change_required`** + **`comparison.rationale`**.

**Explanation depth:** **`comparison.rationale`** **must** be **multiple** bullet strings (**not** single vague lines). **Each** bullet should cite **specific ingest keys** and **why** that drives the recommendation (family fit, worker/autoscale, cost vs risk). When **both** family/SKU and worker/autoscale changes apply, **split bullets** (or explicit sub-clauses) so **E vs D/F**, **SKU size**, and **min/max workers** are **not** conflated. If SKU naming implies a choice between **`*ads_v*`** and **`*ds_v*`** or between **`*ds_v*`** and **`*s_v*`**, include a direct bullet explaining cost/availability and generation trade-off. **`analysis_summary.cluster_level_state`** **must** be substantive **and** **should** state **which cost lever dominates expected $ impact** when inferable from metrics (otherwise **`confidence_notes`**). **`per_node_efficiency`** **must** be substantive (multi-sentence strings as needed). **`key_evidence`** **must** list the **main** metric keys used. Use **`current_configuration.notes`** / **`recommended_configuration.notes`** for SKU generation (**v3** vs **v5**, **`d`** temp SSD) or constraints when helpful **`confidence_notes`** for uncertainty.

If **`change_required`: false**, clone **current** to **recommended** + neutral **`expected_directional_impact`** unless **`confidence_notes`** exception. **`comparison.single_node`** only **`eligible`**/**`recommended`**/**`notes[]`** **never** hide sizing there.

**MUST NOT:** Invent stats extra recommendation trees outside **`comparison`**.

**`cluster_topology`:** **`multi_node`** (default) or **`single_node`** per procedure above.

```json
{
  "schema_version": "2.0.0",
  "recommendation_id": "",
  "analysis_summary": {
    "cluster_level_state": "",
    "per_node_efficiency": "",
    "key_evidence": []
  },
  "reason_codes": [],
  "comparison": {
    "change_required": true,
    "rationale": [],
    "current_configuration": {
      "azure_node_type": "",
      "vm_family": "",
      "vm_generation": "",
      "vcpus_per_node": 4,
      "memory_gb_per_node": 0,
      "cluster_topology": "multi_node|single_node",
      "autoscale": {
        "min_workers": 0,
        "max_workers": 0
      },
      "notes": ""
    },
    "recommended_configuration": {
      "azure_node_type": "",
      "vm_family": "",
      "vm_generation": "",
      "vcpus_per_node": 4,
      "memory_gb_per_node": 0,
      "cluster_topology": "multi_node|single_node",
      "autoscale": {
        "min_workers": 0,
        "max_workers": 0
      },
      "notes": ""
    },
    "expected_directional_impact": {
      "cost": "lower|same|higher",
      "performance": "better|same|worse|uncertain",
      "risk": "low|medium|high"
    },
    "single_node": {
      "eligible": false,
      "recommended": false,
      "notes": []
    }
  },
  "pipeline_recommendations": [
    {
      "priority": "HIGH|MEDIUM|LOW",
      "area": "",
      "recommendation": "",
      "expected_impact": ""
    }
  ],
  "lifecycle": {
    "status": "RECOMMENDED|ACCEPTED|APPLIED|VERIFIED|REJECTED",
    "accepted": false,
    "accepted_by": null,
    "accepted_at": null,
    "applied": false,
    "applied_by": null,
    "applied_at": null,
    "verified": false,
    "verified_by": null,
    "verified_at": null,
    "verification_notes": []
  },
  "maturity": {
    "maturity_score": 0,
    "maturity_level": "generated|accepted|applied|verified|rejected"
  },
  "confidence_notes": []
}
```
