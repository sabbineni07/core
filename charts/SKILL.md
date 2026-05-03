---
name: databricks-efficiency
description: Azure Databricks cost-aware cluster optimization; ingest metrics in body; JSON schema_version 2.0.0.
---

### Objective

**Primary goals:** **reduce unnecessary cost** and recommend **right-sized, well-fitting cluster configuration** (VM series/SKU, workers/autoscale, topology) **using the ingest metrics**. Respect workload safety: use **`confidence_notes`** when uncertain **do not** invent metrics.

**Default stance:** **Prefer** evaluating **cheaper or better-matching** family/SKU when utilization supports it **avoid** keeping the current **`azure_worker_vm_size`** / **`vm_family`** without **metric-backed** justification (same as recommending a change).

---

### Scope

**Azure Databricks on Azure VMs only.** Emit **one** JSON object per **`Output Expectations`**. Cite ingest keys in **`analysis_summary.key_evidence`** and **`comparison.rationale`**.

Historical summaries (if user attaches them): **secondary only** **never** replace missing live metrics.

---

### Procedure (order matters)

1. **Family / SKU (`vm_family` D / E / F):** From **CPU vs memory** utilization (**allocated vs utilized**, **`cluster_avg_*_pct_of_ceiling_capacity`**, **`avg_cpu_*`**, **`avg_cpu_wait_pct`**, **`workflow_task_count`** context), infer memory-heavy (**E**), compute-heavy vs balanced (**F** vs **D**). Emit **`VM_FAMILY_MISMATCH`** when current family does not fit **use metrics + normal Spark/Azure reasoning**.
2. **Workers and autoscaler:** **`min_workers`**/**`max_workers`**, **`max_worker_nodes_cluster_ceiling`** vs consumed tails (**`OVERPROVISIONED_AUTOSCALE`**), low utilized vs allocated (**`PER_NODE_UNDERUTILIZED`**), orchestration (**`LOW_PARALLELISM`**).
3. **`single_node` topology:** **Last resort only.** **Default `multi_node`.** Recommend **`single_node`** **only** when true single-VM (driver+executors colocated) is clearly appropriate **do not** infer from **`autoscale` min=max=1** alone. **`SINGLE_NODE_*`** codes should be **rare.**

---

### Hard rules

- **Minimum `vcpus_per_node`:** **4** (see schema).
- **No peak CPU/memory fields in ingest** **do not** cite **`peak_*_utilization_pct_sample_window`**.
- **Planning buffer** (no peaks): per CPU and memory, **`utilized_pct_of_allocated = ratio * 100`**, plan using **`min(100, utilized_pct + 20)`** unless **`confidence_notes`** override.
- **Bundled telemetry:** **`azure_worker_vm_size`** applies to **driver and workers**. Node-count and **`avg_*_allocated_active_cluster`** fields **include the driver** **not** workers-only splits. **`max_worker_nodes_cluster_ceiling` = `1`** = single-node **ingest posture** classic **`min=max=1`** alone **not** true single-node.

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

**MUST:** All SKU/topo/autoscale deltas live under **`comparison.current_configuration`** / **`comparison.recommended_configuration`**. Gate with **`comparison.change_required`** + **`comparison.rationale`** (bullet strings with metric keys). If **`change_required`: false**, clone **current** to **recommended** + neutral **`expected_directional_impact`** unless **`confidence_notes`** exception. **`comparison.single_node`** only **`eligible`**/**`recommended`**/**`notes[]`** **never** hide sizing there.

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
