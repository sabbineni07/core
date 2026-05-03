---
name: databricks-efficiency
description: Agent rules for Azure Databricks efficiency (ingest fields in body; procedure order: VM family D/E/F first, worker/autoscale second, single-node last resort only; +20 planning buffer; 4 vCPU floor); output strict recommendation JSON schema_version 2.0.0.
---

### Agent directives (read first)

**Scope constraint:** Azure Databricks on Azure VMs only.

**Mandatory:** Apply **`Metric Interpretation Rules`**, then Steps **1 through 3 in order**. Emit **one recommendation JSON object** conforming **`Output Expectations`**. Tie every recommendation to ingest field values cited in **`analysis_summary.key_evidence`** or **`comparison.rationale`**.

**Procedure order (non-negotiable):** **(1)** Decide **VM family** (**D** / **E** / **F**) and CPU-vs-memory fit from utilization **before** locking worker counts. **(2)** Recommend **worker/autoscale** (and ceiling alignment) for that family. **(3)** Consider **`single_node`** topology **only** as a **last resort** when **Step 3** bars are met **default** remains **`multi_node`**.

**Primary lens:** **CPU and memory utilization** (**Consumed vs Utilized** ratios, **`cluster_avg_*_pct_of_ceiling_capacity`**, worker **`p95`/`p99`** tails, CPU diagnostics). **Step 1** interprets this lens for **family**; **Step 2** applies it to **how many workers** and **autoscaler bounds**.

**Planning buffer (no peak samples):** For **CPU** and **memory** separately, compute **utilized percentage of allocated** (**0-100**) from **`avg_*_utilized_by_workload / avg_*_allocated_active_cluster`**. Treat **`min(100, observed_pct + 20)`** as the **minimum stress-planning level** for sizing and shrink decisions **do not** invent unstated spike headroom. Other margins require **`confidence_notes`**.

**Minimal workers + low utilization:** When **`avg_worker_nodes_consumed`** and **`p99_worker_nodes_consumed`** are both **`<= 1`** and ratios are persistently low, **complete Step 1 family/SKU reasoning** and **Step 2** worker/autoscale **do not** emit **autoscale-only** rationale without **Step 1** family conclusions.

**Full procedure (no early exit):** **Always complete Steps 1, 2, and 3** when ingest allows (otherwise **`INSUFFICIENT_EVIDENCE`** / **`confidence_notes`**). **`comparison.rationale`** must cite **Step 1** family logic (when **`vm_family`** or SKU class changes or is defended) and **Step 2** worker/ceiling/utilization logic **before** any **`single_node`** recommendation (**Step 3**).

**Historical summaries** (when the user attaches them explicitly: e.g. `copilot-results/history-summary.{md,json}`): use only as secondary context **after** interpreting current-run metrics; **never** substitute for absent primary metrics **and** never assume access to unmanaged files.

---

## Core Optimization Principles

Lead with **CPU and memory signals** for **Step 1 family choice**, then **Step 2** worker/autoscale. **Single-node topology** is **not** a default path (**Step 3**, last resort only).

### 1. Minimum node size rule
- **Never** recommend node types smaller than **4 vCPUs** per node (platform may still reserve a fraction of vCPU per node).

### 2. Lever order (family, then workers, single-node last)
- **First (Step 1):** From **CPU vs memory** utilization shape (**allocated vs utilized**, **`cluster_avg_*_pct_of_ceiling_capacity`**, diagnostics, pipeline context), decide whether the workload is **memory-biased** (**E**), **compute-biased** (**F** per Step 1 rules), or **balanced** (**D**). Pick **target `vm_family`** / SKU class **before** finalizing worker counts.
- **Second (Step 2):** Given that family, set **`min_workers`**/**`max_workers`**, align **`max_worker_nodes_cluster_ceiling`** story (**OVERPROVISIONED_AUTOSCALE**), consumed vs utilized (**PER_NODE_UNDERUTILIZED**), and **`workflow_task_count`** (**LOW_PARALLELISM**).
- **Third (procedure):** **`single_node`** **only** using **Step 3** last-resort criteria (**below**) **otherwise** **`multi_node`**. **Do not** recommend single-node because the job is “small” alone.
- **Minimal workers + persistently low utilization:** When **`avg`**/**`p99`** workers **`<= 1`** **and** CPU and memory utilized-to-allocated **both below ~40%**, **Step 1** must still justify **D/E/F** and SKU size **Step 2** must not be **only** autoscale tightening without **Step 1** addressed. Apply the **Planning buffer** before claiming spare capacity.
- When node count is **already at a practical floor**, prefer **smaller SKU within the 4 vCPU floor** and correct **family** over pushing single-node for cost.
- Do not recommend shrinking below **4 vCPUs** per node.

### 3. Evidence binding
- Map every SKU/topology/`reason_codes[]` change to **observed ingest fields** (or **`INSUFFICIENT_EVIDENCE`** / **`NO_CHANGE_RECOMMENDED`**).
- Do not recommend larger clusters or higher-memory nodes unless the evidence supports it.
- Do not assume future workload growth unless explicitly stated.

---

## Metric Interpretation Rules

### Input (flat ingest object)

Assume **exactly these field names** in a flat JSON blob (omit keys when unknown). **Unknown field names:** treat absent metrics as **`null`**/**missing** (**no aliases**).

**Suffix / prefix semantics:** **`_pct`** means percentage (**0-100** default); **`p95_`/`p99_`** means percentiles.

**Driver and worker ingest (current vs upcoming)**

**Current ingest (bundled telemetry):**

- **`azure_worker_vm_size`** describes the **same Azure VM SKU** used for **both** the driver and the workers (**one** worker-type string applies to driver + executor nodes for this ingest).
- **Node-count and cluster capacity fields bundle the driver**: **`max_worker_nodes_cluster_ceiling`**, **`avg_worker_nodes_consumed`**, **`p95_worker_nodes_consumed`**, **`p99_worker_nodes_consumed`**, **`total_vcpus_cluster_ceiling`**, **`total_memory_gb_cluster_ceiling`**, **`avg_vcpus_allocated_active_cluster`**, and **`avg_memory_gb_allocated_active_cluster`** are **aggregated totals that include the driver** with workers (upstream definition). Interpret **Ceiling**, **Consumed** node tails, and **Consumed** compute/RAM in **Step 2** **on that bundled basis**, **not** as workers-only.
- When **`max_worker_nodes_cluster_ceiling` equals `1`**, treat as **single-node** posture for **this ingest** (capacity story matches one colocated footprint). **Classic** **`min_workers`/`max_workers` = `1`** is **still not** sufficient alone for **true single-node** topology (**Step 3**) when ingest is bundled.

**Future ingest (split driver vs worker):**

- When **`null`/missing**, keep **bundled rules** above. When **new driver-scoped and worker-scoped fields** arrive in the ingest (names will be appended to **`Metric Interpretation Rules`**), **prioritize worker-scoped** ceilings, consumed node counts, and allocated worker vCPU/memory for **Step 2** worker-pool/autoscale conclusions **prioritize driver-scoped** SKU and aggregates for **driver sizing** narratives and imbalance between driver vs workers. **Do not** use bundled cluster aggregates for worker-only slack if split fields supersede them for the same notion.
- If bundled and split fields **both appear** temporarily, prefer **split** fields for contradictory concepts and note overlap in **`confidence_notes`**.

**Three capacity layers (tie Step 2)**

- **Ceiling:** configured autoscaler/policy **max** (see **bundled** vs split rules above). **Fields:** `max_worker_nodes_cluster_ceiling`, `total_vcpus_cluster_ceiling`, `total_memory_gb_cluster_ceiling`
- **Consumed:** allocation over the window: node **avg** + percentiles and **cluster** allocated vCPU/RAM (**bundled ingest** counts driver with workers **not** workers-only). **Fields:** `avg_worker_nodes_consumed`, `p95_worker_nodes_consumed`, `p99_worker_nodes_consumed`, `avg_vcpus_allocated_active_cluster`, `avg_memory_gb_allocated_active_cluster`
- **Utilized:** workload use **inside** consumed capacity. **Fields:** `avg_vcpus_utilized_by_workload`, `avg_memory_gb_utilized_by_workload`

**Ratios:** if denominators **`> 0`**:

- **`avg_vcpus_utilized_by_workload / avg_vcpus_allocated_active_cluster`**
- **`avg_memory_gb_utilized_by_workload / avg_memory_gb_allocated_active_cluster`**

Else **`INSUFFICIENT_EVIDENCE`** / skip ratio; never fabricate divisors.

**Vs-ceiling aggregates (never replace ratios):**

- **`cluster_avg_cpu_utilization_pct_of_ceiling_capacity`**, **`cluster_avg_memory_utilization_pct_of_ceiling_capacity`**

**Ingest omits sample-window peak CPU/memory utilization:** **Do not** expect or cite **`peak_cpu_utilization_pct_sample_window`** or **`peak_memory_utilization_pct_sample_window`**. Use **averages**, **`p95`/`p99`** worker node counts, **`cluster_avg_*_pct_of_ceiling_capacity`**, and **`confidence_notes`** when burst risk is plausible but unobserved.

**Planning buffer** (same rule as **Agent directives**): convert each ratio to **utilized percent of allocated** (**0-100**), then use **`min(100, observed_pct + 20)`** per dimension when judging shrink, rightsizing, or family moves.

**Supplemental throughput (never outweigh utilization):** **`processed_row_count`**, **`processed_bytes`**

**Consumption-billing heuristic:** SKU waste targets **Consumed vs Utilized**; tightening **Ceiling vs Consumed** guards burst/guardrail misalignment, not spend on dormant nodes.

### Primary decision metrics (high signal)

- **`azure_worker_vm_size`:** Azure VM SKU for **drivers and workers** (**same SKU** until split ingest exposes a driver SKU separately).
- **`max_worker_nodes_cluster_ceiling`:** Bundled ingest: ceiling **includes driver** with workers (not workers-only); value **`1`** matches **single-node** ingest posture (**Driver and worker ingest**).
- **`total_vcpus_cluster_ceiling`**, **`total_memory_gb_cluster_ceiling`:** Bundled: **cluster** vCPU/memory **ceiling includes driver**.
- **`avg_worker_nodes_consumed`**, **`p95_worker_nodes_consumed`**, **`p99_worker_nodes_consumed`:** Bundled: node-count shape **includes driver** (**Consumed** tails for Step 2).
- **`avg_vcpus_allocated_active_cluster`**, **`avg_memory_gb_allocated_active_cluster`:** Bundled: **allocated** totals **include driver RAM/vCPU denominator** (**Consumed**).
- **`avg_vcpus_utilized_by_workload`**, **`avg_memory_gb_utilized_by_workload`:** Workload-attributed use inside that shell (**Utilized**).
- **`cluster_avg_cpu_utilization_pct_of_ceiling_capacity`**, **`cluster_avg_memory_utilization_pct_of_ceiling_capacity`:** Cluster aggregates versus ceiling (0-100); use with ratios above, never as a substitute for **Utilized**/allocated splits.
- **`workflow_task_count`:** Count of job workflow tasks (orchestration step count, not Spark executor parallelism).
- **`duration_seconds`:** Length of the run or sampled window (seconds).
- **`processed_bytes`**, **`processed_row_count`:** Optional throughput hints (**supplemental**).

### Validation metrics (secondary only)

Use only to validate, not drive decisions:

- **`provisioning_efficiency_pct`**
- **`cpu_utilization_efficiency_pct`**
- **`memory_utilization_efficiency_pct`**

### Diagnostic metrics (context only)

- **`avg_cpu_user_pct`**, **`avg_cpu_system_pct`**, **`avg_cpu_wait_pct`**

### Ignore for optimization logic

**Do not** size clusters from identifiers or timestamps alone:

- **`workspace_id`**, **`workspace_name`**, **`cluster_id`**, **`job_id`**, **`job_name`**
- **`run_start_time_utc`**, **`run_end_time_utc`**, **`job_run_date`**, **`metrics_ingest_timestamp_utc`**

---

## Required Analysis Procedure

Follow **Steps 1, 2, then 3**. **Default `multi_node`.** Complete **Step 1** before treating worker/autoscale numbers as final.

### Step 1: VM family and SKU class (D / E / F) — **first**

**Role:** Compare **CPU vs memory** utilization and pipeline bias **first** pick **E** (memory-biased), **F** (compute-biased, gated), or **D** (balanced) **before** locking worker counts.

**Inputs:** **`avg_vcpus_utilized_by_workload / avg_vcpus_allocated_active_cluster`** and **`avg_memory_gb_utilized_by_workload / avg_memory_gb_allocated_active_cluster`** (express as **0-100** utilized-% of allocated **apply Planning buffer** per dimension), **`cluster_avg_*_pct_of_ceiling_capacity`**, **`avg_cpu_user_pct`**, **`avg_cpu_system_pct`**, **`avg_cpu_wait_pct`**, optional **`processed_*`**, **`workflow_task_count`** as orchestration context (**not** Spark executor parallelism).

**Family decision:**
- **GiB-heavy:** elevated memory ratio / **`cluster_avg_memory_*`** / shuffle-cache narrative **prefer **E** ** **`VM_FAMILY_MISMATCH`** when the current series mismatches memory pressure.
- **CPU-heavy + memory slack:** consider **F** **only if** **`avg_cpu_wait_pct`** excludes pure I/O-wait pathology **otherwise** prefer **E-to-D** bias before **F**.
- **Balanced CPU and memory:** default **D**.

**E-series:**
- Prefer when memory pressure dominates: high **`avg_memory_gb_utilized_by_workload` / `avg_memory_gb_allocated_active_cluster`**, high **`cluster_avg_memory_utilization_pct_of_ceiling_capacity`**, or shuffle/broadcast/cache behavior.
- Use **CPU diagnostics** (for example **`avg_cpu_wait_pct`**) so **I/O-bound or waiting** CPUs are **not** read as spare compute that warrants **F**; fix bottlenecks first.
- **`INSUFFICIENT_EVIDENCE`** or conservative **`NO_CHANGE`** when shuffle-heavy stages or noisy averages alone make family choice unclear.

**F-series:**
- Prefer evaluating **F** only when the run shows a **compute-bound** profile with **clear memory headroom** on **`azure_worker_vm_size`** (solid **`avg_vcpus_utilized_by_workload` / `avg_vcpus_allocated_active_cluster`**, subdued memory ratio or **`cluster_avg_memory_utilization_pct_of_ceiling_capacity`** story) sustained across the window, not partial/cold-start except as noted in **`confidence_notes`**.
- **Do not** recommend **F** if memory ratios **or** **`cluster_avg_memory_utilization_pct_of_ceiling_capacity`** contradict headroom claims, or shuffle/broadcast/cache risk is material.
- **High `avg_cpu_wait_pct` alone is not grounds for F**; triangulate disk, network, shuffle, and driver bottlenecks separately from insufficient CPU capacity.
- **E-series to D-series** often suffices when the problem is "**memory-series without memory justification**"; move to **F** when the narrative is specifically **needed compute density per GiB**, not merely "**wrong series**."

**Always** assign **`VM_FAMILY_MISMATCH`** whenever fit skews (**no F-only supplemental code** unless external program extends enumerations).

### Step 2: Workers, autoscale, ceiling vs consumed, and parallelism

**Role:** After Step **1** target family, recommend **driver + worker pool shape**: **`min_workers`**/**`max_workers`**, ceiling alignment, utilization codes, orchestration.

**2A Cluster ceiling vs consumed:** Compare **`max_worker_nodes_cluster_ceiling`**, **`total_vcpus_cluster_ceiling`**, **`total_memory_gb_cluster_ceiling`** to **`avg_worker_nodes_consumed`**, **`p95_worker_nodes_consumed`**, **`p99_worker_nodes_consumed`**. If **configured max materially above** distributional need, **`OVERPROVISIONED_AUTOSCALE`**. Rationale: **burst headroom tolerance** (**not** unspent DBU unless committed-capacity story). Tie **`max_workers`** recommendations to **Step 1** family.

**2B Consumed vs utilized:** Ratios **`avg_vcpus_utilized_by_workload / avg_vcpus_allocated_active_cluster`** and memory analog **triangulate** **`p95`/`p99`** worker nodes and **`cluster_avg_*_pct_of_ceiling_capacity`**. Apply **Planning buffer**. **`PER_NODE_UNDERUTILIZED`** when utilized/allocated is low **and** tails do not contradict shrink. Cite **`cluster_avg_*_pct_of_ceiling_capacity`** separately.

**2C Workflow orchestration parallelism:** **`workflow_task_count`** **means** job workflow **tasks**. **`LOW_PARALLELISM`** when extra workers **do not** materially shorten wall-clock.

**Mandatory:** **`comparison.rationale`** reflects **Step 1** **and** **Step 2** before **`single_node`**. Autoscale-only outcomes **must not** skip **Step 1** family reasoning (**Core Optimization Principles**).

### Step 3: Single-node topology (**last resort only**)

**Default:** **`multi_node`**. **Do not** treat single-node as a routine optimization.

**Definition:** **True single-node** = **one Azure VM** runs **driver and executors** together **not** classic driver VM + worker VM.

Recommend **`cluster_topology":"single_node"`** **only if all** hold **otherwise** **`multi_node`**:
1. **Steps 1 and 2** complete with defensible **`vm_family`**/**SKU** and worker/autoscale story **with** no **`INSUFFICIENT_EVIDENCE`** blockers for sizing.
2. **`workflow_task_count`** + semantics show **no meaningful parallel gain** from scaling workers out for this pipeline.
3. **`p99_worker_nodes_consumed`** (bundled ingest **includes driver**) is consistent with a **narrow** footprint **and** **Planning buffer** fits **one** VM at the **Step 1** SKU class.
4. **`comparison.rationale`** explains why **multi-node** is **unnecessary cost or complexity** **not** only "low utilization."
5. **`autoscale.min==max==1`** on classic **does not** prove single-node (often **still driver + worker**).

**Reason codes:** **`SINGLE_NODE_ELIGIBLE`** and **`SINGLE_NODE_RECOMMENDED`** are **rare** **only** when (1)-(5) are clearly satisfied **if uncertain**, keep **`multi_node`** and explain in **`confidence_notes`**.

Emit **`comparison.single_node.recommended:true`** only when recommending **`single_node`** (**Output Expectations**).

---

## Recommendation Rules

### VM Sizing
- Maintain a minimum of **4 vCPUs per node**.
- Follow **Core Optimization Principles section 2**: **Step 1** family fit **then** **Step 2** worker counts **then** **Step 3** single-node only if warranted.
- Apply **Step 1** when choosing or changing **Azure D / E / F** series so CPU versus GiB utilization skew stays consistent **before** finalizing autoscale.
- If changing family or SKU, ensure proposed memory is sufficient for observed workload behavior, **`cluster_avg_*_pct_of_ceiling_capacity`** story, and shuffle/broadcast/cache risk.

### Single-node memory rule
- When recommending a single-node topology, do not recommend **`memory_gb_per_node`** lower than evidenced need from **`avg_memory_gb_utilized_by_workload`** (and pipeline risk) unless explicitly marked as a validation candidate.
- If memory headroom is unclear, prefer a safer recommendation over an aggressively smaller footprint.

### Topology vs autoscale
- **`multi_node`:** emit realistic **`min_workers`/`max_workers`** for SKU + evidence.
- **`single_node`:** **`autoscale`** numeric pair must obey **Databricks policy** for that SKU (often **`0`** workers when pool N/A).


### Reason codes `NO_CHANGE_RECOMMENDED` and `INSUFFICIENT_EVIDENCE`
- **`NO_CHANGE_RECOMMENDED`:** Metrics support that **no safe cost change** is warranted (for example, already at minimum viable **4 vCPU** footprint, rightsizing would add operational risk without evidence, or pipeline constraints block change). Use with **`change_required`: false** and recommended configuration **mirroring** current.
- **`INSUFFICIENT_EVIDENCE`:** Primary metrics **missing**, **incomplete**, **not representative** (partial/cold-run), **or required denominators are zero/absent** (cannot trust allocated-vs-utilized ratios). Prefer **`change_required`: false**, explain gaps in **`confidence_notes`**, and avoid speculative family or topology changes.

---

## Recommendation ID

Compose stable **`recommendation_id`** as:
- start with `<job_name>__<job_run_date>`
- append `__<job_id>` if available
- append `__<cluster_id>` if available

Illustrative: `pipeline_name_2026-01-01__12345678910111__0101-024567-8gegooked`

---

## Lifecycle & Maturity Rules

Must enforce mapping:

| `lifecycle.status` | `maturity.maturity_score` | `maturity.maturity_level` |
|-------------------|------------------|------------------|
| `RECOMMENDED` | 0 | `generated` |
| `ACCEPTED` | 33 | `accepted` |
| `APPLIED` | 66 | `applied` |
| `VERIFIED` | 100 | `verified` |
| `REJECTED` | 0 | `rejected` |

---

## Reason Codes

**Emit `reason_codes`** (array, length at least **1**) drawing only from:
- `OVERPROVISIONED_AUTOSCALE`
- `PER_NODE_UNDERUTILIZED`
- `VM_FAMILY_MISMATCH`
- `LOW_PARALLELISM`
- `SINGLE_NODE_ELIGIBLE` (only if **Step 3** bars are clearly met **rare**)
- `SINGLE_NODE_RECOMMENDED` (only if **Step 3** bars are clearly met **rare**)
- `NO_CHANGE_RECOMMENDED`
- `INSUFFICIENT_EVIDENCE`

---

## Output Expectations

Respond **exactly once** as **strict JSON**. Schema = below contract + verbatim key order preference from sample (**diff stability**).

**MUST:**
1. Place **every** SKU **vCPU**/**RAM**/**topo**/**`autoscale`** delta **only under** **`comparison.{current,recommended}_configuration`**.
2. Package gatekeeping text in **`comparison.change_required`** + **`comparison.rationale`** (string bullets with metric keys!).
3. If **`change_required:false`**, clone **`current`** to **`recommended`** and neutral **`expected_directional_impact`** unless justified exception in **`confidence_notes`**.
4. Limit **`comparison.single_node`** to **`eligible`/`recommended`/`notes[]`**; **never** tuck sizing blobs there.

**MUST NOT:** spawn parallel recommendation trees outside **`comparison`**; invent ingest stats.

**Enums:** **`cluster_topology`** must be the string **`multi_node`** or **`single_node`** (default **`multi_node`** **Step 3** single-node rules).

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
