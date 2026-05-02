---
name: databricks-efficiency
description: Agent rules for Azure Databricks efficiency (ingest fields in body, ceiling vs consumed vs utilized, VM series D E F, autoscale/topology); output strict recommendation JSON schema_version 2.0.0.
---

### Agent directives (read first)

**Scope constraint:** Azure Databricks on Azure VMs only.

**Mandatory:** Apply **`Metric Interpretation Rules`**, then Steps **1 through 5 in order**. Emit **one recommendation JSON object** conforming **`Output Expectations`**. Tie every recommendation to ingest field values cited in **`analysis_summary.key_evidence`** or **`comparison.rationale`**.

**Primary lens:** **CPU and memory utilization** (**Consumed vs Utilized** ratios, **peak** metrics, **`cluster_avg_*_pct_of_ceiling_capacity`** as supporting aggregates). **Worker counts, autoscale min/max, and topology** align the **capacity shell** to that utilization story; they do **not** replace it.

**Full procedure (no early exit):** **Always evaluate Steps 2, 3, and 4** when the ingest fields those steps require are present (otherwise document missing evidence in **`confidence_notes`** / **`INSUFFICIENT_EVIDENCE`**). **`OVERPROVISIONED_AUTOSCALE` or other Step 1 findings do not** waive Steps **2 through 5**. Never recommend **`recommended_configuration`** (SKU, **`min_workers`**/**`max_workers`**, **`cluster_topology`**) based **only** on Step 1; **`comparison.rationale`** and **`analysis_summary.key_evidence`** must reflect **allocated-vs-utilized CPU/memory** reasoning from Step **2** and, when **`recommended_configuration`** changes SKU / **`vm_family`** versus **`current_configuration`**, **VM family fit** from Step **4** (unless **`INSUFFICIENT_EVIDENCE`** states Step 4 inputs are missing).

**Historical summaries** (when the user attaches them explicitly: e.g. `copilot-results/history-summary.{md,json}`): use only as secondary context **after** interpreting current-run metrics; **never** substitute for absent primary metrics **and** never assume access to unmanaged files.

---

## Core Optimization Principles

Lead with **CPU and memory signals**: **Utilized versus allocated**, **peak** CPU/memory, diagnostics (**`avg_cpu_*_pct`**, especially **`avg_cpu_wait_pct`**), plus **`workflow_task_count`**. Treat **autoscaler ceilings**, **worker percentiles**, and **topology** as **how capacity is shaped**, so Steps **2-4** conclusions stay consistent with measured utilization (**Step 1** alone is shell context).

### 1. Minimum node size rule
- **Never** recommend node types smaller than **8 vCPUs** (about **1** vCPU per node commonly reserved by platform overhead).

### 2. Prefer reducing node count before shrinking node size
- Order levers when idle: tighten **workers / autoscale** before smaller SKUs unless Step 2 already shows **allocated-vs-utilized** saturation.
- When node count is **already low** (for example, **`min_workers`** and actual consumption are at a practical floor, or further reduction would conflict with parallelism or headroom), treat **node-count reduction** as **exhausted** for this pass. Then prefer **rightsizing**: a **smaller SKU** within the **8 vCPU minimum** rule and/or a **better-fitting VM family** (see Step 4), **only when metrics** (**allocated vs utilized** ratios, **`cluster_avg_*_utilization_pct_of_ceiling_capacity`**, **`workflow_task_count`**, node percentiles) **show sustained headroom**. Document uncertainty in **`confidence_notes`** or impact fields rather than implying zero performance risk.
- If the workload is very small and lightly parallelized, evaluate whether switching to a **single-node cluster topology** is justified (see Step 5).
- Do not recommend shrinking below 8 vCPUs per node.

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
- **Node-count and cluster capacity fields bundle the driver**: **`max_worker_nodes_cluster_ceiling`**, **`avg_worker_nodes_consumed`**, **`p95_worker_nodes_consumed`**, **`p99_worker_nodes_consumed`**, **`total_vcpus_cluster_ceiling`**, **`total_memory_gb_cluster_ceiling`**, **`avg_vcpus_allocated_active_cluster`**, and **`avg_memory_gb_allocated_active_cluster`** are **aggregated totals that include the driver** with workers (upstream definition). Interpret **Ceiling**, **Consumed** node tails, and **Consumed** compute/RAM in Step 1 vs Step 2 **on that bundled basis**, **not** as workers-only.
- When **`max_worker_nodes_cluster_ceiling` equals `1`**, treat as **single-node** posture for **this ingest** (capacity story matches one colocated footprint). **Classic** **`min_workers`/`max_workers` = `1`** is **still not** sufficient alone for **true single-node** topology (Step 5) when ingest is bundled.

**Future ingest (split driver vs worker):**

- When **`null`/missing**, keep **bundled rules** above. When **new driver-scoped and worker-scoped fields** arrive in the ingest (names will be appended to **`Metric Interpretation Rules`**), **prioritize worker-scoped** ceilings, consumed node counts, and allocated worker vCPU/memory for **Step 1-3** worker-pool/autoscale conclusions **prioritize driver-scoped** SKU and aggregates for **driver sizing** narratives and imbalance between driver vs workers. **Do not** use bundled cluster aggregates for worker-only slack if split fields supersede them for the same notion.
- If bundled and split fields **both appear** temporarily, prefer **split** fields for contradictory concepts and note overlap in **`confidence_notes`**.

**Three capacity layers (tie Steps 1-2)**

- **Ceiling:** configured autoscaler/policy **max** (see **bundled** vs split rules above). **Fields:** `max_worker_nodes_cluster_ceiling`, `total_vcpus_cluster_ceiling`, `total_memory_gb_cluster_ceiling`
- **Consumed:** allocation over the window: node **avg** + percentiles and **cluster** allocated vCPU/RAM (**bundled ingest** counts driver with workers **not** workers-only). **Fields:** `avg_worker_nodes_consumed`, `p95_worker_nodes_consumed`, `p99_worker_nodes_consumed`, `avg_vcpus_allocated_active_cluster`, `avg_memory_gb_allocated_active_cluster`
- **Utilized:** workload use **inside** consumed capacity. **Fields:** `avg_vcpus_utilized_by_workload`, `avg_memory_gb_utilized_by_workload`

**Ratios:** if denominators **`> 0`**:

- **`avg_vcpus_utilized_by_workload / avg_vcpus_allocated_active_cluster`**
- **`avg_memory_gb_utilized_by_workload / avg_memory_gb_allocated_active_cluster`**

Else **`INSUFFICIENT_EVIDENCE`** / skip ratio; never fabricate divisors.

**Vs-ceiling aggregates (never replace ratios):**

- **`cluster_avg_cpu_utilization_pct_of_ceiling_capacity`**, **`cluster_avg_memory_utilization_pct_of_ceiling_capacity`**
- Peaks **`peak_cpu_utilization_pct_sample_window`**, **`peak_memory_utilization_pct_sample_window`**

**Supplemental throughput (never outweigh utilization/peaks):** **`processed_row_count`**, **`processed_bytes`**

**Consumption-billing heuristic:** SKU waste targets **Consumed vs Utilized**; tightening **Ceiling vs Consumed** guards burst/guardrail misalignment, not spend on dormant nodes.

### Primary decision metrics (high signal)

- **`azure_worker_vm_size`:** Azure VM SKU for **drivers and workers** (**same SKU** until split ingest exposes a driver SKU separately).
- **`max_worker_nodes_cluster_ceiling`:** Bundled ingest: ceiling **includes driver** with workers (not workers-only); value **`1`** matches **single-node** ingest posture (**Driver and worker ingest**).
- **`total_vcpus_cluster_ceiling`**, **`total_memory_gb_cluster_ceiling`:** Bundled: **cluster** vCPU/memory **ceiling includes driver**.
- **`avg_worker_nodes_consumed`**, **`p95_worker_nodes_consumed`**, **`p99_worker_nodes_consumed`:** Bundled: node-count shape **includes driver** (**Consumed** tails for Step 1).
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
- **`peak_cpu_utilization_pct_sample_window`**, **`peak_memory_utilization_pct_sample_window`**

### Ignore for optimization logic

**Do not** size clusters from identifiers or timestamps alone:

- **`workspace_id`**, **`workspace_name`**, **`cluster_id`**, **`job_id`**, **`job_name`**
- **`run_start_time_utc`**, **`run_end_time_utc`**, **`job_run_date`**, **`metrics_ingest_timestamp_utc`**

---

## Required Analysis Procedure

Follow this sequence **to completion**. Do **not** stop after Step 1.

### Step 1: Cluster ceiling vs consumed (autoscaler bounds)

**Role:** Bounds the **capacity shell** (autoscaler/policy **max**, worker tails) so CPU/memory ratios in Steps **2-4** are read **in cluster context**. **Shell slack** is **not** the optimization target by itself.

Compare **`max_worker_nodes_cluster_ceiling`** and **`total_vcpus_cluster_ceiling`** / **`total_memory_gb_cluster_ceiling`** versus **`avg_worker_nodes_consumed`**, **`p95_worker_nodes_consumed`**, **`p99_worker_nodes_consumed`**. Cross-check **`peak_cpu_utilization_pct_sample_window`**, **`peak_memory_utilization_pct_sample_window`**.

If sustained gap shows **configured max materially above distributional need**, use **`OVERPROVISIONED_AUTOSCALE`**. In rationale: anchor on **burst headroom tolerance** (**not** unspent DBU unless user states committed capacity). **`OVERPROVISIONED_AUTOSCALE`** still requires **explicit Step 2 (and Steps 3-4 where applicable)** before final **`recommended_configuration`** (autoscale knobs, SKU, **`cluster_topology`**). **Step 2** must justify **whether** tightening workers is safe given **allocated-vs-utilized** CPU/memory and peaks. **Step 4** applies when **`recommended_configuration`** changes SKU or **`vm_family`** versus **`current_configuration`**, unless **`INSUFFICIENT_EVIDENCE`** states Step 4 inputs are missing.

### Step 2: Consumed (`allocated`) vs utilized (**mandatory** before final sizing)

**Role:** Grounds cost and saturation in **workload CPU and memory** versus what the cluster actually held (**Consumed** allocation). Pair with Step 4 for SKU/family conclusions.

Apply SKU / series / **`PER_NODE_UNDERUTILIZED`** against shells that actually ran (**consumed allocation**):

- Ratio **`avg_vcpus_utilized_by_workload / avg_vcpus_allocated_active_cluster`**
- Ratio **`avg_memory_gb_utilized_by_workload / avg_memory_gb_allocated_active_cluster`**
- Peaks **`peak_cpu_utilization_pct_sample_window`** and **`peak_memory_utilization_pct_sample_window`** to avoid shrinking into transient spikes.

Low **utilized/allocated** ratio **given safe peaks**, use **`PER_NODE_UNDERUTILIZED`**. Still cite **`cluster_avg_*_pct_of_ceiling_capacity`** separately; orthogonal denominator.

### Step 3: Workflow orchestration parallelism

**Role:** Explains whether **scaled-out workers** materially shorten the job or mainly add idle capacity (parallelism context for Steps **1-2** levers).

**`workflow_task_count`** means Databricks workflow **tasks** (**not Spark executor parallelism**).

Decide **`LOW_PARALLELISM`** from:
- Whether multiple job tasks run concurrently or strictly sequentially
- Whether individual tasks are lightweight orchestration steps versus heavy Spark compute
- Whether scaling **additional** workers meaningfully **reduces** end-to-end runtime or **merely increases** idle capacity

If the workflow consists of few tasks, runs primarily sequentially, or each task underutilizes available cores, treat parallelism benefit as limited and flag:
- `LOW_PARALLELISM`

### Step 4: Azure VM series (CPU vs RAM skew)

**Role:** Turns **CPU and memory utilization shape** (**allocated vs utilized**, peaks, wait%) into **`vm_family` / SKU series** (**D**, **E**, **F**) and **`VM_FAMILY_MISMATCH`**. Evaluate **every** pass when SKU/family-change is on the table; default **keep current family** with explicit reasoning if ratios are balanced.

Inputs: ratios **allocated versus utilized**, **`cluster_avg_*_pct_of_ceiling_capacity`**, peak `*_pct_sample_window`, diagnostics **`avg_cpu_user_pct|system_pct|wait_pct`**.

- **GiB-heavy:** elevated memory ratio / **`peak_memory_utilization_pct_sample_window`** / shuffle-cache narrative; take an **E** stance; mismatched **D|F** with stress, use **`VM_FAMILY_MISMATCH`** toward **memory-rightsizing**/**E**.
- **CPU-heavy + memory slack:** conversely consider **F** **only if** **`avg_cpu_wait_pct`** excludes pure I/O-wait pathology; otherwise prefer **E-to-D** bias first.
- **Balanced:** default to **D**. **`processed_*`** optional weight only.

Runs **after** Steps 1-3; obey **Core Optimization Principles section 2** ordering before series churn.

**E-series:**
- Prefer when memory pressure dominates: high **`avg_memory_gb_utilized_by_workload` / `avg_memory_gb_allocated_active_cluster`**, high **`peak_memory_utilization_pct_sample_window`**, or shuffle/broadcast/cache behavior.
- Use **CPU diagnostics** (for example **`avg_cpu_wait_pct`**) so **I/O-bound or waiting** CPUs are **not** read as spare compute that warrants **F**; fix bottlenecks first.
- **`INSUFFICIENT_EVIDENCE`** or conservative **`NO_CHANGE`** when spikes or shuffle make averages misleading alone.

**F-series:**
- Prefer evaluating **F** only when the run shows a **compute-bound** profile with **clear memory headroom** on **`azure_worker_vm_size`** (solid **`avg_vcpus_utilized_by_workload` / `avg_vcpus_allocated_active_cluster`**, subdued memory ratio or **`cluster_avg_memory_utilization_pct_of_ceiling_capacity`** story) sustained across the window, not partial/cold-start except as noted in **`confidence_notes`**.
- **Do not** recommend **F** if memory ratios or **`peak_memory_utilization_pct_sample_window`** contradict headroom claims, or shuffle/broadcast/cache risk is material.
- **High `avg_cpu_wait_pct` alone is not grounds for F**; triangulate disk, network, shuffle, and driver bottlenecks separately from insufficient CPU capacity.
- **E-series to D-series** often suffices when the problem is "**memory-series without memory justification**"; move to **F** when the narrative is specifically **needed compute density per GiB**, not merely "**wrong series**."

**Always** assign **`VM_FAMILY_MISMATCH`** whenever fit skews (**no F-only supplemental code** unless external program extends enumerations).

### Step 5: Single-node topology (**true** driver+executor VM)

**`single-node` here** means **Databricks single-node topology** (**one VM** colocates driver **and** workloads). **`autoscale.min==max==1`** on classic often **still means driver+worker (2-node)**; that signal alone is insufficient.

**Bundled ingest:** **`p95`/`p99`/`avg` node counts** below follow **Driver and worker ingest** (**driver included**). When split worker-only metrics exist, use those for worker tail shapes instead.

Eligible pattern examples:
- **`p95_worker_nodes_consumed`** or **`p99_worker_nodes_consumed`** is less than or equal to **2**
- **`avg_vcpus_utilized_by_workload` / `avg_vcpus_allocated_active_cluster`** (and memory analog) indicate very low utilization of actively allocated worker capacity
- **`workflow_task_count`** plus orchestration semantics suggest minimal parallel gain

If eligible, flag:
- `SINGLE_NODE_ELIGIBLE`

If you recommend switching to a **single-node cluster topology** for the next configuration, also flag:
- `SINGLE_NODE_RECOMMENDED`

If single-node is recommended, emit JSON **`cluster_topology:"single_node"`** and **`comparison.single_node.recommended:true`** (**Output Expectations**).

---

## Recommendation Rules

### VM Sizing
- Maintain a minimum of **8 vCPUs per node**.
- Prefer reducing node count before recommending different node sizes (Core Optimization Principles section 2, including rightsizing SKU or family when count is already low).
- Apply **Step 4** when choosing or changing **Azure D / E / F** series so CPU versus GiB utilization skew stays consistent.
- If changing family or SKU, ensure proposed memory is sufficient for observed workload behavior, peaks, and shuffle/broadcast/cache risk.

### Single-node memory rule
- When recommending a single-node topology, do not recommend **`memory_gb_per_node`** lower than evidenced need from **`avg_memory_gb_utilized_by_workload`** (and peaks / pipeline risk) unless explicitly marked as a validation candidate.
- If memory headroom is unclear, prefer a safer recommendation over an aggressively smaller footprint.

### Topology vs autoscale
- **`multi_node`:** emit realistic **`min_workers`/`max_workers`** for SKU + evidence.
- **`single_node`:** **`autoscale`** numeric pair must obey **Databricks policy** for that SKU (often **`0`** workers when pool N/A).


### Reason codes `NO_CHANGE_RECOMMENDED` and `INSUFFICIENT_EVIDENCE`
- **`NO_CHANGE_RECOMMENDED`:** Metrics support that **no safe cost change** is warranted (for example, already at minimum viable 8 vCPU footprint, rightsizing would add operational risk without evidence, or pipeline constraints block change). Use with **`change_required`: false** and recommended configuration **mirroring** current.
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
- `SINGLE_NODE_ELIGIBLE`
- `SINGLE_NODE_RECOMMENDED`
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

**Enums:** **`cluster_topology`** must be the string **`multi_node`** or **`single_node`** (per Step 5 definition).

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
      "vcpus_per_node": 0,
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
      "vcpus_per_node": 0,
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
