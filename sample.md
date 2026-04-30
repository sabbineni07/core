---
name: databricks-efficiency
description: Evaluate Azure Databricks job and cluster efficiency using runtime metrics, identify over-provisioning, assess CPU and memory utilization, compare multi-node autoscale vs single-node topology, recommend Azure VM family fit (D/E/F), and produce strict JSON for Git-based recommendation tracking.
---

### Databricks Efficiency Skill

**Scope:** Azure Databricks on Azure VMs only (no AWS/GCP).

**Separation of concerns:** This file defines **domain analysis rules** and the **output JSON contract** only. How metrics are ingested, which prompt or agent instruction file is used, and any approval or apply workflow are **out of scope** here and belong in those separate artifacts.

Use this skill when:
- reviewing Databricks job or cluster run metrics
- evaluating whether a workload is over-provisioned
- determining whether a **Databricks single-node cluster** (one VM: driver and executors colocated) is appropriate
- deciding whether a different Azure VM family can improve cluster fit
- producing or updating recommendation JSON files stored in Git

---

### Historical Recommendation Context
If available, use a compact historical recommendation summary stored outside `.github`, such as:
- `copilot-results/history-summary.md`
- or `copilot-results/history-summary.json`

Use historical context as **secondary guidance**, not as a substitute for current run metrics.

Do **not** assume the model automatically scans all files under `copilot-results/`.
If historical context is needed, prefer one explicit summary file or explicitly referenced files over many raw recommendation JSON documents.

---

## Core Optimization Principles

### 1. Minimum node size rule
- Never recommend node types smaller than **8 vCPUs**.
- Reason: Databricks typically reserves approximately one vCPU per node for OS and platform services, which makes small nodes disproportionately inefficient.

### 2. Prefer reducing node count before shrinking node size
- If utilization is low, reduce the number of nodes or tighten **autoscale** bounds first—this is usually the highest-leverage cost lever.
- When node count is **already low** (for example, **`min_workers`** and actual consumption are at a practical floor, or further reduction would conflict with parallelism or headroom), treat **node-count reduction** as **exhausted** for this pass. Then prefer **rightsizing**: a **smaller SKU** within the **8 vCPU minimum** rule and/or a **better-fitting VM family** (see Step 4), **only when metrics** (`cpu_utilization_pct`, `memory_utilization_pct`, nodes consumed, Spark tasks) **show sustained headroom**. Document uncertainty in **`confidence_notes`** or impact fields rather than implying zero performance risk.
- If the workload is very small and lightly parallelized, evaluate whether switching to a **single-node cluster topology** is justified (see Step 5).
- Do not recommend shrinking below 8 vCPUs per node.

### 3. Use evidence, not generic best practice
- Every recommendation must map to observed metrics or clearly described pipeline behavior.
- Do not recommend larger clusters or higher-memory nodes unless the evidence supports it.
- Do not assume future workload growth unless explicitly stated.

---

## Metric Interpretation Rules

### Primary decision metrics (high signal)
Use these as the main evidence for recommendations:
- `node_type`
- `max_nodes_provisioned`
- `avg_nodes_consumed`
- `p95_nodes_consumed`
- `p99_nodes_consumed`
- `cpu_utilization_pct`
- `memory_utilization_pct`
- `avg_cpus_consumed`
- `avg_cpus_utilized`
- `avg_memory_gb_consumed`
- `avg_memory_gb_utilized`
- `number_of_spark_tasks`
- `duration`
- `processed_bytes` (only as an indicator of a small workload)

### Validation metrics (secondary only)
Use these only to validate a conclusion, not to drive it:
- `provisioning_efficiency_pct`
- `cpu_utilization_efficiency_pct`
- `memory_utilization_efficiency_pct`

### Diagnostic metrics (context only)
Use these to explain behavior, not to justify scaling:
- `avg_cpu_user_pct`
- `avg_cpu_system_pct`
- `avg_cpu_wait_pct`
- `peak_cpu_utilization_pct`
- `peak_memory_utilization_pct`

### Ignore for optimization logic
Do not derive recommendations from:
- `workspace_id`
- `workspace_name`
- `cluster_id`
- `job_id`
- `job_name`
- Timestamps such as start/end time or upsert time.

---

## Required Analysis Procedure

Follow this sequence.

### Step 1 — Cluster-level provisioning analysis
Determine whether the cluster is over-provisioned by comparing:
- `max_nodes_provisioned`
- `avg_nodes_consumed`
- `p95_nodes_consumed`
- `p99_nodes_consumed`

If the configured max is far above actual usage, flag:
- `OVERPROVISIONED_AUTOSCALE`

### Step 2 — Per-node efficiency analysis
Evaluate whether active nodes are themselves underutilized:
- CPU very low on active nodes
- Memory very low on active nodes
- Available cores not being effectively used

If active nodes are underutilized, flag:
- `PER_NODE_UNDERUTILIZED`

### Step 3 — Spark parallelism check
Assess whether Spark can actually use the available cores:
- compare `number_of_spark_tasks` to available cores
- identify if the workload is fundamentally too small or too lightly parallelized

If parallelism is low, flag:
- `LOW_PARALLELISM`

### Step 4 — VM family fit (Azure): CPU skew versus RAM skew

**Framing.** Compare **CPU pressure** (`cpu_utilization_pct`, **peak CPU** diagnostics, **`avg_cpu_user_pct`**, **`avg_cpu_wait_pct`**, **`avg_cpu_system_pct`**) to **memory pressure** (`memory_utilization_pct`, **`peak_memory_utilization_pct`**, **`avg_memory_gb_utilized`** / **`avg_memory_gb_consumed`** vs provisioned RAM). Treat **both** skews deliberately:

| Skew | Wrong fit example | Typical move |
|------|-------------------|----------------|
| **GiB-heavy** (memory pressure **high vs** CPU utilization) | **D**/**F** SKU but sustained memory stress, peaks, spill risk | Prefer **E** or **rightsized memory** |
| **CPU-heavy vs provisioned RAM** (compute-bound with **RAM headroom**) | **E** oversized for memory story, or **D** with **low memory utilization** and **high CPU utilization** | **E → D**, or evaluate **F** when compute density fits cost goals |
| **Balanced** CPU and memory | neither dominates | Prefer **D** as default |

**Order.** Runs **after** Steps 1–3. Family moves are SKU-shape decisions; pair with Principle 2—**exhaust sensible node-count and autoscale levers before** leaning on series change alone.

**D-series (general-purpose).** Prefer when **`cpu_utilization_pct`** and **`memory_utilization_pct`** (plus peaks/diagnostics where available) neither tell a purely memory-bound nor purely compute-heavy story—a **balanced** profile.

**E-series (memory-optimized — GiB-leaning workload).**

- Prefer when **memory pressure is high relative to CPU**: sustained **`memory_utilization_pct`**, high **`peak_memory_utilization_pct`**, or workloads that imply large memory footprints (**heavy shuffle**, **large broadcasts**, **cached** stages).
- Use **CPU diagnostics** (for example **`avg_cpu_wait_pct`**) so **I/O-bound or waiting** CPUs are **not** read as spare compute that warrants **F**—fix bottlenecks first.
- **`INSUFFICIENT_EVIDENCE`** or conservative **`NO_CHANGE`** when spikes or shuffle make averages misleading alone.

**F-series (compute-optimized — dense CPU versus provisioned RAM).**

- Prefer evaluating **F** only when the run shows a **compute-bound** profile with **clear memory headroom** on the **current** SKU (high **`cpu_utilization_pct`**, **low memory utilization vs provisioned GiB**) and evidence is sustained—not a partial or cold-start run unless explicitly addressed in **`confidence_notes`**.
- **Do not** recommend **F** if **memory is high or volatile**, **`peak_memory_utilization_pct`** is concerning, or pipeline behavior (**shuffle-heavy**, **broadcast-heavy**, caching) contradicts trusting low average RAM.
- **High `avg_cpu_wait_pct` alone is not grounds for F**—triangulate disk, network, shuffle, and driver bottlenecks separately from insufficient CPU capacity.
- **E → D** often suffices when the problem is “**memory-series without memory justification**”; move to **F** when the narrative is specifically **needed compute density per GiB**, not merely “wrong series.”

**Reason code.**

- **`VM_FAMILY_MISMATCH`** — whenever the observed **CPU-vs-RAM utilization pattern** mismatches current **D**, **E**, or **F** choice (covers both GiB-heavy and compute-heavy corrections). Prefer **explicit rationale** naming **skew direction** (**GiB-heavy** vs **CPU-heavy**) and targeted series; **do not introduce a separate F-only reason code** unless your program chooses to extend the enum outside this skill.

### Step 5 — Single-node cluster topology (not “one worker + driver”)

In this skill, **single-node** means a **Databricks single-node cluster**: **one Azure VM** runs **both** driver and executor workloads (colocated). It does **not** mean “driver VM + one worker VM,” which is typically **two** nodes.

Do **not** infer single-node intent from **`autoscale.min_workers === max_workers === 1`** alone on a classic cluster—that usually implies **one worker in addition to a driver**.

A workload is **single-node eligible** when evidence suggests minimal benefit from spreading work across workers, for example:
- `p95_nodes_consumed` or `p99_nodes_consumed` is less than or equal to 2 (where that metric reflects worker-side scaling for your pipeline)
- CPU and memory utilization are very low on active capacity
- Spark task count is low relative to available cores

If eligible, flag:
- `SINGLE_NODE_ELIGIBLE`

If you recommend switching to a **single-node cluster topology** for the next configuration, also flag:
- `SINGLE_NODE_RECOMMENDED`

Express topology in JSON with **`cluster_topology`: `"single_node"`** on the recommended configuration (see Output Expectations). Pair with **`comparison.single_node.recommended`: true** and narrative in **`rationale`** / **`notes`**.

---

## Recommendation Rules

### VM Sizing
- Maintain a minimum of **8 vCPUs per node**.
- Prefer reducing node count before recommending different node sizes (Core Principle §2—including rightsizing SKU or family when count is already low).
- Apply **Step 4** when choosing or changing **Azure D / E / F** series so CPU versus GiB utilization skew stays consistent.
- If changing family or SKU, ensure proposed memory is sufficient for observed workload behavior, peaks, and shuffle/broadcast/cache risk.

### Single-node memory rule
- When recommending a single-node topology, do not recommend memory lower than the observed average utilized memory unless explicitly marked as a validation candidate.
- If memory headroom is unclear, prefer a safer recommendation over an aggressively smaller footprint.

### Topology vs autoscale
- **Multi-node:** Use **`cluster_topology`: `"multi_node"`** and set **`autoscale.min_workers` / `max_workers`** to the values operators should apply for worker pools.
- **Single-node cluster:** Use **`cluster_topology`: `"single_node"`** and set **`autoscale`** to the values your platform expects for that mode (for example both `0` when worker pool does not apply—align with how your jobs/clusters are configured manually).

### Reason codes `NO_CHANGE_RECOMMENDED` and `INSUFFICIENT_EVIDENCE`
- **`NO_CHANGE_RECOMMENDED`** — Metrics support that **no safe cost change** is warranted (for example, already at minimum viable 8 vCPU footprint, rightsizing would add operational risk without evidence, or pipeline constraints block change). Use with **`change_required`: false** and recommended configuration **mirroring** current.
- **`INSUFFICIENT_EVIDENCE`** — Primary metrics are **missing, incomplete, or not representative** (partial run, cold start only). Prefer **`change_required`: false**, explain gaps in **`confidence_notes`**, and avoid speculative family or topology changes.

---

## Recommendation ID Rule

Every recommendation should include a stable `recommendation_id`

Build it as follows:
- start with `<job_name>__<job_date>`
- append `__<job_id>` if available
- append `__<cluster_id>` if available

Example:
`pipeline_name_2026-01-01_12345678910111_0101-024567-8gegooked`

---

## Lifecycle & Maturity Rules

Supported lifecycle states:
- `RECOMMENDED`
- `ACCEPTED`
- `APPLIED`
- `VERIFIED`
- `REJECTED`

Maturity must stay aligned with lifecycle:

- `RECOMMENDED` -> `maturity_score = 0`, `maturity_level = "generated"`
- `ACCEPTED` -> `maturity_score = 33`, `maturity_level = "accepted"`
- `APPLIED` -> `maturity_score = 66`, `maturity_level = "applied"`
- `VERIFIED` -> `maturity_score = 100`, `maturity_level = "verified"`
- `REJECTED` -> `maturity_score = 0`, `maturity_level = "rejected"`

---

## Reason Codes

Return `reason_codes` as an array of one or more:
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

Return **strict JSON only** using the structure below.

### Output contract (read before emitting JSON)

1. **Single authoritative block** — All actionable sizing (Azure SKU, vCPU/RAM, **cluster topology**, **autoscale**) lives under **`comparison.recommended_configuration`** (and **`comparison.current_configuration`** for the baseline). Do **not** duplicate a second top-level cluster recommendation object.

2. **`change_required` and `rationale`** — Live under **`comparison`**. **`rationale`** is an array of short strings citing metrics or behavior.

3. **When `change_required` is `false`** — Set **`recommended_configuration`** fields **equal** to **`current_configuration`** (same SKU, topology, autoscale). Set **`expected_directional_impact`** to **`cost`/`performance`/`risk`** reflecting **no operational change** unless you document an exceptional case in **`confidence_notes`**.

4. **`comparison.single_node`** — Contains **only** **`eligible`**, **`recommended`**, and **`notes`** (short strings). Do **not** put **`azure_node_type`**, **`autoscale`**, or memory/vCPU here; those belong only in **`recommended_configuration`**.

5. **Topology** — Use **`cluster_topology`**: **`"multi_node"`** for standard driver + workers, **`"single_node"`** for a single-node Databricks cluster (one VM, driver and executors colocated). Align **`autoscale`** with how your organization configures each mode in the Databricks UI/API.

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
PR description (copy/paste)
This PR updates the Databricks efficiency agent skill (copilot_agent_skills/SKILL.md) so recommendations are clearer, easier to validate, and safer to apply on Azure Databricks. The JSON contract moves to schema_version 2.0.0 by merging the old comparison and cluster_recommendation objects into a single comparison block: one place for change_required, rationale, current vs recommended sizing, and expected_directional_impact, with an output contract that avoids conflicting “recommended” truths and requires mirroring current config when no change is advised. single_node is now nested under comparison and carries only eligibility flags and notes; real topology and sizing live on recommended_configuration, including cluster_topology (multi_node vs true single-node Databricks: one VM for driver+executors, not “one worker + driver”), which removes ambiguity with autoscale. Lifecycle text now maps REJECTED to maturity_level: "rejected" for clearer reporting, and the skill scope is Azure-only, with a short separation of concerns note so prompts and agent instructions own orchestration while the skill owns rules and schema.
Analysis content was strengthened to improve accept/verify outcomes: Core Principle §2 now covers rightsizing SKU or family when node count is already low; Step 4 is expanded into a full CPU vs GiB RAM skew framework for D / E / F (when to lean E vs D vs F, exclusions like shuffle/broadcast/caching and wait%, E→D vs F, and VM_FAMILY_MISMATCH for all family misfits without a separate F-only code). Reason-code guidance for NO_CHANGE_RECOMMENDED and INSUFFICIENT_EVIDENCE is spelled out. BACKLOG.md is refreshed to mark these items done and to list only optional follow-ups (e.g. org-specific numeric thresholds, optional conflict rules for combining reason codes, CI outside the skill). Together, the changes reduce duplicate or contradictory JSON, align language with real Databricks topologies, and give agents consistent, evidence-backed levers for cost optimization while leaving room for human validation.
