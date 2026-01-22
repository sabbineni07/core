-- ============================================================================
-- Task-Level Cost Allocation with Parallel Task Support
-- ============================================================================
-- This file contains SQL queries for accurately allocating job costs to tasks
-- when tasks run in parallel. Uses time-based allocation to handle concurrent
-- task execution correctly.
--
-- Key Concept:
-- - Allocates costs based on which tasks were actively running at each time period
-- - Handles parallel execution by dividing costs proportionally among active tasks
-- - More accurate than simple duration-based allocation
--
-- System Tables Used:
-- - system.lakeflow.job_task_run_timeline: Task execution periods
-- - system.lakeflow.job_run_timeline: Job execution periods
-- - system.billing.usage: DBU consumption records
-- - system.billing.list_prices: Pricing information
--
-- References:
-- - Jobs tables: https://docs.databricks.com/aws/en/admin/system-tables/jobs
-- - Billing tables: https://docs.databricks.com/aws/en/admin/system-tables/billing
-- ============================================================================

-- ============================================================================
-- Query 1: Time-Based Task Cost Allocation (Handles Parallel Tasks)
-- ============================================================================
-- Allocates costs to tasks based on actual time periods when tasks were running
-- Properly handles parallel execution by dividing costs among concurrent tasks
--
-- Parameters:
--   ${start_date} - Start date (e.g., '2024-01-01')
--   ${end_date} - End date (e.g., '2024-01-31')
--   ${job_id} - Optional: specific job ID to filter
--   ${job_run_id} - Optional: specific job run ID to filter
-- ============================================================================

WITH job_task_runs AS (
    SELECT 
        tr.workspace_id,
        tr.job_id,
        tr.job_run_id,
        tr.task_key,
        tr.period_start_time AS task_start,
        tr.period_end_time AS task_end,
        tr.result_state,
        tr.compute_ids[0] AS cluster_id,
        TIMESTAMPDIFF(SECOND, tr.period_start_time, tr.period_end_time) AS duration_seconds
    FROM system.lakeflow.job_task_run_timeline tr
    WHERE tr.period_start_time >= '${start_date}'
        AND tr.period_start_time < '${end_date}'
        AND size(tr.compute_ids) > 0
        -- Only include completed tasks (final slices)
        AND tr.result_state IS NOT NULL
),
job_runs AS (
    SELECT 
        jr.workspace_id,
        jr.job_id,
        jr.job_run_id,
        jr.period_start_time AS job_start,
        jr.period_end_time AS job_end,
        jr.compute_ids[0] AS cluster_id,
        TIMESTAMPDIFF(SECOND, jr.period_start_time, jr.period_end_time) AS job_duration_seconds
    FROM system.lakeflow.job_run_timeline jr
    WHERE jr.period_start_time >= '${start_date}'
        AND jr.period_start_time < '${end_date}'
        AND size(jr.compute_ids) > 0
),
usage_records AS (
    SELECT 
        u.metadata.usage_metadata.job_id AS job_id,
        u.metadata.usage_metadata.job_run_id AS job_run_id,
        u.metadata.usage_metadata.cluster_id AS cluster_id,
        u.billing_origin_product AS product,
        u.usage_quantity AS dbu_quantity,
        u.usage_date,
        -- Convert usage_date to timestamp (assuming usage_date is date, use start of day)
        CAST(u.usage_date AS TIMESTAMP) AS usage_timestamp,
        u.sku_name
    FROM system.billing.usage u
    WHERE u.usage_date >= '${start_date}'
        AND u.usage_date < '${end_date}'
        AND u.metadata.usage_metadata.job_id IS NOT NULL
),
pricing_data AS (
    SELECT 
        sku_name,
        price_start_time,
        price_end_time,
        pricing.default AS list_price_per_dbu
    FROM system.billing.list_prices
    WHERE pricing.default IS NOT NULL
),
usage_with_pricing AS (
    SELECT 
        u.job_id,
        u.job_run_id,
        u.cluster_id,
        u.product,
        u.dbu_quantity,
        u.usage_date,
        u.usage_timestamp,
        u.sku_name,
        COALESCE(p.list_price_per_dbu, 0) AS price_per_dbu,
        u.dbu_quantity * COALESCE(p.list_price_per_dbu, 0) AS cost_usd
    FROM usage_records u
    LEFT JOIN pricing_data p
        ON u.sku_name = p.sku_name
        AND p.price_start_time <= u.usage_date
        AND (p.price_end_time IS NULL OR u.usage_date < p.price_end_time)
),
-- Step 1: Find which tasks were running at each usage timestamp
-- This handles parallel tasks by identifying all active tasks at each point in time
tasks_active_at_usage AS (
    SELECT 
        up.job_id,
        up.job_run_id,
        up.usage_timestamp,
        up.dbu_quantity,
        up.cost_usd,
        up.cluster_id,
        jtr.task_key,
        jtr.task_start,
        jtr.task_end,
        jtr.duration_seconds AS task_duration_seconds,
        -- Calculate overlap duration in seconds
        -- Overlap = MIN(task_end, usage_timestamp_end) - MAX(task_start, usage_timestamp_start)
        GREATEST(0, 
            LEAST(
                UNIX_TIMESTAMP(jtr.task_end), 
                UNIX_TIMESTAMP(up.usage_timestamp + INTERVAL 1 DAY)
            ) -
            GREATEST(
                UNIX_TIMESTAMP(jtr.task_start), 
                UNIX_TIMESTAMP(up.usage_timestamp)
            )
        ) AS overlap_seconds
    FROM usage_with_pricing up
    INNER JOIN job_task_runs jtr
        ON up.job_id = jtr.job_id
        AND up.job_run_id = jtr.job_run_id
        AND up.cluster_id = jtr.cluster_id
        -- Task was active during usage period (time overlap check)
        AND jtr.task_start <= up.usage_timestamp + INTERVAL 1 DAY
        AND jtr.task_end >= up.usage_timestamp
),
-- Step 2: Calculate total active task overlap per usage timestamp
-- This gives us the denominator for proportional allocation
total_active_tasks_per_usage AS (
    SELECT 
        job_id,
        job_run_id,
        usage_timestamp,
        SUM(overlap_seconds) AS total_overlap_seconds,
        COUNT(DISTINCT task_key) AS num_parallel_tasks
    FROM tasks_active_at_usage
    GROUP BY job_id, job_run_id, usage_timestamp
),
-- Step 3: Allocate cost proportionally based on overlap duration
-- Formula: allocated_cost = (task_overlap / total_overlap) * usage_cost
task_cost_allocation AS (
    SELECT 
        ta.job_id,
        ta.job_run_id,
        ta.usage_timestamp,
        ta.task_key,
        ta.task_start,
        ta.task_end,
        ta.task_duration_seconds,
        ta.overlap_seconds,
        tat.total_overlap_seconds,
        tat.num_parallel_tasks,
        ta.dbu_quantity,
        ta.cost_usd,
        -- Allocate proportionally: (task_overlap / total_overlap) * cost
        CASE 
            WHEN tat.total_overlap_seconds > 0 
            THEN (ta.cost_usd * ta.overlap_seconds) / tat.total_overlap_seconds
            ELSE 0
        END AS allocated_cost_usd,
        CASE 
            WHEN tat.total_overlap_seconds > 0 
            THEN (ta.dbu_quantity * ta.overlap_seconds) / tat.total_overlap_seconds
            ELSE 0
        END AS allocated_dbus
    FROM tasks_active_at_usage ta
    INNER JOIN total_active_tasks_per_usage tat
        ON ta.job_id = tat.job_id
        AND ta.job_run_id = tat.job_run_id
        AND ta.usage_timestamp = tat.usage_timestamp
),
-- Step 4: Aggregate allocated costs by task
task_usage_summary AS (
    SELECT 
        job_id,
        job_run_id,
        task_key,
        MIN(task_start) AS task_start,
        MAX(task_end) AS task_end,
        SUM(task_duration_seconds) AS total_task_duration_seconds,
        SUM(allocated_dbus) AS total_dbus_allocated,
        SUM(allocated_cost_usd) AS total_cost_allocated,
        COUNT(DISTINCT usage_timestamp) AS usage_records_count,
        AVG(num_parallel_tasks) AS avg_parallel_tasks
    FROM task_cost_allocation
    GROUP BY job_id, job_run_id, task_key
),
-- Step 5: Join with job and task details for final output
job_names AS (
    SELECT DISTINCT
        workspace_id,
        job_id,
        name AS job_name
    FROM system.lakeflow.jobs
    WHERE delete_time IS NULL
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY workspace_id, job_id 
        ORDER BY change_time DESC
    ) = 1
),
task_details AS (
    SELECT DISTINCT
        workspace_id,
        job_id,
        task_key,
        depends_on_keys
    FROM system.lakeflow.job_tasks
    WHERE delete_time IS NULL
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY workspace_id, job_id, task_key 
        ORDER BY change_time DESC
    ) = 1
)
SELECT 
    jr.workspace_id,
    jr.job_id,
    COALESCE(jn.job_name, jr.job_id) AS job_name,
    tus.job_run_id,
    tus.task_key,
    td.depends_on_keys,
    tus.task_start,
    tus.task_end,
    tus.total_task_duration_seconds,
    tus.total_task_duration_seconds / 60.0 AS task_duration_minutes,
    tus.total_dbus_allocated,
    tus.total_cost_allocated,
    tus.usage_records_count,
    tus.avg_parallel_tasks,
    -- Cost efficiency metrics
    CASE 
        WHEN tus.total_task_duration_seconds > 0 
        THEN (tus.total_cost_allocated * 3600.0) / tus.total_task_duration_seconds 
        ELSE 0 
    END AS cost_per_hour_usd,
    CASE 
        WHEN tus.total_dbus_allocated > 0 
        THEN tus.total_cost_allocated / tus.total_dbus_allocated 
        ELSE 0 
    END AS cost_per_dbu_usd,
    -- Percentage of job cost (calculated in next CTE)
    0.0 AS pct_of_job_cost
FROM task_usage_summary tus
INNER JOIN job_runs jr
    ON tus.job_id = jr.job_id
    AND tus.job_run_id = jr.job_run_id
LEFT JOIN job_names jn
    ON jr.workspace_id = jn.workspace_id
    AND jr.job_id = jn.job_id
LEFT JOIN task_details td
    ON jr.workspace_id = td.workspace_id
    AND jr.job_id = td.job_id
    AND tus.task_key = td.task_key
ORDER BY jr.job_id, tus.job_run_id, tus.task_key;


-- ============================================================================
-- Query 2: Task Cost Allocation with Job Cost Percentage
-- ============================================================================
-- Same as Query 1, but includes percentage of total job cost per task
-- ============================================================================

WITH job_task_runs AS (
    SELECT 
        tr.workspace_id,
        tr.job_id,
        tr.job_run_id,
        tr.task_key,
        tr.period_start_time AS task_start,
        tr.period_end_time AS task_end,
        tr.result_state,
        tr.compute_ids[0] AS cluster_id,
        TIMESTAMPDIFF(SECOND, tr.period_start_time, tr.period_end_time) AS duration_seconds
    FROM system.lakeflow.job_task_run_timeline tr
    WHERE tr.period_start_time >= '${start_date}'
        AND tr.period_start_time < '${end_date}'
        AND size(tr.compute_ids) > 0
        AND tr.result_state IS NOT NULL
),
job_runs AS (
    SELECT 
        jr.workspace_id,
        jr.job_id,
        jr.job_run_id,
        jr.period_start_time AS job_start,
        jr.period_end_time AS job_end,
        jr.compute_ids[0] AS cluster_id
    FROM system.lakeflow.job_run_timeline jr
    WHERE jr.period_start_time >= '${start_date}'
        AND jr.period_start_time < '${end_date}'
        AND size(jr.compute_ids) > 0
),
usage_records AS (
    SELECT 
        u.metadata.usage_metadata.job_id AS job_id,
        u.metadata.usage_metadata.job_run_id AS job_run_id,
        u.metadata.usage_metadata.cluster_id AS cluster_id,
        u.billing_origin_product AS product,
        u.usage_quantity AS dbu_quantity,
        u.usage_date,
        CAST(u.usage_date AS TIMESTAMP) AS usage_timestamp,
        u.sku_name
    FROM system.billing.usage u
    WHERE u.usage_date >= '${start_date}'
        AND u.usage_date < '${end_date}'
        AND u.metadata.usage_metadata.job_id IS NOT NULL
),
pricing_data AS (
    SELECT 
        sku_name,
        price_start_time,
        price_end_time,
        pricing.default AS list_price_per_dbu
    FROM system.billing.list_prices
    WHERE pricing.default IS NOT NULL
),
usage_with_pricing AS (
    SELECT 
        u.job_id,
        u.job_run_id,
        u.cluster_id,
        u.product,
        u.dbu_quantity,
        u.usage_date,
        u.usage_timestamp,
        u.sku_name,
        COALESCE(p.list_price_per_dbu, 0) AS price_per_dbu,
        u.dbu_quantity * COALESCE(p.list_price_per_dbu, 0) AS cost_usd
    FROM usage_records u
    LEFT JOIN pricing_data p
        ON u.sku_name = p.sku_name
        AND p.price_start_time <= u.usage_date
        AND (p.price_end_time IS NULL OR u.usage_date < p.price_end_time)
),
tasks_active_at_usage AS (
    SELECT 
        up.job_id,
        up.job_run_id,
        up.usage_timestamp,
        up.dbu_quantity,
        up.cost_usd,
        up.cluster_id,
        jtr.task_key,
        jtr.task_start,
        jtr.task_end,
        jtr.duration_seconds AS task_duration_seconds,
        GREATEST(0, 
            LEAST(
                UNIX_TIMESTAMP(jtr.task_end), 
                UNIX_TIMESTAMP(up.usage_timestamp + INTERVAL 1 DAY)
            ) -
            GREATEST(
                UNIX_TIMESTAMP(jtr.task_start), 
                UNIX_TIMESTAMP(up.usage_timestamp)
            )
        ) AS overlap_seconds
    FROM usage_with_pricing up
    INNER JOIN job_task_runs jtr
        ON up.job_id = jtr.job_id
        AND up.job_run_id = jtr.job_run_id
        AND up.cluster_id = jtr.cluster_id
        AND jtr.task_start <= up.usage_timestamp + INTERVAL 1 DAY
        AND jtr.task_end >= up.usage_timestamp
),
total_active_tasks_per_usage AS (
    SELECT 
        job_id,
        job_run_id,
        usage_timestamp,
        SUM(overlap_seconds) AS total_overlap_seconds,
        COUNT(DISTINCT task_key) AS num_parallel_tasks
    FROM tasks_active_at_usage
    GROUP BY job_id, job_run_id, usage_timestamp
),
task_cost_allocation AS (
    SELECT 
        ta.job_id,
        ta.job_run_id,
        ta.usage_timestamp,
        ta.task_key,
        ta.task_start,
        ta.task_end,
        ta.task_duration_seconds,
        ta.overlap_seconds,
        tat.total_overlap_seconds,
        tat.num_parallel_tasks,
        ta.dbu_quantity,
        ta.cost_usd,
        CASE 
            WHEN tat.total_overlap_seconds > 0 
            THEN (ta.cost_usd * ta.overlap_seconds) / tat.total_overlap_seconds
            ELSE 0
        END AS allocated_cost_usd,
        CASE 
            WHEN tat.total_overlap_seconds > 0 
            THEN (ta.dbu_quantity * ta.overlap_seconds) / tat.total_overlap_seconds
            ELSE 0
        END AS allocated_dbus
    FROM tasks_active_at_usage ta
    INNER JOIN total_active_tasks_per_usage tat
        ON ta.job_id = tat.job_id
        AND ta.job_run_id = tat.job_run_id
        AND ta.usage_timestamp = tat.usage_timestamp
),
task_usage_summary AS (
    SELECT 
        job_id,
        job_run_id,
        task_key,
        MIN(task_start) AS task_start,
        MAX(task_end) AS task_end,
        SUM(task_duration_seconds) AS total_task_duration_seconds,
        SUM(allocated_dbus) AS total_dbus_allocated,
        SUM(allocated_cost_usd) AS total_cost_allocated,
        COUNT(DISTINCT usage_timestamp) AS usage_records_count,
        AVG(num_parallel_tasks) AS avg_parallel_tasks
    FROM task_cost_allocation
    GROUP BY job_id, job_run_id, task_key
),
-- Calculate total job cost for percentage calculation
job_total_costs AS (
    SELECT 
        job_id,
        job_run_id,
        SUM(total_cost_allocated) AS job_total_cost
    FROM task_usage_summary
    GROUP BY job_id, job_run_id
),
job_names AS (
    SELECT DISTINCT
        workspace_id,
        job_id,
        name AS job_name
    FROM system.lakeflow.jobs
    WHERE delete_time IS NULL
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY workspace_id, job_id 
        ORDER BY change_time DESC
    ) = 1
),
task_details AS (
    SELECT DISTINCT
        workspace_id,
        job_id,
        task_key,
        depends_on_keys
    FROM system.lakeflow.job_tasks
    WHERE delete_time IS NULL
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY workspace_id, job_id, task_key 
        ORDER BY change_time DESC
    ) = 1
)
SELECT 
    jr.workspace_id,
    jr.job_id,
    COALESCE(jn.job_name, jr.job_id) AS job_name,
    tus.job_run_id,
    tus.task_key,
    td.depends_on_keys,
    tus.task_start,
    tus.task_end,
    tus.total_task_duration_seconds / 60.0 AS task_duration_minutes,
    tus.total_dbus_allocated,
    tus.total_cost_allocated,
    jtc.job_total_cost,
    -- Percentage of job cost
    CASE 
        WHEN jtc.job_total_cost > 0 
        THEN (tus.total_cost_allocated / jtc.job_total_cost) * 100.0 
        ELSE 0 
    END AS pct_of_job_cost,
    tus.usage_records_count,
    tus.avg_parallel_tasks,
    CASE 
        WHEN tus.total_task_duration_seconds > 0 
        THEN (tus.total_cost_allocated * 3600.0) / tus.total_task_duration_seconds 
        ELSE 0 
    END AS cost_per_hour_usd,
    CASE 
        WHEN tus.total_dbus_allocated > 0 
        THEN tus.total_cost_allocated / tus.total_dbus_allocated 
        ELSE 0 
    END AS cost_per_dbu_usd
FROM task_usage_summary tus
INNER JOIN job_runs jr
    ON tus.job_id = jr.job_id
    AND tus.job_run_id = jr.job_run_id
INNER JOIN job_total_costs jtc
    ON tus.job_id = jtc.job_id
    AND tus.job_run_id = jtc.job_run_id
LEFT JOIN job_names jn
    ON jr.workspace_id = jn.workspace_id
    AND jr.job_id = jn.job_id
LEFT JOIN task_details td
    ON jr.workspace_id = td.workspace_id
    AND jr.job_id = td.job_id
    AND tus.task_key = td.task_key
ORDER BY jr.job_id, tus.job_run_id, tus.total_cost_allocated DESC;


-- ============================================================================
-- Query 3: Parallel Task Analysis - Identify Parallel Execution Patterns
-- ============================================================================
-- Analyzes which tasks run in parallel and their cost implications
-- ============================================================================

WITH job_task_runs AS (
    SELECT 
        tr.workspace_id,
        tr.job_id,
        tr.job_run_id,
        tr.task_key,
        tr.period_start_time AS task_start,
        tr.period_end_time AS task_end,
        tr.result_state,
        tr.compute_ids[0] AS cluster_id,
        TIMESTAMPDIFF(SECOND, tr.period_start_time, tr.period_end_time) AS duration_seconds
    FROM system.lakeflow.job_task_run_timeline tr
    WHERE tr.period_start_time >= '${start_date}'
        AND tr.period_start_time < '${end_date}'
        AND size(tr.compute_ids) > 0
        AND tr.result_state IS NOT NULL
),
-- Find time periods with overlapping tasks
task_overlaps AS (
    SELECT 
        jtr1.job_id,
        jtr1.job_run_id,
        jtr1.task_key AS task_key_1,
        jtr2.task_key AS task_key_2,
        GREATEST(jtr1.task_start, jtr2.task_start) AS overlap_start,
        LEAST(jtr1.task_end, jtr2.task_end) AS overlap_end,
        GREATEST(0, 
            UNIX_TIMESTAMP(LEAST(jtr1.task_end, jtr2.task_end)) -
            UNIX_TIMESTAMP(GREATEST(jtr1.task_start, jtr2.task_start))
        ) AS overlap_seconds
    FROM job_task_runs jtr1
    INNER JOIN job_task_runs jtr2
        ON jtr1.job_id = jtr2.job_id
        AND jtr1.job_run_id = jtr2.job_run_id
        AND jtr1.task_key < jtr2.task_key  -- Avoid duplicates (A-B same as B-A)
        -- Tasks overlap if: task1_start < task2_end AND task1_end > task2_start
        AND jtr1.task_start < jtr2.task_end
        AND jtr1.task_end > jtr2.task_start
),
-- Aggregate parallel execution statistics
parallel_execution_stats AS (
    SELECT 
        job_id,
        job_run_id,
        COUNT(DISTINCT task_key_1) + COUNT(DISTINCT task_key_2) AS total_parallel_tasks,
        COUNT(*) AS parallel_pairs,
        SUM(overlap_seconds) AS total_overlap_seconds,
        MAX(overlap_seconds) AS max_overlap_seconds,
        AVG(overlap_seconds) AS avg_overlap_seconds
    FROM task_overlaps
    GROUP BY job_id, job_run_id
),
job_runs AS (
    SELECT 
        jr.workspace_id,
        jr.job_id,
        jr.job_run_id,
        jr.period_start_time AS job_start,
        jr.period_end_time AS job_end,
        TIMESTAMPDIFF(SECOND, jr.period_start_time, jr.period_end_time) AS job_duration_seconds
    FROM system.lakeflow.job_run_timeline jr
    WHERE jr.period_start_time >= '${start_date}'
        AND jr.period_start_time < '${end_date}'
        AND size(jr.compute_ids) > 0
),
task_counts AS (
    SELECT 
        job_id,
        job_run_id,
        COUNT(DISTINCT task_key) AS total_tasks,
        SUM(duration_seconds) AS sum_task_durations
    FROM job_task_runs
    GROUP BY job_id, job_run_id
),
job_names AS (
    SELECT DISTINCT
        workspace_id,
        job_id,
        name AS job_name
    FROM system.lakeflow.jobs
    WHERE delete_time IS NULL
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY workspace_id, job_id 
        ORDER BY change_time DESC
    ) = 1
)
SELECT 
    jr.workspace_id,
    jr.job_id,
    COALESCE(jn.job_name, jr.job_id) AS job_name,
    jr.job_run_id,
    jr.job_start,
    jr.job_end,
    jr.job_duration_seconds / 60.0 AS job_duration_minutes,
    tc.total_tasks,
    tc.sum_task_durations / 60.0 AS sum_task_durations_minutes,
    -- Parallel execution metrics
    COALESCE(pes.total_parallel_tasks, 0) AS tasks_with_parallelism,
    COALESCE(pes.parallel_pairs, 0) AS parallel_task_pairs,
    COALESCE(pes.total_overlap_seconds, 0) / 60.0 AS total_overlap_minutes,
    COALESCE(pes.max_overlap_seconds, 0) / 60.0 AS max_overlap_minutes,
    -- Parallelism ratio
    CASE 
        WHEN jr.job_duration_seconds > 0 
        THEN (tc.sum_task_durations / jr.job_duration_seconds) 
        ELSE 0 
    END AS parallelism_ratio,
    -- Interpretation
    CASE 
        WHEN (tc.sum_task_durations / NULLIF(jr.job_duration_seconds, 0)) > 1.5 
        THEN 'HIGH_PARALLELISM'  -- Sum of durations much greater than job duration
        WHEN (tc.sum_task_durations / NULLIF(jr.job_duration_seconds, 0)) > 1.1 
        THEN 'MODERATE_PARALLELISM'
        ELSE 'LOW_PARALLELISM'  -- Mostly sequential
    END AS parallelism_level
FROM job_runs jr
LEFT JOIN task_counts tc
    ON jr.job_id = tc.job_id
    AND jr.job_run_id = tc.job_run_id
LEFT JOIN parallel_execution_stats pes
    ON jr.job_id = pes.job_id
    AND jr.job_run_id = pes.job_run_id
LEFT JOIN job_names jn
    ON jr.workspace_id = jn.workspace_id
    AND jr.job_id = jn.job_id
ORDER BY parallelism_ratio DESC, jr.job_start DESC;


-- ============================================================================
-- Query 4: Time Period Breakdown - Cost Allocation by Time Periods
-- ============================================================================
-- Shows how costs are allocated across different time periods within a job run
-- Useful for understanding cost distribution over time
-- ============================================================================

WITH job_task_runs AS (
    SELECT 
        tr.workspace_id,
        tr.job_id,
        tr.job_run_id,
        tr.task_key,
        tr.period_start_time AS task_start,
        tr.period_end_time AS task_end,
        tr.result_state,
        tr.compute_ids[0] AS cluster_id
    FROM system.lakeflow.job_task_run_timeline tr
    WHERE tr.period_start_time >= '${start_date}'
        AND tr.period_start_time < '${end_date}'
        AND size(tr.compute_ids) > 0
        AND tr.result_state IS NOT NULL
),
job_runs AS (
    SELECT 
        jr.job_id,
        jr.job_run_id,
        jr.period_start_time AS job_start,
        jr.period_end_time AS job_end,
        jr.compute_ids[0] AS cluster_id
    FROM system.lakeflow.job_run_timeline jr
    WHERE jr.period_start_time >= '${start_date}'
        AND jr.period_start_time < '${end_date}'
        AND size(jr.compute_ids) > 0
),
usage_records AS (
    SELECT 
        u.metadata.usage_metadata.job_id AS job_id,
        u.metadata.usage_metadata.job_run_id AS job_run_id,
        u.metadata.usage_metadata.cluster_id AS cluster_id,
        u.usage_quantity AS dbu_quantity,
        u.usage_date,
        CAST(u.usage_date AS TIMESTAMP) AS usage_timestamp,
        u.sku_name
    FROM system.billing.usage u
    WHERE u.usage_date >= '${start_date}'
        AND u.usage_date < '${end_date}'
        AND u.metadata.usage_metadata.job_id IS NOT NULL
),
pricing_data AS (
    SELECT 
        sku_name,
        price_start_time,
        price_end_time,
        pricing.default AS list_price_per_dbu
    FROM system.billing.list_prices
    WHERE pricing.default IS NOT NULL
),
usage_with_pricing AS (
    SELECT 
        u.job_id,
        u.job_run_id,
        u.cluster_id,
        u.dbu_quantity,
        u.usage_timestamp,
        u.sku_name,
        COALESCE(p.list_price_per_dbu, 0) AS price_per_dbu,
        u.dbu_quantity * COALESCE(p.list_price_per_dbu, 0) AS cost_usd
    FROM usage_records u
    LEFT JOIN pricing_data p
        ON u.sku_name = p.sku_name
        AND p.price_start_time <= u.usage_date
        AND (p.price_end_time IS NULL OR u.usage_date < p.price_end_time)
),
-- Generate distinct time periods from task boundaries
time_periods AS (
    SELECT DISTINCT
        jr.job_id,
        jr.job_run_id,
        time_point AS period_start,
        LEAD(time_point) OVER(
            PARTITION BY jr.job_id, jr.job_run_id 
            ORDER BY time_point
        ) AS period_end
    FROM job_runs jr
    CROSS JOIN (
        SELECT task_start AS time_point FROM job_task_runs
        UNION
        SELECT task_end AS time_point FROM job_task_runs
    ) time_points
    WHERE time_point >= jr.job_start
        AND time_point <= jr.job_end
),
-- Find which tasks are active in each time period
tasks_in_period AS (
    SELECT 
        tp.job_id,
        tp.job_run_id,
        tp.period_start,
        tp.period_end,
        jtr.task_key,
        TIMESTAMPDIFF(SECOND, tp.period_start, tp.period_end) AS period_duration_seconds
    FROM time_periods tp
    INNER JOIN job_task_runs jtr
        ON tp.job_id = jtr.job_id
        AND tp.job_run_id = jtr.job_run_id
        -- Task overlaps with period
        AND jtr.task_start < tp.period_end
        AND jtr.task_end > tp.period_start
    WHERE tp.period_end IS NOT NULL
),
-- Allocate usage to time periods
period_usage_allocation AS (
    SELECT 
        tip.job_id,
        tip.job_run_id,
        tip.period_start,
        tip.period_end,
        tip.period_duration_seconds,
        COUNT(DISTINCT tip.task_key) AS tasks_active_in_period,
        COLLECT_SET(tip.task_key) AS active_tasks,
        SUM(up.cost_usd) AS period_cost,
        SUM(up.dbu_quantity) AS period_dbus
    FROM tasks_in_period tip
    LEFT JOIN usage_with_pricing up
        ON tip.job_id = up.job_id
        AND tip.job_run_id = up.job_run_id
        AND up.usage_timestamp >= tip.period_start
        AND up.usage_timestamp < tip.period_end
    GROUP BY 
        tip.job_id,
        tip.job_run_id,
        tip.period_start,
        tip.period_end,
        tip.period_duration_seconds
)
SELECT 
    pua.job_id,
    pua.job_run_id,
    pua.period_start,
    pua.period_end,
    pua.period_duration_seconds / 60.0 AS period_duration_minutes,
    pua.tasks_active_in_period,
    pua.active_tasks,
    COALESCE(pua.period_cost, 0) AS period_cost_usd,
    COALESCE(pua.period_dbus, 0) AS period_dbus,
    -- Cost per minute in this period
    CASE 
        WHEN pua.period_duration_seconds > 0 
        THEN (COALESCE(pua.period_cost, 0) * 60.0) / pua.period_duration_seconds 
        ELSE 0 
    END AS cost_per_minute_usd
FROM period_usage_allocation pua
ORDER BY pua.job_id, pua.job_run_id, pua.period_start;


-- ============================================================================
-- Usage Notes
-- ============================================================================
--
-- Key Differences from Simple Duration-Based Allocation:
-- 1. Handles parallel tasks correctly by allocating based on time overlap
-- 2. Accounts for concurrent execution periods
-- 3. More accurate cost distribution when tasks run simultaneously
--
-- When to Use:
-- - Jobs with parallel task execution
-- - Need accurate task-level cost breakdown
-- - Analyzing cost distribution over time
--
-- Performance Considerations:
-- - More complex than simple allocation (more joins and calculations)
-- - May be slower for large datasets
-- - Consider filtering by specific job_id or job_run_id for testing
--
-- Limitations:
-- - Assumes usage records can be matched to task execution periods
-- - If usage_date is daily (not hourly), may need adjustment
-- - Requires accurate task start/end times from job_task_run_timeline
-- ============================================================================

