
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
-- Group usage records by job/run/date/product/sku to handle multiple records per day
usage_grouped AS (
    SELECT 
        u.job_id,
        u.job_run_id,
        u.cluster_id,
        u.product,
        u.usage_date,
        u.usage_timestamp,
        u.sku_name,
        SUM(u.dbu_quantity) AS dbu_quantity,
        -- Get price for the usage date
        MAX(COALESCE(p.list_price_per_dbu, 0)) AS price_per_dbu
    FROM usage_records u
    LEFT JOIN pricing_data p
        ON u.sku_name = p.sku_name
        AND p.price_start_time <= u.usage_date
        AND (p.price_end_time IS NULL OR u.usage_date < p.price_end_time)
    GROUP BY 
        u.job_id,
        u.job_run_id,
        u.cluster_id,
        u.product,
        u.usage_date,
        u.usage_timestamp,
        u.sku_name
),
usage_with_pricing AS (
    SELECT 
        ug.job_id,
        ug.job_run_id,
        ug.cluster_id,
        ug.product,
        ug.dbu_quantity,
        ug.usage_date,
        ug.usage_timestamp,
        ug.sku_name,
        ug.price_per_dbu,
        ug.dbu_quantity * ug.price_per_dbu AS cost_usd
    FROM usage_grouped ug
),
-- Calculate total job costs for validation
job_total_costs AS (
    SELECT 
        job_id,
        job_run_id,
        SUM(cost_usd) AS total_job_cost_usd,
        SUM(dbu_quantity) AS total_job_dbus
    FROM usage_with_pricing
    GROUP BY job_id, job_run_id
),
-- Step 1: Find which tasks were running at each usage timestamp
-- This handles parallel tasks by identifying all active tasks at each point in time
-- Note: Using job_id/job_run_id matching first, then cluster_id as fallback
tasks_active_at_usage AS (
    SELECT 
        up.job_id,
        up.job_run_id,
        up.usage_timestamp,
        up.dbu_quantity,
        up.cost_usd,
        up.cluster_id AS usage_cluster_id,
        jtr.task_key,
        jtr.task_start,
        jtr.task_end,
        jtr.cluster_id AS task_cluster_id,
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
        -- Match by cluster_id if available, otherwise match by job/run only
        AND (
            up.cluster_id = jtr.cluster_id 
            OR up.cluster_id IS NULL 
            OR jtr.cluster_id IS NULL
        )
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
-- Step 4b: Calculate sum of task costs per job run for validation
task_cost_totals AS (
    SELECT 
        job_id,
        job_run_id,
        SUM(total_cost_allocated) AS sum_task_costs_usd,
        SUM(total_dbus_allocated) AS sum_task_dbus
    FROM task_usage_summary
    GROUP BY job_id, job_run_id
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
    -- Validation: Job totals vs task sums
    COALESCE(jtc.total_job_cost_usd, 0) AS total_job_cost_usd,
    COALESCE(tct.sum_task_costs_usd, 0) AS sum_task_costs_usd,
    COALESCE(jtc.total_job_dbus, 0) AS total_job_dbus,
    COALESCE(tct.sum_task_dbus, 0) AS sum_task_dbus,
    -- Cost difference (should be close to 0)
    COALESCE(jtc.total_job_cost_usd, 0) - COALESCE(tct.sum_task_costs_usd, 0) AS cost_difference_usd,
    -- Percentage of job cost
    CASE 
        WHEN COALESCE(jtc.total_job_cost_usd, 0) > 0 
        THEN (tus.total_cost_allocated / jtc.total_job_cost_usd) * 100.0 
        ELSE 0 
    END AS pct_of_job_cost,
    -- Allocation completeness (should be close to 100%)
    CASE 
        WHEN COALESCE(jtc.total_job_cost_usd, 0) > 0 
        THEN (COALESCE(tct.sum_task_costs_usd, 0) / jtc.total_job_cost_usd) * 100.0 
        ELSE 0 
    END AS allocation_completeness_pct
FROM task_usage_summary tus
INNER JOIN job_runs jr
    ON tus.job_id = jr.job_id
    AND tus.job_run_id = jr.job_run_id
LEFT JOIN job_total_costs jtc
    ON tus.job_id = jtc.job_id
    AND tus.job_run_id = jtc.job_run_id
LEFT JOIN task_cost_totals tct
    ON tus.job_id = tct.job_id
    AND tus.job_run_id = tct.job_run_id
LEFT JOIN job_names jn
    ON jr.workspace_id = jn.workspace_id
    AND jr.job_id = jn.job_id
LEFT JOIN task_details td
    ON jr.workspace_id = td.workspace_id
    AND jr.job_id = td.job_id
    AND tus.task_key = td.task_key
ORDER BY jr.job_id, tus.job_run_id, tus.task_key;
