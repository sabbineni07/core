-- Time-Based Task Cost Allocation (Handles Parallel Tasks)
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
-- Step 1: Find which tasks were running at each usage timestamp
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
        -- Calculate overlap duration in seconds
        GREATEST(0, 
            LEAST(UNIX_TIMESTAMP(jtr.task_end), UNIX_TIMESTAMP(up.usage_timestamp + INTERVAL 1 DAY)) -
            GREATEST(UNIX_TIMESTAMP(jtr.task_start), UNIX_TIMESTAMP(up.usage_timestamp))
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
-- Step 2: Calculate total active tasks per usage timestamp
total_active_tasks_per_usage AS (
    SELECT 
        job_id,
        job_run_id,
        usage_timestamp,
        SUM(overlap_seconds) AS total_overlap_seconds
    FROM tasks_active_at_usage
    GROUP BY job_id, job_run_id, usage_timestamp
),
-- Step 3: Allocate cost proportionally based on overlap
task_cost_allocation AS (
    SELECT 
        ta.job_id,
        ta.job_run_id,
        ta.task_key,
        ta.usage_timestamp,
        ta.dbu_quantity,
        ta.cost_usd,
        ta.overlap_seconds,
        tat.total_overlap_seconds,
        -- Allocate proportionally: (task_overlap / total_overlap) * cost
        CASE 
            WHEN tat.total_overlap_seconds > 0 
            THEN (ta.cost_usd * ta.overlap_seconds) / tat.total_overlap_seconds
            ELSE 0
        END AS allocated_cost,
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
-- Step 4: Aggregate by task
task_usage_summary AS (
    SELECT 
        job_id,
        job_run_id,
        task_key,
        SUM(allocated_dbus) AS total_dbus_allocated,
        SUM(allocated_cost) AS total_cost_allocated
    FROM task_cost_allocation
    GROUP BY job_id, job_run_id, task_key
),
-- Step 5: Join back with task details for final output
final_task_costs AS (
    SELECT 
        jtr.workspace_id,
        jtr.job_id,
        jtr.job_run_id,
        jtr.task_key,
        jtr.task_start,
        jtr.task_end,
        jtr.result_state,
        jtr.duration_seconds,
        COALESCE(tus.total_dbus_allocated, 0) AS allocated_dbus,
        COALESCE(tus.total_cost_allocated, 0) AS allocated_cost_usd
    FROM job_task_runs jtr
    LEFT JOIN task_usage_summary tus
        ON jtr.job_id = tus.job_id
        AND jtr.job_run_id = tus.job_run_id
        AND jtr.task_key = tus.task_key
)
SELECT * FROM final_task_costs;
