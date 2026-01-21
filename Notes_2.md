-- ============================================================================
-- Databricks Job Cost and Usage (DBU) Metrics
-- ============================================================================
-- This file contains SQL queries for analyzing job costs and DBU consumption
-- using Databricks system tables.
--
-- System Tables Used:
-- - system.lakeflow.jobs: Job definitions and metadata
-- - system.lakeflow.job_tasks: Job task definitions
-- - system.lakeflow.job_run_timeline: Job run execution data
-- - system.lakeflow.job_task_run_timeline: Task run execution data
-- - system.billing.usage: Billable usage records (DBU consumption)
-- - system.billing.list_prices: Pricing information for SKUs
--
-- References:
-- - Jobs tables: https://docs.databricks.com/aws/en/admin/system-tables/jobs
-- - Billing tables: https://docs.databricks.com/aws/en/admin/system-tables/billing
-- - Pricing tables: https://docs.databricks.com/aws/en/admin/system-tables/pricing
-- ============================================================================

-- ============================================================================
-- Query 1: Job-Level Cost and Usage Summary
-- ============================================================================
-- Aggregates DBU consumption and costs by job across a date range
-- Parameters:
--   ${start_date} - Start date (e.g., '2024-01-01')
--   ${end_date} - End date (e.g., '2024-01-31')
--   ${workspace_ids} - Optional: comma-separated workspace IDs
--   ${job_ids} - Optional: comma-separated job IDs
-- ============================================================================

WITH job_runs AS (
    SELECT 
        workspace_id,
        job_id,
        job_run_id,
        period_start_time,
        period_end_time,
        result_state,
        compute_ids[0] AS cluster_id,
        TIMESTAMPDIFF(SECOND, period_start_time, period_end_time) AS duration_seconds
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= '${start_date}'
        AND period_start_time < '${end_date}'
        AND size(compute_ids) > 0
),
usage_records AS (
    SELECT 
        u.metadata.usage_metadata.job_id AS job_id,
        u.metadata.usage_metadata.job_run_id AS job_run_id,
        u.metadata.usage_metadata.cluster_id AS cluster_id,
        u.billing_origin_product AS product,
        u.usage_unit,
        u.usage_quantity AS dbu_quantity,
        u.usage_date,
        u.sku_name,
        u.custom_tags
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
        u.sku_name,
        COALESCE(p.list_price_per_dbu, 0) AS price_per_dbu,
        u.dbu_quantity * COALESCE(p.list_price_per_dbu, 0) AS cost_usd
    FROM usage_records u
    LEFT JOIN pricing_data p
        ON u.sku_name = p.sku_name
        AND p.price_start_time <= u.usage_date
        AND (p.price_end_time IS NULL OR u.usage_date < p.price_end_time)
),
job_usage_summary AS (
    SELECT 
        jr.workspace_id,
        jr.job_id,
        jr.job_run_id,
        jr.period_start_time,
        jr.period_end_time,
        jr.result_state,
        jr.duration_seconds,
        COUNT(DISTINCT up.job_run_id) AS usage_records_count,
        SUM(up.dbu_quantity) AS total_dbus,
        SUM(up.cost_usd) AS total_cost_usd,
        COUNT(DISTINCT up.product) AS product_count,
        COLLECT_SET(up.product) AS products_used
    FROM job_runs jr
    LEFT JOIN usage_with_pricing up
        ON jr.job_id = up.job_id
        AND jr.job_run_id = up.job_run_id
    GROUP BY 
        jr.workspace_id,
        jr.job_id,
        jr.job_run_id,
        jr.period_start_time,
        jr.period_end_time,
        jr.result_state,
        jr.duration_seconds
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
    jus.workspace_id,
    jus.job_id,
    COALESCE(jn.job_name, jus.job_id) AS job_name,
    COUNT(DISTINCT jus.job_run_id) AS total_runs,
    COUNT(DISTINCT CASE WHEN jus.result_state = 'SUCCEEDED' THEN jus.job_run_id END) AS succeeded_runs,
    COUNT(DISTINCT CASE WHEN jus.result_state = 'FAILED' THEN jus.job_run_id END) AS failed_runs,
    SUM(jus.total_dbus) AS total_dbus_consumed,
    AVG(jus.total_dbus) AS avg_dbus_per_run,
    PERCENTILE_APPROX(jus.total_dbus, 0.95) AS p95_dbus_per_run,
    PERCENTILE_APPROX(jus.total_dbus, 0.99) AS p99_dbus_per_run,
    MAX(jus.total_dbus) AS max_dbus_per_run,
    SUM(jus.total_cost_usd) AS total_cost_usd,
    AVG(jus.total_cost_usd) AS avg_cost_per_run_usd,
    SUM(jus.duration_seconds) / 3600.0 AS total_runtime_hours,
    AVG(jus.duration_seconds) / 60.0 AS avg_duration_minutes,
    -- Cost efficiency metrics
    CASE 
        WHEN SUM(jus.total_dbus) > 0 
        THEN SUM(jus.total_cost_usd) / SUM(jus.total_dbus) 
        ELSE 0 
    END AS avg_cost_per_dbu,
    -- DBU efficiency (DBUs per hour)
    CASE 
        WHEN SUM(jus.duration_seconds) > 0 
        THEN (SUM(jus.total_dbus) * 3600.0) / SUM(jus.duration_seconds) 
        ELSE 0 
    END AS dbus_per_hour,
    COLLECT_SET(jus.products_used[0]) AS products_used
FROM job_usage_summary jus
LEFT JOIN job_names jn
    ON jus.workspace_id = jn.workspace_id
    AND jus.job_id = jn.job_id
GROUP BY 
    jus.workspace_id,
    jus.job_id,
    jn.job_name
ORDER BY total_cost_usd DESC;


-- ============================================================================
-- Query 2: Job Run-Level Cost and Usage Details
-- ============================================================================
-- Detailed cost and usage metrics for each job run
-- Useful for analyzing individual job execution costs
-- ============================================================================

WITH job_runs AS (
    SELECT 
        workspace_id,
        job_id,
        job_run_id,
        period_start_time,
        period_end_time,
        result_state,
        compute_ids[0] AS cluster_id,
        TIMESTAMPDIFF(SECOND, period_start_time, period_end_time) AS duration_seconds
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= '${start_date}'
        AND period_start_time < '${end_date}'
        AND size(compute_ids) > 0
),
usage_records AS (
    SELECT 
        u.metadata.usage_metadata.job_id AS job_id,
        u.metadata.usage_metadata.job_run_id AS job_run_id,
        u.metadata.usage_metadata.cluster_id AS cluster_id,
        u.billing_origin_product AS product,
        u.usage_unit,
        u.usage_quantity AS dbu_quantity,
        u.usage_date,
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
        u.sku_name,
        COALESCE(p.list_price_per_dbu, 0) AS price_per_dbu,
        u.dbu_quantity * COALESCE(p.list_price_per_dbu, 0) AS cost_usd
    FROM usage_records u
    LEFT JOIN pricing_data p
        ON u.sku_name = p.sku_name
        AND p.price_start_time <= u.usage_date
        AND (p.price_end_time IS NULL OR u.usage_date < p.price_end_time)
),
job_run_usage AS (
    SELECT 
        jr.workspace_id,
        jr.job_id,
        jr.job_run_id,
        jr.period_start_time,
        jr.period_end_time,
        jr.result_state,
        jr.cluster_id,
        jr.duration_seconds,
        SUM(up.dbu_quantity) AS total_dbus,
        SUM(up.cost_usd) AS total_cost_usd,
        COUNT(DISTINCT up.product) AS product_count,
        COLLECT_SET(up.product) AS products_used,
        COLLECT_SET(up.sku_name) AS skus_used
    FROM job_runs jr
    LEFT JOIN usage_with_pricing up
        ON jr.job_id = up.job_id
        AND jr.job_run_id = up.job_run_id
    GROUP BY 
        jr.workspace_id,
        jr.job_id,
        jr.job_run_id,
        jr.period_start_time,
        jr.period_end_time,
        jr.result_state,
        jr.cluster_id,
        jr.duration_seconds
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
    jru.workspace_id,
    jru.job_id,
    COALESCE(jn.job_name, jru.job_id) AS job_name,
    jru.job_run_id,
    jru.period_start_time AS run_start_time,
    jru.period_end_time AS run_end_time,
    jru.result_state,
    jru.cluster_id,
    jru.duration_seconds,
    jru.duration_seconds / 60.0 AS duration_minutes,
    jru.duration_seconds / 3600.0 AS duration_hours,
    COALESCE(jru.total_dbus, 0) AS total_dbus,
    COALESCE(jru.total_cost_usd, 0) AS total_cost_usd,
    -- Cost per hour
    CASE 
        WHEN jru.duration_seconds > 0 
        THEN (COALESCE(jru.total_cost_usd, 0) * 3600.0) / jru.duration_seconds 
        ELSE 0 
    END AS cost_per_hour_usd,
    -- DBUs per hour
    CASE 
        WHEN jru.duration_seconds > 0 
        THEN (COALESCE(jru.total_dbus, 0) * 3600.0) / jru.duration_seconds 
        ELSE 0 
    END AS dbus_per_hour,
    -- Cost per DBU
    CASE 
        WHEN COALESCE(jru.total_dbus, 0) > 0 
        THEN COALESCE(jru.total_cost_usd, 0) / COALESCE(jru.total_dbus, 0) 
        ELSE 0 
    END AS cost_per_dbu_usd,
    jru.products_used,
    jru.skus_used
FROM job_run_usage jru
LEFT JOIN job_names jn
    ON jru.workspace_id = jn.workspace_id
    AND jru.job_id = jn.job_id
ORDER BY jru.period_start_time DESC, jru.total_cost_usd DESC;


-- ============================================================================
-- Query 3: Task-Level Cost and Usage Analysis
-- ============================================================================
-- Breaks down costs by task within jobs
-- Helps identify expensive tasks within a job
-- ============================================================================

WITH job_task_runs AS (
    SELECT 
        tr.workspace_id,
        tr.job_id,
        tr.job_run_id,
        tr.task_key,
        tr.period_start_time,
        tr.period_end_time,
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
        u.sku_name,
        COALESCE(p.list_price_per_dbu, 0) AS price_per_dbu,
        u.dbu_quantity * COALESCE(p.list_price_per_dbu, 0) AS cost_usd
    FROM usage_records u
    LEFT JOIN pricing_data p
        ON u.sku_name = p.sku_name
        AND p.price_start_time <= u.usage_date
        AND (p.price_end_time IS NULL OR u.usage_date < p.price_end_time)
),
job_runs AS (
    SELECT DISTINCT
        workspace_id,
        job_id,
        job_run_id,
        compute_ids[0] AS cluster_id
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= '${start_date}'
        AND period_start_time < '${end_date}'
        AND size(compute_ids) > 0
),
task_usage_allocation AS (
    -- Allocate usage to tasks based on cluster_id and time overlap
    SELECT 
        jtr.workspace_id,
        jtr.job_id,
        jtr.job_run_id,
        jtr.task_key,
        jtr.period_start_time,
        jtr.period_end_time,
        jtr.result_state,
        jtr.duration_seconds,
        -- Allocate usage proportionally based on task duration vs total job duration
        SUM(up.dbu_quantity * 
            (jtr.duration_seconds / NULLIF(
                (SELECT SUM(duration_seconds) 
                 FROM job_task_runs jtr2 
                 WHERE jtr2.job_run_id = jtr.job_run_id), 0)
            )) AS allocated_dbus,
        SUM(up.cost_usd * 
            (jtr.duration_seconds / NULLIF(
                (SELECT SUM(duration_seconds) 
                 FROM job_task_runs jtr2 
                 WHERE jtr2.job_run_id = jtr.job_run_id), 0)
            )) AS allocated_cost_usd
    FROM job_task_runs jtr
    INNER JOIN job_runs jr
        ON jtr.job_id = jr.job_id
        AND jtr.job_run_id = jr.job_run_id
        AND jtr.cluster_id = jr.cluster_id
    LEFT JOIN usage_with_pricing up
        ON jr.job_id = up.job_id
        AND jr.job_run_id = up.job_run_id
        AND jr.cluster_id = up.cluster_id
        AND up.usage_date >= DATE(jtr.period_start_time)
        AND up.usage_date <= DATE(jtr.period_end_time)
    GROUP BY 
        jtr.workspace_id,
        jtr.job_id,
        jtr.job_run_id,
        jtr.task_key,
        jtr.period_start_time,
        jtr.period_end_time,
        jtr.result_state,
        jtr.duration_seconds
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
    tua.workspace_id,
    tua.job_id,
    COALESCE(jn.job_name, tua.job_id) AS job_name,
    tua.task_key,
    COUNT(DISTINCT tua.job_run_id) AS total_runs,
    COUNT(DISTINCT CASE WHEN tua.result_state = 'SUCCEEDED' THEN tua.job_run_id END) AS succeeded_runs,
    SUM(tua.allocated_dbus) AS total_dbus_allocated,
    AVG(tua.allocated_dbus) AS avg_dbus_per_run,
    PERCENTILE_APPROX(tua.allocated_dbus, 0.95) AS p95_dbus_per_run,
    MAX(tua.allocated_dbus) AS max_dbus_per_run,
    SUM(tua.allocated_cost_usd) AS total_cost_usd,
    AVG(tua.allocated_cost_usd) AS avg_cost_per_run_usd,
    SUM(tua.duration_seconds) / 3600.0 AS total_runtime_hours,
    AVG(tua.duration_seconds) / 60.0 AS avg_duration_minutes,
    -- Cost efficiency
    CASE 
        WHEN SUM(tua.allocated_dbus) > 0 
        THEN SUM(tua.allocated_cost_usd) / SUM(tua.allocated_dbus) 
        ELSE 0 
    END AS avg_cost_per_dbu,
    -- Percentage of job cost
    CASE 
        WHEN (SELECT SUM(allocated_cost_usd) 
              FROM task_usage_allocation tua2 
              WHERE tua2.job_id = tua.job_id) > 0
        THEN (SUM(tua.allocated_cost_usd) / 
              (SELECT SUM(allocated_cost_usd) 
               FROM task_usage_allocation tua2 
               WHERE tua2.job_id = tua.job_id)) * 100.0
        ELSE 0
    END AS pct_of_job_cost
FROM task_usage_allocation tua
LEFT JOIN job_names jn
    ON tua.workspace_id = jn.workspace_id
    AND tua.job_id = jn.job_id
GROUP BY 
    tua.workspace_id,
    tua.job_id,
    jn.job_name,
    tua.task_key
ORDER BY total_cost_usd DESC;


-- ============================================================================
-- Query 4: Product-Level Cost Breakdown
-- ============================================================================
-- Breaks down costs by billing product (Jobs Compute, SQL Compute, etc.)
-- Helps understand which products are driving costs
-- ============================================================================

WITH usage_records AS (
    SELECT 
        u.metadata.usage_metadata.job_id AS job_id,
        u.metadata.usage_metadata.job_run_id AS job_run_id,
        u.billing_origin_product AS product,
        u.usage_unit,
        u.usage_quantity AS dbu_quantity,
        u.usage_date,
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
        u.product,
        u.dbu_quantity,
        u.usage_date,
        u.sku_name,
        COALESCE(p.list_price_per_dbu, 0) AS price_per_dbu,
        u.dbu_quantity * COALESCE(p.list_price_per_dbu, 0) AS cost_usd
    FROM usage_records u
    LEFT JOIN pricing_data p
        ON u.sku_name = p.sku_name
        AND p.price_start_time <= u.usage_date
        AND (p.price_end_time IS NULL OR u.usage_date < p.price_end_time)
),
job_runs AS (
    SELECT DISTINCT
        workspace_id,
        job_id,
        job_run_id
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= '${start_date}'
        AND period_start_time < '${end_date}'
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
    up.product,
    COUNT(DISTINCT up.job_id) AS jobs_using_product,
    COUNT(DISTINCT up.job_run_id) AS job_runs_using_product,
    SUM(up.dbu_quantity) AS total_dbus,
    AVG(up.dbu_quantity) AS avg_dbus_per_record,
    PERCENTILE_APPROX(up.dbu_quantity, 0.95) AS p95_dbus_per_record,
    SUM(up.cost_usd) AS total_cost_usd,
    AVG(up.cost_usd) AS avg_cost_per_record_usd,
    COUNT(DISTINCT up.sku_name) AS distinct_skus,
    COLLECT_SET(up.sku_name) AS skus_used,
    -- Percentage of total cost
    CASE 
        WHEN (SELECT SUM(cost_usd) FROM usage_with_pricing) > 0
        THEN (SUM(up.cost_usd) / 
              (SELECT SUM(cost_usd) FROM usage_with_pricing)) * 100.0
        ELSE 0
    END AS pct_of_total_cost,
    -- Average price per DBU for this product
    CASE 
        WHEN SUM(up.dbu_quantity) > 0 
        THEN SUM(up.cost_usd) / SUM(up.dbu_quantity) 
        ELSE 0 
    END AS avg_price_per_dbu
FROM usage_with_pricing up
GROUP BY up.product
ORDER BY total_cost_usd DESC;


-- ============================================================================
-- Query 5: Daily Cost Trend Analysis
-- ============================================================================
-- Shows daily cost trends to identify patterns and anomalies
-- ============================================================================

WITH usage_records AS (
    SELECT 
        u.metadata.usage_metadata.job_id AS job_id,
        u.metadata.usage_metadata.job_run_id AS job_run_id,
        u.billing_origin_product AS product,
        u.usage_quantity AS dbu_quantity,
        u.usage_date,
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
        u.product,
        u.dbu_quantity,
        u.usage_date,
        u.sku_name,
        COALESCE(p.list_price_per_dbu, 0) AS price_per_dbu,
        u.dbu_quantity * COALESCE(p.list_price_per_dbu, 0) AS cost_usd
    FROM usage_records u
    LEFT JOIN pricing_data p
        ON u.sku_name = p.sku_name
        AND p.price_start_time <= u.usage_date
        AND (p.price_end_time IS NULL OR u.usage_date < p.price_end_time)
),
job_runs AS (
    SELECT 
        workspace_id,
        job_id,
        job_run_id,
        DATE(period_start_time) AS run_date
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= '${start_date}'
        AND period_start_time < '${end_date}'
)
SELECT 
    up.usage_date,
    COUNT(DISTINCT up.job_id) AS jobs_active,
    COUNT(DISTINCT up.job_run_id) AS job_runs,
    SUM(up.dbu_quantity) AS total_dbus,
    SUM(up.cost_usd) AS total_cost_usd,
    AVG(up.cost_usd) AS avg_cost_per_record,
    -- Moving averages (7-day and 30-day)
    AVG(SUM(up.cost_usd)) OVER(
        ORDER BY up.usage_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_cost_7day,
    AVG(SUM(up.cost_usd)) OVER(
        ORDER BY up.usage_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS avg_cost_30day,
    -- Day-over-day change
    LAG(SUM(up.cost_usd)) OVER(ORDER BY up.usage_date) AS prev_day_cost,
    CASE 
        WHEN LAG(SUM(up.cost_usd)) OVER(ORDER BY up.usage_date) > 0
        THEN ((SUM(up.cost_usd) - LAG(SUM(up.cost_usd)) OVER(ORDER BY up.usage_date)) / 
              LAG(SUM(up.cost_usd)) OVER(ORDER BY up.usage_date)) * 100.0
        ELSE 0
    END AS cost_change_pct
FROM usage_with_pricing up
GROUP BY up.usage_date
ORDER BY up.usage_date DESC;


-- ============================================================================
-- Query 6: Cost Efficiency Analysis (Cost per DBU by Job)
-- ============================================================================
-- Identifies jobs with high cost per DBU (inefficient) vs low cost per DBU
-- ============================================================================

WITH job_runs AS (
    SELECT 
        workspace_id,
        job_id,
        job_run_id,
        period_start_time,
        period_end_time,
        result_state,
        compute_ids[0] AS cluster_id,
        TIMESTAMPDIFF(SECOND, period_start_time, period_end_time) AS duration_seconds
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= '${start_date}'
        AND period_start_time < '${end_date}'
        AND size(compute_ids) > 0
),
usage_records AS (
    SELECT 
        u.metadata.usage_metadata.job_id AS job_id,
        u.metadata.usage_metadata.job_run_id AS job_run_id,
        u.billing_origin_product AS product,
        u.usage_quantity AS dbu_quantity,
        u.usage_date,
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
        u.product,
        u.dbu_quantity,
        u.usage_date,
        u.sku_name,
        COALESCE(p.list_price_per_dbu, 0) AS price_per_dbu,
        u.dbu_quantity * COALESCE(p.list_price_per_dbu, 0) AS cost_usd
    FROM usage_records u
    LEFT JOIN pricing_data p
        ON u.sku_name = p.sku_name
        AND p.price_start_time <= u.usage_date
        AND (p.price_end_time IS NULL OR u.usage_date < p.price_end_time)
),
job_usage_summary AS (
    SELECT 
        jr.workspace_id,
        jr.job_id,
        jr.job_run_id,
        jr.duration_seconds,
        SUM(up.dbu_quantity) AS total_dbus,
        SUM(up.cost_usd) AS total_cost_usd
    FROM job_runs jr
    LEFT JOIN usage_with_pricing up
        ON jr.job_id = up.job_id
        AND jr.job_run_id = up.job_run_id
    GROUP BY 
        jr.workspace_id,
        jr.job_id,
        jr.job_run_id,
        jr.duration_seconds
),
job_aggregates AS (
    SELECT 
        workspace_id,
        job_id,
        COUNT(DISTINCT job_run_id) AS total_runs,
        SUM(total_dbus) AS total_dbus,
        SUM(total_cost_usd) AS total_cost_usd,
        SUM(duration_seconds) AS total_duration_seconds,
        AVG(total_dbus) AS avg_dbus_per_run,
        AVG(total_cost_usd) AS avg_cost_per_run
    FROM job_usage_summary
    GROUP BY workspace_id, job_id
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
overall_stats AS (
    SELECT 
        AVG(CASE WHEN total_dbus > 0 THEN total_cost_usd / total_dbus ELSE NULL END) AS overall_avg_cost_per_dbu,
        PERCENTILE_APPROX(CASE WHEN total_dbus > 0 THEN total_cost_usd / total_dbus ELSE NULL END, 0.5) AS median_cost_per_dbu
    FROM job_aggregates
    WHERE total_dbus > 0
)
SELECT 
    ja.workspace_id,
    ja.job_id,
    COALESCE(jn.job_name, ja.job_id) AS job_name,
    ja.total_runs,
    ja.total_dbus,
    ja.total_cost_usd,
    ja.total_duration_seconds / 3600.0 AS total_runtime_hours,
    ja.avg_dbus_per_run,
    ja.avg_cost_per_run,
    -- Cost per DBU
    CASE 
        WHEN ja.total_dbus > 0 
        THEN ja.total_cost_usd / ja.total_dbus 
        ELSE 0 
    END AS cost_per_dbu,
    -- Efficiency rating (compared to median)
    CASE 
        WHEN ja.total_dbus > 0 AND os.median_cost_per_dbu > 0
        THEN CASE 
            WHEN (ja.total_cost_usd / ja.total_dbus) > (os.median_cost_per_dbu * 1.2)
            THEN 'HIGH_COST'  -- 20% above median
            WHEN (ja.total_cost_usd / ja.total_dbus) < (os.median_cost_per_dbu * 0.8)
            THEN 'LOW_COST'   -- 20% below median
            ELSE 'AVERAGE'
        END
        ELSE 'UNKNOWN'
    END AS efficiency_rating,
    -- DBUs per hour
    CASE 
        WHEN ja.total_duration_seconds > 0 
        THEN (ja.total_dbus * 3600.0) / ja.total_duration_seconds 
        ELSE 0 
    END AS dbus_per_hour
FROM job_aggregates ja
CROSS JOIN overall_stats os
LEFT JOIN job_names jn
    ON ja.workspace_id = jn.workspace_id
    AND ja.job_id = jn.job_id
WHERE ja.total_dbus > 0
ORDER BY cost_per_dbu DESC;


-- ============================================================================
-- Query 7: Workspace-Level Cost Summary
-- ============================================================================
-- Aggregates costs at workspace level for multi-workspace accounts
-- ============================================================================

WITH usage_records AS (
    SELECT 
        u.workspace_id,
        u.metadata.usage_metadata.job_id AS job_id,
        u.metadata.usage_metadata.job_run_id AS job_run_id,
        u.billing_origin_product AS product,
        u.usage_quantity AS dbu_quantity,
        u.usage_date,
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
        u.workspace_id,
        u.job_id,
        u.job_run_id,
        u.product,
        u.dbu_quantity,
        u.usage_date,
        u.sku_name,
        COALESCE(p.list_price_per_dbu, 0) AS price_per_dbu,
        u.dbu_quantity * COALESCE(p.list_price_per_dbu, 0) AS cost_usd
    FROM usage_records u
    LEFT JOIN pricing_data p
        ON u.sku_name = p.sku_name
        AND p.price_start_time <= u.usage_date
        AND (p.price_end_time IS NULL OR u.usage_date < p.price_end_time)
),
job_runs AS (
    SELECT DISTINCT
        workspace_id,
        job_id,
        job_run_id
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= '${start_date}'
        AND period_start_time < '${end_date}'
)
SELECT 
    up.workspace_id,
    COUNT(DISTINCT up.job_id) AS total_jobs,
    COUNT(DISTINCT up.job_run_id) AS total_job_runs,
    COUNT(DISTINCT up.product) AS products_used,
    SUM(up.dbu_quantity) AS total_dbus,
    SUM(up.cost_usd) AS total_cost_usd,
    AVG(up.cost_usd) AS avg_cost_per_record,
    PERCENTILE_APPROX(up.cost_usd, 0.95) AS p95_cost_per_record,
    -- Cost breakdown by product
    SUM(CASE WHEN up.product = 'JOBS_COMPUTE' THEN up.cost_usd ELSE 0 END) AS jobs_compute_cost,
    SUM(CASE WHEN up.product = 'SQL_COMPUTE' THEN up.cost_usd ELSE 0 END) AS sql_compute_cost,
    SUM(CASE WHEN up.product = 'DLT' THEN up.cost_usd ELSE 0 END) AS dlt_cost,
    -- Percentage of total
    CASE 
        WHEN (SELECT SUM(cost_usd) FROM usage_with_pricing) > 0
        THEN (SUM(up.cost_usd) / 
              (SELECT SUM(cost_usd) FROM usage_with_pricing)) * 100.0
        ELSE 0
    END AS pct_of_total_cost
FROM usage_with_pricing up
GROUP BY up.workspace_id
ORDER BY total_cost_usd DESC;


-- ============================================================================
-- Usage Notes
-- ============================================================================
-- 
-- Parameter Replacement:
--   Replace ${start_date} and ${end_date} with actual dates:
--   Example: WHERE period_start_time >= '2024-01-01'
--
-- Optional Filters:
--   Add WHERE clauses to filter by workspace_id or job_id:
--   Example: AND workspace_id IN ('workspace1', 'workspace2')
--   Example: AND job_id IN ('job1', 'job2')
--
-- Query Selection:
--   - Query 1: Job-level summary (best for high-level overview)
--   - Query 2: Job run-level details (best for detailed analysis)
--   - Query 3: Task-level breakdown (best for identifying expensive tasks)
--   - Query 4: Product-level breakdown (best for understanding cost drivers)
--   - Query 5: Daily trends (best for time-series analysis)
--   - Query 6: Cost efficiency (best for identifying optimization opportunities)
--   - Query 7: Workspace-level summary (best for multi-workspace accounts)
--
-- Performance Tips:
--   - Use date filters to limit data volume
--   - Consider partitioning by workspace_id if querying large datasets
--   - Index on usage_date for better performance
-- ============================================================================

