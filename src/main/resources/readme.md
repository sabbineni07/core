-- ============================================================================
-- Job Cost Analysis SQL Scripts
-- Purpose: Calculate end-to-end cost per job run by combining Azure Cost 
--          Management data with Databricks system tables
-- ============================================================================
-- 
-- Usage: Run these queries in sequence using PySpark batch processing
--        Process previous day data (UsageDateTime = yesterday)
--
-- ============================================================================

-- ============================================================================
-- STEP 1: Parse Azure Cost Data - Extract Tags and Filter Previous Day
-- ============================================================================
-- This query parses the Tags string column and extracts key fields
-- Filters for previous day to ensure data completeness

CREATE OR REPLACE TEMPORARY VIEW parsed_azure_costs AS
SELECT
    -- Original fields
    SubscriptionGuid,
    ResourceGroup,
    ResourceLocation,
    UsageDateTime,
    MeterCategory,
    MeterSubCategory,
    MeterId,
    MeterName,
    MeterRegion,
    UsageQuantity,
    ResourceRate,
    PreTaxCost,
    ConsumedService,
    ResourceType,
    InstanceId,
    OfferId,
    AdditionalInfo,
    ServiceInfo1,
    ServiceInfo2,
    ServiceName,
    ServiceTier,
    Currency,
    UnitOfMeasure,
    
    -- Parse Tags string to extract key-value pairs
    -- Tags format: "key1": "value1", "key2": "value2", etc.
    -- Extract JobId
    REGEXP_EXTRACT(
        Tags,
        '"JobId"\\s*:\\s*"([^"]+)"',
        1
    ) AS JobId,
    
    -- Extract ClusterId
    REGEXP_EXTRACT(
        Tags,
        '"ClusterId"\\s*:\\s*"([^"]+)"',
        1
    ) AS ClusterId,
    
    -- Extract RunName
    REGEXP_EXTRACT(
        Tags,
        '"RunName"\\s*:\\s*"([^"]+)"',
        1
    ) AS RunName,
    
    -- Extract Team (custom tag)
    REGEXP_EXTRACT(
        Tags,
        '"Team"\\s*:\\s*"([^"]+)"',
        1
    ) AS Team,
    
    -- Extract JobType (custom tag)
    REGEXP_EXTRACT(
        Tags,
        '"JobType"\\s*:\\s*"([^"]+)"',
        1
    ) AS JobType,
    
    -- Extract Environment (custom tag)
    REGEXP_EXTRACT(
        Tags,
        '"Environment"\\s*:\\s*"([^"]+)"',
        1
    ) AS Environment,
    
    -- Extract ClusterName
    REGEXP_EXTRACT(
        Tags,
        '"ClusterName"\\s*:\\s*"([^"]+)"',
        1
    ) AS ClusterName,
    
    -- Extract DatabricksWorkspace
    REGEXP_EXTRACT(
        Tags,
        '"DatabricksWorkspace"\\s*:\\s*"([^"]+)"',
        1
    ) AS DatabricksWorkspace,
    
    -- Extract ResourceClass
    REGEXP_EXTRACT(
        Tags,
        '"ResourceClass"\\s*:\\s*"([^"]+)"',
        1
    ) AS ResourceClass,
    
    -- Date for filtering
    DATE(UsageDateTime) AS UsageDate,
    
    -- Original Tags for reference
    Tags AS OriginalTags
    
FROM
    azure_cost_usage_data  -- Replace with your actual table name
WHERE
    -- Filter for previous day (process yesterday's data)
    DATE(UsageDateTime) = DATE_SUB(CURRENT_DATE(), 1)
    -- Only include relevant cost categories
    AND MeterCategory IN (
        'Azure Databricks',
        'Virtual Machines',
        'Storage',
        'Bandwidth',
        'Virtual Network',
        'Microsoft Defender for Cloud'
    )
    -- Filter out records with missing JobId or ClusterId
    AND REGEXP_EXTRACT(Tags, '"JobId"\\s*:\\s*"([^"]+)"', 1) IS NOT NULL
    AND REGEXP_EXTRACT(Tags, '"ClusterId"\\s*:\\s*"([^"]+)"', 1) IS NOT NULL;


-- ============================================================================
-- STEP 2: Get Job Run Details from System Tables
-- ============================================================================
-- Join with system.lakeflow.job_run_timeline to get job run metadata
-- Filter for previous day job runs

CREATE OR REPLACE TEMPORARY VIEW job_run_details AS
SELECT
    jrt.account_id,
    jrt.workspace_id,
    jrt.job_id,
    jrt.job_run_id,
    jrt.cluster_id,
    jrt.name AS job_name,
    jrt.run_name,
    jrt.start_time,
    jrt.end_time,
    jrt.duration_seconds,
    jrt.result_state,
    jrt.termination_code,
    jrt.trigger_type,
    jrt.run_type,
    jrt.creator_user_name,
    jrt.run_as_user_name,
    DATE(jrt.start_time) AS run_date,
    
    -- Calculate run duration in hours for cost allocation
    COALESCE(
        (UNIX_TIMESTAMP(jrt.end_time) - UNIX_TIMESTAMP(jrt.start_time)) / 3600.0,
        0
    ) AS duration_hours
    
FROM
    system.lakeflow.job_run_timeline jrt
WHERE
    -- Filter for previous day job runs
    DATE(jrt.start_time) = DATE_SUB(CURRENT_DATE(), 1)
    -- Only include completed runs (exclude in-progress)
    AND jrt.result_state IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')
    -- Exclude null cluster_id (shouldn't happen but safety check)
    AND jrt.cluster_id IS NOT NULL;


-- ============================================================================
-- STEP 3: Get DBU Usage from System Billing Table
-- ============================================================================
-- Join with system.billing.usage to get DBU consumption per job run
-- This helps validate cost allocation

CREATE OR REPLACE TEMPORARY VIEW job_run_dbu_usage AS
SELECT
    bu.account_id,
    bu.workspace_id,
    bu.job_id,
    bu.job_run_id,
    bu.cluster_id,
    DATE(bu.usage_start_time) AS usage_date,
    SUM(bu.usage_quantity) AS total_dbu_consumed,
    SUM(bu.cost_in_billing_currency) AS total_dbu_cost
    
FROM
    system.billing.usage bu
WHERE
    -- Filter for previous day
    DATE(bu.usage_start_time) = DATE_SUB(CURRENT_DATE(), 1)
    -- Only job-related usage
    AND bu.job_id IS NOT NULL
    AND bu.job_run_id IS NOT NULL
GROUP BY
    bu.account_id,
    bu.workspace_id,
    bu.job_id,
    bu.job_run_id,
    bu.cluster_id,
    DATE(bu.usage_start_time);


-- ============================================================================
-- STEP 4: Join Azure Costs with Job Run Details
-- ============================================================================
-- Match cost records to job runs using JobId + ClusterId
-- This creates the foundation for cost per job run calculation

CREATE OR REPLACE TEMPORARY VIEW job_run_costs_raw AS
SELECT
    -- Job Run Identifiers
    jrd.job_run_id,
    jrd.job_id,
    jrd.cluster_id,
    jrd.job_name,
    jrd.run_name,
    
    -- Job Run Metadata
    jrd.start_time AS job_start_time,
    jrd.end_time AS job_end_time,
    jrd.duration_seconds,
    jrd.duration_hours,
    jrd.result_state,
    jrd.termination_code,
    jrd.trigger_type,
    jrd.run_type,
    jrd.creator_user_name,
    jrd.run_as_user_name,
    jrd.account_id,
    jrd.workspace_id,
    
    -- Cost Data
    pac.UsageDateTime,
    pac.MeterCategory,
    pac.MeterSubCategory,
    pac.MeterName,
    pac.UsageQuantity,
    pac.ResourceRate,
    pac.PreTaxCost,
    pac.ConsumedService,
    pac.ResourceType,
    pac.Currency,
    pac.UnitOfMeasure,
    
    -- Tags Metadata
    pac.Team,
    pac.JobType,
    pac.Environment,
    pac.ClusterName,
    pac.DatabricksWorkspace,
    pac.ResourceClass,
    
    -- Azure Metadata
    pac.SubscriptionGuid,
    pac.ResourceGroup,
    pac.ResourceLocation,
    pac.InstanceId
    
FROM
    parsed_azure_costs pac
INNER JOIN
    job_run_details jrd
    ON pac.JobId = jrd.job_id
    AND pac.ClusterId = jrd.cluster_id
    AND DATE(pac.UsageDateTime) = DATE(jrd.start_time)
    
-- Optional: Left join with DBU usage for validation
LEFT JOIN
    job_run_dbu_usage dbu
    ON jrd.job_run_id = dbu.job_run_id
    AND jrd.job_id = dbu.job_id
    AND jrd.cluster_id = dbu.cluster_id;


-- ============================================================================
-- STEP 5: Calculate Cost per Job Run (Aggregated by Category)
-- ============================================================================
-- Aggregate costs by job run and meter category
-- This provides cost breakdown by category per job run

CREATE OR REPLACE TEMPORARY VIEW job_run_cost_by_category AS
SELECT
    job_run_id,
    job_id,
    cluster_id,
    job_name,
    run_name,
    job_start_time,
    job_end_time,
    duration_seconds,
    duration_hours,
    result_state,
    termination_code,
    trigger_type,
    run_type,
    creator_user_name,
    run_as_user_name,
    account_id,
    workspace_id,
    Team,
    JobType,
    Environment,
    ClusterName,
    DatabricksWorkspace,
    ResourceClass,
    SubscriptionGuid,
    ResourceGroup,
    ResourceLocation,
    Currency,
    
    -- Cost by Category
    SUM(CASE WHEN MeterCategory = 'Azure Databricks' THEN PreTaxCost ELSE 0 END) AS cost_databricks,
    SUM(CASE WHEN MeterCategory = 'Virtual Machines' THEN PreTaxCost ELSE 0 END) AS cost_virtual_machines,
    SUM(CASE WHEN MeterCategory = 'Storage' THEN PreTaxCost ELSE 0 END) AS cost_storage,
    SUM(CASE WHEN MeterCategory = 'Bandwidth' THEN PreTaxCost ELSE 0 END) AS cost_bandwidth,
    SUM(CASE WHEN MeterCategory = 'Virtual Network' THEN PreTaxCost ELSE 0 END) AS cost_virtual_network,
    SUM(CASE WHEN MeterCategory = 'Microsoft Defender for Cloud' THEN PreTaxCost ELSE 0 END) AS cost_defender,
    
    -- Total Cost (sum of all categories)
    SUM(PreTaxCost) AS total_cost,
    
    -- Usage quantities by category
    SUM(CASE WHEN MeterCategory = 'Azure Databricks' THEN UsageQuantity ELSE 0 END) AS usage_quantity_databricks,
    SUM(CASE WHEN MeterCategory = 'Virtual Machines' THEN UsageQuantity ELSE 0 END) AS usage_quantity_vm,
    SUM(CASE WHEN MeterCategory = 'Storage' THEN UsageQuantity ELSE 0 END) AS usage_quantity_storage,
    
    -- Count of cost records per category
    COUNT(DISTINCT CASE WHEN MeterCategory = 'Azure Databricks' THEN MeterId END) AS cost_records_databricks,
    COUNT(DISTINCT CASE WHEN MeterCategory = 'Virtual Machines' THEN MeterId END) AS cost_records_vm,
    COUNT(DISTINCT CASE WHEN MeterCategory = 'Storage' THEN MeterId END) AS cost_records_storage,
    COUNT(*) AS total_cost_records,
    
    -- Date
    DATE(job_start_time) AS run_date
    
FROM
    job_run_costs_raw
GROUP BY
    job_run_id,
    job_id,
    cluster_id,
    job_name,
    run_name,
    job_start_time,
    job_end_time,
    duration_seconds,
    duration_hours,
    result_state,
    termination_code,
    trigger_type,
    run_type,
    creator_user_name,
    run_as_user_name,
    account_id,
    workspace_id,
    Team,
    JobType,
    Environment,
    ClusterName,
    DatabricksWorkspace,
    ResourceClass,
    SubscriptionGuid,
    ResourceGroup,
    ResourceLocation,
    Currency,
    DATE(job_start_time);


-- ============================================================================
-- STEP 6: Final Output - Cost per Job Run (Complete View)
-- ============================================================================
-- This is the main output table with end-to-end cost per job run
-- Includes all cost categories aggregated

CREATE OR REPLACE TEMPORARY VIEW job_run_cost_summary AS
SELECT
    -- Job Run Identifiers
    job_run_id,
    job_id,
    cluster_id,
    job_name,
    run_name,
    
    -- Job Run Metadata
    job_start_time,
    job_end_time,
    duration_seconds,
    duration_hours,
    result_state,
    termination_code,
    trigger_type,
    run_type,
    creator_user_name,
    run_as_user_name,
    
    -- Account/Workspace
    account_id,
    workspace_id,
    SubscriptionGuid AS subscription_guid,
    ResourceGroup AS resource_group,
    ResourceLocation AS resource_location,
    
    -- Organizational Tags
    Team,
    JobType,
    Environment,
    ClusterName,
    DatabricksWorkspace,
    ResourceClass,
    
    -- Cost Breakdown by Category
    ROUND(cost_databricks, 4) AS cost_databricks,
    ROUND(cost_virtual_machines, 4) AS cost_virtual_machines,
    ROUND(cost_storage, 4) AS cost_storage,
    ROUND(cost_bandwidth, 4) AS cost_bandwidth,
    ROUND(cost_virtual_network, 4) AS cost_virtual_network,
    ROUND(cost_defender, 4) AS cost_defender,
    
    -- Total Cost
    ROUND(total_cost, 4) AS total_cost,
    
    -- Cost per Hour (efficiency metric)
    CASE 
        WHEN duration_hours > 0 
        THEN ROUND(total_cost / duration_hours, 4)
        ELSE NULL
    END AS cost_per_hour,
    
    -- Usage Quantities
    usage_quantity_databricks,
    usage_quantity_vm,
    usage_quantity_storage,
    
    -- Data Quality Metrics
    total_cost_records,
    cost_records_databricks,
    cost_records_vm,
    cost_records_storage,
    
    -- Currency
    Currency AS currency,
    
    -- Date
    run_date,
    
    -- Processing metadata
    CURRENT_TIMESTAMP() AS processed_at
    
FROM
    job_run_cost_by_category;


-- ============================================================================
-- STEP 7: Daily Cost Summary (Aggregated Views)
-- ============================================================================
-- Summary views for reporting and analysis

-- Summary by Job
CREATE OR REPLACE TEMPORARY VIEW daily_cost_summary_by_job AS
SELECT
    run_date,
    job_id,
    job_name,
    Team,
    JobType,
    Environment,
    account_id,
    workspace_id,
    currency,
    
    COUNT(DISTINCT job_run_id) AS total_job_runs,
    COUNT(DISTINCT CASE WHEN result_state = 'SUCCEEDED' THEN job_run_id END) AS succeeded_runs,
    COUNT(DISTINCT CASE WHEN result_state = 'FAILED' THEN job_run_id END) AS failed_runs,
    
    SUM(total_cost) AS total_cost,
    SUM(cost_databricks) AS total_cost_databricks,
    SUM(cost_virtual_machines) AS total_cost_vm,
    SUM(cost_storage) AS total_cost_storage,
    SUM(cost_bandwidth) AS total_cost_bandwidth,
    SUM(cost_virtual_network) AS total_cost_vnet,
    SUM(cost_defender) AS total_cost_defender,
    
    AVG(total_cost) AS avg_cost_per_run,
    AVG(cost_per_hour) AS avg_cost_per_hour,
    SUM(duration_hours) AS total_duration_hours
    
FROM
    job_run_cost_summary
GROUP BY
    run_date,
    job_id,
    job_name,
    Team,
    JobType,
    Environment,
    account_id,
    workspace_id,
    currency;


-- Summary by Team
CREATE OR REPLACE TEMPORARY VIEW daily_cost_summary_by_team AS
SELECT
    run_date,
    Team,
    Environment,
    account_id,
    workspace_id,
    currency,
    
    COUNT(DISTINCT job_id) AS total_jobs,
    COUNT(DISTINCT job_run_id) AS total_job_runs,
    COUNT(DISTINCT cluster_id) AS total_clusters,
    
    SUM(total_cost) AS total_cost,
    SUM(cost_databricks) AS total_cost_databricks,
    SUM(cost_virtual_machines) AS total_cost_vm,
    SUM(cost_storage) AS total_cost_storage,
    SUM(cost_bandwidth) AS total_cost_bandwidth,
    SUM(cost_virtual_network) AS total_cost_vnet,
    SUM(cost_defender) AS total_cost_defender,
    
    AVG(total_cost) AS avg_cost_per_run,
    SUM(duration_hours) AS total_duration_hours
    
FROM
    job_run_cost_summary
WHERE
    Team IS NOT NULL
GROUP BY
    run_date,
    Team,
    Environment,
    account_id,
    workspace_id,
    currency;


-- Summary by Environment
CREATE OR REPLACE TEMPORARY VIEW daily_cost_summary_by_environment AS
SELECT
    run_date,
    Environment,
    account_id,
    workspace_id,
    currency,
    
    COUNT(DISTINCT job_id) AS total_jobs,
    COUNT(DISTINCT job_run_id) AS total_job_runs,
    COUNT(DISTINCT Team) AS total_teams,
    
    SUM(total_cost) AS total_cost,
    SUM(cost_databricks) AS total_cost_databricks,
    SUM(cost_virtual_machines) AS total_cost_vm,
    SUM(cost_storage) AS total_cost_storage,
    SUM(cost_bandwidth) AS total_cost_bandwidth,
    SUM(cost_virtual_network) AS total_cost_vnet,
    SUM(cost_defender) AS total_cost_defender,
    
    AVG(total_cost) AS avg_cost_per_run,
    SUM(duration_hours) AS total_duration_hours
    
FROM
    job_run_cost_summary
WHERE
    Environment IS NOT NULL
GROUP BY
    run_date,
    Environment,
    account_id,
    workspace_id,
    currency;


-- ============================================================================
-- STEP 8: Data Quality Checks
-- ============================================================================
-- Identify potential data quality issues

CREATE OR REPLACE TEMPORARY VIEW data_quality_issues AS
SELECT
    'Missing Job Run Match' AS issue_type,
    COUNT(*) AS issue_count,
    'Azure cost records without matching job runs' AS description
FROM
    parsed_azure_costs pac
LEFT JOIN
    job_run_details jrd
    ON pac.JobId = jrd.job_id
    AND pac.ClusterId = jrd.cluster_id
    AND DATE(pac.UsageDateTime) = DATE(jrd.start_time)
WHERE
    jrd.job_run_id IS NULL

UNION ALL

SELECT
    'Missing Cost Records' AS issue_type,
    COUNT(*) AS issue_count,
    'Job runs without matching cost records' AS description
FROM
    job_run_details jrd
LEFT JOIN
    parsed_azure_costs pac
    ON pac.JobId = jrd.job_id
    AND pac.ClusterId = jrd.cluster_id
    AND DATE(pac.UsageDateTime) = DATE(jrd.start_time)
WHERE
    pac.JobId IS NULL

UNION ALL

SELECT
    'Zero Cost Job Runs' AS issue_type,
    COUNT(*) AS issue_count,
    'Job runs with zero total cost' AS description
FROM
    job_run_cost_summary
WHERE
    total_cost = 0 OR total_cost IS NULL

UNION ALL

SELECT
    'Missing Tags' AS issue_type,
    COUNT(*) AS issue_count,
    'Cost records with missing JobId or ClusterId in tags' AS description
FROM
    azure_cost_usage_data  -- Replace with your actual table name
WHERE
    DATE(UsageDateTime) = DATE_SUB(CURRENT_DATE(), 1)
    AND (
        REGEXP_EXTRACT(Tags, '"JobId"\\s*:\\s*"([^"]+)"', 1) IS NULL
        OR REGEXP_EXTRACT(Tags, '"ClusterId"\\s*:\\s*"([^"]+)"', 1) IS NULL
    );


-- ============================================================================
-- FINAL OUTPUT QUERIES
-- ============================================================================
-- Use these queries to extract the final results

-- Main output: Cost per Job Run
-- SELECT * FROM job_run_cost_summary ORDER BY total_cost DESC;

-- Cost breakdown by category
-- SELECT * FROM job_run_cost_by_category ORDER BY total_cost DESC;

-- Daily summary by job
-- SELECT * FROM daily_cost_summary_by_job ORDER BY total_cost DESC;

-- Daily summary by team
-- SELECT * FROM daily_cost_summary_by_team ORDER BY total_cost DESC;

-- Daily summary by environment
-- SELECT SELECT * FROM daily_cost_summary_by_environment ORDER BY total_cost DESC;

-- Data quality issues
-- SELECT * FROM data_quality_issues;

