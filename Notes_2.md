-- ============================================================================
-- Databricks SQL Function: Cluster Configuration Recommendation
-- ============================================================================
-- This function takes job resource utilization metrics as input and returns
-- a recommended cluster configuration in JSON format.
--
-- Based on the design framework in CLUSTER_CONFIGURATION_RECOMMENDATION_DESIGN.md
-- ============================================================================

CREATE OR REPLACE FUNCTION recommend_cluster_config(
    -- Input parameters: Resource utilization metrics
    avg_cpu_utilization_pct DOUBLE,
    avg_memory_utilization_pct DOUBLE,
    avg_nodes_consumed DOUBLE,
    p95_nodes_consumed DOUBLE,
    p99_nodes_consumed DOUBLE,
    peak_cpu_utilization_pct DOUBLE,
    peak_memory_utilization_pct DOUBLE,
    avg_cpus_utilized DOUBLE,
    avg_memory_gb_utilized DOUBLE,
    current_node_type STRING,
    max_nodes_provisioned INT,
    rows_added BIGINT DEFAULT NULL,
    num_of_tables INT DEFAULT NULL,
    safety_margin_pct DOUBLE DEFAULT 20.0
)
RETURNS STRING
LANGUAGE SQL
RETURNS NULL ON NULL INPUT
AS $$
    SELECT TO_JSON(NAMED_STRUCT(
        -- Recommended node family (D, E, or F)
        'recommended_family', 
        CASE 
            -- Memory-to-CPU ratio > 1.5 OR simple ETL workload
            WHEN (avg_memory_utilization_pct / NULLIF(avg_cpu_utilization_pct, 0) > 1.5)
                 OR (rows_added > 10000000 AND (num_of_tables IS NULL OR num_of_tables <= 3) AND avg_cpu_utilization_pct < 20)
                THEN 'E'
            -- CPU-to-Memory ratio > 1.5 OR complex aggregation workload
            WHEN (avg_cpu_utilization_pct / NULLIF(avg_memory_utilization_pct, 0) > 1.5)
                 OR (num_of_tables IS NOT NULL AND num_of_tables > 5 AND avg_cpu_utilization_pct > 50)
                THEN 'F'
            -- Default to general purpose
            ELSE 'D'
        END,
        
        -- Recommended vCPU count (2, 4, 8, or 16)
        'recommended_vcpus',
        CASE 
            WHEN avg_cpus_utilized > 0 AND avg_nodes_consumed > 0 THEN
                CASE 
                    WHEN CEIL(avg_cpus_utilized / avg_nodes_consumed) <= 2 THEN 2
                    WHEN CEIL(avg_cpus_utilized / avg_nodes_consumed) <= 4 THEN 4
                    WHEN CEIL(avg_cpus_utilized / avg_nodes_consumed) <= 8 THEN 8
                    ELSE 16
                END
            ELSE 4  -- Default to 4 if no data
        END,
        
        -- Recommended node type (complete string)
        'recommended_node_type',
        CONCAT(
            'Standard_',
            CASE 
                WHEN (avg_memory_utilization_pct / NULLIF(avg_cpu_utilization_pct, 0) > 1.5)
                     OR (rows_added > 10000000 AND (num_of_tables IS NULL OR num_of_tables <= 3) AND avg_cpu_utilization_pct < 20)
                    THEN 'E'
                WHEN (avg_cpu_utilization_pct / NULLIF(avg_memory_utilization_pct, 0) > 1.5)
                     OR (num_of_tables IS NOT NULL AND num_of_tables > 5 AND avg_cpu_utilization_pct > 50)
                    THEN 'F'
                ELSE 'D'
            END,
            CASE 
                WHEN avg_cpus_utilized > 0 AND avg_nodes_consumed > 0 THEN
                    CASE 
                        WHEN CEIL(avg_cpus_utilized / avg_nodes_consumed) <= 2 THEN '2'
                        WHEN CEIL(avg_cpus_utilized / avg_nodes_consumed) <= 4 THEN '4'
                        WHEN CEIL(avg_cpus_utilized / avg_nodes_consumed) <= 8 THEN '8'
                        ELSE '16'
                    END
                ELSE '4'
            END,
            's_v3'
        ),
        
        -- Recommended min workers
        'recommended_min_workers',
        GREATEST(1, CEIL(avg_nodes_consumed * (1 - safety_margin_pct / 100.0))),
        
        -- Recommended max workers
        'recommended_max_workers',
        LEAST(
            32,  -- Cap at 32 workers
            GREATEST(
                GREATEST(1, CEIL(avg_nodes_consumed * (1 - safety_margin_pct / 100.0))),
                CEIL(COALESCE(p95_nodes_consumed, avg_nodes_consumed) * (1 + safety_margin_pct / 100.0))
            )
        ),
        
        -- Current configuration (for comparison)
        'current_config', NAMED_STRUCT(
            'node_type', current_node_type,
            'max_workers', max_nodes_provisioned,
            'min_workers', 1
        ),
        
        -- Utilization metrics (for reference)
        'utilization_metrics', NAMED_STRUCT(
            'avg_cpu_utilization_pct', ROUND(avg_cpu_utilization_pct, 2),
            'avg_memory_utilization_pct', ROUND(avg_memory_utilization_pct, 2),
            'peak_cpu_utilization_pct', ROUND(COALESCE(peak_cpu_utilization_pct, avg_cpu_utilization_pct), 2),
            'peak_memory_utilization_pct', ROUND(COALESCE(peak_memory_utilization_pct, avg_memory_utilization_pct), 2),
            'avg_nodes_consumed', ROUND(avg_nodes_consumed, 2),
            'p95_nodes_consumed', ROUND(COALESCE(p95_nodes_consumed, avg_nodes_consumed), 2),
            'p99_nodes_consumed', ROUND(COALESCE(p99_nodes_consumed, p95_nodes_consumed, avg_nodes_consumed), 2),
            'avg_cpus_utilized', ROUND(COALESCE(avg_cpus_utilized, 0), 2),
            'avg_memory_gb_utilized', ROUND(COALESCE(avg_memory_gb_utilized, 0), 2)
        ),
        
        -- Workload characteristics
        'workload_characteristics', NAMED_STRUCT(
            'cpu_to_memory_ratio', ROUND(avg_cpu_utilization_pct / NULLIF(avg_memory_utilization_pct, 0), 2),
            'memory_to_cpu_ratio', ROUND(avg_memory_utilization_pct / NULLIF(avg_cpu_utilization_pct, 0), 2),
            'workload_type', 
            CASE 
                WHEN rows_added > 10000000 AND (num_of_tables IS NULL OR num_of_tables <= 3) AND avg_cpu_utilization_pct < 20
                    THEN 'SIMPLE_ETL'
                WHEN num_of_tables IS NOT NULL AND num_of_tables > 5 AND avg_cpu_utilization_pct > 50
                    THEN 'COMPLEX_AGGREGATION'
                ELSE 'MIXED'
            END,
            'rows_added', rows_added,
            'num_of_tables', num_of_tables
        ),
        
        -- Recommendation rationale
        'rationale',
        CONCAT(
            CASE 
                WHEN (avg_memory_utilization_pct / NULLIF(avg_cpu_utilization_pct, 0) > 1.5)
                     OR (rows_added > 10000000 AND (num_of_tables IS NULL OR num_of_tables <= 3) AND avg_cpu_utilization_pct < 20)
                    THEN 'E family recommended for memory-optimized workloads. '
                WHEN (avg_cpu_utilization_pct / NULLIF(avg_memory_utilization_pct, 0) > 1.5)
                     OR (num_of_tables IS NOT NULL AND num_of_tables > 5 AND avg_cpu_utilization_pct > 50)
                    THEN 'F family recommended for compute-optimized workloads. '
                ELSE 'D family recommended for general-purpose workloads. '
            END,
            CASE 
                WHEN avg_cpu_utilization_pct < 20 AND avg_memory_utilization_pct < 20
                    THEN 'Low utilization detected - recommending smaller node size. '
                WHEN avg_cpu_utilization_pct > 70 OR avg_memory_utilization_pct > 70
                    THEN 'High utilization detected - current sizing may be appropriate. '
                ELSE ''
            END,
            CASE 
                WHEN CEIL(COALESCE(p95_nodes_consumed, avg_nodes_consumed) * (1 + safety_margin_pct / 100.0)) < max_nodes_provisioned
                    THEN CONCAT('Max workers reduced from ', max_nodes_provisioned, ' to ', 
                               CEIL(COALESCE(p95_nodes_consumed, avg_nodes_consumed) * (1 + safety_margin_pct / 100.0)), 
                               ' based on P95 consumption. ')
                ELSE ''
            END
        ),
        
        -- Confidence score (0-1)
        'confidence_score',
        LEAST(1.0, GREATEST(0.0,
            -- Pattern clarity component (0-0.7)
            (CASE WHEN avg_cpu_utilization_pct < 20 THEN 0.7 ELSE 0.5 END +
             CASE WHEN avg_memory_utilization_pct < 20 THEN 0.7 ELSE 0.5 END) / 2.0 * 0.7 +
            -- Data completeness component (0-0.3)
            (CASE WHEN p95_nodes_consumed IS NOT NULL AND p99_nodes_consumed IS NOT NULL THEN 0.3
                  WHEN p95_nodes_consumed IS NOT NULL THEN 0.2
                  ELSE 0.1 END)
        )),
        
        -- Expected improvements
        'expected_improvements', NAMED_STRUCT(
            'provisioning_efficiency_improvement_pct',
            CASE 
                WHEN max_nodes_provisioned > 0 THEN
                    ROUND(((max_nodes_provisioned - CEIL(COALESCE(p95_nodes_consumed, avg_nodes_consumed) * (1 + safety_margin_pct / 100.0))) / max_nodes_provisioned) * 100, 2)
                ELSE 0
            END,
            'estimated_cost_reduction_pct',
            CASE 
                WHEN max_nodes_provisioned > 0 THEN
                    -- Rough estimate: smaller nodes + fewer max workers = cost reduction
                    ROUND(((max_nodes_provisioned - CEIL(COALESCE(p95_nodes_consumed, avg_nodes_consumed) * (1 + safety_margin_pct / 100.0))) / max_nodes_provisioned) * 100 * 0.8, 2)
                ELSE 0
            END
        ),
        
        -- Timestamp
        'recommendation_timestamp', CURRENT_TIMESTAMP()
    ))
$$;


-- ============================================================================
-- Usage Examples
-- ============================================================================

-- Example 1: Low utilization job (simple ETL)
-- SELECT recommend_cluster_config(
--     4.92,   -- avg_cpu_utilization_pct
--     9.09,   -- avg_memory_utilization_pct
--     3.5,    -- avg_nodes_consumed
--     4.0,    -- p95_nodes_consumed
--     4.5,    -- p99_nodes_consumed
--     8.0,    -- peak_cpu_utilization_pct
--     12.0,   -- peak_memory_utilization_pct
--     6.53,   -- avg_cpus_utilized
--     3.18,   -- avg_memory_gb_utilized
--     'Standard_E8s_v3',  -- current_node_type
--     16,     -- max_nodes_provisioned
--     45000000, -- rows_added
--     2,      -- num_of_tables
--     20.0    -- safety_margin_pct
-- ) AS recommendation;

-- Example 2: High utilization job (complex aggregation)
-- SELECT recommend_cluster_config(
--     67.13,  -- avg_cpu_utilization_pct
--     51.49,  -- avg_memory_utilization_pct
--     12.5,   -- avg_nodes_consumed
--     14.0,   -- p95_nodes_consumed
--     15.0,   -- p99_nodes_consumed
--     85.0,   -- peak_cpu_utilization_pct
--     65.0,   -- peak_memory_utilization_pct
--     91.30,  -- avg_cpus_utilized
--     64.5,   -- avg_memory_gb_utilized
--     'Standard_E8s_v3',  -- current_node_type
--     16,     -- max_nodes_provisioned
--     5000000, -- rows_added
--     8,      -- num_of_tables
--     20.0    -- safety_margin_pct
-- ) AS recommendation;

-- Example 3: Using with actual job data from a table
-- SELECT 
--     job_id,
--     job_name,
--     recommend_cluster_config(
--         avg_cpu_utilization_pct,
--         avg_memory_utilization_pct,
--         avg_nodes_consumed,
--         p95_nodes_consumed,
--         p99_nodes_consumed,
--         peak_cpu_utilization_pct,
--         peak_memory_utilization_pct,
--         avg_cpus_utilized,
--         avg_memory_gb_utilized,
--         'Standard_E8s_v3',  -- or get from cluster config
--         max_nodes_provisioned,
--         rows_added,
--         num_of_tables,
--         20.0
--     ) AS recommendation
-- FROM job_resource_utilization_metrics
-- WHERE job_id = 'your_job_id';


-- ============================================================================
-- Helper Function: Parse JSON recommendation
-- ============================================================================
-- Optional helper function to parse the JSON result into structured columns
-- Usage: SELECT * FROM parse_cluster_recommendation(recommendation_json_string)

-- CREATE OR REPLACE TEMP VIEW parse_cluster_recommendation(recommendation_json STRING)
-- AS
-- SELECT 
--     FROM_JSON(recommendation_json, 'STRUCT<
--         recommended_family: STRING,
--         recommended_vcpus: INT,
--         recommended_node_type: STRING,
--         recommended_min_workers: INT,
--         recommended_max_workers: INT,
--         current_config: STRUCT<node_type: STRING, max_workers: INT, min_workers: INT>,
--         utilization_metrics: STRUCT<
--             avg_cpu_utilization_pct: DOUBLE,
--             avg_memory_utilization_pct: DOUBLE,
--             peak_cpu_utilization_pct: DOUBLE,
--             peak_memory_utilization_pct: DOUBLE,
--             avg_nodes_consumed: DOUBLE,
--             p95_nodes_consumed: DOUBLE,
--             p99_nodes_consumed: DOUBLE,
--             avg_cpus_utilized: DOUBLE,
--             avg_memory_gb_utilized: DOUBLE
--         >,
--         workload_characteristics: STRUCT<
--             cpu_to_memory_ratio: DOUBLE,
--             memory_to_cpu_ratio: DOUBLE,
--             workload_type: STRING,
--             rows_added: BIGINT,
--             num_of_tables: INT
--         >,
--         rationale: STRING,
--         confidence_score: DOUBLE,
--         expected_improvements: STRUCT<
--             provisioning_efficiency_improvement_pct: DOUBLE,
--             estimated_cost_reduction_pct: DOUBLE
--         >,
--         recommendation_timestamp: TIMESTAMP
--     >') AS parsed;

