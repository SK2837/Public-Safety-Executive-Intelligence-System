{{ config(materialized='table') }}

SELECT
    CAST(dispatched_at AS DATE)                     AS report_date,
    agency,
    district,
    priority,
    COUNT(*)                                         AS incident_count,
    AVG(response_time_seconds)                       AS avg_response_time_s,
    PERCENTILE_CONT(0.5)
        WITHIN GROUP (ORDER BY response_time_seconds) AS median_response_time_s,
    PERCENTILE_CONT(0.9)
        WITHIN GROUP (ORDER BY response_time_seconds) AS p90_response_time_s,
    MIN(response_time_seconds)                       AS min_response_time_s,
    MAX(response_time_seconds)                       AS max_response_time_s,
    COUNT(*) FILTER (WHERE response_tier = 'CRITICAL_DELAY') AS critical_delay_count,
    COUNT(*) FILTER (WHERE response_tier = 'FAST')           AS fast_count,
    AVG(units_available_at_dispatch)                 AS avg_units_available
FROM {{ ref('stg_dispatch_logs') }}
GROUP BY 1, 2, 3, 4
