{{ config(materialized='table') }}

WITH hourly AS (
    SELECT
        DATE_TRUNC('hour', dispatched_at)        AS hour_bucket,
        agency,
        district,
        COUNT(DISTINCT incident_id)              AS incident_volume,
        AVG(units_available_at_dispatch)         AS avg_units_available,
        AVG(response_time_seconds)               AS avg_response_time_s,
        COUNT(*) FILTER (
            WHERE response_tier = 'CRITICAL_DELAY'
        )                                        AS critical_delay_count
    FROM {{ ref('stg_dispatch_logs') }}
    GROUP BY 1, 2, 3
)

SELECT
    hour_bucket,
    agency,
    district,
    incident_volume,
    ROUND(avg_units_available, 2)               AS avg_units_available,
    ROUND(avg_response_time_s,  2)              AS avg_response_time_s,
    critical_delay_count,
    -- Gap flag: under-resourced OR response time degraded
    (avg_units_available < 3 AND incident_volume > 5)
        OR avg_response_time_s > 900            AS resource_gap_flag
FROM hourly
