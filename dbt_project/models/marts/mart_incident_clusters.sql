{{ config(materialized='table') }}

-- Bin coordinates into ~111m grid cells (0.001 degree ≈ 111m)
WITH gridded AS (
    SELECT
        reported_date,
        agency,
        ROUND(latitude,  3) AS grid_lat,
        ROUND(longitude, 3) AS grid_lon,
        COUNT(*)            AS incident_count,
        SUM(CASE WHEN is_escalated THEN 1 ELSE 0 END) AS escalated_count,
        AVG(priority)       AS avg_priority
    FROM {{ ref('stg_incidents') }}
    GROUP BY 1, 2, 3, 4
),

stats AS (
    SELECT
        reported_date,
        AVG(incident_count)    AS global_mean,
        STDDEV(incident_count) AS global_std
    FROM gridded
    GROUP BY 1
)

SELECT
    g.reported_date,
    g.agency,
    g.grid_lat,
    g.grid_lon,
    g.incident_count,
    g.escalated_count,
    g.avg_priority,
    CASE
        WHEN s.global_std = 0 THEN 0
        ELSE (g.incident_count - s.global_mean) / s.global_std
    END AS density_zscore,
    RANK() OVER (
        PARTITION BY g.reported_date
        ORDER BY g.incident_count DESC
    ) AS density_rank
FROM gridded g
INNER JOIN stats s ON g.reported_date = s.reported_date
