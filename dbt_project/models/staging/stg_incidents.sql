{{ config(materialized='view') }}

SELECT
    incident_id,
    agency,
    incident_type,
    reported_at,
    CAST(reported_at AS DATE)                   AS reported_date,
    EXTRACT(HOUR FROM reported_at)::INTEGER     AS reported_hour,
    EXTRACT(DOW FROM reported_at)::INTEGER      AS reported_dow,
    latitude,
    longitude,
    priority,
    CASE priority
        WHEN 1 THEN 'CRITICAL'
        WHEN 2 THEN 'HIGH'
        WHEN 3 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                         AS priority_label,
    district,
    status,
    status = 'ESCALATED'                        AS is_escalated,
    is_injected
FROM {{ ref('raw_incidents') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND latitude  BETWEEN 37.70 AND 37.83
  AND longitude BETWEEN -122.52 AND -122.35
  AND reported_at IS NOT NULL
