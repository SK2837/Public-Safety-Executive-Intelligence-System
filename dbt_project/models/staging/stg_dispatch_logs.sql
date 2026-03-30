{{ config(materialized='view') }}

SELECT
    dl.log_id,
    dl.incident_id,
    dl.unit_id,
    dl.dispatched_at,
    dl.arrived_at,
    dl.cleared_at,
    dl.response_time_seconds,
    dl.units_available_at_dispatch,
    -- Service duration: time on-scene until cleared
    DATEDIFF('second', dl.arrived_at, dl.cleared_at) AS service_duration_seconds,
    -- Response tier classification
    CASE
        WHEN dl.response_time_seconds <= 180  THEN 'FAST'
        WHEN dl.response_time_seconds <= 480  THEN 'NORMAL'
        WHEN dl.response_time_seconds <= 1200 THEN 'SLOW'
        ELSE 'CRITICAL_DELAY'
    END AS response_tier,
    i.agency,
    i.district,
    i.priority,
    i.reported_at
FROM {{ ref('raw_dispatch_logs') }} dl
INNER JOIN {{ ref('raw_incidents') }} i
    ON dl.incident_id = i.incident_id
WHERE dl.dispatched_at IS NOT NULL
  AND dl.arrived_at    IS NOT NULL
  AND dl.cleared_at    IS NOT NULL
  AND dl.arrived_at    >= dl.dispatched_at
  AND dl.cleared_at    >= dl.arrived_at
  AND dl.response_time_seconds > 0
