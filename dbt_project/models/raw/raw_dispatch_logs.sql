{{ config(materialized='view') }}

SELECT
    log_id::VARCHAR                       AS log_id,
    incident_id::VARCHAR                  AS incident_id,
    unit_id::VARCHAR                      AS unit_id,
    dispatched_at::TIMESTAMP              AS dispatched_at,
    arrived_at::TIMESTAMP                 AS arrived_at,
    cleared_at::TIMESTAMP                 AS cleared_at,
    response_time_seconds::INTEGER        AS response_time_seconds,
    units_available_at_dispatch::INTEGER  AS units_available_at_dispatch
FROM {{ source('public_safety_raw', 'dispatch_logs') }}
