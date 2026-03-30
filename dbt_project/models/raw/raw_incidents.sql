{{ config(materialized='view') }}

SELECT
    incident_id::VARCHAR          AS incident_id,
    agency::VARCHAR               AS agency,
    incident_type::VARCHAR        AS incident_type,
    reported_at::TIMESTAMP        AS reported_at,
    latitude::DOUBLE              AS latitude,
    longitude::DOUBLE             AS longitude,
    priority::INTEGER             AS priority,
    district::VARCHAR             AS district,
    status::VARCHAR               AS status,
    is_injected::BOOLEAN          AS is_injected
FROM {{ source('public_safety_raw', 'incidents') }}
