{{ config(materialized='view') }}

SELECT
    feed_id::VARCHAR      AS feed_id,
    sensor_id::VARCHAR    AS sensor_id,
    sensor_type::VARCHAR  AS sensor_type,
    recorded_at::TIMESTAMP AS recorded_at,
    latitude::DOUBLE      AS latitude,
    longitude::DOUBLE     AS longitude,
    value::DOUBLE         AS value,
    unit::VARCHAR         AS unit,
    anomaly_flag::BOOLEAN AS anomaly_flag
FROM {{ source('public_safety_raw', 'sensor_feeds') }}
