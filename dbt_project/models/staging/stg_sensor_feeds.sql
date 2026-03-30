{{ config(materialized='view') }}

SELECT
    feed_id,
    sensor_id,
    sensor_type,
    recorded_at,
    latitude,
    longitude,
    value,
    unit,
    anomaly_flag,
    -- Rolling 24-hour average per sensor
    AVG(value) OVER (
        PARTITION BY sensor_id
        ORDER BY recorded_at
        RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
    ) AS rolling_24h_avg
FROM {{ ref('raw_sensor_feeds') }}
WHERE value    IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
