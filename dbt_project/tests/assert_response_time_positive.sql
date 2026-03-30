-- Fails (returns rows) if any response time is zero or negative
SELECT *
FROM {{ ref('stg_dispatch_logs') }}
WHERE response_time_seconds <= 0
