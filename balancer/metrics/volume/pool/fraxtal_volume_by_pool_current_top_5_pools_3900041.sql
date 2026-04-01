-- part of a query repo
-- query name: Fraxtal Volume by Pool, Current Top 5 Pools
-- query link: https://dune.com/queries/3900041


WITH 
volume AS (
    SELECT
        CAST(day AS TIMESTAMP) AS day,
        SUBSTRING(CAST(pool_id AS VARCHAR), 1, 42) AS pool_id,
        SUM(amount_usd) AS volume
    FROM dune.balancer.dataset_fraxtal_snapshots p
    WHERE
        CAST(day AS TIMESTAMP) <= TIMESTAMP '{{2. End date}}'
        AND CAST(day AS TIMESTAMP) >= TIMESTAMP '{{1. Start date}}'
        GROUP BY 1, 2
),

total_volume AS (
    SELECT
        day,
        'Total' AS pool_id,
        SUM(volume) AS total_volume
    FROM volume
    GROUP BY 1, 2
    ORDER BY 1 DESC
),

top_pools AS (
    SELECT
        DISTINCT pool_id,
        volume
    FROM volume t
    WHERE
        day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
        AND volume IS NOT NULL
    ORDER BY 2 DESC
    LIMIT 5
)

SELECT
    CAST(t.day AS TIMESTAMP) AS day,
    p.pool_id,
    CASE
        WHEN p.pool_id IS NOT NULL THEN SUBSTRING(p.pool_id,3,8)
        ELSE '(Others)'
    END AS pool,
    SUM(t.volume) AS "volume"
FROM volume t
LEFT JOIN top_pools p ON p.pool_id = t.pool_id 
LEFT JOIN total_volume tt ON tt.day = t.day
WHERE t.day >= TIMESTAMP '{{1. Start date}}'
GROUP BY 1, 2,3
ORDER BY 1, 4 DESC