-- part of a query repo
-- query name: Mode TVL by Pool, Current Top 5 Pools
-- query link: https://dune.com/queries/3906015


WITH labels AS (
        SELECT
            pool_address AS address,
            pool_symbol AS name
        FROM dune.balancer.dataset_mode_snapshots
        GROUP BY 1, 2
),

tvl AS (
    SELECT
        CAST(day AS TIMESTAMP) AS day,
        SUBSTRING(CAST(pool_id AS VARCHAR), 1, 42) AS pool_id,
        l.name AS pool_symbol,
        CAST(protocol_liquidity_usd AS double) AS tvl
    FROM dune.balancer.dataset_mode_snapshots p
        LEFT JOIN labels l ON p.pool_address = l.address
    WHERE
        CAST(day AS TIMESTAMP) <= TIMESTAMP '{{2. End date}}'
        AND CAST(day AS TIMESTAMP)  >= TIMESTAMP '{{1. Start date}}'
        AND CAST(day AS TIMESTAMP) < CURRENT_DATE
),

total_tvl AS (
    SELECT
        day,
        'Total' AS pool_id,
        SUM(tvl) AS total_tvl
    FROM tvl
    GROUP BY 1, 2
    ORDER BY 1 DESC
),

top_pools AS (
    SELECT
        DISTINCT pool_id,
        t.pool_symbol,
        tvl
    FROM tvl t
    LEFT JOIN labels l ON CAST(t.pool_id AS VARCHAR) = CAST(l.address AS VARCHAR) 
    WHERE CAST(day AS TIMESTAMP) = (SELECT MAX(CAST(day AS TIMESTAMP)) - INTERVAL '1' day FROM dune.balancer.dataset_fraxtal_snapshots)
    ORDER BY 3 DESC
    LIMIT 5
)

SELECT
    CAST(t.day AS TIMESTAMP) AS day,
    p.pool_id,
    CASE
        WHEN p.pool_id IS NOT NULL THEN 
        COALESCE(t.pool_symbol, SUBSTRING(p.pool_id,3,8))
        ELSE '(Others)'
    END AS pool,
    SUM(t.tvl) AS "TVL"
FROM tvl t
LEFT JOIN top_pools p ON p.pool_id = t.pool_id
LEFT JOIN total_tvl tt ON tt.day = t.day
WHERE t.day >= TIMESTAMP '{{1. Start date}}'
GROUP BY 1, 2,3
ORDER BY 1, 4 DESC