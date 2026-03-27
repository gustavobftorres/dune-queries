-- part of a query repo
-- query name: Daily Total Fee by Pools [Arbitrum only]
-- query link: https://dune.com/queries/3277985


WITH top_pools AS (
    SELECT pool_id, pool_symbol, SUM(total_fee_usd) AS total_fee
    FROM query_3274285
    WHERE day <= TIMESTAMP '{{End date}}'
    AND day >= TIMESTAMP '{{Start date}}'
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 10
)

SELECT
    day,
    COALESCE(t.pool_symbol, 'others') as symbol,
    SUM(total_fee_usd) AS total_fee
FROM query_3274285 q
LEFT JOIN top_pools  t
ON t.pool_id = q.pool_id
WHERE day <= TIMESTAMP '{{End date}}'
AND day >= TIMESTAMP '{{Start date}}'
GROUP BY 1, 2
