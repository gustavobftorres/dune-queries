-- part of a query repo
-- query name: Balancer Weekly Median Liquidity by Pool Factory
-- query link: https://dune.com/queries/4084220


WITH summed_liquidity AS (
    SELECT
        day,
        factory_version,
        SUM(pool_liquidity_usd) AS total_liquidity
    FROM balancer.liquidity t
    INNER JOIN query_4080393 q
    ON t.pool_address = q.pool_address
    AND t.blockchain = q.blockchain
    WHERE day >= TIMESTAMP '{{1. Start date}}'
    AND ('{{3. Blockchain}}' = 'All' OR t.blockchain = '{{3. Blockchain}}')
    AND ('{{2. Pool Factory}}' = 'All' OR q.factory_version = '{{2. Pool Factory}}')   
    GROUP BY 1, 2
)

SELECT
    DATE_TRUNC('week', day) AS week,
    factory_version,
    APPROX_PERCENTILE(total_liquidity, 0.5) AS liquidity
FROM summed_liquidity s
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;