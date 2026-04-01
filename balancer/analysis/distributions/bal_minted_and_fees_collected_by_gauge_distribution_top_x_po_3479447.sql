-- part of a query repo
-- query name: BAL Minted and Fees Collected by Gauge - Distribution, top x pools by fees collected
-- query link: https://dune.com/queries/3479447


WITH pool_labels AS(
    SELECT
        blockchain,
        address,
        pool_type
    FROM labels.balancer_v2_pools
)

SELECT * 
FROM(
    SELECT
        gauge,
        q.pool_address,
    CONCAT(CASE 
            WHEN q.blockchain = 'arbitrum' THEN ' 🟦 |'
            WHEN q.blockchain = 'avalanche_c' THEN ' ⬜  |'
            WHEN q.blockchain = 'base' THEN ' 🟨 |'
            WHEN q.blockchain = 'ethereum' THEN ' Ξ |'
            WHEN q.blockchain = 'gnosis' THEN ' 🟩 |'
            WHEN q.blockchain = 'optimism' THEN ' 🔴 |'
            WHEN q.blockchain = 'polygon' THEN ' 🟪 |'
            WHEN q.blockchain = 'zkevm' THEN ' 🟣 |'
        END 
        , ' ', SUBSTRING(q.symbol, 5))
         AS symbol,
        sum(monthly_emissions) AS emissions,
        ROUND(sum(monthly_emissions_usd), 2) AS "BAL Emissions (USD)",
        ROUND(tvl, 2) AS "TVL (USD)",
        ROUND(sum(monthly_fees), 2) AS "Fees Collected (USD)",
        ROW_NUMBER() OVER(ORDER BY SUM(monthly_fees) DESC) AS rn
    FROM query_3480969 q
    LEFT JOIN dune.balancer.dataset_core_pools c 
    ON c.network = q.blockchain AND BYTEARRAY_SUBSTRING(c.pool,1,20) = q.pool_address
    LEFT JOIN (SELECT blockchain, pool_address, sum(protocol_liquidity_usd) AS tvl
                FROM balancer.liquidity
                WHERE day = (SELECT MAX(day) FROM balancer.liquidity)
                GROUP BY 1, 2) l
    ON l.blockchain = q.blockchain AND l.pool_address = q.pool_address
    LEFT JOIN pool_labels p 
    ON p.blockchain = q.blockchain AND p.address = q.pool_address
    WHERE  day >= now() - interval '{{Last x Days}}' day
    AND day <= CURRENT_DATE
    AND ('{{Pool Address}}' = 'All' OR CAST(q.pool_address AS VARCHAR) = '{{Pool Address}}')
    AND ('{{Blockchain}}' = 'All' OR q.blockchain = '{{Blockchain}}')
    AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
    AND ('{{Pool Type}}' = 'All' OR p.pool_type = '{{Pool Type}}')
    GROUP BY 1,2,3,6
    HAVING SUM(monthly_emissions) > 100
    ORDER BY 5 DESC)
WHERE rn <= {{Top x Pools}}