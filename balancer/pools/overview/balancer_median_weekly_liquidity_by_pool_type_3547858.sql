-- part of a query repo
-- query name: Balancer Median Weekly Liquidity by Pool Type
-- query link: https://dune.com/queries/3547858


/* Volume per week */
/* Visualization: bar chart */
WITH

liquidity AS (
    SELECT 
        day,
        CONCAT('v', version, ': ', pool_type) AS pool_type,
        CASE WHEN '{{4. Currency}}' = 'USD'
        THEN SUM(pool_liquidity_usd) 
        WHEN '{{4. Currency}}' = 'ETH'
        THEN SUM(pool_liquidity_eth) 
        END AS tvl
    FROM balancer.liquidity t
    WHERE day >= TIMESTAMP '{{1. Start date}}' 
    AND ('{{3. Blockchain}}' = 'All' OR t.blockchain = '{{3. Blockchain}}')
    AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
    AND protocol_liquidity_usd < 1000000000
    GROUP BY 1, 2
)

SELECT
    CAST(DATE_TRUNC('week', day) AS timestamp) AS week,
    pool_type,
    APPROX_PERCENTILE(tvl, 0.5) AS median_liquidity
FROM liquidity
GROUP BY 1, 2
