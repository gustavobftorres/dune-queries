-- part of a query repo
-- query name: Balancer Median Weekly Liquidity by Version
-- query link: https://dune.com/queries/4116495


/* Volume per week */
/* Visualization: bar chart */
WITH

liquidity AS (
    SELECT 
        day,
        version,
        CASE WHEN '{{4. Currency}}' = 'USD'
        THEN SUM(pool_liquidity_usd) 
        WHEN '{{4. Currency}}' = 'ETH'
        THEN SUM(pool_liquidity_eth) 
        END AS tvl
    FROM balancer.liquidity t
    WHERE day >= TIMESTAMP '{{1. Start date}}' 
    AND ('{{3. Blockchain}}' = 'All' OR t.blockchain = '{{3. Blockchain}}')
    AND protocol_liquidity_usd < 1000000000
    GROUP BY 1, 2
)

SELECT
    CAST(DATE_TRUNC('week', day) AS timestamp) AS week,
    version,
    APPROX_PERCENTILE(tvl, 0.5) AS median_liquidity
FROM liquidity
GROUP BY 1, 2
